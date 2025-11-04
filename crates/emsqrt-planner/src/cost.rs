//! Coarse work estimation for TE planning.
//!
//! We compute a very rough `WorkEstimate` by walking the logical plan. In real
//! deployments, this should be informed by stats (filesize, row count) and/or
//! operator-specific models.

use emsqrt_core::dag::LogicalPlan;
use emsqrt_core::schema::Schema;
use emsqrt_te::WorkEstimate;
use serde::{Deserialize, Serialize};

/// Optional hints you can pass in when estimating work.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkHint {
    /// Rows at sources (if known); map by source URI.
    pub source_rows: Vec<(String, u64)>,
    /// Bytes at sources (if known); map by source URI.
    pub source_bytes: Vec<(String, u64)>,
}

/// Estimate filter selectivity based on expression pattern.
fn estimate_filter_selectivity(expr: &str) -> f64 {
    // Parse simple patterns to estimate selectivity
    if expr.contains('=') && !expr.contains("!=") {
        // Equality: assume moderate selectivity
        0.1 // 10% of rows pass
    } else if expr.contains("!=") {
        // Not-equal: high selectivity
        0.9 // 90% of rows pass
    } else if expr.contains('>') || expr.contains('<') {
        // Range predicates: moderate selectivity
        0.33 // ~33% of rows pass
    } else if expr.contains("IS NULL") {
        // Null checks: low selectivity (most data is not null)
        0.05 // 5% of rows
    } else if expr.contains("IS NOT NULL") {
        // Not null checks: high selectivity
        0.95 // 95% of rows pass
    } else {
        // Default: 50% selectivity for unknown predicates
        0.5
    }
}

/// Estimate join cardinality based on join type and input sizes.
fn estimate_join_cardinality(left_rows: u64, right_rows: u64, join_type: &emsqrt_core::dag::JoinType) -> u64 {
    use emsqrt_core::dag::JoinType;
    
    match join_type {
        JoinType::Inner => {
            // Inner join: assume some correlation, output is fraction of cross product
            // Heuristic: sqrt(L * R) for many-to-many, min(L,R) for one-to-many
            let cross_product = (left_rows as f64 * right_rows as f64).sqrt();
            (cross_product as u64).max(1).min(left_rows.min(right_rows))
        }
        JoinType::Left => {
            // Left join: at least all left rows, possibly more if right has duplicates
            (left_rows as f64 * 1.2) as u64 // 20% inflation for duplicates
        }
        JoinType::Right => {
            // Right join: at least all right rows
            (right_rows as f64 * 1.2) as u64
        }
        JoinType::Full => {
            // Full outer: at least max of both sides
            (left_rows.max(right_rows) as f64 * 1.5) as u64
        }
    }
}

/// Estimate number of groups in an aggregation.
fn estimate_aggregate_groups(input_rows: u64, num_group_keys: usize) -> u64 {
    if num_group_keys == 0 {
        // No group by: single aggregate
        return 1;
    }
    
    // Heuristic based on number of grouping columns
    // More columns → more groups, but with diminishing returns
    let cardinality_factor = match num_group_keys {
        1 => 0.1,  // Single column: ~10% unique values
        2 => 0.25, // Two columns: ~25% unique combinations
        3 => 0.4,  // Three columns: ~40% unique combinations
        _ => 0.5,  // Many columns: ~50% unique combinations
    };
    
    ((input_rows as f64 * cardinality_factor) as u64).max(1).min(input_rows)
}

pub fn estimate_work(plan: &LogicalPlan, hints: Option<&WorkHint>) -> WorkEstimate {
    let mut total_rows = 0u64;
    let mut total_bytes = 0u64;
    let mut max_fan_in = 1u32;

    fn schema_size_bytes(schema: &Schema) -> u64 {
        let mut total = 0u64;
        for field in &schema.fields {
            total += match field.data_type {
                emsqrt_core::schema::DataType::Boolean => 1,
                emsqrt_core::schema::DataType::Int32 | emsqrt_core::schema::DataType::Float32 => 4,
                emsqrt_core::schema::DataType::Int64 
                | emsqrt_core::schema::DataType::Float64 
                | emsqrt_core::schema::DataType::Date64 => 8,
                emsqrt_core::schema::DataType::Utf8 => 32, // Average string size estimate
                emsqrt_core::schema::DataType::Binary => 64, // Average binary size estimate
                emsqrt_core::schema::DataType::Decimal128 => 16,
            };
        }
        total.max(1) // Minimum 1 byte per row
    }

    fn walk(
        lp: &LogicalPlan,
        hints: Option<&WorkHint>,
        acc_rows: &mut u64,
        acc_bytes: &mut u64,
        max_fan_in: &mut u32,
    ) -> u64 {
        use LogicalPlan::*;
        match lp {
            Scan { source, schema } => {
                // Use hints if available; otherwise guess 0 (unknown).
                let rows = hints
                    .and_then(|h| h.source_rows.iter().find(|(s, _)| s == source))
                    .map(|(_, r)| *r)
                    .unwrap_or(0);

                let bytes = hints
                    .and_then(|h| h.source_bytes.iter().find(|(s, _)| s == source))
                    .map(|(_, b)| *b)
                    .unwrap_or(rows * schema_size_bytes(schema));

                *acc_rows += rows;
                *acc_bytes += bytes;
                rows
            }
            Filter { input, expr } => {
                let in_rows = walk(input, hints, acc_rows, acc_bytes, max_fan_in);
                // Estimate selectivity based on expression type
                let selectivity = estimate_filter_selectivity(expr);
                ((in_rows as f64 * selectivity) as u64).max(1)
            }
            Map { input, .. } | Project { input, .. } => {
                walk(input, hints, acc_rows, acc_bytes, max_fan_in)
            }
            Join { left, right, join_type, .. } => {
                *max_fan_in = (*max_fan_in).max(2);
                let l = walk(left, hints, acc_rows, acc_bytes, max_fan_in);
                let r = walk(right, hints, acc_rows, acc_bytes, max_fan_in);
                // Estimate join cardinality based on join type
                estimate_join_cardinality(l, r, join_type)
            }
            Aggregate { input, group_by, .. } => {
                let in_rows = walk(input, hints, acc_rows, acc_bytes, max_fan_in);
                // Estimate number of groups based on cardinality
                estimate_aggregate_groups(in_rows, group_by.len())
            }
            Sink { input, .. } => walk(input, hints, acc_rows, acc_bytes, max_fan_in),
        }
    }

    let _rows_out = walk(
        plan,
        hints,
        &mut total_rows,
        &mut total_bytes,
        &mut max_fan_in,
    );
    WorkEstimate {
        total_rows,
        total_bytes,
        max_fan_in,
    }
}
