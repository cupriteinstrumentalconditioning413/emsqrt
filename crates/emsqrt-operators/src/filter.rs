//! Filter operator with simple predicate evaluation.
//!
//! Supports expressions of the form: "col OP literal" where OP ∈ {==, !=, <, <=, >, >=}

use emsqrt_core::prelude::Schema;
use emsqrt_core::types::{Column, RowBatch, Scalar};

use crate::plan::{Footprint, OpPlan};
use crate::traits::{MemoryBudget, OpError, Operator};

#[derive(Default)]
pub struct Filter {
    /// Simple predicate expression: "column op literal"
    pub expr: Option<String>,
}

impl Operator for Filter {
    fn name(&self) -> &'static str {
        "filter"
    }

    fn memory_need(&self, _rows: u64, _bytes: u64) -> Footprint {
        // Filtering is streaming and should be close to input size.
        Footprint {
            bytes_per_row: 1,
            overhead_bytes: 0,
        }
    }

    fn plan(&self, input_schemas: &[Schema]) -> Result<OpPlan, OpError> {
        let schema = input_schemas
            .get(0)
            .ok_or_else(|| OpError::Plan("filter expects one input".into()))?
            .clone();
        Ok(OpPlan::new(schema, self.memory_need(0, 0)))
    }

    fn eval_block(
        &self,
        inputs: &[RowBatch],
        _budget: &dyn MemoryBudget<Guard = emsqrt_mem::guard::BudgetGuardImpl>,
    ) -> Result<RowBatch, OpError> {
        let input = inputs
            .get(0)
            .ok_or_else(|| OpError::Exec("missing input".into()))?;

        // If no expression, pass through
        let Some(ref expr) = self.expr else {
            return Ok(input.clone());
        };

        // Parse simple expression: "col op literal"
        let (col_name, op, literal) = parse_simple_predicate(expr)?;

        // Find the column
        let col_idx = input
            .columns
            .iter()
            .position(|c| c.name == col_name)
            .ok_or_else(|| OpError::Exec(format!("column '{}' not found", col_name)))?;

        let col = &input.columns[col_idx];

        // Evaluate predicate for each row
        let mut keep = Vec::with_capacity(col.values.len());
        for val in &col.values {
            keep.push(eval_predicate(val, &op, &literal)?);
        }

        // Filter all columns
        let mut filtered_cols = Vec::new();
        for input_col in &input.columns {
            let mut new_col = Column {
                name: input_col.name.clone(),
                values: Vec::new(),
            };
            for (i, val) in input_col.values.iter().enumerate() {
                if keep[i] {
                    new_col.values.push(val.clone());
                }
            }
            filtered_cols.push(new_col);
        }

        Ok(RowBatch {
            columns: filtered_cols,
        })
    }
}

/// Parse a simple predicate like "age > 18" or "name == Alice"
fn parse_simple_predicate(expr: &str) -> Result<(String, String, String), OpError> {
    let ops = ["==", "!=", "<=", ">=", "<", ">"];

    for op in &ops {
        if let Some(pos) = expr.find(op) {
            let col = expr[..pos].trim().to_string();
            let lit = expr[pos + op.len()..].trim().to_string();
            return Ok((col, op.to_string(), lit));
        }
    }

    Err(OpError::Exec(format!("unparseable predicate: {}", expr)))
}

/// Evaluate a simple comparison predicate
fn eval_predicate(val: &Scalar, op: &str, literal: &str) -> Result<bool, OpError> {
    use Scalar::*;

    match val {
        Null => Ok(false), // Null comparisons are false
        Bool(b) => {
            let lit_bool = literal
                .parse::<bool>()
                .map_err(|_| OpError::Exec(format!("cannot parse '{}' as bool", literal)))?;
            match op {
                "==" => Ok(*b == lit_bool),
                "!=" => Ok(*b != lit_bool),
                _ => Err(OpError::Exec(format!("unsupported op '{}' for bool", op))),
            }
        }
        I32(i) => {
            let lit_int = literal
                .parse::<i32>()
                .map_err(|_| OpError::Exec(format!("cannot parse '{}' as i32", literal)))?;
            Ok(match op {
                "==" => *i == lit_int,
                "!=" => *i != lit_int,
                "<" => *i < lit_int,
                "<=" => *i <= lit_int,
                ">" => *i > lit_int,
                ">=" => *i >= lit_int,
                _ => return Err(OpError::Exec(format!("unknown op: {}", op))),
            })
        }
        I64(i) => {
            let lit_int = literal
                .parse::<i64>()
                .map_err(|_| OpError::Exec(format!("cannot parse '{}' as i64", literal)))?;
            Ok(match op {
                "==" => *i == lit_int,
                "!=" => *i != lit_int,
                "<" => *i < lit_int,
                "<=" => *i <= lit_int,
                ">" => *i > lit_int,
                ">=" => *i >= lit_int,
                _ => return Err(OpError::Exec(format!("unknown op: {}", op))),
            })
        }
        F32(f) => {
            let lit_float = literal
                .parse::<f32>()
                .map_err(|_| OpError::Exec(format!("cannot parse '{}' as f32", literal)))?;
            Ok(match op {
                "==" => (*f - lit_float).abs() < f32::EPSILON,
                "!=" => (*f - lit_float).abs() >= f32::EPSILON,
                "<" => *f < lit_float,
                "<=" => *f <= lit_float,
                ">" => *f > lit_float,
                ">=" => *f >= lit_float,
                _ => return Err(OpError::Exec(format!("unknown op: {}", op))),
            })
        }
        F64(f) => {
            let lit_float = literal
                .parse::<f64>()
                .map_err(|_| OpError::Exec(format!("cannot parse '{}' as f64", literal)))?;
            Ok(match op {
                "==" => (*f - lit_float).abs() < f64::EPSILON,
                "!=" => (*f - lit_float).abs() >= f64::EPSILON,
                "<" => *f < lit_float,
                "<=" => *f <= lit_float,
                ">" => *f > lit_float,
                ">=" => *f >= lit_float,
                _ => return Err(OpError::Exec(format!("unknown op: {}", op))),
            })
        }
        Str(s) => {
            // String comparisons
            Ok(match op {
                "==" => s == literal,
                "!=" => s != literal,
                "<" => s.as_str() < literal,
                "<=" => s.as_str() <= literal,
                ">" => s.as_str() > literal,
                ">=" => s.as_str() >= literal,
                _ => return Err(OpError::Exec(format!("unknown op: {}", op))),
            })
        }
        Bin(_) => {
            // Binary data comparisons not supported
            Err(OpError::Exec("cannot filter on binary data".into()))
        }
    }
}
