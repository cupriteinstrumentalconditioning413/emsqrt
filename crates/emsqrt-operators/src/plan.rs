//! Operator planning surfaces: `Footprint` and `OpPlan`.
//!
//! These are lightweight; the real "how many bytes live" math will become more
//! precise as we add Arrow/columnar details.

use emsqrt_core::prelude::Schema;
use serde::{Deserialize, Serialize};

/// Coarse memory model for a block flowing through an operator.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct Footprint {
    /// Estimated bytes per row through this operator (very rough starter).
    pub bytes_per_row: u64,
    /// Estimated additional overhead (hash tables, heap) per block.
    pub overhead_bytes: u64,
}

impl Footprint {
    /// Estimate total live bytes for a block of `rows`/`bytes` at this operator.
    pub fn estimate_live(&self, rows: u64, _bytes: u64) -> u64 {
        self.overhead_bytes + self.bytes_per_row.saturating_mul(rows)
    }
}

/// Operator plan: output schema, partitions, and a cached footprint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpPlan {
    pub output_schema: Schema,

    /// Optional partitioning columns (by name) used by multi-pass joins/sort.
    pub partitions: Vec<String>,

    /// Footprint model cached to avoid recomputation.
    pub footprint: Footprint,
}

impl OpPlan {
    pub fn new(output_schema: Schema, footprint: Footprint) -> Self {
        Self {
            output_schema,
            partitions: vec![],
            footprint,
        }
    }

    pub fn with_partitions(mut self, cols: Vec<String>) -> Self {
        self.partitions = cols;
        self
    }
}
