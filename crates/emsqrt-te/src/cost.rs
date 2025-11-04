//! Very lightweight cost model used by the TE planner.
//!
//! TODOs:
//! - Enrich with per-operator footprints (from emsqrt-operators).
//! - Add calibration hooks (profile → cost updates).
//!
//! For now we estimate work in terms of rows, bytes, and a fan-in bound.

use serde::{Deserialize, Serialize};

/// Cumulative cost characteristics for a subgraph/node.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct NodeCost {
    /// Estimated input rows flowing through the node/block.
    pub rows: u64,
    /// Estimated input bytes flowing through the node/block.
    pub bytes: u64,
    /// Upper bound on immediate dependencies (fan-in).
    pub fan_in: u32,
}

/// Summary for the entire plan (used to pick `b`).
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct WorkEstimate {
    /// Total rows across the pipeline (approximate).
    pub total_rows: u64,
    /// Total bytes across the pipeline (approximate).
    pub total_bytes: u64,
    /// Max fan-in observed (helps bound frontier).
    pub max_fan_in: u32,
}

impl WorkEstimate {
    pub fn from_node_costs(costs: &[NodeCost]) -> Self {
        let mut total_rows = 0;
        let mut total_bytes = 0;
        let mut max_fan_in = 0;
        for c in costs {
            total_rows += c.rows;
            total_bytes += c.bytes;
            max_fan_in = max_fan_in.max(c.fan_in);
        }
        Self {
            total_rows,
            total_bytes,
            max_fan_in,
        }
    }

    /// Combine two work estimates (for merging subgraphs).
    pub fn combine(a: Self, b: Self) -> Self {
        Self {
            total_rows: a.total_rows + b.total_rows,
            total_bytes: a.total_bytes + b.total_bytes,
            max_fan_in: a.max_fan_in.max(b.max_fan_in),
        }
    }
}
