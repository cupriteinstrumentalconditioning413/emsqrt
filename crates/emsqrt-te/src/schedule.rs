//! Block-size selection logic.
//!
//! Inputs:
//! - Engine memory cap (bytes).
//! - `WorkEstimate` (rows/bytes/fan-in).
//!
//! Output:
//! - A block-size hint `b` that downstream planning uses to shape TE blocks.
//!
//! NOTE: This is intentionally simple starter logic. Replace with a proper
//! √(work) analysis once operator footprints are known.

use crate::cost::WorkEstimate;
use serde::{Deserialize, Serialize};

/// Block size hint (rows) used by TE planning.
/// The planner may still adjust per-stage.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct BlockSizeHint {
    pub rows_per_block: u64,
}

pub fn choose_block_size(mem_cap_bytes: usize, work: &WorkEstimate) -> BlockSizeHint {
    // Target block payload ≈ mem_cap/(K*max_fan_in+1) with K≈2..4
    // This ensures that with bounded fan-in, we can keep K blocks of each operator
    // in memory without exceeding the cap.

    let k = 3.0; // Constant factor for buffering
    let max_fan_in = (work.max_fan_in as f64).max(1.0);
    let divisor = (k * max_fan_in + 1.0).max(1.0);
    let target_block_bytes = (mem_cap_bytes as f64 / divisor).max(1.0);

    let rows_per_block = if work.total_bytes > 0 && work.total_rows > 0 {
        // Derive rows_per_block from bytes/row estimate
        let bytes_per_row = (work.total_bytes as f64 / work.total_rows as f64).max(1.0);
        let rows = (target_block_bytes / bytes_per_row).max(1.0) as u64;
        rows.clamp(1, work.total_rows.max(1))
    } else {
        // Fallback: sqrt(total_rows) (coarse heuristic)
        (f64::sqrt(work.total_rows as f64).max(1.0)) as u64
    };

    BlockSizeHint {
        rows_per_block: rows_per_block.max(1),
    }
}
