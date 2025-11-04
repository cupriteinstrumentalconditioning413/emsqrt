//! Tree-Evaluation (TE) block descriptors used by the scheduler and engine.
//!
//! TE walks blocks in a bounded-fan-in order to cap the live frontier.

use serde::{Deserialize, Serialize};
use std::ops::Range;

use crate::id::BlockId;

/// Row/byte range that a block covers (planner fills this).
pub type BlockRange = Range<u64>;

/// Dependencies for a block (parents must complete first).
pub type BlockDeps = Vec<BlockId>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Block {
    pub id: BlockId,
    /// Upstream block dependencies (fan-in kept small by TE).
    pub deps: BlockDeps,
    /// Optional data range for provenance (rows or byte offsets).
    pub range: Option<BlockRange>,
    /// Optional footprint estimation for scheduling/budgeting.
    pub est_footprint_bytes: Option<u64>,
}

impl Block {
    pub fn new(id: BlockId) -> Self {
        Self {
            id,
            deps: vec![],
            range: None,
            est_footprint_bytes: None,
        }
    }
}
