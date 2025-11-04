#![forbid(unsafe_code)]
//! emsqrt-te: Tree Evaluation (TE) planner/scheduler (math → code).
//!
//! Responsibilities:
//! - Convert a `PhysicalPlan` into TE blocks/order under a memory cap.
//! - Choose a block size `b` using a simple cost model (rows/bytes/fan-in).
//! - Track/limit the live frontier (bounded fan-in → bounded memory).
//! - Provide debug-time verification helpers.
//!
//! **No I/O, no alloc policy, no async** here. The exec crate drives this plan.

pub mod cost;
pub mod frontier;
pub mod schedule;
pub mod tree_eval;
pub mod verify;

// Optional pebbling module not yet implemented
// #[cfg(feature = "pebble")]
// pub mod pebbling;

pub use cost::{NodeCost, WorkEstimate};
pub use schedule::{choose_block_size, BlockSizeHint};
pub use tree_eval::{plan_te, TeBlock, TePlan};
