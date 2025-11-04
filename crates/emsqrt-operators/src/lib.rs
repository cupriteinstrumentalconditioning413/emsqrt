#![forbid(unsafe_code)]
//! emsqrt-operators: TE-friendly operators (filter/map/project/agg/sort/join).
//!
//! Design intent:
//! - Keep this crate pure and synchronous for now (no async).
//! - All "big" allocations MUST go through `emsqrt-mem` (via guards/pool).
//! - Each operator exposes a planning surface (`OpPlan`) with an estimated
//    footprint model so TE can choose block sizes and the engine can enforce caps.

pub mod plan;
pub mod registry;
pub mod traits;

pub mod agregate;
pub mod filter;
pub mod map;
pub mod project;

pub mod join;
pub mod sort;

pub use plan::{Footprint, OpPlan};
pub use traits::{BlockStream, OpError, Operator};
