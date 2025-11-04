//! Logical plan surface re-exported from core, plus light helpers.
//!
//! We intentionally alias the core AST to avoid duplication/forking.

pub use emsqrt_core::dag::{Aggregation, JoinType, LogicalPlan};
pub use emsqrt_core::schema::{DataType, Field, Schema};

// Note: LogicalPlan helpers are now in emsqrt-core/dag.rs
// We cannot implement methods on LogicalPlan here since it's defined in emsqrt-core
