#![forbid(unsafe_code)]
//! emsqrt-planner: from (YAML) logical pipelines → optimized logical plan
//! → physical plan + operator bindings + coarse WorkEstimate.
//!
//! Design:
//! - We reuse `emsqrt-core::dag::{LogicalPlan, PhysicalPlan}` node enums.
//! - This crate adds:
//!     * tiny DSL → `LogicalPlan`
//!     * a placeholder optimization pass (`rules`)
//!     * a physical lowering that assigns `OpId`s and operator *keys*
//!       (strings; exec will instantiate via `emsqrt-operators::registry`)
//!     * a coarse `WorkEstimate` for TE block sizing
//!
//! NOTE: We deliberately avoid pulling heavy dependencies (no Arrow/IO here).

pub mod cost;
pub mod dsl;
pub mod logical;
pub mod lower;
pub mod physical;
pub mod rules;

pub use cost::{estimate_work, WorkHint};
pub use dsl::yaml::parse_yaml_pipeline;
pub use logical::{Aggregation, JoinType, LogicalPlan};
pub use lower::lower_to_physical;
pub use physical::{OperatorBinding, PhysicalProgram};
