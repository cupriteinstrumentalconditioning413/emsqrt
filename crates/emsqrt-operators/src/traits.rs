//! Operator trait + common interfaces.
//!
//! The exec runtime will call `plan(...)` to obtain an `OpPlan` (footprints,
//! partition hints), then invoke `eval_block(...)` in TE order.
//!
//! This starter version is synchronous and uses the lightweight `RowBatch`
//! structures from `emsqrt-core`. Later, operators will convert to Arrow arrays
//! internally for performance.

pub use emsqrt_core::budget::MemoryBudget;
use emsqrt_core::prelude::Schema;
use emsqrt_core::types::RowBatch;

use crate::plan::{Footprint, OpPlan};

use thiserror::Error;

/// Minimal "stream of blocks" placeholder for now.
/// The exec crate will replace this with bounded MPMC channels.
pub type BlockStream = Vec<RowBatch>;

#[derive(Debug, Error)]
pub enum OpError {
    #[error("planning error: {0}")]
    Plan(String),

    #[error("execution error: {0}")]
    Exec(String),

    #[error("schema error: {0}")]
    Schema(String),
}

/// Trait that all operators must implement.
///
/// Invariants:
/// - Implementations MUST allocate large buffers only after acquiring a guard
///   from the `MemoryBudget` (via `emsqrt-mem`).
/// - `eval_block` must be deterministic given the same inputs.
pub trait Operator: Send + Sync + 'static {
    /// Human-readable operator name (stable).
    fn name(&self) -> &'static str;

    /// Quick/rough memory footprint model to help TE choose block sizes.
    fn memory_need(&self, rows: u64, bytes: u64) -> Footprint;

    /// Given input schemas, return a concrete plan with output schema and
    /// any partitioning hints. The engine caches this along with the TE plan.
    fn plan(&self, input_schemas: &[Schema]) -> Result<OpPlan, OpError>;

    /// Evaluate one TE block worth of data.
    ///
    /// For unary ops, pass `inputs[0]`. For binary ops (joins), pass two inputs
    /// with aligned block ranges according to the TE plan.
    fn eval_block(
        &self,
        inputs: &[RowBatch],
        budget: &dyn MemoryBudget<Guard = emsqrt_mem::guard::BudgetGuardImpl>,
    ) -> Result<RowBatch, OpError>;
}
