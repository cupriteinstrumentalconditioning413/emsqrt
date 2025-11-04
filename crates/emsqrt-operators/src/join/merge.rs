//! Merge join (starter).
//!
//! Precondition: inputs are pre-sorted on the join keys (enforced by planner/TE).
//! TODOs:
//! - Implement lane-wise merge with bounded lookahead windows.

use emsqrt_core::prelude::Schema;
use emsqrt_core::types::RowBatch;

use crate::plan::{Footprint, OpPlan};
use crate::traits::{MemoryBudget, OpError, Operator};

#[derive(Default)]
pub struct MergeJoin {
    pub on: Vec<(String, String)>,
    pub join_type: String,
}

impl Operator for MergeJoin {
    fn name(&self) -> &'static str {
        "join_merge"
    }

    fn memory_need(&self, _rows: u64, _bytes: u64) -> Footprint {
        // Merge join is streaming; small overhead for buffers.
        Footprint {
            bytes_per_row: 1,
            overhead_bytes: 64 * 1024,
        }
    }

    fn plan(&self, input_schemas: &[Schema]) -> Result<OpPlan, OpError> {
        if input_schemas.len() != 2 {
            return Err(OpError::Plan("merge join expects two inputs".into()));
        }
        let out = input_schemas[0].clone();
        Ok(OpPlan::new(out, self.memory_need(0, 0)))
    }

    fn eval_block(
        &self,
        inputs: &[RowBatch],
        _budget: &dyn MemoryBudget<Guard = emsqrt_mem::guard::BudgetGuardImpl>,
    ) -> Result<RowBatch, OpError> {
        if inputs.len() != 2 {
            return Err(OpError::Exec("merge join needs two block inputs".into()));
        }
        // TODO: implement streaming merge of sorted inputs.
        Ok(inputs[0].clone())
    }
}
