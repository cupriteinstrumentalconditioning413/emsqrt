//! Map operator with column renaming support.

use emsqrt_core::prelude::Schema;
use emsqrt_core::types::{Column, RowBatch};
use std::collections::HashMap;

use crate::plan::{Footprint, OpPlan};
use crate::traits::{MemoryBudget, OpError, Operator};

#[derive(Default)]
pub struct Map {
    /// Column rename map: old_name -> new_name
    pub renames: HashMap<String, String>,
}

impl Operator for Map {
    fn name(&self) -> &'static str {
        "map"
    }

    fn memory_need(&self, _rows: u64, _bytes: u64) -> Footprint {
        // Assume similar to input; adjust when adding real transform costs.
        Footprint {
            bytes_per_row: 1,
            overhead_bytes: 0,
        }
    }

    fn plan(&self, input_schemas: &[Schema]) -> Result<OpPlan, OpError> {
        let mut schema = input_schemas
            .get(0)
            .ok_or_else(|| OpError::Plan("map expects one input".into()))?
            .clone();

        // Apply renames to the schema
        for field in &mut schema.fields {
            if let Some(new_name) = self.renames.get(&field.name) {
                field.name = new_name.clone();
            }
        }

        Ok(OpPlan::new(schema, self.memory_need(0, 0)))
    }

    fn eval_block(
        &self,
        inputs: &[RowBatch],
        _budget: &dyn MemoryBudget<Guard = emsqrt_mem::guard::BudgetGuardImpl>,
    ) -> Result<RowBatch, OpError> {
        let input = inputs
            .get(0)
            .ok_or_else(|| OpError::Exec("missing input".into()))?;

        // If no renames, pass through
        if self.renames.is_empty() {
            return Ok(input.clone());
        }

        // Apply renames to columns
        let mut renamed_cols = Vec::new();
        for col in &input.columns {
            let new_name = self
                .renames
                .get(&col.name)
                .cloned()
                .unwrap_or_else(|| col.name.clone());
            renamed_cols.push(Column {
                name: new_name,
                values: col.values.clone(),
            });
        }

        Ok(RowBatch {
            columns: renamed_cols,
        })
    }
}
