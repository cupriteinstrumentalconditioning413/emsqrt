//! External sort operator with run generation and k-way merge.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::{Arc, Mutex};

use emsqrt_core::budget::MemoryBudget;
use emsqrt_core::id::SpillId;
use emsqrt_core::prelude::Schema;
use emsqrt_core::types::{RowBatch, Scalar};
use emsqrt_mem::guard::BudgetGuardImpl;
use emsqrt_mem::SpillManager;

use crate::plan::{Footprint, OpPlan};
use crate::traits::{OpError, Operator};

use super::run::{RunGenerator, RunMeta};

/// External sort operator.
///
/// For small inputs (fits in memory), sorts in-place.
/// For large inputs, generates sorted runs and performs k-way merge.
pub struct ExternalSort {
    pub by: Vec<String>, // sort keys
    pub spill_mgr: Option<Arc<Mutex<SpillManager>>>,
}

impl Default for ExternalSort {
    fn default() -> Self {
        Self {
            by: Vec::new(),
            spill_mgr: None,
        }
    }
}

impl Operator for ExternalSort {
    fn name(&self) -> &'static str {
        "sort_external"
    }

    fn memory_need(&self, _rows: u64, _bytes: u64) -> Footprint {
        // Heuristic: overhead for heap + buffers; refine later.
        Footprint {
            bytes_per_row: 1,
            overhead_bytes: 256 * 1024,
        }
    }

    fn plan(&self, input_schemas: &[Schema]) -> Result<OpPlan, OpError> {
        let schema = input_schemas
            .get(0)
            .ok_or_else(|| OpError::Plan("sort expects one input".into()))?
            .clone();
        Ok(OpPlan::new(schema, self.memory_need(0, 0)).with_partitions(self.by.clone()))
    }

    fn eval_block(
        &self,
        inputs: &[RowBatch],
        budget: &dyn MemoryBudget<Guard = BudgetGuardImpl>,
    ) -> Result<RowBatch, OpError> {
        let input = inputs
            .get(0)
            .ok_or_else(|| OpError::Exec("missing input".into()))?;

        // If no spill manager, do in-memory sort only
        if self.spill_mgr.is_none() {
            let mut batch = input.clone();
            batch
                .sort_by_columns(&self.by)
                .map_err(|e| OpError::Exec(format!("in-memory sort: {}", e)))?;
            return Ok(batch);
        }

        let spill_mgr = self.spill_mgr.as_ref().unwrap();
        let mut spill_mgr = spill_mgr.lock().unwrap();

        // Generate a unique spill ID for this sort operation
        // In production, this would come from a global counter or UUID
        let spill_id = SpillId::new(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64);

        // For simplicity in this single-batch operator, treat input as one run
        // In a real pipeline, we'd accumulate multiple blocks
        let max_rows_per_run = 10000; // Configurable threshold
        let mut gen = RunGenerator::new(spill_id, self.by.clone(), max_rows_per_run);

        gen.add_batch(input.clone(), &mut spill_mgr, budget)?;
        let runs = gen.finalize(&mut spill_mgr, budget)?;

        // If only one run, just read it back (already sorted)
        if runs.len() <= 1 {
            if let Some(run) = runs.first() {
                let batch = spill_mgr
                    .read_batch(&run.segment, budget)
                    .map_err(|e| OpError::Exec(format!("read run: {}", e)))?;
                return Ok(batch);
            }
            // No runs means empty input
            return Ok(RowBatch {
                columns: input.columns.iter().map(|c| emsqrt_core::types::Column {
                    name: c.name.clone(),
                    values: Vec::new(),
                }).collect(),
            });
        }

        // K-way merge
        k_way_merge(runs, &self.by, &mut spill_mgr, budget)
    }
}

/// Perform k-way merge of sorted runs using a min-heap.
///
/// Each run is read batch-by-batch, and we maintain a heap of
/// (current_value, run_idx, row_idx_within_batch).
fn k_way_merge(
    runs: Vec<RunMeta>,
    sort_keys: &[String],
    spill_mgr: &mut SpillManager,
    budget: &dyn MemoryBudget<Guard = BudgetGuardImpl>,
) -> Result<RowBatch, OpError> {
    // Read all runs into memory (for simplicity; real impl would stream)
    let mut run_batches: Vec<RowBatch> = Vec::new();
    for run in &runs {
        let batch = spill_mgr
            .read_batch(&run.segment, budget)
            .map_err(|e| OpError::Exec(format!("read run for merge: {}", e)))?;
        run_batches.push(batch);
    }

    if run_batches.is_empty() {
        return Err(OpError::Exec("no runs to merge".into()));
    }

    // Build a min-heap of (sort_key_tuple, run_idx, row_idx)
    let mut heap: BinaryHeap<MergeEntry> = BinaryHeap::new();

    // Initialize heap with first row from each run
    for (run_idx, batch) in run_batches.iter().enumerate() {
        if batch.num_rows() > 0 {
            let sort_tuple = extract_sort_tuple(batch, 0, sort_keys)?;
            heap.push(MergeEntry {
                sort_tuple,
                run_idx,
                row_idx: 0,
            });
        }
    }

    // Prepare output columns
    let template = &run_batches[0];
    let mut output_cols: Vec<emsqrt_core::types::Column> = template
        .columns
        .iter()
        .map(|c| emsqrt_core::types::Column {
            name: c.name.clone(),
            values: Vec::new(),
        })
        .collect();

    // Merge loop
    while let Some(entry) = heap.pop() {
        let batch = &run_batches[entry.run_idx];

        // Append this row to output
        for (col_idx, col) in batch.columns.iter().enumerate() {
            if col_idx < output_cols.len() {
                output_cols[col_idx]
                    .values
                    .push(col.values[entry.row_idx].clone());
            }
        }

        // Advance to next row in this run
        let next_row = entry.row_idx + 1;
        if next_row < batch.num_rows() {
            let sort_tuple = extract_sort_tuple(batch, next_row, sort_keys)?;
            heap.push(MergeEntry {
                sort_tuple,
                run_idx: entry.run_idx,
                row_idx: next_row,
            });
        }
    }

    Ok(RowBatch {
        columns: output_cols,
    })
}

/// Extract sort key tuple for a given row.
fn extract_sort_tuple(
    batch: &RowBatch,
    row_idx: usize,
    sort_keys: &[String],
) -> Result<Vec<Scalar>, OpError> {
    let mut tuple = Vec::with_capacity(sort_keys.len());
    for key in sort_keys {
        let col = batch
            .columns
            .iter()
            .find(|c| &c.name == key)
            .ok_or_else(|| OpError::Exec(format!("sort key '{}' not found", key)))?;
        tuple.push(col.values[row_idx].clone());
    }
    Ok(tuple)
}

/// Entry in the merge heap.
///
/// Ordered by sort tuple (reversed for min-heap behavior).
#[derive(Debug, Clone)]
struct MergeEntry {
    sort_tuple: Vec<Scalar>,
    run_idx: usize,
    row_idx: usize,
}

impl PartialEq for MergeEntry {
    fn eq(&self, other: &Self) -> bool {
        self.sort_tuple == other.sort_tuple
    }
}

impl Eq for MergeEntry {}

impl PartialOrd for MergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MergeEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse order for min-heap
        compare_scalar_tuples(&other.sort_tuple, &self.sort_tuple)
    }
}

/// Compare two scalar tuples for ordering.
fn compare_scalar_tuples(a: &[Scalar], b: &[Scalar]) -> Ordering {
    for (x, y) in a.iter().zip(b.iter()) {
        match compare_scalars(x, y) {
            Ordering::Equal => continue,
            other => return other,
        }
    }
    a.len().cmp(&b.len())
}

/// Compare two scalars (simplified version).
fn compare_scalars(a: &Scalar, b: &Scalar) -> Ordering {
    use Scalar::*;
    match (a, b) {
        (Null, Null) => Ordering::Equal,
        (Null, _) => Ordering::Less,
        (_, Null) => Ordering::Greater,
        (Bool(x), Bool(y)) => x.cmp(y),
        (I32(x), I32(y)) => x.cmp(y),
        (I64(x), I64(y)) => x.cmp(y),
        (F32(x), F32(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        (F64(x), F64(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        (Str(x), Str(y)) => x.cmp(y),
        (Bin(x), Bin(y)) => x.cmp(y),
        _ => Ordering::Equal, // Mixed types: treat as equal for simplicity
    }
}
