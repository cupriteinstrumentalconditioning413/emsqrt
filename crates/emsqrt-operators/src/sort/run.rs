//! Run generation utilities for external sort.
//!
//! Accumulates rows in memory (up to budget), sorts them, and writes to spill.

use emsqrt_core::budget::MemoryBudget;
use emsqrt_core::id::SpillId;
use emsqrt_core::types::RowBatch;
use emsqrt_mem::guard::BudgetGuardImpl;
use emsqrt_mem::spill::SegmentMeta;
use emsqrt_mem::SpillManager;

use crate::traits::OpError;

/// Configuration for run generation.
#[derive(Default)]
pub struct RunGenConfig {
    pub max_in_mem_rows: usize,
}

/// Metadata for a sorted run on disk.
#[derive(Clone, Debug)]
pub struct RunMeta {
    pub rows: u64,
    pub segment: SegmentMeta,
}

/// Generator for sorted runs.
///
/// Accumulates rows in memory, sorts when capacity reached, writes to spill.
pub struct RunGenerator {
    spill_id: SpillId,
    sort_keys: Vec<String>,
    accumulator: Vec<RowBatch>,
    accum_rows: usize,
    max_rows: usize,
    runs: Vec<RunMeta>,
}

impl RunGenerator {
    pub fn new(spill_id: SpillId, sort_keys: Vec<String>, max_rows: usize) -> Self {
        Self {
            spill_id,
            sort_keys,
            accumulator: Vec::new(),
            accum_rows: 0,
            max_rows,
            runs: Vec::new(),
        }
    }

    /// Add a batch to the accumulator. Flushes if capacity exceeded.
    pub fn add_batch(
        &mut self,
        batch: RowBatch,
        spill_mgr: &mut SpillManager,
        budget: &dyn MemoryBudget<Guard = BudgetGuardImpl>,
    ) -> Result<(), OpError> {
        let batch_rows = batch.num_rows();
        self.accumulator.push(batch);
        self.accum_rows += batch_rows;

        if self.accum_rows >= self.max_rows {
            self.flush_run(spill_mgr, budget)?;
        }

        Ok(())
    }

    /// Flush the current accumulator to a sorted run on disk.
    fn flush_run(
        &mut self,
        spill_mgr: &mut SpillManager,
        _budget: &dyn MemoryBudget<Guard = BudgetGuardImpl>,
    ) -> Result<(), OpError> {
        if self.accumulator.is_empty() {
            return Ok(());
        }

        // Concatenate all accumulated batches into one
        let mut merged = RowBatch {
            columns: self.accumulator[0].columns.clone(),
        };

        for batch in &self.accumulator[1..] {
            for (i, col) in batch.columns.iter().enumerate() {
                if i < merged.columns.len() {
                    merged.columns[i].values.extend(col.values.iter().cloned());
                }
            }
        }

        // Sort the merged batch
        merged
            .sort_by_columns(&self.sort_keys)
            .map_err(|e| OpError::Exec(format!("sort failed: {}", e)))?;

        // Write to spill
        let run_index = spill_mgr.next_run_index();
        let segment = spill_mgr
            .write_batch(&merged, self.spill_id, run_index)
            .map_err(|e| OpError::Exec(format!("spill write: {}", e)))?;

        let run_meta = RunMeta {
            rows: merged.num_rows() as u64,
            segment,
        };

        self.runs.push(run_meta);

        // Clear accumulator
        self.accumulator.clear();
        self.accum_rows = 0;

        Ok(())
    }

    /// Finalize run generation by flushing any remaining rows.
    pub fn finalize(
        &mut self,
        spill_mgr: &mut SpillManager,
        budget: &dyn MemoryBudget<Guard = BudgetGuardImpl>,
    ) -> Result<Vec<RunMeta>, OpError> {
        self.flush_run(spill_mgr, budget)?;
        Ok(self.runs.clone())
    }
}
