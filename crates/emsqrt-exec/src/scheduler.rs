//! DAG scheduler primitives for parallel TE execution.
//!
//! The AsyncScheduler enables concurrent execution of independent TE blocks
//! while respecting dependencies and memory budget constraints.

#[cfg(not(feature = "async-scheduler"))]
pub use sync_impl::*;

#[cfg(feature = "async-scheduler")]
pub use async_impl::*;

/// Synchronous scheduler implementation (default, no tokio dependency)
#[cfg(not(feature = "async-scheduler"))]
mod sync_impl {
    use std::collections::VecDeque;

    /// A tiny bounded queue used as a placeholder for future mpsc channels.
    /// Replace with `tokio::sync::mpsc` or crossbeam once we go async.
    pub struct BoundedQueue<T> {
        cap: usize,
        q: VecDeque<T>,
    }

    impl<T> BoundedQueue<T> {
        pub fn with_capacity(cap: usize) -> Self {
            Self {
                cap: cap.max(1),
                q: VecDeque::new(),
            }
        }

        pub fn try_push(&mut self, v: T) -> Result<(), T> {
            if self.q.len() >= self.cap {
                Err(v)
            } else {
                self.q.push_back(v);
                Ok(())
            }
        }

        pub fn try_pop(&mut self) -> Option<T> {
            self.q.pop_front()
        }

        pub fn len(&self) -> usize {
            self.q.len()
        }
        pub fn is_empty(&self) -> bool {
            self.q.is_empty()
        }
    }
}

/// Asynchronous scheduler implementation (requires async-scheduler feature)
#[cfg(feature = "async-scheduler")]
mod async_impl {
    use std::collections::{HashMap, HashSet};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    
    use tokio::sync::{mpsc, Semaphore, RwLock};
    
    use emsqrt_core::id::BlockId;
    use emsqrt_core::types::RowBatch;
    use emsqrt_mem::guard::MemoryBudgetImpl;
    use emsqrt_operators::traits::Operator;
    
    use emsqrt_te::tree_eval::TeBlock;
    
    /// Result channel for a block execution
    type BlockResult = Result<RowBatch, String>;
    
    /// Async scheduler for parallel TE execution
    pub struct AsyncScheduler {
        /// Tokio runtime handle
        runtime: tokio::runtime::Handle,
        /// Memory budget (shared across tasks)
        budget: Arc<MemoryBudgetImpl>,
        /// Maximum parallel tasks
        max_parallel: usize,
        /// Semaphore to limit concurrency
        semaphore: Arc<Semaphore>,
        /// Active task counter
        active_tasks: Arc<AtomicUsize>,
    }
    
    impl AsyncScheduler {
        /// Create a new async scheduler
        pub fn new(budget: Arc<MemoryBudgetImpl>, max_parallel: usize) -> Self {
            let runtime = tokio::runtime::Handle::current();
            Self {
                runtime,
                budget,
                max_parallel,
                semaphore: Arc::new(Semaphore::new(max_parallel)),
                active_tasks: Arc::new(AtomicUsize::new(0)),
            }
        }
        
        /// Execute a DAG of TE blocks in parallel
        pub async fn execute_dag(
            &self,
            blocks: Vec<TeBlock>,
            operators: Arc<RwLock<HashMap<u64, Box<dyn Operator>>>>,
        ) -> Result<HashMap<BlockId, RowBatch>, String> {
            // Build dependency tracking structures
            let mut dep_counts: HashMap<BlockId, usize> = HashMap::new();
            let mut dependents: HashMap<BlockId, Vec<BlockId>> = HashMap::new();
            
            for block in &blocks {
                dep_counts.insert(block.id, block.deps.len());
                for dep_id in &block.deps {
                    dependents.entry(*dep_id).or_default().push(block.id);
                }
            }
            
            // Shared result storage
            let results: Arc<RwLock<HashMap<BlockId, RowBatch>>> = 
                Arc::new(RwLock::new(HashMap::new()));
            
            // Channels for coordination
            let (ready_tx, mut ready_rx) = mpsc::unbounded_channel::<BlockId>();
            let (complete_tx, mut complete_rx) = mpsc::unbounded_channel::<(BlockId, BlockResult)>();
            
            // Track completion
            let completed: Arc<RwLock<HashSet<BlockId>>> = Arc::new(RwLock::new(HashSet::new()));
            let pending_deps: Arc<RwLock<HashMap<BlockId, usize>>> = 
                Arc::new(RwLock::new(dep_counts.clone()));
            
            // Enqueue initially ready blocks (no dependencies)
            for block in &blocks {
                if block.deps.is_empty() {
                    ready_tx.send(block.id).map_err(|e| format!("channel send: {}", e))?;
                }
            }
            
            // Block lookup map
            let block_map: HashMap<BlockId, TeBlock> = 
                blocks.into_iter().map(|b| (b.id, b)).collect();
            let block_map = Arc::new(block_map);
            
            let dependents = Arc::new(dependents);
            
            // Spawn task launcher
            let launcher_handle = {
                let semaphore = self.semaphore.clone();
                let budget = self.budget.clone();
                let active_tasks = self.active_tasks.clone();
                let results = results.clone();
                let block_map = block_map.clone();
                let operators = operators.clone();
                let complete_tx = complete_tx.clone();
                
                tokio::spawn(async move {
                    while let Some(block_id) = ready_rx.recv().await {
                        let permit = semaphore.clone().acquire_owned().await.unwrap();
                        let budget = budget.clone();
                        let results = results.clone();
                        let block_map = block_map.clone();
                        let operators = operators.clone();
                        let complete_tx = complete_tx.clone();
                        let active_tasks = active_tasks.clone();
                        
                        active_tasks.fetch_add(1, Ordering::SeqCst);
                        
                        tokio::spawn(async move {
                            let _permit = permit; // Hold permit for duration of task
                            
                            let result = Self::execute_block(
                                block_id,
                                &block_map,
                                &results,
                                &operators,
                                &budget,
                            ).await;
                            
                            complete_tx.send((block_id, result)).ok();
                            active_tasks.fetch_sub(1, Ordering::SeqCst);
                        });
                    }
                })
            };
            
            // Process completions and enqueue newly ready blocks
            let total_blocks = block_map.len();
            let mut completed_count = 0;
            
            drop(complete_tx); // Drop our copy so channel closes when all tasks done
            
            while let Some((block_id, result)) = complete_rx.recv().await {
                match result {
                    Ok(batch) => {
                        {
                            let mut res = results.write().await;
                            res.insert(block_id, batch);
                        }
                        {
                            let mut comp = completed.write().await;
                            comp.insert(block_id);
                        }
                        
                        completed_count += 1;
                        
                        // Check if any dependent blocks are now ready
                        if let Some(deps) = dependents.get(&block_id) {
                            for &dep_block_id in deps {
                                let mut pending = pending_deps.write().await;
                                if let Some(count) = pending.get_mut(&dep_block_id) {
                                    *count -= 1;
                                    if *count == 0 {
                                        ready_tx.send(dep_block_id).ok();
                                    }
                                }
                            }
                        }
                        
                        if completed_count >= total_blocks {
                            break;
                        }
                    }
                    Err(e) => {
                        return Err(format!("block {} failed: {}", block_id, e));
                    }
                }
            }
            
            drop(ready_tx); // Signal launcher to exit
            launcher_handle.await.map_err(|e| format!("launcher join: {}", e))?;
            
            // Extract final results
            let results = Arc::try_unwrap(results)
                .map_err(|_| "failed to unwrap results")?
                .into_inner();
            
            Ok(results)
        }
        
        /// Execute a single block
        async fn execute_block(
            block_id: BlockId,
            block_map: &HashMap<BlockId, TeBlock>,
            results: &RwLock<HashMap<BlockId, RowBatch>>,
            operators: &RwLock<HashMap<u64, Box<dyn Operator>>>,
            budget: &MemoryBudgetImpl,
        ) -> BlockResult {
            let block = block_map.get(&block_id)
                .ok_or_else(|| format!("block {} not found", block_id))?;
            
            // Gather inputs
            let mut inputs = Vec::new();
            {
                let res = results.read().await;
                for dep_id in &block.deps {
                    let batch = res.get(dep_id)
                        .ok_or_else(|| format!("dependency {} not ready", dep_id))?;
                    inputs.push(batch.clone());
                }
            }
            
            // Execute operator
            let ops = operators.read().await;
            let op = ops.get(&block.op.get())
                .ok_or_else(|| format!("operator {} not found", block.op))?;
            
            op.eval_block(&inputs, budget)
                .map_err(|e| format!("operator exec: {}", e))
        }
    }
}
