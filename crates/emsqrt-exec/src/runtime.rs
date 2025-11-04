//! Runtime: execute PhysicalProgram in TE order and emit a RunManifest.
//!
//! Starter behavior:
//! - Instantiates operators via `emsqrt-operators::registry`.
//! - Special-cases "source" and "sink" keys with placeholder ops.
//! - Walks `TePlan.order` sequentially; respects dependencies.
//! - Enforces a hard memory ceiling via `emsqrt-mem::MemoryBudgetImpl`.
//! - Emits a `RunManifest` with stable plan/TE hashes.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;

use emsqrt_core::config::EngineConfig;
use emsqrt_core::hash::{hash_serde, Hash256};
use emsqrt_core::manifest::RunManifest;
use emsqrt_core::prelude::Schema;
use emsqrt_core::types::RowBatch;

use emsqrt_mem::guard::MemoryBudgetImpl;
use emsqrt_mem::{Codec, SpillManager};

use emsqrt_io::storage::FsStorage;
use emsqrt_io::memory_storage::MemoryStorage;

use emsqrt_operators::registry::Registry;
use emsqrt_operators::traits::BlockStream;
use emsqrt_operators::traits::{OpError, Operator}; // placeholder alias (Vec<RowBatch>)

use emsqrt_planner::physical::PhysicalProgram;
use emsqrt_te::tree_eval::{TeBlock, TePlan};

use emsqrt_io::readers::csv::CsvReader;
use emsqrt_io::writers::csv::CsvWriter;

#[derive(Debug, Error)]
pub enum ExecError {
    #[error("operator registry: {0}")]
    Registry(String),
    #[error("operator exec: {0}")]
    Operator(String),
    #[error("invalid plan: {0}")]
    Invalid(String),
    #[error("hashing error: {0}")]
    Hash(String),
}

/// Engine owns the memory budget, operator registry, and spill manager.
pub struct Engine {
    cfg: EngineConfig,
    budget: MemoryBudgetImpl,
    registry: Registry,
    spill_mgr: Arc<Mutex<SpillManager>>,
    memory_store: Arc<MemoryStorage>,
}

impl Engine {
    pub fn new(cfg: EngineConfig) -> Self {
        let cap = cfg.mem_cap_bytes;
        let spill_dir = cfg.spill_dir.clone();

        // Create spill manager with FsStorage
        let storage = Box::new(FsStorage::new());
        let codec = Codec::None; // Default to no compression; can be made configurable
        let spill_mgr = SpillManager::new(storage, codec, spill_dir);

        Self {
            cfg,
            budget: MemoryBudgetImpl::new(cap),
            registry: Registry::new(),
            spill_mgr: Arc::new(Mutex::new(spill_mgr)),
            memory_store: Arc::new(MemoryStorage::new()),
        }
    }

    /// Get access to the memory storage (for tests to pre-populate data)
    pub fn memory_store(&self) -> Arc<MemoryStorage> {
        Arc::clone(&self.memory_store)
    }

    /// Execute a prepared `PhysicalProgram` under `TePlan` and return a manifest.
    pub fn run(
        &mut self,
        program: &PhysicalProgram,
        te: &TePlan,
    ) -> Result<RunManifest, ExecError> {
        // Hash inputs deterministically (logical → physical handled earlier).
        let plan_hash = hash_serde(&program.plan).map_err(|e| ExecError::Hash(e.to_string()))?;
        let bindings_hash =
            hash_serde(&program.bindings).map_err(|e| ExecError::Hash(e.to_string()))?;
        let te_hash = hash_serde(&te.order).map_err(|e| ExecError::Hash(e.to_string()))?;

        // Merge hashes (simple xor of bytes) to capture bindings+plan.
        let plan_hash = xor_hashes(plan_hash, bindings_hash);

        // Instantiate operator table keyed by OpId.
        let mut ops: HashMap<u64, Box<dyn Operator>> = HashMap::new();
        for (op_id, binding) in &program.bindings {
            let key = binding.key.as_str();
            let config = &binding.config;
            let inst: Box<dyn Operator> = match key {
                "source" => {
                    let source_path = config
                        .get("source")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let schema_json = config.get("schema").cloned();
                    Box::new(SourceOp {
                        source: source_path,
                        schema_json,
                        memory_store: Some(Arc::clone(&self.memory_store)),
                    })
                }
                "sink" => {
                    let destination = config
                        .get("destination")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let format = config
                        .get("format")
                        .and_then(|v| v.as_str())
                        .unwrap_or("csv")
                        .to_string();
                    Box::new(SinkOp {
                        destination,
                        format,
                    })
                }
                "filter" => {
                    let mut op = emsqrt_operators::filter::Filter::default();
                    if let Some(expr) = config.get("expr").and_then(|v| v.as_str()) {
                        op.expr = Some(expr.to_string());
                    }
                    Box::new(op)
                }
                "project" => {
                    let mut op = emsqrt_operators::project::Project::default();
                    if let Some(cols) = config.get("columns").and_then(|v| v.as_array()) {
                        op.columns = cols
                            .iter()
                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                            .collect();
                    }
                    Box::new(op)
                }
                "map" => {
                    let mut op = emsqrt_operators::map::Map::default();
                    
                    // Parse expression like "col1 AS alias1, col2 AS alias2"
                    if let Some(expr) = config.get("expr").and_then(|v| v.as_str()) {
                        op.renames = parse_map_expression(expr);
                    }
                    
                    Box::new(op)
                }
                "aggregate" => {
                    let mut op = emsqrt_operators::agregate::Aggregate::default();
                    op.spill_mgr = Some(self.spill_mgr.clone());
                    
                    // Parse group_by (array of strings)
                    if let Some(group_by) = config.get("group_by").and_then(|v| v.as_array()) {
                        op.group_by = group_by
                            .iter()
                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                            .collect();
                    }
                    
                    // Parse aggs (array of Aggregation enums serialized as JSON)
                    if let Some(aggs_json) = config.get("aggs") {
                        // Deserialize from JSON into Vec<Aggregation>
                        let aggs_vec: Vec<emsqrt_core::dag::Aggregation> = serde_json::from_value(aggs_json.clone())
                            .unwrap_or_else(|_| vec![]);
                        
                        // Convert Aggregation enums to strings for the operator
                        op.aggs = aggs_vec.iter().map(|agg| {
                            match agg {
                                emsqrt_core::dag::Aggregation::Count => "COUNT(*)".to_string(),
                                emsqrt_core::dag::Aggregation::Sum(col) => format!("SUM({})", col),
                                emsqrt_core::dag::Aggregation::Avg(col) => format!("AVG({})", col),
                                emsqrt_core::dag::Aggregation::Min(col) => format!("MIN({})", col),
                                emsqrt_core::dag::Aggregation::Max(col) => format!("MAX({})", col),
                            }
                        }).collect();
                    }
                    
                    Box::new(op)
                }
                "sort_external" => {
                    let mut op = emsqrt_operators::sort::external::ExternalSort::default();
                    op.spill_mgr = Some(self.spill_mgr.clone());
                    // Parse sort keys from config if provided
                    if let Some(keys) = config.get("by").and_then(|v| v.as_array()) {
                        op.by = keys
                            .iter()
                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                            .collect();
                    }
                    Box::new(op)
                }
                "join_hash" => {
                    let mut op = emsqrt_operators::join::hash::HashJoin::default();
                    op.spill_mgr = Some(self.spill_mgr.clone());
                    // Parse join keys from config if provided
                    if let Some(on) = config.get("on").and_then(|v| v.as_array()) {
                        op.on = on
                            .iter()
                            .filter_map(|v| {
                                if let Some(pair) = v.as_array() {
                                    if pair.len() == 2 {
                                        let left = pair[0].as_str()?.to_string();
                                        let right = pair[1].as_str()?.to_string();
                                        return Some((left, right));
                                    }
                                }
                                None
                            })
                            .collect();
                    }
                    if let Some(join_type) = config.get("join_type").and_then(|v| v.as_str()) {
                        op.join_type = join_type.to_string();
                    }
                    Box::new(op)
                }
                other => self.registry.make(other).ok_or_else(|| {
                    ExecError::Registry(format!("unknown operator key '{other}'"))
                })?,
            };
            ops.insert(op_id.get(), inst);
        }

        // Map: BlockId → RowBatch result
        let mut results: HashMap<u64, RowBatch> = HashMap::new();

        // Start manifest
        let now_ms = now_millis();
        let mut manifest = RunManifest::new(plan_hash, te_hash, now_ms);

        // Sequential TE order (starter).
        for b in &te.order {
            // Gather input batches from deps in order.
            let mut inputs: Vec<RowBatch> = Vec::with_capacity(b.deps.len());
            for dep in &b.deps {
                let key = dep.get();
                let batch = results.remove(&key).ok_or_else(|| {
                    ExecError::Invalid(format!("missing dependency block result for {}", key))
                })?;
                inputs.push(batch);
            }

            // Dispatch to the operator by op id.
            let op = ops.get(&b.op.get()).ok_or_else(|| {
                ExecError::Invalid(format!("no operator bound for op id {}", b.op))
            })?;

            // In a real runtime, Unary vs Binary would be enforced by plan shape.
            let out = op
                .eval_block(&inputs, &self.budget)
                .map_err(|e| ExecError::Operator(format!("{e}")))?;

            // Store the result for this block (downstream deps will consume/remove it).
            results.insert(b.id.get(), out);

            #[cfg(feature = "tracing")]
            tracing::trace!(block = %b.id.get(), op = %b.op.get(), deps = b.deps.len(), "executed block");
        }

        // TODO: compute outputs digest (e.g., sinks) once sinks actually write data.
        let outputs_digest = None;

        manifest = manifest.finish(now_millis(), outputs_digest);
        Ok(manifest)
    }
}

// --- helpers ---

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn xor_hashes(a: Hash256, b: Hash256) -> Hash256 {
    let mut out = [0u8; 32];
    for i in 0..32 {
        out[i] = a.0[i] ^ b.0[i];
    }
    Hash256(out)
}

/// Parse a Map expression like "col1 AS alias1, col2 AS alias2" into renames HashMap
fn parse_map_expression(expr: &str) -> std::collections::HashMap<String, String> {
    use std::collections::HashMap;
    
    let mut renames = HashMap::new();
    
    // Split by comma to get individual rename clauses
    for clause in expr.split(',') {
        let clause = clause.trim();
        
        // Check if it contains " AS " (case-insensitive)
        if let Some(as_pos) = clause.to_lowercase().find(" as ") {
            let old_name = clause[..as_pos].trim().to_string();
            let new_name = clause[as_pos + 4..].trim().to_string();
            
            if !old_name.is_empty() && !new_name.is_empty() {
                renames.insert(old_name, new_name);
            }
        }
        // If no AS clause, it's just a passthrough (no rename)
    }
    
    renames
}

// --- placeholder source/sink operators (until real IO is wired) ---

struct SourceOp {
    source: String,
    schema_json: Option<serde_json::Value>,
    memory_store: Option<Arc<MemoryStorage>>,
}

impl Operator for SourceOp {
    fn name(&self) -> &'static str {
        "source"
    }
    fn memory_need(&self, _rows: u64, _bytes: u64) -> emsqrt_operators::plan::Footprint {
        emsqrt_operators::plan::Footprint {
            bytes_per_row: 1,
            overhead_bytes: 0,
        }
    }
    fn plan(&self, _input_schemas: &[Schema]) -> Result<emsqrt_operators::plan::OpPlan, OpError> {
        // Decode schema from JSON if provided
        let schema = if let Some(ref schema_json) = self.schema_json {
            serde_json::from_value(schema_json.clone())
                .map_err(|e| OpError::Plan(format!("schema decode: {}", e)))?
        } else {
            Schema { fields: vec![] }
        };
        
        Ok(emsqrt_operators::plan::OpPlan {
            output_schema: schema,
            partitions: vec![],
            footprint: emsqrt_operators::plan::Footprint {
                bytes_per_row: 100,
                overhead_bytes: 1024,
            },
        })
    }
    fn eval_block(
        &self,
        _inputs: &[RowBatch],
        _budget: &dyn emsqrt_core::budget::MemoryBudget<Guard = emsqrt_mem::guard::BudgetGuardImpl>,
    ) -> Result<RowBatch, OpError> {
        // Handle memory:// URIs for in-memory test data
        if self.source.starts_with("memory://") {
            let key = self.source.strip_prefix("memory://").unwrap_or(&self.source);
            
            if let Some(ref memory_store) = self.memory_store {
                // Try to read from memory storage
                if memory_store.contains(key) {
                    use emsqrt_mem::Storage;
                    let bytes = memory_store
                        .read_range(key, 0, usize::MAX)
                        .map_err(|e| OpError::Exec(format!("memory read: {}", e)))?;
                    
                    // Deserialize as RowBatch
                    let batch: RowBatch = serde_json::from_slice(&bytes)
                        .map_err(|e| OpError::Exec(format!("deserialize batch: {}", e)))?;
                    
                    return Ok(batch);
                }
            }
            
            // If no data found, return empty batch
            return Ok(RowBatch { columns: vec![] });
        }
        
        // Parse file path (strip "file://" prefix)
        let file_path = self.source.strip_prefix("file://").unwrap_or(&self.source);
        
        // Read CSV file (schema will be inferred from headers)
        let file = std::fs::File::open(file_path)
            .map_err(|e| OpError::Exec(format!("open file: {}", e)))?;
        let mut reader = CsvReader::from_reader(file, true) // assume headers
            .map_err(|e| OpError::Exec(format!("csv reader: {}", e)))?;
        
        // Read all rows into a single batch (simplified for now)
        let batch = reader
            .next_batch(10000) // Read up to 10K rows per block
            .map_err(|e| OpError::Exec(format!("read batch: {}", e)))?
            .unwrap_or_else(|| RowBatch { columns: vec![] });
        
        Ok(batch)
    }
}

struct SinkOp {
    destination: String,
    format: String,
}

impl Operator for SinkOp {
    fn name(&self) -> &'static str {
        "sink"
    }
    fn memory_need(&self, _rows: u64, _bytes: u64) -> emsqrt_operators::plan::Footprint {
        emsqrt_operators::plan::Footprint {
            bytes_per_row: 0,
            overhead_bytes: 0,
        }
    }
    fn plan(&self, input_schemas: &[Schema]) -> Result<emsqrt_operators::plan::OpPlan, OpError> {
        // Sink passes through the input schema
        let schema = input_schemas
            .get(0)
            .cloned()
            .unwrap_or(Schema { fields: vec![] });
        
        Ok(emsqrt_operators::plan::OpPlan {
            output_schema: schema,
            partitions: vec![],
            footprint: emsqrt_operators::plan::Footprint {
                bytes_per_row: 100,
                overhead_bytes: 1024,
            },
        })
    }
    fn eval_block(
        &self,
        inputs: &[RowBatch],
        _budget: &dyn emsqrt_core::budget::MemoryBudget<Guard = emsqrt_mem::guard::BudgetGuardImpl>,
    ) -> Result<RowBatch, OpError> {
        let batch = inputs
            .get(0)
            .ok_or_else(|| OpError::Exec("sink requires input".to_string()))?;
        
        // Parse destination path (strip "file://" prefix)
        let file_path = self
            .destination
            .strip_prefix("file://")
            .unwrap_or(&self.destination);
        
        // Only handle CSV format for now
        if self.format == "csv" || self.format == "memory" {
            if self.format == "csv" {
                // Write to CSV file
                let file = std::fs::File::create(file_path)
                    .map_err(|e| OpError::Exec(format!("create file: {}", e)))?;
                let mut writer = CsvWriter::to_writer(file);
                writer
                    .write_batch(batch)
                    .map_err(|e| OpError::Exec(format!("write batch: {}", e)))?;
                // CsvWriter flushes on drop
            }
            // For "memory" format, just pass through without writing
        }
        
        // Return the batch (pass-through for terminal)
        Ok(batch.clone())
    }
}
