//! Engine configuration that downstream crates can serialize/deserialize.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    /// Hard memory cap (in bytes). The engine and operators must *never* exceed this.
    pub mem_cap_bytes: usize,

    /// Optional block-size hint; the TE planner may override this based on cost modeling.
    pub block_size_hint: Option<usize>,

    /// Max on-disk spill concurrency (segments in-flight).
    pub max_spill_concurrency: usize,

    /// Optional seed for deterministic shuffles/partitioning.
    pub seed: Option<u64>,

    /// Execution parallelism. The scheduler must respect this when launching tasks.
    pub max_parallel_tasks: usize,

    /// Directory for spill files.
    pub spill_dir: String,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            mem_cap_bytes: 512 * 1024 * 1024, // 512 MiB default
            block_size_hint: None,
            max_spill_concurrency: 4,
            seed: None,
            max_parallel_tasks: 4,
            spill_dir: "/tmp/emsqrt-spill".to_string(),
        }
    }
}

impl EngineConfig {
    /// Create a config from environment variables, falling back to defaults.
    ///
    /// Environment variables:
    /// - `EMSQRT_MEM_CAP_BYTES`: memory cap in bytes
    /// - `EMSQRT_BLOCK_SIZE_HINT`: block size hint
    /// - `EMSQRT_MAX_SPILL_CONCURRENCY`: max spill concurrency
    /// - `EMSQRT_SEED`: random seed
    /// - `EMSQRT_MAX_PARALLEL_TASKS`: max parallel tasks
    pub fn from_env() -> Self {
        let mut cfg = Self::default();

        if let Ok(s) = std::env::var("EMSQRT_MEM_CAP_BYTES") {
            if let Ok(v) = s.parse::<usize>() {
                cfg.mem_cap_bytes = v;
            }
        }

        if let Ok(s) = std::env::var("EMSQRT_BLOCK_SIZE_HINT") {
            if let Ok(v) = s.parse::<usize>() {
                cfg.block_size_hint = Some(v);
            }
        }

        if let Ok(s) = std::env::var("EMSQRT_MAX_SPILL_CONCURRENCY") {
            if let Ok(v) = s.parse::<usize>() {
                cfg.max_spill_concurrency = v;
            }
        }

        if let Ok(s) = std::env::var("EMSQRT_SEED") {
            if let Ok(v) = s.parse::<u64>() {
                cfg.seed = Some(v);
            }
        }

        if let Ok(s) = std::env::var("EMSQRT_MAX_PARALLEL_TASKS") {
            if let Ok(v) = s.parse::<usize>() {
                cfg.max_parallel_tasks = v;
            }
        }

        if let Ok(s) = std::env::var("EMSQRT_SPILL_DIR") {
            cfg.spill_dir = s;
        }

        cfg
    }
}
