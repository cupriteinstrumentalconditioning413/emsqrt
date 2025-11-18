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

    /// Directory for spill files (legacy local-path configuration).
    pub spill_dir: String,

    /// Optional fully-qualified spill URI (e.g., `s3://bucket/prefix`).
    pub spill_uri: Option<String>,

    /// Cloud credential hints / overrides.
    pub spill_aws_region: Option<String>,
    pub spill_aws_access_key_id: Option<String>,
    pub spill_aws_secret_access_key: Option<String>,
    pub spill_aws_session_token: Option<String>,
    pub spill_gcs_service_account_path: Option<String>,
    pub spill_azure_access_key: Option<String>,

    /// Retry policy for spill storage.
    pub spill_retry_max_retries: usize,
    pub spill_retry_initial_backoff_ms: u64,
    pub spill_retry_max_backoff_ms: u64,
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
            spill_uri: None,
            spill_aws_region: None,
            spill_aws_access_key_id: None,
            spill_aws_secret_access_key: None,
            spill_aws_session_token: None,
            spill_gcs_service_account_path: None,
            spill_azure_access_key: None,
            spill_retry_max_retries: 3,
            spill_retry_initial_backoff_ms: 200,
            spill_retry_max_backoff_ms: 5_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub uri: Option<String>,
    pub root: String,
    pub aws_region: Option<String>,
    pub aws_access_key_id: Option<String>,
    pub aws_secret_access_key: Option<String>,
    pub aws_session_token: Option<String>,
    pub gcs_service_account_path: Option<String>,
    pub azure_access_key: Option<String>,
    pub retry_max_retries: usize,
    pub retry_initial_backoff_ms: u64,
    pub retry_max_backoff_ms: u64,
}

impl StorageConfig {
    pub fn scheme(&self) -> Option<&str> {
        self.uri
            .as_deref()
            .and_then(|uri| uri.split("://").next())
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
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

        if let Ok(s) = std::env::var("EMSQRT_SPILL_URI") {
            cfg.spill_uri = Some(s);
        }

        if let Ok(s) = std::env::var("EMSQRT_SPILL_AWS_REGION") {
            cfg.spill_aws_region = Some(s);
        }

        if let Ok(s) = std::env::var("EMSQRT_SPILL_AWS_ACCESS_KEY_ID") {
            cfg.spill_aws_access_key_id = Some(s);
        }

        if let Ok(s) = std::env::var("EMSQRT_SPILL_AWS_SECRET_ACCESS_KEY") {
            cfg.spill_aws_secret_access_key = Some(s);
        }

        if let Ok(s) = std::env::var("EMSQRT_SPILL_AWS_SESSION_TOKEN") {
            cfg.spill_aws_session_token = Some(s);
        }

        if let Ok(s) = std::env::var("EMSQRT_SPILL_GCS_SA_PATH") {
            cfg.spill_gcs_service_account_path = Some(s);
        }

        if let Ok(s) = std::env::var("EMSQRT_SPILL_AZURE_ACCESS_KEY") {
            cfg.spill_azure_access_key = Some(s);
        }

        if let Ok(s) = std::env::var("EMSQRT_SPILL_RETRY_MAX_RETRIES") {
            if let Ok(v) = s.parse::<usize>() {
                cfg.spill_retry_max_retries = v;
            }
        }

        if let Ok(s) = std::env::var("EMSQRT_SPILL_RETRY_INITIAL_MS") {
            if let Ok(v) = s.parse::<u64>() {
                cfg.spill_retry_initial_backoff_ms = v;
            }
        }

        if let Ok(s) = std::env::var("EMSQRT_SPILL_RETRY_MAX_MS") {
            if let Ok(v) = s.parse::<u64>() {
                cfg.spill_retry_max_backoff_ms = v;
            }
        }

        cfg
    }

    /// Produce a storage configuration snapshot used by the IO layer.
    pub fn storage_config(&self) -> StorageConfig {
        let scheme = self
            .spill_uri
            .as_deref()
            .and_then(|uri| uri.split("://").next())
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());

        let root = match (scheme.as_deref(), self.spill_uri.as_ref()) {
            (Some("file"), Some(uri)) => {
                file_uri_to_path(uri).unwrap_or_else(|| self.spill_dir.clone())
            }
            (Some(_), Some(uri)) => uri.trim_end_matches('/').to_string(),
            _ => self.spill_dir.clone(),
        };

        StorageConfig {
            uri: self.spill_uri.clone(),
            root,
            aws_region: self.spill_aws_region.clone(),
            aws_access_key_id: self.spill_aws_access_key_id.clone(),
            aws_secret_access_key: self.spill_aws_secret_access_key.clone(),
            aws_session_token: self.spill_aws_session_token.clone(),
            gcs_service_account_path: self.spill_gcs_service_account_path.clone(),
            azure_access_key: self.spill_azure_access_key.clone(),
            retry_max_retries: self.spill_retry_max_retries,
            retry_initial_backoff_ms: self.spill_retry_initial_backoff_ms,
            retry_max_backoff_ms: self.spill_retry_max_backoff_ms,
        }
    }
}

fn file_uri_to_path(uri: &str) -> Option<String> {
    let stripped = uri.strip_prefix("file://")?;
    if stripped.starts_with('/') {
        Some(stripped.to_string())
    } else {
        Some(format!("/{}", stripped))
    }
}
