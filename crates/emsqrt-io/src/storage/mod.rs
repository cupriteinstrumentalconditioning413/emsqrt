//! Storage adapters implementing `emsqrt_mem::spill::Storage`.
//!
//! - `fs`: Local filesystem (default).
//! - `cloud`: Cloud object stores (S3/GCS/Azure) built on top of `object_store`.
//!
//! Also exposes `RetryConfig` and helper builders that choose the appropriate
//! storage based on the configured spill URI (e.g. `file:///tmp`, `s3://bucket`).

mod fs;
pub use fs::FsStorage;

#[cfg(any(feature = "s3", feature = "gcs", feature = "azure"))]
mod cloud;
#[cfg(any(feature = "s3", feature = "gcs", feature = "azure"))]
pub use cloud::{AzureBlobStorage, CloudStorageBuilderError, GcsStorage, S3Storage};

use std::time::Duration;

use emsqrt_core::config::StorageConfig;
use emsqrt_mem::Storage;

use crate::error::{Error, Result};

/// Retry/backoff configuration shared across cloud adapters.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_retries: usize,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff: Duration::from_millis(200),
            max_backoff: Duration::from_secs(5),
        }
    }
}

/// Build the correct storage backend using the provided configuration.
pub fn build_storage_from_config(cfg: &StorageConfig) -> Result<Box<dyn Storage>> {
    match cfg.scheme() {
        Some("s3") => {
            #[cfg(feature = "s3")]
            {
                let storage = S3Storage::new(cfg)?;
                Ok(Box::new(storage))
            }

            #[cfg(not(feature = "s3"))]
            {
                Err(Error::Config(
                    "EM-√ was built without the `s3` feature; rebuild with `--features emsqrt-io/s3`"
                        .into(),
                ))
            }
        }
        Some("gs") | Some("gcs") => {
            #[cfg(feature = "gcs")]
            {
                let storage = GcsStorage::new(cfg)?;
                Ok(Box::new(storage))
            }

            #[cfg(not(feature = "gcs"))]
            {
                Err(Error::Config(
                    "EM-√ was built without the `gcs` feature; rebuild with `--features emsqrt-io/gcs`"
                        .into(),
                ))
            }
        }
        Some("azure") | Some("azblob") => {
            #[cfg(feature = "azure")]
            {
                let storage = AzureBlobStorage::new(cfg)?;
                Ok(Box::new(storage))
            }

            #[cfg(not(feature = "azure"))]
            {
                Err(Error::Config(
                    "EM-√ was built without the `azure` feature; rebuild with `--features emsqrt-io/azure`"
                        .into(),
                ))
            }
        }
        Some("file") | None => {
            // Default to filesystem (treat URI as file:// or bare path).
            Ok(Box::new(FsStorage::new()))
        }
        Some(other) => Err(Error::Config(format!("unsupported spill scheme '{other}'"))),
    }
}
