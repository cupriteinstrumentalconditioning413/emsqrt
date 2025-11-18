use std::future::Future;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use bytes::Bytes;
use emsqrt_core::config::StorageConfig;
use emsqrt_mem::error::{Error as MemError, Result as MemResult};
use emsqrt_mem::Storage;
use futures::StreamExt;
use object_store::client::backoff::BackoffConfig;
use object_store::path::Path as ObjectPath;
use object_store::{Error as ObjectStoreError, ObjectStore};
use tokio::runtime::Runtime;
use url::Url;

#[cfg(feature = "s3")]
use object_store::aws::{AmazonS3, AmazonS3Builder};
#[cfg(feature = "azure")]
use object_store::azure::{MicrosoftAzure, MicrosoftAzureBuilder};
#[cfg(feature = "gcs")]
use object_store::gcp::{GoogleCloudStorage, GoogleCloudStorageBuilder};

use super::RetryConfig;

#[derive(Debug, thiserror::Error)]
pub enum CloudStorageBuilderError {
    #[error("missing spill URI for {scheme} storage")]
    MissingUri { scheme: &'static str },

    #[error("unsupported or malformed URI '{uri}': {source}")]
    InvalidUri {
        uri: String,
        #[source]
        source: url::ParseError,
    },

    #[error("URI '{uri}' missing bucket/container component")]
    MissingBucket { uri: String },

    #[error("URI '{uri}' missing container segment")]
    MissingContainer { uri: String },

    #[error("failed to initialize async runtime: {0}")]
    Runtime(String),

    #[error("object_store builder error: {0}")]
    Builder(String),
}

impl From<CloudStorageBuilderError> for crate::error::Error {
    fn from(err: CloudStorageBuilderError) -> Self {
        crate::error::Error::Config(err.to_string())
    }
}

#[derive(Debug, Clone)]
struct CloudIdentity {
    scheme: &'static str,
    account: String,
    bucket: String,
    prefix: String,
    root_uri: String,
}

impl CloudIdentity {
    fn new_s3(uri: &str) -> Result<Self, CloudStorageBuilderError> {
        let parsed = Url::parse(uri).map_err(|source| CloudStorageBuilderError::InvalidUri {
            uri: uri.to_string(),
            source,
        })?;
        if parsed.scheme() != "s3" {
            return Err(CloudStorageBuilderError::MissingUri { scheme: "s3" });
        }
        let bucket = parsed
            .host_str()
            .ok_or_else(|| CloudStorageBuilderError::MissingBucket {
                uri: uri.to_string(),
            })?
            .to_string();
        let prefix = parsed.path().trim_matches('/').to_string();
        Ok(Self {
            scheme: "s3",
            account: bucket.clone(),
            bucket,
            prefix,
            root_uri: uri.trim_end_matches('/').to_string(),
        })
    }

    fn new_gcs(uri: &str) -> Result<Self, CloudStorageBuilderError> {
        let parsed = Url::parse(uri).map_err(|source| CloudStorageBuilderError::InvalidUri {
            uri: uri.to_string(),
            source,
        })?;
        let scheme = parsed.scheme();
        if scheme != "gs" && scheme != "gcs" {
            return Err(CloudStorageBuilderError::MissingUri { scheme: "gs" });
        }
        let bucket = parsed
            .host_str()
            .ok_or_else(|| CloudStorageBuilderError::MissingBucket {
                uri: uri.to_string(),
            })?
            .to_string();
        let prefix = parsed.path().trim_matches('/').to_string();
        Ok(Self {
            scheme: if scheme == "gs" { "gs" } else { "gcs" },
            account: bucket.clone(),
            bucket,
            prefix,
            root_uri: uri.trim_end_matches('/').to_string(),
        })
    }

    fn new_azure(uri: &str) -> Result<Self, CloudStorageBuilderError> {
        let parsed = Url::parse(uri).map_err(|source| CloudStorageBuilderError::InvalidUri {
            uri: uri.to_string(),
            source,
        })?;
        let scheme = parsed.scheme();
        if scheme != "azure" && scheme != "azblob" {
            return Err(CloudStorageBuilderError::MissingUri { scheme: "azure" });
        }
        let account = parsed
            .host_str()
            .ok_or_else(|| CloudStorageBuilderError::MissingBucket {
                uri: uri.to_string(),
            })?
            .to_string();
        let mut segments = parsed.path().trim_matches('/').splitn(2, '/');
        let container = segments
            .next()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| CloudStorageBuilderError::MissingContainer {
                uri: uri.to_string(),
            })?
            .to_string();
        let prefix = segments.next().unwrap_or("").trim_matches('/').to_string();
        Ok(Self {
            scheme: if scheme == "azure" { "azure" } else { "azblob" },
            account,
            bucket: container,
            prefix,
            root_uri: uri.trim_end_matches('/').to_string(),
        })
    }

    fn key_from_relative(&self, rel: &str) -> String {
        if self.prefix.is_empty() {
            rel.to_string()
        } else if rel.is_empty() {
            self.prefix.clone()
        } else {
            format!("{}/{}", self.prefix.trim_end_matches('/'), rel)
        }
    }

    fn uri_for_key(&self, key: &str) -> String {
        match self.scheme {
            "azure" | "azblob" => {
                if key.is_empty() {
                    self.root_uri.clone()
                } else {
                    format!(
                        "{}://{}/{}/{}",
                        self.scheme,
                        self.account,
                        self.bucket,
                        key.trim_start_matches('/')
                    )
                }
            }
            _ => {
                if key.is_empty() {
                    self.root_uri.clone()
                } else {
                    format!(
                        "{}://{}/{}",
                        self.scheme,
                        self.bucket,
                        key.trim_start_matches('/')
                    )
                }
            }
        }
    }

    fn relative_from_uri<'a>(&self, path: &'a str) -> MemResult<&'a str> {
        if path == self.root_uri {
            return Ok("");
        }
        let root = format!("{}/", self.root_uri.trim_end_matches('/'));
        if path.starts_with(&root) {
            Ok(&path[root.len()..])
        } else {
            Err(MemError::Storage(format!(
                "path '{path}' outside configured spill root '{}'",
                self.root_uri
            )))
        }
    }
}

struct CloudStorage {
    runtime: Runtime,
    store: Arc<dyn ObjectStore>,
    identity: CloudIdentity,
    retry: RetryConfig,
}

impl CloudStorage {
    fn new(
        store: Arc<dyn ObjectStore>,
        identity: CloudIdentity,
        retry: RetryConfig,
    ) -> Result<Self, CloudStorageBuilderError> {
        let runtime =
            Runtime::new().map_err(|e| CloudStorageBuilderError::Runtime(e.to_string()))?;
        Ok(Self {
            runtime,
            store,
            identity,
            retry,
        })
    }

    fn object_path(&self, uri: &str) -> MemResult<ObjectPath> {
        let rel = self.identity.relative_from_uri(uri)?;
        let key = self.identity.key_from_relative(rel);
        Ok(ObjectPath::from(key))
    }

    fn list_prefix(&self, prefix: &str) -> MemResult<Option<ObjectPath>> {
        let rel = self.identity.relative_from_uri(prefix)?;
        let key = self.identity.key_from_relative(rel);
        if key.is_empty() {
            Ok(None)
        } else {
            Ok(Some(ObjectPath::from(key)))
        }
    }

    fn run_with_retry<F, Fut, T>(&self, mut op: F, retry_not_found: bool) -> MemResult<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = object_store::Result<T>>,
    {
        let mut attempt = 0usize;
        let mut backoff = self.retry.initial_backoff;

        loop {
            let result = self.runtime.block_on(op());
            match result {
                Ok(value) => return Ok(value),
                Err(err) => {
                    let is_not_found = matches!(err, ObjectStoreError::NotFound { .. });
                    if is_not_found && !retry_not_found {
                        return Err(MemError::Storage(format!("{err}")));
                    }
                    if attempt >= self.retry.max_retries || !is_retryable(&err) {
                        return Err(MemError::Storage(format!("{err}")));
                    }
                    attempt += 1;
                    thread::sleep(backoff);
                    backoff = std::cmp::min(backoff * 2, self.retry.max_backoff);
                }
            }
        }
    }
}

fn is_retryable(err: &ObjectStoreError) -> bool {
    match err {
        ObjectStoreError::NotFound { .. } => false,
        ObjectStoreError::AlreadyExists { .. } => false,
        _ => true,
    }
}

impl Storage for CloudStorage {
    fn write(&self, path: &str, bytes: &[u8]) -> MemResult<()> {
        let obj_path = self.object_path(path)?;
        let data = Bytes::copy_from_slice(bytes);
        self.run_with_retry(
            || {
                let bytes = data.clone();
                let store = Arc::clone(&self.store);
                async move { store.put(&obj_path, bytes).await.map(|_| ()) }
            },
            true,
        )
    }

    fn read_range(&self, path: &str, offset: u64, len: usize) -> MemResult<Vec<u8>> {
        let obj_path = self.object_path(path)?;
        let range = (offset as usize)..(offset as usize + len);
        self.run_with_retry(
            || {
                let store = Arc::clone(&self.store);
                async move { store.get_range(&obj_path, range.clone()).await }
            },
            false,
        )
        .map(|bytes| bytes.to_vec())
    }

    fn delete(&self, path: &str) -> MemResult<()> {
        let obj_path = self.object_path(path)?;
        self.run_with_retry(
            || {
                let store = Arc::clone(&self.store);
                async move { store.delete(&obj_path).await }
            },
            true,
        )
    }

    fn list(&self, prefix: &str) -> MemResult<Vec<String>> {
        let prefix_path = self.list_prefix(prefix)?;
        let store = Arc::clone(&self.store);
        let identity = self.identity.clone();
        self.runtime.block_on(async move {
            let mut stream = store.list(prefix_path.as_ref());
            let mut out = Vec::new();
            while let Some(item) = stream.next().await {
                let meta = item.map_err(|e| MemError::Storage(format!("{e}")))?;
                out.push(identity.uri_for_key(meta.location.as_ref()));
            }
            Ok(out)
        })
    }

    fn size(&self, path: &str) -> MemResult<u64> {
        let obj_path = self.object_path(path)?;
        self.run_with_retry(
            || {
                let store = Arc::clone(&self.store);
                async move { store.head(&obj_path).await }
            },
            false,
        )
        .map(|meta| meta.size as u64)
    }

    fn etag(&self, path: &str) -> MemResult<Option<String>> {
        let obj_path = self.object_path(path)?;
        self.run_with_retry(
            || {
                let store = Arc::clone(&self.store);
                async move { store.head(&obj_path).await }
            },
            false,
        )
        .map(|meta| meta.e_tag)
    }
}

fn retry_config_from(cfg: &StorageConfig) -> RetryConfig {
    RetryConfig {
        max_retries: cfg.retry_max_retries,
        initial_backoff: Duration::from_millis(cfg.retry_initial_backoff_ms),
        max_backoff: Duration::from_millis(cfg.retry_max_backoff_ms),
    }
}

fn object_store_retry(retry: &RetryConfig) -> object_store::RetryConfig {
    object_store::RetryConfig {
        max_retries: retry.max_retries,
        retry_timeout: retry.max_backoff,
        backoff: BackoffConfig {
            init_backoff: retry.initial_backoff,
            max_backoff: retry.max_backoff,
            base: 2.0,
        },
    }
}

#[cfg(feature = "s3")]
pub struct S3Storage {
    inner: CloudStorage,
}

#[cfg(feature = "s3")]
impl S3Storage {
    pub fn new(cfg: &StorageConfig) -> Result<Self, CloudStorageBuilderError> {
        let uri = cfg
            .uri
            .as_deref()
            .ok_or(CloudStorageBuilderError::MissingUri { scheme: "s3" })?;
        let identity = CloudIdentity::new_s3(uri)?;
        let retry = retry_config_from(cfg);
        let mut builder = AmazonS3Builder::new().with_bucket_name(identity.bucket.clone());
        if let Some(region) = &cfg.aws_region {
            builder = builder.with_region(region.clone());
        }
        if let Some(access_key) = &cfg.aws_access_key_id {
            builder = builder.with_access_key_id(access_key.clone());
        }
        if let Some(secret_key) = &cfg.aws_secret_access_key {
            builder = builder.with_secret_access_key(secret_key.clone());
        }
        if let Some(token) = &cfg.aws_session_token {
            builder = builder.with_token(token.clone());
        }
        builder = builder.with_retry(object_store_retry(&retry));
        let store: AmazonS3 = builder
            .build()
            .map_err(|e| CloudStorageBuilderError::Builder(e.to_string()))?;
        let inner = CloudStorage::new(Arc::new(store), identity, retry)?;
        Ok(Self { inner })
    }
}

#[cfg(feature = "gcs")]
pub struct GcsStorage {
    inner: CloudStorage,
}

#[cfg(feature = "gcs")]
impl GcsStorage {
    pub fn new(cfg: &StorageConfig) -> Result<Self, CloudStorageBuilderError> {
        let uri = cfg
            .uri
            .as_deref()
            .ok_or(CloudStorageBuilderError::MissingUri { scheme: "gs" })?;
        let identity = CloudIdentity::new_gcs(uri)?;
        let retry = retry_config_from(cfg);
        let mut builder =
            GoogleCloudStorageBuilder::new().with_bucket_name(identity.bucket.clone());
        if let Some(sa_path) = &cfg.gcs_service_account_path {
            builder = builder.with_service_account_path(sa_path);
        }
        builder = builder.with_retry(object_store_retry(&retry));
        let store: GoogleCloudStorage = builder
            .build()
            .map_err(|e| CloudStorageBuilderError::Builder(e.to_string()))?;
        let inner = CloudStorage::new(Arc::new(store), identity, retry)?;
        Ok(Self { inner })
    }
}

#[cfg(feature = "azure")]
pub struct AzureBlobStorage {
    inner: CloudStorage,
}

#[cfg(feature = "azure")]
impl AzureBlobStorage {
    pub fn new(cfg: &StorageConfig) -> Result<Self, CloudStorageBuilderError> {
        let uri = cfg
            .uri
            .as_deref()
            .ok_or(CloudStorageBuilderError::MissingUri { scheme: "azure" })?;
        let identity = CloudIdentity::new_azure(uri)?;
        let retry = retry_config_from(cfg);
        let mut builder = MicrosoftAzureBuilder::new()
            .with_account(identity.account.clone())
            .with_container_name(identity.bucket.clone())
            .with_retry(object_store_retry(&retry));
        if let Some(key) = &cfg.azure_access_key {
            builder = builder.with_access_key(key.clone());
        }
        let store: MicrosoftAzure = builder
            .build()
            .map_err(|e| CloudStorageBuilderError::Builder(e.to_string()))?;
        let inner = CloudStorage::new(Arc::new(store), identity, retry)?;
        Ok(Self { inner })
    }
}

#[cfg(feature = "s3")]
impl Storage for S3Storage {
    fn write(&self, path: &str, bytes: &[u8]) -> MemResult<()> {
        self.inner.write(path, bytes)
    }

    fn read_range(&self, path: &str, offset: u64, len: usize) -> MemResult<Vec<u8>> {
        self.inner.read_range(path, offset, len)
    }

    fn delete(&self, path: &str) -> MemResult<()> {
        self.inner.delete(path)
    }

    fn list(&self, prefix: &str) -> MemResult<Vec<String>> {
        self.inner.list(prefix)
    }

    fn size(&self, path: &str) -> MemResult<u64> {
        self.inner.size(path)
    }

    fn etag(&self, path: &str) -> MemResult<Option<String>> {
        self.inner.etag(path)
    }
}

#[cfg(feature = "gcs")]
impl Storage for GcsStorage {
    fn write(&self, path: &str, bytes: &[u8]) -> MemResult<()> {
        self.inner.write(path, bytes)
    }

    fn read_range(&self, path: &str, offset: u64, len: usize) -> MemResult<Vec<u8>> {
        self.inner.read_range(path, offset, len)
    }

    fn delete(&self, path: &str) -> MemResult<()> {
        self.inner.delete(path)
    }

    fn list(&self, prefix: &str) -> MemResult<Vec<String>> {
        self.inner.list(prefix)
    }

    fn size(&self, path: &str) -> MemResult<u64> {
        self.inner.size(path)
    }

    fn etag(&self, path: &str) -> MemResult<Option<String>> {
        self.inner.etag(path)
    }
}

#[cfg(feature = "azure")]
impl Storage for AzureBlobStorage {
    fn write(&self, path: &str, bytes: &[u8]) -> MemResult<()> {
        self.inner.write(path, bytes)
    }

    fn read_range(&self, path: &str, offset: u64, len: usize) -> MemResult<Vec<u8>> {
        self.inner.read_range(path, offset, len)
    }

    fn delete(&self, path: &str) -> MemResult<()> {
        self.inner.delete(path)
    }

    fn list(&self, prefix: &str) -> MemResult<Vec<String>> {
        self.inner.list(prefix)
    }

    fn size(&self, path: &str) -> MemResult<u64> {
        self.inner.size(path)
    }

    fn etag(&self, path: &str) -> MemResult<Option<String>> {
        self.inner.etag(path)
    }
}
