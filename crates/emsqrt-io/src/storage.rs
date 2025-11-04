//! Concrete implementations of `emsqrt_mem::spill::Storage`.
//!
//! - `FsStorage`: local filesystem implementation (read_range, write, etag).
//! - `S3Storage`, `GcsStorage`, `AzureBlobStorage`: *feature-gated placeholders*
//!    returning unimplemented errors until wired to real SDKs/object_store.

use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use blake3::Hasher;
use emsqrt_mem::error::{Error as MemError, Result as MemResult};
use emsqrt_mem::Storage;

/// Local filesystem storage (rooted at current FS).
pub struct FsStorage;

impl FsStorage {
    pub fn new() -> Self {
        Self
    }
}

impl Storage for FsStorage {
    fn write(&self, path: &str, bytes: &[u8]) -> MemResult<()> {
        let p = Path::new(path);
        if let Some(parent) = p.parent() {
            fs::create_dir_all(parent).map_err(|e| MemError::Storage(format!("mkparent: {e}")))?;
        }
        let mut f = File::create(p).map_err(|e| MemError::Storage(format!("create: {e}")))?;
        f.write_all(bytes)
            .map_err(|e| MemError::Storage(format!("write: {e}")))?;
        f.flush()
            .map_err(|e| MemError::Storage(format!("flush: {e}")))?;
        Ok(())
    }

    fn read_range(&self, path: &str, offset: u64, len: usize) -> MemResult<Vec<u8>> {
        let mut f =
            File::open(Path::new(path)).map_err(|e| MemError::Storage(format!("open: {e}")))?;
        f.seek(SeekFrom::Start(offset))
            .map_err(|e| MemError::Storage(format!("seek: {e}")))?;
        let mut buf = vec![0u8; len];
        let n = f
            .read(&mut buf)
            .map_err(|e| MemError::Storage(format!("read: {e}")))?;
        buf.truncate(n);
        Ok(buf)
    }

    fn delete(&self, path: &str) -> MemResult<()> {
        let p = Path::new(path);
        if p.exists() {
            fs::remove_file(p).map_err(|e| MemError::Storage(format!("delete: {e}")))?;
        }
        Ok(())
    }

    fn list(&self, prefix: &str) -> MemResult<Vec<String>> {
        let prefix_path = Path::new(prefix);
        let mut results = Vec::new();

        if !prefix_path.exists() {
            return Ok(results);
        }

        if prefix_path.is_file() {
            if let Some(s) = prefix_path.to_str() {
                results.push(s.to_string());
            }
            return Ok(results);
        }

        fn visit_dirs(dir: &Path, results: &mut Vec<String>) -> std::io::Result<()> {
            if dir.is_dir() {
                for entry in fs::read_dir(dir)? {
                    let entry = entry?;
                    let path = entry.path();
                    if path.is_dir() {
                        visit_dirs(&path, results)?;
                    } else if let Some(s) = path.to_str() {
                        results.push(s.to_string());
                    }
                }
            }
            Ok(())
        }

        visit_dirs(prefix_path, &mut results)
            .map_err(|e| MemError::Storage(format!("list: {e}")))?;

        Ok(results)
    }

    fn size(&self, path: &str) -> MemResult<u64> {
        let p = Path::new(path);
        let meta = fs::metadata(p).map_err(|e| MemError::Storage(format!("size: {e}")))?;
        Ok(meta.len())
    }

    fn etag(&self, path: &str) -> MemResult<Option<String>> {
        // Lightweight pseudo-ETag: hash(size || mtime || path)
        let p = Path::new(path);
        match fs::metadata(p) {
            Ok(meta) => {
                let mut h = Hasher::new();
                h.update(&meta.len().to_le_bytes());
                if let Ok(m) = meta.modified() {
                    if let Ok(d) = m.duration_since(std::time::SystemTime::UNIX_EPOCH) {
                        h.update(&d.as_secs().to_le_bytes());
                        h.update(&d.subsec_nanos().to_le_bytes());
                    }
                }
                if let Some(s) = p.to_str() {
                    h.update(s.as_bytes());
                }
                let hex = h.finalize().to_hex().to_string();
                Ok(Some(hex))
            }
            Err(_) => Ok(None),
        }
    }
}

#[cfg(feature = "s3")]
pub struct S3Storage;
#[cfg(feature = "s3")]
impl S3Storage {
    pub fn new() -> Self {
        Self
    }
}
#[cfg(feature = "s3")]
impl Storage for S3Storage {
    fn write(&self, _path: &str, _bytes: &[u8]) -> MemResult<()> {
        Err(MemError::Storage("S3Storage::write not implemented".into()))
    }
    fn read_range(&self, _path: &str, _offset: u64, _len: usize) -> MemResult<Vec<u8>> {
        Err(MemError::Storage(
            "S3Storage::read_range not implemented".into(),
        ))
    }
    fn etag(&self, _path: &str) -> MemResult<Option<String>> {
        Err(MemError::Storage("S3Storage::etag not implemented".into()))
    }
}

#[cfg(feature = "gcs")]
pub struct GcsStorage;
#[cfg(feature = "gcs")]
impl GcsStorage {
    pub fn new() -> Self {
        Self
    }
}
#[cfg(feature = "gcs")]
impl Storage for GcsStorage {
    fn write(&self, _path: &str, _bytes: &[u8]) -> MemResult<()> {
        Err(MemError::Storage(
            "GcsStorage::write not implemented".into(),
        ))
    }
    fn read_range(&self, _path: &str, _offset: u64, _len: usize) -> MemResult<Vec<u8>> {
        Err(MemError::Storage(
            "GcsStorage::read_range not implemented".into(),
        ))
    }
    fn etag(&self, _path: &str) -> MemResult<Option<String>> {
        Err(MemError::Storage("GcsStorage::etag not implemented".into()))
    }
}

#[cfg(feature = "azure")]
pub struct AzureBlobStorage;
#[cfg(feature = "azure")]
impl AzureBlobStorage {
    pub fn new() -> Self {
        Self
    }
}
#[cfg(feature = "azure")]
impl Storage for AzureBlobStorage {
    fn write(&self, _path: &str, _bytes: &[u8]) -> MemResult<()> {
        Err(MemError::Storage(
            "AzureBlobStorage::write not implemented".into(),
        ))
    }
    fn read_range(&self, _path: &str, _offset: u64, _len: usize) -> MemResult<Vec<u8>> {
        Err(MemError::Storage(
            "AzureBlobStorage::read_range not implemented".into(),
        ))
    }
    fn etag(&self, _path: &str) -> MemResult<Option<String>> {
        Err(MemError::Storage(
            "AzureBlobStorage::etag not implemented".into(),
        ))
    }
}
