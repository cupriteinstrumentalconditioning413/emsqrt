//! Spill manager for external-memory operators.
//!
//! Orchestrates writing/reading RowBatch segments to/from storage with checksums.

pub mod codec;
pub mod segment;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};

use emsqrt_core::budget::MemoryBudget;
use emsqrt_core::id::SpillId;
use emsqrt_core::types::RowBatch;

use crate::error::{Error, Result};
use crate::guard::BudgetGuardImpl;

pub use codec::Codec;
pub use segment::{SegmentHeader, SegmentMeta, SegmentName, HEADER_LEN};

/// Abstract storage interface for spill segments.
///
/// Implemented by `emsqrt-io::FsStorage` for local filesystem,
/// and by cloud storage adapters (S3, GCS, Azure) when feature-gated.
pub trait Storage: Send + Sync {
    /// Write bytes to a path. Creates parent directories if needed.
    fn write(&self, path: &str, bytes: &[u8]) -> Result<()>;

    /// Read a byte range from a path. Returns exactly `len` bytes or error.
    fn read_range(&self, path: &str, offset: u64, len: usize) -> Result<Vec<u8>>;

    /// Delete a path. Idempotent (no error if path doesn't exist).
    fn delete(&self, path: &str) -> Result<()>;

    /// List all paths under a prefix (optional, for cleanup/debugging).
    fn list(&self, prefix: &str) -> Result<Vec<String>>;

    /// Get size of a path in bytes (optional, for validation).
    fn size(&self, path: &str) -> Result<u64>;

    /// Get an ETag or hash for a path (optional, for caching/validation).
    fn etag(&self, path: &str) -> Result<Option<String>>;
}

/// Central manager for spilling RowBatches to persistent storage.
///
/// Responsibilities:
/// - Serialize/compress RowBatches with checksums
/// - Track segment metadata in memory
/// - Provide read_batch/write_batch APIs for operators
pub struct SpillManager {
    storage: Box<dyn Storage>,
    codec: Codec,
    root_dir: String,
    next_run: AtomicU32,
    segments: HashMap<SegmentName, SegmentMeta>,
}

impl SpillManager {
    /// Create a new SpillManager with the given storage backend.
    pub fn new(storage: Box<dyn Storage>, codec: Codec, root_dir: String) -> Self {
        Self {
            storage,
            codec,
            root_dir,
            next_run: AtomicU32::new(0),
            segments: HashMap::new(),
        }
    }

    /// Write a RowBatch to storage and return its metadata.
    ///
    /// Steps:
    /// 1. Serialize batch with serde_json
    /// 2. Compress payload with configured codec
    /// 3. Create SegmentHeader
    /// 4. Compute BLAKE3 checksum over header + compressed payload
    /// 5. Write to storage
    /// 6. Return SegmentMeta
    pub fn write_batch(
        &mut self,
        batch: &RowBatch,
        spill_id: SpillId,
        run_index: u32,
    ) -> Result<SegmentMeta> {
        // Serialize batch
        let uncompressed =
            serde_json::to_vec(batch).map_err(|e| Error::Codec(format!("json serialize: {e}")))?;
        let uncompressed_len = uncompressed.len() as u64;

        // Compress
        let compressed = codec::compress(self.codec, &uncompressed)?;
        let compressed_len = compressed.len() as u64;

        // Create header
        let header = SegmentHeader::new(self.codec, uncompressed_len, compressed_len);
        let header_bytes = header.to_bytes();

        // Compute checksum over header + payload
        let mut hasher = blake3::Hasher::new();
        hasher.update(&header_bytes);
        hasher.update(&compressed);
        let checksum: [u8; 32] = hasher.finalize().into();

        // Construct path and write
        let name = SegmentName::new(spill_id, run_index);
        let path = format!("{}/{}.seg", self.root_dir, name.0);

        let mut full_segment = Vec::with_capacity(header_bytes.len() + compressed.len());
        full_segment.extend_from_slice(&header_bytes);
        full_segment.extend_from_slice(&compressed);

        self.storage.write(&path, &full_segment)?;

        // Get etag from storage
        let etag = self.storage.etag(&path).ok().flatten();

        let meta = SegmentMeta {
            name: name.clone(),
            path: path.clone(),
            codec: self.codec,
            uncompressed_len,
            compressed_len,
            checksum,
            etag,
        };

        // Store metadata
        self.segments.insert(name, meta.clone());

        Ok(meta)
    }

    /// Read a RowBatch from storage using its metadata.
    ///
    /// Steps:
    /// 1. Read header + payload from storage
    /// 2. Validate checksum
    /// 3. Decompress payload (acquiring budget guard for decompression buffer)
    /// 4. Deserialize to RowBatch
    pub fn read_batch(
        &self,
        meta: &SegmentMeta,
        budget: &dyn MemoryBudget<Guard = BudgetGuardImpl>,
    ) -> Result<RowBatch> {
        // Read full segment
        let total_len = HEADER_LEN + meta.compressed_len as usize;
        let full_segment = self.storage.read_range(&meta.path, 0, total_len)?;

        if full_segment.len() < HEADER_LEN {
            return Err(Error::Storage("segment too short".into()));
        }

        // Verify checksum
        let mut hasher = blake3::Hasher::new();
        hasher.update(&full_segment);
        let computed_checksum: [u8; 32] = hasher.finalize().into();
        if computed_checksum != meta.checksum {
            return Err(Error::Storage("checksum mismatch".into()));
        }

        // Parse header
        let header = SegmentHeader::from_bytes(&full_segment[..HEADER_LEN])?;
        header.validate_sizes(100 * 1024 * 1024, 100 * 1024 * 1024)?; // 100MB sanity limit

        // Extract compressed payload
        let compressed = &full_segment[HEADER_LEN..];

        // Acquire budget for decompression (worst case: uncompressed_len)
        let _guard = budget
            .try_acquire(header.uncompressed_len as usize, "spill_decompress")
            .ok_or_else(|| Error::Budget("cannot acquire for decompression".into()))?;

        // Decompress
        let uncompressed = codec::decompress(header.codec, compressed)?;

        // Deserialize
        let batch: RowBatch = serde_json::from_slice(&uncompressed)
            .map_err(|e| Error::Codec(format!("json deserialize: {e}")))?;

        Ok(batch)
    }

    /// Generate a unique run index for this spill session.
    pub fn next_run_index(&self) -> u32 {
        self.next_run.fetch_add(1, Ordering::Relaxed)
    }

    /// Retrieve stored segment metadata by name.
    pub fn get_segment(&self, name: &SegmentName) -> Option<&SegmentMeta> {
        self.segments.get(name)
    }

    /// Delete a segment from storage and remove its metadata.
    pub fn delete_segment(&mut self, name: &SegmentName) -> Result<()> {
        if let Some(meta) = self.segments.remove(name) {
            self.storage.delete(&meta.path)?;
        }
        Ok(())
    }

    /// List all segment names currently tracked.
    pub fn list_segments(&self) -> Vec<SegmentName> {
        self.segments.keys().cloned().collect()
    }
}
