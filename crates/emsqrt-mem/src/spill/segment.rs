//! Segment file header and metadata.
//!
//! Layout on disk:
//! [ magic: u32 ][ version: u16 ][ codec: u8 ][ reserved: u8 ]
//! [ uncompressed_len: u64 ][ compressed_len: u64 ]
//! [ payload bytes … ]
//!
//! End-to-end checksum is computed over (header || payload) using blake3.

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use super::Codec;

pub const MAGIC: u32 = 0x45534D51; // "ESMQ" (EM-Sqrt)
pub const VERSION: u16 = 1;
pub const HEADER_LEN: usize = 4 + 2 + 1 + 1 + 8 + 8;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentHeader {
    pub magic: u32,
    pub version: u16,
    pub codec: Codec,
    pub uncompressed_len: u64,
    pub compressed_len: u64,
}

impl SegmentHeader {
    pub fn new(codec: Codec, uncompressed_len: u64, compressed_len: u64) -> Self {
        Self { magic: MAGIC, version: VERSION, codec, uncompressed_len, compressed_len }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER_LEN);
        out.extend_from_slice(&self.magic.to_le_bytes());
        out.extend_from_slice(&self.version.to_le_bytes());
        out.push(self.codec as u8);
        out.push(0u8); // reserved
        out.extend_from_slice(&self.uncompressed_len.to_le_bytes());
        out.extend_from_slice(&self.compressed_len.to_le_bytes());
        out
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < HEADER_LEN {
            return Err(Error::Storage("short header".into()));
        }
        let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let version = u16::from_le_bytes(bytes[4..6].try_into().unwrap());
        let codec = super::Codec::from_u8(bytes[6])?;
        // bytes[7] reserved
        let uncompressed_len = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let compressed_len = u64::from_le_bytes(bytes[16..24].try_into().unwrap());

        if magic != MAGIC || version != VERSION {
            return Err(Error::Storage("bad magic/version".into()));
        }

        Ok(Self { magic, version, codec, uncompressed_len, compressed_len })
    }
    
    /// Validate that the sizes in the header are reasonable.
    /// This prevents excessive allocations from corrupted/malicious data.
    pub fn validate_sizes(&self, max_uncompressed: u64, max_compressed: u64) -> Result<()> {
        if self.uncompressed_len > max_uncompressed {
            return Err(Error::Storage(format!(
                "uncompressed_len {} exceeds max {}",
                self.uncompressed_len, max_uncompressed
            )));
        }
        if self.compressed_len > max_compressed {
            return Err(Error::Storage(format!(
                "compressed_len {} exceeds max {}",
                self.compressed_len, max_compressed
            )));
        }
        if self.compressed_len > self.uncompressed_len && self.codec != Codec::None {
            return Err(Error::Storage(
                "compressed_len > uncompressed_len for compressed codec".into()
            ));
        }
        Ok(())
    }
}

/// Human-friendly name for a segment, derived from a spill id and a run index.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SegmentName(pub String);

impl SegmentName {
    pub fn new(id: emsqrt_core::id::SpillId, run_index: u32) -> Self {
        SegmentName(format!("spill{}_run{}", id.get(), run_index))
    }
}

/// Minimal metadata the engine keeps for a spilled segment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentMeta {
    pub name: SegmentName,
    pub path: String,
    pub codec: Codec,
    pub uncompressed_len: u64,
    pub compressed_len: u64,
    pub checksum: [u8; 32],
    pub etag: Option<String>,
}
