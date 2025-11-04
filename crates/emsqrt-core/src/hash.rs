//! Stable hashing helpers for plans, manifests, and content-addressable pieces.

use blake3::Hasher;
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Hash256(pub [u8; 32]);

impl Hash256 {
    pub fn to_hex(&self) -> String {
        // blake3 hex(32b) is 64 hex chars
        let mut s = String::with_capacity(64);
        for b in &self.0 {
            use std::fmt::Write as _;
            let _ = write!(&mut s, "{:02x}", b);
        }
        s
    }
}

impl std::fmt::Display for Hash256 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

pub fn hash_bytes(bytes: &[u8]) -> Hash256 {
    let mut h = Hasher::new();
    h.update(bytes);
    let out = h.finalize();
    Hash256(out.into())
}

pub fn hash_str(s: &str) -> Hash256 {
    hash_bytes(s.as_bytes())
}

/// Hash any serde-serializable value deterministically (via JSON).
/// NOTE: Keep this in core for portability; for heavy paths, prefer manual hashing.
pub fn hash_serde<T: Serialize>(v: &T) -> Result<Hash256, crate::error::Error> {
    let bytes = serde_json::to_vec(v).map_err(|e| crate::error::Error::Hash(e.to_string()))?;
    Ok(hash_bytes(&bytes))
}
