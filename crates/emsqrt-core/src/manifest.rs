//! Deterministic run manifest for audit/replay.
//!
//! The engine emits a manifest after successful execution; replay can rehydrate
//! the exact same outputs given identical inputs, config, and seeds.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::hash::Hash256;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ManifestId(pub Uuid);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunManifest {
    pub id: ManifestId,

    /// Stable hash of the physical plan (and operator params) used.
    pub plan_hash: Hash256,

    /// Stable hash of TE block ordering and ranges.
    pub te_hash: Hash256,

    /// Engine version string for provenance.
    pub engine_version: String,

    /// Optional dataset/input digests (e.g., ETags for object store paths).
    pub inputs_digest: Option<Hash256>,

    /// Optional outputs digest (format-specific; may be a list in future).
    pub outputs_digest: Option<Hash256>,

    /// Milliseconds since Unix epoch (UTC).
    pub started_ms: u64,
    pub finished_ms: u64,
}

impl RunManifest {
    pub fn new(plan_hash: Hash256, te_hash: Hash256, started_ms: u64) -> Self {
        Self {
            id: ManifestId(Uuid::new_v4()),
            plan_hash,
            te_hash,
            engine_version: crate::VERSION.to_string(),
            inputs_digest: None,
            outputs_digest: None,
            started_ms,
            finished_ms: started_ms,
        }
    }

    pub fn finish(mut self, finished_ms: u64, outputs_digest: Option<Hash256>) -> Self {
        self.finished_ms = finished_ms;
        self.outputs_digest = outputs_digest;
        self
    }
}
