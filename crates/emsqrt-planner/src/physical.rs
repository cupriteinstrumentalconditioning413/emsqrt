//! Physical program: `PhysicalPlan` plus operator bindings.
//!
//! The exec runtime will combine this with its operator registry to create
//! concrete operator instances for each `OpId`.

use std::collections::BTreeMap;

use emsqrt_core::dag::PhysicalPlan;
use emsqrt_core::id::OpId;
use serde::{Deserialize, Serialize};

/// Minimal operator binding information the exec can use to instantiate
/// operators. For now it's just a key (e.g., "filter", "join_hash") and a
/// generic JSON config payload (future-proof; can be operator-specific).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorBinding {
    pub key: String,
    pub config: serde_json::Value,
}

/// Physical program = physical tree + a stable map of OpIds → bindings.
/// We use a BTreeMap to keep deterministic order for hashing/manifests.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhysicalProgram {
    pub plan: PhysicalPlan,
    pub bindings: BTreeMap<OpId, OperatorBinding>,
}

impl PhysicalProgram {
    pub fn new(plan: PhysicalPlan, bindings: BTreeMap<OpId, OperatorBinding>) -> Self {
        Self { plan, bindings }
    }
}
