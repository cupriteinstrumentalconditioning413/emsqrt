//! Deterministic replay & provenance helpers.
//!
//! The manifest hashes are computed from the serialized `PhysicalPlan`,
//! operator bindings, and TE order. With identical inputs + seed, the runtime
//! should produce identical block ordering and outputs.

use emsqrt_core::hash::{hash_serde, Hash256};
use emsqrt_planner::physical::PhysicalProgram;
use emsqrt_te::tree_eval::TePlan;

use crate::ExecError;

/// Hash both plan and bindings into one stable digest.
pub fn hash_program(program: &PhysicalProgram) -> Result<Hash256, ExecError> {
    let a = hash_serde(&program.plan).map_err(|e| ExecError::Hash(e.to_string()))?;
    let b = hash_serde(&program.bindings).map_err(|e| ExecError::Hash(e.to_string()))?;
    Ok(xor_hashes(a, b))
}

/// Hash the TE plan (typically just the order).
pub fn hash_te(te: &TePlan) -> Result<Hash256, ExecError> {
    let h = hash_serde(&te.order).map_err(|e| ExecError::Hash(e.to_string()))?;
    Ok(h)
}

fn xor_hashes(a: Hash256, b: Hash256) -> Hash256 {
    let mut out = [0u8; 32];
    for i in 0..32 {
        out[i] = a.0[i] ^ b.0[i];
    }
    Hash256(out)
}
