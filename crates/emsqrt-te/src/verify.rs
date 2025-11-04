//! Debug-time verification helpers for TE plans.
//!
//! These functions are intended for testing and debug builds to catch
//! violations early (cycles, missing deps, etc.). They should be cheap.

use emsqrt_core::id::BlockId;

use crate::tree_eval::TePlan;

/// Verify that the TE order is topological with respect to its own deps.
pub fn assert_topological(plan: &TePlan) {
    let mut seen = std::collections::HashSet::<BlockId>::new();
    for b in &plan.order {
        for d in &b.deps {
            assert!(
                seen.contains(d),
                "dependency {d} not satisfied before block {}",
                b.id
            );
        }
        seen.insert(b.id);
    }
}

/// Verify that dependencies are "small" (bounded fan-in heuristic).
/// This is **advisory** (TE should generate bounded fan-in), not a correctness condition here.
pub fn assert_bounded_fanin(plan: &TePlan, max_fanin: usize) {
    for b in &plan.order {
        assert!(
            b.deps.len() <= max_fanin,
            "block {} has fan-in {} > {}",
            b.id,
            b.deps.len(),
            max_fanin
        );
    }
}
