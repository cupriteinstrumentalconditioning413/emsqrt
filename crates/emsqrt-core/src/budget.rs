//! Abstract memory budget interfaces.
//!
//! The concrete implementation lives in `emsqrt-mem`. We keep only traits here
//! so any crate can depend on the API without pulling the allocator/spill logic.

/// A guard returned by a memory budget when bytes are acquired.
///
/// The concrete type lives in `emsqrt-mem`. It must be RAII (releases on Drop),
/// `Send`, and `panic`-safe.
pub trait BudgetGuard: Send {
    /// Number of bytes currently accounted for by this guard.
    fn bytes(&self) -> usize;
    /// Optional debug tag for metrics/tracing.
    fn tag(&self) -> &'static str {
        "guard"
    }
}

/// A handle representing a memory-cap enforcer.
///
/// Implemented by `emsqrt-mem`. Operators and the engine call `try_acquire`
/// before allocating. If `None` is returned, they must partition/spill/block.
pub trait MemoryBudget: Send + Sync + 'static {
    type Guard: BudgetGuard;

    /// Attempt to acquire `bytes` from the live budget. Returns a guard on success.
    fn try_acquire(&self, bytes: usize, tag: &'static str) -> Option<Self::Guard>;

    /// Total configured capacity (bytes).
    fn capacity_bytes(&self) -> usize;

    /// Approximate currently used bytes (advisory; not a correctness API).
    fn used_bytes(&self) -> usize;
}

// NOTE: Do *not* add default impls here that would silently "allow" allocations.
// The mem crate is the only place where guards should be constructed.
