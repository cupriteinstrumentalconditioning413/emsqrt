//! MemoryBudget + RAII guard implementations.
//!
//! Downstream crates must *always* acquire a guard before allocating. Dropping
//! the guard returns the bytes to the budget (panic-safe).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use emsqrt_core::budget::{BudgetGuard, MemoryBudget};

/// Shared inner state for the budget.
struct BudgetInner {
    capacity: usize,
    used: AtomicUsize,
}

impl BudgetInner {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            used: AtomicUsize::new(0),
        }
    }

    fn try_acquire(&self, bytes: usize) -> bool {
        loop {
            let cur = self.used.load(Ordering::Relaxed);
            let next = cur.saturating_add(bytes);
            if next > self.capacity {
                return false;
            }
            if self
                .used
                .compare_exchange(cur, next, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return true;
            }
        }
    }

    fn release(&self, bytes: usize) {
        self.used.fetch_sub(bytes, Ordering::AcqRel);
    }
}

/// Concrete MemoryBudget implementation used by the engine.
#[derive(Clone)]
pub struct MemoryBudgetImpl {
    inner: Arc<BudgetInner>,
}

impl MemoryBudgetImpl {
    pub fn new(capacity_bytes: usize) -> Self {
        Self {
            inner: Arc::new(BudgetInner::new(capacity_bytes)),
        }
    }

    /// Current usage (advisory).
    pub fn used_bytes(&self) -> usize {
        self.inner.used.load(Ordering::Relaxed)
    }

    pub fn capacity_bytes(&self) -> usize {
        self.inner.capacity
    }
}

/// RAII guard that accounts for a number of bytes.
/// Dropping it returns bytes to the budget.
pub struct BudgetGuardImpl {
    inner: Arc<BudgetInner>,
    bytes: usize,
    tag: &'static str,
}

impl Drop for BudgetGuardImpl {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.inner.release(self.bytes);
            // NOTE: do not log here to keep drop path fast.
            self.bytes = 0;
        }
    }
}

// ----- trait impls -----

impl BudgetGuard for BudgetGuardImpl {
    fn bytes(&self) -> usize {
        self.bytes
    }
    fn tag(&self) -> &'static str {
        self.tag
    }
}

impl BudgetGuardImpl {
    /// Try to resize this guard to a new byte count.
    /// Returns true if successful, false if the new size would exceed capacity.
    /// If new_bytes < current bytes, the guard is always shrunk successfully.
    pub fn try_resize(&mut self, new_bytes: usize) -> bool {
        if new_bytes == self.bytes {
            return true;
        }

        if new_bytes < self.bytes {
            // Shrink: always succeeds
            let delta = self.bytes - new_bytes;
            self.inner.release(delta);
            self.bytes = new_bytes;
            true
        } else {
            // Grow: try to acquire the additional bytes
            let delta = new_bytes - self.bytes;
            if self.inner.try_acquire(delta) {
                self.bytes = new_bytes;
                true
            } else {
                false
            }
        }
    }
}

impl MemoryBudget for MemoryBudgetImpl {
    type Guard = BudgetGuardImpl;

    fn try_acquire(&self, bytes: usize, tag: &'static str) -> Option<Self::Guard> {
        if bytes == 0 {
            return Some(BudgetGuardImpl {
                inner: Arc::clone(&self.inner),
                bytes: 0,
                tag,
            });
        }
        if self.inner.try_acquire(bytes) {
            Some(BudgetGuardImpl {
                inner: Arc::clone(&self.inner),
                bytes,
                tag,
            })
        } else {
            None
        }
    }

    fn capacity_bytes(&self) -> usize {
        self.inner.capacity
    }

    fn used_bytes(&self) -> usize {
        self.inner.used.load(Ordering::Relaxed)
    }
}
