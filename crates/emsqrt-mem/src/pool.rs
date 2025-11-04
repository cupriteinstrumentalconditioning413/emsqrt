//! Fallible buffer pool built on top of the hard MemoryBudget.
//!
//! All big byte buffers should be acquired here to guarantee budget adherence.

use std::ops::{Deref, DerefMut};

use emsqrt_core::budget::{BudgetGuard, MemoryBudget};

use crate::error::{Error, Result};
use crate::guard::BudgetGuardImpl;

/// Owned byte buffer that returns its accounted bytes on drop via the guard.
pub struct OwnedBuf {
    guard: BudgetGuardImpl,
    buf: Vec<u8>,
}

impl OwnedBuf {
    /// Create a new zeroed buffer with `len` bytes, accounting against `budget`.
    pub fn new_zeroed(
        budget: &impl MemoryBudget<Guard = BudgetGuardImpl>,
        len: usize,
        tag: &'static str,
    ) -> Result<Self> {
        let guard = budget
            .try_acquire(len, tag)
            .ok_or_else(|| Error::BudgetExceeded {
                tag,
                requested: len,
                capacity: budget.capacity_bytes(),
                used: budget.used_bytes(),
            })?;

        // Safety note: allocation can still fail even if we acquired budget bytes.
        let mut buf = Vec::with_capacity(len);
        // Initialize length (zeroed)
        buf.resize(len, 0u8);

        Ok(Self { guard, buf })
    }

    /// Create a buffer with capacity `cap` and length 0.
    pub fn with_capacity(
        budget: &impl MemoryBudget<Guard = BudgetGuardImpl>,
        cap: usize,
        tag: &'static str,
    ) -> Result<Self> {
        let guard = budget
            .try_acquire(cap, tag)
            .ok_or_else(|| Error::BudgetExceeded {
                tag,
                requested: cap,
                capacity: budget.capacity_bytes(),
                used: budget.used_bytes(),
            })?;

        let buf = Vec::with_capacity(cap);
        Ok(Self { guard, buf })
    }

    /// Current accounted size (bytes).
    pub fn accounted_bytes(&self) -> usize {
        self.guard.bytes()
    }

    /// Expose the inner Vec if you need to pass it to codecs/IO.
    pub fn into_inner(self) -> (Vec<u8>, BudgetGuardImpl) {
        (self.buf, self.guard)
    }

    /// Try to grow the buffer capacity, acquiring additional budget first.
    /// Returns true if successful, false if budget cannot be acquired.
    pub fn try_grow(&mut self, new_cap: usize) -> bool {
        if new_cap <= self.buf.capacity() {
            return true; // Already have sufficient capacity
        }

        let current_cap = self.guard.bytes();
        let additional = new_cap.saturating_sub(current_cap);

        if additional == 0 {
            return true;
        }

        // Try to acquire additional bytes through the guard
        if self.guard.try_resize(new_cap) {
            // Reserve the new capacity
            self.buf.reserve_exact(additional);
            true
        } else {
            false
        }
    }
}

impl Deref for OwnedBuf {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.buf
    }
}

impl DerefMut for OwnedBuf {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buf
    }
}

/// Simple pool facade. In the future, replace with a slab/chunked allocator to
/// reduce fragmentation and reuse fixed-size pages.
pub struct BufferPool<B: MemoryBudget> {
    budget: B,
}

impl<B: MemoryBudget<Guard = BudgetGuardImpl>> BufferPool<B> {
    pub fn new(budget: B) -> Self {
        Self { budget }
    }

    pub fn alloc_zeroed(&self, len: usize, tag: &'static str) -> Result<OwnedBuf> {
        OwnedBuf::new_zeroed(&self.budget, len, tag)
    }

    pub fn alloc_with_capacity(&self, cap: usize, tag: &'static str) -> Result<OwnedBuf> {
        OwnedBuf::with_capacity(&self.budget, cap, tag)
    }

    pub fn budget(&self) -> &B {
        &self.budget
    }
}
