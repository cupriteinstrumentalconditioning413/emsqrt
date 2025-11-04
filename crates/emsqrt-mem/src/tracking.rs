//! Lightweight peak/owner tracking hooks.
//!
//! Keep this optional and cheap. Downstream can wire to OpenTelemetry/Prom if desired.

use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
pub struct PeakTracker {
    peak_bytes: AtomicUsize,
}

impl PeakTracker {
    pub fn new() -> Self {
        Self {
            peak_bytes: AtomicUsize::new(0),
        }
    }

    /// Record a new "used bytes" value; updates peak if higher.
    pub fn record_used(&self, used_bytes: usize) {
        let mut cur = self.peak_bytes.load(Ordering::Relaxed);
        while used_bytes > cur {
            match self.peak_bytes.compare_exchange(
                cur,
                used_bytes,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => cur = observed,
            }
        }
        #[cfg(feature = "tracing")]
        tracing::trace!(
            used_bytes,
            peak = self.peak_bytes.load(Ordering::Relaxed),
            "mem usage"
        );
    }

    pub fn peak(&self) -> usize {
        self.peak_bytes.load(Ordering::Relaxed)
    }
}
