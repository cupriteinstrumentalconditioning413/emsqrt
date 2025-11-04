//! Strongly-typed identifiers used across the engine.
//!
//! Downstream crates (exec, operators, mem, etc.) should *not* use raw integers for IDs.

use serde::{Deserialize, Serialize};
use std::fmt;

macro_rules! new_id {
    ($name:ident) => {
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Ord, PartialOrd,
        )]
        #[serde(transparent)]
        pub struct $name(u64);

        impl $name {
            pub const fn new(v: u64) -> Self {
                Self(v)
            }
            pub const fn get(self) -> u64 {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({})", stringify!($name), self.0)
            }
        }
    };
}

new_id!(BlockId);
new_id!(OpId);
new_id!(SpillId);

// TODO: If you need globally unique IDs (beyond per-run counters),
// introduce UUID-based wrappers here (but prefer local counters for perf/replay).
