#![forbid(unsafe_code)]
//! emsqrt-exec: runtime/scheduler, deterministic replay, and metrics.
//!
//! Starter runtime executes TE blocks sequentially and emits a RunManifest.
//! Next steps: parallel block scheduling with bounded channels, real sources/sinks,
//! and spill-aware operators.

pub mod failpoints;
pub mod metrics;
pub mod replay;
pub mod runtime;
pub mod scheduler;

pub use runtime::{Engine, ExecError};
