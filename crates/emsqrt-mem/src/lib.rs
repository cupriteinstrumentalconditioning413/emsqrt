#![forbid(unsafe_code)]
//! emsqrt-mem: Hard memory budgeting, buffer pool, and spill manager.
//!
//! This crate provides concrete implementations for the *interfaces* defined
//! in `emsqrt-core::budget`. All allocations in the engine should flow through
//! this crate so we can enforce the hard memory ceiling with RAII guards.
//!
//! No async or object-store IO lives here. A generic `Storage` trait is exposed
//! (in `spill::`) and implemented by `emsqrt-io`.

pub mod error;
pub mod guard;
pub mod pool;
pub mod spill;
pub mod tracking;

pub use guard::{BudgetGuardImpl, MemoryBudgetImpl};
pub use pool::{BufferPool, OwnedBuf};
pub use spill::{Codec, SpillManager, Storage};
