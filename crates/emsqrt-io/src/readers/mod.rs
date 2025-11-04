//! Streaming readers that produce small `RowBatch` chunks.
//!
//! NOTE: These are minimal prototypes. Real engine operators will convert to Arrow
//! arrays inside `emsqrt-operators`. Keeping core IO simple keeps compile times low.

pub mod csv;
pub mod jsonl;

#[cfg(feature = "parquet")]
pub mod parquet;
