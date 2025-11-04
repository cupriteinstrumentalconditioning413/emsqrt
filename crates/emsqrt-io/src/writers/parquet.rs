//! Parquet writer scaffolding (enabled with `--features parquet`).
//!
//! TODO: Replace with proper Arrow-based writer and schema mapping from core.

#[cfg(feature = "parquet")]
use arrow_array::RecordBatch;

use crate::error::{Error, Result};

#[cfg(feature = "parquet")]
pub struct ParquetWriter {
    _placeholder: (),
}

#[cfg(feature = "parquet")]
impl ParquetWriter {
    pub fn to_path(_path: &str) -> Result<Self> {
        // TODO: create writer props, open file, etc.
        Ok(Self { _placeholder: () })
    }

    pub fn write_batch(&mut self, _batch: &RecordBatch) -> Result<()> {
        // TODO: implement using parquet::arrow::arrow_writer
        Ok(())
    }

    pub fn close(self) -> Result<()> {
        Ok(())
    }
}

#[cfg(not(feature = "parquet"))]
compile_error!("parquet.rs was compiled without the `parquet` feature; enable `--features parquet` or exclude this module.");
