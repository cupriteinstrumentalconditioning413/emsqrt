//! Parquet reader scaffolding (enabled with `--features parquet`).
//!
//! TODO: Replace with proper Arrow-based columnar scans and projection/predicate pushdown.

#[cfg(feature = "parquet")]
use arrow_array::RecordBatch;
#[cfg(feature = "parquet")]
use arrow_schema::SchemaRef;

use crate::error::{Error, Result};

#[cfg(feature = "parquet")]
pub struct ParquetReader {
    // TODO: parquet::record_reader, projection, batch size, etc.
    _placeholder: (),
}

#[cfg(feature = "parquet")]
impl ParquetReader {
    pub fn from_path(_path: &str) -> Result<Self> {
        // TODO: open file, parse metadata, plan row groups
        Ok(Self { _placeholder: () })
    }

    pub fn next_batch(&mut self, _limit_rows: usize) -> Result<Option<RecordBatch>> {
        // TODO: implement using parquet::arrow::arrow_reader
        Ok(None)
    }

    pub fn schema(&self) -> SchemaRef {
        // TODO
        use std::sync::Arc;
        Arc::new(arrow_schema::Schema::empty())
    }
}

#[cfg(not(feature = "parquet"))]
compile_error!("parquet.rs was compiled without the `parquet` feature; enable `--features parquet` or exclude this module.");
