//! Streaming NDJSON writer.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufWriter, Write};

use crate::error::Result;
use emsqrt_core::types::{RowBatch, Scalar};

pub struct JsonlWriter<W: Write> {
    writer: BufWriter<W>,
    // header order to keep column ordering stable across batches
    columns: Vec<String>,
}

impl JsonlWriter<File> {
    pub fn to_path(path: &str, columns: Option<Vec<String>>) -> Result<Self> {
        let f = File::create(path)?;
        Ok(Self::to_writer(f, columns))
    }
}

impl<W: Write> JsonlWriter<W> {
    pub fn to_writer(writer: W, columns: Option<Vec<String>>) -> Self {
        Self {
            writer: BufWriter::new(writer),
            columns: columns.unwrap_or_default(),
        }
    }

    /// Write a batch as one JSON object per line.
    /// If `columns` was empty, infer it from the first batch.
    pub fn write_batch(&mut self, batch: &RowBatch) -> Result<()> {
        if self.columns.is_empty() {
            self.columns = batch.columns.iter().map(|c| c.name.clone()).collect();
        }
        let nrows = batch.num_rows();
        for r in 0..nrows {
            let mut obj = BTreeMap::new();
            for (ci, name) in self.columns.iter().enumerate() {
                if let Some(col) = batch.columns.get(ci) {
                    let val = &col.values[r];
                    obj.insert(name.clone(), scalar_to_json(val));
                }
            }
            let line = serde_json::to_string(&obj)?;
            writeln!(self.writer, "{}", line)?;
        }
        self.writer.flush()?;
        Ok(())
    }
}

fn scalar_to_json(v: &Scalar) -> serde_json::Value {
    use Scalar::*;
    match v {
        Null => serde_json::Value::Null,
        Bool(b) => serde_json::Value::Bool(*b),
        I32(i) => serde_json::Value::from(*i),
        I64(i) => serde_json::Value::from(*i),
        F32(f) => serde_json::Value::from(*f as f64),
        F64(f) => serde_json::Value::from(*f),
        Str(s) => serde_json::Value::String(s.clone()),
        Bin(b) => serde_json::Value::String(format!("[binary {} bytes]", b.len())), // base64 not available
    }
}
