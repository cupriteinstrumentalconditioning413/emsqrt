//! Lightweight logical value/column placeholders to avoid bringing Arrow into core.
//!
//! Execution crates can convert these to/from Arrow arrays as needed.
//! This keeps core stable and minimal.

use serde::{Deserialize, Serialize};

use crate::schema::DataType;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Scalar {
    Null,
    Bool(bool),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Str(String),
    Bin(Vec<u8>),
    // TODO: Add Date/Time/Decimal, etc.
}

impl Scalar {
    pub fn data_type(&self) -> DataType {
        match self {
            Scalar::Null => DataType::Utf8, // TODO: carry explicit Null type if needed
            Scalar::Bool(_) => DataType::Boolean,
            Scalar::I32(_) => DataType::Int32,
            Scalar::I64(_) => DataType::Int64,
            Scalar::F32(_) => DataType::Float32,
            Scalar::F64(_) => DataType::Float64,
            Scalar::Str(_) => DataType::Utf8,
            Scalar::Bin(_) => DataType::Binary,
        }
    }
}

/// Minimal column representation. Replace with Arrow arrays downstream.
/// In operators, you'll avoid this `Vec<Scalar>` for performance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub values: Vec<Scalar>,
}

impl Column {
    pub fn len(&self) -> usize {
        self.values.len()
    }
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

/// Minimal row batch for prototyping. Real engine will use columnar representation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowBatch {
    pub columns: Vec<Column>,
}

impl RowBatch {
    pub fn num_rows(&self) -> usize {
        self.columns.first().map(|c| c.len()).unwrap_or(0)
    }

    /// Sort rows by the specified columns (in order).
    ///
    /// Creates a vector of (sort_key_tuple, original_index), sorts it,
    /// then reorders all columns accordingly.
    pub fn sort_by_columns(&mut self, sort_keys: &[String]) -> Result<(), String> {
        let num_rows = self.num_rows();
        if num_rows == 0 {
            return Ok(());
        }

        // Find column indices for sort keys
        let key_indices: Vec<usize> = sort_keys
            .iter()
            .map(|key| {
                self.columns
                    .iter()
                    .position(|c| &c.name == key)
                    .ok_or_else(|| format!("sort key column '{}' not found", key))
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Build (sort_key_tuple, original_index) vector
        let mut indices: Vec<(Vec<Scalar>, usize)> = (0..num_rows)
            .map(|row_idx| {
                let sort_tuple: Vec<Scalar> = key_indices
                    .iter()
                    .map(|&col_idx| self.columns[col_idx].values[row_idx].clone())
                    .collect();
                (sort_tuple, row_idx)
            })
            .collect();

        // Sort by the tuple (lexicographic comparison)
        indices.sort_by(|(a, _), (b, _)| scalar_tuple_cmp(a, b));

        // Reorder all columns based on sorted indices
        for col in &mut self.columns {
            let original = col.values.clone();
            col.values = indices
                .iter()
                .map(|(_, idx)| original[*idx].clone())
                .collect();
        }

        Ok(())
    }

    /// Compute a hash partition index for a row based on specified columns.
    ///
    /// Returns a vector of partition indices (one per row), computed by
    /// hashing the specified columns and taking modulo num_partitions.
    pub fn hash_columns(
        &self,
        hash_keys: &[String],
        num_partitions: usize,
    ) -> Result<Vec<usize>, String> {
        let num_rows = self.num_rows();
        if num_rows == 0 {
            return Ok(Vec::new());
        }

        // Find column indices for hash keys
        let key_indices: Vec<usize> = hash_keys
            .iter()
            .map(|key| {
                self.columns
                    .iter()
                    .position(|c| &c.name == key)
                    .ok_or_else(|| format!("hash key column '{}' not found", key))
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Compute hash for each row
        let mut result = Vec::with_capacity(num_rows);
        for row_idx in 0..num_rows {
            let mut hasher = blake3::Hasher::new();
            for &col_idx in &key_indices {
                hash_scalar(&self.columns[col_idx].values[row_idx], &mut hasher);
            }
            let hash = hasher.finalize();
            let hash_u64 = u64::from_le_bytes(hash.as_bytes()[0..8].try_into().unwrap());
            result.push((hash_u64 as usize) % num_partitions);
        }

        Ok(result)
    }

    /// Concatenate two RowBatches side-by-side (for join results).
    ///
    /// All rows from `left` and `right` must have the same count.
    /// Columns are interleaved: left columns followed by right columns.
    pub fn concat(left: &RowBatch, right: &RowBatch) -> Result<RowBatch, String> {
        if left.num_rows() != right.num_rows() {
            return Err(format!(
                "cannot concat batches with different row counts: {} vs {}",
                left.num_rows(),
                right.num_rows()
            ));
        }

        let mut columns = Vec::with_capacity(left.columns.len() + right.columns.len());

        // Add left columns
        for col in &left.columns {
            columns.push(col.clone());
        }

        // Add right columns (with suffix if name conflicts)
        for col in &right.columns {
            let mut new_col = col.clone();
            // Check if column name already exists in left
            if left.columns.iter().any(|c| c.name == col.name) {
                new_col.name = format!("{}_right", col.name);
            }
            columns.push(new_col);
        }

        Ok(RowBatch { columns })
    }
}

/// Compare two scalar tuples lexicographically for sorting.
fn scalar_tuple_cmp(a: &[Scalar], b: &[Scalar]) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    for (x, y) in a.iter().zip(b.iter()) {
        match scalar_cmp(x, y) {
            Ordering::Equal => continue,
            other => return other,
        }
    }
    a.len().cmp(&b.len())
}

/// Compare two scalars for sorting.
///
/// Nulls are sorted first, then values are compared by type.
fn scalar_cmp(a: &Scalar, b: &Scalar) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    use Scalar::*;

    match (a, b) {
        (Null, Null) => Ordering::Equal,
        (Null, _) => Ordering::Less,
        (_, Null) => Ordering::Greater,
        (Bool(x), Bool(y)) => x.cmp(y),
        (I32(x), I32(y)) => x.cmp(y),
        (I64(x), I64(y)) => x.cmp(y),
        (F32(x), F32(y)) => {
            if x.is_nan() && y.is_nan() {
                Ordering::Equal
            } else if x.is_nan() {
                Ordering::Greater
            } else if y.is_nan() {
                Ordering::Less
            } else {
                x.partial_cmp(y).unwrap_or(Ordering::Equal)
            }
        }
        (F64(x), F64(y)) => {
            if x.is_nan() && y.is_nan() {
                Ordering::Equal
            } else if x.is_nan() {
                Ordering::Greater
            } else if y.is_nan() {
                Ordering::Less
            } else {
                x.partial_cmp(y).unwrap_or(Ordering::Equal)
            }
        }
        (Str(x), Str(y)) => x.cmp(y),
        (Bin(x), Bin(y)) => x.cmp(y),
        // Mixed types: order by variant order
        _ => scalar_type_order(a).cmp(&scalar_type_order(b)),
    }
}

/// Assign a numeric order to scalar types for mixed-type comparisons.
fn scalar_type_order(s: &Scalar) -> u8 {
    use Scalar::*;
    match s {
        Null => 0,
        Bool(_) => 1,
        I32(_) => 2,
        I64(_) => 3,
        F32(_) => 4,
        F64(_) => 5,
        Str(_) => 6,
        Bin(_) => 7,
    }
}

/// Hash a scalar value into a hasher.
fn hash_scalar(scalar: &Scalar, hasher: &mut blake3::Hasher) {
    use Scalar::*;

    // Write type discriminant first
    hasher.update(&[scalar_type_order(scalar)]);

    match scalar {
        Null => {}
        Bool(b) => {
            hasher.update(&[*b as u8]);
        }
        I32(i) => {
            hasher.update(&i.to_le_bytes());
        }
        I64(i) => {
            hasher.update(&i.to_le_bytes());
        }
        F32(f) => {
            hasher.update(&f.to_bits().to_le_bytes());
        }
        F64(f) => {
            hasher.update(&f.to_bits().to_le_bytes());
        }
        Str(s) => {
            hasher.update(s.as_bytes());
        }
        Bin(b) => {
            hasher.update(b);
        }
    }
}
