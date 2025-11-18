use std::cmp::Ordering;
use std::collections::HashMap;

use emsqrt_core::prelude::Schema;
use emsqrt_core::types::{Column, RowBatch, Scalar};
use emsqrt_mem::guard::BudgetGuardImpl;

use crate::plan::{Footprint, OpPlan};
use crate::traits::{MemoryBudget, OpError, Operator};

#[derive(Debug, Default, Clone)]
pub struct WindowOp {
    pub partitions: Vec<String>,
    pub order_by: Vec<String>,
    pub functions: Vec<WindowFnSpec>,
}

#[derive(Debug, Clone)]
pub struct WindowFnSpec {
    pub kind: WindowFnKind,
    pub alias: String,
}

#[derive(Debug, Clone)]
pub enum WindowFnKind {
    RowNumber,
    Sum { column: String },
}

impl Operator for WindowOp {
    fn name(&self) -> &'static str {
        "window"
    }

    fn memory_need(&self, rows: u64, _bytes: u64) -> Footprint {
        Footprint {
            bytes_per_row: 32 * (self.functions.len() as u64),
            overhead_bytes: rows.saturating_mul(16),
        }
    }

    fn plan(&self, input_schemas: &[Schema]) -> Result<OpPlan, OpError> {
        if input_schemas.len() != 1 {
            return Err(OpError::Plan(
                "window operator expects exactly one input schema".into(),
            ));
        }
        let mut schema = input_schemas[0].clone();
        for spec in &self.functions {
            let dt = match &spec.kind {
                WindowFnKind::RowNumber => emsqrt_core::schema::DataType::Int64,
                WindowFnKind::Sum { .. } => emsqrt_core::schema::DataType::Float64,
            };
            schema.fields.push(emsqrt_core::schema::Field::new(
                spec.alias.clone(),
                dt,
                true,
            ));
        }
        Ok(OpPlan::new(schema, self.memory_need(0, 0)))
    }

    fn eval_block(
        &self,
        inputs: &[RowBatch],
        _budget: &dyn MemoryBudget<Guard = BudgetGuardImpl>,
    ) -> Result<RowBatch, OpError> {
        let input = inputs
            .get(0)
            .ok_or_else(|| OpError::Exec("window operator missing input batch".into()))?;
        let num_rows = input.num_rows();
        let mut output = input.clone();

        let mut name_to_index = HashMap::new();
        for (idx, col) in input.columns.iter().enumerate() {
            name_to_index.insert(col.name.clone(), idx);
        }

        let mut tuples: Vec<(Vec<Scalar>, usize)> = (0..num_rows)
            .map(|row_idx| {
                let mut key = Vec::new();
                for key_col in self.partitions.iter().chain(self.order_by.iter()) {
                    if let Some(&col_idx) = name_to_index.get(key_col) {
                        key.push(input.columns[col_idx].values[row_idx].clone());
                    } else {
                        key.push(Scalar::Null);
                    }
                }
                (key, row_idx)
            })
            .collect();
        tuples.sort_by(|(a, _), (b, _)| scalar_tuple_cmp(a, b));
        let order_indices: Vec<usize> = tuples.into_iter().map(|(_, idx)| idx).collect();

        let mut computed_columns: Vec<Vec<Scalar>> = self
            .functions
            .iter()
            .map(|_| vec![Scalar::Null; num_rows])
            .collect();

        let mut current_partition: Vec<Scalar> = Vec::new();
        let mut partition_initialized = false;
        let mut row_counter: i64 = 0;
        let mut running_sums: Vec<f64> = vec![0.0; self.functions.len()];

        for sorted_pos in order_indices {
            let part_key =
                extract_partition_key(input, &self.partitions, sorted_pos, &name_to_index)?;
            if !partition_initialized || part_key != current_partition {
                partition_initialized = true;
                current_partition = part_key;
                row_counter = 0;
                for sum in running_sums.iter_mut() {
                    *sum = 0.0;
                }
            }

            row_counter += 1;
            for (fn_idx, spec) in self.functions.iter().enumerate() {
                match &spec.kind {
                    WindowFnKind::RowNumber => {
                        computed_columns[fn_idx][sorted_pos] = Scalar::I64(row_counter);
                    }
                    WindowFnKind::Sum { column } => {
                        let col_idx = name_to_index.get(column).ok_or_else(|| {
                            OpError::Schema(format!("sum column '{column}' not found"))
                        })?;
                        let value = value_as_f64(&input.columns[*col_idx].values[sorted_pos])
                            .map_err(|e| OpError::Exec(e))?;
                        running_sums[fn_idx] += value;
                        computed_columns[fn_idx][sorted_pos] = Scalar::F64(running_sums[fn_idx]);
                    }
                }
            }
        }

        for (spec, values) in self.functions.iter().zip(computed_columns.into_iter()) {
            output.columns.push(Column {
                name: spec.alias.clone(),
                values,
            });
        }

        Ok(output)
    }
}

#[derive(Debug, Clone)]
pub struct LateralExplodeOp {
    pub column: String,
    pub alias: String,
    pub delimiter: String,
}

impl Default for LateralExplodeOp {
    fn default() -> Self {
        Self {
            column: "values".into(),
            alias: "exploded".into(),
            delimiter: ",".into(),
        }
    }
}

impl Operator for LateralExplodeOp {
    fn name(&self) -> &'static str {
        "lateral_explode"
    }

    fn memory_need(&self, rows: u64, _bytes: u64) -> Footprint {
        Footprint {
            bytes_per_row: 16,
            overhead_bytes: rows * 8,
        }
    }

    fn plan(&self, input_schemas: &[Schema]) -> Result<OpPlan, OpError> {
        if input_schemas.len() != 1 {
            return Err(OpError::Plan("lateral explode expects one input".into()));
        }
        let mut schema = input_schemas[0].clone();
        schema.fields.push(emsqrt_core::schema::Field::new(
            self.alias.clone(),
            emsqrt_core::schema::DataType::Utf8,
            true,
        ));
        Ok(OpPlan::new(schema, self.memory_need(0, 0)))
    }

    fn eval_block(
        &self,
        inputs: &[RowBatch],
        _budget: &dyn MemoryBudget<Guard = BudgetGuardImpl>,
    ) -> Result<RowBatch, OpError> {
        let input = inputs
            .get(0)
            .ok_or_else(|| OpError::Exec("lateral operator missing input".into()))?;

        let mut name_to_index = HashMap::new();
        for (idx, col) in input.columns.iter().enumerate() {
            name_to_index.insert(col.name.clone(), idx);
        }
        let target_idx = name_to_index
            .get(&self.column)
            .ok_or_else(|| OpError::Schema(format!("column '{}' not found", self.column)))?;

        let mut output_columns: Vec<Column> = input
            .columns
            .iter()
            .map(|col| Column {
                name: col.name.clone(),
                values: Vec::new(),
            })
            .collect();

        let mut alias_column = Column {
            name: self.alias.clone(),
            values: Vec::new(),
        };

        for row_idx in 0..input.num_rows() {
            let value = &input.columns[*target_idx].values[row_idx];
            let text = match value {
                Scalar::Str(s) => s.clone(),
                Scalar::Null => "".to_string(),
                other => scalar_to_string(other),
            };
            let parts: Vec<&str> = text.split(&self.delimiter).collect();
            for part in parts {
                for (col_idx, column) in input.columns.iter().enumerate() {
                    output_columns[col_idx]
                        .values
                        .push(column.values[row_idx].clone());
                }
                alias_column.values.push(Scalar::Str(part.to_string()));
            }
        }

        output_columns.push(alias_column);
        Ok(RowBatch {
            columns: output_columns,
        })
    }
}

fn extract_partition_key(
    batch: &RowBatch,
    partitions: &[String],
    row_idx: usize,
    name_map: &HashMap<String, usize>,
) -> Result<Vec<Scalar>, OpError> {
    if partitions.is_empty() {
        return Ok(vec![]);
    }
    let mut key = Vec::with_capacity(partitions.len());
    for name in partitions {
        let idx = name_map
            .get(name)
            .ok_or_else(|| OpError::Schema(format!("partition column '{name}' not found")))?;
        key.push(batch.columns[*idx].values[row_idx].clone());
    }
    Ok(key)
}

fn value_as_f64(value: &Scalar) -> Result<f64, String> {
    match value {
        Scalar::Null => Ok(0.0),
        Scalar::I32(v) => Ok(*v as f64),
        Scalar::I64(v) => Ok(*v as f64),
        Scalar::F32(v) => Ok(*v as f64),
        Scalar::F64(v) => Ok(*v),
        other => Err(format!("unsupported numeric type for sum: {:?}", other)),
    }
}

fn scalar_tuple_cmp(a: &[Scalar], b: &[Scalar]) -> Ordering {
    for (x, y) in a.iter().zip(b.iter()) {
        match scalar_cmp(x, y) {
            Ordering::Equal => continue,
            other => return other,
        }
    }
    a.len().cmp(&b.len())
}

fn scalar_cmp(x: &Scalar, y: &Scalar) -> Ordering {
    use Scalar::*;
    match (x, y) {
        (Null, Null) => Ordering::Equal,
        (Null, _) => Ordering::Less,
        (_, Null) => Ordering::Greater,
        (Bool(a), Bool(b)) => a.cmp(b),
        (I32(a), I32(b)) => a.cmp(b),
        (I64(a), I64(b)) => a.cmp(b),
        (F32(a), F32(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (F64(a), F64(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (Str(a), Str(b)) => a.cmp(b),
        (Bin(a), Bin(b)) => a.cmp(b),
        _ => Ordering::Equal,
    }
}

fn scalar_to_string(value: &Scalar) -> String {
    match value {
        Scalar::Null => "".to_string(),
        Scalar::Bool(v) => v.to_string(),
        Scalar::I32(v) => v.to_string(),
        Scalar::I64(v) => v.to_string(),
        Scalar::F32(v) => v.to_string(),
        Scalar::F64(v) => v.to_string(),
        Scalar::Str(s) => s.clone(),
        Scalar::Bin(bytes) => format!("{:?}", bytes),
    }
}
