//! External sort operator tests

mod test_data_gen;

use emsqrt_core::schema::{DataType, Field, Schema};
use emsqrt_core::types::{Column, RowBatch, Scalar};
use emsqrt_io::storage::FsStorage;
use emsqrt_mem::{Codec, MemoryBudgetImpl, SpillManager};
use emsqrt_operators::sort::external::ExternalSort;
use emsqrt_operators::traits::{MemoryBudget, Operator};
use std::sync::{Arc, Mutex};
use test_data_gen::{create_temp_spill_dir, generate_random_batch, generate_sorted_batch};

fn setup_sort_operator(
    codec: Codec,
    spill_dir: String,
) -> (ExternalSort, Arc<Mutex<SpillManager>>) {
    let storage = Box::new(FsStorage::new());
    let mgr = SpillManager::new(storage, codec, format!("{}/sort-spills", spill_dir));
    let spill_mgr = Arc::new(Mutex::new(mgr));

    let sort_op = ExternalSort {
        by: vec!["sort_key".to_string()],
        spill_mgr: Some(Arc::clone(&spill_mgr)),
    };

    (sort_op, spill_mgr)
}

fn cleanup_spill_dir(dir: &str) {
    let _ = std::fs::remove_dir_all(dir);
}

fn verify_sorted(batch: &RowBatch, key_col: &str) -> bool {
    let key_idx = batch.columns.iter().position(|c| c.name == key_col);
    if key_idx.is_none() {
        return false;
    }
    let key_idx = key_idx.unwrap();

    for i in 1..batch.num_rows() {
        let prev = &batch.columns[key_idx].values[i - 1];
        let curr = &batch.columns[key_idx].values[i];

        // Simple comparison for common types
        match (prev, curr) {
            (Scalar::I32(a), Scalar::I32(b)) => {
                if a > b {
                    return false;
                }
            }
            (Scalar::I64(a), Scalar::I64(b)) => {
                if a > b {
                    return false;
                }
            }
            (Scalar::F64(a), Scalar::F64(b)) => {
                if a > b {
                    return false;
                }
            }
            (Scalar::Str(a), Scalar::Str(b)) => {
                if a > b {
                    return false;
                }
            }
            (Scalar::Null, _) => {
                // Nulls sort first, so ok
            }
            (_, Scalar::Null) => {
                // Non-null after null is wrong
                return false;
            }
            _ => {
                // For other types, assume ok
            }
        }
    }

    true
}

#[test]
fn test_sort_in_memory() {
    let spill_dir = create_temp_spill_dir();
    std::fs::create_dir_all(&spill_dir).expect("Failed to create spill dir");

    let (sort_op, _spill_mgr) = setup_sort_operator(Codec::None, spill_dir.clone());
    let budget = MemoryBudgetImpl::new(10 * 1024 * 1024); // 10MB - plenty for in-memory sort

    // Create unsorted batch (small enough to fit in memory)
    let mut batch = RowBatch {
        columns: vec![
            Column {
                name: "sort_key".to_string(),
                values: vec![
                    Scalar::I64(50),
                    Scalar::I64(10),
                    Scalar::I64(90),
                    Scalar::I64(30),
                    Scalar::I64(70),
                    Scalar::I64(20),
                ],
            },
            Column {
                name: "data".to_string(),
                values: vec![
                    Scalar::Str("e".to_string()),
                    Scalar::Str("a".to_string()),
                    Scalar::Str("i".to_string()),
                    Scalar::Str("c".to_string()),
                    Scalar::Str("g".to_string()),
                    Scalar::Str("b".to_string()),
                ],
            },
        ],
    };

    let result = sort_op
        .eval_block(&[batch.clone()], &budget)
        .expect("Sort failed");

    // Verify sorted
    assert_eq!(result.num_rows(), 6);
    assert!(
        verify_sorted(&result, "sort_key"),
        "Result should be sorted"
    );

    // Check specific values
    assert_eq!(result.columns[0].values[0], Scalar::I64(10));
    assert_eq!(result.columns[0].values[5], Scalar::I64(90));

    // Verify data moved with keys
    assert_eq!(result.columns[1].values[0], Scalar::Str("a".to_string()));
    assert_eq!(result.columns[1].values[5], Scalar::Str("i".to_string()));

    cleanup_spill_dir(&spill_dir);
}

#[test]
fn test_sort_with_spill() {
    let spill_dir = create_temp_spill_dir();
    std::fs::create_dir_all(&spill_dir).expect("Failed to create spill dir");

    let (sort_op, _spill_mgr) = setup_sort_operator(Codec::None, spill_dir.clone());
    let budget = MemoryBudgetImpl::new(50 * 1024); // 50KB - small budget to force spilling

    // Generate larger dataset
    let schema = Schema::new(vec![
        Field::new("sort_key", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]);

    let mut batch = generate_random_batch(1000, &schema);

    // Ensure the sort_key column exists and has variety
    for i in 0..batch.columns[0].values.len() {
        batch.columns[0].values[i] = Scalar::I64((1000 - i) as i64); // Reverse order
    }

    let result = sort_op
        .eval_block(&[batch], &budget)
        .expect("Sort with spill failed");

    // Verify result is sorted
    assert_eq!(result.num_rows(), 1000);
    assert!(
        verify_sorted(&result, "sort_key"),
        "Result should be sorted after spilling"
    );

    // Check first and last values
    assert_eq!(result.columns[0].values[0], Scalar::I64(1));
    assert_eq!(result.columns[0].values[999], Scalar::I64(1000));

    cleanup_spill_dir(&spill_dir);
}

#[test]
fn test_sort_multiple_runs() {
    let spill_dir = create_temp_spill_dir();
    std::fs::create_dir_all(&spill_dir).expect("Failed to create spill dir");

    let (sort_op, _spill_mgr) = setup_sort_operator(Codec::None, spill_dir.clone());
    let budget = MemoryBudgetImpl::new(20 * 1024); // Very small budget to force multiple runs

    // Generate dataset that will create multiple runs
    let mut values = Vec::new();
    for i in 0..500 {
        values.push(Scalar::I64((500 - i) as i64)); // Descending order
    }

    let batch = RowBatch {
        columns: vec![Column {
            name: "sort_key".to_string(),
            values,
        }],
    };

    let result = sort_op
        .eval_block(&[batch], &budget)
        .expect("Multi-run sort failed");

    assert_eq!(result.num_rows(), 500);
    assert!(verify_sorted(&result, "sort_key"));

    // Verify full range
    assert_eq!(result.columns[0].values[0], Scalar::I64(1));
    assert_eq!(result.columns[0].values[499], Scalar::I64(500));

    cleanup_spill_dir(&spill_dir);
}

#[test]
fn test_sort_stability() {
    let spill_dir = create_temp_spill_dir();
    std::fs::create_dir_all(&spill_dir).expect("Failed to create spill dir");

    let (sort_op, _spill_mgr) = setup_sort_operator(Codec::None, spill_dir.clone());
    let budget = MemoryBudgetImpl::new(10 * 1024 * 1024);

    // Create batch with duplicate keys
    let batch = RowBatch {
        columns: vec![
            Column {
                name: "sort_key".to_string(),
                values: vec![
                    Scalar::I32(2),
                    Scalar::I32(1),
                    Scalar::I32(2),
                    Scalar::I32(1),
                    Scalar::I32(2),
                ],
            },
            Column {
                name: "order_marker".to_string(),
                values: vec![
                    Scalar::Str("first_2".to_string()),
                    Scalar::Str("first_1".to_string()),
                    Scalar::Str("second_2".to_string()),
                    Scalar::Str("second_1".to_string()),
                    Scalar::Str("third_2".to_string()),
                ],
            },
        ],
    };

    let result = sort_op.eval_block(&[batch], &budget).expect("Sort failed");

    // Verify sorted by key
    assert!(verify_sorted(&result, "sort_key"));

    // For stable sort, equal keys should maintain relative order
    // All 1's should come before all 2's
    let mut saw_two = false;
    for i in 0..result.num_rows() {
        if let Scalar::I32(key) = result.columns[0].values[i] {
            if key == 2 {
                saw_two = true;
            } else if key == 1 && saw_two {
                panic!("Found 1 after 2 - not sorted correctly");
            }
        }
    }

    cleanup_spill_dir(&spill_dir);
}

#[test]
fn test_sort_large_keys() {
    let spill_dir = create_temp_spill_dir();
    std::fs::create_dir_all(&spill_dir).expect("Failed to create spill dir");

    let (sort_op, _spill_mgr) = setup_sort_operator(Codec::None, spill_dir.clone());
    let budget = MemoryBudgetImpl::new(1 * 1024 * 1024); // 1MB

    // Create batch with large string keys
    let mut values = Vec::new();
    for i in 0..100 {
        let large_string = format!("{:0>1000}", 100 - i); // 1KB strings in reverse order
        values.push(Scalar::Str(large_string));
    }

    let batch = RowBatch {
        columns: vec![Column {
            name: "sort_key".to_string(),
            values,
        }],
    };

    let result = sort_op
        .eval_block(&[batch], &budget)
        .expect("Sort with large keys failed");

    assert_eq!(result.num_rows(), 100);
    assert!(verify_sorted(&result, "sort_key"));

    cleanup_spill_dir(&spill_dir);
}

#[test]
fn test_sort_empty_batch() {
    let spill_dir = create_temp_spill_dir();
    std::fs::create_dir_all(&spill_dir).expect("Failed to create spill dir");

    let (sort_op, _spill_mgr) = setup_sort_operator(Codec::None, spill_dir.clone());
    let budget = MemoryBudgetImpl::new(10 * 1024 * 1024);

    let batch = RowBatch {
        columns: vec![Column {
            name: "sort_key".to_string(),
            values: vec![],
        }],
    };

    let result = sort_op
        .eval_block(&[batch], &budget)
        .expect("Sort empty batch failed");

    assert_eq!(result.num_rows(), 0);

    cleanup_spill_dir(&spill_dir);
}

#[test]
fn test_sort_with_nulls() {
    let spill_dir = create_temp_spill_dir();
    std::fs::create_dir_all(&spill_dir).expect("Failed to create spill dir");

    let (sort_op, _spill_mgr) = setup_sort_operator(Codec::None, spill_dir.clone());
    let budget = MemoryBudgetImpl::new(10 * 1024 * 1024);

    let batch = RowBatch {
        columns: vec![Column {
            name: "sort_key".to_string(),
            values: vec![
                Scalar::I64(50),
                Scalar::Null,
                Scalar::I64(30),
                Scalar::Null,
                Scalar::I64(10),
            ],
        }],
    };

    let result = sort_op
        .eval_block(&[batch], &budget)
        .expect("Sort with nulls failed");

    assert_eq!(result.num_rows(), 5);

    // Nulls should sort first
    assert_eq!(result.columns[0].values[0], Scalar::Null);
    assert_eq!(result.columns[0].values[1], Scalar::Null);
    assert_eq!(result.columns[0].values[2], Scalar::I64(10));
    assert_eq!(result.columns[0].values[3], Scalar::I64(30));
    assert_eq!(result.columns[0].values[4], Scalar::I64(50));

    cleanup_spill_dir(&spill_dir);
}

#[test]
fn test_sort_already_sorted() {
    let spill_dir = create_temp_spill_dir();
    std::fs::create_dir_all(&spill_dir).expect("Failed to create spill dir");

    let (sort_op, _spill_mgr) = setup_sort_operator(Codec::None, spill_dir.clone());
    let budget = MemoryBudgetImpl::new(10 * 1024 * 1024);

    // Create already sorted batch
    let mut values = Vec::new();
    for i in 0..100 {
        values.push(Scalar::I64(i as i64));
    }

    let batch = RowBatch {
        columns: vec![Column {
            name: "sort_key".to_string(),
            values,
        }],
    };

    let result = sort_op
        .eval_block(&[batch], &budget)
        .expect("Sort already sorted failed");

    assert_eq!(result.num_rows(), 100);
    assert!(verify_sorted(&result, "sort_key"));

    cleanup_spill_dir(&spill_dir);
}
