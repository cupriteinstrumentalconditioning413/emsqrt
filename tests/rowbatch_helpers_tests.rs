//! RowBatch helper functions tests (sort, hash, concat)

mod test_data_gen;

use emsqrt_core::schema::{DataType, Field, Schema};
use emsqrt_core::types::{Column, RowBatch, Scalar};
use test_data_gen::generate_sorted_batch;

#[test]
fn test_sort_by_single_column() {
    // Create unsorted batch
    let mut batch = RowBatch {
        columns: vec![Column {
            name: "value".to_string(),
            values: vec![
                Scalar::I64(50),
                Scalar::I64(10),
                Scalar::I64(30),
                Scalar::I64(20),
                Scalar::I64(40),
            ],
        }],
    };

    // Sort by value column
    batch
        .sort_by_columns(&["value".to_string()])
        .expect("Sort failed");

    // Verify ascending order
    let values = &batch.columns[0].values;
    assert_eq!(values[0], Scalar::I64(10));
    assert_eq!(values[1], Scalar::I64(20));
    assert_eq!(values[2], Scalar::I64(30));
    assert_eq!(values[3], Scalar::I64(40));
    assert_eq!(values[4], Scalar::I64(50));
}

#[test]
fn test_sort_by_multiple_columns() {
    // Create batch with two sort keys
    let mut batch = RowBatch {
        columns: vec![
            Column {
                name: "category".to_string(),
                values: vec![
                    Scalar::Str("B".to_string()),
                    Scalar::Str("A".to_string()),
                    Scalar::Str("B".to_string()),
                    Scalar::Str("A".to_string()),
                ],
            },
            Column {
                name: "priority".to_string(),
                values: vec![
                    Scalar::I32(2),
                    Scalar::I32(3),
                    Scalar::I32(1),
                    Scalar::I32(1),
                ],
            },
        ],
    };

    // Sort by category, then priority
    batch
        .sort_by_columns(&["category".to_string(), "priority".to_string()])
        .expect("Sort failed");

    // Verify lexicographic order: (A,1), (A,3), (B,1), (B,2)
    assert_eq!(batch.columns[0].values[0], Scalar::Str("A".to_string()));
    assert_eq!(batch.columns[1].values[0], Scalar::I32(1));

    assert_eq!(batch.columns[0].values[1], Scalar::Str("A".to_string()));
    assert_eq!(batch.columns[1].values[1], Scalar::I32(3));

    assert_eq!(batch.columns[0].values[2], Scalar::Str("B".to_string()));
    assert_eq!(batch.columns[1].values[2], Scalar::I32(1));

    assert_eq!(batch.columns[0].values[3], Scalar::Str("B".to_string()));
    assert_eq!(batch.columns[1].values[3], Scalar::I32(2));
}

#[test]
fn test_sort_with_nulls() {
    let mut batch = RowBatch {
        columns: vec![Column {
            name: "nullable".to_string(),
            values: vec![
                Scalar::I64(5),
                Scalar::Null,
                Scalar::I64(3),
                Scalar::Null,
                Scalar::I64(7),
            ],
        }],
    };

    batch
        .sort_by_columns(&["nullable".to_string()])
        .expect("Sort failed");

    // Nulls should sort first
    assert_eq!(batch.columns[0].values[0], Scalar::Null);
    assert_eq!(batch.columns[0].values[1], Scalar::Null);
    assert_eq!(batch.columns[0].values[2], Scalar::I64(3));
    assert_eq!(batch.columns[0].values[3], Scalar::I64(5));
    assert_eq!(batch.columns[0].values[4], Scalar::I64(7));
}

#[test]
fn test_hash_columns_distribution() {
    // Generate 1000 rows and hash into 10 partitions
    let mut values = Vec::new();
    for i in 0..1000 {
        values.push(Scalar::I64(i as i64));
    }

    let batch = RowBatch {
        columns: vec![Column {
            name: "id".to_string(),
            values,
        }],
    };

    let num_partitions = 10;
    let partition_indices = batch
        .hash_columns(&["id".to_string()], num_partitions)
        .expect("Hash failed");

    assert_eq!(partition_indices.len(), 1000);

    // Count rows per partition
    let mut partition_counts = vec![0; num_partitions];
    for &partition in &partition_indices {
        assert!(partition < num_partitions, "Partition index out of range");
        partition_counts[partition] += 1;
    }

    // Each partition should have roughly 100 rows (1000 / 10)
    // Allow some variance but check none are empty or overly full
    for (i, &count) in partition_counts.iter().enumerate() {
        assert!(
            count > 50 && count < 150,
            "Partition {} has unbalanced count: {}",
            i,
            count
        );
    }

    // Verify at least 8 of 10 partitions are used (not too skewed)
    let non_empty = partition_counts.iter().filter(|&&c| c > 0).count();
    assert!(non_empty >= 8, "Too few partitions used: {}", non_empty);
}

#[test]
fn test_hash_columns_consistency() {
    let batch = RowBatch {
        columns: vec![
            Column {
                name: "key1".to_string(),
                values: vec![
                    Scalar::Str("A".to_string()),
                    Scalar::Str("B".to_string()),
                    Scalar::Str("C".to_string()),
                ],
            },
            Column {
                name: "key2".to_string(),
                values: vec![Scalar::I32(1), Scalar::I32(2), Scalar::I32(3)],
            },
        ],
    };

    let num_partitions = 4;

    // Hash twice
    let hash1 = batch
        .hash_columns(&["key1".to_string(), "key2".to_string()], num_partitions)
        .expect("Hash failed");
    let hash2 = batch
        .hash_columns(&["key1".to_string(), "key2".to_string()], num_partitions)
        .expect("Hash failed");

    // Should produce identical results
    assert_eq!(hash1, hash2, "Hash should be deterministic");
}

#[test]
fn test_concat_schemas() {
    let left = RowBatch {
        columns: vec![
            Column {
                name: "id".to_string(),
                values: vec![Scalar::I64(1), Scalar::I64(2)],
            },
            Column {
                name: "name".to_string(),
                values: vec![
                    Scalar::Str("Alice".to_string()),
                    Scalar::Str("Bob".to_string()),
                ],
            },
        ],
    };

    let right = RowBatch {
        columns: vec![
            Column {
                name: "age".to_string(),
                values: vec![Scalar::I32(30), Scalar::I32(25)],
            },
            Column {
                name: "city".to_string(),
                values: vec![
                    Scalar::Str("NYC".to_string()),
                    Scalar::Str("LA".to_string()),
                ],
            },
        ],
    };

    let result = RowBatch::concat(&left, &right).expect("Concat failed");

    // Verify column count
    assert_eq!(result.columns.len(), 4);
    assert_eq!(result.num_rows(), 2);

    // Verify column names
    assert_eq!(result.columns[0].name, "id");
    assert_eq!(result.columns[1].name, "name");
    assert_eq!(result.columns[2].name, "age");
    assert_eq!(result.columns[3].name, "city");

    // Verify data integrity
    assert_eq!(result.columns[0].values[0], Scalar::I64(1));
    assert_eq!(
        result.columns[1].values[0],
        Scalar::Str("Alice".to_string())
    );
    assert_eq!(result.columns[2].values[0], Scalar::I32(30));
    assert_eq!(result.columns[3].values[0], Scalar::Str("NYC".to_string()));
}

#[test]
fn test_concat_name_collision() {
    let left = RowBatch {
        columns: vec![
            Column {
                name: "id".to_string(),
                values: vec![Scalar::I64(1)],
            },
            Column {
                name: "value".to_string(),
                values: vec![Scalar::I32(100)],
            },
        ],
    };

    let right = RowBatch {
        columns: vec![
            Column {
                name: "id".to_string(), // Collision with left
                values: vec![Scalar::I64(2)],
            },
            Column {
                name: "score".to_string(),
                values: vec![Scalar::I32(95)],
            },
        ],
    };

    let result = RowBatch::concat(&left, &right).expect("Concat failed");

    // Should have 4 columns
    assert_eq!(result.columns.len(), 4);

    // Right side's "id" should be renamed to "id_right"
    assert_eq!(result.columns[0].name, "id");
    assert_eq!(result.columns[1].name, "value");
    assert_eq!(result.columns[2].name, "id_right");
    assert_eq!(result.columns[3].name, "score");

    // Verify data
    assert_eq!(result.columns[0].values[0], Scalar::I64(1));
    assert_eq!(result.columns[2].values[0], Scalar::I64(2));
}

#[test]
fn test_sort_empty_batch() {
    let mut batch = RowBatch { columns: vec![] };

    // Should not crash on empty batch
    let result = batch.sort_by_columns(&["nonexistent".to_string()]);
    assert!(result.is_err() || batch.num_rows() == 0);
}

#[test]
fn test_hash_empty_batch() {
    let batch = RowBatch { columns: vec![] };

    let result = batch.hash_columns(&["nonexistent".to_string()], 4);
    // Should either error or return empty result
    assert!(result.is_err() || result.unwrap().is_empty());
}

#[test]
fn test_concat_empty_batches() {
    let empty1 = RowBatch { columns: vec![] };
    let empty2 = RowBatch { columns: vec![] };

    let result = RowBatch::concat(&empty1, &empty2).expect("Concat failed");
    assert_eq!(result.columns.len(), 0);
}

#[test]
fn test_concat_mismatched_row_counts() {
    let batch1 = RowBatch {
        columns: vec![Column {
            name: "a".to_string(),
            values: vec![Scalar::I64(1), Scalar::I64(2)],
        }],
    };

    let batch2 = RowBatch {
        columns: vec![Column {
            name: "b".to_string(),
            values: vec![Scalar::I64(3)], // Only 1 row
        }],
    };

    let result = RowBatch::concat(&batch1, &batch2);
    assert!(
        result.is_err(),
        "Concat should fail on mismatched row counts"
    );
}

#[test]
fn test_sort_preserves_row_order_across_columns() {
    // Verify that sorting one column keeps other column values aligned
    let mut batch = RowBatch {
        columns: vec![
            Column {
                name: "sort_key".to_string(),
                values: vec![Scalar::I32(30), Scalar::I32(10), Scalar::I32(20)],
            },
            Column {
                name: "associated_data".to_string(),
                values: vec![
                    Scalar::Str("third".to_string()),
                    Scalar::Str("first".to_string()),
                    Scalar::Str("second".to_string()),
                ],
            },
        ],
    };

    batch
        .sort_by_columns(&["sort_key".to_string()])
        .expect("Sort failed");

    // Verify associated data moved with the key
    assert_eq!(batch.columns[0].values[0], Scalar::I32(10));
    assert_eq!(batch.columns[1].values[0], Scalar::Str("first".to_string()));

    assert_eq!(batch.columns[0].values[1], Scalar::I32(20));
    assert_eq!(
        batch.columns[1].values[1],
        Scalar::Str("second".to_string())
    );

    assert_eq!(batch.columns[0].values[2], Scalar::I32(30));
    assert_eq!(batch.columns[1].values[2], Scalar::Str("third".to_string()));
}
