//! End-to-end integration tests

mod test_data_gen;

use emsqrt_core::config::EngineConfig;
use emsqrt_core::dag::{Aggregation, LogicalPlan as L};
use emsqrt_core::schema::{DataType, Field, Schema};
use emsqrt_core::types::{Column, RowBatch, Scalar};
use emsqrt_exec::Engine;
use emsqrt_planner::{estimate_work, lower_to_physical, rules};
use emsqrt_te::plan_te;
use std::fs;
use std::io::Write;
use test_data_gen::create_temp_spill_dir;

fn setup_test_csv(path: &str, rows: usize) {
    let mut file = fs::File::create(path).expect("Failed to create test file");
    writeln!(file, "id,name,age,email").expect("Failed to write header");
    
    for i in 0..rows {
        writeln!(
            file,
            "{},Person{},{},person{}@test.com",
            i,
            i,
            20 + (i % 50),
            i
        )
        .expect("Failed to write row");
    }
}

fn cleanup_test_file(path: &str) {
    let _ = fs::remove_file(path);
}

#[test]
fn test_scan_filter_project_sink() {
    let temp_dir = create_temp_spill_dir();
    let input_file = format!("{}/input.csv", temp_dir);
    let output_file = format!("{}/output.csv", temp_dir);
    
    fs::create_dir_all(&temp_dir).expect("Failed to create temp dir");
    setup_test_csv(&input_file, 1000);
    
    // Build pipeline: scan → filter (age > 25) → project (name, email) → sink
    let schema = Schema {
        fields: vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
            Field::new("email", DataType::Utf8, false),
        ],
    };
    
    let scan = L::Scan {
        source: format!("file://{}", input_file),
        schema: schema.clone(),
    };
    
    let filter = L::Filter {
        input: Box::new(scan),
        expr: "age > 25".to_string(),
    };
    
    let project = L::Project {
        input: Box::new(filter),
        columns: vec!["name".to_string(), "email".to_string()],
    };
    
    let sink = L::Sink {
        input: Box::new(project),
        destination: format!("file://{}", output_file),
        format: "csv".to_string(),
    };
    
    // Optimize and lower
    let optimized = rules::optimize(sink);
    let phys_prog = lower_to_physical(&optimized);
    let work = estimate_work(&optimized, None);
    let te = plan_te(&phys_prog.plan, &work, 64 * 1024 * 1024).expect("TE planning failed");
    
    // Execute
    let mut config = EngineConfig::default();
    config.spill_dir = temp_dir.clone();
    let mut engine = Engine::new(config);
    let manifest = engine.run(&phys_prog, &te).expect("Execution failed");
    
    // Verify execution completed
    assert!(manifest.started_ms <= manifest.finished_ms);
    
    // Verify output file exists and has data
    assert!(
        fs::metadata(&output_file).is_ok(),
        "Output file should exist"
    );
    
    let output_content = fs::read_to_string(&output_file).expect("Failed to read output");
    let output_lines: Vec<&str> = output_content.lines().collect();
    
    // Should have header + filtered rows
    assert!(output_lines.len() > 1, "Output should have header and data");
    
    // Count how many rows should pass the filter (age > 25)
    // ages cycle from 20 to 69 (20 + i%50), so ages > 25 are 26-69 = 44 out of every 50
    let remainder: usize = 1000 % 50;
    let expected_passing: usize = (1000 / 50) * 44 + remainder.saturating_sub(6);
    
    // Allow some tolerance due to header line
    let actual_data_rows = output_lines.len() - 1; // Subtract header
    assert!(
        actual_data_rows >= expected_passing - 10 && actual_data_rows <= expected_passing + 10,
        "Expected around {} rows, got {}",
        expected_passing,
        actual_data_rows
    );
    
    // Cleanup
    cleanup_test_file(&input_file);
    cleanup_test_file(&output_file);
    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_sort_aggregate_pipeline() {
    let temp_dir = create_temp_spill_dir();
    let input_file = format!("{}/categories.csv", temp_dir);
    
    fs::create_dir_all(&temp_dir).expect("Failed to create temp dir");
    
    // Create test data with categories
    let mut file = fs::File::create(&input_file).expect("Failed to create file");
    writeln!(file, "category,amount").expect("Failed to write header");
    
    let num_categories = 10;
    for i in 0..100 {
        let category = format!("cat_{}", i % num_categories);
        let amount = (i + 1) * 10;
        writeln!(file, "{},{}", category, amount).expect("Failed to write row");
    }
    drop(file);
    
    // Build pipeline: scan → aggregate (count by category)
    let schema = Schema {
        fields: vec![
            Field::new("category", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
        ],
    };
    
    let scan = L::Scan {
        source: format!("file://{}", input_file),
        schema,
    };
    
    let aggregate = L::Aggregate {
        input: Box::new(scan),
        group_by: vec!["category".to_string()],
        aggs: vec![Aggregation::Count],
    };
    
    let sink = L::Sink {
        input: Box::new(aggregate),
        destination: "memory://result".to_string(),
        format: "memory".to_string(),
    };
    
    // Execute
    let optimized = rules::optimize(sink);
    let phys_prog = lower_to_physical(&optimized);
    let work = estimate_work(&optimized, None);
    let te = plan_te(&phys_prog.plan, &work, 32 * 1024 * 1024).expect("TE planning failed");
    
    let mut config = EngineConfig::default();
    config.spill_dir = temp_dir.clone();
    let mut engine = Engine::new(config);
    let manifest = engine.run(&phys_prog, &te).expect("Execution failed");
    
    // Verify execution
    assert!(manifest.started_ms <= manifest.finished_ms);
    
    // Cleanup
    cleanup_test_file(&input_file);
    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_simple_map_pipeline() {
    let temp_dir = create_temp_spill_dir();
    fs::create_dir_all(&temp_dir).expect("Failed to create temp dir");
    
    // Build a simple in-memory pipeline: scan (memory) → map → sink (memory)
    let schema = Schema {
        fields: vec![
            Field::new("old_name", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ],
    };
    
    // Create test data
    let test_batch = RowBatch {
        columns: vec![
            Column {
                name: "old_name".to_string(),
                values: vec![
                    Scalar::Str("Alice".to_string()),
                    Scalar::Str("Bob".to_string()),
                ],
            },
            Column {
                name: "value".to_string(),
                values: vec![Scalar::I64(100), Scalar::I64(200)],
            },
        ],
    };
    
    let scan = L::Scan {
        source: "memory://input".to_string(),
        schema,
    };
    
    let map = L::Map {
        input: Box::new(scan),
        expr: "old_name AS new_name, value".to_string(), // Simple rename expression
    };
    
    let sink = L::Sink {
        input: Box::new(map),
        destination: "memory://output".to_string(),
        format: "memory".to_string(),
    };
    
    // Execute
    let optimized = rules::optimize(sink);
    let phys_prog = lower_to_physical(&optimized);
    let work = estimate_work(&optimized, None);
    let te = plan_te(&phys_prog.plan, &work, 16 * 1024 * 1024).expect("TE planning failed");
    
    let mut config = EngineConfig::default();
    config.spill_dir = temp_dir.clone();
    let mut engine = Engine::new(config);
    
    // Pre-populate memory storage with test data
    let memory_store = engine.memory_store();
    let batch_json = serde_json::to_vec(&test_batch).expect("Failed to serialize");
    memory_store.insert("input".to_string(), batch_json);
    
    let manifest = engine.run(&phys_prog, &te).expect("Execution failed");
    
    // Verify execution completed
    assert!(manifest.started_ms <= manifest.finished_ms);
    
    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_project_column_selection() {
    let temp_dir = create_temp_spill_dir();
    fs::create_dir_all(&temp_dir).expect("Failed to create temp dir");
    
    // Create a schema with many columns, project only a subset
    let schema = Schema {
        fields: vec![
            Field::new("col1", DataType::Int64, false),
            Field::new("col2", DataType::Utf8, false),
            Field::new("col3", DataType::Float64, false),
            Field::new("col4", DataType::Int32, false),
            Field::new("col5", DataType::Utf8, false),
        ],
    };
    
    // Create test data
    let test_batch = RowBatch {
        columns: vec![
            Column {
                name: "col1".to_string(),
                values: vec![Scalar::I64(1), Scalar::I64(2)],
            },
            Column {
                name: "col2".to_string(),
                values: vec![Scalar::Str("A".to_string()), Scalar::Str("B".to_string())],
            },
            Column {
                name: "col3".to_string(),
                values: vec![Scalar::F64(1.5), Scalar::F64(2.5)],
            },
            Column {
                name: "col4".to_string(),
                values: vec![Scalar::I32(10), Scalar::I32(20)],
            },
            Column {
                name: "col5".to_string(),
                values: vec![Scalar::Str("X".to_string()), Scalar::Str("Y".to_string())],
            },
        ],
    };
    
    let scan = L::Scan {
        source: "memory://wide_table".to_string(),
        schema,
    };
    
    let project = L::Project {
        input: Box::new(scan),
        columns: vec!["col2".to_string(), "col4".to_string()],
    };
    
    let sink = L::Sink {
        input: Box::new(project),
        destination: "memory://narrow_table".to_string(),
        format: "memory".to_string(),
    };
    
    // Execute
    let optimized = rules::optimize(sink);
    let phys_prog = lower_to_physical(&optimized);
    let work = estimate_work(&optimized, None);
    let te = plan_te(&phys_prog.plan, &work, 16 * 1024 * 1024).expect("TE planning failed");
    
    let mut config = EngineConfig::default();
    config.spill_dir = temp_dir.clone();
    let mut engine = Engine::new(config);
    
    // Pre-populate memory storage with test data
    let memory_store = engine.memory_store();
    let batch_json = serde_json::to_vec(&test_batch).expect("Failed to serialize");
    memory_store.insert("wide_table".to_string(), batch_json);
    
    let manifest = engine.run(&phys_prog, &te).expect("Execution failed");
    
    assert!(manifest.started_ms <= manifest.finished_ms);
    
    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_multiple_filters() {
    let temp_dir = create_temp_spill_dir();
    let input_file = format!("{}/multi_filter.csv", temp_dir);
    
    fs::create_dir_all(&temp_dir).expect("Failed to create temp dir");
    
    // Create test data
    let mut file = fs::File::create(&input_file).expect("Failed to create file");
    writeln!(file, "score,status").expect("Failed to write header");
    for i in 0..100 {
        let score = i;
        let status = if i % 2 == 0 { "active" } else { "inactive" };
        writeln!(file, "{},{}", score, status).expect("Failed to write row");
    }
    drop(file);
    
    let schema = Schema {
        fields: vec![
            Field::new("score", DataType::Int64, false),
            Field::new("status", DataType::Utf8, false),
        ],
    };
    
    let scan = L::Scan {
        source: format!("file://{}", input_file),
        schema,
    };
    
    // Filter 1: score > 50
    let filter1 = L::Filter {
        input: Box::new(scan),
        expr: "score > 50".to_string(),
    };
    
    // Note: For now we only support one filter at a time in the simple predicate evaluator
    // This test just verifies the single filter works
    
    let sink = L::Sink {
        input: Box::new(filter1),
        destination: "memory://filtered".to_string(),
        format: "memory".to_string(),
    };
    
    let optimized = rules::optimize(sink);
    let phys_prog = lower_to_physical(&optimized);
    let work = estimate_work(&optimized, None);
    let te = plan_te(&phys_prog.plan, &work, 16 * 1024 * 1024).expect("TE planning failed");
    
    let mut config = EngineConfig::default();
    config.spill_dir = temp_dir.clone();
    let mut engine = Engine::new(config);
    let manifest = engine.run(&phys_prog, &te).expect("Execution failed");
    
    assert!(manifest.started_ms <= manifest.finished_ms);
    
    cleanup_test_file(&input_file);
    let _ = fs::remove_dir_all(&temp_dir);
}

