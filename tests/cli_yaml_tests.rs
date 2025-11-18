//! CLI YAML parsing and validation tests

use emsqrt_planner::parse_yaml_pipeline;

#[test]
fn test_parse_simple_pipeline() {
    let yaml = r#"
steps:
  - op: scan
    source: "data/input.csv"
    schema:
      - name: "id"
        type: "Int64"
        nullable: false
      - name: "name"
        type: "Utf8"
        nullable: false
  - op: filter
    expr: "id > 10"
  - op: sink
    destination: "output/result.csv"
    format: "csv"
"#;

    let result = parse_yaml_pipeline(yaml);
    assert!(result.is_ok());
}

#[test]
fn test_parse_pipeline_with_aggregate() {
    // Note: Aggregate is not yet supported in YAML DSL parser
    // This test documents current limitation
    let yaml = r#"
steps:
  - op: scan
    source: "data/sales.csv"
    schema:
      - name: "product"
        type: "Utf8"
        nullable: false
      - name: "quantity"
        type: "Int64"
        nullable: false
  - op: aggregate
    group_by:
      - "product"
    aggs:
      - "SUM(quantity)"
      - "COUNT(*)"
  - op: sink
    destination: "output/summary.csv"
    format: "csv"
"#;

    let result = parse_yaml_pipeline(yaml);
    // Currently fails because aggregate is not in Step enum
    // This is expected until aggregate support is added to YAML parser
    assert!(result.is_err());
}

#[test]
fn test_parse_invalid_yaml() {
    let yaml = "invalid: yaml: [";
    let result = parse_yaml_pipeline(yaml);
    assert!(result.is_err());
}

#[test]
fn test_parse_missing_op_field() {
    let yaml = r#"
steps:
  - source: "data/input.csv"
    schema: []
"#;

    let result = parse_yaml_pipeline(yaml);
    assert!(result.is_err());
}

#[test]
fn test_parse_empty_pipeline() {
    let yaml = "steps: []";
    let result = parse_yaml_pipeline(yaml);
    assert!(result.is_err()); // Empty pipeline should be invalid
}

#[test]
fn test_parse_window_pipeline() {
    let yaml = r#"
steps:
  - op: scan
    source: "data/window.csv"
    schema:
      - name: "group"
        type: "Utf8"
        nullable: false
      - name: "ts"
        type: "Int64"
        nullable: false
      - name: "value"
        type: "Float64"
        nullable: false
  - op: window
    partitions: ["group"]
    order_by: ["ts"]
    functions:
      - alias: "rank"
        type: "row_number"
"#;
    let result = parse_yaml_pipeline(yaml);
    assert!(result.is_ok());
}

#[test]
fn test_parse_lateral_pipeline() {
    let yaml = r#"
steps:
  - op: scan
    source: "data/lateral.csv"
    schema:
      - name: "id"
        type: "Int32"
        nullable: false
      - name: "tags"
        type: "Utf8"
        nullable: true
  - op: lateral
    column: "tags"
    alias: "tag"
    delimiter: ";"
"#;
    let result = parse_yaml_pipeline(yaml);
    assert!(result.is_ok());
}

#[test]
fn test_pipeline_config_parsing() {
    let yaml = r#"
config:
  spill_uri: "s3://bucket/spill"
  spill_dir: "/tmp/alt-spill"
  spill_aws_region: "us-west-2"
steps:
  - op: scan
    source: "data/input.csv"
    schema: []
  - op: sink
    destination: "stdout"
    format: "csv"
"#;

    let parsed = parse_yaml_pipeline(yaml).expect("parsed pipeline");
    assert_eq!(
        parsed.config.spill_uri.as_deref(),
        Some("s3://bucket/spill")
    );
    assert_eq!(parsed.config.spill_dir.as_deref(), Some("/tmp/alt-spill"));
    assert_eq!(parsed.config.spill_aws_region.as_deref(), Some("us-west-2"));
}
