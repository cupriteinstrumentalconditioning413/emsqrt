//! Minimal YAML → LogicalPlan parser for *linear* pipelines.
//!
//! Example:
//! ```yaml
//! steps:
//!   - scan: { source: "data/logs.csv", schema:
//!       [ {name: "ts",  type: "Utf8",  nullable: false},
//!         {name: "uid", type: "Utf8",  nullable: false},
//!         {name: "lat", type: "Float64", nullable: true} ] }
//!   - filter: { expr: "uid != ''" }
//!   - project: { columns: ["ts","uid"] }
//!   - sink: { destination: "out/filtered.csv", format: "csv" }
//! ```

use serde::{Deserialize, Serialize};
use serde_yaml;

use emsqrt_core::dag::{LogicalPlan, WindowExpr, WindowFrame, WindowFunction};
use emsqrt_core::schema::{DataType, Field, Schema};

use crate::logical::LogicalPlan as L;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pipeline {
    #[serde(default)]
    pub config: Option<PipelineConfig>,
    pub steps: Vec<Step>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase", tag = "op")]
pub enum Step {
    #[serde(rename = "scan")]
    Scan {
        source: String,
        schema: Vec<FieldDef>,
    },

    #[serde(rename = "filter")]
    Filter { expr: String },

    #[serde(rename = "project")]
    Project { columns: Vec<String> },

    #[serde(rename = "map")]
    Map { expr: String },

    #[serde(rename = "sink")]
    Sink { destination: String, format: String },

    #[serde(rename = "window")]
    Window {
        partitions: Vec<String>,
        order_by: Vec<String>,
        functions: Vec<WindowFunctionDef>,
    },

    #[serde(rename = "lateral")]
    Lateral {
        column: String,
        alias: String,
        #[serde(default)]
        delimiter: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowFunctionDef {
    pub alias: String,
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub column: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldDef {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
    #[serde(default)]
    pub nullable: bool,
}

fn parse_dtype(s: &str) -> DataType {
    match s {
        "Boolean" | "bool" => DataType::Boolean,
        "Int32" | "i32" => DataType::Int32,
        "Int64" | "i64" => DataType::Int64,
        "Float32" | "f32" => DataType::Float32,
        "Float64" | "f64" => DataType::Float64,
        "Binary" | "bytes" => DataType::Binary,
        _ => DataType::Utf8,
    }
}

fn to_schema(fields: &[FieldDef]) -> Schema {
    Schema::new(
        fields
            .iter()
            .map(|f| Field {
                name: f.name.clone(),
                data_type: parse_dtype(&f.data_type),
                nullable: f.nullable,
            })
            .collect(),
    )
}

/// Parse YAML string into a `LogicalPlan`.
/// This supports *linear* pipelines only; joins/branches not yet supported.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct PipelineConfig {
    pub spill_uri: Option<String>,
    pub spill_dir: Option<String>,
    pub spill_aws_region: Option<String>,
    pub spill_aws_access_key_id: Option<String>,
    pub spill_aws_secret_access_key: Option<String>,
    pub spill_aws_session_token: Option<String>,
    pub spill_gcs_service_account: Option<String>,
    pub spill_azure_access_key: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ParsedPipeline {
    pub plan: LogicalPlan,
    pub config: PipelineConfig,
}

pub fn parse_yaml_pipeline(yaml_src: &str) -> Result<ParsedPipeline, serde_yaml::Error> {
    let doc: Pipeline = serde_yaml::from_str(yaml_src)?;
    let mut cur: Option<LogicalPlan> = None;

    for step in doc.steps {
        cur = Some(match (step, cur) {
            (Step::Scan { source, schema }, None) => L::Scan {
                source,
                schema: to_schema(&schema),
            },
            (Step::Scan { .. }, Some(_)) => {
                // serde_yaml::Error doesn't have a custom method, so we'll just parse error
                return Err(
                    serde_yaml::from_str::<()>("invalid: multiple scans not supported")
                        .unwrap_err(),
                );
            }
            (Step::Filter { expr }, Some(input)) => L::Filter {
                input: Box::new(input),
                expr,
            },
            (Step::Project { columns }, Some(input)) => L::Project {
                input: Box::new(input),
                columns,
            },
            (Step::Map { expr }, Some(input)) => L::Map {
                input: Box::new(input),
                expr,
            },
            (
                Step::Sink {
                    destination,
                    format,
                },
                Some(input),
            ) => L::Sink {
                input: Box::new(input),
                destination,
                format,
            },
            (
                Step::Window {
                    partitions,
                    order_by,
                    functions,
                },
                Some(input),
            ) => L::Window {
                input: Box::new(input),
                partitions,
                order_by,
                functions: functions
                    .into_iter()
                    .map(|def| WindowExpr {
                        alias: def.alias,
                        function: match def.kind.as_str() {
                            "row_number" => WindowFunction::RowNumber,
                            "sum" => WindowFunction::Sum {
                                column: def.column.unwrap_or_else(|| "value".into()),
                            },
                            _ => WindowFunction::RowNumber,
                        },
                        frame: WindowFrame::default(),
                    })
                    .collect(),
            },
            (
                Step::Lateral {
                    column,
                    alias,
                    delimiter,
                },
                Some(input),
            ) => L::Lateral {
                input: Box::new(input),
                column,
                alias,
                delimiter,
            },
            (s, None) => {
                // Any non-scan step without a prior plan is invalid in linear pipelines.
                // Return a parse error since serde_yaml::Error doesn't have a constructor
                return Err(serde_yaml::from_str::<()>(&format!(
                    "invalid: first step must be 'scan', got {:?}",
                    s
                ))
                .unwrap_err());
            }
        });
    }

    let plan =
        cur.ok_or_else(|| serde_yaml::from_str::<()>("invalid: empty pipeline").unwrap_err())?;
    Ok(ParsedPipeline {
        plan,
        config: doc.config.unwrap_or_default(),
    })
}
