//! Logical and physical pipeline representations for planning/execution.
//!
//! The planner produces a `LogicalPlan` (what to do), then a `PhysicalPlan`
//! that binds concrete operator implementations and TE block boundaries.

use serde::{Deserialize, Serialize};

use crate::id::OpId;
use crate::schema::Schema;

/// Simple join types (expand as needed).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

/// Simplified aggregations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Aggregation {
    Count,
    Sum(String),
    Avg(String),
    Min(String),
    Max(String),
    // TODO: distinct, multi-agg per group, etc.
}

/// High-level logical nodes (source → transforms → sink).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogicalPlan {
    Scan {
        source: String, // e.g., "s3://bucket/path/*.parquet"
        schema: Schema, // declared or discovered
    },
    Filter {
        input: Box<LogicalPlan>,
        expr: String, // TODO: real expr AST
    },
    Map {
        input: Box<LogicalPlan>,
        expr: String, // TODO: real projection list
    },
    Project {
        input: Box<LogicalPlan>,
        columns: Vec<String>,
    },
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        on: Vec<(String, String)>,
        join_type: JoinType,
    },
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<String>,
        aggs: Vec<Aggregation>,
    },
    Window {
        input: Box<LogicalPlan>,
        partitions: Vec<String>,
        order_by: Vec<String>,
        functions: Vec<WindowExpr>,
    },
    Lateral {
        input: Box<LogicalPlan>,
        column: String,
        alias: String,
        delimiter: Option<String>,
    },
    Sink {
        input: Box<LogicalPlan>,
        destination: String, // e.g., "s3://bucket/out/"
        format: String,      // "parquet", "csv", ...
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowExpr {
    pub function: WindowFunction,
    pub alias: String,
    #[serde(default)]
    pub frame: WindowFrame,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WindowFunction {
    RowNumber,
    Sum { column: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowFrame {
    pub start: WindowFrameBound,
    pub end: WindowFrameBound,
}

impl Default for WindowFrame {
    fn default() -> Self {
        Self {
            start: WindowFrameBound::UnboundedPreceding,
            end: WindowFrameBound::CurrentRow,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WindowFrameBound {
    UnboundedPreceding,
    CurrentRow,
}

/// Physical nodes bind to operator IDs (resolved in `emsqrt-operators`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PhysicalPlan {
    Source {
        op: OpId,
        schema: Schema,
    },
    Unary {
        op: OpId,
        input: Box<PhysicalPlan>,
        schema: Schema,
    },
    Binary {
        op: OpId,
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        schema: Schema,
    },
    Sink {
        op: OpId,
        input: Box<PhysicalPlan>,
    },
}

impl LogicalPlan {
    /// Returns the number of inputs for this node.
    pub fn inputs(&self) -> usize {
        use LogicalPlan::*;
        match self {
            Scan { .. } => 0,
            Filter { .. }
            | Map { .. }
            | Project { .. }
            | Aggregate { .. }
            | Window { .. }
            | Lateral { .. }
            | Sink { .. } => 1,
            Join { .. } => 2,
        }
    }

    /// Returns true if this is a unary operator.
    pub fn is_unary(&self) -> bool {
        self.inputs() == 1
    }

    /// Returns true if this is a binary operator.
    pub fn is_binary(&self) -> bool {
        self.inputs() == 2
    }
}

impl PhysicalPlan {
    /// Returns the number of inputs for this node.
    pub fn inputs(&self) -> usize {
        use PhysicalPlan::*;
        match self {
            Source { .. } => 0,
            Unary { .. } | Sink { .. } => 1,
            Binary { .. } => 2,
        }
    }

    /// Returns true if this is a unary operator.
    pub fn is_unary(&self) -> bool {
        self.inputs() == 1
    }

    /// Returns true if this is a binary operator.
    pub fn is_binary(&self) -> bool {
        self.inputs() == 2
    }
}
