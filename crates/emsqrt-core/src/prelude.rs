//! Convenient re-exports for downstream crates.

pub use crate::block::{Block, BlockDeps, BlockRange};
pub use crate::config::EngineConfig;
pub use crate::dag::{Aggregation, JoinType, LogicalPlan, PhysicalPlan};
pub use crate::error::{Error, Result};
pub use crate::id::{BlockId, OpId, SpillId};
pub use crate::manifest::{ManifestId, RunManifest};
pub use crate::schema::{DataType, Field, Schema};
