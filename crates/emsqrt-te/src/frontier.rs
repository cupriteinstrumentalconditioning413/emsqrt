//! Live-frontier tracking for TE execution.
//!
//! The "frontier" is the set of blocks that are materialized/live at the same time.
//! Bounded fan-in → bounded frontier → bounded peak memory (given per-block footprint).

use emsqrt_core::id::BlockId;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrontierStats {
    pub max_frontier_size: usize,
    pub max_depth: usize,
}

/// Minimal frontier tracker for a DAG order (topological).
/// In real TE, deps per block are bounded; here we track set sizes along an order.
pub struct FrontierTracker {
    in_degree: HashMap<BlockId, usize>,
    dependents: HashMap<BlockId, Vec<BlockId>>,
    ready: VecDeque<BlockId>,
    live: HashSet<BlockId>,
    max_frontier: usize,
    depth: usize,
}

impl FrontierTracker {
    pub fn new(edges: &[(BlockId, BlockId)]) -> Self {
        let mut in_degree: HashMap<BlockId, usize> = HashMap::new();
        let mut dependents: HashMap<BlockId, Vec<BlockId>> = HashMap::new();

        for (u, v) in edges {
            *in_degree.entry(*v).or_default() += 1;
            dependents.entry(*u).or_default().push(*v);
            in_degree.entry(*u).or_default();
        }

        let ready = in_degree
            .iter()
            .filter_map(|(b, &deg)| if deg == 0 { Some(*b) } else { None })
            .collect();

        Self {
            in_degree,
            dependents,
            ready,
            live: HashSet::new(),
            max_frontier: 0,
            depth: 0,
        }
    }

    /// Consume one ready block, advance dependents, and update frontier stats.
    pub fn step(&mut self) -> Option<BlockId> {
        let b = self.ready.pop_front()?;
        self.live.insert(b);
        self.max_frontier = self.max_frontier.max(self.live.len());
        self.depth += 1;

        if let Some(nexts) = self.dependents.get(&b) {
            for v in nexts {
                if let Some(deg) = self.in_degree.get_mut(v) {
                    *deg -= 1;
                    if *deg == 0 {
                        self.ready.push_back(*v);
                    }
                }
            }
        }

        // Once dependents are scheduled, this block can be released from frontier.
        self.live.remove(&b);
        Some(b)
    }

    pub fn stats(&self) -> FrontierStats {
        FrontierStats {
            max_frontier_size: self.max_frontier,
            max_depth: self.depth,
        }
    }
}

/// Compute the maximum frontier size for a given order (simple topological analysis).
/// Input: list of (block_id, deps) where deps are the dependencies for that block.
/// Returns the maximum number of blocks alive at any point.
pub fn compute_max_frontier(order: &[(BlockId, Vec<BlockId>)]) -> usize {
    if order.is_empty() {
        return 0;
    }

    // Build edges: (dep -> block) for each block
    let mut edges = Vec::new();
    for (block, deps) in order {
        for dep in deps {
            edges.push((*dep, *block));
        }
    }

    let mut tracker = FrontierTracker::new(&edges);

    // Step through all blocks in the order
    while tracker.step().is_some() {
        // Just iterate to compute stats
    }

    tracker.stats().max_frontier_size
}
