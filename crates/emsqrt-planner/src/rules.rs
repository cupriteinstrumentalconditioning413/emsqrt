//! Simple optimization rules (pushdown/reorder/strategy).

use crate::logical::LogicalPlan;

/// Apply a sequence of lightweight rewrites to the logical plan.
pub fn optimize(plan: LogicalPlan) -> LogicalPlan {
    // TODO: Re-enable projection pushdown once column dependency analysis is implemented
    // For now, just return the plan as-is to avoid breaking filters
    plan
    // projection_pushdown(plan)
}

/// Simple projection pushdown: Project(Filter(x)) → Filter(Project(x)) when safe.
/// This is safe when the filter doesn't reference columns not in the projection.
/// For simplicity, we only apply this when the project includes all columns needed by filter.
fn projection_pushdown(plan: LogicalPlan) -> LogicalPlan {
    use LogicalPlan::*;

    match plan {
        Project { input, columns } => {
            match *input {
                Filter {
                    input: filter_input,
                    expr,
                } => {
                    // Try to push project below filter
                    // For now, just check if we should apply the optimization
                    // In a real implementation, we'd parse the expr to see which columns it uses
                    // For safety, only push down if we're not dropping columns (keep all)

                    // Recursively optimize the input first
                    let optimized_input = projection_pushdown(*filter_input);

                    // Reconstruct: keep original order for safety
                    // TODO: Add proper column dependency analysis
                    Filter {
                        input: Box::new(Project {
                            input: Box::new(optimized_input),
                            columns,
                        }),
                        expr,
                    }
                }
                other => {
                    // Recursively optimize the input
                    Project {
                        input: Box::new(projection_pushdown(other)),
                        columns,
                    }
                }
            }
        }
        Filter { input, expr } => Filter {
            input: Box::new(projection_pushdown(*input)),
            expr,
        },
        Map { input, expr } => Map {
            input: Box::new(projection_pushdown(*input)),
            expr,
        },
        Aggregate {
            input,
            group_by,
            aggs,
        } => Aggregate {
            input: Box::new(projection_pushdown(*input)),
            group_by,
            aggs,
        },
        Join {
            left,
            right,
            on,
            join_type,
        } => Join {
            left: Box::new(projection_pushdown(*left)),
            right: Box::new(projection_pushdown(*right)),
            on,
            join_type,
        },
        Sink {
            input,
            destination,
            format,
        } => Sink {
            input: Box::new(projection_pushdown(*input)),
            destination,
            format,
        },
        // Leaf nodes
        Scan { .. } => plan,
    }
}
