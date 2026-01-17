use crate::logical::LogicalPlan;

pub fn optimize(plan: LogicalPlan) -> LogicalPlan {
    let plan = pushdown_filter(plan);
    let plan = pushdown_project(plan);
    plan
}

fn pushdown_filter(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter { input, preds } => {
            let input = pushdown_filter(*input);

            match input {
                LogicalPlan::Project { input: inner, cols } => LogicalPlan::Project {
                    input: Box::new(LogicalPlan::Filter {
                        input: inner,
                        preds,
                    }),
                    cols,
                },
                LogicalPlan::Aggregate { .. } => {
                    // Do not move filters across Aggregate in this simple version
                    LogicalPlan::Filter {
                        input: Box::new(input),
                        preds,
                    }
                }
                other => LogicalPlan::Filter {
                    input: Box::new(other),
                    preds,
                },
            }
        }
        LogicalPlan::Project { input, cols } => LogicalPlan::Project {
            input: Box::new(pushdown_filter(*input)),
            cols,
        },
        LogicalPlan::Aggregate {
            input,
            group_keys,
            aggs,
        } => LogicalPlan::Aggregate {
            input: Box::new(pushdown_filter(*input)),
            group_keys,
            aggs,
        },
        LogicalPlan::Limit { input, n } => LogicalPlan::Limit {
            input: Box::new(pushdown_filter(*input)),
            n,
        },
        scan @ LogicalPlan::Scan { .. } => scan,
    }
}

fn pushdown_project(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Project { input, cols } => {
            let input = pushdown_project(*input);

            match input {
                LogicalPlan::Project {
                    input: inner,
                    cols: inner_cols,
                } => {
                    let mut keep = Vec::new();
                    for c in cols {
                        if inner_cols.contains(&c) {
                            keep.push(c);
                        }
                    }
                    LogicalPlan::Project {
                        input: inner,
                        cols: keep,
                    }
                }
                other => LogicalPlan::Project {
                    input: Box::new(other),
                    cols,
                },
            }
        }
        LogicalPlan::Filter { input, preds } => LogicalPlan::Filter {
            input: Box::new(pushdown_project(*input)),
            preds,
        },
        LogicalPlan::Aggregate {
            input,
            group_keys,
            aggs,
        } => LogicalPlan::Aggregate {
            input: Box::new(pushdown_project(*input)),
            group_keys,
            aggs,
        },
        LogicalPlan::Limit { input, n } => LogicalPlan::Limit {
            input: Box::new(pushdown_project(*input)),
            n,
        },
        scan @ LogicalPlan::Scan { .. } => scan,
    }
}
