use crate::ast::{Predicate, Query};
use crate::exec::{AggFunc, AggSpec};

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Scan {
        path: String,
    },
    Filter {
        input: Box<LogicalPlan>,
        preds: Vec<Predicate>,
    },
    Aggregate {
        input: Box<LogicalPlan>,
        group_keys: Vec<String>,
        aggs: Vec<AggSpec>,
    },
    Project {
        input: Box<LogicalPlan>,
        cols: Vec<String>,
    },
    Limit {
        input: Box<LogicalPlan>,
        n: usize,
    },
}

fn parse_select(select: &[String]) -> (Vec<String>, Vec<AggSpec>) {
    let mut cols_out: Vec<String> = Vec::new();
    let mut aggs: Vec<AggSpec> = Vec::new();

    for s in select {
        let t = s.trim();

        // sum(col)
        if let Some(inner) = t.strip_prefix("sum(").and_then(|x| x.strip_suffix(')')) {
            let col = inner.trim().to_string();
            let alias = format!("sum({})", col);
            cols_out.push(alias.clone());
            aggs.push(AggSpec {
                func: AggFunc::Sum,
                col,
                alias,
            });
            continue;
        }

        // count(*)
        if t.eq_ignore_ascii_case("count(*)") {
            let alias = "count(*)".to_string();
            cols_out.push(alias.clone());
            aggs.push(AggSpec {
                func: AggFunc::Count,
                col: "*".to_string(),
                alias,
            });
            continue;
        }

        // plain column
        cols_out.push(t.to_string());
    }

    (cols_out, aggs)
}

pub fn build_logical_plan(q: &Query) -> LogicalPlan {
    let (select_cols_out, aggs) = parse_select(&q.select);

    let mut plan = LogicalPlan::Scan {
        path: q.from.clone(),
    };

    if !q.r#where.is_empty() {
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            preds: q.r#where.clone(),
        };
    }

    let needs_agg = !q.group_by.is_empty() || !aggs.is_empty();
    if needs_agg {
        plan = LogicalPlan::Aggregate {
            input: Box::new(plan),
            group_keys: q.group_by.clone(),
            aggs,
        };
    }

    plan = LogicalPlan::Project {
        input: Box::new(plan),
        cols: select_cols_out,
    };

    if let Some(n) = q.limit {
        plan = LogicalPlan::Limit {
            input: Box::new(plan),
            n,
        };
    }

    plan
}
