use crate::logical::LogicalPlan;

pub fn format_plan(plan: &LogicalPlan) -> String {
    let mut out = String::new();
    fmt(plan, 0, &mut out);
    out
}

fn fmt(plan: &LogicalPlan, indent: usize, out: &mut String) {
    let pad = "  ".repeat(indent);

    match plan {
        LogicalPlan::Scan { path } => {
            out.push_str(&format!("{pad}Scan(path=\"{path}\")\n"));
        }
        LogicalPlan::Filter { input, preds } => {
            out.push_str(&format!("{pad}Filter(preds={})\n", preds.len()));
            fmt(input, indent + 1, out);
        }
        LogicalPlan::Aggregate {
            input,
            group_keys,
            aggs,
        } => {
            out.push_str(&format!(
                "{pad}Aggregate(group_keys={:?}, aggs={})\n",
                group_keys,
                aggs.len()
            ));
            fmt(input, indent + 1, out);
        }
        LogicalPlan::Project { input, cols } => {
            out.push_str(&format!("{pad}Project(cols={:?})\n", cols));
            fmt(input, indent + 1, out);
        }
        LogicalPlan::Limit { input, n } => {
            out.push_str(&format!("{pad}Limit(n={n})\n"));
            fmt(input, indent + 1, out);
        }
    }
}
