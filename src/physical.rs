use anyhow::Result;

use crate::exec::{CsvScan, ExecNode, FilterExec, HashAggregateExec, LimitExec, ProjectExec};
use crate::logical::LogicalPlan;

pub fn to_physical_plan(plan: LogicalPlan) -> Result<Box<dyn ExecNode>> {
    Ok(match plan {
        LogicalPlan::Scan { path } => Box::new(CsvScan::new(path)?),

        LogicalPlan::Filter { input, preds } => {
            let child = to_physical_plan(*input)?;
            Box::new(FilterExec::new(child, preds))
        }

        LogicalPlan::Aggregate {
            input,
            group_keys,
            aggs,
        } => {
            let child = to_physical_plan(*input)?;
            Box::new(HashAggregateExec::new(child, group_keys, aggs))
        }

        LogicalPlan::Project { input, cols } => {
            let child = to_physical_plan(*input)?;
            Box::new(ProjectExec::new(child, cols))
        }

        LogicalPlan::Limit { input, n } => {
            let child = to_physical_plan(*input)?;
            Box::new(LimitExec::new(child, n))
        }
    })
}
