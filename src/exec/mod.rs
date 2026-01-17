mod aggregate;
mod csv_scan;
mod filter;
mod limit;
mod project;

use anyhow::Result;

use crate::ast::Predicate;
use crate::value::Row;

pub use aggregate::{AggFunc, AggSpec, HashAggregateExec};
pub use csv_scan::CsvScan;
pub use filter::FilterExec;
pub use limit::LimitExec;
pub use project::ProjectExec;

pub trait ExecNode {
    fn next_row(&mut self) -> Result<Option<Row>>;
}

pub fn predicate_list_match(row: &Row, preds: &[Predicate]) -> Result<bool> {
    for p in preds {
        let v = row.get(&p.col).unwrap_or(&serde_json::Value::Null);
        if !crate::value::cmp_json(v, &p.op, &p.val)? {
            return Ok(false);
        }
    }
    Ok(true)
}
