use anyhow::Result;

use crate::ast::Query;

pub fn parse_query(raw: &str) -> Result<Query> {
    let q: Query = serde_json::from_str(raw)?;
    Ok(q)
}
