use anyhow::Result;

use crate::exec::ExecNode;
use crate::value::Row;

pub struct ProjectExec {
    input: Box<dyn ExecNode>,
    cols: Vec<String>,
}

impl ProjectExec {
    pub fn new(input: Box<dyn ExecNode>, cols: Vec<String>) -> Self {
        Self { input, cols }
    }
}

impl ExecNode for ProjectExec {
    fn next_row(&mut self) -> Result<Option<Row>> {
        let row = match self.input.next_row()? {
            Some(r) => r,
            None => return Ok(None),
        };

        let mut out = Row::new();
        for c in &self.cols {
            if let Some(v) = row.get(c) {
                out.insert(c.clone(), v.clone());
            } else {
                out.insert(c.clone(), serde_json::Value::Null);
            }
        }
        Ok(Some(out))
    }
}
