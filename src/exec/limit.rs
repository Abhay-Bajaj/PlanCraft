use anyhow::Result;

use crate::exec::ExecNode;
use crate::value::Row;

pub struct LimitExec {
    input: Box<dyn ExecNode>,
    remaining: usize,
}

impl LimitExec {
    pub fn new(input: Box<dyn ExecNode>, n: usize) -> Self {
        Self {
            input,
            remaining: n,
        }
    }
}

impl ExecNode for LimitExec {
    fn next_row(&mut self) -> Result<Option<Row>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let row = self.input.next_row()?;
        if row.is_some() {
            self.remaining -= 1;
        }
        Ok(row)
    }
}
