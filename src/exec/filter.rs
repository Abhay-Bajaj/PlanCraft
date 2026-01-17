use anyhow::Result;

use crate::ast::Predicate;
use crate::exec::{ExecNode, predicate_list_match};
use crate::value::Row;

pub struct FilterExec {
    input: Box<dyn ExecNode>,
    preds: Vec<Predicate>,
}

impl FilterExec {
    pub fn new(input: Box<dyn ExecNode>, preds: Vec<Predicate>) -> Self {
        Self { input, preds }
    }
}

impl ExecNode for FilterExec {
    fn next_row(&mut self) -> Result<Option<Row>> {
        loop {
            let row = match self.input.next_row()? {
                Some(r) => r,
                None => return Ok(None),
            };

            if predicate_list_match(&row, &self.preds)? {
                return Ok(Some(row));
            }
        }
    }
}
