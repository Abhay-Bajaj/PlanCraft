use anyhow::{Context, Result};
use csv::StringRecord;
use std::fs::File;

use crate::exec::ExecNode;
use crate::value::{Row, parse_cell};

pub struct CsvScan {
    headers: Vec<String>,
    rdr: csv::Reader<File>,
}

impl CsvScan {
    pub fn new(path: String) -> Result<Self> {
        let file = File::open(&path).with_context(|| format!("Failed to open CSV: {path}"))?;
        let mut rdr = csv::Reader::from_reader(file);

        let headers = rdr
            .headers()
            .context("CSV missing headers row")?
            .iter()
            .map(|s| s.to_string())
            .collect();

        Ok(Self { headers, rdr })
    }

    fn record_to_row(&self, rec: &StringRecord) -> Row {
        let mut row = Row::new();
        for (i, key) in self.headers.iter().enumerate() {
            let cell = rec.get(i).unwrap_or("");
            row.insert(key.clone(), parse_cell(cell));
        }
        row
    }
}

impl ExecNode for CsvScan {
    fn next_row(&mut self) -> Result<Option<Row>> {
        let mut rec = StringRecord::new();
        let ok = self.rdr.read_record(&mut rec)?;
        if !ok {
            return Ok(None);
        }
        Ok(Some(self.record_to_row(&rec)))
    }
}
