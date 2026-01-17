use anyhow::{Result, anyhow};
use serde_json::Value as JsonValue;
use std::collections::HashMap;

use crate::exec::ExecNode;
use crate::value::Row;

#[derive(Debug, Clone)]
pub enum AggFunc {
    Sum,
    Count,
}

#[derive(Debug, Clone)]
pub struct AggSpec {
    pub func: AggFunc,
    pub col: String,   // column to aggregate
    pub alias: String, // output column name
}

pub struct HashAggregateExec {
    input: Box<dyn ExecNode>,
    group_keys: Vec<String>,
    aggs: Vec<AggSpec>,

    built: bool,
    out_rows: Vec<Row>,
    idx: usize,
}

impl HashAggregateExec {
    pub fn new(input: Box<dyn ExecNode>, group_keys: Vec<String>, aggs: Vec<AggSpec>) -> Self {
        Self {
            input,
            group_keys,
            aggs,
            built: false,
            out_rows: Vec::new(),
            idx: 0,
        }
    }

    fn build(&mut self) -> Result<()> {
        // key: serialized group values
        // state: sums/counts per group
        #[derive(Default)]
        struct State {
            sums: Vec<f64>,
            counts: Vec<u64>,
            key_vals: Vec<JsonValue>,
        }

        let mut map: HashMap<String, State> = HashMap::new();

        while let Some(row) = self.input.next_row()? {
            let mut key_vals: Vec<JsonValue> = Vec::with_capacity(self.group_keys.len());
            for k in &self.group_keys {
                key_vals.push(row.get(k).cloned().unwrap_or(JsonValue::Null));
            }

            let key_str = serde_json::to_string(&key_vals)
                .map_err(|e| anyhow!("Failed to serialize group key: {e}"))?;

            let entry = map.entry(key_str).or_insert_with(|| State {
                sums: vec![0.0; self.aggs.len()],
                counts: vec![0; self.aggs.len()],
                key_vals: key_vals.clone(),
            });

            // Update aggregates
            for (i, agg) in self.aggs.iter().enumerate() {
                match agg.func {
                    AggFunc::Count => {
                        entry.counts[i] += 1;
                    }
                    AggFunc::Sum => {
                        let v = row.get(&agg.col).cloned().unwrap_or(JsonValue::Null);
                        let n = v.as_f64().unwrap_or(0.0);
                        entry.sums[i] += n;
                    }
                }
            }
        }

        // Convert states to rows
        let mut out = Vec::with_capacity(map.len());
        for (_k, st) in map {
            let mut r: Row = Row::new();

            // group-by columns first
            for (i, col) in self.group_keys.iter().enumerate() {
                r.insert(
                    col.clone(),
                    st.key_vals.get(i).cloned().unwrap_or(JsonValue::Null),
                );
            }

            // then aggregate outputs
            for (i, agg) in self.aggs.iter().enumerate() {
                let val = match agg.func {
                    AggFunc::Count => JsonValue::from(st.counts[i] as i64),
                    AggFunc::Sum => JsonValue::from(st.sums[i]),
                };
                r.insert(agg.alias.clone(), val);
            }

            out.push(r);
        }

        self.out_rows = out;
        self.built = true;
        Ok(())
    }
}

impl ExecNode for HashAggregateExec {
    fn next_row(&mut self) -> Result<Option<Row>> {
        if !self.built {
            self.build()?;
        }

        if self.idx >= self.out_rows.len() {
            return Ok(None);
        }

        let r = self.out_rows[self.idx].clone();
        self.idx += 1;
        Ok(Some(r))
    }
}
