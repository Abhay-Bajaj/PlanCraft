use anyhow::{Context, Result};
use clap::Parser;
use std::cmp::Ordering;
use std::fs;

mod ast;
mod exec;
mod explain;
mod logical;
mod optimizer;
mod parser;
mod physical;
mod value;

use crate::logical::build_logical_plan;
use crate::optimizer::optimize;
use crate::parser::parse_query;
use crate::physical::to_physical_plan;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Path to query JSON file
    query_path: String,

    /// Print optimized logical plan instead of running
    #[arg(long)]
    explain: bool,

    /// Print both original and optimized logical plans
    #[arg(long)]
    explain_both: bool,

    /// Output format: table|json
    #[arg(long, default_value = "table")]
    format: String,
}

fn json_cell_to_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        other => other.to_string(),
    }
}

fn is_number(v: &serde_json::Value) -> bool {
    matches!(v, serde_json::Value::Number(_))
}

fn cmp_json_for_sort(a: Option<&serde_json::Value>, b: Option<&serde_json::Value>) -> Ordering {
    match (a, b) {
        (Some(serde_json::Value::String(sa)), Some(serde_json::Value::String(sb))) => sa.cmp(sb),

        (Some(serde_json::Value::Number(na)), Some(serde_json::Value::Number(nb))) => {
            let fa = na.as_f64().unwrap_or(0.0);
            let fb = nb.as_f64().unwrap_or(0.0);
            fa.partial_cmp(&fb).unwrap_or(Ordering::Equal)
        }

        (Some(serde_json::Value::Null), Some(serde_json::Value::Null)) => Ordering::Equal,
        (None, None) => Ordering::Equal,

        (None, _) | (Some(serde_json::Value::Null), _) => Ordering::Greater,
        (_, None) | (_, Some(serde_json::Value::Null)) => Ordering::Less,

        (Some(va), Some(vb)) => va.to_string().cmp(&vb.to_string()),
    }
}

fn main() -> Result<()> {
    let args = Args::parse();

    let raw = fs::read_to_string(&args.query_path)
        .with_context(|| format!("Failed to read query file: {}", args.query_path))?;

    let query = parse_query(&raw).context("Failed to parse query JSON")?;

    let logical = build_logical_plan(&query);
    let optimized = optimize(logical.clone());

    if args.explain_both {
        println!("--- ORIGINAL PLAN ---");
        println!("{}", explain::format_plan(&logical));
        println!("--- OPTIMIZED PLAN ---");
        println!("{}", explain::format_plan(&optimized));
        return Ok(());
    }

    if args.explain {
        println!("{}", explain::format_plan(&optimized));
        return Ok(());
    }

    let mut root = to_physical_plan(optimized)?;

    let mut rows = Vec::new();
    while let Some(r) = root.next_row()? {
        rows.push(r);
    }

    if let Some(sort_key) = query.group_by.first() {
        rows.sort_by(|ra, rb| cmp_json_for_sort(ra.get(sort_key), rb.get(sort_key)));
    }

    match args.format.as_str() {
        "json" => {
            let json = serde_json::to_string_pretty(&rows)?;
            println!("{json}");
        }
        "table" => {
            let headers = query
                .select
                .iter()
                .map(|s| s.trim().to_string())
                .collect::<Vec<_>>();

            let max_rows = 50usize;
            let display_rows = rows.iter().take(max_rows).collect::<Vec<_>>();

            let mut widths = vec![0usize; headers.len()];
            let mut numeric_col = vec![false; headers.len()];

            for (i, h) in headers.iter().enumerate() {
                widths[i] = widths[i].max(h.len());
            }

            let mut rendered: Vec<Vec<String>> = Vec::new();

            for r in &display_rows {
                let mut line: Vec<String> = Vec::with_capacity(headers.len());

                for (i, h) in headers.iter().enumerate() {
                    let v = r.get(h).cloned().unwrap_or(serde_json::Value::Null);
                    if is_number(&v) {
                        numeric_col[i] = true;
                    }
                    let s = json_cell_to_string(&v);
                    widths[i] = widths[i].max(s.len());
                    line.push(s);
                }

                rendered.push(line);
            }

            for (i, h) in headers.iter().enumerate() {
                print!("{:<width$}", h, width = widths[i]);
                if i + 1 < headers.len() {
                    print!("  ");
                }
            }
            println!();

            for (i, _) in headers.iter().enumerate() {
                print!("{:-<width$}", "", width = widths[i]);
                if i + 1 < headers.len() {
                    print!("  ");
                }
            }
            println!();

            for line in rendered {
                for (i, cell) in line.iter().enumerate() {
                    if numeric_col[i] {
                        print!("{:>width$}", cell, width = widths[i]);
                    } else {
                        print!("{:<width$}", cell, width = widths[i]);
                    }

                    if i + 1 < headers.len() {
                        print!("  ");
                    }
                }
                println!();
            }
        }
        other => {
            eprintln!("Unknown format '{other}'. Use --format table or --format json.");
            for r in rows.iter().take(50) {
                println!("{r:?}");
            }
        }
    }

    Ok(())
}
