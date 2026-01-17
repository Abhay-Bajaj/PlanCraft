# PlanCraft

A lightweight analytical query engine written in Rust. The engine parses a JSON-based query DSL, builds a logical query plan, applies safe optimizer rewrites, compiles the plan into a physical execution pipeline, and executes queries over CSV data.

This project focuses on **query planning and execution fundamentals** rather than UI or storage complexity.

---

## Features

- **JSON Query DSL**
  - `from`, `select`, `where`, `group_by`, `limit`
- **Logical Query Planning**
  - Structured logical plan representation
- **Optimizer Passes**
  - Safe rule-based rewrites
  - Plan introspection via `EXPLAIN`
- **Physical Execution Engine**
  - Streaming CSV scan
  - Predicate filtering
  - Column projection
  - **Hash-based aggregation**
    - `GROUP BY`
    - `SUM`
    - `COUNT(*)`
- **Deterministic Output**
  - Grouped results are sorted by the first `group_by` key
- **Explainability**
  - `--explain` prints the optimized logical plan
  - `--explain-both` prints original vs optimized plans
- **Output Formats**
  - Human-readable table (default)
  - JSON (`--format json`)

---

## Quickstart

Clone and Run queries directly from JSON files:

```bash
git clone https://github.com/<your-username>/<your-repo-name>.git
cd <your-repo-name>
```
```bash
cargo run -- queries/q1_sum_by_user.json
cargo run -- queries/q2_group_sum.json
cargo run -- queries/q3_sum_and_count.json
cargo run -- queries/q4_filtered_grouped.json
```

## Explain the Plan

Print the optimized logical plan:
```bash
cargo run -- --explain queries/q3_sum_and_count.json
```
Compare original vs optimized:
```bash
cargo run -- --explain-both queries/q3_sum_and_count.json
```
Output as JSON:
```bash
cargo run -- --format json queries/q3_sum_and_count.json
```


## Using Your Own Data

The engine can run queries against any CSV file.

1. Place your CSV file anywhere in the project (for example, `data/my_data.csv`).
2. Ensure the first row contains column headers.
3. Write a query JSON file that references those column names.
4. Run the engine with your query file.

File Example:

```bash
user_id,amount,category,city
u1,10,food,SF
u2,80,shopping,NY
u1,120,food,SF
u3,55,gas,SJ
u2,15,food,NY
u4,200,rent,SF
```

Build and Run:

```bash
cargo run -- queries/my_query.json
```
