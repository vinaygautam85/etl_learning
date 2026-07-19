# ETL Pipeline — Multi-Source Extract, Transform, Load

A production-shaped ETL pipeline in Python that extracts data from two different source types (a flat CSV file and a live REST API), transforms it with justified data-cleaning logic, and loads it into a SQLite database using an atomic, idempotent load pattern.

Built as a hands-on exercise in writing pipeline code the way it would be written for production — with logging, retry logic, data validation, and failure-safe loads — rather than as throwaway exploratory scripts.

---

## Architecture

```
                  ┌─────────────┐     ┌──────────────┐     ┌─────────────────┐     ┌──────────────┐
  CSV file  ──►   │   EXTRACT   │ ──► │  raw parquet │ ──► │    TRANSFORM    │ ──► │  transformed │ ──► atomic load ──► SQLite
  REST API  ──►   │ (+ retries) │     │  (staging)   │     │ (clean/reshape) │     │   parquet    │     (staging→swap)
                  └─────────────┘     └──────────────┘     └─────────────────┘     └──────────────┘
```

Each stage persists its output to disk (raw and transformed layers kept physically separate), so the pipeline stages are decoupled and the raw source data is never overwritten.

---

## Tech Stack

- **Python** — pipeline logic
- **pandas** — data manipulation and cleaning
- **requests** — API extraction (with retry/backoff)
- **SQLAlchemy** — database interface
- **PyArrow** — Parquet read/write for the raw and transformed layers
- **SQLite** — target database

---

## Project Structure

```
etl_learning/
├── README.md
├── requirements.txt
├── .gitignore
├── src/
│   ├── etl_pipeline.py     # main pipeline: extract → transform → atomic load
│   └── upsert_demo.py      # standalone demo of the incremental upsert pattern
├── data/                   # (gitignored) raw + transformed parquet, SQLite DB
│   ├── raw/
│   └── transformed/
└── logs/                   # (gitignored) pipeline run logs
```

---

## How to Run

1. **Set up the environment**

   ```bash
   python3 -m venv etl_env
   source etl_env/bin/activate
   pip install -r requirements.txt
   ```

2. **Create the required directories** (data and logs are gitignored, so they won't exist on a fresh clone)

   ```bash
   mkdir -p data/raw data/transformed logs
   ```

3. **Run the main pipeline**

   ```bash
   python src/etl_pipeline.py
   ```

   This extracts both sources, transforms them, saves raw + transformed Parquet layers, and atomically loads both tables into the SQLite database.

4. **Run the incremental upsert demonstration** (after the main pipeline has populated the `users` table)

   ```bash
   python src/upsert_demo.py
   ```

---

## Key Concepts Demonstrated

This project is deliberately built around production-grade patterns, not just a working script:

### Atomic, failure-safe loads
Data is loaded into a **staging table** first, then an audit check verifies the row count matches expectations. Only on success is the staging table promoted to production via an atomic table swap (inside a transaction). If a load crashes partway, production data is left completely untouched — avoiding the silent-partial-load failure mode where a table looks "done" but is actually incomplete.

### Idempotency
Running the pipeline multiple times produces the same result as running it once. The main load uses a full-replace-and-swap; the upsert demo uses a key-based insert/update split. Running the upsert repeatedly does **not** create duplicate rows — a record that was an INSERT on the first run correctly becomes an UPDATE on the next.

### Resilient API extraction
The API extractor implements **exponential backoff with jitter** and distinguishes **retryable errors** (429, 5xx) from **permanent errors** (4xx like 404/403), failing fast on the latter instead of pointlessly retrying requests that can never succeed.

### Justified data cleaning
Null handling is decided per column based on meaning, not applied blindly:
- Text fields (e.g. missing descriptions) get explicit placeholder labels.
- Numeric measurements (e.g. missing IMDb vote counts) are **left as null** rather than imputed — fabricating a measured value that was never recorded would corrupt any downstream aggregation. (This decision was validated empirically: score-vs-votes correlation was only ~0.18, confirming votes cannot be reliably estimated from score.)

### Observability
Uses Python's `logging` module (file + console, timestamped, severity-tagged) with a clean separation between pipeline **events** (INFO) and diagnostics. A **null profile** is logged on the raw data every run, surfacing unexpected data drift instead of silently processing it.

### Parameterized, reusable functions
The load logic lives in a single parameterized function called once per table, rather than being copy-pasted — eliminating the class of copy-paste drift bugs and following the Single Responsibility Principle.

---

## Data Sources

- **Netflix TV Shows and Movies** — a CSV dataset (~5,300 titles) representing a static flat-file source.
- **JSONPlaceholder `/users`** — a free public REST API returning nested JSON, representing a live API source (and exercising nested-JSON flattening via `json_normalize`).

---

## Notes & Known Limitations

- The upsert's `UPDATE` statement uses a fixed `SET` clause (email/phone) since it has a single caller; fully generalizing the `SET` clause to arbitrary columns would require dynamic SQL construction and is out of scope for this demo.
- The atomic-swap pattern relies on SQLite's transactional DDL. The same pattern is more robust on warehouse-grade databases (PostgreSQL, Snowflake), which support richer atomic operations.
- SQLite is used for simplicity; the SQLAlchemy interface means the load logic is largely portable to other databases with minimal changes.
