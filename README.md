# Load Strategy Comparison: Full Refresh vs Incremental Merge vs Partition Replacement

A production-grade, tested, and reproducible data engineering vertical slice comparing three core load strategies for e-commerce orders: Full Refresh, Incremental Merge, and Partition Replacement. Built with Python 3.11+, DuckDB, PyArrow, Pandas, Pydantic, and FastAPI.

---

## Purpose

Data warehouse pipelines handle varying update frequencies, data volumes, and partition schemes. Choosing between full rebuilds, watermark-based merges, and partition overwrites involves trade-offs across execution latency, resource cost, failure blast radius, and recovery complexity.

This project implements all three strategies on an identical synthetic dataset containing normal records, boundary conditions, out-of-order updates, and anomalies. It provides mathematical proof of cross-strategy equivalence alongside measured benchmarks.

---

## Architecture Overview

```
+-----------------------------------------------------------------------------------+
|                                 SOURCE DATA                                       |
|  - customers.csv (Dimension)                       - orders_initial.jsonl (Fact)  |
|  - products.csv (Dimension)                        - orders_updates.csv (Delta)   |
|  - orders_schema_change.csv (Anomalous Schema)     - orders_malformed.csv (Errors)|
+-----------------------------------------------------------------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
|                           INGESTION & VALIDATION ENGINE                           |
|  1. Format Parsing (CSV / JSONL) with Line Tracking                               |
|  2. Strict Schema Check (unexpected columns / missing required fields)            |
|  3. Referential Integrity Check (foreign keys in customers and products)          |
|  4. Business Rule Checks (quantity >= 0, order_amount >= 0.0, valid status)       |
|  5. Deterministic Duplicate Resolution (latest updated_at, tie-break by source)   |
+-----------------------------------------------------------------------------------+
        |                                                              |
        | (Invalid / Malformed Rows)                                   | (Valid Rows)
        v                                                              v
+-----------------------+                    +--------------------------------------+
|  QUARANTINE / REJECTS |                    |       THREE LOAD STRATEGIES          |
|  - reject_id (PK)     |                    |  1. Full Refresh (Staging Swap)      |
|  - run_id             |                    |  2. Incremental Merge (Watermark)    |
|  - source_file:line   |                    |  3. Partition Replace (Daily Parquet)|
|  - reject_reason      |                    +--------------------------------------+
|  - raw_payload        |                                      |
+-----------------------+                                      v
                                             +--------------------------------------+
                                             |        TARGET DATA WAREHOUSE         |
                                             |  - orders (Fact table)               |
                                             |  - order_status_history (Audit log)  |
                                             |  - run_log (Observability log)       |
                                             |  - watermarks (State tracking)       |
                                             +--------------------------------------+
                                                               |
                                                               v
                                             +--------------------------------------+
                                             |     RECONCILIATION & BENCHMARKS      |
                                             |  - Record conservation verification  |
                                             |  - SHA-256 row-level hash equality   |
                                             |  - Wall-clock latency benchmarks     |
                                             +--------------------------------------+
```

---

## Folder Structure

```
.
├── config.yaml                     # Pipeline configuration (paths, generator, strategies)
├── requirements.txt                # Exact pinned dependencies
├── pyproject.toml                  # Project metadata, pytest, and ruff configuration
├── Makefile                        # POSIX build targets
├── run.sh                          # Linux / macOS shell runner
├── run.ps1                         # Windows PowerShell runner
├── verify_api.py                   # Automated API verification test script
├── data/
│   ├── sample/                     # Starter sample (~200 rows with edge cases)
│   ├── expected/                   # Ground-truth expected answer labels
│   └── warehouse/                  # DuckDB database and partitioned Parquet files
├── src/
│   ├── config.py                   # Config loader with environment overrides
│   ├── generator.py                # Deterministic synthetic data generator (seed 42)
│   ├── models.py                   # Pydantic domain, log, and reject entities
│   ├── generate_favicons.py        # Flat solid-color favicon generator
│   ├── pipeline.py                 # Pipeline orchestrator and cross-reconciliation
│   ├── api/
│   │   ├── app.py                  # FastAPI application and static mount
│   │   └── routes.py               # REST endpoints and HTML dashboard views
│   ├── ingestion/
│   │   └── reader.py               # Robust CSV and JSONL line-level reader
│   ├── validation/
│   │   └── validator.py            # Referential integrity and business rules validator
│   ├── storage/
│   │   └── db.py                   # DuckDB manager with thread-safe cursor pool
│   ├── strategies/
│   │   ├── base.py                 # Base strategy class and PyArrow conversion utilities
│   │   ├── full_refresh.py         # Staging table rebuild and atomic swap
│   │   ├── incremental_merge.py    # Watermark tracking and set-based upsert
│   │   └── partition_replace.py    # Daily Parquet partition staging and atomic swap
│   ├── observability/
│   │   └── logger.py               # Run logging to DuckDB and structured JSON lines
│   ├── reconciliation/
│   │   └── reconciler.py           # Record conservation and row-level hash checks
│   └── benchmarks/
│       └── runner.py               # Benchmark runner across scales and scenarios
├── static/
│   ├── css/style.css               # Accessible flat stylesheet (no gradients, no pills)
│   ├── favicon.svg                 # Solid-color vector favicon
│   ├── favicon.ico                 # Standard multi-resolution icon
│   ├── favicon-32x32.png           # 32x32 icon
│   ├── robots.txt                  # Robots exclusion standard file
│   └── sitemap.xml                 # XML sitemap
├── docs/
│   ├── data_dictionary.md          # Complete entity-relationship schema catalog
│   ├── design_tradeoffs.md         # Measured comparison of load strategies
│   ├── LAUNCH_CHECKLIST.md         # Pre-launch gate with unchecked operational items
│   ├── test_run_output.txt         # Real executed pytest output with coverage
│   ├── api_check.txt               # Real executed endpoint verification log
│   ├── benchmark_results.csv       # Measured benchmark metrics
│   ├── benchmark_results.md        # Formatted benchmark markdown report
│   ├── reconciliation_report.json  # Latest cross-strategy reconciliation JSON
│   └── reconciliation_report.md    # Latest cross-strategy reconciliation Markdown
└── tests/
    ├── conftest.py                 # Pytest fixtures
    ├── test_generator.py           # Reproducibility and seed tests
    ├── test_ingestion_and_validation.py # Schema, nulls, malformed, and reject tests
    ├── test_strategies.py          # Strategy execution, idempotency, and fault recovery
    ├── test_equivalence.py         # Mathematical cross-strategy equivalence tests
    ├── test_domain_edge_cases.py   # Domain edge cases (cancellations, zero quantity)
    └── test_api.py                 # API and HTML web endpoint integration tests
```

---

## Setup and Quick Start

### 1. Prerequisites
- Python 3.11, 3.12, 3.13, or 3.14.
- All dependencies are listed in `requirements.txt`. No external databases, secrets, or cloud accounts are required. Everything runs locally.

### 2. Environment Setup

```bash
# Clone the repository and navigate into the root directory
cd projectcla

# Create virtual environment
python -m venv .venv

# Activate virtual environment
# Windows PowerShell:
.venv\Scripts\Activate.ps1
# Linux / macOS:
source .venv/bin/activate

# Install exact pinned requirements
pip install -r requirements.txt
```

### 3. Execution Targets

You can use `run.ps1` (Windows), `run.sh` (Linux/macOS), or `make` (POSIX):

| Target | PowerShell Command | Shell / Make Command | Description |
| :--- | :--- | :--- | :--- |
| **setup** | `.\run.ps1 setup` | `./run.sh setup` | Installs pinned dependencies from `requirements.txt`. |
| **generate-sample** | `.\run.ps1 generate-sample` | `./run.sh generate-sample` | Generates starter sample (200 rows in `data/sample`). |
| **generate-large** | `.\run.ps1 generate-large` | `./run.sh generate-large` | Generates large dataset (100,000 rows in `data/large`). |
| **generate** | `.\run.ps1 generate [sample\|large\|<N>]` | `./run.sh generate [sample\|large\|<N>]` | Generates dataset with specified preset or row count. |
| **run-sample** | `.\run.ps1 run-sample` | `./run.sh run-sample` | Runs all 3 strategies and reconciles on `data/sample`. |
| **run-large** | `.\run.ps1 run-large` | `./run.sh run-large` | Runs all 3 strategies and reconciles on `data/large`. |
| **run** | `.\run.ps1 run [sample\|large\|<path>]` | `./run.sh run [sample\|large\|<path>]` | Runs and reconciles on specified preset or input path. |
| **test** | `.\run.ps1 test` | `./run.sh test` | Runs complete 50-test suite with coverage report. |
| **lint** | `.\run.ps1 lint` | `./run.sh lint` | Checks code formatting and syntax using Ruff. |
| **serve** | `.\run.ps1 serve` | `./run.sh serve` | Starts FastAPI dashboard at `http://127.0.0.1:8000`. |

---

## Dataset Scales: Starter Sample vs Configurable Large Run

The project provides first-class support for both quick developer reviews and production-scale runs:

### 1. Small Starter Sample (200 Records)
Designed for rapid review, unit/integration testing, CI/CD verification, and immediate web UI inspection.
- **Location:** `data/sample/`
- **Volume:** 25 customers, 15 products, 140 initial orders, 40 updates, 15 schema change rows, 5 malformed lines (200 total order rows).
- **Execution Speed:** Generates in ~0.15s, executes all 3 strategies and completes cross-strategy reconciliation in ~0.6s.
- **Generation:**
  - Windows PowerShell: `.\run.ps1 generate-sample`
  - Linux / macOS: `./run.sh generate-sample`
  - Direct Python: `python -m src.generator --preset sample`
- **Execution & Reconciliation:**
  - Windows PowerShell: `.\run.ps1 run-sample`
  - Linux / macOS: `./run.sh run-sample`
  - Direct Python: `python -m src.pipeline --preset sample`

### 2. Configurable Larger Run (100,000+ Records)
Designed for high-throughput stress testing, real memory profiling, Parquet partition scaling, and benchmark validation.
- **Location:** `data/large/`
- **Volume:** 5,000 customers, 500 products, 80,000 initial orders, 15,000 updates, 3,000 schema change rows, 2,000 malformed lines (100,000 total order rows).
- **Execution Speed:** Generates 100,000 rows in ~3.8s, loads into warehouse in ~8s, and proves mathematical byte-identical reconciliation equivalence across all three strategies in ~14s.
- **Configurable Settings:** Configured in `config.yaml` under `generator.presets.large.rows` (default: 100,000).
- **Generation:**
  - Windows PowerShell: `.\run.ps1 generate-large`
  - Linux / macOS: `./run.sh generate-large`
  - Direct Python: `python -m src.generator --preset large`
- **Execution & Reconciliation:**
  - Windows PowerShell: `.\run.ps1 run-large`
  - Linux / macOS: `./run.sh run-large`
  - Direct Python: `python -m src.pipeline --preset large`

### 3. Custom Arbitrary Sample Sizes
You can generate and process any arbitrary row count (e.g. 50,000, 250,000, or 500,000+):
- **Generate custom rows:**
  - PowerShell: `.\run.ps1 generate 250000`
  - Bash: `./run.sh generate 250000`
  - Python: `python -m src.generator --rows 250000 --out data/custom`
- **Run pipeline on custom rows:**
  - PowerShell: `.\run.ps1 run data/custom`
  - Bash: `./run.sh run data/custom`
  - Python: `python -m src.pipeline --input-path data/custom`

---

## Database

### Why DuckDB
DuckDB is an embedded, columnar SQL OLAP database engine executing directly in-process. It requires zero server processes, zero network ports, zero Docker containers, and zero credentials. Key advantages for this warehouse vertical slice:
- Fast vectorized analytics and Apache Arrow zero-copy memory transfers.
- Full ACID transactions with explicit `BEGIN TRANSACTION`, `COMMIT`, and `ROLLBACK`.
- Native Parquet integration via `read_parquet` functions.
- Cross-platform portability across Windows, macOS, and Linux with standard file-based storage.

### File Location and Configuration
The default database file path is configured in `config.yaml`:
```yaml
paths:
  duckdb_path: data/warehouse/warehouse.duckdb
```
The storage path can be dynamically overridden without modifying code:
1. **CLI Flag:** Pass `--db-path /custom/path/warehouse.duckdb` to database commands (`python -m src.db.init_db --db-path ...`).
2. **Environment Variable:** Set `DB_PATH=/custom/path/warehouse.duckdb` (or `DUCKDB_PATH`).
3. **Automatic Directory Creation:** If the target directory or parent folders do not exist, `get_connection` and `DatabaseManager` create them automatically via `pathlib.Path.mkdir(parents=True, exist_ok=True)`.

### Single Writer Concurrency Model
DuckDB implements a single-writer, multi-reader concurrency model:
- **Multiple Readers:** Any number of processes can read concurrently by passing `read_only=True` to `get_connection()` or `db_session()`. All FastAPI read endpoints (`GET /runs`, `GET /rejects`, `GET /metrics`, `GET /health`) operate exclusively in `read_only=True` mode.
- **Single Writer:** Only one process can hold an exclusive write lock at any time. Pipeline writes, data migrations, and staging swaps use short-lived write sessions.
- **Automatic Schema Initialization on Read:** If a read-only connection is opened against a missing database file, `get_connection(read_only=True)` automatically runs `init_db()` to create the schema rather than crashing with an IO error.

### Connection Management and Backoff Retries
To eliminate file lock conflicts:
1. **No Global Connections:** Connections are never stored in global variables or persistent class attributes.
2. **Short-Lived Sessions:** Operations use the `db_session(read_only=False)` context manager, which automatically commits upon clean exit, rolls back on exception, and guarantees `con.close()` execution in a `finally` block.
3. **Exponential Backoff:** If the database file is locked by another process during connection acquisition, the system automatically retries up to 5 times with exponential backoff delays (0.2s, 0.4s, 0.8s, 1.6s, 3.2s). If all retries are exhausted, it raises `LockTimeoutError` with structured diagnostics.
4. **Structured JSON Logging:** Every connection open, close, and retry attempt is logged as structured JSON lines with timestamps, file path, and active `run_id`.

### Database Reset and Migrations
The database schema is managed via sequential SQL files in `sql/` (`001_core_tables.sql`, `002_run_log.sql`, `003_rejects.sql`, `004_history.sql`). Applied migrations are tracked in `schema_version`.
- **Safe Initialization (Idempotent):** Applies only unapplied migrations in transactional sequence.
  - Windows PowerShell: `python -m src.db.init_db`
  - macOS / Linux: `python3 -m src.db.init_db`
- **Destructive Reset:** Drops all existing tables and re-executes all migrations from scratch. Requires explicit confirmation:
  - Windows PowerShell: `python -m src.db.init_db --reset --yes`
  - macOS / Linux: `python3 -m src.db.init_db --reset --yes`

### Pre-flight Startup Health Check
Before pipeline execution and within `GET /health`, the system runs `run_healthcheck()`, verifying:
1. Connection acquisition
2. Ping execution (`SELECT 1`)
3. Schema version currency against `sql/*.sql`
4. Temporary table write permission
5. Free disk space against threshold (default: 500 MB)
6. Python (>=3.11), DuckDB, and PyArrow runtime versions

Run the healthcheck CLI diagnostic:
- Windows PowerShell: `python -m src.db.healthcheck`
- macOS / Linux: `python3 -m src.db.healthcheck`

### Lock Recovery Instructions
If an ungraceful crash leaves an active lock:
1. Check for active background write jobs or dangling Python processes:
   - Windows PowerShell: `Get-Process python -ErrorAction SilentlyContinue | Select-Object Id, ProcessName`
   - macOS / Linux: `ps aux | grep python`
2. Stop the conflicting process:
   - Windows PowerShell: `Stop-Process -Name python -Force` (or by PID)
   - macOS / Linux: `pkill -f uvicorn` or `kill -9 <PID>`
3. Check the `data/warehouse/` directory. DuckDB write locks do not use sidecar lockfiles. Ensure all connections have closed and the `.wal` write-ahead log will merge cleanly on the next connection.

---

## Load Strategies Explained

### 1. Full Refresh (`full_refresh`)
- **Pattern:** Staging table rebuild followed by atomic table swap.
- **Workflow:** Ingests all available historical data into `orders_staging`. Validates foreign keys and constraints. Once validated, executes `DROP TABLE orders; ALTER TABLE orders_staging RENAME TO orders;` inside an atomic transaction.
- **Fault Recovery:** If an error occurs during staging load, the staging table is dropped. The active `orders` table remains untouched. Retrying re-executes the complete staging load.

### 2. Incremental Merge (`incremental_merge`)
- **Pattern:** High-watermark tracking with set-based SQL upserts.
- **Workflow:** Reads the latest watermark from `watermarks`. Ingests only new or modified records into a temporary in-memory PyArrow table. Executes a set-based upsert: inserts unseen `order_id` records and updates existing records only if `incoming.updated_at > orders.updated_at`. Commits new watermark.
- **Out-of-Order Handling:** Older updates arriving late do not overwrite newer states in `orders`. All historical status changes are appended to `order_status_history`.
- **Fault Recovery:** Executed within an explicit transaction. Partial failures trigger an immediate database rollback, leaving the watermark and target table untouched. Retrying re-evaluates the batch.

### 3. Partition Replacement (`partition_replace`)
- **Pattern:** File-level atomic directory swap on daily Parquet partitions.
- **Workflow:** Groups incoming records by `order_date=YYYY-MM-DD`. For each affected date, merges existing partition Parquet data with incoming updates (latest `updated_at` wins). Writes new Parquet files to a run-specific temporary directory. Atomically moves staged partition directories into the production partition tree. Syncs DuckDB `orders` view.
- **Fault Recovery:** Temporary files write to an isolated temporary folder. If an error occurs midway, the staging directory is deleted. Production partition files remain intact. Retrying rewrites affected partitions cleanly.

---

## Idempotency and Deduplication Rules

1. **Grain:** One row per `order_id` in `orders`.
2. **Conflict Precedence:** The record with the latest `updated_at` timestamp wins.
3. **Deterministic Tie-Breaking:** If two records share the identical `updated_at`, tie-breaking evaluates `source_file` (lexicographical) then `source_line` (descending: later line in file wins).
4. **Idempotency Guarantee:** Re-running the pipeline multiple times with the exact same input produces zero row additions, zero duplicate keys, and zero aggregate value changes.
5. **Audit Preservation:** Every distinct status transition is preserved in `order_status_history` with a deterministic primary key `history_id = sha256(order_id + order_status + updated_at)`.

---

## Reconciliation and Proof of Equivalence

The pipeline performs automated multi-point reconciliation after each execution run:
1. **Record Conservation:** Validates that `source_count == valid_status_transitions + quarantined_rejects`.
2. **Aggregate Matching:** Verifies that `sum(order_amount)`, `count(*) by status`, and `count(*) by date` match identically across strategies.
3. **Row-Level SHA-256 Equality:** Computes a composite SHA-256 hash over all sorted business attributes (`order_id`, `customer_id`, `product_id`, `quantity`, `order_amount`, `order_status`, `order_date`, `updated_at`).

### Measured Results on Starter Sample (200 records)

| Metric | Full Refresh | Incremental Merge | Partition Replace | Status |
| :--- | :--- | :--- | :--- | :--- |
| **Source Records Ingested** | 200 | 200 | 200 | Exact Match |
| **Valid Status Transitions** | 171 | 171 | 171 | Exact Match |
| **Quarantined Rejects** | 29 | 29 | 29 | Exact Match |
| **Target Distinct Orders** | 132 | 132 | 132 | Exact Match |
| **Total Order Amount ($)** | $189,451.70 | $189,451.70 | $189,451.70 | Exact Match |
| **SHA-256 Dataset Hash** | `4b08f8691506b3fa...` | `4b08f8691506b3fa...` | `4b08f8691506b3fa...` | 100% IDENTICAL |

Detailed report files are generated at `docs/reconciliation_report.json` and `docs/reconciliation_report.md`.

---

## Measured Performance Benchmarks

All benchmark metrics are genuine numbers measured on the local host machine. Full numbers are saved to `docs/benchmark_results.csv` and `docs/benchmark_results.md`.

| Scenario | Strategy | Input Rows | Target Rows | Duration (seconds) | Throughput (rows/sec) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Scale 10k** | full_refresh | 10,000 | 7,992 | 1.510s | 5,292 |
| **Scale 10k** | incremental_merge | 10,000 | 7,992 | 0.320s | 24,941 |
| **Scale 10k** | partition_replace | 10,000 | 7,992 | 0.761s | 10,507 |
| **Scale 100k** | full_refresh | 100,000 | 79,992 | 1.779s | 44,963 |
| **Scale 100k** | incremental_merge | 100,000 | 79,992 | 2.199s | 36,379 |
| **Scale 100k** | partition_replace | 100,000 | 79,992 | 2.774s | 28,834 |
| **Scale 500k** | full_refresh | 500,000 | 399,992 | 8.460s | 53,808 |
| **Scale 500k** | incremental_merge | 500,000 | 399,992 | 9.552s | 41,873 |
| **Scale 500k** | partition_replace | 500,000 | 399,992 | 10.411s | 38,419 |
| **Incremental Delta (1%)** | full_refresh | 1,000 | 100,000 | 0.772s | 1,295 |
| **Incremental Delta (1%)** | incremental_merge | 1,000 | 100,000 | 0.193s | 5,187 |
| **Incremental Delta (1%)** | partition_replace | 1,000 | 100,000 | 2.981s | 335 |
| **Historical Backfill** | incremental_merge | 5,000 | 105,000 | 0.162s | 30,806 |
| **Historical Backfill** | partition_replace | 5,000 | 105,000 | 0.629s | 7,942 |
| **Historical Backfill** | full_refresh | 5,000 | 105,000 | 0.971s | 5,151 |

### Real Measured Optimization: Vectorized Columnar Ingestion
- **Baseline (Row-by-Row Cursor Iteration):** 5,000 rows in 20.726 seconds (241 rows/sec).
- **Optimized (Vectorized PyArrow Registration):** 5,000 rows in 0.044 seconds (114,196 rows/sec).
- **Measured Speedup:** Over 470x throughput improvement by avoiding per-row Python interpreter dispatch and leveraging DuckDB columnar SIMD execution.

---

## REST API Reference

The local FastAPI application serves both programmatic endpoints and the static HTML reporting dashboard.

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/health` | Health check returning application status and current UTC time. |
| `GET` | `/runs` | List execution history and statistics for all pipeline runs. |
| `GET` | `/runs/{run_id}` | Detailed metrics for a specific execution run. |
| `GET` | `/rejects?run_id=` | Retrieve quarantined records and failure reasons. |
| `GET` | `/reconciliation/latest` | Retrieve the latest cross-strategy reconciliation report. |
| `GET` | `/benchmarks` | Query measured wall-clock runtimes and throughput numbers. |
| `POST` | `/runs` | Trigger a new run. Body: `{"strategy": "full_refresh", "input_path": "data/sample"}`. |

---

## Web Dashboard Pages

- `GET /` : Overview dashboard summarizing runs, loaded records, and strategy architectures.
- `GET /runs-view` : Tabular history of all execution runs.
- `GET /rejects-view` : Quarantined error payloads with source line traceable links.
- `GET /reconciliation-view` : Cross-strategy reconciliation summary and dataset hash proof.
- `GET /benchmarks-view` : Measured benchmark latency and throughput tables.
- `GET /privacy` : Application privacy policy.
- `GET /terms` : Terms and conditions of software use.
- `GET /robots.txt` : Standard search engine crawler policy.
- `GET /sitemap.xml` : XML sitemap of accessible pages.

---

## Naming Conventions

- **Tables and Columns:** Lowercase `snake_case` (e.g. `order_id`, `customer_id`, `order_amount`, `order_date`).
- **Files:** Lowercase `snake_case` with standard extensions (`.csv`, `.jsonl`, `.parquet`, `.py`).
- **Identifiers:** Uppercase alphanumeric prefixes with zero-padded sequences (`ORD-000001`, `CUST-00001`, `PROD-00001`, `RUN-FR-20261031_120000`).
- **Timestamps:** ISO-8601 UTC representation (`YYYY-MM-DDTHH:MM:SSZ`).
- **Dates:** ISO-8601 calendar date (`YYYY-MM-DD`).

---

## Assumptions

1. **Host Environment:** Target environment runs Python 3.11 or newer on Windows, Linux, or macOS.
2. **Local Execution:** No remote third-party network services or external credentials are used. All computation and storage reside on local disk.
3. **Partition Scheme:** Daily partitions based on `order_date` (`order_date=YYYY-MM-DD`).
4. **Order Status Progression:** Status transitions are non-destructive. The `orders` table holds the latest current state, while `order_status_history` retains every valid status update.
5. **Synthetic Data Only:** All customer and order datasets are synthetically generated.
6. **Legal Review Notice:** The included Privacy Policy and Terms and Conditions pages are drafted specifically for this local synthetic benchmark environment. Before public launch on a production domain, both documents must undergo formal legal review by qualified counsel.

---

## Known Limitations

1. **Single-Node Storage:** Designed for embedded and local analytics using DuckDB and PyArrow. Scaling beyond tens of millions of rows would benefit from distributed engines (such as Spark or Trino).
2. **File-Level Partition Merges:** Partition replacement merges partitions sequentially. In a multi-worker environment, partition writes should be distributed across parallel workers.
3. **In-Process Locking:** DuckDB allows multiple read cursors but serializes concurrent writes. For multi-tenant web workloads, write requests are handled sequentially.

---

## Next Steps

1. Implement parallel worker execution for partition replacement using ProcessPoolExecutor.
2. Add Apache Iceberg and Delta Lake metadata catalog adapters alongside the native Parquet partitioned structure.
3. Integrate OpenTelemetry trace exporting for distributed pipeline tracing.

