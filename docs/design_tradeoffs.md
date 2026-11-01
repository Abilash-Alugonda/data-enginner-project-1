# Design Trade-Offs: Load Strategy Comparison

A comprehensive comparison of the three primary data warehouse ingestion strategies: Full Refresh, Incremental Merge, and Partition Replacement.

## Comparison Matrix

| Evaluation Dimension | Full Refresh | Incremental Merge | Partition Replacement |
| :--- | :--- | :--- | :--- |
| **Primary Mechanism** | Truncates or drops target, rebuilds staging table from all sources, swaps atomically. | Tracks high-watermark timestamp, updates or inserts records newer than watermark. | Rewrites only affected daily Parquet partition files atomically via temporary directory swap. |
| **10k Rows Runtime** | 1.510s | 0.320s | 0.761s |
| **100k Rows Runtime** | 1.779s | 2.199s | 2.774s |
| **500k Rows Runtime** | 8.460s | 9.552s | 10.411s |
| **1% Incremental Delta (1,000 rows)** | 0.772s (Must rebuild complete 100k state) | 0.193s (Fastest: processes delta only) | 2.981s (Rewrites affected partition files) |
| **Historical Backfill (5,000 rows)** | 0.971s (Rebuilds full warehouse) | 0.162s (Merges into existing tables) | 0.629s (Replaces only historical partition directories) |
| **Implementation Complexity** | Lowest. Standard SQL staging DDL and table swap. No watermark state. | Medium. Requires high-watermark state tracking, transaction isolation, and upsert logic. | High. Requires file system atomic directory operations, Parquet serialization, and partition metadata sync. |
| **Failure Recovery** | Atomic staging swap. If a failure occurs before swap, target is untouched. Retry rebuilds cleanly. | Transaction rollback. Database transaction reverts partial upserts. Watermark only commits on success. | Atomic rename. Staged partitions write to isolated temp folder. On error, temp folder is deleted. |
| **Late and Out-of-Order Data** | Naturally correct: latest version always wins because all source rows are re-evaluated. | Requires explicit check: incoming rows older than existing target records are ignored for target state. | Automatically resolved per partition: partition rewrite merges existing parquet data with incoming rows. |
| **Storage and I/O Overhead** | High cumulative I/O: rewrites 100% of warehouse data on every execution run. | Low I/O: writes only delta records and updates index/table pages. | Targeted I/O: reads and rewrites only partition files containing affected dates. |
| **Idempotency** | Inherent. Running identical source data produces identical staging and target state. | Guaranteed by primary key constraints and deterministic timestamp precedence rules. | Guaranteed by partition-level deterministic deduplication and atomic directory overwrite. |

---

## Detailed Strategy Breakdown

### 1. Full Refresh

- **How it Works:** The pipeline ingests all available history into a temporary staging table (`orders_staging`). Upon completion, DuckDB executes an atomic swap renaming `orders_staging` to `orders`.
- **Strengths:** Simplest implementation, zero watermark state management, completely immune to out-of-order data anomalies.
- **Weaknesses:** Runtime and compute cost scale linearly with total historical volume, becoming prohibitive on multi-terabyte datasets.
- **When to Choose:** Small to medium datasets (under 10 million rows), dimension tables, reporting snapshots, or when upstream data regularly changes historical records arbitrarily.

### 2. Incremental Merge

- **How it Works:** The pipeline queries the `watermarks` table for the last processed timestamp. Incoming records are merged using set-based SQL joins: new keys are inserted, existing keys are updated if the incoming timestamp is strictly newer.
- **Strengths:** Superior performance on small incoming deltas (0.193s for 1% delta versus 0.772s for full refresh).
- **Weaknesses:** Requires robust transaction handling and watermark state storage. Extreme backfills touching wide date ranges require careful indexing.
- **When to Choose:** High-frequency ingestion pipelines, streaming micro-batches, large fact tables where updates represent a tiny fraction of historical volume.

### 3. Partition Replacement

- **How it Works:** Orders are organized into daily Parquet partition folders (`order_date=YYYY-MM-DD`). Incoming records identify affected dates. Staged Parquet files are written to a temporary directory and atomically moved into the target partition location.
- **Strengths:** Excellent file-level isolation, zero full-table lock contention, ideal for data lakehouse architectures (S3, GCS, local Parquet storage).
- **Weaknesses:** If incoming batches scatter single records across dozens of historical partitions, write amplification can degrade throughput.
- **When to Choose:** Daily batch workflows, partitioned Parquet lakehouses, analytical engines queried via partition pruning filters.

