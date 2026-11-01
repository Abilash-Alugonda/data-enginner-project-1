# Data Dictionary

Comprehensive schema documentation for the e-commerce orders data warehouse.

## Tables Overview

1. `schema_version`: Migration tracking table recording applied DDL scripts in sequence.
2. `customers`: Dimension table containing registered customer master records.
3. `products`: Dimension table containing product catalog items and standard prices.
4. `orders_staging`: Intermediate working table used during atomic full refresh rebuilds.
5. `orders`: Target fact table storing the latest deduplicated state per order.
6. `orders_full_refresh`: Strategy-specific fact table representing full refresh output.
7. `orders_incremental`: Strategy-specific fact table representing incremental merge output.
8. `orders_partition_index`: Parquet partition registry tracking date partitions, row counts, and file paths.
9. `order_status_history`: Slowly changing event log preserving all status transitions.
10. `rejects`: Quarantine table storing raw unparseable, malformed, or invalid payloads.
11. `run_log`: Observability table tracking every pipeline execution and run metrics.
12. `watermarks`: High-watermark state tracking for incremental merge load strategy.
13. `reconciliation_results`: Historical storage of reconciliation metrics and dataset hashes.
14. `benchmark_results`: Performance benchmark results recording throughput and duration.

---

## 1. schema_version

- **Grain:** One row per applied migration script.
- **Primary Key:** `version`

| Column | Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `version` | VARCHAR | No | Migration filename (e.g. `001_core_tables.sql`). |
| `applied_at` | VARCHAR | No | ISO-8601 UTC timestamp when migration was executed. |
| `description` | VARCHAR | No | Descriptive summary of changes applied. |

---

## 2. customers

- **Grain:** One row per customer.
- **Primary Key:** `customer_id`

| Column | Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `customer_id` | VARCHAR | No | Unique customer identifier (e.g. `CUST-00001`). |
| `name` | VARCHAR | No | Customer full name. |
| `email` | VARCHAR | No | Customer email address. |
| `country` | VARCHAR | No | ISO country code (e.g. `USA`, `CAN`, `GBR`). |
| `created_at` | VARCHAR | No | ISO-8601 UTC timestamp of account creation. |

---

## 3. products

- **Grain:** One row per catalog item.
- **Primary Key:** `product_id`

| Column | Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `product_id` | VARCHAR | No | Unique product identifier (e.g. `PROD-00001`). |
| `product_name` | VARCHAR | No | Descriptive catalog name. |
| `category` | VARCHAR | No | Product merchandise category (e.g. `Electronics`, `Apparel`). |
| `unit_price` | DOUBLE | No | Non-negative standard item unit price. |

---

## 4. orders_staging

- **Grain:** Transient staging rows during full refresh load.
- **Primary Key:** `order_id`

| Column | Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `order_id` | VARCHAR | No | Unique order identifier. |
| `customer_id` | VARCHAR | No | Foreign key referencing `customers.customer_id`. |
| `product_id` | VARCHAR | No | Foreign key referencing `products.product_id`. |
| `quantity` | BIGINT | No | Item quantity (integer greater than or equal to 0). |
| `order_amount` | DOUBLE | No | Total order amount in USD. |
| `order_status` | VARCHAR | No | Validated order status value. |
| `order_date` | VARCHAR | No | Order date matching `YYYY-MM-DD`. |
| `updated_at` | VARCHAR | No | ISO-8601 UTC timestamp of order state update. |
| `run_id` | VARCHAR | No | Execution run identifier. |
| `source_file` | VARCHAR | No | Source file where record originated. |

---

## 5. orders

- **Grain:** One row per `order_id`. Latest version wins by `updated_at`.
- **Primary Key:** `order_id`
- **Foreign Keys:** `customer_id` references `customers(customer_id)`, `product_id` references `products(product_id)`.
- **Partition Column:** `order_date` (daily partition).

| Column | Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `order_id` | VARCHAR | No | Unique order identifier (e.g. `ORD-000001`). |
| `customer_id` | VARCHAR | No | Foreign key referencing `customers.customer_id`. |
| `product_id` | VARCHAR | No | Foreign key referencing `products.product_id`. |
| `quantity` | BIGINT | No | Item quantity (integer greater than or equal to 0). |
| `order_amount` | DOUBLE | No | Total order amount in USD (greater than or equal to 0.0). |
| `order_status` | VARCHAR | No | Status: `PENDING`, `PROCESSING`, `SHIPPED`, `DELIVERED`, `CANCELLED`, `RETURNED`. |
| `order_date` | VARCHAR | No | Order placement date matching `YYYY-MM-DD`. Hive partition key. |
| `updated_at` | VARCHAR | No | ISO-8601 UTC timestamp of order state update. |
| `run_id` | VARCHAR | No | Identifier of the pipeline run that loaded this record. |
| `source_file` | VARCHAR | No | Filename of the source file where this record originated. |

---

## 6. orders_full_refresh

- **Grain:** One row per `order_id` representing full refresh output.
- **Primary Key:** `order_id`

| Column | Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `order_id` | VARCHAR | No | Unique order identifier. |
| `customer_id` | VARCHAR | No | Foreign key referencing `customers.customer_id`. |
| `product_id` | VARCHAR | No | Foreign key referencing `products.product_id`. |
| `quantity` | BIGINT | No | Item quantity. |
| `order_amount` | DOUBLE | No | Total order amount in USD. |
| `order_status` | VARCHAR | No | Order status value. |
| `order_date` | VARCHAR | No | Order date (`YYYY-MM-DD`). |
| `updated_at` | VARCHAR | No | ISO-8601 UTC timestamp. |
| `run_id` | VARCHAR | No | Pipeline execution run identifier. |
| `source_file` | VARCHAR | No | Filename of source payload. |

---

## 7. orders_incremental

- **Grain:** One row per `order_id` representing incremental merge output.
- **Primary Key:** `order_id`

| Column | Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `order_id` | VARCHAR | No | Unique order identifier. |
| `customer_id` | VARCHAR | No | Foreign key referencing `customers.customer_id`. |
| `product_id` | VARCHAR | No | Foreign key referencing `products.product_id`. |
| `quantity` | BIGINT | No | Item quantity. |
| `order_amount` | DOUBLE | No | Total order amount in USD. |
| `order_status` | VARCHAR | No | Order status value. |
| `order_date` | VARCHAR | No | Order date (`YYYY-MM-DD`). |
| `updated_at` | VARCHAR | No | ISO-8601 UTC timestamp. |
| `run_id` | VARCHAR | No | Pipeline execution run identifier. |
| `source_file` | VARCHAR | No | Filename of source payload. |

---

## 8. orders_partition_index

- **Grain:** One row per registered Parquet partition file.
- **Primary Key:** Composite `(partition_key, file_path)`.

| Column | Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `partition_key` | VARCHAR | No | Partition identifier (e.g. `order_date=2026-01-15`). |
| `partition_date` | VARCHAR | Yes | Date string (`YYYY-MM-DD`). |
| `file_path` | VARCHAR | No | Relative or absolute path to Parquet partition file. |
| `row_count` | BIGINT | No | Verified record count in partition. |
| `record_count` | BIGINT | Yes | Alias record count for backwards compatibility. |
| `updated_at` | VARCHAR | No | ISO-8601 UTC timestamp of partition write. |
| `last_updated_at` | VARCHAR | Yes | Alias timestamp for backwards compatibility. |
| `run_id` | VARCHAR | No | Pipeline run that generated the partition. |

---

## 9. order_status_history

- **Grain:** One row per status transition per order. Preserves history instead of overwriting.
- **Primary Key:** `history_id`

| Column | Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `history_id` | VARCHAR | No | SHA-256 deterministic hash of `order_id + order_status + updated_at`. |
| `order_id` | VARCHAR | No | Reference to target order. |
| `order_status` | VARCHAR | No | Status value recorded during transition. |
| `updated_at` | VARCHAR | No | ISO-8601 UTC timestamp of status transition. |
| `run_id` | VARCHAR | No | Pipeline execution run identifier. |
| `source_file` | VARCHAR | No | Filename of source payload. |

---

## 10. rejects

- **Grain:** One row per rejected raw input record.
- **Primary Key:** `reject_id`

| Column | Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `reject_id` | VARCHAR | No | Deterministic hash of `run_id + source_file + source_line + reject_reason`. |
| `run_id` | VARCHAR | No | Pipeline run identifier. |
| `source_file` | VARCHAR | No | Source file where error occurred. |
| `source_line` | BIGINT | No | Line number (1-based) in source file. |
| `reject_reason` | VARCHAR | No | Categorized failure reason (e.g. `FOREIGN_KEY_VIOLATION`, `UNEXPECTED_SCHEMA`). |
| `raw_payload` | VARCHAR | No | Exact unparsed raw payload string for traceability. |
| `created_at` | VARCHAR | No | ISO-8601 UTC timestamp when quarantined. |

---

## 11. run_log

- **Grain:** One row per pipeline execution run.
- **Primary Key:** `run_id`

| Column | Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `run_id` | VARCHAR | No | Unique run identifier (e.g. `RUN-FULL_REFRESH-20261031_120000`). |
| `strategy` | VARCHAR | No | Load strategy executed: `full_refresh`, `incremental_merge`, `partition_replace`. |
| `start_time` | VARCHAR | No | UTC timestamp when run began. |
| `end_time` | VARCHAR | No | UTC timestamp when run concluded. |
| `input_count` | BIGINT | No | Total raw records ingested. |
| `output_count` | BIGINT | No | Total valid records loaded to target table. |
| `rejected_count` | BIGINT | No | Total records quarantined to rejects. |
| `status` | VARCHAR | No | Execution status: `RUNNING`, `SUCCESS`, or `FAILED`. |
| `failure_reason` | VARCHAR | Yes | Error message if failed, otherwise null. |

---

## 12. watermarks

- **Grain:** One row per strategy storing high watermark.
- **Primary Key:** `strategy`

| Column | Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `strategy` | VARCHAR | No | Strategy identifier (`incremental_merge`). |
| `last_watermark` | VARCHAR | No | Latest committed `updated_at` timestamp. |
| `updated_at` | VARCHAR | No | UTC timestamp when watermark was updated. |

---

## 13. reconciliation_results

- **Grain:** One row per multi-strategy reconciliation audit.
- **Primary Key:** `run_id`

| Column | Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `run_id` | VARCHAR | No | Run identifier for reconciliation execution. |
| `source_count` | BIGINT | No | Total records in raw source dataset. |
| `target_distinct_orders` | BIGINT | No | Count of distinct orders across strategies. |
| `rejects_count` | BIGINT | No | Total records rejected across strategies. |
| `cross_strategy_equivalence` | BOOLEAN | No | Boolean flag indicating whether all strategies produced identical target hashes. |
| `dataset_hash` | VARCHAR | No | SHA-256 fingerprint of the final reconciled fact table. |
| `created_at` | VARCHAR | No | ISO-8601 UTC timestamp of reconciliation audit. |

---

## 14. benchmark_results

- **Grain:** One row per benchmark scenario and strategy run.
- **Primary Key:** `benchmark_id`

| Column | Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `benchmark_id` | VARCHAR | No | Unique benchmark run identifier. |
| `scenario` | VARCHAR | No | Benchmark scenario name (e.g. `initial_load_100k`, `delta_10k`). |
| `strategy` | VARCHAR | No | Load strategy benchmarked. |
| `input_rows` | BIGINT | No | Number of rows processed. |
| `target_rows` | BIGINT | No | Final rows stored in warehouse. |
| `duration_seconds` | DOUBLE | No | Total elapsed wall-clock runtime in seconds. |
| `throughput_rows_per_sec` | BIGINT | No | Ingestion and loading rate in rows per second. |
| `created_at` | VARCHAR | No | ISO-8601 UTC timestamp of benchmark execution. |
