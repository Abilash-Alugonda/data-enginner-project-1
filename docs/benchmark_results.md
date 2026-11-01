# Benchmark Results

Measured wall-clock runtimes across dataset scales and data engineering scenarios.

| Scenario | Strategy | Input Rows | Target Rows | Duration (seconds) | Throughput (rows/sec) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| Scale 10000 | full_refresh | 10,000 | 7,992 | 1.510 | 5,292 |
| Scale 10000 | incremental_merge | 10,000 | 7,992 | 0.320 | 24,941 |
| Scale 10000 | partition_replace | 10,000 | 7,992 | 0.761 | 10,507 |
| Scale 100000 | full_refresh | 100,000 | 79,992 | 1.779 | 44,963 |
| Scale 100000 | incremental_merge | 100,000 | 79,992 | 2.199 | 36,379 |
| Scale 100000 | partition_replace | 100,000 | 79,992 | 2.774 | 28,834 |
| Scale 500000 | full_refresh | 500,000 | 399,992 | 8.460 | 47,278 |
| Scale 500000 | incremental_merge | 500,000 | 399,992 | 9.552 | 41,873 |
| Scale 500000 | partition_replace | 500,000 | 399,992 | 10.411 | 38,419 |
| Incremental Delta (1%) | full_refresh | 1,000 | 100,000 | 0.772 | 1,295 |
| Incremental Delta (1%) | incremental_merge | 1,000 | 100,000 | 0.193 | 5,187 |
| Incremental Delta (1%) | partition_replace | 1,000 | 100,000 | 2.981 | 335 |
| Historical Backfill | incremental_merge | 5,000 | 105,000 | 0.162 | 30,806 |
| Historical Backfill | partition_replace | 5,000 | 105,000 | 0.629 | 7,942 |
| Historical Backfill | full_refresh | 5,000 | 105,000 | 0.971 | 5,151 |
| Optimization: Baseline (Row-by-Row Iteration) | incremental_merge | 5,000 | 5,000 | 20.726 | 241 |
| Optimization: Vectorized (Columnar Arrow Registration) | incremental_merge | 5,000 | 5,000 | 0.044 | 114,196 |

## Optimization Analysis: Vectorized Columnar Ingestion

Row-by-row iteration using parameter tuples incurs severe Python interpreter and database dispatch overhead.
By converting incoming records directly into an in-memory PyArrow table and registering it with DuckDB,
the pipeline leverages vectorized SIMD columnar execution, reducing ingestion latency by orders of magnitude.
