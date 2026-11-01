-- Migration 004: Audit history, reconciliation, and benchmark metrics tables
CREATE TABLE IF NOT EXISTS order_status_history (
    history_id VARCHAR PRIMARY KEY,
    order_id VARCHAR NOT NULL,
    order_status VARCHAR NOT NULL,
    updated_at VARCHAR NOT NULL,
    run_id VARCHAR NOT NULL,
    source_file VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS reconciliation_results (
    run_id VARCHAR PRIMARY KEY,
    source_count BIGINT NOT NULL,
    target_distinct_orders BIGINT NOT NULL,
    rejects_count BIGINT NOT NULL,
    cross_strategy_equivalence BOOLEAN NOT NULL,
    dataset_hash VARCHAR NOT NULL,
    created_at VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS benchmark_results (
    benchmark_id VARCHAR PRIMARY KEY,
    scenario VARCHAR NOT NULL,
    strategy VARCHAR NOT NULL,
    input_rows BIGINT NOT NULL,
    target_rows BIGINT NOT NULL,
    duration_seconds DOUBLE NOT NULL,
    throughput_rows_per_sec BIGINT NOT NULL,
    created_at VARCHAR NOT NULL
);

