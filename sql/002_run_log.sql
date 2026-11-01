-- Migration 002: Observability and execution run log table
CREATE TABLE IF NOT EXISTS run_log (
    run_id VARCHAR PRIMARY KEY,
    strategy VARCHAR NOT NULL,
    start_time VARCHAR NOT NULL,
    end_time VARCHAR NOT NULL,
    input_count BIGINT NOT NULL,
    output_count BIGINT NOT NULL,
    rejected_count BIGINT NOT NULL,
    status VARCHAR NOT NULL,
    failure_reason VARCHAR
);

