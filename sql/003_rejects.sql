-- Migration 003: Quarantine and traceability reject table
CREATE TABLE IF NOT EXISTS rejects (
    reject_id VARCHAR PRIMARY KEY,
    run_id VARCHAR NOT NULL,
    source_file VARCHAR NOT NULL,
    source_line BIGINT NOT NULL,
    reject_reason VARCHAR NOT NULL,
    raw_payload VARCHAR NOT NULL,
    created_at VARCHAR NOT NULL
);

