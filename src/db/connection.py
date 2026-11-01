"""Robust DuckDB connection and session management layer.

Concurrency Architecture:
DuckDB operates as an embedded analytical database engine. It permits either:
1. Multiple concurrent read-only connection processes, OR
2. Exactly one single writer process at any given moment.

To guarantee zero connection and locking conflicts across Windows, macOS, and Linux:
- Connections are kept short-lived per operation and never cached in global state.
- All API queries and reporting views connect with read_only=True.
- Pipeline batch writes operate sequentially under a single dedicated writer.
- Transient file locks trigger exponential backoff retries (0.2s, 0.4s, 0.8s, 1.6s, 3.2s).
- PRAGMA threads and memory_limit are configured safely from config.yaml.
"""

from __future__ import annotations

import datetime
import json
import logging
import os
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import duckdb

from src.config import load_config
from src.db.exceptions import (
    DatabaseConnectionError,
    DatabaseLockError,
    DatabaseSessionError,
    IntegrityError,
    LockTimeoutError,
    SchemaVersionError,
)

logger = logging.getLogger("src.db.connection")

__all__ = [
    "BACKOFF_DELAYS",
    "SCHEMA_DDL",
    "DatabaseConnectionError",
    "DatabaseLockError",
    "DatabaseSessionError",
    "IntegrityError",
    "LockTimeoutError",
    "SchemaVersionError",
    "db_session",
    "get_connection",
    "get_db_path",
    "init_schema",
]

BACKOFF_DELAYS: list[float] = [0.2, 0.4, 0.8, 1.6, 3.2]

# Backwards compatible DDL string
SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS schema_version (
    version VARCHAR PRIMARY KEY,
    applied_at VARCHAR NOT NULL,
    description VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    email VARCHAR NOT NULL,
    country VARCHAR NOT NULL,
    created_at VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR PRIMARY KEY,
    product_name VARCHAR NOT NULL,
    category VARCHAR NOT NULL,
    unit_price DOUBLE NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR NOT NULL,
    product_id VARCHAR NOT NULL,
    quantity BIGINT NOT NULL,
    order_amount DOUBLE NOT NULL,
    order_status VARCHAR NOT NULL,
    order_date VARCHAR NOT NULL,
    updated_at VARCHAR NOT NULL,
    run_id VARCHAR NOT NULL,
    source_file VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS orders_staging (
    order_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR NOT NULL,
    product_id VARCHAR NOT NULL,
    quantity BIGINT NOT NULL,
    order_amount DOUBLE NOT NULL,
    order_status VARCHAR NOT NULL,
    order_date VARCHAR NOT NULL,
    updated_at VARCHAR NOT NULL,
    run_id VARCHAR NOT NULL,
    source_file VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS orders_full_refresh (
    order_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR NOT NULL,
    product_id VARCHAR NOT NULL,
    quantity BIGINT NOT NULL,
    order_amount DOUBLE NOT NULL,
    order_status VARCHAR NOT NULL,
    order_date VARCHAR NOT NULL,
    updated_at VARCHAR NOT NULL,
    run_id VARCHAR NOT NULL,
    source_file VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS orders_incremental (
    order_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR NOT NULL,
    product_id VARCHAR NOT NULL,
    quantity BIGINT NOT NULL,
    order_amount DOUBLE NOT NULL,
    order_status VARCHAR NOT NULL,
    order_date VARCHAR NOT NULL,
    updated_at VARCHAR NOT NULL,
    run_id VARCHAR NOT NULL,
    source_file VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS orders_partition_index (
    partition_key VARCHAR NOT NULL,
    partition_date VARCHAR,
    file_path VARCHAR NOT NULL,
    row_count BIGINT NOT NULL,
    record_count BIGINT,
    updated_at VARCHAR NOT NULL,
    last_updated_at VARCHAR,
    run_id VARCHAR NOT NULL,
    PRIMARY KEY (partition_key, file_path)
);

CREATE TABLE IF NOT EXISTS order_status_history (
    history_id VARCHAR PRIMARY KEY,
    order_id VARCHAR NOT NULL,
    order_status VARCHAR NOT NULL,
    updated_at VARCHAR NOT NULL,
    run_id VARCHAR NOT NULL,
    source_file VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS rejects (
    reject_id VARCHAR PRIMARY KEY,
    run_id VARCHAR NOT NULL,
    source_file VARCHAR NOT NULL,
    source_line BIGINT NOT NULL,
    reject_reason VARCHAR NOT NULL,
    raw_payload VARCHAR NOT NULL,
    created_at VARCHAR NOT NULL
);

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

CREATE TABLE IF NOT EXISTS watermarks (
    strategy VARCHAR PRIMARY KEY,
    last_watermark VARCHAR NOT NULL,
    updated_at VARCHAR NOT NULL
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
"""


def _log_json(level: int, event: str, payload: dict) -> None:
    """Emit structured JSON log message."""
    record = {
        "event": event,
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        **payload,
    }
    logger.log(level, json.dumps(record))


def get_db_path(override_path: str | Path | None = None) -> Path | str:
    """Resolve database path from CLI flag/argument, DB_PATH env var, or config.yaml."""
    if override_path is not None:
        if str(override_path) == ":memory:":
            return ":memory:"
        return Path(override_path).resolve()

    # Environment variable overrides
    env_path = os.environ.get("DB_PATH") or os.environ.get("DUCKDB_PATH")
    if env_path:
        if env_path == ":memory:":
            return ":memory:"
        return Path(env_path).resolve()

    # Fallback to config.yaml
    cfg = load_config()
    configured = cfg.get("paths", {}).get("duckdb_path", "data/warehouse/warehouse.duckdb")
    if str(configured) == ":memory:":
        return ":memory:"
    return Path(configured).resolve()


def init_schema(db_path: Path | str) -> None:
    """Initialize warehouse schema tables on a database."""
    from src.db.schema import apply_migrations

    if str(db_path) == ":memory:":
        con = duckdb.connect(":memory:")
        try:
            apply_migrations(con)
        finally:
            con.close()
        return

    path_obj = Path(db_path).resolve()
    path_obj.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path_obj), read_only=False)
    try:
        apply_migrations(con)
    finally:
        con.close()


def _apply_pragmas(con: duckdb.DuckDBPyConnection) -> None:
    """Set PRAGMA threads and memory_limit from config with safe defaults."""
    cfg = load_config()
    db_cfg = cfg.get("database", {})
    threads = int(db_cfg.get("threads", 4))
    memory_limit = str(db_cfg.get("memory_limit", "2GB"))

    try:
        con.execute(f"PRAGMA threads={threads};")
        con.execute(f"PRAGMA memory_limit='{memory_limit}';")
    except Exception:
        pass


def get_connection(
    read_only: bool = False,
    db_path: str | Path | None = None,
    run_id: str | None = None,
) -> duckdb.DuckDBPyConnection:
    """Return a short-lived DuckDB connection to the configured database path.

    If the file is locked by another process, retries up to 5 times with exponential backoff:
    (0.2s, 0.4s, 0.8s, 1.6s, 3.2s).
    If the database file is missing on a read_only open, automatically initializes the schema first.
    Configures PRAGMA threads and memory_limit.
    """
    resolved = get_db_path(db_path)
    if str(resolved) == ":memory:":
        con = duckdb.connect(":memory:")
        _apply_pragmas(con)
        _log_json(
            logging.DEBUG,
            "db_connection_open",
            {"db_path": ":memory:", "read_only": read_only, "run_id": run_id},
        )
        return con

    path_obj = Path(resolved).resolve()
    path_obj.parent.mkdir(parents=True, exist_ok=True)

    # If file is missing on read_only open, create schema first via init_db
    if read_only and not path_obj.exists():
        from src.db.init_db import init_db

        init_db(db_path=path_obj)

    # Attempt connection with exponential backoff on file locks
    for attempt, delay in enumerate(BACKOFF_DELAYS):
        try:
            con = duckdb.connect(str(path_obj), read_only=read_only)
            _apply_pragmas(con)
            _log_json(
                logging.DEBUG,
                "db_connection_open",
                {
                    "db_path": str(path_obj),
                    "read_only": read_only,
                    "attempt": attempt,
                    "run_id": run_id,
                },
            )
            return con
        except duckdb.IOException as exc:
            err_msg = str(exc).lower()
            is_locked = (
                "used by another process" in err_msg
                or "cannot open file" in err_msg
                or "lock" in err_msg
            )
            if not is_locked:
                raise DatabaseConnectionError(
                    what_failed="Failed to open DuckDB database file.",
                    db_path=path_obj,
                    likely_cause=str(exc),
                    fix_command="Check database file path permissions or recreate directory.",
                ) from exc

            _log_json(
                logging.WARNING,
                "db_connection_retry",
                {
                    "db_path": str(path_obj),
                    "attempt": attempt + 1,
                    "delay_seconds": delay,
                    "run_id": run_id,
                    "error": str(exc),
                },
            )
            time.sleep(delay)

    # Final attempt after all 5 backoff retries exhausted
    try:
        con = duckdb.connect(str(path_obj), read_only=read_only)
        _apply_pragmas(con)
        _log_json(
            logging.DEBUG,
            "db_connection_open",
            {"db_path": str(path_obj), "read_only": read_only, "run_id": run_id},
        )
        return con
    except duckdb.IOException as exc:
        mode_str = "read-only" if read_only else "read-write"
        raise LockTimeoutError(
            what_failed=f"Database file '{path_obj}' is locked by another process after 5 retries in {mode_str} mode.",
            db_path=path_obj,
            likely_cause="An active pipeline ingestion job, uvicorn server, or external SQL client holds an exclusive file lock.",
            fix_command="Stop the conflicting process, wait for the background write job to finish, or ensure queries use read_only=True.",
        ) from exc


@contextmanager
def db_session(
    read_only: bool = False,
    db_path: str | Path | None = None,
    run_id: str | None = None,
) -> Iterator[duckdb.DuckDBPyConnection]:
    """Context manager for short-lived database operations.

    Always closes the connection in a finally block, commits on success, rolls back
    on exception, and re-raises with a clear diagnostic message including the database path.
    """
    resolved = get_db_path(db_path)
    con = get_connection(read_only=read_only, db_path=resolved, run_id=run_id)
    in_tx = False

    if not read_only and str(resolved) != ":memory:":
        try:
            con.begin()
            in_tx = True
        except Exception:
            in_tx = False

    try:
        yield con
        if in_tx:
            con.commit()
            in_tx = False
    except Exception as exc:
        if in_tx:
            try:
                con.rollback()
            except Exception:
                pass
            in_tx = False
        raise DatabaseSessionError(
            what_failed="Operation within database session encountered an unhandled exception.",
            db_path=resolved,
            likely_cause=str(exc),
            fix_command="Review the SQL statement, referential constraints, or transaction payload.",
        ) from exc
    finally:
        con.close()
        _log_json(
            logging.DEBUG,
            "db_connection_close",
            {"db_path": str(resolved), "run_id": run_id},
        )
