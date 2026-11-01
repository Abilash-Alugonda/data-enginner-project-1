"""Comprehensive test suite for DuckDB database layer.

Covers:
1. Connection open and close
2. Context manager rollback on error
3. Read-only connection blocks write queries
4. Missing parent directories are created automatically
5. Missing database file is initialized on read_only open
6. Lock contention triggers exponential backoff retries and clear diagnostic LockTimeoutError
7. init_db is idempotent when run repeatedly
8. Migrations apply only missing versions
9. Healthcheck passes on good setup and fails with actionable fix on bad setup
10. Transaction rollback under fault injection and recovery on retry
11. Concurrent API read sessions execute cleanly while pipeline is idle
12. Windows-style paths with spaces and backslashes resolve correctly
13. Referential integrity validation detects violations and enforces constraints
"""

from __future__ import annotations

import concurrent.futures
import csv
from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest

from src.db.connection import (
    BACKOFF_DELAYS,
    DatabaseConnectionError,
    DatabaseSessionError,
    LockTimeoutError,
    db_session,
    get_connection,
)
from src.db.exceptions import IntegrityError
from src.db.healthcheck import assert_healthy, run_healthcheck
from src.db.init_db import init_db
from src.db.schema import apply_migrations, get_applied_migrations, validate_integrity
from src.generator import generate_dataset
from src.ingestion.reader import ingest_file_or_dir
from src.storage.db import DatabaseManager
from src.strategies.full_refresh import FullRefreshStrategy
from src.validation.validator import DataValidator


def test_connection_open_and_close(tmp_path: Path) -> None:
    """Verify short-lived DuckDB connection opens, queries, and closes cleanly."""
    db_file = tmp_path / "conn_test.duckdb"
    con = get_connection(read_only=False, db_path=db_file)
    try:
        res = con.execute("SELECT 42 AS result;").fetchone()
        assert res is not None
        assert res[0] == 42
    finally:
        con.close()

    # Querying after close must raise an exception
    with pytest.raises(Exception):
        con.execute("SELECT 1;")


def test_context_manager_rollback_on_error(tmp_path: Path) -> None:
    """Verify db_session rolls back uncommitted changes on exception and includes diagnostics."""
    db_file = tmp_path / "rollback_test.duckdb"
    init_db(db_file)

    with pytest.raises(DatabaseSessionError) as exc_info:
        with db_session(read_only=False, db_path=db_file) as con:
            con.execute(
                """
                INSERT INTO customers (customer_id, name, email, country, created_at)
                VALUES ('CUST-FAIL', 'Rollback Candidate', 'rb@test.com', 'US', '2026-01-01T00:00:00Z');
                """
            )
            raise RuntimeError("Simulated failure inside transaction block")

    # Diagnostic verification
    error_message = str(exc_info.value)
    assert str(db_file.resolve()) in error_message
    assert "Simulated failure inside transaction block" in error_message

    # Verify customer was rolled back
    with db_session(read_only=True, db_path=db_file) as con:
        count = con.execute(
            "SELECT count(*) FROM customers WHERE customer_id = 'CUST-FAIL';"
        ).fetchone()[0]
        assert count == 0


def test_read_only_blocks_writes(tmp_path: Path) -> None:
    """Verify connections opened with read_only=True forbid DDL and write mutations."""
    db_file = tmp_path / "readonly_test.duckdb"
    init_db(db_file)

    con = get_connection(read_only=True, db_path=db_file)
    try:
        with pytest.raises(Exception) as exc_info:
            con.execute("CREATE TABLE unauthorized_tbl (id INT);")
        # DuckDB rejects write queries in read-only mode
        err = str(exc_info.value).lower()
        assert "read-only" in err or "cannot" in err or "permission" in err
    finally:
        con.close()


def test_missing_folder_is_created(tmp_path: Path) -> None:
    """Verify get_connection automatically creates deeply nested directories."""
    deep_path = tmp_path / "level1" / "level2" / "level3" / "auto_created.duckdb"
    assert not deep_path.parent.exists()

    con = get_connection(read_only=False, db_path=deep_path)
    try:
        con.execute("SELECT 1;")
    finally:
        con.close()

    assert deep_path.parent.exists()
    assert deep_path.exists()


def test_missing_file_is_initialized_on_read_only(tmp_path: Path) -> None:
    """Verify opening a non-existent database file with read_only=True initializes schema instead of crashing."""
    fresh_file = tmp_path / "fresh_warehouse.duckdb"
    assert not fresh_file.exists()

    con = get_connection(read_only=True, db_path=fresh_file)
    try:
        tables = [r[0] for r in con.execute("SHOW TABLES;").fetchall()]
        assert "orders" in tables
        assert "schema_version" in tables
        assert "run_log" in tables
    finally:
        con.close()

    assert fresh_file.exists()


def test_lock_contention_triggers_retry_then_clear_error(tmp_path: Path) -> None:
    """Verify continuous file locking attempts 5 exponential backoff retries and raises LockTimeoutError."""
    db_file = tmp_path / "lock_test.duckdb"
    init_db(db_file)

    call_count = 0

    def mock_connect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        raise duckdb.IOException(
            "The process cannot access the file because it is being used by another process."
        )

    with patch("duckdb.connect", side_effect=mock_connect):
        with patch("time.sleep") as mock_sleep:
            with pytest.raises(LockTimeoutError) as exc_info:
                get_connection(read_only=False, db_path=db_file)

            # Initial attempt + 5 retries = 6 calls
            assert call_count == len(BACKOFF_DELAYS) + 1
            assert mock_sleep.call_count == len(BACKOFF_DELAYS)

            err = str(exc_info.value)
            assert "locked by another process after 5 retries" in err
            assert "Likely cause" in err
            assert "Fix command" in err
            assert str(db_file.resolve()) in err


def test_init_db_idempotency(tmp_path: Path) -> None:
    """Verify running init_db multiple times is idempotent and safe."""
    db_file = tmp_path / "idempotent.duckdb"

    # First run applies migrations
    first_applied = init_db(db_file)
    assert len(first_applied) == 4
    assert "001_core_tables.sql" in first_applied

    # Second run detects all migrations are already present
    second_applied = init_db(db_file)
    assert len(second_applied) == 0

    # Ensure schema_version table records exactly 4 entries
    con = duckdb.connect(str(db_file), read_only=True)
    try:
        count = con.execute("SELECT count(*) FROM schema_version;").fetchone()[0]
        assert count == 4
    finally:
        con.close()


def test_migration_applies_only_missing_versions(tmp_path: Path) -> None:
    """Verify migration runner applies only unapplied versions."""
    db_file = tmp_path / "partial_migrations.duckdb"
    init_db(db_file)

    # Artificially remove migration 004 from schema_version
    with duckdb.connect(str(db_file)) as con:
        con.execute("DELETE FROM schema_version WHERE version = '004_history.sql';")
        applied_before = [m["version"] for m in get_applied_migrations(con)]
        assert "004_history.sql" not in applied_before

        applied_now = apply_migrations(con)
        assert applied_now == ["004_history.sql"]

        applied_after = [m["version"] for m in get_applied_migrations(con)]
        assert "004_history.sql" in applied_after


def test_healthcheck_good_and_bad_setup(tmp_path: Path) -> None:
    """Verify healthcheck passes on valid warehouse and flags issues with actionable messages."""
    good_db = tmp_path / "healthy.duckdb"
    init_db(good_db)

    # 1. Healthy database verification
    report_good = run_healthcheck(good_db)
    assert report_good["status"] == "pass"
    assert len(report_good["fixes"]) == 0
    assert report_good["checks"]["connection"]["status"] == "pass"
    assert report_good["checks"]["ping"]["status"] == "pass"
    assert report_good["checks"]["schema_version"]["status"] == "pass"
    assert report_good["checks"]["write_permission"]["status"] == "pass"
    assert report_good["checks"]["disk_space"]["status"] == "pass"
    assert report_good["checks"]["versions"]["status"] == "pass"

    # assert_healthy should not raise on healthy DB
    assert_healthy(good_db)

    # 2. Out-of-date schema verification
    bad_db = tmp_path / "unmigrated.duckdb"
    con = duckdb.connect(str(bad_db))
    con.execute("CREATE TABLE dummy (id INT);")
    con.close()

    report_bad = run_healthcheck(bad_db)
    assert report_bad["status"] == "fail"
    assert report_bad["checks"]["schema_version"]["status"] == "fail"
    assert any("python -m src.db.init_db" in f for f in report_bad["fixes"])

    # assert_healthy must raise DatabaseConnectionError on failing health check
    with pytest.raises(DatabaseConnectionError) as exc_info:
        assert_healthy(bad_db)
    assert "Database startup health check failed." in str(exc_info.value)


def test_transaction_rollback_under_fault_injection(tmp_path: Path) -> None:
    """Verify strategy aborts and rolls back changes when fault injection triggers."""
    db_file = tmp_path / "fault_test.duckdb"
    db_mgr = DatabaseManager(db_file)

    # Prepare starter dataset
    data_dir = tmp_path / "sample_data"
    generate_dataset(total_rows=30, seed=123, out_dir=data_dir)
    records = ingest_file_or_dir(data_dir)

    with open(data_dir / "customers.csv", "r", encoding="utf-8") as f:
        cust_ids = {r["customer_id"] for r in csv.DictReader(f)}
    with open(data_dir / "products.csv", "r", encoding="utf-8") as f:
        prod_ids = {r["product_id"] for r in csv.DictReader(f)}

    validator = DataValidator(cust_ids, prod_ids)
    batch = validator.validate_batch(records, "FAULT-RUN-1")
    assert len(batch.valid_orders) > 0

    # 1. Fault injection run
    strategy = FullRefreshStrategy(db_mgr)
    result_fault = strategy.load(batch, "FAULT-RUN-1", fail_after=1)
    assert result_fault.status == "FAILED"
    assert result_fault.error_message is not None
    assert "Fault injected" in result_fault.error_message

    # Target table should have 0 records
    with db_session(read_only=True, db_path=db_file) as con:
        orders_cnt = con.execute("SELECT count(*) FROM orders;").fetchone()[0]
        assert orders_cnt == 0

    # 2. Retry run without fault injection reaches correct state
    result_retry = strategy.load(batch, "RETRY-RUN-2", fail_after=None)
    assert result_retry.status == "SUCCESS"
    assert result_retry.records_loaded > 0

    with db_session(read_only=True, db_path=db_file) as con:
        orders_cnt = con.execute("SELECT count(*) FROM orders;").fetchone()[0]
        assert orders_cnt == result_retry.records_loaded


def test_concurrent_api_reads_while_pipeline_idle(tmp_path: Path) -> None:
    """Verify multiple concurrent read_only sessions execute without conflict."""
    db_file = tmp_path / "concurrent_reads.duckdb"
    init_db(db_file)

    with db_session(read_only=False, db_path=db_file) as con:
        con.execute(
            """
            INSERT INTO customers (customer_id, name, email, country, created_at)
            VALUES ('CUST-CONC', 'Concurrent User', 'conc@test.com', 'US', '2026-01-01T00:00:00Z');
            """
        )

    def worker_read(worker_id: int) -> int:
        with db_session(read_only=True, db_path=db_file) as con:
            res = con.execute(
                "SELECT count(*) FROM customers WHERE customer_id = 'CUST-CONC';"
            ).fetchone()[0]
            return int(res)

    worker_count = 10
    with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = [executor.submit(worker_read, i) for i in range(worker_count)]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]

    assert len(results) == worker_count
    assert all(cnt == 1 for cnt in results)


def test_windows_style_path_handling(tmp_path: Path) -> None:
    """Verify Windows paths with spaces, backslashes, and pathlib objects work without errors."""
    win_dir = tmp_path / "Space Folder Name" / "Sub Dir"
    win_file = win_dir / "warehouse test.duckdb"

    # Use resolved Windows-style string representation
    win_path_str = str(win_file.resolve())

    # Initialize and connect
    applied = init_db(win_path_str)
    assert len(applied) == 4
    assert win_file.exists()

    with db_session(read_only=False, db_path=win_path_str) as con:
        con.execute(
            "INSERT INTO watermarks VALUES ('test_strat', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');"
        )

    with db_session(read_only=True, db_path=win_file) as con:
        row = con.execute(
            "SELECT strategy FROM watermarks WHERE strategy = 'test_strat';"
        ).fetchone()
        assert row is not None
        assert row[0] == "test_strat"


def test_referential_integrity_validation(tmp_path: Path) -> None:
    """Verify validate_integrity detects violations and enforces strict constraints."""
    db_file = tmp_path / "integrity_test.duckdb"
    init_db(db_file)

    with duckdb.connect(str(db_file)) as con:
        # Insert customer and product
        con.execute(
            """
            INSERT INTO customers VALUES ('CUST-1', 'Alice', 'a@a.com', 'US', '2026-01-01T00:00:00Z');
            INSERT INTO products VALUES ('PROD-1', 'Widget', 'Cat', 10.0);
            """
        )

        # Clean state check
        report_clean = validate_integrity(con, db_path=db_file)
        assert report_clean["passed"] is True
        assert len(report_clean["violations"]) == 0

        # Insert order with orphan customer_id and orphan product_id
        con.execute(
            """
            INSERT INTO orders VALUES (
                'ORD-ORPHAN', 'CUST-MISSING', 'PROD-MISSING', 1, 10.0,
                'PENDING', '2026-01-01', '2026-01-01T00:00:00Z', 'RUN-1', 'file.csv'
            );
            """
        )

        report_dirty = validate_integrity(con, db_path=db_file)
        assert report_dirty["passed"] is False
        assert len(report_dirty["violations"]) >= 2
        assert any(
            "customer_ids in orders table not found" in v for v in report_dirty["violations"]
        )
        assert any("product_ids in orders table not found" in v for v in report_dirty["violations"])

        # Strict mode must raise IntegrityError
        with pytest.raises(IntegrityError) as exc_info:
            validate_integrity(con, db_path=db_file, strict=True)
        assert "Database referential integrity or primary key check failed." in str(exc_info.value)
