"""Unit tests for robust DuckDB connection and session layer."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest

from src.db.connection import (
    BACKOFF_DELAYS,
    SCHEMA_DDL,
    DatabaseLockError,
    DatabaseSessionError,
    db_session,
    get_connection,
    get_db_path,
    init_schema,
)


def test_get_db_path_resolution(tmp_path: Path) -> None:
    # 1. Direct override
    custom_p = tmp_path / "custom.duckdb"
    assert get_db_path(custom_p) == custom_p.resolve()

    # 2. In-memory
    assert get_db_path(":memory:") == ":memory:"

    # 3. Environment variable DB_PATH
    env_p = tmp_path / "env_db.duckdb"
    with patch.dict(os.environ, {"DB_PATH": str(env_p)}):
        assert get_db_path() == env_p.resolve()


def test_init_schema_creates_all_tables(tmp_path: Path) -> None:
    db_file = tmp_path / "schema_test.duckdb"
    init_schema(db_file)
    assert db_file.exists()

    con = duckdb.connect(str(db_file), read_only=True)
    tables = [r[0] for r in con.execute("SHOW TABLES;").fetchall()]
    con.close()

    expected_tables = {
        "customers",
        "products",
        "orders",
        "order_status_history",
        "rejects",
        "run_log",
        "watermarks",
    }
    assert expected_tables.issubset(set(tables))


def test_missing_database_file_on_read_only_creates_schema(tmp_path: Path) -> None:
    # DuckDB normally crashes with IOException when opening non-existent file in read-only mode
    db_file = tmp_path / "fresh_subfolder" / "missing_warehouse.duckdb"
    assert not db_file.exists()

    # get_connection with read_only=True must detect missing file, initialize schema, and succeed
    con = get_connection(read_only=True, db_path=db_file)
    try:
        tables = [r[0] for r in con.execute("SHOW TABLES;").fetchall()]
        assert "orders" in tables
        assert "rejects" in tables
    finally:
        con.close()

    assert db_file.exists()


def test_in_memory_connection_works() -> None:
    con = get_connection(read_only=False, db_path=":memory:")
    try:
        con.execute(SCHEMA_DDL)
        res = con.execute("SELECT count(*) FROM orders;").fetchone()
        assert res is not None
        assert res[0] == 0
    finally:
        con.close()


def test_db_session_commits_on_success(tmp_path: Path) -> None:
    db_file = tmp_path / "commit_test.duckdb"
    init_schema(db_file)

    with db_session(read_only=False, db_path=db_file) as con:
        con.execute(
            """
            INSERT INTO customers (customer_id, name, email, country, created_at)
            VALUES ('CUST-999', 'Test User', 'test@example.com', 'US', '2026-01-01T00:00:00Z');
            """
        )

    # Verify data persisted across connections
    with db_session(read_only=True, db_path=db_file) as con:
        count = con.execute(
            "SELECT count(*) FROM customers WHERE customer_id = 'CUST-999';"
        ).fetchone()[0]
        assert count == 1


def test_db_session_rolls_back_on_error(tmp_path: Path) -> None:
    db_file = tmp_path / "rollback_test.duckdb"
    init_schema(db_file)

    with pytest.raises(DatabaseSessionError) as exc_info:
        with db_session(read_only=False, db_path=db_file) as con:
            con.execute(
                """
                INSERT INTO customers (customer_id, name, email, country, created_at)
                VALUES ('CUST-ERR', 'Rollback User', 'err@example.com', 'US', '2026-01-01T00:00:00Z');
                """
            )
            # Deliberately raise exception to trigger rollback
            raise ValueError("Forced error during write operation")

    # Verify diagnostic message includes db path and original error
    assert str(db_file.resolve()) in str(exc_info.value)
    assert "Forced error during write operation" in str(exc_info.value)

    # Verify customer was NOT committed
    with db_session(read_only=True, db_path=db_file) as con:
        count = con.execute(
            "SELECT count(*) FROM customers WHERE customer_id = 'CUST-ERR';"
        ).fetchone()[0]
        assert count == 0


def test_db_session_always_closes_connection(tmp_path: Path) -> None:
    db_file = tmp_path / "close_test.duckdb"
    init_schema(db_file)

    captured_con = None
    with db_session(read_only=True, db_path=db_file) as con:
        captured_con = con
        con.execute("SELECT 1;").fetchall()

    # Connection should be closed after exiting context manager
    assert captured_con is not None
    with pytest.raises(Exception):
        captured_con.execute("SELECT 1;")


def test_backoff_retry_and_lock_error_reporting(tmp_path: Path) -> None:
    db_file = tmp_path / "lock_test.duckdb"
    init_schema(db_file)

    attempts = 0

    def mock_connect(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        raise duckdb.IOException(
            "The process cannot access the file because it is being used by another process."
        )

    with patch("duckdb.connect", side_effect=mock_connect):
        with patch("time.sleep") as mock_sleep:
            with pytest.raises(DatabaseLockError) as exc_info:
                get_connection(read_only=False, db_path=db_file)

            # Verifies all 5 backoff intervals were attempted
            assert attempts == len(BACKOFF_DELAYS) + 1
            assert mock_sleep.call_count == len(BACKOFF_DELAYS)

            err_str = str(exc_info.value)
            assert "locked by another process after 5 retries" in err_str
            assert "read-write mode" in err_str
            assert "Likely cause" in err_str
            assert "Action" in err_str


def test_backoff_retry_succeeds_after_transient_lock(tmp_path: Path) -> None:
    db_file = tmp_path / "transient_lock_test.duckdb"
    init_schema(db_file)

    real_connect = duckdb.connect
    calls = 0

    def flaky_connect(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls < 3:
            raise duckdb.IOException("IO Error: Cannot open file: used by another process.")
        return real_connect(*args, **kwargs)

    with patch("duckdb.connect", side_effect=flaky_connect):
        with patch("time.sleep"):
            con = get_connection(read_only=True, db_path=db_file)
            try:
                res = con.execute("SELECT 1;").fetchone()
                assert res == (1,)
            finally:
                con.close()

    assert calls == 3
