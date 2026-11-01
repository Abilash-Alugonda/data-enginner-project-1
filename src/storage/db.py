"""DuckDB warehouse storage manager and schema initializations."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb

from src.db.connection import (
    SCHEMA_DDL,
    db_session,
    get_connection,
    init_schema,
)

__all__ = ["DatabaseManager", "SCHEMA_DDL"]


class DatabaseManager:
    """Manages DuckDB database operations using short-lived connections and backoff sessions."""

    def __init__(self, db_path: str | Path = "data/warehouse/warehouse.duckdb") -> None:
        if str(db_path) == ":memory:":
            self.db_path: Path | str = ":memory:"
        else:
            self.db_path = Path(db_path).resolve()
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.init_schema()

    def get_connection(self, read_only: bool = False) -> duckdb.DuckDBPyConnection:
        """Return a fresh short-lived connection to avoid file lock conflicts."""
        return get_connection(read_only=read_only, db_path=self.db_path)

    @classmethod
    def close_all(cls) -> None:
        """No-op retained for backwards compatibility (connections are not cached globally)."""
        pass

    def init_schema(self) -> None:
        """Create standard tables if they do not exist."""
        init_schema(self.db_path)

    def load_reference_data(self, customers_csv: Path | str, products_csv: Path | str) -> None:
        """Load customers and products reference tables."""
        cust_p = Path(customers_csv)
        prod_p = Path(products_csv)
        if not cust_p.exists() or not prod_p.exists():
            return

        with db_session(read_only=False, db_path=self.db_path) as con:
            con.execute(
                f"INSERT OR REPLACE INTO customers SELECT * FROM read_csv_auto('{cust_p.as_posix()}', header=True)"
            )
            con.execute(
                f"INSERT OR REPLACE INTO products SELECT * FROM read_csv_auto('{prod_p.as_posix()}', header=True)"
            )

    def execute_query(self, sql: str, params: list[Any] | None = None) -> list[tuple[Any, ...]]:
        """Execute a query and fetch all rows using a read-only session."""
        with db_session(read_only=True, db_path=self.db_path) as con:
            cursor = con.execute(sql, params or [])
            return cursor.fetchall()

    def reset_orders(self) -> None:
        """Clear orders, status history, and watermarks for fresh runs."""
        with db_session(read_only=False, db_path=self.db_path) as con:
            con.execute("DELETE FROM orders;")
            con.execute("DELETE FROM order_status_history;")
            con.execute("DELETE FROM watermarks;")
