"""Database backend abstraction interface and implementations."""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from src.config import load_config


class DatabaseBackend(ABC):
    """Abstract base class for storage engines."""

    @abstractmethod
    def get_connection(self, read_only: bool = False) -> Any:
        """Acquire a connection."""

    @abstractmethod
    def db_session(self, read_only: bool = False) -> Iterator[Any]:
        """Context manager session."""

    @abstractmethod
    def init_schema(self) -> list[str]:
        """Initialize database schema and apply pending migrations."""

    @abstractmethod
    def healthcheck(self) -> dict[str, Any]:
        """Execute backend health diagnostic checks."""


class DuckDBBackend(DatabaseBackend):
    """Default local embedded DuckDB analytical database backend."""

    def __init__(self, db_path: str | Path | None = None) -> None:
        from src.db.connection import get_db_path

        self.db_path = get_db_path(db_path)

    def get_connection(self, read_only: bool = False) -> Any:
        from src.db.connection import get_connection

        return get_connection(read_only=read_only, db_path=self.db_path)

    @contextmanager
    def db_session(self, read_only: bool = False) -> Iterator[Any]:
        from src.db.connection import db_session

        with db_session(read_only=read_only, db_path=self.db_path) as con:
            yield con

    def init_schema(self) -> list[str]:
        from src.db.init_db import init_db

        return init_db(db_path=self.db_path)

    def healthcheck(self) -> dict[str, Any]:
        from src.db.healthcheck import run_healthcheck

        return run_healthcheck(db_path=self.db_path)


class PostgresBackend(DatabaseBackend):
    """Optional PostgreSQL backend interface.

    Only active when configured in config.yaml under backend: postgres.
    Requires psycopg and DATABASE_URL environment variable.
    """

    def __init__(self) -> None:
        self.conn_url = os.environ.get("DATABASE_URL")
        if not self.conn_url:
            raise ValueError(
                "PostgreSQL backend requires the DATABASE_URL environment variable. "
                "Example: DATABASE_URL='postgresql://user:pass@localhost:5432/warehouse'"
            )

    def get_connection(self, read_only: bool = False) -> Any:
        try:
            import psycopg
        except ImportError as e:
            raise ImportError(
                "psycopg is required for the optional Postgres backend. "
                "Install with: pip install 'psycopg[binary]'"
            ) from e
        return psycopg.connect(self.conn_url)

    @contextmanager
    def db_session(self, read_only: bool = False) -> Iterator[Any]:
        con = self.get_connection(read_only=read_only)
        try:
            yield con
            con.commit()
        except Exception:
            con.rollback()
            raise
        finally:
            con.close()

    def init_schema(self) -> list[str]:
        raise NotImplementedError(
            "PostgreSQL migrations must be applied via external migration runner."
        )

    def healthcheck(self) -> dict[str, Any]:
        return {"backend": "postgres", "status": "pass" if self.conn_url else "fail"}


def get_backend() -> DatabaseBackend:
    """Instantiate configured database backend."""
    cfg = load_config()
    backend_type = cfg.get("database", {}).get("backend", "duckdb").lower()
    if backend_type == "postgres":
        return PostgresBackend()
    return DuckDBBackend()
