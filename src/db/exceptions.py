"""Custom database exceptions with actionable diagnostic guidance."""

from __future__ import annotations

from pathlib import Path


class DatabaseConnectionError(Exception):
    """Base error for database connection issues."""

    def __init__(
        self,
        what_failed: str,
        db_path: str | Path,
        likely_cause: str,
        fix_command: str,
    ) -> None:
        self.what_failed = what_failed
        self.db_path = str(db_path)
        self.likely_cause = likely_cause
        self.fix_command = fix_command
        message = (
            f"[{self.__class__.__name__}] Operation failed: {what_failed}\n"
            f"Database path: {self.db_path}\n"
            f"Likely cause: {likely_cause}\n"
            f"Fix command / Action: {fix_command}"
        )
        super().__init__(message)


class DatabaseSessionError(DatabaseConnectionError):
    """Raised when an operation inside a database session context manager fails."""


class DatabaseLockError(DatabaseConnectionError):
    """Raised when the database file is locked by another process."""


class LockTimeoutError(DatabaseLockError):
    """Raised when connection retries are exhausted due to continuous file locking."""


class SchemaVersionError(DatabaseConnectionError):
    """Raised when database schema migrations are missing, out of order, or corrupt."""


class IntegrityError(DatabaseConnectionError):
    """Raised when referential integrity or primary key constraints are violated."""
