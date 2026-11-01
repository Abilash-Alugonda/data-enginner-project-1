"""CLI and programmatic database initialization tool."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from src.db.connection import get_connection, get_db_path
from src.db.schema import apply_migrations, get_applied_migrations


def init_db(
    db_path: str | Path | None = None,
    reset: bool = False,
    sql_dir: str | Path = "sql",
) -> list[str]:
    """Initialize database schema, optionally resetting all tables first."""
    resolved_path = get_db_path(db_path)
    is_memory = str(resolved_path) == ":memory:"

    if not is_memory:
        Path(resolved_path).parent.mkdir(parents=True, exist_ok=True)

    con = get_connection(read_only=False, db_path=resolved_path)
    try:
        if reset:
            # Drop all user tables
            tables = [r[0] for r in con.execute("SHOW TABLES;").fetchall()]
            for tbl in tables:
                con.execute(f"DROP TABLE IF EXISTS {tbl};")

        applied = apply_migrations(con, sql_dir=sql_dir)
        return applied
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Initialize or reset DuckDB warehouse schema.")
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Drop and recreate all tables in the warehouse.",
    )
    parser.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="Confirm reset without interactive prompt.",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=None,
        help="Path to DuckDB database file (overrides config and environment).",
    )
    parser.add_argument(
        "--sql-dir",
        type=str,
        default="sql",
        help="Directory containing versioned SQL migration files.",
    )

    args = parser.parse_args()
    target_path = get_db_path(args.db_path)

    if args.reset and not args.yes:
        # Prompt for confirmation if stdin is interactive
        if sys.stdin.isatty():
            confirm = input(
                f"Are you sure you want to drop and reset all tables at '{target_path}'? [y/N]: "
            )
            if confirm.strip().lower() not in ("y", "yes"):
                print("Reset cancelled.")
                return
        else:
            print("Error: --reset requires --yes in non-interactive environments.", file=sys.stderr)
            sys.exit(1)

    print(f"Initializing database at: {target_path}")
    applied = init_db(db_path=target_path, reset=args.reset, sql_dir=args.sql_dir)

    if args.reset:
        print(f"Database reset complete. Applied {len(applied)} migrations.")
        for m in applied:
            print(f"  - {m}")
    elif applied:
        print(f"Applied {len(applied)} new migrations:")
        for m in applied:
            print(f"  - {m}")
    else:
        con = get_connection(read_only=True, db_path=target_path)
        try:
            curr = get_applied_migrations(con)
            print(f"Database is already up to date ({len(curr)} migrations applied).")
        finally:
            con.close()


if __name__ == "__main__":
    main()
