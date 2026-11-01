"""Schema discovery, migration execution, and integrity validation."""

from __future__ import annotations

import datetime
from pathlib import Path
from typing import Any

import duckdb

from src.db.exceptions import IntegrityError

DEFAULT_SQL_DIR = Path("sql")


def get_migration_files(sql_dir: Path | str = DEFAULT_SQL_DIR) -> list[Path]:
    """Retrieve versioned migration SQL files in sorted order."""
    p = Path(sql_dir).resolve()
    if not p.exists():
        return []
    return sorted(p.glob("*.sql"), key=lambda f: f.name)


def apply_migrations(
    con: duckdb.DuckDBPyConnection,
    sql_dir: Path | str = DEFAULT_SQL_DIR,
) -> list[str]:
    """Apply any pending SQL migrations in numerical order inside transactions.

    Returns the list of version names applied during this execution.
    """
    migration_files = get_migration_files(sql_dir)
    if not migration_files:
        return []

    # Ensure schema_version table exists
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_version (
            version VARCHAR PRIMARY KEY,
            applied_at VARCHAR NOT NULL,
            description VARCHAR NOT NULL
        );
        """
    )

    applied_rows = con.execute("SELECT version FROM schema_version;").fetchall()
    applied_versions = {row[0] for row in applied_rows}

    applied_now: list[str] = []

    for sql_file in migration_files:
        version_name = sql_file.name
        if version_name in applied_versions:
            continue

        sql_content = sql_file.read_text(encoding="utf-8")
        first_line = sql_content.strip().splitlines()[0] if sql_content.strip() else ""
        desc = first_line.lstrip("-").strip() if first_line.startswith("--") else version_name

        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

        # Execute migration and record in schema_version
        con.execute("BEGIN TRANSACTION;")
        try:
            con.execute(sql_content)
            con.execute(
                """
                INSERT INTO schema_version (version, applied_at, description)
                VALUES (?, ?, ?);
                """,
                [version_name, now_iso, desc],
            )
            con.execute("COMMIT;")
            applied_now.append(version_name)
        except Exception:
            try:
                con.execute("ROLLBACK;")
            except Exception:
                pass
            raise

    return applied_now


def get_applied_migrations(con: duckdb.DuckDBPyConnection) -> list[dict[str, str]]:
    """Query currently applied migration versions."""
    tables = [r[0] for r in con.execute("SHOW TABLES;").fetchall()]
    if "schema_version" not in tables:
        return []

    rows = con.execute(
        """
        SELECT version, applied_at, description
        FROM schema_version
        ORDER BY version ASC;
        """
    ).fetchall()
    return [{"version": r[0], "applied_at": r[1], "description": r[2]} for r in rows]


def validate_integrity(
    con: duckdb.DuckDBPyConnection,
    db_path: str | Path = "data/warehouse/warehouse.duckdb",
    strict: bool = False,
) -> dict[str, Any]:
    """Execute referential integrity and uniqueness validation queries.

    Checks:
    1. Foreign key references from orders to customers.
    2. Foreign key references from orders to products.
    3. Primary key uniqueness on target orders table.
    """
    tables = {r[0] for r in con.execute("SHOW TABLES;").fetchall()}
    violations: list[str] = []

    # 1. Uniqueness check on orders
    if "orders" in tables:
        dups = con.execute(
            """
            SELECT order_id, count(*) as cnt
            FROM orders
            GROUP BY order_id
            HAVING count(*) > 1;
            """
        ).fetchall()
        if dups:
            violations.append(
                f"Primary key violation: {len(dups)} duplicate order_ids found in orders table."
            )

    # 2. Foreign key check: customers
    if "orders" in tables and "customers" in tables:
        missing_custs = con.execute(
            """
            SELECT count(DISTINCT customer_id)
            FROM orders
            WHERE customer_id NOT IN (SELECT customer_id FROM customers);
            """
        ).fetchone()[0]
        if missing_custs > 0:
            violations.append(
                f"Foreign key violation: {missing_custs} distinct customer_ids in orders table not found in customers master."
            )

    # 3. Foreign key check: products
    if "orders" in tables and "products" in tables:
        missing_prods = con.execute(
            """
            SELECT count(DISTINCT product_id)
            FROM orders
            WHERE product_id NOT IN (SELECT product_id FROM products);
            """
        ).fetchone()[0]
        if missing_prods > 0:
            violations.append(
                f"Foreign key violation: {missing_prods} distinct product_ids in orders table not found in products master."
            )

    passed = len(violations) == 0
    report = {
        "passed": passed,
        "violations": violations,
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }

    if not passed and strict:
        raise IntegrityError(
            what_failed="Database referential integrity or primary key check failed.",
            db_path=db_path,
            likely_cause="Invalid records were inserted without proper validation or dirty data bypassed quarantine.",
            fix_command="Run 'python -m src.db.init_db --reset --yes' or execute validation quarantine on incoming batches.",
        )

    return report
