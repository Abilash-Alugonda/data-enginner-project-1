"""Startup and runtime health check diagnostics."""

from __future__ import annotations

import datetime
import shutil
import sys
from pathlib import Path
from typing import Any

import duckdb
import pyarrow

from src.config import load_config
from src.db.connection import get_connection, get_db_path
from src.db.exceptions import DatabaseConnectionError
from src.db.schema import get_applied_migrations, get_migration_files


def run_healthcheck(db_path: str | Path | None = None) -> dict[str, Any]:
    """Execute startup health check diagnostics and return structured JSON report.

    Checks:
    1. Connection acquisition
    2. SQL execution ping (SELECT 1)
    3. Schema version currency against sql/*.sql migration files
    4. Write permissions via temporary table creation
    5. Free disk space against threshold
    6. Runtime library versions (Python, DuckDB, PyArrow)
    """
    resolved = get_db_path(db_path)
    is_memory = str(resolved) == ":memory:"
    cfg = load_config()
    db_cfg = cfg.get("database", {})
    threshold_mb = int(db_cfg.get("min_free_disk_mb", 500))

    checks: dict[str, dict[str, Any]] = {}
    fixes: list[str] = []

    # 1. Connection check
    con: duckdb.DuckDBPyConnection | None = None
    try:
        con = get_connection(read_only=False, db_path=resolved)
        checks["connection"] = {"status": "pass", "details": "Successfully connected to DuckDB."}
    except Exception as exc:
        checks["connection"] = {"status": "fail", "error": str(exc)}
        fixes.append(
            f"Unable to connect to database at '{resolved}'. Ensure no conflicting write process holds an exclusive lock."
        )

    # 2. Ping check
    if con:
        try:
            res = con.execute("SELECT 1;").fetchone()
            if res == (1,):
                checks["ping"] = {"status": "pass", "details": "SELECT 1 succeeded."}
            else:
                checks["ping"] = {"status": "fail", "details": f"Unexpected ping result: {res}"}
                fixes.append(
                    "Database query engine returned invalid ping response. Reinitialize database."
                )
        except Exception as exc:
            checks["ping"] = {"status": "fail", "error": str(exc)}
            fixes.append(
                "Database query failed. File may be corrupted. Run 'python -m src.db.init_db --reset --yes'."
            )

    # 3. Schema version currency check
    if con:
        try:
            applied = {m["version"] for m in get_applied_migrations(con)}
            disk_files = {f.name for f in get_migration_files()}
            missing = sorted(disk_files - applied)
            if not missing:
                checks["schema_version"] = {
                    "status": "pass",
                    "applied_count": len(applied),
                    "pending_count": 0,
                }
            else:
                checks["schema_version"] = {
                    "status": "fail",
                    "applied_count": len(applied),
                    "pending_migrations": missing,
                }
                fixes.append(
                    f"Missing {len(missing)} database migration(s): {', '.join(missing)}. "
                    "Run 'python -m src.db.init_db' to apply them."
                )
        except Exception as exc:
            checks["schema_version"] = {"status": "fail", "error": str(exc)}
            fixes.append("Could not inspect schema_version table. Run 'python -m src.db.init_db'.")

    # 4. Write permission check
    if con:
        try:
            con.execute("CREATE TEMPORARY TABLE _healthcheck_write_test (id INT);")
            con.execute("INSERT INTO _healthcheck_write_test VALUES (1);")
            con.execute("DROP TABLE _healthcheck_write_test;")
            checks["write_permission"] = {
                "status": "pass",
                "details": "Temporary table write verified.",
            }
        except Exception as exc:
            checks["write_permission"] = {"status": "fail", "error": str(exc)}
            fixes.append(
                f"Write permission denied on database at '{resolved}'. Check filesystem permissions."
            )

    # Always close test connection
    if con:
        try:
            con.close()
        except Exception:
            pass

    # 5. Free disk space check
    if not is_memory:
        try:
            target_dir = Path(resolved).parent
            target_dir.mkdir(parents=True, exist_ok=True)
            usage = shutil.disk_usage(str(target_dir))
            free_mb = int(usage.free / (1024 * 1024))
            if free_mb >= threshold_mb:
                checks["disk_space"] = {
                    "status": "pass",
                    "free_mb": free_mb,
                    "threshold_mb": threshold_mb,
                }
            else:
                checks["disk_space"] = {
                    "status": "fail",
                    "free_mb": free_mb,
                    "threshold_mb": threshold_mb,
                }
                fixes.append(
                    f"Low disk space: {free_mb} MB available, but {threshold_mb} MB required. "
                    "Free up space on the volume hosting data/warehouse."
                )
        except Exception as exc:
            checks["disk_space"] = {"status": "fail", "error": str(exc)}
            fixes.append(
                "Could not verify free disk space. Ensure warehouse folder path is accessible."
            )
    else:
        checks["disk_space"] = {"status": "pass", "details": "In-memory database (bypassed)."}

    # 6. Library version check
    py_ok = sys.version_info >= (3, 11)
    duckdb_ver = duckdb.__version__
    pyarrow_ver = pyarrow.__version__

    if py_ok:
        checks["versions"] = {
            "status": "pass",
            "python": sys.version.split()[0],
            "duckdb": duckdb_ver,
            "pyarrow": pyarrow_ver,
        }
    else:
        checks["versions"] = {
            "status": "fail",
            "python": sys.version.split()[0],
            "required": ">=3.11",
        }
        fixes.append(f"Python {sys.version.split()[0]} is unsupported. Upgrade to Python 3.11+.")

    overall_status = "pass" if all(c.get("status") == "pass" for c in checks.values()) else "fail"

    return {
        "status": overall_status,
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "database": {
            "backend": "duckdb",
            "path": str(resolved),
        },
        "checks": checks,
        "fixes": fixes,
    }


def assert_healthy(db_path: str | Path | None = None) -> dict[str, Any]:
    """Execute health checks and raise DatabaseConnectionError if unhealthy."""
    report = run_healthcheck(db_path)
    if report["status"] != "pass":
        raise DatabaseConnectionError(
            what_failed="Database startup health check failed.",
            db_path=report["database"]["path"],
            likely_cause="One or more system pre-flight checks failed.",
            fix_command="\n".join(report["fixes"]),
        )
    return report


if __name__ == "__main__":
    import json

    res = run_healthcheck()
    print(json.dumps(res, indent=2))
    if res["status"] != "pass":
        sys.exit(1)
