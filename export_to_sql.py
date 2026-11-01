"""Export all DuckDB warehouse tables to a standard SQL dump file (MySQL/PostgreSQL compatible)."""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb


def export_sql_dump(output_file: str = "warehouse_dump.sql") -> None:
    db_path = Path("data/warehouse/warehouse.duckdb")
    if not db_path.exists():
        print(f"Database file not found at {db_path}. Run the pipeline first: .\\run.ps1 run")
        sys.exit(1)

    try:
        con = duckdb.connect(str(db_path), read_only=True)
    except duckdb.IOException as e:
        if "used by another process" in str(e):
            print(
                "Note: DuckDB is locked by a running server. Please stop the server with Ctrl+C first."
            )
            sys.exit(1)
        raise e

    tables = ["customers", "products", "orders", "order_status_history", "rejects", "run_log"]
    lines: list[str] = [
        "-- Load Strategy Comparison Warehouse SQL Dump",
        "-- Compatible with MySQL, PostgreSQL, and SQLite",
        "",
        "SET FOREIGN_KEY_CHECKS = 0;",
        "",
    ]

    for table in tables:
        try:
            df = con.execute(f"SELECT * FROM {table}").df()
        except Exception:
            continue

        lines.append(f"-- Table: {table} ({len(df)} rows)")
        lines.append(f"DROP TABLE IF EXISTS {table};")

        # Generate DDL
        cols_ddl: list[str] = []
        for col_name, dtype in zip(df.columns, df.dtypes):
            if "int" in str(dtype).lower():
                sql_type = "BIGINT"
            elif "float" in str(dtype).lower():
                sql_type = "DOUBLE"
            else:
                sql_type = "VARCHAR(255)"
            cols_ddl.append(f"    {col_name} {sql_type}")

        lines.append(f"CREATE TABLE {table} (\n" + ",\n".join(cols_ddl) + "\n);")

        # Generate INSERTs
        if not df.empty:
            cols_str = ", ".join(df.columns)
            for _, row in df.iterrows():
                vals: list[str] = []
                for v in row:
                    if pd_isna(v):
                        vals.append("NULL")
                    elif isinstance(v, (int, float)):
                        vals.append(str(v))
                    else:
                        escaped = str(v).replace("'", "''")
                        vals.append(f"'{escaped}'")
                lines.append(f"INSERT INTO {table} ({cols_str}) VALUES ({', '.join(vals)});")
        lines.append("")

    lines.append("SET FOREIGN_KEY_CHECKS = 1;")
    lines.append("")

    with open(output_file, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    con.close()
    print(f"SQL dump successfully written to {output_file} ({len(lines)} lines).")


def pd_isna(value: object) -> bool:
    import pandas as pd

    return bool(pd.isna(value))


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else "warehouse_dump.sql"
    export_sql_dump(out)
