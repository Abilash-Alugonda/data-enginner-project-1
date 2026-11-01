"""CLI tool to query the local SQL warehouse and display clean, formatted tabular output."""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pandas as pd


def print_table(title: str, df: pd.DataFrame) -> None:
    print("\n" + "=" * 80)
    print(f" {title} ({len(df)} rows)")
    print("=" * 80)
    if df.empty:
        print("(empty)")
    else:
        print(df.to_string(index=False))


def main() -> None:
    db_path = Path("data/warehouse/warehouse.duckdb")
    if not db_path.exists():
        print(
            f"Database file not found at {db_path}. Please execute the pipeline first: .\\run.ps1 run"
        )
        sys.exit(1)

    try:
        con = duckdb.connect(str(db_path), read_only=True)
    except duckdb.IOException as e:
        if "used by another process" in str(e):
            print(
                "Note: The warehouse file is currently opened by another process (e.g. uvicorn server)."
            )
            print(
                "Stop the server with Ctrl+C or view tables in the web dashboard at http://127.0.0.1:8000"
            )
            sys.exit(1)
        raise e

    print("Connected to DuckDB SQL Warehouse.")

    # 1. Existing warehouse tables
    tables_df = con.execute("SHOW TABLES;").df()
    print_table("DATABASE TABLES", tables_df)

    # 2. Recent execution runs
    runs_df = con.execute(
        """
        SELECT run_id, strategy, status, input_count, output_count, rejected_count, start_time
        FROM run_log
        ORDER BY start_time DESC
        LIMIT 5;
        """
    ).df()
    print_table("RECENT EXECUTION RUNS", runs_df)

    # 3. Orders aggregated summary
    summary_df = con.execute(
        """
        SELECT
            order_status,
            COUNT(*) AS order_count,
            ROUND(SUM(order_amount), 2) AS total_revenue,
            ROUND(AVG(order_amount), 2) AS avg_order_value,
            SUM(quantity) AS total_items_sold
        FROM orders
        GROUP BY order_status
        ORDER BY order_count DESC;
        """
    ).df()
    print_table("ORDERS SUMMARY BY STATUS", summary_df)

    # 4. Top sample orders
    orders_df = con.execute(
        """
        SELECT order_id, customer_id, product_id, quantity, order_amount, order_status, order_date
        FROM orders
        LIMIT 10;
        """
    ).df()
    print_table("SAMPLE ORDERS (FIRST 10)", orders_df)

    # 5. Quarantined rejects
    rejects_df = con.execute(
        """
        SELECT reject_id, source_file, source_line, reject_reason
        FROM rejects
        LIMIT 10;
        """
    ).df()
    print_table("SAMPLE REJECTED ROWS (FIRST 10)", rejects_df)

    con.close()


if __name__ == "__main__":
    main()
