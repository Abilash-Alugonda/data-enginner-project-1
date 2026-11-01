"""Reconciliation engine comparing source counts, target aggregates, and cross-strategy hashes."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from src.storage.db import DatabaseManager


class ReconciliationEngine:
    """Computes conservation metrics, key aggregates, and cross-strategy equivalence."""

    def __init__(self, reports_dir: str | Path = "docs") -> None:
        self.reports_dir = Path(reports_dir)
        self.reports_dir.mkdir(parents=True, exist_ok=True)

    def compute_strategy_metrics(self, db: DatabaseManager) -> dict[str, Any]:
        """Compute key aggregates and dataset hash for orders table in a database."""
        with db.get_connection() as con:
            # 1. Total count
            total_orders = con.execute("SELECT count(*) FROM orders;").fetchone()[0]

            # 2. Sum of order_amount
            sum_amount = con.execute(
                "SELECT COALESCE(round(sum(order_amount), 2), 0.0) FROM orders;"
            ).fetchone()[0]

            # 3. Count by status
            status_rows = con.execute(
                "SELECT order_status, count(*) FROM orders GROUP BY order_status ORDER BY order_status;"
            ).fetchall()
            count_by_status = {row[0]: row[1] for row in status_rows}

            # 4. Count by date
            date_rows = con.execute(
                "SELECT order_date, count(*) FROM orders GROUP BY order_date ORDER BY order_date;"
            ).fetchall()
            count_by_date = {row[0]: row[1] for row in date_rows}

            # 5. Row-level dataset hash
            order_rows = con.execute(
                """
                SELECT
                    order_id,
                    customer_id,
                    product_id,
                    quantity,
                    round(order_amount, 2),
                    order_status,
                    order_date,
                    updated_at
                FROM orders
                ORDER BY order_id;
                """
            ).fetchall()

            hasher = hashlib.sha256()
            for r in order_rows:
                row_str = f"{r[0]}|{r[1]}|{r[2]}|{r[3]}|{r[4]:.2f}|{r[5]}|{r[6]}|{r[7]}"
                hasher.update(row_str.encode("utf-8"))
            dataset_hash = hasher.hexdigest()

            # 6. Status history and rejects counts
            history_count = con.execute("SELECT count(*) FROM order_status_history;").fetchone()[0]
            rejects_count = con.execute("SELECT count(*) FROM rejects;").fetchone()[0]

        return {
            "total_orders": total_orders,
            "sum_order_amount": float(sum_amount),
            "count_by_status": count_by_status,
            "count_by_date": count_by_date,
            "dataset_hash": dataset_hash,
            "status_history_count": history_count,
            "rejects_count": rejects_count,
        }

    def generate_report(
        self,
        source_count: int,
        metrics_by_strategy: dict[str, dict[str, Any]],
        run_id: str,
    ) -> dict[str, Any]:
        """Generate reconciliation JSON and Markdown reports."""
        hashes = {strat: data["dataset_hash"] for strat, data in metrics_by_strategy.items()}
        unique_hashes = set(hashes.values())
        is_equivalent = len(unique_hashes) == 1

        # Check conservation of records for each strategy
        conservation_by_strategy = {}
        for strat, data in metrics_by_strategy.items():
            valid_updates = data["status_history_count"]
            rejects = data["rejects_count"]
            # Sum of valid transitions plus rejects equals total source records
            balanced = (valid_updates + rejects) == source_count
            conservation_by_strategy[strat] = {
                "source_count": source_count,
                "status_history_count": valid_updates,
                "rejects_count": rejects,
                "target_distinct_orders": data["total_orders"],
                "balanced": balanced,
            }

        report = {
            "run_id": run_id,
            "source_count": source_count,
            "cross_strategy_equivalence": is_equivalent,
            "dataset_hashes": hashes,
            "conservation": conservation_by_strategy,
            "metrics": metrics_by_strategy,
        }

        # Save JSON report
        json_path = self.reports_dir / "reconciliation_report.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)

        # Save Markdown report (ensuring NO em dashes anywhere)
        md_path = self.reports_dir / "reconciliation_report.md"
        md_content = self._render_markdown(report)
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(md_content)

        return report

    def _render_markdown(self, report: dict[str, Any]) -> str:
        """Render report as clean Markdown without em dashes or icons."""
        lines = [
            f"# Reconciliation Report: {report['run_id']}",
            "",
            "## Summary",
            "",
            f"- Source Record Count: {report['source_count']}",
            f"- Cross-Strategy Equivalence: {'PASSED' if report['cross_strategy_equivalence'] else 'FAILED'}",
            "",
            "## Record Conservation",
            "",
            "| Strategy | Source Records | Status Transitions | Quarantined Rejects | Distinct Target Orders | Balanced |",
            "| :--- | :--- | :--- | :--- | :--- | :--- |",
        ]

        for strat, cons in report["conservation"].items():
            lines.append(
                f"| {strat} | {cons['source_count']} | {cons['status_history_count']} | {cons['rejects_count']} | {cons['target_distinct_orders']} | {'Yes' if cons['balanced'] else 'No'} |"
            )

        lines.extend(
            [
                "",
                "## Dataset Hash Equivalence",
                "",
                "| Strategy | SHA-256 Dataset Hash |",
                "| :--- | :--- |",
            ]
        )

        for strat, h in report["dataset_hashes"].items():
            lines.append(f"| {strat} | `{h}` |")

        lines.extend(
            [
                "",
                "## Financial and Status Aggregates",
                "",
                "| Strategy | Total Amount | Status Breakdown |",
                "| :--- | :--- | :--- |",
            ]
        )

        for strat, m in report["metrics"].items():
            status_summary = ", ".join(f"{k}: {v}" for k, v in m["count_by_status"].items())
            lines.append(f"| {strat} | ${m['sum_order_amount']:,.2f} | {status_summary} |")

        lines.append("")
        return "\n".join(lines)
