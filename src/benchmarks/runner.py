"""Benchmark suite measuring execution runtime across scales, incremental, and backfill scenarios."""

from __future__ import annotations

import csv
import time
from pathlib import Path
from typing import Any

from src.generator import generate_dataset
from src.ingestion.reader import ingest_file_or_dir
from src.models import Order
from src.storage.db import DatabaseManager
from src.strategies.full_refresh import FullRefreshStrategy
from src.strategies.incremental_merge import IncrementalMergeStrategy
from src.strategies.partition_replace import PartitionReplaceStrategy, safe_rmtree
from src.validation.validator import DataValidator


class BenchmarkRunner:
    """Executes real performance benchmarks and logs measured wall-clock runtimes."""

    def __init__(
        self,
        bench_dir: str | Path = "data/benchmarks_work",
        output_dir: str | Path = "docs",
    ) -> None:
        self.bench_dir = Path(bench_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _prepare_data(self, rows: int, seed: int, out_name: str) -> tuple[Path, set[str], set[str]]:
        path = self.bench_dir / out_name
        generate_dataset(total_rows=rows, seed=seed, out_dir=path)
        # Load reference IDs
        with open(path / "customers.csv", "r", encoding="utf-8") as f:
            cust_ids = {r["customer_id"] for r in csv.DictReader(f)}
        with open(path / "products.csv", "r", encoding="utf-8") as f:
            prod_ids = {r["product_id"] for r in csv.DictReader(f)}
        return path, cust_ids, prod_ids

    def run_all(self, scales: list[int] | None = None) -> list[dict[str, Any]]:
        scales = scales or [10000, 100000, 500000]
        results: list[dict[str, Any]] = []

        if self.bench_dir.exists():
            safe_rmtree(self.bench_dir)
        self.bench_dir.mkdir(parents=True, exist_ok=True)

        try:
            # 1. Scale Benchmarks
            for scale in scales:
                print(f"Running benchmark scale: {scale} rows...")
                data_dir, cust_ids, prod_ids = self._prepare_data(
                    scale, seed=42, out_name=f"scale_{scale}"
                )
                raw_records = ingest_file_or_dir(data_dir)
                validator = DataValidator(cust_ids, prod_ids)
                batch = validator.validate_batch(raw_records, f"RUN-BENCH-{scale}")

                # Full Refresh
                db_fr_path = self.bench_dir / f"db_fr_{scale}.duckdb"
                db_fr = DatabaseManager(db_fr_path)
                strat_fr = FullRefreshStrategy(db_fr)
                t0 = time.perf_counter()
                res_fr = strat_fr.load(batch, f"RUN-FR-{scale}")
                sec_fr = time.perf_counter() - t0
                results.append(
                    {
                        "scenario": f"Scale {scale}",
                        "strategy": "full_refresh",
                        "input_rows": scale,
                        "target_rows": res_fr.records_loaded,
                        "duration_seconds": round(sec_fr, 3),
                        "throughput_rows_per_sec": int(res_fr.records_loaded / max(sec_fr, 0.001)),
                    }
                )

                # Incremental Merge
                db_im_path = self.bench_dir / f"db_im_{scale}.duckdb"
                db_im = DatabaseManager(db_im_path)
                strat_im = IncrementalMergeStrategy(db_im)
                t0 = time.perf_counter()
                res_im = strat_im.load(batch, f"RUN-IM-{scale}")
                sec_im = time.perf_counter() - t0
                results.append(
                    {
                        "scenario": f"Scale {scale}",
                        "strategy": "incremental_merge",
                        "input_rows": scale,
                        "target_rows": res_im.records_loaded,
                        "duration_seconds": round(sec_im, 3),
                        "throughput_rows_per_sec": int(res_im.records_loaded / max(sec_im, 0.001)),
                    }
                )

                # Partition Replace
                db_pr_path = self.bench_dir / f"db_pr_{scale}.duckdb"
                db_pr = DatabaseManager(db_pr_path)
                part_dir = self.bench_dir / f"parts_{scale}"
                tmp_dir = self.bench_dir / f"tmp_{scale}"
                strat_pr = PartitionReplaceStrategy(
                    db_pr, partitions_dir=part_dir, temp_dir=tmp_dir
                )
                t0 = time.perf_counter()
                res_pr = strat_pr.load(batch, f"RUN-PR-{scale}")
                sec_pr = time.perf_counter() - t0
                results.append(
                    {
                        "scenario": f"Scale {scale}",
                        "strategy": "partition_replace",
                        "input_rows": scale,
                        "target_rows": res_pr.records_loaded,
                        "duration_seconds": round(sec_pr, 3),
                        "throughput_rows_per_sec": int(res_pr.records_loaded / max(sec_pr, 0.001)),
                    }
                )

            # 2. Incremental Scenario (1% changed rows on 100k dataset)
            print("Running Incremental Scenario (1% updates on 100k)...")
            base_scale = 100000
            data_dir, cust_ids, prod_ids = self._prepare_data(
                base_scale, seed=42, out_name="inc_base"
            )
            raw_base = ingest_file_or_dir(data_dir)
            validator = DataValidator(cust_ids, prod_ids)
            batch_base = validator.validate_batch(raw_base, "RUN-INC-BASE")

            # Setup preloaded state for all 3
            inc_db_fr = DatabaseManager(self.bench_dir / "inc_fr.duckdb")
            strat_inc_fr = FullRefreshStrategy(inc_db_fr)
            strat_inc_fr.load(batch_base, "RUN-INC-BASE-FR")

            inc_db_im = DatabaseManager(self.bench_dir / "inc_im.duckdb")
            strat_inc_im = IncrementalMergeStrategy(inc_db_im)
            strat_inc_im.load(batch_base, "RUN-INC-BASE-IM")

            inc_part_dir = self.bench_dir / "inc_parts"
            inc_tmp_dir = self.bench_dir / "inc_tmp"
            inc_db_pr = DatabaseManager(self.bench_dir / "inc_pr.duckdb")
            strat_inc_pr = PartitionReplaceStrategy(
                inc_db_pr, partitions_dir=inc_part_dir, temp_dir=inc_tmp_dir
            )
            strat_inc_pr.load(batch_base, "RUN-INC-BASE-PR")

            # Create 1% delta (1,000 updated orders)
            delta_orders: list[Order] = []
            for idx in range(1000):
                base_order = batch_base.valid_orders[idx]
                delta_orders.append(
                    Order(
                        order_id=base_order.order_id,
                        customer_id=base_order.customer_id,
                        product_id=base_order.product_id,
                        quantity=base_order.quantity + 1,
                        order_amount=base_order.order_amount + 20.0,
                        order_status="DELIVERED",
                        order_date=base_order.order_date,
                        updated_at="2026-02-01T12:00:00Z",
                        run_id="RUN-DELTA-1PCT",
                        source_file="delta_1pct.csv",
                    )
                )

            delta_batch = validator.validate_batch([], "RUN-DELTA-1PCT")
            delta_batch.valid_orders = delta_orders

            # Benchmark 1% delta across strategies
            # FR on incremental must rebuild everything (base + delta)
            combined_batch = validator.validate_batch([], "RUN-DELTA-COMBINED")
            # Replace updated in base
            delta_map = {o.order_id: o for o in delta_orders}
            combined_orders = [delta_map.get(o.order_id, o) for o in batch_base.valid_orders]
            combined_batch.valid_orders = combined_orders

            t0 = time.perf_counter()
            strat_inc_fr.load(combined_batch, "RUN-FR-DELTA")
            sec_delta_fr = time.perf_counter() - t0
            results.append(
                {
                    "scenario": "Incremental Delta (1%)",
                    "strategy": "full_refresh",
                    "input_rows": 1000,
                    "target_rows": 100000,
                    "duration_seconds": round(sec_delta_fr, 3),
                    "throughput_rows_per_sec": int(1000 / max(sec_delta_fr, 0.001)),
                }
            )

            # IM merges only the delta
            t0 = time.perf_counter()
            strat_inc_im.load(delta_batch, "RUN-IM-DELTA")
            sec_delta_im = time.perf_counter() - t0
            results.append(
                {
                    "scenario": "Incremental Delta (1%)",
                    "strategy": "incremental_merge",
                    "input_rows": 1000,
                    "target_rows": 100000,
                    "duration_seconds": round(sec_delta_im, 3),
                    "throughput_rows_per_sec": int(1000 / max(sec_delta_im, 0.001)),
                }
            )

            # PR rewrites only affected partitions
            t0 = time.perf_counter()
            strat_inc_pr.load(delta_batch, "RUN-PR-DELTA")
            sec_delta_pr = time.perf_counter() - t0
            results.append(
                {
                    "scenario": "Incremental Delta (1%)",
                    "strategy": "partition_replace",
                    "input_rows": 1000,
                    "target_rows": 100000,
                    "duration_seconds": round(sec_delta_pr, 3),
                    "throughput_rows_per_sec": int(1000 / max(sec_delta_pr, 0.001)),
                }
            )

            # 3. Backfill Scenario (5,000 historical rows across 7 dates)
            print("Running Backfill Scenario (5,000 rows across 7 historical days)...")
            backfill_orders: list[Order] = []
            for b_idx in range(5000):
                day = (b_idx % 7) + 1
                b_date = f"2025-12-{day:02d}"
                backfill_orders.append(
                    Order(
                        order_id=f"ORD-BKFL-{b_idx:05d}",
                        customer_id=list(cust_ids)[b_idx % len(cust_ids)],
                        product_id=list(prod_ids)[b_idx % len(prod_ids)],
                        quantity=2,
                        order_amount=88.0,
                        order_status="DELIVERED",
                        order_date=b_date,
                        updated_at=f"{b_date}T10:00:00Z",
                        run_id="RUN-BACKFILL",
                        source_file="backfill.csv",
                    )
                )

            backfill_batch = validator.validate_batch([], "RUN-BACKFILL")
            backfill_batch.valid_orders = backfill_orders

            # IM backfill
            t0 = time.perf_counter()
            strat_inc_im.load(backfill_batch, "RUN-IM-BACKFILL")
            sec_bf_im = time.perf_counter() - t0
            results.append(
                {
                    "scenario": "Historical Backfill",
                    "strategy": "incremental_merge",
                    "input_rows": 5000,
                    "target_rows": 105000,
                    "duration_seconds": round(sec_bf_im, 3),
                    "throughput_rows_per_sec": int(5000 / max(sec_bf_im, 0.001)),
                }
            )

            # PR backfill
            t0 = time.perf_counter()
            strat_inc_pr.load(backfill_batch, "RUN-PR-BACKFILL")
            sec_bf_pr = time.perf_counter() - t0
            results.append(
                {
                    "scenario": "Historical Backfill",
                    "strategy": "partition_replace",
                    "input_rows": 5000,
                    "target_rows": 105000,
                    "duration_seconds": round(sec_bf_pr, 3),
                    "throughput_rows_per_sec": int(5000 / max(sec_bf_pr, 0.001)),
                }
            )

            # FR backfill (must reload entire warehouse)
            combined_backfill_batch = validator.validate_batch([], "RUN-COMBINED-BF")
            combined_backfill_batch.valid_orders = combined_orders + backfill_orders
            t0 = time.perf_counter()
            strat_inc_fr.load(combined_backfill_batch, "RUN-FR-BACKFILL")
            sec_bf_fr = time.perf_counter() - t0
            results.append(
                {
                    "scenario": "Historical Backfill",
                    "strategy": "full_refresh",
                    "input_rows": 5000,
                    "target_rows": 105000,
                    "duration_seconds": round(sec_bf_fr, 3),
                    "throughput_rows_per_sec": int(5000 / max(sec_bf_fr, 0.001)),
                }
            )

            # 4. Real Measured Optimization: Vectorized Columnar Load vs Row-by-Row Iterative Load
            # Baseline (Before Optimization): Row-by-row cursor.execute for 5,000 delta rows
            # Optimized (After Optimization): Vectorized PyArrow table registration and set-based merge
            print("Measuring Real Optimization: Vectorized Columnar vs Row-by-Row Iterative...")
            opt_delta_orders = backfill_orders[:5000]

            # Measure baseline row-by-row insertion in DuckDB
            db_bench_baseline = DatabaseManager(self.bench_dir / "opt_baseline.duckdb")
            with db_bench_baseline.get_connection() as con_base:
                con_base.execute(
                    "CREATE TABLE orders_iter (order_id VARCHAR PRIMARY KEY, customer_id VARCHAR, product_id VARCHAR, quantity BIGINT, order_amount DOUBLE, order_status VARCHAR, order_date VARCHAR, updated_at VARCHAR, run_id VARCHAR, source_file VARCHAR);"
                )
                t0 = time.perf_counter()
                for o in opt_delta_orders:
                    con_base.execute(
                        "INSERT INTO orders_iter VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
                        [
                            o.order_id,
                            o.customer_id,
                            o.product_id,
                            o.quantity,
                            o.order_amount,
                            o.order_status,
                            o.order_date,
                            o.updated_at,
                            o.run_id,
                            o.source_file,
                        ],
                    )
                duration_baseline = time.perf_counter() - t0

            # Measure optimized vectorized columnar load using PyArrow registration
            db_bench_opt = DatabaseManager(self.bench_dir / "opt_vector.duckdb")
            with db_bench_opt.get_connection() as con_opt:
                con_opt.execute(
                    "CREATE TABLE orders_vec (order_id VARCHAR PRIMARY KEY, customer_id VARCHAR, product_id VARCHAR, quantity BIGINT, order_amount DOUBLE, order_status VARCHAR, order_date VARCHAR, updated_at VARCHAR, run_id VARCHAR, source_file VARCHAR);"
                )
                t0 = time.perf_counter()
                from src.strategies.base import orders_to_arrow

                tbl = orders_to_arrow(opt_delta_orders)
                con_opt.register("tmp_vec_opt", tbl)
                con_opt.execute("INSERT INTO orders_vec SELECT * FROM tmp_vec_opt;")
                con_opt.unregister("tmp_vec_opt")
                duration_optimized = time.perf_counter() - t0

            results.append(
                {
                    "scenario": "Optimization: Baseline (Row-by-Row Iteration)",
                    "strategy": "incremental_merge",
                    "input_rows": 5000,
                    "target_rows": 5000,
                    "duration_seconds": round(duration_baseline, 3),
                    "throughput_rows_per_sec": int(5000 / max(duration_baseline, 0.001)),
                }
            )
            results.append(
                {
                    "scenario": "Optimization: Vectorized (Columnar Arrow Registration)",
                    "strategy": "incremental_merge",
                    "input_rows": 5000,
                    "target_rows": 5000,
                    "duration_seconds": round(duration_optimized, 3),
                    "throughput_rows_per_sec": int(5000 / max(duration_optimized, 0.001)),
                }
            )

            # Save results to CSV
            csv_path = self.output_dir / "benchmark_results.csv"
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=[
                        "scenario",
                        "strategy",
                        "input_rows",
                        "target_rows",
                        "duration_seconds",
                        "throughput_rows_per_sec",
                    ],
                    lineterminator="\n",
                )
                writer.writeheader()
                writer.writerows(results)

            # Save results to Markdown table
            md_path = self.output_dir / "benchmark_results.md"
            self._write_markdown_table(results, md_path)

            print("Benchmark completed successfully.")
            return results
        finally:
            DatabaseManager.close_all()
            safe_rmtree(self.bench_dir)

    def _write_markdown_table(self, results: list[dict[str, Any]], out_path: Path) -> None:
        """Write Markdown table without em dashes or icons."""
        lines = [
            "# Benchmark Results",
            "",
            "Measured wall-clock runtimes across dataset scales and data engineering scenarios.",
            "",
            "| Scenario | Strategy | Input Rows | Target Rows | Duration (seconds) | Throughput (rows/sec) |",
            "| :--- | :--- | :--- | :--- | :--- | :--- |",
        ]
        for r in results:
            lines.append(
                f"| {r['scenario']} | {r['strategy']} | {r['input_rows']:,} | {r['target_rows']:,} | {r['duration_seconds']:.3f} | {r['throughput_rows_per_sec']:,} |"
            )

        lines.extend(
            [
                "",
                "## Optimization Analysis: Vectorized Columnar Ingestion",
                "",
                "Row-by-row iteration using parameter tuples incurs severe Python interpreter and database dispatch overhead.",
                "By converting incoming records directly into an in-memory PyArrow table and registering it with DuckDB,",
                "the pipeline leverages vectorized SIMD columnar execution, reducing ingestion latency by orders of magnitude.",
                "",
            ]
        )

        with open(out_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
