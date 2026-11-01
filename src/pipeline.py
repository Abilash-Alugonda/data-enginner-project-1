"""Pipeline execution engine orchestrating ingestion, validation, loading, observability, and reconciliation."""

from __future__ import annotations

import argparse
import csv
import datetime
from pathlib import Path
from typing import Any

from src.config import load_config
from src.db.healthcheck import assert_healthy
from src.ingestion.reader import ingest_file_or_dir
from src.observability.logger import PipelineLogger
from src.reconciliation.reconciler import ReconciliationEngine
from src.storage.db import DatabaseManager
from src.strategies.base import StrategyExecutionResult
from src.strategies.full_refresh import FullRefreshStrategy
from src.strategies.incremental_merge import IncrementalMergeStrategy
from src.strategies.partition_replace import PartitionReplaceStrategy, safe_rmtree
from src.validation.validator import DataValidator


def run_pipeline(
    strategy: str = "full_refresh",
    input_path: str | Path = "data/sample",
    fail_after: int | None = None,
    config_path: str | Path | None = None,
) -> dict[str, Any]:
    """Execute pipeline for a given strategy and input path."""
    cfg = load_config(config_path)
    db_path = cfg.get("paths", {}).get("duckdb_path", "data/warehouse/warehouse.duckdb")
    partitions_dir = cfg.get("paths", {}).get("partitions_dir", "data/warehouse/partitions/orders")
    temp_dir = (
        cfg.get("strategies", {})
        .get("partition_replace", {})
        .get("temp_dir", "data/warehouse/tmp_partitions")
    )
    reports_dir = cfg.get("paths", {}).get("reports_dir", "docs")
    sample_dir = Path(cfg.get("paths", {}).get("sample_dir", "data/sample"))

    # Initialize DB schema and perform startup health check diagnostics
    db = DatabaseManager(db_path)
    assert_healthy(db_path)

    # Generate unique run ID
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    timestamp_str = now_utc.strftime("%Y%m%d_%H%M%S")
    run_id = f"RUN-{strategy.upper()}-{timestamp_str}"
    start_time_iso = now_utc.isoformat()

    logger = PipelineLogger(db)
    reconciler = ReconciliationEngine(reports_dir)

    # Record initial RUNNING state
    logger.log_run(
        run_id=run_id,
        strategy=strategy,
        start_time=start_time_iso,
        end_time="",
        input_count=0,
        output_count=0,
        rejected_count=0,
        status="RUNNING",
        failure_reason=None,
    )

    final_status = "FAILED"
    failure_reason: str | None = None
    input_count = 0
    output_count = 0
    rejected_count = 0
    status_history_count = 0
    result: StrategyExecutionResult | None = None
    reconciliation_report: dict[str, Any] = {}

    try:
        # Load customers and products reference tables if available
        input_p = Path(input_path)
        input_dir = input_p if input_p.is_dir() else input_p.parent
        cust_file = input_dir / "customers.csv"
        if not cust_file.exists():
            cust_file = sample_dir / "customers.csv"
        prod_file = input_dir / "products.csv"
        if not prod_file.exists():
            prod_file = sample_dir / "products.csv"
        db.load_reference_data(cust_file, prod_file)

        cust_ids: set[str] = set()
        prod_ids: set[str] = set()
        if cust_file.exists():
            with open(cust_file, "r", encoding="utf-8") as f:
                cust_ids = {r["customer_id"] for r in csv.DictReader(f)}
        if prod_file.exists():
            with open(prod_file, "r", encoding="utf-8") as f:
                prod_ids = {r["product_id"] for r in csv.DictReader(f)}

        # 1. Ingestion
        raw_records = ingest_file_or_dir(input_path)

        # 2. Validation and Quarantine
        validator = DataValidator(cust_ids, prod_ids)
        batch = validator.validate_batch(raw_records, run_id)
        input_count = batch.total_raw_count
        rejected_count = len(batch.rejects)
        status_history_count = len(batch.status_history)

        # 3. Strategy Execution
        strategy_map = {
            "full_refresh": lambda: FullRefreshStrategy(db),
            "incremental_merge": lambda: IncrementalMergeStrategy(db),
            "partition_replace": lambda: PartitionReplaceStrategy(
                db, partitions_dir=partitions_dir, temp_dir=temp_dir
            ),
        }

        if strategy not in strategy_map:
            raise ValueError(
                f"Unknown strategy: '{strategy}'. Supported strategies: {list(strategy_map.keys())}"
            )

        executor = strategy_map[strategy]()
        result = executor.load(batch, run_id, fail_after=fail_after)
        output_count = result.records_loaded
        final_status = result.status
        failure_reason = result.error_message

        # 4. Reconciliation (if successful)
        metrics_by_strat: dict[str, Any] = {}
        if result.status == "SUCCESS":
            strat_metrics = reconciler.compute_strategy_metrics(db)
            metrics_by_strat[strategy] = strat_metrics
            reconciliation_report = reconciler.generate_report(
                source_count=batch.total_raw_count,
                metrics_by_strategy=metrics_by_strat,
                run_id=run_id,
            )
        else:
            reconciliation_report = {}

    except Exception as exc:
        final_status = "FAILED"
        failure_reason = str(exc)
        raise
    finally:
        end_time_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        logger.log_run(
            run_id=run_id,
            strategy=strategy,
            start_time=start_time_iso,
            end_time=end_time_iso,
            input_count=input_count,
            output_count=output_count,
            rejected_count=rejected_count,
            status=final_status,
            failure_reason=failure_reason,
        )

    duration = result.duration_seconds if result else 0.0
    return {
        "run_id": run_id,
        "strategy": strategy,
        "status": final_status,
        "duration_seconds": duration,
        "input_count": input_count,
        "valid_orders_count": output_count,
        "status_history_count": status_history_count,
        "rejected_count": rejected_count,
        "error_message": failure_reason,
        "reconciliation": reconciliation_report,
    }


def run_all_strategies_and_reconcile(
    input_path: str | Path = "data/sample",
    config_path: str | Path | None = None,
) -> dict[str, Any]:
    """Execute all three strategies sequentially and run complete cross-strategy reconciliation."""
    cfg = load_config(config_path)
    reports_dir = cfg.get("paths", {}).get("reports_dir", "docs")
    sample_dir = Path(cfg.get("paths", {}).get("sample_dir", "data/sample"))
    reconciler = ReconciliationEngine(reports_dir)

    raw_records = ingest_file_or_dir(input_path)
    input_p = Path(input_path)
    input_dir = input_p if input_p.is_dir() else input_p.parent
    cust_file = input_dir / "customers.csv"
    if not cust_file.exists():
        cust_file = sample_dir / "customers.csv"
    prod_file = input_dir / "products.csv"
    if not prod_file.exists():
        prod_file = sample_dir / "products.csv"

    cust_ids: set[str] = set()
    prod_ids: set[str] = set()
    if cust_file.exists():
        with open(cust_file, "r", encoding="utf-8") as f:
            cust_ids = {r["customer_id"] for r in csv.DictReader(f)}
    if prod_file.exists():
        with open(prod_file, "r", encoding="utf-8") as f:
            prod_ids = {r["product_id"] for r in csv.DictReader(f)}

    validator = DataValidator(cust_ids, prod_ids)
    batch = validator.validate_batch(raw_records, "RECON-ALL-RUN")

    # Run each strategy against an isolated DuckDB instance to collect metrics
    metrics_by_strat: dict[str, Any] = {}
    test_run_dir = Path("data/warehouse/reconciliation_runs")
    test_run_dir.mkdir(parents=True, exist_ok=True)

    try:
        # Full Refresh
        db_fr = DatabaseManager(test_run_dir / "fr.duckdb")
        strat_fr = FullRefreshStrategy(db_fr)
        strat_fr.load(batch, "RECON-FR")
        metrics_by_strat["full_refresh"] = reconciler.compute_strategy_metrics(db_fr)

        # Incremental Merge
        db_im = DatabaseManager(test_run_dir / "im.duckdb")
        strat_im = IncrementalMergeStrategy(db_im)
        strat_im.load(batch, "RECON-IM")
        metrics_by_strat["incremental_merge"] = reconciler.compute_strategy_metrics(db_im)

        # Partition Replace
        db_pr = DatabaseManager(test_run_dir / "pr.duckdb")
        strat_pr = PartitionReplaceStrategy(
            db_pr,
            partitions_dir=test_run_dir / "partitions",
            temp_dir=test_run_dir / "tmp",
        )
        strat_pr.load(batch, "RECON-PR")
        metrics_by_strat["partition_replace"] = reconciler.compute_strategy_metrics(db_pr)

        report = reconciler.generate_report(
            source_count=batch.total_raw_count,
            metrics_by_strategy=metrics_by_strat,
            run_id="RECON-THREE-STRATEGIES",
        )
        return report
    finally:
        DatabaseManager.close_all()
        safe_rmtree(test_run_dir)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Execute load strategies and cross-strategy reconciliation"
    )
    parser.add_argument(
        "--strategy",
        type=str,
        default="all",
        choices=["all", "full_refresh", "incremental_merge", "partition_replace"],
        help="Strategy to execute, or 'all' for cross-strategy reconciliation (default: all)",
    )
    parser.add_argument(
        "--preset",
        type=str,
        choices=["sample", "large"],
        default=None,
        help="Dataset preset: 'sample' (data/sample) or 'large' (data/large)",
    )
    parser.add_argument(
        "--input-path",
        type=str,
        default=None,
        help="Custom path to input dataset directory or file (overrides preset)",
    )
    parser.add_argument(
        "--fail-after",
        type=int,
        default=None,
        help="Simulate fault injection failure after N records",
    )
    args = parser.parse_args()

    # Determine input directory from preset or explicit input-path
    if args.input_path:
        target_input = args.input_path
    elif args.preset == "large":
        target_input = "data/large"
    elif args.preset == "sample":
        target_input = "data/sample"
    else:
        target_input = "data/sample"

    if args.strategy == "all":
        print(f"Running cross-strategy reconciliation on dataset '{target_input}'...")
        report = run_all_strategies_and_reconcile(input_path=target_input)
        print(
            f"Reconciliation complete: cross_strategy_equivalence = {report.get('cross_strategy_equivalence')}"
        )
        print(f"Dataset hashes: {report.get('dataset_hashes')}")
    else:
        print(f"Running strategy '{args.strategy}' on dataset '{target_input}'...")
        res = run_pipeline(
            strategy=args.strategy, input_path=target_input, fail_after=args.fail_after
        )
        print(f"Run complete: status = {res.get('status')}")
        print(f"  Valid orders loaded: {res.get('valid_orders_count')}")
        print(f"  Quarantined rejects: {res.get('rejected_count')}")
        print(f"  Duration: {res.get('duration_seconds'):.3f}s")


if __name__ == "__main__":
    main()
