"""Tests verifying mathematical equivalence and record conservation across all three strategies."""

from __future__ import annotations

import json
from pathlib import Path

from src.reconciliation.reconciler import ReconciliationEngine
from src.storage.db import DatabaseManager
from src.strategies.full_refresh import FullRefreshStrategy
from src.strategies.incremental_merge import IncrementalMergeStrategy
from src.strategies.partition_replace import PartitionReplaceStrategy
from src.validation.validator import ValidatedBatch


def test_cross_strategy_equivalence(sample_batch: ValidatedBatch, tmp_path: Path) -> None:
    # 1. Full Refresh
    db_fr = DatabaseManager(tmp_path / "fr.duckdb")
    strat_fr = FullRefreshStrategy(db_fr)
    strat_fr.load(sample_batch, "RUN-FR")

    # 2. Incremental Merge
    db_im = DatabaseManager(tmp_path / "im.duckdb")
    strat_im = IncrementalMergeStrategy(db_im)
    strat_im.load(sample_batch, "RUN-IM")

    # 3. Partition Replace
    db_pr = DatabaseManager(tmp_path / "pr.duckdb")
    strat_pr = PartitionReplaceStrategy(
        db_pr,
        partitions_dir=tmp_path / "partitions",
        temp_dir=tmp_path / "tmp_parts",
    )
    strat_pr.load(sample_batch, "RUN-PR")

    reconciler = ReconciliationEngine(reports_dir=tmp_path / "reports")
    m_fr = reconciler.compute_strategy_metrics(db_fr)
    m_im = reconciler.compute_strategy_metrics(db_im)
    m_pr = reconciler.compute_strategy_metrics(db_pr)

    assert m_fr["total_orders"] == m_im["total_orders"] == m_pr["total_orders"]
    assert m_fr["sum_order_amount"] == m_im["sum_order_amount"] == m_pr["sum_order_amount"]
    assert m_fr["dataset_hash"] == m_im["dataset_hash"] == m_pr["dataset_hash"]
    assert m_fr["count_by_status"] == m_im["count_by_status"] == m_pr["count_by_status"]

    report = reconciler.generate_report(
        source_count=sample_batch.total_raw_count,
        metrics_by_strategy={
            "full_refresh": m_fr,
            "incremental_merge": m_im,
            "partition_replace": m_pr,
        },
        run_id="TEST-RECON",
    )
    assert report["cross_strategy_equivalence"] is True


def test_matches_expected_labels(sample_batch: ValidatedBatch, tmp_path: Path) -> None:
    expected_orders_file = Path("data/expected/sample_expected_orders.json")
    expected_metrics_file = Path("data/expected/sample_expected_metrics.json")

    assert expected_orders_file.exists()
    assert expected_metrics_file.exists()

    with open(expected_metrics_file, "r", encoding="utf-8") as f:
        expected_metrics = json.load(f)

    db = DatabaseManager(tmp_path / "verify_expected.duckdb")
    strat = FullRefreshStrategy(db)
    strat.load(sample_batch, "RUN-EXPECTED")

    reconciler = ReconciliationEngine(tmp_path)
    metrics = reconciler.compute_strategy_metrics(db)

    assert metrics["total_orders"] == expected_metrics["valid_distinct_orders"]
    assert metrics["sum_order_amount"] == expected_metrics["sum_order_amount"]
    assert metrics["count_by_status"] == expected_metrics["count_by_status"]
