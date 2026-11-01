"""Tests for load strategies, idempotency, fault injection, and retry recovery."""

from __future__ import annotations

from pathlib import Path

from src.models import Order
from src.storage.db import DatabaseManager
from src.strategies.full_refresh import FullRefreshStrategy
from src.strategies.incremental_merge import IncrementalMergeStrategy
from src.strategies.partition_replace import PartitionReplaceStrategy
from src.validation.validator import ValidatedBatch


def test_full_refresh_happy_path_and_idempotency(
    test_db: DatabaseManager, sample_batch: ValidatedBatch
) -> None:
    strat = FullRefreshStrategy(test_db)

    # First run
    res1 = strat.load(sample_batch, "RUN-FR-1")
    assert res1.status == "SUCCESS"
    assert res1.records_loaded == len(sample_batch.valid_orders)

    rows1 = test_db.execute_query("SELECT count(*) FROM orders;")[0][0]
    assert rows1 == len(sample_batch.valid_orders)

    # Idempotency check: rerun same input
    res2 = strat.load(sample_batch, "RUN-FR-2")
    assert res2.status == "SUCCESS"
    rows2 = test_db.execute_query("SELECT count(*) FROM orders;")[0][0]
    assert rows2 == rows1


def test_incremental_merge_watermark_and_out_of_order(test_db: DatabaseManager) -> None:
    strat = IncrementalMergeStrategy(test_db)

    # 1. Base initial order
    order_base = Order(
        order_id="ORD-IMM-01",
        customer_id="CUST-01",
        product_id="PROD-01",
        quantity=1,
        order_amount=50.0,
        order_status="PENDING",
        order_date="2026-01-01",
        updated_at="2026-01-01T10:00:00Z",
        run_id="RUN-1",
        source_file="file1.csv",
    )
    b1 = ValidatedBatch(
        valid_orders=[order_base],
        status_history=[],
        rejects=[],
        total_raw_count=1,
    )
    strat.load(b1, "RUN-1")

    row = test_db.execute_query(
        "SELECT order_status, updated_at FROM orders WHERE order_id = 'ORD-IMM-01';"
    )[0]
    assert row[0] == "PENDING"

    # 2. Late-arriving newer update
    order_newer = Order(
        order_id="ORD-IMM-01",
        customer_id="CUST-01",
        product_id="PROD-01",
        quantity=1,
        order_amount=50.0,
        order_status="DELIVERED",
        order_date="2026-01-01",
        updated_at="2026-01-15T12:00:00Z",
        run_id="RUN-2",
        source_file="file2.csv",
    )
    b2 = ValidatedBatch(
        valid_orders=[order_newer],
        status_history=[],
        rejects=[],
        total_raw_count=1,
    )
    strat.load(b2, "RUN-2")

    row2 = test_db.execute_query(
        "SELECT order_status, updated_at FROM orders WHERE order_id = 'ORD-IMM-01';"
    )[0]
    assert row2[0] == "DELIVERED"
    assert row2[1] == "2026-01-15T12:00:00Z"

    # 3. Out-of-order older update arriving later (older updated_at)
    order_older = Order(
        order_id="ORD-IMM-01",
        customer_id="CUST-01",
        product_id="PROD-01",
        quantity=1,
        order_amount=50.0,
        order_status="PROCESSING",
        order_date="2026-01-01",
        updated_at="2026-01-05T10:00:00Z",  # older than 2026-01-15!
        run_id="RUN-3",
        source_file="file3.csv",
    )
    b3 = ValidatedBatch(
        valid_orders=[order_older],
        status_history=[],
        rejects=[],
        total_raw_count=1,
    )
    strat.load(b3, "RUN-3")

    # State must NOT be overwritten by older update!
    row3 = test_db.execute_query(
        "SELECT order_status, updated_at FROM orders WHERE order_id = 'ORD-IMM-01';"
    )[0]
    assert row3[0] == "DELIVERED"
    assert row3[1] == "2026-01-15T12:00:00Z"


def test_partition_replace_atomic_and_idempotency(
    test_db: DatabaseManager, sample_batch: ValidatedBatch, tmp_path: Path
) -> None:
    part_dir = tmp_path / "partitions"
    temp_dir = tmp_path / "tmp_parts"
    strat = PartitionReplaceStrategy(test_db, partitions_dir=part_dir, temp_dir=temp_dir)

    # First load
    res1 = strat.load(sample_batch, "RUN-PR-1")
    assert res1.status == "SUCCESS"
    rows1 = test_db.execute_query("SELECT count(*) FROM orders;")[0][0]
    assert rows1 == len(sample_batch.valid_orders)

    # Idempotent rerun
    res2 = strat.load(sample_batch, "RUN-PR-2")
    assert res2.status == "SUCCESS"
    rows2 = test_db.execute_query("SELECT count(*) FROM orders;")[0][0]
    assert rows2 == rows1


def test_fault_injection_and_retry_full_refresh(
    test_db: DatabaseManager, sample_batch: ValidatedBatch
) -> None:
    strat = FullRefreshStrategy(test_db)

    # Inject failure midway
    res_fail = strat.load(sample_batch, "RUN-FAIL", fail_after=10)
    assert res_fail.status == "FAILED"
    assert "Fault injected" in str(res_fail.error_message)

    # Target table has not been corrupted (remains 0 rows)
    rows_after_fail = test_db.execute_query("SELECT count(*) FROM orders;")[0][0]
    assert rows_after_fail == 0

    # Retry without fault injection succeeds
    res_retry = strat.load(sample_batch, "RUN-RETRY", fail_after=None)
    assert res_retry.status == "SUCCESS"
    assert test_db.execute_query("SELECT count(*) FROM orders;")[0][0] == len(
        sample_batch.valid_orders
    )


def test_fault_injection_and_retry_incremental_merge(
    test_db: DatabaseManager, sample_batch: ValidatedBatch
) -> None:
    strat = IncrementalMergeStrategy(test_db)

    # Inject failure
    res_fail = strat.load(sample_batch, "RUN-FAIL-IM", fail_after=10)
    assert res_fail.status == "FAILED"
    assert "Fault injected" in str(res_fail.error_message)
    assert test_db.execute_query("SELECT count(*) FROM orders;")[0][0] == 0

    # Retry
    res_retry = strat.load(sample_batch, "RUN-RETRY-IM", fail_after=None)
    assert res_retry.status == "SUCCESS"
    assert test_db.execute_query("SELECT count(*) FROM orders;")[0][0] == len(
        sample_batch.valid_orders
    )


def test_fault_injection_and_retry_partition_replace(
    test_db: DatabaseManager, sample_batch: ValidatedBatch, tmp_path: Path
) -> None:
    part_dir = tmp_path / "partitions_fail"
    temp_dir = tmp_path / "tmp_parts_fail"
    strat = PartitionReplaceStrategy(test_db, partitions_dir=part_dir, temp_dir=temp_dir)

    # Inject failure
    res_fail = strat.load(sample_batch, "RUN-FAIL-PR", fail_after=5)
    assert res_fail.status == "FAILED"
    assert "Fault injected" in str(res_fail.error_message)
    # Temporary run directory cleaned up
    assert not (temp_dir / "RUN-FAIL-PR").exists()

    # Retry succeeds
    res_retry = strat.load(sample_batch, "RUN-RETRY-PR", fail_after=None)
    assert res_retry.status == "SUCCESS"
    assert test_db.execute_query("SELECT count(*) FROM orders;")[0][0] == len(
        sample_batch.valid_orders
    )
