"""Tests for ingestion reader and validation quarantine rules."""

from __future__ import annotations

from pathlib import Path

from src.ingestion.reader import RawRecord, read_csv
from src.validation.validator import DataValidator


def test_happy_path_validation() -> None:
    validator = DataValidator(
        valid_customer_ids={"CUST-00001"},
        valid_product_ids={"PROD-00001"},
    )
    raw = [
        RawRecord(
            source_file="valid.jsonl",
            source_line=1,
            raw_payload='{"order_id":"ORD-01","customer_id":"CUST-00001","product_id":"PROD-00001","quantity":2,"order_amount":39.99,"order_status":"PENDING","order_date":"2026-01-01","updated_at":"2026-01-01T12:00:00Z"}',
            data={
                "order_id": "ORD-01",
                "customer_id": "CUST-00001",
                "product_id": "PROD-00001",
                "quantity": 2,
                "order_amount": 39.99,
                "order_status": "PENDING",
                "order_date": "2026-01-01",
                "updated_at": "2026-01-01T12:00:00Z",
            },
        )
    ]
    batch = validator.validate_batch(raw, "RUN-HAPPY")
    assert len(batch.valid_orders) == 1
    assert len(batch.rejects) == 0
    assert batch.valid_orders[0].order_id == "ORD-01"


def test_empty_input_handling(tmp_path: Path) -> None:
    empty_csv = tmp_path / "empty.csv"
    empty_csv.write_text("", encoding="utf-8")
    records = read_csv(empty_csv)
    assert records == []

    validator = DataValidator()
    batch = validator.validate_batch(records, "RUN-EMPTY")
    assert len(batch.valid_orders) == 0
    assert len(batch.rejects) == 0
    assert batch.total_raw_count == 0


def test_malformed_records_isolation(tmp_path: Path) -> None:
    corrupt_file = tmp_path / "corrupt.csv"
    corrupt_file.write_text(
        "order_id,customer_id,product_id,quantity,order_amount,order_status,order_date,updated_at\n"
        'ORD-01,CUST-01,PROD-01,1,20.00,"UNCLOSED_QUOTE,2026-01-01,2026-01-01T12:00:00Z\n'
        "CORRUPT_PAYLOAD_NO_COMMAS\n",
        encoding="utf-8",
    )
    raw = read_csv(corrupt_file)
    validator = DataValidator(valid_customer_ids={"CUST-01"}, valid_product_ids={"PROD-01"})
    batch = validator.validate_batch(raw, "RUN-MAL")

    assert len(batch.valid_orders) == 0
    assert len(batch.rejects) == 2
    for r in batch.rejects:
        assert "MALFORMED_LINE" in r.reject_reason
        assert r.source_line in (2, 3)


def test_missing_required_fields_nulls() -> None:
    validator = DataValidator(
        valid_customer_ids={"CUST-01"},
        valid_product_ids={"PROD-01"},
    )
    raw = [
        RawRecord(
            source_file="nulls.csv",
            source_line=2,
            raw_payload="empty order_id",
            data={
                "order_id": "",
                "customer_id": "CUST-01",
                "product_id": "PROD-01",
                "quantity": 1,
                "order_amount": 10.0,
                "order_status": "PENDING",
                "order_date": "2026-01-01",
                "updated_at": "2026-01-01T12:00:00Z",
            },
        ),
        RawRecord(
            source_file="nulls.csv",
            source_line=3,
            raw_payload="empty customer_id",
            data={
                "order_id": "ORD-02",
                "customer_id": None,
                "product_id": "PROD-01",
                "quantity": 1,
                "order_amount": 10.0,
                "order_status": "PENDING",
                "order_date": "2026-01-01",
                "updated_at": "2026-01-01T12:00:00Z",
            },
        ),
    ]
    batch = validator.validate_batch(raw, "RUN-NULLS")
    assert len(batch.valid_orders) == 0
    assert len(batch.rejects) == 2
    assert "MISSING_REQUIRED_FIELD" in batch.rejects[0].reject_reason
    assert "MISSING_REQUIRED_FIELD" in batch.rejects[1].reject_reason


def test_unexpected_schema_change() -> None:
    validator = DataValidator(valid_customer_ids={"CUST-01"}, valid_product_ids={"PROD-01"})
    raw = [
        RawRecord(
            source_file="schema_extra.csv",
            source_line=2,
            raw_payload="extra col",
            data={
                "order_id": "ORD-01",
                "customer_id": "CUST-01",
                "product_id": "PROD-01",
                "quantity": 1,
                "order_amount": 10.0,
                "order_status": "PENDING",
                "order_date": "2026-01-01",
                "updated_at": "2026-01-01T12:00:00Z",
                "unexpected_bonus_column": "bonus_123",
            },
        ),
        RawRecord(
            source_file="schema_renamed.csv",
            source_line=2,
            raw_payload="renamed col",
            data={
                "order_number": "ORD-02",
                "customer_id": "CUST-01",
                "product_id": "PROD-01",
                "quantity": 1,
                "order_amount": 10.0,
                "order_status": "PENDING",
                "order_date": "2026-01-01",
                "updated_at": "2026-01-01T12:00:00Z",
            },
        ),
    ]
    batch = validator.validate_batch(raw, "RUN-SCHEMA")
    assert len(batch.valid_orders) == 0
    assert len(batch.rejects) == 2
    assert "UNEXPECTED_SCHEMA" in batch.rejects[0].reject_reason
    assert "UNEXPECTED_SCHEMA" in batch.rejects[1].reject_reason


def test_invalid_reference_key() -> None:
    validator = DataValidator(
        valid_customer_ids={"CUST-00001"},
        valid_product_ids={"PROD-00001"},
    )
    raw = [
        RawRecord(
            source_file="fk_bad.csv",
            source_line=2,
            raw_payload="bad cust",
            data={
                "order_id": "ORD-01",
                "customer_id": "CUST-99999",
                "product_id": "PROD-00001",
                "quantity": 1,
                "order_amount": 10.0,
                "order_status": "PENDING",
                "order_date": "2026-01-01",
                "updated_at": "2026-01-01T12:00:00Z",
            },
        ),
        RawRecord(
            source_file="fk_bad.csv",
            source_line=3,
            raw_payload="bad prod",
            data={
                "order_id": "ORD-02",
                "customer_id": "CUST-00001",
                "product_id": "PROD-99999",
                "quantity": 1,
                "order_amount": 10.0,
                "order_status": "PENDING",
                "order_date": "2026-01-01",
                "updated_at": "2026-01-01T12:00:00Z",
            },
        ),
    ]
    batch = validator.validate_batch(raw, "RUN-FK")
    assert len(batch.valid_orders) == 0
    assert len(batch.rejects) == 2
    assert "FOREIGN_KEY_VIOLATION" in batch.rejects[0].reject_reason
    assert "FOREIGN_KEY_VIOLATION" in batch.rejects[1].reject_reason


def test_duplicate_resolution_deterministic_latest_wins() -> None:
    validator = DataValidator(
        valid_customer_ids={"CUST-01"},
        valid_product_ids={"PROD-01"},
    )
    raw = [
        RawRecord(
            source_file="batch1.csv",
            source_line=2,
            raw_payload="first version",
            data={
                "order_id": "ORD-DUP-01",
                "customer_id": "CUST-01",
                "product_id": "PROD-01",
                "quantity": 1,
                "order_amount": 10.0,
                "order_status": "PENDING",
                "order_date": "2026-01-01",
                "updated_at": "2026-01-01T10:00:00Z",
            },
        ),
        RawRecord(
            source_file="batch2.csv",
            source_line=5,
            raw_payload="second version newer",
            data={
                "order_id": "ORD-DUP-01",
                "customer_id": "CUST-01",
                "product_id": "PROD-01",
                "quantity": 1,
                "order_amount": 10.0,
                "order_status": "SHIPPED",
                "order_date": "2026-01-01",
                "updated_at": "2026-01-01T16:00:00Z",
            },
        ),
    ]
    batch = validator.validate_batch(raw, "RUN-DUP")
    assert len(batch.valid_orders) == 1
    assert batch.valid_orders[0].order_status == "SHIPPED"
    assert batch.valid_orders[0].updated_at == "2026-01-01T16:00:00Z"
    # Both transitions are recorded in history
    assert len(batch.status_history) == 2
