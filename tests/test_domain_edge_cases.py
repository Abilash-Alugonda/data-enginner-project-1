"""Tests for domain specific business rules and boundary edge cases."""

from __future__ import annotations

from src.models import Order, OrderStatusHistory
from src.storage.db import DatabaseManager
from src.strategies.incremental_merge import IncrementalMergeStrategy
from src.validation.validator import DataValidator, ValidatedBatch


def test_domain_edge_case_order_cancelled_after_shipped(test_db: DatabaseManager) -> None:
    """Verify that an order cancelled after shipping updates target state and records transitions."""
    strat = IncrementalMergeStrategy(test_db)

    # 1. Order created as SHIPPED
    order_shipped = Order(
        order_id="ORD-EDGE-01",
        customer_id="CUST-00001",
        product_id="PROD-00001",
        quantity=1,
        order_amount=50.0,
        order_status="SHIPPED",
        order_date="2026-01-05",
        updated_at="2026-01-05T10:00:00Z",
        run_id="RUN-1",
        source_file="orders_1.csv",
    )
    h1 = OrderStatusHistory.create(
        order_id="ORD-EDGE-01",
        order_status="SHIPPED",
        updated_at="2026-01-05T10:00:00Z",
        run_id="RUN-1",
        source_file="orders_1.csv",
    )
    b1 = ValidatedBatch(
        valid_orders=[order_shipped],
        status_history=[h1],
        rejects=[],
        total_raw_count=1,
    )
    strat.load(b1, "RUN-1")

    # Target table is SHIPPED
    row1 = test_db.execute_query("SELECT order_status FROM orders WHERE order_id = 'ORD-EDGE-01';")[
        0
    ]
    assert row1[0] == "SHIPPED"

    # 2. Later update cancels the order
    order_cancelled = Order(
        order_id="ORD-EDGE-01",
        customer_id="CUST-00001",
        product_id="PROD-00001",
        quantity=1,
        order_amount=50.0,
        order_status="CANCELLED",
        order_date="2026-01-05",
        updated_at="2026-01-08T15:30:00Z",
        run_id="RUN-2",
        source_file="orders_2.csv",
    )
    h2 = OrderStatusHistory.create(
        order_id="ORD-EDGE-01",
        order_status="CANCELLED",
        updated_at="2026-01-08T15:30:00Z",
        run_id="RUN-2",
        source_file="orders_2.csv",
    )
    b2 = ValidatedBatch(
        valid_orders=[order_cancelled],
        status_history=[h2],
        rejects=[],
        total_raw_count=1,
    )
    strat.load(b2, "RUN-2")

    # Current target state is CANCELLED
    row2 = test_db.execute_query("SELECT order_status FROM orders WHERE order_id = 'ORD-EDGE-01';")[
        0
    ]
    assert row2[0] == "CANCELLED"

    # Both history transitions are preserved
    history_rows = test_db.execute_query(
        "SELECT order_status, updated_at FROM order_status_history WHERE order_id = 'ORD-EDGE-01' ORDER BY updated_at;"
    )
    assert len(history_rows) == 2
    assert history_rows[0][0] == "SHIPPED"
    assert history_rows[1][0] == "CANCELLED"


def test_domain_edge_case_quantity_zero_with_nonzero_amount() -> None:
    """Verify that quantity 0 with positive order amount (e.g. warranty / service fee) is valid."""
    validator = DataValidator(
        valid_customer_ids={"CUST-00001"},
        valid_product_ids={"PROD-00001"},
    )
    from src.ingestion.reader import RawRecord

    raw = [
        RawRecord(
            source_file="service_fee.csv",
            source_line=2,
            raw_payload="zero quantity service fee",
            data={
                "order_id": "ORD-SERVICE-01",
                "customer_id": "CUST-00001",
                "product_id": "PROD-00001",
                "quantity": 0,
                "order_amount": 35.00,
                "order_status": "DELIVERED",
                "order_date": "2026-01-10",
                "updated_at": "2026-01-10T12:00:00Z",
            },
        )
    ]
    batch = validator.validate_batch(raw, "RUN-SERVICE")
    assert len(batch.valid_orders) == 1
    assert len(batch.rejects) == 0
    assert batch.valid_orders[0].quantity == 0
    assert batch.valid_orders[0].order_amount == 35.00
