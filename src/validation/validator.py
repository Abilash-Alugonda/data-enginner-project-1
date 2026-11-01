"""Validation and quarantine logic enforcing schema, referential integrity, and business rules."""

from __future__ import annotations

import datetime
from dataclasses import dataclass

from src.ingestion.reader import RawRecord
from src.models import (
    ISO_DATE_REGEX,
    ISO_TIMESTAMP_REGEX,
    VALID_ORDER_STATUSES,
    Order,
    OrderStatusHistory,
    RejectRecord,
)

EXPECTED_ORDER_COLUMNS = {
    "order_id",
    "customer_id",
    "product_id",
    "quantity",
    "order_amount",
    "order_status",
    "order_date",
    "updated_at",
}


@dataclass
class ValidatedBatch:
    valid_orders: list[Order]
    status_history: list[OrderStatusHistory]
    rejects: list[RejectRecord]
    total_raw_count: int


class DataValidator:
    """Validates records against schema, constraints, business rules, and foreign keys."""

    def __init__(
        self,
        valid_customer_ids: set[str] | None = None,
        valid_product_ids: set[str] | None = None,
    ) -> None:
        self.valid_customer_ids = valid_customer_ids or set()
        self.valid_product_ids = valid_product_ids or set()

    def set_reference_keys(self, customer_ids: set[str], product_ids: set[str]) -> None:
        self.valid_customer_ids = customer_ids
        self.valid_product_ids = product_ids

    def validate_batch(self, raw_records: list[RawRecord], run_id: str) -> ValidatedBatch:
        rejects: list[RejectRecord] = []
        status_history: list[OrderStatusHistory] = []
        # Map: order_id -> (Order, source_file, source_line)
        order_candidates: dict[str, tuple[Order, str, int]] = {}

        for rec in raw_records:
            # 1. Parse errors
            if rec.parse_error:
                rejects.append(
                    RejectRecord.create(
                        run_id=run_id,
                        source_file=rec.source_file,
                        source_line=rec.source_line,
                        reject_reason=rec.parse_error,
                        raw_payload=rec.raw_payload,
                    )
                )
                continue

            data = rec.data or {}

            # 2. Schema check: unexpected extra columns or missing mandatory columns
            record_keys = set(data.keys())
            extra_cols = record_keys - EXPECTED_ORDER_COLUMNS
            missing_cols = EXPECTED_ORDER_COLUMNS - record_keys
            if extra_cols or missing_cols:
                reason = "UNEXPECTED_SCHEMA: "
                parts = []
                if extra_cols:
                    parts.append(f"extra columns {sorted(extra_cols)}")
                if missing_cols:
                    parts.append(f"missing columns {sorted(missing_cols)}")
                reason += ", ".join(parts)
                rejects.append(
                    RejectRecord.create(
                        run_id=run_id,
                        source_file=rec.source_file,
                        source_line=rec.source_line,
                        reject_reason=reason,
                        raw_payload=rec.raw_payload,
                    )
                )
                continue

            # 3. Required string fields
            order_id = str(data.get("order_id") or "").strip()
            if not order_id:
                rejects.append(
                    RejectRecord.create(
                        run_id=run_id,
                        source_file=rec.source_file,
                        source_line=rec.source_line,
                        reject_reason="MISSING_REQUIRED_FIELD: order_id is empty or null",
                        raw_payload=rec.raw_payload,
                    )
                )
                continue

            customer_id = str(data.get("customer_id") or "").strip()
            if not customer_id:
                rejects.append(
                    RejectRecord.create(
                        run_id=run_id,
                        source_file=rec.source_file,
                        source_line=rec.source_line,
                        reject_reason="MISSING_REQUIRED_FIELD: customer_id is empty or null",
                        raw_payload=rec.raw_payload,
                    )
                )
                continue

            product_id = str(data.get("product_id") or "").strip()
            if not product_id:
                rejects.append(
                    RejectRecord.create(
                        run_id=run_id,
                        source_file=rec.source_line,
                        source_line=rec.source_line,
                        reject_reason="MISSING_REQUIRED_FIELD: product_id is empty or null",
                        raw_payload=rec.raw_payload,
                    )
                )
                continue

            # 4. Numeric types and constraints
            try:
                quantity = int(data.get("quantity"))  # type: ignore
            except (ValueError, TypeError):
                rejects.append(
                    RejectRecord.create(
                        run_id=run_id,
                        source_file=rec.source_file,
                        source_line=rec.source_line,
                        reject_reason=f"INVALID_QUANTITY: Cannot parse as integer: {data.get('quantity')}",
                        raw_payload=rec.raw_payload,
                    )
                )
                continue

            if quantity < 0:
                rejects.append(
                    RejectRecord.create(
                        run_id=run_id,
                        source_file=rec.source_file,
                        source_line=rec.source_line,
                        reject_reason=f"INVALID_QUANTITY: Must be non-negative, got {quantity}",
                        raw_payload=rec.raw_payload,
                    )
                )
                continue

            try:
                order_amount = float(data.get("order_amount"))  # type: ignore
            except (ValueError, TypeError):
                rejects.append(
                    RejectRecord.create(
                        run_id=run_id,
                        source_file=rec.source_file,
                        source_line=rec.source_line,
                        reject_reason=f"INVALID_ORDER_AMOUNT: Cannot parse as float: {data.get('order_amount')}",
                        raw_payload=rec.raw_payload,
                    )
                )
                continue

            if order_amount < 0.0:
                rejects.append(
                    RejectRecord.create(
                        run_id=run_id,
                        source_file=rec.source_file,
                        source_line=rec.source_line,
                        reject_reason=f"INVALID_ORDER_AMOUNT: Must be non-negative, got {order_amount}",
                        raw_payload=rec.raw_payload,
                    )
                )
                continue

            # 5. Order status check
            order_status = str(data.get("order_status") or "").strip()
            if order_status not in VALID_ORDER_STATUSES:
                rejects.append(
                    RejectRecord.create(
                        run_id=run_id,
                        source_file=rec.source_file,
                        source_line=rec.source_line,
                        reject_reason=f"INVALID_STATUS: '{order_status}' not in {sorted(VALID_ORDER_STATUSES)}",
                        raw_payload=rec.raw_payload,
                    )
                )
                continue

            # 6. Date and Timestamp formats
            order_date = str(data.get("order_date") or "").strip()
            if not ISO_DATE_REGEX.match(order_date):
                rejects.append(
                    RejectRecord.create(
                        run_id=run_id,
                        source_file=rec.source_file,
                        source_line=rec.source_line,
                        reject_reason=f"INVALID_ORDER_DATE: Must match YYYY-MM-DD, got '{order_date}'",
                        raw_payload=rec.raw_payload,
                    )
                )
                continue

            try:
                datetime.date.fromisoformat(order_date)
            except ValueError as e:
                rejects.append(
                    RejectRecord.create(
                        run_id=run_id,
                        source_file=rec.source_file,
                        source_line=rec.source_line,
                        reject_reason=f"INVALID_ORDER_DATE: Invalid calendar date: {e}",
                        raw_payload=rec.raw_payload,
                    )
                )
                continue

            updated_at = str(data.get("updated_at") or "").strip()
            if not ISO_TIMESTAMP_REGEX.match(updated_at):
                rejects.append(
                    RejectRecord.create(
                        run_id=run_id,
                        source_file=rec.source_file,
                        source_line=rec.source_line,
                        reject_reason=f"INVALID_UPDATED_AT: Must be ISO-8601 UTC timestamp, got '{updated_at}'",
                        raw_payload=rec.raw_payload,
                    )
                )
                continue

            try:
                datetime.datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
            except ValueError as e:
                rejects.append(
                    RejectRecord.create(
                        run_id=run_id,
                        source_file=rec.source_file,
                        source_line=rec.source_line,
                        reject_reason=f"INVALID_UPDATED_AT: Invalid ISO timestamp: {e}",
                        raw_payload=rec.raw_payload,
                    )
                )
                continue

            # 7. Referential Integrity Checks
            if self.valid_customer_ids and customer_id not in self.valid_customer_ids:
                rejects.append(
                    RejectRecord.create(
                        run_id=run_id,
                        source_file=rec.source_file,
                        source_line=rec.source_line,
                        reject_reason=f"FOREIGN_KEY_VIOLATION: customer_id '{customer_id}' does not exist in customers table",
                        raw_payload=rec.raw_payload,
                    )
                )
                continue

            if self.valid_product_ids and product_id not in self.valid_product_ids:
                rejects.append(
                    RejectRecord.create(
                        run_id=run_id,
                        source_file=rec.source_file,
                        source_line=rec.source_line,
                        reject_reason=f"FOREIGN_KEY_VIOLATION: product_id '{product_id}' does not exist in products table",
                        raw_payload=rec.raw_payload,
                    )
                )
                continue

            # Record is valid!
            order_obj = Order(
                order_id=order_id,
                customer_id=customer_id,
                product_id=product_id,
                quantity=quantity,
                order_amount=order_amount,
                order_status=order_status,
                order_date=order_date,
                updated_at=updated_at,
                run_id=run_id,
                source_file=rec.source_file,
            )

            # Record in status history
            history_entry = OrderStatusHistory.create(
                order_id=order_id,
                order_status=order_status,
                updated_at=updated_at,
                run_id=run_id,
                source_file=rec.source_file,
            )
            status_history.append(history_entry)

            # Duplicate resolution rule:
            # Latest updated_at wins. Tie broken by source_file then source_line (larger line wins).
            if order_id not in order_candidates:
                order_candidates[order_id] = (order_obj, rec.source_file, rec.source_line)
            else:
                existing_order, ex_file, ex_line = order_candidates[order_id]
                if order_obj.updated_at > existing_order.updated_at:
                    order_candidates[order_id] = (order_obj, rec.source_file, rec.source_line)
                elif order_obj.updated_at == existing_order.updated_at:
                    # Tie-breaking by source_file then source_line
                    if (rec.source_file, rec.source_line) > (ex_file, ex_line):
                        order_candidates[order_id] = (order_obj, rec.source_file, rec.source_line)

        valid_orders = [item[0] for item in order_candidates.values()]
        # Sort valid orders deterministically by order_id
        valid_orders.sort(key=lambda o: o.order_id)

        return ValidatedBatch(
            valid_orders=valid_orders,
            status_history=status_history,
            rejects=rejects,
            total_raw_count=len(raw_records),
        )
