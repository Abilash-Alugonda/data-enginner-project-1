"""Base class, schemas, and columnar conversion utilities for load strategies."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

import pyarrow as pa

from src.models import Order, OrderStatusHistory, RejectRecord
from src.storage.db import DatabaseManager
from src.validation.validator import ValidatedBatch

ARROW_ORDER_SCHEMA = pa.schema(
    [
        ("order_id", pa.string()),
        ("customer_id", pa.string()),
        ("product_id", pa.string()),
        ("quantity", pa.int64()),
        ("order_amount", pa.float64()),
        ("order_status", pa.string()),
        ("order_date", pa.string()),
        ("updated_at", pa.string()),
        ("run_id", pa.string()),
        ("source_file", pa.string()),
    ]
)

ARROW_STATUS_HISTORY_SCHEMA = pa.schema(
    [
        ("history_id", pa.string()),
        ("order_id", pa.string()),
        ("order_status", pa.string()),
        ("updated_at", pa.string()),
        ("run_id", pa.string()),
        ("source_file", pa.string()),
    ]
)

ARROW_REJECT_SCHEMA = pa.schema(
    [
        ("reject_id", pa.string()),
        ("run_id", pa.string()),
        ("source_file", pa.string()),
        ("source_line", pa.int64()),
        ("reject_reason", pa.string()),
        ("raw_payload", pa.string()),
        ("created_at", pa.string()),
    ]
)


def orders_to_arrow(orders: list[Order]) -> pa.Table:
    """Convert orders to PyArrow Table efficiently."""
    if not orders:
        return ARROW_ORDER_SCHEMA.empty_table()
    data_dict = {
        "order_id": [o.order_id for o in orders],
        "customer_id": [o.customer_id for o in orders],
        "product_id": [o.product_id for o in orders],
        "quantity": [o.quantity for o in orders],
        "order_amount": [o.order_amount for o in orders],
        "order_status": [o.order_status for o in orders],
        "order_date": [o.order_date for o in orders],
        "updated_at": [o.updated_at for o in orders],
        "run_id": [o.run_id for o in orders],
        "source_file": [o.source_file for o in orders],
    }
    return pa.Table.from_pydict(data_dict, schema=ARROW_ORDER_SCHEMA)


class FaultInjectionError(Exception):
    """Raised when fault injection threshold is reached."""

    pass


@dataclass
class StrategyExecutionResult:
    strategy_name: str
    run_id: str
    records_loaded: int
    status_history_loaded: int
    rejects_loaded: int
    duration_seconds: float
    status: str
    error_message: str | None = None


class BaseLoadStrategy(ABC):
    """Abstract base class that all load strategies must implement."""

    def __init__(self, db_manager: DatabaseManager) -> None:
        self.db = db_manager

    @abstractmethod
    def load(
        self,
        batch: ValidatedBatch,
        run_id: str,
        fail_after: int | None = None,
    ) -> StrategyExecutionResult:
        """Execute the load strategy with the validated batch."""
        pass

    def record_rejects(self, rejects: list[RejectRecord], con: Any) -> int:
        """Insert rejects into quarantine table using vectorized Arrow registration."""
        if not rejects:
            return 0
        data_dict = {
            "reject_id": [r.reject_id for r in rejects],
            "run_id": [r.run_id for r in rejects],
            "source_file": [r.source_file for r in rejects],
            "source_line": [r.source_line for r in rejects],
            "reject_reason": [r.reject_reason for r in rejects],
            "raw_payload": [r.raw_payload for r in rejects],
            "created_at": [r.created_at for r in rejects],
        }
        tbl = pa.Table.from_pydict(data_dict, schema=ARROW_REJECT_SCHEMA)
        con.register("tmp_rejects_arrow", tbl)
        con.execute("INSERT OR REPLACE INTO rejects SELECT * FROM tmp_rejects_arrow;")
        con.unregister("tmp_rejects_arrow")
        return len(rejects)

    def record_status_history(self, history: list[OrderStatusHistory], con: Any) -> int:
        """Insert status transitions idempotently using vectorized Arrow registration."""
        if not history:
            return 0
        data_dict = {
            "history_id": [h.history_id for h in history],
            "order_id": [h.order_id for h in history],
            "order_status": [h.order_status for h in history],
            "updated_at": [h.updated_at for h in history],
            "run_id": [h.run_id for h in history],
            "source_file": [h.source_file for h in history],
        }
        tbl = pa.Table.from_pydict(data_dict, schema=ARROW_STATUS_HISTORY_SCHEMA)
        con.register("tmp_sh_arrow", tbl)
        con.execute("INSERT OR REPLACE INTO order_status_history SELECT * FROM tmp_sh_arrow;")
        con.unregister("tmp_sh_arrow")
        return len(history)
