"""Pydantic data models for domain entities, logs, rejects, and validation."""

from __future__ import annotations

import datetime
import hashlib
import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

VALID_ORDER_STATUSES = {
    "PENDING",
    "PROCESSING",
    "SHIPPED",
    "DELIVERED",
    "CANCELLED",
    "RETURNED",
}

ISO_DATE_REGEX = re.compile(r"^\d{4}-\d{2}-\d{2}$")
ISO_TIMESTAMP_REGEX = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$"
)


class Customer(BaseModel):
    model_config = ConfigDict(strict=False)

    customer_id: str = Field(min_length=1)
    name: str = Field(min_length=1)
    email: str = Field(min_length=3)
    country: str = Field(min_length=2, max_length=3)
    created_at: str

    @field_validator("created_at")
    @classmethod
    def validate_created_at(cls, v: str) -> str:
        if not ISO_TIMESTAMP_REGEX.match(v):
            raise ValueError(f"Invalid ISO-8601 timestamp: {v}")
        return v


class Product(BaseModel):
    model_config = ConfigDict(strict=False)

    product_id: str = Field(min_length=1)
    product_name: str = Field(min_length=1)
    category: str = Field(min_length=1)
    unit_price: float = Field(ge=0.0)


class Order(BaseModel):
    model_config = ConfigDict(strict=False)

    order_id: str = Field(min_length=1)
    customer_id: str = Field(min_length=1)
    product_id: str = Field(min_length=1)
    quantity: int = Field(ge=0)
    order_amount: float = Field(ge=0.0)
    order_status: str
    order_date: str
    updated_at: str
    run_id: str = ""
    source_file: str = ""

    @field_validator("order_status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        if v not in VALID_ORDER_STATUSES:
            raise ValueError(
                f"Invalid order status: {v}. Must be one of {sorted(VALID_ORDER_STATUSES)}"
            )
        return v

    @field_validator("order_date")
    @classmethod
    def validate_date(cls, v: str) -> str:
        if not ISO_DATE_REGEX.match(v):
            raise ValueError(f"Invalid ISO date (YYYY-MM-DD): {v}")
        # Verify valid calendar date
        datetime.date.fromisoformat(v)
        return v

    @field_validator("updated_at")
    @classmethod
    def validate_timestamp(cls, v: str) -> str:
        if not ISO_TIMESTAMP_REGEX.match(v):
            raise ValueError(f"Invalid ISO-8601 timestamp: {v}")
        # Verify valid datetime
        datetime.datetime.fromisoformat(v.replace("Z", "+00:00"))
        return v

    def business_hash(self) -> str:
        """Deterministic hash of order business attributes."""
        payload = f"{self.order_id}|{self.customer_id}|{self.product_id}|{self.quantity}|{self.order_amount:.2f}|{self.order_status}|{self.order_date}|{self.updated_at}"
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class OrderStatusHistory(BaseModel):
    model_config = ConfigDict(strict=False)

    history_id: str
    order_id: str
    order_status: str
    updated_at: str
    run_id: str
    source_file: str

    @classmethod
    def create(
        cls,
        order_id: str,
        order_status: str,
        updated_at: str,
        run_id: str,
        source_file: str,
    ) -> OrderStatusHistory:
        raw_key = f"{order_id}|{order_status}|{updated_at}"
        history_id = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()
        return cls(
            history_id=history_id,
            order_id=order_id,
            order_status=order_status,
            updated_at=updated_at,
            run_id=run_id,
            source_file=source_file,
        )


class RejectRecord(BaseModel):
    model_config = ConfigDict(strict=False)

    reject_id: str
    run_id: str
    source_file: str
    source_line: int
    reject_reason: str
    raw_payload: str
    created_at: str

    @classmethod
    def create(
        cls,
        run_id: str,
        source_file: str,
        source_line: int,
        reject_reason: str,
        raw_payload: str,
    ) -> RejectRecord:
        now_utc = datetime.datetime.now(datetime.timezone.utc).isoformat()
        raw_key = f"{run_id}|{source_file}|{source_line}|{reject_reason}"
        reject_id = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()
        return cls(
            reject_id=reject_id,
            run_id=run_id,
            source_file=source_file,
            source_line=source_line,
            reject_reason=reject_reason,
            raw_payload=raw_payload,
            created_at=now_utc,
        )


class RunLog(BaseModel):
    model_config = ConfigDict(strict=False)

    run_id: str
    strategy: Literal["full_refresh", "incremental_merge", "partition_replace"]
    start_time: str
    end_time: str
    input_count: int
    output_count: int
    rejected_count: int
    status: Literal["RUNNING", "SUCCESS", "FAILED"]
    failure_reason: str | None = None
