"""Incremental merge load strategy.

Upserts only rows newer than a stored watermark, handling late and out-of-order data.
"""

from __future__ import annotations

import datetime
import time

from src.strategies.base import (
    BaseLoadStrategy,
    FaultInjectionError,
    StrategyExecutionResult,
    orders_to_arrow,
)
from src.validation.validator import ValidatedBatch


class IncrementalMergeStrategy(BaseLoadStrategy):
    """Incremental merge strategy implementing watermark tracking and deterministic upserts."""

    def load(
        self,
        batch: ValidatedBatch,
        run_id: str,
        fail_after: int | None = None,
    ) -> StrategyExecutionResult:
        start_time = time.perf_counter()
        con = self.db.get_connection()

        try:
            # 1. Fetch current watermark
            cur_wm = con.execute(
                "SELECT last_watermark FROM watermarks WHERE strategy = 'incremental_merge';"
            ).fetchone()
            last_watermark = cur_wm[0] if cur_wm else "1970-01-01T00:00:00Z"

            # 2. Begin transaction for atomic merge
            con.execute("BEGIN TRANSACTION;")

            # 3. Create a temp table of incoming valid orders for set-based merge
            con.execute("DROP TABLE IF EXISTS incoming_orders_temp;")
            con.execute(
                """
                CREATE TEMP TABLE incoming_orders_temp (
                    order_id VARCHAR PRIMARY KEY,
                    customer_id VARCHAR NOT NULL,
                    product_id VARCHAR NOT NULL,
                    quantity BIGINT NOT NULL,
                    order_amount DOUBLE NOT NULL,
                    order_status VARCHAR NOT NULL,
                    order_date VARCHAR NOT NULL,
                    updated_at VARCHAR NOT NULL,
                    run_id VARCHAR NOT NULL,
                    source_file VARCHAR NOT NULL
                );
                """
            )

            # Fault injection check before committing merge
            if fail_after is not None and len(batch.valid_orders) >= fail_after:
                con.execute("ROLLBACK;")
                raise FaultInjectionError(
                    f"Fault injected: simulated failure after {fail_after} records in incremental_merge"
                )

            if batch.valid_orders:
                tbl = orders_to_arrow(batch.valid_orders)
                con.register("tmp_incoming_arrow", tbl)
                con.execute("INSERT INTO incoming_orders_temp SELECT * FROM tmp_incoming_arrow;")
                con.unregister("tmp_incoming_arrow")

            # 4. Perform set-based upsert:
            # - Insert rows that do not exist in orders
            # - Update rows where incoming updated_at > orders.updated_at
            # Out-of-order updates (incoming updated_at <= orders.updated_at) will NOT overwrite target!
            con.execute(
                """
                INSERT INTO orders
                SELECT i.*
                FROM incoming_orders_temp i
                LEFT JOIN orders o ON i.order_id = o.order_id
                WHERE o.order_id IS NULL;
                """
            )

            con.execute(
                """
                UPDATE orders
                SET
                    customer_id = i.customer_id,
                    product_id = i.product_id,
                    quantity = i.quantity,
                    order_amount = i.order_amount,
                    order_status = i.order_status,
                    order_date = i.order_date,
                    updated_at = i.updated_at,
                    run_id = i.run_id,
                    source_file = i.source_file
                FROM incoming_orders_temp i
                WHERE orders.order_id = i.order_id
                  AND i.updated_at > orders.updated_at;
                """
            )

            # 5. Update watermark to max of previous watermark and max incoming updated_at
            max_incoming = max((o.updated_at for o in batch.valid_orders), default=last_watermark)
            new_watermark = max(last_watermark, max_incoming)
            now_utc = datetime.datetime.now(datetime.timezone.utc).isoformat()

            con.execute(
                """
                INSERT OR REPLACE INTO watermarks (strategy, last_watermark, updated_at)
                VALUES ('incremental_merge', ?, ?);
                """,
                [new_watermark, now_utc],
            )

            # 6. Record status history and rejects idempotently
            self.record_rejects(batch.rejects, con)
            self.record_status_history(batch.status_history, con)

            con.execute("COMMIT;")

            duration = time.perf_counter() - start_time
            return StrategyExecutionResult(
                strategy_name="incremental_merge",
                run_id=run_id,
                records_loaded=len(batch.valid_orders),
                status_history_loaded=len(batch.status_history),
                rejects_loaded=len(batch.rejects),
                duration_seconds=duration,
                status="SUCCESS",
            )
        except Exception as e:
            try:
                con.execute("ROLLBACK;")
            except Exception:
                pass
            duration = time.perf_counter() - start_time
            if isinstance(e, FaultInjectionError):
                return StrategyExecutionResult(
                    strategy_name="incremental_merge",
                    run_id=run_id,
                    records_loaded=0,
                    status_history_loaded=0,
                    rejects_loaded=0,
                    duration_seconds=duration,
                    status="FAILED",
                    error_message=str(e),
                )
            raise e
        finally:
            con.close()
