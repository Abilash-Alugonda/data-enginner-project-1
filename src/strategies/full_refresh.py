"""Full refresh load strategy.

Rebuilds target from all source data in a staging table, then swaps atomically.
"""

from __future__ import annotations

import time

from src.strategies.base import (
    BaseLoadStrategy,
    FaultInjectionError,
    StrategyExecutionResult,
    orders_to_arrow,
)
from src.validation.validator import ValidatedBatch


class FullRefreshStrategy(BaseLoadStrategy):
    """Full refresh strategy implementing atomic staging rebuild and swap."""

    def load(
        self,
        batch: ValidatedBatch,
        run_id: str,
        fail_after: int | None = None,
    ) -> StrategyExecutionResult:
        start_time = time.perf_counter()
        con = self.db.get_connection()
        try:
            # 1. Create staging table with identical schema as orders
            con.execute("DROP TABLE IF EXISTS orders_staging;")
            con.execute(
                """
                CREATE TABLE orders_staging (
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

            # Fault injection check during staging load
            if fail_after is not None and len(batch.valid_orders) >= fail_after:
                raise FaultInjectionError(
                    f"Fault injected: simulated failure after {fail_after} records in full_refresh staging load"
                )

            # 2. Populate staging table via Arrow vectorization
            if batch.valid_orders:
                tbl = orders_to_arrow(batch.valid_orders)
                con.register("tmp_staging_arrow", tbl)
                con.execute("INSERT INTO orders_staging SELECT * FROM tmp_staging_arrow;")
                con.unregister("tmp_staging_arrow")

            # 3. Atomic swap
            con.execute("BEGIN TRANSACTION;")
            con.execute("DROP TABLE IF EXISTS orders;")
            con.execute("ALTER TABLE orders_staging RENAME TO orders;")
            con.execute("COMMIT;")

            # 4. Record rejects and status history
            self.record_rejects(batch.rejects, con)
            self.record_status_history(batch.status_history, con)

            duration = time.perf_counter() - start_time
            return StrategyExecutionResult(
                strategy_name="full_refresh",
                run_id=run_id,
                records_loaded=len(batch.valid_orders),
                status_history_loaded=len(batch.status_history),
                rejects_loaded=len(batch.rejects),
                duration_seconds=duration,
                status="SUCCESS",
            )
        except Exception as e:
            # Cleanup staging on failure
            try:
                con.execute("DROP TABLE IF EXISTS orders_staging;")
            except Exception:
                pass
            duration = time.perf_counter() - start_time
            if isinstance(e, FaultInjectionError):
                return StrategyExecutionResult(
                    strategy_name="full_refresh",
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
