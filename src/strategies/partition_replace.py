"""Partition replacement load strategy.

Rewrites only affected order_date partitions (Parquet files), atomically via temp directory and rename.
"""

from __future__ import annotations

import datetime
import os
import shutil
import stat
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from src.models import Order
from src.strategies.base import BaseLoadStrategy, FaultInjectionError, StrategyExecutionResult
from src.validation.validator import ValidatedBatch


def safe_rmtree(path: Path) -> None:
    """Safely remove a directory tree handling Windows read-only flags."""

    def on_rm_error(func: Any, p: str, exc_info: Any) -> None:
        try:
            os.chmod(p, stat.S_IWRITE)
            func(p)
        except Exception:
            pass

    if path.exists():
        shutil.rmtree(str(path), onerror=on_rm_error)


ARROW_SCHEMA = pa.schema(
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


class PartitionReplaceStrategy(BaseLoadStrategy):
    """Partition replacement strategy operating on daily Parquet partitions."""

    def __init__(
        self,
        db_manager: Any,
        partitions_dir: str | Path = "data/warehouse/partitions/orders",
        temp_dir: str | Path = "data/warehouse/tmp_partitions",
    ) -> None:
        super().__init__(db_manager)
        self.partitions_dir = Path(partitions_dir)
        self.partitions_dir.mkdir(parents=True, exist_ok=True)
        self.temp_dir = Path(temp_dir)
        self.temp_dir.mkdir(parents=True, exist_ok=True)

    def load(
        self,
        batch: ValidatedBatch,
        run_id: str,
        fail_after: int | None = None,
    ) -> StrategyExecutionResult:
        start_time = time.perf_counter()
        run_temp_dir = self.temp_dir / run_id
        run_temp_dir.mkdir(parents=True, exist_ok=True)

        try:
            # 1. Group incoming valid orders by partition (order_date)
            incoming_by_date: dict[str, list[Order]] = defaultdict(list)
            for o in batch.valid_orders:
                incoming_by_date[o.order_date].append(o)

            # Fault injection counter
            processed_records = 0

            # 2. Stage new parquet partitions in run_temp_dir
            staged_partitions: list[tuple[str, Path, Path]] = []

            for order_date, incoming_orders in sorted(incoming_by_date.items()):
                target_part_dir = self.partitions_dir / f"order_date={order_date}"
                temp_part_dir = run_temp_dir / f"order_date={order_date}"
                temp_part_dir.mkdir(parents=True, exist_ok=True)
                temp_file = temp_part_dir / "data.parquet"

                # Check if partition already exists in warehouse
                merged_orders: dict[str, Order] = {}
                target_file = target_part_dir / "data.parquet"
                if target_file.exists():
                    table = pq.read_table(str(target_file))
                    pydict = table.to_pydict()
                    for i in range(len(pydict["order_id"])):
                        oid = pydict["order_id"][i]
                        merged_orders[oid] = Order(
                            order_id=oid,
                            customer_id=pydict["customer_id"][i],
                            product_id=pydict["product_id"][i],
                            quantity=int(pydict["quantity"][i]),
                            order_amount=float(pydict["order_amount"][i]),
                            order_status=pydict["order_status"][i],
                            order_date=pydict["order_date"][i],
                            updated_at=pydict["updated_at"][i],
                            run_id=pydict["run_id"][i],
                            source_file=pydict["source_file"][i],
                        )

                # Merge incoming orders (latest updated_at wins)
                for inc in incoming_orders:
                    if inc.order_id not in merged_orders:
                        merged_orders[inc.order_id] = inc
                    else:
                        existing = merged_orders[inc.order_id]
                        if inc.updated_at >= existing.updated_at:
                            merged_orders[inc.order_id] = inc

                # Fault injection check during staging
                processed_records += len(incoming_orders)
                if fail_after is not None and processed_records >= fail_after:
                    raise FaultInjectionError(
                        f"Fault injected: simulated failure after {fail_after} records in partition_replace staging"
                    )

                # Write staged partition parquet
                sorted_orders = sorted(merged_orders.values(), key=lambda x: x.order_id)
                data_dict = {
                    "order_id": [o.order_id for o in sorted_orders],
                    "customer_id": [o.customer_id for o in sorted_orders],
                    "product_id": [o.product_id for o in sorted_orders],
                    "quantity": [o.quantity for o in sorted_orders],
                    "order_amount": [o.order_amount for o in sorted_orders],
                    "order_status": [o.order_status for o in sorted_orders],
                    "order_date": [o.order_date for o in sorted_orders],
                    "updated_at": [o.updated_at for o in sorted_orders],
                    "run_id": [o.run_id for o in sorted_orders],
                    "source_file": [o.source_file for o in sorted_orders],
                }
                arrow_table = pa.Table.from_pydict(data_dict, schema=ARROW_SCHEMA)
                pq.write_table(arrow_table, str(temp_file))

                staged_partitions.append((order_date, temp_part_dir, target_part_dir))

            # 3. Atomically replace each affected partition
            for order_date, temp_part_dir, target_part_dir in staged_partitions:
                backup_part_dir = self.temp_dir / f"bak_{run_id}_{order_date}"
                if target_part_dir.exists():
                    # Move target to backup
                    shutil.move(str(target_part_dir), str(backup_part_dir))
                # Move staged temp to target
                shutil.move(str(temp_part_dir), str(target_part_dir))
                # Cleanup backup
                if backup_part_dir.exists():
                    safe_rmtree(backup_part_dir)

            # 4. Synchronize DuckDB orders table and register partitions inside transaction
            con = self.db.get_connection()
            try:
                con.execute("BEGIN TRANSACTION;")
                parquet_pattern = (self.partitions_dir / "*" / "*.parquet").as_posix()
                # Check if any parquet files exist
                existing_files = list(self.partitions_dir.glob("*/*.parquet"))
                con.execute("DROP TABLE IF EXISTS orders;")
                if existing_files:
                    con.execute(
                        f"""
                        CREATE TABLE orders AS
                        SELECT
                            order_id,
                            customer_id,
                            product_id,
                            CAST(quantity AS BIGINT) as quantity,
                            CAST(order_amount AS DOUBLE) as order_amount,
                            order_status,
                            CAST(order_date AS VARCHAR) as order_date,
                            updated_at,
                            run_id,
                            source_file
                        FROM read_parquet('{parquet_pattern}')
                        ORDER BY order_id;
                        """
                    )
                else:
                    self.db.init_schema()

                # Register touched partitions in orders_partition_index table inside the transaction
                for order_date, _, target_part_dir in staged_partitions:
                    part_file = target_part_dir / "data.parquet"
                    part_rec_count = 0
                    if part_file.exists():
                        table = pq.read_table(str(part_file))
                        part_rec_count = table.num_rows
                    now_utc = datetime.datetime.now(datetime.timezone.utc).isoformat()
                    con.execute(
                        """
                        INSERT OR REPLACE INTO orders_partition_index (
                            partition_key, partition_date, file_path, row_count, record_count, updated_at, last_updated_at, run_id
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                        """,
                        [
                            order_date,
                            order_date,
                            part_file.as_posix(),
                            part_rec_count,
                            part_rec_count,
                            now_utc,
                            now_utc,
                            run_id,
                        ],
                    )

                # Record rejects and status history
                self.record_rejects(batch.rejects, con)
                self.record_status_history(batch.status_history, con)
                con.execute("COMMIT;")
            except Exception:
                try:
                    con.execute("ROLLBACK;")
                except Exception:
                    pass
                raise
            finally:
                con.close()

            duration = time.perf_counter() - start_time
            return StrategyExecutionResult(
                strategy_name="partition_replace",
                run_id=run_id,
                records_loaded=len(batch.valid_orders),
                status_history_loaded=len(batch.status_history),
                rejects_loaded=len(batch.rejects),
                duration_seconds=duration,
                status="SUCCESS",
            )
        except Exception as e:
            # Cleanup temp directory on failure
            if run_temp_dir.exists():
                safe_rmtree(run_temp_dir)
            duration = time.perf_counter() - start_time
            if isinstance(e, FaultInjectionError):
                return StrategyExecutionResult(
                    strategy_name="partition_replace",
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
            if run_temp_dir.exists():
                safe_rmtree(run_temp_dir)
