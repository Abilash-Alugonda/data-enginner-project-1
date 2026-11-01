"""Observability module providing structured JSON logging and DuckDB run_log records."""

from __future__ import annotations

import json
from pathlib import Path

from src.models import RunLog
from src.storage.db import DatabaseManager


class PipelineLogger:
    """Handles structured execution logging to disk and database."""

    def __init__(
        self,
        db_manager: DatabaseManager,
        logs_dir: str | Path = "logs",
    ) -> None:
        self.db = db_manager
        self.logs_dir = Path(logs_dir)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.jsonl_file = self.logs_dir / "runs.jsonl"

    def log_run(
        self,
        run_id: str,
        strategy: str,
        start_time: str,
        end_time: str,
        input_count: int,
        output_count: int,
        rejected_count: int,
        status: str,
        failure_reason: str | None = None,
    ) -> RunLog:
        """Record run entry in DuckDB run_log table and append JSON line to disk."""
        log_entry = RunLog(
            run_id=run_id,
            strategy=strategy,  # type: ignore
            start_time=start_time,
            end_time=end_time,
            input_count=input_count,
            output_count=output_count,
            rejected_count=rejected_count,
            status=status,  # type: ignore
            failure_reason=failure_reason,
        )

        # 1. Write to DuckDB run_log table
        with self.db.get_connection() as con:
            con.execute(
                """
                INSERT OR REPLACE INTO run_log (
                    run_id, strategy, start_time, end_time, input_count,
                    output_count, rejected_count, status, failure_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                [
                    log_entry.run_id,
                    log_entry.strategy,
                    log_entry.start_time,
                    log_entry.end_time,
                    log_entry.input_count,
                    log_entry.output_count,
                    log_entry.rejected_count,
                    log_entry.status,
                    log_entry.failure_reason,
                ],
            )

        # 2. Append JSON line
        with open(self.jsonl_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(log_entry.model_dump(), separators=(",", ":")) + "\n")

        return log_entry
