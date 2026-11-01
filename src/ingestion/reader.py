"""Ingestion reader supporting CSV and JSONL files with line tracking and parse error isolation."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any


class RawRecord:
    """Represents a raw record read from a source file before validation."""

    def __init__(
        self,
        source_file: str,
        source_line: int,
        raw_payload: str,
        data: dict[str, Any] | None = None,
        parse_error: str | None = None,
    ) -> None:
        self.source_file = source_file
        self.source_line = source_line
        self.raw_payload = raw_payload
        self.data = data
        self.parse_error = parse_error


def read_jsonl(path: Path) -> list[RawRecord]:
    """Read a JSONL file line by line with error isolation."""
    records: list[RawRecord] = []
    source_file = path.name
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line_num, line in enumerate(f, start=1):
            raw = line.rstrip("\r\n")
            if not raw.strip():
                continue
            try:
                parsed = json.loads(raw)
                if not isinstance(parsed, dict):
                    records.append(
                        RawRecord(
                            source_file=source_file,
                            source_line=line_num,
                            raw_payload=raw,
                            parse_error="MALFORMED_LINE: Expected JSON object",
                        )
                    )
                else:
                    records.append(
                        RawRecord(
                            source_file=source_file,
                            source_line=line_num,
                            raw_payload=raw,
                            data=parsed,
                        )
                    )
            except Exception as e:
                records.append(
                    RawRecord(
                        source_file=source_file,
                        source_line=line_num,
                        raw_payload=raw,
                        parse_error=f"MALFORMED_LINE: {e}",
                    )
                )
    return records


def read_csv(path: Path) -> list[RawRecord]:
    """Read a CSV file with line tracking, header checking, and malformed line capture."""
    records: list[RawRecord] = []
    source_file = path.name

    with open(path, "r", encoding="utf-8", errors="replace") as f:
        # Read header
        header_raw = f.readline()
        if not header_raw:
            return records
        header_raw = header_raw.rstrip("\r\n")
        try:
            header_reader = csv.reader([header_raw])
            headers = next(header_reader)
        except Exception as e:
            records.append(
                RawRecord(
                    source_file=source_file,
                    source_line=1,
                    raw_payload=header_raw,
                    parse_error=f"MALFORMED_LINE: Header parse failed: {e}",
                )
            )
            return records

        for line_num, line in enumerate(f, start=2):
            line_str = line.rstrip("\r\n")
            if not line_str.strip():
                continue
            try:
                row_reader = csv.reader([line_str])
                row_values = next(row_reader)
                if len(row_values) != len(headers):
                    records.append(
                        RawRecord(
                            source_file=source_file,
                            source_line=line_num,
                            raw_payload=line_str,
                            parse_error=f"MALFORMED_LINE: Column count mismatch (expected {len(headers)}, got {len(row_values)})",
                        )
                    )
                else:
                    row_dict = dict(zip(headers, row_values))
                    records.append(
                        RawRecord(
                            source_file=source_file,
                            source_line=line_num,
                            raw_payload=line_str,
                            data=row_dict,
                        )
                    )
            except Exception as e:
                records.append(
                    RawRecord(
                        source_file=source_file,
                        source_line=line_num,
                        raw_payload=line_str,
                        parse_error=f"MALFORMED_LINE: {e}",
                    )
                )

    return records


def ingest_file_or_dir(target_path: str | Path) -> list[RawRecord]:
    """Ingest CSV and JSONL files from a path or directory in deterministic order."""
    path = Path(target_path)
    if not path.exists():
        raise FileNotFoundError(f"Path does not exist: {target_path}")

    files: list[Path] = []
    if path.is_file():
        files = [path]
    else:
        # Collect all jsonl and csv files, sorted by name
        files = sorted(
            [
                p
                for p in path.iterdir()
                if p.is_file() and p.suffix.lower() in {".csv", ".jsonl", ".ndjson"}
            ]
        )

    all_records: list[RawRecord] = []
    for f in files:
        # Skip reference tables customers.csv and products.csv if in same dir
        if f.name in {"customers.csv", "products.csv"}:
            continue
        if f.suffix.lower() in {".jsonl", ".ndjson"}:
            all_records.extend(read_jsonl(f))
        elif f.suffix.lower() == ".csv":
            all_records.extend(read_csv(f))
    return all_records
