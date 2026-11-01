"""Configuration management with YAML loading and CLI override support."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONFIG_PATH = Path("config.yaml")


def load_config(config_path: str | Path | None = None) -> dict[str, Any]:
    """Load config from YAML file with fallback to default paths."""
    path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
    if not path.exists():
        # Fallback to local default dict if file missing
        return {
            "app": {
                "name": "load-strategy-comparison",
                "domain": "example.com",
                "company_name": "Data Strategy Project Lab",
                "contact_email": "compliance@example.com",
            },
            "paths": {
                "sample_dir": "data/sample",
                "expected_dir": "data/expected",
                "warehouse_dir": "data/warehouse",
                "duckdb_path": "data/warehouse/warehouse.duckdb",
                "partitions_dir": "data/warehouse/partitions",
                "reports_dir": "docs",
                "logs_dir": "logs",
            },
            "generator": {"seed": 42, "default_rows": 100000},
        }

    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    # Allow environment variable overrides for paths (DB_PATH or DUCKDB_PATH)
    env_db_path = os.environ.get("DB_PATH") or os.environ.get("DUCKDB_PATH")
    if env_db_path:
        data.setdefault("paths", {})["duckdb_path"] = env_db_path

    return data
