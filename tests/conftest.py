"""Pytest fixtures providing clean test databases, directories, and mock batches."""

from __future__ import annotations

import csv
import os
from pathlib import Path

import pytest

from src.generator import generate_dataset
from src.ingestion.reader import ingest_file_or_dir
from src.storage.db import DatabaseManager
from src.validation.validator import DataValidator, ValidatedBatch


@pytest.fixture(autouse=True, scope="session")
def setup_test_env(tmp_path_factory: pytest.TempPathFactory) -> None:
    """Ensure test suite uses an isolated DuckDB instance so it never conflicts with a running server."""
    test_dir = tmp_path_factory.mktemp("db_env")
    test_db = test_dir / "test_isolated_warehouse.duckdb"
    os.environ["DB_PATH"] = str(test_db)
    os.environ["DUCKDB_PATH"] = str(test_db)
    from src.db.init_db import init_db

    init_db(test_db)


@pytest.fixture
def tmp_test_dir(tmp_path: Path) -> Path:
    """Provide clean temporary directory for isolated tests."""
    return tmp_path


@pytest.fixture
def test_db(tmp_test_dir: Path) -> DatabaseManager:
    """Provide isolated DuckDB instance."""
    db_path = tmp_test_dir / "test_warehouse.duckdb"
    return DatabaseManager(db_path)


@pytest.fixture(scope="session")
def sample_dataset_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Generate small deterministic starter dataset for tests."""
    out_dir = tmp_path_factory.mktemp("sample_data")
    generate_dataset(total_rows=200, seed=42, out_dir=out_dir)
    return out_dir


@pytest.fixture
def sample_batch(sample_dataset_dir: Path) -> ValidatedBatch:
    """Return validated batch from starter sample."""
    with open(sample_dataset_dir / "customers.csv", "r", encoding="utf-8") as f:
        cust_ids = {r["customer_id"] for r in csv.DictReader(f)}
    with open(sample_dataset_dir / "products.csv", "r", encoding="utf-8") as f:
        prod_ids = {r["product_id"] for r in csv.DictReader(f)}

    raw_records = ingest_file_or_dir(sample_dataset_dir)
    validator = DataValidator(cust_ids, prod_ids)
    return validator.validate_batch(raw_records, "TEST-RUN-FIXTURE")
