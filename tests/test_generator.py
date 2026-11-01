"""Tests for synthetic dataset generator verifying byte-identical reproducibility."""

from __future__ import annotations

import hashlib
from pathlib import Path

from src.generator import generate_dataset


def _hash_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest()


def test_generator_byte_identical_reproducibility(tmp_path: Path) -> None:
    """Verify same seed produces byte-identical output across distinct generation runs."""
    dir1 = tmp_path / "run1"
    dir2 = tmp_path / "run2"

    counts1 = generate_dataset(total_rows=200, seed=42, out_dir=dir1)
    counts2 = generate_dataset(total_rows=200, seed=42, out_dir=dir2)

    assert counts1 == counts2

    for file_path in dir1.iterdir():
        if file_path.is_file():
            matching_file = dir2 / file_path.name
            assert matching_file.exists(), f"Missing file in run2: {file_path.name}"
            hash1 = _hash_file(file_path)
            hash2 = _hash_file(matching_file)
            assert hash1 == hash2, f"Byte mismatch detected for {file_path.name}"


def test_generator_different_seed_different_data(tmp_path: Path) -> None:
    """Verify different seeds produce divergent output."""
    dir1 = tmp_path / "seed42"
    dir2 = tmp_path / "seed99"

    generate_dataset(total_rows=200, seed=42, out_dir=dir1)
    generate_dataset(total_rows=200, seed=99, out_dir=dir2)

    hash_init_42 = _hash_file(dir1 / "orders_initial.jsonl")
    hash_init_99 = _hash_file(dir2 / "orders_initial.jsonl")

    assert hash_init_42 != hash_init_99


def test_generator_scalable_large_runs(tmp_path: Path) -> None:
    """Verify generator scales cleanly to larger configured row counts."""
    large_dir = tmp_path / "large_run"
    counts = generate_dataset(total_rows=1000, seed=42, out_dir=large_dir)

    assert counts["total_order_rows"] == 1000
    assert counts["customers"] == 50
    assert counts["products"] == 20
    assert counts["malformed_lines"] == 20
    assert counts["schema_change_rows"] == 30
    assert counts["updates"] == 150
    assert counts["initial_orders"] == 1000 - 150 - 30 - 20

    # Ensure all required files exist
    assert (large_dir / "customers.csv").exists()
    assert (large_dir / "products.csv").exists()
    assert (large_dir / "orders_initial.jsonl").exists()
    assert (large_dir / "orders_updates.csv").exists()
    assert (large_dir / "orders_schema_change.csv").exists()
    assert (large_dir / "orders_malformed.csv").exists()


def test_generator_starter_sample_preset(tmp_path: Path) -> None:
    """Verify starter sample produces exact 200 rows with 5 malformed records for fast review."""
    sample_dir = tmp_path / "sample_preset"
    counts = generate_dataset(total_rows=200, seed=42, out_dir=sample_dir)

    assert counts["total_order_rows"] == 200
    assert counts["customers"] == 25
    assert counts["products"] == 15
    assert counts["initial_orders"] == 140
    assert counts["updates"] == 40
    assert counts["schema_change_rows"] == 15
    assert counts["malformed_lines"] == 5
