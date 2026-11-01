#!/usr/bin/env bash
set -euo pipefail

TARGET="${1:-help}"
OPTION="${2:-sample}"

case "$TARGET" in
  setup)
    echo "Installing dependencies from requirements.txt..."
    python -m pip install -r requirements.txt
    ;;
  generate)
    if [ "$OPTION" = "large" ]; then
      echo "Generating large dataset (100,000 rows in data/large)..."
      python -m src.generator --preset large
    elif [[ "$OPTION" =~ ^[0-9]+$ ]]; then
      echo "Generating custom dataset ($OPTION rows in data/custom)..."
      python -m src.generator --rows "$OPTION" --out data/custom
    else
      echo "Generating starter sample dataset (200 rows in data/sample)..."
      python -m src.generator --preset sample
    fi
    python -m src.generate_favicons
    ;;
  generate-sample)
    echo "Generating starter sample dataset (200 rows in data/sample)..."
    python -m src.generator --preset sample
    python -m src.generate_favicons
    ;;
  generate-large)
    echo "Generating large dataset (100,000 rows in data/large)..."
    python -m src.generator --preset large
    python -m src.generate_favicons
    ;;
  run)
    RUN_PATH="data/sample"
    if [ "$OPTION" = "large" ]; then
      RUN_PATH="data/large"
    elif [ "$OPTION" != "sample" ] && [ -n "$OPTION" ]; then
      RUN_PATH="$OPTION"
    fi
    echo "Executing load strategies and reconciliation on $RUN_PATH..."
    python -c "from src.pipeline import run_all_strategies_and_reconcile; r = run_all_strategies_and_reconcile('$RUN_PATH'); print('Cross-Strategy Equivalence:', r['cross_strategy_equivalence']); print('Dataset Hashes:', r['dataset_hashes'])"
    ;;
  run-sample)
    echo "Executing load strategies on starter sample (data/sample)..."
    python -c "from src.pipeline import run_all_strategies_and_reconcile; r = run_all_strategies_and_reconcile('data/sample'); print('Cross-Strategy Equivalence:', r['cross_strategy_equivalence'])"
    ;;
  run-large)
    echo "Executing load strategies on large dataset (data/large)..."
    python -c "from src.pipeline import run_all_strategies_and_reconcile; r = run_all_strategies_and_reconcile('data/large'); print('Cross-Strategy Equivalence:', r['cross_strategy_equivalence'])"
    ;;
  test)
    echo "Running full test suite with coverage..."
    pytest --cov=src --cov-report=term-missing --cov-report=html:docs/coverage_html
    ;;
  lint)
    echo "Checking code formatting and quality with ruff..."
    ruff check src tests verify_api.py
    ruff format --check src tests verify_api.py
    ;;
  serve)
    echo "Starting local FastAPI server at http://127.0.0.1:8000..."
    uvicorn src.api.app:app --host 127.0.0.1 --port 8000 --reload
    ;;
  *)
    echo "Usage: ./run.sh [setup|generate|generate-sample|generate-large|run|run-sample|run-large|test|lint|serve] [sample|large|<rows>|<path>]"
    exit 1
    ;;
esac

