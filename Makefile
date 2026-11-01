.PHONY: setup generate generate-sample generate-large run run-sample run-large test lint serve help

help:
	@echo "Available Makefile targets:"
	@echo "  setup            Install dependencies from requirements.txt"
	@echo "  generate         Generate starter sample dataset (200 rows in data/sample)"
	@echo "  generate-sample  Generate starter sample dataset (200 rows in data/sample)"
	@echo "  generate-large   Generate large dataset (100,000 rows in data/large)"
	@echo "  run              Execute load strategies and reconciliation on data/sample"
	@echo "  run-sample       Execute load strategies on starter sample (data/sample)"
	@echo "  run-large        Execute load strategies on large dataset (data/large)"
	@echo "  test             Run pytest test suite with coverage report"
	@echo "  lint             Run ruff check and formatting verification"
	@echo "  serve            Start local FastAPI server on port 8000"

setup:
	python -m pip install -r requirements.txt

generate:
	python -m src.generator --preset sample
	python -m src.generate_favicons

generate-sample:
	python -m src.generator --preset sample
	python -m src.generate_favicons

generate-large:
	python -m src.generator --preset large
	python -m src.generate_favicons

run:
	python -c "from src.pipeline import run_all_strategies_and_reconcile; r = run_all_strategies_and_reconcile('data/sample'); print('Cross-Strategy Equivalence:', r['cross_strategy_equivalence'])"

run-sample:
	python -c "from src.pipeline import run_all_strategies_and_reconcile; r = run_all_strategies_and_reconcile('data/sample'); print('Cross-Strategy Equivalence:', r['cross_strategy_equivalence'])"

run-large:
	python -c "from src.pipeline import run_all_strategies_and_reconcile; r = run_all_strategies_and_reconcile('data/large'); print('Cross-Strategy Equivalence:', r['cross_strategy_equivalence'])"

test:
	pytest --cov=src --cov-report=term-missing --cov-report=html:docs/coverage_html

lint:
	ruff check src tests verify_api.py
	ruff format --check src tests verify_api.py

serve:
	uvicorn src.api.app:app --host 127.0.0.1 --port 8000 --reload


