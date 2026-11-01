# PowerShell task runner for Load Strategy Comparison
param (
    [Parameter(Position=0)]
    [string]$Target = "help",
    [Parameter(Position=1)]
    [string]$Option = "sample"
)

$ErrorActionPreference = "Stop"

function Show-Help {
    Write-Host "Usage: .\run.ps1 <target> [option]" -ForegroundColor Cyan
    Write-Host "Targets:"
    Write-Host "  setup            - Install dependencies from requirements.txt"
    Write-Host "  generate         - Generate dataset (options: sample, large, or row count; default: sample)"
    Write-Host "  generate-sample  - Generate starter sample dataset (200 rows in data/sample)"
    Write-Host "  generate-large   - Generate large dataset (100,000 rows in data/large)"
    Write-Host "  run              - Execute strategies and reconciliation (options: sample, large, or path)"
    Write-Host "  run-sample       - Run and reconcile on starter sample (data/sample)"
    Write-Host "  run-large        - Run and reconcile on large dataset (data/large)"
    Write-Host "  test             - Run full test suite with coverage"
    Write-Host "  lint             - Check code style and formatting using ruff"
    Write-Host "  serve            - Start local FastAPI web dashboard"
}

switch ($Target.ToLower()) {
    "setup" {
        Write-Host "Installing dependencies from requirements.txt..." -ForegroundColor Cyan
        python -m pip install -r requirements.txt
    }
    "generate" {
        if ($Option -eq "large") {
            Write-Host "Generating large dataset (100,000 rows in data/large)..." -ForegroundColor Cyan
            python -m src.generator --preset large
        } elseif ($Option -match '^\d+$') {
            Write-Host "Generating custom dataset ($Option rows in data/custom)..." -ForegroundColor Cyan
            python -m src.generator --rows $Option --out data/custom
        } else {
            Write-Host "Generating starter sample dataset (200 rows in data/sample)..." -ForegroundColor Cyan
            python -m src.generator --preset sample
        }
        python -m src.generate_favicons
    }
    "generate-sample" {
        Write-Host "Generating starter sample dataset (200 rows in data/sample)..." -ForegroundColor Cyan
        python -m src.generator --preset sample
        python -m src.generate_favicons
    }
    "generate-large" {
        Write-Host "Generating large dataset (100,000 rows in data/large)..." -ForegroundColor Cyan
        python -m src.generator --preset large
        python -m src.generate_favicons
    }
    "run" {
        $runPath = "data/sample"
        if ($Option -eq "large") {
            $runPath = "data/large"
        } elseif ($Option -ne "sample" -and $Option -ne "") {
            $runPath = $Option
        }
        Write-Host "Executing load strategies and reconciliation on $runPath..." -ForegroundColor Cyan
        python -c "from src.pipeline import run_all_strategies_and_reconcile; r = run_all_strategies_and_reconcile('$runPath'); print('Cross-Strategy Equivalence:', r['cross_strategy_equivalence']); print('Dataset Hashes:', r['dataset_hashes'])"
    }
    "run-sample" {
        Write-Host "Executing load strategies on starter sample (data/sample)..." -ForegroundColor Cyan
        python -c "from src.pipeline import run_all_strategies_and_reconcile; r = run_all_strategies_and_reconcile('data/sample'); print('Cross-Strategy Equivalence:', r['cross_strategy_equivalence'])"
    }
    "run-large" {
        Write-Host "Executing load strategies on large dataset (data/large)..." -ForegroundColor Cyan
        python -c "from src.pipeline import run_all_strategies_and_reconcile; r = run_all_strategies_and_reconcile('data/large'); print('Cross-Strategy Equivalence:', r['cross_strategy_equivalence'])"
    }
    "test" {
        Write-Host "Running pytest with coverage..." -ForegroundColor Cyan
        pytest --cov=src --cov-report=term-missing --cov-report=html:docs/coverage_html
    }
    "lint" {
        Write-Host "Running ruff linter..." -ForegroundColor Cyan
        ruff check src tests verify_api.py
        ruff format --check src tests verify_api.py
    }
    "serve" {
        Write-Host "Starting local FastAPI server at http://127.0.0.1:8000..." -ForegroundColor Cyan
        uvicorn src.api.app:app --host 127.0.0.1 --port 8000 --reload
    }
    Default {
        Show-Help
    }
}

