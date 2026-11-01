"""API verification script testing every endpoint and saving results to docs/api_check.txt."""

import os
import tempfile
from pathlib import Path

# Use isolated DB if default warehouse is locked by running server
if "DUCKDB_PATH" not in os.environ and "DB_PATH" not in os.environ:
    _tmp_dir = tempfile.mkdtemp(prefix="verify_db_")
    db_p = str(Path(_tmp_dir) / "verify_api.duckdb")
    os.environ["DUCKDB_PATH"] = db_p
    os.environ["DB_PATH"] = db_p
    from src.db.init_db import init_db

    init_db(db_p)

from fastapi.testclient import TestClient

from src.api.app import app


def run_api_check() -> None:
    client = TestClient(app)
    out_lines: list[str] = [
        "API AND ENDPOINTS VERIFICATION REPORT",
        "=====================================",
        "",
    ]

    endpoints = [
        ("GET", "/health", None),
        ("POST", "/runs", {"strategy": "full_refresh", "input_path": "data/sample"}),
        ("GET", "/runs", None),
        ("GET", "/runs/1", None),
        ("GET", "/rejects", None),
        ("GET", "/rejects?run_id=1", None),
        ("GET", "/reconciliation/latest", None),
        ("GET", "/benchmarks", None),
        ("GET", "/", None),
        ("GET", "/runs-view", None),
        ("GET", "/rejects-view", None),
        ("GET", "/reconciliation-view", None),
        ("GET", "/benchmarks-view", None),
        ("GET", "/privacy", None),
        ("GET", "/terms", None),
        ("GET", "/robots.txt", None),
        ("GET", "/sitemap.xml", None),
        ("GET", "/static/favicon.svg", None),
        ("GET", "/static/favicon-32x32.png", None),
        ("GET", "/static/favicon.ico", None),
        ("GET", "/non-existent-url-testing-404", None),
    ]

    for method, path, json_body in endpoints:
        if method == "GET":
            resp = client.get(path)
        else:
            resp = client.post(path, json=json_body)

        status_code = resp.status_code
        content_type = resp.headers.get("content-type", "")
        body_preview = resp.text[:120].replace("\n", " ").replace("\r", "")

        out_lines.append(f"[{method}] {path}")
        out_lines.append(f"Status: {status_code}")
        out_lines.append(f"Content-Type: {content_type}")
        out_lines.append(f"Response Preview: {body_preview}...")
        out_lines.append("-" * 50)

    # Test single run endpoint with the run_id from /runs
    runs_resp = client.get("/runs")
    if runs_resp.status_code == 200 and runs_resp.json():
        sample_run_id = runs_resp.json()[0]["run_id"]
        run_resp = client.get(f"/runs/{sample_run_id}")
        out_lines.append(f"[GET] /runs/{sample_run_id}")
        out_lines.append(f"Status: {run_resp.status_code}")
        out_lines.append(f"Content-Type: {run_resp.headers.get('content-type', '')}")
        out_lines.append(f"Response: {run_resp.text[:120]}...")
        out_lines.append("-" * 50)

    docs_dir = Path("docs")
    docs_dir.mkdir(parents=True, exist_ok=True)
    report_file = docs_dir / "api_check.txt"
    with open(report_file, "w", encoding="utf-8") as f:
        f.write("\n".join(out_lines) + "\n")

    print(f"API verification complete. Saved to {report_file}")


if __name__ == "__main__":
    run_api_check()
