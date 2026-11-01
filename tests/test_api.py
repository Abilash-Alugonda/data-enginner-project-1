"""Tests for FastAPI endpoints and static web pages."""

from __future__ import annotations

from fastapi.testclient import TestClient

from src.api.app import app

client = TestClient(app)


def test_api_health() -> None:
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert data["app"] == "load-strategy-comparison"


def test_api_runs_and_trigger() -> None:
    # Trigger a run via POST /runs with preset
    post_resp = client.post("/runs", json={"strategy": "full_refresh", "preset": "sample"})
    assert post_resp.status_code == 200
    run_data = post_resp.json()
    assert run_data["status"] == "SUCCESS"
    run_id = run_data["run_id"]

    # Retrieve all runs
    list_resp = client.get("/runs")
    assert list_resp.status_code == 200
    runs = list_resp.json()
    assert any(r["run_id"] == run_id for r in runs)

    # Retrieve single run by exact ID
    single_resp = client.get(f"/runs/{run_id}")
    assert single_resp.status_code == 200
    assert single_resp.json()["run_id"] == run_id

    # Retrieve run by index alias (e.g. /runs/1)
    index_resp = client.get("/runs/1")
    assert index_resp.status_code == 200
    assert "status" in index_resp.json()


def test_api_rejects() -> None:
    resp = client.get("/rejects")
    assert resp.status_code == 200
    rejects = resp.json()
    assert isinstance(rejects, list)


def test_api_reconciliation_latest() -> None:
    resp = client.get("/reconciliation/latest")
    assert resp.status_code == 200
    data = resp.json()
    assert "source_count" in data or data.get("status") == "empty"


def test_api_benchmarks() -> None:
    resp = client.get("/benchmarks")
    assert resp.status_code == 200
    benchmarks = resp.json()
    assert isinstance(benchmarks, list)


def test_html_pages() -> None:
    pages = [
        "/",
        "/runs-view",
        "/rejects-view",
        "/reconciliation-view",
        "/benchmarks-view",
        "/privacy",
        "/terms",
    ]
    for p in pages:
        resp = client.get(p)
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]
        assert "<!DOCTYPE html>" in resp.text


def test_static_files_and_404() -> None:
    # Robots and sitemap
    r_robots = client.get("/robots.txt")
    assert r_robots.status_code == 200
    assert "User-agent:" in r_robots.text

    r_sitemap = client.get("/sitemap.xml")
    assert r_sitemap.status_code == 200
    assert "<urlset" in r_sitemap.text

    # Favicon SVG
    r_fav = client.get("/static/favicon.svg")
    assert r_fav.status_code == 200
    assert "<svg" in r_fav.text

    # Custom 404
    r_404 = client.get("/this-route-does-not-exist")
    assert r_404.status_code == 404
    assert "404: Page Not Found" in r_404.text
