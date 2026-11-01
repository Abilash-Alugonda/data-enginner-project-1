"""FastAPI application initialization, static mounts, and custom 404 handler."""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from src.api.routes import _render_page_shell, router


def create_app() -> FastAPI:
    """Build and configure FastAPI application."""
    app = FastAPI(
        title="Load Strategy Comparison API",
        description="REST API and static dashboard comparing full refresh, incremental merge, and partition replacement.",
        version="0.1.0",
    )

    static_dir = Path("static")
    if static_dir.exists():
        app.mount("/static", StaticFiles(directory="static"), name="static")

    app.include_router(router)

    @app.exception_handler(404)
    async def custom_404_handler(request: Request, exc: Exception) -> HTMLResponse:
        content = """
        <h1>404: Page Not Found</h1>
        <p>The requested URL was not found on this server.</p>
        <div class="card">
          <p>Please check the address or return to the <a href="/" class="btn">Overview Dashboard</a>.</p>
        </div>
        """
        html = _render_page_shell(
            title="404 Page Not Found",
            meta_desc="Requested page was not found.",
            canonical_path="/404",
            active_nav="",
            content_html=content,
        )
        return HTMLResponse(content=html, status_code=404)

    return app


app = create_app()
