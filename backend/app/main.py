"""SFT Dataset Curation Workbench — FastAPI app."""

from __future__ import annotations

import contextlib
import os
from pathlib import Path
from typing import Any

from fastapi import APIRouter, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .filters.pipeline import REGISTRY
from .persistence.task_service import TaskService
from .routes_tasks import register_task_routes

# Filter groupings for UI (GET /filters?grouped=true) — question+response SFT, no eval metadata
FILTER_GROUPS: dict[str, list[str]] = {
    "cleanup": [
        "remove_hacking",
        "remove_duplicates",
    ],
    "validity": [
        "format_validity",
        "length_anomaly",
    ],
    "balancing": [
        "random_drop",
        "balance_to_mean",
    ],
    "script": [
        "custom_script",
    ],
}

task_svc = TaskService()

API_VERSION = "0.1.0"


@contextlib.asynccontextmanager
async def _lifespan(_app: FastAPI):
    task_svc.initialize()
    yield


app = FastAPI(
    title="SFT Dataset Curation Workbench",
    description="Transparent, step-by-step SFT dataset curation. Every filter creates a new stage.",
    version=API_VERSION,
    lifespan=_lifespan,
)
api_router = APIRouter()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


register_task_routes(api_router, task_svc)


@api_router.get("/filters")
def list_filters(grouped: bool = False) -> dict[str, Any]:
    keys = sorted(REGISTRY.keys())
    out: dict[str, Any] = {"filters": keys}
    if grouped:
        out["groups"] = {g: [f for f in fl if f in REGISTRY] for g, fl in FILTER_GROUPS.items()}
    return out


@api_router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@api_router.get("/version")
def version_info() -> dict[str, str]:
    return {
        "version": os.environ.get("APP_VERSION", API_VERSION),
        "build_time": os.environ.get("BUILD_TIME", "unknown"),
    }


app.include_router(api_router, prefix="/api")


def _install_frontend(spa: FastAPI) -> None:
    """
    Serve Vite production build at / and /assets/...; SPA fallback for client routes.
    Skips if frontend/dist is missing (local dev uses Vite on :5173).
    """
    root = Path(__file__).resolve().parents[2] / "frontend" / "dist"
    if not root.is_dir():
        return
    base = root.resolve()
    assets = root / "assets"
    if assets.is_dir():
        spa.mount("/assets", StaticFiles(directory=assets), name="static_assets")

    @spa.get("/")
    def _index() -> FileResponse:
        return FileResponse(root / "index.html")

    @spa.get("/{full_path:path}")
    def _spa_catchall(full_path: str) -> FileResponse:
        if full_path.startswith("api"):
            raise HTTPException(404, "not found")
        p = (root / full_path).resolve()
        try:
            p.relative_to(base)
        except ValueError:
            return FileResponse(root / "index.html")
        if p.is_file():
            return FileResponse(p)
        return FileResponse(root / "index.html")


_install_frontend(app)
