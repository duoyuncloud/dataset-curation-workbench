from __future__ import annotations

import os
from pathlib import Path


def data_dir() -> Path:
    return Path(os.environ.get("DATA_DIR", "./data")).resolve()


def database_url() -> str | None:
    u = os.environ.get("DATABASE_URL", "").strip()
    return u or None
