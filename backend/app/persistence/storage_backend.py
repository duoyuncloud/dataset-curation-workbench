"""Pluggable storage for JSONL row sets; default is local filesystem under DATA_DIR."""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import pandas as pd


class StorageBackend(ABC):
    """Future: S3, GCS, etc. For now only LocalStorageBackend is implemented."""

    @abstractmethod
    def save_jsonl(self, relative_path: str, df: pd.DataFrame) -> None:
        """Write ``df`` as JSON lines at ``relative_path`` under the backend root."""

    @abstractmethod
    def load_jsonl(self, relative_path: str) -> pd.DataFrame:
        """Load JSON lines; missing file → empty DataFrame."""

    @abstractmethod
    def save_bytes(self, relative_path: str, data: bytes) -> None:
        """Binary blob (e.g. raw upload copy)."""

    @abstractmethod
    def exists(self, relative_path: str) -> bool:
        ...

    @abstractmethod
    def resolve_path(self, relative_path: str) -> Path:
        """Absolute path for ``relative_path`` under this backend (for streaming reads, etc.)."""


class LocalStorageBackend(StorageBackend):
    def __init__(self, root: Path) -> None:
        self._root = root.resolve()

    def _abs(self, relative_path: str) -> Path:
        p = (self._root / relative_path).resolve()
        if not str(p).startswith(str(self._root)):
            raise ValueError("path escapes root")
        return p

    def resolve_path(self, relative_path: str) -> Path:
        """Absolute path under this backend root (same rules as ``save_jsonl``)."""
        return self._abs(relative_path)

    def save_jsonl(self, relative_path: str, df: pd.DataFrame) -> None:
        path = self._abs(relative_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        if len(df) == 0:
            path.write_text("", encoding="utf-8")
            return
        df.to_json(path, orient="records", lines=True, force_ascii=False)

    def load_jsonl(self, relative_path: str) -> pd.DataFrame:
        path = self._abs(relative_path)
        if not path.is_file() or path.stat().st_size == 0:
            return pd.DataFrame()
        return pd.read_json(path, lines=True)

    def save_bytes(self, relative_path: str, data: bytes) -> None:
        path = self._abs(relative_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def exists(self, relative_path: str) -> bool:
        return self._abs(relative_path).is_file()
