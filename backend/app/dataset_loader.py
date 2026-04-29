"""Load JSONL datasets into pandas with stable row ids."""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Awaitable
from io import StringIO
from pathlib import Path
from typing import Callable, Optional

import pandas as pd
from starlette.datastructures import UploadFile

from .persistence.config import data_dir


REQUIRED_NAMED_FIELDS = [
    "prompt",
    "response",
    "source_model",
    "signature",
    "problem_type",
    "behavior_type",
    "correctness",
    "runtime_ms",
    "compiled",
    "hacked",
    "duplicate",
    "metadata",
]
# Allow prompt/question, response, etc. — normalized after load


def _normalize_row(rec: dict) -> dict:
    # Real data: question + response (Megatron SFT)
    if "question" in rec and "prompt" not in rec:
        rec = {**rec, "prompt": rec.get("question", "")}
    if "prompt" in rec and "question" not in rec:
        rec = {**rec, "question": rec.get("prompt", "")}
    if "response" not in rec and "output" in rec:
        rec = {**rec, "response": rec.get("output", "")}
    for k in REQUIRED_NAMED_FIELDS:
        if k not in rec and k in ("hacked", "duplicate", "compiled", "correctness", "metadata"):
            rec[k] = None
        elif k not in rec:
            rec[k] = None
    return rec


def load_jsonl_bytes(data: bytes | bytearray) -> pd.DataFrame:
    text = data.decode("utf-8", errors="replace")
    return load_jsonl_string(text)


def load_jsonl_string(text: str) -> pd.DataFrame:
    rows = []
    for line in StringIO(text):
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(_normalize_row(json.loads(line)))
        except json.JSONDecodeError:
            continue
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    # Stable ids for transparency
    df.insert(0, "_row_id", range(len(df)))
    return df


CHUNK = 8 * 1024 * 1024  # 8 MiB — parse line-by-line without a second full-file `str` copy


ProgressCbAsync = Optional[Callable[[float, str], Awaitable[None] | None]]
ProgressCbSync = Optional[Callable[[float, str], None]]


async def load_jsonl_from_upload_file(
    file: UploadFile,
    *,
    expected_total_bytes: int = 0,
    on_progress: ProgressCbAsync = None,
) -> pd.DataFrame:
    """
    Stream the upload in chunks and parse JSON lines. Avoids `bytes.decode("utf-8")` of the
    whole file, which can double memory and fail for multi-hundred-MB JSONL.
    ``on_progress(frac, message)`` is called after each chunk; ``frac`` is 0–1 when
    ``expected_total_bytes`` > 0 (from client Content-Length / X-Expected-Size), else a soft ramp.
    """
    rows: list[dict] = []
    buf = b""
    bytes_read = 0

    async def emit(frac: float, msg: str) -> None:
        if not on_progress:
            return
        r = on_progress(min(1.0, max(0.0, frac)), msg)
        if asyncio.iscoroutine(r):
            await r

    while True:
        chunk = await file.read(CHUNK)
        if not chunk:
            break
        bytes_read += len(chunk)
        if expected_total_bytes > 0:
            await emit(bytes_read / expected_total_bytes, "Receiving and parsing JSONL…")
        else:
            await emit(min(0.92, bytes_read / max(bytes_read + 8 * CHUNK, 1)), "Receiving and parsing JSONL…")
        buf += chunk
        while b"\n" in buf:
            line, buf = buf.split(b"\n", 1)
            if not line.strip():
                continue
            try:
                rec = json.loads(line.decode("utf-8", errors="replace"))
            except json.JSONDecodeError:
                continue
            rows.append(_normalize_row(rec))
    if buf.strip():
        try:
            rec = json.loads(buf.decode("utf-8", errors="replace"))
            rows.append(_normalize_row(rec))
        except json.JSONDecodeError:
            pass
    await emit(1.0, "Parsing complete…")
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df.insert(0, "_row_id", range(len(df)))
    return df


def resolve_jsonl_import_path(raw: str) -> Path:
    """
    Resolve a server-local JSONL path for import.

    - Relative paths are resolved under ``DATA_DIR``.
    - Absolute paths are allowed only when ``ALLOW_JSONL_IMPORT_ANYWHERE`` is set (e.g. ``1``);
      otherwise the resolved path must lie under ``DATA_DIR``.
    """
    raw_stripped = (raw or "").strip()
    if not raw_stripped:
        raise ValueError("path is empty")
    p = Path(raw_stripped).expanduser()
    if not p.is_absolute():
        p = (data_dir() / p).resolve()
    else:
        p = p.resolve()
    allow_any = os.environ.get("ALLOW_JSONL_IMPORT_ANYWHERE", "").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    root = data_dir().resolve()
    if not allow_any:
        try:
            p.relative_to(root)
        except ValueError as e:
            raise ValueError(
                "path must be under DATA_DIR or use a relative path; "
                "set ALLOW_JSONL_IMPORT_ANYWHERE=1 to allow absolute paths outside DATA_DIR"
            ) from e
    if not p.is_file():
        raise ValueError(f"not a file or missing: {p}")
    return p


def load_jsonl_from_path(path: Path, on_progress: ProgressCbSync = None) -> pd.DataFrame:
    """Stream-read JSONL from disk (same chunk strategy as multipart upload)."""
    rows: list[dict] = []
    buf = b""
    try:
        total_size = path.stat().st_size
    except OSError:
        total_size = 0
    read_bytes = 0

    def emit(frac: float, msg: str) -> None:
        if on_progress:
            on_progress(min(1.0, max(0.0, frac)), msg)

    with path.open("rb") as f:
        while True:
            chunk = f.read(CHUNK)
            if not chunk:
                break
            read_bytes += len(chunk)
            if total_size > 0:
                emit(read_bytes / total_size, "Reading JSONL…")
            else:
                emit(min(0.92, read_bytes / max(read_bytes + 8 * CHUNK, 1)), "Reading JSONL…")
            buf += chunk
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                if not line.strip():
                    continue
                try:
                    rec = json.loads(line.decode("utf-8", errors="replace"))
                except json.JSONDecodeError:
                    continue
                rows.append(_normalize_row(rec))
    if buf.strip():
        try:
            rec = json.loads(buf.decode("utf-8", errors="replace"))
            rows.append(_normalize_row(rec))
        except json.JSONDecodeError:
            pass
    emit(1.0, "Parsing complete…")
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df.insert(0, "_row_id", range(len(df)))
    return df
