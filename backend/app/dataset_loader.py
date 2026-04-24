"""Load JSONL datasets into pandas with stable row ids."""

from __future__ import annotations

import json
from io import StringIO

import pandas as pd
from starlette.datastructures import UploadFile


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


async def load_jsonl_from_upload_file(file: UploadFile) -> pd.DataFrame:
    """
    Stream the upload in chunks and parse JSON lines. Avoids `bytes.decode("utf-8")` of the
    whole file, which can double memory and fail for multi-hundred-MB JSONL.
    """
    rows: list[dict] = []
    buf = b""
    while True:
        chunk = await file.read(CHUNK)
        if not chunk:
            break
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
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df.insert(0, "_row_id", range(len(df)))
    return df
