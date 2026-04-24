"""Export stages to JSONL, CSV, and filter_log.json."""

from __future__ import annotations

import json
import io
import math
from typing import Any


def _drop_internal_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    drop = [c for c in out.columns if c.startswith("_") and c not in ("_row_id",)]
    if drop:
        out = out.drop(columns=drop, errors="ignore")
    return out


def _row_to_json_dict(row: pd.Series) -> dict[str, Any]:
    d = row.to_dict()
    for k, v in list(d.items()):
        if v is not None and isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            d[k] = None
    if "_row_id" in d and d.get("_row_id") is not None and "row_id" not in d:
        d["row_id"] = d.pop("_row_id")
    return d


def export_jsonl(df: pd.DataFrame) -> str:
    clean = _drop_internal_columns(df)
    lines = []
    for _, row in clean.iterrows():
        lines.append(json.dumps(_row_to_json_dict(row), ensure_ascii=False, default=str))
    return "\n".join(lines) + ("\n" if lines else "")


def export_csv(df: pd.DataFrame) -> str:
    clean = _drop_internal_columns(df)
    if "_row_id" in clean.columns:
        clean = clean.rename(columns={"_row_id": "row_id"})
    buf = io.StringIO()
    clean.to_csv(buf, index=False)
    return buf.getvalue()


def build_filter_log(entries: list[dict[str, Any]]) -> str:
    return json.dumps(entries, indent=2, ensure_ascii=False)
