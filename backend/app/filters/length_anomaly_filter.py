"""Length limits and simple truncation heuristics on question/response (question–response SFT)."""

from __future__ import annotations

from typing import Any

import pandas as pd

from .base_filter import FilterResult


def _q(row: pd.Series) -> str:
    v = row.get("question", row.get("prompt", ""))
    return "" if v is None or (isinstance(v, float) and pd.isna(v)) else str(v)


def _r(row: pd.Series) -> str:
    v = row.get("response", row.get("output", ""))
    return "" if v is None or (isinstance(v, float) and pd.isna(v)) else str(v)


def _truncation_flags(text: str) -> str | None:
    if not text:
        return None
    t = text
    n_fence = t.count("```")
    if n_fence % 2 == 1:
        return "unfinished markdown code fence (odd number of ```)"
    return None


def apply_length_anomaly(df: pd.DataFrame, config: dict[str, Any]) -> FilterResult:
    min_q = int(config.get("min_question_chars", 0) or 0)
    max_q = int(config.get("max_question_chars", 2_000_000) or 2_000_000)
    min_r = int(config.get("min_response_chars", 0) or 0)
    max_r = int(config.get("max_response_chars", 2_000_000) or 2_000_000)
    check_trunc = bool(config.get("detect_truncation", True))

    keep_idx: list[int] = []
    removed_parts: list[dict] = []
    for i in range(len(df)):
        row = df.iloc[i]
        q, r = _q(row), _r(row)
        lq, lr = len(q), len(r)
        reason: str | None = None
        if lq < min_q or lq > max_q:
            reason = f"length_anomaly: question len {lq} not in [{min_q}, {max_q}]"
        elif lr < min_r or lr > max_r:
            reason = f"length_anomaly: response len {lr} not in [{min_r}, {max_r}]"
        elif check_trunc and (e := _truncation_flags(r)) is not None:
            reason = f"length_anomaly: {e}"
        elif check_trunc and (e := _truncation_flags(q)) is not None:
            reason = f"length_anomaly (question): {e}"

        rid = int(row["_row_id"]) if "_row_id" in row and pd.notna(row["_row_id"]) else i
        if reason:
            d = row.to_dict()
            d["removal_reason"] = reason
            d["row_id"] = rid
            removed_parts.append(d)
        else:
            keep_idx.append(i)

    kept = df.iloc[keep_idx].reset_index(drop=True)
    rem = pd.DataFrame(removed_parts) if removed_parts else pd.DataFrame()
    if len(rem) and "_row_id" in rem.columns:
        rem = rem.copy()
        rem["row_id"] = rem["_row_id"]
    return FilterResult(
        name="Length anomaly",
        filter_type="length_anomaly",
        filter_config={**config},
        input_count=len(df),
        kept=kept,
        removed=rem,
    )
