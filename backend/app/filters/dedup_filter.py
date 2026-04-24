"""Exact deduplication by question, response, or both; optional reasoning repetition (avoid_repetition)."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path
from typing import Any

import pandas as pd

from .base_filter import FilterResult

_REF = Path(__file__).resolve().parent.parent.parent / "references"
if str(_REF) not in sys.path:
    sys.path.insert(0, str(_REF))
import avoid_repetition  # type: ignore


def _q(row: pd.Series) -> str:
    v = row.get("question", row.get("prompt", ""))
    return "" if v is None or (isinstance(v, float) and pd.isna(v)) else str(v)


def _r(row: pd.Series) -> str:
    v = row.get("response", row.get("output", ""))
    return "" if v is None or (isinstance(v, float) and pd.isna(v)) else str(v)


def _hash(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", errors="replace")).hexdigest()[:32]


def _dup_key(row: pd.Series, mode: str) -> str:
    q, r = _q(row), _r(row)
    if mode == "question":
        return f"question:sha256:{_hash(q)}"
    if mode == "response":
        return f"response:sha256:{_hash(r)}"
    return f"question+response:sha256:{_hash(q + '\0' + r)}"


def apply_remove_duplicates(df: pd.DataFrame, config: dict[str, Any]) -> FilterResult:
    mode = str(config.get("mode", "question+response")).lower().replace(" ", "")
    if mode in ("both", "q+r"):
        mode = "question+response"
    if mode not in ("question", "response", "question+response"):
        mode = "question+response"
    repetition_on = bool(config.get("reasoning_repetition", True))

    working = df.reset_index(drop=True)
    n = len(working)
    keep_mask = [True] * n
    reasons = [""] * n
    dup_keys = [""] * n
    first_id_for_key: dict[str, int] = {}

    for i in range(n):
        if not keep_mask[i]:
            continue
        row = working.iloc[i]
        rid = int(row["_row_id"]) if "_row_id" in working.columns else i
        dk = _dup_key(row, mode)
        dup_keys[i] = dk
        if dk in first_id_for_key:
            keep_mask[i] = False
            reasons[i] = f"exact_duplicate mode={mode}"
        else:
            first_id_for_key[dk] = rid

    if repetition_on:
        for i in range(n):
            if not keep_mask[i]:
                continue
            resp = str(working.iloc[i].get("response") or "")
            if avoid_repetition.has_strong_repetition(resp):
                keep_mask[i] = False
                reasons[i] = "strong_reasoning_repetition (avoid_repetition)"
                dup_keys[i] = "repetition:internal_ngram"

    m = pd.Series(keep_mask)
    removed_idx = [i for i in range(n) if not m.iloc[i]]
    removed = working.loc[~m].copy().reset_index(drop=True)
    removed["removal_reason"] = [reasons[i] for i in removed_idx]
    removed["duplicate_key"] = [dup_keys[i] for i in removed_idx]
    k_ids: list[int | None] = []
    for i in removed_idx:
        rsn, dk = reasons[i], dup_keys[i]
        if "exact_duplicate" in rsn and dk in first_id_for_key:
            k_ids.append(int(first_id_for_key[dk]))
        else:
            k_ids.append(None)
    if any(x is not None for x in k_ids):
        removed["kept_row_id"] = k_ids
    if "_row_id" in removed.columns:
        removed["row_id"] = removed["_row_id"]
    kept = working[m].reset_index(drop=True)
    return FilterResult(
        name="Deduplication",
        filter_type="remove_duplicates",
        filter_config={**config, "mode": mode, "reasoning_repetition": repetition_on},
        input_count=len(df),
        kept=kept,
        removed=removed,
    )
