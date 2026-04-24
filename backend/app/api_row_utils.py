"""Shared helpers for paginated row JSON and removed-row filtering (no store dependency)."""

from __future__ import annotations

import json
from typing import Any

import numpy as np
import pandas as pd

from .removal_labels import (
    REMOVAL_CATEGORIES,
    categories_for_row,
    friendly_removal_label,
    primary_category,
    row_matches_removal_category,
)


def df_to_records(df: pd.DataFrame, limit: int, offset: int) -> list[dict[str, Any]]:
    if df is None or len(df) == 0:
        return []
    part = df.iloc[offset : offset + limit]
    return json.loads(part.to_json(orient="records", default_handler=str))


_ALLOWED_SORT = frozenset({"row", "signature", "stage_focus", "question", "response"})


def _row_id_tiebreak(df: pd.DataFrame) -> pd.Series:
    """Numeric row id for stable ordering; falls back to row order index."""
    if "_row_id" in df.columns:
        s = pd.to_numeric(df["_row_id"], errors="coerce")
        if s.notna().any():
            return s.fillna(-1e18)
    if "row_id" in df.columns:
        s = pd.to_numeric(df["row_id"], errors="coerce")
        if s.notna().any():
            return s.fillna(-1e18)
    return pd.Series(np.arange(len(df), dtype=float), index=df.index)


def sort_kept_dataframe(df: pd.DataFrame, sort: str | None, sort_dir: str) -> pd.DataFrame:
    """
    Sort full kept frame before pagination. ``sort`` must be in ``_ALLOWED_SORT`` or None.
    For signature / stage_focus / question / response, ties break by ``_row_id`` ascending.
    """
    if df is None or len(df) == 0:
        return df
    if not sort:
        return df
    key = str(sort).strip().lower()
    if key not in _ALLOWED_SORT:
        raise ValueError(f"invalid sort: {sort!r}; use one of {sorted(_ALLOWED_SORT)}")
    asc = str(sort_dir or "asc").strip().lower() != "desc"
    rid = _row_id_tiebreak(df)
    kind: str = "mergesort"

    if key == "row":
        out = df.assign(__rid=rid).sort_values("__rid", ascending=asc, kind=kind)
        return out.drop(columns=["__rid"])

    if key == "signature":
        col = (
            df["signature"].fillna("").astype(str) if "signature" in df.columns else pd.Series("", index=df.index)
        )
        out = df.assign(__p=col, __rid=rid).sort_values(["__p", "__rid"], ascending=[asc, True], kind=kind)
        return out.drop(columns=["__p", "__rid"])

    if key == "stage_focus":
        col = (
            df["stage_focus"].fillna("").astype(str)
            if "stage_focus" in df.columns
            else pd.Series("", index=df.index)
        )
        out = df.assign(__p=col, __rid=rid).sort_values(["__p", "__rid"], ascending=[asc, True], kind=kind)
        return out.drop(columns=["__p", "__rid"])

    if key == "question":
        col = df["question"].fillna("").astype(str) if "question" in df.columns else pd.Series("", index=df.index)
        out = df.assign(__p=col, __rid=rid).sort_values(["__p", "__rid"], ascending=[asc, True], kind=kind)
        return out.drop(columns=["__p", "__rid"])

    if key == "response":
        col = df["response"].fillna("").astype(str) if "response" in df.columns else pd.Series("", index=df.index)
        out = df.assign(__p=col, __rid=rid).sort_values(["__p", "__rid"], ascending=[asc, True], kind=kind)
        return out.drop(columns=["__p", "__rid"])

    raise AssertionError("sort key validated above")


def apply_removed_row_filters(
    work: pd.DataFrame,
    category: str | None,
    categories: list[str] | None,
    signatures: list[str] | None,
) -> pd.DataFrame:
    w = work
    cat_list: list[str] = []
    if categories and len(categories) > 0:
        cat_list = [c.strip().lower() for c in categories if c and c.strip().lower() in REMOVAL_CATEGORIES]
    else:
        c0 = (category or "").strip().lower()
        if c0 and c0 not in ("all",) and c0 in REMOVAL_CATEGORIES:
            cat_list = [c0]
    if cat_list:
        if "removal_reason" not in w.columns:
            w = w.head(0)
        else:
            rr = w["removal_reason"].fillna("").astype(str)
            m = rr.apply(lambda s: any(row_matches_removal_category(s, c) for c in cat_list))
            w = w[m]
    if signatures and len(signatures) > 0 and "signature" in w.columns:
        want = {str(x) for x in signatures if str(x) != ""}
        if want:
            sig = w["signature"].fillna("").astype(str)
            w = w[sig.isin(want)]
    return w


def removed_dataframe_paginated(
    removed: pd.DataFrame,
    limit: int,
    offset: int,
    category: str | None,
    categories: list[str] | None,
    signatures: list[str] | None,
) -> tuple[list[dict[str, Any]], int]:
    if removed is None or len(removed) == 0:
        return [], 0
    work = apply_removed_row_filters(removed, category, categories, signatures)
    n = len(work)
    if n == 0:
        return [], 0
    part = work.iloc[offset : offset + limit]
    records: list[dict[str, Any]] = json.loads(part.to_json(orient="records", default_handler=str))
    for r in records:
        r["removal_label"] = friendly_removal_label(r)
        r["removal_category"] = primary_category(str(r.get("removal_reason") or ""))
    return records, n
