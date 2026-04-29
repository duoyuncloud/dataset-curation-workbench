"""
Apply multiple filters in one stage: each filter is evaluated on the same input dataframe
(independently), then a row is kept only if *none* of the filters would remove it.
removal_reasons lists every reason from every filter that flags the row.
"""

from __future__ import annotations

from typing import Any, Callable

import numpy as np
import pandas as pd

from .pipeline import apply_filter
from .base_filter import FilterResult

FilterSpec = dict[str, Any]  # {"filter_type": str, "filter_config": dict}


def _row_id_series(df: pd.DataFrame) -> pd.Series:
    if "_row_id" in df.columns:
        return df["_row_id"]
    return pd.RangeIndex(0, len(df))


def apply_filters_independent_batch(
    df: pd.DataFrame,
    filters: list[FilterSpec],
    on_filter_done: Callable[[int, int, str], None] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, int]]:
    """
    Returns: kept_df, removed_df, per_filter_removed_count
    per_filter keys are ``"{idx}:{filter_type}"`` for stable naming when the same type appears twice.
    """
    if not len(filters):
        return df.copy(), pd.DataFrame(), {}

    if len(df) == 0:
        return df.copy(), pd.DataFrame(), {f"{i}:{s['filter_type']}": 0 for i, s in enumerate(filters)}

    # One copy shared across independent filter passes (filters must not mutate this frame in place).
    immutable = df.copy()
    by_rid: dict[Any, dict[str, Any]] = {}
    per_filter: dict[str, int] = {}

    for i, spec in enumerate(filters):
        ftype = spec.get("filter_type", "")
        fcfg = spec.get("filter_config") or {}
        if not ftype:
            continue
        key = f"{i}:{ftype}"
        res: FilterResult = apply_filter(ftype, immutable, fcfg)
        per_filter[key] = len(res.removed)
        if on_filter_done is not None:
            on_filter_done(i + 1, len(filters), ftype)
        rem = res.removed
        if len(rem) == 0:
            continue
        for r in rem.to_dict(orient="records"):
            rid = r.get("_row_id", r.get("row_id"))
            if rid is None or (isinstance(rid, float) and np.isnan(rid)):
                continue
            reason = r.get("removal_reason", "")
            sreason = str(reason) if reason is not None and str(reason) != "nan" else ""
            if rid not in by_rid:
                d = {k: v for k, v in r.items() if k != "removal_reasons"}
                by_rid[rid] = {"row": d, "reasons": []}
            if sreason and sreason not in by_rid[rid]["reasons"]:
                by_rid[rid]["reasons"].append(sreason)

    all_rids = set(by_rid.keys())
    ids = _row_id_series(df)
    m = ~ids.isin(list(all_rids))
    kept = df.loc[m].copy().reset_index(drop=True)
    out_removed: list[dict[str, Any]] = []
    for rid, v in by_rid.items():
        rec = dict(v["row"])
        rec["removal_reasons"] = v["reasons"]
        rec["removal_reason"] = " | ".join(v["reasons"])
        out_removed.append(rec)
    removed_df = pd.DataFrame(out_removed) if out_removed else pd.DataFrame()
    return kept, removed_df, per_filter


def mask_view_in(df: pd.DataFrame, field: str, values: list[str]) -> pd.Series:
    """Rows where ``field`` is in ``values`` (OR). Empty ``values`` matches nothing."""
    if field not in df.columns or not values:
        return pd.Series(False, index=df.index)
    s_str = df[field].fillna("").astype(str)
    want = {str(v) for v in values}
    return s_str.isin(want)


def mask_view(df: pd.DataFrame, field: str, value: str) -> pd.Series:
    return mask_view_in(df, field, [str(value) if value is not None else ""])


def mask_subset_filter(
    df: pd.DataFrame,
    signatures: list[str],
    stage_focuses: list[str] | None = None,
) -> pd.Series:
    """
    AND across dimensions; within each dimension, OR on the value list.
    ``stage_focuses`` filters the human step title column ``stage_focus``.
    """
    m = pd.Series(True, index=df.index)
    if signatures:
        if "signature" not in df.columns:
            raise ValueError("signature column required for subset signature filter")
        m &= mask_view_in(df, "signature", signatures)
    sf = stage_focuses or []
    if sf:
        if "stage_focus" not in df.columns:
            raise ValueError("stage_focus column required for subset stage_focus filter")
        m &= mask_view_in(df, "stage_focus", sf)
    return m
