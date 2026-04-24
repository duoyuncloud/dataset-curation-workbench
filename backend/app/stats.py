"""Per-stage summary statistics and distributions (question–response SFT + extracted metadata)."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def _safe_count_series(s: pd.Series) -> dict[str, int]:
    s = s.fillna("__missing__").astype(str)
    return s.value_counts().head(50).to_dict()


def _coerce_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def _histogram(series: pd.Series, bins: int = 20) -> list[dict[str, Any]]:
    vals = _coerce_num(series).dropna()
    if vals.empty:
        return []
    arr = vals.to_numpy(dtype=float)
    counts, edges = np.histogram(arr, bins=bins)
    out: list[dict[str, Any]] = []
    for i, c in enumerate(counts):
        if c == 0:
            continue
        out.append(
            {
                "bin_start": float(edges[i]),
                "bin_end": float(edges[i + 1]),
                "count": int(c),
            }
        )
    return out


def compute_summary_and_distributions(
    df: pd.DataFrame,
) -> tuple[dict[str, Any], dict[str, Any]]:
    n = len(df)
    summary: dict[str, Any] = {
        "total_samples": n,
        "removed_samples": 0,
        "removal_ratio": 0.0,
    }
    if n == 0:
        dist: dict[str, Any] = {
            "signature": {},
            "operator_family": {},
            "stage": {},
            "technique": {},
            "runtime_ms_histogram": [],
        }
        return summary, dist

    for col, key in [
        ("signature", "signature"),
        ("operator_family", "operator_family"),
        ("stage", "stage"),
        ("technique", "technique"),
    ]:
        if col in df.columns:
            summary[f"{key}_n_unique"] = int(df[col].nunique(dropna=True))

    if "runtime_ms" in df.columns:
        r = _coerce_num(df["runtime_ms"])
        if not r.isna().all():
            summary["runtime_ms_mean"] = float(r.mean())
            summary["runtime_ms_p50"] = float(r.median())
    if "correctness" in df.columns:
        summary["correctness_true_count"] = int((df["correctness"] == True).sum())  # noqa: E712
    if "compiled" in df.columns:
        summary["compiled_true_count"] = int((df["compiled"] == True).sum())  # noqa: E712

    dist: dict[str, Any] = {
        "signature": _safe_count_series(df["signature"]) if "signature" in df.columns else {},
        "operator_family": _safe_count_series(df["operator_family"])
        if "operator_family" in df.columns
        else {},
        "stage": _safe_count_series(df["stage"]) if "stage" in df.columns else {},
        "technique": _safe_count_series(df["technique"])
        if "technique" in df.columns
        else {},
        "problem_type": _safe_count_series(df["problem_type"])
        if "problem_type" in df.columns
        else {},
        "source_model": _safe_count_series(df["source_model"])
        if "source_model" in df.columns
        else {},
        "behavior_type": _safe_count_series(df["behavior_type"])
        if "behavior_type" in df.columns
        else {},
        "runtime_ms_histogram": _histogram(df["runtime_ms"])
        if "runtime_ms" in df.columns
        else [],
    }
    if "correctness" in df.columns:
        dist["correctness"] = {
            "true": int((df["correctness"] == True).sum()),  # noqa: E712
            "false": int((df["correctness"] == False).sum()),  # noqa: E712
            "missing": int(df["correctness"].isna().sum()),
        }
    return summary, dist
