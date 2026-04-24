"""Per-stage summary statistics and distributions (question–response SFT + extracted metadata)."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


def _safe_count_series(s: pd.Series) -> dict[str, int]:
    s = s.fillna("__missing__").astype(str)
    return s.value_counts().head(50).to_dict()


_DIST_TOP = 50
_DIST_CAT_COLS = (
    "signature",
    "operator_family",
    "stage_focus",
    "technique",
    "problem_type",
    "source_model",
    "behavior_type",
)


def _finalize_counter(c: Counter[str]) -> dict[str, int]:
    if not c:
        return {}
    return {str(k): int(v) for k, v in c.most_common(_DIST_TOP)}


def _streaming_cat_key(v: Any) -> str:
    if v is None:
        return "__missing__"
    if isinstance(v, float) and np.isnan(v):
        return "__missing__"
    s = str(v).strip()
    return s if s else "__missing__"


def _histogram_array(arr: np.ndarray, bins: int = 20) -> list[dict[str, Any]]:
    if arr.size == 0:
        return []
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


def distributions_from_jsonl_path(path: Path) -> dict[str, Any]:
    """
    Build the same ``distributions`` shape as :func:`compute_summary_and_distributions` by scanning
    ``kept.jsonl`` once — avoids loading the full file into pandas (used by charts / signatures-by-stage).
    """
    empty: dict[str, Any] = {
        "signature": {},
        "operator_family": {},
        "stage_focus": {},
        "technique": {},
        "problem_type": {},
        "source_model": {},
        "behavior_type": {},
        "runtime_ms_histogram": [],
    }
    if not path.is_file() or path.stat().st_size == 0:
        return empty

    counters: dict[str, Counter[str]] = {k: Counter() for k in _DIST_CAT_COLS}
    runtime_vals: list[float] = []
    correctness_counts: dict[str, int] | None = None

    with path.open("r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            for col in _DIST_CAT_COLS:
                counters[col][_streaming_cat_key(obj.get(col))] += 1
            rv = obj.get("runtime_ms")
            if rv is not None:
                try:
                    fv = float(rv)
                    if not np.isnan(fv):
                        runtime_vals.append(fv)
                except (TypeError, ValueError):
                    pass
            if "correctness" in obj:
                if correctness_counts is None:
                    correctness_counts = {"true": 0, "false": 0, "missing": 0}
                cv = obj.get("correctness")
                if cv is True:
                    correctness_counts["true"] += 1
                elif cv is False:
                    correctness_counts["false"] += 1
                else:
                    correctness_counts["missing"] += 1
            elif correctness_counts is not None:
                correctness_counts["missing"] += 1

    dist: dict[str, Any] = {
        "signature": _finalize_counter(counters["signature"]),
        "operator_family": _finalize_counter(counters["operator_family"]),
        "stage_focus": _finalize_counter(counters["stage_focus"]),
        "technique": _finalize_counter(counters["technique"]),
        "problem_type": _finalize_counter(counters["problem_type"]),
        "source_model": _finalize_counter(counters["source_model"]),
        "behavior_type": _finalize_counter(counters["behavior_type"]),
        "runtime_ms_histogram": _histogram_array(np.asarray(runtime_vals, dtype=float))
        if runtime_vals
        else [],
    }
    if correctness_counts is not None:
        dist["correctness"] = correctness_counts
    return dist


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
            "stage_focus": {},
            "technique": {},
            "runtime_ms_histogram": [],
        }
        return summary, dist

    for col, key in [
        ("signature", "signature"),
        ("operator_family", "operator_family"),
        ("technique", "technique"),
    ]:
        if col in df.columns:
            summary[f"{key}_n_unique"] = int(df[col].nunique(dropna=True))
    if "stage_focus" in df.columns:
        summary["stage_focus_n_unique"] = int(df["stage_focus"].fillna("unknown").astype(str).nunique(dropna=True))

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
        "stage_focus": _safe_count_series(df["stage_focus"])
        if "stage_focus" in df.columns
        else {},
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
