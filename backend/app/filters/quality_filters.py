"""Quality-based filters: correctness, compiled, runtime."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from .base_filter import FilterResult


def apply_empty_prompt(df: pd.DataFrame, config: dict[str, Any]) -> FilterResult:
    s = df["prompt"].fillna("").astype(str) if "prompt" in df.columns else pd.Series([""] * len(df))
    mask = s.str.strip() != ""
    return _result(df, mask, "empty prompt", "remove_empty_prompt", config)


def apply_empty_response(df: pd.DataFrame, config: dict[str, Any]) -> FilterResult:
    s = df["response"].fillna("").astype(str) if "response" in df.columns else pd.Series([""] * len(df))
    mask = s.str.strip() != ""
    return _result(df, mask, "empty response", "remove_empty_response", config)


def apply_length_filter(df: pd.DataFrame, config: dict[str, Any]) -> FilterResult:
    col = config.get("column", "response")
    min_tok = int(config.get("min_chars", 0) or 0)  # treat as chars in MVP
    max_tok = int(config.get("max_chars", 1_000_000) or 1_000_000)
    s = (
        df[col].fillna("").astype(str)
        if col in df.columns
        else pd.Series([""] * len(df))
    )
    lengths = s.str.len()
    ok = (lengths >= min_tok) & (lengths <= max_tok)
    return _result(df, ok, f"length not in [{min_tok}, {max_tok}]", "length_filter", {**config, "column": col})


def apply_correctness_only(df: pd.DataFrame, config: dict[str, Any]) -> FilterResult:
    if "correctness" not in df.columns:
        return _result(df, pd.Series(True, index=df.index), "no correctness column (kept all)", "correctness_only", config)
    m = df["correctness"] == True  # noqa: E712
    return _result(df, m, "correctness is not true", "correctness_only", config)


def apply_compiled_only(df: pd.DataFrame, config: dict[str, Any]) -> FilterResult:
    if "compiled" not in df.columns:
        return _result(df, pd.Series(True, index=df.index), "no compiled column (kept all)", "compiled_only", config)
    m = df["compiled"] == True  # noqa: E712
    return _result(df, m, "compiled is not true", "compiled_only", config)


def apply_runtime_range(df: pd.DataFrame, config: dict[str, Any]) -> FilterResult:
    if "runtime_ms" not in df.columns:
        return _result(
            df,
            pd.Series(True, index=df.index),
            "no runtime_ms column (kept all)",
            "runtime_range_filter",
            config,
        )
    lo = float(config.get("min", 0))
    hi = float(config.get("max", float("inf")))
    vals = pd.to_numeric(df["runtime_ms"], errors="coerce")
    in_range = (vals >= lo) & (vals <= hi)
    if config.get("keep_nan", False):
        ok = in_range | vals.isna()
    else:
        ok = in_range & vals.notna()
    return _result(
        df,
        ok,
        f"runtime_ms outside [{lo}, {hi}]",
        "runtime_range_filter",
        {**config, "min": lo, "max": hi},
    )


def apply_remove_slow_samples(df: pd.DataFrame, config: dict[str, Any]) -> FilterResult:
    if "runtime_ms" not in df.columns:
        return _result(df, pd.Series(True, index=df.index), "no runtime_ms (kept all)", "remove_slow_samples", config)
    v = pd.to_numeric(df["runtime_ms"], errors="coerce")
    p = float(config.get("percentile", 90))
    th = v.quantile(p / 100.0)
    if np.isnan(th):
        th = float("inf")
    ok = v < th
    return _result(df, ok, f"runtime >= p{p} = {th:.2f} ms (slow sample)", "remove_slow_samples", config)


def apply_remove_unstable_samples(df: pd.DataFrame, config: dict[str, Any]) -> FilterResult:
    if "runtime_ms" not in df.columns:
        return _result(
            df,
            pd.Series(True, index=df.index),
            "no runtime_ms (kept all)",
            "remove_unstable_samples",
            config,
        )
    v = pd.to_numeric(df["runtime_ms"], errors="coerce")
    zlim = float(config.get("z", 3.0))
    has = v.notna()
    if has.sum() < 2:
        return _result(df, pd.Series(True, index=df.index), "not enough runtimes to score", "remove_unstable_samples", config)
    mu, sd = float(v[has].mean()), float(v[has].std(ddof=0) or 0.0)
    sd = sd if sd > 1e-9 else 1.0
    z = (v - mu) / sd
    # Outliers: extreme runtime deviates (flagged "unstable" in MVP sense)
    ok = z.abs() <= zlim
    ok = ok | v.isna()
    return _result(
        df,
        ok,
        f"extreme runtime (|z-score| > {zlim} vs all samples)",
        "remove_unstable_samples",
        {**config, "z": zlim},
    )


def _result(
    df: pd.DataFrame,
    mask: pd.Series,
    reason: str,
    filter_type: str,
    filter_config: dict,
) -> FilterResult:
    kept = df[mask].reset_index(drop=True)
    removed = df[~mask].copy()
    for col in list(removed.columns):
        if col == "removal_reason":
            removed = removed.drop(columns=[col])
    removed["removal_reason"] = reason
    if "_row_id" in removed.columns:
        removed["row_id"] = removed["_row_id"]
    return FilterResult(
        name=filter_type,
        filter_type=filter_type,
        filter_config=filter_config,
        input_count=len(df),
        kept=kept,
        removed=removed,
    )
