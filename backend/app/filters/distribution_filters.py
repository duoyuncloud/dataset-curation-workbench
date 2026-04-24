"""Rebalancing and downsampling."""

from __future__ import annotations

from collections import Counter
from typing import Any

import numpy as np
import pandas as pd

from .base_filter import FilterResult
from .quality_filters import _result


def apply_balance_by_signature(
    df: pd.DataFrame,
    config: dict[str, Any],
) -> FilterResult:
    if "signature" not in df.columns or len(df) == 0:
        return _result(
            df,
            pd.Series(True, index=df.index),
            "no signature column (kept all)",
            "balance_by_signature",
            config,
        )
    g = list(df.groupby("signature", dropna=False, sort=False))
    mode = str(config.get("target", "min"))
    counts = [len(gg) for _, gg in g]
    if not counts:
        return _result(
            df,
            pd.Series(True, index=df.index),
            "empty (kept all)",
            "balance_by_signature",
            config,
        )
    if mode == "min":
        target = min(counts)
    else:
        target = int(np.median(counts)) or 1
    rng = np.random.default_rng(int(config.get("random_seed", 0)))
    keep_idx: list[int] = []
    for _, part in g:
        idxs = part.index.tolist()
        if len(idxs) <= target:
            keep_idx.extend(idxs)
        else:
            chosen = rng.choice(idxs, size=target, replace=False)
            keep_idx.extend(chosen.tolist())
    m = df.index.isin(keep_idx)
    return _result(
        df,
        pd.Series(m, index=df.index),
        "balance_by_signature: downsampled to per-signature cap",
        "balance_by_signature",
        {**config, "per_signature_cap": target, "target_mode": mode},
    )


def apply_downsample_overrepresented(
    df: pd.DataFrame,
    config: dict[str, Any],
) -> FilterResult:
    col = config.get("column", "signature")
    if col not in df.columns or len(df) == 0:
        return _result(
            df,
            pd.Series(True, index=df.index),
            f"no {col} (kept all)",
            "downsample_overrepresented",
            config,
        )
    cap = int(config.get("max_per_value", 100))
    rng = np.random.default_rng(int(config.get("random_seed", 0)))
    c = Counter()
    keep: list[int] = []
    # deterministic order: shuffle per bucket then take first `cap` appearances
    order = np.arange(len(df))
    rng.shuffle(order)
    seen_c = Counter()
    for i in order:
        v = str(df[col].iloc[i]) if not pd.isna(df[col].iloc[i]) else "__na__"
        if seen_c[v] < cap:
            seen_c[v] += 1
            keep.append(int(i))
    keep.sort()
    m = [False] * len(df)
    for i in keep:
        m[i] = True
    return _result(
        df,
        pd.Series(m, index=df.index),
        f"downsample_overrepresented: {col} capped at {cap} per value",
        "downsample_overrepresented",
        {**config, "column": col, "max_per_value": cap},
    )
