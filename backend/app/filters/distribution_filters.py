"""Rebalancing and downsampling."""

from __future__ import annotations

from collections import Counter
from typing import Any

import numpy as np
import pandas as pd

from .signature_extraction import ensure_stage_focus_column
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


def apply_random_drop(df: pd.DataFrame, config: dict[str, Any]) -> FilterResult:
    frac = float(config.get("drop_fraction", 0))
    frac = max(0.0, min(1.0, frac))
    seed = int(config.get("random_seed", 0))
    rng = np.random.default_rng(seed)
    n = len(df)
    if n == 0 or frac <= 0:
        return _result(
            df,
            pd.Series(True, index=df.index),
            "random_drop: nothing to drop",
            "random_drop",
            {**config, "drop_fraction": frac, "random_seed": seed},
        )
    n_drop = min(n, int(round(n * frac)))
    if n_drop <= 0:
        return _result(
            df,
            pd.Series(True, index=df.index),
            "random_drop: rounded drop count is 0",
            "random_drop",
            {**config, "drop_fraction": frac, "random_seed": seed},
        )
    pos = np.arange(n)
    rng.shuffle(pos)
    drop_pos = set(pos[:n_drop].tolist())
    m = pd.Series([i not in drop_pos for i in range(n)], index=df.index)
    kept = df[m].reset_index(drop=True)
    removed = df[~m].copy()
    for col in list(removed.columns):
        if col == "removal_reason":
            removed = removed.drop(columns=[col])
    removed["removal_reason"] = "random_drop"
    removed["drop_fraction"] = frac
    removed["random_seed"] = seed
    if "_row_id" in removed.columns:
        removed["row_id"] = removed["_row_id"]
    return FilterResult(
        name="random_drop",
        filter_type="random_drop",
        filter_config={**config, "drop_fraction": frac, "random_seed": seed},
        input_count=n,
        kept=kept,
        removed=removed,
    )


def apply_balance_to_mean(df: pd.DataFrame, config: dict[str, Any]) -> FilterResult:
    group_by = str(config.get("group_by", "signature"))
    if group_by not in ("signature", "stage_focus"):
        return _result(
            df,
            pd.Series(True, index=df.index),
            "balance_to_mean: group_by must be signature or stage_focus",
            "balance_to_mean",
            config,
        )
    # Read-only groupby / masks — no in-place mutation; avoids a full-table copy on large frames.
    work = df
    if group_by == "stage_focus" and "stage_focus" not in work.columns:
        work = ensure_stage_focus_column(work.copy())
    col = group_by
    if col not in work.columns or len(work) == 0:
        return _result(
            df,
            pd.Series(True, index=df.index),
            f"balance_to_mean: no {col} column (kept all)",
            "balance_to_mean",
            config,
        )
    rng = np.random.default_rng(int(config.get("random_seed", 0)))
    parts = list(work.groupby(col, dropna=False, sort=False))
    if len(parts) <= 1:
        raise ValueError(
            "balance_to_mean needs at least two distinct "
            f"{col} values in the rows being filtered. "
            "If you narrowed to a single signature (or one stage_focus), every row is already one group, "
            "so the per-group mean equals the group size and nothing is removed. "
            "Clear the subset and run again on the full stage, or include every signature you want to balance together."
        )
    counts = [len(g) for _, g in parts]
    mean_val = float(np.mean(counts)) if counts else 0.0
    target = max(1, int(np.floor(mean_val)))
    keep_row_index: list[Any] = []
    removed_chunks: list[pd.DataFrame] = []
    for name, part in parts:
        idxs = part.index.tolist()
        n = len(idxs)
        orig = n
        if n <= target:
            keep_row_index.extend(idxs)
            continue
        # Integer positions → avoid passing a huge Python list into rng.choice twice.
        pick = rng.choice(n, size=target, replace=False)
        chosen = [idxs[i] for i in pick]
        ch_set = set(chosen)
        keep_row_index.extend(chosen)
        drop_mask = ~part.index.isin(ch_set)
        dropped = part.loc[drop_mask].copy()
        dropped["removal_reason"] = "balance_to_mean"
        dropped["group_by"] = group_by
        gv = name
        if gv is not None and not (isinstance(gv, float) and pd.isna(gv)):
            dropped["group_value"] = str(gv)
        else:
            dropped["group_value"] = ""
        dropped["original_group_count"] = int(orig)
        dropped["target_count"] = int(target)
        removed_chunks.append(dropped)
    kept_df = work.loc[keep_row_index].reset_index(drop=True) if keep_row_index else pd.DataFrame()
    removed_df = pd.concat(removed_chunks, ignore_index=True) if removed_chunks else pd.DataFrame()
    if len(removed_df) and "_row_id" in removed_df.columns:
        removed_df["row_id"] = removed_df["_row_id"]
    return FilterResult(
        name="balance_to_mean",
        filter_type="balance_to_mean",
        filter_config={**config, "group_by": group_by, "random_seed": int(config.get("random_seed", 0))},
        input_count=len(df),
        kept=kept_df,
        removed=removed_df,
    )
