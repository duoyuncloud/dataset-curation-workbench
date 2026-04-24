"""Filter by signature, problem type, behavior, source model."""

from __future__ import annotations

from typing import Any

import pandas as pd

from .base_filter import FilterResult
from .quality_filters import _result


def _in_set_or_eq(
    df: pd.DataFrame,
    col: str,
    config: dict[str, Any],
    filter_type: str,
) -> FilterResult:
    if col not in df.columns:
        return _result(
            df,
            pd.Series(True, index=df.index),
            f"no {col} column (kept all)",
            filter_type,
            config,
        )
    if "values" in config and config.get("values") is not None:
        allow = config["values"]
        if not isinstance(allow, (list, set, tuple)):
            allow = [allow]
        s = df[col].astype(str)
        allow_s = {str(x) for x in allow}
        m = s.isin(allow_s)
    elif "equals" in config:
        m = df[col] == config["equals"]
    else:
        m = pd.Series(True, index=df.index, dtype=bool)
    return _result(df, m, f"not in allowed {col} values", filter_type, config)


def apply_filter_by_signature(df: pd.DataFrame, config: dict[str, Any]) -> FilterResult:
    return _in_set_or_eq(df, "signature", config, "filter_by_signature")


def apply_filter_by_problem_type(df: pd.DataFrame, config: dict[str, Any]) -> FilterResult:
    return _in_set_or_eq(df, "problem_type", config, "filter_by_problem_type")


def apply_filter_by_behavior(df: pd.DataFrame, config: dict[str, Any]) -> FilterResult:
    return _in_set_or_eq(df, "behavior_type", config, "filter_by_behavior")


def apply_filter_by_source_model(df: pd.DataFrame, config: dict[str, Any]) -> FilterResult:
    return _in_set_or_eq(df, "source_model", config, "filter_by_source_model")
