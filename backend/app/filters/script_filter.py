"""Built-in filter type ``custom_script``: Python ``removal_mask(df, config)``."""

from __future__ import annotations

from typing import Any

import pandas as pd

from .base_filter import FilterResult
from .script_runtime import run_removal_mask


def apply_custom_script(df: pd.DataFrame, config: dict[str, Any]) -> FilterResult:
    code = str(config.get("code") or "")
    extra = {k: v for k, v in config.items() if k != "code"}
    mask = run_removal_mask(df, code, extra)
    removed = df.loc[mask].copy()
    kept = df.loc[~mask].copy()
    if len(removed) > 0:
        rr = removed.copy()
        rr["removal_reason"] = str(config.get("removal_reason_label") or "custom_script")
    else:
        rr = pd.DataFrame(columns=df.columns.tolist() + ["removal_reason"])
    return FilterResult(
        name=str(config.get("name") or "Custom script"),
        filter_type="custom_script",
        filter_config=dict(config),
        input_count=len(df),
        kept=kept,
        removed=rr,
    )
