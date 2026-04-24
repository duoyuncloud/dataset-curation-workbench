"""Check that the response looks like a valid CUDA SFT sample (ModelNew, load_inline, __global__, etc.)."""

from __future__ import annotations

import re
from typing import Any

import pandas as pd

from .base_filter import FilterResult


def _q(row: pd.Series) -> str:
    v = row.get("question", row.get("prompt", ""))
    return "" if v is None or (isinstance(v, float) and pd.isna(v)) else str(v)


def _r(row: pd.Series) -> str:
    v = row.get("response", row.get("output", ""))
    return "" if v is None or (isinstance(v, float) and pd.isna(v)) else str(v)


def _check(
    q: str,
    r: str,
    require_modelnew: bool,
    require_load_inline: bool,
    require_global_kernel: bool,
    require_forward: bool,
    require_cuda_source: bool,
) -> str | None:
    r_stripped = (r or "").strip()
    q_stripped = (q or "").strip()
    if not q_stripped:
        return "empty question"
    if not r_stripped:
        return "empty response"
    if require_modelnew and not re.search(r"\bclass\s+ModelNew\b", r, re.I | re.DOTALL):
        return "missing class ModelNew"
    if require_load_inline and "load_inline" not in r:
        return "missing load_inline"
    if require_global_kernel and "__global__" not in r:
        return "missing __global__ kernel"
    if require_forward and "def forward" not in r and "def forward(" not in r:
        return "missing def forward in response"
    if require_cuda_source:
        if "load_inline" not in r and "cpp" not in r.lower() and "CPP" not in r and ".cu" not in r:
            if "__global__" not in r and "torch" not in r:
                return "no cuda/cpp or torch extension glue detected in response (require_cuda_source)"
    return None


def apply_format_validity(df: pd.DataFrame, config: dict[str, Any]) -> FilterResult:
    require_modelnew = bool(config.get("require_modelnew", True))
    require_load_inline = bool(config.get("require_load_inline", True))
    require_global_kernel = bool(config.get("require_global_kernel", True))
    require_forward = bool(config.get("require_forward", True))
    require_cuda_source = bool(
        config.get("require_cuda_source", False)
    )  # strict; off by default for Megatron SFT

    keep_idx: list[int] = []
    removed_parts: list[dict] = []
    for i in range(len(df)):
        row = df.iloc[i]
        rid = int(row["_row_id"]) if "_row_id" in row and pd.notna(row["_row_id"]) else i
        q, r = _q(row), _r(row)
        err = _check(
            q,
            r,
            require_modelnew,
            require_load_inline,
            require_global_kernel,
            require_forward,
            require_cuda_source,
        )
        if err is not None:
            rdict = row.to_dict()
            rdict["removal_reason"] = f"format_validity: {err}"
            rdict["row_id"] = rid
            removed_parts.append(rdict)
            continue
        keep_idx.append(i)

    kept = df.iloc[keep_idx].reset_index(drop=True)
    rem = pd.DataFrame(removed_parts) if removed_parts else pd.DataFrame()
    if len(rem) and "_row_id" in rem.columns:
        rem = rem.copy()
        rem["row_id"] = rem["_row_id"]
    return FilterResult(
        name="Format validity",
        filter_type="format_validity",
        filter_config={**config},
        input_count=len(df),
        kept=kept,
        removed=rem,
    )
