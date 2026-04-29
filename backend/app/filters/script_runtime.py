"""
Execute curator-supplied Python for ``custom_script`` filters and optional subset scripts.

Security: disabled unless ``ALLOW_CUSTOM_SCRIPT_FILTERS=1`` (trusted deployments only).
The namespace exposes ``pd``, ``np``, ``re``, ``math`` and a restricted ``__builtins__``.
"""

from __future__ import annotations

import builtins
import math
import os
import re
from typing import Any, Callable

import numpy as np
import pandas as pd


def custom_scripts_allowed() -> bool:
    return os.environ.get("ALLOW_CUSTOM_SCRIPT_FILTERS", "").strip().lower() in (
        "1",
        "true",
        "yes",
    )


def _restricted_builtins() -> dict[str, Any]:
    names = (
        "len",
        "range",
        "min",
        "max",
        "sum",
        "abs",
        "str",
        "int",
        "float",
        "bool",
        "enumerate",
        "zip",
        "isinstance",
        "dict",
        "list",
        "tuple",
        "set",
        "frozenset",
        "True",
        "False",
        "None",
        "round",
        "sorted",
        "any",
        "all",
        "print",
        "hex",
        "ord",
        "chr",
        "repr",
        "slice",
        "hash",
    )
    out: dict[str, Any] = {}
    for n in names:
        if hasattr(builtins, n):
            out[n] = getattr(builtins, n)
    return out


def _globals_dict() -> dict[str, Any]:
    return {
        "pd": pd,
        "np": np,
        "re": re,
        "math": math,
        "__builtins__": _restricted_builtins(),
    }


def load_callable(code: str, fn_name: str) -> Callable[..., Any]:
    if not custom_scripts_allowed():
        raise ValueError(
            "Custom Python filters/subsets are disabled. "
            "On trusted hosts set ALLOW_CUSTOM_SCRIPT_FILTERS=1 in the server environment."
        )
    src = (code or "").strip()
    if not src:
        raise ValueError("script code is empty")
    g = _globals_dict()
    ns: dict[str, Any] = {}
    exec(compile(src, "<user_script>", "exec"), g, ns)
    fn = ns.get(fn_name)
    if fn is None or not callable(fn):
        raise ValueError(f"script must define callable {fn_name}(df, config)")
    return fn


def run_subset_mask(df: pd.DataFrame, code: str, config: dict[str, Any]) -> pd.Series:
    """
    ``subset_mask(df, config) -> pd.Series`` bool, aligned with ``df.index``;
    True means the row is **inside** the subset (filters will apply to these rows).
    """
    fn = load_callable(code, "subset_mask")
    out = fn(df, dict(config or {}))
    if not isinstance(out, pd.Series):
        raise ValueError("subset_mask must return a pandas Series")
    if len(out) != len(df) or not out.index.equals(df.index):
        raise ValueError("subset_mask Series must match df.index")
    if out.dtype != bool:
        out = out.astype(bool)
    return out


def run_removal_mask(df: pd.DataFrame, code: str, config: dict[str, Any]) -> pd.Series:
    """
    ``removal_mask(df, config) -> pd.Series`` bool aligned with ``df.index``;
    True means **remove** this row (same semantics as other filters).
    """
    fn = load_callable(code, "removal_mask")
    out = fn(df, dict(config or {}))
    if not isinstance(out, pd.Series):
        raise ValueError("removal_mask must return a pandas Series")
    if len(out) != len(df) or not out.index.equals(df.index):
        raise ValueError("removal_mask Series must match df.index")
    if out.dtype != bool:
        out = out.astype(bool)
    return out
