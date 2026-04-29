"""Combine signature / stage_focus chips with optional ``subset_mask`` Python."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pandas as pd

from .batch import mask_view_in
from .script_runtime import custom_scripts_allowed, run_subset_mask

if TYPE_CHECKING:
    from ..models import SubsetFilterIn


def mask_subset_from_body(df: pd.DataFrame, body: Any) -> pd.Series:
    """
    Build inclusion mask from a ``SubsetFilterIn``-like object:
    optional ``subset_script`` + AND with signature / stage_focus OR lists.
    """
    m = pd.Series(True, index=df.index)
    code = str(getattr(body, "subset_script", None) or "").strip()
    cfg = getattr(body, "subset_script_config", None) or {}
    if not isinstance(cfg, dict):
        cfg = {}
    if code:
        if not custom_scripts_allowed():
            raise ValueError(
                "subset_script requires ALLOW_CUSTOM_SCRIPT_FILTERS=1 on the server."
            )
        sm = run_subset_mask(df, code, cfg)
        m &= sm

    sigs = body.signature_values() if hasattr(body, "signature_values") else []
    sfo = body.stage_focus_values() if hasattr(body, "stage_focus_values") else []

    if sigs:
        if "signature" not in df.columns:
            raise ValueError("signature column required for subset signature filter")
        m &= mask_view_in(df, "signature", sigs)
    if sfo:
        if "stage_focus" not in df.columns:
            raise ValueError("stage_focus column required for subset stage_focus filter")
        m &= mask_view_in(df, "stage_focus", sfo)

    return m


def subset_filter_in_from_lists(signatures: list[str], stage_focuses: list[str]) -> "SubsetFilterIn":
    """Build ``SubsetFilterIn`` from query-param style lists (singleton vs list fields)."""
    from ..models import SubsetFilterIn

    kw: dict[str, Any] = {}
    sigs = [str(x).strip() for x in signatures if x is not None and str(x).strip() != ""]
    sfo = [str(x).strip() for x in stage_focuses if x is not None and str(x).strip() != ""]
    if len(sigs) == 1:
        kw["signature"] = sigs[0]
    elif len(sigs) > 1:
        kw["signatures"] = sigs
    if len(sfo) == 1:
        kw["stage_focus"] = sfo[0]
    elif len(sfo) > 1:
        kw["stage_focuses"] = sfo
    return SubsetFilterIn(**kw)

