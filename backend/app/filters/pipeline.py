"""Registry: question–response SFT filters (no eval-metadata filters in v1)."""

from __future__ import annotations

from typing import Any, Callable

import pandas as pd

from .base_filter import FilterResult
from .hacking_filter import apply_remove_hacking
from .dedup_filter import apply_remove_duplicates
from .format_validity_filter import apply_format_validity
from .length_anomaly_filter import apply_length_anomaly
from .signature_extraction import apply_signature_extraction

FilterFn = Callable[[pd.DataFrame, dict[str, Any]], FilterResult]

REGISTRY: dict[str, FilterFn] = {
    "remove_hacking": apply_remove_hacking,
    "remove_duplicates": apply_remove_duplicates,
    "format_validity": apply_format_validity,
    "length_anomaly": apply_length_anomaly,
    "signature_extraction": apply_signature_extraction,
}


def get_filter(filter_type: str) -> FilterFn:
    if filter_type not in REGISTRY:
        raise KeyError(
            f"Unknown filter_type: {filter_type}. One of: {', '.join(sorted(REGISTRY))}"
        )
    return REGISTRY[filter_type]


def apply_filter(filter_type: str, df: pd.DataFrame, config: dict[str, Any]) -> FilterResult:
    return get_filter(filter_type)(df, config)
