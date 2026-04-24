from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, Protocol

import pandas as pd


@dataclass
class FilterResult:
    name: str
    filter_type: str
    filter_config: dict[str, Any]
    input_count: int
    kept: pd.DataFrame
    removed: pd.DataFrame


class BaseFilter(Protocol):
    """Each filter: apply(df) -> (kept_df, removed_df) with removal_reason on removed rows."""

    name: str
    filter_type: str

    def apply(self, df: pd.DataFrame, config: dict[str, Any]) -> FilterResult: ...
