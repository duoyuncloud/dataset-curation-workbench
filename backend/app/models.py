"""Pydantic and shared types for the SFT Dataset Curation Workbench API."""

from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field, model_validator


# --- API bodies ---


class FilterApplyBody(BaseModel):
    """Request body for POST /datasets/{id}/apply-filter."""

    stage_id: int = Field(..., description="Apply filter to this stage's output; creates a new stage after it.")
    filter_type: str
    filter_config: dict[str, Any] = Field(default_factory=dict)


class OneFilterInBatch(BaseModel):
    filter_type: str
    filter_config: dict[str, Any] = Field(default_factory=dict)


class ViewFilterIn(BaseModel):
    """Scope batch / exploration to rows where ``field`` matches ``value`` or any of ``values``."""

    field: str
    value: Optional[str] = None
    values: Optional[list[str]] = None

    @model_validator(mode="after")
    def _require_value(self) -> "ViewFilterIn":
        if self.values and len(self.values) > 0:
            return self
        if self.value is not None and str(self.value) != "":
            return self
        raise ValueError("view_filter needs `value` or a non-empty `values` list")

    def mask_values(self) -> list[str]:
        if self.values and len(self.values) > 0:
            return [str(x) for x in self.values]
        if self.value is not None:
            return [str(self.value)]
        return []


class ApplyFiltersBody(BaseModel):
    """Batch filters → one new stage. Optional view_filter limits which rows are processed."""

    base_stage_id: int
    view_filter: Optional[ViewFilterIn] = None
    filters: list[OneFilterInBatch] = Field(
        min_length=1,
        description="Each filter is evaluated on the same input; kept rows must pass all.",
    )


# --- Stages (serialized for API) ---


class StageSummaryView(BaseModel):
    stage_id: int
    stage_name: str
    filter_type: str
    filter_config: dict[str, Any]
    input_count: int
    output_count: int
    removed_count: int
    per_filter_removed_count: Optional[dict[str, int]] = None
    view_filter: Optional[dict[str, Any]] = None
    affected_row_count: Optional[int] = None
    untouched_row_count: Optional[int] = None


class StageDetailView(StageSummaryView):
    summary_stats: dict[str, Any] = Field(default_factory=dict)
    previous_stage_id: Optional[int] = None


class DistributionView(BaseModel):
    signature: dict[str, int] = Field(default_factory=dict)
    problem_type: dict[str, int] = Field(default_factory=dict)
    source_model: dict[str, int] = Field(default_factory=dict)
    behavior_type: dict[str, int] = Field(default_factory=dict)
    runtime_ms_histogram: list[dict[str, Any]] = Field(default_factory=list)
    correctness: dict[str, int] = Field(default_factory=dict)
    compiled: dict[str, int] = Field(default_factory=dict)


# --- Upload ---


class UploadResponse(BaseModel):
    dataset_id: str
    stage0_count: int
    message: str = "ok"


# --- Filter log export ---

FilterLogEntry = dict[str, Any]  # {"stage": int, "filter": str, "filter_type": str, ...}

__all__ = [
    "FilterApplyBody",
    "OneFilterInBatch",
    "ViewFilterIn",
    "ApplyFiltersBody",
    "StageSummaryView",
    "StageDetailView",
    "DistributionView",
    "UploadResponse",
]
