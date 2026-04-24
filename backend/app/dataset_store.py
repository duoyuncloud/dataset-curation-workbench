"""In-memory store: datasets, append-only stages, never overwrite."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any, Optional

import pandas as pd

from .filters.signature_extraction import enrich_dataframe_signatures
from .stats import compute_summary_and_distributions

FilterLogEntry = dict[str, Any]


@dataclass
class Stage:
    """One immutable snapshot after a filter (or raw upload for stage 0)."""

    stage_id: int
    stage_name: str
    filter_type: str
    filter_config: dict[str, Any]
    input_count: int
    output_count: int
    removed_count: int
    kept_rows: pd.DataFrame
    removed_rows: pd.DataFrame
    summary_stats: dict[str, Any] = field(default_factory=dict)
    distributions: dict[str, Any] = field(default_factory=dict)
    input_stage_id: Optional[int] = None  # source stage; None for raw
    # Batch / subset audit (optional)
    per_filter_removed_count: Optional[dict[str, int]] = None
    batch_filters: Optional[list[dict[str, Any]]] = None
    view_filter: Optional[dict[str, Any]] = None
    base_stage_id: Optional[int] = None
    affected_row_count: Optional[int] = None
    untouched_row_count: Optional[int] = None


@dataclass
class Dataset:
    """A dataset is a list of stages; each filter appends a new stage."""

    dataset_id: str
    stages: list[Stage] = field(default_factory=list)
    log: list[FilterLogEntry] = field(default_factory=list)
    # Original filename for display
    source_name: str = "upload.jsonl"

    def latest_stage_id(self) -> int:
        return self.stages[-1].stage_id if self.stages else -1


def _empty_removed() -> pd.DataFrame:
    return pd.DataFrame()


class DatasetStore:
    def __init__(self) -> None:
        self._datasets: dict[str, Dataset] = {}

    def create_from_df(self, df: pd.DataFrame, source_name: str = "upload.jsonl") -> str:
        dataset_id = str(uuid.uuid4())
        df = enrich_dataframe_signatures(df)
        summary, dist = compute_summary_and_distributions(df)
        summary["input_count"] = len(df)
        summary["output_count"] = len(df)
        summary["removed_count"] = 0
        summary["removal_ratio"] = 0.0
        stage0 = Stage(
            stage_id=0,
            stage_name="Raw dataset",
            filter_type="raw",
            filter_config={},
            input_count=len(df),
            output_count=len(df),
            removed_count=0,
            kept_rows=df.reset_index(drop=True).copy(),
            removed_rows=_empty_removed(),
            summary_stats=summary,
            distributions=dist,
            input_stage_id=None,
        )
        self._datasets[dataset_id] = Dataset(
            dataset_id=dataset_id, stages=[stage0], log=[], source_name=source_name
        )
        return dataset_id

    def get(self, dataset_id: str) -> Optional[Dataset]:
        return self._datasets.get(dataset_id)

    def append_stage(
        self,
        dataset_id: str,
        new_stage: Stage,
        log_entry: FilterLogEntry,
    ) -> None:
        ds = self._datasets.get(dataset_id)
        if not ds:
            raise KeyError("dataset not found")
        new_stage.stage_id = len(ds.stages)
        s2, d2 = compute_summary_and_distributions(new_stage.kept_rows)
        new_stage.summary_stats = s2
        new_stage.summary_stats["input_count"] = new_stage.input_count
        new_stage.summary_stats["output_count"] = new_stage.output_count
        new_stage.summary_stats["removed_count"] = new_stage.removed_count
        new_stage.summary_stats["total_samples"] = new_stage.output_count
        new_stage.summary_stats["removed_samples"] = new_stage.removed_count
        new_stage.summary_stats["removal_ratio"] = (
            new_stage.removed_count / new_stage.input_count if new_stage.input_count else 0.0
        )
        new_stage.distributions = d2
        if new_stage.per_filter_removed_count:
            new_stage.summary_stats["per_filter_removed_count"] = new_stage.per_filter_removed_count
        if new_stage.view_filter is not None:
            new_stage.summary_stats["view_filter"] = new_stage.view_filter
        if new_stage.affected_row_count is not None:
            new_stage.summary_stats["affected_row_count"] = new_stage.affected_row_count
        if new_stage.untouched_row_count is not None:
            new_stage.summary_stats["untouched_row_count"] = new_stage.untouched_row_count
        ds.stages.append(new_stage)
        log_entry = {**log_entry, "stage": new_stage.stage_id, "filter": new_stage.filter_type}
        ds.log.append(log_entry)

    def get_stage(self, dataset_id: str, stage_id: int) -> Optional[Stage]:
        ds = self.get(dataset_id)
        if not ds or stage_id < 0 or stage_id >= len(ds.stages):
            return None
        return ds.stages[stage_id]
