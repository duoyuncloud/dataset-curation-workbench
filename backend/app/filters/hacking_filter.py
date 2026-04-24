"""Hacking / CUDA fallback detection (references/hack_detect). Maps question->instruction, response->output."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd

from .base_filter import FilterResult

_REF = Path(__file__).resolve().parent.parent.parent / "references"
if str(_REF) not in sys.path:
    sys.path.insert(0, str(_REF))
import hack_detect  # type: ignore


def _q(row: pd.Series) -> str:
    v = row.get("question", row.get("prompt", ""))
    return "" if v is None or (isinstance(v, float) and pd.isna(v)) else str(v)


def _r(row: pd.Series) -> str:
    v = row.get("response", row.get("output", ""))
    return "" if v is None or (isinstance(v, float) and pd.isna(v)) else str(v)


def _row_to_entry(row: pd.Series) -> dict:
    p = _q(row)
    r = _r(row)
    if "### Original PyTorch Operator:" in p:
        instruction = p
    else:
        instruction = f"### Original PyTorch Operator:\n```python\n{p}\n```"
    return {"instruction": instruction, "output": r}


def _serialize_cuda_fb(d: Any) -> str:
    if not d:
        return "[]"
    if isinstance(d, dict):
        out = {k: (v if isinstance(v, (str, int, float, bool)) else str(v)[:200]) for k, v in list(d.items())[:8]}
        return json.dumps(out, ensure_ascii=False)[:2000]
    return str(d)[:2000]


def apply_remove_hacking(df: pd.DataFrame, config: dict[str, Any]) -> FilterResult:
    level = int(config.get("level", 2))
    use_dataset_field = config.get("use_dataset_hacked_field", False)
    if "hacked" not in df.columns:
        use_dataset_field = False

    keep_idx: list[int] = []
    removed_parts: list[dict] = []

    for i in range(len(df)):
        row = df.iloc[i]
        rid = int(row["_row_id"]) if "_row_id" in row and pd.notna(row["_row_id"]) else i

        if use_dataset_field and row.get("hacked") is True:
            r = row.to_dict()
            r["removal_reason"] = "hacked == true (dataset field)"
            r["severity"] = None
            r["hack_types"] = ""
            r["forward_fallback_ops"] = "[]"
            r["cuda_fallback_ops"] = "{}"
            r["has_global_kernel"] = None
            r["row_id"] = rid
            removed_parts.append(r)
            continue

        entry = _row_to_entry(row)
        report = hack_detect.detect_hacks(entry, i)
        if hack_detect.should_filter(report, level):
            r = row.to_dict()
            types = sorted(report.hack_types) if report.hack_types else []
            fops = list(report.forward_hack.ops_on_main_path.keys()) if report.forward_hack.ops_on_main_path else []
            r["removal_reason"] = (
                f"hack filter level {level} severity {report.severity} types {','.join(types)}"
            )
            r["severity"] = int(report.severity)
            r["hack_types"] = ",".join(types)
            r["forward_fallback_ops"] = json.dumps(fops)[:2000]
            r["cuda_fallback_ops"] = _serialize_cuda_fb(report.fallback_on_required)
            r["has_global_kernel"] = bool(report.has_global_kernel)
            r["row_id"] = rid
            removed_parts.append(r)
            continue

        keep_idx.append(i)

    kept = df.iloc[keep_idx].reset_index(drop=True)
    removed = pd.DataFrame(removed_parts) if removed_parts else pd.DataFrame()
    if len(removed) and "_row_id" in removed.columns:
        removed = removed.copy()
        removed["row_id"] = removed["_row_id"]
    return FilterResult(
        name="Remove hacking",
        filter_type="remove_hacking",
        filter_config={**config, "level": level},
        input_count=len(df),
        kept=kept,
        removed=removed,
    )
