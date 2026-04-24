"""Task-scoped API routes under /api/tasks/..."""

from __future__ import annotations

from typing import Any

import pandas as pd
from fastapi import APIRouter, File, HTTPException, Query, Response, UploadFile
from fastapi.responses import PlainTextResponse

from .api_row_utils import df_to_records, removed_dataframe_paginated, sort_kept_dataframe
from .dataset_loader import load_jsonl_from_upload_file
from .export import export_csv, export_jsonl
from .filters.batch import mask_subset_filter, mask_view_in
from .models import (
    ApplyFiltersBody,
    FilterApplyBody,
    StageDetailView,
    StageSummaryView,
    TaskCreateIn,
    TaskPatchIn,
    UploadResponse,
)
from .persistence.task_service import TaskService
from .removal_labels import REMOVAL_CATEGORIES, categories_for_row
from .stats import compute_summary_and_distributions, distributions_from_jsonl_path


def register_task_routes(r: APIRouter, task_svc: TaskService) -> None:
    @r.post("/tasks")
    def create_task(body: TaskCreateIn | None = None) -> dict[str, Any]:
        task_svc.initialize()
        b = body or TaskCreateIn()
        tid = task_svc.create_task(b.task_name)
        row = task_svc.get_task(tid)
        return row or {"task_id": tid}

    @r.get("/tasks")
    def list_tasks() -> list[dict[str, Any]]:
        task_svc.initialize()
        return task_svc.list_tasks()

    @r.get("/tasks/{task_id}")
    def get_task(task_id: str) -> dict[str, Any]:
        task_svc.initialize()
        row = task_svc.get_task(task_id)
        if not row:
            raise HTTPException(404, "task not found")
        return row

    @r.delete("/tasks/{task_id}")
    def delete_task(task_id: str) -> dict[str, str]:
        task_svc.initialize()
        if not task_svc.get_task(task_id):
            raise HTTPException(404, "task not found")
        task_svc.delete_task(task_id)
        return {"ok": "true"}

    @r.patch("/tasks/{task_id}")
    def patch_task(task_id: str, body: TaskPatchIn) -> dict[str, Any]:
        task_svc.initialize()
        if not body.task_name:
            raise HTTPException(400, "task_name required")
        if not task_svc.patch_task(task_id, body.task_name):
            raise HTTPException(404, "task not found")
        return task_svc.get_task(task_id) or {}

    @r.post("/tasks/{task_id}/datasets/upload", response_model=UploadResponse)
    async def upload_dataset(task_id: str, file: UploadFile = File(...)) -> UploadResponse:
        task_svc.initialize()
        if not task_svc.get_task(task_id):
            raise HTTPException(404, "task not found")
        try:
            df = await load_jsonl_from_upload_file(file)
        except MemoryError:
            raise HTTPException(
                500,
                "Not enough server memory to hold this dataset. Try a smaller file or more RAM.",
            ) from None
        if df is None or len(df) == 0:
            raise HTTPException(
                400,
                "No valid JSON lines parsed. Check JSONL format (one JSON object per line, UTF-8).",
            )
        name = file.filename or "upload.jsonl"
        try:
            n = task_svc.upload_raw(task_id, df, name)
        except KeyError:
            raise HTTPException(404, "task not found") from None
        except ValueError as e:
            raise HTTPException(400, str(e)) from e
        return UploadResponse(task_id=task_id, stage0_count=n, message="ok")

    @r.get("/tasks/{task_id}/stages")
    def list_stages(task_id: str) -> list[StageSummaryView]:
        task_svc.initialize()
        if not task_svc.get_task(task_id):
            raise HTTPException(404, "task not found")
        out: list[StageSummaryView] = []
        for row in task_svc.list_stage_rows(task_id):
            d = task_svc.row_to_stage_summary(row)
            out.append(StageSummaryView(**d))
        return out

    def _truncate_stages_body(task_id: str, stage_id: int) -> dict[str, Any]:
        task_svc.initialize()
        if not task_svc.get_task(task_id):
            raise HTTPException(404, "task not found")
        try:
            return task_svc.truncate_stages_from(task_id, stage_id)
        except KeyError as e:
            raise HTTPException(404, str(e)) from e
        except ValueError as e:
            raise HTTPException(400, str(e)) from e

    @r.delete("/tasks/{task_id}/stages/from/{stage_id}")
    def truncate_stages_delete(task_id: str, stage_id: int) -> dict[str, Any]:
        return _truncate_stages_body(task_id, stage_id)

    @r.post("/tasks/{task_id}/stages/truncate-from/{stage_id}")
    def truncate_stages_post(task_id: str, stage_id: int) -> dict[str, Any]:
        """Same as DELETE …/stages/from/… — POST avoids proxies that strip or mishandle DELETE."""
        return _truncate_stages_body(task_id, stage_id)

    @r.get("/tasks/{task_id}/stages/{stage_id}")
    def get_stage_detail(task_id: str, stage_id: int) -> dict[str, Any]:
        task_svc.initialize()
        if not task_svc.get_task(task_id):
            raise HTTPException(404, "task not found")
        try:
            s = task_svc.stage_as_runtime(task_id, stage_id)
        except KeyError:
            raise HTTPException(404, "stage not found") from None
        prev = stage_id - 1 if stage_id > 0 else None
        return StageDetailView(
            stage_id=s.stage_id,
            stage_name=s.stage_name,
            filter_type=s.filter_type,
            filter_config=s.filter_config,
            input_count=s.input_count,
            output_count=s.output_count,
            removed_count=s.removed_count,
            summary_stats=s.summary_stats,
            previous_stage_id=prev,
            per_filter_removed_count=s.per_filter_removed_count,
            view_filter=s.view_filter,
            affected_row_count=s.affected_row_count,
            untouched_row_count=s.untouched_row_count,
        ).model_dump()

    @r.get("/tasks/{task_id}/stages/{stage_id}/summary")
    def stage_summary(task_id: str, stage_id: int) -> StageDetailView:
        task_svc.initialize()
        if not task_svc.get_task(task_id):
            raise HTTPException(404, "task not found")
        try:
            s = task_svc.stage_as_runtime(task_id, stage_id)
        except KeyError:
            raise HTTPException(404, "stage not found") from None
        prev = stage_id - 1 if stage_id > 0 else None
        return StageDetailView(
            stage_id=s.stage_id,
            stage_name=s.stage_name,
            filter_type=s.filter_type,
            filter_config=s.filter_config,
            input_count=s.input_count,
            output_count=s.output_count,
            removed_count=s.removed_count,
            summary_stats=s.summary_stats,
            previous_stage_id=prev,
            per_filter_removed_count=s.per_filter_removed_count,
            view_filter=s.view_filter,
            affected_row_count=s.affected_row_count,
            untouched_row_count=s.untouched_row_count,
        )

    @r.get("/tasks/{task_id}/stages/{stage_id}/rows")
    def stage_rows(
        task_id: str,
        stage_id: int,
        limit: int = Query(200, le=10_000),
        offset: int = Query(0, ge=0),
        sort: str | None = Query(
            None,
            description="Optional: row | signature | stage_focus | question | response.",
        ),
        sort_dir: str = Query("asc", description="asc or desc"),
    ) -> dict[str, Any]:
        task_svc.initialize()
        try:
            kept = task_svc.load_kept(task_id, stage_id)
        except KeyError:
            raise HTTPException(404, "stage not found") from None
        try:
            kept_sorted = sort_kept_dataframe(kept, sort, sort_dir)
        except ValueError as e:
            raise HTTPException(400, str(e)) from None
        return {
            "rows": df_to_records(kept_sorted, limit, offset),
            "total": len(kept_sorted),
            "limit": limit,
            "offset": offset,
        }

    @r.get("/tasks/{task_id}/stages/{stage_id}/view")
    def stage_view_subset(
        task_id: str,
        stage_id: int,
        field: str | None = Query(
            None, description="Legacy: column to filter (e.g. signature). Omit when using signature/stage_focus subset params."
        ),
        value: str | None = Query(None, description="Single value (legacy; use values for multi)"),
        values: list[str] | None = Query(None, description="One or more values, OR-matched; repeat param"),
        signature: list[str] | None = Query(
            None, description="Subset: signature values (OR). Repeat param. AND with stage_focus if both given."
        ),
        stage_focus: list[str] | None = Query(
            None, description="Subset: active step title (OR). Repeat param."
        ),
        limit: int = Query(200, le=10_000),
        offset: int = Query(0, ge=0),
        sort: str | None = Query(
            None,
            description="Optional: row | signature | stage_focus | question | response.",
        ),
        sort_dir: str = Query("asc", description="asc or desc"),
    ) -> dict[str, Any]:
        task_svc.initialize()
        try:
            df = task_svc.load_kept(task_id, stage_id)
        except KeyError:
            raise HTTPException(404, "stage not found") from None
        sigs = [str(x) for x in (signature or []) if x is not None and str(x).strip() != ""]
        sfo = [str(x) for x in (stage_focus or []) if x is not None and str(x).strip() != ""]
        if sigs or sfo:
            try:
                m = mask_subset_filter(df, sigs, sfo)
            except ValueError as e:
                raise HTTPException(400, str(e)) from e
            sub = df[m]
            summ, dist = compute_summary_and_distributions(sub)
            try:
                sub_sorted = sort_kept_dataframe(sub, sort, sort_dir)
            except ValueError as e:
                raise HTTPException(400, str(e)) from e
            return {
                "subset_filter": {"signatures": sigs, "stage_focuses": sfo},
                "field": None,
                "values": [],
                "value": None,
                "total": len(sub_sorted),
                "summary_stats": {**summ, "output_count": len(sub)},
                "distributions": dist,
                "rows": df_to_records(sub_sorted, limit, offset),
                "limit": limit,
                "offset": offset,
            }
        if not field:
            raise HTTPException(
                400,
                "Provide `signature` and/or `stage_focus` query params, or legacy `field` + `value`/`values`.",
            )
        if field not in df.columns:
            raise HTTPException(400, f"field not in data: {field}")
        if values and len(values) > 0:
            vals: list[str] = list(values)
        elif value is not None:
            vals = [value]
        else:
            raise HTTPException(400, "query param `value` or at least one `values` is required")
        m = mask_view_in(df, field, vals)
        sub = df[m]
        summ, dist = compute_summary_and_distributions(sub)
        try:
            sub_sorted = sort_kept_dataframe(sub, sort, sort_dir)
        except ValueError as e:
            raise HTTPException(400, str(e)) from None
        return {
            "subset_filter": None,
            "field": field,
            "values": vals,
            "value": vals[0] if len(vals) == 1 else None,
            "total": len(sub_sorted),
            "summary_stats": {**summ, "output_count": len(sub)},
            "distributions": dist,
            "rows": df_to_records(sub_sorted, limit, offset),
            "limit": limit,
            "offset": offset,
        }

    @r.get("/tasks/{task_id}/stages/{stage_id}/removed-summary")
    def stage_removed_summary(task_id: str, stage_id: int) -> dict[str, Any]:
        task_svc.initialize()
        try:
            rem = task_svc.load_removed(task_id, stage_id)
        except KeyError:
            raise HTTPException(404, "stage not found") from None
        if rem is None or len(rem) == 0:
            return {
                "total": 0,
                "by_category": {c: 0 for c in REMOVAL_CATEGORIES},
                "row_count": 0,
                "by_signature": {},
            }
        by: dict[str, int] = {c: 0 for c in REMOVAL_CATEGORIES}
        n_rows = 0
        for _, row in rem.iterrows():
            n_rows += 1
            rr = str(row.get("removal_reason") or "")
            for c in categories_for_row(rr):
                if c in by:
                    by[c] += 1
        by_sig: dict[str, int] = {}
        if "signature" in rem.columns:
            for val, cnt in rem["signature"].fillna("").astype(str).value_counts().items():
                by_sig[str(val) if str(val) != "" else ""] = int(cnt)
            if "" in by_sig and by_sig[""] == 0:
                del by_sig[""]
        return {
            "total": n_rows,
            "row_count": n_rows,
            "by_category": by,
            "by_signature": by_sig,
        }

    @r.get("/tasks/{task_id}/stages/{stage_id}/removed-rows")
    def stage_removed(
        task_id: str,
        stage_id: int,
        limit: int = Query(200, le=10_000),
        offset: int = Query(0, ge=0),
        category: str | None = Query(
            None,
            description="Single reason category (legacy; use categories for multi-select)",
        ),
        categories: list[str] | None = Query(
            None,
            description="One or more reason categories (OR). Repeats query param.",
        ),
        signatures: list[str] | None = Query(
            None,
            description="Filter to these signature (operator family) values (OR). Repeats query param.",
        ),
    ) -> dict[str, Any]:
        task_svc.initialize()
        try:
            rem = task_svc.load_removed(task_id, stage_id)
        except KeyError:
            raise HTTPException(404, "stage not found") from None
        rows, total = removed_dataframe_paginated(
            rem if rem is not None else pd.DataFrame(),
            limit,
            offset,
            category,
            categories,
            signatures,
        )
        return {"rows": rows, "total": total, "limit": limit, "offset": offset}

    @r.get("/tasks/{task_id}/signatures-by-stage")
    def signatures_by_stage(task_id: str) -> dict[str, Any]:
        task_svc.initialize()
        if not task_svc.get_task(task_id):
            raise HTTPException(404, "task not found")
        stages_out: list[dict[str, Any]] = []
        for row in task_svc.list_stage_rows(task_id):
            idx = row["stage_index"]
            path = task_svc.kept_jsonl_path(task_id, idx)
            dist = distributions_from_jsonl_path(path)
            sig = dist.get("signature") or {}
            if not isinstance(sig, dict):
                sig = {}
            total = int(row["output_count"])
            stages_out.append(
                {
                    "stage_id": idx,
                    "total": total,
                    "by_signature": {str(k): int(v) for k, v in sig.items() if v},
                }
            )
        return {"stages": stages_out}

    @r.get("/tasks/{task_id}/stages/{stage_id}/distribution")
    def stage_distribution(task_id: str, stage_id: int) -> dict[str, Any]:
        task_svc.initialize()
        try:
            path = task_svc.kept_jsonl_path(task_id, stage_id)
        except KeyError:
            raise HTTPException(404, "stage not found") from None
        return distributions_from_jsonl_path(path)

    @r.get("/tasks/{task_id}/export")
    def export_dataset(
        task_id: str,
        stage_id: int = Query(..., description="Export kept rows at this stage"),
        out_format: str = Query("jsonl", pattern="^(jsonl|csv|filter_log)$", alias="format"),
        subset_only: bool = Query(
            False,
            description="If true, export only rows matching signature / stage_focus params (AND).",
        ),
        signature: list[str] | None = Query(
            None, description="Export subset: signature values (OR). Repeat param."
        ),
        stage_focus: list[str] | None = Query(
            None, description="Export subset: stage_focus title (OR). Repeat param."
        ),
        scope: str = Query(
            "full",
            description="full = all kept rows; signature = legacy rows matching view_field+values",
            pattern="^(full|signature)$",
        ),
        view_field: str | None = Query(
            None, description="Column to filter when scope=signature (e.g. signature)"
        ),
        values: list[str] | None = Query(
            None, description="OR list; repeat query param. Required for scope=signature"
        ),
    ) -> Response:
        task_svc.initialize()
        if not task_svc.get_task(task_id):
            raise HTTPException(404, "task not found")
        rows = task_svc.list_stage_rows(task_id)
        if stage_id < 0 or stage_id >= len(rows):
            raise HTTPException(400, "invalid stage_id")

        if out_format == "filter_log":
            content = task_svc.build_filter_log_export(task_id)
            return PlainTextResponse(
                content=content,
                media_type="application/json",
                headers={"Content-Disposition": 'attachment; filename="filter_log.json"'},
            )

        try:
            st = task_svc.stage_as_runtime(task_id, stage_id)
        except KeyError:
            raise HTTPException(404, "stage not found") from None
        export_df = st.kept_rows
        sigs = [str(x) for x in (signature or []) if x is not None and str(x).strip() != ""]
        sfo = [str(x) for x in (stage_focus or []) if x is not None and str(x).strip() != ""]
        suffix = ""
        if subset_only:
            if not sigs and not sfo:
                raise HTTPException(
                    400,
                    "subset_only=true requires at least one signature or stage_focus query param",
                )
            try:
                m = mask_subset_filter(export_df, sigs, sfo)
            except ValueError as e:
                raise HTTPException(400, str(e)) from e
            export_df = export_df[m]
            suffix = "_subset"
        elif scope == "signature":
            if not view_field or not values or len(values) == 0:
                raise HTTPException(
                    400,
                    "scope=signature requires view_field and at least one values query param",
                )
            if view_field not in export_df.columns:
                raise HTTPException(400, f"view_field not in data: {view_field}")
            m = mask_view_in(export_df, view_field, list(values))
            export_df = export_df[m]
            suffix = f"_{scope}"
        if out_format == "jsonl":
            body = export_jsonl(export_df)
            return Response(
                content=body,
                media_type="application/x-ndjson",
                headers={
                    "Content-Disposition": f'attachment; filename="stage_{stage_id}{suffix}.jsonl"',
                },
            )
        body = export_csv(export_df)
        return Response(
            content=body,
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="stage_{stage_id}{suffix}.csv"'},
        )

    @r.post("/tasks/{task_id}/apply-filter")
    def apply_filter_endpoint(task_id: str, body: FilterApplyBody) -> dict[str, Any]:
        task_svc.initialize()
        if not task_svc.get_task(task_id):
            raise HTTPException(404, "task not found")
        try:
            return task_svc.apply_single_filter(task_id, body)
        except KeyError as e:
            raise HTTPException(404, str(e)) from e
        except ValueError as e:
            raise HTTPException(400, str(e)) from e

    @r.post("/tasks/{task_id}/apply-filters")
    def apply_filters_batch(task_id: str, body: ApplyFiltersBody) -> dict[str, Any]:
        task_svc.initialize()
        if not task_svc.get_task(task_id):
            raise HTTPException(404, "task not found")
        try:
            return task_svc.apply_batch_filters(task_id, body)
        except KeyError as e:
            raise HTTPException(404, str(e)) from e
        except ValueError as e:
            raise HTTPException(400, str(e)) from e
