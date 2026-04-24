"""SFT Dataset Curation Workbench — FastAPI app."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import APIRouter, FastAPI, File, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, PlainTextResponse, Response
from fastapi.staticfiles import StaticFiles

from .dataset_loader import load_jsonl_from_upload_file
from .dataset_store import DatasetStore, Stage
from .export import build_filter_log, export_csv, export_jsonl
from .models import (
    ApplyFiltersBody,
    FilterApplyBody,
    StageDetailView,
    StageSummaryView,
    UploadResponse,
)
from .filters.batch import apply_filters_independent_batch, mask_view, mask_view_in
from .filters.pipeline import apply_filter, REGISTRY
from .removal_labels import (
    REMOVAL_CATEGORIES,
    categories_for_row,
    friendly_removal_label,
    primary_category,
    row_matches_removal_category,
)
from .stats import compute_summary_and_distributions

# Filter groupings for UI (GET /filters?grouped=true) — question+response SFT, no eval metadata
FILTER_GROUPS: dict[str, list[str]] = {
    "core": [
        "remove_hacking",
        "remove_duplicates",
        "format_validity",
        "length_anomaly",
    ],
    "analysis": [
        "signature_extraction",
    ],
}

store = DatasetStore()

API_VERSION = "0.1.0"
app = FastAPI(
    title="SFT Dataset Curation Workbench",
    description="Transparent, step-by-step SFT dataset curation. Every filter creates a new stage.",
    version=API_VERSION,
)
api_router = APIRouter()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _df_to_records(df: pd.DataFrame, limit: int, offset: int) -> list[dict[str, Any]]:
    if df is None or len(df) == 0:
        return []
    part = df.iloc[offset : offset + limit]
    return json.loads(part.to_json(orient="records", default_handler=str))


def _stage_name_for(filter_type: str) -> str:
    return {
        "raw": "Raw",
        "remove_hacking": "Hacking removed",
        "remove_duplicates": "Deduped",
    }.get(filter_type, filter_type)


@api_router.post("/datasets/upload", response_model=UploadResponse)
async def upload_dataset(
    file: UploadFile = File(...),
) -> UploadResponse:
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
    did = store.create_from_df(df, source_name=name)
    return UploadResponse(dataset_id=did, stage0_count=len(df), message="ok")


@api_router.get("/datasets/{dataset_id}/stages")
def list_stages(dataset_id: str) -> list[StageSummaryView]:
    ds = store.get(dataset_id)
    if not ds:
        raise HTTPException(404, "dataset not found")
    out: list[StageSummaryView] = []
    for s in ds.stages:
        out.append(
            StageSummaryView(
                stage_id=s.stage_id,
                stage_name=s.stage_name,
                filter_type=s.filter_type,
                filter_config=s.filter_config,
                input_count=s.input_count,
                output_count=s.output_count,
                removed_count=s.removed_count,
                per_filter_removed_count=s.per_filter_removed_count,
                view_filter=s.view_filter,
                affected_row_count=s.affected_row_count,
                untouched_row_count=s.untouched_row_count,
            )
        )
    return out


@api_router.post("/datasets/{dataset_id}/apply-filter")
def apply_filter_endpoint(dataset_id: str, body: FilterApplyBody) -> dict[str, Any]:
    ds = store.get(dataset_id)
    if not ds:
        raise HTTPException(404, "dataset not found")
    if body.stage_id < 0 or body.stage_id >= len(ds.stages):
        raise HTTPException(400, "invalid stage_id")
    if body.filter_type not in REGISTRY:
        raise HTTPException(400, f"unknown filter_type. Valid: {sorted(REGISTRY)}")

    base = ds.stages[body.stage_id].kept_rows
    if base is None or len(base) == 0 and body.filter_type not in (
        "remove_empty_prompt",
        "remove_empty_response",
    ):
        pass  # allow empty? still run

    try:
        res = apply_filter(body.filter_type, base.copy(), body.filter_config)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(400, f"filter failed: {e}") from e

    new_stage_id = len(ds.stages)
    name = _stage_name_for(res.filter_type)
    removed = res.removed
    if len(removed) and "removed_at_stage" not in removed.columns:
        removed = removed.copy()
        removed["removed_at_stage"] = new_stage_id
    pfc = {f"0:{res.filter_type}": len(removed)}

    stage = Stage(
        stage_id=new_stage_id,
        stage_name=name,
        filter_type=res.filter_type,
        filter_config=res.filter_config,
        input_count=res.input_count,
        output_count=len(res.kept),
        removed_count=len(removed),
        kept_rows=res.kept,
        removed_rows=removed,
        summary_stats={},
        distributions={},
        input_stage_id=body.stage_id,
        per_filter_removed_count=pfc,
        batch_filters=[{"filter_type": res.filter_type, "filter_config": res.filter_config}],
    )
    log_entry: dict[str, Any] = {
        "from_stage": body.stage_id,
        "filter_type": res.filter_type,
    }
    store.append_stage(dataset_id, stage, log_entry)
    return {
        "new_stage_id": new_stage_id,
        "input_count": res.input_count,
        "output_count": len(res.kept),
        "removed_count": len(removed),
        "per_filter_removed_count": pfc,
    }


@api_router.post("/datasets/{dataset_id}/apply-filters")
def apply_filters_batch(
    dataset_id: str, body: ApplyFiltersBody
) -> dict[str, Any]:
    """Run multiple filters as one stage; each filter is evaluated on the same input (independent), kept = rows none remove."""
    ds = store.get(dataset_id)
    if not ds:
        raise HTTPException(404, "dataset not found")
    if body.base_stage_id < 0 or body.base_stage_id >= len(ds.stages):
        raise HTTPException(400, "invalid base_stage_id")

    for f in body.filters:
        if f.filter_type not in REGISTRY:
            raise HTTPException(400, f"unknown filter_type: {f.filter_type}")

    specs = [{"filter_type": f.filter_type, "filter_config": dict(f.filter_config)} for f in body.filters]
    base = ds.stages[body.base_stage_id].kept_rows.copy()
    if len(base) == 0 and not any(
        x["filter_type"] in ("remove_empty_prompt", "remove_empty_response")
        for x in specs
    ):
        # still allow creating empty result stage
        pass

    vf: dict[str, Any] | None = None
    if body.view_filter is not None:
        vfi = body.view_filter
        vals = vfi.mask_values()
        vf = {"field": vfi.field, "values": vals}
        if len(vals) == 1:
            vf["value"] = vals[0]
        if vfi.field not in base.columns:
            raise HTTPException(400, f"field not in data: {vfi.field}")
        m = mask_view_in(base, vfi.field, vals)
        subset = base[m].copy()
        untouched = base[~m].copy()
    else:
        subset = base
        untouched = base.head(0).copy()

    try:
        kept_sub, removed, pfc = apply_filters_independent_batch(subset, specs)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(400, f"filter batch failed: {e}") from e

    n_untouched = len(untouched)
    n_aff = len(subset)
    n_removed = len(removed)
    if n_untouched:
        final_kept = pd.concat([untouched, kept_sub], ignore_index=True)
    else:
        final_kept = kept_sub

    new_stage_id = len(ds.stages)
    if len(removed) and "removed_at_stage" not in removed.columns:
        removed = removed.copy()
        removed["removed_at_stage"] = new_stage_id

    n_batch = len(specs)
    stage_name = f"Batch ({n_batch} filter{'s' if n_batch != 1 else ''})"
    if vf:
        shown = vf.get("values") or [vf.get("value")]
        stage_name = f"Batch on subset: {vf['field']} in {shown!r} ({n_batch} filters)"
    fcfg: dict[str, Any] = {"filters": specs, "view_filter": vf}
    stage = Stage(
        stage_id=new_stage_id,
        stage_name=stage_name,
        filter_type="batch",
        filter_config=fcfg,
        input_count=n_aff,
        output_count=len(final_kept),
        removed_count=n_removed,
        kept_rows=final_kept,
        removed_rows=removed,
        summary_stats={},
        distributions={},
        input_stage_id=body.base_stage_id,
        per_filter_removed_count=pfc,
        batch_filters=specs,
        view_filter=vf,
        base_stage_id=body.base_stage_id,
        affected_row_count=n_aff,
        untouched_row_count=n_untouched,
    )
    log_entry: dict[str, Any] = {
        "from_stage": body.base_stage_id,
        "filter_type": "batch",
        "filters": specs,
        "view_filter": vf,
    }
    store.append_stage(dataset_id, stage, log_entry)
    return {
        "new_stage_id": new_stage_id,
        "input_count": n_aff,
        "output_count": len(final_kept),
        "removed_count": n_removed,
        "per_filter_removed_count": pfc,
        "affected_row_count": n_aff,
        "untouched_row_count": n_untouched,
    }


@api_router.get("/datasets/{dataset_id}/stages/{stage_id}/view")
def stage_view_subset(
    dataset_id: str,
    stage_id: int,
    field: str = Query(..., description="Column to filter, e.g. signature (operator family)"),
    value: str | None = Query(None, description="Single value (legacy; use values for multi)"),
    values: list[str] | None = Query(None, description="One or more values, OR-matched; repeat param"),
    limit: int = Query(200, le=10_000),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    """
    Exploration: rows matching field in (values) for this stage, without creating a new stage.
    """
    s = store.get_stage(dataset_id, stage_id)
    if not s:
        raise HTTPException(404, "stage not found")
    df = s.kept_rows
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
    return {
        "field": field,
        "values": vals,
        "value": vals[0] if len(vals) == 1 else None,
        "total": len(sub),
        "summary_stats": {**summ, "output_count": len(sub)},
        "distributions": dist,
        "rows": _df_to_records(sub, limit, offset),
        "limit": limit,
        "offset": offset,
    }


@api_router.get("/datasets/{dataset_id}/stages/{stage_id}/summary")
def stage_summary(dataset_id: str, stage_id: int) -> StageDetailView:
    s = store.get_stage(dataset_id, stage_id)
    if not s:
        raise HTTPException(404, "stage not found")
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


@api_router.get("/datasets/{dataset_id}/stages/{stage_id}/rows")
def stage_rows(
    dataset_id: str,
    stage_id: int,
    limit: int = Query(200, le=10_000),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    s = store.get_stage(dataset_id, stage_id)
    if not s:
        raise HTTPException(404, "stage not found")
    return {
        "rows": _df_to_records(s.kept_rows, limit, offset),
        "total": len(s.kept_rows),
        "limit": limit,
        "offset": offset,
    }


def _apply_removed_row_filters(
    work: pd.DataFrame,
    category: str | None,
    categories: list[str] | None,
    signatures: list[str] | None,
) -> pd.DataFrame:
    w = work
    cat_list: list[str] = []
    if categories and len(categories) > 0:
        cat_list = [c.strip().lower() for c in categories if c and c.strip().lower() in REMOVAL_CATEGORIES]
    else:
        c0 = (category or "").strip().lower()
        if c0 and c0 not in ("all",) and c0 in REMOVAL_CATEGORIES:
            cat_list = [c0]
    if cat_list:
        if "removal_reason" not in w.columns:
            w = w.head(0)
        else:
            rr = w["removal_reason"].fillna("").astype(str)
            m = rr.apply(
                lambda s: any(row_matches_removal_category(s, c) for c in cat_list)
            )
            w = w[m]
    if signatures and len(signatures) > 0 and "signature" in w.columns:
        want = {str(x) for x in signatures if str(x) != ""}
        if want:
            sig = w["signature"].fillna("").astype(str)
            w = w[sig.isin(want)]
    return w


def _removed_dataframe_paginated(
    removed: pd.DataFrame,
    limit: int,
    offset: int,
    category: str | None,
    categories: list[str] | None,
    signatures: list[str] | None,
) -> tuple[list[dict[str, Any]], int]:
    if removed is None or len(removed) == 0:
        return [], 0
    work = _apply_removed_row_filters(removed, category, categories, signatures)
    n = len(work)
    if n == 0:
        return [], 0
    part = work.iloc[offset : offset + limit]
    records: list[dict[str, Any]] = json.loads(part.to_json(orient="records", default_handler=str))
    for r in records:
        r["removal_label"] = friendly_removal_label(r)
        r["removal_category"] = primary_category(str(r.get("removal_reason") or ""))
    return records, n


@api_router.get("/datasets/{dataset_id}/stages/{stage_id}/removed-summary")
def stage_removed_summary(dataset_id: str, stage_id: int) -> dict[str, Any]:
    """Count removed rows per category (a row can increment multiple categories)."""
    s = store.get_stage(dataset_id, stage_id)
    if not s:
        raise HTTPException(404, "stage not found")
    rem = s.removed_rows
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


@api_router.get("/datasets/{dataset_id}/stages/{stage_id}/removed-rows")
def stage_removed(
    dataset_id: str,
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
    s = store.get_stage(dataset_id, stage_id)
    if not s:
        raise HTTPException(404, "stage not found")
    rem = s.removed_rows
    rows, total = _removed_dataframe_paginated(
        rem if rem is not None else pd.DataFrame(),
        limit,
        offset,
        category,
        categories,
        signatures,
    )
    return {
        "rows": rows,
        "total": total,
        "limit": limit,
        "offset": offset,
    }


@api_router.get("/datasets/{dataset_id}/signatures-by-stage")
def signatures_by_stage(dataset_id: str) -> dict[str, Any]:
    """Per-stage signature (operator family) counts for a single stacked bar chart."""
    ds = store.get(dataset_id)
    if not ds:
        raise HTTPException(404, "dataset not found")
    stages_out: list[dict[str, Any]] = []
    for s in ds.stages:
        _, dist = compute_summary_and_distributions(s.kept_rows)
        sig = dist.get("signature") or {}
        if not isinstance(sig, dict):
            sig = {}
        stages_out.append(
            {
                "stage_id": s.stage_id,
                "total": int(len(s.kept_rows)),
                "by_signature": {str(k): int(v) for k, v in sig.items() if v},
            }
        )
    return {"stages": stages_out}


@api_router.get("/datasets/{dataset_id}/stages/{stage_id}/distribution")
def stage_distribution(dataset_id: str, stage_id: int) -> dict[str, Any]:
    s = store.get_stage(dataset_id, stage_id)
    if not s:
        raise HTTPException(404, "stage not found")
    if stage_id == 0:
        _, dist = compute_summary_and_distributions(s.kept_rows)
        return dist
    # stored at append
    if s.distributions:
        return s.distributions
    _, dist = compute_summary_and_distributions(s.kept_rows)
    return dist


@api_router.get("/datasets/{dataset_id}/export")
def export_dataset(
    dataset_id: str,
    stage_id: int = Query(..., description="Export kept rows at this stage"),
    out_format: str = Query("jsonl", pattern="^(jsonl|csv|filter_log)$", alias="format"),
    scope: str = Query(
        "full",
        description="full = all kept rows; signature = rows matching view_field+values",
        pattern="^(full|signature)$",
    ),
    view_field: str | None = Query(
        None, description="Column to filter when scope=signature (e.g. signature)"
    ),
    values: list[str] | None = Query(
        None, description="OR list; repeat query param. Required for scope=signature"
    ),
) -> Response:
    ds = store.get(dataset_id)
    if not ds:
        raise HTTPException(404, "dataset not found")
    if stage_id < 0 or stage_id >= len(ds.stages):
        raise HTTPException(400, "invalid stage_id")

    if out_format == "filter_log":
        content = build_filter_log(ds.log)
        return PlainTextResponse(
            content=content,
            media_type="application/json",
            headers={"Content-Disposition": 'attachment; filename="filter_log.json"'},
        )

    st = ds.stages[stage_id]
    export_df = st.kept_rows
    if scope == "signature":
        if not view_field or not values or len(values) == 0:
            raise HTTPException(
                400,
                "scope=signature requires view_field and at least one values query param",
            )
        if view_field not in export_df.columns:
            raise HTTPException(400, f"view_field not in data: {view_field}")
        m = mask_view_in(export_df, view_field, list(values))
        export_df = export_df[m]
        # Allow empty export (same as an empty table) instead of 400
    suffix = f"_{scope}" if scope == "signature" else ""
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


@api_router.get("/filters")
def list_filters(grouped: bool = False) -> dict[str, Any]:
    keys = sorted(REGISTRY.keys())
    out: dict[str, Any] = {"filters": keys}
    if grouped:
        out["groups"] = {g: [f for f in fl if f in REGISTRY] for g, fl in FILTER_GROUPS.items()}
    return out


@api_router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@api_router.get("/version")
def version_info() -> dict[str, str]:
    return {
        "version": os.environ.get("APP_VERSION", API_VERSION),
        "build_time": os.environ.get("BUILD_TIME", "unknown"),
    }


app.include_router(api_router, prefix="/api")


def _install_frontend(spa: FastAPI) -> None:
    """
    Serve Vite production build at / and /assets/...; SPA fallback for client routes.
    Skips if frontend/dist is missing (local dev uses Vite on :5173).
    """
    root = Path(__file__).resolve().parents[2] / "frontend" / "dist"
    if not root.is_dir():
        return
    base = root.resolve()
    assets = root / "assets"
    if assets.is_dir():
        spa.mount("/assets", StaticFiles(directory=assets), name="static_assets")

    @spa.get("/")
    def _index() -> FileResponse:
        return FileResponse(root / "index.html")

    @spa.get("/{full_path:path}")
    def _spa_catchall(full_path: str) -> FileResponse:
        if full_path.startswith("api"):
            raise HTTPException(404, "not found")
        p = (root / full_path).resolve()
        try:
            p.relative_to(base)
        except ValueError:
            return FileResponse(root / "index.html")
        if p.is_file():
            return FileResponse(p)
        return FileResponse(root / "index.html")


_install_frontend(app)
