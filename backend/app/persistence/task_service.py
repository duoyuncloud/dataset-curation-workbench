from __future__ import annotations

import json
import shutil
import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from ..dataset_store import Stage
from ..export import build_filter_log
from ..filters.batch import apply_filters_independent_batch, mask_subset_filter, mask_view_in
from ..filters.pipeline import apply_filter, REGISTRY
from ..filters.signature_extraction import enrich_dataframe_signatures, ensure_stage_focus_column
from ..models import ApplyFiltersBody, FilterApplyBody, ViewFilterIn
from ..stats import compute_summary_and_distributions
from .config import data_dir, database_url
from .storage_backend import LocalStorageBackend


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA foreign_keys = ON;
        CREATE TABLE IF NOT EXISTS tasks (
            task_id TEXT PRIMARY KEY,
            task_name TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            dataset_name TEXT NOT NULL DEFAULT '',
            current_stage_id INTEGER NOT NULL DEFAULT 0,
            total_rows INTEGER NOT NULL DEFAULT 0,
            num_stages INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS stages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id TEXT NOT NULL,
            stage_index INTEGER NOT NULL,
            base_stage_id INTEGER,
            stage_name TEXT NOT NULL,
            filter_type TEXT NOT NULL,
            filter_config_json TEXT NOT NULL,
            view_filter_json TEXT,
            input_count INTEGER NOT NULL,
            output_count INTEGER NOT NULL,
            removed_count INTEGER NOT NULL,
            affected_count INTEGER,
            untouched_count INTEGER,
            kept_file_path TEXT NOT NULL,
            removed_file_path TEXT NOT NULL,
            per_filter_removed_json TEXT,
            created_at TEXT NOT NULL,
            UNIQUE(task_id, stage_index),
            FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_stages_task ON stages(task_id);
        """
    )
    conn.commit()


class TaskService:
    """SQLite metadata + JSONL files under DATA_DIR/tasks/{task_id}/."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # Schema/bootstrap only — never contend with long `with self._lock` writers (e.g. upload_raw).
        self._init_lock = threading.Lock()
        self._root = data_dir()
        self._tasks_root = self._root / "tasks"
        self._storage = LocalStorageBackend(self._root)
        db_url = database_url()
        if db_url and not db_url.startswith("sqlite"):
            raise RuntimeError("Only sqlite DATABASE_URL is supported for now; omit for default SQLite file.")
        self._db_path = self._root / "tasks.db"
        self._initialized = False

    def initialize(self) -> None:
        with self._init_lock:
            if self._initialized:
                return
            self._root.mkdir(parents=True, exist_ok=True)
            self._tasks_root.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(self._db_path, check_same_thread=False)
            try:
                _init_schema(conn)
            finally:
                conn.close()
            self._initialized = True

    def _conn(self) -> sqlite3.Connection:
        c = sqlite3.connect(self._db_path, check_same_thread=False)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA foreign_keys = ON")
        return c

    def _task_rel(self, task_id: str) -> str:
        return f"tasks/{task_id}"

    def create_task(self, task_name: str) -> str:
        tid = str(uuid.uuid4())
        now = _utc_now()
        with self._lock:
            conn = self._conn()
            try:
                conn.execute(
                    "INSERT INTO tasks (task_id, task_name, created_at, updated_at, dataset_name, current_stage_id, total_rows, num_stages) VALUES (?,?,?,?,?,?,?,?)",
                    (tid, task_name, now, now, "", 0, 0, 0),
                )
                conn.commit()
            finally:
                conn.close()
        (self._root / self._task_rel(tid)).mkdir(parents=True, exist_ok=True)
        (self._root / self._task_rel(tid) / "raw").mkdir(exist_ok=True)
        (self._root / self._task_rel(tid) / "stages").mkdir(exist_ok=True)
        (self._root / self._task_rel(tid) / "meta").mkdir(exist_ok=True)
        (self._root / self._task_rel(tid) / "meta" / "filter_log.json").write_text("[]", encoding="utf-8")
        return tid

    def list_tasks(self) -> list[dict[str, Any]]:
        self.initialize()
        conn = self._conn()
        try:
            cur = conn.execute(
                "SELECT task_id, task_name, created_at, updated_at, dataset_name, current_stage_id, total_rows, num_stages FROM tasks ORDER BY updated_at DESC"
            )
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    def get_task(self, task_id: str) -> Optional[dict[str, Any]]:
        self.initialize()
        conn = self._conn()
        try:
            cur = conn.execute(
                "SELECT task_id, task_name, created_at, updated_at, dataset_name, current_stage_id, total_rows, num_stages FROM tasks WHERE task_id = ?",
                (task_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def delete_task(self, task_id: str) -> bool:
        self.initialize()
        with self._lock:
            conn = self._conn()
            try:
                conn.execute("DELETE FROM tasks WHERE task_id = ?", (task_id,))
                conn.commit()
            finally:
                conn.close()
        p = self._root / self._task_rel(task_id)
        if p.is_dir():
            shutil.rmtree(p, ignore_errors=True)
        return True

    def patch_task(self, task_id: str, task_name: Optional[str]) -> bool:
        if not task_name:
            return False
        self.initialize()
        with self._lock:
            conn = self._conn()
            try:
                cur = conn.execute(
                    "UPDATE tasks SET task_name = ?, updated_at = ? WHERE task_id = ?",
                    (task_name, _utc_now(), task_id),
                )
                conn.commit()
                return cur.rowcount > 0
            finally:
                conn.close()

    def _filter_log_read(self, task_id: str) -> list[dict[str, Any]]:
        path = self._root / self._task_rel(task_id) / "meta" / "filter_log.json"
        if not path.is_file():
            return []
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return []

    def _filter_log_write(self, task_id: str, entries: list[dict[str, Any]]) -> None:
        path = self._root / self._task_rel(task_id) / "meta" / "filter_log.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(entries, indent=2, ensure_ascii=False), encoding="utf-8")

    def _filter_log_append(self, task_id: str, entry: dict[str, Any]) -> None:
        with self._lock:
            log = self._filter_log_read(task_id)
            log.append(entry)
            self._filter_log_write(task_id, log)

    def _stage_rel_paths(self, task_id: str, stage_index: int) -> tuple[str, str]:
        base = f"{self._task_rel(task_id)}/stages/stage_{stage_index}"
        return f"{base}/kept.jsonl", f"{base}/removed.jsonl"

    def upload_raw(self, task_id: str, df: pd.DataFrame, source_name: str) -> int:
        """Persist raw + enriched stage 0; replaces existing stages for this task."""
        self.initialize()
        if df is None or len(df) == 0:
            raise ValueError("empty dataframe")
        if not self.get_task(task_id):
            raise KeyError("task not found")

        raw_rel = f"{self._task_rel(task_id)}/raw/input.jsonl"
        enriched = enrich_dataframe_signatures(df)
        kept_rel, removed_rel = self._stage_rel_paths(task_id, 0)

        summary, dist = compute_summary_and_distributions(enriched)
        summary["input_count"] = len(enriched)
        summary["output_count"] = len(enriched)
        summary["removed_count"] = 0
        summary["removal_ratio"] = 0.0

        stage_row = Stage(
            stage_id=0,
            stage_name="Raw dataset",
            filter_type="raw",
            filter_config={},
            input_count=len(enriched),
            output_count=len(enriched),
            removed_count=0,
            kept_rows=enriched,
            removed_rows=pd.DataFrame(),
            summary_stats=summary,
            distributions=dist,
            input_stage_id=None,
        )

        with self._lock:
            conn = self._conn()
            try:
                conn.execute("DELETE FROM stages WHERE task_id = ?", (task_id,))
                conn.commit()
            finally:
                conn.close()
            # Remove old stage dirs, then write fresh stage_0 files (never rmtree after writing).
            stages_dir = self._root / self._task_rel(task_id) / "stages"
            if stages_dir.is_dir():
                for child in list(stages_dir.iterdir()):
                    if child.is_dir() and child.name.startswith("stage_"):
                        shutil.rmtree(child, ignore_errors=True)

            self._storage.save_jsonl(raw_rel, df.reset_index(drop=True))
            self._storage.save_jsonl(kept_rel, enriched)
            self._storage.save_jsonl(removed_rel, pd.DataFrame())

            self._persist_stage_record(
                task_id,
                0,
                None,
                stage_row,
                kept_rel,
                removed_rel,
                None,
                None,
            )
            now = _utc_now()
            conn = self._conn()
            try:
                conn.execute(
                    "UPDATE tasks SET dataset_name = ?, total_rows = ?, num_stages = ?, current_stage_id = 0, updated_at = ? WHERE task_id = ?",
                    (source_name, len(enriched), 1, now, task_id),
                )
                conn.commit()
            finally:
                conn.close()
            self._filter_log_write(task_id, [])
        return len(enriched)

    def _persist_stage_record(
        self,
        task_id: str,
        stage_index: int,
        base_stage_id: Optional[int],
        stage: Stage,
        kept_rel: str,
        removed_rel: str,
        view_filter: Optional[dict[str, Any]],
        per_filter_removed: Optional[dict[str, int]],
    ) -> None:
        conn = self._conn()
        try:
            conn.execute(
                """INSERT INTO stages (
                    task_id, stage_index, base_stage_id, stage_name, filter_type, filter_config_json,
                    view_filter_json, input_count, output_count, removed_count, affected_count, untouched_count,
                    kept_file_path, removed_file_path, per_filter_removed_json, created_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    task_id,
                    stage_index,
                    base_stage_id,
                    stage.stage_name,
                    stage.filter_type,
                    json.dumps(stage.filter_config, ensure_ascii=False),
                    json.dumps(view_filter, ensure_ascii=False) if view_filter is not None else None,
                    stage.input_count,
                    stage.output_count,
                    stage.removed_count,
                    stage.affected_row_count,
                    stage.untouched_row_count,
                    kept_rel,
                    removed_rel,
                    json.dumps(per_filter_removed, ensure_ascii=False) if per_filter_removed else None,
                    _utc_now(),
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def list_stage_rows(self, task_id: str) -> list[sqlite3.Row]:
        self.initialize()
        conn = self._conn()
        try:
            cur = conn.execute(
                "SELECT * FROM stages WHERE task_id = ? ORDER BY stage_index ASC",
                (task_id,),
            )
            return cur.fetchall()
        finally:
            conn.close()

    def get_stage_meta(self, task_id: str, stage_index: int) -> Optional[sqlite3.Row]:
        self.initialize()
        conn = self._conn()
        try:
            cur = conn.execute(
                "SELECT * FROM stages WHERE task_id = ? AND stage_index = ?",
                (task_id, stage_index),
            )
            return cur.fetchone()
        finally:
            conn.close()

    def kept_jsonl_path(self, task_id: str, stage_index: int) -> Path:
        """Absolute path to this stage's ``kept.jsonl`` (for streaming stats without a full DataFrame)."""
        row = self.get_stage_meta(task_id, stage_index)
        if not row:
            raise KeyError("stage not found")
        return self._storage.resolve_path(row["kept_file_path"])

    def load_kept(self, task_id: str, stage_index: int) -> pd.DataFrame:
        row = self.get_stage_meta(task_id, stage_index)
        if not row:
            raise KeyError("stage not found")
        return ensure_stage_focus_column(self._storage.load_jsonl(row["kept_file_path"]))

    def load_removed(self, task_id: str, stage_index: int) -> pd.DataFrame:
        row = self.get_stage_meta(task_id, stage_index)
        if not row:
            raise KeyError("stage not found")
        return ensure_stage_focus_column(self._storage.load_jsonl(row["removed_file_path"]))

    def _append_stage_disk_and_db(
        self,
        task_id: str,
        new_index: int,
        base_stage_id: Optional[int],
        stage: Stage,
        view_filter: Optional[dict[str, Any]],
        log_entry: dict[str, Any],
    ) -> None:
        kept_rel, removed_rel = self._stage_rel_paths(task_id, new_index)
        self._storage.save_jsonl(kept_rel, stage.kept_rows)
        self._storage.save_jsonl(removed_rel, stage.removed_rows if len(stage.removed_rows) else pd.DataFrame())

        s2, d2 = compute_summary_and_distributions(stage.kept_rows)
        stage.summary_stats = s2
        stage.summary_stats["input_count"] = stage.input_count
        stage.summary_stats["output_count"] = stage.output_count
        stage.summary_stats["removed_count"] = stage.removed_count
        stage.summary_stats["total_samples"] = stage.output_count
        stage.summary_stats["removed_samples"] = stage.removed_count
        stage.summary_stats["removal_ratio"] = (
            stage.removed_count / stage.input_count if stage.input_count else 0.0
        )
        stage.distributions = d2
        if stage.per_filter_removed_count:
            stage.summary_stats["per_filter_removed_count"] = stage.per_filter_removed_count
        if stage.view_filter is not None:
            stage.summary_stats["view_filter"] = stage.view_filter
        if stage.affected_row_count is not None:
            stage.summary_stats["affected_row_count"] = stage.affected_row_count
        if stage.untouched_row_count is not None:
            stage.summary_stats["untouched_row_count"] = stage.untouched_row_count

        self._persist_stage_record(
            task_id,
            new_index,
            base_stage_id,
            stage,
            kept_rel,
            removed_rel,
            view_filter,
            stage.per_filter_removed_count,
        )
        le = {**log_entry, "stage": new_index, "filter": stage.filter_type}
        self._filter_log_append(task_id, le)
        now = _utc_now()
        out_rows = int(stage.output_count)
        conn = self._conn()
        try:
            conn.execute(
                "UPDATE tasks SET current_stage_id = ?, num_stages = ?, total_rows = ?, updated_at = ? WHERE task_id = ?",
                (new_index, new_index + 1, out_rows, now, task_id),
            )
            conn.commit()
        finally:
            conn.close()

    def apply_single_filter(self, task_id: str, body: FilterApplyBody) -> dict[str, Any]:
        self.initialize()
        rows = self.list_stage_rows(task_id)
        if not rows:
            raise KeyError("no stages")
        n = len(rows)
        if body.stage_id < 0 or body.stage_id >= n:
            raise ValueError("invalid stage_id")
        if body.filter_type not in REGISTRY:
            raise ValueError("unknown filter_type")

        base = self.load_kept(task_id, body.stage_id)
        res = apply_filter(body.filter_type, base.copy(), body.filter_config)
        new_index = n
        removed = res.removed
        if len(removed) and "removed_at_stage" not in removed.columns:
            removed = removed.copy()
            removed["removed_at_stage"] = new_index
        pfc = {f"0:{res.filter_type}": len(removed)}

        name = {
            "raw": "Raw",
            "remove_hacking": "Hacking removed",
            "remove_duplicates": "Deduped",
        }.get(res.filter_type, res.filter_type)

        stage = Stage(
            stage_id=new_index,
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
        log_entry: dict[str, Any] = {"from_stage": body.stage_id, "filter_type": res.filter_type}
        with self._lock:
            self._append_stage_disk_and_db(task_id, new_index, body.stage_id, stage, None, log_entry)
        return {
            "new_stage_id": new_index,
            "input_count": res.input_count,
            "output_count": len(res.kept),
            "removed_count": len(removed),
            "per_filter_removed_count": pfc,
        }

    def apply_batch_filters(self, task_id: str, body: ApplyFiltersBody) -> dict[str, Any]:
        self.initialize()
        rows = self.list_stage_rows(task_id)
        if not rows:
            raise KeyError("no stages")
        n = len(rows)
        if body.base_stage_id < 0 or body.base_stage_id >= n:
            raise ValueError("invalid base_stage_id")
        for f in body.filters:
            if f.filter_type not in REGISTRY:
                raise ValueError(f"unknown filter_type: {f.filter_type}")

        specs = [{"filter_type": f.filter_type, "filter_config": dict(f.filter_config)} for f in body.filters]
        base = self.load_kept(task_id, body.base_stage_id).copy()

        vf: dict[str, Any] | None = None
        if body.subset_filter is not None and body.subset_filter.is_active():
            sigs = body.subset_filter.signature_values()
            sfo = body.subset_filter.stage_focus_values()
            m = mask_subset_filter(base, sigs, sfo)
            subset = base[m].copy()
            untouched = base[~m].copy()
            vf = {"subset_filter": body.subset_filter.to_stored_dict()}
        elif body.view_filter is not None:
            vfi = body.view_filter
            vals = vfi.mask_values()
            vf = {"field": vfi.field, "values": vals}
            if len(vals) == 1:
                vf["value"] = vals[0]
            if vfi.field not in base.columns:
                raise ValueError(f"field not in data: {vfi.field}")
            m = mask_view_in(base, vfi.field, vals)
            subset = base[m].copy()
            untouched = base[~m].copy()
        else:
            subset = base
            untouched = base.head(0).copy()

        kept_sub, removed, pfc = apply_filters_independent_batch(subset, specs)
        n_untouched = len(untouched)
        n_aff = len(subset)
        n_removed = len(removed)
        if n_untouched:
            final_kept = pd.concat([untouched, kept_sub], ignore_index=True)
        else:
            final_kept = kept_sub

        new_index = n
        if len(removed) and "removed_at_stage" not in removed.columns:
            removed = removed.copy()
            removed["removed_at_stage"] = new_index

        n_batch = len(specs)
        stage_name = f"Batch ({n_batch} filter{'s' if n_batch != 1 else ''})"
        if vf:
            if "subset_filter" in vf:
                sf = vf["subset_filter"] or {}
                sigs = sf.get("signatures") or []
                sfo = sf.get("stage_focuses") or []
                bits: list[str] = []
                if sigs:
                    bits.append(f"signature∈{sigs!r}")
                if sfo:
                    bits.append(f"stage_focus∈{sfo!r}")
                label = " AND ".join(bits) if bits else "subset"
                stage_name = f"Batch on subset: {label} ({n_batch} filters)"
            else:
                shown = vf.get("values") or [vf.get("value")]
                stage_name = f"Batch on subset: {vf['field']} in {shown!r} ({n_batch} filters)"
        fcfg: dict[str, Any] = {"filters": specs, "view_filter": vf}
        stage = Stage(
            stage_id=new_index,
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
        with self._lock:
            self._append_stage_disk_and_db(task_id, new_index, body.base_stage_id, stage, vf, log_entry)
        return {
            "new_stage_id": new_index,
            "input_count": n_aff,
            "output_count": len(final_kept),
            "removed_count": n_removed,
            "per_filter_removed_count": pfc,
            "affected_row_count": n_aff,
            "untouched_row_count": n_untouched,
        }

    def truncate_stages_from(self, task_id: str, from_stage_index: int) -> dict[str, Any]:
        """
        Delete ``from_stage_index`` and every later stage (DB rows + ``stages/stage_*`` dirs).
        Stages ``0 .. from_stage_index - 1`` remain. Stage 0 cannot be removed this way (re-upload to replace raw).
        """
        if from_stage_index <= 0:
            raise ValueError("cannot truncate from stage 0; upload a new JSONL to replace the raw dataset")
        self.initialize()
        if not self.get_task(task_id):
            raise KeyError("task not found")
        rows = self.list_stage_rows(task_id)
        n = len(rows)
        if n == 0:
            raise ValueError("no stages")
        if from_stage_index >= n:
            raise ValueError("invalid stage index: nothing to remove at this step")

        t = self.get_task(task_id)
        if t is None:
            raise KeyError("task not found")
        old_cur = int(t["current_stage_id"])
        keep_upto = from_stage_index - 1
        last_meta = self.get_stage_meta(task_id, keep_upto)
        if last_meta is None:
            raise ValueError("internal error: missing stage metadata")
        total_rows = int(last_meta["output_count"])
        new_num = from_stage_index
        new_current = min(old_cur, keep_upto)
        now = _utc_now()

        with self._lock:
            conn = self._conn()
            try:
                conn.execute(
                    "DELETE FROM stages WHERE task_id = ? AND stage_index >= ?",
                    (task_id, from_stage_index),
                )
                conn.commit()
            finally:
                conn.close()

            stages_root = self._root / self._task_rel(task_id) / "stages"
            for i in range(from_stage_index, n):
                d = stages_root / f"stage_{i}"
                if d.is_dir():
                    shutil.rmtree(d, ignore_errors=True)

            log = self._filter_log_read(task_id)
            log_f = [
                e
                for e in log
                if not (isinstance(e.get("stage"), int) and e["stage"] >= from_stage_index)
            ]
            self._filter_log_write(task_id, log_f)

            conn = self._conn()
            try:
                conn.execute(
                    "UPDATE tasks SET num_stages = ?, current_stage_id = ?, total_rows = ?, updated_at = ? WHERE task_id = ?",
                    (new_num, new_current, total_rows, now, task_id),
                )
                conn.commit()
            finally:
                conn.close()

        return {
            "task_id": task_id,
            "num_stages": new_num,
            "current_stage_id": new_current,
            "truncated_from": from_stage_index,
            "total_rows": total_rows,
        }

    def build_filter_log_export(self, task_id: str) -> str:
        return build_filter_log(self._filter_log_read(task_id))

    def row_to_stage_summary(self, row: sqlite3.Row) -> dict[str, Any]:
        d = dict(row)
        fc = json.loads(d["filter_config_json"]) if d.get("filter_config_json") else {}
        vf = json.loads(d["view_filter_json"]) if d.get("view_filter_json") else None
        pfc = json.loads(d["per_filter_removed_json"]) if d.get("per_filter_removed_json") else None
        return {
            "stage_id": d["stage_index"],
            "stage_name": d["stage_name"],
            "filter_type": d["filter_type"],
            "filter_config": fc,
            "input_count": d["input_count"],
            "output_count": d["output_count"],
            "removed_count": d["removed_count"],
            "per_filter_removed_count": pfc,
            "view_filter": vf,
            "affected_row_count": d["affected_count"],
            "untouched_row_count": d["untouched_count"],
        }

    def stage_as_runtime(self, task_id: str, stage_index: int) -> Stage:
        """Load stage from disk + DB into a Stage object (for summary/distribution/export)."""
        row = self.get_stage_meta(task_id, stage_index)
        if not row:
            raise KeyError("stage not found")
        kept = self.load_kept(task_id, stage_index)
        removed = self.load_removed(task_id, stage_index)
        r = dict(row)
        fc = json.loads(r["filter_config_json"]) if r.get("filter_config_json") else {}
        vf = json.loads(r["view_filter_json"]) if r.get("view_filter_json") else None
        pfc = json.loads(r["per_filter_removed_json"]) if r.get("per_filter_removed_json") else None
        s2, d2 = compute_summary_and_distributions(kept)
        s2["input_count"] = r["input_count"]
        s2["output_count"] = r["output_count"]
        s2["removed_count"] = r["removed_count"]
        s2["total_samples"] = r["output_count"]
        s2["removed_samples"] = r["removed_count"]
        s2["removal_ratio"] = r["removed_count"] / r["input_count"] if r["input_count"] else 0.0
        if pfc:
            s2["per_filter_removed_count"] = pfc
        if vf is not None:
            s2["view_filter"] = vf
        if r.get("affected_count") is not None:
            s2["affected_row_count"] = r["affected_count"]
        if r.get("untouched_count") is not None:
            s2["untouched_row_count"] = r["untouched_count"]
        return Stage(
            stage_id=r["stage_index"],
            stage_name=r["stage_name"],
            filter_type=r["filter_type"],
            filter_config=fc,
            input_count=r["input_count"],
            output_count=r["output_count"],
            removed_count=r["removed_count"],
            kept_rows=kept,
            removed_rows=removed,
            summary_stats=s2,
            distributions=d2,
            input_stage_id=r["base_stage_id"],
            per_filter_removed_count=pfc,
            view_filter=vf,
            base_stage_id=r["base_stage_id"],
            affected_row_count=r["affected_count"],
            untouched_row_count=r["untouched_count"],
        )
