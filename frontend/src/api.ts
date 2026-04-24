/**
 * API base including the `/api` prefix.
 * - Production: default ` /api` (same origin as the single FastAPI service).
 * - Local Vite: default ` /api` with `server.proxy` → `http://127.0.0.1:8000`.
 * - Optional: `VITE_API_BASE_URL=http://127.0.0.1:8000/api` to call the API directly (no proxy).
 */
function apiBase(): string {
  const raw = import.meta.env.VITE_API_BASE_URL as string | undefined;
  if (raw != null && String(raw).trim() !== '') {
    return String(raw).replace(/\/$/, '');
  }
  return '/api';
}

const API = apiBase();

/** Rows where `field` is one of `values` (OR). Use `field: "signature"` for operator family. */
export type ViewFilter = { field: string; values: string[] };

export function formatViewFilterLabel(vf: ViewFilter): string {
  if (vf.values.length === 1) return `${vf.field} = ${vf.values[0]}`;
  return `${vf.field} ∈ {${vf.values.join(', ')}}`;
}

/**
 * API `view_filter` objects use `field` plus `value` and/or `values` (OR).
 * Used to recover a table/export filter from a stage created via “Apply” on a subset.
 */
export function viewFilterFromRecord(
  vf: Record<string, unknown> | null | undefined
): ViewFilter | null {
  if (!vf || typeof vf !== 'object') return null;
  const field = vf.field;
  if (typeof field !== 'string' || field === '') return null;
  const values: string[] = [];
  if (Array.isArray(vf.values)) {
    for (const x of vf.values) {
      if (x != null && String(x) !== '') values.push(String(x));
    }
  }
  if (values.length === 0 && vf.value != null && String(vf.value) !== '') {
    values.push(String(vf.value));
  }
  if (values.length === 0) return null;
  return { field, values };
}

export type Stage = {
  stage_id: number;
  stage_name: string;
  filter_type: string;
  filter_config: Record<string, unknown>;
  input_count: number;
  output_count: number;
  removed_count: number;
  per_filter_removed_count?: Record<string, number> | null;
  view_filter?: Record<string, unknown> | null;
  affected_row_count?: number | null;
  untouched_row_count?: number | null;
};

export type StageDetail = Stage & {
  summary_stats: Record<string, unknown>;
  previous_stage_id: number | null;
};

export type Dist = {
  signature: Record<string, number>;
  operator_family: Record<string, number>;
  stage: Record<string, number>;
  technique: Record<string, number>;
  problem_type?: Record<string, number>;
  source_model?: Record<string, number>;
  behavior_type?: Record<string, number>;
  runtime_ms_histogram: { bin_start: number; bin_end: number; count: number }[];
  correctness?: { true: number; false: number; missing: number };
  compiled?: { true: number; false: number };
};

export type StageViewResponse = {
  field: string;
  values: string[];
  value: string | null;
  total: number;
  summary_stats: Record<string, unknown>;
  distributions: Dist;
  rows: Record<string, unknown>[];
  limit: number;
  offset: number;
};

export type FilterGroupsResponse = {
  filters: string[];
  groups: Record<string, string[]>;
};

async function j<T>(r: Response | Promise<Response>): Promise<T> {
  const res = await r;
  if (!res.ok) {
    const t = await res.text();
    throw new Error(t || res.statusText);
  }
  return res.json() as Promise<T>;
}

export async function uploadJsonl(
  file: File
): Promise<{ dataset_id: string; stage0_count: number }> {
  const fd = new FormData();
  fd.append('file', file);
  const r = await fetch(`${API}/datasets/upload`, { method: 'POST', body: fd });
  if (!r.ok) {
    const t = await r.text();
    let msg = t || r.statusText;
    try {
      const o = JSON.parse(t) as { detail?: string | { msg?: string }[] };
      if (typeof o.detail === 'string') msg = o.detail;
    } catch {
      /* use raw */
    }
    if (r.status === 413) {
      throw new Error(
        'File too large for the server (413). If you use a reverse proxy, raise its body size limit, or use streaming upload to the API directly.'
      );
    }
    throw new Error(msg);
  }
  return r.json() as Promise<{ dataset_id: string; stage0_count: number }>;
}

export function listFilters(): Promise<{ filters: string[] }> {
  return j(fetch(`${API}/filters`));
}

export function listFiltersGrouped(): Promise<FilterGroupsResponse> {
  return j(fetch(`${API}/filters?grouped=true`));
}

export function listStages(datasetId: string): Promise<Stage[]> {
  return j(fetch(`${API}/datasets/${datasetId}/stages`));
}

export function getSummary(
  datasetId: string,
  stageId: number
): Promise<StageDetail> {
  return j(fetch(`${API}/datasets/${datasetId}/stages/${stageId}/summary`));
}

export function getRows(
  datasetId: string,
  stageId: number,
  limit = 200,
  offset = 0
): Promise<{ rows: Record<string, unknown>[]; total: number }> {
  return j(
    fetch(
      `${API}/datasets/${datasetId}/stages/${stageId}/rows?limit=${limit}&offset=${offset}`
    )
  );
}

export function getStageView(
  datasetId: string,
  stageId: number,
  view: ViewFilter,
  limit = 200,
  offset = 0
): Promise<StageViewResponse> {
  const q = new URLSearchParams({
    field: view.field,
    limit: String(limit),
    offset: String(offset),
  });
  for (const v of view.values) {
    q.append('values', v);
  }
  return j(
    fetch(
      `${API}/datasets/${datasetId}/stages/${stageId}/view?${q.toString()}`
    )
  );
}

export type RemovalCategory = 'hacking' | 'duplicate' | 'length' | 'format' | 'other';

export function getRemovedSummary(
  datasetId: string,
  stageId: number
): Promise<{
  total: number;
  row_count: number;
  by_category: Record<string, number>;
  by_signature: Record<string, number>;
}> {
  return j(
    fetch(`${API}/datasets/${datasetId}/stages/${stageId}/removed-summary`)
  );
}

export type GetRemovedOptions = {
  reasonCategories?: RemovalCategory[];
  /** Non-empty: OR-filter removed rows to these signature values. */
  signatures?: string[];
};

export function getRemoved(
  datasetId: string,
  stageId: number,
  limit = 200,
  offset = 0,
  options: GetRemovedOptions = {}
): Promise<{ rows: Record<string, unknown>[]; total: number }> {
  const q = new URLSearchParams({
    limit: String(limit),
    offset: String(offset),
  });
  const { reasonCategories, signatures } = options;
  if (reasonCategories && reasonCategories.length > 0) {
    for (const c of reasonCategories) {
      q.append('categories', c);
    }
  }
  if (signatures && signatures.length > 0) {
    for (const s of signatures) {
      q.append('signatures', s);
    }
  }
  return j(
    fetch(
      `${API}/datasets/${datasetId}/stages/${stageId}/removed-rows?${q.toString()}`
    )
  );
}

export function getDistribution(
  datasetId: string,
  stageId: number
): Promise<Dist> {
  return j(
    fetch(`${API}/datasets/${datasetId}/stages/${stageId}/distribution`)
  );
}

export type SignaturesByStageRow = {
  stage_id: number;
  total: number;
  by_signature: Record<string, number>;
};

export function getSignaturesByStage(
  datasetId: string
): Promise<{ stages: SignaturesByStageRow[] }> {
  return j(fetch(`${API}/datasets/${datasetId}/signatures-by-stage`));
}

export function applyFilter(
  datasetId: string,
  body: {
    stage_id: number;
    filter_type: string;
    filter_config: Record<string, unknown>;
  }
): Promise<{
  new_stage_id: number;
  input_count: number;
  output_count: number;
  removed_count: number;
  per_filter_removed_count: Record<string, number>;
}> {
  return j(
    fetch(`${API}/datasets/${datasetId}/apply-filter`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
  );
}

export function applyFilters(
  datasetId: string,
  body: {
    base_stage_id: number;
    view_filter: ViewFilter | null;
    filters: { filter_type: string; filter_config: Record<string, unknown> }[];
  }
): Promise<{
  new_stage_id: number;
  input_count: number;
  output_count: number;
  removed_count: number;
  per_filter_removed_count: Record<string, number>;
  affected_row_count: number;
  untouched_row_count: number;
}> {
  return j(
    fetch(`${API}/datasets/${datasetId}/apply-filters`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
  );
}

export type VersionInfo = { version: string; build_time: string };

export function getVersion(): Promise<VersionInfo> {
  return j(fetch(`${API}/version`));
}

export function exportUrl(
  datasetId: string,
  stageId: number,
  format: 'jsonl' | 'csv' | 'filter_log',
  options?: {
    /** full = entire stage kept rows; signature = only rows matching the current table filter */
    scope: 'full' | 'signature';
    viewFilter: ViewFilter | null;
  }
): string {
  const q = new URLSearchParams({
    stage_id: String(stageId),
    format,
  });
  if (format === 'filter_log') {
    return `${API}/datasets/${datasetId}/export?${q.toString()}`;
  }
  if (options?.scope === 'signature' && options.viewFilter) {
    q.set('scope', 'signature');
    q.set('view_field', options.viewFilter.field);
    for (const v of options.viewFilter.values) {
      q.append('values', v);
    }
  } else {
    q.set('scope', 'full');
  }
  return `${API}/datasets/${datasetId}/export?${q.toString()}`;
}
