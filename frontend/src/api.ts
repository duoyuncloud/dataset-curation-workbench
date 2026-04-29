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

/** Active subset: OR within each list; AND across ``signatures`` vs ``stageFocus``. */
export type SubsetFilter = {
  signatures: string[];
  stageFocus: string[];
  /** Server-side Python: ``subset_mask(df, config)`` → Series[bool], True = row is in subset */
  subsetScript?: string;
  subsetScriptConfig?: Record<string, unknown>;
};

export function emptySubset(): SubsetFilter {
  return { signatures: [], stageFocus: [] };
}

export function subsetFilterActive(s: SubsetFilter | null | undefined): boolean {
  if (!s) return false;
  if (s.subsetScript != null && String(s.subsetScript).trim() !== '') return true;
  return s.signatures.length > 0 || s.stageFocus.length > 0;
}

export function formatSubsetFilterLabel(s: SubsetFilter): string {
  const parts: string[] = [];
  if (s.subsetScript != null && String(s.subsetScript).trim() !== '') parts.push('subset_script');
  if (s.signatures.length)
    parts.push(`signature ∈ {${s.signatures.join(', ')}}`);
  if (s.stageFocus.length)
    parts.push(`stage_focus ∈ {${s.stageFocus.join(', ')}}`);
  return parts.join(' AND ') || 'subset';
}

export function subsetFilterToApiBody(
  s: SubsetFilter
): Record<string, unknown> | null {
  if (!subsetFilterActive(s)) return null;
  const o: Record<string, unknown> = {};
  if (s.signatures.length === 1) o.signature = s.signatures[0];
  else if (s.signatures.length > 1) o.signatures = [...s.signatures];
  if (s.stageFocus.length === 1) o.stage_focus = s.stageFocus[0];
  else if (s.stageFocus.length > 1) o.stage_focuses = [...s.stageFocus];
  if (s.subsetScript != null && String(s.subsetScript).trim() !== '') {
    o.subset_script = s.subsetScript;
    o.subset_script_config = s.subsetScriptConfig ?? {};
  }
  return o;
}

/**
 * Recover subset from stage `view_filter` JSON (new `subset_filter` or legacy `field`/`values`).
 */
export function subsetFilterFromStageRecord(
  vf: Record<string, unknown> | null | undefined
): SubsetFilter | null {
  if (!vf || typeof vf !== 'object') return null;
  const inner = vf.subset_filter;
  if (inner && typeof inner === 'object') {
    const sf = inner as Record<string, unknown>;
    const sigs: string[] = [];
    const sfo: string[] = [];
    if (Array.isArray(sf.signatures)) {
      for (const x of sf.signatures) {
        if (x != null && String(x).trim() !== '') sigs.push(String(x).trim());
      }
    }
    if (Array.isArray(sf.stage_focuses)) {
      for (const x of sf.stage_focuses) {
        if (x != null && String(x).trim() !== '') sfo.push(String(x).trim());
      }
    }
    const script = typeof sf.subset_script === 'string' ? sf.subset_script : '';
    const scfg =
      sf.subset_script_config && typeof sf.subset_script_config === 'object'
        ? (sf.subset_script_config as Record<string, unknown>)
        : undefined;
    if (sigs.length || sfo.length || String(script).trim() !== '') {
      const out: SubsetFilter = {
        signatures: sigs,
        stageFocus: sfo,
      };
      if (String(script).trim() !== '') {
        out.subsetScript = script;
        if (scfg) out.subsetScriptConfig = scfg;
      }
      return out;
    }
  }
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
  if (field === 'signature') return { signatures: values, stageFocus: [] };
  if (field === 'stage_focus') return { signatures: [], stageFocus: values };
  return null;
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
  stage_focus: Record<string, number>;
  technique: Record<string, number>;
  problem_type?: Record<string, number>;
  source_model?: Record<string, number>;
  behavior_type?: Record<string, number>;
  runtime_ms_histogram: { bin_start: number; bin_end: number; count: number }[];
  correctness?: { true: number; false: number; missing: number };
  compiled?: { true: number; false: number };
};

export type StageViewResponse = {
  subset_filter: {
    signatures: string[];
    stage_focuses: string[];
  } | null;
  field: string | null;
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

export type TaskRow = {
  task_id: string;
  task_name: string;
  created_at: string;
  updated_at: string;
  dataset_name: string;
  current_stage_id: number;
  total_rows: number;
  num_stages: number;
};

async function j<T>(r: Response | Promise<Response>): Promise<T> {
  const res = await r;
  if (!res.ok) {
    const t = await res.text();
    throw new Error(t || res.statusText);
  }
  return res.json() as Promise<T>;
}

const LIST_TASKS_TIMEOUT_MS = 45_000;

function isAbortError(e: unknown): boolean {
  return (
    (e instanceof DOMException || e instanceof Error) &&
    e.name === 'AbortError'
  );
}

/** GET JSON with timeout; optional `outerSignal` abort = caller cancelled (not a timeout). */
async function fetchJsonWithTimeout<T>(
  url: string,
  init: RequestInit | undefined,
  timeoutMs: number,
  outerSignal?: AbortSignal
): Promise<T> {
  const timeoutCtrl = new AbortController();
  const t = setTimeout(() => timeoutCtrl.abort(), timeoutMs);
  let signal: AbortSignal = timeoutCtrl.signal;
  if (outerSignal) {
    if (
      typeof AbortSignal !== 'undefined' &&
      typeof AbortSignal.any === 'function'
    ) {
      signal = AbortSignal.any([outerSignal, timeoutCtrl.signal]);
    } else {
      if (outerSignal.aborted) timeoutCtrl.abort();
      else
        outerSignal.addEventListener('abort', () => timeoutCtrl.abort(), {
          once: true,
        });
    }
  }
  try {
    return await j(
      fetch(url, {
        ...init,
        signal,
      })
    );
  } catch (e) {
    if (isAbortError(e)) {
      if (outerSignal?.aborted) throw e;
      if (timeoutCtrl.signal.aborted) {
        throw new Error(`Request timed out after ${Math.round(timeoutMs / 1000)}s`);
      }
      throw e;
    }
    throw e;
  } finally {
    clearTimeout(t);
  }
}

export function listTasks(signal?: AbortSignal): Promise<TaskRow[]> {
  return fetchJsonWithTimeout<TaskRow[]>(
    `${API}/tasks`,
    undefined,
    LIST_TASKS_TIMEOUT_MS,
    signal
  );
}

export function getTask(taskId: string): Promise<TaskRow> {
  return j(fetch(`${API}/tasks/${encodeURIComponent(taskId)}`));
}

/** Create an empty task; then upload JSONL to `POST /tasks/{task_id}/datasets/upload`. */
export async function createTask(taskName = 'Untitled task'): Promise<TaskRow> {
  return j(
    fetch(`${API}/tasks`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ task_name: taskName }),
    })
  );
}

export function patchTask(taskId: string, taskName: string): Promise<TaskRow> {
  return j(
    fetch(`${API}/tasks/${encodeURIComponent(taskId)}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ task_name: taskName }),
    })
  );
}

export function deleteTask(taskId: string): Promise<{ ok: string }> {
  return j(
    fetch(`${API}/tasks/${encodeURIComponent(taskId)}`, { method: 'DELETE' })
  );
}

export type UploadProgressEvent =
  | { phase: 'upload'; loaded: number; total: number }
  | { phase: 'processing' };

export async function uploadJsonl(
  taskId: string,
  file: File,
  onProgress?: (ev: UploadProgressEvent) => void
): Promise<{ task_id: string; stage0_count: number }> {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open('POST', `${API}/tasks/${encodeURIComponent(taskId)}/datasets/upload`);

    xhr.upload.addEventListener('progress', (e) => {
      if (e.lengthComputable && onProgress) {
        onProgress({ phase: 'upload', loaded: e.loaded, total: e.total });
      }
    });
    xhr.upload.addEventListener('load', () => {
      if (onProgress) onProgress({ phase: 'processing' });
    });

    xhr.onload = () => {
      const text = xhr.responseText || '';
      if (xhr.status >= 200 && xhr.status < 300) {
        try {
          const body = JSON.parse(text) as { task_id?: string; stage0_count?: number };
          resolve({
            task_id: String(body.task_id ?? taskId),
            stage0_count: Number(body.stage0_count ?? 0),
          });
        } catch {
          reject(new Error('Invalid response from server'));
        }
        return;
      }
      let msg = xhr.statusText || 'Upload failed';
      try {
        const o = JSON.parse(text) as { detail?: string };
        if (typeof o.detail === 'string') msg = o.detail;
      } catch {
        /* keep */
      }
      if (xhr.status === 413) {
        reject(
          new Error(
            'File too large for the server (413). If you use a reverse proxy, raise its body size limit, or use streaming upload to the API directly.'
          )
        );
        return;
      }
      reject(new Error(msg));
    };
    xhr.onerror = () => reject(new Error('Network error during upload'));
    const fd = new FormData();
    fd.append('file', file);
    xhr.send(fd);
  });
}

export function loadJsonlFromPath(
  taskId: string,
  path: string
): Promise<{ task_id: string; stage0_count: number }> {
  return j(
    fetch(`${API}/tasks/${encodeURIComponent(taskId)}/datasets/load-from-path`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ path: path.trim() }),
    })
  );
}

export type DatasetImportStreamResult = {
  task_id: string;
  stage0_count: number;
  message?: string;
};

async function readDatasetImportNdjson(
  res: Response,
  onProgress: (pct: number, message: string) => void
): Promise<DatasetImportStreamResult> {
  if (!res.ok) {
    const t = await res.text();
    throw new Error(t || `HTTP ${res.status}`);
  }
  const reader = res.body?.getReader();
  if (!reader) throw new Error('No response body');
  const dec = new TextDecoder();
  let buf = '';
  let donePayload: DatasetImportStreamResult | null = null;
  while (true) {
    const { value, done } = await reader.read();
    if (value) buf += dec.decode(value, { stream: true });
    const lines = buf.split('\n');
    buf = lines.pop() ?? '';
    for (const line of lines) {
      const s = line.trim();
      if (!s) continue;
      let ev: Record<string, unknown>;
      try {
        ev = JSON.parse(s) as Record<string, unknown>;
      } catch {
        continue;
      }
      const typ = ev.type as string;
      if (typ === 'progress') {
        const pct = typeof ev.pct === 'number' ? ev.pct : 0;
        const message = typeof ev.message === 'string' ? ev.message : '';
        onProgress(pct, message);
      } else if (typ === 'error') {
        throw new Error(typeof ev.message === 'string' ? ev.message : 'Import failed');
      } else if (typ === 'done') {
        donePayload = {
          task_id: String(ev.task_id ?? ''),
          stage0_count: Number(ev.stage0_count ?? 0),
          message: typeof ev.message === 'string' ? ev.message : undefined,
        };
      }
    }
    if (done) {
      const tail = buf.trim();
      if (tail && !donePayload) {
        try {
          const ev = JSON.parse(tail) as Record<string, unknown>;
          if (ev.type === 'done') {
            donePayload = {
              task_id: String(ev.task_id ?? ''),
              stage0_count: Number(ev.stage0_count ?? 0),
              message: typeof ev.message === 'string' ? ev.message : undefined,
            };
          }
        } catch {
          /* ignore */
        }
      }
      break;
    }
  }
  if (!donePayload) throw new Error('Stream ended without result');
  return donePayload;
}

/** Server path import with real progress (NDJSON). */
export async function loadJsonlFromPathStream(
  taskId: string,
  path: string,
  onProgress: (pct: number, message: string) => void
): Promise<DatasetImportStreamResult> {
  const res = await fetch(
    `${API}/tasks/${encodeURIComponent(taskId)}/datasets/load-from-path-stream`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ path: path.trim() }),
    }
  );
  return readDatasetImportNdjson(res, onProgress);
}

/** Multipart upload with server-reported parse + save progress. Sends ``X-Expected-Size`` when known. */
export async function uploadJsonlStream(
  taskId: string,
  file: File,
  onProgress: (pct: number, message: string) => void
): Promise<DatasetImportStreamResult> {
  const fd = new FormData();
  fd.append('file', file);
  const res = await fetch(
    `${API}/tasks/${encodeURIComponent(taskId)}/datasets/upload-stream`,
    {
      method: 'POST',
      headers: { 'X-Expected-Size': String(file.size) },
      body: fd,
    }
  );
  return readDatasetImportNdjson(res, onProgress);
}

export function listFilters(): Promise<{ filters: string[] }> {
  return j(fetch(`${API}/filters`));
}

export function listFiltersGrouped(): Promise<FilterGroupsResponse> {
  return j(fetch(`${API}/filters?grouped=true`));
}

export async function listStages(taskId: string): Promise<Stage[]> {
  const rows = (await j(fetch(`${API}/tasks/${encodeURIComponent(taskId)}/stages`))) as Stage[];
  return rows.map((r) => ({
    ...r,
    stage_id: Number(r.stage_id),
    input_count: Number(r.input_count ?? 0),
    output_count: Number(r.output_count ?? 0),
    removed_count: Number(r.removed_count ?? 0),
  }));
}

export function getSummary(taskId: string, stageId: number): Promise<StageDetail> {
  return j(fetch(`${API}/tasks/${encodeURIComponent(taskId)}/stages/${stageId}/summary`));
}

/** Server-side sort for kept-row tables; ties on signature / stage / text use ``_row_id`` ascending. */
export type RowsSortKey =
  | 'row'
  | 'signature'
  | 'stage_focus'
  | 'question'
  | 'thinking'
  | 'response';

export function getRows(
  taskId: string,
  stageId: number,
  limit = 200,
  offset = 0,
  sort: RowsSortKey | null = 'row',
  sortDir: 'asc' | 'desc' = 'asc'
): Promise<{ rows: Record<string, unknown>[]; total: number }> {
  const q = new URLSearchParams({
    limit: String(limit),
    offset: String(offset),
  });
  if (sort) {
    q.set('sort', sort);
    q.set('sort_dir', sortDir);
  }
  return j(
    fetch(`${API}/tasks/${encodeURIComponent(taskId)}/stages/${stageId}/rows?${q.toString()}`)
  );
}

export function getStageView(
  taskId: string,
  stageId: number,
  subset: SubsetFilter,
  limit = 200,
  offset = 0,
  sort: RowsSortKey | null = 'row',
  sortDir: 'asc' | 'desc' = 'asc'
): Promise<StageViewResponse> {
  const body = subsetFilterToApiBody(subset);
  if (!body) {
    return Promise.reject(new Error('subset filter required'));
  }
  const usePost = subset.subsetScript != null && String(subset.subsetScript).trim() !== '';
  if (usePost) {
    return j(
      fetch(
        `${API}/tasks/${encodeURIComponent(taskId)}/stages/${stageId}/view`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            subset_filter: body,
            limit,
            offset,
            sort,
            sort_dir: sortDir,
          }),
        }
      )
    );
  }
  const q = new URLSearchParams({
    limit: String(limit),
    offset: String(offset),
  });
  for (const v of subset.signatures) q.append('signature', v);
  for (const v of subset.stageFocus) q.append('stage_focus', v);
  if (sort) {
    q.set('sort', sort);
    q.set('sort_dir', sortDir);
  }
  return j(
    fetch(
      `${API}/tasks/${encodeURIComponent(taskId)}/stages/${stageId}/view?${q.toString()}`
    )
  );
}

export type RemovalCategory =
  | 'hacking'
  | 'duplicate'
  | 'length'
  | 'format'
  | 'balancing'
  | 'other';

export function getRemovedSummary(
  taskId: string,
  stageId: number
): Promise<{
  total: number;
  row_count: number;
  by_category: Record<string, number>;
  by_signature: Record<string, number>;
}> {
  return j(
    fetch(`${API}/tasks/${encodeURIComponent(taskId)}/stages/${stageId}/removed-summary`)
  );
}

export type GetRemovedOptions = {
  reasonCategories?: RemovalCategory[];
  /** Non-empty: OR-filter removed rows to these signature values. */
  signatures?: string[];
};

export function getRemoved(
  taskId: string,
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
      `${API}/tasks/${encodeURIComponent(taskId)}/stages/${stageId}/removed-rows?${q.toString()}`
    )
  );
}

export function getDistribution(taskId: string, stageId: number): Promise<Dist> {
  return j(
    fetch(`${API}/tasks/${encodeURIComponent(taskId)}/stages/${stageId}/distribution`)
  );
}

export type SignaturesByStageRow = {
  stage_id: number;
  total: number;
  by_signature: Record<string, number>;
};

export function getSignaturesByStage(
  taskId: string
): Promise<{ stages: SignaturesByStageRow[] }> {
  return j(fetch(`${API}/tasks/${encodeURIComponent(taskId)}/signatures-by-stage`));
}

export function applyFilter(
  taskId: string,
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
    fetch(`${API}/tasks/${encodeURIComponent(taskId)}/apply-filter`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
  );
}

export function truncateStagesFrom(
  taskId: string,
  fromStageId: number
): Promise<{
  task_id: string;
  num_stages: number;
  current_stage_id: number;
  truncated_from: number;
  total_rows: number;
}> {
  return j(
    fetch(
      `${API}/tasks/${encodeURIComponent(taskId)}/stages/truncate-from/${fromStageId}`,
      { method: 'POST' }
    )
  );
}

export type ApplyFiltersResult = {
  new_stage_id: number;
  input_count: number;
  output_count: number;
  removed_count: number;
  per_filter_removed_count: Record<string, number>;
  affected_row_count: number;
  untouched_row_count: number;
};

export function applyFilters(
  taskId: string,
  body: {
    base_stage_id: number;
    subset_filter: Record<string, unknown> | null;
    filters: { filter_type: string; filter_config: Record<string, unknown> }[];
  }
): Promise<ApplyFiltersResult> {
  return j(
    fetch(`${API}/tasks/${encodeURIComponent(taskId)}/apply-filters`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
  );
}

/**
 * NDJSON stream: ``progress`` lines then one ``done`` (same fields as {@link applyFilters}) or ``error``.
 */
export async function applyFiltersStream(
  taskId: string,
  body: {
    base_stage_id: number;
    subset_filter: Record<string, unknown> | null;
    filters: { filter_type: string; filter_config: Record<string, unknown> }[];
  },
  onProgress: (pct: number, message: string) => void
): Promise<ApplyFiltersResult> {
  const res = await fetch(`${API}/tasks/${encodeURIComponent(taskId)}/apply-filters-stream`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const t = await res.text();
    throw new Error(t || `HTTP ${res.status}`);
  }
  const reader = res.body?.getReader();
  if (!reader) throw new Error('No response body');
  const dec = new TextDecoder();
  let buf = '';
  let donePayload: ApplyFiltersResult | null = null;
  while (true) {
    const { value, done } = await reader.read();
    if (value) buf += dec.decode(value, { stream: true });
    const lines = buf.split('\n');
    buf = lines.pop() ?? '';
    for (const line of lines) {
      const s = line.trim();
      if (!s) continue;
      let ev: Record<string, unknown>;
      try {
        ev = JSON.parse(s) as Record<string, unknown>;
      } catch {
        continue;
      }
      const typ = ev.type as string;
      if (typ === 'progress') {
        const pct = typeof ev.pct === 'number' ? ev.pct : 0;
        const message = typeof ev.message === 'string' ? ev.message : '';
        onProgress(pct, message);
      } else if (typ === 'error') {
        throw new Error(typeof ev.message === 'string' ? ev.message : 'Apply failed');
      } else if (typ === 'done') {
        donePayload = {
          new_stage_id: Number(ev.new_stage_id),
          input_count: Number(ev.input_count),
          output_count: Number(ev.output_count),
          removed_count: Number(ev.removed_count),
          per_filter_removed_count: (ev.per_filter_removed_count as Record<string, number>) ?? {},
          affected_row_count: Number(ev.affected_row_count),
          untouched_row_count: Number(ev.untouched_row_count),
        };
      }
    }
    if (done) {
      const tail = buf.trim();
      if (tail) {
        try {
          const ev = JSON.parse(tail) as Record<string, unknown>;
          if (ev.type === 'done' && !donePayload) {
            donePayload = {
              new_stage_id: Number(ev.new_stage_id),
              input_count: Number(ev.input_count),
              output_count: Number(ev.output_count),
              removed_count: Number(ev.removed_count),
              per_filter_removed_count: (ev.per_filter_removed_count as Record<string, number>) ?? {},
              affected_row_count: Number(ev.affected_row_count),
              untouched_row_count: Number(ev.untouched_row_count),
            };
          }
        } catch {
          /* ignore */
        }
      }
      break;
    }
  }
  if (!donePayload) throw new Error('Stream ended without result');
  return donePayload;
}

export type VersionInfo = { version: string; build_time: string };

export function getVersion(): Promise<VersionInfo> {
  return j(fetch(`${API}/version`));
}

export function exportUrl(
  taskId: string,
  stageId: number,
  format: 'jsonl' | 'csv' | 'filter_log',
  options?: {
    scope?: 'full' | 'signature';
    /** Legacy export when scope=signature */
    viewField?: string;
    viewValues?: string[];
    subsetOnly?: boolean;
    subsetFilter?: SubsetFilter | null;
  }
): string {
  const q = new URLSearchParams({
    stage_id: String(stageId),
    format,
  });
  if (format === 'filter_log') {
    return `${API}/tasks/${encodeURIComponent(taskId)}/export?${q.toString()}`;
  }
  const sf = options?.subsetFilter;
  if (options?.subsetOnly && subsetFilterActive(sf ?? null)) {
    q.set('subset_only', 'true');
    q.set('scope', 'full');
    for (const v of sf!.signatures) q.append('signature', v);
    for (const v of sf!.stageFocus) q.append('stage_focus', v);
  } else if (
    options?.scope === 'signature' &&
    options.viewField &&
    options.viewValues &&
    options.viewValues.length > 0
  ) {
    q.set('scope', 'signature');
    q.set('view_field', options.viewField);
    for (const v of options.viewValues) {
      q.append('values', v);
    }
  } else {
    q.set('scope', 'full');
  }
  return `${API}/tasks/${encodeURIComponent(taskId)}/export?${q.toString()}`;
}
