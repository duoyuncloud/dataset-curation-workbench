import { useMemo, useState } from 'react';
import type { TaskRow } from '../api';

type TaskHomeSort = 'dataset_asc' | 'updated_desc';

type Props = {
  tasks: TaskRow[];
  loading: boolean;
  error: string | null;
  onRefresh: () => void;
  onNewTask: () => void;
  onOpen: (taskId: string) => void;
  onRename: (task: TaskRow) => void;
  onDelete: (task: TaskRow) => void;
};

/** Beijing wall time, ``YYYY-MM-DD HH:mm`` (no seconds, no long locale string). */
function fmtWhen(iso: string): string {
  const BJ = 'Asia/Shanghai';
  try {
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return iso;
    const f = new Intl.DateTimeFormat('en-CA', {
      timeZone: BJ,
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      hour12: false,
    });
    return f.format(d).replace(', ', ' ');
  } catch {
    return iso;
  }
}

function updatedTitle(iso: string): string {
  try {
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return iso;
    return `北京时间：${fmtWhen(iso)}\n存库 ISO：${iso}`;
  } catch {
    return iso;
  }
}

function dash(s: string | undefined | null): string {
  if (s == null || String(s).trim() === '') return '—';
  return String(s);
}

function datasetSortKey(t: TaskRow): string {
  const d = t.dataset_name != null && String(t.dataset_name).trim() !== '' ? String(t.dataset_name) : '';
  return d || String(t.task_name ?? '');
}

/** Stages after Raw (filters only). Raw-only upload has num_stages 1 → 0. */
export function filterStageCount(numStages: number | undefined | null): number {
  const n = Number(numStages);
  if (!Number.isFinite(n)) return 0;
  return Math.max(0, n - 1);
}

function compareTasks(a: TaskRow, b: TaskRow, sort: TaskHomeSort): number {
  if (sort === 'dataset_asc') {
    const c = datasetSortKey(a).localeCompare(datasetSortKey(b), undefined, { sensitivity: 'base' });
    if (c !== 0) return c;
    return String(a.task_name ?? '').localeCompare(String(b.task_name ?? ''), undefined, {
      sensitivity: 'base',
    });
  }
  const ta = new Date(a.updated_at).getTime();
  const tb = new Date(b.updated_at).getTime();
  const byTime = (Number.isNaN(tb) ? 0 : tb) - (Number.isNaN(ta) ? 0 : ta);
  if (byTime !== 0) return byTime;
  return String(a.task_name ?? '').localeCompare(String(b.task_name ?? ''), undefined, {
    sensitivity: 'base',
  });
}

export function TaskHome({
  tasks,
  loading,
  error,
  onRefresh,
  onNewTask,
  onOpen,
  onRename,
  onDelete,
}: Props) {
  const [sort, setSort] = useState<TaskHomeSort>('updated_desc');
  const sortedTasks = useMemo(() => [...tasks].sort((a, b) => compareTasks(a, b, sort)), [tasks, sort]);

  return (
    <div className="task-home">
      <header className="header task-home-header">
        <div>
          <h1>Tasks</h1>
          <p className="tagline">Open a task to curate, or create a new workspace.</p>
        </div>
        <div className="task-home-actions">
          <label className="task-home-sort-label">
            <span className="muted small">Sort</span>
            <select
              className="task-home-sort-select"
              value={sort}
              onChange={(e) => setSort(e.target.value as TaskHomeSort)}
              aria-label="Sort tasks"
            >
              <option value="dataset_asc">Dataset name (A–Z)</option>
              <option value="updated_desc">Latest update</option>
            </select>
          </label>
          <button type="button" className="btn primary" onClick={onNewTask}>
            New task
          </button>
          <button type="button" className="btn" onClick={onRefresh} disabled={loading}>
            {loading ? 'Loading…' : 'Refresh'}
          </button>
        </div>
      </header>

      {error && (
        <div className="banner">
          {error}
          <button type="button" className="linkish" onClick={onRefresh}>
            Retry
          </button>
        </div>
      )}

      {!loading && !error && tasks.length === 0 && (
        <p className="muted task-home-empty">No tasks yet. Create one to get started.</p>
      )}

      <ul className="task-card-list">
        {sortedTasks.map((t) => (
          <li key={t.task_id} className="card task-card task-card-row">
            <div className="task-card-row-main">
              <h2 className="task-card-title">{dash(t.task_name)}</h2>
              <div className="task-card-dataset" title={t.dataset_name ? String(t.dataset_name) : undefined}>
                {dash(t.dataset_name)}
              </div>
            </div>
            <dl className="task-card-meta">
              <div>
                <dt>Rows</dt>
                <dd>{Number(t.total_rows ?? 0).toLocaleString()}</dd>
              </div>
              <div>
                <dt title="Number of pipeline steps after Raw. Uploading JSONL only (Raw) counts as 0.">
                  Filter stages
                </dt>
                <dd>{filterStageCount(t.num_stages)}</dd>
              </div>
              <div>
                <dt>Updated</dt>
                <dd title={updatedTitle(t.updated_at)}>{fmtWhen(t.updated_at)}</dd>
              </div>
            </dl>
            <div className="task-card-btns">
              <button type="button" className="btn primary" onClick={() => onOpen(t.task_id)}>
                Open
              </button>
              <button type="button" className="btn" onClick={() => onRename(t)}>
                Rename
              </button>
              <button type="button" className="btn danger-outline" onClick={() => onDelete(t)}>
                Delete
              </button>
            </div>
          </li>
        ))}
      </ul>
    </div>
  );
}
