import { subsetFilterActive, type StageDetail, type SubsetFilter } from '../api';

type Props = {
  detail: StageDetail | null;
  /** When drilling, show subset stats from GET /view. */
  exploreSummary: Record<string, unknown> | null;
  subsetFilter: SubsetFilter | null;
};

export function StatsPanel({ detail, exploreSummary, subsetFilter }: Props) {
  if (!detail) {
    return (
      <div className="card">
        <h2>Stats</h2>
        <p className="muted">Select a stage after upload.</p>
      </div>
    );
  }
  const s =
    subsetFilterActive(subsetFilter) && exploreSummary ? exploreSummary : detail.summary_stats;
  return (
    <div className="card">
      <h2>Stats {subsetFilterActive(subsetFilter) ? '(exploration subset)' : ''}</h2>
      <dl className="stats-dl">
        <div>
          <dt>Input</dt>
          <dd>{String(s.input_count ?? '—')}</dd>
        </div>
        <div>
          <dt>Output</dt>
          <dd>{String(s.output_count ?? s.total_samples ?? '—')}</dd>
        </div>
        <div>
          <dt>Removed</dt>
          <dd>{String(s.removed_count ?? s.removed_samples ?? '—')}</dd>
        </div>
        <div>
          <dt>Removal ratio</dt>
          <dd>
            {s.removal_ratio != null
              ? `${(Number(s.removal_ratio) * 100).toFixed(1)}%`
              : '—'}
          </dd>
        </div>
        {s.runtime_ms_mean != null && (
          <div>
            <dt>Runtime mean (ms)</dt>
            <dd>{Number(s.runtime_ms_mean).toFixed(1)}</dd>
          </div>
        )}
        {s.affected_row_count != null && !subsetFilterActive(subsetFilter) && (
          <div>
            <dt>Affected (subset op)</dt>
            <dd>{String(s.affected_row_count)}</dd>
          </div>
        )}
        {s.untouched_row_count != null && !subsetFilterActive(subsetFilter) && (
          <div>
            <dt>Untouched (subset op)</dt>
            <dd>{String(s.untouched_row_count)}</dd>
          </div>
        )}
      </dl>
    </div>
  );
}
