import type { StageDetail } from '../api';

type Props = {
  detail: StageDetail | null;
};

export function PerFilterRemovalSummary({ detail }: Props) {
  const raw = detail?.summary_stats?.per_filter_removed_count;
  const pfc = raw as Record<string, number> | undefined;
  if (!pfc || typeof pfc !== 'object' || !Object.keys(pfc).length) {
    return null;
  }
  return (
    <div className="card pfcard">
      <h2>Removed by each filter (batch run)</h2>
      <p className="muted small">
        Counts are per filter when run independently on the same input; rows may overlap across
        filters, but the kept set excludes any row that any filter removes.
      </p>
      <ul className="pfc-list">
        {Object.entries(pfc).map(([k, n]) => (
          <li key={k}>
            <code>{k}</code> — <strong>{n}</strong>
          </li>
        ))}
      </ul>
    </div>
  );
}
