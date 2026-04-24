import type { Stage } from '../api';

type Props = {
  stages: Stage[];
  current: number;
  onSelect: (id: number) => void;
};

function signatureBlock(s: Stage): string {
  const vf = s.view_filter;
  if (!vf || typeof vf !== 'object') {
    return 'All signatures';
  }
  const o = vf as { field?: string; values?: string[]; value?: string };
  const vals = o.values?.length
    ? o.values
    : o.value != null && o.value !== ''
      ? [String(o.value)]
      : [];
  if (o.field === 'signature' && vals.length) {
    return vals.length > 2 ? `${vals.slice(0, 2).join(', ')} +${vals.length - 2}` : vals.join(', ');
  }
  if (o.field && vals.length) {
    return `${o.field}: ${vals.join(', ')}`;
  }
  return 'All signatures';
}

function filterBlock(s: Stage): string {
  if (s.filter_type === 'raw') {
    return 'Import';
  }
  const cfg = s.filter_config as { filters?: { filter_type: string }[] } | undefined;
  if (s.filter_type === 'batch' && cfg?.filters?.length) {
    return cfg.filters.map((f) => f.filter_type).join(' · ');
  }
  return s.filter_type;
}

function countBlock(s: Stage): string {
  const out = s.output_count;
  if (s.view_filter == null) {
    return `out ${out.toLocaleString()}`;
  }
  const aff = s.affected_row_count;
  const un = s.untouched_row_count;
  if (aff != null && un != null) {
    return `in scope ${aff.toLocaleString()} · passthrough ${un.toLocaleString()} · out ${out.toLocaleString()}`;
  }
  if (aff != null) {
    return `in scope ${aff.toLocaleString()} · out ${out.toLocaleString()}`;
  }
  return `out ${out.toLocaleString()}`;
}

export function PipelineTimeline({ stages, current, onSelect }: Props) {
  return (
    <div className="timeline-wrap">
      <h2 className="timeline-h2">Stages</h2>
      <p className="muted small timeline-hint">
        Each step: <strong>signature scope</strong> (if any) and <strong>filters</strong> applied, plus
        how many rows were in the subset and total output.
      </p>
      <div className="timeline" role="list">
        {stages.map((s, i) => (
          <div key={s.stage_id} className="timeline-item" role="listitem">
            {i > 0 && <span className="timeline-connector" aria-hidden />}
            <button
              type="button"
              className={`timeline-node ${s.stage_id === current ? 'active' : ''}`}
              onClick={() => onSelect(s.stage_id)}
            >
              <span className="timeline-st-label">
                {s.stage_id === 0 ? 'Raw' : `Stage ${s.stage_id}`}
              </span>
              <div className="timeline-two">
                <div className="timeline-row">
                  <span className="timeline-k">Scope</span>
                  <span className="timeline-v" title={signatureBlock(s)}>
                    {signatureBlock(s)}
                  </span>
                </div>
                <div className="timeline-row">
                  <span className="timeline-k">Filters</span>
                  <span className="timeline-v" title={filterBlock(s)}>
                    {filterBlock(s)}
                  </span>
                </div>
                <div className="timeline-row timeline-counts">
                  <span className="timeline-k">Rows</span>
                  <span className="timeline-v mono-sm">{countBlock(s)}</span>
                </div>
                {s.removed_count > 0 && (
                  <div className="timeline-removed">−{s.removed_count.toLocaleString()} removed</div>
                )}
              </div>
            </button>
          </div>
        ))}
      </div>
    </div>
  );
}
