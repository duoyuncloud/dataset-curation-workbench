import type { Stage } from '../api';

type Props = {
  stages: Stage[];
  current: number;
  onSelect: (id: number) => void;
  /** Remove this stage and all later ones (server truncates pipeline). */
  onTruncateFrom?: (stageId: number) => void | Promise<void>;
  truncatingFromStageId?: number | null;
};

function subsetScopeBlock(s: Stage): string {
  const vf = s.view_filter;
  if (!vf || typeof vf !== 'object') {
    return 'Full stage';
  }
  const o = vf as {
    subset_filter?: { signatures?: string[]; stage_focuses?: string[] };
    field?: string;
    values?: string[];
    value?: string;
  };
  if (o.subset_filter && typeof o.subset_filter === 'object') {
    const sf = o.subset_filter;
    const sigs = sf.signatures ?? [];
    const sfo = sf.stage_focuses ?? [];
    const bits: string[] = [];
    if (sigs.length) bits.push(`sig: ${sigs.slice(0, 2).join(', ')}${sigs.length > 2 ? '…' : ''}`);
    if (sfo.length)
      bits.push(`focus: ${sfo.slice(0, 1).join(', ')}${sfo.length > 1 ? '…' : ''}`);
    return bits.length ? bits.join(' · ') : 'Full stage';
  }
  const vals = o.values?.length
    ? o.values
    : o.value != null && o.value !== ''
      ? [String(o.value)]
      : [];
  if (o.field && vals.length) {
    return `${o.field}: ${vals.slice(0, 2).join(', ')}${vals.length > 2 ? '…' : ''}`;
  }
  return 'Full stage';
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

export function PipelineTimeline({
  stages,
  current,
  onSelect,
  onTruncateFrom,
  truncatingFromStageId,
}: Props) {
  return (
    <div className="timeline-wrap">
      <h2 className="timeline-h2">Stages</h2>
      <p className="muted small timeline-hint">
        Each step: <strong>subset scope</strong> (if any) and <strong>filters</strong> applied, plus how
        many rows were in the subset and total output. Use <strong>Remove from here</strong> to drop this
        step and all later ones (disk + DB).
      </p>
      <div className="timeline" role="list">
        {stages.map((s, i) => (
          <div key={s.stage_id} className="timeline-item" role="listitem">
            {i > 0 && <span className="timeline-connector" aria-hidden />}
            <div className="timeline-node-wrap">
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
                    <span className="timeline-v" title={subsetScopeBlock(s)}>
                      {subsetScopeBlock(s)}
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
              {s.stage_id >= 1 && onTruncateFrom ? (
                <button
                  type="button"
                  className="timeline-truncate-btn btn small"
                  disabled={truncatingFromStageId != null}
                  aria-label={`Remove stage ${s.stage_id} and all later stages`}
                  title={`Remove stage ${s.stage_id} and all later stages (cannot undo)`}
                  onClick={(e) => {
                    e.preventDefault();
                    e.stopPropagation();
                    void onTruncateFrom(s.stage_id);
                  }}
                >
                  {truncatingFromStageId === s.stage_id ? 'Removing…' : 'Remove from here'}
                </button>
              ) : null}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
