import { useCallback, useEffect, useState } from 'react';
import { applyFilters, listFiltersGrouped, type ViewFilter } from '../api';
import { FILTER_DEFAULTS, PRESETS } from '../filterConfig';

type Props = {
  datasetId: string | null;
  maxStage: number;
  /** Current timeline stage; when it changes, batch filter checkboxes are cleared. */
  activeStage: number;
  viewFilter: ViewFilter | null;
  onApplied: () => void;
  onError: (m: string | null) => void;
  /** No outer card; use under a parent “Workspace” card. */
  embedded?: boolean;
};

export function FilterPanel({
  datasetId,
  maxStage,
  activeStage,
  viewFilter,
  onApplied,
  onError,
  embedded,
}: Props) {
  const [groups, setGroups] = useState<Record<string, string[]> | null>(null);
  const [fromStage, setFromStage] = useState(0);
  const [selected, setSelected] = useState<Record<string, boolean>>({});
  const [busy, setBusy] = useState(false);

  const load = useCallback(() => {
    listFiltersGrouped()
      .then((r) => {
        setGroups(r.groups);
        setSelected((sel) => {
          const n = { ...sel };
          for (const f of r.filters) {
            if (n[f] === undefined) n[f] = false;
          }
          return n;
        });
      })
      .catch(() => setGroups(null));
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  useEffect(() => {
    setFromStage(maxStage);
  }, [maxStage, datasetId]);

  useEffect(() => {
    setSelected((s) => {
      if (!Object.keys(s).length) return s;
      const n = { ...s };
      for (const k of Object.keys(n)) n[k] = false;
      return n;
    });
  }, [activeStage, datasetId]);

  function setPreset(keys: string[]) {
    onError(null);
    const n: Record<string, boolean> = {};
    if (groups) {
      for (const list of Object.values(groups)) {
        for (const f of list) n[f] = false;
      }
    } else {
      for (const k of Object.keys(FILTER_DEFAULTS)) n[k] = false;
    }
    for (const k of keys) n[k] = true;
    setSelected(n);
  }

  function selectAllIn(groupKey: string) {
    const list = groups?.[groupKey];
    if (!list) return;
    setSelected((s) => {
      const n = { ...s };
      for (const f of list) n[f] = true;
      return n;
    });
  }

  function clearAll() {
    setSelected((s) => {
      const n = { ...s };
      for (const k of Object.keys(n)) n[k] = false;
      return n;
    });
    onError(null);
  }

  async function run() {
    if (!datasetId) return;
    onError(null);
    const keys = Object.keys(selected).filter((k) => selected[k]);
    if (keys.length === 0) {
      onError('Select at least one filter.');
      return;
    }
    const batch = keys.map((ft) => ({
      filter_type: ft,
      filter_config: { ...(FILTER_DEFAULTS[ft] || {}) } as Record<string, unknown>,
    }));
    setBusy(true);
    try {
      await applyFilters(datasetId, {
        base_stage_id: fromStage,
        view_filter: viewFilter,
        filters: batch,
      });
      onApplied();
    } catch (e) {
      onError(e instanceof Error ? e.message : 'Apply failed');
    } finally {
      setBusy(false);
    }
  }

  if (!groups) {
    return (
      <div className={embedded ? 'ws-section' : 'card'}>
        {!embedded && <h2>Apply filters</h2>}
        {embedded && <h3 className="ws-h3">Apply filters</h3>}
        <p className="muted">Loading filter list…</p>
      </div>
    );
  }

  const body = (
    <>
      {!embedded && <h2>Apply filters</h2>}
      {embedded && <h3 className="ws-h3">Apply filters</h3>}
      <p className="muted small">
        Optional: restrict with the <strong>Signature</strong> filter in the main table, then run
        rules on that subset. One “Apply” creates a <strong>single</strong> new stage. Other rows
        are left unchanged in the new stage.
      </p>

      <label className="field">
        <span>From stage (input rows)</span>
        <select
          value={fromStage}
          onChange={(e) => setFromStage(Number(e.target.value))}
          disabled={!datasetId}
        >
          {Array.from({ length: maxStage + 1 }, (_, i) => (
            <option key={i} value={i}>
              Stage {i}
            </option>
          ))}
        </select>
      </label>

      {viewFilter && (
        <p className="view-hint small">
          Batch applies to the <strong>table signature subset</strong> only, then merges with
          unchanged rows.
        </p>
      )}

      <div className="presets">
        {Object.values(PRESETS).map((p) => (
          <button
            type="button"
            key={p.label}
            className="btn small"
            disabled={!datasetId}
            onClick={() => setPreset(p.keys)}
          >
            {p.label}
          </button>
        ))}
      </div>

      <div className="select-actions">
        <span className="label-col">Group:</span>
        <button type="button" className="link-btn" onClick={() => selectAllIn('core')}>
          all core
        </button>
        <button
          type="button"
          className="link-btn"
          onClick={() => selectAllIn('analysis')}
        >
          all analysis
        </button>
        <button type="button" className="link-btn" onClick={clearAll}>
          clear all
        </button>
      </div>

      {(['core', 'analysis'] as const).map((gk) => {
        const list = groups[gk] ?? [];
        if (!list.length) return null;
        return (
          <fieldset className="filter-group" key={gk}>
            <legend>{gk}</legend>
            {list.map((ft) => (
              <div className="filter-line" key={ft}>
                <label className="ck">
                  <input
                    type="checkbox"
                    checked={!!selected[ft]}
                    onChange={(e) =>
                      setSelected((s) => ({ ...s, [ft]: e.target.checked }))
                    }
                    disabled={!datasetId}
                  />
                  <span>{ft}</span>
                </label>
              </div>
            ))}
          </fieldset>
        );
      })}

      <button
        type="button"
        className="btn primary full"
        disabled={!datasetId || busy}
        onClick={() => void run()}
      >
        {busy ? 'Applying…' : 'Apply selected filters'}
      </button>
    </>
  );

  return embedded ? <div className="ws-section">{body}</div> : <div className="card">{body}</div>;
}
