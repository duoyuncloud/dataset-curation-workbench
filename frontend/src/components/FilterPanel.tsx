import { useCallback, useEffect, useState } from 'react';
import {
  applyFilters,
  listFiltersGrouped,
  subsetFilterActive,
  subsetFilterToApiBody,
  type SubsetFilter,
} from '../api';
import {
  FILTER_DEFAULTS,
  FILTER_HELP,
  FILTER_LABELS,
  GROUP_HELP,
  PRESET_ORDER,
  PRESETS,
} from '../filterConfig';

const GROUP_ORDER = ['cleanup', 'validity', 'balancing'] as const;

const GROUP_LABELS: Record<(typeof GROUP_ORDER)[number], string> = {
  cleanup: 'Cleanup',
  validity: 'Validity',
  balancing: 'Balancing',
};

type Props = {
  taskId: string | null;
  maxStage: number;
  activeStage: number;
  subsetFilter: SubsetFilter | null;
  onApplied: () => void;
  onError: (m: string | null) => void;
  embedded?: boolean;
};

function clamp01(x: number): number {
  if (Number.isNaN(x)) return 0;
  return Math.min(1, Math.max(0, x));
}

export function FilterPanel({
  taskId,
  maxStage,
  activeStage,
  subsetFilter,
  onApplied,
  onError,
  embedded,
}: Props) {
  const [groups, setGroups] = useState<Record<string, string[]> | null>(null);
  const [fromStage, setFromStage] = useState(0);
  const [selected, setSelected] = useState<Record<string, boolean>>({});
  /** Per-filter config merged over FILTER_DEFAULTS when applying */
  const [configs, setConfigs] = useState<Record<string, Record<string, unknown>>>({});
  const [busy, setBusy] = useState(false);

  const load = useCallback(() => {
    listFiltersGrouped()
      .then((r) => {
        setGroups(r.groups);
        const cfg: Record<string, Record<string, unknown>> = {};
        for (const f of r.filters) {
          cfg[f] = { ...(FILTER_DEFAULTS[f] || {}) };
        }
        setConfigs(cfg);
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
  }, [load, taskId]);

  useEffect(() => {
    if (maxStage >= 0) setFromStage(maxStage);
    else setFromStage(0);
  }, [maxStage, taskId]);

  useEffect(() => {
    setSelected((s) => {
      if (!Object.keys(s).length) return s;
      const n = { ...s };
      for (const k of Object.keys(n)) n[k] = false;
      return n;
    });
  }, [activeStage, taskId]);

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

  function selectAllInGroup(groupKey: string) {
    const list = groups?.[groupKey];
    if (!list) return;
    setSelected((s) => {
      const n = { ...s };
      for (const f of list) n[f] = true;
      return n;
    });
  }

  function selectNoneInGroup(groupKey: string) {
    const list = groups?.[groupKey];
    if (!list) return;
    setSelected((s) => {
      const n = { ...s };
      for (const f of list) n[f] = false;
      return n;
    });
  }

  function selectAllGlobal() {
    if (!groups) return;
    onError(null);
    setSelected((s) => {
      const n = { ...s };
      for (const list of Object.values(groups)) {
        for (const f of list) n[f] = true;
      }
      return n;
    });
  }

  function selectNoneGlobal() {
    clearAll();
  }

  function clearAll() {
    setSelected((s) => {
      const n = { ...s };
      for (const k of Object.keys(n)) n[k] = false;
      return n;
    });
    onError(null);
  }

  function patchConfig(ft: string, patch: Record<string, unknown>) {
    setConfigs((c) => ({
      ...c,
      [ft]: { ...(c[ft] || { ...(FILTER_DEFAULTS[ft] || {}) }), ...patch },
    }));
  }

  async function run() {
    if (!taskId || maxStage < 0) return;
    onError(null);
    const keys = Object.keys(selected).filter((k) => selected[k]);
    if (keys.length === 0) {
      onError('Select at least one filter.');
      return;
    }
    const batch = keys.map((ft) => ({
      filter_type: ft,
      filter_config: {
        ...(FILTER_DEFAULTS[ft] || {}),
        ...(configs[ft] || {}),
      } as Record<string, unknown>,
    }));
    setBusy(true);
    try {
      await applyFilters(taskId, {
        base_stage_id: fromStage,
        subset_filter: subsetFilterToApiBody(
          subsetFilter ?? { signatures: [], stageFocus: [] }
        ),
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

  const canApply = Boolean(taskId) && maxStage >= 0;
  const rdFrac = Number(configs.random_drop?.drop_fraction ?? 0.1);

  const body = (
    <>
      {!embedded && <h2>Apply filters</h2>}
      {embedded && <h3 className="ws-h3">Apply filters</h3>}
      {maxStage < 0 ? (
        <p className="muted small">Upload a JSONL dataset to create stage 0, then you can run filters.</p>
      ) : (
        <>
          <p className="muted small" title="Table subset + Apply = new stage; other rows unchanged.">
            Subset from the table is optional; each Apply creates one new stage.
          </p>

          <label className="field">
            <span>From stage (input rows)</span>
            <select
              value={fromStage}
              onChange={(e) => setFromStage(Number(e.target.value))}
              disabled={!canApply}
            >
              {Array.from({ length: maxStage + 1 }, (_, i) => (
                <option key={i} value={i}>
                  Stage {i}
                </option>
              ))}
            </select>
          </label>

          {subsetFilterActive(subsetFilter) && (
            <p className="view-hint small" title="Filters run only on matching rows; rest pass through.">
              Using active <strong>table subset</strong>.
            </p>
          )}

          <div className="presets">
            {PRESET_ORDER.map((id) => {
              const p = PRESETS[id];
              return (
                <button
                  type="button"
                  key={id}
                  className="btn small"
                  disabled={!canApply}
                  onClick={() => setPreset(p.keys)}
                >
                  {p.label}
                </button>
              );
            })}
          </div>

          <div className="filter-global-actions">
            <button
              type="button"
              className="link-btn"
              disabled={!canApply}
              onClick={selectAllGlobal}
            >
              Select all
            </button>
            <span className="muted" aria-hidden>
              ·
            </span>
            <button
              type="button"
              className="link-btn"
              disabled={!canApply}
              onClick={selectNoneGlobal}
            >
              Select none
            </button>
          </div>

          {GROUP_ORDER.map((gk) => {
            const list = groups[gk] ?? [];
            if (!list.length) return null;
            return (
              <fieldset className="filter-group" key={gk}>
                <legend title={GROUP_HELP[gk] || ''}>{GROUP_LABELS[gk]}</legend>
                <div className="filter-group-tools">
                  <button
                    type="button"
                    className="btn micro"
                    disabled={!canApply}
                    onClick={() => selectAllInGroup(gk)}
                  >
                    All in group
                  </button>
                  <button
                    type="button"
                    className="btn micro"
                    disabled={!canApply}
                    onClick={() => selectNoneInGroup(gk)}
                  >
                    None in group
                  </button>
                </div>
                {list.map((ft) => (
                  <div className="filter-block" key={ft}>
                    <div className="filter-line">
                      <label className="ck">
                        <input
                          type="checkbox"
                          checked={!!selected[ft]}
                          onChange={(e) =>
                            setSelected((s) => ({ ...s, [ft]: e.target.checked }))
                          }
                          disabled={!canApply}
                        />
                        <span className="filter-name" title={FILTER_HELP[ft] || ''}>
                          {FILTER_LABELS[ft] || ft}
                        </span>
                      </label>
                    </div>
                    {ft === 'random_drop' && selected.random_drop && (
                      <div className="filter-nested-config">
                        <label className="filter-inline-num">
                          <span>Drop fraction (0–1)</span>
                          <input
                            type="number"
                            min={0}
                            max={1}
                            step={0.01}
                            value={Number.isFinite(rdFrac) ? rdFrac : 0}
                            onChange={(e) =>
                              patchConfig('random_drop', {
                                drop_fraction: clamp01(Number(e.target.value)),
                              })
                            }
                            disabled={!canApply}
                          />
                        </label>
                        <label className="filter-inline-num">
                          <span>Random seed</span>
                          <input
                            type="number"
                            step={1}
                            value={Number(configs.random_drop?.random_seed ?? 42)}
                            onChange={(e) =>
                              patchConfig('random_drop', {
                                random_seed: Math.trunc(Number(e.target.value)) || 0,
                              })
                            }
                            disabled={!canApply}
                          />
                        </label>
                      </div>
                    )}
                    {ft === 'balance_to_mean' && selected.balance_to_mean && (
                      <div className="filter-nested-config">
                        <label className="filter-inline-select">
                          <span>Group by</span>
                          <select
                            value={String(configs.balance_to_mean?.group_by ?? 'signature')}
                            onChange={(e) =>
                              patchConfig('balance_to_mean', { group_by: e.target.value })
                            }
                            disabled={!canApply}
                          >
                            <option value="signature">signature</option>
                            <option value="stage_focus">stage_focus</option>
                          </select>
                        </label>
                        <label className="filter-inline-num">
                          <span>Random seed</span>
                          <input
                            type="number"
                            step={1}
                            value={Number(configs.balance_to_mean?.random_seed ?? 42)}
                            onChange={(e) =>
                              patchConfig('balance_to_mean', {
                                random_seed: Math.trunc(Number(e.target.value)) || 0,
                              })
                            }
                            disabled={!canApply}
                          />
                        </label>
                      </div>
                    )}
                  </div>
                ))}
              </fieldset>
            );
          })}

          <button
            type="button"
            className="btn primary full"
            disabled={!canApply || busy}
            onClick={() => void run()}
          >
            {busy ? 'Applying…' : 'Apply selected filters'}
          </button>
        </>
      )}
    </>
  );

  return embedded ? <div className="ws-section">{body}</div> : <div className="card">{body}</div>;
}
