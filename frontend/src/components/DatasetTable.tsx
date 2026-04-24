import { useEffect, useRef, useState } from 'react';
import {
  getDistribution,
  getRows,
  getStageView,
  subsetFilterActive,
  type Dist,
  type RowsSortKey,
  type Stage,
  type SubsetFilter,
} from '../api';
import { PaginationBar } from './PaginationBar';

type Props = {
  taskId: string | null;
  /** When false, no stage data exists yet (empty task). */
  hasStages: boolean;
  stages: Stage[];
  /** Which stage’s kept rows to list (can differ from timeline while browsing). */
  viewStageId: number;
  onViewStageIdChange: (stageId: number) => void;
  subsetFilter: SubsetFilter | null;
  onSubsetFilter: (v: SubsetFilter | null) => void;
};

const PREVIEW_LEN = 220;

function stageLabel(id: number): string {
  return id === 0 ? 'Raw' : `S${id}`;
}

function rowId(r: Record<string, unknown>, i: number, offset: number): string {
  const id = r._row_id ?? r.row_id;
  if (id != null && id !== '') return String(id);
  return String(offset + i + 1);
}

function questionText(r: Record<string, unknown>): string {
  if (r.question == null) return '';
  return String(r.question);
}

function answerText(r: Record<string, unknown>): string {
  if (r.response == null) return '';
  return String(r.response);
}

function signatureLabel(r: Record<string, unknown>): string {
  const s = r.signature;
  if (s != null && String(s) !== '') return String(s);
  return '—';
}

function stageFocusLabel(r: Record<string, unknown>): string {
  const s = r.stage_focus;
  if (s != null && String(s).trim() !== '') return String(s);
  return '—';
}

/** Prefer signature counts; fall back to operator_family so chips work if column naming differs. */
function signatureDistForUi(d: Dist): Record<string, number> {
  const sig = d.signature || {};
  if (Object.keys(sig).length > 0) return sig;
  const fam = d.operator_family || {};
  return typeof fam === 'object' && fam !== null ? fam : {};
}

export function DatasetTable({
  taskId,
  hasStages,
  stages,
  viewStageId,
  onViewStageIdChange,
  subsetFilter,
  onSubsetFilter,
}: Props) {
  const [rows, setRows] = useState<Record<string, unknown>[]>([]);
  const [total, setTotal] = useState(0);
  const [rowsLoading, setRowsLoading] = useState(false);
  const [rowsError, setRowsError] = useState<string | null>(null);
  const [offset, setOffset] = useState(0);
  const [fullRow, setFullRow] = useState<Record<string, unknown> | null>(null);
  const [sigOpen, setSigOpen] = useState(false);
  const [focusOpen, setFocusOpen] = useState(false);
  const [distKeys, setDistKeys] = useState<string[]>([]);
  const [sigCounts, setSigCounts] = useState<Record<string, number>>({});
  const [focusDistKeys, setFocusDistKeys] = useState<string[]>([]);
  const [focusCounts, setFocusCounts] = useState<Record<string, number>>({});
  const [draftSigs, setDraftSigs] = useState<Set<string>>(new Set());
  const [draftFocus, setDraftFocus] = useState<Set<string>>(new Set());
  const [sortKey, setSortKey] = useState<RowsSortKey>('row');
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('asc');
  const limit = 100;
  const rowsFetchGen = useRef(0);

  const stageOptions =
    stages.length > 0
      ? [...stages].sort((a, b) => Number(a.stage_id) - Number(b.stage_id))
      : [];

  // Changing viewed stage: reset subset picker state
  useEffect(() => {
    setDraftSigs(new Set());
    setDraftFocus(new Set());
    setSigOpen(false);
    setFocusOpen(false);
  }, [viewStageId]);

  useEffect(() => {
    if (!taskId || !hasStages) {
      setDistKeys([]);
      setSigCounts({});
      return;
    }
    let o = true;
    getDistribution(taskId, viewStageId)
      .then((d: Dist) => {
        if (!o) return;
        const sig = signatureDistForUi(d);
        const keys = Object.keys(sig)
          .filter((k) => k !== '' && k !== '__missing__')
          .sort((a, b) => a.localeCompare(b));
        setDistKeys(keys);
        setSigCounts(sig);
        const sf = d.stage_focus && typeof d.stage_focus === 'object' ? d.stage_focus : {};
        const fk = Object.keys(sf)
          .filter((k) => k !== '' && k !== '__missing__')
          .sort((a, b) => a.localeCompare(b));
        setFocusDistKeys(fk);
        setFocusCounts(sf as Record<string, number>);
      })
      .catch(() => {
        /* keep last distribution on transient error */
      });
    return () => {
      o = false;
    };
  }, [taskId, hasStages, viewStageId]);

  useEffect(() => {
    setOffset(0);
  }, [subsetFilter, viewStageId, taskId]);

  useEffect(() => {
    if (!taskId || !hasStages) {
      rowsFetchGen.current += 1;
      setRows([]);
      setTotal(0);
      setRowsError(null);
      setRowsLoading(false);
      return;
    }
    const gen = ++rowsFetchGen.current;
    let ok = true;
    setRowsLoading(true);
    setRowsError(null);
    const p =
      subsetFilter && subsetFilterActive(subsetFilter)
        ? getStageView(taskId, viewStageId, subsetFilter, limit, offset, sortKey, sortDir)
        : getRows(taskId, viewStageId, limit, offset, sortKey, sortDir);
    p.then((r) => {
      if (ok) {
        setRows(r.rows);
        setTotal(r.total);
        setRowsError(null);
      }
    })
      .catch((err: unknown) => {
        if (ok) {
          setRowsError(err instanceof Error ? err.message : 'Failed to load rows');
        }
      })
      .finally(() => {
        if (gen === rowsFetchGen.current) setRowsLoading(false);
      });
    return () => {
      ok = false;
    };
  }, [taskId, hasStages, viewStageId, offset, subsetFilter, limit, sortKey, sortDir]);

  function onSortHeaderClick(key: RowsSortKey) {
    setOffset(0);
    if (sortKey === key) setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'));
    else {
      setSortKey(key);
      setSortDir('asc');
    }
  }

  function sortIndicator(key: RowsSortKey): string {
    if (sortKey !== key) return '';
    return sortDir === 'asc' ? ' ▲' : ' ▼';
  }

  useEffect(() => {
    if (!sigOpen) return;
    if (subsetFilter?.signatures.length) {
      setDraftSigs(new Set(subsetFilter.signatures));
    } else if (distKeys.length) {
      setDraftSigs(new Set(distKeys));
    }
  }, [sigOpen, distKeys.join('|'), subsetFilter]);

  useEffect(() => {
    if (!focusOpen) return;
    if (subsetFilter?.stageFocus.length) {
      setDraftFocus(new Set(subsetFilter.stageFocus));
    } else if (focusDistKeys.length) {
      setDraftFocus(new Set(focusDistKeys));
    }
  }, [focusOpen, focusDistKeys.join('|'), subsetFilter]);

  function mergeSubset(nextSigs: string[], nextFocus: string[]) {
    const s = [...nextSigs].sort();
    const f = [...nextFocus].sort();
    if (s.length === 0 && f.length === 0) {
      onSubsetFilter(null);
      return;
    }
    onSubsetFilter({
      signatures: s,
      stageFocus: f,
    });
  }

  function applySignatureDraft() {
    if (distKeys.length === 0) {
      mergeSubset([], subsetFilter?.stageFocus ?? []);
      setSigOpen(false);
      return;
    }
    const next =
      draftSigs.size === 0 || draftSigs.size === distKeys.length ? [] : [...draftSigs].sort();
    mergeSubset(next, subsetFilter?.stageFocus ?? []);
    setSigOpen(false);
  }

  function applyStageFocusDraft() {
    if (focusDistKeys.length === 0) {
      mergeSubset(subsetFilter?.signatures ?? [], []);
      setFocusOpen(false);
      return;
    }
    const next =
      draftFocus.size === 0 || draftFocus.size === focusDistKeys.length
        ? []
        : [...draftFocus].sort();
    mergeSubset(subsetFilter?.signatures ?? [], next);
    setFocusOpen(false);
  }

  return (
    <div className="card table-card">
      <div className="table-head table-head-removed">
        <div>
          <h2>{subsetFilterActive(subsetFilter) ? 'Kept rows (subset)' : 'Kept rows'}</h2>
          <label className="removed-stage-pick">
            <span className="muted">View stage</span>
            <select
              className="charts-select"
              value={String(viewStageId)}
              onChange={(e) => onViewStageIdChange(Number(e.target.value))}
              aria-label="Stage whose kept rows to list"
              disabled={!hasStages || stageOptions.length === 0}
            >
              {stageOptions.map((s) => (
                <option key={String(s.stage_id)} value={String(s.stage_id)}>
                  {`${stageLabel(Number(s.stage_id))} (${(s.output_count ?? 0).toLocaleString()} kept)`}
                </option>
              ))}
            </select>
          </label>
        </div>
        <span className="muted">
          Columns use <code>question</code> and <code>response</code>; <code>signature</code> and{' '}
          <code>stage_focus</code> (active step title from the prompt) are derived for subsetting. Click a column
          title to sort (server-side); same column again toggles direction. Tiebreaker uses <code>_row_id</code>.
        </span>
      </div>
      <p className="muted small">
        The timeline above selects the <strong>workspace</strong> stage (filters, exports, stats). Here you
        can browse <strong>kept rows at any stage</strong>; changing the timeline resets this selector to match.
      </p>

      {!hasStages && (
        <p className="muted pad">Upload a JSONL file in the workspace to load kept rows.</p>
      )}

      {hasStages && (
        <>
          {rowsError && (
            <div className="banner" style={{ marginBottom: 12 }}>
              {rowsError}
            </div>
          )}
          {rowsLoading && <p className="muted small">Loading rows…</p>}
          <div className="table-filter-strip">
            <button
              type="button"
              className="btn small"
              disabled={!taskId || !distKeys.length}
              onClick={() => setSigOpen((v) => !v)}
            >
              Signature {sigOpen ? '▲' : '▼'}
            </button>
            <button
              type="button"
              className="btn small"
              disabled={!taskId || !focusDistKeys.length}
              onClick={() => setFocusOpen((v) => !v)}
            >
              Stage focus {focusOpen ? '▲' : '▼'}
            </button>
            {subsetFilterActive(subsetFilter) && (
              <span className="muted small">
                <button
                  type="button"
                  className="link-btn"
                  onClick={() => onSubsetFilter(null)}
                >
                  Clear subset
                </button>
              </span>
            )}
          </div>

          {sigOpen && distKeys.length > 0 && (
            <div className="table-inline-filter">
              <p className="muted small">Select one or more signature values (OR). All = no filter.</p>
              <div className="filter-chips">
                {distKeys.map((k) => (
                  <label key={k} className="ck">
                    <input
                      type="checkbox"
                      checked={draftSigs.has(k)}
                      onChange={() => {
                        setDraftSigs((prev) => {
                          const n = new Set(prev);
                          if (n.has(k)) n.delete(k);
                          else n.add(k);
                          return n;
                        });
                      }}
                    />
                    <span className="mono-sm">
                      {k}{' '}
                      <span className="sig-n">({(sigCounts[k] ?? 0).toLocaleString()})</span>
                    </span>
                  </label>
                ))}
              </div>
              <div className="filter-actions">
                <button
                  type="button"
                  className="btn small"
                  onClick={() => setDraftSigs(new Set(distKeys))}
                >
                  All
                </button>
                <button type="button" className="btn small" onClick={() => setDraftSigs(new Set())}>
                  None
                </button>
                <button type="button" className="btn primary small" onClick={applySignatureDraft}>
                  Apply
                </button>
              </div>
            </div>
          )}

          {focusOpen && focusDistKeys.length > 0 && (
            <div className="table-inline-filter">
              <p className="muted small">
                Select one or more <code>stage_focus</code> labels (OR). All = no filter on this dimension.
              </p>
              <div className="filter-chips">
                {focusDistKeys.map((k) => (
                  <label key={k} className="ck">
                    <input
                      type="checkbox"
                      checked={draftFocus.has(k)}
                      onChange={() => {
                        setDraftFocus((prev) => {
                          const n = new Set(prev);
                          if (n.has(k)) n.delete(k);
                          else n.add(k);
                          return n;
                        });
                      }}
                    />
                    <span className="mono-sm">
                      {k}{' '}
                      <span className="sig-n">({(focusCounts[k] ?? 0).toLocaleString()})</span>
                    </span>
                  </label>
                ))}
              </div>
              <div className="filter-actions">
                <button
                  type="button"
                  className="btn small"
                  onClick={() => setDraftFocus(new Set(focusDistKeys))}
                >
                  All
                </button>
                <button type="button" className="btn small" onClick={() => setDraftFocus(new Set())}>
                  None
                </button>
                <button type="button" className="btn primary small" onClick={applyStageFocusDraft}>
                  Apply
                </button>
              </div>
            </div>
          )}

          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th className="table-sortable">
                    <button
                      type="button"
                      className="th-sort-btn"
                      onClick={() => onSortHeaderClick('row')}
                      aria-sort={sortKey === 'row' ? (sortDir === 'asc' ? 'ascending' : 'descending') : 'none'}
                    >
                      Row{sortIndicator('row')}
                    </button>
                  </th>
                  <th className="table-sortable">
                    <button
                      type="button"
                      className="th-sort-btn"
                      onClick={() => onSortHeaderClick('signature')}
                      aria-sort={
                        sortKey === 'signature' ? (sortDir === 'asc' ? 'ascending' : 'descending') : 'none'
                      }
                    >
                      Signature{sortIndicator('signature')}
                    </button>
                  </th>
                  <th className="table-sortable">
                    <button
                      type="button"
                      className="th-sort-btn"
                      onClick={() => onSortHeaderClick('stage_focus')}
                      aria-sort={
                        sortKey === 'stage_focus' ? (sortDir === 'asc' ? 'ascending' : 'descending') : 'none'
                      }
                    >
                      Stage focus{sortIndicator('stage_focus')}
                    </button>
                  </th>
                  <th className="table-sortable">
                    <button
                      type="button"
                      className="th-sort-btn"
                      onClick={() => onSortHeaderClick('question')}
                      aria-sort={
                        sortKey === 'question' ? (sortDir === 'asc' ? 'ascending' : 'descending') : 'none'
                      }
                    >
                      Question{sortIndicator('question')}
                    </button>
                  </th>
                  <th className="table-sortable">
                    <button
                      type="button"
                      className="th-sort-btn"
                      onClick={() => onSortHeaderClick('response')}
                      aria-sort={
                        sortKey === 'response' ? (sortDir === 'asc' ? 'ascending' : 'descending') : 'none'
                      }
                    >
                      Answer{sortIndicator('response')}
                    </button>
                  </th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {rows.map((r, i) => {
                  const q = questionText(r);
                  const a = answerText(r);
                  const qPrev = q.length > PREVIEW_LEN ? `${q.slice(0, PREVIEW_LEN)}…` : q;
                  const aPrev = a.length > PREVIEW_LEN ? `${a.slice(0, PREVIEW_LEN)}…` : a;
                  return (
                    <tr key={rowId(r, i, offset)}>
                      <td className="nowrap">{rowId(r, i, offset)}</td>
                      <td>
                        <span className="cell-preview small-mono">{signatureLabel(r)}</span>
                      </td>
                      <td>
                        <span className="cell-preview small-mono">{stageFocusLabel(r)}</span>
                      </td>
                      <td>
                        <span className="cell-preview">{qPrev || '—'}</span>
                      </td>
                      <td>
                        <span className="cell-preview">{aPrev || '—'}</span>
                      </td>
                      <td>
                        <button type="button" className="btn small" onClick={() => setFullRow(r)}>
                          View full
                        </button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
            {rows.length === 0 && !rowsLoading && (
              <p className="muted pad">{rowsError ? 'No rows loaded.' : 'No rows in this stage.'}</p>
            )}
          </div>
          {fullRow && <KeptFullModal row={fullRow} onClose={() => setFullRow(null)} />}
          <PaginationBar total={total} limit={limit} offset={offset} onOffset={setOffset} />
        </>
      )}
    </div>
  );
}

function textBlock(v: unknown): { text: string; has: boolean } {
  if (v == null || v === '') return { text: '', has: false };
  const t = typeof v === 'string' || typeof v === 'number' ? String(v) : JSON.stringify(v, null, 2);
  return { text: t, has: t.length > 0 };
}

function KeptFullModal({
  row,
  onClose,
}: {
  row: Record<string, unknown>;
  onClose: () => void;
}) {
  const q = questionText(row);
  const a = answerText(row);
  const meta: { k: string; label: string }[] = [
    { k: 'signature', label: 'Signature' },
    { k: 'stage_focus', label: 'Stage focus' },
    { k: 'curation_path', label: 'Curation path' },
    { k: 'technique', label: 'Technique' },
  ];

  return (
    <div className="modal-root" role="dialog" aria-modal>
      <button
        type="button"
        className="modal-backdrop"
        aria-label="Close"
        onClick={onClose}
      />
      <div className="modal-panel">
        <div className="modal-head">
          <h3>Row {String(row._row_id ?? row.row_id ?? '—')}</h3>
          <button type="button" className="btn" onClick={onClose}>
            Close
          </button>
        </div>
        <div className="modal-body">
          {(() => {
            const pb = textBlock(q);
            if (!pb.has) return null;
            return (
              <section className="modal-block">
                <h4 className="modal-h4">Question</h4>
                <pre className="modal-pre">{pb.text}</pre>
              </section>
            );
          })()}
          {(() => {
            const ab = textBlock(a);
            if (!ab.has) return null;
            return (
              <section className="modal-block">
                <h4 className="modal-h4">Answer (response)</h4>
                <pre className="modal-pre">{ab.text}</pre>
              </section>
            );
          })()}
          {meta.map(({ k, label }) => {
            const t = textBlock(row[k]);
            if (!t.has) return null;
            return (
              <section key={k} className="modal-block">
                <h4 className="modal-h4">{label}</h4>
                <pre className="modal-pre">{t.text}</pre>
              </section>
            );
          })}
        </div>
      </div>
    </div>
  );
}
