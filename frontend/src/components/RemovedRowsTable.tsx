import { useEffect, useState } from 'react';
import { getRemoved, getRemovedSummary, type RemovalCategory, type Stage } from '../api';
import { PaginationBar } from './PaginationBar';
import { RichDocBlock } from './RichDocBlock';
import {
  splitResponseForView,
  stripSplitBoundaryTagsForView,
  stripTableThinkingPreview,
} from '../responseSplit';

type Props = {
  taskId: string | null;
  hasStages: boolean;
  /** Current workspace stage; syncs the “view removed in stage …” default when the timeline changes. */
  stageId: number;
  stages: Stage[];
};

function stageLabel(id: number): string {
  return id === 0 ? 'Raw' : `S${id}`;
}

const REASONS: { id: RemovalCategory; label: string }[] = [
  { id: 'hacking', label: 'Hacking' },
  { id: 'duplicate', label: 'Duplicate' },
  { id: 'length', label: 'Length' },
  { id: 'format', label: 'Format' },
  { id: 'balancing', label: 'Balancing' },
  { id: 'other', label: 'Other' },
];

const PREVIEW_LEN = 200;
const PREVIEW_CELL = 150;

function questionText(r: Record<string, unknown>): string {
  if (r.question == null) return '';
  return String(r.question);
}

function signatureLabel(r: Record<string, unknown>): string {
  const s = r.signature;
  if (s != null && String(s) !== '') return String(s);
  return '—';
}

function rowId(r: Record<string, unknown>, i: number, offset: number): string {
  const id = r._row_id ?? r.row_id;
  if (id != null && id !== '') return String(id);
  return String(offset + i + 1);
}

function reasonLine(r: Record<string, unknown>): string {
  const l = r.removal_label;
  if (l != null && String(l) !== '') return String(l);
  return String(r.removal_reason ?? '—');
}

export function RemovedRowsTable({ taskId, hasStages, stageId, stages }: Props) {
  const [viewStageId, setViewStageId] = useState(stageId);
  const [rows, setRows] = useState<Record<string, unknown>[]>([]);
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);
  const [summary, setSummary] = useState<{
    by_category: Record<string, number>;
    by_signature: Record<string, number>;
  } | null>(null);
  const [fullRow, setFullRow] = useState<Record<string, unknown> | null>(null);

  const [sigOpen, setSigOpen] = useState(false);
  const [reasonOpen, setReasonOpen] = useState(false);
  const [draftSigs, setDraftSigs] = useState<Set<string>>(new Set());
  const [draftReasons, setDraftReasons] = useState<Set<RemovalCategory>>(new Set());

  const [appliedSigs, setAppliedSigs] = useState<string[]>([]);
  const [appliedReasons, setAppliedReasons] = useState<RemovalCategory[]>([]);

  const stageOptions = (() => {
    if (stages.length) return [...stages].sort((a, b) => a.stage_id - b.stage_id);
    return [{ stage_id: stageId, removed_count: 0, stage_name: '' } as Stage];
  })();

  useEffect(() => {
    setViewStageId(stageId);
  }, [stageId]);

  // Workspace stage changed: clear local removed-row filters
  useEffect(() => {
    setAppliedSigs([]);
    setAppliedReasons([]);
    setSigOpen(false);
    setReasonOpen(false);
  }, [stageId, taskId]);

  const limit = 100;
  const sigKeys = Object.keys(summary?.by_signature || {})
    .filter((k) => k !== '')
    .sort((a, b) => a.localeCompare(b));

  useEffect(() => {
    if (!taskId || !hasStages) {
      setSummary(null);
      return;
    }
    let o = true;
    getRemovedSummary(taskId, viewStageId)
      .then((s) => {
        if (o)
          setSummary({ by_category: s.by_category, by_signature: s.by_signature || {} });
      })
      .catch(() => {
        if (o) setSummary(null);
      });
    return () => {
      o = false;
    };
  }, [taskId, hasStages, viewStageId]);

  useEffect(() => {
    setOffset(0);
  }, [viewStageId, taskId, appliedSigs, appliedReasons]);

  useEffect(() => {
    if (!taskId || !hasStages) {
      setRows([]);
      setTotal(0);
      return;
    }
    let ok = true;
    const opt: { reasonCategories?: RemovalCategory[]; signatures?: string[] } = {};
    if (appliedReasons.length > 0) opt.reasonCategories = appliedReasons;
    if (appliedSigs.length > 0) opt.signatures = appliedSigs;
    getRemoved(taskId, viewStageId, limit, offset, opt)
      .then((r) => {
        if (ok) {
          setRows(r.rows);
          setTotal(r.total);
        }
      })
      .catch(() => {
        if (ok) {
          setRows([]);
          setTotal(0);
        }
      });
    return () => {
      ok = false;
    };
  }, [taskId, hasStages, viewStageId, offset, appliedSigs, appliedReasons, limit]);

  useEffect(() => {
    if (!sigOpen || !sigKeys.length) return;
    setDraftSigs(new Set(appliedSigs.length > 0 ? appliedSigs : sigKeys));
  }, [sigOpen, sigKeys.join('|'), appliedSigs.join('|')]);

  useEffect(() => {
    if (!reasonOpen) return;
    setDraftReasons(
      new Set(appliedReasons.length > 0 ? appliedReasons : REASONS.map((r) => r.id))
    );
  }, [reasonOpen, appliedReasons.join(',')]);

  function applySigFilter() {
    if (sigKeys.length === 0) {
      setSigOpen(false);
      return;
    }
    if (draftSigs.size === 0 || draftSigs.size === sigKeys.length) {
      setAppliedSigs([]);
    } else {
      setAppliedSigs([...draftSigs].sort());
    }
    setSigOpen(false);
  }

  function applyReasonFilter() {
    const all = new Set(REASONS.map((r) => r.id));
    if (draftReasons.size === 0 || draftReasons.size === all.size) {
      setAppliedReasons([]);
    } else {
      setAppliedReasons([...draftReasons]);
    }
    setReasonOpen(false);
  }

  return (
    <div className="card table-card">
      <div className="table-head table-head-removed">
        <div>
          <h2>Removed in {stageLabel(viewStageId)}</h2>
          <label className="removed-stage-pick">
            <span className="muted">View stage</span>
            <select
              className="charts-select"
              value={viewStageId}
              onChange={(e) => setViewStageId(Number(e.target.value))}
              aria-label="Stage whose removed rows to list"
            >
              {stageOptions.map((s) => (
                <option key={s.stage_id} value={s.stage_id}>
                  {`${stageLabel(s.stage_id)} (−${(s.removed_count ?? 0).toLocaleString()})`}
                </option>
              ))}
            </select>
          </label>
        </div>
        <span className="muted">
          Previews use <code>question</code> and <code>response</code> (response split into Thinking / Response).
        </span>
      </div>
      <p className="muted small">
        The workspace timeline is on <strong>{stageLabel(stageId)}</strong>; you can list removals from
        any past stage above. Expand <strong>Signature</strong> or <strong>Reason</strong> to restrict
        which removed rows you see. Empty selection on Apply = no filter for that axis. Counts in
        Summary include all removals; the table total respects both filters.
      </p>

      {!hasStages ? (
        <p className="muted pad">Upload a JSONL dataset first to inspect removed rows by stage.</p>
      ) : (
        <>
      <div className="table-filter-strip">
        <button
          type="button"
          className="btn small"
          disabled={!taskId || !sigKeys.length}
          onClick={() => setSigOpen((v) => !v)}
        >
          Signature {sigOpen ? '▲' : '▼'}
        </button>
        <button
          type="button"
          className="btn small"
          disabled={!taskId}
          onClick={() => setReasonOpen((v) => !v)}
        >
          Reason {reasonOpen ? '▲' : '▼'}
        </button>
        {(appliedSigs.length > 0 || appliedReasons.length > 0) && (
          <button
            type="button"
            className="link-btn"
            onClick={() => {
              setAppliedSigs([]);
              setAppliedReasons([]);
            }}
          >
            Clear table filters
          </button>
        )}
      </div>

      {sigOpen && sigKeys.length > 0 && (
        <div className="table-inline-filter">
          <p className="muted small">Include rows with any of these signature values (OR).</p>
          <div className="filter-chips">
            {sigKeys.map((k) => (
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
                  {k} ({(summary?.by_signature[k] ?? 0).toLocaleString()})
                </span>
              </label>
            ))}
          </div>
          <div className="filter-actions">
            <button
              type="button"
              className="btn small"
              onClick={() => setDraftSigs(new Set(sigKeys))}
            >
              All
            </button>
            <button type="button" className="btn small" onClick={() => setDraftSigs(new Set())}>
              None
            </button>
            <button type="button" className="btn primary small" onClick={applySigFilter}>
              Apply
            </button>
          </div>
        </div>
      )}

      {reasonOpen && (
        <div className="table-inline-filter">
          <p className="muted small">Include rows matching any of these reason categories (OR).</p>
          <div className="filter-chips">
            {REASONS.map(({ id, label }) => (
              <label key={id} className="ck">
                <input
                  type="checkbox"
                  checked={draftReasons.has(id)}
                  onChange={() => {
                    setDraftReasons((prev) => {
                      const n = new Set(prev);
                      if (n.has(id)) n.delete(id);
                      else n.add(id);
                      return n;
                    });
                  }}
                />
                <span>
                  {label} (
                  {(summary?.by_category[id] ?? 0).toLocaleString()})
                </span>
              </label>
            ))}
          </div>
          <div className="filter-actions">
            <button
              type="button"
              className="btn small"
              onClick={() => setDraftReasons(new Set(REASONS.map((r) => r.id)))}
            >
              All
            </button>
            <button
              type="button"
              className="btn small"
              onClick={() => setDraftReasons(new Set())}
            >
              None
            </button>
            <button type="button" className="btn primary small" onClick={applyReasonFilter}>
              Apply
            </button>
          </div>
        </div>
      )}

      <div className="table-scroll">
        <table>
          <thead>
            <tr>
              <th>Row</th>
              <th>Signature</th>
              <th>Reason</th>
              <th>Question</th>
              <th>Thinking</th>
              <th>Response</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => {
              const q = questionText(r);
              const qPrev = q.length > PREVIEW_LEN ? `${q.slice(0, PREVIEW_LEN)}…` : q;
              const sp = splitResponseForView(String(r.response ?? ''));
              const th = stripTableThinkingPreview(sp.thinking);
              const ans = sp.answer;
              const tPrev = th.length > PREVIEW_CELL ? `${th.slice(0, PREVIEW_CELL)}…` : th;
              const aPrev = ans.length > PREVIEW_CELL ? `${ans.slice(0, PREVIEW_CELL)}…` : ans;
              return (
                <tr key={String(r._row_id ?? i)}>
                  <td className="nowrap">{rowId(r, i, offset)}</td>
                  <td>
                    <span className="cell-preview small-mono">{signatureLabel(r)}</span>
                  </td>
                  <td>
                    <span className="cell-preview reason-td">{reasonLine(r)}</span>
                  </td>
                  <td>
                    <span className="cell-preview">{qPrev || '—'}</span>
                  </td>
                  <td>
                    <span className="cell-preview cell-preview-muted">{tPrev || '—'}</span>
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
        {rows.length === 0 && total === 0 && (
          <p className="muted pad">
            No removed rows for {stageLabel(viewStageId)} (or for these filters).
          </p>
        )}
      </div>
      {fullRow && <RemovedFullModal row={fullRow} onClose={() => setFullRow(null)} />}
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

function RemovedFullModal({
  row,
  onClose,
}: {
  row: Record<string, unknown>;
  onClose: () => void;
}) {
  const q = questionText(row);
  const a = String(row.response ?? '');

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
            const t = textBlock(String(row.removal_label ?? row.removal_reason ?? ''));
            if (!t.has) return null;
            return (
              <section className="modal-block">
                <h4 className="modal-h4">Reason (summary)</h4>
                <RichDocBlock source={t.text} />
              </section>
            );
          })()}
          {row.removal_reasons != null && (
            <section className="modal-block">
              <h4 className="modal-h4">Per-filter reasons (raw)</h4>
              <pre className="modal-pre">
                {Array.isArray(row.removal_reasons)
                  ? (row.removal_reasons as unknown[]).map((x) => `• ${String(x)}`).join('\n')
                  : String(row.removal_reasons)}
              </pre>
            </section>
          )}
          {(() => {
            const t = textBlock(q);
            if (!t.has) return null;
            return (
              <section className="modal-block modal-qa-card">
                <h4 className="modal-h4">Question</h4>
                <RichDocBlock source={t.text} />
              </section>
            );
          })()}
          {(() => {
            const t = textBlock(a);
            if (!t.has) return null;
            const sp = splitResponseForView(t.text);
            const thinkDoc = stripSplitBoundaryTagsForView(sp.thinking);
            const answerDoc = stripSplitBoundaryTagsForView(sp.answer);
            return (
              <>
                {thinkDoc.trim() ? (
                  <section className="modal-block modal-thinking-card">
                    <h4 className="modal-h4">Thinking</h4>
                    <RichDocBlock source={thinkDoc} />
                  </section>
                ) : null}
                {answerDoc.trim() ? (
                  <section className="modal-block modal-answer-card">
                    <h4 className="modal-h4">Answer</h4>
                    <RichDocBlock source={answerDoc} />
                  </section>
                ) : null}
              </>
            );
          })()}
        </div>
      </div>
    </div>
  );
}
