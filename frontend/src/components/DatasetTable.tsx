import { useEffect, useState } from 'react';
import { getDistribution, getRows, getStageView, type Dist, type ViewFilter } from '../api';
import { PaginationBar } from './PaginationBar';

type Props = {
  datasetId: string | null;
  stageId: number;
  viewFilter: ViewFilter | null;
  onViewFilter: (v: ViewFilter | null) => void;
};

const PREVIEW_LEN = 220;
const SIG = 'signature';

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

export function DatasetTable({ datasetId, stageId, viewFilter, onViewFilter }: Props) {
  const [rows, setRows] = useState<Record<string, unknown>[]>([]);
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);
  const [fullRow, setFullRow] = useState<Record<string, unknown> | null>(null);
  const [sigOpen, setSigOpen] = useState(false);
  const [distKeys, setDistKeys] = useState<string[]>([]);
  const [sigCounts, setSigCounts] = useState<Record<string, number>>({});
  const [draftSigs, setDraftSigs] = useState<Set<string>>(new Set());
  const limit = 100;

  // Timeline / stage switch: clear local signature pickers (viewFilter cleared in App)
  useEffect(() => {
    setDraftSigs(new Set());
    setSigOpen(false);
  }, [stageId]);

  useEffect(() => {
    if (!datasetId) {
      setDistKeys([]);
      return;
    }
    let o = true;
    getDistribution(datasetId, stageId)
      .then((d: Dist) => {
        if (!o) return;
        const sig = d.signature || {};
        const keys = Object.keys(sig).sort((a, b) => a.localeCompare(b));
        setDistKeys(keys);
        setSigCounts(sig);
      })
      .catch(() => {
        if (o) {
          setDistKeys([]);
          setSigCounts({});
        }
      });
    return () => {
      o = false;
    };
  }, [datasetId, stageId]);

  useEffect(() => {
    setOffset(0);
  }, [viewFilter, stageId, datasetId]);

  useEffect(() => {
    if (!datasetId) {
      setRows([]);
      setTotal(0);
      return;
    }
    let ok = true;
    const p = viewFilter
      ? getStageView(datasetId, stageId, viewFilter, limit, offset)
      : getRows(datasetId, stageId, limit, offset);
    p.then((r) => {
      if (ok) {
        setRows(r.rows);
        setTotal(r.total);
      }
    }).catch(() => {
      if (ok) {
        setRows([]);
        setTotal(0);
      }
    });
    return () => {
      ok = false;
    };
  }, [datasetId, stageId, offset, viewFilter, limit]);

  useEffect(() => {
    if (!sigOpen) return;
    if (viewFilter?.field === SIG && viewFilter.values.length > 0) {
      setDraftSigs(new Set(viewFilter.values));
    } else if (distKeys.length) {
      setDraftSigs(new Set(distKeys));
    }
  }, [sigOpen, distKeys.join('|'), viewFilter]);

  function applySignatureDraft() {
    if (distKeys.length === 0) {
      onViewFilter(null);
      setSigOpen(false);
      return;
    }
    if (draftSigs.size === 0 || draftSigs.size === distKeys.length) {
      onViewFilter(null);
    } else {
      onViewFilter({ field: SIG, values: [...draftSigs].sort() });
    }
    setSigOpen(false);
  }

  return (
    <div className="card table-card">
      <div className="table-head">
        <h2>{viewFilter ? 'Kept rows (subset)' : 'Kept rows'}</h2>
        <span className="muted">
          Columns use <code>question</code> and <code>response</code> from the file; <code>signature</code>{' '}
          is derived for grouping.
        </span>
      </div>

      <div className="table-filter-strip">
        <button
          type="button"
          className="btn small"
          disabled={!datasetId || !distKeys.length}
          onClick={() => setSigOpen((v) => !v)}
        >
          Signature {sigOpen ? '▲' : '▼'}
        </button>
        {viewFilter?.field === SIG && (
          <span className="muted small">
            Active: {viewFilter.values.join(', ')}
            <button
              type="button"
              className="link-btn"
              onClick={() => onViewFilter(null)}
            >
              Clear
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
            <button type="button" className="btn small" onClick={() => setDraftSigs(new Set(distKeys))}>
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

      <div className="table-scroll">
        <table>
          <thead>
            <tr>
              <th>Row</th>
              <th>Signature</th>
              <th>Question</th>
              <th>Answer</th>
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
        {rows.length === 0 && <p className="muted pad">No rows (or still loading).</p>}
      </div>
      {fullRow && <KeptFullModal row={fullRow} onClose={() => setFullRow(null)} />}
      <PaginationBar total={total} limit={limit} offset={offset} onOffset={setOffset} />
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
    { k: 'curation_path', label: 'Curation path' },
    { k: 'stage', label: 'Stage' },
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
