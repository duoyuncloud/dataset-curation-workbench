import { useEffect, useMemo, useState, type CSSProperties } from 'react';
import { Cell, Legend, Pie, PieChart, ResponsiveContainer, Tooltip } from 'recharts';
import { getSignaturesByStage, type SignaturesByStageRow, type ViewFilter } from '../api';

const tooltipStyle: CSSProperties = {
  background: '#141414',
  border: '1px solid #2a2a2a',
  borderRadius: 8,
  color: '#fff',
};

const FILL: string[] = [
  '#ffffff',
  '#b0b0b0',
  '#7a7a7a',
  '#4a4a4a',
  '#c4b5fd',
  '#7dd3fc',
  '#86efac',
  '#5c5c5c',
];

const TOP_K = 7;

type PieSlice = { name: string; value: number };

type Props = {
  datasetId: string | null;
  currentStageId: number;
  /** Bumps when the pipeline gets a new stage; refetch signature distribution for all stages. */
  stageCount: number;
  viewFilter: ViewFilter | null;
  /** `sourceStageId` is the stage shown in the pie; parent should align the workspace to this stage when applying the table filter. */
  onDrill: (field: string, value: string, sourceStageId: number) => void;
};

function stageLabel(id: number): string {
  return id === 0 ? 'Raw' : `S${id}`;
}

function topSlicesForStage(stage: SignaturesByStageRow | undefined): PieSlice[] {
  if (!stage) return [];
  const by = stage.by_signature || {};
  const entries = Object.entries(by)
    .map(([k, v]) => ({ k, v: Number(v) || 0 }))
    .filter((e) => e.v > 0);
  if (entries.length === 0) return [];
  entries.sort((a, b) => b.v - a.v);
  const top = entries.slice(0, TOP_K);
  const rest = entries.slice(TOP_K);
  const out: PieSlice[] = top.map((e) => ({ name: e.k, value: e.v }));
  if (rest.length) {
    const o = rest.reduce((s, e) => s + e.v, 0);
    if (o > 0) out.push({ name: 'other', value: o });
  }
  return out;
}

export function ChartsPanel({ datasetId, currentStageId, stageCount, viewFilter, onDrill }: Props) {
  const [rows, setRows] = useState<SignaturesByStageRow[] | null>(null);
  const [err, setErr] = useState(false);
  const [selectedStageId, setSelectedStageId] = useState(currentStageId);

  useEffect(() => {
    if (!datasetId) {
      setRows(null);
      return;
    }
    let ok = true;
    getSignaturesByStage(datasetId)
      .then((r) => {
        if (ok) {
          setRows(r.stages);
          setErr(false);
        }
      })
      .catch(() => {
        if (ok) {
          setRows(null);
          setErr(true);
        }
      });
    return () => {
      ok = false;
    };
  }, [datasetId, stageCount]);

  // Follow main workspace stage when the user changes the pipeline selection
  useEffect(() => {
    setSelectedStageId(currentStageId);
  }, [currentStageId, datasetId]);

  const pieData = useMemo(() => {
    const st = rows?.find((r) => r.stage_id === selectedStageId);
    return topSlicesForStage(st);
  }, [rows, selectedStageId]);

  const totalKept = useMemo(() => {
    return pieData.reduce((s, p) => s + p.value, 0);
  }, [pieData]);

  if (!datasetId) {
    return (
      <div className="card charts charts-compact">
        <h2>Signature distribution</h2>
        <p className="muted">Upload a dataset to see the chart.</p>
      </div>
    );
  }

  if (err) {
    return (
      <div className="card charts charts-compact">
        <h2>Signature distribution</h2>
        <p className="muted">Could not load signature counts.</p>
      </div>
    );
  }

  if (!rows || !rows.length) {
    return (
      <div className="card charts charts-compact">
        <h2>Signature distribution</h2>
        <p className="muted">No data yet.</p>
      </div>
    );
  }

  return (
    <div className="card charts charts-compact">
      <div className="charts-pie-head">
        <h2>Signature distribution</h2>
        <label className="charts-stage-pick">
          <span className="muted">Stage</span>
          <select
            className="charts-select"
            value={selectedStageId}
            onChange={(e) => setSelectedStageId(Number(e.target.value))}
            aria-label="Stage for signature pie chart"
          >
            {rows
              .slice()
              .sort((a, b) => a.stage_id - b.stage_id)
              .map((r) => (
                <option key={r.stage_id} value={r.stage_id}>
                  {stageLabel(r.stage_id)} ({(r.total ?? 0).toLocaleString()} kept)
                </option>
              ))}
          </select>
        </label>
      </div>
      <p className="muted small">
        Kept-row signature share at <strong>{stageLabel(selectedStageId)}</strong>
        {totalKept > 0 ? ` (${totalKept.toLocaleString()} rows)` : ''}. Top {TOP_K} families +{' '}
        <code>other</code>. Click a slice to move the workspace to this chart’s stage and set the
        table’s signature filter (so export matches what you see). Timeline was:{' '}
        <strong>{stageLabel(currentStageId)}</strong>.
      </p>
      {viewFilter && (
        <p className="muted small" style={{ marginTop: 4 }}>
          Table filter: <code>{viewFilter.field}</code> = {viewFilter.values.join(', ')}
        </p>
      )}
      {pieData.length === 0 ? (
        <p className="muted small">No kept rows with a signature for this stage.</p>
      ) : (
        <div className="rechart rechart-pie rechart-compact">
          <ResponsiveContainer width="100%" height={220}>
            <PieChart margin={{ top: 0, right: 8, left: 8, bottom: 0 }}>
              <Pie
                data={pieData}
                dataKey="value"
                nameKey="name"
                cx="50%"
                cy="50%"
                innerRadius={0}
                outerRadius={88}
                paddingAngle={1}
                onClick={(d, index) => {
                  const name =
                    d?.name != null
                      ? String(d.name)
                      : typeof index === 'number' && pieData[index]
                        ? String(pieData[index].name)
                        : '';
                  if (name && name !== 'other') onDrill('signature', name, selectedStageId);
                }}
              >
                {pieData.map((_, i) => (
                  <Cell key={pieData[i].name} fill={FILL[i % FILL.length]} style={{ cursor: 'pointer' }} />
                ))}
              </Pie>
              <Tooltip
                contentStyle={tooltipStyle}
                labelStyle={{ color: '#fff' }}
                formatter={(value) => {
                  const n = typeof value === 'number' ? value : Number(value);
                  const t = pieData.reduce((s, x) => s + x.value, 0);
                  const pct = t > 0 && !Number.isNaN(n) ? ((n / t) * 100).toFixed(1) : '—';
                  return [`${Number.isNaN(n) ? '—' : n.toLocaleString()} (${pct}%)`, 'Count'];
                }}
              />
              <Legend
                layout="vertical"
                align="right"
                verticalAlign="middle"
                wrapperStyle={{ fontSize: 11, color: '#a1a1a1' }}
                formatter={(value) => (value === 'other' ? 'other' : value)}
              />
            </PieChart>
          </ResponsiveContainer>
        </div>
      )}
    </div>
  );
}
