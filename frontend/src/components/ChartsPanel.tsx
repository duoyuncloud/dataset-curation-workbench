import { useEffect, useMemo, useRef, useState, type CSSProperties } from 'react';
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  LabelList,
  Legend,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import {
  formatSubsetFilterLabel,
  getDistribution,
  getSignaturesByStage,
  type Dist,
  type SignaturesByStageRow,
  type SubsetFilter,
} from '../api';

const tooltipStyle: CSSProperties = {
  background: '#141414',
  border: '1px solid #2a2a2a',
  borderRadius: 8,
  color: '#fff',
};

/** Distinct colors for pie / bars / histogram (readable on dark UI). */
const CHART_COLORS: string[] = [
  '#60a5fa',
  '#34d399',
  '#fbbf24',
  '#f472b6',
  '#a78bfa',
  '#fb923c',
  '#22d3ee',
  '#4ade80',
  '#f87171',
  '#c084fc',
  '#2dd4bf',
  '#818cf8',
];

const TOP_K = 7;
const BAR_TOP = 12;

type PieSlice = { name: string; value: number };

type BarRow = { name: string; count: number };

type HistBinRow = {
  label: string;
  count: number;
  bin_start: number;
  bin_end: number;
};

type Props = {
  taskId: string | null;
  hasStages: boolean;
  currentStageId: number;
  stageCount: number;
  subsetFilter: SubsetFilter | null;
  onPickSubset: (
    dimension: 'signature' | 'stage_focus',
    value: string,
    sourceStageId: number
  ) => void;
};

function stageLabel(id: number): string {
  return id === 0 ? 'Raw' : `S${id}`;
}

function topSlicesForStage(stage: SignaturesByStageRow | undefined): PieSlice[] {
  if (!stage) return [];
  const by = stage.by_signature || {};
  return topSlicesFromCountMap(by as Record<string, number>);
}

/** Pie slices from a count map (e.g. distribution ``stage_focus``). */
function topSlicesFromCountMap(by: Record<string, number> | undefined): PieSlice[] {
  if (!by || typeof by !== 'object') return [];
  const entries = Object.entries(by)
    .map(([k, v]) => ({ k, v: Number(v) || 0 }))
    .filter((e) => e.v > 0 && e.k !== '' && e.k !== '__missing__');
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

function distToBarRows(d: Record<string, number> | undefined, top: number): BarRow[] {
  const o = d && typeof d === 'object' ? d : {};
  const entries = Object.entries(o)
    .map(([name, count]) => ({ name, count: Number(count) || 0 }))
    .filter((e) => e.count > 0 && e.name !== '' && e.name !== '__missing__');
  entries.sort((a, b) => b.count - a.count);
  return entries.slice(0, top);
}

export function ChartsPanel({
  taskId,
  hasStages,
  currentStageId,
  stageCount,
  subsetFilter,
  onPickSubset,
}: Props) {
  const [rows, setRows] = useState<SignaturesByStageRow[] | null>(null);
  const [metaLoading, setMetaLoading] = useState(false);
  const [dist, setDist] = useState<Dist | null>(null);
  const [err, setErr] = useState(false);
  const [selectedStageId, setSelectedStageId] = useState(currentStageId);
  const [chartMetric, setChartMetric] = useState<'signature' | 'stage_focus'>('signature');
  const prevTaskIdForCharts = useRef<string | null>(null);

  useEffect(() => {
    setSelectedStageId(currentStageId);
  }, [currentStageId, taskId]);

  useEffect(() => {
    if (!taskId || !hasStages) {
      setRows(null);
      setMetaLoading(false);
      setErr(false);
      prevTaskIdForCharts.current = null;
      return;
    }
    const taskSwitched = prevTaskIdForCharts.current !== taskId;
    prevTaskIdForCharts.current = taskId;
    let ok = true;
    if (taskSwitched) setRows(null);
    setMetaLoading(true);
    getSignaturesByStage(taskId)
      .then((r) => {
        if (ok) {
          setRows(r.stages);
          setErr(false);
        }
      })
      .catch(() => {
        if (ok) {
          setErr(true);
        }
      })
      .finally(() => {
        if (ok) setMetaLoading(false);
      });
    return () => {
      ok = false;
    };
  }, [taskId, hasStages, stageCount]);

  useEffect(() => {
    if (!taskId || !hasStages) {
      setDist(null);
      return;
    }
    let ok = true;
    getDistribution(taskId, selectedStageId)
      .then((d) => {
        if (ok) setDist(d);
      })
      .catch(() => {
        /* keep previous dist on transient error */
      });
    return () => {
      ok = false;
    };
  }, [taskId, hasStages, selectedStageId, stageCount]);

  const pieData = useMemo(() => {
    if (chartMetric === 'signature') {
      const st = rows?.find((r) => r.stage_id === selectedStageId);
      return topSlicesForStage(st);
    }
    return topSlicesFromCountMap(dist?.stage_focus);
  }, [rows, selectedStageId, chartMetric, dist]);

  const barData = useMemo(() => {
    if (chartMetric === 'signature') return distToBarRows(dist?.signature, BAR_TOP);
    return distToBarRows(dist?.stage_focus, BAR_TOP);
  }, [dist, chartMetric]);

  const totalKept = useMemo(() => {
    return pieData.reduce((s, p) => s + p.value, 0);
  }, [pieData]);

  const runtimeHistRows = useMemo((): HistBinRow[] => {
    const h = dist?.runtime_ms_histogram;
    if (!Array.isArray(h) || h.length === 0) return [];
    return h
      .map((b) => {
        const a = Number(b.bin_start);
        const c = Number(b.bin_end);
        const cnt = Number(b.count) || 0;
        return {
          label: `${Number.isFinite(a) ? Math.round(a) : '?'}-${Number.isFinite(c) ? Math.round(c) : '?'} ms`,
          count: cnt,
          bin_start: a,
          bin_end: c,
        };
      })
      .filter((r) => r.count > 0);
  }, [dist]);

  if (!taskId) {
    return (
      <div className="card charts charts-compact">
        <h2>Distributions</h2>
        <p className="muted">Open a task to see charts.</p>
      </div>
    );
  }

  if (!hasStages) {
    return (
      <div className="card charts charts-compact">
        <h2>Distributions</h2>
        <p className="muted">Upload a JSONL file to see signature and stage_focus counts.</p>
      </div>
    );
  }

  if (rows === null) {
    return (
      <div className="card charts charts-compact">
        <h2>Distributions</h2>
        <p className="muted">Loading…</p>
      </div>
    );
  }

  if (err && (!rows || rows.length === 0)) {
    return (
      <div className="card charts charts-compact">
        <h2>Distributions</h2>
        <p className="muted">Could not load stage metadata.</p>
      </div>
    );
  }

  if (!rows.length) {
    return (
      <div className="card charts charts-compact">
        <h2>Distributions</h2>
        <p className="muted">No stages yet.</p>
      </div>
    );
  }

  return (
    <div className={`card charts charts-compact${metaLoading ? ' charts-refreshing' : ''}`}>
      <div className="charts-pie-head">
        <h2>Distributions</h2>
        <div className="charts-head-controls">
          <label className="charts-stage-pick">
            <span className="muted">Stage</span>
            <select
              className="charts-select"
              value={selectedStageId}
              onChange={(e) => setSelectedStageId(Number(e.target.value))}
              aria-label="Stage for charts"
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
          <label className="charts-stage-pick">
            <span className="muted">Metric</span>
            <select
              className="charts-select"
              value={chartMetric}
              onChange={(e) => setChartMetric(e.target.value as 'signature' | 'stage_focus')}
              aria-label="Distribution metric"
            >
              <option value="signature">Signature</option>
              <option value="stage_focus">Stage focus</option>
            </select>
          </label>
        </div>
      </div>
      <p
        className="muted small"
        title="Click a slice or bar to set the table subset (view-only). Does not create a stage."
      >
        Stage <strong>{stageLabel(selectedStageId)}</strong> · workspace{' '}
        <strong>{stageLabel(currentStageId)}</strong> — hover for tip.
      </p>
      {subsetFilter &&
        (subsetFilter.signatures.length > 0 || subsetFilter.stageFocus.length > 0) && (
          <p className="muted small" style={{ marginTop: 4 }}>
            Active subset: <code>{formatSubsetFilterLabel(subsetFilter)}</code>
          </p>
        )}
      {pieData.length === 0 ? (
        <p className="muted small">
          {chartMetric === 'signature'
            ? 'No kept rows with a signature for this stage.'
            : 'No stage_focus labels for this stage (or all unknown).'}
        </p>
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
                onClick={(d) => {
                  const name =
                    d?.name != null
                      ? String(d.name)
                      : '';
                  if (name && name !== 'other')
                    onPickSubset(chartMetric === 'signature' ? 'signature' : 'stage_focus', name, selectedStageId);
                }}
              >
                {pieData.map((_, i) => (
                  <Cell
                    key={pieData[i].name}
                    fill={CHART_COLORS[i % CHART_COLORS.length]}
                    style={{ cursor: 'pointer' }}
                  />
                ))}
              </Pie>
              <Tooltip
                content={({ active, payload }) => {
                  if (!active || !payload?.length) return null;
                  const row = payload[0];
                  const legendLabel = String(row.name ?? row.payload?.name ?? '');
                  const raw = row.value;
                  const n = typeof raw === 'number' ? raw : Number(raw);
                  const total = pieData.reduce((s, x) => s + x.value, 0);
                  const pct =
                    total > 0 && !Number.isNaN(n) ? ((n / total) * 100).toFixed(1) : '—';
                  const countStr = Number.isNaN(n) ? '—' : n.toLocaleString();
                  const dim = chartMetric === 'signature' ? 'Signature' : 'Stage focus';
                  return (
                    <div
                      style={{
                        ...tooltipStyle,
                        padding: '10px 12px',
                        minWidth: 120,
                      }}
                    >
                      <div style={{ fontWeight: 600, color: '#fff', marginBottom: 6 }}>
                        {legendLabel || '—'}
                      </div>
                      <div style={{ fontSize: 12, color: '#c8c8c8' }}>{dim}</div>
                      <div style={{ fontSize: 13, color: '#eee', marginTop: 4 }}>
                        {countStr} <span style={{ color: '#888' }}>({pct}%)</span>
                      </div>
                    </div>
                  );
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

      {barData.length > 0 && (
        <div className="rechart" style={{ marginTop: 16 }}>
          <p className="muted small" style={{ marginBottom: 6 }}>
            {chartMetric === 'signature' ? 'Signature' : 'Stage focus'} (top {BAR_TOP}) — click a bar
          </p>
          <ResponsiveContainer width="100%" height={Math.max(160, barData.length * 28)}>
            <BarChart
              layout="vertical"
              data={barData}
              margin={{ top: 4, right: 52, left: 8, bottom: 4 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#2a2a2a" />
              <XAxis type="number" stroke="#888" tick={{ fill: '#aaa', fontSize: 11 }} />
              <YAxis
                type="category"
                dataKey="name"
                width={chartMetric === 'signature' ? 100 : 140}
                tick={{ fill: '#ccc', fontSize: 11 }}
                stroke="#666"
              />
              <Tooltip
                content={({ active, payload }) => {
                  if (!active || !payload?.length) return null;
                  const row = payload[0]?.payload as BarRow | undefined;
                  if (!row) return null;
                  const dim = chartMetric === 'signature' ? 'Signature' : 'Stage focus';
                  return (
                    <div style={{ ...tooltipStyle, padding: '10px 12px' }}>
                      <div style={{ fontWeight: 600, color: '#fff', marginBottom: 6 }}>{row.name}</div>
                      <div style={{ fontSize: 12, color: '#c8c8c8' }}>{dim}</div>
                      <div style={{ fontSize: 13, color: '#eee', marginTop: 4 }}>
                        Count: <strong>{row.count.toLocaleString()}</strong>
                      </div>
                    </div>
                  );
                }}
              />
              <Bar
                dataKey="count"
                radius={[0, 4, 4, 0]}
                cursor="pointer"
                onClick={(data: unknown) => {
                  const row = data as BarRow;
                  if (row?.name)
                    onPickSubset(chartMetric === 'signature' ? 'signature' : 'stage_focus', row.name, selectedStageId);
                }}
              >
                <LabelList dataKey="count" position="right" fill="#c4c4c4" fontSize={11} />
                {barData.map((_, i) => (
                  <Cell key={barData[i].name} fill={CHART_COLORS[i % CHART_COLORS.length]} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}

      {runtimeHistRows.length > 0 && (
        <div className="rechart" style={{ marginTop: 16 }}>
          <p className="muted small" style={{ marginBottom: 6 }}>
            Runtime (ms) histogram — bar height and label = count
          </p>
          <ResponsiveContainer width="100%" height={Math.max(200, Math.min(runtimeHistRows.length * 36, 320))}>
            <BarChart
              data={runtimeHistRows}
              margin={{ top: 28, right: 12, left: 4, bottom: 8 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#2a2a2a" />
              <XAxis
                dataKey="label"
                stroke="#666"
                tick={{ fill: '#aaa', fontSize: 10 }}
                interval={0}
                angle={runtimeHistRows.length > 10 ? -30 : 0}
                textAnchor={runtimeHistRows.length > 10 ? 'end' : 'middle'}
                height={runtimeHistRows.length > 10 ? 56 : 36}
              />
              <YAxis stroke="#888" tick={{ fill: '#aaa', fontSize: 11 }} allowDecimals={false} />
              <Tooltip
                content={({ active, payload }) => {
                  if (!active || !payload?.length) return null;
                  const row = payload[0]?.payload as HistBinRow;
                  if (!row) return null;
                  return (
                    <div
                      style={{
                        ...tooltipStyle,
                        padding: '10px 12px',
                      }}
                    >
                      <div style={{ fontWeight: 600, color: '#fff', marginBottom: 6 }}>
                        {Number.isFinite(row.bin_start) && Number.isFinite(row.bin_end)
                          ? `${Math.round(row.bin_start)} – ${Math.round(row.bin_end)} ms`
                          : row.label}
                      </div>
                      <div style={{ fontSize: 13, color: '#eee' }}>
                        Count: <strong>{row.count.toLocaleString()}</strong>
                      </div>
                    </div>
                  );
                }}
              />
              <Bar dataKey="count" radius={[4, 4, 0, 0]} maxBarSize={48}>
                <LabelList dataKey="count" position="top" fill="#c4c4c4" fontSize={11} />
                {runtimeHistRows.map((_, i) => (
                  <Cell key={runtimeHistRows[i].label} fill={CHART_COLORS[i % CHART_COLORS.length]} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}

      {totalKept > 0 && (
        <p className="muted small" style={{ marginTop: 8 }}>
          Pie: top {TOP_K} {chartMetric === 'signature' ? 'signatures' : 'stage_focus values'} + other (
          {totalKept.toLocaleString()} rows in slice).
        </p>
      )}
    </div>
  );
}
