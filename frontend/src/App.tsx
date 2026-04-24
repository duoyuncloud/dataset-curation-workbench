import { useEffect, useMemo, useState } from 'react';
import {
  exportUrl,
  formatViewFilterLabel,
  getStageView,
  getSummary,
  getVersion,
  listStages,
  type Stage,
  type StageDetail,
  type VersionInfo,
  type ViewFilter,
  uploadJsonl,
  viewFilterFromRecord,
} from './api';
import { ChartsPanel } from './components/ChartsPanel';
import { DatasetTable } from './components/DatasetTable';
import { FilterPanel } from './components/FilterPanel';
import { PerFilterRemovalSummary } from './components/PerFilterRemovalSummary';
import { PipelineTimeline } from './components/PipelineTimeline';
import { RemovedRowsTable } from './components/RemovedRowsTable';
import { StatsPanel } from './components/StatsPanel';
import { UploadPanel } from './components/UploadPanel';
import './App.css';

function App() {
  const [datasetId, setDatasetId] = useState<string | null>(null);
  const [stages, setStages] = useState<Stage[]>([]);
  const [current, setCurrent] = useState(0);
  const [detail, setDetail] = useState<StageDetail | null>(null);
  const [uploadBusy, setUploadBusy] = useState(false);
  const [msg, setMsg] = useState<string | null>(null);
  const [viewFilter, setViewFilter] = useState<ViewFilter | null>(null);
  const [exploreSummary, setExploreSummary] = useState<Record<string, unknown> | null>(null);
  const [apiVersion, setApiVersion] = useState<VersionInfo | null>(null);

  useEffect(() => {
    let o = true;
    getVersion()
      .then((v) => {
        if (o) setApiVersion(v);
      })
      .catch(() => {
        if (o) setApiVersion(null);
      });
    return () => {
      o = false;
    };
  }, []);

  useEffect(() => {
    if (!datasetId) {
      setDetail(null);
      return;
    }
    void getSummary(datasetId, current)
      .then(setDetail)
      .catch(() => setDetail(null));
  }, [datasetId, current]);

  useEffect(() => {
    if (!datasetId || !viewFilter) {
      setExploreSummary(null);
      return;
    }
    let ok = true;
    void getStageView(datasetId, current, viewFilter, 1, 0)
      .then((r) => {
        if (ok) setExploreSummary(r.summary_stats);
      })
      .catch(() => {
        if (ok) setExploreSummary(null);
      });
    return () => {
      ok = false;
    };
  }, [datasetId, current, viewFilter]);

  const maxStage = Math.max(0, stages.length - 1);

  /** Table exploration wins; else use signature scope stored on the current stage (after batch on a subset). */
  const signatureExportFilter = useMemo((): ViewFilter | null => {
    if (viewFilter?.field === 'signature' && viewFilter.values.length > 0) {
      return viewFilter;
    }
    const raw = stages.find((s) => s.stage_id === current)?.view_filter;
    const fromStage = viewFilterFromRecord(
      raw as Record<string, unknown> | null | undefined
    );
    if (fromStage?.field === 'signature' && fromStage.values.length > 0) {
      return fromStage;
    }
    return null;
  }, [viewFilter, stages, current]);

  const canExportSignature = signatureExportFilter != null;

  return (
    <div className="app">
      <header className="header">
        <div>
          <h1>SFT Dataset Curation</h1>
          <p className="tagline">
            Transparent, step-by-step curation. Batches, exploration, and auditable stages.
          </p>
        </div>
        {datasetId && (
          <div className="export-btns">
            <span className="export-label">Export</span>
            <a
              className="btn"
              href={exportUrl(datasetId, current, 'jsonl', { scope: 'full', viewFilter: null })}
              target="_blank"
              rel="noreferrer"
              title="All kept rows at this stage (including passthrough rows when a filter used a subset)"
            >
              JSONL (full)
            </a>
            <a
              className={`btn ${canExportSignature ? '' : 'btn-dim'}`}
              href={
                canExportSignature && signatureExportFilter
                  ? exportUrl(datasetId, current, 'jsonl', {
                      scope: 'signature',
                      viewFilter: signatureExportFilter,
                    })
                  : '#'
              }
              target="_blank"
              rel="noreferrer"
              title="Kept rows matching the signature scope: table filter if any, else the subset this stage was built from"
              onClick={(e) => {
                if (!canExportSignature) e.preventDefault();
              }}
            >
              JSONL (signature)
            </a>
            <a
              className="btn"
              href={exportUrl(datasetId, current, 'csv', { scope: 'full', viewFilter: null })}
              target="_blank"
              rel="noreferrer"
            >
              CSV (full)
            </a>
            <a
              className={`btn ${canExportSignature ? '' : 'btn-dim'}`}
              href={
                canExportSignature && signatureExportFilter
                  ? exportUrl(datasetId, current, 'csv', {
                      scope: 'signature',
                      viewFilter: signatureExportFilter,
                    })
                  : '#'
              }
              target="_blank"
              rel="noreferrer"
              title="Kept rows matching the signature scope: table filter if any, else the subset this stage was built from"
              onClick={(e) => {
                if (!canExportSignature) e.preventDefault();
              }}
            >
              CSV (signature)
            </a>
            <a
              className="btn"
              href={exportUrl(datasetId, current, 'filter_log')}
              target="_blank"
              rel="noreferrer"
            >
              filter_log.json
            </a>
          </div>
        )}
      </header>

      {msg && (
        <div className="banner">
          {msg}
          <button type="button" className="linkish" onClick={() => setMsg(null)}>
            Dismiss
          </button>
        </div>
      )}

      {viewFilter && (
        <div className="sub-banner" role="status">
          <span>
            Viewing subset: <strong>{formatViewFilterLabel(viewFilter)}</strong>
          </span>
          <button
            type="button"
            className="btn small"
            onClick={() => setViewFilter(null)}
          >
            Clear view filter
          </button>
        </div>
      )}

      <PipelineTimeline
        stages={stages}
        current={current}
        onSelect={(id) => {
          setCurrent(id);
          setViewFilter(null);
        }}
      />

      <div className="grid3">
        <aside className="side">
          <div className="card workspace">
            <h2>Workspace</h2>
            <UploadPanel
              embedded
              busy={uploadBusy}
              onFile={async (file) => {
                setMsg(null);
                setViewFilter(null);
                setUploadBusy(true);
                try {
                  const r = await uploadJsonl(file);
                  setDatasetId(r.dataset_id);
                  setStages([
                    {
                      stage_id: 0,
                      stage_name: 'Raw',
                      filter_type: 'raw',
                      filter_config: {},
                      input_count: r.stage0_count,
                      output_count: r.stage0_count,
                      removed_count: 0,
                    },
                  ]);
                  setCurrent(0);
                  setMsg(`Loaded ${r.stage0_count} rows.`);
                } catch (e) {
                  setMsg(e instanceof Error ? e.message : 'Upload failed');
                } finally {
                  setUploadBusy(false);
                }
              }}
            />
            <div className="ws-divider" />
            <FilterPanel
              embedded
              datasetId={datasetId}
              maxStage={maxStage}
              activeStage={current}
              viewFilter={viewFilter}
              onError={setMsg}
              onApplied={() => {
                if (!datasetId) return;
                setViewFilter(null);
                void listStages(datasetId)
                  .then((s) => {
                    setStages(s);
                    setCurrent(s.length - 1);
                    setMsg('New stage created (batch or single).');
                  })
                  .catch(() => setMsg('Failed to load stages'));
              }}
            />
          </div>
        </aside>

        <main className="main">
          <DatasetTable
            datasetId={datasetId}
            stageId={current}
            viewFilter={viewFilter}
            onViewFilter={setViewFilter}
          />
        </main>

        <aside className="right">
          <StatsPanel
            detail={detail}
            exploreSummary={exploreSummary}
            viewFilter={viewFilter}
          />
          <ChartsPanel
            datasetId={datasetId}
            currentStageId={current}
            stageCount={stages.length}
            viewFilter={viewFilter}
            onDrill={(field, value, sourceStageId) => {
              // Table + signature export use `current`; if the pie was for another stage, align first.
              setCurrent(sourceStageId);
              setViewFilter({ field, values: [String(value)] });
              setMsg('Exploration subset (no new stage). Clear or apply filters when done.');
            }}
          />
        </aside>
      </div>

      <section className="bottom">
        <PerFilterRemovalSummary detail={detail} />
        <RemovedRowsTable datasetId={datasetId} stageId={current} stages={stages} />
      </section>

      <footer className="app-footer">
        {apiVersion && (
          <p className="muted small">
            API v{apiVersion.version}
            {apiVersion.build_time && apiVersion.build_time !== 'unknown'
              ? ` · ${apiVersion.build_time}`
              : ''}
          </p>
        )}
      </footer>
    </div>
  );
}

export default App;
