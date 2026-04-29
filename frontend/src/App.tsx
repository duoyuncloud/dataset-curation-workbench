import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  createTask,
  deleteTask,
  exportUrl,
  formatSubsetFilterLabel,
  getStageView,
  getSummary,
  getTask,
  getVersion,
  listStages,
  listTasks,
  patchTask,
  emptySubset,
  subsetFilterActive,
  subsetFilterFromStageRecord,
  type Stage,
  type StageDetail,
  type SubsetFilter,
  type TaskRow,
  type VersionInfo,
  uploadJsonlStream,
  loadJsonlFromPathStream,
  truncateStagesFrom,
} from './api';
import { ChartsPanel } from './components/ChartsPanel';
import { DatasetTable } from './components/DatasetTable';
import { FilterPanel } from './components/FilterPanel';
import { PerFilterRemovalSummary } from './components/PerFilterRemovalSummary';
import { PipelineTimeline } from './components/PipelineTimeline';
import { RemovedRowsTable } from './components/RemovedRowsTable';
import { StatsPanel } from './components/StatsPanel';
import { TaskHome, filterStageCount } from './components/TaskHome';
import { UploadPanel } from './components/UploadPanel';
import './App.css';

type Screen = 'home' | 'task';

function App() {
  const [screen, setScreen] = useState<Screen>('home');
  const [tasks, setTasks] = useState<TaskRow[]>([]);
  const [tasksLoading, setTasksLoading] = useState(false);
  const [tasksError, setTasksError] = useState<string | null>(null);

  const [taskId, setTaskId] = useState<string | null>(null);
  const [taskMeta, setTaskMeta] = useState<TaskRow | null>(null);
  const [stages, setStages] = useState<Stage[]>([]);
  const [current, setCurrent] = useState(0);
  const [detail, setDetail] = useState<StageDetail | null>(null);
  const [uploadBusy, setUploadBusy] = useState(false);
  const [uploadProgress, setUploadProgress] = useState<{ pct: number; message: string } | null>(
    null
  );
  const [msg, setMsg] = useState<string | null>(null);
  const [subsetFilter, setSubsetFilter] = useState<SubsetFilter | null>(null);
  const [exploreSummary, setExploreSummary] = useState<Record<string, unknown> | null>(null);
  /** Kept-rows table can view any past stage; timeline `current` resets this to stay aligned. */
  const [keptViewStageId, setKeptViewStageId] = useState(0);
  /** Increment when JSONL is re-uploaded so table/charts/removal views remount and refetch. */
  const [datasetVersion, setDatasetVersion] = useState(0);
  const [truncatingFromStageId, setTruncatingFromStageId] = useState<number | null>(null);
  const [apiVersion, setApiVersion] = useState<VersionInfo | null>(null);
  /** Latest in-flight `listTasks`; new `loadTasks` aborts the previous fetch. */
  const listTasksAbortRef = useRef<AbortController | null>(null);
  /** Overlapping `loadTasks` calls (e.g. Strict Mode + refresh); loading clears when all finish. */
  const tasksListInFlight = useRef(0);

  const loadTasks = useCallback(async () => {
    listTasksAbortRef.current?.abort();
    const ctrl = new AbortController();
    listTasksAbortRef.current = ctrl;
    tasksListInFlight.current += 1;
    setTasksLoading(true);
    setTasksError(null);
    try {
      const list = await listTasks(ctrl.signal);
      if (listTasksAbortRef.current === ctrl) {
        setTasks(list);
      }
    } catch (e) {
      if (e instanceof DOMException && e.name === 'AbortError') return;
      if (e instanceof Error && e.name === 'AbortError') return;
      if (listTasksAbortRef.current === ctrl) {
        setTasksError(e instanceof Error ? e.message : 'Failed to load tasks');
      }
    } finally {
      tasksListInFlight.current = Math.max(0, tasksListInFlight.current - 1);
      if (tasksListInFlight.current === 0) {
        setTasksLoading(false);
      }
      if (listTasksAbortRef.current === ctrl) {
        listTasksAbortRef.current = null;
      }
    }
  }, []);

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
    if (screen !== 'home') {
      listTasksAbortRef.current?.abort();
      listTasksAbortRef.current = null;
      tasksListInFlight.current = 0;
      setTasksLoading(false);
      return;
    }
    void loadTasks();
  }, [screen, loadTasks]);

  const hasStages = stages.length > 0;

  /** Largest stage_id present (timeline / filters assume ids 0…N). */
  const maxStageIndex = useMemo(() => {
    if (!hasStages || stages.length === 0) return -1;
    const ids = stages
      .map((s) => Number(s.stage_id))
      .filter((n) => Number.isFinite(n));
    if (ids.length === 0) return -1;
    return Math.max(...ids);
  }, [hasStages, stages]);

  /** Clamp kept-table stage to an existing stage_id (avoids one frame of /rows on a deleted id). */
  const tableViewStage = useMemo(() => {
    if (maxStageIndex < 0) return 0;
    const v = Math.floor(Number(keptViewStageId));
    const n = Number.isFinite(v) ? v : 0;
    return Math.min(Math.max(0, n), maxStageIndex);
  }, [maxStageIndex, keptViewStageId]);

  useEffect(() => {
    setKeptViewStageId(current);
  }, [current]);

  useEffect(() => {
    if (screen !== 'task' || !taskId || !hasStages) {
      setDetail(null);
      return;
    }
    void getSummary(taskId, current)
      .then(setDetail)
      .catch(() => setDetail(null));
  }, [screen, taskId, current, hasStages]);

  useEffect(() => {
    if (screen !== 'task' || !taskId || !subsetFilterActive(subsetFilter) || !hasStages) {
      setExploreSummary(null);
      return;
    }
    let ok = true;
    void getStageView(taskId, tableViewStage, subsetFilter!, 1, 0)
      .then((r) => {
        if (ok) setExploreSummary(r.summary_stats);
      })
      .catch(() => {
        if (ok) setExploreSummary(null);
      });
    return () => {
      ok = false;
    };
  }, [screen, taskId, tableViewStage, subsetFilter, hasStages]);

  const exportSubsetFilter = useMemo((): SubsetFilter | null => {
    if (subsetFilterActive(subsetFilter)) {
      return subsetFilter;
    }
    const raw = stages.find((s) => s.stage_id === current)?.view_filter;
    return subsetFilterFromStageRecord(
      raw as Record<string, unknown> | null | undefined
    );
  }, [subsetFilter, stages, current]);

  const canExportSubset = subsetFilterActive(exportSubsetFilter);

  function goHome() {
    setScreen('home');
    setTaskId(null);
    setTaskMeta(null);
    setStages([]);
    setCurrent(0);
    setKeptViewStageId(0);
    setDetail(null);
    setSubsetFilter(null);
    setExploreSummary(null);
    setDatasetVersion(0);
    setMsg(null);
    void loadTasks();
  }

  async function openTaskWorkspace(id: string) {
    setMsg(null);
    try {
      const [meta, stageList] = await Promise.all([getTask(id), listStages(id)]);
      setTaskId(id);
      setTaskMeta(meta);
      setStages(stageList);
      setSubsetFilter(null);
      setDatasetVersion(0);
      if (stageList.length === 0) {
        setCurrent(0);
        setKeptViewStageId(0);
      } else {
        let idx = meta.current_stage_id ?? 0;
        if (idx < 0 || idx >= stageList.length) idx = stageList.length - 1;
        setCurrent(idx);
        setKeptViewStageId(idx);
      }
      setScreen('task');
    } catch (e) {
      setMsg(e instanceof Error ? e.message : 'Failed to open task');
    }
  }

  async function handleNewTaskFromHome() {
    const name = window.prompt('Task name', 'Untitled task');
    if (name === null) return;
    const trimmed = name.trim() || 'Untitled task';
    try {
      const t = await createTask(trimmed);
      setTaskId(t.task_id);
      setTaskMeta(t);
      setStages([]);
      setCurrent(0);
      setKeptViewStageId(0);
      setSubsetFilter(null);
      setDatasetVersion(0);
      setScreen('task');
      if (t.task_name !== trimmed) {
        setMsg(`Saved as “${t.task_name}” (another task already used that name).`);
      } else {
        setMsg(null);
      }
    } catch (e) {
      setMsg(e instanceof Error ? e.message : 'Could not create task');
    }
  }

  async function handleRenameTask(task: TaskRow) {
    const name = window.prompt('Rename task', task.task_name);
    if (name === null) return;
    const trimmed = name.trim() || task.task_name;
    try {
      const updated = await patchTask(task.task_id, trimmed);
      if (screen === 'task' && taskId === task.task_id) {
        setTaskMeta(updated);
      }
      await loadTasks();
      if (updated.task_name !== trimmed) {
        setMsg(`Renamed to “${updated.task_name}” (that name was already taken).`);
      } else {
        setMsg(null);
      }
    } catch (e) {
      setMsg(e instanceof Error ? e.message : 'Rename failed');
    }
  }

  async function handleDeleteTask(task: TaskRow) {
    if (
      !window.confirm(
        `Delete task “${task.task_name}”? All files and stages for this task will be removed. This cannot be undone.`
      )
    ) {
      return;
    }
    try {
      await deleteTask(task.task_id);
      if (screen === 'task' && taskId === task.task_id) {
        goHome();
      } else {
        await loadTasks();
      }
      setMsg(null);
    } catch (e) {
      setMsg(e instanceof Error ? e.message : 'Delete failed');
    }
  }

  function renameFromWorkspace() {
    if (!taskMeta) return;
    void handleRenameTask(taskMeta);
  }

  if (screen === 'home') {
    return (
      <div className="app">
        <TaskHome
          tasks={tasks}
          loading={tasksLoading}
          error={tasksError}
          onRefresh={() => void loadTasks()}
          onNewTask={() => void handleNewTaskFromHome()}
          onOpen={(id) => void openTaskWorkspace(id)}
          onRename={(t) => void handleRenameTask(t)}
          onDelete={(t) => void handleDeleteTask(t)}
        />
        <footer className="app-footer">
          <p className="muted small">dataset-curation-workbench</p>
        </footer>
      </div>
    );
  }

  return (
    <div className="app">
      <div className="task-workspace-top">
        <div className="task-workspace-title">
          <div className="task-back">
            <button type="button" className="btn small" onClick={goHome}>
              ← Back to home
            </button>
          </div>
          <div className="task-title-row">
            <h1>{taskMeta?.task_name ?? 'Task'}</h1>
          </div>
          <p className="muted small" style={{ margin: 0 }}>
            {taskMeta?.dataset_name
              ? `Dataset: ${taskMeta.dataset_name} · ${taskMeta.total_rows?.toLocaleString?.() ?? taskMeta.total_rows} rows · ${filterStageCount(taskMeta.num_stages)} filter stages`
              : hasStages
                ? `${taskMeta?.total_rows?.toLocaleString?.() ?? 0} rows · ${filterStageCount(taskMeta?.num_stages ?? stages.length)} filter stages`
                : 'No dataset uploaded yet.'}
          </p>
          <div className="task-workspace-actions">
            <button type="button" className="linkish" onClick={renameFromWorkspace}>
              Rename task
            </button>
            {taskId ? (
              <UploadPanel
                compact
                busy={uploadBusy}
                uploadProgress={uploadProgress}
                onPathLoad={async (serverPath) => {
                  setMsg(null);
                  setSubsetFilter(null);
                  if (hasStages) {
                    if (
                      !window.confirm(
                        'Replace the current dataset? All stages will be reset and replaced with a new raw stage from this file.'
                      )
                    ) {
                      return;
                    }
                  }
                  setUploadBusy(true);
                  setUploadProgress({ pct: 0, message: 'Connecting…' });
                  try {
                    const r = await loadJsonlFromPathStream(taskId, serverPath, (pct, message) => {
                      setUploadProgress({ pct, message });
                    });
                    const s = await listStages(taskId);
                    const meta = await getTask(taskId);
                    setStages(s);
                    setTaskMeta(meta);
                    setCurrent(0);
                    setKeptViewStageId(0);
                    setDatasetVersion((v) => v + 1);
                    setMsg(`Loaded ${r.stage0_count} rows from server path.`);
                  } catch (e) {
                    setMsg(e instanceof Error ? e.message : 'Load from path failed');
                  } finally {
                    setUploadBusy(false);
                    setUploadProgress(null);
                  }
                }}
                onFile={async (file) => {
                  setMsg(null);
                  setSubsetFilter(null);
                  if (hasStages) {
                    if (
                      !window.confirm(
                        'Replace the current dataset? All stages will be reset and replaced with a new raw stage from this file.'
                      )
                    ) {
                      return;
                    }
                  }
                  setUploadBusy(true);
                  setUploadProgress({ pct: 0, message: 'Uploading…' });
                  try {
                    const r = await uploadJsonlStream(taskId, file, (pct, message) => {
                      setUploadProgress({ pct, message });
                    });
                    const s = await listStages(taskId);
                    const meta = await getTask(taskId);
                    setStages(s);
                    setTaskMeta(meta);
                    setCurrent(0);
                    setKeptViewStageId(0);
                    setDatasetVersion((v) => v + 1);
                    setMsg(`Loaded ${r.stage0_count} rows.`);
                  } catch (e) {
                    setMsg(e instanceof Error ? e.message : 'Upload failed');
                  } finally {
                    setUploadBusy(false);
                    setUploadProgress(null);
                  }
                }}
              />
            ) : null}
          </div>
        </div>
        {taskId && hasStages && (
          <div className="export-btns">
            <span className="export-label">Export</span>
            <a
              className="btn"
              href={exportUrl(taskId, current, 'jsonl')}
              target="_blank"
              rel="noreferrer"
              title="All kept rows at this stage (including passthrough rows when a filter used a subset)"
            >
              JSONL (full)
            </a>
            <a
              className={`btn ${canExportSubset ? '' : 'btn-dim'}`}
              href={
                canExportSubset && exportSubsetFilter
                  ? exportUrl(taskId, current, 'jsonl', {
                      subsetOnly: true,
                      subsetFilter: exportSubsetFilter,
                    })
                  : '#'
              }
              target="_blank"
              rel="noreferrer"
              title="Kept rows matching the active signature / stage_focus subset (table or stage metadata)"
              onClick={(e) => {
                if (!canExportSubset) e.preventDefault();
              }}
            >
              JSONL (subset)
            </a>
            <a
              className="btn"
              href={exportUrl(taskId, current, 'csv')}
              target="_blank"
              rel="noreferrer"
            >
              CSV (full)
            </a>
            <a
              className={`btn ${canExportSubset ? '' : 'btn-dim'}`}
              href={
                canExportSubset && exportSubsetFilter
                  ? exportUrl(taskId, current, 'csv', {
                      subsetOnly: true,
                      subsetFilter: exportSubsetFilter,
                    })
                  : '#'
              }
              target="_blank"
              rel="noreferrer"
              title="Kept rows matching the active signature / stage_focus subset"
              onClick={(e) => {
                if (!canExportSubset) e.preventDefault();
              }}
            >
              CSV (subset)
            </a>
            <a
              className="btn"
              href={exportUrl(taskId, current, 'filter_log')}
              target="_blank"
              rel="noreferrer"
            >
              filter_log.json
            </a>
          </div>
        )}
      </div>

      {msg && (
        <div className="banner">
          {msg}
          <button type="button" className="linkish" onClick={() => setMsg(null)}>
            Dismiss
          </button>
        </div>
      )}

      {subsetFilterActive(subsetFilter) && subsetFilter && (
        <div className="sub-banner" role="status">
          <span>
            Active subset: <strong>{formatSubsetFilterLabel(subsetFilter)}</strong>
          </span>
          <button type="button" className="btn small" onClick={() => setSubsetFilter(null)}>
            Clear subset
          </button>
        </div>
      )}

      {hasStages ? (
        <PipelineTimeline
          stages={stages}
          current={current}
          truncatingFromStageId={truncatingFromStageId}
          onSelect={(id) => {
            setCurrent(id);
            setSubsetFilter(null);
          }}
          onTruncateFrom={async (fromStageId) => {
            if (!taskId) return;
            if (
              !window.confirm(
                `Remove stage ${fromStageId} and all later stages? Kept data falls back to stage ${fromStageId - 1}. This cannot be undone.`
              )
            ) {
              return;
            }
            setTruncatingFromStageId(fromStageId);
            setMsg(null);
            try {
              const r = await truncateStagesFrom(taskId, fromStageId);
              const s = await listStages(taskId);
              setStages(s);
              setCurrent(r.current_stage_id);
              setKeptViewStageId(r.current_stage_id);
              setSubsetFilter(null);
              setExploreSummary(null);
              setDatasetVersion((v) => v + 1);
              try {
                setTaskMeta(await getTask(taskId));
              } catch {
                /* ignore */
              }
              setMsg(
                `Removed stages from ${fromStageId} onward. Now at stage ${r.current_stage_id} (${r.total_rows.toLocaleString()} rows).`
              );
            } catch (e) {
              setMsg(e instanceof Error ? e.message : 'Failed to remove stages');
            } finally {
              setTruncatingFromStageId(null);
            }
          }}
        />
      ) : (
        <div className="timeline-wrap">
          <h2 className="timeline-h2">Stages</h2>
          <p className="muted small timeline-hint">
            Use <strong>Upload JSONL</strong> (above) to create stage 0. Filters run after that.
          </p>
        </div>
      )}

      <div className="grid3">
        <aside className="side">
          <div className="card workspace">
            <h2>Workspace</h2>
            <div className="ws-divider" />
            <FilterPanel
              key={`fp-${taskId}-${datasetVersion}`}
              embedded
              taskId={taskId}
              maxStage={maxStageIndex}
              activeStage={current}
              subsetFilter={subsetFilter}
              onError={setMsg}
              onApplied={() => {
                if (!taskId) return;
                setSubsetFilter(null);
                void listStages(taskId)
                  .then(async (s) => {
                    setStages(s);
                    setCurrent(s.length - 1);
                    try {
                      setTaskMeta(await getTask(taskId));
                    } catch {
                      /* ignore */
                    }
                    setMsg('New stage created (batch or single).');
                  })
                  .catch(() => setMsg('Failed to load stages'));
              }}
            />
          </div>
        </aside>

        <main className="main">
          <DatasetTable
            key={`dt-${taskId}-${datasetVersion}`}
            taskId={taskId}
            hasStages={hasStages}
            stages={stages}
            viewStageId={tableViewStage}
            onViewStageIdChange={setKeptViewStageId}
            subsetFilter={subsetFilter}
            onSubsetFilter={setSubsetFilter}
          />
        </main>

        <aside className="right">
          <StatsPanel
            detail={detail}
            exploreSummary={exploreSummary}
            subsetFilter={subsetFilter}
          />
          <ChartsPanel
            key={`ch-${taskId}-${datasetVersion}`}
            taskId={taskId}
            hasStages={hasStages}
            currentStageId={current}
            stageCount={stages.length}
            subsetFilter={subsetFilter}
            onPickSubset={(dimension, value, sourceStageId) => {
              setCurrent(sourceStageId);
              setSubsetFilter((prev) => {
                const p = prev ?? emptySubset();
                if (dimension === 'signature') {
                  return {
                    signatures: [String(value)],
                    stageFocus: [...p.stageFocus],
                  };
                }
                return {
                  signatures: [...p.signatures],
                  stageFocus: [String(value)],
                };
              });
              setMsg('Exploration subset (no new stage). Clear or apply filters when done.');
            }}
          />
        </aside>
      </div>

      <section className="bottom">
        <PerFilterRemovalSummary detail={detail} />
        <RemovedRowsTable
          key={`rr-${taskId}-${datasetVersion}`}
          taskId={taskId}
          hasStages={hasStages}
          stageId={current}
          stages={stages}
        />
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
