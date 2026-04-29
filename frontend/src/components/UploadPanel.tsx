import { useState } from 'react';
import { LinearProgress } from './LinearProgress';

type Props = {
  onFile: (f: File) => void;
  busy: boolean;
  /** When true, no outer card (use inside a parent “Workspace” card). */
  embedded?: boolean;
  /** Header row: single compact button, no extra copy */
  compact?: boolean;
  /** Server-reported import progress (0–1) + status line */
  uploadProgress?: { pct: number; message: string } | null;
  /**
   * Load JSONL from a path on the API server host (parallel to choosing a file).
   * Relative paths resolve under the server’s DATA_DIR unless ALLOW_JSONL_IMPORT_ANYWHERE is set.
   */
  onPathLoad?: (serverPath: string) => void | Promise<void>;
};

export function UploadPanel({
  onFile,
  busy,
  embedded,
  compact,
  uploadProgress = null,
  onPathLoad,
}: Props) {
  const [pathDraft, setPathDraft] = useState('');
  const input = (
    <input
      type="file"
      accept=".jsonl,application/x-ndjson,application/json,.json"
      disabled={busy}
      onChange={(e) => {
        const f = e.target.files?.[0];
        if (f) onFile(f);
        e.target.value = '';
      }}
    />
  );

  if (compact) {
    const busyLabel = busy ? 'Loading dataset…' : 'Upload JSONL';
    return (
      <div className="upload-toolbar-wrap">
        <div className="upload-toolbar-row">
          <label className="upload-toolbar upload-toolbar-header">
            {input}
            <span className="btn primary upload-toolbar-btn" aria-busy={busy}>
              {busyLabel}
            </span>
          </label>
          {onPathLoad ? (
            <div className="upload-path-inline">
              <input
                type="text"
                className="upload-path-input"
                placeholder="Server JSONL path…"
                title="Path on the machine running the API. Relative paths are under DATA_DIR."
                value={pathDraft}
                onChange={(e) => setPathDraft(e.target.value)}
                disabled={busy}
                onKeyDown={(e) => {
                  const p = pathDraft.trim();
                  if (e.key === 'Enter' && p && !busy) void onPathLoad(p);
                }}
              />
              <button
                type="button"
                className="btn small upload-path-btn"
                disabled={busy || !pathDraft.trim()}
                onClick={() => void onPathLoad(pathDraft.trim())}
              >
                Load path
              </button>
            </div>
          ) : null}
        </div>
        {busy ? (
          <div className="upload-progress-slot upload-progress-slot-wide">
            {uploadProgress ? (
              <LinearProgress value={uploadProgress.pct} label={uploadProgress.message} />
            ) : (
              <LinearProgress indeterminate label="Starting…" />
            )}
          </div>
        ) : null}
      </div>
    );
  }

  const inner = (
    <>
      {!embedded && <h2>Upload</h2>}
      {embedded && <h3 className="ws-h3">Upload</h3>}
      <p className="muted">Distilled SFT JSONL (one JSON per line, e.g. question + response).</p>
      <label className="upload-label">
        {input}
        <span className="btn primary">{busy ? 'Uploading…' : 'Choose JSONL'}</span>
      </label>
      {onPathLoad ? (
        <div className="upload-path-block">
          <label className="upload-path-block-label">
            <span className="muted small">Or load from server path</span>
            <div className="upload-path-inline upload-path-inline-wide">
              <input
                type="text"
                className="upload-path-input"
                placeholder="e.g. tasks/my-id/raw/input.jsonl"
                title="Path on the API server; relative paths use DATA_DIR."
                value={pathDraft}
                onChange={(e) => setPathDraft(e.target.value)}
                disabled={busy}
                onKeyDown={(e) => {
                  const p = pathDraft.trim();
                  if (e.key === 'Enter' && p && !busy) void onPathLoad(p);
                }}
              />
              <button
                type="button"
                className="btn small"
                disabled={busy || !pathDraft.trim()}
                onClick={() => void onPathLoad(pathDraft.trim())}
              >
                Load path
              </button>
            </div>
          </label>
        </div>
      ) : null}
    </>
  );
  if (embedded) {
    return <div className="ws-section">{inner}</div>;
  }
  return <div className="card">{inner}</div>;
}
