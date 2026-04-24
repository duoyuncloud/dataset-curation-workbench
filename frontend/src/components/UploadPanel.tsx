type Props = {
  onFile: (f: File) => void;
  busy: boolean;
  /** When true, no outer card (use inside a parent “Workspace” card). */
  embedded?: boolean;
  /** Header row: single compact button, no extra copy */
  compact?: boolean;
};

export function UploadPanel({ onFile, busy, embedded, compact }: Props) {
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
    return (
      <label className="upload-toolbar upload-toolbar-header">
        {input}
        <span className="btn primary upload-toolbar-btn" aria-busy={busy}>
          {busy ? 'Uploading…' : 'Upload JSONL'}
        </span>
      </label>
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
    </>
  );
  if (embedded) {
    return <div className="ws-section">{inner}</div>;
  }
  return <div className="card">{inner}</div>;
}
