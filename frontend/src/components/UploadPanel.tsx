type Props = {
  onFile: (f: File) => void;
  busy: boolean;
  /** When true, no outer card (use inside a parent “Workspace” card). */
  embedded?: boolean;
};

export function UploadPanel({ onFile, busy, embedded }: Props) {
  const inner = (
    <>
      {!embedded && <h2>Upload</h2>}
      {embedded && <h3 className="ws-h3">Upload</h3>}
      <p className="muted">Distilled SFT JSONL (one JSON per line, e.g. question + response).</p>
      <label className="upload-label">
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
        <span className="btn primary">{busy ? 'Uploading…' : 'Choose JSONL'}</span>
      </label>
    </>
  );
  if (embedded) {
    return <div className="ws-section">{inner}</div>;
  }
  return <div className="card">{inner}</div>;
}
