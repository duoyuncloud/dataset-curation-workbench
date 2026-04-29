type Props = {
  /** 0–1 when determinate; omit with indeterminate */
  value?: number;
  indeterminate?: boolean;
  label?: string;
};

export function LinearProgress({ value, indeterminate, label }: Props) {
  const det = !indeterminate && typeof value === 'number';
  const pct = det ? Math.round(Math.min(100, Math.max(0, value * 100))) : null;
  const ariaText =
    label && pct !== null ? `${label} ${pct}%` : label ?? (pct !== null ? `${pct}%` : undefined);
  return (
    <div
      className="linear-progress"
      role="progressbar"
      aria-busy={indeterminate || undefined}
      aria-valuenow={det ? pct ?? undefined : undefined}
      aria-valuetext={ariaText}
    >
      {label || pct !== null ? (
        <div className="linear-progress-label">
          {label ? <span>{label}</span> : null}
          {pct !== null ? (
            <span className="linear-progress-pct">{pct}%</span>
          ) : null}
        </div>
      ) : null}
      <div className="linear-progress-track">
        {det ? (
          <div className="linear-progress-fill" style={{ width: `${pct}%` }} />
        ) : (
          <div className="linear-progress-indeterminate" />
        )}
      </div>
    </div>
  );
}
