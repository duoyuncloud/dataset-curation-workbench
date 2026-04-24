import { useEffect, useState } from 'react';

type Props = {
  total: number;
  limit: number;
  offset: number;
  onOffset: (next: number) => void;
};

export function PaginationBar({ total, limit, offset, onOffset }: Props) {
  const totalPages = total <= 0 ? 1 : Math.max(1, Math.ceil(total / limit));
  const currentPage = total <= 0 ? 1 : Math.min(totalPages, Math.floor(offset / limit) + 1);
  const [jump, setJump] = useState(String(currentPage));

  useEffect(() => {
    setJump(String(currentPage));
  }, [currentPage, total, offset]);

  function goPage(p: number) {
    const tp = total <= 0 ? 1 : Math.max(1, Math.ceil(total / limit));
    const n = Number.isFinite(p) && p > 0 ? Math.floor(p) : 1;
    const next = Math.max(1, Math.min(n, tp));
    onOffset((next - 1) * limit);
  }

  return (
    <div className="pager pager-extended">
      <button
        type="button"
        className="btn"
        disabled={offset === 0}
        onClick={() => onOffset(Math.max(0, offset - limit))}
      >
        Previous
      </button>
      <span className="pager-meta">
        Page {currentPage} of {totalPages} ({total.toLocaleString()} rows)
      </span>
      <label className="pager-jump">
        Go to
        <input
          type="number"
          min={1}
          max={totalPages}
          value={jump}
          onChange={(e) => setJump(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter') {
              goPage(parseInt(jump, 10));
            }
          }}
        />
      </label>
      <button type="button" className="btn small" onClick={() => goPage(parseInt(jump, 10))}>
        Go
      </button>
      <button
        type="button"
        className="btn"
        disabled={total === 0 || offset + limit >= total}
        onClick={() => onOffset(offset + limit)}
      >
        Next
      </button>
    </div>
  );
}
