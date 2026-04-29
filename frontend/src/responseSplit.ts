/**
 * Split model response into chain-of-thought vs final answer (e.g. CUDA kernel).
 * Order (first match wins):
 * 1) `<think>…</think>` — inner = thinking, after `</…>` = answer
 *    (text before the opening tag is prepended to answer).
 * 2) Other paired `<thinking>`-style tags — collected inner text = thinking; remainder = answer.
 * 3) Last ``` fence whose info/body looks like CUDA/C++ / kernel code — before fence = thinking,
 *    from fence onward = answer.
 * 4) First heading like ## Implementation / ## CUDA / ## Solution — before = thinking, from heading = answer.
 * 5) Else all text is answer, thinking empty.
 */

const THINKING_TAGS = [
  'redacted_thinking',
  'redacted_reasoning',
  'thinking',
  'thought',
  'analysis',
  'reasoning',
  'tool_call',
  'scratchpad',
] as const;

export type SplitResponse = {
  thinking: string;
  answer: string;
};

/**
 * Megatron-style `<think>…</think>` — use index-based slice so
 * huge inner bodies (with ``` fences inside) don’t confuse non-greedy regex.
 */
function extractRedactedThinkingBlock(full: string): SplitResponse | null {
  const openRe = /<redacted_thinking(?:\s[^>]*)?>/i;
  const om = openRe.exec(full);
  if (!om) return null;
  const startInner = om.index + om[0].length;
  const lower = full.toLowerCase();
  const closeLit = '</think>';
  const ci = lower.indexOf(closeLit, startInner);
  if (ci === -1) return null;
  const thinking = full.slice(startInner, ci).trim();
  const closeMatch = full.slice(ci).match(/^<\/redacted_thinking>/i);
  const closeLen = closeMatch ? closeMatch[0].length : closeLit.length;
  let answer = full.slice(ci + closeLen).trim();
  const prefix = full.slice(0, om.index).trim();
  if (prefix) answer = prefix + (answer ? `\n\n${answer}` : '');
  return { thinking, answer };
}

/** Remove any leftover thinking-tag markup from preview strings (table cells). */
export function stripThinkingTagLiterals(s: string): string {
  let out = s;
  for (const tag of THINKING_TAGS) {
    const pair = new RegExp(`<${tag}(?:\\s[^>]*)>[\\s\\S]*?</${tag}>`, 'gi');
    out = out.replace(pair, '');
    out = out.replace(new RegExp(`</${tag}>`, 'gi'), '');
    out = out.replace(new RegExp(`<${tag}(?:\\s[^>]*)>`, 'gi'), '');
  }
  return out.replace(/\n{3,}/g, '\n\n').trim();
}

/**
 * Table **Thinking** column only: strip tag literals plus leaked `redacted_thinking` text.
 * Do **not** use in View full — keep modal rendering faithful to split content.
 */
export function stripTableThinkingPreview(s: string): string {
  let out = stripThinkingTagLiterals(s);
  out = out.replace(/<\/?redacted_thinking>/gi, '');
  out = out.replace(/\bredacted_thinking\b/gi, '');
  out = out.replace(/\n{3,}/g, '\n\n').trim();
  return out;
}

/**
 * View full only: strip **leading** thinking-style open/close tags left on a split half.
 * Does not remove tags inside the body (unlike `stripThinkingTagLiterals`).
 */
export function stripSplitBoundaryTagsForView(s: string): string {
  let out = s.trim();
  for (let guard = 0; guard < 64; guard++) {
    let hit = false;
    for (const tag of THINKING_TAGS) {
      const open = new RegExp(`^<${tag}(?:\\s[^>]*)?>\\s*`, 'i');
      const close = new RegExp(`^</${tag}>\\s*`, 'i');
      if (open.test(out)) {
        out = out.replace(open, '').trim();
        hit = true;
        break;
      }
      if (close.test(out)) {
        out = out.replace(close, '').trim();
        hit = true;
        break;
      }
    }
    if (!hit) break;
  }
  return out;
}

function extractTaggedThinking(raw: string): { thinking: string; remainder: string } {
  let s = raw;
  const parts: string[] = [];
  for (const tag of THINKING_TAGS) {
    const re = new RegExp(`<${tag}(?:\\s[^>]*)?>([\\s\\S]*?)</${tag}>`, 'gi');
    s = s.replace(re, (_, inner: string) => {
      parts.push(inner.trim());
      return '\n\n';
    });
  }
  const remainder = s.replace(/\n{3,}/g, '\n\n').trim();
  const thinking = parts.filter(Boolean).join('\n\n---\n\n');
  return { thinking, remainder };
}

function infoLooksLikeAnswerLang(info: string): boolean {
  const head = info.trim().split(/\s+/)[0]?.toLowerCase() ?? '';
  return /^(cuda|cu|cpp|c\+\+|cxx|c)\b/.test(head);
}

function fenceLooksLikeKernel(body: string): boolean {
  const b = body.slice(0, 12000);
  return /\b(__global__|__device__|extern\s+"C"|launch_kernel|blockDim|threadIdx)\b/.test(b);
}

/** Line-based ``` info / body / ``` — robust for blank lines inside fences */
function listFences(full: string): Array<{ start: number; end: number; info: string; body: string }> {
  const lines = full.split('\n');
  const blocks: Array<{ start: number; end: number; info: string; body: string }> = [];

  function lineStartOffset(idx: number): number {
    let o = 0;
    for (let j = 0; j < idx; j++) o += lines[j].length + 1;
    return o;
  }

  let i = 0;
  while (i < lines.length) {
    const line = lines[i];
    if (!line.startsWith('```')) {
      i++;
      continue;
    }
    const info = line.slice(3).trim();
    const start = lineStartOffset(i);
    i++;
    const bodyStartIdx = i;
    while (i < lines.length && !/^```\s*$/.test(lines[i])) i++;
    if (i >= lines.length) break;
    const body = lines.slice(bodyStartIdx, i).join('\n');
    const end = lineStartOffset(i) + lines[i].length;
    blocks.push({ start, end, info, body });
    i++;
  }
  return blocks;
}

function splitByLastAnswerFence(full: string): SplitResponse | null {
  const fences = listFences(full);
  let lastOk: (typeof fences)[0] | null = null;
  for (const f of fences) {
    if (infoLooksLikeAnswerLang(f.info) || fenceLooksLikeKernel(f.body)) lastOk = f;
  }
  if (!lastOk) return null;
  const thinking = full.slice(0, lastOk.start).trim();
  const answer = full.slice(lastOk.start).trim();
  return { thinking, answer };
}

function splitByHeading(full: string): SplitResponse | null {
  const idx = full.search(/\n#{1,3}\s*(Implementation|CUDA|Solution|Final\s+answer|Kernel|Complete\s+code)\b/i);
  if (idx <= 0) return null;
  return {
    thinking: full.slice(0, idx).trim(),
    answer: full.slice(idx).trim(),
  };
}

export function splitResponseForView(raw: string): SplitResponse {
  const full = (raw ?? '').trim();
  if (!full) return { thinking: '', answer: '' };

  const redacted = extractRedactedThinkingBlock(full);
  if (redacted) return redacted;

  const tagged = extractTaggedThinking(full);
  if (tagged.thinking) {
    return { thinking: tagged.thinking, answer: tagged.remainder };
  }

  const byFence = splitByLastAnswerFence(full);
  if (byFence) return byFence;

  const byHead = splitByHeading(full);
  if (byHead) return byHead;

  return { thinking: '', answer: full };
}
