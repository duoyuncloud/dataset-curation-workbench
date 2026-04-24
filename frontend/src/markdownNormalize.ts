/**
 * Model outputs often wrap chain-of-thought in tags like `<think>`.
 * In CommonMark / GFM those start **HTML blocks**, so everything until the
 * closing tag is **not** parsed as Markdown — fenced ``` code stays plain text.
 * Removing known wrappers restores normal Markdown (including code fences).
 */
const PAIRED_WRAPPER_TAGS = [
  'redacted_thinking',
  'redacted_reasoning',
  'thinking',
  'thought',
  'analysis',
  'reasoning',
] as const;

export function normalizeMarkdownSource(raw: string): string {
  let s = raw;
  for (const tag of PAIRED_WRAPPER_TAGS) {
    const open = new RegExp(`<${tag}(?:\\s[^>]*)?>`, 'gi');
    const close = new RegExp(`</${tag}>`, 'gi');
    s = s.replace(open, '\n\n').replace(close, '\n\n');
  }
  return s;
}
