/**
 * Model outputs often wrap chain-of-thought in tags like `<think>`.
 * In CommonMark / GFM those start **HTML blocks**, so everything until the
 * closing tag is **not** parsed as Markdown — fenced ``` code stays plain text.
 * Removing known wrappers restores normal Markdown (including code fences).
 */
export const PAIRED_WRAPPER_TAGS = [
  'redacted_thinking',
  'redacted_reasoning',
  'thinking',
  'thought',
  'analysis',
  'reasoning',
  'tool_call',
  'scratchpad',
] as const;

/** Lines like `#include <torch/extension.h>` start HTML blocks if `<...>` looks like a tag—wrap as inline code. */
export function fixIncludeAndAngleLines(s: string): string {
  const lines = s.split('\n');
  let fenceDepth = 0;
  const out: string[] = [];
  const fenceStart = /^(\s*)(```+|~~~+)/;

  for (const line of lines) {
    const fm = line.match(fenceStart);
    if (fm) {
      const marker = fm[2];
      if (marker.startsWith('```') || marker.startsWith('~~~')) {
        fenceDepth ^= 1;
      }
      out.push(line);
      continue;
    }
    if (fenceDepth === 0) {
      const inc = line.match(/^(\s*)(#include\s+<[^>\n]+>)\s*$/);
      if (inc) {
        out.push(`${inc[1]}\`${inc[2]}\``);
        continue;
      }
      const lone = line.match(/^(\s*)(<[A-Za-z0-9_./+-]+\.[hhc](?:pp|xx)?>)\s*$/);
      if (lone) {
        out.push(`${lone[1]}\`${lone[2]}\``);
        continue;
      }
    }
    out.push(line);
  }
  return out.join('\n');
}

const PY_ASSIGN_TRIPLE_START = /^(\s*)([a-zA-Z_]\w*)\s*=\s*r?"""\s*$/;
const PY_TRIPLE_CLOSE = /^\s*"""\s*$/;
const MAX_TRIPLE_BODY_LINES = 8000;

/** Heuristic: triple-quoted string holds CUDA / PyTorch extension / glue code (not plain prose). */
function tripleQuoteBodyLooksLikeCudaSource(body: string): boolean {
  return (
    /#include\s*[<"]|__global__|__device__|__host__|cuda_runtime|torch\/extension|TORCH_EXTENSION|nvrtc|\.cu\b/.test(
      body
    ) ||
    /\b(PYBIND11|TORCH_LIBRARY|DEFINE_DISPATCH|TORCH_LIBRARY_IMPL|cpp_extension|CUDAExtension|load_inline|pybind11|BuildExtension|ninja)\b/.test(
      body
    )
  );
}

/**
 * Outside fenced blocks, wrap Python assignments like `cuda_source = """ ... """`
 * in a ` ```python ` fence so the CUDA/C++ inside is highlighted instead of parsed as Markdown/HTML.
 */
export function fencePythonCudaTripleQuotes(raw: string): string {
  const lines = raw.split('\n');
  const out: string[] = [];
  const fenceRe = /^(\s*)(```+|~~~+)/;
  let fenceDepth = 0;
  let i = 0;
  while (i < lines.length) {
    const line = lines[i]!;
    const fm = line.match(fenceRe);
    if (fm && (fm[2].startsWith('```') || fm[2].startsWith('~~~'))) {
      fenceDepth ^= 1;
      out.push(line);
      i += 1;
      continue;
    }
    if (fenceDepth !== 0) {
      out.push(line);
      i += 1;
      continue;
    }
    const m = line.match(PY_ASSIGN_TRIPLE_START);
    if (!m) {
      out.push(line);
      i += 1;
      continue;
    }
    const body: string[] = [];
    let j = i + 1;
    let n = 0;
    while (j < lines.length && n < MAX_TRIPLE_BODY_LINES) {
      const L = lines[j]!;
      if (PY_TRIPLE_CLOSE.test(L)) break;
      body.push(L);
      j += 1;
      n += 1;
    }
    if (j >= lines.length || !PY_TRIPLE_CLOSE.test(lines[j]!)) {
      out.push(line);
      i += 1;
      continue;
    }
    const inner = body.join('\n');
    if (!tripleQuoteBodyLooksLikeCudaSource(inner)) {
      out.push(line);
      i += 1;
      continue;
    }
    const blockLines = [line, ...body, lines[j]!];
    out.push('');
    out.push('```python');
    out.push(...blockLines);
    out.push('```');
    out.push('');
    i = j + 1;
  }
  return out.join('\n');
}

function measureLeadingIndentCols(line: string): number {
  const m = line.match(/^(\s*)/);
  if (!m) return 0;
  let n = 0;
  for (const ch of m[1]) {
    if (ch === ' ') n += 1;
    else if (ch === '\t') n += 4;
  }
  return n;
}

const MIN_INDENTED_CODE_COLS = 4;

/**
 * CommonMark-style indented code (≥4 spaces) is easy to lose when `<...>` breaks HTML parsing.
 * Outside fences, convert consistent runs of deeply indented lines into fenced blocks.
 */
export function fenceIndentedCodeBlocks(raw: string): string {
  const lines = raw.split('\n');
  const fenceRe = /^(\s*)(```+|~~~+)/;
  let fenceDepth = 0;
  const out: string[] = [];
  let i = 0;

  while (i < lines.length) {
    const line = lines[i]!;
    const fm = line.match(fenceRe);
    if (fm && (fm[2].startsWith('```') || fm[2].startsWith('~~~'))) {
      fenceDepth ^= 1;
      out.push(line);
      i += 1;
      continue;
    }
    if (fenceDepth !== 0) {
      out.push(line);
      i += 1;
      continue;
    }

    if (line.trim() === '' || measureLeadingIndentCols(line) < MIN_INDENTED_CODE_COLS) {
      out.push(line);
      i += 1;
      continue;
    }

    const unindentedPreview = line.replace(/^\s+/, '');
    if (/^[-*+]\s+\S/.test(unindentedPreview) || /^\d+\.\s+\S/.test(unindentedPreview)) {
      out.push(line);
      i += 1;
      continue;
    }

    let j = i;
    const chunk: string[] = [];
    while (j < lines.length) {
      const L = lines[j]!;
      const fm2 = L.match(fenceRe);
      if (fm2 && (fm2[2].startsWith('```') || fm2[2].startsWith('~~~'))) break;

      if (L.trim() === '') {
        const next = lines[j + 1];
        if (next !== undefined && next.trim() !== '' && measureLeadingIndentCols(next) >= MIN_INDENTED_CODE_COLS) {
          chunk.push(L);
          j += 1;
          continue;
        }
        break;
      }

      if (measureLeadingIndentCols(L) < MIN_INDENTED_CODE_COLS) break;
      chunk.push(L);
      j += 1;
    }

    const nonEmpty = chunk.filter((l) => l.trim() !== '');
    if (nonEmpty.length === 0) {
      out.push(line);
      i += 1;
      continue;
    }

    let minIndent = Infinity;
    for (const l of nonEmpty) {
      const n = measureLeadingIndentCols(l);
      if (n < minIndent) minIndent = n;
    }

    const dedented = chunk.map((l) => {
      if (l.trim() === '') return '';
      return l.slice(minIndent);
    });
    const joined = dedented.join('\n').trimEnd();
    const firstReal = nonEmpty[0]!.slice(minIndent);

    const looksLikeCode =
      nonEmpty.length >= 2 ||
      strongCodeLine(firstReal) ||
      isProbablyCodeLine(firstReal) ||
      /#include|__global__|^(def |class |import |from )|torch::|PYBIND11|cuda_runtime/.test(joined);

    if (looksLikeCode) {
      const lang = inferFenceLang(firstReal);
      const fenceLang = lang === 'text' ? '' : lang;
      out.push('');
      out.push('```' + fenceLang);
      out.push(joined);
      out.push('```');
      out.push('');
      i = j;
    } else {
      out.push(line);
      i += 1;
    }
  }

  return out.join('\n');
}

function strongCodeLine(line: string): boolean {
  const t = line.trim();
  if (!t) return false;
  if (/^#(include|define|pragma|if|elif|else|endif)\b/.test(t)) return true;
  if (/\b(__global__|__device__|__host__|__forceinline|__shared__|__restrict__)\b/.test(t)) return true;
  if (/^extern\s+"C"/.test(t)) return true;
  if (/^\s*(PYBIND11_MODULE|TORCH_LIBRARY|TORCH_LIBRARY_IMPL)\s*\(/.test(t)) return true;
  return false;
}

/** Avoid fencing Megatron-style English reasoning paragraphs as code. */
function looksLikeEnglishProseLine(line: string): boolean {
  const t = line.trim();
  if (!t) return false;
  if (/^(Okay|Okay,|Let me|I need to|I'll |We should|The goal|My initial|Specifically|Furthermore|However|Note that|Remember that|From the plan)/i.test(t))
    return true;
  if (/^\*\*[^*]+\*\*\s*$/.test(t) && t.length < 220 && !/\b(__global__|#include|torch::)\b/.test(t))
    return true;
  if (
    /^[A-Z][a-z]{2,}(\s+[a-z][a-z,.;:''\-–\s]*){5,}[.?!]\s*$/.test(t) &&
    !/[;{}]{2}/.test(t) &&
    !/\b(std::|torch::|cuda[A-Z]|__device__)\b/.test(t)
  )
    return true;
  return false;
}

function isProbablyCodeLine(line: string): boolean {
  const t = line.trim();
  if (!t) return false;
  if (looksLikeEnglishProseLine(line)) return false;
  if (strongCodeLine(line)) return true;
  if (/^(def |class |import |from |export |async def |@)/.test(t)) return true;
  if (/^(const |let |var |function |fn |pub |use |mod |impl |struct |enum |type |match )/.test(t)) return true;
  if (
    /^(void|int|float|double|bool|char|size_t|auto|static|inline|virtual|explicit|return)\b/.test(t) &&
    /[;{]/.test(t)
  )
    return true;
  if (/^(public|private|protected|namespace|template|using)\b/.test(t)) return true;
  if (
    /\b(torch::|c10::|at::|std::|cuda|CUDA|__syncthreads|warp(?:Shuffle|Reduce)?|cooperative_groups|__launch_bounds__|unsigned\s+\w+|int64_t|size_t)\b/.test(
      t
    )
  )
    return true;
  if (
    /\b(AT_DISPATCH|TORCH_CHECK|TORCH_LIBRARY|TORCH_LIBRARY_IMPL|CUDA_CHECK|C10_CUDA_KERNEL_LAUNCH|nvrtc|PYBIND11|DEFINE_DISPATCH)\b/.test(
      t
    )
  )
    return true;
  if (/\b(cpp_extension|CUDAExtension|load_inline|pybind11|BuildExtension|ninja|setuptools)\b/.test(t)) return true;
  if (/^\s*(m\.(def|impl)|torch\.|nn\.|F\.)\b/.test(t)) return true;
  if (/^\s*Tensor(\s*[<(]|\s+[a-zA-Z_])/.test(t)) return true;
  if (/^\s*(setup\s*\(|ext_modules|cmdclass\s*=|Compiler|extra_compile_args)/.test(t)) return true;
  if (/^\s*(\{|\}|\};\s*)$/.test(t)) return true;
  if (/^\s*(\/\/|\/\*|\*\/)/.test(t)) return true;
  if (/\b(printf|sprintf|malloc|free|memset|memcpy|sizeof)\s*\(/.test(t)) return true;
  if (/\b(typedef|struct|enum)\b/.test(t) && /[;{]/.test(t)) return true;
  if (/\b(throw|new|delete|static_assert)\b/.test(t)) return true;
  if (/[;{}]\s*$/.test(t) && t.length < 200 && /[=<>()&*+\-/%!|&^~]/.test(t)) {
    if (/^[A-Z][a-z]+(\s[a-z,;]+){2,}\.$/.test(t)) return false;
    return true;
  }
  return false;
}

function inferFenceLang(firstLine: string): string {
  const t = firstLine.trim();
  if (/^\s*(setup\s*\(|ext_modules|cmdclass\s*=)/.test(t)) return 'python';
  if (/^\s*(PYBIND11_MODULE|TORCH_LIBRARY)\s*\(/.test(t)) return 'cpp';
  if (/\b(torch::|c10::|at::)\b/.test(t)) return 'cpp';
  if (/^#(include|define|pragma)/.test(t) || /^(template|namespace|using|class|struct|enum)\b/.test(t))
    return 'cpp';
  if (/^(return|throw|case|default|break|continue)\b/.test(t)) return 'cpp';
  if (/\b(__global__|__device__|__host__|__forceinline|blockDim|threadIdx)\b/.test(t)) return 'cuda';
  if (/^(def |class |import |from |@)/.test(t)) return 'python';
  if (/^(fn |let |const |use |mod |impl )/.test(t)) return 'rust';
  if (/^(function|const|let|var)\b/.test(t) && /=>|{/.test(t)) return 'javascript';
  return 'text';
}

/** Single-line code run: fence when language is obvious; avoid English sentences that matched `using`/`template` etc. */
function shouldFenceBareRun(nonEmpty: string[]): boolean {
  if (nonEmpty.length >= 2) return true;
  if (nonEmpty.length !== 1) return false;
  const L = nonEmpty[0]!;
  if (strongCodeLine(L)) return true;
  const t = L.trim();
  if (/^(def |class |import |from |async def |export )/.test(t)) return true;
  if (/^(return|throw|case|default|break|continue)\b/.test(t) && /[;{}]/.test(t)) return true;
  const lang = inferFenceLang(L);
  if (lang === 'text') return false;
  if (/[;{}]|::|\([^)]*\)\s*\{?|\)\s*=>/.test(t)) return true;
  if (/^(m\.|torch\.|nn\.|F\.|at\.)/.test(t)) return true;
  return false;
}

/**
 * Model output often has code without ``` fences. Wrap consecutive “code-like” lines
 * (outside existing fences) so GFM + highlight.js treat them as code blocks.
 */
export function wrapBareCodeRuns(raw: string): string {
  const lines = raw.split('\n');
  const out: string[] = [];
  const fenceRe = /^(\s*)(```+|~~~+)/;
  let fenceDepth = 0;
  let i = 0;
  while (i < lines.length) {
    const line = lines[i];
    const fm = line.match(fenceRe);
    if (fm && (fm[2].startsWith('```') || fm[2].startsWith('~~~'))) {
      fenceDepth ^= 1;
      out.push(line);
      i += 1;
      continue;
    }
    if (fenceDepth !== 0) {
      out.push(line);
      i += 1;
      continue;
    }
    if (!isProbablyCodeLine(line)) {
      out.push(line);
      i += 1;
      continue;
    }
    let j = i;
    const block: string[] = [];
    while (j < lines.length) {
      const L = lines[j];
      const fm2 = L.match(fenceRe);
      if (fm2 && (fm2[2].startsWith('```') || fm2[2].startsWith('~~~'))) break;
      if (L.trim() === '') {
        const next = lines[j + 1];
        if (next !== undefined && isProbablyCodeLine(next)) {
          block.push(L);
          j += 1;
          continue;
        }
        break;
      }
      if (!isProbablyCodeLine(L)) break;
      block.push(L);
      j += 1;
    }
    const nonEmpty = block.filter((l) => l.trim() !== '');
    if (shouldFenceBareRun(nonEmpty)) {
      const lang = inferFenceLang(nonEmpty[0]!);
      out.push('```' + lang);
      out.push(...block);
      out.push('```');
      i = j;
    } else {
      out.push(line);
      i += 1;
    }
  }
  return out.join('\n');
}

/** Short lines that are clearly C++ templates/angles but not in fences — inline backticks. */
export function backtickTemplateShapedLines(s: string): string {
  const lines = s.split('\n');
  let fenceDepth = 0;
  const fenceStart = /^(\s*)(```+|~~~+)/;
  const out: string[] = [];
  for (const line of lines) {
    const fm = line.match(fenceStart);
    if (fm && (fm[2].startsWith('```') || fm[2].startsWith('~~~'))) {
      fenceDepth ^= 1;
      out.push(line);
      continue;
    }
    if (fenceDepth !== 0 || line.includes('`')) {
      out.push(line);
      continue;
    }
    const t = line.trim();
    if (
      t.length > 5 &&
      t.length <= 160 &&
      /<[^>\n]+>/.test(t) &&
      /::|typename|template/.test(t) &&
      !/[.。!?]$/.test(t)
    ) {
      const ind = line.match(/^(\s*)/)?.[1] ?? '';
      out.push(ind + '`' + t + '`');
    } else {
      out.push(line);
    }
  }
  return out.join('\n');
}

export function normalizeMarkdownSource(raw: string): string {
  let s = fixIncludeAndAngleLines(raw);
  s = fencePythonCudaTripleQuotes(s);
  for (const tag of PAIRED_WRAPPER_TAGS) {
    const open = new RegExp(`<${tag}(?:\\s[^>]*)?>`, 'gi');
    const close = new RegExp(`</${tag}>`, 'gi');
    s = s.replace(open, '\n\n').replace(close, '\n\n');
  }
  s = fenceIndentedCodeBlocks(s);
  s = backtickTemplateShapedLines(s);
  s = wrapBareCodeRuns(s);
  return s;
}
