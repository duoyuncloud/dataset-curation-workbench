import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import rehypeHighlight from 'rehype-highlight';
import rehypeSanitize, { defaultSchema } from 'rehype-sanitize';
import { normalizeMarkdownSource } from '../markdownNormalize';
import 'highlight.js/styles/github-dark-dimmed.css';

type Props = {
  source: string;
};

/**
 * Renders markdown (GFM), sanitizes HTML, then applies lowlight / highlight.js
 * to fenced code (trusted step after sanitize per rehype-highlight docs).
 */
export function RichDocBlock({ source }: Props) {
  const md = normalizeMarkdownSource(source);
  if (!md.trim()) return null;
  return (
    <div className="modal-rich-md">
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        rehypePlugins={[[rehypeSanitize, defaultSchema], [rehypeHighlight, { detect: true }]]}
      >
        {md}
      </ReactMarkdown>
    </div>
  );
}
