"""
Split model response into chain-of-thought vs final answer (mirrors frontend ``responseSplit.ts``).
Used for table column split and for server-side sort on thinking/answer.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

THINKING_TAGS = (
    "redacted_thinking",
    "redacted_reasoning",
    "thinking",
    "thought",
    "analysis",
    "reasoning",
    "tool_call",
    "scratchpad",
)


@dataclass(frozen=True)
class SplitResponse:
    thinking: str
    answer: str


def _extract_tagged_thinking(raw: str) -> tuple[str, str]:
    s = raw
    parts: list[str] = []
    for tag in THINKING_TAGS:
        pat = re.compile(
            rf"<{re.escape(tag)}(?:\s[^>]*)?>([\s\S]*?)</{re.escape(tag)}>",
            re.IGNORECASE,
        )
        for m in pat.finditer(s):
            parts.append(m.group(1).strip())
        s = pat.sub("\n\n", s)
    remainder = re.sub(r"\n{3,}", "\n\n", s).strip()
    thinking = "\n\n---\n\n".join(p for p in parts if p)
    return thinking, remainder


def _info_looks_like_answer_lang(info: str) -> bool:
    head = info.strip().split()[:1]
    if not head:
        return False
    h = head[0].lower()
    return bool(re.match(r"^(cuda|cu|cpp|c\+\+|cxx|c)\b", h))


def _fence_looks_like_kernel(body: str) -> bool:
    b = body[:12000]
    return bool(
        re.search(
            r"\b(__global__|__device__|extern\s+\"C\"|launch_kernel|blockDim|threadIdx)\b",
            b,
        )
    )


def _list_fences(full: str) -> list[tuple[int, int, str, str]]:
    lines = full.split("\n")
    blocks: list[tuple[int, int, str, str]] = []

    def line_start_offset(idx: int) -> int:
        o = 0
        for j in range(idx):
            o += len(lines[j]) + 1
        return o

    i = 0
    while i < len(lines):
        line = lines[i]
        if not line.startswith("```"):
            i += 1
            continue
        info = line[3:].strip()
        start = line_start_offset(i)
        i += 1
        body_start = i
        while i < len(lines) and not re.match(r"^```\s*$", lines[i]):
            i += 1
        if i >= len(lines):
            break
        body = "\n".join(lines[body_start:i])
        end = line_start_offset(i) + len(lines[i])
        blocks.append((start, end, info, body))
        i += 1
    return blocks


def _split_by_last_answer_fence(full: str) -> SplitResponse | None:
    fences = _list_fences(full)
    last_ok: tuple[int, int, str, str] | None = None
    for start, end, info, body in fences:
        if _info_looks_like_answer_lang(info) or _fence_looks_like_kernel(body):
            last_ok = (start, end, info, body)
    if not last_ok:
        return None
    start, _end, _info, _body = last_ok
    thinking = full[:start].strip()
    answer = full[start:].strip()
    return SplitResponse(thinking=thinking, answer=answer)


def _split_by_heading(full: str) -> SplitResponse | None:
    m = re.search(
        r"\n#{1,3}\s*(Implementation|CUDA|Solution|Final\s+answer|Kernel|Complete\s+code)\b",
        full,
        re.IGNORECASE,
    )
    if not m or m.start() <= 0:
        return None
    idx = m.start()
    return SplitResponse(
        thinking=full[:idx].strip(),
        answer=full[idx:].strip(),
    )


def _extract_redacted_thinking_block(full: str) -> SplitResponse | None:
    """Index-based slice for ``<think>…</think>`` (large inner bodies)."""
    m = re.search(r"<redacted_thinking(?:\s[^>]*)?>", full, re.IGNORECASE)
    if not m:
        return None
    start_inner = m.end()
    low = full.lower()
    close_marker = "</think>"
    ci = low.find(close_marker, start_inner)
    if ci < 0:
        return None
    thinking = full[start_inner:ci].strip()
    tail = full[ci:]
    cm = re.match(r"</think>", tail, re.IGNORECASE)
    close_len = len(cm.group(0)) if cm else len(close_marker)
    answer = full[ci + close_len :].strip()
    prefix = full[: m.start()].strip()
    if prefix:
        answer = prefix + ("\n\n" + answer if answer else "")
    return SplitResponse(thinking=thinking, answer=answer)


def split_response_for_view(raw: str) -> SplitResponse:
    full = (raw or "").strip()
    if not full:
        return SplitResponse(thinking="", answer="")

    rb = _extract_redacted_thinking_block(full)
    if rb is not None:
        return rb

    thinking, remainder = _extract_tagged_thinking(full)
    if thinking:
        return SplitResponse(thinking=thinking, answer=remainder)

    by_fence = _split_by_last_answer_fence(full)
    if by_fence is not None:
        return by_fence

    by_head = _split_by_heading(full)
    if by_head is not None:
        return by_head

    return SplitResponse(thinking="", answer=full)
