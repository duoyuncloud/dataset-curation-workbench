"""Map raw removal_reason strings to categories and short English labels for the UI."""

from __future__ import annotations

import re
from typing import Any

# Query / filter slugs
REMOVAL_CATEGORIES = ("hacking", "duplicate", "length", "format", "other")

_HACK_TOKENS = re.compile(
    r"hack|hacked|severity|hack_types",
    re.I,
)


def _split_reasons(combined: str) -> list[str]:
    s = (combined or "").strip()
    if not s:
        return []
    return [p.strip() for p in s.split("|") if p.strip()]


def category_for_fragment(text: str) -> str:
    t = (text or "").lower()
    if "format_validity" in t or t.startswith("format_validity"):
        return "format"
    if "length_anomaly" in t:
        return "length"
    if "exact_duplicate" in t or "repetition" in t or "strong_reasoning" in t:
        return "duplicate"
    if _HACK_TOKENS.search(t) or " hack " in f" {t} ":
        return "hacking"
    if "hacked == true" in t:
        return "hacking"
    return "other"


def categories_for_row(removal_reason: str) -> set[str]:
    """A row can match multiple categories if the batch joined several reasons with |."""
    out: set[str] = set()
    for part in _split_reasons(removal_reason):
        out.add(category_for_fragment(part))
    if not out:
        return {"other"}
    return out


def row_matches_removal_category(removal_reason: str, category: str) -> bool:
    cat = category.lower().strip()
    if cat in ("", "all"):
        return True
    if cat not in REMOVAL_CATEGORIES:
        return True
    return cat in categories_for_row(removal_reason)


def primary_category(removal_reason: str) -> str:
    """Category of the first `|` segment (for badges)."""
    parts = _split_reasons(removal_reason)
    if not parts:
        return "other"
    return category_for_fragment(parts[0])


def _parse_hack_types_from_reason(reason: str) -> str:
    m = re.search(r"types\s+([^\s|]+(?:,[^\s|]+)*)", reason, re.I)
    if m:
        return m.group(1).replace(",", ", ")[:200]
    return ""


def friendly_removal_label(row: dict[str, Any]) -> str:
    """One-line English summary for the removed-rows table."""
    reason = str(row.get("removal_reason") or "")
    rlow = reason.lower()
    hack_types = str(row.get("hack_types") or "").strip()

    if "hacked == true" in rlow:
        return "Hacking (dataset hacked flag)"
    if "hack" in rlow or hack_types or row.get("severity") is not None:
        types = hack_types or _parse_hack_types_from_reason(reason)
        if types:
            return f"Hacking ({types})"
        return "Hacking"

    if "strong_reasoning_repetition" in rlow or "repetition" in rlow and "avoid" in rlow:
        return "Duplicate (repetition in response)"
    if "exact_duplicate" in rlow:
        return "Duplicate (exact key)"

    if "format_validity" in rlow:
        tail = reason.split("format_validity:", 1)[-1].strip()
        if len(tail) > 100:
            tail = tail[:97] + "…"
        return f"Format: {tail}" if tail else "Format invalid"

    if "length_anomaly" in rlow:
        if "question len" in rlow:
            return f"Length: {reason.split('length_anomaly:', 1)[-1].strip()[:120]}"
        if "response len" in rlow:
            return f"Length: {reason.split('length_anomaly:', 1)[-1].strip()[:120]}"
        tail = reason.split("length_anomaly", 1)[-1].strip()[:120]
        return f"Length / truncation ({tail})" if tail else "Length / truncation"

    if reason:
        return reason[:200] + ("…" if len(reason) > 200 else "")
    return "Unknown"
