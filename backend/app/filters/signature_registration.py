"""
Map enriched row signals to the single canonical *signature* string on each row.

The default policy is *operator family only*; you can later extend
:class:`SignatureContext` and :func:`signature_registration` with composite
keys, heuristics, or pluggable strategies without changing dataframe column
names in the app.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SignatureContext:
    """Signals available when computing ``signature``; add fields as needed."""

    operator_family: str
    raw_question: str = ""


def signature_registration(ctx: SignatureContext) -> str:
    """
    Produce the ``signature`` string from a :class:`SignatureContext`.

    *Current implementation*: return the operator family (same as
    ``operator_family`` in extraction). Replace or extend the body when you
    adopt a richer signature scheme.
    """
    fam = (ctx.operator_family or "unknown").strip() or "unknown"
    return fam
