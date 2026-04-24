"""
Enrich each row: ``signature`` from :mod:`.signature_registration` (operator family),
``stage_focus`` as the human title for the active step (Megatron ``Current Stage Plan`` when present),
and ``curation_path`` tying focus to a short excerpt. Digits from ``Stage N`` are only used internally
to match the plan block, not stored as ``data_stage``. Heuristics; not a full operator taxonomy.
"""

from __future__ import annotations

import re
from typing import Any

import numpy as np
import pandas as pd

from .base_filter import FilterResult
from .signature_registration import SignatureContext, signature_registration

# Row-level optimization stage from question text (not platform stage_0 / stage_1).
_DATA_STAGE_RE = re.compile(r"Stage\s*(\d+)", re.I)
# Megatron-style: "### Current Stage Plan: **Stage N: Human title**"
_CURRENT_STAGE_PLAN_RE = re.compile(
    r"Current Stage Plan:\s*\*\*Stage\s*(\d+)\s*:\s*([^*]+)\*\*",
    re.I | re.DOTALL,
)
# “算子: X” or "Operator: X"
_OP_LINE_RE = re.compile(
    r"(?:算子|operator)[:：]\s*([^\n\r]{2,80})",
    re.I,
)
# PyTorch / CUDA op families (class names) — not generic nn.Module/Sequential; those are structural.
_OP_FAMILY_CUES = re.compile(
    r"\b(Conv2d|Conv3d|Conv1d|ConvTranspose2d|Linear|Matmul|BMM|LayerNorm|BatchNorm\d*d?|"
    r"GELU|ReLU|SiLU|Hardswish|Softmax|Sigmoid|Tanh|Dropout|MaxPool|AvgPool|AdaptiveAvgPool|"
    r"Embedding|Multihead|Attention|Flash|Reduce|LSTM|GRU)\b",
    re.I,
)
# nn.Foo in code (stronger signal than a bare word match)
_NN_DOT = re.compile(
    r"\bnn\.(Conv2d|Conv3d|Linear|LayerNorm|BatchNorm\d*d?|ReLU|GELU|SiLU|Hardswish|ConvTranspose2d)\b",
    re.I,
)
# Not operator families: language, stack labels, and container classes
_INVALID_FAM = frozenset(
    {
        "python",
        "import",
        "from",
        "def",
        "class",
        "return",
        "self",
        "none",
        "true",
        "false",
        "pass",
        "model",
        "tensor",
        "forward",
        "backward",
        "torch",
        "nn",
        "cuda",
        "device",
        "the",
        "a",
        "an",
        "module",
        "sequential",
        "modulelist",
        "parameter",
        "parameterlist",
    }
)
_TECH_RE = re.compile(
    r"(?:technique|optimization|fused|fusion|tiling|vectori[sz]ed|kernel)[:：]?\s*([^\n\r]{0,100})",
    re.I,
)


def _valid_op_family(name: str) -> bool:
    s = (name or "").strip().lower()
    if len(s) < 2:
        return False
    if s in _INVALID_FAM:
        return False
    return True


def extract_data_stage_from_question(q: str) -> str:
    """MVP: last ``Stage\\s*(\\d+)`` match in question; else ``unknown``."""
    t = (q or "").replace("\r\n", "\n")
    matches = list(_DATA_STAGE_RE.finditer(t))
    if not matches:
        return "unknown"
    return str(matches[-1].group(1))


def _norm_stage_focus_title(s: str) -> str:
    t = re.sub(r"\s+", " ", (s or "").strip())
    if len(t) > 220:
        t = t[:217] + "..."
    return t


def extract_stage_focus_from_question(q: str) -> str:
    """
    Short human-readable label for *what the active step is doing* (not the digit alone).

    Prefer the ``Current Stage Plan`` block used in Megatron multi-stage CUDA prompts; fall back
    to the first ``Stage N: title`` line for the active ``N`` (last ``Stage N`` in the prompt).
    """
    t = (q or "").replace("\r\n", "\n")
    n = extract_data_stage_from_question(t)
    if n == "unknown":
        return "unknown"
    m = _CURRENT_STAGE_PLAN_RE.search(t)
    if m and str(m.group(1)) == str(n):
        got = _norm_stage_focus_title(m.group(2))
        if got:
            return got
    # Fallback: first "Stage N: ..." headline (curriculum list), same N
    fb = re.compile(rf"(?:^|\s)Stage\s*{re.escape(n)}\s*:\s*([^\n(]+)", re.I)
    m2 = fb.search(t)
    if m2:
        got = _norm_stage_focus_title(m2.group(1))
        if got:
            return got
    return "unknown"


def _op_family_from_text(t: str) -> str:
    """Best-effort op family: prefer nn., then first valid _OP_FAMILY_CUES match."""
    nn_m = _NN_DOT.search(t)
    if nn_m and _valid_op_family(nn_m.group(1)):
        return nn_m.group(1).lower()
    for m in _OP_FAMILY_CUES.finditer(t):
        cand = m.group(1).lower()
        if _valid_op_family(cand):
            return cand
    return "unknown"


def extract_from_question(q: str) -> dict[str, str]:
    t = (q or "").replace("\r\n", "\n")
    stage_focus = extract_stage_focus_from_question(t)
    op_line = _OP_LINE_RE.search(t)
    op_line_g = (op_line.group(1).strip() if op_line else "").strip()[:120]
    raw_sig = op_line_g
    fam = _op_family_from_text(t)
    if fam == "unknown" and op_line_g:
        w0 = re.sub(r"^[^A-Za-z0-9_]+", "", op_line_g.split()[0] if op_line_g.split() else "")
        w0 = w0[:40].lower()
        if _valid_op_family(w0):
            fam = w0
        else:
            fam = "unknown"
    tech_m = _TECH_RE.search(t)
    technique = (tech_m.group(1).strip() if tech_m else raw_sig)[:200] or "unknown"
    # Canonical row signature: policy lives in `signature_registration` (operator family for now)
    signature = signature_registration(
        SignatureContext(operator_family=fam, raw_question=t)
    )
    short = raw_sig or fam
    if len(short) > 60:
        short = short[:57] + "..."
    detail_path = f"{stage_focus}::{short}" if short else stage_focus
    return {
        "signature": signature,
        "operator_family": fam,
        "curation_path": detail_path,
        "stage_focus": stage_focus,
        "technique": technique or "unknown",
    }


def _cell_q(row: pd.Series) -> str:
    v = row.get("question", row.get("prompt", ""))
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    return str(v)


def _question_texts_column(df: pd.DataFrame) -> np.ndarray:
    """Per-row question string; same precedence as ``_cell_q`` (``question`` then ``prompt``)."""
    n = len(df)
    if n == 0:
        return np.array([], dtype=object)
    if "question" in df.columns:
        col = df["question"].to_numpy(copy=False)
    elif "prompt" in df.columns:
        col = df["prompt"].to_numpy(copy=False)
    else:
        return np.asarray([""] * n, dtype=object)
    out = np.empty(n, dtype=object)
    for i in range(n):
        v = col[i]
        if v is None or (isinstance(v, float) and pd.isna(v)):
            out[i] = ""
        else:
            out[i] = str(v)
    return out


def enrich_dataframe_signatures(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill signature-related columns from question text.

    Uses list accumulation + vectorized column assignment instead of per-row ``.at`` updates,
    which is orders of magnitude slower on large uploads.
    """
    out = df.copy()
    n = len(out)
    for col in ("signature", "operator_family", "curation_path", "stage_focus", "technique"):
        if col not in out.columns:
            out[col] = None
    if n == 0:
        return out
    texts = _question_texts_column(out)
    sigs: list[str] = []
    fams: list[str] = []
    paths: list[str] = []
    focuses: list[str] = []
    techs: list[str] = []
    for i in range(n):
        m = extract_from_question(texts[i])
        sigs.append(m["signature"])
        fams.append(m["operator_family"])
        paths.append(m["curation_path"])
        focuses.append(m["stage_focus"])
        techs.append(m["technique"])
    out["signature"] = sigs
    out["operator_family"] = fams
    out["curation_path"] = paths
    out["stage_focus"] = focuses
    out["technique"] = techs
    if "stage" in out.columns:
        out = out.drop(columns=["stage"])
    return out


def ensure_stage_focus_column(df: pd.DataFrame) -> pd.DataFrame:
    """Backfill ``stage_focus`` when missing (older JSONL on disk)."""
    if df is None or len(df) == 0:
        return df
    out = df.copy()
    if "question" not in out.columns and "prompt" not in out.columns:
        if "stage_focus" not in out.columns:
            out["stage_focus"] = "unknown"
        return out
    qcol = out["question"] if "question" in out.columns else out["prompt"]
    computed = qcol.fillna("").astype(str).map(extract_stage_focus_from_question)
    if "stage_focus" not in out.columns:
        out["stage_focus"] = computed
        return out
    mask = out["stage_focus"].isna() | (out["stage_focus"].astype(str).str.strip() == "")
    out.loc[mask, "stage_focus"] = computed[mask]
    return out


def apply_signature_extraction(df: pd.DataFrame, config: dict[str, Any]) -> FilterResult:
    """
    Re-compute signature columns for all kept rows; does not remove rows.
    """
    e = enrich_dataframe_signatures(df)
    return FilterResult(
        name="Signature extraction",
        filter_type="signature_extraction",
        filter_config=dict(config),
        input_count=len(df),
        kept=e,
        removed=pd.DataFrame(),
    )
