"""
Enrich each row: ``signature`` comes from :mod:`.signature_registration` (for now
operator family only), ``curation_path`` = stage + excerpt, from SFT *question*
text (Megatron-style). Heuristics; not a full operator taxonomy.
"""

from __future__ import annotations

import re
from typing import Any

import pandas as pd

from .base_filter import FilterResult
from .signature_registration import SignatureContext, signature_registration

# e.g. "You are implementing stage 3 of a multi-stage..."
_STAGE_RE = re.compile(
    r"(?:implementing|阶段|stage)\s*(\d+)\s*(?:\s*of|\s*\/|\s*阶段)?\s*",
    re.I,
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
    stage_m = _STAGE_RE.search(t)
    stage = f"stage_{stage_m.group(1)}" if stage_m else "unknown"
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
    detail_path = f"{stage}::{short}" if short else stage
    return {
        "signature": signature,
        "operator_family": fam,
        "curation_path": detail_path,
        "stage": stage,
        "technique": technique or "unknown",
    }


def _cell_q(row: pd.Series) -> str:
    v = row.get("question", row.get("prompt", ""))
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    return str(v)


def enrich_dataframe_signatures(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    n = len(out)
    for col in ("signature", "operator_family", "curation_path", "stage", "technique"):
        if col not in out.columns:
            out[col] = None
    for i in range(n):
        s = _cell_q(out.iloc[i])
        m = extract_from_question(s)
        for k, v in m.items():
            out.at[out.index[i], k] = v
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
