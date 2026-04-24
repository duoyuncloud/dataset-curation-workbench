#!/usr/bin/env python3
"""
将 round 中正确且无内部重复的 case 拼接成 SFT 训练数据（JSONL 格式）。

数据来源：
  - torch/*.py          → 原始 PyTorch 代码（嵌入 question 的 {torch_code} 占位符）
  - cuda/*_reasoning.txt → 模型推理过程（作为 <think>...</think> 块）
  - cuda/*.py            → 最终生成的 CUDA 代码（作为 response 中的代码部分）
  - eval/*.json          → 评测结果（筛选 correct=true 的 case）
  - prompt_sft.txt       → question 模板（包含 {torch_code} 占位符）

输出格式与 example.jsonl 一致：
  {"question": "...", "response": "<think>...</think>```python\n...\n```"}

Usage:
    python build_sft_data.py --dataset-root full_set --round 3
    python build_sft_data.py --dataset-root full_set --round 3 --skip-dedup
    python build_sft_data.py --dataset-root full_set --round 3 --output my_sft.jsonl
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent


# ============================================================
# 重复检测（复用 check_reasoning_repetition.py 的核心逻辑）
# ============================================================

def _norm(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    return re.sub(r"[ \t]+", " ", text)


def _repeated_ngram_blocks(lines: list[str], n: int, min_chars: int) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for i in range(len(lines) - n + 1):
        block = "\n".join(lines[i : i + n]).strip()
        if len(block) >= min_chars:
            counter[block] += 1
    return {b: c for b, c in counter.items() if c >= 2}


def has_strong_repetition(text: str) -> bool:
    """检测 reasoning 文本是否存在严格口径的内部重复（8 行块重复或相邻段落复读）。"""
    normed = _norm(text)
    lines = [ln.strip() for ln in normed.split("\n") if ln.strip()]

    # 第 3 层：重复 8 行代码块
    rep_b8 = _repeated_ngram_blocks(lines, n=8, min_chars=400)
    if rep_b8:
        return True

    # 第 4 层：相邻段落直接重复
    paras = [p for raw in re.split(r"\n\s*\n+", normed) if (p := raw.strip()) and len(p) >= 80]
    for a, b in zip(paras, paras[1:]):
        if a == b:
            return True

    return False


# ============================================================
# 类名替换：Model → ModelNew
# ============================================================

def rename_model_class(code: str) -> str:
    """
    将生成代码中的 class Model 重命名为 class ModelNew，
    同时更新 get_inputs / get_init_inputs 中可能的引用。
    这样和 example.jsonl 的 ModelNew 约定一致。
    """
    code = re.sub(r'\bclass Model\b(\s*\()', r'class ModelNew\1', code)
    return code


# ============================================================
# 可复用的核心函数（供 pipeline.py 调用）
# ============================================================

def build_sft_data(
    round_dir: Path,
    torch_dir: Path,
    *,
    prompt_template_path: Path | str | None = None,
    output_path: Path | str | None = None,
    skip_dedup: bool = False,
    dedup_level: str = "strong",
) -> dict:
    """
    将一轮中正确且去重后的 case 拼接成 SFT JSONL 文件。

    Parameters
    ----------
    round_dir : 轮次目录，如 .../results/round_003
    torch_dir : PyTorch 源码目录，如 .../torch
    prompt_template_path : prompt 模板路径（含 {torch_code}），默认 prompt_sft.txt
    output_path : 输出 JSONL 路径，默认 round_dir/sft_data.jsonl
    skip_dedup : 是否跳过去重
    dedup_level : "strong" 或 "any"

    Returns
    -------
    dict with keys: written, dedup_removed, skipped_missing, output_path, correct_total
    """
    cuda_dir = round_dir / "cuda"
    eval_dir = round_dir / "eval"

    for d, name in [(torch_dir, "torch"), (cuda_dir, "cuda"), (eval_dir, "eval")]:
        if not d.exists():
            raise FileNotFoundError(f"{name} directory not found: {d}")

    # --- 读取 prompt 模板 ---
    if prompt_template_path is None:
        prompt_template_path = BASE_DIR / "prompt_sft.txt"
    prompt_template_path = Path(prompt_template_path)
    if not prompt_template_path.exists():
        raise FileNotFoundError(f"prompt template not found: {prompt_template_path}")
    prompt_template = prompt_template_path.read_text(encoding="utf-8")
    if "{torch_code}" not in prompt_template:
        print("WARNING: prompt template does not contain {torch_code} placeholder", file=sys.stderr)

    # --- 收集正确的 case ID ---
    correct_ids: set[str] = set()
    for p in eval_dir.glob("*.json"):
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        if data.get("correct") is True:
            correct_ids.add(p.stem)
    correct_total = len(correct_ids)

    # --- 去重：过滤掉 reasoning 内部有重复的 case ---
    dedup_removed_ids: list[str] = []
    if not skip_dedup:
        to_remove: set[str] = set()
        for pid in sorted(correct_ids):
            reasoning_path = cuda_dir / f"{pid}_reasoning.txt"
            if not reasoning_path.exists():
                continue
            reasoning_text = reasoning_path.read_text(encoding="utf-8", errors="ignore")
            if dedup_level == "strong":
                if has_strong_repetition(reasoning_text):
                    to_remove.add(pid)
            else:
                from check_reasoning_repetition import analyse_file
                m = analyse_file(reasoning_text)
                if m["any"]:
                    to_remove.add(pid)
        dedup_removed_ids = sorted(to_remove)
        correct_ids -= to_remove

    # --- 拼接 SFT 数据 ---
    out = Path(output_path) if output_path else (round_dir / "sft_data.jsonl")
    written = 0
    skipped_missing = 0

    with out.open("w", encoding="utf-8") as out_f:
        for pid in sorted(correct_ids):
            torch_path = torch_dir / f"{pid}.py"
            if not torch_path.exists():
                skipped_missing += 1
                continue

            reasoning_path = cuda_dir / f"{pid}_reasoning.txt"
            if not reasoning_path.exists():
                skipped_missing += 1
                continue

            cuda_path = cuda_dir / f"{pid}.py"
            if not cuda_path.exists():
                skipped_missing += 1
                continue

            torch_code = torch_path.read_text(encoding="utf-8").rstrip()
            reasoning = reasoning_path.read_text(encoding="utf-8", errors="ignore").strip()
            cuda_code = cuda_path.read_text(encoding="utf-8").rstrip()

            question = prompt_template.replace("{torch_code}", torch_code)
            cuda_code_renamed = rename_model_class(cuda_code)
            response = f"<think>{reasoning}\n</think>```python\n{cuda_code_renamed}\n```"

            record = {"question": question, "response": response}
            out_f.write(json.dumps(record, ensure_ascii=False) + "\n")
            written += 1

    return {
        "written": written,
        "dedup_removed": len(dedup_removed_ids),
        "dedup_removed_ids": dedup_removed_ids,
        "skipped_missing": skipped_missing,
        "output_path": str(out),
        "correct_total": correct_total,
        "dedup_level": dedup_level,
    }


# ============================================================
# CLI 入口
# ============================================================

def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--dataset-root", default="full_set", help="数据集根目录 (default: full_set)")
    ap.add_argument("--round", type=int, required=True, help="轮次编号")
    ap.add_argument(
        "--prompt-template",
        default=None,
        help="question 模板文件路径，包含 {torch_code} 占位符 (default: prompt_sft.txt)",
    )
    ap.add_argument("--output", default=None, help="输出 JSONL 文件路径 (default: <round_dir>/sft_data.jsonl)")
    ap.add_argument("--skip-dedup", action="store_true", help="跳过 reasoning 去重，保留所有正确样本")
    ap.add_argument(
        "--dedup-level",
        choices=["any", "strong"],
        default="strong",
        help="去重口径：strong=仅去掉大段重复, any=去掉所有有重复迹象的 (default: strong)",
    )
    args = ap.parse_args()

    dataset_root = Path(args.dataset_root)
    if not dataset_root.is_absolute():
        dataset_root = (BASE_DIR / dataset_root).resolve()
    round_dir = dataset_root / "results" / f"round_{args.round:03d}"
    torch_dir = dataset_root / "torch"

    try:
        result = build_sft_data(
            round_dir=round_dir,
            torch_dir=torch_dir,
            prompt_template_path=args.prompt_template,
            output_path=args.output,
            skip_dedup=args.skip_dedup,
            dedup_level=args.dedup_level,
        )
    except FileNotFoundError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1

    print(f"Correct cases: {result['correct_total']}")
    if not args.skip_dedup:
        print(f"Dedup removed ({result['dedup_level']}): {result['dedup_removed']}")
        print(f"Remaining after dedup: {result['correct_total'] - result['dedup_removed']}")
    print(f"\nSFT data written: {result['written']} samples")
    if result["skipped_missing"]:
        print(f"Skipped (missing files): {result['skipped_missing']}")
    print(f"Output: {result['output_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
