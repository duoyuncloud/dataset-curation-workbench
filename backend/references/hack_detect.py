"""
SFT训练数据Hack检测与过滤工具

检测CUDA算子实现中的hack问题：模型输出本应是自定义CUDA kernel实现，
但实际上回退调用了PyTorch/ATen/cuBLAS/cuDNN等高层库函数。

Hack类型:
  Type A - CUDA源码中ATen/Torch算子回退: at::conv*, torch::matmul等
  Type B - CUDA源码中cuBLAS/cuDNN库回退: cublasSgemm等
  Type C - 无自定义Kernel: CUDA源码中没有__global__定义
  Type D - Python层torch调用混入CUDA源码
  Type E - CUDA源码部分实现Hack: required op回退到ATen
  Type F - ModelNew.forward()中torch seed op回退 (重点!)
           F1: 无条件回退 (torch op始终在主路径上)
           F2: 训练模式回退 (if self.training: torch ops)
           F3: 条件参数回退 (if dim != X: torch ops)

过滤层级:
  Level 1 (宽松): 仅过滤无kernel且全靠回退 + 无条件forward回退(F1)
  Level 2 (中等): 额外过滤核心算子(conv/gemm/norm)的forward回退 + CUDA源码核心回退
  Level 3 (严格): 过滤所有forward回退 + CUDA源码回退
  Level 4 (极严格): 过滤所有hack迹象
"""

import json
import re
import argparse
import sys
from dataclasses import dataclass, field
from collections import Counter


# ============================================================================
# Seed Operators (完整列表，来自 gemm_conv_ops / reduction_ops / elementwise_ops)
# ============================================================================

SEED_OPS = {
    "gemm_conv": [
        "torch.matmul", "torch.nn.Conv1d", "torch.nn.Conv2d", "torch.nn.Conv3d",
        "torch.nn.ConvTranspose1d", "torch.nn.ConvTranspose2d", "torch.nn.ConvTranspose3d",
        "torch.nn.Linear",
    ],
    "reduction": [
        "torch.max", "torch.min", "torch.sum", "torch.mean", "torch.logsumexp",
        "torch.nn.GroupNorm", "torch.nn.LogSoftmax",
        "torch.nn.InstanceNorm2d", "torch.nn.InstanceNorm3d",
        "torch.nn.MaxPool1d", "torch.nn.MaxPool2d", "torch.nn.MaxPool3d",
        "torch.nn.AdaptiveAvgPool2d", "torch.nn.AdaptiveAvgPool3d",
        "torch.nn.BatchNorm1d", "torch.nn.BatchNorm2d", "torch.nn.BatchNorm3d",
        "torch.nn.AvgPool1d", "torch.nn.AvgPool2d", "torch.nn.AvgPool3d",
        "torch.nn.LayerNorm",
    ],
    "elementwise": [
        "torch.add", "torch.clamp", "torch.tanh", "torch.sub", "torch.mul", "torch.div",
        "torch.relu", "torch.sigmoid",
        "torch.nn.LeakyReLU", "torch.nn.Dropout", "torch.nn.GELU",
        "torch.nn.Hardswish", "torch.nn.Hardtanh", "torch.nn.Mish", "torch.nn.Softplus",
    ],
}

CORE_SEED_OPS = {"conv", "gemm", "norm", "pool", "softmax"}

# 算子到归一化类别的映射
OP_CATEGORY = {
    "conv": "conv", "gemm": "gemm", "linear": "gemm",
    "batch_norm": "norm", "layer_norm": "norm", "group_norm": "norm", "instance_norm": "norm",
    "pool": "pool", "maxpool": "pool", "avgpool": "pool",
    "softmax": "softmax", "log_softmax": "softmax",
    "reduce_max": "reduction", "reduce_min": "reduction",
    "reduce_sum": "reduction", "reduce_mean": "reduction",
    "logsumexp": "reduction",
    "relu": "activation", "sigmoid": "activation", "tanh": "activation",
    "gelu": "activation", "silu": "activation", "leaky_relu": "activation",
    "hardswish": "activation", "hardtanh": "activation", "mish": "activation",
    "softplus": "activation", "dropout": "activation",
    "add": "elementwise", "sub": "elementwise", "mul": "elementwise",
    "div": "elementwise", "clamp": "elementwise",
}


# ============================================================================
# CUDA源码中的回退检测 Patterns
# ============================================================================

ATEN_TORCH_FALLBACK_PATTERNS = {
    "conv": [
        r"at::conv\w+", r"torch::conv\w+", r"at::cudnn_convolution\w*",
    ],
    "gemm": [
        r"torch::matmul", r"torch::mm\b", r"torch::bmm\b",
        r"at::mm\b", r"at::bmm\b", r"at::matmul",
        r"at::linear\b", r"torch::linear\b",
        r"at::addmm\b", r"torch::addmm\b",
    ],
    "pool": [
        r"at::max_pool\w+", r"at::avg_pool\w+",
        r"at::adaptive_max_pool\w+", r"at::adaptive_avg_pool\w+",
        r"torch::max_pool\w+", r"torch::avg_pool\w+",
    ],
    "norm": [
        r"at::batch_norm\w*", r"at::layer_norm\w*", r"at::group_norm\w*",
        r"at::instance_norm\w*",
        r"at::native_batch_norm\w*", r"at::native_layer_norm\w*", r"at::native_group_norm\w*",
        r"torch::batch_norm\w*", r"torch::layer_norm\w*", r"torch::group_norm\w*",
    ],
    "softmax": [
        r"at::softmax\b", r"at::log_softmax\b",
        r"torch::softmax\b", r"torch::log_softmax\b",
    ],
    "activation": [
        r"at::relu\b", r"at::sigmoid\b", r"at::tanh\b", r"at::gelu\b",
        r"at::silu\b", r"at::leaky_relu\b", r"at::elu\b",
        r"torch::relu\b", r"torch::sigmoid\b", r"torch::tanh\b", r"torch::gelu\b",
    ],
    "reduction": [
        r"at::sum\b", r"at::mean\b", r"at::max\b", r"at::min\b",
        r"at::logsumexp\b",
        r"torch::sum\b", r"torch::mean\b", r"torch::max\b", r"torch::min\b",
    ],
    "embedding": [
        r"at::embedding\w*", r"torch::embedding\w*",
    ],
    "misc_torch": [
        r"F\.conv\w+", r"F\.linear\b",
        r"F\.max_pool\w+", r"F\.avg_pool\w+",
        r"F\.adaptive_max_pool\w+", r"F\.adaptive_avg_pool\w+",
        r"F\.batch_norm\b", r"F\.layer_norm\b", r"F\.group_norm\b", r"F\.instance_norm\b",
        r"F\.softmax\b", r"F\.log_softmax\b",
        r"F\.relu\b", r"F\.sigmoid\b", r"F\.tanh\b", r"F\.gelu\b", r"F\.silu\b",
        r"F\.leaky_relu\b", r"F\.elu\b", r"F\.embedding\b",
        r"F\.mish\b", r"F\.hardswish\b", r"F\.hardtanh\b", r"F\.softplus\b",
        r"F\.dropout\b",
    ],
}

CUBLAS_CUDNN_PATTERNS = {
    "cublas": [
        r"cublasSgemm\w*", r"cublasDgemm\w*", r"cublasHgemm\w*",
        r"cublasGemmEx\w*", r"cublasGemmStridedBatched\w*", r"cublasLtMatmul\w*",
    ],
    "cudnn": [
        r"cudnnConvolution\w+", r"cudnnPooling\w+",
        r"cudnnBatchNorm\w+", r"cudnnSoftmax\w+", r"cudnnActivation\w+",
    ],
}


# ============================================================================
# ModelNew.forward() 中检测 torch seed op 回退的 Patterns
# ============================================================================

FORWARD_SEED_OP_PATTERNS = {
    "conv": r"self\.conv\w*\(",
    "conv_F": r"F\.conv\w+\(",
    "gemm": r"torch\.matmul\(|torch\.mm\(|torch\.bmm\(",
    "linear": r"(?:self\.linear\w*\(|self\.fc\w*\(|F\.linear\()",
    "batch_norm": r"(?:self\.bn\w*\(|self\.batch_norm\w*\(|F\.batch_norm\()",
    "layer_norm": r"(?:self\.ln\w*\(|self\.layer_norm\w*\(|F\.layer_norm\()",
    "group_norm": r"(?:self\.gn\w*\(|self\.group_norm\w*\(|F\.group_norm\()",
    "instance_norm": r"(?:self\.instance_norm\w*\(|F\.instance_norm\()",
    "pool": r"(?:self\.pool\w*\(|self\.maxpool\w*\(|self\.avgpool\w*\(|self\.adaptive\w*\()",
    "pool_F": r"(?:F\.max_pool\w+\(|F\.avg_pool\w+\(|F\.adaptive_avg_pool\w+\(|F\.adaptive_max_pool\w+\()",
    "softmax": r"(?:F\.softmax\(|F\.log_softmax\(|torch\.softmax\()",
    "reduce_max": r"(?:torch\.max\([^)]*dim|\.max\([^)]*dim|torch\.max\(\s*\w+\s*,)",
    "reduce_min": r"(?:torch\.min\([^)]*dim|\.min\([^)]*dim|torch\.min\(\s*\w+\s*,)",
    "reduce_sum": r"(?:torch\.sum\(|\.sum\([^)]*dim|\.sum\([^)]*keepdim)",
    "reduce_mean": r"(?:torch\.mean\(|\.mean\([^)]*dim|\.mean\([^)]*keepdim)",
    "logsumexp": r"torch\.logsumexp\(",
    "relu": r"(?:F\.relu\(|torch\.relu\()",
    "sigmoid": r"(?:F\.sigmoid\(|torch\.sigmoid\()",
    "tanh": r"(?:F\.tanh\(|torch\.tanh\()",
    "gelu": r"(?:F\.gelu\()",
    "silu": r"(?:F\.silu\()",
    "leaky_relu": r"(?:F\.leaky_relu\()",
    "hardswish": r"(?:F\.hardswish\()",
    "hardtanh": r"(?:F\.hardtanh\()",
    "mish": r"(?:F\.mish\()",
    "softplus": r"(?:F\.softplus\()",
    "dropout": r"(?:F\.dropout\()",
    "clamp": r"(?:torch\.clamp\(|\.clamp\()",
}


# ============================================================================
# instruction 中提取 required ops 的 Patterns
# ============================================================================

REQUIRED_OPS_PATTERNS = {
    "conv": r"conv\d*d|Conv\d*d|ConvTranspose",
    "gemm": r"matmul|\.mm\(|\.bmm\(|F\.linear|self\.fc\b|self\.linear\b|nn\.Linear",
    "pool": r"max_pool|MaxPool|avg_pool|AvgPool|AdaptiveAvgPool|AdaptiveMaxPool",
    "norm": r"batch_norm|BatchNorm|layer_norm|LayerNorm|group_norm|GroupNorm|instance_norm|InstanceNorm",
    "softmax": r"softmax|Softmax|log_softmax|LogSoftmax",
    "activation": (
        r"(?<!\w)relu|ReLU|gelu|GELU|silu|SiLU|sigmoid|Sigmoid(?!Forward)"
        r"|(?<!\w)tanh|Tanh|leaky_relu|LeakyReLU|hardtanh|Hardtanh"
        r"|elu(?!de)|ELU|mish|Mish|swish|Swish|hardsigmoid|Hardsigmoid"
        r"|hardswish|Hardswish|softplus|Softplus"
    ),
    "reduction": r"torch\.max\(|torch\.min\(|torch\.sum\(|torch\.mean\(|torch\.logsumexp\(",
    "elementwise": r"torch\.add\(|torch\.sub\(|torch\.mul\(|torch\.div\(|torch\.clamp\(",
    "dropout": r"Dropout",
    "embedding": r"Embedding\(|nn\.Embedding",
}


# ============================================================================
# 数据解析
# ============================================================================

def extract_forward_body(instruction: str) -> str:
    pytorch_code = re.search(
        r"### Original PyTorch Operator:\n```python\n(.*?)```",
        instruction, re.DOTALL,
    )
    if not pytorch_code:
        return ""
    code = pytorch_code.group(1)
    forward = re.search(
        r"def forward\(self.*?(?=\n    def |\nclass |\ndef |\Z)", code, re.DOTALL,
    )
    return forward.group(0) if forward else ""


def extract_required_ops(instruction: str) -> set[str]:
    fwd = extract_forward_body(instruction)
    if not fwd:
        return set()
    ops = set()
    for op_name, pattern in REQUIRED_OPS_PATTERNS.items():
        if re.search(pattern, fwd):
            ops.add(op_name)
    return ops


def extract_cuda_sources(output: str) -> list[str]:
    """
    从output中提取所有CUDA/C++源码。
    优先返回含 __global__ 的CUDA kernel源码，过滤纯pybind绑定代码。
    """
    named_sources = re.findall(
        r"(\w+)\s*=\s*r?(?:\"\"\"|\'\'\')(.*?)(?:\"\"\"|\'\'\')",
        output, re.DOTALL,
    )

    kernel_sources = []
    binding_sources = []

    for var_name, src in named_sources:
        is_binding = (
            var_name in ("cpp_src", "cpp_source", "CPP_SOURCE", "_cpp_source", "cpp_sources", "cpp")
            or "PYBIND11" in src
            or "m.def(" in src
        )
        has_global = "__global__" in src
        if is_binding and not has_global:
            binding_sources.append(src)
        else:
            kernel_sources.append(src)

    if kernel_sources:
        return kernel_sources
    if binding_sources:
        return binding_sources
    return []


def extract_model_new_forward_body(output: str) -> str:
    """提取 ModelNew 类的 forward 方法体（严格基于缩进）"""
    match = re.search(
        r"class\s+ModelNew\s*\(.*?\).*?def\s+forward\s*\(self.*?\)\s*(?:->.*?)?:",
        output, re.DOTALL,
    )
    if not match:
        return ""
    rest = output[match.end():]
    lines = rest.split("\n")
    body_lines = []
    for line in lines:
        if not line or line[0] == " " or line[0] == "\t":
            body_lines.append(line)
        else:
            if re.match(r"\S", line) and body_lines:
                break
            body_lines.append(line)
    body = "\n".join(body_lines)
    end_match = re.search(r"\n    def \w", body)
    if end_match:
        body = body[:end_match.start()]
    return body


def strip_comments(code: str) -> str:
    code = re.sub(r"//.*", "", code)
    code = re.sub(r"/\*.*?\*/", "", code, flags=re.DOTALL)
    return code


def strip_python_comments(code: str) -> str:
    code = re.sub(r"#.*", "", code)
    code = re.sub(r'""".*?"""', "", code, flags=re.DOTALL)
    code = re.sub(r"'''.*?'''", "", code, flags=re.DOTALL)
    return code


# ============================================================================
# Hack检测器
# ============================================================================

@dataclass
class ForwardHackInfo:
    ops_on_main_path: dict = field(default_factory=dict)    # op -> [match_strings]
    ops_in_training_guard: dict = field(default_factory=dict)
    ops_in_param_guard: dict = field(default_factory=dict)
    has_fused_call: bool = False

    @property
    def is_unconditional(self) -> bool:
        return bool(self.ops_on_main_path) and not self.has_fused_call

    @property
    def all_fallback_ops(self) -> set[str]:
        ops = set(self.ops_on_main_path.keys())
        ops.update(self.ops_in_training_guard.keys())
        ops.update(self.ops_in_param_guard.keys())
        return ops

    @property
    def main_path_categories(self) -> set[str]:
        cats = set()
        for op in self.ops_on_main_path:
            cat = OP_CATEGORY.get(op, op)
            cats.add(cat)
        return cats


@dataclass
class HackReport:
    entry_index: int
    required_ops: set = field(default_factory=set)
    has_global_kernel: bool = True
    aten_torch_fallbacks: dict = field(default_factory=dict)
    cublas_cudnn_fallbacks: dict = field(default_factory=dict)
    python_torch_in_cuda: bool = False
    no_cuda_source: bool = False
    fallback_on_required: dict = field(default_factory=dict)
    forward_hack: ForwardHackInfo = field(default_factory=ForwardHackInfo)

    @property
    def hack_types(self) -> set[str]:
        types = set()
        if self.aten_torch_fallbacks:
            types.add("A")
        if self.cublas_cudnn_fallbacks:
            types.add("B")
        if not self.has_global_kernel:
            types.add("C")
        if self.python_torch_in_cuda:
            types.add("D")
        if self.fallback_on_required:
            types.add("E")
        if self.forward_hack.ops_on_main_path:
            types.add("F")
        return types

    @property
    def severity(self) -> int:
        """
        0=clean, 1=minor, 2=moderate, 3=severe, 4=critical
        F类hack单独评估，与A-E叠加取最高
        """
        sev = 0

        # A-E severity
        if self.no_cuda_source:
            sev = max(sev, 4)
        if not self.has_global_kernel and (self.aten_torch_fallbacks or self.cublas_cudnn_fallbacks):
            sev = max(sev, 4)
        if self.fallback_on_required:
            core = any(op in CORE_SEED_OPS for op in self.fallback_on_required)
            sev = max(sev, 3 if core else 2)
        if self.cublas_cudnn_fallbacks:
            sev = max(sev, 2)
        if self.aten_torch_fallbacks:
            non_misc = {k for k in self.aten_torch_fallbacks if k != "misc_torch"}
            sev = max(sev, 2 if non_misc else 1)
        if self.python_torch_in_cuda:
            sev = max(sev, 1)
        if not self.has_global_kernel and sev == 0:
            sev = 1

        # F类hack severity
        fh = self.forward_hack
        if fh.ops_on_main_path:
            core_on_path = fh.main_path_categories & CORE_SEED_OPS
            if fh.is_unconditional:
                sev = max(sev, 4)
            elif core_on_path:
                sev = max(sev, 3)
            else:
                sev = max(sev, 2)
        if fh.ops_in_training_guard:
            sev = max(sev, 1)
        if fh.ops_in_param_guard:
            cats = {OP_CATEGORY.get(op, op) for op in fh.ops_in_param_guard}
            core_guarded = cats & CORE_SEED_OPS
            sev = max(sev, 2 if core_guarded else 1)

        return sev


def _detect_forward_hacks(output: str) -> ForwardHackInfo:
    info = ForwardHackInfo()
    body = extract_model_new_forward_body(output)
    if not body:
        return info

    body_clean = strip_python_comments(body)

    info.has_fused_call = bool(re.search(
        r"fused_ops\.|_module\.|cuda_module\.|custom_ops\.|_ext\.|_cuda\.",
        body_clean,
    ))

    # 分离代码区域
    # 1. training guard: if self.training: ...
    training_blocks = re.findall(
        r"if\s+self\.training\s*:(.+?)(?=\n        (?:else|elif|return|if\s+)|$)",
        body_clean, re.DOTALL,
    )
    training_code = "\n".join(training_blocks)

    # 2. CUDA fallback guard: if not is_cuda / if fused_ops is None
    fallback_blocks = re.findall(
        r"if\s+(?:.*?(?:not\s+\w+\.is_cuda|fused_ops\s+is\s+None|_module\s+is\s+None"
        r"|not\s+torch\.cuda\.is_available|_ext\s+is\s+None)).*?:"
        r"(.+?)(?=\n        (?:else|elif|return\s+fused|if\s+)|$)",
        body_clean, re.DOTALL,
    )
    fallback_code = "\n".join(fallback_blocks)

    # 3. parameter conditional guard: if self.dim != X etc
    param_blocks = re.findall(
        r"if\s+self\.\w+\s*(?:!=|==|>|<|not in|in)\s*.+?:"
        r"(.+?)(?=\n        (?:else|elif|return\s+fused|if\s+)|$)",
        body_clean, re.DOTALL,
    )
    param_code = "\n".join(param_blocks)

    # 4. Main path: everything minus the above blocks
    main_code = body_clean
    for block in training_blocks + fallback_blocks + param_blocks:
        main_code = main_code.replace(block, "", 1)

    def scan_ops(code: str) -> dict:
        found = {}
        for op_name, pat in FORWARD_SEED_OP_PATTERNS.items():
            matches = re.findall(pat, code)
            if matches:
                found[op_name] = matches
        return found

    info.ops_on_main_path = scan_ops(main_code)
    info.ops_in_training_guard = scan_ops(training_code)
    info.ops_in_param_guard = scan_ops(param_code)

    return info


def detect_hacks(entry: dict, index: int) -> HackReport:
    report = HackReport(entry_index=index)
    instruction = entry["instruction"]
    output = entry["output"]

    report.required_ops = extract_required_ops(instruction)

    # --- CUDA source level checks ---
    sources = extract_cuda_sources(output)
    if not sources:
        report.no_cuda_source = True
        all_code_blocks = re.findall(r"```(?:cpp|cuda|c\+\+)?\n(.*?)```", output, re.DOTALL)
        if all_code_blocks:
            sources = all_code_blocks

    if sources:
        merged_src = "\n".join(sources)
        cleaned = strip_comments(merged_src)

        report.has_global_kernel = bool(re.search(r"__global__", cleaned))

        for op_cat, patterns in ATEN_TORCH_FALLBACK_PATTERNS.items():
            matches = []
            for pat in patterns:
                matches.extend(re.findall(pat, cleaned))
            if matches:
                report.aten_torch_fallbacks[op_cat] = matches

        for lib_cat, patterns in CUBLAS_CUDNN_PATTERNS.items():
            matches = []
            for pat in patterns:
                matches.extend(re.findall(pat, cleaned))
            if matches:
                report.cublas_cudnn_fallbacks[lib_cat] = matches

        for src in sources:
            has_cuda_code = "#include" in src or "__global__" in src
            has_python_import = bool(re.search(r"^import torch|^from torch", src, re.MULTILINE))
            if has_cuda_code and has_python_import:
                is_python_wrapper = bool(re.search(
                    r"load_inline|cpp_extension|torch\.utils\.cpp_extension", src,
                ))
                if not is_python_wrapper:
                    report.python_torch_in_cuda = True
                    break

        # required ops fallback in CUDA source
        conv_fb = (
            ATEN_TORCH_FALLBACK_PATTERNS.get("conv", [])
            + [p for p in ATEN_TORCH_FALLBACK_PATTERNS.get("misc_torch", []) if "conv" in p]
        )
        gemm_fb = (
            ATEN_TORCH_FALLBACK_PATTERNS.get("gemm", [])
            + [p for p in ATEN_TORCH_FALLBACK_PATTERNS.get("misc_torch", []) if "linear" in p]
            + CUBLAS_CUDNN_PATTERNS.get("cublas", [])
        )
        pool_fb = (
            ATEN_TORCH_FALLBACK_PATTERNS.get("pool", [])
            + [p for p in ATEN_TORCH_FALLBACK_PATTERNS.get("misc_torch", []) if "pool" in p]
        )
        norm_fb = (
            ATEN_TORCH_FALLBACK_PATTERNS.get("norm", [])
            + [p for p in ATEN_TORCH_FALLBACK_PATTERNS.get("misc_torch", []) if "norm" in p]
        )
        softmax_fb = (
            ATEN_TORCH_FALLBACK_PATTERNS.get("softmax", [])
            + [p for p in ATEN_TORCH_FALLBACK_PATTERNS.get("misc_torch", []) if "softmax" in p]
        )
        reduction_fb = ATEN_TORCH_FALLBACK_PATTERNS.get("reduction", [])

        op_to_fb = {
            "conv": conv_fb, "gemm": gemm_fb, "pool": pool_fb,
            "norm": norm_fb, "softmax": softmax_fb, "reduction": reduction_fb,
        }

        for op in report.required_ops:
            fb_patterns = op_to_fb.get(op, [])
            if not fb_patterns:
                continue
            matches = []
            for pat in fb_patterns:
                matches.extend(re.findall(pat, cleaned))
            if matches:
                report.fallback_on_required[op] = matches

    # --- ModelNew.forward() level checks ---
    report.forward_hack = _detect_forward_hacks(output)

    return report


# ============================================================================
# 过滤策略
# ============================================================================

def should_filter(report: HackReport, level: int) -> bool:
    """
    Level 1 (宽松): severity >= 4
        无kernel全靠回退 / 无CUDA源码 / forward无条件全回退
    Level 2 (中等): severity >= 3
        核心算子(conv/gemm/norm/pool/softmax)在forward或CUDA源码中回退
    Level 3 (严格): severity >= 2
        任何seed op在主路径回退 / cuBLAS/cuDNN / ATen回退
    Level 4 (极严格): severity >= 1
        任何hack迹象(含training guard回退)
    """
    threshold = {1: 4, 2: 3, 3: 2, 4: 1}
    return report.severity >= threshold.get(level, 2)


# ============================================================================
# 报告生成
# ============================================================================

def generate_report(data: list[dict], reports: list[HackReport]) -> str:
    lines = []
    lines.append("=" * 70)
    lines.append("SFT数据Hack检测报告 (含ModelNew.forward()检测)")
    lines.append("=" * 70)
    lines.append(f"\n总数据量: {len(data)}")

    hack_count = sum(1 for r in reports if r.hack_types)
    lines.append(f"检测到hack的数据: {hack_count} ({hack_count/len(data)*100:.1f}%)")
    lines.append(f"干净数据: {len(data) - hack_count} ({(len(data)-hack_count)/len(data)*100:.1f}%)")

    lines.append("\n--- Hack类型分布 ---")
    type_names = {
        "A": "CUDA源码ATen/Torch算子回退",
        "B": "CUDA源码cuBLAS/cuDNN库回退",
        "C": "无自定义__global__ Kernel",
        "D": "Python层torch调用混入CUDA",
        "E": "CUDA源码部分实现Hack(required op回退)",
        "F": "ModelNew.forward()中torch seed op回退 ★",
    }
    for t, name in type_names.items():
        cnt = sum(1 for r in reports if t in r.hack_types)
        if cnt > 0:
            lines.append(f"  Type {t} ({name}): {cnt}")

    # F type breakdown
    f_entries = [r for r in reports if "F" in r.hack_types]
    if f_entries:
        lines.append("\n--- Type F (forward回退) 详细分布 ---")
        main_path_core = sum(
            1 for r in f_entries
            if r.forward_hack.main_path_categories & CORE_SEED_OPS
        )
        main_path_non_core = sum(
            1 for r in f_entries
            if r.forward_hack.ops_on_main_path
            and not (r.forward_hack.main_path_categories & CORE_SEED_OPS)
        )
        uncond = sum(1 for r in f_entries if r.forward_hack.is_unconditional)
        lines.append(f"  主路径核心算子回退 (conv/gemm/norm/pool/softmax): {main_path_core}")
        lines.append(f"  主路径非核心算子回退 (activation/reduction/elementwise): {main_path_non_core}")
        lines.append(f"  完全无条件回退 (无fused_ops调用): {uncond}")

        training_guard = sum(1 for r in reports if r.forward_hack.ops_in_training_guard)
        param_guard = sum(1 for r in reports if r.forward_hack.ops_in_param_guard)
        lines.append(f"  训练模式回退 (if self.training): {training_guard}")
        lines.append(f"  参数条件回退 (if self.dim != X): {param_guard}")

        # Forward op breakdown
        fwd_op_counter = Counter()
        for r in f_entries:
            for op in r.forward_hack.ops_on_main_path:
                cat = OP_CATEGORY.get(op, op)
                fwd_op_counter[f"{cat}/{op}"] += 1
        lines.append("\n  Forward主路径回退算子 Top 20:")
        for name, cnt in fwd_op_counter.most_common(20):
            lines.append(f"    {name}: {cnt}")

    lines.append("\n--- 严重程度分布 ---")
    severity_names = {0: "Clean", 1: "Minor", 2: "Moderate", 3: "Severe", 4: "Critical"}
    for sev in range(5):
        cnt = sum(1 for r in reports if r.severity == sev)
        lines.append(f"  Severity {sev} ({severity_names[sev]}): {cnt}")

    lines.append("\n--- 各级过滤后剩余数据量 ---")
    for level in [1, 2, 3, 4]:
        remaining = sum(1 for r in reports if not should_filter(r, level))
        filtered = len(data) - remaining
        lines.append(
            f"  Level {level}: 过滤 {filtered} 条, 剩余 {remaining} 条 "
            f"({remaining/len(data)*100:.1f}%)"
        )

    lines.append("\n--- CUDA源码回退算子统计 ---")
    op_counter = Counter()
    for r in reports:
        for op, matches in r.fallback_on_required.items():
            op_counter[op] += 1
    for op, cnt in op_counter.most_common():
        lines.append(f"  {op} fallback: {cnt} 条")

    lib_counter = Counter()
    for r in reports:
        for lib, matches in r.cublas_cudnn_fallbacks.items():
            lib_counter[lib] += 1
    if lib_counter:
        lines.append("\n--- 库函数调用统计 ---")
        for lib, cnt in lib_counter.most_common():
            lines.append(f"  {lib}: {cnt} 条")

    lines.append("\n--- Hack数据样例 (severity >= 3, 前15条) ---")
    severe = [(r.entry_index, r) for r in reports if r.severity >= 3]
    for idx, r in severe[:15]:
        lines.append(f"\n  Entry {idx}: types={r.hack_types}, severity={r.severity}")
        lines.append(f"    required_ops: {r.required_ops}")
        if r.forward_hack.ops_on_main_path:
            ops_summary = {k: len(v) for k, v in r.forward_hack.ops_on_main_path.items()}
            lines.append(f"    forward主路径回退: {ops_summary}")
        if r.forward_hack.ops_in_training_guard:
            ops_summary = {k: len(v) for k, v in r.forward_hack.ops_in_training_guard.items()}
            lines.append(f"    forward训练回退: {ops_summary}")
        if r.fallback_on_required:
            lines.append(f"    CUDA源码required回退: {dict(r.fallback_on_required)}")
        if r.cublas_cudnn_fallbacks:
            lines.append(f"    cuBLAS/cuDNN: {dict(r.cublas_cudnn_fallbacks)}")

    lines.append("\n" + "=" * 70)
    return "\n".join(lines)


# ============================================================================
# 主程序
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="SFT训练数据Hack检测与过滤工具 (含ModelNew.forward()检测)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
过滤层级说明:
  Level 1 (宽松):   过滤无kernel全回退 + forward完全无条件回退
  Level 2 (中等):   额外过滤核心算子(conv/gemm/norm/pool)的forward/CUDA回退
  Level 3 (严格):   额外过滤所有seed op的forward主路径回退、cuBLAS/cuDNN
  Level 4 (极严格): 过滤所有hack迹象(含training guard回退)

用法示例:
  python sft_data_hack_detector.py input.json --report
  python sft_data_hack_detector.py input.json --filter-level 2 -o cleaned.json
  python sft_data_hack_detector.py input.json --filter-level 2 -o clean.json --dump-filtered bad.json
        """,
    )
    parser.add_argument("input", help="输入SFT数据JSON文件路径")
    parser.add_argument("--report", action="store_true", help="输出检测报告到stdout")
    parser.add_argument(
        "--filter-level", type=int, choices=[1, 2, 3, 4], default=None,
        help="过滤层级 (1=宽松, 2=中等, 3=严格, 4=极严格)",
    )
    parser.add_argument("--output", "-o", help="过滤后数据的输出路径")
    parser.add_argument("--dump-filtered", help="被过滤掉的数据输出路径")
    parser.add_argument("--dump-report", help="检测报告输出到文件(JSON格式)")
    parser.add_argument("--show-hack-indices", action="store_true", help="打印所有hack数据的index")

    args = parser.parse_args()

    if not args.report and not args.filter_level and not args.dump_report and not args.show_hack_indices:
        args.report = True

    print(f"加载数据: {args.input}", file=sys.stderr)
    with open(args.input) as f:
        data = json.load(f)
    print(f"数据量: {len(data)}", file=sys.stderr)

    print("运行hack检测 (CUDA源码 + ModelNew.forward())...", file=sys.stderr)
    reports = []
    for i, entry in enumerate(data):
        report = detect_hacks(entry, i)
        reports.append(report)
        if (i + 1) % 1000 == 0:
            print(f"  已处理 {i + 1}/{len(data)}", file=sys.stderr)

    if args.report:
        print(generate_report(data, reports))

    if args.show_hack_indices:
        for r in reports:
            if r.hack_types:
                fwd_ops = list(r.forward_hack.ops_on_main_path.keys()) if r.forward_hack.ops_on_main_path else []
                print(
                    f"idx={r.entry_index} types={r.hack_types} "
                    f"severity={r.severity} fwd_ops={fwd_ops} "
                    f"cuda_fb={dict(r.fallback_on_required)}"
                )

    if args.dump_report:
        report_data = []
        for r in reports:
            report_data.append({
                "index": r.entry_index,
                "required_ops": sorted(r.required_ops),
                "has_global_kernel": r.has_global_kernel,
                "hack_types": sorted(r.hack_types),
                "severity": r.severity,
                "aten_torch_fallbacks": r.aten_torch_fallbacks,
                "cublas_cudnn_fallbacks": r.cublas_cudnn_fallbacks,
                "fallback_on_required": r.fallback_on_required,
                "python_torch_in_cuda": r.python_torch_in_cuda,
                "no_cuda_source": r.no_cuda_source,
                "forward_hack": {
                    "ops_on_main_path": r.forward_hack.ops_on_main_path,
                    "ops_in_training_guard": r.forward_hack.ops_in_training_guard,
                    "ops_in_param_guard": r.forward_hack.ops_in_param_guard,
                    "has_fused_call": r.forward_hack.has_fused_call,
                },
            })
        with open(args.dump_report, "w") as f:
            json.dump(report_data, f, indent=2, ensure_ascii=False)
        print(f"检测报告已保存到: {args.dump_report}", file=sys.stderr)

    if args.filter_level is not None:
        level = args.filter_level
        kept = []
        filtered_out = []
        for entry, report in zip(data, reports):
            if should_filter(report, level):
                filtered_out.append(entry)
            else:
                kept.append(entry)

        print(
            f"\nLevel {level} 过滤结果: "
            f"保留 {len(kept)}/{len(data)} ({len(kept)/len(data)*100:.1f}%), "
            f"过滤 {len(filtered_out)} 条",
            file=sys.stderr,
        )

        if args.output:
            with open(args.output, "w") as f:
                json.dump(kept, f, indent=2, ensure_ascii=False)
            print(f"过滤后数据已保存到: {args.output}", file=sys.stderr)

        if args.dump_filtered:
            with open(args.dump_filtered, "w") as f:
                json.dump(filtered_out, f, indent=2, ensure_ascii=False)
            print(f"被过滤数据已保存到: {args.dump_filtered}", file=sys.stderr)


# ============================================================================
# Sandbox adapter
# ============================================================================

def check_for_hacks(
    torch_code: str,
    triton_code: str,
    filter_level: int = 2,
) -> dict | None:
    """Check triton_code for hack/fallback patterns.

    Wraps the SFT detector for use inside zmq_tbench.
    ``torch_code`` is treated as *instruction* (original PyTorch impl),
    ``triton_code`` as *output* (LLM-generated CUDA/Triton impl).

    Args:
        torch_code: PyTorch reference (used to infer required ops).
        triton_code: LLM-generated kernel code.
        filter_level: 1-4, higher = stricter (default 2 = moderate).

    Returns:
        None if clean, otherwise a dict with hack details.
    """
    entry = {
        "instruction": (
            "### Original PyTorch Operator:\n```python\n"
            + torch_code
            + "\n```"
        ),
        "output": triton_code,
    }
    report = detect_hacks(entry, 0)
    if not should_filter(report, filter_level):
        return None
    return {
        "hack_types": sorted(report.hack_types),
        "severity": report.severity,
        "forward_fallback_ops": sorted(report.forward_hack.ops_on_main_path.keys())
            if report.forward_hack.ops_on_main_path else [],
        "cuda_fallback_ops": {k: v for k, v in report.fallback_on_required.items()},
        "has_global_kernel": report.has_global_kernel,
        "no_cuda_source": report.no_cuda_source,
    }


if __name__ == "__main__":
    main()
