"""token/cost 归集口径与一致性守卫（plaita#37）。

设计裁决（issue #37 线程，维护者四句拍板）：

* 总量落 ``ExecutionState.usage``（**可选**，``None`` = 老状态 ⇒ 零回归，
  与 ``flow_hash``/``engine_version``/``node_timings`` 同款套路）；
* 每节点**不新增列**：真源是 ``context["$NODE"][node_id]["usage"]``
  （LLM 形状节点输出本来就有），聚合**只读不写**；
* ``source`` ∈ ``observed|estimated|mixed``：把「估算值混进观测值」这个展示层
  事故在 schema 层堵死；
* 精度错（普通 ``ValueError``）与值不自洽（:class:`TokenCountMismatch`）
  **分开**——处置动作不同（调用方 bug vs 数据被改过），混一类会让告警路由错。

本模块只依赖标准库：它属于存储层口径，**不得** import ``plaita.obs``
（Langfuse 是可选 extra，且方向是 obs → 引擎，不是引擎 → obs）。
"""

from __future__ import annotations

import math
from typing import Any, Dict, List, Optional

__all__ = [
    "MAX_PRECISION",
    "TOKENS_PER_COUNT",
    "TOKEN_COUNT_TOLERANCE",
    "USAGE_SOURCES",
    "TokenCountMismatch",
    "aggregate_node_usage",
    "format_count",
    "format_tokens_per_count",
    "guard_token_counter",
    "merge_usage",
    "node_results_from_context",
    "node_usage_breakdown",
    "normalize_usage",
    "precision_of",
    "ratio_is_consistent",
    "usage_view",
    "validate_count",
]

#: tokens 与 count 的约定比例（``patch_count`` 回填时的默认口径）。
TOKENS_PER_COUNT = 4
#: ``|tokens/count − tokens_per_count|`` 的相对容差（近似计数器的边界）。
TOKEN_COUNT_TOLERANCE = 0.05
#: ``tokens_per_count`` 的小数位上限——超限是**精度错**（调用方 bug），
#: 与「值不自洽」分开抛。
MAX_PRECISION = 6
#: usage 来源三态。
USAGE_SOURCES = ("observed", "estimated", "mixed")

#: 各口径键名 → 本模块规范键。
_PROMPT_KEYS = ("prompt_tokens", "input_tokens", "input")
_COMPLETION_KEYS = ("completion_tokens", "output_tokens", "output")
_TOTAL_KEYS = ("total_tokens", "total")


class TokenCountMismatch(Exception):
    """``count`` 与 ``tokens_per_count``/``total_tokens`` **值不自洽**。

    与「精度错」（普通 :class:`ValueError`）**刻意分开**，且**刻意不继承
    ``ValueError``**（同 ``plaita.storage.base.StateSerializationError`` 的
    先例）：调用方的 ``except ValueError`` 不得把它当成普通入参错误吞掉——
    「历史成本被改过」需要单独的路由与留痕。
    """


def _int_or_none(value: Any) -> Optional[int]:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def precision_of(value: Any) -> int:
    """小数位数（不含科学计数法尾巴）。非数值/整数 ⇒ 0。"""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return 0
    if isinstance(value, int) or not math.isfinite(float(value)):
        return 0
    text = repr(float(value))
    if "e" in text or "E" in text:
        # 极小值用科学计数法：按容差内可表示的最坏精度处理
        return MAX_PRECISION
    _, _, frac = text.partition(".")
    return len(frac.rstrip("0"))


def normalize_usage(raw: Any) -> Optional[Dict[str, Any]]:
    """把节点返回的 usage 形状归一为规范键；**无任何可用计数** ⇒ ``None``。

    已知形状：OpenAI（``prompt_tokens``/``completion_tokens``）、Anthropic
    （``input_tokens``/``output_tokens``）、Langfuse（``input``/``output``/
    ``total``）。``total`` 缺失时由 input+output 兜底。

    ``{"total_tokens": 0}`` 归一为**含 0 的 dict**（采集到 0），不是 ``None``
    （未采集）——这条区分是本单展示口径的地基。
    """
    if not isinstance(raw, dict):
        return None

    def _pick(keys) -> Optional[int]:
        for key in keys:
            got = _int_or_none(raw.get(key))
            if got is not None:
                return got
        return None

    prompt = _pick(_PROMPT_KEYS)
    completion = _pick(_COMPLETION_KEYS)
    total = _pick(_TOTAL_KEYS)
    if prompt is None and completion is None and total is None:
        return None
    prompt = prompt or 0
    completion = completion or 0
    if total is None:
        total = prompt + completion
    out: Dict[str, Any] = {
        "prompt_tokens": prompt,
        "completion_tokens": completion,
        "total_tokens": total,
    }
    source = raw.get("source")
    if source in ("observed", "estimated"):
        out["source"] = source
    precision = raw.get("precision")
    if isinstance(precision, int) and not isinstance(precision, bool) and precision >= 0:
        out["precision"] = precision
    return out


def _source_for(norm: Dict[str, Any], result: Dict[str, Any]) -> str:
    """归一后的 usage 来源：只认 ``observed|estimated``，其余按观测值兜底。

    未知来源**不得**静默当成观测值蒙混（那正是「估算值混进观测值」的事故面）：
    调用方要标注估算，必须显式写 ``source="estimated"`` 或节点结果的
    ``usage_source``。
    """
    source = norm.get("source") or result.get("usage_source") or "observed"
    return source if source in ("observed", "estimated") else "observed"


def aggregate_node_usage(node_results: Any) -> Optional[Dict[str, Any]]:
    """按节点读取 ``usage`` 并聚合（**只读**，不新增第二真相源）。

    返回 ``{prompt_tokens, completion_tokens, total_tokens, llm_calls, models[],
    source, precision?}``；**没有任何节点报 usage ⇒ None**（未采集）。

    ``count``/``tokens_per_count`` **不在这里编造**：它们属于 ``patch_count``
    的回填语义（历史 run 的补写 / 估算），由调用方显式给出。
    """
    if not isinstance(node_results, dict):
        return None
    prompt = completion = total = llm_calls = 0
    models: List[str] = []
    sources = set()
    precision = 0
    for result in node_results.values():
        if not isinstance(result, dict):
            continue
        norm = normalize_usage(result.get("usage"))
        if norm is None:
            continue
        llm_calls += 1
        prompt += norm["prompt_tokens"]
        completion += norm["completion_tokens"]
        total += norm["total_tokens"]
        model = result.get("model")
        if isinstance(model, str) and model and model not in models:
            models.append(model)
        sources.add(_source_for(norm, result))
        if isinstance(norm.get("precision"), int):
            precision = max(precision, norm["precision"])
    if llm_calls == 0:
        return None
    if sources == {"estimated"}:
        source = "estimated"
    elif sources == {"observed"}:
        source = "observed"
    else:
        source = "mixed"
    return {
        "prompt_tokens": prompt,
        "completion_tokens": completion,
        "total_tokens": total,
        "llm_calls": llm_calls,
        "models": models,
        "source": source,
        "precision": precision,
    }


def merge_usage(first: Optional[Dict[str, Any]],
                second: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """两段 usage 相加（多进程/多段推进时合并，不丢任一段）。"""
    parts = [u for u in (first, second) if isinstance(u, dict) and u]
    if not parts:
        return None
    if len(parts) == 1:
        return dict(parts[0])
    totals = {k: 0 for k in ("prompt_tokens", "completion_tokens", "total_tokens", "llm_calls")}
    models: List[str] = []
    sources = set()
    precision = 0
    for usage in parts:
        for key in totals:
            value = usage.get(key)
            if isinstance(value, int) and not isinstance(value, bool):
                totals[key] += value
        for model in usage.get("models") or []:
            if isinstance(model, str) and model not in models:
                models.append(model)
        src = usage.get("source")
        sources.add(src if src in ("observed", "estimated") else "observed")
        if isinstance(usage.get("precision"), int):
            precision = max(precision, usage["precision"])
    if sources == {"estimated"}:
        source = "estimated"
    elif sources == {"observed"}:
        source = "observed"
    else:
        source = "mixed"
    merged: Dict[str, Any] = {**totals, "models": models, "source": source,
                              "precision": precision}
    # count/tokens_per_count 属于补写语义：任一段有就带上（同段值冲突时以后段为准）
    for usage in parts:
        for key in ("count", "tokens_per_count"):
            if key in usage and usage[key] is not None:
                merged[key] = usage[key]
    return merged


def node_usage_breakdown(node_results: Any) -> Dict[str, Dict[str, Any]]:
    """每节点 usage 的**只读投影**（真源 = ``context["$NODE"][id]["usage"]``）。

    与 :func:`aggregate_node_usage` 同源同口径，差别只在粒度：本函数给
    「哪几个节点花了多少」，聚合函数给 run 级合计。**不新增第二真相源**——
    不写回 context、不建列，只在读取侧把已有值折算成稳定形状。

    返回 ``{node_id: {prompt_tokens, completion_tokens, total_tokens, source,
    precision?, model?}}``；**没有任何节点报 usage ⇒ 空 dict**（调用方据此
    保留「未采集」语义，绝不折算成 0）。明确报了 ``total_tokens=0`` 的节点
    **在表里**（采集到 0）——这两者可分是本模块的地基。
    """
    out: Dict[str, Dict[str, Any]] = {}
    if not isinstance(node_results, dict):
        return out
    for node_id, result in node_results.items():
        if not isinstance(result, dict):
            continue
        norm = normalize_usage(result.get("usage"))
        if norm is None:
            continue
        entry: Dict[str, Any] = dict(norm)
        entry["source"] = _source_for(norm, result)
        model = result.get("model")
        if isinstance(model, str) and model:
            entry["model"] = model
        out[str(node_id)] = entry
    return out


def usage_view(usage: Any) -> Optional[Dict[str, Any]]:
    """把 usage 折算成列表/详情可判读的**展示视图**；``None`` ⇒ 未采集。

    纪律（plaita#37 P2）：

    - ``None``/空 dict ⇒ ``None``：**「未采集」与「0」严格可分**。采集到 0 的
      run 返回的是含 ``total_tokens=0`` 的对象，读取方不得把 ``None`` 渲染成 0；
    - ``estimated`` 布尔 + ``source`` 三态：``source != "observed"``（含
      ``mixed``）必须显式标注，**估算值不得与观测值混同**；
    - ``count``/``tokens_per_count`` 属 ``patch_count`` 回填口径，缺失一律
      ``None``（未采集，不是 0），并给出人类可读串（``None`` ⇒ 「未采集」）。
    """
    if not isinstance(usage, dict) or not usage:
        return None
    view: Dict[str, Any] = {}
    for key in ("prompt_tokens", "completion_tokens", "total_tokens", "llm_calls"):
        value = usage.get(key)
        if isinstance(value, int) and not isinstance(value, bool):
            view[key] = value
    total = view.get("total_tokens")
    view["total_tokens_display"] = format_count(total)
    models = usage.get("models")
    if isinstance(models, list):
        view["models"] = [m for m in models if isinstance(m, str)]
    source = usage.get("source")
    source = source if source in USAGE_SOURCES else "observed"
    view["source"] = source
    view["estimated"] = source != "observed"
    precision = usage.get("precision")
    if isinstance(precision, int) and not isinstance(precision, bool):
        view["precision"] = precision
    count = usage.get("count")
    view["count"] = count if isinstance(count, int) and not isinstance(count, bool) else None
    tpc = usage.get("tokens_per_count")
    view["tokens_per_count"] = (
        tpc if isinstance(tpc, (int, float)) and not isinstance(tpc, bool) else None
    )
    view["count_display"] = format_count(view["count"])
    view["tokens_per_count_display"] = format_tokens_per_count(view["tokens_per_count"])
    return view


def node_results_from_context(context: Any) -> Dict[str, Any]:
    """从持久化 context 取 ``$NODE`` 表（兼容自定义 ``EXPRESS_PREFIX``）。"""
    if not isinstance(context, dict):
        return {}
    prefix = context.get("EXPRESS_PREFIX") or "$"
    results = context.get(f"{prefix}NODE")
    if not isinstance(results, dict):
        results = context.get("$NODE")
    return results if isinstance(results, dict) else {}


def guard_token_counter(value: Any, *, field: str = "count") -> int:
    """计数字段守卫：非负整数，拒绝 bool / float / 负数 / 非数。"""
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field} 必须是非负整数，得到 {value!r}")
    if value < 0:
        raise ValueError(f"{field} 不得为负，得到 {value!r}")
    return value


def _guard_tokens_per_count(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"tokens_per_count 必须是数值，得到 {value!r}")
    num = float(value)
    if not math.isfinite(num):
        raise ValueError(f"tokens_per_count 必须是有限值，得到 {value!r}")
    if num < 0:
        raise ValueError(f"tokens_per_count 不得为负，得到 {value!r}")
    if precision_of(num) > MAX_PRECISION:
        raise ValueError(
            f"tokens_per_count 精度超限：最多 {MAX_PRECISION} 位小数，得到 {value!r}"
        )
    return num


def ratio_is_consistent(total_tokens: Any, count: Any, tokens_per_count: Any,
                        *, tolerance: float = TOKEN_COUNT_TOLERANCE) -> bool:
    """``|total/count − tokens_per_count| ≤ tolerance``；count<=0 时无定义 ⇒ False。"""
    count = guard_token_counter(count)
    if count <= 0:
        return False
    if isinstance(total_tokens, bool) or not isinstance(total_tokens, (int, float)):
        return False
    per = _guard_tokens_per_count(tokens_per_count)
    try:
        return abs(float(total_tokens) / count - per) <= tolerance
    except (TypeError, ValueError, ZeroDivisionError):
        return False


def validate_count(count: Any, *, tokens_per_count: Any = None,
                   total_tokens: Any = None) -> int:
    """``count`` 落盘前守卫。

    抛错分两类（**刻意分开**）：

    * :class:`ValueError`——入参精度/类型错（调用方 bug）：tokens_per_count 小数位
      > :data:`MAX_PRECISION`、负值、非有限值；count 非非负整数；
    * :class:`TokenCountMismatch`——值不自洽：``count>0`` 且给定
      ``tokens_per_count``/``total_tokens`` 时比例越界（数据被改过）。
    """
    count = guard_token_counter(count)
    if tokens_per_count is None:
        return count
    per = _guard_tokens_per_count(tokens_per_count)      # 精度/有限性先于自洽性
    if total_tokens is None or count <= 0:
        return count
    if isinstance(total_tokens, bool) or not isinstance(total_tokens, (int, float)):
        raise ValueError(f"total_tokens 必须是数值，得到 {total_tokens!r}")
    if abs(float(total_tokens) / count - per) > TOKEN_COUNT_TOLERANCE:
        raise TokenCountMismatch(
            f"值不自洽：total_tokens={total_tokens} / count={count} = "
            f"{float(total_tokens) / count:.6f}，与 tokens_per_count={per} 相差超过 "
            f"容差 {TOKEN_COUNT_TOLERANCE}"
        )
    return count


def format_count(value: Any) -> str:
    """展示口径：``None`` ⇒ 「未采集」（**不是 0**）；0 ⇒ 「0」；千分位。"""
    if value is None:
        return "未采集"
    value = guard_token_counter(value, field="count")
    return f"{value:,}"


def format_tokens_per_count(value: Any) -> str:
    """展示口径：``None`` ⇒ 「未采集」；小数位 ≤ :data:`MAX_PRECISION`。"""
    if value is None:
        return "未采集"
    num = _guard_tokens_per_count(value)
    text = f"{num:.{MAX_PRECISION}f}".rstrip("0").rstrip(".")
    return text or "0"
