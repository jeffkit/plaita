"""graph generate 共享生成器（plaita#305 Phase 2）——意图 → GraphDTO 草稿。

``POST /api/graphs/generate``（``api/graphs.py``）的实现面：自然语言业务需求
（+可选 hints）经 LLM 单发补全产出 GraphDTO 草稿，**生成后必须过
``graph_validation.validate_graph``**——不过则把 validate 的 errors 原样回喂
模型自纠（有界轮数，ai_flow 先例），轮数用尽仍不合法就原样带回调用方
（200 ``valid=false``，宿主 AI 可据此重试或改写需求）。端点纯函数式：
**不落库**，保存仍走宿主既有 save。

## 生成闭环（generate → validate → 回喂）

1. 组任务文本：GraphDTO 输出契约 + 可用节点类型摘要 + hints（节点类型偏好/
   租户内可引用子流程清单）+ 需求描述（+ 上轮 validate 错误）；
2. LLM 单发补全（``complete`` 注入面，缺省 :func:`default_model`）；
3. 剥围栏解析 JSON（:func:`parse_graph_json`）；
4. **A128-01 内容级自检**：``graph_validation.childflow_double_write_errors``
   ——generate 是第一个非编辑器 GraphDTO 产源，而折叠豁免子节点不核内容
   已双写进 owner.childFlow；投影内容未双写的草稿一律
   :class:`GraphGenerationInvariantError` 具名上抛（路由转 500），**不出
   草稿**——这是生成管线的内部不变量，不进回喂自纠；
5. ``validate_graph``（与 Phase 1 validate 同一入口）：valid → 返回草稿
   （errors 仅剩 warning 级）；否则 errors 原样作为下一轮任务的一部分回喂。

## 模型接入面

LLM 调用走仓内既有接入：pydantic-ai Anthropic 协议端点，凭证从 flowcast
providers.json 读取（与 copilot flow_agent 同源，后端不落密钥），
``PLAITA_CONSOLE_GRAPH_GEN_PROVIDER`` / ``PLAITA_CONSOLE_GRAPH_GEN_MODEL``
可覆盖 provider 名与模型名。窄域无工具单发补全——不是 agent 宿主、不 spawn
CLI 子进程、无工作区文件面，与 #292 平台闸的「spawn 宿主大脑」不同级；
鉴权照 #305 Phase 1 validate 同口径（editor+ 写闸 +
``tenant_scope(required=True)``）。测试经 ``complete`` 参数注入打桩，
不真出网。
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger("services.graph_generation")

#: 初生成 + 校验错误回喂自纠的有界轮数（API 阻塞面取小值；ai_flow 的
#: agent 宿主形态是 SSE 长任务才用 1+3）
_MAX_ATTEMPTS = 3

_GENERATION_TYPE = "generation"


class GraphGenerationInvariantError(Exception):
    """生成产物违反 GraphDTO 内部不变量（A128-01：投影内容未双写）。

    ``violations`` 为 ``graph_validation.childflow_double_write_errors`` 的
    违规项列表（``{node_id, owner_id, message}``）。语义：不出草稿、不进
    回喂自纠——能产出这种草稿说明生成管线自身的形状约束被击穿，必须响亮
    失败而不是让调用方拿到一张会无声丢内容的图。
    """

    def __init__(self, violations: List[Dict[str, Any]]):
        first = violations[0] if violations else {}
        super().__init__(str(first.get("message") or "生成产物未通过内容级自检"))
        self.violations = violations


# ---------------------------------------------------------------------------
# Prompt 组装
# ---------------------------------------------------------------------------

_INSTRUCTIONS = (
    "你是 plaita 流程图生成器。严格按用户消息里的契约输出唯一的 GraphDTO "
    "JSON，不输出任何解释文字。"
)

_TASK_HEADER = """\
## 任务
根据「需求描述」生成一张 plaita 流程图，输出 GraphDTO JSON。

## 输出格式（唯一输出；不要输出解释文字）
一个 JSON 对象（可放在 ```json 代码块里）：
{"nodes": [...], "edges": [...], "meta": {...}}

- nodes 每项：{"id": "<画布内唯一短id>", "type": "plaitaNode",
  "position": {"x": 0, "y": 0},
  "data": {"type": "<plaita节点类型>", "name": "<展示名>",
           "fields": {<类型特定字段，平铺>}}}
- edges 每项：{"id": "e1", "source": "<源节点id>", "target": "<目标节点id>"}
  - if 节点两条出边：sourceHandle 分别为 "true" / "false"
  - switch/case 每条出边：sourceHandle = 对应分支名（fields.branches[i].name）

## 图结构硬约束
- 恰有一个 data.type="start" 入口；每个分支终点接 data.type="end" 节点
  （fields: {"output": "<结果表达式>", "resultType": "success"}）
- 线性节点至多一条出边；禁止悬空连线（source/target 必须都是 nodes 里的 id）
- if 节点 fields.condition = {"field": "INPUT.x", "operator": "eq",
  "value": ...}（结构化条件对象，不是表达式字符串）
- switch/case：fields.branches = [{"name": "分支名", "isDefault": false},
  ..., {"name": "默认", "isDefault": true}]——必须恰有一个 isDefault=true
- 容器类节点（map/filter/find/reduce/while/loop/child）：子流程整体写在
  fields.childFlow = {"flow_id": "<短id>", "nodes": [...自带 start/end 与
  next 拓扑的完整子流程...]}；保持折叠形态，【不要】把子节点单独画成
  parentId/isContainerChild 展开态节点
- assignment 节点：fields.output（"$INPUT.x" / "$NODE.<id>.<输出字段>"）与
  fields.outputType = {"dataType": "string|number|boolean|object"}
- reference 节点引用已有流程：fields.flow_id = <流程id>；reference 仅
  runtime=go 池可执行，未明确要求时优先用显式 childFlow 子流程
- meta：{"flow_id": "<kebab-case短id>", "desc": "<一句话描述>"}

## 可用节点类型
{nodes_digest}
"""


def _available_nodes_digest() -> str:
    try:
        from . import ai_flow

        return ai_flow._available_nodes_digest()
    except Exception:  # noqa: BLE001 — 摘要不可得时 prompt 仍可发
        return "（无法枚举）"


def _hints_value(hints: Any, key: str) -> Any:
    if hints is None:
        return None
    if isinstance(hints, dict):
        return hints.get(key)
    return getattr(hints, key, None)


def _fmt_error(e: Dict[str, Any]) -> str:
    where = ""
    if e.get("node_id"):
        where = f"（节点 {e['node_id']!r}"
        if e.get("field"):
            where += f" 字段 {e['field']!r}"
        where += "）"
    return f"- [{e.get('severity') or 'error'}] {e.get('code')}: " \
           f"{e.get('message')}{where}"


def build_task(prompt: str, hints: Any = None,
               last_errors: Optional[List[Dict[str, Any]]] = None) -> str:
    """组装单轮任务文本（契约 + 节点摘要 + hints + 需求 + 上轮错误）。"""
    parts = [_TASK_HEADER.replace("{nodes_digest}", _available_nodes_digest())]

    preferred = _hints_value(hints, "preferred_node_types")
    if preferred:
        parts.append("## 节点类型偏好\n优先使用以下类型（仍须是可用类型）："
                     + "、".join(str(t) for t in preferred))
    ref_flows = _hints_value(hints, "reference_flows")
    if ref_flows:
        lines = []
        for f in ref_flows:
            lines.append("- " + (f if isinstance(f, str)
                                 else json.dumps(f, ensure_ascii=False)))
        parts.append("## 可引用的子流程清单（reference 节点 fields.flow_id 只能"
                     "从中选择；不在此清单的流程不可引用）\n" + "\n".join(lines))

    parts.append(f"## 需求描述\n{prompt}")

    if last_errors:
        parts.append("## 上一次生成的校验错误（必须全部修复后，重新输出完整的 "
                     "GraphDTO JSON）\n"
                     + "\n".join(_fmt_error(e) for e in last_errors))
    return "\n\n".join(parts)


# ---------------------------------------------------------------------------
# 模型回复解析
# ---------------------------------------------------------------------------

def parse_graph_json(text: str) -> Dict[str, Any]:
    """从模型回复提取 GraphDTO JSON（剥围栏 / 取 {…} 切片，尽力而为）。

    解析失败抛 ``ValueError``（回喂重试面，不是内部错误）。
    """
    raw = (text or "").strip()
    if not raw:
        raise ValueError("模型回复为空")
    candidates: List[str] = []
    start, end = raw.find("{"), raw.rfind("}")
    if 0 <= start < end:
        candidates.append(raw[start:end + 1])
    if "```" in raw:
        stripped = re.sub(r"^\s*```[a-zA-Z]*\s*", "", raw)
        stripped = re.sub(r"\s*```\s*$", "", stripped).strip()
        start, end = stripped.find("{"), stripped.rfind("}")
        if 0 <= start < end:
            candidates.append(stripped[start:end + 1])
        candidates.append(stripped)
    candidates.append(raw)
    for cand in candidates:
        try:
            obj = json.loads(cand)
        except (ValueError, TypeError):
            continue
        if isinstance(obj, dict):
            return obj
    raise ValueError("回复中没有可解析的 JSON 对象")


# ---------------------------------------------------------------------------
# 生成闭环
# ---------------------------------------------------------------------------

def _gen_err(code: str, message: str) -> Dict[str, Any]:
    return {"type": _GENERATION_TYPE, "code": code, "message": message,
            "severity": "error"}


def generate_graph(
    prompt: str, *, runtime: str, redis: Any,
    store: Any = None, tenant_id: Optional[str] = None,
    hints: Any = None,
    complete: Optional[Callable[[str], str]] = None,
    model_name: Optional[str] = None,
    usage: Optional[Dict[str, int]] = None,
    max_attempts: Optional[int] = None,
) -> Dict[str, Any]:
    """意图 → GraphDTO 草稿（generate→validate→回喂闭环，不落库）。

    返回 ``{valid, graph, definition, errors, generation}``：

    - ``valid=true``：草稿过 validate（``errors`` 仅剩 warning 级，不阻断）；
    - ``valid=false``：有界轮数内未产出合法草稿——``errors`` 为最后一轮
      validate 的 errors **原样**（或生成面具名错误
      ``generation.unparseable_output`` / ``generation.provider_error``），
      ``graph``/``definition`` 为最后一次草稿（解析失败时为 ``None``）；
    - ``generation``：``{model, duration_ms, attempts, usage}`` 元数据；
    - 投影内容未双写（A128-01）→ :class:`GraphGenerationInvariantError`，
      不出草稿。

    ``complete``/``model_name``/``usage`` 为注入面（测试打桩不真出网）；
    缺省经 :func:`default_model` 构造。
    """
    from . import graph_validation

    if complete is None or model_name is None:
        d_complete, d_model, d_usage = default_model()
        complete = complete if complete is not None else d_complete
        model_name = model_name if model_name is not None else d_model
        usage = usage if usage is not None else d_usage

    attempts_limit = int(max_attempts or _MAX_ATTEMPTS)
    started = time.monotonic()
    gen_errors: List[Dict[str, Any]] = []
    last_graph: Optional[Dict[str, Any]] = None
    last_definition: Dict[str, Any] = {}
    attempts = 0

    for attempt in range(1, attempts_limit + 1):
        attempts = attempt
        task = build_task(prompt, hints, gen_errors or None)
        try:
            text = complete(task)
        except Exception as exc:  # noqa: BLE001 — provider 故障回喂重试（有界）
            logger.warning("graph generate 模型调用失败（第 %d 轮）: %s",
                           attempt, exc)
            gen_errors = [_gen_err("generation.provider_error",
                                   f"模型调用失败：{exc}")]
            continue
        try:
            graph = parse_graph_json(text)
        except ValueError as exc:
            gen_errors = [_gen_err("generation.unparseable_output",
                                   f"模型回复不是可解析的 GraphDTO JSON：{exc}")]
            last_graph = None
            continue
        violations = graph_validation.childflow_double_write_errors(graph)
        if violations:
            raise GraphGenerationInvariantError(violations)
        report = graph_validation.validate_graph(
            graph, runtime=runtime, redis=redis, store=store,
            tenant_id=tenant_id,
        )
        if report["valid"]:
            return _result(True, graph, report["definition"],
                           report["errors"], attempts, model_name,
                           started, usage)
        gen_errors = report["errors"]  # 原样回喂下一轮
        last_graph, last_definition = graph, report["definition"]

    return _result(False, last_graph, last_definition, gen_errors,
                   attempts, model_name, started, usage)


def _result(valid: bool, graph: Optional[Dict[str, Any]],
            definition: Dict[str, Any], errors: List[Dict[str, Any]],
            attempts: int, model_name: str, started: float,
            usage: Optional[Dict[str, int]]) -> Dict[str, Any]:
    return {
        "valid": valid,
        "graph": graph,
        "definition": definition or {},
        "errors": errors,
        "generation": {
            "model": model_name,
            "duration_ms": int((time.monotonic() - started) * 1000),
            "attempts": attempts,
            "usage": dict(usage) if usage else None,
        },
    }


# ---------------------------------------------------------------------------
# 缺省模型接入（与 copilot flow_agent 同源；测试注入打桩，勿在此出网）
# ---------------------------------------------------------------------------

def default_model() -> Tuple[Callable[[str], str], str, Dict[str, int]]:
    """构造 ``(complete, model_name, usage_acc)``。

    凭证与端点走 flowcast providers.json（后端不落密钥）；构造失败抛异常，
    由路由层转为具名 503。``usage_acc`` 为跨轮累积的 token 计数
    （input/output/total），随 ``generation.usage`` 透出。
    """
    from . import flow_agent

    provider_name = os.getenv("PLAITA_CONSOLE_GRAPH_GEN_PROVIDER", "glm-52")
    api_key, api_base, configured = flow_agent._provider_config(provider_name)
    model_name = (os.getenv("PLAITA_CONSOLE_GRAPH_GEN_MODEL")
                  or configured or "GLM-5.2")
    if not api_key:
        raise RuntimeError(
            f"flowcast providers.json 缺少 provider {provider_name!r} 的 apiKey"
        )
    from pydantic_ai import Agent
    from pydantic_ai.models.anthropic import AnthropicModel
    from pydantic_ai.providers.anthropic import AnthropicProvider

    model = AnthropicModel(
        model_name,
        provider=AnthropicProvider(api_key=api_key, base_url=api_base or None),
    )
    agent: "Agent[None, str]" = Agent(model, instructions=_INSTRUCTIONS,
                                      retries=2)
    usage_acc: Dict[str, int] = {}

    def complete(task: str) -> str:
        result = agent.run_sync(task)
        _accumulate_usage(usage_acc, result)
        return result.output

    return complete, model_name, usage_acc


def _accumulate_usage(acc: Dict[str, int], result: Any) -> None:
    """pydantic-ai 用量跨轮累积（best-effort；字段缺席不致命）。"""
    try:
        u = result.usage()
        for key in ("input_tokens", "output_tokens", "total_tokens"):
            v = getattr(u, key, None)
            if isinstance(v, int):
                acc[key] = acc.get(key, 0) + v
    except Exception:  # noqa: BLE001 — 计量面失败不影响生成
        pass
