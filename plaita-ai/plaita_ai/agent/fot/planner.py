"""FoT planner — LangChain 1.x messages API (no legacy Chain / JSON actions)."""

from __future__ import annotations

import logging
from typing import Any, List, Optional, Sequence

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage

from plaita import Node
from plaita.node import get_default_registry

from plaita_ai.agent.fot.extract import extract_flow_source
from plaita_ai.agent.fot.prompts import (
    COMPOSE_SYSTEM,
    COMPOSE_USER,
    REVIEW_SYSTEM,
    REVIEW_USER,
    RUN_REVIEW_SYSTEM,
    RUN_REVIEW_USER,
    format_compile_errors,
    format_dsl_section,
    format_instruction_section,
    format_nodes_section,
    format_spec_section,
    format_tools_section,
)
from plaita_ai.agent.fot.tools import ToolLike, ToolSpec, register_tool_node, tools_prompt_section
from plaita_ai.flow_runner import CompileError, CompileResult, compile_flow, get_skill_reference


logger = logging.getLogger(__name__)

_REFERENCES_DIR_HINT = "plaita_ai/skills/flow-coder/references/"


def _load_dsl_reference() -> str:
    """Load the canonical flow-coder DSL reference — 每次调用现读，不进模块缓存。

    MCP server 等常驻进程里 skill 参考可能随版本升级，模块 import 时读一次
    会让注入内容凝固在首个请求时刻；13KB 级文本 IO 相比一次 LLM 调用可忽略。
    缺失时行为仍降级为空段（prompts 层出占位护栏），但必须 warning 可观测，
    不允许静默的质量劣化。
    """
    try:
        return get_skill_reference("flow-coder", "codeflow-reference.md")
    except FileNotFoundError as exc:
        logger.warning(
            "flow-coder DSL 参考加载失败（%s）。FoT 提示词将降级为速记护栏，"
            "生成质量可能下降；请检查 %s 下 codeflow-reference.md 是否完整。",
            exc,
            _REFERENCES_DIR_HINT,
        )
        return ""


def _load_authoring_reference() -> str:
    """Load the canonical authoring-spec — 每次调用现读（时效性同上）。

    authoring-spec 随 flow-coder skill v0.3 起提供；旧版本包里没有该文件时
    返回空串，prompts 层降级为内置硬约束底线提示，并 warning 可观测。
    """
    try:
        return get_skill_reference("flow-coder", "authoring-spec.md")
    except FileNotFoundError as exc:
        logger.warning(
            "flow-coder authoring-spec 加载失败（%s）。FoT 提示词将降级为内置"
            "硬约束底线；请检查 %s 下 authoring-spec.md 是否完整。",
            exc,
            _REFERENCES_DIR_HINT,
        )
        return ""

# 内置专用占位符/合成节点类型（来源：plaita.dsl.codeflow._common 的
# _BUILTIN_HANDLED_TYPES；此处复制以免耦合编译器私有集合）
_BUILTIN_PLACEHOLDER_TYPES = {
    "http", "code", "event", "child", "reference", "parallel",
    "map", "filter", "find", "loop", "reduce",
    "start", "end", "if", "assignment", "switch", "bool",
}
# Node 基类实例字段（所有节点共有，列进节点清单只是噪音）
_BASE_NODE_FIELDS = frozenset(
    ("id", "name", "desc", "output", "next", "timeout",
     "source_line", "timeout_handler", "error_handler")
)
_MAX_LISTED_NODES = 40


def _registered_nodes_section(tool_specs: Optional[List[ToolSpec]] = None) -> str:
    """从默认 NodeRegistry 生成已注册业务节点清单（供 LLM 直接以大写占位符调用）。

    排除：内置专用类型、通用 TOOL 兜底节点、FoT 已在「可用工具」段落列出的
    动态工具节点。registry 不可用时返回空串（该段落整体消失）。
    """
    exclude = set(_BUILTIN_PLACEHOLDER_TYPES) | {"tool"}
    for spec in tool_specs or []:
        if spec.node_type:
            exclude.add(spec.node_type)
    try:
        registry = get_default_registry()
        types = sorted(t for t in registry.list_types() if t not in exclude)
    except Exception:
        return ""
    lines: List[str] = []
    for node_type in types:
        cls = registry.get(node_type)
        if cls is None or not (isinstance(cls, type) and issubclass(cls, Node)):
            continue
        fields = [
            name for name in getattr(cls, "model_fields", {})
            if name not in _BASE_NODE_FIELDS
        ]
        doc = (cls.__doc__ or "").strip().splitlines()
        desc = doc[0].strip() if doc else ""
        line = f"- {node_type.upper()}({', '.join(fields)})"
        if desc:
            line += f" —— {desc}"
        lines.append(line)
        if len(lines) >= _MAX_LISTED_NODES:
            remaining = len(types) - len(lines)
            if remaining > 0:
                lines.append(f"…（其余 {remaining} 个已注册节点省略）")
            break
    return "\n".join(lines)


def _invoke_model(model: BaseChatModel, system: str, user: str) -> str:
    response = model.invoke(
        [SystemMessage(content=system), HumanMessage(content=user)],
    )
    content = response.content
    if isinstance(content, list):
        parts = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict) and block.get("type") == "text":
                parts.append(str(block.get("text", "")))
        content = "".join(parts)
    return str(content)


def plan_flow_source(
    model: BaseChatModel,
    task: str,
    *,
    tools: Optional[Sequence[ToolLike]] = None,
    instruction: str = "",
    tool_specs: Optional[List[ToolSpec]] = None,
) -> str:
    specs = tool_specs or (register_tool_node(*tools) if tools else [])
    system = COMPOSE_SYSTEM.format(
        tools_section=format_tools_section(tools_prompt_section(specs)),
        dsl_section=format_dsl_section(_load_dsl_reference()),
        spec_section=format_spec_section(_load_authoring_reference()),
        nodes_section=format_nodes_section(_registered_nodes_section(specs)),
        instruction_section=format_instruction_section(instruction),
    )
    user = COMPOSE_USER.format(task=task)
    raw = _invoke_model(model, system, user)
    return extract_flow_source(raw)


def review_flow_source(
    model: BaseChatModel,
    task: str,
    source: str,
    errors: List[CompileError],
    *,
    instruction: str = "",
    tool_specs: Optional[List[ToolSpec]] = None,
    tools: Optional[Sequence[ToolLike]] = None,
) -> str:
    specs = tool_specs or (register_tool_node(*tools) if tools else [])
    system = REVIEW_SYSTEM.format(
        tools_section=format_tools_section(tools_prompt_section(specs)),
        dsl_section=format_dsl_section(_load_dsl_reference()),
        spec_section=format_spec_section(_load_authoring_reference()),
        nodes_section=format_nodes_section(_registered_nodes_section(specs)),
        instruction_section=format_instruction_section(instruction),
    )
    user = REVIEW_USER.format(
        task=task,
        source=source,
        errors=format_compile_errors(errors),
    )
    raw = _invoke_model(model, system, user)
    return extract_flow_source(raw)


def rewrite_flow_source_for_run_error(
    model: BaseChatModel,
    task: str,
    source: str,
    *,
    error: str,
    error_type: str = "",
    instruction: str = "",
    tool_specs: Optional[List[ToolSpec]] = None,
    tools: Optional[Sequence[ToolLike]] = None,
) -> str:
    """Rewrite a compiled-but-run-failed source once, guided by the run error.

    REVIEW 通道处理「编译没过」；本函数处理「编译过了、执行炸了」——同一套
    注入口径（工具/DSL 参考/编写规范/已注册节点），错误段换为运行期报错。
    源码解析失败仍抛 ``ValueError``（与 plan/review 通道一致，由调用方决定
    是否计入重试预算）。
    """
    specs = tool_specs or (register_tool_node(*tools) if tools else [])
    system = RUN_REVIEW_SYSTEM.format(
        tools_section=format_tools_section(tools_prompt_section(specs)),
        dsl_section=format_dsl_section(_load_dsl_reference()),
        spec_section=format_spec_section(_load_authoring_reference()),
        nodes_section=format_nodes_section(_registered_nodes_section(specs)),
        instruction_section=format_instruction_section(instruction),
    )
    error_text = f"[{error_type}] {error}" if error_type else error
    user = RUN_REVIEW_USER.format(task=task, source=source, errors=error_text)
    raw = _invoke_model(model, system, user)
    return extract_flow_source(raw)


def plan_with_compile_loop(
    model: BaseChatModel,
    task: str,
    *,
    tools: Optional[Sequence[ToolLike]] = None,
    instruction: str = "",
    max_retries: int = 3,
    flow_id: Optional[str] = None,
) -> tuple[str, CompileResult, int]:
    """Compose/review until compile succeeds or retries exhausted.

    输出解析失败（模型没输出含 ``@flow`` 的代码块）与编译失败同等对待：
    只烧一次 attempt，错误结构化记入 ``CompileResult.errors`` 并回喂下一轮
    （compose 通道附加格式提示、review 通道走 errors），预算耗尽返回结构化
    失败而非抛异常。

    语义备注：``max_retries`` 是**总尝试次数**（含首次 compose，即
    ``range(max_retries)``），不是额外重试次数——3 意味着至多
    1 次 compose + 2 次 review。运行期错误的重试由 FoTAgent 的
    ``max_run_retries`` 环独立预算（见 agent.invoke）。
    """
    specs = register_tool_node(*tools) if tools else []
    source = ""
    compiled = CompileResult(ok=False, errors=[CompileError(line=None, message="未开始")])
    attempts = 0
    parse_feedback = ""

    for attempt in range(max_retries):
        attempts = attempt + 1
        try:
            if attempt == 0 or not source:
                source = plan_flow_source(
                    model,
                    task,
                    instruction=f"{instruction}\n{parse_feedback}" if parse_feedback else instruction,
                    tool_specs=specs,
                )
            else:
                source = review_flow_source(
                    model,
                    task,
                    source,
                    compiled.errors,
                    instruction=instruction,
                    tool_specs=specs,
                )
        except ValueError as exc:
            # 上游 extract_flow_source 没找到含 @flow 的代码块：不再炸穿
            # 整个重试回路，记一次失败 attempt 并把可操作的错误回喂下一轮。
            compiled = CompileResult(
                ok=False,
                errors=[CompileError(line=None, message=str(exc))],
            )
            parse_feedback = f"上一轮输出未包含 @flow 源码：{exc}"
            continue

        compiled = compile_flow(source, flow_id=flow_id)
        if compiled.ok:
            return source, compiled, attempts

    return source, compiled, attempts
