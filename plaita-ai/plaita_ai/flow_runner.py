"""Shared compile/run kernel for MCP, CLI, and FoT agent."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from enum import Enum
from importlib import resources
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, get_args, get_origin

from plaita.core.errors import SuspendNotExecutableError
from plaita.dsl.codeflow import compile_source, flow_from_source
from plaita.dsl.ir_validate import FlowIRValidationError, build_flow
from plaita.node import get_default_registry

_CODEFLOW_ERR = re.compile(r"^\[codeflow\] 第 (\?|\d+) 行: (.+)$", re.DOTALL)

# plaita#351 P2-2：报错文本 → 稳定 code + fix 提示。匹配按子串、取首条命中；
# 未命中保持旧形状 {line, message}（code/fix 稀疏键，既有消费者逐字节兼容）。
# 子串取自编译器具名报错的稳定前缀（断言钉在 tests/test_compile_error_codes.py）。
_ERROR_CODES: tuple = (
    ("f-string", "fstring_unsupported",
     '用 F.concat(...) 拼接，例：F.concat("hi ", INPUT.name)'),
    ("下标只支持整数常量", "subscript_unsupported",
     "字符串/动态键下标改用字段访问 INPUT.x；仅 [0]/[-1] 整数常量下标受支持"),
    ("被多次赋值", "duplicate_assignment",
     "跨分支同名赋值被编译期拒绝：各分支用不同变量名，或让每条分支直接 return"),
    ("的赋值右侧引用了它自己", "self_reference_assignment",
     "循环计数用 for w in WHILE(条件, id=...) 写法，循环态走 item/rounds"),
    ("迭代对象必须是", "for_iter_unsupported",
     "for 只能迭代 MAP/FILTER/FIND/LOOP/REDUCE/WHILE(...) 节点调用；列表变换用 MAP/FILTER"),
    ("不支持的语句", "unsupported_statement",
     "不支持 break/continue/try/except；退出条件写进 WHILE 条件，错误处理用 on_error=ErrorHandler(...)"),
    ("end(...) 的 error 必须是字面数据", "end_error_not_literal",
     "end(error=...) 只收字面 dict；动态值放 output= 或上游节点"),
    ("caps 字段不支持动态展开", "dynamic_kwargs_unsupported",
     "节点传参写具名 kwargs；** 展开仅支持字符串键的 dict 字面量"),
    ("未注册的自定义节点", "unregistered_node",
     "按报错列出的可用类型修正占位符名（node_type 大写），或先 get_default_registry().register(...)"),
    ("未知名字", "unknown_name",
     "检查拼写；未赋值的变量不可引用——先赋值再用，或改 INPUT.x / NODE.x"),
    ("只能作为语句或赋值右侧", "node_call_in_expression",
     "拆成两条语句：r = HTTP.post(...); return r.data"),
    ("不支持的调用", "unsupported_call",
     "表达式里只能调 F.xxx(...) 或内置 len/abs/round/str"),
)


def _error_code_fix(message: str) -> tuple:
    for needle, code, fix in _ERROR_CODES:
        if needle in message:
            return code, fix
    return None, None


@dataclass
class CompileError:
    line: Optional[int]
    message: str
    # plaita#351 P2-2：稳定错误码与修复提示（稀疏键——未命中分类时保持 None，
    # to_dict 不产出该键，旧 {line, message} 消费者形状不变）。
    code: Optional[str] = None
    fix: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"line": self.line, "message": self.message}
        if self.code is not None:
            payload["code"] = self.code
        if self.fix is not None:
            payload["fix"] = self.fix
        return payload


@dataclass
class CompileResult:
    ok: bool
    ir: Optional[Dict[str, Any]] = None
    errors: List[CompileError] = field(default_factory=list)
    flow_id: Optional[str] = None
    # plaita#331 作者期信号：``compile_flow(source, profile=...)`` 时附带非致命
    # ``{"profile", "required", "missing"}``（递归闭包 × 目标执行档）。**稀疏键**：
    # 仅传 profile 时进 to_dict——既有载荷形状逐字节不变、``ok`` 判定不受影响
    # （A2 r61 评审 §4 向后兼容；验收：含 approval 的源码 compile(profile="normal")
    # 返回本键但 ok 不变）。
    execution_requirements: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "ok": self.ok,
            "flow_id": self.flow_id,
            "ir": self.ir,
            "errors": [e.to_dict() for e in self.errors],
        }
        if self.execution_requirements is not None:
            payload["execution_requirements"] = self.execution_requirements
        return payload


@dataclass
class RunResult:
    ok: bool
    result: Any = None
    error: Optional[str] = None
    error_type: Optional[str] = None
    # plaita#330：具名错误的结构化错误体（目前仅 suspend_not_executable 携带
    # {error_type, node_id, node_type, source_line, flow_id, cause, fix,
    # frame_path, message}）。**稀疏键**：仅非 None 时进 to_dict ——ok/普通失败
    # 载荷的 JSON 形状逐字节不变（test_json_guard 形状负控钉此契约），具名错误
    # 才多出 error_detail 键。
    error_detail: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "ok": self.ok,
            "result": self.result,
            "error": self.error,
            "error_type": self.error_type,
        }
        if self.error_detail is not None:
            payload["error_detail"] = self.error_detail
        return payload


def _parse_codeflow_error(exc: Exception) -> CompileError:
    text = str(exc).strip()
    match = _CODEFLOW_ERR.match(text)
    if not match:
        code, fix = _error_code_fix(text)
        return CompileError(line=None, message=text, code=code, fix=fix)
    line_raw, message = match.group(1), match.group(2)
    line = None if line_raw == "?" else int(line_raw)
    code, fix = _error_code_fix(message)
    return CompileError(line=line, message=message, code=code, fix=fix)


def compile_flow(
    source: str,
    *,
    flow_id: Optional[str] = None,
    profile: Optional[str] = None,
    **opts: Any,
) -> CompileResult:
    """Compile @flow source to Flow IR + 拓扑校验。

    与 ``flow_from_source`` / ``FlowBuilder.build`` 走同一 ``validate_flow_ir`` 门，
    避免 Agent 自纠循环把「仅 AST 通过」当成编译成功。

    ``profile``（plaita#331 作者期信号）：给目标环境（台阶名 inline/managed/
    managed:queue，或旧档名 normal/generator/distributed/local——归一化兼容）即附
    非致命 ``execution_requirements``——**只读报告，不改 ``ok``**（拒绝面在执行
    入口 / publish/start，作者期只提示）。未传 profile 时结果形状与历史逐字节
    一致。
    """
    try:
        ir = compile_source(source, flow_id=flow_id, **opts)
        # validate_flow_ir + Flow.model_validate（与 flow_from_source 同门）
        flow = build_flow(ir)
    except FlowIRValidationError as exc:
        code, fix = _error_code_fix(str(exc))
        return CompileResult(
            ok=False,
            errors=[CompileError(line=None, message=str(exc), code=code, fix=fix)],
        )
    except Exception as exc:
        err = _parse_codeflow_error(exc)
        return CompileResult(ok=False, errors=[err])

    requirements: Optional[Dict[str, Any]] = None
    if profile is not None:
        from plaita.core.capabilities import check_execution_requirements

        try:
            report = check_execution_requirements(flow, profile)
        except ValueError as exc:
            # 未知 profile 名是调用方错误：按一条编译错误呈现，不炸工具体。
            return CompileResult(
                ok=False,
                errors=[CompileError(line=None, message=str(exc))],
            )
        requirements = {
            "profile": report["profile"],
            "required": report["required"],
            "missing": report["missing"],
        }

    resolved_flow_id = ir.get("flow_id") or flow_id
    return CompileResult(
        ok=True, ir=ir, flow_id=resolved_flow_id, execution_requirements=requirements,
    )


def run_flow(
    source: str,
    inputs: Optional[Dict[str, Any]] = None,
    *,
    flow_id: Optional[str] = None,
    globals_ctx: Optional[Dict[str, Any]] = None,
    **opts: Any,
) -> RunResult:
    """Compile @flow source and execute. Compiles first; does not run on compile failure.

    成功编译后直接用已校验 IR 构建 Flow，避免二次 ``compile_source``。
    """
    compiled = compile_flow(source, flow_id=flow_id, **opts)
    if not compiled.ok:
        messages = "; ".join(
            f"第{e.line}行: {e.message}" if e.line is not None else e.message
            for e in compiled.errors
        )
        return RunResult(ok=False, error=messages or "compile failed", error_type="compile")

    try:
        flow = build_flow(compiled.ir) if compiled.ir is not None else flow_from_source(
            source, flow_id=flow_id, **opts
        )
        if globals_ctx:
            flow.global_context = dict(globals_ctx)
        result = flow.run(**(inputs or {}))
    except Exception as exc:
        error_type = _stable_error_type(exc)
        detail = exc.to_dict() if isinstance(exc, SuspendNotExecutableError) else None
        return RunResult(ok=False, error=str(exc), error_type=error_type, error_detail=detail)

    return RunResult(ok=True, result=result)


def _stable_error_type(exc: Exception) -> str:
    """具名错误类型 → 稳定字符串（plaita#330）。

    ``FlowErrorType`` 枚举取 ``.value``（历史上 ``str(Enum)`` 产出
    ``FlowErrorType.FLOW_ERROR`` 这种机器键，AI 作者无法按它分流）；自带
    ``error_type`` 字符串属性的异常原样采用；其余回落异常类名。
    """
    raw = getattr(exc, "error_type", None)
    if isinstance(raw, Enum):
        return raw.value
    if isinstance(raw, str) and raw:
        return raw
    return type(exc).__name__


def list_node_types(*, discover_plugins: bool = True) -> List[Dict[str, Any]]:
    """List registered node types for Planner prompts / MCP introspection.

    plaita#331：行内附作者期信号 ``tier``（执行台阶，数据源 = registry 声明面，
    list 时**不**重扫源码——A2 r61 评审 §4）与 ``suspends``（is_suspending，
    台阶派生值）。台阶化 2026-10-25：旧 ``requires``（token 列表）退役为
    ``tier`` 单值（inline/managed/managed:queue）。
    """
    from plaita.node.capabilities import declared_tier

    registry = get_default_registry()
    if discover_plugins:
        registry.discover()
    rows: List[Dict[str, Any]] = []
    for node_type in sorted(registry.list_types()):
        cls = registry.get(node_type)
        placeholder = node_type.upper()
        rows.append(
            {
                "node_type": node_type,
                "placeholder": placeholder,
                "node_name": getattr(cls, "node_name", node_type) if cls else node_type,
                # plaita#331：tier/suspends 让作者在选节点时即看到执行要求
                # （managed 族在 inline 直跑下不可执行；managed:queue 族还
                # 需要队列唤醒服务在场）。
                "tier": declared_tier(cls) if cls else "inline",
                "suspends": getattr(cls, "is_suspending", False) is True if cls else False,
            }
        )
    return rows


_NODE_RESERVED_FIELDS: Dict[str, str] = {
    "id": "节点 id；赋值形态用变量名充当 id，不要同时传 id=",
    "timeout": "保留 kwargs，ISO 时长（如 PT5S）",
    "on_error": "ErrorHandler(strategy, default=...)，错误策略走基类",
}

# 基类拓扑/标签字段：不进 expression_fields（next 是节点寻址不是表达式，
# name/desc 是展示标签），也不进 example（example 只放节点自身必填字段）。
_NODE_BASE_FIELDS = frozenset(
    {"id", "name", "desc", "next", "output", "timeout", "on_error", "errorHandler"}
)


def _is_expression_field(field_info: Any) -> bool:
    """Optional[Any]/Optional[str] 字段可接表达式串（$INPUT.x 等）。

    与 skill 约定一致（authoring-spec §2.4）：强类型 int/bool 字段构建期会拒绝
    表达式串；判据 = 去掉 Optional 后是 Any 或恰好 str。
    """
    ann = getattr(field_info, "annotation", None)
    if ann is None:
        return False
    if ann is Any:
        return True
    if get_origin(ann) is Union:
        args = [a for a in get_args(ann) if a is not type(None)]
        if any(a is Any for a in args):
            return True
        return args == [str]
    return False


def _example_value(name: str, field_info: Any) -> Any:
    """必填字段的占位示例值（最小可用 kwargs 的原料）。"""
    ann = getattr(field_info, "annotation", None)
    if ann is bool:
        return True
    if ann is int:
        return 1
    if ann is float:
        return 1.0
    if get_origin(ann) in (dict, Dict):
        return {}
    if get_origin(ann) in (list, List):
        return []
    return f"<{name}>"


def describe_node_type(node_type: str, *, discover_plugins: bool = True) -> Dict[str, Any]:
    """Describe one registered node type: pydantic schema + example + notes.

    plaita#351 P0-2：业务节点字段 schema 此前不可查（flow_list_nodes 只有
    tier/suspends），AI 作者只能猜 agentrun/hitl/连接器的字段名，猜错落到节点
    构造期 pydantic 错误且无 source_line，修复闭环弱于编译错误。本工具让作者期
    即可查：schema 用 ``model_json_schema()``，example 给必填字段的最小 kwargs，
    notes 标保留字段（id/timeout/on_error）与可接表达式的字段
    （Optional[Any]/Optional[str]）。
    """
    from plaita.node.capabilities import declared_tier

    if not isinstance(node_type, str) or not node_type.strip():
        return {"ok": False, "error": "node_type must be a non-empty string", "available": []}
    registry = get_default_registry()
    if discover_plugins:
        registry.discover()
    available = sorted(registry.list_types())
    cls = registry.get(node_type.strip())
    if cls is None:
        return {
            "ok": False,
            "error": (
                f"未知节点类型 {node_type.strip()!r}（@flow 占位符用其大写形式）。"
                "完整行（含 tier/suspends）见 flow_list_nodes。"
            ),
            "available": available,
        }
    fields = getattr(cls, "model_fields", {})
    required = [
        name for name, f in fields.items()
        if f.is_required() and name not in _NODE_BASE_FIELDS
    ]
    return {
        "ok": True,
        "node_type": node_type.strip(),
        "placeholder": node_type.strip().upper(),
        "node_name": getattr(cls, "node_name", node_type) if cls else node_type,
        "tier": declared_tier(cls),
        "suspends": getattr(cls, "is_suspending", False) is True,
        "schema": cls.model_json_schema(),
        "example": {name: _example_value(name, fields[name]) for name in required},
        "notes": {
            "reserved_fields": _NODE_RESERVED_FIELDS,
            "expression_fields": sorted(
                name for name, f in fields.items()
                if _is_expression_field(f) and name not in _NODE_BASE_FIELDS
            ),
            "hint": "可接表达式的字段已声明 Optional[Any]/Optional[str]；强类型字段只收字面量",
        },
    }


def list_tools_registered(as_code: bool = True) -> List[Any]:
    """List all tools registered via ToolNode / @tool / register_tool_node.

    Args:
        as_code: When True (default), return Python-style function signature
            strings — ideal for AI writing @flow DSL.  When False, return
            JSON-serialisable dicts (backward compat / programmatic use).

    Returns an empty list if no tools have been registered yet.
    """
    try:
        from plaita_ai.agent.fot.tools import list_tools
        return list_tools(as_code=as_code)
    except ImportError:
        return []


_SKILL_NAME_RE = re.compile(r"^[a-z0-9-]+$")


class PathTraversalError(ValueError):
    """Caller-supplied path escapes the skill root (#54/#248)."""


def _contained_skill_dir(skill_name: str) -> Path:
    """Resolve one bundled skill directory, refusing anything outside skills/.

    Two independent guards (plaita#248; same containment contract as #54):

    1. whitelist: the name must be a bare lowercase alnum/hyphen directory
       name — no ``/``, no ``.``/``..``, no absolute path, no uppercase or
       other separators that could move the root;
    2. containment: after ``resolve()`` the directory must still be inside the
       resolved skills root (lexical and symlink escapes both fail).
    """
    if not isinstance(skill_name, str) or not _SKILL_NAME_RE.fullmatch(skill_name):
        raise PathTraversalError(
            "skill_name must match ^[a-z0-9-]+$ (bare skill directory name), "
            f"got: {skill_name!r}"
        )
    skills_root = (Path(__file__).resolve().parent / "skills").resolve()
    contained = (skills_root / skill_name).resolve()
    if contained == skills_root or not contained.is_relative_to(skills_root):
        raise PathTraversalError(
            f"skill_name escapes the skills root: {skill_name!r} resolves to {contained}"
        )
    return contained


def get_skill_text(skill_name: str = "flow-coder") -> str:
    """Load bundled skill markdown (for agents without a local skill install).

    ``skill_name`` is untrusted caller input (MCP tool, CLI): it is validated
    and resolved under the bundled skills root before either the package
    resource or the editable fallback is read (plaita#248).
    """
    contained = _contained_skill_dir(skill_name)
    package = "plaita_ai.skills"
    filename = f"{skill_name}/SKILL.md"
    try:
        return resources.files(package).joinpath(filename).read_text(encoding="utf-8")
    except (FileNotFoundError, OSError, TypeError):
        pass

    # Editable / dev fallback: plaita-ai/plaita_ai/skills/...
    here = contained / "SKILL.md"
    if here.is_file():
        return here.read_text(encoding="utf-8")
    raise FileNotFoundError(f"skill not found: {skill_name}")


def get_skill_reference(skill_name: str, ref_name: str) -> str:
    """Load a bundled skill reference file, contained to the references root.

    ``skill_name``/``ref_name`` are untrusted caller input (MCP tool, CLI).
    Containment is resolve-then-check, not blacklisting: the request is
    resolved against the real references root and the resolved real path must
    stay inside it — absolute paths, ``..`` segments and symlink escapes all
    resolve outside the root and are refused with :class:`PathTraversalError`.
    No silent fallback to a wider path.
    """
    if not skill_name or Path(skill_name).name != skill_name:
        # A skill is a bare directory under skills/, never a path — otherwise
        # the caller could move the containment root itself.
        raise PathTraversalError(
            f"skill_name must be a bare skill directory name, got: {skill_name!r}"
        )
    skills_root = (Path(__file__).resolve().parent / "skills").resolve()
    root = (skills_root / skill_name / "references").resolve()
    requested = (root / ref_name).resolve()
    if not requested.is_relative_to(root):
        raise PathTraversalError(
            f"skill reference escapes the references root: "
            f"{skill_name}/{ref_name} resolves to {requested}"
        )

    package = "plaita_ai.skills"
    path = f"{skill_name}/references/{ref_name}"
    try:
        return resources.files(package).joinpath(path).read_text(encoding="utf-8")
    except (FileNotFoundError, OSError, TypeError):
        pass

    if requested.is_file():
        return requested.read_text(encoding="utf-8")
    raise FileNotFoundError(f"skill reference not found: {skill_name}/{ref_name}")


def dumps_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2, default=str)


def result_json(data: Any) -> str:
    try:
        if hasattr(data, "to_dict"):
            return dumps_json(data.to_dict())
        return dumps_json(data)
    except Exception as exc:  # noqa: BLE001 — 盲捕是守卫本体（深嵌套/循环引用整族具名降级）
        # result 由候选 flow 自控：深嵌套结构（~10k 层约 20KB）让 json.dumps
        # 抛 RecursionError，循环引用抛 ValueError（A2R51-02 第二面）。这是
        # 「结果不可序列化」，不是执行失败——以 RunResult 同形状的具名信封
        # 降级，不穿出工具体（MCP/CLI/ReAct 三面共用此出口）。
        return json.dumps(
            {
                "ok": False,
                "result": None,
                "error": f"result is not JSON-serializable: {type(exc).__name__}: {exc}"[:500],
                "error_type": type(exc).__name__,
            },
            ensure_ascii=False,
        )
