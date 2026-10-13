"""@flow / @childflow decorators and source-string compilation entrypoints."""
from __future__ import annotations

import ast
import inspect
import json
import textwrap
import warnings
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from plaita.core.flow import Flow
from plaita.dsl.codeflow._common import (
    _ChildFlowMarker,
    _CodeflowError,
    _CompileCtx,
)
from plaita.dsl.codeflow._stmt import _compile_block

def _func_ast(func: Callable) -> ast.FunctionDef:
    try:
        src = inspect.getsource(func)
    except OSError as e:
        # REPL / Jupyter / python -c 里定义的函数没有源文件——历史上裸抛
        # ``OSError: could not get source code``，用户不知道该往哪走。
        raise _CodeflowError(
            f"could not read source of function {getattr(func, '__name__', '?')!r}: "
            "functions defined in a REPL / Jupyter notebook / `python -c` have no "
            "source file for @flow to compile. Define the flow function in a .py "
            "file, or use flow_from_source(src) to compile from a source string.",
            None,
        ) from e
    src = textwrap.dedent(src)
    mod = ast.parse(src)
    # 去掉装饰器行后取第一个 FunctionDef
    fdef = None
    for n in mod.body:
        if isinstance(n, ast.FunctionDef):
            fdef = n
            break
    if fdef is None:
        raise _CodeflowError("找不到函数定义", None)
    return fdef


def _warn_deprecated_type_opts(opts: Dict[str, Any]) -> None:
    if opts.get("input_type") is not None or opts.get("output_type") is not None:
        warnings.warn(
            "input_type/output_type on @flow/@childflow is deprecated and ignored; "
            "$INPUT is always a dict from run(**kwargs) or run({...}).",
            DeprecationWarning,
            stacklevel=4,
        )


def _default_input_type() -> Dict[str, str]:
    return {"dataType": "object"}


def _iter_child_scopes(node: Dict[str, Any]) -> List[Dict[str, Any]]:
    """一个节点内嵌的子流程作用域：``childFlow`` / ``child_flow`` / ``branches[].flow``。

    ``childFlow`` 可能是 JSON 字符串（发动机 ``Flow.model_validate_json`` 口径），
    这里按同一口径解析；解析失败就当没有（发射器另有 EmitError 兜底）。
    """
    scopes: List[Dict[str, Any]] = []
    for key in ("childFlow", "child_flow"):
        inner = node.get(key)
        if isinstance(inner, str):
            try:
                inner = json.loads(inner)
            except ValueError:
                inner = None
        if isinstance(inner, dict):
            scopes.append(inner)
    for branch in node.get("branches") or []:
        if isinstance(branch, dict) and isinstance(branch.get("flow"), dict):
            scopes.append(branch["flow"])
    return scopes


def _warn_duplicate_child_flow_ids(ir: Dict[str, Any]) -> None:
    """同一份文档里两个**不同**子流程声明了同一个 ``flow_id`` 时告警（#145）。

    ``flow_id`` 不参与节点寻址/查找（但会写进执行上下文，``$FLOW_ID`` 可见），
    重复不影响执行，但排障时无法区分是哪一份子流程；
    而"不给 id 就不回落函数名"的理由正是避免同名撞 id ⇒ 显式撞 id 至少要可见。
    只告警不报错（存量 flow 不因此中断）。

    **按对象身份去重**：同一份子流程被父流程多处引用时，父 IR 里嵌的是同一个
    dict 对象（实测 ``nodes[i]["childFlow"] is nodes[j]["childFlow"]``），属合法
    复用而非重复声明，不得告警。
    """
    seen: Dict[Any, Dict[str, Any]] = {}
    visited: Set[int] = set()

    def walk(scope: Dict[str, Any]) -> None:
        for node in scope.get("nodes") or []:
            if not isinstance(node, dict):
                continue
            for child in _iter_child_scopes(node):
                if id(child) in visited:
                    continue
                visited.add(id(child))
                flow_id = child.get("flow_id")
                if flow_id is not None:
                    if flow_id in seen:
                        warnings.warn(
                            f"内联子流程 flow_id 重复: {flow_id!r} 被多个不同的子流程"
                            "声明使用；flow_id 不参与节点寻址/查找（但会写进执行上下文，$FLOW_ID 可见），重复不影响"
                            "执行，但排障时无法区分——请改用唯一 id，或全部省略。",
                            UserWarning,
                            stacklevel=3,
                        )
                    else:
                        seen[flow_id] = child
                walk(child)

    walk(ir)


def _compile_fdef(
    fdef: ast.FunctionDef,
    flow_id: Optional[str],
    opts: Dict[str, Any],
    module_globals: Optional[Dict[str, Any]] = None,
    childflows: Optional[Dict[str, Dict[str, Any]]] = None,
    known_node_types: Optional[set] = None,
) -> Dict[str, Any]:
    """编译一个 ``FunctionDef`` AST 到 Flow IR dict（装饰器模式与源码模式共用）。"""
    ctx = _CompileCtx(
        module_globals=module_globals or {},
        childflows=childflows or {},
        known_node_types=known_node_types,
    )
    entry = _compile_block(list(fdef.body), ctx, succ=None)
    if entry is None:
        raise _CodeflowError("函数体为空", fdef)
    # 自动补 start 节点指向 entry
    start_id = ctx.auto_id("start")
    start_node = {"type": "start", "id": start_id, "next": entry}
    ctx.nodes.insert(0, start_node)

    data: Dict[str, Any] = {"runtime": "python", "flow_id": flow_id or fdef.name}
    _warn_deprecated_type_opts(opts)
    data["inputType"] = _default_input_type()
    for fld in ("desc", "version", "author", "timeout"):
        if opts.get(fld) is not None:
            data[fld] = opts[fld]
    gc = opts.get("global_context")
    if gc is not None:
        data["globalContext"] = gc
    md = opts.get("metadata")
    if md is not None:
        data["metadata"] = md
    data["nodes"] = ctx.nodes
    # 主流程是唯一能看到全部内嵌子流程的装配点（装饰器与 compile_source 两条
    # 入口都汇到这里），重名检测放这里可同时覆盖两条路径（#145）。
    _warn_duplicate_child_flow_ids(data)
    return data


def _compile_childflow_fdef(
    fdef: ast.FunctionDef,
    flow_id: Optional[str] = None,
    opts: Optional[Dict[str, Any]] = None,
    module_globals: Optional[Dict[str, Any]] = None,
    childflows: Optional[Dict[str, Dict[str, Any]]] = None,
    known_node_types: Optional[set] = None,
) -> Dict[str, Any]:
    """编译一个 ``@childflow`` FunctionDef 到子流程 IR dict。

    ``flow_id`` 来自 ``@childflow("<id>")`` 的位置参数。它**不参与节点寻址 /
    查找**（``flow.py`` 里四处引用全在 ``logger.warning`` 里），但**会写进执行
    上下文**（``context.py:314`` → ``$GLOBAL.flow_id``；``context.py:331`` /
    ``state.py:632`` → ``$FLOW_ID``，别名 ``$FLOW``），所以子流程内的表达式
    **能读到它**。不给就是 ``None``——**不回落函数名**（内联子流程没有函数名
    可用，回落会让两个同名子流程撞 id）；此时 ``_resolve_expression_string``
    的父上下文回落（``context.py:137-148``）让 ``$FLOW_ID`` 取到**父流程**的 id。
    """
    if flow_id is not None and not isinstance(flow_id, str):
        raise _CodeflowError(
            "@childflow 的位置参数是子流程 flow_id，必须是 str，"
            f"得到 {type(flow_id).__name__}: {flow_id!r}",
            fdef,
        )
    opts = opts or {}
    ctx = _CompileCtx(
        module_globals=module_globals or {},
        childflows=childflows or {},
        known_node_types=known_node_types,
    )
    entry = _compile_block(list(fdef.body), ctx, succ=None)
    if entry is None:
        raise _CodeflowError("子流程函数体为空", fdef)
    start_id = ctx.auto_id("start")
    ctx.nodes.insert(0, {"type": "start", "id": start_id, "next": entry})
    data: Dict[str, Any] = {"runtime": "python"}
    if flow_id is not None:
        data["flow_id"] = flow_id
    data["nodes"] = ctx.nodes
    _warn_deprecated_type_opts(opts)
    data["inputType"] = _default_input_type()
    if opts.get("desc") is not None:
        data["desc"] = opts["desc"]
    return data


def _compile_func(func: Callable, flow_id: Optional[str], opts: Dict[str, Any]) -> Dict[str, Any]:
    fdef = _func_ast(func)
    return _compile_fdef(
        fdef, flow_id, opts,
        module_globals=getattr(func, "__globals__", {}),
    )


def flow(
    flow_id: Optional[str] = None,
    *,
    desc: Optional[str] = None,
    version: Optional[str] = None,
    author: Optional[str] = None,
    timeout: Optional[str] = None,
    global_context: Optional[Dict[str, Any]] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Callable[[Callable], Flow]:
    """把一个纯 Python 函数编译成 ``Flow``。

    装饰后该名字绑定到 ``Flow`` 对象，调用 ``.run(**params)`` 或
    ``.run({...})`` 执行；``$INPUT`` 始终为传入的 dict。
    """
    opts = dict(desc=desc, version=version, author=author, timeout=timeout,
                global_context=global_context, metadata=metadata)

    def decorator(func: Callable) -> Flow:
        data = _compile_func(func, flow_id, opts)
        # 与 flow_from_source / FlowBuilder.build / parse_sexpr 同一校验门：
        # 历史上装饰器路径只走 Flow.model_validate（非致命 warning 版拓扑
        # 检查），同一前端两条入口校验双标。现在统一走 build_flow 硬门禁
        # （含 DEFAULT_RULES：缺 start / 不可达 / $NODE / $F 引用等）。
        from plaita.dsl.ir_validate import build_flow

        fl = build_flow(data)
        # 保留编译期 IR：装饰器路径没有源字符串，部署方追加规则（如
        # forbid_node_types_in_childflow）只能对 IR 复检，没有它就各自由
        # Flow 对象重写遍历——规则的单一实现会被稀释。
        fl.__plaita_ir__ = data  # type: ignore[attr-defined]
        fl.__wrapped__ = func  # type: ignore[attr-defined]
        return fl

    return decorator


def childflow(
    flow_id: Optional[str] = None,
    *,
    desc: Optional[str] = None,
) -> Callable[[Callable], "_ChildFlowMarker"]:
    """``@childflow`` 装饰一个子流程函数，供 ``CHILD/REFERENCE/PARALLEL`` 引用。

    编译产物挂在 ``.__codeflow_ir__`` 上，被父流程编译时取出嵌入。

    签名与 ``@flow`` 对齐：位置参数是子流程的 ``flow_id``，``desc`` 是纯关键字
    参数（``@childflow("cf")`` / ``@childflow(desc="…")`` / ``@childflow("cf", desc="…")``
    / ``@childflow()`` 均合法）。``flow_id`` **不参与节点寻址**，但**会写进执行
    上下文**（``$FLOW_ID`` / ``$GLOBAL.flow_id``），子流程内的表达式能读到它；
    不给即 ``None``，**不回落函数名**（此时 ``$FLOW_ID`` 经父上下文回落取到父
    flow 的 id）。详见 ``_compile_childflow_fdef`` 与
    ``plaita-ai/.../references/authoring-spec.md`` §2.7。
    """

    def decorator(func: Callable) -> _ChildFlowMarker:
        fdef = _func_ast(func)
        data = _compile_childflow_fdef(
            fdef,
            flow_id,
            {"desc": desc},
            module_globals=getattr(func, "__globals__", {}),
        )
        # 历史上 @childflow 完全无校验。这里先做一次独立校验（硬编码拓扑 +
        # DEFAULT_RULES），错误在装饰期即暴露；子 flow 的完整校验仍以父流程
        # 编译时随父 IR 递归校验为准（嵌入后 childFlow 子图还会再过一遍，
        # 两处规则集一致，不会出现"独立过、嵌入假错"的双标）。
        from plaita.dsl.ir_validate import DEFAULT_RULES, validate_flow_ir

        validate_flow_ir(data, rules=DEFAULT_RULES)
        return _ChildFlowMarker(data, func)

    return decorator


def compile_func(func: Callable, flow_id: Optional[str] = None, **opts: Any) -> Dict[str, Any]:
    """编译一个函数到 Flow IR dict（不构建 Flow，便于审查/序列化）。"""
    return _compile_func(func, flow_id, opts)


# ---------------------------------------------------------------------------
# 源码模式：从字符串编译（运行期生成场景，绕开 inspect.getsource）
# ---------------------------------------------------------------------------

_DECO_OPT_KEYS = (
    "desc", "version", "author", "timeout", "global_context", "metadata",
)
# 装饰器关键字参数名 → IR opts 键
_DECO_KW_ALIASES = {
    "input_type": "input_type", "inputType": "input_type",
    "output_type": "output_type", "outputType": "output_type",
    "desc": "desc", "description": "desc",
    "version": "version", "author": "author", "timeout": "timeout",
    "global_context": "global_context", "globalContext": "global_context",
    "metadata": "metadata",
}


def _deco_name(deco: ast.expr) -> Optional[str]:
    """``@flow`` / ``@childflow`` / ``@flow(...)`` → 返回装饰器基名。"""
    if isinstance(deco, ast.Name):
        return deco.id
    if isinstance(deco, ast.Call) and isinstance(deco.func, ast.Name):
        return deco.func.id
    return None


def _eval_deco_value(node: ast.AST) -> Any:
    """把装饰器参数节点求值为 Python 值（仅字面量；非字面量返回 None 跳过）。"""
    try:
        return ast.literal_eval(node)
    except (ValueError, SyntaxError):
        return None


def _extract_deco_opts(deco: ast.expr) -> Tuple[Optional[Any], Dict[str, Any]]:
    """从 ``@flow("id", input_type=..., desc=...)`` 取 (位置 flow_id, opts)。"""
    if not isinstance(deco, ast.Call):
        return None, {}
    pos_id: Optional[Any] = None
    if deco.args:
        v = _eval_deco_value(deco.args[0])
        if v is not None:
            pos_id = v
    opts: Dict[str, Any] = {}
    for kw in deco.keywords:
        if kw.arg is None:
            continue
        key = _DECO_KW_ALIASES.get(kw.arg)
        if key is None:
            continue
        val = _eval_deco_value(kw.value)
        if val is not None:
            opts[key] = val
    return pos_id, opts


def compile_source(src: str, flow_id: Optional[str] = None, **opts: Any) -> Dict[str, Any]:
    """把一段 Python 源码字符串编译成 Flow IR dict。

    源码里可含多个函数：``@childflow`` 装饰的子流程会被收集进注册表，
    供主流程的 ``CHILD/REFERENCE/PARALLEL`` 用 ``flow=<name>`` 引用；
    主流程是 ``@flow`` 装饰的函数，或唯一一个非 childflow 函数。

    显式传入的 ``flow_id`` / ``opts`` 覆盖装饰器里的同名字段。
    """
    src = textwrap.dedent(src)
    mod = ast.parse(src)
    childflows: Dict[str, Dict[str, Any]] = {}
    main_candidates: List[ast.FunctionDef] = []
    flow_deco_candidates: List[ast.FunctionDef] = []

    for stmt in mod.body:
        if not isinstance(stmt, ast.FunctionDef):
            continue
        deco_names = [n for n in (_deco_name(d) for d in stmt.decorator_list) if n]
        if "childflow" in deco_names:
            cf_deco = next(d for d in stmt.decorator_list if _deco_name(d) == "childflow")
            cf_flow_id, cf_opts = _extract_deco_opts(cf_deco)
            childflows[stmt.name] = _compile_childflow_fdef(
                stmt, cf_flow_id, cf_opts, childflows=childflows)
            continue
        if "flow" in deco_names:
            flow_deco_candidates.append(stmt)
        main_candidates.append(stmt)

    # 选主流程
    chosen: Optional[ast.FunctionDef] = None
    if flow_id is not None:
        for c in main_candidates:
            if c.name == flow_id:
                chosen = c
                break
        if chosen is None:
            raise _CodeflowError(f"源码里找不到名为 {flow_id!r} 的函数", None)
    elif len(flow_deco_candidates) == 1:
        chosen = flow_deco_candidates[0]
    elif len(main_candidates) == 1:
        chosen = main_candidates[0]
    elif len(main_candidates) == 0:
        raise _CodeflowError("源码里找不到可编译的函数", None)
    else:
        raise _CodeflowError(
            "源码里有多个候选函数，请用 flow_id=... 指定主流程", None)

    # 合并 @flow 装饰器参数 + 显式 opts（显式覆盖）
    merged: Dict[str, Any] = {}
    deco_flow_id: Optional[Any] = None
    flow_deco = next((d for d in chosen.decorator_list if _deco_name(d) == "flow"), None)
    if flow_deco is not None:
        deco_flow_id, deco_opts = _extract_deco_opts(flow_deco)
        merged.update(deco_opts)
    merged.update(opts)

    final_id = flow_id or deco_flow_id or chosen.name
    return _compile_fdef(chosen, final_id, merged, childflows=childflows)


def flow_from_source(src: str, flow_id: Optional[str] = None, **opts: Any) -> Flow:
    """把一段 Python 源码字符串编译成 ``Flow`` 并返回（运行期生成场景）。

    经共享 ``validate_flow_ir``（拓扑）再 ``Flow.model_validate``，
    与 ``FlowBuilder.build`` / ``parse_sexpr`` 同一校验门。
    """
    from plaita.dsl.ir_validate import build_flow

    return build_flow(compile_source(src, flow_id, **opts))
