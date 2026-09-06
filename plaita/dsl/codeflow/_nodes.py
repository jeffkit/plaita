"""Node-call compilation for @flow AST (HTTP/CODE/EVENT/CHILD/...)."""
from __future__ import annotations

import ast
from typing import Any, Dict, List, Optional

from plaita.dsl.codeflow._common import (
    _ChildFlowMarker,
    _CodeflowError,
    _CompileCtx,
    _const_bool,
    _custom_node_type,
    _is_upper_ident,
    _node_call_kind,
    _raise_if_unregistered_custom,
)
from plaita.dsl.codeflow._expr import _compile_expr

def _compile_node_call(
    node: ast.Call, ctx: _CompileCtx, assign_name: Optional[str],
) -> Dict[str, Any]:
    kind = _node_call_kind(node.func)
    is_custom = kind is None
    if kind is None:
        kind = _custom_node_type(node.func, ctx)
    if kind is None:
        # 大写占位名但未注册 → 可读错误（列可用类型，供 AI 自纠）
        if isinstance(node.func, ast.Name) and _is_upper_ident(node.func.id):
            _raise_if_unregistered_custom(node.func, ctx)
        raise _CodeflowError("不支持的节点调用", node)
    kw = {k.arg: k.value for k in node.keywords}
    pos = node.args

    def _const(k: Optional[ast.expr]) -> Any:
        if k is None:
            return None
        return _compile_expr(k, ctx)

    # 自定义节点的 id 由 _compile_custom_node 自行 claim（支持 id= 覆盖）；
    # 内置节点在此处统一 claim/auto_id。
    nid = None if is_custom else (ctx.claim(assign_name) if assign_name else ctx.auto_id())

    if kind == "HTTP":
        method = "POST"
        # HTTP.post(...) / HTTP.get(...) / HTTP(url=..., method=...)
        if isinstance(node.func, ast.Attribute):  # type: ignore[redudundant-expr]
            method = node.func.attr.upper()  # type: ignore[attr-defined]
        m_kw = kw.get("method")
        if m_kw is not None and isinstance(m_kw, ast.Constant):
            method = str(m_kw.value).upper()
        url = _const(kw.get("url")) or (pos[0].value if pos and isinstance(pos[0], ast.Constant) else None)
        if url is None:
            raise _CodeflowError("HTTP 需要 url", node)
        spec: Dict[str, Any] = {"type": "http", "id": nid, "method": method, "url": url}
        if "headers" in kw:
            spec["headers"] = _const(kw["headers"])
        if "body" in kw:
            spec["body"] = _const(kw["body"])
        if "timeout" in kw:
            spec["timeout"] = _const(kw["timeout"])
        if "input" in kw:
            spec["input"] = _const(kw["input"])
        eh = kw.get("on_error") or kw.get("on_error_handler")
        if eh is not None:
            spec["errorHandler"] = _eval_error_handler(eh, ctx)
        return spec

    if kind == "CODE":
        code = _const(kw.get("code"))
        lang = _const(kw.get("lang") or kw.get("language"))
        if isinstance(node.func, ast.Attribute):  # CODE.python("...")
            lang = node.func.attr  # type: ignore[attr-defined]
        if code is None and pos:
            code = pos[0].value if isinstance(pos[0], ast.Constant) else _compile_expr(pos[0], ctx)
        if code is None or lang is None:
            raise _CodeflowError("CODE 需要 code 和 lang", node)
        spec = {"type": "code", "id": nid, "language": lang, "code": code}
        if "input" in kw:
            spec["input"] = _const(kw["input"])
        return spec

    if kind == "EVENT":
        etype = _const(kw.get("type") or kw.get("event_type") or kw.get("eventType"))
        if etype is None and pos:
            etype = pos[0].value if isinstance(pos[0], ast.Constant) else _compile_expr(pos[0], ctx)
        if etype is None:
            raise _CodeflowError("EVENT 需要 type", node)
        spec = {"type": "event", "id": nid, "eventType": etype}
        if "filter" in kw:
            spec["eventFilter"] = _const(kw["filter"])
        return spec

    if kind in ("CHILD", "REFERENCE"):
        inp = _const(kw.get("input"))
        if inp is None and pos:
            inp = _compile_expr(pos[0], ctx)
        flow_arg = kw.get("flow")
        if flow_arg is None:
            raise _CodeflowError(f"{kind} 需要 flow=<childflow 函数>", node)
        child_ir = _eval_childflow_arg(flow_arg, ctx)
        spec = {"type": kind.lower(), "id": nid, "input": inp, "childFlow": child_ir}
        return spec

    if kind == "PARALLEL":
        branches_arg = kw.get("branches") or (pos[0] if pos else None)
        if branches_arg is None:
            raise _CodeflowError("PARALLEL 需要 branches", node)
        branches = _eval_parallel_branches(branches_arg, ctx)
        spec = {"type": "parallel", "id": nid, "branches": branches, "mode": "thread"}
        if "mode" in kw and isinstance(kw["mode"], ast.Constant):
            spec["mode"] = kw["mode"].value
        join = kw.get("join")
        if join is not None:
            spec["joinBranches"] = _eval_join(join, ctx)
        if "conditional" in kw and _const_bool(kw["conditional"]):
            spec["isConditional"] = True
        return spec

    # 走到这里：kind 来自 _custom_node_type（自定义节点占位符）
    return _compile_custom_node(node, ctx, assign_name, kind)


def _compile_custom_node(
    node: ast.Call, ctx: _CompileCtx, assign_name: Optional[str], node_type: str,
) -> Dict[str, Any]:
    """编译自定义节点调用 → ``{"type": node_type, "id": ..., <字段>: ...}``。

    字段值经 ``_compile_expr`` 编译（``INPUT.x`` → ``$INPUT.x``，字面量原样，
    含 ``{% ... %}`` 模板的字符串原样透传给节点的 ``execute`` 自行求值）。
    通用字段：``id=``（覆盖节点 id）、``timeout=``、``on_error=ErrorHandler(...)``；
    其余 kwargs 按名进 IR，须与注册 Node 子类的字段名（snake_case）一致。
    """
    if node.args:
        raise _CodeflowError(
            f"自定义节点 {node_type} 只接受关键字参数（字段名=值），不支持位置参数", node)
    kw = {k.arg: k.value for k in node.keywords}

    id_kw = kw.pop("id", None)
    if assign_name:
        if id_kw is not None:
            raise _CodeflowError(
                f"自定义节点已用赋值变量 {assign_name!r} 作为 id，不要同时传 id=", id_kw)
        nid = ctx.claim(assign_name)
    elif id_kw is not None:
        if not isinstance(id_kw, ast.Constant) or not isinstance(id_kw.value, str):
            raise _CodeflowError("自定义节点 id= 必须是字符串常量", id_kw)
        nid = ctx.claim(id_kw.value)
    else:
        nid = ctx.auto_id()

    spec: Dict[str, Any] = {"type": node_type, "id": nid}
    if "timeout" in kw:
        spec["timeout"] = _compile_expr(kw.pop("timeout"), ctx)
    eh = kw.pop("on_error", None) or kw.pop("on_error_handler", None)
    if eh is not None:
        spec["errorHandler"] = _eval_error_handler(eh, ctx)
    for arg_name, arg_node in kw.items():
        if arg_name is None:  # **kwargs 解包
            raise _CodeflowError("自定义节点不支持 **kwargs 解包", node)
        spec[arg_name] = _compile_expr(arg_node, ctx)
    return spec


def _eval_error_handler(node: ast.AST, ctx: _CompileCtx) -> Dict[str, Any]:
    # ErrorHandler(...) 直接构造 —— 但函数体不执行，所以不能调；这里识别 AST 调用
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) \
            and node.func.id == "ErrorHandler":
        kw = {k.arg: k.value for k in node.keywords}
        pos = node.args
        strategy = pos[0].value if pos and isinstance(pos[0], ast.Constant) else "abort"
        if "strategy" in kw and isinstance(kw["strategy"], ast.Constant):
            strategy = kw["strategy"].value
        # "continue-with" 是 ErrorStrategy 的规范枚举值——R1 已让
        # builder/sexpr/_common 三处接受，这里补齐 codeflow AST 路径，
        # 五前端口径一致。
        if strategy == "continue-with":
            strategy = "continue_with"
        if strategy not in ("abort", "continue", "continue_with"):
            raise _CodeflowError(
                f"unknown error handler strategy: {strategy!r} "
                f"(valid: abort / continue / continue_with / continue-with)",
                node,
            )
        spec: Dict[str, Any] = {"strategy": strategy}
        rt = kw.get("retry_times") or kw.get("retryTimes")
        if rt is not None and isinstance(rt, ast.Constant):
            spec["retryTimes"] = rt.value
        dv = kw.get("default") or kw.get("default_value") or kw.get("defaultValue")
        if dv is not None:
            spec["defaultValue"] = _compile_expr(dv, ctx)
        ec = kw.get("error_code") or kw.get("errorCode")
        if ec is not None and isinstance(ec, ast.Constant):
            spec["errorCode"] = ec.value
        em = kw.get("error_message") or kw.get("errorMessage")
        if em is not None and isinstance(em, ast.Constant):
            spec["errorMessage"] = em.value
        return spec
    raise _CodeflowError("on_error 必须是 ErrorHandler(...) 调用", node)


def _eval_childflow_arg(node: ast.AST, ctx: _CompileCtx) -> Dict[str, Any]:
    if isinstance(node, ast.Name):
        # 源码模式：先查注册表
        if node.id in ctx.childflows:
            return ctx.childflows[node.id]
        # 装饰器模式：查模块全局里的 _ChildFlowMarker
        obj = ctx.module_globals.get(node.id)
        if isinstance(obj, _ChildFlowMarker):
            return obj.__codeflow_ir__
        raise _CodeflowError(
            f"{node.id!r} 不是 @childflow 装饰的子流程", node)
    raise _CodeflowError("不支持的 childflow 参数", node)


def _eval_parallel_branches(node: ast.AST, ctx: _CompileCtx) -> List[Dict[str, Any]]:
    branches: List[Dict[str, Any]] = []
    if isinstance(node, ast.Dict):
        for k, v in zip(node.keys, node.values):
            if not (isinstance(k, ast.Constant) and isinstance(k.value, str)):
                raise _CodeflowError("PARALLEL 分支名必须是字符串常量", node)
            branches.append(_parallel_branch(k.value, v, ctx))
        return branches
    if isinstance(node, ast.List):
        for elt in node.elts:
            if not (isinstance(elt, ast.Tuple) and len(elt.elts) == 2):
                raise _CodeflowError("PARALLEL 分支需要 (name, flow) 元组", node)
            name_e, flow_e = elt.elts
            if not (isinstance(name_e, ast.Constant) and isinstance(name_e.value, str)):
                raise _CodeflowError("PARALLEL 分支名必须是字符串常量", name_e)
            branches.append(_parallel_branch(name_e.value, flow_e, ctx))
        return branches
    raise _CodeflowError("PARALLEL branches 需要是 dict 字面量或 (name, flow) 元组列表", node)


def _parallel_branch(name: str, flow_node: ast.AST, ctx: _CompileCtx) -> Dict[str, Any]:
    spec: Dict[str, Any] = {"name": name, "flow": _eval_childflow_arg(flow_node, ctx)}
    return spec


def _eval_join(node: ast.AST, ctx: _CompileCtx) -> List[str]:
    if isinstance(node, ast.List):
        out = []
        for e in node.elts:
            if isinstance(e, ast.Constant):
                out.append(str(e.value))
            else:
                raise _CodeflowError("join 列表元素必须是字符串常量", node)
        return out
    raise _CodeflowError("join 必须是列表字面量", node)

