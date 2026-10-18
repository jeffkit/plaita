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
from plaita.dsl.codeflow._expr import _compile_condition, _compile_expr

def _apply_base_kwargs(spec: Dict[str, Any], kw: Dict[str, Any], ctx: _CompileCtx) -> None:
    """#142 编译侧 / #143：把 **``Node`` 基类共有**的字段在所有内置 kind 上统一消费。

    根因：内置 kind 走**手写 ``if`` 链**、无 catch-all、也不报未知 kwarg ⇒ 链外的
    kwarg 被**静默丢弃**（HTTP 有 `timeout`、CODE/EVENT 没有；`on_error` 只在 HTTP
    与自定义节点两条路径被消费）。而 ``Node`` 基类**本来就有** ``timeout``
    （``basic.py:95``）与 ``timeout_handler``（``:101``）⇒ 这些不是新契约，
    是**既有契约在手写链里漏了**。

    **不覆盖** spec 里已有的键（各 kind 自己的写法优先）。
    """
    if "timeout" in kw and "timeout" not in spec:
        spec["timeout"] = _compile_expr(kw["timeout"], ctx)
    th = kw.get("timeoutHandler") or kw.get("timeout_handler") or kw.get("on_timeout")
    if th is not None and "timeoutHandler" not in spec:
        spec["timeoutHandler"] = _eval_error_handler(th, ctx)
    eh = kw.get("on_error") or kw.get("on_error_handler")
    if eh is not None and "errorHandler" not in spec:
        spec["errorHandler"] = _eval_error_handler(eh, ctx)


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
        # #238：Content-Type 此前两侧都没有表达面 ⇒ 往返静默落回默认。
        content_type = kw.get("content_type") or kw.get("contentType")
        if content_type is not None:
            spec["content_type"] = _const(content_type)
        # plaita#220/L4：output / query / addressing / delegate 此前不在内置
        # kind 的手写链里 ⇒ 发射器写出也会被编译期静默丢弃（与 L4 的 sexpr
        # 链同一面）。字段名 snake == camel，无需双拼写。
        for ir_key in ("output", "query", "addressing", "delegate"):
            if ir_key in kw:
                spec[ir_key] = _const(kw[ir_key])
        # #200：HTTP 访问策略字段（SSRF 防护面）此前不在内置 kind 的手写链里
        # ⇒ 发射器即使写出 `allowed_hosts=...` 也会被编译期静默丢弃。camel 与
        # snake 两种 kwarg 拼写都收（与 `_emit.py` 的发射侧对称）。
        for ir_key, spellings in (
            ("allowedHosts", ("allowed_hosts", "allowedHosts")),
            ("deniedHosts", ("denied_hosts", "deniedHosts")),
            ("blockPrivateNetworks",
             ("block_private_networks", "blockPrivateNetworks")),
            ("requestTimeout", ("request_timeout", "requestTimeout")),
            ("maxRedirects", ("max_redirects", "maxRedirects")),
        ):
            for name in spellings:
                if name in kw:
                    spec[ir_key] = _const(kw[name])
                    break
        eh = kw.get("on_error") or kw.get("on_error_handler")
        if eh is not None:
            spec["errorHandler"] = _eval_error_handler(eh, ctx)
        _apply_base_kwargs(spec, kw, ctx)
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
        # CodeNode 的节点级字段必须原样进 IR。漏掉 sandbox_backend 会让源码里
        # 显式声明的沙箱档位被静默丢弃，运行期一律落到 register_code_node 的
        # default_backend——2026-09-28 实证：flow 源码写了 sandbox_backend="unsafe"，
        # 编译产物里却是 None，deliver/merge 这类要跑 git push 的节点被默认
        # subprocess 后端的 10s 墙钟掐死，报成引擎异常。
        if kw.get("sandbox_backend") is not None:
            spec["sandbox_backend"] = _const(kw["sandbox_backend"])
        _apply_base_kwargs(spec, kw, ctx)
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
        # #200：订阅超时同样不在内置链里，发射出来也会被静默丢弃。
        for name in ("subscription_timeout", "subscriptionTimeout"):
            if name in kw:
                spec["subscriptionTimeout"] = _const(kw[name])
                break
        _apply_base_kwargs(spec, kw, ctx)
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
        _apply_base_kwargs(spec, kw, ctx)
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
        _apply_base_kwargs(spec, kw, ctx)
        return spec

    if kind == "ASSIGN":
        # plaita#220/L5：赋值节点的 `outputType` / `upstreamOutput` 在
        # `n = expr` 语句形态里没有表达面（发射器 `_emit_assignment` 只能写
        # 赋值右侧）。带附加字段时发射器改用本显式构造形，编译回同一个
        # `type=assignment` 节点（IR 键用 console/sexpr 一致的 camelCase）。
        if len(pos) > 1:
            raise _CodeflowError("ASSIGN 最多一个位置参数（output）", node)
        output = kw.get("output")
        if output is None and pos:
            output = pos[0]
        if output is None:
            raise _CodeflowError("ASSIGN 需要 output 表达式", node)
        spec = {
            "type": "assignment",
            "id": nid,
            "output": _compile_expr(output, ctx),
        }
        ot = kw.get("output_type")
        if ot is None:
            ot = kw.get("outputType")
        if ot is not None:
            spec["outputType"] = _compile_expr(ot, ctx)
        up = kw.get("upstream_output")
        if up is None:
            up = kw.get("upstreamOutput")
        if up is not None:
            spec["upstreamOutput"] = _compile_expr(up, ctx)
        _apply_base_kwargs(spec, kw, ctx)
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
    # #238：`on_timeout=ErrorHandler(...)` 必须在通用 kwarg 循环前消费——否则
    # `ErrorHandler(...)` 会被当普通表达式编译并直接报「不支持的调用」。
    th = kw.pop("on_timeout", None)
    if th is not None:
        spec["timeoutHandler"] = _eval_error_handler(th, ctx)
    for arg_name, arg_node in kw.items():
        if arg_name is None:  # **kwargs 解包
            raise _CodeflowError("自定义节点不支持 **kwargs 解包", node)
        spec[arg_name] = _compile_expr(arg_node, ctx)
    _apply_base_kwargs(spec, kw, ctx)
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
            branches.append(_parallel_branch_value(k.value, v, ctx))
        return branches
    if isinstance(node, ast.List):
        for elt in node.elts:
            if not (isinstance(elt, ast.Tuple) and 2 <= len(elt.elts) <= 4):
                raise _CodeflowError(
                    "PARALLEL 分支需要 (name, flow) 元组"
                    "（可选第 3/4 位为 input / condition）", node)
            name_e = elt.elts[0]
            if not (isinstance(name_e, ast.Constant) and isinstance(name_e.value, str)):
                raise _CodeflowError("PARALLEL 分支名必须是字符串常量", name_e)
            extra = list(elt.elts[2:])
            branches.append(_parallel_branch(
                name_e.value, elt.elts[1], ctx,
                input_node=extra[0] if len(extra) > 0 else None,
                condition_node=extra[1] if len(extra) > 1 else None,
            ))
        return branches
    raise _CodeflowError(
        "PARALLEL branches 需要是 dict 字面量或 (name, flow) 元组列表"
        "（值可为 {'flow': …, 'input': …, 'condition': …} 的 dict；"
        "元组可选第 3/4 位 input / condition）", node)


def _parallel_branch_value(name: str, value: ast.AST, ctx: _CompileCtx) -> Dict[str, Any]:
    """dict 形态的分支值：``flow_func`` 或 ``{"flow": …, "input": …, "condition": …}``。

    plaita#220/L2：@flow 此前只发 ``branches={name: childflow}``，分支的
    ``input``/``condition`` 全丢——条件分支退化成无条件分支、分支输入丢失。
    分支值的显式 dict 形态让这两个可选字段有表达面（sexpr/IR 早已保留）。
    """
    if not isinstance(value, ast.Dict):
        return _parallel_branch(name, value, ctx)
    fields: Dict[str, ast.AST] = {}
    for k, v in zip(value.keys, value.values):
        if not (isinstance(k, ast.Constant) and isinstance(k.value, str)):
            raise _CodeflowError("PARALLEL 分支字段名必须是字符串常量", value)
        fields[k.value] = v
    flow_node = fields.get("flow")
    if flow_node is None:
        raise _CodeflowError("PARALLEL 分支 dict 需要 flow=<@childflow 函数>", value)
    unknown = set(fields) - {"flow", "input", "condition"}
    if unknown:
        raise _CodeflowError(
            f"PARALLEL 分支 dict 有未知字段 {sorted(unknown)}"
            "（可用：flow / input / condition）", value)
    return _parallel_branch(
        name, flow_node, ctx,
        input_node=fields.get("input"),
        condition_node=fields.get("condition"),
    )


def _parallel_branch(
    name: str,
    flow_node: ast.AST,
    ctx: _CompileCtx,
    input_node: Optional[ast.AST] = None,
    condition_node: Optional[ast.AST] = None,
) -> Dict[str, Any]:
    spec: Dict[str, Any] = {"name": name, "flow": _eval_childflow_arg(flow_node, ctx)}
    if input_node is not None:
        spec["input"] = _compile_expr(input_node, ctx)
    if condition_node is not None:
        spec["condition"] = _compile_condition(condition_node, ctx)
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

