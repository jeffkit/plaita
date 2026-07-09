"""Statement compilation for @flow AST (if/for/assign/return blocks)."""
from __future__ import annotations

import ast
from typing import Any, Dict, List, Optional

from plaita.dsl.codeflow._common import (
    _COLLECTION_CALL_NAMES,
    _CodeflowError,
    _CompileCtx,
    _annotate_source,
    _const_bool,
    _custom_node_type,
    _node_call_kind,
    _unpack_names,
)
from plaita.dsl.codeflow._expr import (
    _compile_condition,
    _compile_expr,
)
from plaita.dsl.codeflow._nodes import _compile_node_call

def _compile_block(
    stmts: List[ast.stmt],
    ctx: _CompileCtx,
    succ: Optional[str],
) -> Optional[str]:
    """编译一个语句块，返回入口节点 id（块为空时返回 succ）。

    ``succ`` 是「正常 fall-through 后该去哪」；为 None 表示该块必须自行终止
    （用 return/end），否则报错。
    """
    if not stmts:
        return succ
    head, rest = stmts[0], stmts[1:]

    if isinstance(head, ast.Return):
        end_id = ctx.auto_id()
        output = _compile_expr(head.value, ctx) if head.value is not None else None
        end_node: Dict[str, Any] = {
            "type": "end", "id": end_id,
            "output": output, "resultType": "success",
        }
        _annotate_source(end_node, head)
        ctx.nodes.append(end_node)
        if rest:
            raise _CodeflowError("return 之后还有不可达语句", rest[0])
        return end_id

    if isinstance(head, ast.If):
        return _compile_if(head, ctx, succ, rest)

    if isinstance(head, ast.For):
        return _compile_for(head, ctx, succ, rest)

    if isinstance(head, ast.Assign):
        return _compile_assign(head, ctx, succ, rest)

    if isinstance(head, ast.Expr):
        return _compile_expr_stmt(head, ctx, succ, rest)

    if isinstance(head, ast.Pass):
        return _compile_block(rest, ctx, succ)

    raise _CodeflowError(f"不支持的语句 {type(head).__name__}", head)


def _compile_if(
    head: ast.If, ctx: _CompileCtx, succ: Optional[str], rest: List[ast.stmt],
) -> str:
    cond = _compile_condition(head.test, ctx)
    if_id = ctx.auto_id()
    # 先在节点列表里占位，保证输出顺序 if 在前
    if_node: Dict[str, Any] = {"type": "if", "id": if_id, "condition": cond}
    _annotate_source(if_node, head)
    ctx.nodes.append(if_node)

    # if 之后的语句入口（即两条分支 fall-through 的去处）
    after = _compile_block(rest, ctx, succ)

    # elif 链：orelse 里单个 If 视为 elif，编译成 if 节点（复用 _compile_if）
    body_entry = _compile_block(head.body, ctx, after)
    if body_entry is None:
        body_entry = after
    if head.orelse:
        orelse_entry = _compile_block(head.orelse, ctx, after)
        if orelse_entry is None:
            orelse_entry = after
    else:
        orelse_entry = after

    if body_entry is None:
        raise _CodeflowError("if 真分支悬空：请补 return 或后续语句", head)
    if orelse_entry is None:
        raise _CodeflowError("if 假分支悬空：请补 else/return 或后续语句", head)

    if_node["next"] = body_entry
    if_node["else_next"] = orelse_entry
    return if_id


def _compile_for(
    head: ast.For, ctx: _CompileCtx, succ: Optional[str], rest: List[ast.stmt],
) -> str:
    """``for x in MAP/FILTER/FIND/LOOP(...)`` / ``for a,b in REDUCE(...)``。"""
    coll_call = head.iter
    if not isinstance(coll_call, ast.Call) or not isinstance(coll_call.func, ast.Name) \
            or coll_call.func.id not in _COLLECTION_CALL_NAMES:
        raise _CodeflowError(
            "for 循环的迭代对象必须是 MAP/FILTER/FIND/LOOP/REDUCE(...) 节点调用", coll_call)
    kind = coll_call.func.id
    kw = {k.arg: k.value for k in coll_call.keywords}
    pos = coll_call.args
    if not pos:
        raise _CodeflowError(f"{kind} 需要一个集合表达式", coll_call)
    collection = _compile_expr(pos[0], ctx)

    # 循环变量 → 子流程的 $INPUT.item / $INPUT.index / $INPUT.first / $INPUT.second
    loop_vars: Dict[str, str] = {}
    target = head.target
    child_input_type: Dict[str, Any] = {"dataType": "object"}
    if kind == "REDUCE":
        # Reduce 节点以位置参数 (first, second) 调子流程，object 输入下会拿不到，
        # 故子流程用 array 输入，按 $INPUT[0]/[1] 取 first/second。
        names = _unpack_names(target)
        if len(names) != 2:
            raise _CodeflowError("REDUCE 的循环变量必须是 (first, second) 两个名字", target)
        loop_vars[names[0]] = "$INPUT[0]"
        loop_vars[names[1]] = "$INPUT[1]"
        child_input_type = {"dataType": "array"}
    else:
        names = _unpack_names(target)
        loop_vars[names[0]] = "$INPUT.item"
        if len(names) > 1:
            loop_vars[names[1]] = "$INPUT.index"

    child_ctx = _CompileCtx(loop_vars=loop_vars, module_globals=ctx.module_globals)
    child_entry = _compile_block(list(head.body), child_ctx, succ=None)  # 子流程体必须自行 return
    if child_entry is None:
        raise _CodeflowError("循环体为空或全部悬空：请补 return", head.body[0] if head.body else head)

    # 节点 id：优先 id= 关键字，否则自动
    node_id = None
    id_kw = kw.get("id")
    if id_kw is not None and isinstance(id_kw, ast.Constant):
        node_id = str(id_kw.value)
    node_id = ctx.auto_id(node_id)

    # 子流程节点列表前置一个 Start 节点指向编译出的入口节点；
    # Flow.start_node 2026-07 起不再做"入度 0 推断"，没有 Start 就直接报错。
    child_start_id = child_ctx.auto_id("start")
    child_nodes: List[Dict[str, Any]] = [
        {"type": "start", "id": child_start_id, "next": child_entry},
        *child_ctx.nodes,
    ]

    spec: Dict[str, Any] = {
        "type": kind.lower(),
        "id": node_id,
        "collection": collection,
        "childFlow": {
            "runtime": "python",
            "inputType": child_input_type,
            "nodes": child_nodes,
        },
    }
    if kind == "MAP":
        if "concurrent" in kw and _const_bool(kw["concurrent"]):
            spec["concurrent"] = True
            mc = kw.get("max_concurrent") or kw.get("maxConcurrent")
            if mc is not None and isinstance(mc, ast.Constant):
                spec["maxConcurrent"] = mc.value
    if kind == "REDUCE":
        init = kw.get("initial")
        if init is not None:
            spec["initial"] = _compile_expr(init, ctx)

    after = _compile_block(rest, ctx, succ)
    if after is None:
        raise _CodeflowError("集合节点之后悬空：请补 return 或后续语句", head)
    spec["next"] = after
    _annotate_source(spec, head)
    ctx.nodes.append(spec)
    return node_id


def _compile_assign(
    head: ast.Assign, ctx: _CompileCtx, succ: Optional[str], rest: List[ast.stmt],
) -> str:
    if len(head.targets) != 1 or not isinstance(head.targets[0], ast.Name):
        raise _CodeflowError("赋值目标必须是单个变量名", head)
    name = head.targets[0].id
    value = head.value

    # 先登记名字映射（不预 claim，避免与 _compile_node_call 的 auto_id 冲突），
    # 这样后续语句引用 name 时能解析成 $NODE.<name>
    ctx.names[name] = f"$NODE.{name}"
    anchor = len(ctx.nodes)
    after = _compile_block(rest, ctx, succ)
    if after is None:
        raise _CodeflowError(f"赋值 {name} 之后悬空：请补 return 或后续语句", head)

    if isinstance(value, ast.Call) and (
        _node_call_kind(value.func) is not None
        or _custom_node_type(value.func, ctx) is not None
    ):
        spec = _compile_node_call(value, ctx, name)
        spec["next"] = after
        _annotate_source(spec, value)
        ctx.nodes.insert(anchor, spec)
        return name

    output = _compile_expr(value, ctx)
    ctx.claim(name)
    assign_node: Dict[str, Any] = {
        "type": "assignment", "id": name, "output": output, "next": after,
    }
    _annotate_source(assign_node, head)
    ctx.nodes.insert(anchor, assign_node)
    return name


def _compile_expr_stmt(
    head: ast.Expr, ctx: _CompileCtx, succ: Optional[str], rest: List[ast.stmt],
) -> Optional[str]:
    value = head.value
    anchor = len(ctx.nodes)
    after = _compile_block(rest, ctx, succ)
    if after is None:
        raise _CodeflowError("表达式语句之后悬空：请补 return 或后续语句", head)
    if isinstance(value, ast.Call) and (
        _node_call_kind(value.func) is not None
        or _custom_node_type(value.func, ctx) is not None
    ):
        spec = _compile_node_call(value, ctx, None)
        spec["next"] = after
        _annotate_source(spec, value)
        ctx.nodes.insert(anchor, spec)
        return spec["id"]
    nid = ctx.auto_id()
    expr_node: Dict[str, Any] = {
        "type": "assignment", "id": nid, "output": _compile_expr(value, ctx), "next": after,
    }
    _annotate_source(expr_node, head)
    ctx.nodes.insert(anchor, expr_node)
    return nid

