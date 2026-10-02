"""Statement compilation for @flow AST (if/for/assign/return blocks)."""
from __future__ import annotations

import ast
from typing import Any, Dict, List, Optional

from plaita.dsl.codeflow._common import (
    _COLLECTION_CALL_NAMES,
    _NS_PREFIX,
    _CodeflowError,
    _CompileCtx,
    _annotate_source,
    _cond_slug,
    _const_bool,
    _custom_node_type,
    _human_label,
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
        output = _compile_expr(head.value, ctx) if head.value is not None else None
        # 语义化 id/标签：return "A" -> ret_a；canvas/dry-run/报错不再显示 _n3
        ret_slug = _cond_slug(head.value)
        end_id = ctx.semantic_id(f"ret_{ret_slug}" if ret_slug else None)
        expr_text = ast.unparse(head.value) if head.value is not None else ""
        end_node: Dict[str, Any] = {
            "type": "end", "id": end_id,
            "output": output, "resultType": "success",
        }
        if expr_text:
            end_node["name"] = _human_label(f"return {expr_text}")
        end_node["desc"] = _human_label(f"return {expr_text}（第 {head.lineno} 行）", 60)
        _annotate_source(end_node, head)
        ctx.nodes.append(end_node)
        if rest:
            raise _CodeflowError("return 之后还有不可达语句", rest[0])
        return end_id

    if isinstance(head, ast.If):
        return _compile_if(head, ctx, succ, rest)

    if isinstance(head, ast.While):
        return _compile_while(head, ctx, succ, rest)

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
    # 语义化 id/标签：if INPUT.score >= 90 -> id=score_ge_90，画布/报错自带语义
    cond_text = ast.unparse(head.test)
    if_id = ctx.semantic_id(_cond_slug(head.test))
    # 先在节点列表里占位，保证输出顺序 if 在前
    if_node: Dict[str, Any] = {
        "type": "if", "id": if_id, "condition": cond,
        "name": _human_label(f"{cond_text}?"),
        "desc": _human_label(f"if {cond_text}（第 {head.lineno} 行）", 80),
    }
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


# While 循环变量名 → 上下文键。条件与体的求值上下文不同：
# 条件在引擎侧拿 $LOOP-ITEM/$LOOP-INDEX（父 context + 注入键），
# 体是子流程，item/rounds 走 $INPUT.item/$INPUT.index。
_WHILE_COND_VARS: Dict[str, str] = {"item": "$LOOP-ITEM", "rounds": "$LOOP-INDEX"}
_WHILE_BODY_VARS: Dict[str, str] = {"item": "$INPUT.item", "rounds": "$INPUT.index"}


def _while_cond(test: ast.expr, ctx: _CompileCtx) -> Dict[str, Any]:
    """编译 while 条件：``item``→$LOOP-ITEM、``rounds``→$LOOP-INDEX，外层名按父侧引用。"""
    cond_ctx = _CompileCtx(module_globals=ctx.module_globals)
    cond_ctx.names = {**ctx.names, **_WHILE_COND_VARS}
    return _compile_condition(test, cond_ctx)


def _while_child_flow(body: List[ast.stmt], ctx: _CompileCtx) -> Dict[str, Any]:
    """编译 while 循环体为子流程。体外层赋值名自动映射 $PARENT.NODE.<名>。"""
    parent_names = {name: "$PARENT." + ref[1:] for name, ref in ctx.names.items()}
    child_ctx = _CompileCtx(
        loop_vars={**parent_names, **_WHILE_BODY_VARS}, module_globals=ctx.module_globals)
    child_entry = _compile_block(list(body), child_ctx, succ=None)
    if child_entry is None:
        raise _CodeflowError(
            "while 循环体为空或全部悬空：请以 return 结束（返回值成为下一轮的 item）", body[0] if body else None)
    child_start_id = child_ctx.auto_id("start")
    return {
        "runtime": "python",
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": child_start_id, "next": child_entry},
            *child_ctx.nodes,
        ],
    }


def _compile_while(
    head: ast.While, ctx: _CompileCtx, succ: Optional[str], rest: List[ast.stmt],
) -> str:
    """``while <cond>:`` 条件循环 -> While 节点（引擎 plaita.node.loop.While）。

    语义与引擎对齐，循环体编译为子流程并**必须以 return 结束**——return 值
    即下一轮的 ``item``（首轮为 None），循环结束后也是本节点的输出。子流程
    状态与父隔离，体内 assignment 不会写回父 context，循环状态只能靠 return
    串（函数式）。条件与体内可用 ``item`` 与 ``rounds``（轮次，从 0 起）；
    首轮 item 为 None，bootstrap 惯用 ``while rounds == 0 or item.xxx:``。

    可依赖的引擎语义（2026-09-30 实测钉死）：条件组求值不短路但点路径打
    None 优雅返回 None、None 参与算子比较不炸（引擎吞 TypeError 记 False），
    故 ``item.n > 0`` 等写法在首轮是安全的。唯一退出通道是条件转假（引擎
    max_iterations=1000 兜底，业务上限应写进条件）；不支持 break/continue/
    while-else。``while True:`` 合法但只会跑满 max_iterations。

    作用域：体的子流程 INPUT 只有 item/index。外层已赋值变量由编译器自动
    映射为 ``$PARENT.NODE.<名>``（$PARENT 是子流程启动时的父 context 快照，
    循环期间父侧冻结）——体内直接裸用外层变量名即可。条件运行在父侧
    loop_ctx，外层变量按父侧引用解析。外层原始 INPUT 在体内须显式写
    ``PARENT.INPUT.<名>``（裸 INPUT 指子流程输入，静默 None 是陷阱）。
    """
    if head.orelse:
        raise _CodeflowError("while-else 不支持，请把 else 体移到 while 之后", head.orelse[0])

    cond = _while_cond(head.test, ctx)
    child_flow = _while_child_flow(head.body, ctx)

    cond_text = ast.unparse(head.test)
    node_id = ctx.semantic_id(_cond_slug(head.test))

    after = _compile_block(rest, ctx, succ)
    if after is None:
        raise _CodeflowError("while 节点之后悬空：请补 return 或后续语句", head)

    spec: Dict[str, Any] = {
        "type": "while",
        "id": node_id,
        "condition": cond,
        "name": _human_label(f"{cond_text}?"),
        "desc": _human_label(f"while {cond_text}（第 {head.lineno} 行）", 80),
        # While 模型只认 child_flow（无 childFlow camelCase 兼容键，
        # 2026-09-30 实测：写 childFlow 会被 schema 当未知键静默忽略）。
        "child_flow": child_flow,
        "next": after,
    }
    _annotate_source(spec, head)
    ctx.nodes.append(spec)
    return node_id


def _compile_while_for(
    head: ast.For, coll_call: ast.Call, ctx: _CompileCtx, succ: Optional[str], rest: List[ast.stmt],
) -> str:
    """``for x in WHILE(cond, id="w"):`` —— While 的 for-head 形态。

    与 ``while`` 语句同引擎节点，差别仅在节点可命名（id=），循环的最终态
    （最后一轮 return 值）下游经 ``NODE.<id>`` 引用。可选
    ``max_iterations=`` 覆盖引擎默认 1000。
    """
    kw = {k.arg: k.value for k in coll_call.keywords}
    pos = coll_call.args
    if not pos:
        raise _CodeflowError("WHILE 需要一个条件表达式", coll_call)

    cond = _while_cond(pos[0], ctx)
    child_flow = _while_child_flow(head.body, ctx)

    node_id = None
    id_kw = kw.get("id")
    if id_kw is not None and isinstance(id_kw, ast.Constant):
        node_id = str(id_kw.value)
    node_id = ctx.auto_id(node_id)

    spec: Dict[str, Any] = {
        "type": "while",
        "id": node_id,
        "condition": cond,
        "child_flow": child_flow,
    }
    mi = kw.get("max_iterations")
    if mi is not None and isinstance(mi, ast.Constant) and isinstance(mi.value, int):
        spec["max_iterations"] = mi.value

    after = _compile_block(rest, ctx, succ)
    if after is None:
        raise _CodeflowError("WHILE 节点之后悬空：请补 return 或后续语句", head)
    spec["next"] = after
    _annotate_source(spec, head)
    ctx.nodes.append(spec)
    return node_id


def _compile_for(
    head: ast.For, ctx: _CompileCtx, succ: Optional[str], rest: List[ast.stmt],
) -> str:
    """``for x in MAP/FILTER/FIND/LOOP(...)`` / ``for a,b in REDUCE(...)``。

    作用域：循环目标名（x / a,b）映射子流程输入；外层已赋值变量自动映射为
    ``$PARENT.NODE.<名>`` 快照引用（与 while 同一约定，见 ``_while_child_flow``），
    体内可读集合节点执行前已确定的父侧变量。子流程写不回父 context——
    聚合语义用 REDUCE（累积值经 return 串）或对 ``NODE.<集合节点id>`` 的
    下游表达式表达，不要试图在子流程 end 引用集合节点自身的结果
    （父侧结果此刻尚未写回，快照里没有）。
    """
    coll_call = head.iter
    if not isinstance(coll_call, ast.Call) or not isinstance(coll_call.func, ast.Name):
        raise _CodeflowError(
            "for 循环的迭代对象必须是 MAP/FILTER/FIND/LOOP/REDUCE/WHILE(...) 节点调用", coll_call)
    if coll_call.func.id == "WHILE":
        return _compile_while_for(head, coll_call, ctx, succ, rest)
    if coll_call.func.id not in _COLLECTION_CALL_NAMES:
        raise _CodeflowError(
            "for 循环的迭代对象必须是 MAP/FILTER/FIND/LOOP/REDUCE/WHILE(...) 节点调用", coll_call)
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

    # 外层已赋值名映射为 $PARENT 快照引用（与 while 的 _while_child_flow 同一
    # 约定）：集合节点执行前已在父侧赋值的变量，体内可读。$PARENT 是子流程
    # 启动时的父 context 快照，只读且循环期间冻结——所以只映射"此刻已在
    # ctx.names 里的名字"（顺序编译保证它们都在集合节点之前赋值，快照必有值）。
    # loop_vars 放在合并后侧：循环目标名遮蔽外层同名（与 Python 遮蔽规则一致）。
    parent_names = {name: "$PARENT." + ref[1:] for name, ref in ctx.names.items()}
    child_ctx = _CompileCtx(
        loop_vars={**parent_names, **loop_vars}, module_globals=ctx.module_globals)
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


def _references_assign_name(expr: ast.expr, name: str) -> bool:
    """赋值右侧是否引用了正在赋值的变量名（自引用检测，C1-1）。

    codeflow 禁止同名重复赋值（每个赋值是一个节点），故 RHS 里出现的裸
    ``name`` 只能解析成本赋值节点自己（``$NODE.<name>``）——编译期放行的话
    运行期必然 ``NoneType`` 参与运算炸掉。覆盖：

    - 裸名 ``x``（含三元 ``x = x if ... else ...``、嵌套调用/字面量内）；
    - 显式 ``NODE.x``（赋值节点 id 即变量名，同为自引用）。

    保留命名空间（INPUT/NODE/GLOBAL/...，见 ``_NS_PREFIX``）不作裸名检测——
    它们在 ``_resolve_name`` 里先于 ``ctx.names`` 解析，赋值同名变量不会让
    RHS 自指（如 ``NODE = NODE.x`` 写 ``$NODE.NODE``，读的是 x 节点，非环形）。
    """
    for node in ast.walk(expr):
        if isinstance(node, ast.Name) and node.id == name and name not in _NS_PREFIX:
            return True
        if (
            isinstance(node, ast.Attribute)
            and node.attr == name
            and isinstance(node.value, ast.Name)
            and node.value.id == "NODE"
        ):
            return True
    return False


def _compile_assign(
    head: ast.Assign, ctx: _CompileCtx, succ: Optional[str], rest: List[ast.stmt],
) -> str:
    if len(head.targets) != 1 or not isinstance(head.targets[0], ast.Name):
        raise _CodeflowError("赋值目标必须是单个变量名", head)
    name = head.targets[0].id
    value = head.value

    # C1-1：自引用在编译期显式拦截。检测先于名字登记（登记本身不受影响，
    # 后续语句仍能解析该名字）；三元/嵌套形态经 ast.walk 全量覆盖。
    if _references_assign_name(value, name):
        raise _CodeflowError(
            f"变量 {name!r} 的赋值右侧引用了它自己：自引用赋值请引入中间变量，"
            "或用 REDUCE 做聚合", head)

    # 先登记名字映射（不预 claim，避免与 _compile_node_call 的 auto_id 冲突），
    # 这样后续语句引用 name 时能解析成 $NODE.<name>
    ctx.names[name] = f"$NODE.{name}"
    anchor = len(ctx.nodes)
    after = _compile_block(rest, ctx, succ)
    if after is None:
        raise _CodeflowError(f"赋值 {name} 之后悬空：请补 return 或后续语句", head)

    # C1-1：同名重复赋值此前落到 ctx.claim 的「节点 id 重复」文案，对赋值
    # 场景有误导（变量名即节点 id，重复的其实是赋值本身）。注意 rest 先于
    # 本赋值编译：重复赋值时后一条的节点已先 claim 了该名字，在这里统一按
    # 赋值语义报错（节点调用路径 x = HTTP(...) 同样被此检查覆盖）。
    if name in ctx._claimed:
        raise _CodeflowError(
            f"变量 {name!r} 被多次赋值：codeflow 每个赋值是一个节点，变量名即"
            "节点 id，同一 id 只能出现一次。请换用新变量名，或把赋值收拢到"
            "单一节点/分支里", head)

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
    expr_text = ast.unparse(value)
    expr_node: Dict[str, Any] = {
        "type": "assignment", "id": nid, "output": _compile_expr(value, ctx), "next": after,
        "name": _human_label(expr_text),
        "desc": _human_label(f"{expr_text}（第 {head.lineno} 行）", 60),
    }
    _annotate_source(expr_node, head)
    ctx.nodes.insert(anchor, expr_node)
    return nid

