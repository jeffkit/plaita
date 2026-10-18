"""Statement compilation for @flow AST (if/for/assign/return blocks)."""
from __future__ import annotations

import ast
from typing import Any, Dict, List, Optional, cast

from plaita.dsl.codeflow._common import (
    _COLLECTION_CALL_NAMES,
    _END_RESULT_TYPES,
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
from plaita.dsl.codeflow._nodes import _apply_base_kwargs, _compile_node_call

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

    if isinstance(head, ast.Expr) and _is_end_call(head):
        return _compile_end_call(head, ctx, rest)

    if isinstance(head, ast.Assign):
        return _compile_assign(head, ctx, succ, rest)

    if isinstance(head, ast.Expr):
        return _compile_expr_stmt(head, ctx, succ, rest)

    if isinstance(head, ast.Pass):
        return _compile_block(rest, ctx, succ)

    raise _CodeflowError(f"不支持的语句 {type(head).__name__}", head)


# `end(...)` 终止语句的参数表（#76：return 只能表达 success 出口）。
# #238：end 也是 Node，基类 timeout / errorHandler / timeoutHandler 与其它
# 节点同口径可配——此前这三个字段在两条链上都没有表达面，往返静默丢。
_END_CALL_NAME = "end"
_END_CALL_KWARGS = (
    "output", "result_type", "error", "timeout", "on_error", "on_timeout",
)


def _is_end_call(stmt: ast.stmt) -> bool:
    """``end(...)`` 语句形态判定（与 ``EVENT(...)`` 等节点调用同一语句位）。"""
    value = stmt.value if isinstance(stmt, ast.Expr) else None
    return (
        isinstance(value, ast.Call)
        and isinstance(value.func, ast.Name)
        and value.func.id == _END_CALL_NAME
    )


def _compile_end_call(head: ast.Expr, ctx: _CompileCtx, rest: List[ast.stmt]) -> str:
    """编译 ``end(output=..., result_type=..., error=...)`` 为 end 节点。

    与 ``return`` 同为块终止语句（之后语句不可达），区别是能显式表达
    ``resultType``（``error``/``nop``）与 ``error`` 载荷——``return`` 恒定产出
    ``success``，失败出口会在 round-trip 中被静默洗成成功（#76）。
    """
    call = cast(ast.Call, head.value)
    kwargs: Dict[str, ast.expr] = {}
    for kw in call.keywords:
        if kw.arg is None:
            raise _CodeflowError("end(...) 不支持 ** 解包", kw.value)
        if kw.arg not in _END_CALL_KWARGS:
            raise _CodeflowError(
                f"end(...) 不支持的参数 {kw.arg!r}"
                f"（可用：{'/'.join(_END_CALL_KWARGS)}）", kw.value)
        if kw.arg in kwargs:
            raise _CodeflowError(f"end(...) 重复的参数 {kw.arg!r}", kw.value)
        kwargs[kw.arg] = kw.value

    if len(call.args) > 1:
        raise _CodeflowError("end(...) 最多一个位置参数（output）", head)
    output_node: Optional[ast.expr] = kwargs.get("output")
    if output_node is not None and call.args:
        raise _CodeflowError("end(...) 的 output 不能同时给位置参数与关键字参数", head)
    if output_node is None and call.args:
        output_node = call.args[0]
    output = _compile_expr(output_node, ctx) if output_node is not None else None

    rt_node = kwargs.get("result_type")
    result_type: Any = "success"
    if rt_node is not None:
        if not (isinstance(rt_node, ast.Constant) and isinstance(rt_node.value, str)):
            raise _CodeflowError("end(...) 的 result_type 必须是字符串字面量", rt_node)
        result_type = rt_node.value
        # 取值域硬校验：发动机 End.setup_end 只认小写 success/error/nop，其它值
        # （含大小写变体）会被**静默降级成 success**——正是 #76 要消灭的静默降级，
        # 不能让手写/LLM 源码从新入口重新引入。
        if result_type not in _END_RESULT_TYPES:
            raise _CodeflowError(
                "end(...) 的 result_type 取值必须是 success/error/nop 之一"
                f"（发动机口径，大小写敏感），得到 {result_type!r}", rt_node)

    error_node = kwargs.get("error")
    error: Optional[Dict[str, Any]] = None
    if error_node is not None:
        # #76：error 是纯数据（引擎 `End.execute` 直接 self.error.get(...)，
        # 从不求值）——按字面数据解析，不走表达式编译。含字面 `$` 的字符串
        # （如 {"message": "$100 fee missing"}）与键名（如 {"$ref": "x"}）原样保留。
        try:
            literal = ast.literal_eval(error_node)
        except (ValueError, SyntaxError) as exc:
            raise _CodeflowError(
                "end(...) 的 error 必须是字面数据（dict/str/num/bool/None，"
                "不参与表达式编译）", error_node) from exc
        if not isinstance(literal, dict):
            raise _CodeflowError("end(...) 的 error 必须是 dict 字面量", error_node)
        error = literal

    # 与 return 同一套语义化 id/标签约定：ret_<返回表达式 slug>。
    ret_slug = _cond_slug(output_node)
    end_id = ctx.semantic_id(f"ret_{ret_slug}" if ret_slug else None)
    expr_text = ast.unparse(output_node) if output_node is not None else ""
    end_node: Dict[str, Any] = {
        "type": "end", "id": end_id, "output": output, "resultType": result_type,
    }
    if error is not None:
        end_node["error"] = error
    # #238：基类共有字段与所有内置节点同一套消费逻辑（on_timeout 只在
    # `_apply_base_kwargs` 里处理，别在这里重复实现）。
    _apply_base_kwargs(end_node, kwargs, ctx)
    if expr_text:
        end_node["name"] = _human_label(f"end {expr_text}")
    end_node["desc"] = _human_label(f"end {expr_text}（第 {head.lineno} 行）", 60)
    _annotate_source(end_node, head)
    ctx.nodes.append(end_node)
    if rest:
        raise _CodeflowError("end(...) 之后还有不可达语句", rest[0])
    return end_id


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
    # #77：子作用域必须继承 childflows 注册表，否则体内 CHILD/PARALLEL/REFERENCE
    # 解析 @childflow 名字时必然落空（源码字符串模式 module_globals 为空）。
    cond_ctx = _CompileCtx(
        module_globals=ctx.module_globals, childflows=ctx.childflows)
    cond_ctx.names = {**ctx.names, **_WHILE_COND_VARS}
    return _compile_condition(test, cond_ctx)


def _while_child_flow(body: List[ast.stmt], ctx: _CompileCtx) -> Dict[str, Any]:
    """编译 while 循环体为子流程。体外层赋值名自动映射 $PARENT.NODE.<名>。"""
    parent_names = {name: "$PARENT." + ref[1:] for name, ref in ctx.names.items()}
    # #77：同 _while_cond——子作用域漏传 childflows 会让体内 CHILD(...) 误报
    # 「不是 @childflow 装饰的子流程」，而同一句移出循环体即可编译。
    child_ctx = _CompileCtx(
        loop_vars={**parent_names, **_WHILE_BODY_VARS}, module_globals=ctx.module_globals,
        childflows=ctx.childflows)
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
    mi = kw.get("max_iterations") or kw.get("maxIterations")
    if mi is not None and isinstance(mi, ast.Constant) and isinstance(mi.value, int):
        spec["max_iterations"] = mi.value
    # #238：While 的基类字段与集合节点同一套消费逻辑；此前这条 for-head
    # 路径漏调 `_apply_base_kwargs` ⇒ 发射器写出也会被编译期静默丢弃。
    _apply_base_kwargs(spec, kw, ctx)

    # plaita#220/L1（同族）：`for x in WHILE(cond, id='w')` 的 id 同样没有赋值
    # 语句可借，下游 `output=w` 需要名字作用域里有它。
    ctx.names[node_id] = f"$NODE.{node_id}"

    after = _compile_block(rest, ctx, succ)
    if after is None:
        raise _CodeflowError("WHILE 节点之后悬空：请补 return 或后续语句", head)
    spec["next"] = after
    _annotate_source(spec, head)
    ctx.nodes.append(spec)
    return node_id


# LOOP 条件名 → 引擎 loop_ctx 注入键（与 `_emit._LOOP_COND_NS` 对称）。
# 条件在引擎侧与 WHILE 同一求值上下文：父 context + 注入的 `$LOOP-*`；
# 循环体才是子流程（item/index 走 `$INPUT.item`/`$INPUT.index`）。
_LOOP_COND_VARS: Dict[str, str] = {
    "item": "$LOOP-ITEM",
    "index": "$LOOP-INDEX",
    "result": "$LOOP-RESULT",
}


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
    # #77：集合体子作用域同样必须继承 childflows 注册表——漏传会让 MAP/FILTER/
    # FIND/LOOP/REDUCE 体内的 CHILD/PARALLEL/REFERENCE 误报「不是 @childflow
    # 装饰的子流程」，而同一句移出循环体即可编译。
    child_ctx = _CompileCtx(
        loop_vars={**parent_names, **loop_vars}, module_globals=ctx.module_globals,
        childflows=ctx.childflows)
    child_entry = _compile_block(list(head.body), child_ctx, succ=None)  # 子流程体必须自行 return
    if child_entry is None:
        raise _CodeflowError("循环体为空或全部悬空：请补 return", head.body[0] if head.body else head)

    # 节点 id：优先 id= 关键字，否则自动
    node_id = None
    id_kw = kw.get("id")
    if id_kw is not None and isinstance(id_kw, ast.Constant):
        node_id = str(id_kw.value)
    node_id = ctx.auto_id(node_id)

    # plaita#220/L1：集合节点的 id 必须进名字作用域。发射器为下游
    # `$NODE.<id>` 生成的是**裸变量名**（`_invert_value` 的 `$NODE.` → `` 规则），
    # 而集合节点在源码里是 `for item in MAP(..., id='m')`——没有 `m = MAP(...)`
    # 这种赋值可依赖。不登记 ⇒ 重编译 `output=m` 报「未知名字 'm'」（5/5 集合
    # 类型都踩）。与 `_compile_assign` 登记 `$NODE.<name>` 同形。
    # 位置在子流程编译**之后**：体内读不到集合节点自己的结果（父侧尚未写回，
    # 与 docstring 的声明一致），故不能让 id 漏进体的 parent 快照。
    ctx.names[node_id] = f"$NODE.{node_id}"

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
    # #238：`itemType`（循环元素类型声明）两条链都没有表达面 ⇒ 往返静默丢。
    # 存量 IR 用 camel，模型字段是 snake；发射器 `_emit_collection` 双读。
    item_type = kw.get("item_type") or kw.get("itemType")
    if item_type is not None:
        spec["itemType"] = _compile_expr(item_type, ctx)
    if kind == "LOOP" and kw.get("condition") is not None:
        # #227：@flow 链此前不读 `condition=` ⇒ 发射器即使写出也被编译期丢弃。
        # 条件上下文与 while 条件同形（父名字 + `$LOOP-*` 注入名，item/index 遮蔽
        # 外层同名），childflows 注册表沿用 #77 的继承约定。
        cond_ctx = _CompileCtx(
            module_globals=ctx.module_globals, childflows=ctx.childflows)
        cond_ctx.names = {**ctx.names, **_LOOP_COND_VARS}
        spec["condition"] = _compile_condition(kw["condition"], cond_ctx)

    # #142 编译侧 / #143：集合节点（MAP/FILTER/FIND/LOOP/REDUCE/WHILE）同样要
    # 消费 `Node` 基类共有字段（timeout / timeoutHandler / on_error）。这条路径与
    # `_nodes.py` 的内置 kind 路径是**两套 spec 构造**，须在各自收口处各施加一次，
    # 否则集合节点的同类 kwarg 仍被静默丢弃。
    _apply_base_kwargs(spec, kw, ctx)

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

