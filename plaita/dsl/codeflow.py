"""
plaita.dsl.codeflow — 用纯 Python 函数写 flow（AST 编译到 Flow IR）。

这是「极致」形态：用户写一个普通 Python 函数，``@flow`` 装饰器用 ``ast``
解析函数体，把 ``if`` / ``for`` / ``return`` / 赋值 / 表达式翻译成 flow 节点，
把 ``INPUT.age >= 18`` 这类真 Python 表达式编译成 ``$INPUT.age`` 表达式串。
函数体**从不被当作 Python 执行**——它只是一段被静态分析的语法树。

因此：

- 写起来就是纯 Python，有补全、有类型、写错编译期就报；
- 编译产物仍是 ``Flow`` IR，跑在现有运行时上，断点续执/并行/错误处理/
  可视化/可审计全部保留；
- ``INPUT`` / ``F`` / ``HTTP`` 等名字**不需要 import**——它们在函数体里
  只是语法占位，AST 编译期被识别，运行期不会真正查这几个名字。

速览::

    from plaita.dsl.codeflow import flow, HTTP, ErrorHandler

    @flow("create_user", desc="创建用户")
    def create_user(INPUT):
        if INPUT.age >= 18:
            resp = HTTP.post(
                url="https://api.example.com/users",
                body={"name": INPUT.name},
                timeout="PT5S",
                on_error=ErrorHandler("continue_with", default={"data": None}),
            )
            return resp.data
        return "未成年"

    create_user.run(name="alice", age=20)  # $INPUT = {"name": "alice", "age": 20}

支持的语句：``if/elif/else``、``return``、赋值、表达式语句、
``for x in MAP/FILTER/FIND/LOOP(...)``、``for a,b in REDUCE(...)``。
支持的表达式：``INPUT.x`` / ``F.func(...)`` / ``NODE.x`` / ``resp.attr``
/ 字面量 / ``+ - * / %`` / 比较与 ``and/or/not`` / 下标 / dict/list。

工程量是一次性的：之后加节点类型只需加一个 ``Call`` 分支。
"""

from __future__ import annotations

import ast
import inspect
import textwrap
import warnings
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from plaita.core.flow import Flow

__all__ = [
    "flow",
    "childflow",
    "flow_from_source",
    "compile_source",
    "compile_func",
    "HTTP",
    "CODE",
    "EVENT",
    "MAP",
    "FILTER",
    "FIND",
    "LOOP",
    "REDUCE",
    "CHILD",
    "REFERENCE",
    "PARALLEL",
    "ErrorHandler",
    "F",
    "INPUT",
    "NODE",
    "GLOBAL",
    "PARENT",
    "ENV",
]


# ---------------------------------------------------------------------------
# 占位对象：仅用于 IDE 友好与可选 import，函数体从不真正执行
# ---------------------------------------------------------------------------

class _Placeholder:
    def __init__(self, name: str) -> None:
        self._name = name

    def __getattr__(self, item: str) -> Any:
        return _Placeholder(f"{self._name}.{item}")

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self

    def __repr__(self) -> str:
        return f"<codeflow placeholder {self._name}>"


INPUT = _Placeholder("INPUT")
NODE = _Placeholder("NODE")
GLOBAL = _Placeholder("GLOBAL")
PARENT = _Placeholder("PARENT")
ENV = _Placeholder("ENV")
F = _Placeholder("F")
HTTP = _Placeholder("HTTP")
CODE = _Placeholder("CODE")
EVENT = _Placeholder("EVENT")
MAP = _Placeholder("MAP")
FILTER = _Placeholder("FILTER")
FIND = _Placeholder("FIND")
LOOP = _Placeholder("LOOP")
REDUCE = _Placeholder("REDUCE")
CHILD = _Placeholder("CHILD")
REFERENCE = _Placeholder("REFERENCE")
PARALLEL = _Placeholder("PARALLEL")


class ErrorHandler:
    """``errorHandler`` 的字面构造器，``HTTP(..., on_error=ErrorHandler(...))``。"""

    def __init__(
        self,
        strategy: str = "abort",
        retry_times: Optional[int] = None,
        default_value: Any = None,
        error_code: Optional[int] = None,
        error_message: Optional[str] = None,
    ) -> None:
        if strategy not in ("abort", "continue", "continue_with"):
            raise ValueError(f"unknown error handler strategy: {strategy!r}")
        self.spec: Dict[str, Any] = {"strategy": strategy}
        if retry_times is not None:
            self.spec["retryTimes"] = retry_times
        if default_value is not None:
            self.spec["defaultValue"] = default_value
        if error_code is not None:
            self.spec["errorCode"] = error_code
        if error_message is not None:
            self.spec["errorMessage"] = error_message


# ---------------------------------------------------------------------------
# 编译期上下文
# ---------------------------------------------------------------------------

_NS_PREFIX = {
    "INPUT": "$INPUT",
    "NODE": "$NODE",
    "GLOBAL": "$GLOBAL",
    "PARENT": "$PARENT",
    "ENV": "$ENV",
    "F": "$F",
}

# Python 内置 → $F 函数名
_BUILTIN_TO_F = {
    "len": "len", "abs": "abs", "round": "round",
    "str": "concat",  # 粗略映射；精确类型转换不在表达式语言范围
}

_BINOP_TO_F = {
    ast.Add: "add", ast.Sub: "sub", ast.Mult: "mul",
    ast.Div: "div", ast.Mod: "mod", ast.Pow: "pow",
}

_COMPARE_OP = {
    ast.Gt: "gt", ast.GtE: "gte", ast.Lt: "lt", ast.LtE: "lte",
    ast.Eq: "eq", ast.NotEq: "ne", ast.In: "in", ast.NotIn: "notIn",
}

_NEGATE_OP = {
    "eq": "ne", "ne": "eq", "gt": "lte", "gte": "lt",
    "lt": "gte", "lte": "gt", "in": "notIn", "notIn": "in",
}

_NODE_CALL_NAMES = {"HTTP", "CODE", "EVENT", "CHILD", "REFERENCE", "PARALLEL"}
_COLLECTION_CALL_NAMES = {"MAP", "FILTER", "FIND", "LOOP", "REDUCE"}

# 常见 Python 写法在 @flow 表达式里不支持——给出可读的重写提示, 而不是抛
# 不可读的 ast.dump / 裸类型名。这些构造当前本就会编译失败, 加提示是纯 DX 提升。
_FOOTGUN_HINTS = {
    ast.IfExp: "三元表达式 `a if c else b` 不支持, 请用 if/else 语句分支实现",
    ast.JoinedStr: "f-string 不支持, 请用 F.concat(...) 拼接, 例: F.concat('hi ', INPUT.name)",
    ast.FormattedValue: "f-string 不支持, 请用 F.concat(...) 拼接",
    ast.Lambda: "lambda 不支持, @flow 函数体本身即流程, 请拆成节点",
    ast.Await: "await 不支持, @flow 函数体不被当作 Python 执行",
    ast.Starred: "*args / **kwargs 解包不支持",
    ast.Set: "集合字面量不支持, 请用列表",
    ast.ListComp: "列表推导式不支持, 请用 MAP/FILTER 节点",
    ast.SetComp: "集合推导式不支持, 请用 MAP/FILTER 节点",
    ast.DictComp: "字典推导式不支持, 请用 MAP 节点构造",
    ast.GeneratorExp: "生成器表达式不支持, 请用 MAP/FILTER 节点",
    ast.NamedExpr: "海象运算符 := 不支持, 请用普通赋值语句",
}


def _describe_call(func: ast.expr) -> str:
    """把 Call 的 func 渲染成可读字符串, 用于报错。"""
    if isinstance(func, ast.Name):
        return f"{func.id}(...)"
    if isinstance(func, ast.Attribute):
        base = _describe_call(func.value) if isinstance(func.value, ast.Call) else (
            func.value.id if isinstance(func.value, ast.Name) else type(func.value).__name__
        )
        return f"{base}.{func.attr}(...)"
    return type(func).__name__


def _node_call_kind(func: ast.expr) -> Optional[str]:
    """识别节点调用种类：``HTTP(...)`` / ``HTTP.post(...)`` / ``CODE.python(...)``。

    同时支持 ``Name``（``HTTP(...)``）与 ``Attribute``（``HTTP.post(...)``）两种
    写法，返回基名（``HTTP`` / ``CODE`` / ...）或 ``None``。
    """
    if isinstance(func, ast.Name):
        return func.id if func.id in _NODE_CALL_NAMES else None
    if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
        return func.value.id if func.value.id in _NODE_CALL_NAMES else None
    return None


class _CompileCtx:
    """单条 flow / childflow 的编译上下文：id 分配 + 节点收集 + 名字解析。"""

    def __init__(
        self,
        loop_vars: Optional[Dict[str, str]] = None,
        module_globals: Optional[Dict[str, Any]] = None,
        childflows: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> None:
        self._counter = 0
        self._claimed: set = set()
        self.nodes: List[Dict[str, Any]] = []
        # 名字 → 表达式串。loop_vars: {"x": "$INPUT.item", "i": "$INPUT.index"}
        self.names: Dict[str, str] = dict(loop_vars or {})
        # 被赋值为节点结果的变量名 → 自动 $NODE.<name>
        # （names 里存的就是 "$NODE.<name>"，赋值时登记）
        self.module_globals: Dict[str, Any] = module_globals or {}
        # 源码模式：name → 子流程 IR，供 CHILD/REFERENCE/PARALLEL 的 flow=<name> 解析。
        # 装饰器模式则走 module_globals 里的 _ChildFlowMarker。
        self.childflows: Dict[str, Dict[str, Any]] = childflows or {}

    def auto_id(self, hint: Optional[str] = None) -> str:
        if hint:
            if hint not in self._claimed:
                self._claimed.add(hint)
                return hint
        self._counter += 1
        cand = f"_n{self._counter}"
        while cand in self._claimed:
            self._counter += 1
            cand = f"_n{self._counter}"
        self._claimed.add(cand)
        return cand

    def claim(self, nid: str) -> str:
        if nid in self._claimed:
            raise ValueError(f"节点 id 重复: {nid!r}")
        self._claimed.add(nid)
        return nid


class _CodeflowError(Exception):
    """编译期错误，附带源码行号便于定位。"""

    def __init__(self, msg: str, node: Optional[ast.AST] = None) -> None:
        line = getattr(node, "lineno", "?") if node else "?"
        super().__init__(f"[codeflow] 第 {line} 行: {msg}")


# ---------------------------------------------------------------------------
# 表达式编译：ast.expr -> Python 值（字面量）或 "$..." 表达式串
# ---------------------------------------------------------------------------

def _compile_expr(node: ast.AST, ctx: _CompileCtx) -> Any:
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name):
        return _resolve_name(node.id, node, ctx)
    if isinstance(node, ast.Attribute):
        base = _compile_expr(node.value, ctx)
        if not isinstance(base, str):
            raise _CodeflowError(f"属性访问的基底必须是命名空间/节点引用，得到 {base!r}", node)
        return f"{base}.{node.attr}"
    if isinstance(node, ast.Call):
        return _compile_call_expr(node, ctx)
    if isinstance(node, ast.BinOp):
        fname = _BINOP_TO_F.get(type(node.op))
        if fname is None:
            raise _CodeflowError(f"不支持的二元运算 {type(node.op).__name__}", node)
        left = _compile_expr(node.left, ctx)
        right = _compile_expr(node.right, ctx)
        return f"$F.{fname}({_render_arg(left)}, {_render_arg(right)})"
    if isinstance(node, ast.UnaryOp):
        if isinstance(node.op, ast.USub):
            operand = _compile_expr(node.operand, ctx)
            return f"$F.sub(0, {_render_arg(operand)})"
        if isinstance(node.op, ast.Not):
            raise _CodeflowError("not 要用在条件位置（if/while 判断），不能出现在表达式里", node)
        raise _CodeflowError(f"不支持的一元运算 {type(node.op).__name__}", node)
    if isinstance(node, ast.Subscript):
        base = _compile_expr(node.value, ctx)
        idx = _eval_subscript_index(node.slice, ctx)
        if not isinstance(base, str):
            raise _CodeflowError("下标访问的基底必须是命名空间/节点引用", node)
        return f"{base}[{idx}]"
    if isinstance(node, ast.Dict):
        d: Dict[Any, Any] = {}
        for k, v in zip(node.keys, node.values):
            if k is None:
                raise _CodeflowError("不支持 ** 解包", node)
            if not isinstance(k, ast.Constant):
                raise _CodeflowError("dict 的 key 必须是常量", node)
            d[k.value] = _compile_expr(v, ctx)
        return d
    if isinstance(node, (ast.List, ast.Tuple)):
        return [_compile_expr(e, ctx) for e in node.elts]
    if isinstance(node, ast.BoolOp) or isinstance(node, ast.Compare):
        raise _CodeflowError("比较/and/or 只能出现在条件位置（if 判断）", node)
    # 常见 Python 写法在 @flow 里不支持——给重写提示, 而不是抛 ast.dump
    _footgun_hint = _FOOTGUN_HINTS.get(type(node))
    if _footgun_hint:
        raise _CodeflowError(_footgun_hint, node)
    raise _CodeflowError(
        f"不支持的表达式 {type(node).__name__}（@flow 只支持字面量/变量/属性/下标/"
        f"函数调用/算术/字典与列表字面量, 详见 code-dsl 文档「表达式语义边界」）",
        node,
    )


def _resolve_name(name: str, node: ast.Name, ctx: _CompileCtx) -> str:
    if name in _NS_PREFIX:
        return _NS_PREFIX[name]
    if name in ctx.names:
        return ctx.names[name]
    if name in _BUILTIN_TO_F:
        # 单独出现的 len(x) 走 Call 分支；裸 len 名字无意义
        raise _CodeflowError(f"内置函数 {name} 必须以调用形式使用", node)
    raise _CodeflowError(
        f"未知名字 {name!r}：可用 INPUT/NODE/GLOBAL/PARENT/ENV/F 或已赋值的节点变量", node)


def _eval_subscript_index(sl: ast.AST, ctx: _CompileCtx) -> str:
    # 仅支持常量下标（数字）；负数也支持（引擎认 [-?\d+]）
    if isinstance(sl, ast.Index):  # py<3.9 兼容
        sl = sl.value
    if isinstance(sl, ast.Constant) and isinstance(sl.value, int):
        return str(sl.value)
    raise _CodeflowError("下标只支持整数常量", sl)


def _render_arg(v: Any) -> str:
    """把一个编译后的参数渲染进 ``$F.func(...)`` 的参数串。"""
    if isinstance(v, str):
        if v.startswith("$"):
            return v
        return '"' + v.replace("\\", "\\\\").replace('"', '\\"') + '"'
    if isinstance(v, bool):
        return "true" if v else "false"
    if v is None:
        return "null"
    if isinstance(v, (int, float)):
        return str(v)
    raise _CodeflowError(f"无法作为 $F 参数渲染的值: {v!r}")


def _compile_call_expr(node: ast.Call, ctx: _CompileCtx) -> Any:
    func = node.func
    # F.func(...) -> "$F.func(args)"
    if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name) \
            and func.value.id == "F":
        args = [_compile_expr(a, ctx) for a in node.args]
        rendered = ", ".join(_render_arg(a) for a in args)
        return f"$F.{func.attr}({rendered})"
    # 节点调用出现在表达式位置：不允许（节点调用只能作为语句或赋值右侧）
    node_kind = _node_call_kind(func)
    if node_kind is not None or (
        isinstance(func, ast.Name) and func.id in _COLLECTION_CALL_NAMES
    ):
        bad = node_kind or (func.id if isinstance(func, ast.Name) else "节点")
        raise _CodeflowError(
            f"{bad}(...) 是节点调用，只能作为语句或赋值右侧，不能嵌在表达式里", node)
    # 内置 len/abs/round -> $F.len(...)
    if isinstance(func, ast.Name) and func.id in _BUILTIN_TO_F:
        args = [_compile_expr(a, ctx) for a in node.args]
        rendered = ", ".join(_render_arg(a) for a in args)
        return f"$F.{_BUILTIN_TO_F[func.id]}({rendered})"
    # 其它调用都不支持——给可读的调用名而不是 ast.dump
    call_name = _describe_call(func)
    raise _CodeflowError(
        f"不支持的调用 {call_name}：@flow 表达式里只能调 F.xxx(...) 或内置 len/abs/round/str",
        node,
    )


# ---------------------------------------------------------------------------
# 条件编译：ast.expr -> Condition / ConditionGroup dict
# ---------------------------------------------------------------------------

def _compile_condition(node: ast.AST, ctx: _CompileCtx) -> Dict[str, Any]:
    if isinstance(node, ast.BoolOp):
        relation = "and" if isinstance(node.op, ast.And) else "or"
        return {"relation": relation,
                "conditions": [_compile_condition(v, ctx) for v in node.values]}
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
        return _negate_condition(_compile_condition(node.operand, ctx), node)
    if isinstance(node, ast.Compare):
        return _compile_compare(node, ctx)
    # 裸表达式真值测试 -> (expr != False)
    expr = _compile_expr(node, ctx)
    return {"field": expr, "operator": "ne", "value": False}


def _compile_compare(node: ast.Compare, ctx: _CompileCtx) -> Dict[str, Any]:
    left = _compile_expr(node.left, ctx)
    # 链式比较 a < b < c -> and(a<b, b<c)
    conds: List[Dict[str, Any]] = []
    cur_left = left
    for op, comp in zip(node.ops, node.comparators):
        op_name = _COMPARE_OP.get(type(op))
        if op_name is None:
            raise _CodeflowError(f"不支持的比较运算 {type(op).__name__}", node)
        right = _compile_expr(comp, ctx)
        conds.append({"field": cur_left, "operator": op_name, "value": right})
        cur_left = right
    if len(conds) == 1:
        return conds[0]
    return {"relation": "and", "conditions": conds}


def _negate_condition(cond: Dict[str, Any], node: ast.AST) -> Dict[str, Any]:
    if "operator" in cond:
        op = cond["operator"]
        if op not in _NEGATE_OP:
            raise _CodeflowError(f"not 无法翻转运算符 {op!r}", node)
        return {"field": cond["field"], "operator": _NEGATE_OP[op], "value": cond["value"]}
    rel = cond.get("relation")
    if rel == "and":
        return {"relation": "or", "conditions": [_negate_condition(c, node) for c in cond["conditions"]]}
    if rel == "or":
        return {"relation": "and", "conditions": [_negate_condition(c, node) for c in cond["conditions"]]}
    raise _CodeflowError("not 的操作数不合法", node)


# ---------------------------------------------------------------------------
# 语句编译：结构化代码 -> 节点图（带 successor 串接）
# ---------------------------------------------------------------------------

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
        ctx.nodes.append({
            "type": "end", "id": end_id,
            "output": output, "resultType": "success",
        })
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
    _compile_block(list(head.body), child_ctx, succ=None)  # 子流程体必须自行 return

    # 节点 id：优先 id= 关键字，否则自动
    node_id = None
    id_kw = kw.get("id")
    if id_kw is not None and isinstance(id_kw, ast.Constant):
        node_id = str(id_kw.value)
    node_id = ctx.auto_id(node_id)

    spec: Dict[str, Any] = {
        "type": kind.lower(),
        "id": node_id,
        "collection": collection,
        "childFlow": {
            "runtime": "python",
            "inputType": child_input_type,
            "nodes": child_ctx.nodes,
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

    if isinstance(value, ast.Call) and _node_call_kind(value.func) is not None:
        spec = _compile_node_call(value, ctx, name)
        spec["next"] = after
        ctx.nodes.insert(anchor, spec)
        return name

    output = _compile_expr(value, ctx)
    ctx.claim(name)
    ctx.nodes.insert(anchor, {
        "type": "assignment", "id": name, "output": output, "next": after,
    })
    return name


def _compile_expr_stmt(
    head: ast.Expr, ctx: _CompileCtx, succ: Optional[str], rest: List[ast.stmt],
) -> Optional[str]:
    value = head.value
    anchor = len(ctx.nodes)
    after = _compile_block(rest, ctx, succ)
    if after is None:
        raise _CodeflowError("表达式语句之后悬空：请补 return 或后续语句", head)
    if isinstance(value, ast.Call) and _node_call_kind(value.func) is not None:
        spec = _compile_node_call(value, ctx, None)
        spec["next"] = after
        ctx.nodes.insert(anchor, spec)
        return spec["id"]
    nid = ctx.auto_id()
    ctx.nodes.insert(anchor, {
        "type": "assignment", "id": nid, "output": _compile_expr(value, ctx), "next": after,
    })
    return nid


def _compile_node_call(
    node: ast.Call, ctx: _CompileCtx, assign_name: Optional[str],
) -> Dict[str, Any]:
    kind = _node_call_kind(node.func)
    if kind is None:
        raise _CodeflowError("不支持的节点调用", node)
    kw = {k.arg: k.value for k in node.keywords}
    pos = node.args

    def _const(k: Optional[ast.expr]) -> Any:
        if k is None:
            return None
        return _compile_expr(k, ctx)

    nid = ctx.claim(assign_name) if assign_name else ctx.auto_id()

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

    raise _CodeflowError(f"未知节点调用 {kind}", node)


def _eval_error_handler(node: ast.AST, ctx: _CompileCtx) -> Dict[str, Any]:
    # ErrorHandler(...) 直接构造 —— 但函数体不执行，所以不能调；这里识别 AST 调用
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) \
            and node.func.id == "ErrorHandler":
        kw = {k.arg: k.value for k in node.keywords}
        pos = node.args
        strategy = pos[0].value if pos and isinstance(pos[0], ast.Constant) else "abort"
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


def _unpack_names(target: ast.AST) -> List[str]:
    if isinstance(target, ast.Name):
        return [target.id]
    if isinstance(target, (ast.Tuple, ast.List)):
        names = []
        for e in target.elts:
            if not isinstance(e, ast.Name):
                raise _CodeflowError("循环变量解包必须是纯名字", target)
            names.append(e.id)
        return names
    raise _CodeflowError("不支持的循环变量形式", target)


def _const_bool(node: ast.AST) -> bool:
    return isinstance(node, ast.Constant) and node.value is True


# ---------------------------------------------------------------------------
# 顶层：@flow / @childflow 装饰器
# ---------------------------------------------------------------------------

def _func_ast(func: Callable) -> ast.FunctionDef:
    src = inspect.getsource(func)
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


def _compile_fdef(
    fdef: ast.FunctionDef,
    flow_id: Optional[str],
    opts: Dict[str, Any],
    module_globals: Optional[Dict[str, Any]] = None,
    childflows: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """编译一个 ``FunctionDef`` AST 到 Flow IR dict（装饰器模式与源码模式共用）。"""
    ctx = _CompileCtx(module_globals=module_globals or {}, childflows=childflows or {})
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
    return data


def _compile_childflow_fdef(
    fdef: ast.FunctionDef,
    opts: Dict[str, Any],
    module_globals: Optional[Dict[str, Any]] = None,
    childflows: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """编译一个 ``@childflow`` FunctionDef 到子流程 IR dict。"""
    ctx = _CompileCtx(module_globals=module_globals or {}, childflows=childflows or {})
    entry = _compile_block(list(fdef.body), ctx, succ=None)
    if entry is None:
        raise _CodeflowError("子流程函数体为空", fdef)
    start_id = ctx.auto_id("start")
    ctx.nodes.insert(0, {"type": "start", "id": start_id, "next": entry})
    data: Dict[str, Any] = {"runtime": "python", "nodes": ctx.nodes}
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
        fl = Flow.model_validate(data)
        fl.__wrapped__ = func  # type: ignore[attr-defined]
        return fl

    return decorator


def childflow(
    desc: Optional[str] = None,
) -> Callable[[Callable], "_ChildFlowMarker"]:
    """``@childflow`` 装饰一个子流程函数，供 ``CHILD/REFERENCE/PARALLEL`` 引用。

    编译产物挂在 ``.__codeflow_ir__`` 上，被父流程编译时取出嵌入。
    """

    def decorator(func: Callable) -> _ChildFlowMarker:
        fdef = _func_ast(func)
        data = _compile_childflow_fdef(
            fdef,
            {"desc": desc},
            module_globals=getattr(func, "__globals__", {}),
        )
        return _ChildFlowMarker(data, func)

    return decorator


class _ChildFlowMarker:
    """``@childflow`` 的产物：携带编译好的 IR，可被父流程引用。"""

    def __init__(self, ir: Dict[str, Any], func: Callable) -> None:
        self.__codeflow_ir__ = ir
        self._func = func

    # 允许被父流程编译器按 AST Name 引用——但 AST 阶段拿不到运行时对象，
    # 所以 CHILD/REFERENCE 需通过字面 ``flow=<name>`` 且 name 在父模块可解析。
    # 为支持该路径，父编译器在 _eval_childflow_arg 里会尝试从装饰器闭包捕获。


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
            _, cf_opts = _extract_deco_opts(cf_deco)
            childflows[stmt.name] = _compile_childflow_fdef(stmt, cf_opts, childflows=childflows)
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

    等价于 ``Flow.model_validate(compile_source(src, flow_id, **opts))``。
    """
    return Flow.model_validate(compile_source(src, flow_id, **opts))
