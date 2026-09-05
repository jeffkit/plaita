"""
plaita.dsl.sexpr — S-表达式前端（**experimental**）。

.. warning::

   **Experimental / 非一等作者路径。** 新功能与自定义节点优先实现在
   ``@flow``（``codeflow``）与 ``FlowBuilder``；sexpr **不保证**与内置节点
   表同步演进，也不支持自定义节点占位符。生产 / AI 生成请用
   ``flow_from_source`` 或 JSON/YAML。

一种 Lisp 风格的 flow 编写方式，编译到现有 ``Flow`` IR，复用同一套
共享 ``validate_flow_ir`` 与运行时——不是新运行时，只是可选前端。

语法速览::

    (flow adult_check
      :input-type object
      :desc "判断成年"
      (start -> check_age)
      (if (cond "$INPUT.age" >= 18) -> adult :else minor)
      (end adult :output "成年")
      (end minor :output "未成年"))

加载与执行::

    from plaita.dsl.sexpr import parse_sexpr
    flow = parse_sexpr(src)
    flow.run(age=20)  # -> "成年"

也可只编译到 IR dict：``compile_sexpr(src)``。
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple, Union

from plaita.core.flow import Flow

__all__ = ["parse_sexpr", "compile_sexpr", "flow_to_sexpr"]


# ---------------------------------------------------------------------------
# 原子类型
# ---------------------------------------------------------------------------

class Symbol(str):
    """S-expr 符号：节点类型、id、运算符、``$INPUT.x`` 表达式等都是 Symbol。

    它是 ``str`` 子类，直接作为字符串放进 IR dict 即可（id、表达式字符串、
    字面量都按上下文解释，无需额外标记）。
    """

    __slots__ = ()


class Keyword(str):
    """关键字参数标记，``:foo`` 在源码里写成 ``:foo``，编译期取其名 ``foo``。"""

    __slots__ = ()


# 关键字别名：把可读的符号糖映射成规范关键字名
_KW_ALIASES: Dict[str, str] = {
    "->": "next",
    "then": "then",
    "else": "else",
}

_OP_ALIASES: Dict[str, str] = {
    "==": "eq", "!=": "ne", ">": "gt", ">=": "gte",
    "<": "lt", "<=": "lte", "in": "in", "not in": "notIn",
    "contains": "contains", "not contains": "notContains",
    "notIn": "notIn", "notContains": "notContains",
}

# 运算符翻转表，用于 (not <cond>) 的单条件取反
_OP_NEGATIONS: Dict[str, str] = {
    "eq": "ne", "ne": "eq",
    "gt": "lte", "gte": "lt", "lt": "gte", "lte": "gt",
    "in": "notIn", "notIn": "in",
    "contains": "notContains", "notContains": "contains",
}


# ---------------------------------------------------------------------------
# Reader：tokenize + 解析成嵌套 list
# ---------------------------------------------------------------------------

_TOKEN_RE = re.compile(
    r"""
    (?P<ws>\s+)
  | (?P<comment>;[^\n]*)
  | (?P<string>"(?:\\.|[^"\\])*")
  | (?P<lparen>\()
  | (?P<rparen>\))
  | (?P<atom>[^\s()";]+)
    """,
    re.VERBOSE,
)

_NUM_RE = re.compile(r"^-?\d+\.\d+$")
_INT_RE = re.compile(r"^-?\d+$")


def _tokenize(src: str) -> List[Tuple[str, str]]:
    tokens: List[Tuple[str, str]] = []
    pos = 0
    n = len(src)
    while pos < n:
        m = _TOKEN_RE.match(src, pos)
        if not m:
            raise SyntaxError(f"sexpr 词法错误，无法识别的字符: {src[pos]!r} @ {pos}")
        pos = m.end()
        kind = m.lastgroup
        if kind in ("ws", "comment"):
            continue
        tokens.append((kind, m.group()))
    return tokens


def _atom(kind: str, text: str) -> Any:
    if kind == "string":
        return _decode_string(text)
    if kind == "lparen":
        return "("
    if kind == "rparen":
        return ")"
    # atom
    if text in ("true", "True"):
        return True
    if text in ("false", "False"):
        return False
    if text in ("nil", "None", "null"):
        return None
    if _INT_RE.match(text):
        return int(text)
    if _NUM_RE.match(text):
        return float(text)
    if text.startswith(":"):
        return Keyword(text[1:])
    return Symbol(text)


def _decode_string(text: str) -> str:
    # 去掉首尾引号，处理常见转义
    body = text[1:-1]
    return body.encode("utf-8").decode("unicode_escape") if "\\" in body else body


def read_forms(src: str) -> List[Any]:
    """把源码解析成顶层 form 列表（通常只有一个 ``(flow ...)``）。"""
    tokens = _tokenize(src)
    forms: List[Any] = []
    i = 0

    def parse_expr(i: int) -> Tuple[Any, int]:
        kind, text = tokens[i]
        if kind == "lparen":
            i += 1
            lst: List[Any] = []
            while i < len(tokens) and tokens[i][0] != "rparen":
                expr, i = parse_expr(i)
                lst.append(expr)
            if i >= len(tokens):
                raise SyntaxError("sexpr 语法错误：缺少右括号 )")
            return lst, i + 1
        if kind == "rparen":
            raise SyntaxError("sexpr 语法错误：多余的右括号 )")
        return _atom(kind, text), i + 1

    while i < len(tokens):
        expr, i = parse_expr(i)
        forms.append(expr)
    return forms


# ---------------------------------------------------------------------------
# 编译辅助
# ---------------------------------------------------------------------------

def _is_symbol(x: Any) -> bool:
    return isinstance(x, Symbol)


def _is_keyword(x: Any) -> bool:
    return isinstance(x, Keyword)


def _split_args(args: List[Any]) -> Tuple[List[Any], Dict[str, Any]]:
    """把一个 form 的参数列表拆成 (位置参数, 关键字参数)。

    关键字形如 ``:foo value`` 或别名 ``-> value`` / ``then value`` / ``else value``。
    """
    positionals: List[Any] = []
    kwargs: Dict[str, Any] = {}
    i = 0
    while i < len(args):
        a = args[i]
        if _is_keyword(a):
            name = str(a)
            if i + 1 >= len(args):
                raise SyntaxError(f"关键字 :{name} 后缺少值")
            kwargs[name] = args[i + 1]
            i += 2
            continue
        if _is_symbol(a) and str(a) in _KW_ALIASES:
            name = _KW_ALIASES[str(a)]
            if i + 1 >= len(args):
                raise SyntaxError(f"关键字 {a} 后缺少值")
            kwargs[name] = args[i + 1]
            i += 2
            continue
        positionals.append(a)
        i += 1
    return positionals, kwargs


def _opt(kwargs: Dict[str, Any], *names: str) -> Any:
    for n in names:
        if n in kwargs:
            return kwargs[n]
    return None


def _value(x: Any) -> Any:
    """把原子转成 IR 值。Symbol/Keyword 都是 str 子类，直接用即可。

    嵌套 ``list`` 若以 ``dict`` / ``list`` 开头则编译成 Python dict/list 字面量，
    供 ``:headers`` / ``:body`` / ``:global-context`` 等使用：

    - ``(dict :Content-Type "application/json" :name "$INPUT.name")`` —— 关键字键
    - ``(dict ("Content-Type" "application/json") ("name" "$INPUT.name"))`` —— 元组键
    - ``(list 1 2 "$INPUT.x")``
    """
    if isinstance(x, list) and x and _is_symbol(x[0]):
        head = str(x[0])
        if head == "dict":
            return _compile_dict_literal(x[1:])
        if head == "list":
            return [_value(v) for v in x[1:]]
    return x


def _compile_dict_literal(args: List[Any]) -> Dict[Any, Any]:
    """``(dict ...)`` 字面量：支持 ``:key value`` 与 ``(key value)`` 两种条目。"""
    out: Dict[Any, Any] = {}
    i = 0
    while i < len(args):
        a = args[i]
        if _is_keyword(a):
            if i + 1 >= len(args):
                raise SyntaxError(f"dict 关键字 :{a} 后缺少值")
            out[str(a)] = _value(args[i + 1])
            i += 2
            continue
        if isinstance(a, list) and len(a) == 2:
            out[_value(a[0])] = _value(a[1])
            i += 1
            continue
        raise SyntaxError(f"dict 条目必须是 :key value 或 (key value)，得到 {a!r}")
    return out


class _Ctx:
    """单条 flow / childflow 的 id 分配上下文。

    id 在每条 flow 内唯一；未显式给出 ``:id`` 时自动生成 ``_n1`` / ``_n2``…，
    只在被分支引用或被 ``$NODE.<id>`` 引用时才需要显式命名（与 ``linear``
    builder 同样的策略）。
    """

    def __init__(self) -> None:
        self._counter = 0
        self._claimed: set = set()

    def claim(self, nid: Optional[str]) -> str:
        if nid:
            nid = str(nid)
            if nid in self._claimed:
                raise ValueError(f"节点 id 重复: {nid!r}")
            self._claimed.add(nid)
            return nid
        self._counter += 1
        cand = f"_n{self._counter}"
        while cand in self._claimed:
            self._counter += 1
            cand = f"_n{self._counter}"
        self._claimed.add(cand)
        return cand


# ---------------------------------------------------------------------------
# 条件 / 错误处理 / childflow
# ---------------------------------------------------------------------------

def _negate_condition(cond: Dict[str, Any]) -> Dict[str, Any]:
    """Negate an already-compiled condition dict using De Morgan's laws."""
    if "operator" in cond:
        op = cond["operator"]
        if op not in _OP_NEGATIONS:
            raise SyntaxError(f"not 无法翻转运算符 {op!r}，请改用 notIn/notContains/ne")
        return {"field": cond["field"], "operator": _OP_NEGATIONS[op], "value": cond["value"]}
    if cond.get("relation") == "and":
        return {"relation": "or", "conditions": [_negate_condition(c) for c in cond["conditions"]]}
    if cond.get("relation") == "or":
        return {"relation": "and", "conditions": [_negate_condition(c) for c in cond["conditions"]]}
    raise SyntaxError("not 只能作用于单个 cond 或 and/or 组")


def _compile_condition(form: Any) -> Dict[str, Any]:
    if not isinstance(form, list) or not form:
        raise SyntaxError(f"条件必须是 (cond ...)/(and ...)/(or ...)/(not ...) 列表，得到 {form!r}")
    head = form[0]
    if not _is_symbol(head):
        raise SyntaxError(f"条件 form 的头必须是符号，得到 {head!r}")
    name = str(head)
    rest = form[1:]
    if name == "cond":
        if len(rest) != 3:
            raise SyntaxError(f"cond 需要三个参数 (field op value)，得到 {rest!r}")
        field, op, value = rest
        op_s = str(op)
        return {"field": _value(field), "operator": _normalize_op(op_s), "value": _value(value)}
    if name == "and":
        return {"relation": "and", "conditions": [_compile_condition(c) for c in rest]}
    if name == "or":
        return {"relation": "or", "conditions": [_compile_condition(c) for c in rest]}
    if name == "not":
        if len(rest) != 1:
            raise SyntaxError("not 只接受单个条件")
        inner = _compile_condition(rest[0])
        return _negate_condition(inner)
    raise SyntaxError(f"未知条件形式 {name!r}")


def _normalize_op(op: str) -> str:
    return _OP_ALIASES.get(op, op)


def _compile_error_handler(form: Any) -> Dict[str, Any]:
    if not isinstance(form, list) or not _is_symbol(form[0]) or str(form[0]) != "on-error":
        raise SyntaxError(f"错误处理必须是 (on-error strategy ...) 形式，得到 {form!r}")
    pos, kw = _split_args(form[1:])
    if not pos:
        raise SyntaxError("on-error 需要指定 strategy")
    strategy = str(pos[0])
    if strategy not in ("abort", "continue", "continue_with", "continue-with"):
        # 连字符 "continue-with" 是 ErrorStrategy 的规范枚举值 (core 层两种拼写
        # 都收并归一化); DSL 层历史上只收下划线, 用户从 enum 取 .value 填进来会被拒。
        strategy = "continue_with" if strategy == "continue-with" else strategy
        raise SyntaxError(f"unknown error handler strategy: {strategy!r}")
    spec: Dict[str, Any] = {"strategy": strategy}
    retry = _opt(kw, "retry", "retry-times")
    if retry is not None:
        spec["retryTimes"] = retry
    default = _opt(kw, "default", "default-value")
    if default is not None:
        spec["defaultValue"] = _value(default)
    code = _opt(kw, "code", "error-code")
    if code is not None:
        spec["errorCode"] = code
    msg = _opt(kw, "msg", "message", "error-message")
    if msg is not None:
        spec["errorMessage"] = msg
    return spec


def _compile_childflow(form: Any) -> Dict[str, Any]:
    if not isinstance(form, list) or not _is_symbol(form[0]) or str(form[0]) != "childflow":
        raise SyntaxError(f"子流程必须是 (childflow ...) 形式，得到 {form!r}")
    pos, kw = _split_args(form[1:])
    body = pos  # 子流程体由节点 form 组成
    data: Dict[str, Any] = {"runtime": "python"}
    input_type = _opt(kw, "input-type", "inputType")
    if input_type is not None:
        data["inputType"] = _type_spec(input_type)
    output_type = _opt(kw, "output-type", "outputType")
    if output_type is not None:
        data["outputType"] = _type_spec(output_type)
    desc = _opt(kw, "desc")
    if desc is not None:
        data["desc"] = desc
    ctx = _Ctx()  # 子流程有独立 id 空间
    data["nodes"] = [_compile_node(n, ctx) for n in body]
    return data


def _type_spec(x: Any) -> Any:
    if isinstance(x, str):
        return {"dataType": x}
    return x


# ---------------------------------------------------------------------------
# 节点编译
# ---------------------------------------------------------------------------

def _compile_node(form: Any, ctx: _Ctx) -> Dict[str, Any]:
    if not isinstance(form, list) or not form:
        raise SyntaxError(f"节点必须是 (type ...) 列表，得到 {form!r}")
    head = form[0]
    if not _is_symbol(head):
        raise SyntaxError(f"节点 form 的头必须是符号，得到 {head!r}")
    ntype = str(head)
    handler = _NODE_COMPILERS.get(ntype)
    if handler is None:
        raise SyntaxError(f"未知节点类型 {ntype!r}")
    return handler(form[1:], ctx)


def _common_fields(kw: Dict[str, Any], spec: Dict[str, Any], ctx: _Ctx) -> None:
    spec["id"] = ctx.claim(_opt(kw, "id"))
    nxt = _opt(kw, "next", "then")
    if nxt is not None:
        spec["next"] = str(nxt)
    timeout = _opt(kw, "timeout")
    if timeout is not None:
        spec["timeout"] = timeout
    eh = _opt(kw, "on-error", "error-handler")
    if eh is not None:
        spec["errorHandler"] = _compile_error_handler(eh)


_NON_ID_SYMBOLS = {"true", "false", "nil", "True", "False", "None"}


def _take_id(pos: List[Any], kw: Dict[str, Any]) -> List[Any]:
    """为支持 ``(end end_adult "成年")`` 这种「位置 id」写法：

    若 ``:id`` 已通过关键字给出，直接返回；否则当首个位置参数是「不像表达式/字面量」
    的裸符号时，把它当作 id，剩余位置参数往后挪。``$INPUT.x`` 这类表达式符号
    不会被误当 id。
    """
    if _opt(kw, "id") is not None:
        return pos
    if pos and _is_symbol(pos[0]):
        s = str(pos[0])
        if not s.startswith("$") and s not in _NON_ID_SYMBOLS:
            kw["id"] = pos[0]
            return pos[1:]
    return pos


def _c_start(args: List[Any], ctx: _Ctx) -> Dict[str, Any]:
    pos, kw = _split_args(args)
    pos = _take_id(pos, kw)
    spec: Dict[str, Any] = {"type": "start"}
    _common_fields(kw, spec, ctx)
    return spec


def _c_end(args: List[Any], ctx: _Ctx) -> Dict[str, Any]:
    pos, kw = _split_args(args)
    pos = _take_id(pos, kw)
    spec: Dict[str, Any] = {"type": "end"}
    _common_fields(kw, spec, ctx)
    output = pos[0] if pos else _opt(kw, "output")
    if output is not None:
        spec["output"] = _value(output)
    rt = _opt(kw, "result-type", "resultType")
    spec["resultType"] = str(rt) if rt is not None else "success"
    error = _opt(kw, "error")
    if error is not None:
        spec["error"] = error if isinstance(error, dict) else {"message": str(error)}
    return spec


def _c_assignment(args: List[Any], ctx: _Ctx) -> Dict[str, Any]:
    pos, kw = _split_args(args)
    pos = _take_id(pos, kw)
    if not pos:
        raise SyntaxError("assign 需要一个 output 表达式")
    spec: Dict[str, Any] = {"type": "assignment", "output": _value(pos[0])}
    _common_fields(kw, spec, ctx)
    ot = _opt(kw, "output-type", "outputType")
    if ot is not None:
        spec["outputType"] = _type_spec(ot)
    return spec


def _c_if(args: List[Any], ctx: _Ctx) -> Dict[str, Any]:
    pos, kw = _split_args(args)
    if not pos:
        raise SyntaxError("if 需要一个条件")
    spec: Dict[str, Any] = {"type": "if", "condition": _compile_condition(pos[0])}
    _common_fields(kw, spec, ctx)
    then = _opt(kw, "next", "then")
    else_ = _opt(kw, "else", "else_next", "else-next")
    if then is None:
        raise SyntaxError("if 需要 :then（或 ->）真分支目标")
    if else_ is None:
        raise SyntaxError("if 需要 :else 假分支目标")
    spec["next"] = str(then)
    spec["else_next"] = str(else_)
    return spec


def _c_switch(args: List[Any], ctx: _Ctx) -> Dict[str, Any]:
    pos, kw = _split_args(args)
    branches = []
    for b in pos:
        if not isinstance(b, list) or not _is_symbol(b[0]) or str(b[0]) != "branch":
            raise SyntaxError("switch 的参数必须是 (branch ...) 形式")
        branches.append(_c_branch(b[1:]))
    if not branches:
        raise SyntaxError("switch 至少需要一条分支")
    spec: Dict[str, Any] = {"type": "switch", "branches": branches}
    _common_fields(kw, spec, ctx)
    return spec


def _c_branch(args: List[Any]) -> Dict[str, Any]:
    pos, kw = _split_args(args)
    if not pos:
        raise SyntaxError("branch 需要一个 name")
    spec: Dict[str, Any] = {"name": str(pos[0])}
    nxt = _opt(kw, "next")
    if nxt is not None:
        spec["next"] = str(nxt)
    prio = _opt(kw, "priority")
    if prio is not None:
        spec["priority"] = prio
    when = _opt(kw, "when", "condition")
    if when is not None:
        spec["condition"] = _compile_condition(when)
    if _opt(kw, "default", "is-default", "isDefault"):
        spec["isDefault"] = True
    return spec


def _c_case(args: List[Any], ctx: _Ctx) -> Dict[str, Any]:
    pos, kw = _split_args(args)
    target = _opt(kw, "target")
    if target is None:
        raise SyntaxError("case 需要 :target")
    cases = []
    for m in pos:
        if not isinstance(m, list) or not _is_symbol(m[0]) or str(m[0]) != "match":
            raise SyntaxError("case 的参数必须是 (match value next) 形式")
        cases.append(_c_match(m[1:]))
    spec: Dict[str, Any] = {"type": "case", "target": _value(target), "cases": cases}
    _common_fields(kw, spec, ctx)
    default = _opt(kw, "default")
    if default is not None:
        spec["default"] = str(default)
    return spec


def _c_match(args: List[Any]) -> Dict[str, Any]:
    pos, kw = _split_args(args)
    if len(pos) < 2:
        raise SyntaxError("match 需要 (value next) 两个参数")
    return {"name": str(_opt(kw, "name", "name") or pos[0]), "value": _value(pos[0]), "id": str(pos[1])}


def _collection_common(ntype: str, args: List[Any], ctx: _Ctx) -> Dict[str, Any]:
    pos, kw = _split_args(args)
    collection = _opt(kw, "collection")
    if collection is None:
        raise SyntaxError(f"{ntype} 需要 :collection")
    child = _opt(kw, "child", "child-flow", "childFlow")
    if child is None:
        raise SyntaxError(f"{ntype} 需要 :child (childflow ...)")
    spec: Dict[str, Any] = {
        "type": ntype,
        "collection": _value(collection),
        "childFlow": _compile_childflow(child),
    }
    _common_fields(kw, spec, ctx)
    return spec


def _c_loop(args: List[Any], ctx: _Ctx) -> Dict[str, Any]:
    spec = _collection_common("loop", args, ctx)
    _, kw = _split_args(args)
    when = _opt(kw, "when", "condition")
    if when is not None:
        spec["condition"] = _compile_condition(when)
    return spec


def _c_map(args: List[Any], ctx: _Ctx) -> Dict[str, Any]:
    spec = _collection_common("map", args, ctx)
    _, kw = _split_args(args)
    if _opt(kw, "concurrent"):
        spec["concurrent"] = True
        mc = _opt(kw, "max-concurrent", "maxConcurrent")
        if mc is not None:
            spec["maxConcurrent"] = mc
    return spec


def _c_child(args: List[Any], ctx: _Ctx) -> Dict[str, Any]:
    pos, kw = _split_args(args)
    inp = _opt(kw, "input")
    if inp is None and pos:
        inp = pos[0]
    if inp is None:
        raise SyntaxError("child 需要 :input")
    child = _opt(kw, "child", "child-flow", "childFlow")
    if child is None:
        raise SyntaxError("child 需要 :child (childflow ...)")
    spec: Dict[str, Any] = {"type": "child", "input": _value(inp), "childFlow": _compile_childflow(child)}
    _common_fields(kw, spec, ctx)
    return spec


def _c_reference(args: List[Any], ctx: _Ctx) -> Dict[str, Any]:
    spec = _c_child(args, ctx)
    spec["type"] = "reference"
    return spec


def _c_parallel(args: List[Any], ctx: _Ctx) -> Dict[str, Any]:
    pos, kw = _split_args(args)
    branches = []
    for b in pos:
        if not isinstance(b, list) or not _is_symbol(b[0]) or str(b[0]) != "pbranch":
            raise SyntaxError("parallel 的参数必须是 (pbranch ...) 形式")
        branches.append(_c_pbranch(b[1:]))
    spec: Dict[str, Any] = {"type": "parallel", "branches": branches}
    _common_fields(kw, spec, ctx)
    mode = _opt(kw, "mode")
    if mode is not None:
        spec["mode"] = str(mode)
    join = _opt(kw, "join", "join-branches", "joinBranches")
    if join is not None:
        spec["joinBranches"] = [str(x) for x in join] if isinstance(join, list) else [str(join)]
    if _opt(kw, "conditional", "is-conditional", "isConditional"):
        spec["isConditional"] = True
    return spec


def _c_pbranch(args: List[Any]) -> Dict[str, Any]:
    pos, kw = _split_args(args)
    if not pos:
        raise SyntaxError("pbranch 需要一个 name")
    spec: Dict[str, Any] = {"name": str(pos[0])}
    flow = _opt(kw, "flow", "child", "childFlow")
    if flow is None:
        raise SyntaxError("pbranch 需要 :flow (childflow ...)")
    spec["flow"] = _compile_childflow(flow)
    inp = _opt(kw, "input")
    if inp is not None:
        spec["input"] = _value(inp)
    when = _opt(kw, "when", "condition")
    if when is not None:
        spec["condition"] = _compile_condition(when)
    return spec


def _c_code(args: List[Any], ctx: _Ctx) -> Dict[str, Any]:
    pos, kw = _split_args(args)
    lang = _opt(kw, "lang", "language")
    code = _opt(kw, "code")
    if lang is None or code is None:
        raise SyntaxError("code 需要 :lang 和 :code")
    spec: Dict[str, Any] = {"type": "code", "language": str(lang), "code": code}
    _common_fields(kw, spec, ctx)
    inp = _opt(kw, "input")
    if inp is not None:
        spec["input"] = _value(inp)
    return spec


def _c_http(args: List[Any], ctx: _Ctx) -> Dict[str, Any]:
    pos, kw = _split_args(args)
    method = _opt(kw, "method")
    url = _opt(kw, "url")
    if method is None or url is None:
        raise SyntaxError("http 需要 :method 和 :url")
    spec: Dict[str, Any] = {"type": "http", "method": str(method), "url": url}
    _common_fields(kw, spec, ctx)
    headers = _opt(kw, "headers")
    if headers is not None:
        spec["headers"] = _value(headers)
    body = _opt(kw, "body")
    if body is not None:
        spec["body"] = _value(body)
    inp = _opt(kw, "input")
    if inp is not None:
        spec["input"] = _value(inp)
    return spec


def _c_event(args: List[Any], ctx: _Ctx) -> Dict[str, Any]:
    pos, kw = _split_args(args)
    etype = _opt(kw, "type", "event-type", "eventType")
    if etype is None:
        raise SyntaxError("event 需要 :type")
    spec: Dict[str, Any] = {"type": "event", "eventType": etype}
    _common_fields(kw, spec, ctx)
    flt = _opt(kw, "filter", "event-filter", "eventFilter")
    if flt is not None:
        spec["eventFilter"] = flt
    return spec


def _reduce(args: List[Any], ctx: _Ctx) -> Dict[str, Any]:
    spec = _collection_common("reduce", args, ctx)
    _, kw = _split_args(args)
    initial = _opt(kw, "initial")
    if initial is not None:
        spec["initial"] = _value(initial)
    return spec


_NODE_COMPILERS = {
    "start": _c_start,
    "end": _c_end,
    "assignment": _c_assignment,
    "assign": _c_assignment,
    "if": _c_if,
    "switch": _c_switch,
    "case": _c_case,
    "loop": _c_loop,
    "map": _c_map,
    "filter": lambda a, ctx: _collection_common("filter", a, ctx),
    "find": lambda a, ctx: _collection_common("find", a, ctx),
    "reduce": _reduce,
    "child": _c_child,
    "reference": _c_reference,
    "parallel": _c_parallel,
    "code": _c_code,
    "http": _c_http,
    "event": _c_event,
}


# ---------------------------------------------------------------------------
# 顶层 flow 编译
# ---------------------------------------------------------------------------

def _compile_flow(form: Any) -> Dict[str, Any]:
    if not isinstance(form, list) or not form or not _is_symbol(form[0]) or str(form[0]) != "flow":
        raise SyntaxError("顶层 form 必须是 (flow <id> ...)")
    pos, kw = _split_args(form[1:])
    if not pos:
        raise SyntaxError("flow 需要一个 flow_id")
    data: Dict[str, Any] = {"runtime": "python", "flow_id": str(pos[0])}
    input_type = _opt(kw, "input-type", "inputType")
    if input_type is not None:
        data["inputType"] = _type_spec(input_type)
    output_type = _opt(kw, "output-type", "outputType")
    if output_type is not None:
        data["outputType"] = _type_spec(output_type)
    for fld, key in (("desc", "desc"), ("version", "version"), ("author", "author"),
                     ("timeout", "timeout")):
        v = _opt(kw, fld)
        if v is not None:
            data[fld] = v
    gc = _opt(kw, "global-context", "globalContext")
    if gc is not None:
        data["globalContext"] = gc
    md = _opt(kw, "metadata")
    if md is not None:
        data["metadata"] = md
    body = pos[1:]
    ctx = _Ctx()
    data["nodes"] = [_compile_node(n, ctx) for n in body]
    return data


def compile_sexpr(src: str) -> Dict[str, Any]:
    """把 S-expr 源码编译成 Flow IR dict（经 ``Flow.model_validate`` 即可执行）。"""
    forms = read_forms(src)
    if not forms:
        raise ValueError("sexpr 源码为空")
    if len(forms) > 1:
        raise SyntaxError("目前只支持单个 (flow ...) 顶层 form")
    return _compile_flow(forms[0])


# ---------------------------------------------------------------------------
# 静态校验：委托共享 validate_flow_ir（含 recursive 子流程 / parallel）
# ---------------------------------------------------------------------------

def _static_validate(data: Dict[str, Any]) -> None:
    from plaita.dsl.ir_validate import validate_flow_ir

    validate_flow_ir(data, recursive=True)


def parse_sexpr(src: str) -> Flow:
    """编译、静态校验并构建，返回可执行的 ``Flow``。"""
    from plaita.dsl.ir_validate import build_flow

    return build_flow(compile_sexpr(src))


# ---------------------------------------------------------------------------
# 反向：IR dict -> sexpr（可逆，便于互转与审查）
# ---------------------------------------------------------------------------

def _expr_to_src(x: Any) -> str:
    if isinstance(x, str):
        if x == "":
            return '""'
        # 表达式（$开头）或纯标识符/字面量字符串都原样输出；含空格/特殊则加引号
        if re.fullmatch(r"[^\s()\";]+", x):
            return x
        return '"' + x.replace("\\", "\\\\").replace('"', '\\"') + '"'
    if isinstance(x, bool):
        return "true" if x else "false"
    if isinstance(x, (int, float)):
        return str(x)
    if x is None:
        return "nil"
    if isinstance(x, list):
        return "(" + " ".join(_expr_to_src(i) for i in x) + ")"
    if isinstance(x, dict):
        return _dict_to_sexpr(x)
    return str(x)


def _dict_to_sexpr(d: Dict[str, Any]) -> str:
    # 没有结构的 dict 用 :kv 对表示；目前仅用于 headers/globalContext 等
    items = []
    for k, v in d.items():
        items.append(f":{k} {_expr_to_src(v)}")
    return "(dict " + " ".join(items) + ")" if items else "(dict)"


def _cond_to_src(c: Any) -> str:
    if isinstance(c, dict):
        if "relation" in c:
            inner = " ".join(_cond_to_src(x) for x in c["conditions"])
            return f"({c['relation']} {inner})"
        op = c["operator"]
        return f'(cond {_expr_to_src(c["field"])} {op} {_expr_to_src(c["value"])})'
    return _expr_to_src(c)


def _node_to_src(n: Dict[str, Any]) -> str:
    t = n.get("type")
    parts: List[str] = [t]
    nid = n.get("id")
    if nid is not None:
        parts.append(f":id {nid}")
    if t == "start":
        if n.get("next") is not None:
            parts.append(f"-> {n['next']}")
    elif t == "end":
        if n.get("output") is not None:
            parts.append(f":output {_expr_to_src(n['output'])}")
        if n.get("resultType") and n["resultType"] != "success":
            parts.append(f":result-type {n['resultType']}")
    elif t == "assignment":
        parts.append(_expr_to_src(n.get("output")))
        if n.get("next") is not None:
            parts.append(f"-> {n['next']}")
    elif t == "if":
        parts.append(_cond_to_src(n.get("condition")))
        parts.append(f"-> {n.get('next')}")
        parts.append(f":else {n.get('else_next')}")
    elif t == "switch":
        for b in n.get("branches", []):
            bp = ["branch", b.get("name"), f"-> {b.get('next')}"]
            if b.get("condition") is not None:
                bp.append(f":when {_cond_to_src(b['condition'])}")
            if b.get("isDefault"):
                bp.append(":default true")
            parts.append("(" + " ".join(str(x) for x in bp) + ")")
    elif t == "case":
        parts.append(f":target {_expr_to_src(n.get('target'))}")
        for c in n.get("cases", []):
            parts.append(f"(match {_expr_to_src(c.get('value'))} {c.get('id')})")
        if n.get("default") is not None:
            parts.append(f":default {n['default']}")
    elif t in ("loop", "map", "filter", "find", "reduce"):
        parts.append(f":collection {_expr_to_src(n.get('collection'))}")
        parts.append(f":child ({_flow_inner_to_src(n.get('childFlow'))})")
        if t == "loop" and n.get("condition") is not None:
            parts.append(f":when {_cond_to_src(n['condition'])}")
        if t == "map" and n.get("concurrent"):
            parts.append(":concurrent true")
        if t == "reduce" and n.get("initial") is not None:
            parts.append(f":initial {_expr_to_src(n['initial'])}")
        if n.get("next") is not None:
            parts.append(f"-> {n['next']}")
    elif t in ("child", "reference"):
        parts.append(f":input {_expr_to_src(n.get('input'))}")
        parts.append(f":child ({_flow_inner_to_src(n.get('childFlow'))})")
        if n.get("next") is not None:
            parts.append(f"-> {n['next']}")
    elif t == "http":
        parts.append(f":method {n.get('method')}")
        parts.append(f":url {_expr_to_src(n.get('url'))}")
        if n.get("headers") is not None:
            parts.append(f":headers {_expr_to_src(n['headers'])}")
        if n.get("body") is not None:
            parts.append(f":body {_expr_to_src(n['body'])}")
        if n.get("next") is not None:
            parts.append(f"-> {n['next']}")
    elif t == "code":
        parts.append(f":lang {n.get('language')}")
        parts.append(f":code {_expr_to_src(n.get('code'))}")
        if n.get("next") is not None:
            parts.append(f"-> {n['next']}")
    elif t == "event":
        parts.append(f":type {_expr_to_src(n.get('eventType'))}")
    if n.get("errorHandler") is not None:
        eh = n["errorHandler"]
        ep = ["on-error", eh.get("strategy", "abort")]
        if "retryTimes" in eh:
            ep.append(f":retry {eh['retryTimes']}")
        if "defaultValue" in eh:
            ep.append(f":default {_expr_to_src(eh['defaultValue'])}")
        parts.append("(" + " ".join(str(x) for x in ep) + ")")
    return "(" + " ".join(str(x) for x in parts) + ")"


def _flow_inner_to_src(d: Dict[str, Any]) -> str:
    parts: List[str] = ["childflow"]
    if d.get("inputType") is not None:
        it = d["inputType"]
        parts.append(f":input-type {it.get('dataType', 'object') if isinstance(it, dict) else it}")
    for n in d.get("nodes", []):
        parts.append(_node_to_src(n))
    return " ".join(parts)


def flow_to_sexpr(data: Dict[str, Any]) -> str:
    """把 Flow IR dict 反编译成 S-expr 源码（可逆，用于审查/互转）。"""
    parts: List[str] = ["flow", str(data.get("flow_id") or "flow")]
    if data.get("inputType") is not None:
        it = data["inputType"]
        parts.append(f":input-type {it.get('dataType', 'object') if isinstance(it, dict) else it}")
    if data.get("desc"):
        parts.append(f":desc {_expr_to_src(data['desc'])}")
    for n in data.get("nodes", []):
        parts.append(_node_to_src(n))
    return "(" + " ".join(parts) + ")"
