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

内联子流程（``(child ... :child (childflow ...))`` / ``map`` / ``loop`` 等）的
自身 id 用 ``:flow-id`` 给出——``:id`` 已被节点自身占用，故不复用；不给则
``flow_id`` 保持缺失（**不凭空造名**），发射侧同样不写该键（#149）::

    (child :id c :input "$INPUT.x"
      :child (childflow :flow-id "cf" (start -> e) (end e :output "$INPUT")))
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple, Union

from plaita.core.flow import Flow

__all__ = ["parse_sexpr", "compile_sexpr", "flow_to_sexpr", "SexprEmitError"]


class SexprEmitError(ValueError):
    """发射侧拒绝写出「读不回来」的源码（#76/#148 原则）。

    ``flow_to_sexpr`` 只应在能保证 ``compile_sexpr`` 忠实还原时才产出文本；
    遇到确实无法表达的 IR 形状（见 ``_dict_entry_to_src`` 的 dict 键边界），
    它**在产出任何文本之前**抛本异常，而不是写出会被静默改形的源码。
    """


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

#: 读侧**裸原子**接受的字符集（与 ``_TOKEN_RE`` 的 ``atom`` 分支同源）。发射侧
#: 用它判断一个字符串能否裸写：``_expr_to_src`` 对**值**用它，``_dict_entry_to_src``
#: 对**键**用它。两处共用同一常量，避免两条"什么算安全原子"的规则漂移。
_BARE_ATOM_RE = re.compile(r'[^\s()";]+')


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


# 字符串转义映射：显式白名单而非 unicode_escape——后者把多字节 UTF-8 按
# latin-1 逐字节解码，含中文/emoji 的字符串静默乱码（如「中文」→
# 'ä¸\xadæ\x96\x87'，2026-10 评审修复包 A3）。未列出的转义序列原样保留。
_STRING_ESCAPES = {
    "n": "\n",
    "t": "\t",
    "r": "\r",
    '"': '"',
    "\\": "\\",
}


def _decode_string(text: str) -> str:
    # 去掉首尾引号，处理常见转义
    body = text[1:-1]
    if "\\" not in body:
        return body
    out: List[str] = []
    i = 0
    n = len(body)
    while i < n:
        ch = body[i]
        if ch == "\\" and i + 1 < n:
            mapped = _STRING_ESCAPES.get(body[i + 1])
            if mapped is not None:
                out.append(mapped)
                i += 2
                continue
        out.append(ch)
        i += 1
    return "".join(out)


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


def _dict_entry_key(x: Any) -> Any:
    """读 ``(key value)`` 元组条目里的 **key**（``_dict_entry_to_src`` 的读侧配对件）。

    #148：写侧对「不能裸写成 ``:key``」的键改用**元组条目** ``("a b" 1)``；本函数
    是那条拼写的读侧真源——引号字符串经 ``_decode_string`` 原样还原，``true`` /
    ``1`` / ``nil`` 等标量也按字面量还原，故写侧发射的键能逐字回到 IR。

    为什么不让元组键继续走通用 ``_value()``：``_value`` 会把 ``(dict …)`` /
    ``(list …)`` 这类**复合**形状编译成不可哈希的 dict/list，随后在 ``out[...]``
    处炸出 ``TypeError``。dict 键的合法域是字符串与标量，这里显式收紧并给出
    **具名** ``SyntaxError``，与写侧的 fail-closed 边界（``SexprEmitError``）对齐。
    """
    if isinstance(x, str):  # 含 Symbol/Keyword（str 子类）
        return str(x)
    if x is None or isinstance(x, bool) or isinstance(x, int) or isinstance(x, float):
        return x
    raise SyntaxError(f"dict 键必须是字符串或标量，得到 {x!r}")


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
            out[_dict_entry_key(a[0])] = _value(a[1])
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
    # #149: a nested child flow's own id. The maintainer fixed the spelling as
    # ``:flow-id`` (``:id`` is already the node's own id; ``:flow_id``/``:flowId``
    # stay unclaimed on purpose so the decision is not quietly widened). Only
    # written when given — ``None`` must stay absent, never become ``"None"``.
    flow_id = _opt(kw, "flow-id")
    if flow_id is not None:
        data["flow_id"] = str(flow_id)
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
    # #136 (same root, item 1): `:error` must go through `_value()`. Without it
    # the `(dict :code 4001 :message bad)` literal emitted by the writer stays
    # a raw list and is then wrapped by the else-branch below as
    # `{"message": "[dict, ':code', 4001, ...]"}` -- both the error code and the
    # message are corrupted by a round-trip. `_value()` is the very path that
    # `:headers` / `:filter` already take.
    error = _value(_opt(kw, "error"))
    if error is not None:
        spec["error"] = error if isinstance(error, dict) else {"message": str(error)}
    return spec


def _c_assignment(args: List[Any], ctx: _Ctx) -> Dict[str, Any]:
    pos, kw = _split_args(args)
    pos = _take_id(pos, kw)
    if not pos:
        raise SyntaxError("assign 需要一个 output 表达式")
    if len(pos) > 1:
        # 静默丢弃多余位置参数曾造成"按 Lisp 直觉不加引号的函数调用"被
        # 错编译成 output="$F.mul"（参数列表丢失），运行期得到错值。
        raise SyntaxError(
            f"assign 只接受一个 output 表达式，收到 {len(pos)} 个；"
            '函数调用表达式需要加引号，如 "$F.mul($INPUT.x, 10)"'
        )
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
    # #220-L3：`sandbox_backend` 是逐节点的沙箱档位（安全字段，
    # `plaita/node/code.py::CodeNode.execute` 直接读该属性选隔离实现；缺省回退
    # 进程级默认）。读侧此前不认这个关键字 ⇒ sexpr 往返静默回退默认档位。
    # `execute` 不对该字段做表达式求值，故只接受**非空字符串字面量**；其它形状
    # （list/dict/bool/数字/空串）响亮拒绝，不留给运行期才炸。
    backend = _opt(kw, "sandbox-backend", "sandbox_backend", "sandboxBackend")
    if backend is not None:
        value = _value(backend)
        if not isinstance(value, str) or not value:
            raise SyntaxError(
                f"code 的 :sandbox-backend 需要非空字符串，得到 {value!r}"
            )
        spec["sandbox_backend"] = str(value)
    return spec


def _host_list(x: Any, field: str) -> List[str]:
    """读侧归一 ``:allowed-hosts`` / ``:denied-hosts``：list 或单值 → ``list[str]``。

    ``(list "a" "b")`` 由 ``_value()`` 编译成 list；单值（``:allowed-hosts a``）
    也接受。元素必须是字符串（主机名 / ``*.suffix`` / CIDR 字面量）；数字或
    bool 是作者笔误，**响亮拒绝**而不是让 ``Flow.model_validate`` 给出难懂的
    类型错误（#200）。
    """
    values = _value(x)
    values = values if isinstance(values, list) else [values]
    out: List[str] = []
    for v in values:
        if isinstance(v, str):
            out.append(str(v))
            continue
        raise SyntaxError(
            f"http 的 :{field} 需要主机字符串（可写成 :{field} host 或 "
            f":{field} (list host1 host2 ...)），得到 {v!r}"
        )
    return out


def _positive_seconds(kw: Dict[str, Any], *names: str) -> Any:
    """读以秒为单位的策略字段（``:request-timeout`` / ``:subscription-timeout``）。

    必须是正数：bool 是作者笔误（``true`` 不是 1 秒），0/负数会让订阅一注册就
    超时或让请求立刻失败，因此**响亮拒绝**而不是留给运行期。
    """
    value = _opt(kw, *names)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SyntaxError(f":{names[0]} 需要数字（秒），得到 {value!r}")
    if value <= 0:
        raise SyntaxError(f":{names[0]} 需要正数（秒），得到 {value!r}")
    return value


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
    # #200：访问策略是 SSRF 防护面（HTTP.allowed_hosts / denied_hosts /
    # block_private_networks / request_timeout / max_redirects）。读侧此前一条
    # 都不认 ⇒ 写侧也无法发射 ⇒ 任何 sexpr 往返都把白名单/私网阻断**静默清空**，
    # 节点回落「无限制」默认。这里补齐读侧，写侧 `_node_to_src` 对称发射。
    allowed = _opt(kw, "allowed-hosts", "allowedHosts", "allowed_hosts")
    if allowed is not None:
        spec["allowedHosts"] = _host_list(allowed, "allowed-hosts")
    denied = _opt(kw, "denied-hosts", "deniedHosts", "denied_hosts")
    if denied is not None:
        spec["deniedHosts"] = _host_list(denied, "denied-hosts")
    block = _opt(kw, "block-private-networks", "blockPrivateNetworks",
                 "block_private_networks")
    if block is not None:
        if not isinstance(block, bool):
            raise SyntaxError(
                "http 的 :block-private-networks 需要 true/false，"
                f"得到 {block!r}"
            )
        spec["blockPrivateNetworks"] = block
    timeout = _positive_seconds(kw, "request-timeout", "requestTimeout",
                                 "request_timeout")
    if timeout is not None:
        spec["requestTimeout"] = timeout
    redirects = _opt(kw, "max-redirects", "maxRedirects", "max_redirects")
    if redirects is not None:
        ok = (not isinstance(redirects, bool)
              and isinstance(redirects, (int, float))
              and redirects >= 0 and int(redirects) == redirects)
        if not ok:
            raise SyntaxError(
                "http 的 :max-redirects 需要非负整数，"
                f"得到 {redirects!r}"
            )
        spec["maxRedirects"] = int(redirects)
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
        # #136：必须经 `_value()` 编译，否则 `(dict :a 1)` 会**原样保留为列表**
        # `['dict','a',1]`：发射时走 list 分支（丢冒号）⇒ 再解析仍是 list
        # ⇒ 模型校验把它压成 `{}` ⇒ **事件静默失去过滤条件、不再按条件触发**。
        # 其它关键字（如 `:headers`）走的是 `_value` 路径，故本处属遗漏。
        spec["eventFilter"] = _value(flt)
    # #200：订阅超时（EventNode.subscription_timeout）此前读写两侧都没有表达面，
    # 往返即丢 ⇒ 本应超时唤醒的挂起退化成无限等待。
    sub_timeout = _positive_seconds(kw, "subscription-timeout",
                                     "subscriptionTimeout", "subscription_timeout")
    if sub_timeout is not None:
        spec["subscriptionTimeout"] = sub_timeout
    return spec


def _c_mock(args: List[Any], ctx: _Ctx) -> Dict[str, Any]:
    """``(mock <id> [:value v] -> next)``：#146 补的节点表条目。

    ``Mock``（``plaita/node/mock.py``）是数据固定（pin）占位节点，唯一自有字段是
    ``value``（原样作为节点输出返回），另有继承自 ``Node`` 的 ``output``——
    console 的 dryrun 把被 pin 的节点整体替换成 mock 时**保留原节点的 ``output``**，
    故这里一并读回，避免"替换后往返一次就丢字段"。

    ``:value`` 必须经 ``_value()`` 编译（#87e4dd7 的 dict 字面量规则）：否则
    ``(dict :a 1)`` 会原样留成 ``['dict', 'a', 1]`` 列表，发射侧再按 list 分支
    丢冒号 ⇒ 往返后不再是 dict。位置写法（``(mock m 42)``）等价于 ``:value``，
    与 ``_c_assignment`` / ``_c_end`` 的单值位置参数同形；多余位置参数**报错**
    而不是静默丢弃（同 ``_c_assignment`` 的理由）。
    """
    pos, kw = _split_args(args)
    pos = _take_id(pos, kw)
    spec: Dict[str, Any] = {"type": "mock"}
    _common_fields(kw, spec, ctx)
    raw = pos[0] if pos else _opt(kw, "value")
    if len(pos) > 1:
        raise SyntaxError(
            f"mock 最多接受一个位置值，收到 {len(pos)} 个；"
            "如需固定多个字段请用 :value 包一个 (dict ...) 字面量"
        )
    if raw is not None:
        spec["value"] = _value(raw)
    out = _opt(kw, "output")
    if out is not None:
        spec["output"] = _value(out)
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
    # #146：节点表覆盖缺口——`mock`（plaita/node/mock.py）读侧从来没有 dispatch
    # 分支，写侧也没有发射臂，含 mock 节点的 flow 在 sexpr 里既发不出字段也读不
    # 回来（发射 `(mock :id m)` 丢 value/next，再解析直接 "未知节点类型 'mock'"）。
    "mock": _c_mock,
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

def _string_to_src(s: str) -> str:
    """把一个字符串发射成 sexpr **字符串字面量**（总是加引号）。

    转义规则与读侧 ``_STRING_ESCAPES`` / ``_decode_string`` 配对：反斜杠与双引号
    转义，其余字符（含 unicode、换行）原样留在引号内——词法正则
    ``"(?:\\\\.|[^"\\\\])*"`` 接受引号内的裸换行，读回不丢。

    ``_expr_to_src`` 只在"裸原子不安全"时走这里；``:flow-id`` 的值是**标识符**
    而非表达式，裸写会把 ``nil``/``true``/``:x`` 这类值读成语义不同的原子
    （实测 ``nil`` 变 None、``true`` 变 ``True``、``:x`` 变 ``x``），故它**总是**
    走这里。
    """
    return '"' + s.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _expr_to_src(x: Any) -> str:
    if isinstance(x, str):
        if x == "":
            return '""'
        # 表达式（$开头）或纯标识符/字面量字符串都原样输出；含空格/特殊则加引号
        if _BARE_ATOM_RE.fullmatch(x):
            return x
        return _string_to_src(x)
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


def _scalar_dict_key_to_src(k: Any) -> str:
    """非 ``str`` 键的裸标量拼写；不能忠实回读的标量在此 fail closed。

    - ``True`` / ``False`` → ``true`` / ``false``，``None`` → ``nil``：读侧
      ``_dict_entry_key`` 经 ``_value``-等价规则还原为同名标量；
    - ``int`` → 十进制（``str(k)`` 恒匹配读侧 ``_INT_RE``）；
    - ``float`` → 仅当 ``str(k)`` 匹配读侧 ``_NUM_RE``（``1e20`` / ``inf`` /
      ``nan`` 会落成 ``Symbol``，类型被改，故拒绝）。
    """
    if isinstance(k, bool):
        return "true" if k else "false"
    if k is None:
        return "nil"
    if isinstance(k, int):
        return str(k)
    if isinstance(k, float) and _NUM_RE.match(str(k)):
        return str(k)
    raise SexprEmitError(
        f"dict 键 {k!r}（类型 {type(k).__name__}）无法发射成可忠实回读的源码；"
        "可表达的键是 str 与有限标量 (bool/int/十进制 float/None)"
    )


def _dict_entry_to_src(k: Any, v: Any) -> str:
    """发射一条 ``(dict …)`` 条目——**键**的拼写是 #148 的核心。

    写侧两条拼写，与读侧 ``_compile_dict_literal`` / ``_dict_entry_key`` 一一配对：

    - **裸关键字** ``:k v``：仅当 ``k`` 是非空 ``str`` 且字符全在裸原子集内
      （无空白 / ``()`` / ``"`` / ``;``）时使用。此时读侧 ``_atom`` 把 ``:k``
      取成 ``Keyword(k)``，``str()`` 后逐字还原——**base 已正确的简单键保持逐字节
      不变**（既有语料零改动）。
    - **元组条目** ``("k" v)``：其余键（空格 / 引号 / 括号 / 分号 / 空串 / 数字样
      ``"1"`` / ``nil`` 等）一律走 ``_string_to_src`` 恒加引号；读侧
      ``_dict_entry_key`` 解出精确字符串。**不能**沿用 ``_expr_to_src`` 的裸原子
      约定——那会把键 ``1`` 读成 ``int``、``nil`` 读成 ``None``、``:x`` 读成 ``x``。
    """
    vsrc = _expr_to_src(v)
    if isinstance(k, str):
        if k and _BARE_ATOM_RE.fullmatch(k):
            return f":{k} {vsrc}"
        return f"({_string_to_src(k)} {vsrc})"
    return f"({_scalar_dict_key_to_src(k)} {vsrc})"


def _dict_to_sexpr(d: Dict[str, Any]) -> str:
    # 没有结构的 dict 用 :kv 对表示；目前仅用于 headers/globalContext 等
    items = [_dict_entry_to_src(k, v) for k, v in d.items()]
    return "(dict " + " ".join(items) + ")" if items else "(dict)"


def _cond_to_src(c: Any) -> str:
    if isinstance(c, dict):
        if "relation" in c:
            inner = " ".join(_cond_to_src(x) for x in c["conditions"])
            return f"({c['relation']} {inner})"
        op = c["operator"]
        return f'(cond {_expr_to_src(c["field"])} {op} {_expr_to_src(c["value"])})'
    return _expr_to_src(c)


def _error_handler_to_src(eh: Dict[str, Any]) -> str:
    """Emit ``errorHandler`` as the nested form ``(on-error <strategy> ...)``.

    #136 (same root, item 3): it must be emitted as the **value** of
    ``:on-error``, not as a trailing positional argument of the node. A
    positional argument is swallowed by the reader as ``output``:

        (end :id n :output x (on-error abort :retry 2))
        -> output = ['on-error', 'abort', 'retry', 2], retry_times reset to 0

    The fields mirror the keys ``_compile_error_handler`` accepts, so the whole
    ``errorHandler`` payload round-trips losslessly (including ``errorCode`` /
    ``errorMessage``, which the writer used to drop silently as well).
    """
    ep = ["on-error", str(eh.get("strategy", "abort"))]
    if "retryTimes" in eh:
        ep.append(f":retry {eh['retryTimes']}")
    if "defaultValue" in eh:
        ep.append(f":default {_expr_to_src(eh['defaultValue'])}")
    if "errorCode" in eh:
        ep.append(f":code {eh['errorCode']}")
    if "errorMessage" in eh:
        ep.append(f":msg {_expr_to_src(eh['errorMessage'])}")
    return "(" + " ".join(ep) + ")"


def _common_kw_to_src(n: Dict[str, Any]) -> List[str]:
    """Emit the node-level common keywords (``:timeout`` / ``:on-error``).

    #136 (same root, item 2): the reader (``_common_fields``) has always read
    both fields for **every** node type, while the writer emitted neither --
    so a round-trip silently dropped ``timeout`` and corrupted the handler.

    Position: emitted right after ``:id`` and before any ``-> next``.

    Measured correction (2026-10-13, adversarial review): keyword position is
    **not** what makes a field readable. ``_split_args`` scans the whole flat
    argument list, so a flat ``:kw value`` pair is recognised on either side of
    ``-> next`` -- ``(assignment :id a 1 -> e :timeout 5)`` yields
    ``timeout == 5`` and ``(event :id ev :type t -> e :filter (dict :a 1))``
    yields ``eventFilter == {'a': 1}``, on the base tree and here alike. What
    actually degrades into a positional argument is a **nested list form**
    written outside a keyword slot: the old trailing
    ``(on-error abort :retry 2)`` was collected as the node's ``output`` (and on
    ``assignment`` it raised "assign only accepts one output expression").

    Emitting the common keywords before the arrow is therefore *not required
    for correctness*; it is kept because it is stable, because it matches the
    "keywords before the successor" shape ``7d0add9`` established for
    ``event``'s ``:filter``, and so this emitter does not depend on the
    reader's flat scan.
    """
    out: List[str] = []
    timeout = n.get("timeout")
    if timeout is not None:
        out.append(f":timeout {_expr_to_src(timeout)}")
    eh = n.get("errorHandler")
    if eh is not None:
        out.append(f":on-error {_error_handler_to_src(eh)}")
    return out


def _dual(n: Dict[str, Any], camel: str, snake: str) -> Any:
    """同一字段的 camelCase / snake_case 双读（#200/#207 写侧的统一口径）。

    ``--format ir`` / 手写 JSON 用 camel，``Flow.model_dump()`` 用 snake；写侧
    只看一种就会静默丢字段。取先出现的非 ``None`` 值——`False` / `0` / `[]`
    都是合法 IR 值，不能用真值判断。
    """
    value = n.get(camel)
    if value is None:
        value = n.get(snake)
    return value


def _host_list_to_src(values: Any) -> str:
    """发射主机策略列表为 ``(list "a" "*.b" ...)``。

    必须显式写 ``list`` 头：裸写 ``(a b)`` 时若首元素恰好是 ``dict`` / ``list``，
    读侧 ``_value()`` 会把它当字面量容器编译，**静默改形**。每个元素恒走
    ``_string_to_src``：``1.2.3.4`` / ``true`` / ``nil`` 裸写会被读成
    int/bool/None，主机名必须永远是字符串（#200）。
    """
    if isinstance(values, str):
        values = [values]
    elif not isinstance(values, (list, tuple)):
        raise SexprEmitError(
            f"主机策略字段需要字符串列表，得到 {values!r}"
        )
    return "(list " + " ".join(_string_to_src(str(v)) for v in values) + ")"


def _node_to_src(n: Dict[str, Any]) -> str:
    t = n.get("type")
    parts: List[str] = [t]
    nid = n.get("id")
    if nid is not None:
        parts.append(f":id {nid}")
    # Common keywords right after `:id` (see `_common_kw_to_src` for why this
    # is a shape/robustness choice rather than a reader requirement).
    parts.extend(_common_kw_to_src(n))
    if t == "start":
        if n.get("next") is not None:
            parts.append(f"-> {n['next']}")
    elif t == "end":
        if n.get("output") is not None:
            parts.append(f":output {_expr_to_src(n['output'])}")
        # #207：读侧 `_c_end` 只产出 camelCase `resultType`，但同一 IR 的
        # snake_case 形态（`--format ir` / `Flow.model_dump()`）用
        # `result_type`——写侧只看 camel 时**不发射 `:result-type`**，
        # 读侧缺省回填 `success` ⇒ `result_type=error` 的失败出口往返后被
        # 洗成成功（error 载荷还在，语义已错）。与 codeflow 发射器
        # （`_emit.py`：`resultType or result_type`）同口径双读。
        result_type = n.get("resultType") or n.get("result_type")
        if result_type and result_type != "success":
            parts.append(f":result-type {result_type}")
        # #136 (item 1 of this patch, the issue itself): `error` used to exist
        # only on the reader side (`_c_end` reads `:error`) while the writer
        # never emitted it -> after a round-trip the payload is gone and the
        # terminal state changes from `ErrorResultException`
        # (error_result/4001) to `NodeExecutionError` (node_error/-9527).
        # An empty dict `{}` is data too (`End.execute` uses it to tell "error
        # exit without a code" apart from "no error exit at all"), hence the
        # `is not None` test rather than truthiness.
        if n.get("error") is not None:
            parts.append(f":error {_expr_to_src(n['error'])}")
        # #150: `_common_fields` reads `:next` for *every* node, `end` included;
        # this arm was the one place it was not written back.
        if n.get("next") is not None:
            parts.append(f"-> {n['next']}")
    elif t == "assignment":
        parts.append(_expr_to_src(n.get("output")))
        # #150: `_c_assignment` reads `:output-type` and the field is declared on
        # `Assignment.output_type`; the writer dropped it (unlike `:output-type`
        # on the root flow, which #147 made emit).
        if n.get("outputType") is not None:
            parts.append(f":output-type {_type_to_src(n['outputType'])}")
        if n.get("next") is not None:
            parts.append(f"-> {n['next']}")
    elif t == "if":
        parts.append(_cond_to_src(n.get("condition")))
        parts.append(f"-> {n.get('next')}")
        parts.append(f":else {n.get('else_next')}")
    elif t == "switch":
        for b in n.get("branches", []):
            bp = ["branch", b.get("name"), f"-> {b.get('next')}"]
            # #150: `_c_branch` reads `:priority`, and `Branch.priority` decides
            # the match order in `Switch.execute` (sorted desc) -- dropping it
            # silently changes which branch wins. Emitted only when present, so
            # branches without one keep the base bytes.
            if b.get("priority") is not None:
                bp.append(f":priority {b['priority']}")
            if b.get("condition") is not None:
                bp.append(f":when {_cond_to_src(b['condition'])}")
            if b.get("isDefault"):
                bp.append(":default true")
            parts.append("(" + " ".join(str(x) for x in bp) + ")")
        # #150: the switch node's own successor (read by `_common_fields`) was
        # the second omission in this arm.
        if n.get("next") is not None:
            parts.append(f"-> {n['next']}")
    elif t == "case":
        parts.append(f":target {_expr_to_src(n.get('target'))}")
        for c in n.get("cases", []):
            # #150: `_c_match` accepts `:name` and stores ``cases[i]["name"]``
            # (surfaced downstream as the branch `label`). It also *defaults*
            # that name to ``str(pos[0])`` -- for the overwhelming majority of
            # matches the IR name therefore already equals ``str(value)``, and
            # omitting `:name` re-reads to exactly the same value. Emitting it
            # only when it differs keeps every such match byte-identical to base
            # (measured: 0 unrelated corpus changes) while making a genuinely
            # custom name survive. The name goes through `_string_to_src` for the
            # same reason `:flow-id` does: it is a label, not an expression, so a
            # numeric-looking name (`"01"`) must not collapse to an int.
            head = f"(match {_expr_to_src(c.get('value'))} {c.get('id')}"
            cname = c.get("name")
            if cname is not None and str(cname) != str(c.get("value")):
                head += f" :name {_string_to_src(str(cname))}"
            parts.append(head + ")")
        if n.get("default") is not None:
            parts.append(f":default {n['default']}")
    elif t in ("loop", "map", "filter", "find", "reduce"):
        parts.append(f":collection {_expr_to_src(n.get('collection'))}")
        parts.append(f":child ({_flow_inner_to_src(n.get('childFlow'))})")
        if t == "loop" and n.get("condition") is not None:
            parts.append(f":when {_cond_to_src(n['condition'])}")
        if t == "map" and n.get("concurrent"):
            parts.append(":concurrent true")
        # #150: `_c_map` reads `:max-concurrent`; `Map.max_concurrent` is a
        # declared field. Only meaningful alongside `:concurrent` (the reader
        # sets it inside that guard), which is exactly how the reader produces
        # it, so the round-trip is faithful.
        if t == "map" and n.get("maxConcurrent") is not None:
            parts.append(f":max-concurrent {n['maxConcurrent']}")
        if t == "reduce" and n.get("initial") is not None:
            parts.append(f":initial {_expr_to_src(n['initial'])}")
        if n.get("next") is not None:
            parts.append(f"-> {n['next']}")
    elif t in ("child", "reference"):
        parts.append(f":input {_expr_to_src(n.get('input'))}")
        parts.append(f":child ({_flow_inner_to_src(n.get('childFlow'))})")
        if n.get("next") is not None:
            parts.append(f"-> {n['next']}")
    elif t == "parallel":
        # #150: the reader (`_c_parallel` / `_c_pbranch`) has always accepted the
        # whole parallel node, while the writer emitted **none** of it -- a
        # parallel node round-tripped to the hollow shell `(parallel :id p)`.
        # Branches, the join set, the conditional flag and the successor all
        # vanished and the recompiled graph was no longer connected. Note for
        # greppers: the `for b in n.get("branches", [])` in the `switch` arm
        # above is *switch*'s; this arm is the only place `pbranch` is written.
        #
        # Keyword order: every `:kw value` is emitted before the `(pbranch …)`
        # positionals so that `_split_args` (flat scan) reads them as keywords;
        # `-> next` stays last, exactly as in the other arms.
        if n.get("mode") is not None:
            parts.append(f":mode {n['mode']}")
        if n.get("joinBranches") is not None:
            # The reader accepts a single name or a list; the list spelling is
            # used uniformly so it round-trips for any cardinality (including
            # the empty join set, `:join-branches ()`).
            parts.append(f":join-branches {_expr_to_src(list(n['joinBranches']))}")
        if n.get("isConditional"):
            # Truthy-only, like the reader's `if _opt(...): spec[...] = True`:
            # `False` is not an IR value the reader can produce, and emitting
            # `:is-conditional false` would still set the flag to True.
            parts.append(":is-conditional true")
        for b in n.get("branches", []):
            bp = ["pbranch", _expr_to_src(b.get("name")),
                  f":flow ({_flow_inner_to_src(b.get('flow'))})"]
            if b.get("input") is not None:
                bp.append(f":input {_expr_to_src(b['input'])}")
            if b.get("condition") is not None:
                bp.append(f":when {_cond_to_src(b['condition'])}")
            parts.append("(" + " ".join(str(x) for x in bp) + ")")
        if n.get("next") is not None:
            parts.append(f"-> {n['next']}")
    elif t == "http":
        parts.append(f":method {n.get('method')}")
        parts.append(f":url {_expr_to_src(n.get('url'))}")
        if n.get("headers") is not None:
            parts.append(f":headers {_expr_to_src(n['headers'])}")
        if n.get("body") is not None:
            parts.append(f":body {_expr_to_src(n['body'])}")
        # #150: `_c_http` reads `:input` into `HTTP.input`; the writer dropped it.
        if n.get("input") is not None:
            parts.append(f":input {_expr_to_src(n['input'])}")
        # #200：访问策略（SSRF 防护面）此前写侧一个字都不发。读侧补齐后这里
        # 对称发射；全部排在 `-> next` 之前（关键字列表在此终止）。
        allowed = _dual(n, "allowedHosts", "allowed_hosts")
        if allowed is not None:
            parts.append(f":allowed-hosts {_host_list_to_src(allowed)}")
        denied = _dual(n, "deniedHosts", "denied_hosts")
        if denied is not None:
            parts.append(f":denied-hosts {_host_list_to_src(denied)}")
        block = _dual(n, "blockPrivateNetworks", "block_private_networks")
        if block is not None:
            if not isinstance(block, bool):
                raise SexprEmitError(
                    f"节点 {n.get('id')!r} 的 blockPrivateNetworks 不是 bool"
                    f"（得到 {block!r}），无法发射成可忠实回读的 :block-private-networks"
                )
            parts.append(
                f":block-private-networks {'true' if block else 'false'}")
        req_timeout = _dual(n, "requestTimeout", "request_timeout")
        if req_timeout is not None:
            parts.append(f":request-timeout {_expr_to_src(req_timeout)}")
        redirects = _dual(n, "maxRedirects", "max_redirects")
        if redirects is not None:
            parts.append(f":max-redirects {redirects}")
        if n.get("next") is not None:
            parts.append(f"-> {n['next']}")
    elif t == "code":
        parts.append(f":lang {n.get('language')}")
        parts.append(f":code {_expr_to_src(n.get('code'))}")
        # #150: `_c_code` reads `:input` into `CodeNode.input`; dropped before.
        if n.get("input") is not None:
            parts.append(f":input {_expr_to_src(n['input'])}")
        # #220-L3：写侧此前一个字都不发 `sandbox_backend` ⇒ sexpr 往返把
        # `subprocess`/`restricted`/`unsafe` 静默洗回进程级默认（默认 `docker`），
        # 隔离档位被无声降级。值只能是字面量字符串（执行期不做表达式求值），
        # 恒走 `_string_to_src` 加引号：`true`/`1` 裸写会被读成 bool/int。
        # 不可忠实表达的值 fail-closed，绝不静默丢。
        backend = _dual(n, "sandboxBackend", "sandbox_backend")
        if backend is not None:
            if not isinstance(backend, str) or not backend:
                raise SexprEmitError(
                    f"节点 {n.get('id')!r} 的 sandbox_backend 必须是非空字符串"
                    f"（得到 {backend!r}），无法发射成可忠实回读的 :sandbox-backend"
                )
            parts.append(f":sandbox-backend {_string_to_src(backend)}")
        if n.get("next") is not None:
            parts.append(f"-> {n['next']}")
    elif t == "mock":
        # #146：`mock` 不在节点表里，base 的 `_node_to_src` 会穿过所有分支、
        # 只发出空壳 `(mock :id m)`：`value` 与 `next` 静默丢失，再解析直接
        # 抛 "未知节点类型 'mock'"（不是静默丢字段，而是硬失败）。
        #
        # 关键字顺序：所有 `:kw value` 排在 `-> next` 之前，沿用 `7d0add9` 给
        # event `:filter` 立下的形状（也是 `_common_kw_to_src` 的发射形状）。
        #
        # 注意别把这条读成"读侧要求"：`_split_args` 是**平扫**，箭头之后的平铺
        # `:kw value` 照样被读成关键字（实测 `(mock :id m -> e :value 42)` 与
        # `(mock :id m -> e :value (dict :a 1))` 都能读回）。退化成位置参数的是
        # **不在关键字槽位上的**嵌套 list form（#136 的尾随 `(on-error …)`）。
        # 这里仍按"关键字先于后继"发射：形状稳定，且不依赖读侧平扫的实现细节。
        #
        # `value = None` 与"字段缺席"语义等价（`Mock.value` 默认 None），故按
        # `is not None` 判断，与 `_flow_level_fields_to_src` 的"None 一律不发"一致。
        if n.get("value") is not None:
            parts.append(f":value {_expr_to_src(n['value'])}")
        # `output` 继承自 `Node`：dryrun 把被 pin 的节点换成 mock 时保留原节点的
        # `output`（见 plaita-console/backend/services/dryrun.py），漏发即往返丢字段。
        if n.get("output") is not None:
            parts.append(f":output {_expr_to_src(n['output'])}")
        if n.get("next") is not None:
            parts.append(f"-> {n['next']}")
    elif t == "event":
        parts.append(f":type {_expr_to_src(n.get('eventType', n.get('event_type')))}")
        # #127（同根第二处）：`eventFilter` 只在解析侧存在过——`_c_event` 读
        # `:filter` / `:event-filter` / `:eventFilter` 三个别名并写入 `eventFilter`，
        # 而发射侧原本一个字都不写 ⇒ 往返**静默丢过滤条件**。
        # 后果比丢 next 更隐蔽：事件仍会触发，只是**不再按条件过滤**。
        #
        # ⚠ 顺序要紧（2026-10-13 由交付方在 #136 核查中指出，我复现确认）：
        # `-> next` **终止**关键字列表，放在它之后的 `:filter` 会退化成**位置参数**。
        # 我最初的 #127 修复把 filter 放在 next 之后 ⇒ 字符串 filter 侥幸幸存
        # （`$INPUT.x`），但 **dict filter 在 `Flow.model_validate` 后变成 None**
        # （实测：发射出 `… -> e :filter (dict a 1)`，校验后 eventFilter 消失）。
        # 因此**所有关键字必须排在 `-> next` 之前**。
        ev_filter = n.get("eventFilter", n.get("event_filter"))
        if ev_filter is not None:
            parts.append(f":filter {_expr_to_src(ev_filter)}")
        # #200：订阅超时同样只在节点声明里存在过——读写两侧都没有表达面，
        # 往返即丢 ⇒ 本应超时唤醒的挂起退化成无限等待。与 :filter 同理排在
        # `-> next` 之前。
        sub_timeout = _dual(n, "subscriptionTimeout", "subscription_timeout")
        if sub_timeout is not None:
            parts.append(f":subscription-timeout {_expr_to_src(sub_timeout)}")
        # `-> next` 必须**最后**发（它终止关键字列表）
        if n.get("next") is not None:
            parts.append(f"-> {n['next']}")
    return "(" + " ".join(str(x) for x in parts) + ")"


# ---------------------------------------------------------------------------
# flow 级字段：读得到就必须发得出（#147）
# ---------------------------------------------------------------------------
#
# 读侧 ``_compile_flow`` / ``_compile_childflow`` 接受的 flow 级关键字，
# 到写侧规范拼写的映射（列表顺序即发射顺序）。这两张表是**写侧唯一真源**：
# ``flow_to_sexpr`` / ``_flow_inner_to_src`` 都经由 ``_flow_level_fields_to_src``
# 消费它们，``tests/unit/test_sexpr.py`` 的 ``TestFlowLevelFieldSymmetry`` 再用
# AST 反向核对读侧 ``_opt(kw, …)`` 的拼写集合，防止将来读侧单方面扩面而写侧
# 静默漏发（#147 的病根，也正是 #136 在节点层的同一形状）。

#: 顶层 ``(flow …)`` 的 flow 级字段。``flow_id`` 不在此表内——它是位置参数。
_FLOW_LEVEL_FIELDS: Tuple[Tuple[str, str], ...] = (
    ("inputType", "input-type"),
    ("outputType", "output-type"),
    ("desc", "desc"),
    ("version", "version"),
    ("author", "author"),
    ("timeout", "timeout"),
    ("globalContext", "global-context"),
    ("metadata", "metadata"),
)

#: 内联子流程 ``(childflow …)`` 的 flow 级字段。``flow-id`` 是子流程自身的 id
#: （``:id`` 已被节点占用，#149），只在子流程层出现。
_CHILD_FLOW_LEVEL_FIELDS: Tuple[Tuple[str, str], ...] = (
    ("flow_id", "flow-id"),
    ("inputType", "input-type"),
    ("outputType", "output-type"),
    ("desc", "desc"),
)

#: 读侧按 ``str`` 解释、发射时必须**恒加引号**的字段。
#:
#: ``_expr_to_src`` 的裸原子约定对字符串有损：``"1.2"`` 会被重读成 ``float``、
#: ``"true"`` 成 ``bool``、``"nil"`` 成 ``None``。#149 已为标识符 ``:flow-id``
#: 定下"一律走 ``_string_to_src``"这条规则，``version``/``author`` 同属
#: ``Optional[str]`` 字段（且 ``:version "1.2"`` 正是 #147 报的取值），沿用同一
#: 规则，否则本单要求的"往返相等"对这两个字段不成立。
#: ``desc`` 不在表内：它自 base 起就由 ``_expr_to_src`` 发射且对常规文本正确，
#: 本单只补"子流程 desc 漏发"，不改动 root desc 的既有字节；统一引号约定
#: （数字样文本 ``"1.2"`` 的 desc 同样会失真）属单独变更。
_QUOTED_FLOW_FIELDS = frozenset({"flow_id", "version", "author"})


def _type_to_src(t: Any) -> Any:
    """``{"dataType": "object"}`` → ``object``：读侧 ``_type_spec`` 的逆。

    保持 base 的既有拼写（裸 ``dataType``，非 dict 原样透出）；dict 里
    ``dataType`` 之外的键读侧不表达，故不发射。
    """
    return t.get("dataType", "object") if isinstance(t, dict) else t


def _flow_field_to_src(ir_key: str, value: Any) -> str:
    if ir_key in ("inputType", "outputType"):
        return _type_to_src(value)
    if ir_key in _QUOTED_FLOW_FIELDS:
        return _string_to_src(str(value))
    return _expr_to_src(value)


def _flow_level_fields_to_src(data: Dict[str, Any], fields: Any) -> List[str]:
    """把 ``fields`` 表里"IR 里存在且非 None"的字段发射成 ``:key value`` 片段。

    ``None`` 一律跳过：缺席字段不得长出 ``:field nil``，也不得凭空造值——这是
    ``_opt`` 读侧的精确逆（读侧 ``is not None`` 才写入 IR）。
    """
    return [
        f":{sexpr_key} {_flow_field_to_src(ir_key, data[ir_key])}"
        for ir_key, sexpr_key in fields
        if data.get(ir_key) is not None
    ]


def _flow_inner_to_src(d: Dict[str, Any]) -> str:
    parts: List[str] = ["childflow"]
    # #147: every flow-level field `_compile_childflow` reads (desc / outputType /
    # inputType / flow-id) is emitted, in the same order the reader accepts them.
    # Absent fields emit nothing, so a child carrying none of them is byte-identical
    # to base (the #149 "no-id child" regression still holds).
    parts.extend(_flow_level_fields_to_src(d, _CHILD_FLOW_LEVEL_FIELDS))
    for n in d.get("nodes", []):
        parts.append(_node_to_src(n))
    return " ".join(parts)


def flow_to_sexpr(data: Dict[str, Any]) -> str:
    """把 Flow IR dict 反编译成 S-expr 源码（可逆，用于审查/互转）。"""
    parts: List[str] = ["flow", str(data.get("flow_id") or "flow")]
    # #147: previously only `inputType` (and, via a truthiness check, `desc`) were
    # emitted; `outputType`/`version`/`author`/`timeout`/`globalContext`/`metadata`
    # were read into the IR and then silently dropped on a round-trip.
    parts.extend(_flow_level_fields_to_src(data, _FLOW_LEVEL_FIELDS))
    for n in data.get("nodes", []):
        parts.append(_node_to_src(n))
    return "(" + " ".join(parts) + ")"
