"""Shared placeholders, constants, and compile context for @flow."""
from __future__ import annotations

import ast
import re
import warnings
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

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
# WHILE 只在 for-head 位置合法：`for x in WHILE(cond, id="w")` —— 条件循环
# （引擎 While 节点），体必须以 return 结束（返回值即下一轮 item）。别处使用
# 会被当成未注册自定义节点拒绝。
WHILE = _Placeholder("WHILE")
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
        if strategy not in ("abort", "continue", "continue_with", "continue-with"):
            # 连字符 "continue-with" 是 ErrorStrategy 的规范枚举值 (core 层两种拼写
            # 都收并归一化); DSL 层历史上只收下划线, 用户从 enum 取 .value 填进来会被拒。
            strategy = "continue_with" if strategy == "continue-with" else strategy
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
# WHILE 走 for-head 专属编译路径（条件循环，无 collection），不算 collection call
_WHILE_CALL_NAME = "WHILE"

# 已被专用占位符（HTTP/CODE/CHILD/...）或合成节点（start/end/if/assignment/switch/bool）
# 接手的 node_type 集合。自定义节点路径不走这些类型，避免 START(...)/END(...) 之类
# 被误当成自定义节点调用。
_BUILTIN_HANDLED_TYPES = {
    "http", "code", "event", "child", "reference", "parallel",
    "map", "filter", "find", "loop", "reduce",
    "start", "end", "if", "assignment", "switch", "bool",
}


def _is_upper_ident(name: str) -> bool:
    """全大写 ASCII 标识符（字母/数字/下划线，至少含一个字母）。

    自定义节点占位符的命名约定：``node_type`` 大写化（``llm`` → ``LLM``）。
    """
    return (
        name.isupper()
        and name.replace("_", "").isalnum()
        and any(c.isalpha() for c in name)
    )


def _default_known_node_types() -> set:
    """从默认 NodeRegistry 取已知 node_type 集合（懒加载，避免循环 import）。

    registry 不可用时显式失败，不再 ``except Exception: return set()``——
    静默空集会导致自定义节点误报「未注册」或漏检，随进程状态漂移。
    """
    from plaita.node import get_default_registry

    return set(get_default_registry().list_types())


def _custom_node_type(func: ast.expr, ctx: "_CompileCtx") -> Optional[str]:
    """若 ``func`` 是自定义节点占位调用，返回 node_type；否则 None。

    识别规则：``func`` 是 ``ast.Name``、名字全大写、``name.lower()`` 在 registry 里、
    且不在 ``_BUILTIN_HANDLED_TYPES``（内置专用类型走各自的占位符）。
    """
    if isinstance(func, ast.Name) and _is_upper_ident(func.id):
        nt = func.id.lower()
        if nt in _BUILTIN_HANDLED_TYPES:
            return None
        if nt in ctx.known_node_types:
            return nt
    return None


def _raise_if_unregistered_custom(func: ast.expr, ctx: "_CompileCtx") -> None:
    """大写占位名但未注册 → 报可读错误并列出可用类型，供 AI 自纠。"""
    if isinstance(func, ast.Name) and _is_upper_ident(func.id):
        nt = func.id.lower()
        if nt in _BUILTIN_HANDLED_TYPES or nt in ctx.known_node_types:
            return
        available = sorted(t for t in ctx.known_node_types if t not in _BUILTIN_HANDLED_TYPES)
        raise _CodeflowError(
            f"未注册的自定义节点 {func.id}(...)：node_type {nt!r} 不在 registry 中。"
            f"可用类型：{available or '（无）'}",
            func,
        )

# 常见 Python 写法在 @flow 表达式里不支持——给出可读的重写提示, 而不是抛
# 不可读的 ast.dump / 裸类型名。这些构造当前本就会编译失败, 加提示是纯 DX 提升。
_FOOTGUN_HINTS = {
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
        known_node_types: Optional[set] = None,
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
        # 已注册的 node_type 集合，用于识别自定义节点占位符（LLM/RETRIEVE/...）。
        # 默认从默认 NodeRegistry 取；调用方可覆盖（如隔离测试）。
        self.known_node_types: set = known_node_types if known_node_types is not None else _default_known_node_types()

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

    def semantic_id(self, slug: Optional[str]) -> str:
        """语义化 id：slug 可用则直接采用，冲突时依次尝试 ``<slug>_2.._5``，
        仍冲突或 slug 为空回退 ``auto_id()``（``_n{n}``）。

        用于 if/while/end 等无自然名字的节点，让 console 画布、dry-run、
        运行期报错里的 id 自带语义（如 ``score_ge_90``）。
        """
        if slug:
            if slug not in self._claimed:
                self._claimed.add(slug)
                return slug
            for k in range(2, 6):
                cand = f"{slug}_{k}"
                if cand not in self._claimed:
                    self._claimed.add(cand)
                    return cand
        return self.auto_id()

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


def _annotate_source(spec: Dict[str, Any], node: Optional[ast.AST]) -> Dict[str, Any]:
    """把 AST 节点的源码行号写进 IR spec，供运行期错误回标定位。

    仅 ``@flow`` 前端调用；合成节点（如自动 start）传 ``node=None`` 即跳过。
    """
    if node is not None:
        line = getattr(node, "lineno", None)
        if line is not None:
            spec["source_line"] = line
    return spec


# ---------------------------------------------------------------------------
# 语义化 id 与人类可读标签（console 画布 / dry-run / 报错信息可读性）
# ---------------------------------------------------------------------------

_SLUG_MAX_LEN = 24
_CMP_OP_SLUGS = {
    ast.Gt: "gt", ast.GtE: "ge", ast.Lt: "lt", ast.LtE: "le",
    ast.Eq: "eq", ast.NotEq: "ne", ast.In: "in", ast.NotIn: "not_in",
    ast.Is: "is", ast.IsNot: "is_not",
}


def _slug_tokens(node: Optional[ast.expr], out: List[str], depth: int = 0) -> None:
    """把表达式 AST 摘成语义 token（顺序即阅读顺序），供 ``_cond_slug`` 拼接。

    只取「稳定语义」的部分：字段/变量名、比较算子、and/or/not、字面量。
    深度与 token 数量设上限，保证纯函数有界。
    """
    if node is None or depth > 6 or len(out) >= 8:
        return
    if isinstance(node, ast.Compare):
        _slug_tokens(node.left, out, depth + 1)
        for op, comp in zip(node.ops, node.comparators):
            out.append(_CMP_OP_SLUGS.get(type(op), "cmp"))
            _slug_tokens(comp, out, depth + 1)
    elif isinstance(node, ast.BoolOp):
        op = "and" if isinstance(node.op, ast.And) else "or"
        for i, v in enumerate(node.values):
            if i:
                out.append(op)
            _slug_tokens(v, out, depth + 1)
    elif isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
        out.append("not")
        _slug_tokens(node.operand, out, depth + 1)
    elif isinstance(node, ast.Attribute):
        out.append(node.attr)
    elif isinstance(node, ast.Name):
        out.append(node.id)
    elif isinstance(node, ast.Call):
        fn = node.func
        fname = fn.attr if isinstance(fn, ast.Attribute) else (fn.id if isinstance(fn, ast.Name) else "")
        if fname:
            out.append(fname)
        for a in node.args:
            _slug_tokens(a, out, depth + 1)
    elif isinstance(node, ast.Subscript):
        _slug_tokens(node.value, out, depth + 1)
        if isinstance(node.slice, ast.Constant) and isinstance(node.slice.value, int):
            out.append(f"at{node.slice.value}")
    elif isinstance(node, ast.Constant):
        v = node.value
        if v is None:
            out.append("none")
        elif isinstance(v, bool):
            out.append("true" if v else "false")
        elif isinstance(v, (int, float)):
            out.append(re.sub(r"[^0-9]+", "_", str(v)).strip("_") or "num")
        elif isinstance(v, str):
            out.append(v)


def _cond_slug(node: Optional[ast.expr]) -> Optional[str]:
    """表达式 → 语义 slug（如 ``INPUT.score >= 90`` → ``score_ge_90``）。

    仅 ASCII 小写字母/数字/下划线；不可 slug（全中文/空表达式等）返回 None，
    调用方回退 ``_n{n}``。纯函数，供单测钉行为。
    """
    toks: List[str] = []
    _slug_tokens(node, toks)
    cleaned = []
    for t in toks:
        t = re.sub(r"[^0-9a-zA-Z]+", "_", t).strip("_").lower()
        if t:
            cleaned.append(t)
    slug = "_".join(cleaned)
    if not slug:
        return None
    return slug[:_SLUG_MAX_LEN].rstrip("_")


def _human_label(text: str, limit: int = 28) -> str:
    """压缩空白并截断成人类可读标签（超长以 … 结尾）。"""
    t = " ".join(text.split())
    return t if len(t) <= limit else t[: max(limit - 1, 1)] + "…"

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


class _ChildFlowMarker:
    """``@childflow`` 的产物：携带编译好的 IR，可被父流程引用。"""

    def __init__(self, ir: Dict[str, Any], func: Callable) -> None:
        self.__codeflow_ir__ = ir
        self._func = func

    # 允许被父流程编译器按 AST Name 引用——但 AST 阶段拿不到运行时对象，
    # 所以 CHILD/REFERENCE 需通过字面 ``flow=<name>`` 且 name 在父模块可解析。
    # 为支持该路径，父编译器在 _eval_childflow_arg 里会尝试从装饰器闭包捕获。

