"""plaita.core.expression_parser — unified expression parser/evaluator.

This module replaces the historical dual-track expression engine in
``plaita.io`` (regex for variable paths / interpolation + pyparsing for
function calls) with a **single** pyparsing grammar that is built once per
prefix and cached.

The grammar covers everything the old engine supported:

* literals   — number / boolean / quoted string
* variables  — ``$INPUT``, ``$INPUT.x``, ``$INPUT[0]``, ``$INPUT[-1].y``,
               ``$INPUT.names[0]``, ``$INPUT.users.0``
* functions  — ``$F.add(1, 2)``, ``$F.add($F.mul(2, 3), $INPUT.x)`` (nested,
               variadic, trailing-comma default)
* templates  — ``{% $F.add(3, $INPUT) %}`` interpolation inside otherwise
               literal text (multi-line, multiple matches per string)

Evaluation semantics are preserved bit-for-bit against the battle-tested
``plaita.io.evaluate`` (see ``tests/unit/test_expression_golden.py``):

* root variable lookup raises ``KeyError`` for a missing context key
* intermediate field accesses recurse through ``evaluate`` with the **parent
  object** as the context (so nested expression strings resolve against the
  parent, not the root)
* index segments (``[n]`` / ``.n``) index directly without recursion
* unknown functions fall back to the ``"undefined"`` sentinel
* ``{% ... %}`` only fires when the inner expression starts with the prefix

Thread-safety: the grammar is built once and shared; per-call context is
carried on a thread-local call-frame stack, so concurrent ``evaluate`` calls
do not clobber each other.
"""

from __future__ import annotations

import threading
from typing import Any, Callable, Dict, Optional

import pyparsing as pp

from plaita.core.expression import (
    ExpressionRegistry,
    get_default_expression_registry,
)
from plaita.logger import logger


# ---------------------------------------------------------------------------
# Sentinels & helpers
# ---------------------------------------------------------------------------

_UNDEFINED: Callable[..., Any] = lambda *args, **kwargs: "undefined"  # noqa: E731

_SPECIAL_ROOTS = ("INPUT", "NODE", "PARENT", "GLOBAL", "ENV")


def _lookup_function(registry: Optional[Any], func_name: str) -> Optional[Callable]:
    """Resolve a function callable by name from *registry* (or the default).

    Returns ``None`` when the function is not registered.  Callers should
    fall back to the ``"undefined"`` sentinel to preserve historical behavior
    (scoped registries deliberately return ``"undefined"`` for functions they
    don't expose — see ``tests/unit/test_expression.py``).
    """
    if registry is None:
        return get_default_expression_registry().get_callable(func_name)
    if isinstance(registry, ExpressionRegistry):
        return registry.get_callable(func_name)
    # Backward-compatible dict-like proxy
    return registry.get(func_name)


def _get_attr(obj: Any, path: str) -> Any:
    """Read *path* off *obj* — mirrors the non-bracket branch of the old
    ``plaita.io.get_attr``.

    Order matters: dict-like objects (``dict`` or anything exposing both
    ``__getitem__`` and ``get``) are read via ``.get(path)`` so storage-key
    mappings such as a live ``CheckpointState`` resolve correctly (its
    ``$INPUT`` is a storage key, not a Python attribute). Only objects that
    are neither dict-like fall back to ``getattr`` — this covers plain
    attribute access on user data classes / Pydantic models passed as input.
    """
    if isinstance(obj, dict):
        return obj.get(path, None)
    if hasattr(obj, "__getitem__") and hasattr(obj, "get"):
        return obj.get(path, None)
    if hasattr(obj, "__dict__"):
        return getattr(obj, path, None)
    return None


# ---------------------------------------------------------------------------
# Per-call context — thread-local call-frame stack
# ---------------------------------------------------------------------------

_frame_local = threading.local()


def _push_frame(context: Dict[str, Any], registry: Optional[Any], prefix: str) -> None:
    stack = getattr(_frame_local, "stack", None)
    if stack is None:
        stack = []
        _frame_local.stack = stack
    stack.append((context, registry, prefix))


def _pop_frame() -> None:
    _frame_local.stack.pop()


def _current_frame():
    return _frame_local.stack[-1]


# ---------------------------------------------------------------------------
# ExpressionParser
# ---------------------------------------------------------------------------


class ExpressionParser:
    """Single-grammar, cached expression parser & evaluator.

    One instance per *prefix* is cached and reused — the pyparsing grammar
    (including the recursive ``function_call`` rule, which the old engine
    rebuilt on every call) is constructed exactly once.
    """

    _instances: Dict[str, "ExpressionParser"] = {}
    _instances_lock = threading.Lock()

    def __init__(self, prefix: str = "$") -> None:
        self.prefix = prefix
        self._build_grammar()

    # --- construction ----------------------------------------------------

    @classmethod
    def for_prefix(cls, prefix: str = "$") -> "ExpressionParser":
        with cls._instances_lock:
            inst = cls._instances.get(prefix)
            if inst is None:
                inst = cls(prefix)
                cls._instances[prefix] = inst
        return inst

    def _build_grammar(self) -> None:
        prefix = self.prefix

        # --- literals ----------------------------------------------------
        number = pp.pyparsing_common.number
        boolean = (
            pp.Keyword("True") | pp.Keyword("False")
            | pp.Keyword("true") | pp.Keyword("false")
        )
        boolean.set_parse_action(lambda s, l, t: self._eval_boolean(t))
        string = pp.QuotedString('"') | pp.QuotedString("'")
        constant = boolean | string | number

        # --- identifiers / integers --------------------------------------
        # ``identifier`` is strict (alpha-first) for function names, matching
        # the old ``[a-zA-Z_][a-zA-Z0-9_]*`` regex.  ``name_token`` is the
        # permissive form used for variable roots and field names: the old
        # engine split on "." only, so a root/field could contain ``-`` and
        # even a leading ``$`` (e.g. ``$PARENT.$INPUT.name`` — the second
        # segment is the literal key "$INPUT" on the parent context).
        identifier = pp.Word(pp.alphas, pp.alphanums + "_")
        name_token = pp.Word(pp.alphanums + "_-$")
        pos_int = pp.Word(pp.nums).set_parse_action(lambda s, l, t: int(t[0]))
        signed_int = pp.Combine(pp.Optional("-") + pp.Word(pp.nums))
        signed_int.set_parse_action(lambda s, l, t: int(t[0]))

        # --- variable path segments --------------------------------------
        # dot_int must be tried before dot_field so ``.0`` is an index, not a
        # field named "0" (matches ``str.isdigit`` behaviour of the old walk).
        index_seg = pp.Suppress("[") + signed_int + pp.Suppress("]")
        index_seg.set_parse_action(lambda s, l, t: f"index:{t[0]}")
        dot_int = pp.Suppress(".") + pos_int
        dot_int.set_parse_action(lambda s, l, t: f"index:{t[0]}")
        dot_field = pp.Suppress(".") + name_token
        dot_field.set_parse_action(lambda s, l, t: f"field:{t[0]}")

        segment = index_seg | dot_int | dot_field
        # Root key is any ``$<name>`` — the old engine resolved the first
        # path segment straight from the context dict, so arbitrary keys
        # (not just the five special prefixes, and including ``-`` / ``$``)
        # must work.  ``name_token`` is Optional so a bare ``$`` root (e.g.
        # ``$.not_exist``) still parses and raises KeyError at lookup time,
        # matching the old walk's ``context[paths[0]]`` behaviour.
        root = pp.Combine(pp.Literal(prefix) + pp.Optional(name_token))

        # Forward declaration for the recursive function-call rule
        function_call = pp.Forward()

        # variable = root + zero-or-more segments
        variable = root + pp.Group(pp.ZeroOrMore(segment))
        # Use full 3-arg signature (s, loc, toks) for all parse actions to
        # bypass pyparsing's _trim_arity arity-discovery mechanism. _trim_arity
        # stores discovered arity in a closure-level nonlocal variable that is
        # NOT thread-safe; concurrent evaluate() calls from PARALLEL branches
        # can corrupt the shared state, causing subsequent calls to be invoked
        # with the wrong number of arguments. Fixed-3-arg wrappers skip the
        # discovery loop entirely and are always called with (s, loc, toks).
        variable.set_parse_action(lambda s, l, t: self._eval_variable(t))

        # ``expr`` is the union of literals, variables and function calls,
        # used for function arguments and interpolation bodies. function_call
        # must be tried *before* variable, otherwise ``$F.mul(...)`` would be
        # partially consumed as the variable ``$F.mul``.
        expr = constant | function_call | variable

        # --- function call ----------------------------------------------
        func_head = pp.Combine(pp.Literal(f"{prefix}F.") + identifier + pp.Literal("("))
        arg_list = pp.Optional(pp.DelimitedList(expr) + pp.Optional(","))
        function_call <<= (
            func_head + pp.Group(arg_list) + pp.Suppress(")")
        )
        function_call.set_parse_action(lambda s, l, t: self._eval_function_call(t))

        # Full-expression grammar (parseAll=True target)
        self._prefix_expr = function_call | variable

        # --- interpolation / template -----------------------------------
        # ``scanString`` yields (tokens, start, end) for each {% ... %} match;
        # ``_eval_template`` stitches literal segments + str(value) back
        # together, mirroring the old ``re.sub(lambda m: str(evaluate(...)))``.
        interpolation = pp.Suppress("{%") + expr + pp.Suppress("%}")
        interpolation.set_parse_action(lambda s, l, t: [t[0]])
        self._interpolation = interpolation

    # --- parse actions ---------------------------------------------------
    #
    # Every parse action wraps its return value in a single-element list.
    # pyparsing treats a ``None`` return as "leave tokens unchanged" and a
    # bare list return as "these are the new tokens" (flattening one level),
    # so wrapping guarantees ``parsed[0]`` is always the evaluated value —
    # including when that value is ``None`` or a list.

    @staticmethod
    def _eval_boolean(tokens):
        raw = tokens[0]
        return [raw in ("True", "true")]

    def _eval_variable(self, tokens) -> Any:
        context, _registry, prefix = _current_frame()
        root_key = tokens[0]
        # ``$FLOW`` 是 ``$FLOW_ID`` 的文档别名——历史上 ``$FLOW`` 根不存在，
        # 直接 KeyError 崩，与其他前缀缺省返回 None 的口径不一致。
        if root_key == f"{prefix}FLOW":
            root_key = f"{prefix}FLOW_ID"
        # Root lookup: KeyError preserved for missing keys (matches old engine)
        obj = context[root_key]
        segments = tokens[1]  # ParseResults of "index:n" / "field:name"
        for seg in segments:
            kind, _, raw = seg.partition(":")
            if kind == "index":
                obj = obj[int(raw)]
            else:  # field
                name = f"{prefix}{raw}" if raw in _SPECIAL_ROOTS else raw
                if root_key == f"{prefix}INPUT" and isinstance(obj, dict) and name not in obj:
                    # 静默 None 是 INPUT 缺键的历史语义；debug 留痕便于排查拼写错误
                    logger.debug(
                        "expression references missing input key %r; evaluating to None", raw,
                    )
                attr = _get_attr(obj, name)
                # 仅当字符串属性值本身是表达式（$ 前缀变量 / {% %} 模板）时才递归
                # 求值——这是"嵌套表达式字符串"的历史语义。任意普通字符串（可能含
                # [tag]、引号、换行等元字符）不再二次解析，否则节点输出一旦被下游
                # $NODE 路径引用就会因内容触发误解析（如 "[promo] ..." 被当列表）。
                if (isinstance(attr, str)
                        and (attr.startswith(prefix) or "{%" in attr)):
                    obj = self.evaluate(attr, obj, _registry)
                else:
                    obj = attr
        return [obj]

    def _eval_function_call(self, tokens) -> Any:
        _context, registry, _prefix = _current_frame()
        head = tokens[0]            # e.g. "$F.add("
        func_name = head.split(".")[1].split("(")[0]
        args = list(tokens[1])      # already-evaluated arg values
        logger.debug("parse_function: func_name=%s, args=%s", func_name, args)
        func = _lookup_function(registry, func_name)
        if func is None:
            # 不静默：函数未注册时记一条 warning，让拼写错误 / 沙箱漏注册可被
            # 日志捕捉。返回值仍是 "undefined" 以兼容 scoped registry 语义
            # （scoped registry 故意对未暴露函数返回 "undefined"）。
            logger.warning(
                "expression function %r not registered (registry=%r); returning 'undefined'",
                func_name, registry,
            )
            func = _UNDEFINED
        return [func(*args)]

    # --- entry points ----------------------------------------------------

    def evaluate(self, value: Any, context: Dict[str, Any],
                 registry: Optional[Any] = None) -> Any:
        _push_frame(context, registry, self.prefix)
        try:
            return self._eval(value)
        finally:
            _pop_frame()

    def _eval(self, value: Any) -> Any:
        if not isinstance(value, str):
            return self._eval_non_string(value)
        if not value.startswith(self.prefix):
            return self._eval_template(value)
        return self._eval_prefix(value)

    def _eval_non_string(self, value: Any) -> Any:
        if isinstance(value, list):
            return [self._eval(item) for item in value]
        if isinstance(value, dict):
            return {key: self._eval(val) for key, val in value.items()}
        return value

    def _eval_prefix(self, value: str) -> Any:
        # Parse the whole prefix string as a function call or variable path.
        # On success the parse action returns the evaluated value.  A
        # ParseException propagates: the old engine's path walk had no
        # "return unchanged" path for prefix strings — an unresolvable key
        # raised KeyError, which the runtime surfaces as a node error.  Letting
        # the parse failure propagate preserves that "invalid prefix
        # expression -> node error" behaviour.  (``parse_function`` wraps
        # calls that should instead return the raw string on parse failure.)
        parsed = self._prefix_expr.parse_string(value, parse_all=True)
        return parsed[0]

    def _eval_template(self, value: str) -> Any:
        # Scan for {% ... %} matches and stitch the string back together,
        # substituting str(evaluated_value) for each match — identical to the
        # old ``re.sub`` behaviour. When no match is present the string is
        # returned unchanged.
        # 快速预筛（2026-09 性能评审 P1）：不含模板标记的纯文本直接返回，
        # 省掉 scanString 全串扫描——纯文本路径实测 43.5µs/次。
        if "{%" not in value:
            return value
        out: list = []
        last = 0
        matched = False
        for tokens, start, end in self._interpolation.scan_string(value):
            matched = True
            out.append(value[last:start])
            out.append(str(tokens[0]))
            last = end
        if not matched:
            return value
        out.append(value[last:])
        return "".join(out)

    # --- backward-compat shim -------------------------------------------

    def parse_function(self, expression: str, context: Dict[str, Any],
                       registry: Optional[Any] = None) -> Any:
        """Evaluate a (possibly function) expression.

        Mirrors the old ``plaita.io.parse_function`` contract:

        * a string without ``{prefix}F.`` is returned unchanged;
        * a string that contains ``{prefix}F.`` but fails to parse as a
          function call is returned unchanged (preserving the old
          ``except ParseException: return expression`` behaviour);
        * otherwise the evaluated value is returned.
        """
        if f"{self.prefix}F." not in expression:
            return expression
        try:
            return self.evaluate(expression, context, registry)
        except pp.exceptions.ParseException:
            return expression


# Backward-compat: old test imports ``_parser_components_cache`` from
# ``plaita.io`` and expects it keyed by prefix after a call. The cache is
# populated lazily by ``plaita.io.parse_function``.
_parser_components_cache: Dict[str, ExpressionParser] = {}
