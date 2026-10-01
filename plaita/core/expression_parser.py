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

Compile-to-thunk caching (2026-10 BFF hot-path review): parse actions no
longer evaluate against a thread-local frame inline; they build **context-
parameterized thunks** (``(context, registry) -> value`` closures).  The
grammar itself is deterministic per ``(parser, string)``, so successful
compiles are memoized in a per-instance LRU (``_compile_cache``).  Thunks
capture only *structure* — root keys, normalized segments, function names,
argument thunks — never context values, so the same compiled thunk is
correct for every context it is later called with (guards against cache
poisoning are in ``tests/unit/test_expression_cache.py``).  Function
resolution stays at call time so per-call scoped registries and runtime
``registry.register()`` keep working unchanged.  Compile failures
(ParseException) are not cached and propagate exactly as before, keeping
the ``parse_function`` "return the raw string on parse failure" contract.

Thread-safety: the grammar is built once and shared; compiled thunks are
pure functions of their frame, so concurrent ``evaluate`` calls (parallel
branches, thread pools) are safe without any shared mutable eval state.
The historical thread-local frame stack (``_push_frame`` / ``_pop_frame``)
is kept only for backward compatibility of the private helpers — the hot
path passes the frame explicitly.
"""

from __future__ import annotations

import threading
from collections import OrderedDict
from typing import Any, Callable, Dict, List, Optional, Tuple

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

# A call frame: (context, registry, prefix). Thunks receive it explicitly —
# no thread-local state on the evaluation hot path.
Frame = Tuple[Any, Optional[Any], str]
Thunk = Callable[[Frame], Any]

# 可疑字面量特征（2026-09 LLM 作者模拟 P0-1）：Jinja 双花括号 / Python f-string /
# await 调用 / 裸函数调用——这些字符串几乎从不是合法的 plaita 表达式意图
_SUSPICIOUS_LITERAL = __import__("re").compile(
    r"\{\{|\}\}|\bf['\"]|\bawait\b|[A-Za-z_]\w*\([^)]*\$")
_warned_literals: "set[str]" = set()


def _warn_suspicious_literal(value: str) -> None:
    if value in _warned_literals:
        return
    if len(_warned_literals) > 512:
        _warned_literals.clear()
    _warned_literals.add(value)
    logger.warning(
        "expression %r looks like templating/Python code from another language and was "
        "returned AS-IS (no evaluation happened). plaita expressions use $INPUT.x / "
        "$NODE.id / $F.fn(...) and {%% ... %%} for interpolation — rewrite the string.",
        value if len(value) < 120 else value[:117] + "...",
    )

_SPECIAL_ROOTS = ("INPUT", "NODE", "PARENT", "GLOBAL", "ENV")


def _const_thunk(value: Any) -> Thunk:
    """Compile a literal token into a constant thunk."""
    def thunk(frame: Frame) -> Any:
        return value
    return thunk


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
# Per-call context — legacy thread-local frame stack
# ---------------------------------------------------------------------------
# The evaluation hot path no longer uses these: thunks receive the frame
# explicitly.  Kept as private helpers because they were part of this
# module's historical surface; nothing in plaita itself calls them anymore.

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
    """Single-grammar, compile-once expression parser & evaluator.

    One instance per *prefix* is cached and reused — the pyparsing grammar
    (including the recursive ``function_call`` rule, which the old engine
    rebuilt on every call) is constructed exactly once.

    On top of the grammar cache, each expression/template *string* is
    compiled once into a thunk tree and memoized in a per-instance LRU
    (``_MAX_CACHE_ENTRIES`` entries).  Strings longer than
    ``_MAX_CACHED_LEN`` are evaluated uncached every call — they are data
    (long prompt bodies with an interpolation slot), not expressions, and
    caching them would pin large texts in memory for the process lifetime.
    """

    _instances: Dict[str, "ExpressionParser"] = {}
    _instances_lock = threading.Lock()

    _MAX_CACHE_ENTRIES = 8192
    _MAX_CACHED_LEN = 4096

    def __init__(self, prefix: str = "$") -> None:
        self.prefix = prefix
        self._compile_cache: "OrderedDict[str, Any]" = OrderedDict()
        self._cache_lock = threading.Lock()
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
        # null/None 字面量（2026-09-30）：codeflow 编译器把 None 参数渲染成
        # "null"（_render_arg），文法里没有它时整句函数调用解析失败、退化成
        # 变量路径解析，报 "$F not found" 误导排查方向。
        boolean = (
            pp.Keyword("True") | pp.Keyword("False")
            | pp.Keyword("true") | pp.Keyword("false")
            | pp.Keyword("null") | pp.Keyword("None")
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
        #
        # Parse actions here COMPILE: they return thunks and never evaluate —
        # there is no context available at parse time in this design.
        variable.set_parse_action(lambda s, l, t: self._compile_variable(t))

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
        function_call.set_parse_action(lambda s, l, t: self._compile_function_call(t))

        # Full-expression grammar (parseAll=True target)
        self._prefix_expr = function_call | variable

        # --- interpolation / template -----------------------------------
        # ``scanString`` yields (tokens, start, end) for each {% ... %} match;
        # compilation records (start, end, thunk) offsets so the literal text
        # is sliced at call time instead of being copied into the cache.
        interpolation = pp.Suppress("{%") + expr + pp.Suppress("%}")
        interpolation.set_parse_action(lambda s, l, t: [t[0]])
        self._interpolation = interpolation

    # --- compile-time helpers --------------------------------------------

    @staticmethod
    def _eval_boolean(tokens):
        raw = tokens[0]
        if raw in ("null", "None"):
            return [None]
        return [raw in ("True", "true")]

    def _compile_variable(self, tokens) -> Thunk:
        """Compile a variable path into a thunk.

        The thunk captures only structure: the (alias-rewritten) root key and
        pre-normalized segments as ``(kind, target, raw)`` tuples — ``raw`` is
        kept solely for the missing-INPUT-key debug log, matching the
        historical log wording.  All context/registry access happens at call
        time; nested expression strings stored as attribute values re-enter
        the public ``evaluate`` (cache lookup applies) with the *parent
        object* as context, exactly like the fused parse-and-eval engine.
        """
        parser = self
        root_key = tokens[0]
        # ``$FLOW`` 是 ``$FLOW_ID`` 的文档别名——历史上 ``$FLOW`` 根不存在，
        # 直接 KeyError 崩，与其他前缀缺省返回 None 的口径不一致。
        if root_key == f"{self.prefix}FLOW":
            root_key = f"{self.prefix}FLOW_ID"
        segments: List[Tuple[str, Any, str]] = []
        for seg in tokens[1]:
            kind, _, raw = seg.partition(":")
            if kind == "index":
                segments.append(("index", int(raw), raw))
            else:
                name = f"{self.prefix}{raw}" if raw in _SPECIAL_ROOTS else raw
                segments.append(("field", name, raw))

        def thunk(frame: Frame) -> Any:
            context, registry, prefix = frame
            # Root lookup: KeyError preserved for missing keys (matches old engine)
            try:
                obj = context[root_key]
            except KeyError:
                # 保留 KeyError 类型（语义：流程逻辑错误），但给出可用根清单——
                # 历史上裸 KeyError: '$node' 让 AI 作者无从自纠
                # （2026-09 LLM 作者模拟 P1-2）。
                raise KeyError(
                    f"{root_key!r} not found in expression context. "
                    f"Available roots (with {parser.prefix!r} prefix): "
                    "INPUT, NODE, GLOBAL, PARENT, ENV, FLOW_ID, FLOW(alias of FLOW_ID)"
                ) from None
            for kind, target, raw in segments:
                if kind == "index":
                    obj = obj[target]
                    continue
                if root_key == f"{prefix}INPUT" and isinstance(obj, dict) and target not in obj:
                    # 静默 None 是 INPUT 缺键的历史语义；debug 留痕便于排查拼写错误
                    logger.debug(
                        "expression references missing input key %r; evaluating to None", raw,
                    )
                attr = _get_attr(obj, target)
                # 仅当字符串属性值本身是表达式（$ 前缀变量 / {% %} 模板）时才递归
                # 求值——这是"嵌套表达式字符串"的历史语义。任意普通字符串（可能含
                # [tag]、引号、换行等元字符）不再二次解析，否则节点输出一旦被下游
                # $NODE 路径引用就会因内容触发误解析（如 "[promo] ..." 被当列表）。
                if (isinstance(attr, str)
                        and (attr.startswith(prefix) or "{%" in attr)):
                    obj = parser.evaluate(attr, obj, registry)
                else:
                    obj = attr
            return obj

        return thunk

    def _compile_function_call(self, tokens) -> Thunk:
        """Compile ``$F.name(arg, ...)`` into a thunk.

        Function *resolution* stays at call time (registry arrives per call —
        scoped registries must keep returning ``"undefined"`` for functions
        they don't expose, and the default-registry miss must keep raising
        the NameError with the difflib hint).  Arguments are thunks compiled
        bottom-up by pyparsing; constant tokens arrive as plain values and
        are wrapped here.
        """
        head = tokens[0]            # e.g. "$F.add("
        func_name = head.split(".")[1].split("(")[0]
        arg_thunks: List[Thunk] = [
            tok if callable(tok) else _const_thunk(tok) for tok in tokens[1]
        ]

        def thunk(frame: Frame) -> Any:
            _context, registry, _prefix = frame
            args = [arg(frame) for arg in arg_thunks]
            logger.debug("parse_function: func_name=%s, args=%s", func_name, args)
            func = _lookup_function(registry, func_name)
            if func is None:
                if registry is None:
                    # 默认注册表未命中 = 调用方（或 AI 作者）拼写错误——返回
                    # 'undefined' 字符串会让错误值静默流入下游（LLM 作者模拟 P0-3）。
                    import difflib

                    available = sorted(get_default_expression_registry().names())
                    close = difflib.get_close_matches(func_name, available, n=3, cutoff=0.6)
                    hint = f" Did you mean {close!r}?" if close else ""
                    raise NameError(
                        f"Unknown expression function {func_name!r} (default registry)."
                        f"{hint} Available: {available}"
                    )
                # scoped registry 故意对未暴露函数返回 "undefined"——保留契约
                logger.warning(
                    "expression function %r not registered (registry=%r); returning 'undefined'",
                    func_name, registry,
                )
                func = _UNDEFINED
            return func(*args)

        return thunk

    # --- compilation cache -------------------------------------------------

    def _cached_compile(self, value: str, compile_fn: Callable[[str], Any]) -> Any:
        """Return the memoized compilation of *value*, compiling on miss.

        Compile failures propagate and are NOT cached (a failed parse must
        keep failing identically on every call — ``parse_function`` relies on
        catching the ParseException).  Only strings up to ``_MAX_CACHED_LEN``
        are memoized; longer strings are data, not expressions.
        """
        cache = self._compile_cache
        compiled = cache.get(value)
        if compiled is not None:
            try:
                cache.move_to_end(value)
            except KeyError:  # pragma: no cover - evicted between get and move
                pass
            return compiled
        compiled = compile_fn(value)
        if len(value) <= self._MAX_CACHED_LEN:
            with self._cache_lock:
                cache[value] = compiled
                cache.move_to_end(value)
                while len(cache) > self._MAX_CACHE_ENTRIES:
                    cache.popitem(last=False)
        return compiled

    # --- entry points ----------------------------------------------------

    def evaluate(self, value: Any, context: Dict[str, Any],
                 registry: Optional[Any] = None) -> Any:
        return self._eval(value, context, registry)

    def _eval(self, value: Any, context: Dict[str, Any],
              registry: Optional[Any] = None) -> Any:
        if not isinstance(value, str):
            return self._eval_non_string(value, context, registry)
        if not value.startswith(self.prefix):
            return self._eval_template(value, context, registry)
        return self._eval_prefix(value, context, registry)

    def _eval_non_string(self, value: Any, context: Dict[str, Any],
                         registry: Optional[Any] = None) -> Any:
        if isinstance(value, list):
            return [self._eval(item, context, registry) for item in value]
        if isinstance(value, dict):
            return {key: self._eval(val, context, registry) for key, val in value.items()}
        return value

    def _eval_prefix(self, value: str, context: Dict[str, Any],
                     registry: Optional[Any] = None) -> Any:
        # Parse the whole prefix string as a function call or variable path,
        # then apply the compiled thunk.  A ParseException propagates: the old
        # engine's path walk had no "return unchanged" path for prefix
        # strings — an unresolvable key raised KeyError, which the runtime
        # surfaces as a node error.  Letting the parse failure propagate
        # preserves that "invalid prefix expression -> node error" behaviour.
        # (``parse_function`` wraps calls that should instead return the raw
        # string on parse failure.)
        thunk = self._cached_compile(value, self._compile_prefix)
        return thunk((context, registry, self.prefix))

    def _compile_prefix(self, value: str) -> Thunk:
        parsed = self._prefix_expr.parse_string(value, parse_all=True)
        return parsed[0]

    def _eval_template(self, value: str, context: Dict[str, Any],
                       registry: Optional[Any] = None) -> Any:
        # Scan for {% ... %} matches and stitch the string back together,
        # substituting str(evaluated_value) for each match — identical to the
        # old ``re.sub`` behaviour. When no match is present the string is
        # returned unchanged.
        # 快速预筛（2026-09 性能评审 P1）：不含模板标记的纯文本直接返回，
        # 省掉 scanString 全串扫描——纯文本路径实测 43.5µs/次。
        if "{%" not in value:
            # 可疑字面量告警（2026-09 LLM 作者模拟 P0-1）：{{name}}/f''/
            # await xxx()/fn(...) 是 LLM 从其他模板语言/Python 迁移来的
            # 高频写法——历史上原样返回字面量且零告警，AI 会把字面量当
            # 正确结果直接消费。同串只告警一次（LRU 去重）。
            if _SUSPICIOUS_LITERAL.search(value):
                _warn_suspicious_literal(value)
            return value
        segs = self._cached_compile(value, self._compile_template)
        if not segs:
            return value
        frame = (context, registry, self.prefix)
        out: list = []
        for start, end, seg_thunk in segs:
            if seg_thunk is None:  # literal gap between matches
                out.append(value[start:end])
            else:
                out.append(str(seg_thunk(frame)))
        return "".join(out)

    def _compile_template(self, value: str) -> list:
        """Compile a template string into ``[(start, end, thunk), ...]``.

        Literal segments are stored as offsets (sliced at call time) so the
        cache never pins copies of long template bodies.  An empty list means
        "no match" — the caller returns the string unchanged, mirroring the
        historical ``matched=False`` behaviour (e.g. unclosed ``"a {% b"``).
        """
        segs: list = []
        last = 0
        for tokens, start, end in self._interpolation.scan_string(value):
            if start > last:
                segs.append((last, start, None))
            inner = tokens[0]
            segs.append((start, end, inner if callable(inner) else _const_thunk(inner)))
            last = end
        if last < len(value):
            segs.append((last, len(value), None))
        return segs

    # --- backward-compat shim -------------------------------------------

    @staticmethod
    def get_registered_names() -> list[str]:
        return sorted(get_default_expression_registry().all_functions())

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
