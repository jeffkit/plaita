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
  parent, not the root) — but only when every context root the nested string
  references actually exists in that parent object (#104, plan A).  Otherwise
  the value is data text (LLM/agent output, file content, prompt echo) and is
  returned unchanged instead of being evaluated and silently rewritten.
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

import re
import threading
from collections import OrderedDict
from functools import lru_cache
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Tuple

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
_SUSPICIOUS_LITERAL = re.compile(
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


# 含 `{%` 但没有任何插值匹配成功的串（内层表达式不合法/未闭合）——历史上原样
# 返回且零告警，AI 作者会把未求值模板当正确结果消费（2026-10 评审修复包 A4）。
# 同串只告警一次（去重集合，与 _warned_literals 同口径）。
_warned_unmatched_templates: "set[str]" = set()


def _warn_unmatched_template(value: str) -> None:
    if value in _warned_unmatched_templates:
        return
    if len(_warned_unmatched_templates) > 512:
        _warned_unmatched_templates.clear()
    _warned_unmatched_templates.add(value)
    logger.warning(
        "expression %r contains '{%%' but no valid {%% ... %%} expression matched "
        "(inner expression is malformed or unclosed); it was returned AS-IS without "
        "evaluation — fix the inner expression (e.g. {%% $INPUT.x %%}).",
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
# Nested-expression root scan (plaita#104, plan A)
# ---------------------------------------------------------------------------
# A string attribute value used to be re-evaluated unconditionally with the
# *parent container* as context.  When the value is really data text (LLM/agent
# output, file content, prompt echo) that contains a ``{% ... %}`` fragment, the
# implicit recursion either raised a misleading ``KeyError`` (the referenced root
# exists at the flow root, not in the parent container) or silently rewrote the
# text (``"{% $F.eq(1, 1) %}"`` -> ``"True"``).
#
# Plan A narrows the recursion: scan the string for context roots and recurse
# only when *all* of them exist in the parent container.  The scan is a plain
# regex over the raw text — the grammar is not re-run (the recursion decision
# happens before evaluation, and a full parse would cost as much as evaluating).

# The documented context roots.  Anything else after the prefix — ``$F`` (the
# function namespace), ``$FOO`` or an arbitrary storage key such as
# ``$INPUT-x`` — is deliberately NOT treated as a root reference, so a nested
# string never gets misjudged because of an unrelated ``$`` token.
_ROOT_REF_NAMES: FrozenSet[str] = frozenset(
    {"INPUT", "NODE", "PARENT", "GLOBAL", "ENV", "FLOW_ID"}
)
# ``$FLOW`` is the documented alias of ``$FLOW_ID`` (see _compile_variable).
_ROOT_REF_ALIASES = {"FLOW": "FLOW_ID"}
# Quoted string literals are data, never root references: ``'$INPUT'`` inside
# ``$F.concat('$INPUT', $NODE.x)`` is a constant, not a context lookup.
_QUOTED_SPAN_RE = re.compile(r"'(?:\\.|[^'\\])*'|\"(?:\\.|[^\"\\])*\"")
# Same memory discipline as the compile LRUs: only short strings are memoized,
# long data text is scanned uncached so it cannot be pinned by this cache.
_MAX_SCAN_CACHE_LEN = 4096
_MISSING = object()


def _scan_root_refs_impl(prefix: str, text: str) -> FrozenSet[str]:
    """Return the resolved context roots (``{prefix}NAME``) *text* references."""
    if not text:
        return frozenset()
    # Strip quoted literals first so their content cannot be mistaken for a
    # reference (offsets are irrelevant afterwards — only matches matter).
    stripped = _QUOTED_SPAN_RE.sub(" ", text)
    # ``[A-Za-z0-9_$-]*`` munches maximally, matching the grammar's
    # ``name_token``: ``$INPUT-x`` is the root ``$INPUT-x`` (unknown -> no
    # reference), not ``$INPUT`` followed by literal text.
    token_re = re.compile(re.escape(prefix) + r"([A-Za-z0-9_$-]*)")
    found = set()
    for match in token_re.finditer(stripped):
        name = match.group(1)
        if not name:
            continue            # a bare ``$`` root — nothing scannable
        if match.start() > 0 and stripped[match.start() - 1] == ".":
            # Field-name position (``$PARENT.$INPUT.name``): the second segment
            # is the literal key ``$INPUT`` on the parent context, not a root.
            continue
        resolved = _ROOT_REF_ALIASES.get(name, name)
        if resolved in _ROOT_REF_NAMES:
            found.add(prefix + resolved)
    return frozenset(found)


@lru_cache(maxsize=256)
def _scan_root_refs_cached(prefix: str, text: str) -> FrozenSet[str]:
    return _scan_root_refs_impl(prefix, text)


def _scan_root_refs(prefix: str, text: str) -> FrozenSet[str]:
    if len(text) <= _MAX_SCAN_CACHE_LEN:
        return _scan_root_refs_cached(prefix, text)
    return _scan_root_refs_impl(prefix, text)


def _container_has_root(obj: Any, root_key: str) -> bool:
    """Would ``obj[root_key]`` (the nested thunk's root lookup) succeed?"""
    if isinstance(obj, dict):
        return root_key in obj
    if hasattr(obj, "__contains__"):
        try:
            return root_key in obj
        except Exception as exc:  # noqa: BLE001 — exotic containers must not break eval
            # 探针失败按"该容器没有这个根键"继续往下试（保守：宁可把数据文本原样
            # 返回，也不让它带着误导性的归因去递归求值）；留痕便于排查自定义上下文。
            logger.debug(
                "contains probe failed for %s root %r: %s", type(obj).__name__, root_key, exc,
            )
    get = getattr(obj, "get", None)
    if callable(get):
        try:
            return get(root_key, _MISSING) is not _MISSING
        except Exception as exc:  # noqa: BLE001
            logger.debug(
                "get probe failed for %s root %r: %s", type(obj).__name__, root_key, exc,
            )
    if hasattr(obj, "__dict__"):
        return hasattr(obj, root_key)
    return False


def _render_path(root_key: str, walked: List[Tuple[str, Any, str]]) -> str:
    """Render the variable path walked so far (``$NODE.llm.text``)."""
    parts = [root_key]
    for kind, target, raw in walked:
        parts.append(f"[{target}]" if kind == "index" else f".{raw}")
    return "".join(parts)


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
    compiled once into a thunk tree and memoized: the main LRU holds up to
    ``_MAX_CACHE_ENTRIES`` short expressions; long strings (long template
    bodies with an interpolation slot) go into a small dedicated LRU of
    ``_LONG_CACHE_ENTRIES`` entries so repeated evaluation skips the
    expensive full-string scan without pinning unbounded text in memory.
    """

    _instances: Dict[str, "ExpressionParser"] = {}
    _instances_lock = threading.Lock()

    _MAX_CACHE_ENTRIES = 8192
    _MAX_CACHED_LEN = 4096
    # >_MAX_CACHED_LEN 的长串（长模板正文带插值槽）进独立小 LRU：条数少所以
    # 键内存有界（64 × 串长），同时给模板密集流程免去每次 19~24ms 的
    # scanString 全串扫描（2026-10 wave2）。
    _LONG_CACHE_ENTRIES = 64

    def __init__(self, prefix: str = "$") -> None:
        self.prefix = prefix
        self._compile_cache: "OrderedDict[str, Any]" = OrderedDict()
        self._long_cache: "OrderedDict[str, Any]" = OrderedDict()
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
        # esc_char 必须配齐（2026-10 评审修复包 A2）：codeflow 编译器 _render_arg
        # 对 `\` `"` `\n` `\r` `\t` 做反斜杠转义，文法侧不配 esc_char 时 `\"` 让
        # 字符串匹配提前终止（整个函数调用解析失败），`\\` 不回退、且
        # convert_whitespace_escapes 会把 `\t` 吃成制表符——求值结果静默腐蚀。
        # 本配置与 _render_arg 严格互逆（round-trip 金标见
        # tests/unit/test_reviewfix_a_strings.py）。
        # esc_quote 用**成对引号序列**（""/''）：pyparsing 语义是「转义一个内嵌
        # 引号的字符序列」，传单个引号字符会把每个内嵌引号当转义序列，手写
        # `$F.concat('a', 'b')` 会整段吞成 "a', 'b"（已实证）。
        string = (
            pp.QuotedString('"', esc_char="\\", esc_quote='""')
            | pp.QuotedString("'", esc_char="\\", esc_quote="''")
        )
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
        #
        # ``$F.`` 负前瞻（2026-10 评审修复包 A5）：``$F`` 是函数命名空间，不是
        # 上下文根。没有这个前瞻时，残缺的 ``$F.concat(...)``（如字符串引号未
        # 转义）回退被 variable 吞成 ``$F.concat`` 变量路径——残缺串恰好整体
        # 匹配时求值期抛 KeyError '$F' not found（假诊断，掩盖真实文法错误与
        # 列号）。前瞻后 ``$F.`` 开头的非函数调用串直接在文法层失败，报
        # ParseException。``$FLOW`` / ``$FOO`` 等根不受影响（``$F`` 后无 ``.``）。
        root = pp.Combine(
            ~pp.Literal(f"{prefix}F.") + pp.Literal(prefix) + pp.Optional(name_token)
        )

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
            for position, (kind, target, raw) in enumerate(segments):
                if kind == "index":
                    obj = obj[target]
                    continue
                if root_key == f"{prefix}INPUT" and isinstance(obj, dict) and target not in obj:
                    # 静默 None 是 INPUT 缺键的历史语义；debug 留痕便于排查拼写错误
                    logger.debug(
                        "expression references missing input key %r; evaluating to None", raw,
                    )
                container = obj
                attr = _get_attr(container, target)
                # 仅当字符串属性值本身是表达式（$ 前缀变量 / {% %} 模板）时才递归
                # 求值——这是"嵌套表达式字符串"的历史语义。任意普通字符串（可能含
                # [tag]、引号、换行等元字符）不再二次解析，否则节点输出一旦被下游
                # $NODE 路径引用就会因内容触发误解析（如 "[promo] ..." 被当列表）。
                if (isinstance(attr, str)
                        and (attr.startswith(prefix) or "{%" in attr)):
                    # 方案 A（#104）：递归前先扫根键。嵌套串以**持有它的父容器**
                    # 为 context 求值，所以只要它引用的任一上下文根在当前父容器里
                    # 不存在，它就不是"嵌套表达式字符串"而是数据文本（LLM/agent
                    # 输出原文、文件内容、prompt 回显）——原样返回，既不抛归因错误
                    # 的 KeyError，也不静默改写（"{% $F.eq(1,1) %}" -> "True"）。
                    # 不引用任何根键的串（如 "{% $F.concat('a','b') %}"）保持历史
                    # 语义继续递归。
                    missing_roots = [
                        root for root in _scan_root_refs(parser.prefix, attr)
                        if not _container_has_root(container, root)
                    ]
                    field_path = _render_path(root_key, segments[:position + 1])
                    if missing_roots:
                        logger.debug(
                            "nested expression string in %s references root(s) %s missing "
                            "from its parent container; returned AS-IS as data text",
                            field_path,
                            ", ".join(sorted(missing_roots)),
                        )
                        obj = attr
                    else:
                        try:
                            obj = parser.evaluate(attr, container, registry)
                        except KeyError as exc:
                            # 兜底归因：扫描没识别出的引用（如未知根 $INPUT-x、
                            # 更深一层才失败的串）仍会抛 KeyError——必须说明错误
                            # 发生在**嵌套求值**里、parent 容器缺键，而不是流程根
                            # 上下文缺键（#104 ①）。
                            preview = attr if len(attr) < 120 else attr[:117] + "..."
                            raise KeyError(
                                f"解析 {field_path} 时，字段值是一段嵌套表达式字符串 "
                                f"{preview!r}：嵌套字符串以**持有它的父容器**为 context "
                                f"求值（不是流程根上下文），而该父容器里缺少它引用的"
                                f"根键。原始错误：{exc}。若这个字段是数据文本"
                                f"（LLM/agent 输出原文、文件内容、prompt 回显），"
                                f"它不应被当作表达式求值。"
                            ) from None
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
        catching the ParseException).  Strings up to ``_MAX_CACHED_LEN`` go
        into the main LRU; longer strings (long template bodies) go into a
        small dedicated LRU (``_LONG_CACHE_ENTRIES``) so repeat evaluation
        skips the scanString pass without pinning unbounded text in memory.
        """
        if len(value) > self._MAX_CACHED_LEN:
            return self._lookup_and_store(self._long_cache, self._LONG_CACHE_ENTRIES,
                                          value, compile_fn)
        return self._lookup_and_store(self._compile_cache, self._MAX_CACHE_ENTRIES,
                                      value, compile_fn)

    def _lookup_and_store(self, cache: "OrderedDict[str, Any]", maxsize: int,
                          value: str, compile_fn: Callable[[str], Any]) -> Any:
        compiled = cache.get(value)
        if compiled is not None:
            try:
                cache.move_to_end(value)
            except KeyError:  # pragma: no cover - evicted between get and move
                pass
            return compiled
        compiled = compile_fn(value)
        with self._cache_lock:
            cache[value] = compiled
            cache.move_to_end(value)
            while len(cache) > maxsize:
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
        try:
            thunk = self._cached_compile(value, self._compile_prefix)
        except pp.ParseException as exc:
            # 保留 ParseException 类型（parse_function 契约 + 节点错误语义），
            # 但补上下文：裸「Expected end of text」不带原文、原因提示，AI 作者
            # 无从自纠（2026-10 评审修复包 A5）。
            raise self._annotate_parse_error(value, exc) from None
        return thunk((context, registry, self.prefix))

    def _annotate_parse_error(self, value: str,
                              exc: pp.ParseException) -> pp.ParseException:
        """把文法解析失败包装成带原文/列号/常见原因提示的 ParseException。"""
        line = exc.line or value
        col = max(exc.column, 1)
        caret = " " * (col - 1) + "^"
        msg = (
            f"无法解析表达式：{value!r}\n"
            f"  {line}\n  {caret}（第 {col} 列附近：{exc.msg}）\n"
            "常见原因：字符串常量里的引号/反斜杠未转义（写作 \\\" 或 \\\\）；"
            "使用了不支持的写法（列表/字典字面量、{{...}}、f-string、裸函数调用）；"
            "或 {% ... %} 模板未闭合。"
        )
        return pp.ParseException(value, exc.loc, msg, self._prefix_expr)

    def _compile_prefix(self, value: str) -> Thunk:
        parsed = self._prefix_expr.parse_string(value, parse_all=True)
        return parsed[0]

    def _eval_template(self, value: str, context: Dict[str, Any],
                       registry: Optional[Any] = None) -> Any:
        # Scan for {% ... %} matches and stitch the string back together,
        # substituting str(evaluated_value) for each match — identical to the
        # old ``re.sub`` behaviour. When no match is present the string is
        # returned unchanged.
        # 可疑字面量告警在模板入口统一检查（2026-10 评审修复包 A4：原只在无
        # `{%` 的快速路径检查，含 `{%` 但内层不合法的串走不到）。
        if _SUSPICIOUS_LITERAL.search(value):
            _warn_suspicious_literal(value)
        # 快速预筛（2026-09 性能评审 P1）：不含模板标记的纯文本直接返回，
        # 省掉 scanString 全串扫描——纯文本路径实测 43.5µs/次。
        if "{%" not in value:
            return value
        segs = self._cached_compile(value, self._compile_template)
        if not any(seg_thunk is not None for _, _, seg_thunk in segs):
            # 含 `{%` 但没有任何插值匹配成功（内层表达式不合法/未闭合）——
            # 保持原样返回（不抛错，避免破坏存量语义），补告警留痕。
            _warn_unmatched_template(value)
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
