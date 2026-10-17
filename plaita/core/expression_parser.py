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
* ``$NODE.raw.<node>[.<path>...]`` bypasses that re-evaluation entirely and
  returns the stored value verbatim — strings are never evaluated at any depth
  (#130, the explicit opt-in for data text).  ``$NODE.<node>...`` keeps the
  semantics described above, unchanged.
* index segments (``[n]`` / ``.n``) index directly without recursion
* field segments never resolve Python-internal names: a ``_``-prefixed
  *attribute* read (``$NODE.n1.__class__``) raises ``ValueError`` instead of
  traversing to ``__globals__`` / process environment, and ``__``-prefixed
  *path segments* into a mapping are rejected too.  Single-underscore keys stay
  readable (``$NODE._n2`` node ids, a ``dict`` with a ``_foo`` key), and a dunder
  key stays readable through the explicit ``$F.get`` / ``$F.keys`` mapping
  helpers.  Only the ``_`` prefix is gated — public-attribute traversal is **not**
  sandboxed (``$NODE.h.os.environ`` for a node-returning object with a public
  ``os`` attribute); see the attribute-traversal gate below and MIGRATION.md #55.
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

plaita#69 copy discipline (r6–r13) — the places where host code is *not* entered
--------------------------------------------------------------------------------

``_engine_copy`` / ``state._snapshot_value`` read host containers only through
**base-class protocols** (``dict.items(sub)``, ``list.__iter__(sub)``, …) and
normalise scalar subclasses through base descriptors, so a host ``items`` /
``__iter__`` / ``__getitem__`` / ``__deepcopy__`` / ``__str__`` / ``__buffer__``
override is never called. The same now holds for the mapping helpers
``$F.keys``/``$F.values``/``$F.items`` (and their ``getDictKeys``/``getDictValues``
aliases): for a ``dict`` argument they read the **real storage** through
``dict.keys/values/items`` instead of calling the argument's own method, and for a
non-``dict`` mapping they refuse by name (the engine cannot read a foreign
mapping's storage without entering host code). Four documented residues remain:

* **Dict-view materialisation** (:func:`_detach_alias`, r3/r8 behaviour, narrowed
  in r11 and again in r12).  A *dict view* is not indexable, so a shallow-producer
  result that is a view is materialised with ``list(value)`` before the structural
  copy.  ``list`` calls ``__iter__`` and ``__len__``; only the views the engine
  itself produces (``dict_keys``/``dict_values``/``dict_items`` and the C-derived
  ``odict_*`` types — the **exact types** in ``_ENGINE_VIEW_TYPES``) may be
  materialised, because only there do both names resolve to C slot descriptors of
  that view's own layout.  For an ABC view (``collections.abc.ItemsView`` & co.)
  those two names are *Python* implementations that read ``self._mapping``, and a
  host subclass may override them, so ``list(value)`` would execute host code and
  persist the host's **self-described** contents.  r11 refused such views **by
  name** instead (``_dict_view_is_materialisable``); the previous behaviour
  (``isinstance(value, _VIEW_TYPES)`` + ``list(value)``) measured hooks
  ``['view.__iter__', 'view.__len__']`` on a host ``ItemsView`` subclass.  r12
  closed the two residual bypasses of that gate: "the resolved implementation is a
  C descriptor *type*" is not the same as "the descriptor belongs to this object's
  layout", so (a) ``class V1(KeysView): __iter__ = dict.__iter__`` used to pass the
  gate and then raise a **bare** ``TypeError`` from ``list(v1)``, and (b)
  ``KeysView.register(LieList)`` (a plain ``list`` subclass — ABC registration is
  host-controlled) used to be materialised as if it were a view.  Materialisation
  now requires the **exact engine view type**, and the ``list(value)`` call itself
  is wrapped so **any** failure becomes a named ``plaita#69`` refusal.  The blast
  radius was bounded — ``_engine_copy(<view subclass>)`` is itself named-refused,
  and the materialised value is a fresh plain ``list`` (never the live object) —
  but the predicate keeps this hop hook-free too.
* **Host objects outside the rebuilt container kinds are carried by reference.**
  ``state._snapshot_value`` rebuilds exactly the five container kinds the engine
  owns (``dict``/``list``/``tuple``/``set``/``frozenset``, subclasses included)
  plus their keys/members/scalars; every other value is returned **by reference**.
  The r8 wording said "non-container host objects" — that is false: the shared
  set also contains **containers** that are not ``dict``/``list``/``tuple``/
  ``set``/``frozenset`` instances, e.g. ``types.MappingProxyType``,
  ``collections.ChainMap``, ``collections.UserDict`` and ``os.environ``
  (``os._Environ``).  They are shared, not rebuilt, because rebuilding them
  would have to guess their construction protocol.  That is safe on the snapshot
  path only because the snapshot is read-only for these types (nothing in the
  engine mutates a snapshot in place — the write path is the DSL copy below),
  and it is **not** a data channel: on the DSL path ``_engine_copy`` refuses all
  four by name (they are outside ``plaita.core.types``' persistable data model),
  so no live mapping-like host object is ever *written through*.
  Values whose real type is one of the five kinds are always rebuilt; a
  ``__class__``-spoofed "container" fails the base descriptor and raises the
  named ``_EngineCopyRefused``/``StateSnapshotError`` instead of being shared
  (r8 G3).
* **Off-object state is out of scope by design** (r12, declared; reason corrected
  in r13).  The predicate above is a *zero-hook static* judgement over the class
  object: it can see state that lives on the instance (``__dict__``, ``__slots__``,
  a shadowing descriptor, a ``member_descriptor``/``getset_descriptor`` slot in some
  layer's namespace before ``dict`` — declared by a Python-defined class *or* an
  extension type) and state the host *declares* through serialisation hooks.  It cannot see
  state that does not sit on the instance at all: a **class attribute** holding the
  data, a **closure** cell, a **module-level side table** keyed by ``id(self)``, or
  data reached only behind a **property/data descriptor**.  The **class-attribute**
  family is *not* invisible to hook-free reading — ``_class_namespace(klass)``
  already walks every layer's real ``tp_dict`` with zero host hooks — so the reason
  there is different and is stated for what it is: once an attribute is in the
  namespace, "**is this attribute content, or a constant/cache/class-level
  default?**" is undecidable from the class object alone, and refusing every
  ``dict`` subclass that carries *any* non-dunder class attribute would refuse the
  language itself (``__module__``/``__qualname__``/``__doc__`` on every subclass,
  every decorated method, every class constant).  The other three families
  (closure / module-level side table / property-or-data-descriptor) genuinely
  require either (i) calling host code — which the whole r6–r13 discipline forbids,
  because a hook that answers "what are my contents" is exactly the untrusted
  channel the engine is built to avoid — or (ii) solving an undecidable
  aliasing/reachability question over arbitrary Python objects.  So the boundary is
  drawn at "on-object storage declarations + declared serialisation hooks", and the
  off-object families are accepted as **out of scope** rather than silently implied
  to be covered.  See ``MIGRATION.md`` and
  ``docs-site/docs/guide/expressions.md`` (r13 section) for the same declaration in
  user-facing prose.

``isinstance()`` itself is a host-hook channel: when the real type is not already
a subtype, CPython falls back to reading ``value.__class__`` through
``tp_getattro``, i.e. the host's ``__getattribute__``.  r11 wraps every such probe
on host data (``_canonical_persistable_kind``, ``state._snapshot_isinstance``) so
a raising host ``__getattribute__`` produces the **named** ``_EngineCopyRefused`` /
``StateSnapshotError`` instead of a bare ``RuntimeError``; ``type(value)`` reads the
C ``ob_type`` slot and always stays safe.
"""

from __future__ import annotations

import logging
import re
import sys
import threading
import types
import weakref
from collections import OrderedDict
from collections.abc import ItemsView, KeysView, Mapping, ValuesView
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import date, datetime
from decimal import Decimal
from functools import lru_cache
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Tuple

import pyparsing as pp

from plaita.core import expression as _expression

# 文档承诺**原样抛出**的原生算子错误（类型/键/转换/除零…）：这些不得被改写成尺寸错误。
_NATIVE_OPERATOR_ERRORS = (
    TypeError, ValueError, KeyError, IndexError, AttributeError,
    ZeroDivisionError, OverflowError, UnicodeError, MemoryError, StopIteration,
)
from plaita.core.expression import (
    ExpressionRegistry,
    _fn_getDictKeys,
    _fn_getDictValues,
    _fn_items,
    _fn_keys,
    _fn_values,
    get_default_expression_registry,
)
from plaita.core.types import (
    PERSISTABLE_CONTAINER_TYPES,
    PERSISTABLE_IMMUTABLE_TYPES,
    PERSISTABLE_MUTABLE_TYPES,
    PERSISTABLE_SCALAR_TYPES,
    PERSISTABLE_TYPES,
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


def _size_guard_result(func_name: str, value: Any, input_size: int) -> Any:
    """求值侧结果规模网（issue #68）：**对每一次 $F.* 调用都生效**。

    判定只依赖调用点**在调用之前**算好并作为值传进来的 ``input_size``（本帧
    局部量）：

    * 没有线程本地、没有跨函数通道——``func(*args)`` 期间跑什么代码都无法改变它；
    * 也**不**在调用后回读实参——被调用者改写自己的可变实参（
      ``lst.clear(); lst.extend([...])``）同样无效（第六轮验收 必修 1）。

    仍然要如实说明：本函数在 ``func(*args)`` **之后**运行，对**没有**预检的路径
    只能"检测到"（值已分配）；已知放大算子在 ``expression.py`` 里已做分配前预检，
    本函数是兜底网（含数值分支、未枚举算子、自定义注册函数）。
    """
    size = _expression._measure(value)
    hard = _expression._EXPR_MAX_RESULT_HARD
    if size > hard:
        raise ValueError(
            f"$F.{func_name} result too large: {size} {_expression._measured_unit(value)} "
            f"(limit {hard}). Refusing to build an unbounded result."
        )
    if size <= _expression._EXPR_MAX_RESULT_SIZE:
        return value
    allowed = _expression._allowed_result_size(input_size)
    if size > allowed:
        raise ValueError(
            f"$F.{func_name} result too large: {size} {_expression._measured_unit(value)} "
            f"(limit {allowed}). Refusing to build an unbounded result."
        )
    return value


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


# ---------------------------------------------------------------------------
# Attribute-traversal gate (plaita#55, security)
# ---------------------------------------------------------------------------
# Every non-index segment used to be handed to ``_get_attr``, whose fallback is
# a bare ``getattr(obj, path, None)`` with no underscore filter — while
# ``name_token`` makes ``_`` a legal segment character.  A node whose ``execute``
# returns a dataclass / Pydantic model / plain instance therefore made
# ``$NODE.n1.__class__.__init__.__globals__.os.environ`` a syntactically legal
# expression that dumps the worker process environment; the ``$ENV`` expose
# allowlist (``plaita.core.context._safe_environment``) is not on that path.
#
# The gate is applied at the single point where a caller-supplied segment is
# turned into a Python attribute (``_get_attr``), with two tiers because the two
# resolution paths mean different things:
#
# * **Attribute path** (``getattr`` fallback): *every* ``_``-prefixed name is
#   rejected — a Python-internal name is never flow data.
# * **Mapping path** (``dict`` / ``__getitem__`` + ``get``): the segment is a
#   **key**, not an attribute, so a single-underscore key stays readable —
#   ``$NODE._n2`` (builder-generated node ids) and flows that deliberately store
#   ``_foo`` keys keep working.  ``__``-prefixed (dunder) *path segments* are
#   rejected on this path too: in the path form a dunder segment cannot be told
#   apart from an attribute-traversal attempt, and evaluating it to ``None``
#   reproduces exactly the "looks like it is blocked" illusion that makes an
#   unguarded traversal hard to notice.  The key itself stays ordinary data: the
#   explicit mapping helpers (``$F.get`` / ``$F.getDictValue`` / ``$F.keys`` …)
#   still read/see it — that asymmetry is documented in MIGRATION.md (#55).
#
# **Scope (do not over-read this gate).** Only the ``_`` *prefix* is gated.  Public
# attribute traversal is NOT sandboxed: a node that returns an object with a
# public attribute pointing at a module (``SimpleNamespace(os=os)``) is still
# traversable — ``$NODE.h.os.environ`` evaluates to the live process environment.
# That is the trusted-authoring boundary this issue's Boundary section keeps
# ("no whole-engine sandboxing"); the fix removes the *reflection* path to Python
# internals, it does not turn node return values into untrusted data.  Node
# plugins must therefore not return objects that expose sensitive modules.
#
# Rejection is a hard ``ValueError`` (the exception type the issue selects and
# the one this layer already raises — ``ExpressionRegistry``): a silent ``None``
# is indistinguishable from a missing key, which is the failure mode #55 calls
# out.  Nothing about the object is echoed in the message.
_RESERVED_FIELD_PREFIX = "_"
_RESERVED_DUNDER_PREFIX = "__"

# Exact wording is pinned by tests/unit/test_issue55_attr_traversal.py (runtime
# message == the string quoted in MIGRATION.md and the expression guide).
_REJECTION_TEMPLATE = (
    "expression field name {path!r} is not allowed in an expression path: names "
    "starting with '_' are reserved for Python internals (for example __class__ / "
    "__globals__ / __init__), so the attribute form reaches process state and "
    "bypasses the $ENV allowlist, which is an opt-in exposure list and not a "
    "sandbox. Only this prefix is gated: public attributes of node-returned "
    "objects stay readable by design (MIGRATION.md #55)."
)


def _reject_reserved_field(path: str) -> None:
    """Raise ``ValueError`` for a rejected traversal segment (plaita#55)."""
    raise ValueError(_REJECTION_TEMPLATE.format(path=path))


# ---------------------------------------------------------------------------
# In-place mutator gate (plaita#69)
# ---------------------------------------------------------------------------
# The nine built-in mutators ($F.pop / set / clear / setListItem / …) operate on
# the argument object **in place**.  For the five checkpoint-persisted roots the
# evaluator hands out the live object, so a mutator anywhere outside a loop
# condition rewrites $INPUT/$NODE/$GLOBAL/$PARENT/$ENV itself and the dirty value
# is serialised into the checkpoint (measured in the #69 evidence table).  The
# gate below refuses that shape with a **named path** error before evaluation,
# while keeping the documented in-place contract for every other target:
#   * non-root context keys / loop-local values stay mutable (owner requirement);
#   * a call whose result is a **new object** is valid as the mutation target
#     (`$F.set($F.json_loads($INPUT.raw), …)`, `$F.pop($F.slice($INPUT.items,…),0)`);
#   * a *shallow* producer (new container, aliasing elements — `slice` / `add` /
#     `values` …) stored as a **value argument** over a persisted root is
#     **detached** (deep-copied) instead of being stored as an alias (r3 F1);
#   * a call that may return a **live reference** into a persisted root
#     (`get` / `getListItem` / `ifelse` / the mutators themselves) is refused in
#     every position, but only when one of *its own* arguments can statically
#     reach a persisted root — `$F.set($VAR,'k',$F.get($VAR,'x'))` stays allowed
#     (r3 F2);
#   * an **undeclared** producer stays fail-closed (a declaration is the only
#     proof the gate can get).
#   * loop conditions keep their isolated-copy semantics: they evaluate against a
#     plain dict of deep-copied roots, which is not the live state, so the gate
#     does not fire (contract pinned by tests/unit/test_loop.py).
_PERSISTED_FIELDS = frozenset({
    "input_value", "node_results", "global_context", "parent_context", "env",
})

#: 规范根名（去掉 prefix）——**只作嵌套容器的快速路径**（r4）。容器判据自 r4 起
#: 由**对象身份**决定：名字（含这五个）不再能决定"这个值是不是活根"。快速路径
#: 命中的前提是容器里那个键的值**确实是**活持久化对象（同一对象），否则一律交给
#: ``_probe_container_alias`` 的传递身份探测。见 #69 r4 反例（任意键名别名）。
_CANONICAL_ROOT_NAMES = ("INPUT", "NODE", "PARENT", "GLOBAL", "ENV")

#: Default-registry names that run in place (fallback when a dict-like registry
#: does not expose descriptors; the authoritative flag is ``has_side_effects``).
_DEFAULT_MUTATOR_NAMES = frozenset({
    "pop", "delListItem", "setListItem", "set", "delete", "clear",
    "setDictValue", "delDictValue", "clearDict",
})

# ---------------------------------------------------------------------------
# Producer classification (plaita#69 r3, F1)
# ---------------------------------------------------------------------------
# A call used as an argument of an in-place mutator is classified by **what its
# result can reference**, never by the callee's identity alone:
#
# * DEEP    — the result cannot contain a reference into the arguments (scalars,
#             strings, bools, freshly built containers of new values, JSON
#             round-trips).  Safe in every argument position.
# * SHALLOW — the result is a **new container** whose *elements* are still the
#             caller's objects (`slice` / `reverse` / `append` / `add` … and the
#             dict views `keys` / `values` / `items`).  Valid as the mutation
#             **target** (the mutator only adds/removes/replaces entries of the
#             new container), but when it is stored as a **value argument** while
#             one of its arguments reaches a persisted root, the stored element
#             aliases the root and a later ordinary expression writes through
#             (the r3 F1 counter-example: `$F.set($VAR,'k',$F.slice($INPUT.items,0,1))`
#             followed by `$F.set($VAR.k[0],'x',9)`).  The gate **detaches**
#             (deep-copies) such a value instead of surfacing an alias.
# * LIVE    — the result can be one of the arguments' own objects (`get` /
#             `getItem` / `ifelse` / `and` / `or` and the mutators themselves):
#             never valid as a mutation target, refused in every position.
# * UNKNOWN — not declared and not classified: fail-closed.
#
# A guard test pins this classification against the default registry, so a new
# registration is red until it is classified or declared ``fresh_producer=True``.
_DEEP_PRODUCER_FUNCTIONS = frozenset({
    "abs", "capitalize", "ceil", "concat", "contains", "div", "endswith", "eq",
    "floor", "gt", "gte", "in", "index", "isDigit", "join", "json_dumps",
    "json_loads", "len", "length", "lower", "lstrip", "lt", "lte", "mod", "ne",
    "not", "notContains", "notIn", "now", "pow", "replace", "round", "rstrip",
    "split", "sqrt", "startswith", "strip", "sub", "title", "today", "trunc",
    "upper",
})

#: New container, **aliasing elements** — see the SHALLOW paragraph above.
_SHALLOW_PRODUCER_FUNCTIONS = frozenset({
    "add", "addListItem", "append", "extend", "getDictKeys", "getDictValues",
    "insert", "insertListItem", "items", "keys", "mul", "remove", "reverse",
    "slice", "sort", "values",
})

#: May return one of the arguments' own objects (a live reference).
_LIVE_REFERENCE_FUNCTIONS = frozenset({
    "get", "getDictValue", "getListItem", "ifelse", "and", "or",
})

#: Umbrella kept for the guard test / "returns a new object" wording.
_FRESH_PRODUCER_FUNCTIONS = _DEEP_PRODUCER_FUNCTIONS | _SHALLOW_PRODUCER_FUNCTIONS

#: Result classes returned by ``_callee_result_class``.
_RESULT_DEEP = "deep"
_RESULT_SHALLOW = "shallow"
_RESULT_LIVE = "live"
_RESULT_UNKNOWN = "unknown"

#: Dict view types (`$F.keys|values|items|getDictKeys|getDictValues` results):
#: a *new* view object whose elements alias the argument, but not indexable and
#: not a path target — detaching materialises it into a plain list.
_VIEW_TYPES = (KeysView, ValuesView, ItemsView)

#: 引擎**自产**的 C 视图类型（r12）：``dict`` 的 ``dict_keys``/``dict_values``/
#: ``dict_items`` 与 C 派生 ``OrderedDict`` 的 ``odict_keys``/``odict_values``/
#: ``odict_items``。用**精确类型**做交集（不是 ``isinstance``、也不是 ABC 注册）：
#: ABC 注册是宿主可控的（``KeysView.register(LieList)`` 会让一个 ``list`` 子类通过
#: :func:`_is_dict_view_type`），而物化只允许发生在引擎自己产出的视图类型上。
#: 集合在导入时算一次（六个视图实例都是空容器，构造零副作用）。
_ENGINE_VIEW_TYPES = tuple(dict.fromkeys(
    type(view)
    for view in (dict().keys(), dict().values(), dict().items(),
                 OrderedDict().keys(), OrderedDict().values(),
                 OrderedDict().items())
))

#: 引擎自带的映射视图函数（``expression.py`` 里的 ``_fn_keys`` 等）与"读**真实**存储"
#: 的基类描述符读者（r13）。
#:
#: r11/r12 的契约里 ``$F.keys``/``$F.values``/``$F.items`` 会调用**宿主自己的**
#: ``a.keys()`` —— 于是 ``class LieKeysPlainList(dict): def keys(self): return ["LIE"]``
#: 可以把自述内容当数据写进 ``$VAR``（实测 ``$VAR == {'k': ['LIE']}``、宿主钩子
#: ``keys`` 被调用；``items``/``values`` 同形）。引擎的固定规则是**宿主自述不是证据**，
#: 所以对 ``dict`` 参数改读**真实存储**（``dict.keys(v)`` / ``dict.values(v)`` /
#: ``dict.items(v)``：基类 C 描述符，宿主覆写零调用）；非 ``dict`` 的映射无法在不进入
#: 宿主代码的前提下读到真实存储 ⇒ **具名拒绝**（见
#: :data:`_MAPPING_VIEW_NON_DICT_TEMPLATE`）。
#:
#: 只拦**引擎自带**的实现（按 callable 身份比对，``expression.py`` 的模块级函数对象在
#: 默认注册表重建后仍是同一对象）：宿主在自定义 registry 里注册自己的 ``keys`` 时，
#: 那是宿主显式提供的函数，不在本拦截范围内。
_STORAGE_VIEW_BUILTINS: Dict[str, Callable] = {
    "keys": _fn_keys,
    "values": _fn_values,
    "items": _fn_items,
    "getDictKeys": _fn_getDictKeys,
    "getDictValues": _fn_getDictValues,
}

#: 名字 → ``dict`` 的基类 C 描述符（真实存储读者）。
_MAPPING_VIEW_BASE_READERS: Dict[str, Callable] = {
    "keys": dict.keys,
    "values": dict.values,
    "items": dict.items,
    "getDictKeys": dict.keys,
    "getDictValues": dict.values,
}

_MAPPING_VIEW_NON_DICT_TEMPLATE = (
    "expression $F.{func}(...) received {type_name!r}, which is not a dict: the "
    "engine reads a mapping's contents only through the base dict descriptors "
    "(dict.keys/dict.values/dict.items), so that a host override of "
    "keys()/values()/items() can never hand back self-described contents as data. "
    "For a non-dict mapping the real storage cannot be read without entering host "
    "code, so this is refused (plaita#69). Rebuild the mapping as a plain dict from "
    "known-good data (a producer / $F.json_loads(...)) when its contents are data."
)

_MAPPING_VIEW_SPOOFED_TEMPLATE = (
    "expression $F.{func}(...) received {type_name!r}, which passes isinstance(dict) "
    "only through a spoofed __class__: the base dict descriptor rejects it, so the "
    "real storage cannot be read (plaita#69)."
)

_MAPPING_VIEW_PROBE_FAILED_TEMPLATE = (
    "expression $F.{func}(...) received {type_name!r} whose host class raises from "
    "__getattribute__ while the engine probes isinstance(dict), so it cannot tell "
    "whether the real storage is readable (plaita#69)."
)


def _engine_mapping_view(func_name: str, value: Any) -> Any:
    """Read ``value``'s **real** ``dict`` storage for ``$F.{func_name}`` (r13).

    只对 ``dict``（含子类）调用基类 C 描述符；宿主覆写的 ``keys``/``values``/
    ``items`` **零调用**。非 ``dict`` 的映射、``__class__`` 伪造的形状、以及宿主
    ``__getattribute__`` 在 ``isinstance`` 探测时抛异常的形状，都以具名 ``ValueError``
    （含 ``plaita#69``）拒绝，绝不回落到宿主自述、也绝不漏裸异常。
    """
    try:
        is_dict = isinstance(value, dict)
    except Exception as exc:                    # noqa: BLE001 - 宿主 __getattribute__
        raise ValueError(_MAPPING_VIEW_PROBE_FAILED_TEMPLATE.format(
            func=func_name, type_name=type(value).__name__)) from exc
    if not is_dict:
        raise ValueError(_MAPPING_VIEW_NON_DICT_TEMPLATE.format(
            func=func_name, type_name=type(value).__name__))
    reader = _MAPPING_VIEW_BASE_READERS[func_name]
    try:
        return reader(value)
    except TypeError as exc:                    # __class__ 伪造 / 非真 dict
        raise ValueError(_MAPPING_VIEW_SPOOFED_TEMPLATE.format(
            func=func_name, type_name=type(value).__name__)) from exc


_MUTATOR_ROOT_TEMPLATE = (
    "expression {path!r} cannot be the target of the in-place mutator "
    "$F.{func}: {path!r} is a live path into the checkpoint-persisted execution "
    "context (root {root!r} is one of $INPUT/$NODE/$GLOBAL/$PARENT/$ENV, aliases "
    "included) — the write is visible to the caller, is serialised into the "
    "checkpoint and survives restore, and no copy isolates it outside a loop "
    "condition. Refusing before evaluation (plaita#69). Mutate a fresh object "
    "instead, e.g. $F.{func}($F.slice({path}, ...), ...) or "
    "$F.{func}($F.json_loads(...), ...); loop conditions keep their isolated-copy "
    "semantics."
)
_MUTATOR_ARG_ROOT_TEMPLATE = (
    "argument #{position} of the in-place mutator $F.{func} is the live "
    "checkpoint-persisted path {path!r} (root {root!r} is one of "
    "$INPUT/$NODE/$GLOBAL/$PARENT/$ENV, aliases included): passing a live root as "
    "a value argument lets the mutation reach the persisted context through an "
    "alias and the write is serialised into the checkpoint. Refusing before "
    "evaluation (plaita#69). Derive a fresh object instead, e.g. "
    "$F.{func}(..., $F.json_loads($F.json_dumps({path})))."
)
_MUTATOR_UNMAPPED_TEMPLATE = (
    "expression {path!r} cannot be the target of the in-place mutator $F.{func}: "
    "the execution state's persisted-root key map could not be read, so the gate "
    "cannot prove {root!r} is a temporary — failing closed (plaita#69). Retry with "
    "a readable state or mutate a fresh object ($F.slice(...) / $F.json_loads(...))."
)
_MUTATOR_LIVE_REFERENCE_TEMPLATE = (
    "expression $F.{callee}(...){arg_note} cannot be used with the in-place mutator "
    "$F.{func}: {callee!r} may return a live reference into the checkpoint-persisted "
    "path {path!r} (root {root!r} is one of $INPUT/$NODE/$GLOBAL/$PARENT/$ENV, "
    "aliases included), so the in-place write would reach the persisted context and "
    "be serialised into the checkpoint. Refusing before evaluation (plaita#69). "
    "Derive a copy explicitly ($F.json_loads($F.json_dumps(...)))."
)
_MUTATOR_UNDECLARED_PRODUCER_TEMPLATE = (
    "expression $F.{callee}(...){arg_note} cannot be used with the in-place mutator "
    "$F.{func}: {callee!r} is not declared as a fresh-object producer and is not a "
    "classified built-in, so the gate cannot prove its result does not reference the "
    "checkpoint-persisted context — failing closed (plaita#69). Register the "
    "producer with register(..., fresh_producer=True) if it always returns a new "
    "object, or derive a copy explicitly ($F.json_loads($F.json_dumps(...)))."
)
_MUTATOR_DETACH_NOT_DATA_MODEL_TEMPLATE = (
    "argument #{position} of the in-place mutator $F.{func} is the result of "
    "$F.{callee}(...), which may alias the checkpoint-persisted path {path!r} "
    "(root {root!r}): the value cannot be detached by the engine-owned structural "
    "copy because it contains {type_name!r}, which is **not part of the persistable "
    "data model** (the DSL can persist only dict/list/tuple/set/frozenset and the "
    "scalars declared in plaita.core.types.native_types — Decimal/datetime/date "
    "included — plus bytes/complex; container and scalar **subclasses** are "
    "normalised to their plain base value, while a genuine mutable non-data-model "
    "object — e.g. a custom MutableMapping, a bytearray or an opaque host class — "
    "is refused so a 'detached' copy can never secretly be the live object) — "
    "failing closed (plaita#69 r7). Derive an explicit copy "
    "($F.json_loads($F.json_dumps(...))) or pass plain data."
)
#: r4 身份探测的具名拒绝：容器里那个键叫什么都无所谓，判据是对象身份。
_MUTATOR_CONTAINER_ALIAS_TEMPLATE = (
    "expression {path!r} cannot be the target of the in-place mutator $F.{func}: the "
    "expression is evaluated against a container that holds a **live** "
    "checkpoint-persisted object at {alias_path} — that object is the live root "
    "{root!r} (one of $INPUT/$NODE/$GLOBAL/$PARENT/$ENV; the container key name is "
    "irrelevant, identity decides), so the in-place write reaches the persisted "
    "context through the alias and is serialised into the checkpoint. Refusing "
    "before evaluation (plaita#69). Mutate a fresh object instead, e.g. "
    "$F.{func}($F.json_loads($F.json_dumps({path})), ...)."
)
_MUTATOR_CONTAINER_ALIAS_ARG_TEMPLATE = (
    "argument #{position} of the in-place mutator $F.{func} is the live "
    "checkpoint-persisted path {path!r} reached through the container alias "
    "{alias_path} (root {root!r} is one of $INPUT/$NODE/$GLOBAL/$PARENT/$ENV; the "
    "container key name is irrelevant, identity decides): the value is a live "
    "reference into the persisted context, so the mutation is serialised into the "
    "checkpoint. Refusing before evaluation (plaita#69). Derive a fresh object "
    "instead, e.g. $F.{func}(..., $F.json_loads($F.json_dumps({path})))."
)
_MUTATOR_CONTAINER_ALIAS_BUDGET_TEMPLATE = (
    "expression {path!r} cannot be the target of the in-place mutator $F.{func}: the "
    "expression is evaluated against a container that is too large for the identity "
    "probe to exhaust (budget {budget} nodes, stopped at {alias_path!r}), so the gate "
    "cannot prove the container holds no live checkpoint-persisted object — failing "
    "closed (plaita#69). Mutate a fresh object instead or evaluate the expression "
    "against the execution state directly."
)
#: r5 **default-deny on provenance**：门不再试图证明"这个对象是活的"（那个集合
#: 无界：任意嵌套子对象、任意容器、惰性快照、宿主对象），而是只允许**能证明是本次
#: 求值新建**的目标（freshness registry）。其余一律具名拒绝。
_MUTATOR_UNPROVEN_FRESH_TEMPLATE = (
    "expression {path!r} cannot be the target of the in-place mutator $F.{func}: the "
    "gate could not prove that this object was created by the engine during this "
    "evaluation — provenance could not be proven fresh (the object is reachable from "
    "the execution state, is a host-provided value, or is a value inside a "
    "scope/container at arbitrary depth), so an in-place write could reach the live "
    "checkpoint-persisted state. Default-deny refuses (plaita#69 r5). Derive an object "
    "the engine builds, e.g. $F.{func}($F.json_loads($F.json_dumps({path})), ...), a "
    "$F.slice/... producer result, or a value the engine materialised."
)


def _descriptor_for(registry: Optional[Any], func_name: str) -> Optional[Any]:
    """Return the ``FunctionDescriptor`` for *func_name*, if the registry exposes one."""
    if registry is None:
        return get_default_expression_registry().get(func_name)
    if isinstance(registry, ExpressionRegistry):
        return registry.get(func_name)
    getter = getattr(registry, "get", None)
    if getter is None:
        return None
    try:
        desc = getter(func_name)
    except Exception as exc:                  # noqa: BLE001 - dict-like proxy may raise
        logger.debug("expression registry descriptor lookup failed for %r: %s",
                     func_name, exc)
        return None
    return desc if hasattr(desc, "has_side_effects") else None


def _is_inplace_mutator(registry: Optional[Any], func_name: str) -> bool:
    desc = _descriptor_for(registry, func_name)
    if desc is not None:
        return bool(getattr(desc, "has_side_effects", False))
    return func_name in _DEFAULT_MUTATOR_NAMES


def _is_persisted_key(context: Any, root_key: str) -> bool:
    """Is *root_key* a persisted-field storage key of the live *context*?

    热路径专用：与 :func:`_persisted_root_keys` 同义，但只做一次 ``dict`` 查键，
    不构造 frozenset（那个版本给门的多个调用点复用）。非 CheckpointState / 键映射
    读不到 ⇒ ``False``（登记侧不贡献身份；门的 fail-closed 判定不受影响）。
    """
    mapper = getattr(context, "_key_to_field", None)
    if not callable(mapper):
        return False
    try:
        mapping = mapper()
    except Exception as exc:                  # noqa: BLE001 - 读不到就不登记，门另有 fail-closed
        logger.debug("persisted-key probe failed for %r: %s", root_key, exc)
        return False
    return isinstance(mapping, dict) and mapping.get(root_key) in _PERSISTED_FIELDS


def _is_persisted_key_strict(context: Any, root_key: str) -> Optional[bool]:
    """Target-position fast path (r6): one ``dict`` lookup, no frozenset, no index.

    ``True`` = *root_key* maps to one of the five checkpoint-persisted fields of a
    live state; ``False`` = it does not (including every non-live/plain-``dict``
    scope); ``None`` = the state exposes a key map but reading it failed — the
    caller fails **closed**.  Identity evidence is not needed here: a target that
    is not proven fresh is refused by the freshness plan anyway, so the r4 probe
    (`_live_alias_index`) is skipped on this path only (see docs §P).
    """
    mapper = getattr(context, "_key_to_field", None)
    if not callable(mapper):
        return False
    try:
        mapping = mapper()
    except Exception as exc:                  # noqa: BLE001 - fail closed
        logger.warning("plaita#69 mutation gate: cannot read the state key map "
                       "(%s: %s); failing closed", type(exc).__name__, exc)
        return None
    if not isinstance(mapping, dict):
        logger.warning("plaita#69 mutation gate: state key map is %s, not a dict; "
                       "failing closed", type(mapping).__name__)
        return None
    return mapping.get(root_key) in _PERSISTED_FIELDS


#: ``_persisted_root_keys`` 读不到状态映射时的哨兵 —— 门按 **fail-closed** 处理
#: （读不到就拒绝，绝不因为状态层异常而放行写穿）。
_PERSISTED_LOOKUP_FAILED = object()


def _persisted_root_keys(context: Any) -> Optional[Any]:
    """Storage keys of *context* that map to the five checkpoint-persisted fields.

    Only a live ``CheckpointState`` exposes ``_key_to_field``; the loop's isolated
    condition context is a plain ``dict`` (deep-copied roots) and returns ``None``,
    which is what keeps the loop contract intact.  A state that exposes the map but
    errors while reading it returns the ``_PERSISTED_LOOKUP_FAILED`` sentinel so the
    gate fails **closed** (owner 口径：失败方向选安全侧).
    """
    mapper = getattr(context, "_key_to_field", None)
    if not callable(mapper):
        return None
    try:
        mapping = mapper()
    except Exception as exc:                  # noqa: BLE001 - fail closed, with a log
        logger.warning("plaita#69 mutation gate: cannot read the state key map "
                       "(%s: %s); failing closed", type(exc).__name__, exc)
        return _PERSISTED_LOOKUP_FAILED
    if not isinstance(mapping, dict):
        logger.warning("plaita#69 mutation gate: state key map is %s, not a dict; "
                       "failing closed", type(mapping).__name__)
        return _PERSISTED_LOOKUP_FAILED
    return frozenset(key for key, field in mapping.items() if field in _PERSISTED_FIELDS)


def _path_alias_state(root_key: str, segments: Any, context: Any,
                      prefix: str) -> Optional[bool]:
    """Can ``{root_key}{segments}`` be a persisted-root path in *context*?

    ``True`` = yes, ``False`` = no, ``None`` = the state's key map is unreadable
    (fail-closed).  A loop-condition isolated copy is a plain ``dict``; its roots
    were deep-copied, so nothing aliases the live context and the answer is
    ``False`` (the loop contract must not be tightened).

    普通容器（嵌套表达式串的父容器）**不由键名决定**（plaita#69 r4）：规范根名只
    做快速路径，且必须由对象身份坐实；任意/非规范键名一律交给
    ``_probe_container_alias`` 的传递身份探测。
    """
    persisted = _persisted_root_keys(context)
    if persisted is _PERSISTED_LOOKUP_FAILED:
        return None
    if persisted is not None:
        # 活 CheckpointState：键映射（prefix/别名已内建）是精确判据；**非持久化键**
        # 也按身份判——宿主把某键设成某个活根/活根子对象时（T2-1），键名不含任何根
        # 信息，只有对象身份能看出来（r4：名字不再能决定）。
        if root_key in persisted:
            return True
        return _non_root_key_identity_alias(context, root_key)
    if getattr(type(context), "_plaita_isolated_copy", False):
        return False
    return _canonical_container_fast_path(root_key, context, prefix)


def _non_root_key_identity_alias(context: Any, root_key: str) -> bool:
    """活状态下的**非持久化键**是否恰好持有某个活持久化对象（宿主级别名）。

    键名（``$V`` 之类）本身不提供任何信息；判据是该键当下值的**对象身份**是否落在
    活持久化对象集合里。取不到（缺键 / 非法键）即 ``False``。
    """
    getter = getattr(context, "get", None)
    if not callable(getter):
        return False
    try:
        value = getter(root_key, _MISSING)
    except Exception as exc:                  # noqa: BLE001 - 探测失败不决定
        logger.debug("non-root key identity probe failed for %r: %s", root_key, exc)
        return False
    if value is _MISSING:
        return False
    return id(value) in _live_alias_index()


def _canonical_container_fast_path(root_key: str, context: Any,
                                   prefix: str) -> bool:
    """规范根名的快速路径——但**只有身份坐实**才返回 ``True``。

    ``$INPUT`` 这样的名字本身不再是判据（r4）：容器里 ``$INPUT`` 键的值必须是**活**
    持久化对象（同一对象），否则返回 ``False``，由身份探测兜底。这样"物化副本挂在
    规范名下"不会被名字误拒，而"任意键名挂活根"也不会被名字漏放。
    """
    name = root_key[len(prefix):] if prefix and root_key.startswith(prefix) else root_key
    if name not in _CANONICAL_ROOT_NAMES:
        return False
    getter = getattr(context, "get", None)
    if not callable(getter):
        return False
    try:
        value = getter(root_key, _MISSING)
    except Exception as exc:                  # noqa: BLE001 - 探测失败不决定，交给探测
        logger.debug("canonical fast-path probe failed for %r: %s", root_key, exc)
        return False
    if value is _MISSING:
        return False
    return id(value) in _live_alias_index()


# ---------------------------------------------------------------------------
# Identity-based alias detection (plaita#69 r4, structural half 2)
# ---------------------------------------------------------------------------
# r1→r3 的容器判据是**名字白名单**（``_CANONICAL_ROOT_NAMES``），因此把活根挂在
# 任意键名下（``{"$myroot": "$INPUT"}``）就能绕过。r4 改为**对象身份**：只要容器
# 里任何可达对象**就是**（或就在其路径上）一个活 ``CheckpointState`` 的持久化根，
# 就地改写就必然写穿——与键名叫什么无关。
#
# 活根索引来自两处来源，二者都由弱引用约束，不泄漏：
#   * 每个"被当作求值上下文用过"的活 ``CheckpointState``（``_LIVE_STATES``，
#     弱引用，state 死亡即消失）的五个持久化字段值；
#   * 求值器沿持久化根路径**实际交出过**的对象（``$INPUT.cfg`` 这类子对象，
#     在 ``_compile_variable_thunk`` 里登记，挂在对应 state 的条目上）。
# 第二条覆盖"活根子对象"：子对象不是根本身，但它同样属于持久化图，就地改写同样写穿。
_LIVE_STATES: "dict[int, list]" = {}          # id(state) -> [weakref.ref, {id(obj): (obj, root, rendered)}]
_LIVE_STATES_LOCK = threading.Lock()
_LIVE_STATES_PRUNE_AT = 64

#: 容器身份探测的节点预算。超出即**失败方向选安全侧**（具名拒绝），绝不因为容器太大
#: 而静默放行一次可能的写穿。
_ALIAS_PROBE_BUDGET = 50_000


def _is_live_state(context: Any) -> bool:
    """Is *context* a live ``CheckpointState`` (exposes the key<->field map)?"""
    return callable(getattr(context, "_key_to_field", None))


def _note_live_state(context: Any) -> list:
    """Return (creating if needed) the live-object registry entry for *context*."""
    entry = _LIVE_STATES.get(id(context))
    if entry is not None and entry[0]() is context:
        return entry
    ref = weakref.ref(context)
    entry = [ref, {}]
    with _LIVE_STATES_LOCK:
        # 顺带回收已死 state 的条目（弱引用，条目本身持有强引用 → 必须显式清理）。
        if len(_LIVE_STATES) >= _LIVE_STATES_PRUNE_AT:
            for dead in [k for k, e in _LIVE_STATES.items() if e[0]() is None]:
                _LIVE_STATES.pop(dead, None)
        _LIVE_STATES[id(context)] = entry
    return entry


def _register_live_objects(state: Any, root_key: str, objects: Any) -> None:
    """Register objects the evaluator handed out along a persisted-root path.

    *objects* is an iterable of ``(obj, rendered_path)``.  Only **mutable
    containers** are registered: scalars / immutable values cannot be rewritten
    in place, so they are not an alias channel (and registering interned strings
    would only create false refusals).
    """
    entry = None
    for obj, rendered in objects:
        if not isinstance(obj, (dict, list, set)):
            continue
        if entry is None:
            entry = _note_live_state(state)
        key = id(obj)
        # 先查后写：``setdefault((obj, root, rendered))`` 会**急切构造**三元组，热路径上
        # 同一批对象每次求值都重复构造后丢弃（实测为登记侧的主导开销）。
        if key not in entry[1]:
            entry[1][key] = (obj, root_key, rendered)


def _live_alias_index() -> Dict[int, Tuple[str, str]]:
    """``{id(obj): (root_key, rendered)}`` for every known live persisted object.

    Reads the five persisted field values of every live state plus the objects the
    evaluator registered along persisted-root paths.  Never materialises a
    snapshot, never copies data.
    """
    index: Dict[int, Tuple[str, str]] = {}
    with _LIVE_STATES_LOCK:
        entries = list(_LIVE_STATES.values())
    for entry in entries:
        state = entry[0]()
        if state is None:
            continue
        try:
            mapping = state._key_to_field()
        except Exception as exc:              # noqa: BLE001 - 读不到就不贡献，不静默吞来源
            logger.debug("live-state key map unreadable during alias probe: %s", exc)
            mapping = None
        if mapping:
            for key, field in mapping.items():
                if field not in _PERSISTED_FIELDS:
                    continue
                try:
                    obj = state[key]
                except Exception as exc:      # noqa: BLE001 - 缺席/不可读的根不贡献身份
                    logger.debug("live root %r unreadable during alias probe: %s", key, exc)
                    continue
                if isinstance(obj, (dict, list, set)):
                    index.setdefault(id(obj), (key, key))
        for oid, (_obj, root_key, rendered) in entry[1].items():
            index.setdefault(oid, (root_key, rendered))
    return index


def _probe_container_alias(container: Any) -> Optional[Tuple[str, str, str]]:
    """Identity-walk *container* for a live persisted object (no key names).

    Returns ``("hit", container_path, root_key)`` when a value transitively
    reachable **in** the container **is** a live persisted object (the container
    instance itself is deliberately not a refusal ground — see the note in the
    body); ``("budget", path, "")`` when the node budget was exhausted (fail
    closed); ``None`` when the walk found nothing.

    The walk covers plain containers only (``dict`` / ``list`` / ``tuple`` /
    ``set`` / ``frozenset``): an alias travels inside data, and walking arbitrary
    object attribute graphs would both cost unbounded time and materialise lazy
    snapshots.  A live state or a loop-condition isolated copy is never probed —
    the former's key map is the exact answer, the latter's referenced roots are
    deep-copied.
    """
    if _is_live_state(container):
        return None
    if getattr(type(container), "_plaita_isolated_copy", False):
        return None
    index = _live_alias_index()
    if not index:
        return None
    # 只判"容器里可达的**值**"（issue #69 r4 的结构口径）：容器实例自身的身份不是
    # 拒绝理由——否则"物化副本挂在任意/规范键名下"会被误拒，名字就又成了判据。
    # 边界别名（活根整对象被带进子流程）由结构半边 1（``_materialise_child_input``）
    # 在源头物化掉。
    seen = {id(container)}
    stack: List[Tuple[str, Any]] = [("", container)]
    budget = _ALIAS_PROBE_BUDGET
    while stack:
        prefix, node = stack.pop()
        if isinstance(node, dict):
            entries: Any = node.items()
        elif isinstance(node, (list, tuple)):
            entries = enumerate(node)
        elif isinstance(node, (set, frozenset)):
            entries = ((None, value) for value in node)
        else:
            continue
        for key, value in entries:
            budget -= 1
            if budget < 0:
                return ("budget", prefix, "")
            path = prefix + ("[?]" if key is None else f"[{key!r}]")
            hit = index.get(id(value))
            if hit is not None:
                return ("hit", path, hit[0])
            if isinstance(value, (dict, list, tuple, set, frozenset)):
                if id(value) not in seen:
                    seen.add(id(value))
                    stack.append((path, value))
    return None


# ---------------------------------------------------------------------------
# Freshness scope (plaita#69 r6): "fresh" == "created by the engine during THIS
# evaluation"
# ---------------------------------------------------------------------------
# r1→r4 tried to *prove an object is live*; that set is unbounded (any nested
# sub-object, any container used as a scope, lazy snapshots, host objects), so
# the verifier closed one shape per round and the next one appeared. r5 inverted
# the predicate — allow only what the engine created — but it stored the proof in
# a **module-level, process-wide** LRU (`_FRESH_IDS`).  The verifier measured the
# two consequences (r6 counterexamples):
#
#   C2: an object created by one evaluation and later persisted as a node result
#       (`ctx.set_state("$NODE", {"n1": ctx.evaluate({...})})`) stayed "fresh"
#       forever, so a later evaluation could mutate the **live checkpoint**;
#   side effects: up to 65536 containers were pinned by strong reference for the
#       whole process (the weakref branch was dead code — dict/list/set are not
#       weak-referenceable) and every registration/lookup took a process-wide
#       lock; "fresh" also leaked across executions.
#
# r6 replaces it with a **per-evaluation scope**: a ContextVar holding the idents
# the engine created while the current logical evaluation runs.  Exact
# entry / exit / nesting / exception semantics:
#
#   * openers: ``ExpressionParser.evaluate`` (the public parser entry) and
#     ``ExecutionContext.evaluate`` (the node-field entry; ``Executor.evaluate``
#     delegates to it).  Whichever runs first opens the scope; a loop-condition
#     evaluation opened by ``Loop.execute`` (materialisation + ``Condition.match``)
#     is one logical evaluation by construction.
#   * nesting: an opener that finds the ContextVar already set for this context
#     **shares** that scope and does not reset it — nested expression strings,
#     ``parse_function``, nested container rebuilds and every ``note_fresh_*``
#     inside them land in the same logical evaluation.
#   * exit: the opener that created the scope resets the ContextVar to its
#     previous value in a ``finally``, so the scope is dropped both on normal
#     return and on exception (the strong references it held go away with it).
#   * outside any scope a ``note_fresh_*`` call is a **no-op** and ``_fresh_has``
#     is ``False``: an object the engine materialised between evaluations can
#     never be proven fresh inside one — fail-closed, and no cross-execution
#     contamination.
#
# Storage is ``dict[id(obj) -> obj]`` with strong references: it lives exactly as
# long as the evaluation, and the identity re-check at lookup keeps a recycled id
# from producing a false "fresh" hit.  A per-scope budget bounds hostile input;
# eviction can only turn a later lookup into "not fresh" (fail-closed).
_FRESH_SCOPE: "ContextVar[Optional[Dict[int, Any]]]" = ContextVar(
    "plaita_freshness_scope", default=None)

#: Per-scope capacity. Evicting the oldest entry can only turn a later lookup
#: into "not proven fresh" ⇒ the gate refuses (fail-closed), never the reverse.
_FRESH_BUDGET = 65536

#: Per-object closure-walk budget. Exhaustion stops the walk: the objects not
#: walked are simply absent from the scope, so they cannot be used as a
#: mutation target — fail-closed, and never an unbounded walk on hostile input.
_FRESH_WALK_BUDGET = 65536

# plaita#69 r7：freshness 登记面与"引擎自有结构拷贝"共用 ``plaita.core.types``
# 里的**同一批类型对象**（单一事实来源）。登记判据是**精确类型**（``type(x) is``）
# —— 子类（``class Evil(dict)`` 覆写 ``__deepcopy__`` 返回 ``self``）永远不是
# "数据模型里的容器"，因此不可能被登记成 fresh（C1 反例的入口）。
#: Containers that can be mutated in place (the only kinds a mutator can target).
_FRESH_MUTABLE = PERSISTABLE_MUTABLE_TYPES
#: Containers whose interior is walked when a whole graph is known to be new.
#: **同一个元组对象**被拷贝器（``_ENGINE_COPY_CONTAINER_TYPES``）与登记面消费。
_FRESH_WALKABLE = PERSISTABLE_CONTAINER_TYPES
#: The copier's canonical container kinds — identical object to ``_FRESH_WALKABLE``
#: (asserted by ``tests/unit/test_issue69_single_copy_point.py``).
_ENGINE_COPY_CONTAINER_TYPES = PERSISTABLE_CONTAINER_TYPES
#: Values that cannot be mutated in place at all: a mutator targeting them fails
#: natively instead, so they never need a freshness proof.  Exact type as well —
#: a *subclass* with a side-effectful ``__setitem__`` must not inherit the
#: exemption (it goes back to default-deny and is refused).
_IMMUTABLE_TARGET_TYPES = PERSISTABLE_IMMUTABLE_TYPES


def _is_walkable_container(value: Any) -> bool:
    """Exact-type test: is *value* one of the data model's container kinds?

    **Never** ``isinstance``: a ``dict``/``list``/``tuple`` subclass is not a data
    model container, cannot be registered as fresh and cannot smuggle a live host
    object into the per-evaluation scope.
    """
    return type(value) in _FRESH_WALKABLE


def _is_mutable_container(value: Any) -> bool:
    """Exact-type test: can *value* be mutated in place by the DSL?"""
    return type(value) in _FRESH_MUTABLE


def _is_immutable_target(value: Any) -> bool:
    """Exact-type test: does *value* fail natively for any in-place mutator?"""
    return type(value) in _IMMUTABLE_TARGET_TYPES


@contextmanager
def freshness_scope():
    """Open the per-evaluation freshness scope (or share the active one).

    Nesting is detected through the ContextVar itself: an opener called while a
    scope is already active for this context yields the active scope unchanged
    and does **not** reset it, so it is the *outermost* entry that owns the
    lifetime.
    """
    if _FRESH_SCOPE.get() is not None:
        yield _FRESH_SCOPE.get()          # nested: same logical evaluation
        return
    scope: Dict[int, Any] = {}
    token = _FRESH_SCOPE.set(scope)
    try:
        yield scope
    finally:
        _FRESH_SCOPE.reset(token)         # dropped on return **and** on exception


def _fresh_note(obj: Any) -> None:
    """Mark *obj* as engine-created **in the active evaluation scope**."""
    scope = _FRESH_SCOPE.get()
    if scope is None:
        return                            # outside an evaluation: fail-closed
    key = id(obj)
    if scope.get(key) is obj:
        return
    scope[key] = obj
    while len(scope) > _FRESH_BUDGET:
        scope.pop(next(iter(scope)))      # oldest first (insertion ordered)


def _fresh_has(obj: Any) -> bool:
    """Was *obj* created by the engine during the **current** evaluation?"""
    scope = _FRESH_SCOPE.get()
    if scope is None:
        return False
    return scope.get(id(obj)) is obj


def note_fresh_object(obj: Any) -> None:
    """Public hook: the engine just created *obj* (a mutable shell only)."""
    if _is_mutable_container(obj):
        _fresh_note(obj)


def note_fresh_closure(root: Any, budget: Optional[int] = None) -> None:
    """Public hook: the engine created *root* and everything reachable inside it.

    Used only where the engine really built the whole graph (JSON round-trip /
    engine-owned structural copy / materialised isolation boundary). The walk is
    bounded; on budget exhaustion the un-walked objects stay unregistered
    (fail-closed). Outside an active evaluation this is a no-op.

    The walk is **exact-type** (see ``_is_walkable_container``): the copier
    ``_engine_copy`` only ever produces exact ``dict``/``list``/``tuple``/``set``/
    ``frozenset`` nodes, so registration can never register a live host subclass
    — and a subclass that reaches here by any other route is skipped
    (fail-closed: it can never become a mutation target).
    """
    if _FRESH_SCOPE.get() is None:
        return
    if not _is_walkable_container(root):
        return
    if _is_mutable_container(root):
        _fresh_note(root)
    seen = {id(root)}
    stack: List[Any] = [root]
    remaining = _FRESH_WALK_BUDGET if budget is None else budget
    while stack:
        node = stack.pop()
        values: Any = node.values() if type(node) is dict else node
        for value in values:
            remaining -= 1
            if remaining < 0:
                return
            if not _is_walkable_container(value):
                continue
            if _is_mutable_container(value):
                _fresh_note(value)
            if id(value) not in seen:
                seen.add(id(value))
                stack.append(value)


def reset_freshness_registry() -> None:
    """Test hook: drop every freshness fact of the **active** evaluation scope.

    There is no process-wide registry to clear any more; outside an evaluation
    nothing is registered, so this is a no-op there.
    """
    scope = _FRESH_SCOPE.get()
    if scope is not None:
        scope.clear()


def freshness_registry_size() -> int:
    """Test hook: number of idents in the active evaluation scope (0 outside)."""
    scope = _FRESH_SCOPE.get()
    return 0 if scope is None else len(scope)


class _FreshnessPlan:
    """A gate plan: after the target thunk is evaluated, prove it is fresh."""

    __slots__ = ("label", "root", "callee")

    def __init__(self, label: str, root: Optional[str] = None,
                 callee: Optional[str] = None) -> None:
        self.label = label
        self.root = root
        self.callee = callee


def _enforce_fresh(value: Any, plan: "_FreshnessPlan", func_name: str) -> None:
    """Refuse (named) unless the evaluated mutation target is provably fresh."""
    if _is_immutable_target(value):
        # Cannot be rewritten in place at all: the mutator fails natively; an
        # "allow" here is not a write-through channel.  Exact type only — a
        # subclass may define a side-effectful ``__setitem__``.
        return
    if _fresh_has(value):
        return
    raise ValueError(_MUTATOR_UNPROVEN_FRESH_TEMPLATE.format(
        func=func_name, path=plan.label, root=plan.root or plan.label,
        callee=plan.callee or plan.label))


def _note_fresh_result(registry: Optional[Any], func_name: str, result: Any) -> None:
    """Register a producer result as created by this evaluation."""
    result_class = _callee_result_class(registry, func_name)
    if result_class == _RESULT_DEEP:
        note_fresh_closure(result)
    elif result_class == _RESULT_SHALLOW:
        # Only the new container is fresh: its elements may alias the arguments,
        # and a later mutation of an element must not be allowed.
        note_fresh_object(result)


def _callee_result_class(registry: Optional[Any], callee: str) -> str:
    """Classify what a call to *callee* can return (see the class constants)."""
    desc = _descriptor_for(registry, callee)
    if desc is not None and getattr(desc, "fresh_producer", False):
        return _RESULT_DEEP
    if callee in _DEEP_PRODUCER_FUNCTIONS:
        return _RESULT_DEEP
    if callee in _SHALLOW_PRODUCER_FUNCTIONS:
        return _RESULT_SHALLOW
    if callee in _LIVE_REFERENCE_FUNCTIONS or callee in _DEFAULT_MUTATOR_NAMES:
        return _RESULT_LIVE
    return _RESULT_UNKNOWN


def _thunk_alias_source(thunk: Thunk, context: Any, registry: Optional[Any],
                        prefix: str) -> Tuple[Optional[bool], Optional[str], Optional[str]]:
    """``(may_alias, source_path, source_root)`` for a compiled argument thunk.

    ``may_alias`` is ``True`` when the thunk's value can be (part of) an object
    reachable from a persisted root, ``False`` when the gate can prove it cannot,
    and ``None`` when it cannot be proven (unreadable state / undeclared callee).
    For a call the answer is derived from the **callee's own arguments** (r3 F2):
    a DEEP result is independent of them; a SHALLOW/LIVE result inherits the
    aliasing of its arguments; an UNKNOWN callee is fail-closed.
    """
    path = getattr(thunk, "_plaita_path", None)
    if path is not None:
        root_key, segments = path
        rendered = _render_path(root_key, list(segments))
        may_alias = _path_alias_state(root_key, segments, context, prefix)
        if may_alias is False:
            # r4：名字判据之外的身份兜底——容器里任意键名挂着活根时，这个路径参数
            # 同样是活引用。预算耗尽 ⇒ 不能证明 ⇒ None（调用方 fail-closed）。
            probed = _probe_container_alias(context)
            if probed is not None:
                if probed[0] == "budget":
                    return None, None, None
                return True, rendered, probed[2]
        return may_alias, rendered, root_key
    callee = getattr(thunk, "_plaita_call", None)
    if callee is None:
        return False, None, None              # literal / constant thunk
    result_class = _callee_result_class(registry, callee)
    if result_class == _RESULT_DEEP:
        return False, None, None
    if result_class == _RESULT_UNKNOWN:
        return None, None, None               # fail-closed
    args = getattr(thunk, "_plaita_args", ())
    if not args:
        return None, None, None               # no argument to derive provenance from
    unproven = False
    first: Optional[Tuple[Optional[str], Optional[str]]] = None
    for argument in args:
        may, source_path, source_root = _thunk_alias_source(argument, context,
                                                            registry, prefix)
        if may is None:
            unproven = True
        elif may:
            return True, source_path, source_root
        elif first is None:
            first = (source_path, source_root)
    if unproven:
        return None, None, None
    if first is not None:
        return False, first[0], first[1]
    return False, None, None


def _argument_note(position: int) -> str:
    return "" if position == 0 else f" as argument #{position + 1}"


class _EngineCopyRefused(TypeError):
    """Internal: ``_engine_copy`` met a value outside the persistable data model.

    ``type_name`` is always the **real** ``type(value).__name__`` (r8 G3): a host
    class that spoofs ``__class__`` to look like ``dict``/``list``/… passes
    ``isinstance`` but fails the C base-class descriptor, so the refusal carries
    the spoofed class's real name plus a ``detail`` explaining which base
    protocol rejected it — instead of a bare, unattributable ``TypeError``.
    """

    def __init__(self, type_name: str, detail: Optional[str] = None) -> None:
        super().__init__(type_name if detail is None else f"{type_name}: {detail}")
        self.type_name = type_name
        self.detail = detail


def _engine_copy_base_read(kind: type, reader: Callable[[Any], Any], value: Any) -> Any:
    """Run a **base-class** container protocol reader under the named-error contract.

    ``dict.items(sub)`` / ``list.__iter__(sub)`` / … never fall back to the host
    class's overrides, but they *do* reject an object whose real ``type`` is not
    the C container even when ``isinstance`` was fooled by a spoofed
    ``__class__``.  That rejection is converted here into the named
    ``_EngineCopyRefused`` (r8 G3) so callers always see an attributable error.
    """
    try:
        return reader(value)
    except TypeError as exc:
        raise _EngineCopyRefused(
            type(value).__name__,
            f"spoofed __class__? the value passes isinstance({kind.__name__}) but "
            f"not the {kind.__name__} base protocol ({exc}); it cannot be rebuilt "
            f"as a plain {kind.__name__}",
        ) from exc


def _scalar_base_str(value: Any) -> str:
    """``str`` 子类 → 普通 ``str``，只走 **C 基类** ``str.__str__``。

    r9 Finding A：r7/r8 用内建构造器 ``str(value)``，它会**落回宿主覆写的**
    ``__str__``/``__repr__`` —— 一个 ``__str__`` 返回
    ``'<LiarStr SECRET=…>'`` 的 ``str`` 子类会被**静默**写进数据模型（宿主 repr
    里的秘密变成数据，且无任何错误）。``str.__str__(sub)`` 是基类描述符，只读
    C 字符串存储。
    """
    return str.__str__(value)


def _scalar_base_bytes(value: Any) -> bytes:
    """``bytes`` 子类 → 普通 ``bytes``，只走 **C 基类描述符**，不调宿主钩子。

    r9 Finding A：``bytes(value)`` 会调用宿主覆写的 ``__bytes__``/``__iter__``/
    ``__len__``（实测 ``bytes(sub)`` 命中 ``__bytes__`` 覆写并抛裸 ``RuntimeError``）。
    r9 改用 ``bytes(memoryview(sub))``（buffer 协议）；r10 复核发现该式**在 3.12+
    仍会落回宿主钩子**：PEP 688 起 ``memoryview()`` 认 Python 级 ``__buffer__``，
    ``class EvilBytes(bytes): def __buffer__(self, flags): return memoryview(b"SECRET")``
    在 3.12/3.13 实测让 ``bytes(memoryview(v))`` 返回 ``b'SECRET'``（3.9–3.11 无此
    协议，故旧式在这三版"看起来"干净）—— 与 r9 的 str 秘密落地同一类**静默**数据
    损坏，DSL 可达。

    r10 改成 C 基类描述符 ``bytes.__getitem__(value, slice(None))``：它直接按
    CPython 的 bytes 存储做一次整体拷贝，返回**精确** ``bytes``，宿主覆写的
    ``__bytes__``/``__index__``/``__iter__``/``__len__``/``__getitem__``/
    ``__buffer__`` 一次都不进入（实测 3.9.6 / 3.10.21 / 3.11.16 / 3.12.x / 3.13.15
    三处读数一致）。``__class__`` 伪造的假 bytes 过不了该描述符的实体检查
    （``TypeError``）⇒ 由调用方具名拒绝。
    """
    return bytes.__getitem__(value, slice(None))


def _scalar_base_int(value: Any) -> int:
    """``int`` 子类（``IntEnum`` …）→ 普通 ``int``（基类 ``__int__`` 描述符）。"""
    return int.__int__(value)


def _scalar_base_float(value: Any) -> float:
    """``float`` 子类 → 普通 ``float``（基类 ``__float__`` 描述符）。"""
    return float.__float__(value)


def _scalar_base_complex(value: Any) -> complex:
    """``complex`` 子类 → 普通 ``complex``。

    ``complex.__reduce__`` 会落回宿主的 ``__complex__``（实测抛宿主异常），
    所以这里用 C 级 ``complex.__getnewargs__`` 读实部/虚部再重建。
    """
    return complex(*complex.__getnewargs__(value))


def _scalar_base_decimal(value: Any) -> Decimal:
    """``Decimal`` 子类 → 普通 ``Decimal``（``Decimal.__str__`` 读 C 数字，非宿主）。"""
    return Decimal(Decimal.__str__(value))


def _scalar_base_datetime(value: Any) -> datetime:
    """``datetime`` 子类 → 普通 ``datetime``，**逐字段保真**（含 PEP 495 ``fold``）。

    r7/r8 读 ``value.year``/``.month``/… —— 子类的 ``property`` 可以覆写这些属性。
    r9 改走 C 级 ``datetime.__reduce__``（宿主钩子零调用），但 ``__reduce__`` 是
    **protocol 2** 形态：它把 7 个分量 + ``tzinfo`` 打包成字节，**不含 ``fold``**
    （3.9.6 / 3.10.21 / 3.11.16 / 3.12.x / 3.13.15 实测一致；只有
    ``__reduce_ex__(v, 4)`` 才把 fold 位写进打包字节）。于是 DST 歧义墙钟时间被
    **静默**改成 ``fold=0``：``ZoneInfo("America/New_York")`` 的
    2020-11-01 01:30 ``fold=1`` 的 ``timestamp()`` 从 1604212200.0 变成
    1604208600.0（差 3600s），而 ``type(copy) is datetime`` 与 ``copy == value``
    都仍成立（同一个 ``ZoneInfo`` 对象 ⇒ CPython 只比 naive 部分），无任何错误。

    r10：仍从**基类状态**重建（``datetime.__reduce__`` 读 C 结构体打包的
    year…microsecond + tzinfo），再用**基类** ``fold`` 描述符
    ``datetime.fold.__get__(value)`` 取值、由**基类** ``datetime.replace(d, fold=…)``
    写回 ``d``（``d`` 是本函数现造的精确 ``datetime``，所以结果必然是精确
    ``datetime``）。三个调用都显式走基类描述符：宿主子类覆写 ``fold`` property /
    ``replace`` / ``__reduce__`` / ``__reduce_ex__`` 都不会被进入（实测零钩子）。
    """
    built = datetime(*datetime.__reduce__(value)[1])
    return datetime.replace(built, fold=datetime.fold.__get__(value))


def _scalar_base_date(value: Any) -> date:
    """``date`` 子类 → 普通 ``date``（C 级 ``date.__reduce__``，同 datetime）。

    ``date`` 的可观察字段只有 ``year``/``month``/``day``（无 ``fold``/``tzinfo``），
    protocol 2 的打包字节把它们**全带上**（实测 ``date.__reduce__(date(1,1,1))``
    ⇒ ``b'\\x00\\x01\\x01\\x01'``），因此没有 datetime 那种缺字段问题。

    **``datetime`` 永远进不了这个读取器**：``_canonical_persistable_kind`` 按
    ``PERSISTABLE_SCALAR_TYPES`` 的**顺序**做 ``isinstance``，而 ``datetime`` 排
    在 ``date`` **之前**（``native_types`` 里 ``DATETIME`` 在 ``DATE`` 前，推导顺序
    稳定），所以 ``datetime`` 实例（含子类）先命中 ``datetime`` 那一格。
    这条顺序是保真性的一部分：``date.__reduce__(<datetime>)`` 只打包
    ``year/month/day``（实测返回 4 字节），若顺序翻转，时刻与 ``fold`` 会被静默丢掉。
    回归：``TestIssue69R10ScalarFieldPreservation::test_datetime_is_never_routed_to_the_date_reader``。
    """
    return date(*date.__reduce__(value)[1])


def _scalar_base_bool(value: Any) -> bool:
    """``bool`` 无子类（CPython 不允许继承）⇒ 只接受**精确** ``bool``。

    ``__class__`` 伪造的对象能骗过 ``isinstance(value, bool)``，但走不到任何
    "bool 基类协议"（``bool.__bool__`` 是 ``int`` 描述符，对假对象抛 ``TypeError``）
    —— 这里显式拒绝，由调用方包成具名 ``_EngineCopyRefused``（r9 Finding A：
    r7/r8 的 ``bool(value)`` 会调用宿主 ``__len__`` 并漏出裸 ``RuntimeError``）。
    """
    if type(value) is not bool:
        raise TypeError(
            f"{type(value).__name__} is not a real bool (bool is final, no subclasses)")
    return value


def _scalar_base_none(value: Any) -> None:
    """``NoneType`` 同理：只接受精确 ``None``，伪造的一律具名拒绝。"""
    if type(value) is not type(None):
        raise TypeError(f"{type(value).__name__} is not a real None")
    return None


#: Exact-type conversion of a persistable-scalar **subclass** to its plain base
#: value, so the host class never travels into persisted data.  Keyed by the
#: canonical kind from ``PERSISTABLE_SCALAR_TYPES``.
#:
#: plaita#69 **r9 Finding A**：本表只允许**基类协议**读取器（``str.__str__`` /
#: ``bytes.__getitem__(v, slice(None))`` / ``int.__int__`` / ``float.__float__`` /
#: ``complex.__getnewargs__`` / ``Decimal.__str__`` / ``datetime.__reduce__`` +
#: 基类 ``datetime.fold``/``datetime.replace`` / ``date.__reduce__``），**绝不**用内建
#: 构造器 ``str(value)``/``bytes(value)``/``bool(value)`` —— 它们会落回宿主覆写的
#: ``__str__``/``__repr__``/``__bytes__``/``__len__``/``__iter__``，把宿主 repr
#: （可能含秘密）静默写进数据模型，或漏出裸 ``RuntimeError``。读不出来的一律
#: ``TypeError`` ⇒ 由 :func:`_engine_copy` 具名拒绝。
#:
#: plaita#69 **r10**：① ``datetime`` 读取器补回 PEP 495 ``fold``（protocol 2 的
#: ``__reduce__`` 不带它，DST 歧义墙钟时间会被静默改掉 1 小时）；② ``bytes`` 读取器
#: 从 ``bytes(memoryview(v))`` 换成 C 基类描述符 ``bytes.__getitem__(v, slice(None))``
#: —— PEP 688（3.12+）起 ``memoryview()`` 会调用宿主 ``__buffer__``，旧式在 3.12/3.13
#: 上同样会把宿主字节静默当成数据。
_SCALAR_NORMALISERS: Dict[type, Callable[[Any], Any]] = {
    str: _scalar_base_str,
    bytes: _scalar_base_bytes,
    int: _scalar_base_int,
    float: _scalar_base_float,
    bool: _scalar_base_bool,
    complex: _scalar_base_complex,
    Decimal: _scalar_base_decimal,
    datetime: _scalar_base_datetime,
    date: _scalar_base_date,
    type(None): _scalar_base_none,
}


def _canonical_persistable_kind(value: Any) -> Optional[type]:
    """Canonical data-model kind *value* can be copied into, or ``None``.

    Exact canonical types map to themselves.  A **subclass** of a persistable
    container maps to its plain base kind — ``OrderedDict``/``defaultdict``/
    ``Counter`` → ``dict``, ``namedtuple`` → ``tuple``, any custom ``dict``/
    ``list``/``set`` subclass → the plain base: the host class is normalised away
    and never carried into the copy.  A subclass of a persistable **scalar**
    (``IntEnum``, ``str``/``int`` subclasses, ``Decimal``/``datetime``/``date``
    subclasses) maps to the immutable base, normalised by value.  Anything else
    is not part of the data model → ``None`` (the caller refuses, named).

    The *registration* half never calls this: ``note_fresh_closure`` /
    ``note_fresh_object`` accept **exact** types only, so a subclass can never be
    registered as fresh (see ``_is_walkable_container``).

    plaita#69 **r11 finding 5**：``isinstance(value, base)`` 在真实类型不命中时会
    回落去读 ``value.__class__``（``tp_getattro`` ⇒ 宿主 ``__getattribute__``）。
    宿主把它覆写成抛异常时，这里会漏出**裸** ``RuntimeError``（r6 起就存在，DSL
    可达）—— 与 r8 G3 的"绝不漏裸异常"承诺冲突。这里把它包成具名
    ``_EngineCopyRefused``；``type(value)`` 读的是 C 的 ``ob_type`` 槽，不经
    ``tp_getattro``，所以归因用的类型名仍然拿得到。
    """
    kind = type(value)
    if kind in PERSISTABLE_TYPES:
        return kind
    try:
        for base in PERSISTABLE_CONTAINER_TYPES:
            if isinstance(value, base):
                return base
        for base in PERSISTABLE_SCALAR_TYPES:
            if isinstance(value, base):
                return base
    except Exception as exc:                # noqa: BLE001 - 宿主 __getattribute__ 的任意异常
        raise _EngineCopyRefused(
            type(value).__name__,
            f"its host class raises from __getattribute__ while isinstance() probes "
            f"the persistable data model ({exc})",
        ) from exc
    return None


def unhashable_offender(copied: Any, original: Any) -> Tuple[Any, Any]:
    """定位"归一化后不可哈希"的**真实 offender**：返回 ``(copied_offender, original_offender)``。

    r10：r9 的具名拒绝用 ``type(original).__name__``，于是嵌套形状
    ``{(HashableDict({'a': 1}),): 1}`` 只点名外层的 ``'tuple'``（失败发生在
    ``hash(copied)`` 那一跳），归因不到真正的宿主类。``tuple`` 与 ``original`` 的
    元素按下标一一对应（``_engine_copy`` 逐项重建、保持顺序），因此可以一路下钻到
    真实的 offender。``frozenset`` 没有下标对应，退化为按 ``copied`` 侧报告（其成员
    已是引擎现造的数据模型类型）。

    只读 ``copied``（引擎现造的值）并做 ``hash``，零宿主钩子。
    """
    if (isinstance(copied, tuple) and isinstance(original, tuple)
            and len(copied) == len(original)):
        for copied_item, original_item in zip(copied, original):
            try:
                hash(copied_item)
            except TypeError:
                return unhashable_offender(copied_item, original_item)
    return copied, original


def _check_hashable_key(copied: Any, original: Any, role: str) -> None:
    """键/成员归一化后必须仍可哈希，否则**具名**拒绝（r9 Finding B）。

    ``class HashableDict(dict): __hash__ = object.__hash__`` 作为 ``set`` 成员会被
    归一化成普通 ``dict``，随后集合重建抛裸 ``TypeError: unhashable type: 'dict'``
    —— DSL 可达（``$INPUT={"s":{HashableDict(...)}}`` 走循环条件拷贝）。这里把
    "成员/键在数据模型里不可表示"钉成带**真实类型名**的 ``_EngineCopyRefused``。
    ``copied`` 是引擎现造的普通数据模型值，``hash()`` 不会触碰宿主钩子。

    r10：类型名取 :func:`unhashable_offender` 定位到的**最内层** offender（嵌套
    ``tuple`` 键不再只报 ``'tuple'``）。
    """
    try:
        hash(copied)
    except TypeError as exc:
        offender_copied, offender_original = unhashable_offender(copied, original)
        raise _EngineCopyRefused(
            type(offender_original).__name__,
            f"the {role} normalises to an unhashable "
            f"{type(offender_copied).__name__} and cannot be represented in the "
            f"data model ({exc})",
        ) from exc


def _engine_copy_members(kind: type, reader: Callable[[Any], Any], value: Any,
                         memo: Optional[Dict[int, Any]], kind_name: str) -> set:
    """``set``/``frozenset`` 的成员逐一归一化 + 可哈希校验（r9 Finding B）。"""
    out: set = set()
    for item in _engine_copy_base_read(kind, reader, value):
        copied = _engine_copy(item, memo)
        _check_hashable_key(copied, item, f"{kind_name} member")
        out.add(copied)
    return out


#: ``dict`` 子类里"真实 dict 存储之外还能自述内容"的那组映射协议（r10 Finding D1）。
#: 引擎的权威读数是基类 ``dict.items``/``dict.__len__``/…（真实存储）。
_DICT_STORAGE_PROTOCOL = ("items", "keys", "values", "__iter__", "__len__",
                          "__getitem__")

#: 其中"**容器的外延声明**"——``__iter__``/``__len__`` 决定这个对象**自称装了什么**
#: （``Mapping`` 混入、``ItemsLiar`` 这类"数据在别处"的形状都会覆写它们）。引擎读
#: 真实存储用的是 ``dict.items``/``dict.__len__``，与这两个覆写**无关**：一旦子类用
#: **Python 级**实现覆写其中之一，就说明"自述内容"与真实存储是两套，按 D 的口径
#: 具名拒绝（fail-closed）。
#:
#: 边界（**明说的残余**，r10）：只覆写 ``items``/``keys``/``values``/``__getitem__``
#: **且不带实例状态**的子类仍按 r7/r8 契约**归一化**（r8 G1 的 ``HookDict``：数据仍在
#: 真实存储里，基类协议读它、宿主覆写零调用）；带实例状态（数据在 ``self._d``）的
#: 形状由第三条判据拒绝 —— 见 :func:`reads_mapping_outside_dict_storage`。
_DICT_STORAGE_IDENTITY_PROTOCOL = ("__iter__", "__len__")

#: 声明"实例状态在真实 ``dict`` 存储之外"的**序列化钩子**（r11 clause iii 第 4 条）。
#:
#: ``__reduce__`` **刻意不在表内**：``Counter``（本仓已验证的负控）用 Python 级
#: ``__reduce__`` 覆写，而它的数据就在真实 ``dict`` 存储里 ⇒ 这个名字本身不构成
#: "存储之外"的证据（加进来会把 ``Counter`` 误拒、推翻 §M/§P 的负控契约）。
#: ``__getstate__``/``__setstate__``/``__reduce_ex__`` 才表达"实例状态另有一套
#: 序列化口径"；C 槽描述符（例如 ``OrderedDict.__reduce__``、
#: ``defaultdict.__reduce__``）一律不算。
_DICT_INSTANCE_STATE_HOOKS = ("__getstate__", "__setstate__", "__reduce_ex__")

#: ``__slots__`` 里不算"额外存储"的两项：前者就是实例字典（由 ``__dict__`` 判据管），
#: 后者是弱引用槽。``__slots__ = ()`` 与只含这两项的形态一律算**空**。
_SLOT_PLACEHOLDERS = ("__dict__", "__weakref__")

#: CPython C 类型的槽描述符：``OrderedDict.items`` / ``OrderedDict.__iter__`` 这类
#: 是**基类存储自己的**实现（C method descriptor / slot wrapper），不算宿主覆写。
_C_SLOT_DESCRIPTOR_TYPES = (
    types.MethodDescriptorType,
    types.WrapperDescriptorType,
    types.ClassMethodDescriptorType,
    types.GetSetDescriptorType,
    types.MemberDescriptorType,
)

#: C 描述符里**直接在对象布局上放数据**的两类（r12，clause iii 第 5 条）：
#: ``tp_members`` 生成的 ``member_descriptor``（每个实例一个 C struct 槽，
#: 例如 ``defaultdict.default_factory``）与 ``tp_getset`` 生成的
#: ``getset_descriptor``。它们既不是实例 ``__dict__``、也不是真实 ``dict`` 存储，
#: 却能让对象**在真实存储之外持有全部数据**：
#:
#:     class N_DDLiar(defaultdict):
#:         def __init__(self, d):
#:             super().__init__(None)
#:             self.default_factory = dict(d)   # 写进 member_descriptor 的 C 槽
#:         def items(self): return self.default_factory.items()
#:
#: 实测（CPython 3.9.6）：``dict.items(v) == {}``、``v.items() == {'a': 1}``、
#: ``object.__getattribute__(v,'__dict__') == {}`` ⇒ r11 的四条判据全不命中 ⇒
#: ``_engine_copy(v) == {}``、DSL 报成功。``types.MemberDescriptorType`` /
#: ``GetSetDescriptorType`` 已在 :data:`_C_SLOT_DESCRIPTOR_TYPES` 里（那是"是不是
#: C 实现"的问句），这里问的是"**是不是对象布局上的存储声明**"。方法/槽包装描述符
#: （``MethodDescriptorType``/``WrapperDescriptorType``/``ClassMethodDescriptorType``）
#: 不在此列：``defaultdict.__reduce__``、``OrderedDict.items`` 这类**不**声明存储。
_C_ON_OBJECT_STORAGE_DESCRIPTOR_TYPES = (
    types.MemberDescriptorType,
    types.GetSetDescriptorType,
)

_MISSING = object()

#: 读**真实** ``tp_mro`` / ``tp_dict`` 的 C 描述符（r11）。
#:
#: 宿主**元类**可以覆写 ``__getattribute__`` 来拒绝或伪造 ``cls.__mro__`` /
#: ``vars(cls)``：实测一个抛异常的元类让判据漏出**裸** ``RuntimeError``，而一个
#: "说谎"的元类能把声明了 ``__slots__`` 的基类从判据里抹掉 ⇒ 重新静默丢数据（正是
#: r11 要修的那类洞）。``type.__dict__`` 里这两个描述符直接读类型对象的 C 槽，
#: 不经元类的属性协议 —— 与 ``type(value)`` 读 ``ob_type`` 同一手法。
_REAL_MRO_GETTER = type.__dict__["__mro__"]
_REAL_CLASS_DICT_GETTER = type.__dict__["__dict__"]
_REAL_TYPE_FLAGS_GETTER = type.__dict__["__flags__"]

#: CPython ``Py_TPFLAGS_HEAPTYPE``（``Include/object.h``：``(1UL << 9)`` = ``0x200``）。
#:
#: 置位 ⇒ ``cls`` 的类型对象是**堆分配**的（运行期 ``class`` 语句 / ``type()`` /
#: ``PyType_FromSpec`` 造出来的类型）；清除 ⇒ 静态编译期类型（``dict``/``tuple`` 等）。
#:
#: **单独一个 heap 位判不出"Python 定义的类"**（r13 实测修正）：CPython 3.12/3.13 的
#: ``collections.defaultdict`` **自己就是堆类型**（``__flags__`` 实测 ``0x20405740``，
#: heap 位置位），只用 heap 会把精确 ``defaultdict`` 误拒、推翻已验证的负控契约。
#: 五个解释器实测（3.9.6/3.10.21/3.11.16/3.12.14/3.13.15，只看两位）：
#:
#: ==============  ==================  ====================
#: 类型              heap 位（0x200）     IMMUTABLE 位（0x100）
#: ==============  ==================  ====================
#: ``dict``        清（五版）           3.10+ 置 / 3.9 清
#: ``defaultdict`` 3.9–3.11 清，        置（五版）
#:                 **3.12/3.13 置**
#: ``OrderedDict`` 清（五版）           3.10+ 置 / 3.9 清
#: Python 类       置（五版）           清（五版）
#: ==============  ==================  ====================
#:
#: （裸解释器里 3.12/3.13 的 ``defaultdict.__flags__`` 是 ``0x20405740``；引擎进程里
#: 导入 ``pyparsing`` 后观测到 ``0x20485740``，多出的 ``0x80000`` 是无关的 version-tag
#: 位。判据只读 ``0x200``/``0x100`` 两位，与这个无关位无关。）
#:
#: 所以"Python 定义的类"的机械判据是 **堆类型 ∧ ¬不可变类型**：
#: :data:`_PY_TPFLAGS_IMMUTABLETYPE`（``1 << 8`` = ``0x100``，CPython 3.10 起）表示
#: "类型对象不可改属性" —— C 扩展类型（静态类型，或 ``PyType_FromSpec`` 带
#: ``Py_TPFLAGS_IMMUTABLETYPE`` 的堆类型，3.12 的 ``defaultdict`` 属于后者）置位，
#: Python ``class`` 语句造出来的类**不**置位（它的属性可以继续设置）。
#: 3.9 上该位尚未启用（所有类型都清），判据退化成"只看 heap"—— 而 3.9 上所有扩展类型
#: 都是静态类型（heap 位清），结论一致。见 :func:`_is_python_defined_class`。
_PY_TPFLAGS_IMMUTABLETYPE = 1 << 8
_PY_TPFLAGS_HEAPTYPE = 1 << 9


def _type_flags(cls: type) -> int:
    """``cls`` 的**真实** ``tp_flags``（不经元类 ``__getattribute__``）。"""
    return _REAL_TYPE_FLAGS_GETTER.__get__(cls, type(cls))


def _is_heap_type(cls: type) -> bool:
    """``cls`` 是否是**堆分配**的类型（``Py_TPFLAGS_HEAPTYPE``）。

    只读类型对象的 ``tp_flags`` 槽（``type.__dict__['__flags__']`` 的 getset 描述符），
    与 :func:`_real_mro` / :func:`_class_namespace` 同一手法：宿主**元类**覆写
    ``__getattribute__`` 也伪造不了这个读数。
    """
    return bool(_type_flags(cls) & _PY_TPFLAGS_HEAPTYPE)


def _is_immutable_type(cls: type) -> bool:
    """``cls`` 是否**不可改属性**（``Py_TPFLAGS_IMMUTABLETYPE``，CPython 3.10+）。

    C 扩展类型（静态类型，或带该 flag 的 ``PyType_FromSpec`` 类型）置位；Python
    ``class`` 语句造出来的类不置位。3.9 上该位未启用 ⇒ 恒 ``False``。
    """
    return bool(_type_flags(cls) & _PY_TPFLAGS_IMMUTABLETYPE)


def _is_python_defined_class(cls: type) -> bool:
    """``cls`` 是否是**Python 定义的类**（``class`` 语句 / ``type()``；r13 布局判据用）。

    判据 = ``_is_heap_type(cls) and not _is_immutable_type(cls)`` —— 即"类型对象是
    运行期造出来的，**并且** Python 还能继续改它的属性"。这才是"这个 ``tp_dict`` 里的
    槽描述符可能是 Python 类体的 ``__slots__`` 写的"的机械问句：

    * ``defaultdict``：3.9–3.11 不是堆类型 ⇒ ``False``；3.12/3.13 是堆类型**但**
      带 ``IMMUTABLETYPE``（``0x20405740``）⇒ ``False`` ⇒ 精确 ``defaultdict`` 的
      ``default_factory`` 槽照常豁免（负控契约不因版本翻车）；
    * 任意 Python 类（含 ``Counter``/``NodeSpec``/``HiddenSlotsLiar``）：堆类型且可改
      ⇒ ``True`` ⇒ 它**自己** ``tp_dict`` 里的槽描述符一律计入（即使 ``__slots__``
      声明名被删/清空）。

    fail-closed 方向：一个 **C 扩展在运行期造的、没带 IMMUTABLETYPE 的**堆类型会被
    判成"Python 定义的类" ⇒ 它自己 ``tp_dict`` 里的 on-object 描述符会被拒。这类形状
    没有真实用例，取 fail-closed；见 ``reads_mapping_outside_dict_storage`` 的过拒段。
    """
    return _is_heap_type(cls) and not _is_immutable_type(cls)


def _real_mro(cls: type) -> Tuple[type, ...]:
    """``cls`` 的**真实** MRO（``tp_mro``），不经元类 ``__getattribute__``。"""
    return tuple(_REAL_MRO_GETTER.__get__(cls, type(cls)))


def _class_namespace(klass: type) -> Any:
    """``klass`` 的**真实**类命名空间（``tp_dict`` 的 mappingproxy），不经元类钩子。"""
    return _REAL_CLASS_DICT_GETTER.__get__(klass, type(klass))


def dict_storage_protocol_overrides(cls: type) -> Tuple[str, ...]:
    """``cls`` 用 **Python 级**实现覆写的 ``dict`` 映射协议名（r10 Finding D1）。

    逐层扫 ``cls` 的**真实** MRO 里 ``dict`` **之前**的类，只看**真实**类命名空间
    （``_class_namespace``，不经元类 ``__getattribute__``）中
    ``_DICT_STORAGE_PROTOCOL`` 的名字：实现是 C 槽描述符（``OrderedDict`` 的
    ``items``/``keys``/``values``/``__iter__`` 都是 C 的）就跳过，其余（函数 /
    ``property`` / 任意对象，含静态/类方法）都算"宿主自述"。

    这个判据是**机械可判且零宿主钩子**的：只看类对象，不实例化、不调用任何方法。
    返回值同时用于**具名消息**（点名被覆写的方法）。
    """
    if cls is dict or not issubclass(cls, dict):
        return ()
    mro = _real_mro(cls)
    try:
        stop = mro.index(dict)
    except ValueError:                          # 理论上不可达（issubclass 已保证）
        return ()
    found: List[str] = []
    for klass in mro[:stop]:
        namespace = _class_namespace(klass)
        for name in _DICT_STORAGE_PROTOCOL:
            implementation = namespace.get(name, _MISSING)
            if implementation is _MISSING:
                continue
            if isinstance(implementation, (staticmethod, classmethod)):
                implementation = implementation.__func__
            if not isinstance(implementation, _C_SLOT_DESCRIPTOR_TYPES):
                found.append(name)
    return tuple(sorted(set(found)))


def _mro_before_dict(cls: type) -> Tuple[type, ...]:
    """**真实** MRO 里 ``dict`` **之前**的那一段（``dict``/``object`` 自己的层不算）。"""
    mro = _real_mro(cls)
    try:
        return mro[:mro.index(dict)]
    except ValueError:                      # 理论上不可达（只在 dict 子类上调用）
        return ()


def _normalised_slot_names(klass: type) -> Tuple[Any, ...]:
    """``klass`` 真实命名空间里的 ``__slots__`` 归一化成"额外存储槽"名字元组（零宿主钩子）。

    * 字符串简写（``__slots__ = "_d"``）→ 单元素元组；
    * ``tuple``/``list``/``set``/``frozenset``/``dict``（``dict`` 的键是槽名）→ 取其元素；
    * 任何**其它**形态（含这些内建类型的子类）→ 不迭代（迭代会进入宿主钩子），
      当作"声明了额外存储"（fail-closed，返回一个哨兵名）；
    * ``__dict__`` / ``__weakref__`` 不算额外存储（前者由``__dict__`` 判据管）。
    """
    raw = _class_namespace(klass).get("__slots__", _MISSING)
    if raw is _MISSING:
        return ()
    if type(raw) is str:
        names: Tuple[Any, ...] = (raw,)
    elif type(raw) in (tuple, list, set, frozenset, dict):
        names = tuple(raw)
    else:
        return ("<opaque __slots__>",)      # fail-closed：无法静态判空
    return tuple(name for name in names if name not in _SLOT_PLACEHOLDERS)


def declared_instance_slots(cls: type) -> Tuple[Any, ...]:
    """MRO 里 ``dict`` 之前任一层声明的**非空** ``__slots__``（r11 clause iii 第 2 条）。

    覆盖字符串简写、``tuple``/``list``/``set``/``dict`` 形态，以及**继承/合并**的槽
    （基类与 mixin 上的声明都在 MRO 里，逐层扫即可）。``__slots__ = ()`` 与只含
    ``__dict__``/``__weakref__`` 的形态算空。

    只按"名字非空"判定是**充分**的方向，但它**不是**"额外存储"的全部：对象在真实
    ``dict`` 存储之外能持有的 **on-object** 状态有五个来源 —— 实例 ``__dict__``、
    ``__slots__``、遮蔽 ``__dict__`` 的描述符、序列化钩子，以及 **MRO 里 ``dict``
    之前某一层命名空间里真实存在的 on-object 描述符**（r12 新增的第 5 条；r13 改成
    **布局判据**，不再问"名字是不是 ``__slots__``"）。最后一条里有两个形状：r11 那条
    **已被推翻的引理**的所在 —— ``defaultdict.default_factory`` 是 ``member_descriptor``
    （C struct 的成员槽，声明层 ``defaultdict`` ≠ ``cls``），一个 Python 子类把全部数据
    写进去时 ``dict.items(v) == {}``（真实存储是空的）、实例 ``__dict__`` 也是空的、没有
    ``__slots__``、没有遮蔽、没有序列化钩子 —— 四条旧判据全不命中；**以及** r12 漏掉的
    堆类型自己声明的槽 —— ``class HiddenSlotsLiar(dict): __slots__ = ("_store",)``
    在 ``cls`` **自己的** ``tp_dict`` 里留下 ``member_descriptor``，之后
    ``del cls.__slots__``（或 ``cls.__slots__ = ()``）只抹掉**名字**。本函数只管第一条，
    第 5 条由 :func:`declared_c_storage_descriptors` 管（它按 :func:`_is_python_defined_class`
    区分"扩展类型自己的槽"与"Python 类自己的槽"）；五条都不命中，引擎才认为真实
    ``dict`` 存储就是全部内容。有槽 ⇒ 引擎**无法**在不调用宿主钩子的前提下证明槽是空的
    ⇒ fail-closed。
    """
    found: List[Any] = []
    for klass in _mro_before_dict(cls):
        found.extend(_normalised_slot_names(klass))
    return tuple(found)


def declared_c_storage_descriptors(cls: type) -> Tuple[Tuple[str, str], ...]:
    """MRO 里 ``dict`` **之前**的层声明的 on-object 存储描述符（r12 第 5 条，r13 改按布局）。

    逐层扫 ``_mro_before_dict(cls)`` 的**真实**类命名空间（``_class_namespace``，不经
    宿主元类 ``__getattribute__``），收集 C member/getset 描述符
    （:data:`_C_ON_OBJECT_STORAGE_DESCRIPTOR_TYPES`）；返回 ``(属性名, 声明层类名)``
    元组，按属性名排序、去重。命名空间里除 ``__dict__``/``__weakref__``
    （:data:`_SLOT_PLACEHOLDERS`，标准实例字典槽与弱引用槽，由 clause iii 第 1/3 条管）
    之外的任何 C 成员/getset 都算。

    **判据是布局的，不是名字的（r13）**。一个 ``member_descriptor``（``tp_members``
    生成的 per-instance C struct 槽）出现在某一层命名空间里，含义是"**这一层**声明了
    一个实例槽"。它**不是**"只有 C 扩展才写得出"——Python 类体的 ``__slots__`` 会让
    解释器在该类**自己的** ``tp_dict`` 里生成 ``member_descriptor``：

        class HiddenSlotsLiar(dict):
            __slots__ = ("_store",)          # cls 自己的 tp_dict 里出现 member_descriptor
            def __init__(self, d): self._store = dict(d)
            def items(self): return self._store.items()
        del HiddenSlotsLiar.__slots__            # 抹掉声明名，槽描述符仍在

    r12 的措辞（"C 描述符只可能由拥有 C 布局的类型在自己的 ``tp_dict`` 里生成，Python
    类体写不出 ``member_descriptor``"）**是假的**，且 r12 的 `if klass is cls: continue`
    让**声明层就是 cls 自己**的槽一律豁免 ⇒ 上例在 r12 下：``declared_instance_slots``
    为 ``()``（clause 2 按**名字** ``__slots__`` 找）、本函数为 ``()``（跳过 ``cls``）、
    ``instance_state_signal(v) is None`` ⇒ 两条拷贝路径静默产出 ``{}``、DSL **报成功**。
    三个等价形态都复现（``del cls.__slots__`` / 事后 ``cls.__slots__ = ()`` / 元类
    ``__new__`` 里设置后再删）。

    r13 规则（只看**每个层命名空间里真实存在的描述符**）：

    1. 逐层枚举 :data:`_C_ON_OBJECT_STORAGE_DESCRIPTOR_TYPES`（member/getset），忽略
       :data:`_SLOT_PLACEHOLDERS`（``__dict__``/``__weakref__``，它们不是"额外"存储）；
    2. **豁免**声明层 ``klass is cls`` **且** ``cls`` 不是 :func:`_is_python_defined_class`
       （即扩展类型：静态 C 类型，或 3.12+ 那种带 ``IMMUTABLETYPE`` 的
       ``PyType_FromSpec`` 堆类型 —— ``dict``/``defaultdict``/``OrderedDict`` 等）：
       这正是 :data:`_C_ON_OBJECT_STORAGE_DESCRIPTOR_TYPES` 注释里那个"由它自己的
       ``tp_members`` 生成"的动机场景（``defaultdict.default_factory`` 必须照常
       **归一化**，否则推翻已验证的负控契约）。**为什么不是裸 ``Py_TPFLAGS_HEAPTYPE``**：
       3.12/3.13 上 ``defaultdict`` 自己是堆类型（实测 ``0x20405740``），只看 heap 位
       就会把精确 ``defaultdict`` 误拒 —— 判据因此是"堆类型 ∧ ¬不可变类型"；
    3. 其余一律计入 ⇒ **Python** 类自己 ``tp_dict`` 里的槽描述符**无论
       ``__slots__`` 声明名是否还在**都被看见；**子类**继承到的扩展类型层描述符（声明层
       ``defaultdict`` ≠ ``cls``）照旧被看见。

    判据零宿主钩子（只读类对象，不实例化、不调用任何宿主方法）。
    """
    found: List[Tuple[str, str]] = []
    python_defined = _is_python_defined_class(cls)
    for klass in _mro_before_dict(cls):
        for name, implementation in _class_namespace(klass).items():
            if name in _SLOT_PLACEHOLDERS:
                continue
            if not isinstance(implementation, _C_ON_OBJECT_STORAGE_DESCRIPTOR_TYPES):
                continue
            if klass is cls and not python_defined:
                # 扩展类型自己的 tp_members/tp_getset：是它自己的存储口径，
                # 负控契约要求照常归一化（旧措辞把这条豁免说成"C 特性"，改成布局问句）。
                continue
            found.append((name, klass.__name__))
    return tuple(sorted(set(found)))


def _instance_dict_status(cls: type) -> Tuple[bool, bool]:
    """``(shadowed, has_standard_slot)``：类型是否遮蔽 / 是否提供标准实例字典槽。

    正常类的 ``__dict__`` 是 CPython 自动挂上的 ``getset_descriptor``
    （``types.GetSetDescriptorType``）。``class HidingDict(dict): @property def
    __dict__(self): return {}`` 用 ``property`` 顶替它 —— 此时
    ``object.__getattribute__(v, "__dict__")`` 会**调用宿主 getter**、读到的也不是
    真实实例字典。只看类对象（真实类命名空间 ``_class_namespace``），零宿主钩子。
    """
    for klass in _mro_before_dict(cls):
        implementation = _class_namespace(klass).get("__dict__", _MISSING)
        if implementation is _MISSING:
            continue
        if isinstance(implementation, (types.GetSetDescriptorType,
                                       types.MemberDescriptorType)):
            return False, True              # 标准实例字典槽（真实存储可读）
        return True, False                  # 被描述符遮蔽（property / 任意数据描述符）
    return False, False                     # 例如 __slots__ = () 的类型


def shadows_instance_dict(cls: type) -> bool:
    """类型是否用描述符**遮蔽**了 ``__dict__``（r11 clause iii 第 3 条）。"""
    return _instance_dict_status(cls)[0]


def declares_instance_state_hooks(cls: type) -> Tuple[str, ...]:
    """MRO 里 ``dict`` 之前任一层用**非 C 槽**实现声明的序列化钩子（r11 clause iii 第 4 条）。

    判据与 :func:`dict_storage_protocol_overrides` 同形：只看真实类命名空间，
    C 槽描述符跳过（``OrderedDict.__reduce__`` 是 C 的，不算宿主覆写）。
    """
    found: List[str] = []
    for klass in _mro_before_dict(cls):
        namespace = _class_namespace(klass)
        for name in _DICT_INSTANCE_STATE_HOOKS:
            implementation = namespace.get(name, _MISSING)
            if implementation is _MISSING:
                continue
            if isinstance(implementation, (staticmethod, classmethod)):
                implementation = implementation.__func__
            if not isinstance(implementation, _C_SLOT_DESCRIPTOR_TYPES):
                found.append(name)
    return tuple(sorted(set(found)))


def instance_state_signal(value: Any) -> Optional[str]:
    """实例是否**可能**在真实 ``dict`` 存储之外持有状态；返回触发的信号名或 ``None``。

    plaita#69 r11：r10 的 clause (iii) 测的是**代理**（"实例有非空 ``__dict__``"），
    不是它宣称的**性质**（"对象在真实 ``dict`` 存储之外持有状态"）。``__slots__``
    形状（``class SlotsItemsLiar(dict): __slots__ = ("_d",)``）没有实例 ``__dict__``，
    于是 r10 判 ``False`` ⇒ ``_engine_copy``/``_snapshot_value`` 静默产出 ``{}``
    且 DSL **报成功**；同族的 ``HidingDict``（``@property def __dict__`` 遮蔽真实
    实例字典）也判 ``False``。而只多一个 ``__iter__`` 的 ``SlotsIterLiar`` 却被
    clause (ii) 拒 —— 两者的差别只在**数据放在哪**，正是本条判据要机械判定的东西。

    判据是**五条机械可判、零宿主钩子**的析取（只读 ``type(value)`` 与各层
    ``_class_namespace(klass)``；不调用宿主 ``__getattribute__``/``items``/``__iter__``
    —— 类自身的读取也走 C 描述符 ``type.__dict__["__mro__"]`` /
    ``type.__dict__["__dict__"]``，所以宿主**元类**覆写 ``__getattribute__`` 同样进不来）：

    1. ``"instance state (__dict__)"`` —— 实例的真实 ``__dict__`` 非空（r10 原判据）；
    2. ``"instance state (__slots__)"`` —— MRO 里 ``dict`` 之前任一层声明了**非空**
       ``__slots__``（字符串简写 / 继承合并的槽 / mixin 上的槽）；
    3. ``"shadowed __dict__"`` —— 某层用描述符遮蔽了 ``__dict__``；
    4. ``"instance state hooks (...)"`` —— 某层用非 C 槽实现声明了
       :data:`_DICT_INSTANCE_STATE_HOOKS` 之一；
    5. ``"instance state (C descriptor <名> on <层>)"`` —— MRO 里 ``dict`` 之前的某一层
       命名空间里存在 C member/getset 描述符（r12 引入，**r13 改成布局判据**）—— 即
       :func:`declared_c_storage_descriptors` 非空。豁免**只有一条**：声明层
       ``is cls`` **且** ``cls`` **不是 Python 定义的类**（:func:`_is_python_defined_class`
       —— 即扩展类型；判据是 ``Py_TPFLAGS_HEAPTYPE`` ∧ ¬``Py_TPFLAGS_IMMUTABLETYPE``，
       因为 3.12/3.13 的 ``defaultdict`` 自己是堆类型，只看 heap 位会误拒精确
       ``defaultdict``）。这正是 ``defaultdict.default_factory`` 必须照常归一化
       的动机场景。**Python 类自己 ``tp_dict`` 里的槽描述符一律计入** ——
       哪怕 ``__slots__`` 声明名被删掉/清空：

           class HiddenSlotsLiar(dict):
               __slots__ = ("_store",)          # cls 自己的 member_descriptor
               def __init__(self, d): self._store = dict(d)
               def items(self): return self._store.items()
           del HiddenSlotsLiar.__slots__        # 抹掉名字，槽还在

       这条判据同时覆盖 r11/r12 的**规则级漏洞**（r13 改为布局判据）：

           class N_DDLiar(defaultdict):
               def __init__(self, d):
                   super().__init__(None)
                   self.default_factory = dict(d)   # member_descriptor 的 C 槽
               def items(self): return self.default_factory.items()

       实测 ``dict.items(v) == {}``、``v.items() == {'a': 1}``、
       ``object.__getattribute__(v,'__dict__') == {}`` ⇒ 前四条全不命中 ⇒
       ``_engine_copy(v) == {}``、``_snapshot_value({'m': v}) == {'m': {}}``、
       DSL ``$F.set($INPUT.cfg,'k','v')`` **报成功**（``raised=None``）而 ``cfg`` 未变。
       ``defaultdict.default_factory`` 在 ``defaultdict.__dict__`` 里是
       ``member_descriptor``（CPython 3.9.6/3.10.21/3.11.16/3.12.14/3.13.15 全部实测），
       且声明层 ``defaultdict`` **先于** ``dict`` —— 正是本条判据。

    ``__slots__ = ()`` / 只含 ``__weakref__``/``__dict__`` 的形态算空；C 描述符判据
    排除 ``__dict__``/``__weakref__`` 两个名字。负控
    （全部实测不命中）：``OrderedDict``/``defaultdict``/``Counter``/纯 ``dict`` 子类/
    精确 ``dict``/``NodeSpec``（``__slots__ = ()``）/``_IsolatedConditionContext``/
    r8 的 ``HookDict`` 与 r7 的 ``EvilDict``。**声明的过拒面（明说）**：无数据在
    存储之外的**良性 ``defaultdict`` 子类**（``class BenignDD(defaultdict): pass``）也
    被拒 —— ``default_factory`` 的声明层是 ``defaultdict``（≠ ``cls``）；同类还有
    stdlib 的 ``importlib.metadata._collections.FreezableDefaultDict``。见
    :func:`reads_mapping_outside_dict_storage` 的过拒代价段。
    """
    cls = type(value)
    shadowed, has_dict_slot = _instance_dict_status(cls)
    if shadowed:
        return "shadowed __dict__"
    if declared_instance_slots(cls):
        return "instance state (__slots__)"
    c_descriptors = declared_c_storage_descriptors(cls)
    if c_descriptors:
        return ("instance state (C descriptor "
                + ", ".join(f"{name} on {layer}" for name, layer in c_descriptors)
                + ")")
    hooks = declares_instance_state_hooks(cls)
    if hooks:
        return "instance state hooks (" + ", ".join(hooks) + ")"
    if not has_dict_slot:
        return None
    try:
        state = object.__getattribute__(value, "__dict__")
    except AttributeError:                  # 竞态兜底：槽在但没有实例字典
        return None
    return "instance state (__dict__)" if state else None


def instance_carries_own_state(value: Any) -> bool:
    """实例是否**可能**在真实 dict 存储之外持有状态（r11 广义判据）。

    r10 只测 ``__dict__`` 这个**代理**；r11 改成 :func:`instance_state_signal` 的
    广义判定（``__dict__`` / ``__slots__`` / 遮蔽的 ``__dict__`` / 序列化钩子；r12 再
    加"非 ``cls`` 层声明的 C member/getset 描述符"），仍然零宿主钩子。数据放
    ``self._d`` 的子类（``ItemsLiar``/``SlotsItemsLiar``/
    ``MappingBackedDict``/``HidingDict``）与"数据就在真实存储里"的子类（r8 的
    ``HookDict``、r7 的 ``EvilDict``、``AttrDict`` 之外的纯子类）由此**机械可分**。
    """
    return instance_state_signal(value) is not None


def reads_mapping_outside_dict_storage(value: Any) -> bool:
    """对象是否是"**自述内容**可能与真实 ``dict`` 存储不一致"的 ``dict`` 子类。

    r9 Finding D 只看了 ``class M(MutableMapping, dict)``（``Mapping`` 排在 ``dict``
    **之前**的 MRO）：

    * r7 的 ``_snapshot_value`` 走 ``Mapping.items`` ⇒ ``{'a': 1}``；
    * r8 的基类协议重建（``dict.items``）⇒ ``{}`` —— **静默丢数据**。

    r10 Finding D1：MRO 判据**漏掉镜像形状** —— 数据同样放在自有属性里、只是不用
    ``Mapping`` 基类的两个反例，两条拷贝路径都静默产出 ``{}`` 且 DSL 报"成功"：

    * ``class ItemsLiar(dict)``：数据在 ``_d``，用 Python 覆写
      ``items``/``keys``/``values``/``__iter__``/``__len__``/``__getitem__`` 自述；
    * ``class DictFirstMapping(dict, Mapping)``：``Mapping`` 排在 ``dict`` **之后**。

    判据因而扩成**四条机械可判、零宿主钩子的析取**（r12 把 clause iii 内部再扩一条）：

    1. ``Mapping`` 排在 ``dict`` 之前的 MRO（r9 D 原判据）；
    2. 用 **Python 级**实现覆写了**容器外延声明** ``__iter__``/``__len__``
       （自称装了什么，与真实存储是两套）；
    3. **实例可能在真实 ``dict`` 存储之外持有状态**（r11 把 r10 的代理判据换成
       性质判据）—— 见 :func:`instance_state_signal`：非空 ``__dict__`` ∨ MRO 里
       非空 ``__slots__`` ∨ 遮蔽的 ``__dict__`` ∨ 序列化钩子 ∨ **任一层命名空间里的
       on-object 描述符**（r12 clause iii 第 5 条；r13 起该条按**布局**判：豁免仅限
       "声明层 ``is cls`` 且 ``cls`` 不是 Python 定义的类"，见
       :func:`_is_python_defined_class`）。

    **为什么不取"覆写六个映射协议中的任何一个"**（验收建议的最宽口径）：那会把
    **r7/r8 已验证为好的契约**一起拒掉 —— ``HookDict``（只覆写 ``items()``、数据仍在
    真实存储，r8 G1 的快照回归要求它被**归一化**成普通 ``dict``、零钩子、非活对象）
    与 ``EvilDict``（覆写 ``items()``/``__getitem__``，r7 要求归一化）。引擎**不采信**
    宿主自述，所以"覆写 ``items()`` 且数据在存储里"与"覆写 ``items()`` 且数据在别处"
    在只看覆写名时**不可区分**；把前者的**数据位置**作为依据（判据 3）才能既抓住
    D1 的真实形状（数据在 ``self._d``），又不推翻已验证的归一化契约。

    **过拒代价（明说）**：带任意实例属性（例如缓存/元数据）或任何非空 ``__slots__``
    的 ``dict`` 子类会被具名拒绝，即使它的数据确实在真实存储里 —— 因为"实例状态是不是
    数据"无法在不调用宿主钩子的前提下判定。**r12 起再扩一面**：任何 ``defaultdict`` 的
    **Python 子类**（哪怕只是 ``class BenignDD(defaultdict): pass``、数据全在真实
    存储里）也被拒，因为 ``default_factory`` 的 ``member_descriptor`` 声明层是
    ``defaultdict``（≠ ``cls``）⇒ 引擎无法证明那个槽没被用来装数据；stdlib 的真实
    例子是 ``importlib.metadata._collections.FreezableDefaultDict``。这是判据
    第 5 条的**代价**，与 ``AttrDict``/``SlotsCacheDict`` 同类；取 fail-closed：需要它进
    数据模型时改用 ``ctx.evaluate`` / producer 从**已知良好**的数据重建（**不要**用
    ``dict(...)``：它读的是真实存储，对"数据在别处"的形状恰好返回 ``{}`` —— 见
    :func:`_engine_copy` 与 `MIGRATION.md`）。

    负控：``OrderedDict`` / ``defaultdict`` / ``Counter`` / 空 ``dict`` 子类 /
    ``NodeSpec`` / ``_IsolatedConditionContext`` 四条判据都不命中，实测不受影响
    （见 ``TestIssue69R10MappingStorageProtocol`` 与
    ``TestIssue69R11InstanceStateGeneralisation``）；**精确** ``OrderedDict``/
    ``defaultdict``/``Counter`` 也不命中（声明层就是 ``cls`` 自己，或只有被排除的
    ``__dict__``/``__weakref__``）。
    """
    cls = type(value)
    if cls is dict or not issubclass(cls, dict):
        return False
    mro = _real_mro(cls)
    try:
        if mro.index(Mapping) < mro.index(dict):
            return True
    except ValueError:                      # MRO 里没有 Mapping 或没有 dict
        pass
    overrides = dict_storage_protocol_overrides(cls)
    if any(name in overrides for name in _DICT_STORAGE_IDENTITY_PROTOCOL):
        return True
    return instance_state_signal(value) is not None


def mapping_storage_refusal_detail(value: Any) -> str:
    """D/D1/D2 具名拒绝的**可读原因**（两条拷贝路径共用，措辞一致）。

    只读类对象与 ``object.__getattribute__(value, '__dict__')``（且只在类型没遮蔽
    ``__dict__`` 时读），零宿主钩子。r11：第三条判据命中的信号**逐个点名**
    （``instance state (__slots__)`` / ``instance state (__dict__)`` /
    ``shadowed __dict__`` / ``instance state hooks (...)``）；r12 增加
    ``instance state (C descriptor <名> on <层>)``。
    """
    overrides = dict_storage_protocol_overrides(type(value))
    signals: List[str] = []
    if overrides:
        signals.append("overrides " + ", ".join(overrides) + " in Python")
    state_signal = instance_state_signal(value)
    if state_signal is not None:
        signals.append(f"carries {state_signal}")
    if not signals:
        signals.append("declares Mapping ahead of dict in its MRO")
    return ("also implements Mapping from elsewhere and/or "
            + " and ".join(signals)
            + ", so its declared contents are not the real dict storage")


def _engine_copy(value: Any, memo: Optional[Dict[int, Any]] = None) -> Any:
    """Structural copy owned by the engine (plaita#69 r6/r7) — **the one copy
    entry point** for data-model values on the DSL path.

    Replaces ``copy.deepcopy`` everywhere on that path.  The accepted set is
    ``plaita.core.types``' persistable data model (``dict`` / ``list`` /
    ``tuple`` / ``set`` / ``frozenset`` + the scalar set derived from
    ``native_types``).  Canonical kinds are copied by walking the **base class**
    implementation (``dict.items(value)``, ``list.__iter__(value)``, …), so a
    host class overriding ``__deepcopy__`` / ``__copy__`` / ``items`` /
    ``__iter__`` / ``__getitem__`` is never invoked and can never hand back the
    live object; container subclasses (``OrderedDict``, ``defaultdict``,
    ``namedtuple``, …) are **normalised** to plain base containers, scalar
    subclasses by value.  Every container returned here was constructed right in
    this function, so registering the result as fresh can never register a live
    object (the r6 C1 counterexample).

    Anything outside the data model raises the internal ``_EngineCopyRefused``
    (mapped to the named ``not part of the persistable data model`` error by the
    caller) — fail-closed: a genuine mutable non-data-model container is never
    silently shared.  That includes a host class whose **spoofed** ``__class__``
    fools ``isinstance`` but not the C base-class descriptor: the refusal carries
    the real ``type(value).__name__`` (r8 G3), so the DSL never surfaces a bare
    ``TypeError``.
    """
    kind = _canonical_persistable_kind(value)
    if kind is dict:
        if reads_mapping_outside_dict_storage(value):
            # r9 Finding D：`class M(MutableMapping, dict)` 的数据在 `Mapping` 侧，
            # 基类 `dict.items` 读到的存储是空的 —— r7/r8 会**静默**归一化成 `{}`。
            # r10 Finding D1：镜像形状（`ItemsLiar(dict)` / `dict, Mapping` 顺序）与
            # "数据在实例 `__dict__`"形状由扩后的判据抓到，消息给出具体信号。
            raise _EngineCopyRefused(
                type(value).__name__,
                f"this dict subclass {mapping_storage_refusal_detail(value)}; the "
                "base-class rebuild would silently produce an empty dict",
            )
        if memo is None:
            memo = {}
        cached = memo.get(id(value))
        if cached is not None:
            return cached
        out: Dict[Any, Any] = {}
        memo[id(value)] = out
        # Base-class protocol: a subclass's overridden items()/__getitem__ is
        # never called (it cannot hand us a live object behind our back).  A
        # spoofed __class__ is refused, named (r8 G3).
        for key, item in _engine_copy_base_read(dict, dict.items, value):
            copied_key = _engine_copy(key, memo)
            _check_hashable_key(copied_key, key, "dict key")
            out[copied_key] = _engine_copy(item, memo)
        return out
    if kind is list:
        if memo is None:
            memo = {}
        cached = memo.get(id(value))
        if cached is not None:
            return cached
        out_list: List[Any] = []
        memo[id(value)] = out_list
        out_list.extend(_engine_copy(item, memo)
                        for item in _engine_copy_base_read(list, list.__iter__, value))
        return out_list
    if kind is tuple:
        return tuple(_engine_copy(item, memo)
                     for item in _engine_copy_base_read(tuple, tuple.__iter__, value))
    if kind is set:
        return _engine_copy_members(set, set.__iter__, value, memo, "set")
    if kind is frozenset:
        return frozenset(_engine_copy_members(
            frozenset, frozenset.__iter__, value, memo, "frozenset"))
    if kind is not None:
        if type(value) is kind:
            return value                      # exact immutable scalar: nothing to copy
        try:
            return _SCALAR_NORMALISERS[kind](value)
        except (TypeError, ValueError) as exc:
            # r8 G3：伪造成标量的宿主对象（__class__ spoofing）也不能漏出裸
            # TypeError —— 统一具名拒绝，名字取真实类型。
            raise _EngineCopyRefused(
                type(value).__name__,
                f"spoofed __class__? the value passes isinstance({kind.__name__}) but "
                f"cannot be normalised to a plain {kind.__name__} ({exc})",
            ) from exc
    raise _EngineCopyRefused(type(value).__name__)


#: r11 finding 6 / r12：**被自称是视图、但不是引擎自产 C 视图类型**的对象被具名拒绝时
#: 的附加说明（与 :data:`_MUTATOR_DETACH_NOT_DATA_MODEL_TEMPLATE` 拼在一起）。两种机制
#: 都涵盖：ABC 视图（``__iter__``/``__len__`` 是 Python 实现、要读 ``self._mapping``）
#: 与**宿主 ABC 注册**的非视图类型（``KeysView.register(LieList)``：解析到的 C 描述符
#: 属于 ``list`` 的布局而不是任何视图布局）。
_MUTATOR_DETACH_HOST_VIEW_DETAIL = (
    " — in addition it is a **dict view** that is not one of the C view types this "
    "engine itself produces (`dict_keys`/`dict_values`/`dict_items`/`odict_*`): its "
    "`__iter__`/`__len__` either resolve to a host Python implementation or belong to "
    "another type's layout (the object only classifies as a view through "
    "`collections.abc` registration, which the host controls), so materialising it "
    "would call host code and persist its **self-described** contents "
    "(plaita#69 r11 finding 6, narrowed in r12)"
)

#: r12：引擎自产视图**在物化那一跳失败**时的附加说明（``list(value)`` 抛任何异常都被
#: 包成具名 ``plaita#69`` 拒绝，绝不泄露裸异常）。
_MUTATOR_DETACH_VIEW_MATERIALISATION_FAILED_DETAIL = (
    " — in addition it is a **dict view** whose materialisation (`list(value)`) raised "
    "before the engine-owned structural copy could run, so the value cannot be "
    "detached (plaita#69 r12)"
)


def _is_dict_view_type(cls: type) -> bool:
    """``cls`` 是否**自称**是 dict 视图（真实 MRO 子类或 ABC **虚拟注册**；零宿主钩子）。

    ``isinstance(value, _VIEW_TYPES)`` 走 ``ABCMeta.__instancecheck__``，它会先
    ``getattr(instance, "__class__")`` —— 宿主 ``__getattribute__`` 抛异常时同样漏出
    裸异常（r11 finding 5 的同族）。这里只看 ``type(value)``：真实 MRO 子类与虚拟注册
    （``KeysView.register(dict_keys)``）都照旧命中，且**不碰实例**。

    r12：本判据只回答"要不要按视图**处理**"（命中就走 :func:`_detach_alias` 的具名
    拒绝/物化分支）——**物化**还要过 :func:`_dict_view_is_materialisable` 的精确类型闸门
    （ABC 注册是宿主可控的，不能作为"可以物化"的依据）。
    """
    return issubclass(cls, _VIEW_TYPES)


def _resolved_slot_implementation(cls: type, name: str) -> Optional[str]:
    """``name`` 在 ``cls` 上**实际会被调用**的实现是 C 槽还是 Python 实现。

    沿真实 MRO（``_real_mro``）找第一个提供 ``name`` 的命名空间（属性查找的真实顺序），
    返回 ``"c"`` / ``"python"``；都没有则 ``None``。只看类对象，零宿主钩子。

    r12 警告：``"c"`` **只**说明这个描述符是 C 实现，**不**说明它属于 ``cls`` 实例的
    真实布局 —— ``class V1(KeysView): __iter__ = dict.__iter__`` 会解析到 ``dict`` 的
    包装描述符（``"c"``）而调用时抛 ``TypeError``。因此物化闸门
    （:func:`_dict_view_is_materialisable`）必须**先**做精确类型交集。
    """
    for klass in _real_mro(cls):
        implementation = _class_namespace(klass).get(name, _MISSING)
        if implementation is _MISSING:
            continue
        if isinstance(implementation, (staticmethod, classmethod)):
            implementation = implementation.__func__
        return "c" if isinstance(implementation, _C_SLOT_DESCRIPTOR_TYPES) else "python"
    return None


def _dict_view_is_materialisable(cls: type) -> bool:
    """``cls`` 是否**引擎自产的 C 视图类型**、只能用 C 迭代器/长度物化（r11/r12）。

    ``list(value)`` 会调用 ``__iter__``（并用 ``__len__`` 做长度提示）。只有引擎自己
    产出的视图 —— ``dict.keys/values/items`` 的 ``dict_keys``/``dict_values``/
    ``dict_items``，以及 C 派生 ``OrderedDict`` 的 ``odict_keys``/``odict_values``/
    ``odict_items``（:data:`_ENGINE_VIEW_TYPES`）—— 两者的实现才都是该视图**自己布局
    上**的 C 槽描述符 ⇒ 物化零宿主钩子。

    r12 finding：r11 只检查"解析到的实现是 C 描述符**类型**"，没有检查"这个描述符
    **属于该对象的真实布局**"，于是两条旁路都过了闸：

    * ``class V1(KeysView): __iter__ = dict.__iter__; __len__ = dict.__len__``：
      ``dict.__iter__`` 确实是 C 包装描述符 ⇒ 判 True ⇒ ``list(v1)`` 抛**裸**
      ``TypeError: descriptor '__iter__' requires a 'dict' object but received a 'V1'``
      （DSL 可达、消息里没有 ``plaita#69``）；
    * ``KeysView.register(LieList)``（``LieList(list)`` 不覆写任何名字）：ABC **虚拟
      注册**让 :func:`_is_dict_view_type` 为真，而 ``list.__iter__``/``list.__len__``
      是 C 描述符 ⇒ 判 True ⇒ 宿主登记的**非视图**类型被当视图物化（实测拿到
      ``['LIE']``）。

    所以判据改成**先按真实类型取交集**（``cls in _ENGINE_VIEW_TYPES``，不经
    ``issubclass``/ABC 注册），再要求两个名字都解析到 C 槽描述符。真视图类型在五个
    解释器上都**不能做基类**（``type 'dict_keys' is not an acceptable base type``），
    所以"宿主子类"这一档不存在；能过 :func:`_is_dict_view_type` 而在这里被挡下的就是
    ABC 视图与**虚拟注册**的类型 ⇒ 具名拒绝。
    """
    if cls not in _ENGINE_VIEW_TYPES:
        return False
    return (_resolved_slot_implementation(cls, "__iter__") == "c"
            and _resolved_slot_implementation(cls, "__len__") == "c")


def _detach_alias(value: Any, func_name: str, plan: Tuple[str, int, Optional[str],
                                                         Optional[str]]) -> Any:
    """Detach a shallow-producer result so it cannot alias a persisted root (F1).

    Dict views cannot be copied directly; they are materialised into a plain list
    first (a view is not indexable, so the detached list is the usable form).  The
    copy is the **engine-owned structural copy** ``_engine_copy`` (r6/r7) —
    ``copy.deepcopy`` is gone from this path, so a host class overriding
    ``__deepcopy__``/``__copy__`` (or ``items``/``__iter__``/``__getitem__``) can no
    longer smuggle the live object into the freshness scope, and container/scalar
    subclasses are normalised to plain data-model values instead of being refused.

    r11 finding 6：视图分支**只**对 ``__iter__``/``__len__`` 都解析到 C 槽描述符的
    视图（引擎自产的 ``dict_keys``/``dict_values``/``dict_items`` 及 C 派生）调用
    ``list(value)``；ABC 视图 / 宿主子类（实现是 Python，会读 ``self._mapping`` 并
    进入宿主 ``__getattribute__``）**具名拒绝**。r3/r8 起旧式实现是
    ``isinstance(value, _VIEW_TYPES)`` + ``list(value)``：实测宿主导出的
    ``ItemsView`` 子类会命中 ``['view.__iter__','view.__len__']``，宿主的自述内容被
    物化成数据（``_engine_copy(<view subclass>)`` 本身会具名拒绝，所以风险只限这一跳）
    —— r11 把这一跳也钉成零宿主钩子。

    r12：物化闸门改成"**精确类型必须是引擎自产的视图类型**"（见
    :func:`_dict_view_is_materialisable`），并且 ``list(value)`` **包在同一具名
    fail-closed 包装里** —— 于是

    * ``KeysView.register(LieList)``（宿主登记的普通 ``list`` 子类）不再被当视图物化；
    * ``class V1(KeysView): __iter__ = dict.__iter__`` 那种"解析到 C 描述符但不属于
      自己布局"的形状不再漏**裸** ``TypeError``，而是具名 ``plaita#69`` 拒绝；
    * 引擎自产视图若在物化时因任何原因失败（例如迭代中底层 dict 被并发改写），
      也变成具名拒绝而不是裸异常。
    """
    callee, position, path, root = plan
    if _is_dict_view_type(type(value)):
        if not _dict_view_is_materialisable(type(value)):
            raise ValueError(_MUTATOR_DETACH_NOT_DATA_MODEL_TEMPLATE.format(
                func=func_name, callee=callee, position=position + 1,
                path=path or f"$F.{callee}(...)", root=root or callee,
                type_name=type(value).__name__) + _MUTATOR_DETACH_HOST_VIEW_DETAIL)
        try:
            value = list(value)
        except Exception as exc:                # noqa: BLE001 - 具名化：不泄露裸异常
            raise ValueError(_MUTATOR_DETACH_NOT_DATA_MODEL_TEMPLATE.format(
                func=func_name, callee=callee, position=position + 1,
                path=path or f"$F.{callee}(...)", root=root or callee,
                type_name=type(value).__name__)
                + _MUTATOR_DETACH_VIEW_MATERIALISATION_FAILED_DETAIL) from exc
    try:
        detached = _engine_copy(value)
    except _EngineCopyRefused as exc:
        raise ValueError(_MUTATOR_DETACH_NOT_DATA_MODEL_TEMPLATE.format(
            func=func_name, callee=callee, position=position + 1,
            path=path or f"$F.{callee}(...)", root=root or callee,
            type_name=exc.type_name)) from exc
    # r6: only the containers this copier built are registered — and only while
    # the current evaluation scope is open (`note_fresh_closure` is a no-op
    # otherwise, which sends the value back to default-deny).
    note_fresh_closure(detached)
    return detached


def _gate_mutator_argument(target: Thunk, context: Any, registry: Optional[Any],
                           func_name: str, position: int = 0,
                           prefix: str = "$") -> Optional[Any]:
    """Classify one argument of an in-place mutator before it is evaluated.

    Returns ``None`` when the argument may be evaluated as-is, a ``_FreshnessPlan``
    when the value must be proven fresh **after** evaluation (r5 default-deny —
    the target position), or a detach plan ``(callee, position, path, root)`` when
    the value must be deep-copied before it is handed to the mutator (shallow
    producer over a persisted root, r3 F1).  Raises ``ValueError`` — with a
    **named path / callee and argument position** — for any argument whose value
    can reach the persisted context as a live reference, and for every undeclared
    callee (fail-closed).
    """
    path = getattr(target, "_plaita_path", None)
    if path is not None:
        root_key, segments = path
        rendered = _render_path(root_key, list(segments))
        if position == 0:
            # r6 热路径：目标位**唯一**的放行来源是求值后的 freshness 判定。
            # r4 身份探测里最贵的一段（`_path_alias_state` 对活 state 的非持久化键
            # 走 `_live_alias_index`，实测 ~8µs/次）只是补充证据：非 fresh 的目标
            # 必被 default-deny 独立拒绝。这里改成"单键查 + 容器值身份探测"：
            # 持久化键 ⇒ 立刻具名拒绝；容器里挂活对象 ⇒ 仍点名键路径与根（r4 语义
            # 保留）；其余 ⇒ 交给 freshness plan。
            persisted = _is_persisted_key_strict(context, root_key)
            if persisted is None:
                logger.warning("plaita#69 mutation gate: failing closed for %r", root_key)
                raise ValueError(_MUTATOR_UNMAPPED_TEMPLATE.format(
                    func=func_name, root=root_key, path=rendered))
            if persisted:
                raise ValueError(_MUTATOR_ROOT_TEMPLATE.format(
                    func=func_name, root=root_key, position=position + 1, path=rendered))
            probed = _probe_container_alias(context)
            if probed is not None and probed[0] != "budget":
                raise ValueError(_MUTATOR_CONTAINER_ALIAS_TEMPLATE.format(
                    func=func_name, path=rendered, alias_path=probed[1],
                    root=probed[2], position=position + 1))
            return _FreshnessPlan(label=rendered, root=root_key)
        may_alias = _path_alias_state(root_key, segments, context, prefix)
        if may_alias is None:
            logger.warning("plaita#69 mutation gate: failing closed for %r", root_key)
            raise ValueError(_MUTATOR_UNMAPPED_TEMPLATE.format(
                func=func_name, root=root_key, path=rendered))
        if may_alias:
            raise ValueError(_MUTATOR_ARG_ROOT_TEMPLATE.format(
                func=func_name, root=root_key, position=position + 1, path=rendered))
        # r4（结构半边 2）：名字说"不是根"不算数——按**对象身份**传递探测容器。
        # 值参位没有 freshness 判定兜底，身份探测是这里唯一的拒绝依据，必须做。
        probed = _probe_container_alias(context)
        if probed is not None and probed[0] != "budget":
            raise ValueError(_MUTATOR_CONTAINER_ALIAS_ARG_TEMPLATE.format(
                func=func_name, path=rendered, alias_path=probed[1],
                root=probed[2], position=position + 1))
        return None
    callee = getattr(target, "_plaita_call", None)
    if callee is None:
        return None                           # literal / template / other fresh value
    result_class = _callee_result_class(registry, callee)
    may_alias, source_path, source_root = _thunk_alias_source(
        target, context, registry, prefix)
    if result_class == _RESULT_DEEP:
        # Deep producers build a whole new object; declared fresh_producer=True
        # maps here too.  By construction the result is fresh.
        return None
    if result_class == _RESULT_SHALLOW:
        if position == 0:
            # Mutating a *new* container never reaches the persisted root: the
            # nine mutators only add/remove/replace entries of their target.
            return None
        if may_alias is True:
            return (callee, position, source_path, source_root)
        if may_alias is False:
            return None
        raise ValueError(_MUTATOR_UNDECLARED_PRODUCER_TEMPLATE.format(
            func=func_name, callee=callee, arg_note=_argument_note(position)))
    if result_class == _RESULT_LIVE:
        if may_alias is True:
            raise ValueError(_MUTATOR_LIVE_REFERENCE_TEMPLATE.format(
                func=func_name, callee=callee, arg_note=_argument_note(position),
                path=source_path or f"$F.{callee}(...)", root=source_root or callee))
        if position == 0:
            # `get`/`getDictValue`/`getListItem`/`ifelse`/`and`/`or` over
            # non-root-fresh data: the result may be a member of a fresh object
            # (then it was registered when the graph was built) or a host object
            # (then it was not).  Verify the identity at runtime.
            return _FreshnessPlan(label=f"$F.{callee}(...)", root=source_root,
                                  callee=callee)
        return None
        return None
    raise ValueError(_MUTATOR_UNDECLARED_PRODUCER_TEMPLATE.format(
        func=func_name, callee=callee, arg_note=_argument_note(position)))


def _mapping_get(obj: Any, path: str) -> Any:
    """Read *path* as a **key** of a mapping-like *obj* (data path).

    Dunder *path segments* are rejected (plaita#55) — see the gate comment above;
    other ``_``-prefixed keys stay readable because they are keys, not
    attributes.  The key remains readable through the explicit ``$F.*`` mapping
    helpers, which do not go through this resolver.
    """
    if path.startswith(_RESERVED_DUNDER_PREFIX):
        _reject_reserved_field(path)
    return obj.get(path, None)


#: plaita#163（#69 r13 确认的前置残留）：路径遍历的 ``isinstance`` 探测在宿主
#: ``__class__`` 抛异常时的**具名拒绝**。
#:
#: 为什么探测会进宿主代码：``isinstance(x, C)`` 在**真实类型不命中**时会回落读
#: ``x.__class__``（``tp_getattro`` ⇒ 宿主 ``__getattribute__`` / ``property``）。
#: 宿主在那里抛异常时，探测把**裸**异常冒到用户侧（r6 起就存在、DSL 可达），与
#: r11（``_canonical_persistable_kind``）/ r13（``_engine_mapping_view``）建立的
#: 「能进宿主代码的读取要么走 C 槽、要么具名拒绝」冲突。与
#: :data:`_MAPPING_VIEW_PROBE_FAILED_TEMPLATE` 是同一条纪律，只是发生在**路径遍历**
#: 这一跳（同形缺陷在 ``_engine_mapping_view`` 直调时已具名，先经过路径遍历就漏）。
#:
#: 刻意**不回显**宿主异常的文本：宿主控制它，可能带凭据/对象内部状态（与 r13 的
#: probe-failed 模板同一处置）。
_TRAVERSAL_PROBE_FAILED_TEMPLATE = (
    "expression path {path!r} read a value whose host class {type_name!r} raises "
    "{exc_type} from __getattribute__ while the engine probes isinstance({kind}), "
    "so the engine cannot tell whether the value is engine data. A read that enters "
    "host code must either go through a C slot or be refused by name (plaita#69). "
    "Make the host class answer __class__ normally, or hand the engine a plain "
    "dict/list/str built from known-good data."
)


def _probe_isinstance(value: Any, kinds: Any, *, kind_name: str, path: str) -> bool:
    """``isinstance`` 探测的**具名化**包装（plaita#69 / #163）。

    判据**一字不改**：能判出来时返回值与 ``isinstance`` 逐字相同（含 ``__class__``
    伪造与 ABC 注册的既有语义），热路径代价也不变（无异常时 ``try`` 是零代价的）。
    只有在宿主把探测炸掉时，把裸异常换成具名 ``ValueError``（含 ``plaita#69``）。

    真实类型命中时 CPython 根本不会去读 ``__class__``、也就走不到抛异常那一跳；
    所以**触发本拒绝的输入，在修复前拿到的是裸异常**——不存在「本来能跑通的路径
    被拒掉」的语义回归。
    """
    try:
        return isinstance(value, kinds)
    except Exception as exc:                # noqa: BLE001 - 宿主 __getattribute__ 的任意异常
        raise ValueError(_TRAVERSAL_PROBE_FAILED_TEMPLATE.format(
            path=path, type_name=type(value).__name__,
            exc_type=type(exc).__name__, kind=kind_name,
        )) from exc


#: plaita#163 **同形残面**（属性探针一侧）：宿主 ``__getattribute__`` 在
#: ``hasattr`` 探测上抛非 ``AttributeError`` 时的**具名拒绝**。
#:
#: ``hasattr(x, name)`` 只把 ``AttributeError`` 当"没有这个属性"，宿主抛别的异常
#: （``RuntimeError`` 等）时**原样上抛**——引擎在做"这个对象是不是映射/容器/属性
#: 风格上下文"的分支判定，却把宿主的裸异常漏给用户。与
#: :data:`_TRAVERSAL_PROBE_FAILED_TEMPLATE` 是同一条纪律、同一处置（含真实类名、
#: 刻意不回显宿主文本），只是探针从 ``isinstance`` 换成了属性存在性。
_HOST_ATTRIBUTE_PROBE_FAILED_TEMPLATE = (
    "expression path {path!r} read a value whose host class {type_name!r} raises "
    "{exc_type} from __getattribute__ while the engine probes the {attribute!r} "
    "attribute, so the engine cannot tell whether the value is engine data. A read "
    "that enters host code must either go through a C slot or be refused by name "
    "(plaita#69). Make the host class answer {attribute!r} normally (or not at all), "
    "or hand the engine a plain dict/list/str built from known-good data."
)

#: ``$F.*`` 结果中"可登记为新容器 / 可作 in-place 变更目标"的形状（#163 残面里
#: **tuple 型** ``isinstance`` 的判据；两处调用点共享，避免判据漂移）。
_CONTAINER_RESULT_KINDS = (dict, list, set, frozenset, tuple)
_CONTAINER_RESULT_KIND_NAMES = "dict/list/set/frozenset/tuple"


def _probe_hasattr(value: Any, attribute: str, *, path: str) -> bool:
    """``hasattr`` 探测的**具名化**包装（plaita#163 的同形残面）。

    判据**一字不改**：能判出来时返回值与 ``hasattr`` 逐字相同（``AttributeError``
    照旧 ⇒ ``False``），无异常时代价也不变（``try`` 零代价）。只有宿主在
    ``__getattribute__`` 里抛非 ``AttributeError`` 时，才把裸异常换成具名
    ``ValueError``（含 ``plaita#69``，用 ``type(value).__name__`` 给出真实类名；
    刻意不回显宿主异常文本）。
    """
    try:
        return hasattr(value, attribute)
    except Exception as exc:                # noqa: BLE001 - 宿主 __getattribute__ 的任意异常
        raise ValueError(_HOST_ATTRIBUTE_PROBE_FAILED_TEMPLATE.format(
            path=path, type_name=type(value).__name__,
            exc_type=type(exc).__name__, attribute=attribute,
        )) from exc


def _probe_getattr(value: Any, attribute: str, default: Any, *, path: str) -> Any:
    """``getattr(value, attribute, default)`` 探针的**具名化**包装（同形残面）。

    与 :func:`_probe_hasattr` 是同一处置，只是探针要拿到**属性值**才能判调用
    （``_container_has_root`` 里 ``getattr(obj, "get", None)`` + ``callable``）。
    判据一字不改：缺席（``AttributeError``）照旧返回 *default*，只有宿主
    ``__getattribute__`` 抛别的异常时才具名拒绝。
    """
    try:
        return getattr(value, attribute, default)
    except Exception as exc:                # noqa: BLE001 - 宿主 __getattribute__ 的任意异常
        raise ValueError(_HOST_ATTRIBUTE_PROBE_FAILED_TEMPLATE.format(
            path=path, type_name=type(value).__name__,
            exc_type=type(exc).__name__, attribute=attribute,
        )) from exc


def _get_attr(obj: Any, path: str) -> Any:
    """Read *path* off *obj* — mirrors the non-bracket branch of the old
    ``plaita.io.get_attr``.

    Order matters: dict-like objects (``dict`` or anything exposing both
    ``__getitem__`` and ``get``) are read via ``.get(path)`` so storage-key
    mappings such as a live ``CheckpointState`` resolve correctly (its
    ``$INPUT`` is a storage key, not a Python attribute). Only objects that
    are neither dict-like fall back to ``getattr`` — this covers plain
    attribute access on user data classes / Pydantic models passed as input.

    Both paths are gated against Python-internal names (plaita#55): the
    ``getattr`` fallback rejects every ``_``-prefixed name, the mapping path
    rejects ``__``-prefixed path segments (the key itself stays readable through
    the ``$F.*`` mapping helpers).  Rejection raises ``ValueError`` — never a
    silent ``None``.  See the gate comment above for the rationale and scope.

    plaita#163：``isinstance(obj, dict)`` 走 :func:`_probe_isinstance`——宿主
    ``__class__`` 抛异常时是具名 ``ValueError``（含 ``plaita#69``），不是裸异常。

    plaita#163 同形残面（W22-A）：三个属性探针（``__getitem__`` / ``get`` /
    ``__dict__``）走 :func:`_probe_hasattr`——宿主 ``__getattribute__`` 抛异常时
    同样具名拒绝，不是裸异常。

    **未改**：末段 ``getattr(obj, path, None)``（以及映射分支的
    ``_mapping_get`` → ``obj.get(path, None)``）是**真实读取**（不是探针）：宿主
    属性/描述符自己抛的异常是它的操作错误，带原生类型契约（``#68 ④`` 同一纪律：
    文档承诺的原生 ``TypeError``/``KeyError``/``ValueError`` 照旧原样抛出），
    具名化会改写原生异常类型 ⇒ 保留现状（见 W22-A RED-PROOF §3）。
    """
    if _probe_isinstance(obj, dict, kind_name="dict", path=path):
        return _mapping_get(obj, path)
    if (_probe_hasattr(obj, "__getitem__", path=path)
            and _probe_hasattr(obj, "get", path=path)):
        return _mapping_get(obj, path)
    if path.startswith(_RESERVED_FIELD_PREFIX):
        _reject_reserved_field(path)
    if _probe_hasattr(obj, "__dict__", path=path):
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
    """Would ``obj[root_key]`` (the nested thunk's root lookup) succeed?

    plaita#163 同形残面（W22-A）：本函数外层的五处**属性/类型探针**
    （``isinstance(obj, dict)`` / ``hasattr(__contains__)`` / ``getattr("get")`` /
    ``hasattr(__dict__)`` / ``hasattr(root_key)``）在宿主 ``__class__`` /
    ``__getattribute__`` 抛异常时原本漏 **裸** ``RuntimeError``；现在走
    :func:`_probe_isinstance` / :func:`_probe_hasattr` / :func:`_probe_getattr`
    具名拒绝（含 ``plaita#69``）。

    为什么是具名拒绝而不是"当作没有这个根键"：本函数是**嵌套字符串该不该求值**的
    分支判据（``#104`` 方案 A）。宿主拒绝回答"这个容器里有没有该根键"时，静默答
    "没有"会把一段合法的嵌套表达式串当成**数据文本**原样返回——一次看不见的语义
    改写；具名拒绝是 fail-closed 且用户可见的处置（与 #163 的论据一致）。

    **未改**：内层 ``root_key in obj`` / ``obj.get(...)`` 的异常照旧吞掉（继续往下
    试 ⇒ 最终 ``False``，DEBUG 留痕）。那不是引擎的探针失败，而是**容器自己的读取
    协议**在回答"没有"；这条"保守返回数据文本"的契约有既有测试钉住
    （``tests/unit/test_expression_parser_root_scan.py`` / #151 的日志可见性），
    本单不动。
    """
    if _probe_isinstance(obj, dict, kind_name="dict", path=root_key):
        return root_key in obj
    if _probe_hasattr(obj, "__contains__", path=root_key):
        try:
            return root_key in obj
        except Exception as exc:  # noqa: BLE001 — exotic containers must not break eval
            # 探针失败按"该容器没有这个根键"继续往下试（保守：宁可把数据文本原样
            # 返回，也不让它带着误导性的归因去递归求值）；留痕便于排查自定义上下文。
            logger.debug(
                "contains probe failed for %s root %r: %s",
                _expression._type_name(obj), root_key,
                _expression._exception_message(exc),
            )
    get = _probe_getattr(obj, "get", None, path=root_key)
    if callable(get):
        try:
            return get(root_key, _MISSING) is not _MISSING
        except Exception as exc:  # noqa: BLE001
            logger.debug(
                "get probe failed for %s root %r: %s",
                _expression._type_name(obj), root_key,
                _expression._exception_message(exc),
            )
    if _probe_hasattr(obj, "__dict__", path=root_key):
        # 兜底分支，与 _get_attr 的 getattr 分支对齐（属性风格的自定义上下文）。
        # 真实容器（dict / CheckpointState 这类带前缀存储键的映射）都走上面两条；
        # 根键一律带前缀（`$INPUT`），普通 dataclass / Pydantic 模型上恒为假，
        # 因此这条实际只在显式 setattr(obj, "$INPUT", …) 的容器上成立。
        return _probe_hasattr(obj, root_key, path=root_key)
    return False


def _render_path(root_key: str, walked: List[Tuple[str, Any, str]]) -> str:
    """Render the variable path walked so far (``$NODE.llm.text``)."""
    parts = [root_key]
    for kind, target, raw in walked:
        parts.append(f"[{target}]" if kind == "index" else f".{raw}")
    return "".join(parts)


# ---------------------------------------------------------------------------
# ``$NODE.raw.<node>[.<path>...]`` — explicit verbatim accessor (plaita#130)
# ---------------------------------------------------------------------------
# ``$NODE.<node>.<field>`` keeps the historical semantics: a string value that
# looks like an expression/template may be re-evaluated with its parent
# container as context (#104 plan A narrows that, it does not remove it).  Data
# text that a node produced (LLM output, document content, prompt echo) can
# therefore still be silently rewritten — ``"{% $F.eq(1, 1) %}"`` -> ``"True"``.
# The accessor makes "return the stored value, do not evaluate it" **explicit**:
# with the ``raw`` modifier no string is re-evaluated at any depth (the #104
# root scan is bypassed too).  Non-string values are returned exactly as the
# non-``raw`` path returns them.
#
# Name-collision rule (deterministic, backwards compatible): the first segment
# after ``$NODE`` is the modifier **only when** the ``$NODE`` container has no
# node/key with that id **and** the segment after it is a field name (the node
# id).  Once a node literally named ``raw`` has produced a value, ``$NODE.raw.
# <path>`` means *that node* again — an existing flow never silently changes
# meaning — and the collision is reported once per distinct path string with a
# WARNING.  Such a flow cannot use the accessor (there is no spelling left that
# could mean it).  The decision is necessarily per-call: while the declaring
# node has not produced yet, the container has no ``raw`` key, so the same
# expression resolves as the accessor and emits no warning.  That runtime window
# is documented in MIGRATION.md and pinned by
# ``tests/unit/test_expression_parser_raw_accessor.py``.
#
# ``_warned_raw_collisions`` uses the same process-level, lock-free,
# 512-entry-cleared dedupe idiom as ``_warned_literals`` /
# ``_warned_unmatched_templates``.
_RAW_MODIFIER = "raw"
_warned_raw_collisions: "set[str]" = set()


def _warn_raw_accessor_collision(path: str) -> None:
    if path in _warned_raw_collisions:
        return
    if len(_warned_raw_collisions) > 512:
        _warned_raw_collisions.clear()
    _warned_raw_collisions.add(path)
    logger.warning(
        "expression %s: the segment after $NODE is 'raw', but this flow also has a "
        "node/key with id 'raw' — it is resolved as that node (backwards "
        "compatibility), so the verbatim '$NODE.raw.<id>...' accessor (#130) is NOT "
        "available in this flow; rename the node to use the accessor.",
        _expression._clip(path),
    )


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
            # plaita#69 r4（结构半边 2 的登记侧）：这条路径若起自活 CheckpointState 的
            # 持久化根，则它交出的对象**属于持久化图**——登记其身份，使别处任何容器
            # （任意键名）挂上它时能被身份探测认出来。热路径上只做一次 ``dict`` 查键
            # （``_is_persisted_key``，不建 frozenset），路径串在遍历中增量拼接、
            # 只 join 一次，登记**根对象 + 最终对象**两项（只有被表达式实际交出过的
            # 对象才可能出现在别的容器里，中间对象由它自己那次求值登记）。
            live_walk = _is_persisted_key(context, root_key)
            root_obj = obj
            path_parts: List[str] = [root_key] if live_walk else []
            # ``$NODE.raw.<node>...`` — explicit verbatim accessor (#130).  The
            # decision is made here (not at compile time) because it depends on
            # the runtime ``$NODE`` container: the modifier only applies when no
            # node/key named ``raw`` exists and a node id follows.  See
            # ``_RAW_MODIFIER`` above for the collision rule.
            raw_mode = False
            effective_segments = segments
            if (root_key == f"{parser.prefix}NODE" and len(segments) >= 2
                    and segments[0][0] == "field" and segments[0][2] == _RAW_MODIFIER
                    and segments[1][0] == "field"):
                if _container_has_root(obj, _RAW_MODIFIER):
                    _warn_raw_accessor_collision(_render_path(root_key, segments))
                else:
                    raw_mode = True
                    effective_segments = segments[1:]
            for position, (kind, target, raw) in enumerate(effective_segments):
                if kind == "index":
                    obj = obj[target]
                    if live_walk:
                        path_parts.append(f"[{target}]")
                    continue
                if (root_key == f"{prefix}INPUT"
                        and _probe_isinstance(obj, dict, kind_name="dict", path=raw)
                        and target not in obj):
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
                # ``raw`` 模式下整条路径都不递归：字符串原样返回（#130）。
                if (not raw_mode
                        and _probe_isinstance(attr, str, kind_name="str", path=raw)
                        and (attr.startswith(prefix) or "{%" in attr)):
                    # 方案 A（#104）：递归前先扫根键。嵌套串以**持有它的父容器**
                    # 为 context 求值，所以只要它引用的任一上下文根在当前父容器里
                    # 不存在，它就不是"嵌套表达式字符串"而是数据文本（LLM/agent
                    # 输出原文、文件内容、prompt 回显）——原样返回，既不抛归因错误
                    # 的 KeyError，也不静默改写（如引用了只在流程根上下文里存在的
                    # 根键的串）。不引用任何根键的串（如 "{% $F.concat('a','b') %}"）
                    # 保持历史语义继续递归；"无根引用且无字面量"的模板因此仍会被
                    # 求值——要拿到原文请走显式 `$NODE.raw.<id>...`（#130）。
                    missing_roots = [
                        root for root in _scan_root_refs(parser.prefix, attr)
                        if not _container_has_root(container, root)
                    ]
                    field_path = _render_path(root_key, effective_segments[:position + 1])
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
                            # 只捕获 KeyError：表达式内部（如 $F.* 参数类型不符）
                            # 抛出的 TypeError 是真错误，不能被改写成"父容器缺键"。
                            # 极端的"__contains__ 为真但不可下标"容器会漏出 TypeError，
                            # 真实容器（dict / CheckpointState）都可下标，视为不可达。
                            preview = attr if len(attr) < 120 else attr[:117] + "..."
                            # 用异常原文（args[0]）而不是 str(exc)：KeyError.__str__
                            # 会对消息再 repr 一次，嵌进来会多一层引号。
                            original = exc.args[0] if exc.args else exc
                            # 保留 from None：原文已带上游错误，链式化会把整条递归
                            # 求值 traceback 重复打印一遍（与顶层缺键 KeyError 一致）。
                            raise KeyError(
                                f"解析 {field_path} 时，字段值是一段嵌套表达式字符串 "
                                f"{preview!r}：嵌套字符串以**持有它的父容器**为 context "
                                f"求值（不是流程根上下文），而该父容器里缺少它引用的"
                                f"根键。原始错误：{original}。若这个字段是数据文本"
                                f"（LLM/agent 输出原文、文件内容、prompt 回显），"
                                f"它不应被当作表达式求值。"
                            ) from None
                else:
                    obj = attr
                if live_walk:
                    path_parts.append(f".{raw}")
            if live_walk:
                _register_live_objects(context, root_key, (
                    (root_obj, root_key), (obj, "".join(path_parts))))
            return obj

        thunk._plaita_path = (root_key, tuple(segments))   # plaita#69 mutator gate
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
            context, registry, _prefix = frame
            # plaita#69: the runtime consumer of ``has_side_effects`` — classify
            # every argument **before** any of them is evaluated, so a refusal can
            # never leave a half-applied side effect behind.
            detach_plans: List[Optional[Any]] = [None] * len(arg_thunks)
            if arg_thunks and _is_inplace_mutator(registry, func_name):
                # A1（#69 r2）+ F1（#69 r3）：**每一个实参位**都要过门，不只是改写
                # 目标；浅 producer 的别名值在这里拿到 detach 计划（求值后立刻深拷贝）。
                # r5：目标位拿到 freshness plan（求值后按身份证明"是新建的"才放行）。
                for position, argument in enumerate(arg_thunks):
                    detach_plans[position] = _gate_mutator_argument(
                        argument, context, registry, func_name, position, _prefix)
            args = []
            for position, argument in enumerate(arg_thunks):
                value = argument(frame)
                plan = detach_plans[position]
                if plan is not None:
                    if isinstance(plan, _FreshnessPlan):
                        # r5 default-deny: refuse unless the engine created it.
                        _enforce_fresh(value, plan, func_name)
                    else:
                        value = _detach_alias(value, func_name, plan)
                args.append(value)
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("parse_function: func_name=%s, argc=%d, arg_types=%s",
                             _expression._clip(func_name), len(args),
                             [_expression._type_name(arg) for arg in args])
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
                    "expression function %r not registered (registry_type=%s); "
                    "returning 'undefined'",
                    _expression._clip(func_name),
                    _expression._clip(type(registry).__name__),
                )
                func = _UNDEFINED
            # plaita#69 r13: the engine's own mapping-view helpers read the **real**
            # dict storage through the base C descriptors instead of calling the
            # argument's own ``keys``/``values``/``items`` (host self-description is
            # not evidence). Only the engine-builtin callables are intercepted; a
            # host callable registered under the same name is left untouched.
            # ⚠ 拦截必须先于 #68 的实参测量——测量会探锚宿主对象，把 r13 的
            # 具名拒绝（spoofed __class__ 等）吃成 #68 的 unmeasurable 包装。
            if (len(args) == 1
                    and _STORAGE_VIEW_BUILTINS.get(func_name) is func):
                result = _engine_mapping_view(func_name, args[0])
                if (_probe_isinstance(result, _CONTAINER_RESULT_KINDS,
                                      kind_name=_CONTAINER_RESULT_KIND_NAMES,
                                      path=f"$F.{func_name}(...)")):
                    _note_fresh_result(registry, func_name, result)
                # 引擎视图返回后再测量（此时对象已被 r13 判决为真 dict 存储）
                return _size_guard_result(
                    func_name, result, _expression._measure_input(args))
            # plaita#68 ①②（调用前）：会消费可迭代对象的算子先把单次迭代器物化一次
            #    （按名字而不是函数对象 ⇒ partial/lambda/绑定方法/同名覆盖同样生效）；
            #    只物化**被消费的那个实参**（join 的元素表 / sort 的序列）——分隔符等
            #    标量实参原样传下去（第十二轮 M1 实测，str 子类分隔符物化会变原生 TypeError）。
            if func_name in _expression.ITERABLE_CONSUMING_NAMES and args:
                args = ((_expression._materialize_for_consuming(args[0]),)
                        + tuple(args[1:]))
            # plaita#68 ②：内建算子的语义预检（按函数对象；覆盖同名算子不会被误用）
            prepare = _expression.PROJECTIONS.get(func)
            if prepare is not None:
                prepared = prepare(args)        # 可能抛"分配前拒绝"
                if prepared is not None:
                    args = prepared             # 物化后的实参（判定与执行共用）
            # plaita#68 ③：**调用之前**算好输入规模（本帧局部量，调用后不再回读实参）
            input_size = _expression._measure_input(args)
            # plaita#68 ④（H2）：算子内部（`bool()`/`len()`/`__sizeof__` 等用户协议）
            # 抛出的**非原生**异常也要转成守卫的"不可界定 ⇒ ValueError"；
            # 文档承诺的原生 TypeError/KeyError/ValueError/… 照旧原样抛出。
            try:
                result = func(*args)
            except _NATIVE_OPERATOR_ERRORS:
                raise
            except Exception as exc:          # noqa: BLE001 - 用户对象异常不得漏出
                _expression._render_unmeasurable(exc)
                raise ValueError(
                    f"$F.* unmeasurable value ({type(exc).__name__}) "
                    f"result too large: 0 bytes (limit 0). "
                    "Refusing to build an unbounded result."
                ) from exc
            # plaita#69 r5 freshness registration: only a container result can ever
            # be a mutation target, and only producers create new containers.
            # plaita#163 同形残面（W22-A）：tuple 型 ``isinstance`` 同样回落读
            # ``__class__``——宿主注册的函数返回非容器 shape 的对象时可达（见
            # W22-A RED-PROOF §2 D），改走具名化探针。
            if (_probe_isinstance(result, _CONTAINER_RESULT_KINDS,
                                  kind_name=_CONTAINER_RESULT_KIND_NAMES,
                                  path=f"$F.{func_name}(...)")):
                _note_fresh_result(registry, func_name, result)
            # plaita#68：输出规模守卫（所有结果的单一判定点）。
            return _size_guard_result(func_name, result, input_size)

        thunk._plaita_call = func_name                  # plaita#69 producer classification
        thunk._plaita_args = tuple(arg_thunks)          # plaita#69 r3 F2: callee-arg reach
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
        # plaita#69 r6：**求值入口**。若本次求值已在作用域内（嵌套表达式串 /
        # parse_function / 由 ExecutionContext.evaluate 打开的求值），
        # freshness_scope() 共享外层作用域、不重置；否则它在这里开、在这里关。
        with freshness_scope():
            return self._eval(value, context, registry)

    def _eval(self, value: Any, context: Dict[str, Any],
              registry: Optional[Any] = None) -> Any:
        # plaita#69 r4：任一活 CheckpointState 被当作求值上下文用过，就登记为"活根
        # 来源"——身份探测据此把它的五个持久化字段值认成持久化对象。放在入口（而不是
        # 只在路径 thunk 里）是为了让"嵌套串的父容器**就是**某个活根"这种形状也能在
        # 嵌套求值**当下**被认出（thunk 末尾的登记要到遍历结束才发生）。
        if _is_live_state(context):
            _note_live_state(context)
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
                # 插值也要过结果守卫（第六轮验收 必修 4；第七轮验收 必修 1：
                # **渲染之前**就要拒绝）。渲染规模无法界定（自定义 __str__/__repr__
                # 且类型不在内建渲染白名单里）或预估超过额度 ⇒ 直接抛错，绝不
                # "先渲染再判"（LyingMapping 实测元素合计 1MB、真实渲染 99MB）。
                raw = seg_thunk(frame)
                bound = _expression._render_size(raw)
                if bound is None:
                    raise ValueError(
                        f"$F.template cannot bound the rendered size of "
                        f"{type(raw).__name__!r} (渲染语义不在可证白名单里). "
                        f"Refusing to build an unbounded result. "
                        f"{_expression._RENDER_ESCAPE_HINT}"
                    )
                # 渲染额度＝渲染上界本身（不能用"内容量/元素数下界"抬高额度，
                # 第八轮验收 必修 4）：只需撞硬上限就拒。
                if bound > _expression._EXPR_MAX_RESULT_HARD:
                    raise ValueError(
                        f"$F.template result too large: {bound} chars "
                        f"(limit {_expression._EXPR_MAX_RESULT_HARD}). "
                        "Refusing to build an unbounded result."
                    )
                raw_size = bound
                text = str(raw)
                out.append(_size_guard_result("template", text, raw_size))
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
