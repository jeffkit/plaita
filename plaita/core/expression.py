"""
plaita.core.expression — Structured expression engine.

Provides:
- FunctionCategory enum for classifying expression functions.
- FunctionDescriptor frozen dataclass with side-effect metadata.
- ExpressionRegistry for querying functions by category / side-effect.
- ExpressionEvaluator that delegates to plaita.io evaluate/parse_function.
- get_default_expression_registry() factory with all 90+ built-in functions.
"""

from __future__ import annotations

import json
import math
import logging
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Any, Callable, List, Optional

logger = logging.getLogger("plaita.core.expression")


# ---------------------------------------------------------------------------
# FunctionCategory enum
# ---------------------------------------------------------------------------

class FunctionCategory(str, Enum):
    """Classification categories for expression functions."""

    MATH = "math"
    STRING = "string"
    LOGIC = "logic"
    ARRAY = "array"
    DICT = "dict"
    DATETIME = "datetime"
    JSON = "json"
    TYPE = "type"


# ---------------------------------------------------------------------------
# FunctionDescriptor
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class FunctionDescriptor:
    """Metadata wrapper around a registered expression function.

    Attributes:
        name: Function name (used in expressions as ``$F.name(...)``).
        func: The underlying callable.
        category: Classification category.
        has_side_effects: ``True`` if the function mutates its input.
            Side-effect functions (e.g. pop, set, delete, clear) are **not
            thread-safe** and must not be used in concurrent execution
            contexts without external synchronisation.
        description: Optional human-readable description.
    """

    name: str
    func: Callable
    category: FunctionCategory
    has_side_effects: bool = False
    description: str = ""


# ---------------------------------------------------------------------------
# ExpressionRegistry
# ---------------------------------------------------------------------------

class ExpressionRegistry:
    """Registry for expression functions with category and side-effect metadata."""

    def __init__(self) -> None:
        self._functions: dict[str, FunctionDescriptor] = {}

    def register(
        self,
        name: str,
        func: Callable,
        category: FunctionCategory,
        *,
        has_side_effects: bool = False,
        description: str = "",
        override: bool = False,
    ) -> None:
        """Register an expression function.

        Args:
            name: Function name (used in expressions as ``$F.name(...)``).
            func: The callable to execute.
            category: Classification category.
            has_side_effects: ``True`` if the function mutates its input.
            description: Human-readable description.
            override: If ``True``, overwrite an existing registration with the
                same name instead of raising ``ValueError``.  Use this for
                intentional re-registration (e.g. backward-compatible
                ``register_function`` / dict-style assignment) rather than
                reaching into ``_functions`` from outside the class.

        Raises:
            ValueError: If *name* is empty, or already registered with
                ``override=False``.
        """
        if not name:
            raise ValueError("Function name must not be empty")
        if name in self._functions and not override:
            raise ValueError(f"Function '{name}' is already registered")
        self._functions[name] = FunctionDescriptor(
            name=name,
            func=func,
            category=category,
            has_side_effects=has_side_effects,
            description=description,
        )

    def get(self, name: str) -> Optional[FunctionDescriptor]:
        """Look up a function descriptor by name."""
        return self._functions.get(name)

    def unregister(self, name: str) -> None:
        """Remove a function registration. No-op if *name* is not registered."""
        self._functions.pop(name, None)

    def get_callable(self, name: str) -> Optional[Callable]:
        """Look up just the callable by name (for backward compatibility)."""
        desc = self._functions.get(name)
        return desc.func if desc else None

    def by_category(self, category: FunctionCategory) -> List[FunctionDescriptor]:
        """Return all functions in *category*."""
        return [d for d in self._functions.values() if d.category == category]

    def side_effect_functions(self) -> List[FunctionDescriptor]:
        """Return all functions marked with side effects."""
        return [d for d in self._functions.values() if d.has_side_effects]

    def all_functions(self) -> dict[str, FunctionDescriptor]:
        """Return the full registry as a read-only snapshot."""
        return dict(self._functions)

    def __contains__(self, name: str) -> bool:
        return name in self._functions

    def __len__(self) -> int:
        return len(self._functions)

    def __repr__(self) -> str:
        return f"<ExpressionRegistry functions={len(self._functions)}>"


# ---------------------------------------------------------------------------
# Module-level function definitions (must be picklable for multiprocessing)
# ---------------------------------------------------------------------------

def _fn_add(a, b): return a + b
def _fn_sub(a, b): return a - b
def _fn_mul(a, b): return a * b
def _fn_div(a, b): return a / b
def _fn_mod(a, b): return a % b
def _fn_pow(a, b): return a ** b

def _fn_lower(a): return a.lower()
def _fn_upper(a): return a.upper()
def _fn_capitalize(a): return a.capitalize()
def _fn_title(a): return a.title()
def _fn_strip(a): return a.strip()
def _fn_lstrip(a): return a.lstrip()
def _fn_rstrip(a): return a.rstrip()
def _fn_replace(a, b, c): return a.replace(b, c)
def _fn_split(a, b): return a.split(b)
def _fn_join(a, b): return b.join(a)
def _fn_startswith(a, b): return a.startswith(b)
def _fn_endswith(a, b): return a.endswith(b)
def _fn_concat(*args): return "".join([str(arg) for arg in args])
def _fn_isDigit(a): return a.isdigit()

def _fn_and(a, b): return a and b
def _fn_or(*args): return next((arg for arg in args if arg), False)
def _fn_not(a): return not a

def _fn_index(a, b): return a.index(b)
def _fn_slice(a, b, c): return a[b:c]
def _fn_append(a, b): return a + [b]
def _fn_extend(a, b): return a + b
def _fn_insert(a, b, c): return a[:b] + [c] + a[b:]
def _fn_pop(a, b): return a.pop(b)
def _fn_remove(a, b): return a[:a.index(b)] + a[a.index(b) + 1:]
def _fn_reverse(a): return a[::-1]
def _fn_sort(a, sort_key=None, desc=False):
    return sorted(a, key=lambda x: x.get(sort_key) if isinstance(x, dict) else x, reverse=desc)
def _fn_getListItem(a, b): return a[b]
def _fn_delListItem(a, b): return a.pop(b)
def _fn_setListItem(a, b, c): return a.__setitem__(b, c)
def _fn_addListItem(a, b): return a + [b]
def _fn_insertListItem(a, b, c): return a[:b] + [c] + a[b:]

def _fn_keys(a): return a.keys()
def _fn_values(a): return a.values()
def _fn_items(a): return a.items()
def _fn_get(a, b, c=None): return a.get(b, c)
def _fn_set(a, b, c): return a.update({b: c})
def _fn_delete(a, b): return a.pop(b)
def _fn_clear(a): return a.clear()
def _fn_getDictValue(a, b, c=None): return a.get(b, c)
def _fn_delDictValue(a, b): return a.pop(b)
def _fn_setDictValue(a, b, c): return a.update({b: c})
def _fn_getDictKeys(a): return a.keys()
def _fn_getDictValues(a): return a.values()
def _fn_clearDict(a): return a.clear()

def _fn_now(fmt="%Y-%m-%d %H:%M:%S"): return datetime.now().strftime(fmt)
def _fn_today(fmt="%Y-%m-%d"): return date.today().strftime(fmt)

def _fn_json_loads(a): return json.loads(a)
def _fn_json_dumps(a): return json.dumps(a)


# ---------------------------------------------------------------------------
# Default registry factory
# ---------------------------------------------------------------------------

_default_registry: Optional[ExpressionRegistry] = None


def get_default_expression_registry() -> ExpressionRegistry:
    """Return the module-level default registry with all built-in functions.

    The registry is created lazily on first access and cached.
    """
    global _default_registry
    if _default_registry is None:
        _default_registry = _build_default_registry()
    return _default_registry


def _reset_default_registry() -> None:
    """Reset the cached default registry. Used in tests to force re-initialization."""
    global _default_registry
    _default_registry = None


def _register_math(reg: ExpressionRegistry) -> None:
    """Register math functions — all pure, no side effects."""
    _fns: list[tuple[str, Callable, str]] = [
        ("add", _fn_add, "Add two values"),
        ("sub", _fn_sub, "Subtract b from a"),
        ("mul", _fn_mul, "Multiply two values"),
        ("div", _fn_div, "Divide a by b"),
        ("mod", _fn_mod, "Modulo a by b"),
        ("pow", _fn_pow, "Raise a to the power of b"),
        ("abs", abs, "Absolute value"),
        ("ceil", math.ceil, "Ceiling"),
        ("floor", math.floor, "Floor"),
        ("round", round, "Round to n digits"),
        ("trunc", math.trunc, "Truncate to integer"),
        ("sqrt", math.sqrt, "Square root"),
    ]
    for name, func, desc in _fns:
        reg.register(name, func, FunctionCategory.MATH, description=desc)


def _register_string(reg: ExpressionRegistry) -> None:
    """Register string functions — all pure, no side effects."""
    _fns: list[tuple[str, Callable, str]] = [
        ("lower", _fn_lower, "Lowercase"),
        ("upper", _fn_upper, "Uppercase"),
        ("capitalize", _fn_capitalize, "Capitalize first char"),
        ("title", _fn_title, "Title-case"),
        ("strip", _fn_strip, "Strip whitespace"),
        ("lstrip", _fn_lstrip, "Strip leading whitespace"),
        ("rstrip", _fn_rstrip, "Strip trailing whitespace"),
        ("replace", _fn_replace, "Replace substring"),
        ("split", _fn_split, "Split string"),
        ("join", _fn_join, "Join iterable with separator"),
        ("startswith", _fn_startswith, "Check string prefix"),
        ("endswith", _fn_endswith, "Check string suffix"),
        ("concat", _fn_concat, "Concatenate values as strings"),
        ("isDigit", _fn_isDigit, "Check if string is all digits"),
    ]
    for name, func, desc in _fns:
        reg.register(name, func, FunctionCategory.STRING, description=desc)


def _register_array(reg: ExpressionRegistry) -> None:
    """Register array functions with correct side-effect labels.

    Side-effect functions (``pop``, ``delListItem``, ``setListItem``) mutate
    the input list in-place.  They are **not thread-safe** — using them in
    concurrent execution contexts (e.g. parallel node execution, shared
    context across async tasks) may corrupt data.  Callers should either
    serialize access or copy the list before mutation.
    """
    _pure: list[tuple[str, Callable, str]] = [
        ("len", len, "Length of sequence"),
        ("length", len, "Length of sequence (alias)"),
        ("index", _fn_index, "Find index of element"),
        ("slice", _fn_slice, "Slice list"),
        ("append", _fn_append, "Append element (returns new list)"),
        ("extend", _fn_extend, "Extend list (returns new list)"),
        ("insert", _fn_insert, "Insert element (returns new list)"),
        ("remove", _fn_remove, "Remove first occurrence (returns new list)"),
        ("reverse", _fn_reverse, "Reverse list (returns new list)"),
        ("sort", _fn_sort, "Sort list (returns new list)"),
        ("getListItem", _fn_getListItem, "Get item by index"),
        ("addListItem", _fn_addListItem, "Add item (returns new list)"),
        ("insertListItem", _fn_insertListItem, "Insert item (returns new list)"),
    ]
    for name, func, desc in _pure:
        reg.register(name, func, FunctionCategory.ARRAY, description=desc)

    _side_effect: list[tuple[str, Callable, str]] = [
        ("pop", _fn_pop, "Pop item at index (mutates list)"),
        ("delListItem", _fn_delListItem, "Delete item at index (mutates list)"),
        ("setListItem", _fn_setListItem, "Set item at index (mutates list)"),
    ]
    for name, func, desc in _side_effect:
        reg.register(name, func, FunctionCategory.ARRAY, has_side_effects=True, description=desc)


def _register_dict(reg: ExpressionRegistry) -> None:
    """Register dict functions with correct side-effect labels.

    Side-effect functions (``set``, ``delete``, ``clear``, ``setDictValue``,
    ``delDictValue``, ``clearDict``) mutate the input dict in-place.  They
    are **not thread-safe** — using them in concurrent execution contexts
    (e.g. parallel node execution, shared context across async tasks) may
    corrupt data.  Callers should either serialize access or copy the dict
    before mutation.
    """
    _pure: list[tuple[str, Callable, str]] = [
        ("keys", _fn_keys, "Dict keys"),
        ("values", _fn_values, "Dict values"),
        ("items", _fn_items, "Dict items"),
        ("get", _fn_get, "Get value with default"),
        ("getDictValue", _fn_getDictValue, "Get dict value with default"),
        ("getDictKeys", _fn_getDictKeys, "Get dict keys"),
        ("getDictValues", _fn_getDictValues, "Get dict values"),
    ]
    for name, func, desc in _pure:
        reg.register(name, func, FunctionCategory.DICT, description=desc)

    _side_effect: list[tuple[str, Callable, str]] = [
        ("set", _fn_set, "Set dict value (mutates dict)"),
        ("delete", _fn_delete, "Delete dict key (mutates dict)"),
        ("clear", _fn_clear, "Clear all dict entries (mutates dict)"),
        ("setDictValue", _fn_setDictValue, "Set dict value (mutates dict)"),
        ("delDictValue", _fn_delDictValue, "Delete dict key (mutates dict)"),
        ("clearDict", _fn_clearDict, "Clear all dict entries (mutates dict)"),
    ]
    for name, func, desc in _side_effect:
        reg.register(name, func, FunctionCategory.DICT, has_side_effects=True, description=desc)


def _register_logic(reg: ExpressionRegistry) -> None:
    """Register logic functions — all pure."""
    reg.register("and", _fn_and, FunctionCategory.LOGIC, description="Logical AND")
    reg.register("or", _fn_or, FunctionCategory.LOGIC, description="Logical OR (returns first truthy value)")
    reg.register("not", _fn_not, FunctionCategory.LOGIC, description="Logical NOT")


def _register_datetime(reg: ExpressionRegistry) -> None:
    """Register datetime functions — pure (snapshot of current time)."""
    reg.register("now", _fn_now, FunctionCategory.DATETIME, description="Current datetime formatted")
    reg.register("today", _fn_today, FunctionCategory.DATETIME, description="Current date formatted")


def _register_json(reg: ExpressionRegistry) -> None:
    """Register JSON functions — pure."""
    reg.register("json_loads", _fn_json_loads, FunctionCategory.JSON, description="Parse JSON string")
    reg.register("json_dumps", _fn_json_dumps, FunctionCategory.JSON, description="Serialize to JSON string")


def _build_default_registry() -> ExpressionRegistry:
    """Build the default registry with all built-in functions."""
    reg = ExpressionRegistry()
    _register_math(reg)
    _register_string(reg)
    _register_array(reg)
    _register_dict(reg)
    _register_logic(reg)
    _register_datetime(reg)
    _register_json(reg)
    return reg


# ---------------------------------------------------------------------------
# ExpressionEvaluator
# ---------------------------------------------------------------------------

class ExpressionEvaluator:
    """Evaluates expressions against an execution context.

    This class wraps the existing ``plaita.io.evaluate`` / ``parse_function``
    logic and exposes the structured ``ExpressionRegistry``.  The actual
    parsing and evaluation is delegated to the battle-tested functions in
    ``plaita.io`` to guarantee identical results.
    """

    def __init__(self, registry: Optional[ExpressionRegistry] = None) -> None:
        self._registry = registry or get_default_expression_registry()

    @property
    def registry(self) -> ExpressionRegistry:
        return self._registry

    def evaluate(
        self,
        value: Any,
        context: dict[str, Any],
        prefix: str = "$",
    ) -> Any:
        """Evaluate a value, resolving any expressions.

        Delegates to ``plaita.io.evaluate`` but passes this evaluator's
        ``ExpressionRegistry`` so that ``$F.func(...)`` calls resolve against
        the scoped registry.  When the registry is the default one this is
        behavior-preserving; when a custom registry is supplied, function
        lookups go through it instead of the module-level global.
        """
        from plaita.io import evaluate as _io_evaluate
        return _io_evaluate(value, context, prefix, registry=self._registry)
