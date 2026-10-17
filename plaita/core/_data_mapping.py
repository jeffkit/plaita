"""plaita#203 — which mapping-like objects may be read as flow data.

``#133`` closed the *attribute* route to modules / functions / classes / frames
(``$NODE.h.os.environ``).  The *mapping* route remained open: a node could
return ``SimpleNamespace(env=os.environ)`` or ``{"env": os.environ}`` and
``$NODE.h.env.HOME`` / ``$F.get($NODE.h.env, 'HOME')`` read the live process
environment by treating ``os._Environ`` as just another mapping.

The rules are deliberately narrow:

* **real ``dict``** (including subclasses) is data; reads go through the
  ``dict`` C slots (``dict.get`` / ``dict.keys`` …), so a subclass's overridden
  ``get`` / ``__getitem__`` / ``__getattribute__`` is never executed — the same
  discipline #69 r13 uses for ``$F.keys``/``values``/``items``;
* **the engine's exact mapping types** are data: ``CheckpointState`` (backs
  ``$INPUT`` / ``$PARENT`` in live flows) and the ``$PARENT`` lazy snapshot
  (``_LazyRootSnapshot``; nested loops / inline flows).  Their ``get`` is engine
  code, not a host channel;
* every other *mapping-like* object — ``os._Environ``, ``ChainMap``,
  ``UserDict``, ``MappingProxyType``, a custom ``Mapping``, a duck-typed
  ``__getitem__`` + ``get`` host object — is a **host container** and is refused
  by name before any key or value is read.  This is a type/source judgement,
  not an attribute-name or value-shape judgement.

The module is intentionally dependency-free: ``state.py`` is imported lazily
inside :func:`_checkpoint_state_type` so importing ``expression`` stays cheap
and no import cycle is introduced.
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

_ENGINE_MAPPING_TYPES: Any = None


def _engine_mapping_types() -> Any:
    """Import the engine-owned mapping types lazily (no cycle, cached).

    ``CheckpointState`` backs ``$INPUT``/``$PARENT`` in live flows;
    ``_LazyRootSnapshot`` is the engine's lazy parent snapshot used by nested
    loops / inline flows.  Both are engine code, not host containers, and both
    need the mapping path to keep working (plaita#203 scope).
    """
    global _ENGINE_MAPPING_TYPES
    if _ENGINE_MAPPING_TYPES is None:
        from .state import CheckpointState, _LazyRootSnapshot

        _ENGINE_MAPPING_TYPES = (CheckpointState, _LazyRootSnapshot)
    return _ENGINE_MAPPING_TYPES


def is_engine_state(value: Any) -> bool:
    """Return ``True`` for the engine's exact mapping types.

    Exact type (not ``isinstance``): a host subclass could override ``get`` and
    turn the state back into a host-code channel.  Identity (not ``in``) keeps a
    lying metaclass ``__eq__`` out of the verdict.
    """
    try:
        return any(type(value) is cls for cls in _engine_mapping_types())
    except Exception:  # noqa: BLE001 - a broken environment must fail closed
        return False


def is_real_dict(value: Any) -> bool:
    """Return ``True`` when the **real type** (C ``ob_type``) is ``dict``/subclass."""
    try:
        return any(base is dict for base in type(value).__mro__)
    except Exception:  # noqa: BLE001 - unreadable type: not a real dict
        return False


def is_data_mapping(value: Any) -> bool:
    """Mapping-like value the engine may read as data (plain dict or engine state)."""
    return is_real_dict(value) or is_engine_state(value)


def is_mapping_like(value: Any) -> bool:
    """Does *value* look like a mapping container (the #203 gate input)?

    ``Mapping`` registration and the duck type ``__getitem__`` + ``get`` both
    count — exactly the shape ``_get_attr`` routes to ``_mapping_get``.  Any
    probe failure is treated as **mapping-like** (fail closed): the caller then
    refuses it with a named ``ValueError`` instead of letting a bare host
    exception escape.  The probes never call ``__getitem__`` / ``get``.
    """
    if is_data_mapping(value):
        return True
    try:
        if isinstance(value, Mapping):
            return True
    except Exception:  # noqa: BLE001 - host __class__ / ABC hook
        return True
    try:
        return hasattr(value, "__getitem__") and hasattr(value, "get")
    except Exception:  # noqa: BLE001 - host __getattribute__
        return True


#: Exact wording is pinned by tests/unit/test_issue203_data_mapping_traversal.py
#: (runtime message == the strings quoted in MIGRATION.md and the expression
#: guide).  It names the path, the real container type and the issue, and never
#: echoes keys or values.
NON_DATA_MAPPING_TEMPLATE = (
    "expression path {path!r} read a non-data mapping container of type "
    "{type_name!r}: only plain dict (including subclasses, read through dict's C "
    "slots) and the engine's CheckpointState may be traversed as a mapping "
    "(plaita#203). Host mapping-like objects such as os.environ / ChainMap / "
    "UserDict / custom Mapping are refused by name before any key or value is "
    "read; convert the data to a plain dict first."
)


def reject_non_data_mapping(value: Any, path: str) -> None:
    """Raise the named plaita#203 refusal for a host mapping-like *value*."""
    raise ValueError(NON_DATA_MAPPING_TEMPLATE.format(
        path=path, type_name=type(value).__name__))
