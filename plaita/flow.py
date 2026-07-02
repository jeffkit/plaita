"""
plaita.flow — Backward-compatible shim (slated for removal in 0.6.0).

Canonical implementations live in plaita.core.flow / plaita.core.executor /
plaita.core.callback / plaita.core.errors. This module re-exports them lazily
via ``__getattr__`` so that *every* access (``from plaita.flow import Flow`` or
``plaita.flow.Flow``) emits a ``DeprecationWarning`` naming the removal version.
Use the ``plaita.core.*`` paths for new code.
"""

from __future__ import annotations

import warnings as _warnings

# symbol -> canonical module (imported on demand so access actually warns)
_SHIM_SOURCES = {
    "Flow": "plaita.core.flow",
    "parse": "plaita.core.flow",
    "parse_and_run": "plaita.core.flow",
    "FlowExecution": "plaita.core.executor",
    "ExecutionMode": "plaita.core.executor",
    "FlowCallback": "plaita.core.callback",
    "FlowEvent": "plaita.core.callback",
    "BaseCallbackManager": "plaita.core.callback",
    "CallbackManager": "plaita.core.callback",
    "LoggerCallback": "plaita.core.callback",
    "FlowExecutionException": "plaita.core.errors",
}

__all__ = [
    "Flow",
    "FlowExecution",
    "FlowEvent",
    "FlowCallback",
    "FlowExecutionException",
    "BaseCallbackManager",
    "CallbackManager",
    "LoggerCallback",
    "ExecutionMode",
    "parse",
    "parse_and_run",
    "types",
]


def __getattr__(name: str):
    if name == "types":
        _warnings.warn(
            "Importing 'types' from 'plaita.flow' is deprecated and will be "
            "removed in plaita 0.6.0. Use 'from plaita.core import types' instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        from plaita.core import types as _t
        globals()["types"] = _t
        return _t
    src = _SHIM_SOURCES.get(name)
    if src is None:
        raise AttributeError(f"module 'plaita.flow' has no attribute {name!r}")
    _warnings.warn(
        f"Importing '{name}' from 'plaita.flow' is deprecated and will be "
        f"removed in plaita 0.6.0. Use '{src}' instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    import importlib
    value = getattr(importlib.import_module(src), name)
    globals()[name] = value  # cache so subsequent access is warning-free
    return value


def __dir__():
    return sorted(__all__)
