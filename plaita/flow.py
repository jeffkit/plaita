"""
plaita.flow — Backward-compatible shim.

Canonical implementations live in plaita.core.flow and plaita.core.executor.
Importing from this module still works but emits DeprecationWarning for
IDE migration tooling.
"""

from __future__ import annotations

import warnings as _warnings

from plaita.core.flow import Flow, parse, parse_and_run
from plaita.core.executor import (
    ExecutionMode,
    FlowExecution,
)
from plaita.core.callback import (
    BaseCallbackManager,
    CallbackManager,
    FlowCallback,
    FlowEvent,
    LoggerCallback,
)
from plaita.core.errors import FlowExecutionException
from plaita.core import types


def __getattr__(name: str):
    _deprecated = {
        "Flow", "FlowExecution", "FlowEvent", "FlowCallback",
        "BaseCallbackManager", "CallbackManager", "LoggerCallback",
        "ExecutionMode", "FlowExecutionException", "parse", "parse_and_run",
        "types",
    }
    if name in _deprecated:
        _warnings.warn(
            f"Importing '{name}' from 'plaita.flow' is deprecated. "
            f"Use 'plaita.core.flow' or 'plaita.core.executor' instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return globals()[name]
    raise AttributeError(f"module 'plaita.flow' has no attribute {name!r}")


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
