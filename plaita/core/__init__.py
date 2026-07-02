"""
plaita.core — Core layer of the Plaita flow engine.

This subpackage contains the canonical definitions for errors, types,
execution components, and the Flow model.  It has **zero** dependencies
on plaita.server, plaita.storage.redis, or plaita.storage.sqlalchemy.

Compatibility shims at the old import paths (plaita.errors, plaita.types,
plaita.flow, …) re-export from here with DeprecationWarning.

NOTE: Flow, FlowExecution, and related modules are imported lazily via
``__getattr__`` to break a circular dependency chain:
  plaita.io → plaita.core (types) → plaita.core.flow → plaita.io
"""

from plaita.core.errors import (
    ErrorHandler,
    ErrorStrategy,
    ErrorResultException,
    FlowErrorType,
    FlowExecutionException,
    FlowErrorException,
    FlowResultError,
    FlowStartMissingError,
    FlowTimeoutError,
    NodeException,
    NodeExecutionError,
    NodeNotFoundError,
    NodeTimeoutError,
    RecoverableErrorHandler,
    ResumeError,
    ResumeType,
)
from plaita.core.types import (
    ANY,
    ARRAY,
    BOOL,
    DATE,
    DATETIME,
    DECIMAL,
    FLOAT,
    INTEGER,
    MAP,
    NULL,
    NUMBER,
    OBJECT,
    OPTIONAL,
    STRING,
    TIMESTAMP,
    TYPE,
    TYPE_NOT_SUPPORTED,
    UNION,
    ValidationError,
    get_native_type,
    native_types,
    valid,
)

_LAZY_IMPORTS = {
    "Flow": "plaita.core.flow",
    "parse": "plaita.core.flow",
    "parse_and_run": "plaita.core.flow",
    "FlowExecution": "plaita.core.executor",
    "ExecutionMode": "plaita.core.executor",
    "ExecutionContext": "plaita.core.context",
    "NodeRunner": "plaita.core.runner",
    "CallbackManager": "plaita.core.callback",
    "FlowCallback": "plaita.core.callback",
    "ExpressionEvaluator": "plaita.core.expression",
    "ExpressionRegistry": "plaita.core.expression",
    "FunctionCategory": "plaita.core.expression",
    "FunctionDescriptor": "plaita.core.expression",
    "get_default_expression_registry": "plaita.core.expression",
}


def __getattr__(name: str):
    if name in _LAZY_IMPORTS:
        import importlib
        module = importlib.import_module(_LAZY_IMPORTS[name])
        return getattr(module, name)
    raise AttributeError(f"module 'plaita.core' has no attribute {name!r}")


__all__ = [
    # flow model & entry points
    "Flow",
    "parse",
    "parse_and_run",
    # execution
    "FlowExecution",
    "ExecutionMode",
    "ExecutionContext",
    "NodeRunner",
    "CallbackManager",
    "FlowCallback",
    # errors
    "FlowResultError",
    "NodeException",
    "FlowErrorType",
    "FlowExecutionException",
    "FlowErrorException",
    "FlowStartMissingError",
    "FlowTimeoutError",
    "NodeNotFoundError",
    "NodeExecutionError",
    "NodeTimeoutError",
    "ErrorResultException",
    "ResumeError",
    "ResumeType",
    "ErrorStrategy",
    "ErrorHandler",
    "RecoverableErrorHandler",
    # types
    "ValidationError",
    "STRING",
    "BOOL",
    "INTEGER",
    "FLOAT",
    "NUMBER",
    "DECIMAL",
    "ARRAY",
    "MAP",
    "OBJECT",
    "DATETIME",
    "DATE",
    "TIMESTAMP",
    "TYPE",
    "UNION",
    "OPTIONAL",
    "ANY",
    "TYPE_NOT_SUPPORTED",
    "NULL",
    "native_types",
    "get_native_type",
    "valid",
    # expression engine
    "ExpressionEvaluator",
    "ExpressionRegistry",
    "FunctionCategory",
    "FunctionDescriptor",
    "get_default_expression_registry",
]
