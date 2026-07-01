"""
plaita.errors — Compatibility shim.

Re-exports all symbols from ``plaita.core.errors``. Importing the module itself
is silent; a ``DeprecationWarning`` is emitted only when an attribute is
actually accessed, so users can migrate gradually. Use ``plaita.core.errors``
for new code.
"""

from __future__ import annotations

import warnings as _warnings

from plaita.core import errors as _core_errors

__all__ = [
    "FlowResultError",
    "NodeException",
    "FlowErrorType",
    "FlowExecutionException",
    "ErrorStrategy",
    "ErrorHandler",
    "RecoverableErrorHandler",
]


def __getattr__(name: str):
    if name in __all__:
        _warnings.warn(
            f"Importing '{name}' from 'plaita.errors' is deprecated. "
            f"Use 'plaita.core.errors' instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        value = getattr(_core_errors, name)
        globals()[name] = value  # cache so subsequent access is warning-free
        return value
    raise AttributeError(f"module 'plaita.errors' has no attribute {name!r}")


def __dir__():
    return sorted(__all__)
