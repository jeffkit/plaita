"""
plaita.types — Compatibility shim.

Re-exports all symbols from ``plaita.core.types``. Importing the module itself
is silent; a ``DeprecationWarning`` is emitted only when an attribute is
actually accessed, so users can migrate gradually. Use ``plaita.core.types``
for new code.
"""

from __future__ import annotations

import warnings as _warnings

from plaita.core import types as _core_types

__all__ = [
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
    "data_validators",
    "register_validator",
    "valid",
]


def __getattr__(name: str):
    if name in __all__:
        _warnings.warn(
            f"Importing '{name}' from 'plaita.types' is deprecated. "
            f"Use 'plaita.core.types' instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        value = getattr(_core_types, name)
        globals()[name] = value  # cache so subsequent access is warning-free
        return value
    raise AttributeError(f"module 'plaita.types' has no attribute {name!r}")


def __dir__():
    return sorted(__all__)
