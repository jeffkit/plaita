"""
T022: Backward-compatibility import tests.
Old import paths (plaita.errors, plaita.types) should emit DeprecationWarning but still work.
"""
import warnings

import pytest


class TestBackwardCompatImports:

    def test_plaita_errors_import_emits_deprecation(self):
        # Shim uses lazy __getattr__: bare import is silent, but accessing a
        # re-exported name emits DeprecationWarning.
        import importlib
        import plaita.errors
        importlib.reload(plaita.errors)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            _ = plaita.errors.FlowExecutionException
            deprecation_warnings = [
                x for x in w if issubclass(x.category, DeprecationWarning)
            ]
            assert len(deprecation_warnings) >= 1, (
                "Accessing names from plaita.errors should emit DeprecationWarning"
            )

    def test_plaita_types_import_emits_deprecation(self):
        # Shim uses lazy __getattr__: bare import is silent, but accessing a
        # re-exported name emits DeprecationWarning.
        import importlib
        import plaita.types
        importlib.reload(plaita.types)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            _ = plaita.types.STRING
            deprecation_warnings = [
                x for x in w if issubclass(x.category, DeprecationWarning)
            ]
            assert len(deprecation_warnings) >= 1, (
                "Accessing names from plaita.types should emit DeprecationWarning"
            )

    def test_plaita_errors_classes_still_accessible(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            from plaita.errors import (
                FlowResultError,
                NodeException,
                FlowErrorType,
                FlowExecutionException,
                ErrorStrategy,
                ErrorHandler,
                RecoverableErrorHandler,
            )
        assert FlowResultError is not None
        assert NodeException is not None
        assert FlowErrorType is not None
        assert FlowExecutionException is not None
        assert ErrorStrategy is not None
        assert ErrorHandler is not None
        assert RecoverableErrorHandler is not None

    def test_plaita_types_objects_still_accessible(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            from plaita.types import (
                ValidationError,
                STRING,
                BOOL,
                INTEGER,
                FLOAT,
                NUMBER,
                native_types,
                get_native_type,
                valid,
                register_validator,
                data_validators,
            )
        assert ValidationError is not None
        assert STRING == "string"
        assert BOOL == "boolean"
        assert callable(get_native_type)
        assert callable(valid)
        assert callable(register_validator)

    def test_error_classes_are_same_objects(self):
        """Shim re-exports must be the exact same class objects as plaita.core."""
        from plaita.core.errors import FlowResultError as Core
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            from plaita.errors import FlowResultError as Shim
        assert Core is Shim

    def test_type_objects_are_same(self):
        from plaita.core.types import native_types as core_nt
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            from plaita.types import native_types as shim_nt
        assert core_nt is shim_nt

    def test_existing_code_using_errors_import_still_works(self):
        """Simulate the pattern used by plaita/node/basic.py."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            from plaita.errors import ErrorHandler, RecoverableErrorHandler
        handler = ErrorHandler()
        # strategy 字段 2026-07 起存为 ErrorStrategy enum; == enum 或其 .value 均成立
        from plaita.core.errors import ErrorStrategy
        assert handler.strategy == ErrorStrategy.ABORT
        assert handler.strategy.value == "abort"
        recoverable = RecoverableErrorHandler()
        assert recoverable.retry_times == 0
