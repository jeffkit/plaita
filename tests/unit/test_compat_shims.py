"""Tests for plaita.errors and plaita.types compatibility shims.

Both modules are deprecated re-export shims that:
- Emit DeprecationWarning on first access to each symbol
- Cache the resolved value so subsequent accesses are warning-free
- Raise AttributeError for unknown symbols
- Expose __dir__() listing __all__

Coverage target: plaita/errors.py (0%) and plaita/types.py (0%)
"""

from __future__ import annotations

import importlib
import sys
import unittest
import warnings


def _fresh_import(module_name: str):
    """Re-import a module bypassing the cache (to reset globals())."""
    if module_name in sys.modules:
        del sys.modules[module_name]
    return importlib.import_module(module_name)


class TestErrorsShim(unittest.TestCase):
    def setUp(self):
        # Fresh import ensures globals() cache is empty
        if "plaita.errors" in sys.modules:
            del sys.modules["plaita.errors"]

    def test_getattr_emits_deprecation_warning(self):
        """Accessing a known name from plaita.errors triggers DeprecationWarning."""
        import plaita.errors as m
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            _ = m.NodeExecutionError
        self.assertTrue(any(issubclass(x.category, DeprecationWarning) for x in w))
        self.assertTrue(any("plaita.errors" in str(x.message) for x in w))

    def test_getattr_caches_after_first_access(self):
        """Second access to the same name produces no new warning."""
        import plaita.errors as m
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            _ = m.NodeExecutionError  # first access (warning)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            _ = m.NodeExecutionError  # second access (cached, no warning)
        dep_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
        self.assertEqual(dep_warnings, [])

    def test_getattr_returns_correct_class(self):
        """Resolved symbol is the actual class from plaita.core.errors."""
        import plaita.errors as m
        from plaita.core.errors import NodeExecutionError
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            cls = m.NodeExecutionError
        self.assertIs(cls, NodeExecutionError)

    def test_getattr_raises_for_unknown(self):
        """Unknown attribute raises AttributeError."""
        import plaita.errors as m
        with self.assertRaises(AttributeError):
            _ = m.NonExistentSymbol

    def test_dir_lists_all_symbols(self):
        """__dir__ returns all items in __all__."""
        import plaita.errors as m
        d = dir(m)
        self.assertIn("NodeExecutionError", d)
        self.assertIn("FlowResultError", d)
        self.assertIn("ResumeError", d)

    def test_multiple_symbols(self):
        """All key symbols from __all__ are accessible."""
        import plaita.errors as m
        symbols_to_check = [
            "FlowResultError", "NodeExecutionError", "NodeTimeoutError",
            "ResumeError", "ErrorStrategy", "FlowTimeoutError",
        ]
        for sym in symbols_to_check:
            with warnings.catch_warnings(record=True):
                warnings.simplefilter("always")
                obj = getattr(m, sym)
            self.assertIsNotNone(obj, f"{sym} should not be None")


class TestTypesShim(unittest.TestCase):
    def setUp(self):
        if "plaita.types" in sys.modules:
            del sys.modules["plaita.types"]

    def test_getattr_emits_deprecation_warning(self):
        """Accessing a known name from plaita.types triggers DeprecationWarning."""
        import plaita.types as m
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            _ = m.STRING
        self.assertTrue(any(issubclass(x.category, DeprecationWarning) for x in w))

    def test_getattr_caches_after_first_access(self):
        """Second access has no DeprecationWarning."""
        import plaita.types as m
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            _ = m.STRING
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            _ = m.STRING
        dep = [x for x in w if issubclass(x.category, DeprecationWarning)]
        self.assertEqual(dep, [])

    def test_getattr_returns_correct_value(self):
        """Resolved symbol matches the value in plaita.core.types."""
        import plaita.types as m
        from plaita.core import types as core_types
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            val = m.STRING
        self.assertEqual(val, core_types.STRING)

    def test_getattr_raises_for_unknown(self):
        import plaita.types as m
        with self.assertRaises(AttributeError):
            _ = m.NonExistentType

    def test_dir_lists_all_symbols(self):
        import plaita.types as m
        d = dir(m)
        self.assertIn("STRING", d)
        self.assertIn("ARRAY", d)
        self.assertIn("OBJECT", d)
        self.assertIn("ANY", d)

    def test_all_core_type_constants(self):
        """All constant-type symbols from __all__ are accessible."""
        import plaita.types as m
        constants = ["STRING", "BOOL", "INTEGER", "FLOAT", "ARRAY", "OBJECT", "ANY", "NULL"]
        for c in constants:
            with warnings.catch_warnings(record=True):
                warnings.simplefilter("always")
                val = getattr(m, c)
            self.assertIsNotNone(val, f"{c} should not be None")

    def test_callables_accessible(self):
        """Function-type symbols (get_native_type, valid) are callable."""
        import plaita.types as m
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            fn = m.get_native_type
        self.assertTrue(callable(fn))


if __name__ == "__main__":
    unittest.main()
