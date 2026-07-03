"""
Tests for clean package layering.

Layering model enforced here:

    foundation = {plaita.core, plaita.node, plaita.io}
        may import each other freely
        may **lazily** import plaita.event (function-body ``from plaita.event
        import ...`` only — see "Lazy event import" below) for default-bus
        fallback resolution
        MUST NOT import plaita.storage / plaita.server
        MUST NOT ``from plaita.event import ...`` at module top level (would
        force every foundation user to drag event's deps even if they never
        touch distributed execution)
    plaita.event
        may import foundation
        MUST NOT import plaita.storage / plaita.server
    plaita.storage
        may import foundation + plaita.event
        MUST NOT import plaita.server
    plaita.server
        may import anything (top layer)

``TYPE_CHECKING`` imports are exempt — they are type-only and never execute
at runtime, so they cannot create a real reverse dependency.

Lazy event import
-----------------
Historically foundation→event was forbidden outright and worked around with a
global mutable ``_default_event_bus_provider`` (an anti-pattern of its own).
Foundation now lazy-imports ``plaita.event`` inside function bodies for the
default-bus fallback path — runtime cost is one import per cold-cache miss,
no global state, full type hints. This is allowed **only inside functions**;
top-level imports remain forbidden so importing ``plaita.core`` does not
force event's optional deps on every consumer.
"""

import ast
import pathlib
import pytest

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
_LOKI = _REPO_ROOT / "plaita"

# layer name -> (set of module prefixes that members of this layer may NOT import)
_FORBIDDEN = {
    "foundation": ("plaita.storage", "plaita.server"),
    "event": ("plaita.storage", "plaita.server"),
    "storage": ("plaita.server",),
    "server": (),
}

# Foundation is also allowed to **lazily** (function-body only) import
# ``plaita.event`` for default-bus fallback — see module docstring.
# Foundation MUST NOT import event at module top level. We approximate
# "top level" as "import not nested in any FunctionDef/AsyncFunctionDef" —
# ``_collect_top_level_imports`` below enforces this.

# module prefix -> layer name
def _layer_for(module_path: pathlib.Path) -> str | None:
    rel = module_path.relative_to(_LOKI).as_posix()
    if rel.startswith("core/"):
        return "foundation"
    if rel.startswith("node/"):
        return "foundation"
    if rel == "io.py":
        return "foundation"
    if rel.startswith("event/"):
        return "event"
    if rel.startswith("storage/"):
        return "storage"
    if rel.startswith("server/"):
        return "server"
    return None


def _is_type_checking_block(node: ast.If) -> bool:
    test = node.test
    if isinstance(test, ast.Name) and test.id == "TYPE_CHECKING":
        return True
    # `if typing.TYPE_CHECKING:` / `from typing import TYPE_CHECKING as TC`
    if isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING":
        return True
    return False


def _collect_runtime_imports(tree: ast.AST) -> list[str]:
    """Return module names imported at runtime (skipping TYPE_CHECKING)."""
    imports: list[str] = []

    class _Walker(ast.NodeVisitor):
        def __init__(self):
            self.in_type_checking = 0

        def visit_Import(self, node: ast.Import):
            if not self.in_type_checking:
                for alias in node.names:
                    imports.append(alias.name)
            self.generic_visit(node)

        def visit_ImportFrom(self, node: ast.ImportFrom):
            # Only absolute imports resolve to a real module dependency;
            # relative imports stay inside the package and are handled by
            # the per-layer prefix mapping elsewhere.
            if not self.in_type_checking and node.level == 0 and node.module:
                imports.append(node.module)
            self.generic_visit(node)

        def visit_If(self, node: ast.If):
            if _is_type_checking_block(node):
                self.in_type_checking += 1
                for child in node.body:
                    self.visit(child)
                self.in_type_checking -= 1
                for child in node.orelse:
                    self.visit(child)
            else:
                self.generic_visit(node)

    _Walker().visit(tree)
    return imports


def _all_py_files(layer: str):
    for pyfile in _LOKI.rglob("*.py"):
        if _layer_for(pyfile) != layer:
            continue
        # skip the compat shim package __init__ noise but keep modules
        yield pyfile


def _collect_top_level_imports(tree: ast.AST) -> list[str]:
    """Imports that live at module top level (not nested in any function).

    Foundation files may lazy-import ``plaita.event`` inside a function body
    for default-bus fallback, but top-level foundation→event imports are still
    forbidden (would force event's optional deps on every foundation consumer).

    ``if TYPE_CHECKING:`` blocks are exempt — those imports are type-only and
    never execute at runtime, so they cannot create a real reverse dependency.
    """
    imports: list[str] = []

    class _Walker(ast.NodeVisitor):
        def __init__(self):
            self.in_function = 0
            self.in_type_checking = 0

        def visit_Import(self, node: ast.Import):
            if not self.in_function and not self.in_type_checking:
                for alias in node.names:
                    imports.append(alias.name)
            self.generic_visit(node)

        def visit_ImportFrom(self, node: ast.ImportFrom):
            if not self.in_function and not self.in_type_checking and node.level == 0 and node.module:
                imports.append(node.module)
            self.generic_visit(node)

        def visit_FunctionDef(self, node):
            self.in_function += 1
            self.generic_visit(node)
            self.in_function -= 1

        def visit_AsyncFunctionDef(self, node):
            self.in_function += 1
            self.generic_visit(node)
            self.in_function -= 1

        def visit_If(self, node):
            # ``if TYPE_CHECKING:`` (optionally ``if False:`` / ``if typing.TYPE_CHECKING``)
            # guards type-only imports that never run — exempt them.
            if self._is_type_checking_guard(node.test):
                self.in_type_checking += 1
                self.generic_visit(node)
                self.in_type_checking -= 1
            else:
                self.generic_visit(node)

        @staticmethod
        def _is_type_checking_guard(test: ast.AST) -> bool:
            if isinstance(test, ast.Name) and test.id == "TYPE_CHECKING":
                return True
            if (
                isinstance(test, ast.Attribute)
                and isinstance(test.value, ast.Name)
                and test.value.id == "typing"
                and test.attr == "TYPE_CHECKING"
            ):
                return True
            return False

    _Walker().visit(tree)
    return imports


class TestCoreLayeringImports:
    """T017: Verify plaita.core modules import cleanly without server/redis packages."""

    def test_import_plaita_core(self):
        import plaita.core
        assert plaita.core is not None

    def test_import_plaita_core_errors(self):
        from plaita.core.errors import (
            FlowResultError,
            NodeException,
            FlowErrorType,
            FlowExecutionException,
            ErrorStrategy,
            ErrorHandler,
            RecoverableErrorHandler,
        )
        assert FlowResultError is not None

    def test_import_plaita_core_types(self):
        from plaita.core.types import (
            ValidationError,
            STRING,
            BOOL,
            INTEGER,
            native_types,
            get_native_type,
            valid,
        )
        assert STRING == "string"

    def test_core_errors_identity(self):
        from plaita.core.errors import FlowResultError as CoreFRE
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            from plaita.errors import FlowResultError as ShimFRE
        assert CoreFRE is ShimFRE

    def test_core_types_identity(self):
        from plaita.core.types import ValidationError as CoreVE
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            from plaita.types import ValidationError as ShimVE
        assert CoreVE is ShimVE


class TestReverseImportStaticAnalysis:
    """T018: Scan plaita modules for forbidden upward (reverse) imports.

    Covers the full layering model, not just ``plaita.core``. TYPE_CHECKING
    imports are exempt.
    """

    @staticmethod
    def _violations_for_layer(layer: str) -> list[str]:
        forbidden = _FORBIDDEN[layer]
        violations = []
        for pyfile in _all_py_files(layer):
            try:
                tree = ast.parse(pyfile.read_text(encoding="utf-8"), filename=str(pyfile))
            except SyntaxError:
                continue
            for imp in _collect_runtime_imports(tree):
                for prefix in forbidden:
                    if imp == prefix or imp.startswith(prefix + "."):
                        violations.append(f"{pyfile.relative_to(_REPO_ROOT)}: imports {imp}")
        return violations

    def test_foundation_no_upward_imports(self):
        violations = self._violations_for_layer("foundation")
        assert violations == [], (
            "Foundation layer (core/node/io) must not import storage/server:\n"
            + "\n".join(f"  - {v}" for v in violations)
        )

    def test_foundation_no_top_level_event_imports(self):
        """Foundation may lazy-import ``plaita.event`` inside function bodies
        (for default-bus fallback), but a top-level foundation→event import
        would force event's optional deps on every consumer and is forbidden.
        """
        violations = []
        for pyfile in _all_py_files("foundation"):
            try:
                tree = ast.parse(pyfile.read_text(encoding="utf-8"), filename=str(pyfile))
            except SyntaxError:
                continue
            for imp in _collect_top_level_imports(tree):
                if imp == "plaita.event" or imp.startswith("plaita.event."):
                    violations.append(f"{pyfile.relative_to(_REPO_ROOT)}: top-level import {imp}")
        assert violations == [], (
            "Foundation layer may only lazy-import plaita.event inside function "
            "bodies (top-level imports force event's deps on every consumer):\n"
            + "\n".join(f"  - {v}" for v in violations)
        )

    def test_event_no_upward_imports(self):
        violations = self._violations_for_layer("event")
        assert violations == [], (
            "plaita.event must not import storage/server:\n"
            + "\n".join(f"  - {v}" for v in violations)
        )

    def test_storage_no_upward_imports(self):
        violations = self._violations_for_layer("storage")
        assert violations == [], (
            "plaita.storage must not import server:\n"
            + "\n".join(f"  - {v}" for v in violations)
        )

    @pytest.mark.parametrize("forbidden_prefix", [
        "plaita.server",
        "plaita.storage.redis",
        "plaita.storage.sqlalchemy",
    ])
    def test_no_optional_backends_in_core(self, forbidden_prefix):
        """Original SC-002 guard: core never pulls optional backends."""
        violations = []
        for pyfile in (_LOKI / "core").rglob("*.py"):
            try:
                tree = ast.parse(pyfile.read_text(encoding="utf-8"), filename=str(pyfile))
            except SyntaxError:
                continue
            for imp in _collect_runtime_imports(tree):
                if imp == forbidden_prefix or imp.startswith(forbidden_prefix + "."):
                    violations.append(f"{pyfile.name}: imports {imp}")
        assert violations == [], "\n".join(violations)
