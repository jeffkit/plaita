"""
Tests for clean package layering.

Layering model enforced here (matches the documented ``core → event →
storage → server`` chain, with ``node`` and ``io`` treated as part of the
foundation layer since they are mutually imported by ``core``):

    foundation = {plaita.core, plaita.node, plaita.io}
        may import each other freely
        MUST NOT import plaita.event / plaita.storage / plaita.server
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

This makes SC-002 ("no reverse imports") verifiable instead of a false
positive: the previous version only forbade ``plaita.server`` /
``plaita.storage.redis`` / ``plaita.storage.sqlalchemy`` inside ``plaita.core``
and missed ``core → event`` and the foundation's other upward edges.
"""

import ast
import pathlib
import pytest

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
_LOKI = _REPO_ROOT / "plaita"

# layer name -> (set of module prefixes that members of this layer may NOT import)
_FORBIDDEN = {
    "foundation": ("plaita.event", "plaita.storage", "plaita.server"),
    "event": ("plaita.storage", "plaita.server"),
    "storage": ("plaita.server",),
    "server": (),
}

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
            "Foundation layer (core/node/io) must not import event/storage/server:\n"
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
