"""
T096: Import layering static analysis.

Verifies no plaita.core module imports from plaita.server, plaita.storage.redis,
or plaita.storage.sqlalchemy — enforcing the dependency direction:

    plaita.core  ←  plaita.server (allowed)
    plaita.core  →  plaita.server (FORBIDDEN)
"""
import ast
import pathlib

import pytest


FORBIDDEN_PREFIXES = (
    "plaita.server",
    "plaita.storage.redis",
    "plaita.storage.sqlalchemy",
)

CORE_DIR = pathlib.Path(__file__).resolve().parents[2] / "plaita" / "core"


def _extract_imports(filepath: pathlib.Path):
    source = filepath.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(filepath))
    imports = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.append((alias.name, node.lineno))
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.append((node.module, node.lineno))
    return imports


def _get_core_python_files():
    return sorted(CORE_DIR.glob("**/*.py"))


class TestImportLayering:
    """Verify plaita.core does not import from forbidden packages."""

    def test_no_core_to_server_imports(self):
        violations = []
        for pyfile in _get_core_python_files():
            for imp, lineno in _extract_imports(pyfile):
                for prefix in FORBIDDEN_PREFIXES:
                    if imp == prefix or imp.startswith(prefix + "."):
                        violations.append(
                            f"{pyfile.relative_to(CORE_DIR.parent.parent)}:{lineno} imports {imp}"
                        )

        assert violations == [], (
            "Reverse imports found in plaita/core/:\n"
            + "\n".join(f"  - {v}" for v in violations)
        )

    def test_no_type_checking_reverse_imports(self):
        """Even TYPE_CHECKING-guarded imports from server are forbidden in core."""
        violations = []
        for pyfile in _get_core_python_files():
            source = pyfile.read_text(encoding="utf-8")
            tree = ast.parse(source, filename=str(pyfile))
            for node in ast.walk(tree):
                if isinstance(node, ast.If):
                    test = node.test
                    is_type_checking = (
                        (isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING")
                        or (isinstance(test, ast.Name) and test.id == "TYPE_CHECKING")
                    )
                    if not is_type_checking:
                        continue
                    for child in ast.walk(node):
                        if isinstance(child, ast.ImportFrom) and child.module:
                            for prefix in FORBIDDEN_PREFIXES:
                                if child.module == prefix or child.module.startswith(prefix + "."):
                                    violations.append(
                                        f"{pyfile.relative_to(CORE_DIR.parent.parent)}:{child.lineno} "
                                        f"TYPE_CHECKING imports {child.module}"
                                    )

        assert violations == [], (
            "TYPE_CHECKING reverse imports found in plaita/core/:\n"
            + "\n".join(f"  - {v}" for v in violations)
        )

    def test_core_modules_all_importable(self):
        """All plaita.core modules should import cleanly."""
        import importlib
        core_modules = [
            "plaita.core",
            "plaita.core.errors",
            "plaita.core.types",
            "plaita.core.expression",
            "plaita.core.context",
            "plaita.core.callback",
            "plaita.core.runner",
            "plaita.core.executor",
            "plaita.core.flow",
        ]
        for mod_name in core_modules:
            mod = importlib.import_module(mod_name)
            assert mod is not None, f"Failed to import {mod_name}"
