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


def _pkg_parts_of(pyfile: pathlib.Path) -> tuple[str, ...]:
    """pyfile 所定义模块的**绝对包路径**：``plaita/storage/fenced.py`` → ``("plaita", "storage")``。

    ``__init__.py`` 的父目录恰好就是包自身（``plaita/storage/__init__.py`` 的
    ``parts[:-1]`` 同样是 ``("storage",)``），两种情况同一式。
    """
    return ("plaita",) + pyfile.resolve().relative_to(_LOKI).parts[:-1]


def _resolve_import_from(node: ast.ImportFrom, pkg_parts: tuple[str, ...]) -> list[str]:
    """把 ImportFrom（含相对导入）解析成绝对模块名（PEP 328：按所在包上溯）。

    ``from ..server.execution_lease import X`` in ``plaita.storage.fenced``
    → ``plaita.server.execution_lease``。``from . import name``（module 为
    None）解析为「包 + alias 名」的子模块候选——对前缀禁表是保守近似：
    真子模块必命中，普通属性多记一条无害名。level 上溯越过 plaita 顶层
    （导入已落在包外）不产生 ``plaita.*`` 名，返回空。
    """
    if node.level == 0:
        return [node.module] if node.module else []
    up = node.level - 1
    if up >= len(pkg_parts):
        return []
    head = ".".join(pkg_parts[: len(pkg_parts) - up] if up else pkg_parts)
    if node.module:
        return [f"{head}.{node.module}"]
    names = [alias.name for alias in node.names if alias.name != "*"]
    return [f"{head}.{name}" for name in names] or [head]


def _collect_runtime_imports(tree: ast.AST, pyfile: pathlib.Path) -> list[str]:
    """Return module names imported at runtime (skipping TYPE_CHECKING).

    相对导入（``node.level > 0``）按所在文件包路径解析成绝对名后再参与
    禁表比对（#96：此前 level>0 被整体跳过，storage→server 的相对反向
    依赖恒绿）。
    """
    imports: list[str] = []
    pkg_parts = _pkg_parts_of(pyfile)

    class _Walker(ast.NodeVisitor):
        def __init__(self):
            self.in_type_checking = 0

        def visit_Import(self, node: ast.Import):
            if not self.in_type_checking:
                for alias in node.names:
                    imports.append(alias.name)
            self.generic_visit(node)

        def visit_ImportFrom(self, node: ast.ImportFrom):
            # 相对导入与绝对导入同权：解析成绝对名后进同一份禁表比对（#96）。
            if not self.in_type_checking:
                imports.extend(_resolve_import_from(node, pkg_parts))
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


def _collect_top_level_imports(tree: ast.AST, pyfile: pathlib.Path) -> list[str]:
    """Imports that live at module top level (not nested in any function).

    Foundation files may lazy-import ``plaita.event`` inside a function body
    for default-bus fallback, but top-level foundation→event imports are still
    forbidden (would force event's optional deps on every foundation consumer).

    ``if TYPE_CHECKING:`` blocks are exempt — those imports are type-only and
    never execute at runtime, so they cannot create a real reverse dependency.

    相对导入同样解析成绝对名（#96：否则 foundation 顶层 ``from ..event …``
    的相对写法可绕过本约束）。
    """
    imports: list[str] = []
    pkg_parts = _pkg_parts_of(pyfile)

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
            if not self.in_function and not self.in_type_checking:
                imports.extend(_resolve_import_from(node, pkg_parts))
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
            for imp in _collect_runtime_imports(tree, pyfile):
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
            for imp in _collect_top_level_imports(tree, pyfile):
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
            for imp in _collect_runtime_imports(tree, pyfile):
                if imp == forbidden_prefix or imp.startswith(forbidden_prefix + "."):
                    violations.append(f"{pyfile.name}: imports {imp}")
        assert violations == [], "\n".join(violations)


class TestRelativeImportResolution:
    """#96：相对导入必须解析成绝对名再比对禁表。

    门禁曾对 ``node.level > 0`` 的 ImportFrom 整体跳过（旧注释谎称由别处的
    per-layer prefix mapping 处理，实际并不存在），``plaita/storage/fenced.py``
    相对反向导入 ``..server.execution_lease`` 因此恒绿。
    """

    def test_relative_importfrom_resolves_to_absolute(self):
        node = ast.parse(
            "from ..server.execution_lease import ExecutionLeaseError\n"
        ).body[0]
        assert _resolve_import_from(node, ("plaita", "storage")) == [
            "plaita.server.execution_lease"
        ]

    def test_relative_from_package_imports_resolve_as_submodule_candidates(self):
        # ``from ..server import execution_lease`` 的 node.module 是 "server"：
        # 解析到基名 plaita.server（alias 是属性/子模块二选一，基名已足以
        # 命中禁前缀 plaita.server，不会漏报）。
        node = ast.parse("from ..server import execution_lease\n").body[0]
        assert _resolve_import_from(node, ("plaita", "storage")) == ["plaita.server"]

    def test_single_dot_import_resolves_to_sibling(self):
        node = ast.parse("from . import base\n").body[0]
        assert _resolve_import_from(node, ("plaita", "storage")) == ["plaita.storage.base"]

    def test_overshoot_beyond_plaita_resolves_to_nothing(self):
        node = ast.parse("from ...outside import thing\n").body[0]
        assert _resolve_import_from(node, ("plaita", "storage")) == []

    def test_absolute_importfrom_unchanged(self):
        node = ast.parse("from plaita.server.flow_worker import FlowWorker\n").body[0]
        assert _resolve_import_from(node, ("plaita", "storage")) == [
            "plaita.server.flow_worker"
        ]


class TestRelativeImportGateCatchesReverseDeps:
    """端到端负控：相对写法的反向依赖必须被门禁报出（修前恒绿 = 假绿）。"""

    def test_storage_relative_reverse_import_is_flagged(self, tmp_path, monkeypatch):
        root = tmp_path / "plaita" / "storage"
        root.mkdir(parents=True)
        (root / "violator.py").write_text(
            "from ..server.execution_lease import ExecutionLeaseError\n", encoding="utf-8"
        )
        monkeypatch.setitem(globals(), "_LOKI", tmp_path / "plaita")
        monkeypatch.setitem(globals(), "_REPO_ROOT", tmp_path)
        violations = TestReverseImportStaticAnalysis._violations_for_layer("storage")
        assert any(
            "violator.py" in v and "plaita.server.execution_lease" in v for v in violations
        ), f"相对导入的反向依赖必须被门禁报出（#96 前恒空）: {violations}"

    def test_foundation_relative_top_level_event_import_is_flagged(
        self, tmp_path, monkeypatch
    ):
        root = tmp_path / "plaita" / "core"
        root.mkdir(parents=True)
        (root / "violator.py").write_text(
            "from ..event import bus\n", encoding="utf-8"
        )
        monkeypatch.setitem(globals(), "_LOKI", tmp_path / "plaita")
        violations = []
        for pyfile in _all_py_files("foundation"):
            tree = ast.parse(pyfile.read_text(encoding="utf-8"), filename=str(pyfile))
            for imp in _collect_top_level_imports(tree, pyfile):
                if imp == "plaita.event" or imp.startswith("plaita.event."):
                    violations.append(f"{pyfile.name}: top-level import {imp}")
        assert violations, (
            "foundation 顶层相对写法的 event 导入必须被拦（#96 前恒空）: " + str(violations)
        )
