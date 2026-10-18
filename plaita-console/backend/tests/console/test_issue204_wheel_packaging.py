"""#204：wheel 打包（运行时代码白名单）+ 包内导入布局回归。

两腿：
1. ``scripts/wheel_stage.py`` 的暂存规则——backend 里的 tests/build/db/egg-info/
   pyproject/requirements 不进 wheel，运行时代码与可选 webDist 保留；
2. 包布局导入：``api/``、``services/`` 里不存在「只在平铺布局可用」的裸导入
   （``from services import obs_link`` 这类会让 pip 安装后的包 import 直接炸）。
"""
from __future__ import annotations

import ast
import importlib.util
from pathlib import Path

import pytest

try:  # Python 3.11+
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - 项目支持 3.10
    tomllib = pytest.importorskip("tomli")

CONSOLE_DIR = Path(__file__).resolve().parents[3]
BACKEND_DIR = CONSOLE_DIR / "backend"
STAGE_SCRIPT = CONSOLE_DIR / "scripts" / "wheel_stage.py"


def _load_wheel_stage():
    spec = importlib.util.spec_from_file_location("_wheel_stage_test", STAGE_SCRIPT)
    assert spec and spec.loader, f"找不到 {STAGE_SCRIPT}"
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write(path: Path, text: str = "x") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _fake_project(root: Path) -> Path:
    backend = root / "backend"
    for name in ("__init__.py", "__main__.py", "auth.py", "config.py", "main.py", "run.py"):
        _write(backend / name)
    _write(backend / "api" / "__init__.py")
    _write(backend / "api" / "executions.py")
    _write(backend / "api" / "__pycache__" / "executions.pyc")
    _write(backend / "services" / "__init__.py")
    _write(backend / "migrations" / "env.py")
    _write(backend / "webDist" / "index.html", "<html></html>")
    # 非运行时产物：全部不得进 wheel
    _write(backend / "tests" / "console" / "test_x.py")
    _write(backend / "build" / "lib" / "plaita_console" / "main.py")
    _write(backend / "plaita_console.db")
    _write(backend / "plaita_console_backend.egg-info" / "entry_points.txt")
    _write(backend / "pyproject.toml", "[project]\nname='x'\n")
    _write(backend / "requirements.txt", "fastapi\n")
    return backend


def test_stage_keeps_runtime_and_webdist_drops_packaging_junk(tmp_path):
    wheel_stage = _load_wheel_stage()
    _fake_project(tmp_path)

    stage = wheel_stage.stage_package(tmp_path)
    assert stage == tmp_path / "build" / "wheel-package" / "plaita_console"
    staged = sorted(p.relative_to(stage).as_posix() for p in stage.rglob("*") if p.is_file())

    for expected in (
        "__init__.py",
        "__main__.py",
        "auth.py",
        "config.py",
        "main.py",
        "run.py",
        "api/executions.py",
        "services/__init__.py",
        "migrations/env.py",
        "webDist/index.html",
    ):
        assert expected in staged, f"{expected} 不在暂存包里：{staged}"

    forbidden_markers = ("/tests/", "/build/")
    for relative in staged:
        assert not relative.endswith((".db", ".pyc", ".pyo")), relative
        assert "egg-info" not in relative, relative
        assert relative not in {"pyproject.toml", "requirements.txt"}, relative
        assert "__pycache__" not in relative, relative
        assert not any(marker in f"/{relative}" for marker in forbidden_markers), relative


def test_pyproject_wires_stage_hook_and_stage_path():
    config = tomllib.loads((CONSOLE_DIR / "pyproject.toml").read_text(encoding="utf-8"))
    wheel = config["tool"]["hatch"]["build"]["targets"]["wheel"]
    assert wheel["force-include"] == {
        "build/wheel-package/plaita_console": "plaita_console"
    }, "force-include 必须指向暂存目录（否则 backend/tests 等会进 wheel）"
    assert wheel["hooks"]["custom"]["path"] == "scripts/hatch_build.py"


# ---------------------------------------------------------------------------
# 包布局导入：平铺导入只能作为 except ImportError 的回退
# ---------------------------------------------------------------------------
_TOP_LEVEL = {"services", "models", "auth", "config", "api"}


class _FlatImportFinder(ast.NodeVisitor):
    def __init__(self) -> None:
        self._protected: list[bool] = []
        self.violations: list[str] = []

    def _protected_now(self) -> bool:
        return any(self._protected)

    def visit_Try(self, node: ast.Try) -> None:
        has_relative = any(
            isinstance(n, ast.ImportFrom) and n.level > 0
            for n in ast.walk(ast.Module(body=node.body, type_ignores=[]))
        )
        self._protected.append(has_relative)
        for child in node.body:
            self.visit(child)
        self._protected.pop()
        if has_relative:
            # except 分支里的平铺导入是合法回退
            for branch in (*node.orelse, *node.finalbody):
                self.visit(branch)
        else:
            for handler in node.handlers:
                for child in handler.body:
                    self.visit(child)
            for branch in (*node.orelse, *node.finalbody):
                self.visit(branch)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if (
            node.level == 0
            and node.module
            and node.module.split(".")[0] in _TOP_LEVEL
            and not self._protected_now()
        ):
            names = ", ".join(alias.name for alias in node.names)
            self.violations.append(f"{node.lineno}: from {node.module} import {names}")

    def visit_Import(self, node: ast.Import) -> None:
        if self._protected_now():
            return
        for alias in node.names:
            if alias.name.split(".")[0] in _TOP_LEVEL:
                self.violations.append(f"{node.lineno}: import {alias.name}")


def test_package_imports_do_not_rely_on_flat_layout():
    violations: dict[str, list[str]] = {}
    for module_dir in (BACKEND_DIR / "api", BACKEND_DIR / "services"):
        for path in sorted(module_dir.glob("*.py")):
            finder = _FlatImportFinder()
            finder.visit(ast.parse(path.read_text(encoding="utf-8")))
            if finder.violations:
                violations[path.relative_to(BACKEND_DIR).as_posix()] = finder.violations

    assert not violations, (
        "这些平铺导入在 pip 安装后的包布局里会 ModuleNotFoundError（包内应先用 "
        f"相对导入 + except ImportError 回退）：{violations}"
    )
