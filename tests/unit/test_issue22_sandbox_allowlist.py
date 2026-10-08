"""plaita#22 回归：沙箱后端白名单必须在生产入口真正接线。

机制（``CodeNode`` 解析期拒绝白名单外的 ``sandbox_backend``）2026-09 评审就已落地，
但 worker / console 入口都不传 ``allowed_backends``——等于没接线：流程 JSON 写一行
``"sandbox_backend": "unsafe"``（进程内 raw ``exec``）即任意租户在 worker / console
进程内执行任意代码（可读宿主凭据）。

覆盖三层：
1. ``resolve_sandbox_allowed_backends`` 的默认档与 env 解析；
2. worker 入口端到端：未配置白名单时 ``unsafe`` 在**解析期**被拒、默认后端仍可用；
3. 生产入口接线守卫（AST 扫描）：任何 ``register_code_node(...)`` 调用必须传
   ``allowed_backends=``，否则新入口又会把白名单晾在一边。
"""
from __future__ import annotations

import ast
import logging
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture()
def sandbox_globals(monkeypatch):
    """恢复 ``code`` 模块级沙箱全局，并清空两个相关 env（防跨用例污染）。"""
    import plaita.node.code as code_mod

    saved_allowed = code_mod._ALLOWED_SANDBOX_BACKENDS
    saved_default = code_mod._DEFAULT_SANDBOX_BACKEND
    monkeypatch.delenv("PLAITA_SANDBOX_ALLOWED_BACKENDS", raising=False)
    monkeypatch.delenv("PLAITA_CODE_BACKEND", raising=False)
    yield code_mod
    code_mod._ALLOWED_SANDBOX_BACKENDS = saved_allowed
    code_mod._DEFAULT_SANDBOX_BACKEND = saved_default


def test_default_whitelist_is_docker_plus_effective_backend(sandbox_globals, caplog):
    """未配置 env → 默认 (docker,) ∪ 生效后端；unsafe 不在其中，且打 WARNING。"""
    from plaita.node import resolve_sandbox_allowed_backends

    with caplog.at_level(logging.WARNING, logger="plaita.node"):
        worker_allowed = resolve_sandbox_allowed_backends("subprocess", "flow-worker")
    assert worker_allowed == ("docker", "subprocess")
    assert "unsafe" not in worker_allowed
    assert any(
        r.levelno == logging.WARNING and "PLAITA_SANDBOX_ALLOWED_BACKENDS" in r.getMessage()
        for r in caplog.records
    ), [r.getMessage() for r in caplog.records]

    # console 档：模块默认后端 docker → 只剩 docker
    sandbox_globals._DEFAULT_SANDBOX_BACKEND = "docker"
    assert resolve_sandbox_allowed_backends(component="console") == ("docker",)


def test_env_whitelist_is_honoured_and_validated(sandbox_globals, monkeypatch):
    from plaita.node import resolve_sandbox_allowed_backends

    monkeypatch.setenv("PLAITA_SANDBOX_ALLOWED_BACKENDS", "docker, restricted")
    assert resolve_sandbox_allowed_backends("docker") == ("docker", "restricted")

    monkeypatch.setenv("PLAITA_SANDBOX_ALLOWED_BACKENDS", "docker bogus")
    with pytest.raises(ValueError, match="unknown sandbox backend"):
        resolve_sandbox_allowed_backends("docker")


def test_explicit_unsafe_logs_critical(sandbox_globals, caplog):
    """显式放行/默认选 unsafe 时不静默：CRITICAL 告警指名后果。"""
    from plaita.node import resolve_sandbox_allowed_backends

    with caplog.at_level(logging.CRITICAL, logger="plaita.node"):
        allowed = resolve_sandbox_allowed_backends("unsafe", "flow-worker")
    assert "unsafe" in allowed
    assert any(r.levelno == logging.CRITICAL for r in caplog.records)


def test_worker_entry_rejects_unsafe_at_parse(sandbox_globals, caplog):
    """worker 入口：未配置白名单时流程 JSON 的 ``sandbox_backend: unsafe`` 解析期即拒。"""
    pytest.importorskip("cachetools")
    pytest.importorskip("redis")

    from plaita.node import get_default_registry
    from plaita.server import flow_worker as fw

    with caplog.at_level(logging.WARNING, logger="plaita.node"):
        fw._register_code_node_for_worker()

    registry = get_default_registry()
    with pytest.raises(Exception, match="not allowed"):
        registry.parse_node({
            "type": "code", "id": "c", "code": "def run(i):\n    return i",
            "sandbox_backend": "unsafe",
        })
    node = registry.parse_node({
        "type": "code", "id": "c2", "code": "def run(i):\n    return i",
    })
    assert node.sandbox_backend == "subprocess"
    assert sandbox_globals._ALLOWED_SANDBOX_BACKENDS == frozenset({"docker", "subprocess"})


# --- 接线守卫 -------------------------------------------------------------

ENTRY_FILES = (
    ROOT / "plaita" / "server" / "flow_worker.py",
    ROOT / "plaita-console" / "backend" / "main.py",
    ROOT / "plaita-console" / "backend" / "api" / "cluster.py",
)


def _register_code_node_calls(path: Path):
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in ast.walk(tree):
        func = node.func if isinstance(node, ast.Call) else None
        name = getattr(func, "id", None) or getattr(func, "attr", None)
        if name == "register_code_node":
            yield node


def _production_call_sites():
    for directory in (ROOT / "plaita", ROOT / "plaita-console" / "backend"):
        for path in directory.rglob("*.py"):
            if "/tests/" in path.as_posix() or path.name.startswith("test_"):
                continue
            for call in _register_code_node_calls(path):
                yield path, call


def test_production_entry_points_pass_allowed_backends():
    missing = []
    seen_files = set()
    for path, call in _production_call_sites():
        seen_files.add(path)
        if "allowed_backends" not in {kw.arg for kw in call.keywords}:
            missing.append(f"{path.relative_to(ROOT)}:{call.lineno}")
    assert not missing, (
        "生产入口调用 register_code_node 必须显式传 allowed_backends=...（plaita#22："
        f"否则流程 JSON 可逐节点降级到 unsafe）：{missing}"
    )
    not_wired = [p for p in ENTRY_FILES if p not in seen_files]
    assert not not_wired, f"部署入口未注册 CodeNode：{[str(p) for p in not_wired]}"
