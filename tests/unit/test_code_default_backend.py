"""P0-4 回归: CodeNode 默认沙箱后端 = docker, daemon 不可用时拒绝注册。

0.5.0 安全模型翻转:
- 库默认后端从 ``"restricted"`` (RestrictedPython, AST 级, 有绕过向量) 改为
  ``"docker"`` (容器级隔离);
- ``register_code_node()`` 在生效后端为 docker 但 daemon 不可用时**拒绝注册**,
  不允许静默降级到 restricted;
- 调用方可经 ``register_code_node(default_backend=...)`` 显式选弱后端。
"""
from __future__ import annotations

import unittest
from unittest import mock

import pytest

from plaita.node import code as code_module
from plaita.node import get_default_registry, register_code_node
from plaita.node.code import CodeNode


@pytest.fixture(autouse=True)
def _restore_module_default():
    """每个用例后恢复 _DEFAULT_SANDBOX_BACKEND 与 registry 状态, 避免污染。"""
    saved = code_module._DEFAULT_SANDBOX_BACKEND
    yield
    code_module._DEFAULT_SANDBOX_BACKEND = saved
    get_default_registry().unregister("code")


class TestDefaultBackendIsDocker:
    def test_module_default_is_docker(self):
        # 直接检查模块变量——不依赖注册副作用
        # (test_code.py 注册时改成 restricted, 但本文件不调 register_code_node)
        assert code_module._DEFAULT_SANDBOX_BACKEND == "docker"

    def test_codenode_field_default_is_none_then_resolved(self):
        """CodeNode 字段默认 None, validator 填模块默认 (docker)。"""
        # 临时把模块默认设回 docker (本模块 autouse fixture 没注册过 code)
        code_module._DEFAULT_SANDBOX_BACKEND = "docker"
        node = CodeNode(id="c", code="def run(a):\n    return a", language="python")
        assert node.sandbox_backend == "docker"

    def test_explicit_backend_overrides_default(self):
        code_module._DEFAULT_SANDBOX_BACKEND = "docker"
        node = CodeNode(id="c", code="def run(a):\n    return a",
                        language="python", sandbox_backend="subprocess")
        assert node.sandbox_backend == "subprocess"


class TestRegisterRefusesWithoutDocker:
    def test_register_raises_when_docker_unavailable(self, monkeypatch):
        """默认后端 docker + daemon 不可用 → 拒绝注册, 报错指明降级路径。"""
        monkeypatch.setattr(code_module, "_docker_available", lambda: False)
        code_module._DEFAULT_SANDBOX_BACKEND = "docker"  # 确保生效后端是 docker
        with pytest.raises(RuntimeError, match="Docker daemon is not available"):
            register_code_node()

    def test_register_with_explicit_subprocess_skips_docker_probe(self, monkeypatch):
        """显式 default_backend='subprocess' 不触发 docker 探测, 直接注册。"""
        called = {"n": 0}
        def _fail():
            called["n"] += 1
            return False
        monkeypatch.setattr(code_module, "_docker_available", _fail)
        register_code_node(default_backend="subprocess")
        assert called["n"] == 0  # 没探 docker
        assert code_module._DEFAULT_SANDBOX_BACKEND == "subprocess"
        assert "code" in get_default_registry()

    def test_error_message_lists_downgrade_paths(self, monkeypatch):
        monkeypatch.setattr(code_module, "_docker_available", lambda: False)
        code_module._DEFAULT_SANDBOX_BACKEND = "docker"
        with pytest.raises(RuntimeError) as exc:
            register_code_node()
        msg = str(exc.value)
        assert "subprocess" in msg
        assert "unsafe" in msg
        assert "restricted" in msg  # 解释为何不再默认 restricted
