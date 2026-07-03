"""P1-5 回归: ``init_default_registry`` 显式初始化入口。

历史上默认 registry 是模块级 ``_default_registry = NodeRegistry()`` 在 import 期
静默创建, 插件发现隐式触发——隐式可变单例 + import 期副作用。0.5.0 新增
``init_default_registry(*extra_nodes)`` 把它变成显式启动步骤:
- 重建默认 registry (重注册内置节点 + 可选插件发现 + extra 节点);
- 标记"已显式初始化", 之后 ``get_default_registry()`` 不再发隐式初始化 debug。
``get_default_registry()`` 仍向后兼容。
"""
from __future__ import annotations

import logging

import pytest

from plaita.node import (
    NodeRegistry,
    get_default_registry,
    init_default_registry,
)
from plaita.node.basic import Node


class _DummyNode(Node):
    node_type = "dummy-p15"
    node_name = "P1-5 dummy"

    def execute(self, execution):  # pragma: no cover
        return None


@pytest.fixture(autouse=True)
def _restore_registry():
    """每个用例后恢复默认 registry 到干净状态, 不污染其他测试。

    用 ``auto_discover=True`` 重建, 使全局 registry 回到"内置 + 已发现插件"的
    等价于首次 ``get_default_registry()`` 后的状态, 避免留给后续测试一个"插件
    被清空"的非典型环境。
    """
    yield
    init_default_registry(auto_discover=True)
    get_default_registry().unregister("dummy-p15")


class TestInitDefaultRegistry:
    def test_explicit_init_registers_extra_nodes(self):
        reg = init_default_registry(_DummyNode, auto_discover=False)
        assert "dummy-p15" in reg
        assert reg is get_default_registry()

    def test_explicit_init_marks_initialized_no_implicit_debug(self, caplog):
        init_default_registry(auto_discover=False)
        with caplog.at_level(logging.DEBUG, logger="plaita.node"):
            get_default_registry()
        assert not any("without explicit init_default_registry" in r.message
                       for r in caplog.records)

    def test_implicit_get_emits_debug_when_not_initialized(self, caplog, monkeypatch):
        # 强制未初始化 + 未 discover 状态
        import plaita.node as node_mod
        monkeypatch.setattr(node_mod, "_default_registry_explicitly_initialized", False)
        reg = node_mod._default_registry
        monkeypatch.setattr(reg, "_discovered", False)
        with caplog.at_level(logging.DEBUG, logger="plaita.node"):
            get_default_registry()
        assert any("without explicit init_default_registry" in r.message
                   for r in caplog.records)

    def test_reinit_clears_previous_extra_nodes(self):
        init_default_registry(_DummyNode, auto_discover=False)
        assert "dummy-p15" in get_default_registry()
        # 再次 init 不带 dummy -> dummy 应被清掉
        init_default_registry(auto_discover=False)
        assert "dummy-p15" not in get_default_registry()

    def test_get_default_registry_still_works_without_explicit_init(self):
        """向后兼容: 不调 init 也能用, 只是隐式 discover。"""
        import plaita.node as node_mod
        # 不改状态, 直接拿默认 registry 应能解析内置 start/end
        reg = get_default_registry()
        assert "start" in reg and "end" in reg
        assert isinstance(reg, NodeRegistry)
