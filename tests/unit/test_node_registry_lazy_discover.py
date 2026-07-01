"""A1 复现: NodeRegistry 不应在构造/import 时扫描 entry_points。

当前 ``NodeRegistry.__init__`` 默认 ``auto_discover=True``, 模块加载时
``_default_registry = NodeRegistry()`` 会立即扫描 ``plaita.nodes`` entry_points,
进而加载 ``plaita.server.nodes.*`` (依赖 fastapi/redis/kafka), 让 core 在
``[all]`` 安装时 import 期就耦合到 server。

期望:
- ``NodeRegistry()`` 默认不扫描 entry_points (构造纯净);
- ``get_default_registry()`` 首次调用时惰性发现一次, 且幂等。
显式 ``auto_discover=True`` 仍保留立即发现语义 (供测试/插件用)。
"""

import warnings
from unittest.mock import MagicMock, patch

import pytest

from plaita.node.basic import Node


class _DummyNode(Node):
    node_type = "a1_dummy"
    node_name = "A1 Dummy"

    def execute(self, execution=None):
        return {"r": 1}


class TestNodeRegistryNoImportTimeScan:
    def test_default_construction_does_not_scan_entry_points(self):
        import plaita.node as nodemod

        with patch.object(nodemod, "entry_points") as ep:
            nodemod.NodeRegistry()  # 默认不应扫描
            ep.assert_not_called()

    def test_explicit_auto_discover_still_scans(self):
        import plaita.node as nodemod

        mock_ep = MagicMock()
        mock_ep.name = "a1_ep"
        mock_ep.load.return_value = _DummyNode
        with patch.object(nodemod, "entry_points", return_value=[mock_ep]):
            reg = nodemod.NodeRegistry(auto_discover=True)
        assert "a1_dummy" in reg

    def test_get_default_registry_is_lazy_and_idempotent(self, monkeypatch):
        import plaita.node as nodemod

        fresh = nodemod.NodeRegistry()  # 默认不扫描
        monkeypatch.setattr(nodemod, "_default_registry", fresh)

        mock_ep = MagicMock()
        mock_ep.name = "a1_lazy_ep"
        mock_ep.load.return_value = _DummyNode
        monkeypatch.setattr(nodemod, "entry_points", lambda **kw: [mock_ep])

        assert fresh._discovered is False, "新构造的默认注册表不应已发现插件"

        reg = nodemod.get_default_registry()
        assert fresh._discovered is True
        assert "a1_dummy" in reg

        # 再次调用应幂等, 不重复加载
        nodemod.get_default_registry()
        assert mock_ep.load.call_count == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
