"""
Unit tests for NodeRegistry (Phase 2).
Covers register, unregister, get, parse_node, list_types, copy, scoped isolation,
entry_points discovery, and backward-compatible dict proxy + deprecated helpers.
"""
import warnings
from unittest.mock import MagicMock, patch

import pytest

from plaita.node.basic import Node


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _DummyNode(Node):
    node_type = "test_dummy"
    node_name = "Dummy"

    def execute(self, execution=None):
        return {"result": "dummy"}


class _AnotherDummyNode(Node):
    node_type = "test_another"
    node_name = "Another Dummy"

    def execute(self, execution=None):
        return {"result": "another"}


# ---------------------------------------------------------------------------
# T019: NodeRegistry core API
# ---------------------------------------------------------------------------

class TestNodeRegistry:

    def _make_registry(self, **kwargs):
        from plaita.node import NodeRegistry
        return NodeRegistry(**kwargs)

    def test_register_and_get(self):
        reg = self._make_registry(auto_discover=False)
        reg.register(_DummyNode)
        assert reg.get("test_dummy") is _DummyNode

    def test_register_returns_class(self):
        """register() should return the node class so it can be used as a decorator."""
        reg = self._make_registry(auto_discover=False)
        result = reg.register(_DummyNode)
        assert result is _DummyNode

    def test_unregister(self):
        reg = self._make_registry(auto_discover=False)
        reg.register(_DummyNode)
        reg.unregister("test_dummy")
        assert reg.get("test_dummy") is None

    def test_unregister_missing_is_noop(self):
        reg = self._make_registry(auto_discover=False)
        reg.unregister("nonexistent")

    def test_get_missing_returns_none(self):
        reg = self._make_registry(auto_discover=False)
        assert reg.get("no_such_type") is None

    def test_parse_node_creates_instance(self):
        reg = self._make_registry(auto_discover=False)
        reg.register(_DummyNode)
        node = reg.parse_node({"type": "test_dummy", "id": "n1"})
        assert isinstance(node, _DummyNode)
        assert node.id == "n1"

    def test_parse_node_passthrough(self):
        """If already a Node instance, return as-is."""
        reg = self._make_registry(auto_discover=False)
        existing = _DummyNode(id="n1")
        assert reg.parse_node(existing) is existing

    def test_parse_node_unknown_type_raises(self):
        reg = self._make_registry(auto_discover=False)
        with pytest.raises(RuntimeError, match="unRecognized"):
            reg.parse_node({"type": "unknown_type_xyz", "id": "n1"})

    def test_parse_node_missing_type_raises(self):
        reg = self._make_registry(auto_discover=False)
        with pytest.raises(RuntimeError, match="not specific node type"):
            reg.parse_node({"id": "n1"})

    def test_list_types(self):
        reg = self._make_registry(auto_discover=False)
        reg.register(_DummyNode)
        reg.register(_AnotherDummyNode)
        types = reg.list_types()
        assert "test_dummy" in types
        assert "test_another" in types

    def test_contains(self):
        reg = self._make_registry(auto_discover=False)
        reg.register(_DummyNode)
        assert "test_dummy" in reg
        assert "nonexistent" not in reg

    def test_len(self):
        reg = self._make_registry(auto_discover=False)
        initial_len = len(reg)
        reg.register(_DummyNode)
        assert len(reg) == initial_len + 1

    def test_copy_isolation(self):
        """Copied registry is independent — changes don't propagate."""
        reg = self._make_registry(auto_discover=False)
        reg.register(_DummyNode)
        copy = reg.copy()
        copy.register(_AnotherDummyNode)
        assert "test_another" in copy
        assert "test_another" not in reg

    def test_builtins_registered(self):
        """Default registry should have all built-in node types."""
        reg = self._make_registry(auto_discover=False)
        expected = [
            "start", "end", "switch", "case", "if",
            "assignment", "loop", "map", "filter", "find",
            "reduce", "child", "reference", "code",
            "parallel", "http", "event",
        ]
        for nt in expected:
            assert nt in reg, f"Built-in node type '{nt}' not registered"


# ---------------------------------------------------------------------------
# T020: Entry-points discovery
# ---------------------------------------------------------------------------

class TestEntryPointsDiscovery:

    def test_discover_entry_points_loads_node(self):
        """Mock an entry point that loads a valid node class."""
        from plaita.node import NodeRegistry

        mock_ep = MagicMock()
        mock_ep.name = "test_ep_node"
        mock_ep.load.return_value = _DummyNode

        with patch("plaita.node.entry_points", return_value=[mock_ep]):
            reg = NodeRegistry(auto_discover=True)

        assert "test_dummy" in reg

    def test_discover_entry_points_error_tolerant(self):
        """If an entry point fails to load, it should be skipped gracefully."""
        from plaita.node import NodeRegistry

        mock_ep = MagicMock()
        mock_ep.name = "broken_plugin"
        mock_ep.load.side_effect = ImportError("missing package")

        with patch("plaita.node.entry_points", return_value=[mock_ep]):
            reg = NodeRegistry(auto_discover=True)

        assert "broken_plugin" not in reg


# ---------------------------------------------------------------------------
# T021: Backward compatibility — dict proxy, node_register, parse_node
# ---------------------------------------------------------------------------

class TestBackwardCompatDictProxy:

    def test_nodes_dict_contains(self):
        """Module-level `nodes` dict should contain built-in node types."""
        from plaita.node import nodes
        assert "start" in nodes
        assert "end" in nodes

    def test_nodes_dict_getitem(self):
        from plaita.node import nodes
        from plaita.node.start import Start
        assert nodes["start"] is Start

    def test_nodes_dict_len(self):
        from plaita.node import nodes
        assert len(nodes) >= 17

    def test_nodes_dict_iter(self):
        from plaita.node import nodes
        keys = list(nodes)
        assert "start" in keys
        assert "end" in keys

    def test_node_register_emits_deprecation(self):
        from plaita.node import node_register
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            node_register(_DummyNode)
            deprecation_warnings = [
                x for x in w if issubclass(x.category, DeprecationWarning)
            ]
            assert len(deprecation_warnings) >= 1

    def test_parse_node_module_level_emits_deprecation(self):
        from plaita.node import parse_node, node_register
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            node_register(_DummyNode)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            node = parse_node({"type": "test_dummy", "id": "n1"})
            deprecation_warnings = [
                x for x in w if issubclass(x.category, DeprecationWarning)
            ]
            assert len(deprecation_warnings) >= 1
        assert isinstance(node, _DummyNode)

    def test_get_default_registry(self):
        from plaita.node import get_default_registry, NodeRegistry
        reg = get_default_registry()
        assert isinstance(reg, NodeRegistry)
        assert "start" in reg
