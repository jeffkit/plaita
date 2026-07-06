"""变异测试专项断言 — plaita.node (NodeRegistry)

针对 node/__init__.py 47 个 survived 变异精准杀灭。
主要变异类别：
  1. __init__ 默认参数翻转 (auto_discover: bool = False → True)
  2. _discovered 初始值 (False → None)
  3. parse_node 中 get("type", None) 参数变化
  4. _register_builtins 注册值改为 None
  5. _discover_entry_points 版本号比较变化 (>= → >，(3,10) → (4,10))
  6. discover() 中 _discovered 相关逻辑
"""
from __future__ import annotations
import sys
import unittest
from unittest.mock import MagicMock, patch

from plaita.node import NodeRegistry
from plaita.node.start import Start
from plaita.node.end import End
from plaita.node.loop import Loop, Map, Filter, Find, Reduce
from plaita.node.concurrent import Parallel
from plaita.node.decide import Bool, Switch
from plaita.node.assignment import Assignment
from plaita.node.http import HTTP
from plaita.node.event_node import EventNode


class TestNodeRegistryInit(unittest.TestCase):

    def test_auto_discover_default_is_false(self):
        """_1: auto_discover 默认值 False → True。"""
        with patch.object(NodeRegistry, "_discover_entry_points") as mock_disc:
            reg = NodeRegistry()
            mock_disc.assert_not_called()

    def test_auto_discover_true_triggers_discover(self):
        """auto_discover=True 应调用 _discover_entry_points。"""
        with patch.object(NodeRegistry, "_discover_entry_points") as mock_disc:
            reg = NodeRegistry(auto_discover=True)
            mock_disc.assert_called_once()

    def test_discovered_flag_is_false_initially(self):
        """_3: self._discovered = False（若变为 None，discover() 逻辑会出错）。"""
        reg = NodeRegistry()
        self.assertFalse(reg._discovered)
        # None 与 False 行为不同：None 是 falsy 但 _discovered 应该是 bool
        self.assertIsInstance(reg._discovered, bool)

    def test_parent_copies_nodes(self):
        """parent 参数应将父 registry 的节点复制到新实例。"""
        parent = NodeRegistry()
        parent.register(Start)
        child = NodeRegistry(parent=parent)
        self.assertIn("start", child)

    def test_empty_registry_contains_builtins(self):
        """构造后应包含内置节点。"""
        reg = NodeRegistry()
        self.assertIn("start", reg)
        self.assertIn("end", reg)
        self.assertIn("loop", reg)


class TestNodeRegistryRegisterBuiltins(unittest.TestCase):

    def test_builtins_are_node_classes_not_none(self):
        """_1: self._nodes[cls.node_type] = None → 内置节点值应为类，不为 None。"""
        reg = NodeRegistry()
        for node_type in ("start", "end", "loop", "parallel", "http", "event"):
            cls = reg.get(node_type)
            self.assertIsNotNone(cls, f"{node_type} 节点类应不为 None")
            self.assertTrue(callable(cls), f"{node_type} 应为可调用类")

    def test_start_maps_to_start_class(self):
        reg = NodeRegistry()
        self.assertIs(reg.get("start"), Start)

    def test_end_maps_to_end_class(self):
        reg = NodeRegistry()
        self.assertIs(reg.get("end"), End)

    def test_loop_maps_to_loop_class(self):
        reg = NodeRegistry()
        self.assertIs(reg.get("loop"), Loop)

    def test_parallel_maps_to_parallel_class(self):
        reg = NodeRegistry()
        self.assertIs(reg.get("parallel"), Parallel)


class TestNodeRegistryParseNode(unittest.TestCase):

    def test_parse_node_returns_existing_node(self):
        """传入 Node 实例时直接返回。"""
        reg = NodeRegistry()
        node = Start.model_validate({"id": "s1"})
        self.assertIs(reg.parse_node(node), node)

    def test_parse_node_from_dict(self):
        """_4: node_dict.get("type", ) 语法错误 → 应正确读取 type 字段。"""
        reg = NodeRegistry()
        node = reg.parse_node({"type": "start", "id": "s1"})
        self.assertIsInstance(node, Start)
        self.assertEqual(node.id, "s1")

    def test_parse_node_missing_type_raises(self):
        """无 type 字段应抛出 RuntimeError。"""
        reg = NodeRegistry()
        with self.assertRaises(RuntimeError) as ctx:
            reg.parse_node({"id": "s1"})
        self.assertIn("node type", str(ctx.exception))

    def test_parse_node_unknown_type_raises(self):
        """_12,13,14,15,16: 未知 type 应抛 RuntimeError 含 'unRecognized'。"""
        reg = NodeRegistry()
        with self.assertRaises(RuntimeError) as ctx:
            reg.parse_node({"type": "nonexistent_type"})
        self.assertIn("unRecognized", str(ctx.exception))

    def test_parse_code_node_hint(self):
        """type='code' 的错误消息应包含 CodeNode 提示。"""
        reg = NodeRegistry()
        with self.assertRaises(RuntimeError) as ctx:
            reg.parse_node({"type": "code"})
        self.assertIn("CodeNode", str(ctx.exception))

    def test_parse_node_end(self):
        reg = NodeRegistry()
        node = reg.parse_node({"type": "end", "id": "e1"})
        self.assertIsInstance(node, End)

    def test_parse_assignment_node(self):
        reg = NodeRegistry()
        node = reg.parse_node({"type": "assignment", "id": "a1", "key": "x", "value": "1"})
        self.assertIsInstance(node, Assignment)


class TestNodeRegistryDiscoverEntryPoints(unittest.TestCase):

    def test_discover_idempotent(self):
        """discover() 多次调用只执行一次 entry points 扫描。"""
        reg = NodeRegistry()
        with patch.object(reg, "_discover_entry_points") as mock_disc:
            reg.discover()
            reg.discover()
            reg.discover()
            mock_disc.assert_called_once()

    def test_discover_sets_discovered_flag(self):
        """discover() 后 _discovered 应为 True。"""
        reg = NodeRegistry()
        with patch.object(reg, "_discover_entry_points"):
            reg.discover()
        self.assertTrue(reg._discovered)

    @patch("plaita.node.entry_points")
    def test_discover_loads_plugin_nodes(self, mock_ep):
        """_1: sys.version_info >= (3, 10) 的语义应保持。"""
        # 创建 mock entry point
        fake_node_cls = MagicMock()
        fake_node_cls.node_type = "fake_plugin_node"
        ep = MagicMock()
        ep.load.return_value = fake_node_cls
        mock_ep.return_value = [ep]

        reg = NodeRegistry()
        reg._discover_entry_points()

        if sys.version_info >= (3, 10):
            mock_ep.assert_called_once_with(group="plaita.nodes")
        self.assertIn("fake_plugin_node", reg)

    @patch("plaita.node.entry_points")
    def test_discover_handles_failed_plugin(self, mock_ep):
        """插件 load 失败时应跳过并继续（不抛异常）。"""
        ep_bad = MagicMock()
        ep_bad.load.side_effect = ImportError("broken plugin")
        ep_bad.name = "bad_plugin"
        mock_ep.return_value = [ep_bad]

        reg = NodeRegistry()
        reg._discover_entry_points()  # 不应抛异常

    def test_version_check_gte_310(self):
        """_2,9: sys.version_info >= (3, 10) 用 >= 而非 >。
        在 Python 3.10+ 上运行时，entry_points(group=...) 应被直接调用。"""
        if sys.version_info < (3, 10):
            self.skipTest("仅在 Python 3.10+ 运行此测试")
        with patch("plaita.node.entry_points") as mock_ep:
            mock_ep.return_value = []
            reg = NodeRegistry()
            reg._discover_entry_points()
            # 应使用关键字参数调用（>= 3.10 分支）
            mock_ep.assert_called_with(group="plaita.nodes")


class TestNodeRegistryRegisterUnregister(unittest.TestCase):

    def test_register_returns_class(self):
        reg = NodeRegistry()
        result = reg.register(Start)
        self.assertIs(result, Start)

    def test_unregister_removes_node(self):
        reg = NodeRegistry()
        reg.register(Start)
        reg.unregister("start")
        self.assertNotIn("start", reg)

    def test_unregister_nonexistent_no_error(self):
        reg = NodeRegistry()
        reg.unregister("totally_fake")  # 不应抛异常

    def test_get_returns_none_for_unknown(self):
        reg = NodeRegistry()
        self.assertIsNone(reg.get("doesnotexist"))

    def test_len_returns_builtin_count(self):
        reg = NodeRegistry()
        self.assertGreater(len(reg), 0)

    def test_copy_independent(self):
        reg = NodeRegistry()
        copy = reg.copy()
        copy.unregister("start")
        self.assertIn("start", reg)
        self.assertNotIn("start", copy)


if __name__ == "__main__":
    unittest.main()
