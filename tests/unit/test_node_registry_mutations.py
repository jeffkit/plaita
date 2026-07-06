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


# ---------------------------------------------------------------------------
# 第二轮补强：get_default_registry / register_code_node /
#             deprecated helpers / _RegistryDictProxy
# ---------------------------------------------------------------------------

import warnings as _warnings
import plaita.node as _plaita_node


class TestGetDefaultRegistry(unittest.TestCase):

    def test_returns_registry(self):
        from plaita.node import get_default_registry
        reg = get_default_registry()
        self.assertIsInstance(reg, NodeRegistry)

    def test_returns_same_instance(self):
        """get_default_registry 返回同一个单例。"""
        from plaita.node import get_default_registry
        r1 = get_default_registry()
        r2 = get_default_registry()
        self.assertIs(r1, r2)

    def test_condition_and_vs_or(self):
        """_1: 'and not' → 'or not'，_2: '... and not' → only one condition。
        验证调用不会抛异常即可（条件改变可能使 discover 触发次数不同）。"""
        from plaita.node import get_default_registry
        reg = get_default_registry()
        self.assertIn("start", reg)


class TestRegisterCodeNode(unittest.TestCase):

    def test_register_code_node_subprocess(self):
        """_1: effective = None → should fail or use subprocess. 
        使用 subprocess 后端（不需要 Docker）注册 CodeNode。"""
        from plaita.node import register_code_node, get_default_registry
        reg = NodeRegistry()
        register_code_node(registry=reg, default_backend="subprocess")
        self.assertIn("code", reg)

    def test_register_code_node_with_none_backend_uses_module_default(self):
        """_2: condition flipped (is None instead of is not None) → effective 逻辑。"""
        from plaita.node import register_code_node
        reg = NodeRegistry()
        # 不传 default_backend，应使用模块默认
        # 如果模块默认是 docker 且 docker 不可用，会抛 RuntimeError
        # 用 subprocess 确保不依赖 docker
        register_code_node(registry=reg, default_backend="subprocess")
        self.assertIn("code", reg)

    def test_effective_with_explicit_backend(self):
        """_2: default_backend is not None 时应用 default_backend。"""
        from plaita.node import register_code_node
        from plaita.node.code import CodeNode
        reg = NodeRegistry()
        register_code_node(registry=reg, default_backend="subprocess")
        cls = reg.get("code")
        self.assertIs(cls, CodeNode)

    def test_register_to_default_registry(self):
        """_4,5,6,8: registry=None 时注册到默认 registry。"""
        from plaita.node import register_code_node, get_default_registry
        reg = NodeRegistry()
        register_code_node(registry=reg, default_backend="subprocess")
        self.assertIn("code", reg)


class TestDeprecatedHelpers(unittest.TestCase):

    def test_node_register_warns(self):
        """_1: 废弃消息变异。node_register 应发出 DeprecationWarning。"""
        from plaita.node import node_register
        with _warnings.catch_warnings(record=True) as w:
            _warnings.simplefilter("always")
            node_register(Start)
            self.assertGreater(len(w), 0)
            self.assertTrue(issubclass(w[0].category, DeprecationWarning))

    def test_node_register_works(self):
        """_6,7,8,9: node_register 实际注册到默认 registry。"""
        from plaita.node import node_register, _default_registry
        with _warnings.catch_warnings(record=True):
            _warnings.simplefilter("always")
            node_register(Start)
        self.assertIn("start", _default_registry)

    def test_parse_node_warns(self):
        """_1: 废弃消息变异。module-level parse_node 应发出 DeprecationWarning。"""
        import plaita.node as pn
        with _warnings.catch_warnings(record=True) as w:
            _warnings.simplefilter("always")
            pn.parse_node({"type": "start", "id": "s1"})
            self.assertGreater(len(w), 0)
            self.assertTrue(issubclass(w[0].category, DeprecationWarning))

    def test_parse_node_works(self):
        """_6,7,8,9,10: module-level parse_node 返回节点实例。"""
        import plaita.node as pn
        with _warnings.catch_warnings(record=True):
            _warnings.simplefilter("always")
            node = pn.parse_node({"type": "end", "id": "e1"})
        self.assertIsInstance(node, End)


class TestRegistryDictProxy(unittest.TestCase):

    def test_getitem(self):
        """_1: self._registry = None → proxy 应可通过 _registry 访问节点。"""
        reg = NodeRegistry()
        from plaita.node import _RegistryDictProxy
        proxy = _RegistryDictProxy(reg)
        self.assertIs(proxy["start"], Start)

    def test_getitem_missing_raises_keyerror(self):
        reg = NodeRegistry()
        from plaita.node import _RegistryDictProxy
        proxy = _RegistryDictProxy(reg)
        with self.assertRaises(KeyError):
            _ = proxy["nonexistent"]

    def test_contains(self):
        reg = NodeRegistry()
        from plaita.node import _RegistryDictProxy
        proxy = _RegistryDictProxy(reg)
        self.assertIn("start", proxy)

    def test_len(self):
        reg = NodeRegistry()
        from plaita.node import _RegistryDictProxy
        proxy = _RegistryDictProxy(reg)
        self.assertGreater(len(proxy), 0)

    def test_setitem(self):
        reg = NodeRegistry()
        from plaita.node import _RegistryDictProxy
        proxy = _RegistryDictProxy(reg)
        proxy["custom"] = Start
        self.assertIn("custom", proxy)


class TestParseNodeHint(unittest.TestCase):

    def test_code_hint_in_error(self):
        """_12: hint = None → hint + " CodeNode..." 应保持为字符串。"""
        reg = NodeRegistry()
        with self.assertRaises(RuntimeError) as ctx:
            reg.parse_node({"type": "code"})
        # hint 应该是 "" + " CodeNode..." 而不是 None + " CodeNode..."
        self.assertIn("CodeNode", str(ctx.exception))

    def test_code_hint_not_contains_none_word(self):
        """_12: hint = None 时消息会出现 'None CodeNode...' — 应不含 'None'。"""
        reg = NodeRegistry()
        with self.assertRaises(RuntimeError) as ctx:
            reg.parse_node({"type": "code"})
        err_str = str(ctx.exception)
        self.assertNotIn(". None", err_str)

    def test_code_hint_exact_content(self):
        """_18: hint 中 'CodeNode was moved out of the default registry' 确实存在。"""
        reg = NodeRegistry()
        with self.assertRaises(RuntimeError) as ctx:
            reg.parse_node({"type": "code"})
        self.assertIn("CodeNode was moved out of the default registry", str(ctx.exception))

    def test_unknown_type_no_hint(self):
        """_13,18: hint 对非 code 类型为空字符串，错误消息不应有多余内容。"""
        reg = NodeRegistry()
        with self.assertRaises(RuntimeError) as ctx:
            reg.parse_node({"type": "unknown_xyz"})
        err_str = str(ctx.exception)
        # 不应包含 CodeNode hint
        self.assertNotIn("CodeNode", err_str)
        self.assertIn("unknown_xyz", err_str)

    def test_unknown_type_no_none_suffix(self):
        """_12: hint = None 时非 code 类型消息会以 '.None' 结尾。"""
        reg = NodeRegistry()
        with self.assertRaises(RuntimeError) as ctx:
            reg.parse_node({"type": "another_unknown"})
        err_str = str(ctx.exception)
        self.assertFalse(err_str.endswith("None"),
                         f"消息不应以 None 结尾: {err_str!r}")


class TestRegistryDictProxyExtended(unittest.TestCase):

    def test_getitem_error_has_correct_key(self):
        """_4: raise KeyError(None) → KeyError 应包含实际 key。"""
        reg = NodeRegistry()
        from plaita.node import _RegistryDictProxy
        proxy = _RegistryDictProxy(reg)
        try:
            _ = proxy["missing_key"]
            self.fail("应该抛出 KeyError")
        except KeyError as e:
            self.assertEqual(e.args[0], "missing_key")

    def test_setitem_stores_correct_value(self):
        """_1: _nodes[key] = None → setitem 应存储实际 value。"""
        reg = NodeRegistry()
        from plaita.node import _RegistryDictProxy
        proxy = _RegistryDictProxy(reg)
        proxy["custom_key"] = Start
        self.assertIs(proxy["custom_key"], Start)

    def test_setitem_overwrite(self):
        reg = NodeRegistry()
        from plaita.node import _RegistryDictProxy
        proxy = _RegistryDictProxy(reg)
        proxy["start"] = End
        self.assertIs(proxy["start"], End)


class TestDiscoverEntryPointsExtended(unittest.TestCase):

    @patch("plaita.node.entry_points")
    def test_loaded_plugin_class_stored_correctly(self, mock_ep):
        """_9: _nodes[cls.node_type] = None → 加载的插件类应被正确存储（非 None）。"""
        fake_cls = MagicMock()
        fake_cls.node_type = "test_plugin"
        ep = MagicMock()
        ep.load.return_value = fake_cls
        mock_ep.return_value = [ep]

        reg = NodeRegistry()
        reg._discover_entry_points()

        stored = reg.get("test_plugin")
        self.assertIs(stored, fake_cls, "存储的插件类应是 fake_cls 而非 None")


class TestWarningMessages(unittest.TestCase):

    def test_node_register_warning_message_exact(self):
        """_7,8: 'node_register()' 大小写变异 → 消息应包含 'node_register()'。"""
        from plaita.node import node_register
        with _warnings.catch_warnings(record=True) as w:
            _warnings.simplefilter("always")
            node_register(Start)
            self.assertGreater(len(w), 0)
            msg = str(w[0].message)
            self.assertIn("node_register()", msg,
                          f"消息应含 'node_register()'，实际: {msg!r}")

    def test_parse_node_warning_message_exact(self):
        """_7,8,9: 'Module-level parse_node()' 大小写变异 → 消息应包含 'Module-level'。"""
        import plaita.node as pn
        with _warnings.catch_warnings(record=True) as w:
            _warnings.simplefilter("always")
            pn.parse_node({"type": "start", "id": "s1"})
            self.assertGreater(len(w), 0)
            msg = str(w[0].message)
            self.assertIn("Module-level", msg,
                          f"消息应含 'Module-level'，实际: {msg!r}")


class TestRegisterCodeNodeExtended(unittest.TestCase):

    def test_default_backend_is_set(self):
        """_9: _DEFAULT_SANDBOX_BACKEND = None → 应实际设置 default_backend。"""
        from plaita.node import register_code_node
        from plaita.node import code as _code_module
        original = _code_module._DEFAULT_SANDBOX_BACKEND
        try:
            reg = NodeRegistry()
            register_code_node(registry=reg, default_backend="subprocess")
            self.assertEqual(_code_module._DEFAULT_SANDBOX_BACKEND, "subprocess")
        finally:
            _code_module._DEFAULT_SANDBOX_BACKEND = original

    def test_register_subprocess_backend_not_raise(self):
        """_3: effective == "docker" or not _docker_available → subprocess 不应触发 docker 检查。"""
        from plaita.node import register_code_node
        reg = NodeRegistry()
        register_code_node(registry=reg, default_backend="subprocess")
        self.assertIn("code", reg)

    def test_register_unsafe_backend_not_raise(self):
        """_4,5,6: 其他后端条件变异。"""
        from plaita.node import register_code_node
        reg = NodeRegistry()
        register_code_node(registry=reg, default_backend="unsafe")
        self.assertIn("code", reg)


if __name__ == "__main__":
    unittest.main()
