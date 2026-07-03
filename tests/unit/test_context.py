"""Tests for plaita.core.context — ExecutionContext."""

import os
import unittest
from unittest.mock import patch

from plaita.core.context import ExecutionContext
from plaita.core.expression import ExpressionEvaluator
from plaita.core.expression_parser import _get_attr
from plaita.core.state import CheckpointState


class TestExecutionContextState(unittest.TestCase):
    """T037: set_state, get_state, clean, setup_flow, evaluate, get_global_variable, child, to_dict/from_dict."""

    def test_set_and_get_state(self):
        ctx = ExecutionContext()
        ctx.set_state("key1", "value1")
        self.assertEqual(ctx.get_state("key1"), "value1")

    def test_get_state_default(self):
        ctx = ExecutionContext()
        self.assertIsNone(ctx.get_state("missing"))
        self.assertEqual(ctx.get_state("missing", "fallback"), "fallback")

    def test_clean_resets_context(self):
        ctx = ExecutionContext()
        ctx.set_state("existing", 123)
        ctx.clean()
        self.assertIsNone(ctx.get_state("existing"))
        env = ctx.get_state("$ENV")
        self.assertIsInstance(env, dict)

    def test_context_dict_property(self):
        ctx = ExecutionContext()
        ctx.set_state("a", 1)
        self.assertIn("a", ctx.context)
        self.assertEqual(ctx.context["a"], 1)

    def test_context_dict_setter(self):
        ctx = ExecutionContext()
        ctx.context = {"b": 2}
        self.assertEqual(ctx.get_state("b"), 2)

    def test_evaluate_string(self):
        ctx = ExecutionContext()
        ctx.set_state("$INPUT", {"name": "Alice"})
        result = ctx.evaluate("$INPUT.name")
        self.assertEqual(result, "Alice")

    def test_evaluate_list(self):
        ctx = ExecutionContext()
        ctx.set_state("$INPUT", {"x": 1, "y": 2})
        result = ctx.evaluate(["$INPUT.x", "$INPUT.y"])
        self.assertEqual(result, [1, 2])

    def test_evaluate_dict(self):
        ctx = ExecutionContext()
        ctx.set_state("$INPUT", {"x": 1})
        result = ctx.evaluate({"val": "$INPUT.x"})
        self.assertEqual(result, {"val": 1})

    def test_evaluate_non_expression(self):
        ctx = ExecutionContext()
        self.assertEqual(ctx.evaluate(42), 42)
        self.assertEqual(ctx.evaluate("plain"), "plain")

    def test_evaluate_with_parent_fallback(self):
        parent = ExecutionContext()
        parent.set_state("$INPUT", {"name": "parent_value"})
        child = ExecutionContext(parent=parent)
        child.set_state("$INPUT", {})
        result = child.evaluate("$INPUT.name")
        self.assertEqual(result, "parent_value")

    def test_get_global_variable(self):
        ctx = ExecutionContext()
        ctx.set_state("$GLOBAL", {"flow_id": "test", "foo": "bar"})
        self.assertEqual(ctx.get_global_variable("foo"), "bar")
        self.assertEqual(ctx.get_global_variable("missing", "default"), "default")

    def test_get_global_variable_parent_chain(self):
        parent = ExecutionContext()
        parent.set_state("$GLOBAL", {"root_key": "root_val"})
        child = ExecutionContext(parent=parent)
        child.set_state("$GLOBAL", {})
        self.assertEqual(child.get_global_variable("root_key", "default"), "root_val")

    def test_update_node_result(self):
        ctx = ExecutionContext()
        ctx.set_state("$NODE", {})

        class FakeNode:
            id = "n1"

        ctx.update_node_result(FakeNode(), "result_val")
        node_results = ctx.get_state("$NODE")
        self.assertEqual(node_results["n1"], "result_val")

    def test_execution_id_generated_once(self):
        ctx = ExecutionContext()
        eid1 = ctx.execution_id
        eid2 = ctx.execution_id
        self.assertEqual(eid1, eid2)
        self.assertTrue(len(eid1) > 0)

    def test_child_context(self):
        parent = ExecutionContext()
        parent.set_state("$INPUT", {"data": "parent"})
        child = parent.child()
        self.assertIs(child.parent, parent)
        self.assertEqual(child.express_prefix, parent.express_prefix)

    def test_to_dict_from_dict(self):
        ctx = ExecutionContext()
        ctx.set_state("key", "val")
        ctx.set_state("$INPUT", {"a": 1})
        d = ctx.to_dict()
        restored = ExecutionContext.from_dict(d)
        self.assertEqual(restored.get_state("key"), "val")
        self.assertEqual(restored.get_state("$INPUT"), {"a": 1})

    def test_setup_flow(self):
        from plaita.io import Property
        from plaita.core import types
        from plaita.core.context import _coerce_input_value

        class FakeFlow:
            flow_id = "f1"
            id = "f1"
            input_type = Property(data_type=types.OBJECT)
            global_context = {"env": "test"}

        ctx = ExecutionContext()
        ctx.clean()
        ctx.setup_flow(FakeFlow(), (), {"x": 1})
        self.assertEqual(ctx.get_state("$INPUT"), {"x": 1})
        global_ctx = ctx.get_state("$GLOBAL")
        self.assertEqual(global_ctx["env"], "test")
        self.assertEqual(global_ctx["flow_id"], "f1")

    def test_coerce_input_value_dict_positional(self):
        from plaita.core.context import _coerce_input_value
        self.assertEqual(_coerce_input_value(({"a": 1},), {}), {"a": 1})

    def test_coerce_input_value_kwargs(self):
        from plaita.core.context import _coerce_input_value
        self.assertEqual(_coerce_input_value((), {"a": 1}), {"a": 1})

    def test_coerce_input_value_merge_dict_and_kwargs(self):
        from plaita.core.context import _coerce_input_value
        self.assertEqual(
            _coerce_input_value(({"a": 1},), {"b": 2}),
            {"a": 1, "b": 2},
        )

    def test_coerce_input_value_no_input_type_empty(self):
        from plaita.core.context import _coerce_input_value
        self.assertEqual(_coerce_input_value((), {}), {})

    def test_get_or_create_event_bus_no_bus(self):
        ctx = ExecutionContext()
        bus = ctx.get_or_create_event_bus()
        # Without a configured bus, should return None or a default
        # Just verify it doesn't crash


class TestExecutionContextEnvFiltering(unittest.TestCase):
    """2026-07 安全模型重构：``$ENV`` 默认空（不再泄漏 os.environ），
    flow 通过 ``expose_env`` allowlist 显式声明需要的环境变量。本次移除了
    不完备的「敏感前缀黑名单」——allowlist 即用户责任，命中即打 warning
    做审计可见性，不做任何「看起来敏感」的拦截。"""

    def test_default_env_is_empty(self):
        """没有 allowlist 时 ``$ENV`` 必须为空——这是反转后的核心保证。"""
        with patch.dict(os.environ, {"HOME": "/h", "PATH": "/p", "SECRET_KEY": "s"}, clear=True):
            ctx = ExecutionContext()
            ctx.clean()
            env = ctx.get_state("$ENV")
            self.assertEqual(env, {})

    def test_expose_env_allowlist_returns_only_listed_keys(self):
        with patch.dict(os.environ, {"HOME": "/h", "PATH": "/p", "OTHER": "x"}, clear=True):
            ctx = ExecutionContext(expose_env=["HOME", "PATH", "MISSING"])
            ctx.clean()
            env = ctx.get_state("$ENV")
            self.assertEqual(env, {"HOME": "/h", "PATH": "/p"})
            self.assertNotIn("OTHER", env)
            self.assertNotIn("MISSING", env)  # 不存在的 key 静默跳过

    def test_expose_env_warns_on_every_hit(self):
        """allowlist 命中即 warning（审计可见性），不区分 key 是否「看起来敏感」。"""
        with patch.dict(os.environ, {"HOME": "/h", "OPENAI_API_KEY": "sk-x"}, clear=True):
            ctx = ExecutionContext(expose_env=["HOME", "OPENAI_API_KEY"])
            with self.assertLogs("plaita.core.context", level="WARNING") as cm:
                ctx.clean()
            env = ctx.get_state("$ENV")
            # 两个 key 都被暴露——allowlist 即用户责任，不做启发式拦截
            self.assertEqual(env, {"HOME": "/h", "OPENAI_API_KEY": "sk-x"})
            joined = "\n".join(cm.output)
            self.assertIn("HOME", joined)
            self.assertIn("OPENAI_API_KEY", joined)

    def test_sensitive_vendor_prefixed_key_is_not_blocked(self):
        """回归保证：vendor 前缀密钥（旧黑名单 startswith 漏掉的那类）在
        allowlist 命中时会被暴露——不再有虚假的「第二层防御」。用户需自行
        把关 expose_env 内容。"""
        with patch.dict(os.environ, {"OPENAI_API_KEY": "sk-x", "STRIPE_KEY": "sk-y"}, clear=True):
            ctx = ExecutionContext(expose_env=["OPENAI_API_KEY", "STRIPE_KEY"])
            with self.assertLogs("plaita.core.context", level="WARNING"):
                ctx.clean()
            env = ctx.get_state("$ENV")
            self.assertEqual(env, {"OPENAI_API_KEY": "sk-x", "STRIPE_KEY": "sk-y"})

    def test_custom_express_prefix(self):
        ctx = ExecutionContext(express_prefix="#", expose_env=["HOME"])
        with patch.dict(os.environ, {"HOME": "/h"}, clear=True):
            ctx.clean()
            env = ctx.get_state("#ENV")
            self.assertEqual(env, {"HOME": "/h"})


class TestExecutionContextCancelPropagation(unittest.TestCase):
    """cancel_event 进程内传播：父 set → 子能观测到（thread 模式）。
    跨进程不传播（pickle 剔除），见 parallel_executor 的协议声明。"""

    def test_child_shares_parent_cancel_event(self):
        parent = ExecutionContext()
        child = ExecutionContext(parent=parent)
        self.assertIs(child.cancel_event, parent.cancel_event)
        parent.cancel_event.set()
        self.assertTrue(child.cancel_event.is_set())

    def test_child_method_shares_cancel_event(self):
        parent = ExecutionContext()
        child = parent.child()
        self.assertIs(child.cancel_event, parent.cancel_event)
        parent.cancel_event.set()
        self.assertTrue(child.cancel_event.is_set())

    def test_root_clean_creates_fresh_event(self):
        ctx = ExecutionContext()
        old = ctx.cancel_event
        ctx.clean()
        self.assertIsNot(ctx.cancel_event, old)
        self.assertFalse(ctx.cancel_event.is_set())

    def test_child_clean_resyncs_to_parent_event(self):
        """子 clean() 后仍指向父当前 Event，不脱离取消链。"""
        parent = ExecutionContext()
        child = ExecutionContext(parent=parent)
        parent.clean()  # 父换新 Event
        child.clean()   # 子重新同步到父的新 Event
        self.assertIs(child.cancel_event, parent.cancel_event)
        parent.cancel_event.set()
        self.assertTrue(child.cancel_event.is_set())


class TestGetAttrRootFix(unittest.TestCase):
    """_get_attr 根因修复：mapping 对象（含 live CheckpointState）走 ``.get``，
    而非 ``getattr``——``$INPUT`` 是 storage key 不是 Python 属性。"""

    def test_dict_uses_get(self):
        self.assertEqual(_get_attr({"a": 1}, "a"), 1)
        self.assertIsNone(_get_attr({"a": 1}, "b"))

    def test_object_with_dict_falls_back_to_getattr(self):
        class Obj:
            def __init__(self):
                self.field = "v"
        self.assertEqual(_get_attr(Obj(), "field"), "v")
        self.assertIsNone(_get_attr(Obj(), "missing"))

    def test_live_checkpoint_state_resolves_storage_key(self):
        cs = CheckpointState()
        cs["$INPUT"] = {"name": "alice"}
        # 旧行为：getattr(cs, "$INPUT") → None。修复后：cs.get("$INPUT") → 值。
        self.assertEqual(_get_attr(cs, "$INPUT"), {"name": "alice"})

    def test_parent_input_walked_through_live_checkpoint_state(self):
        """$PARENT.$INPUT.name 在 $PARENT 指向 live CheckpointState 时也能解析
        （不再依赖 setup_flow 把 $PARENT 拍成 plain dict）。"""
        parent = CheckpointState()
        parent["$INPUT"] = {"name": "alice"}
        child = CheckpointState()
        child["$PARENT"] = parent  # 故意塞 live 对象，模拟「不靠快照」
        ev = ExpressionEvaluator()
        self.assertEqual(ev.evaluate("$PARENT.$INPUT.name", child), "alice")


if __name__ == "__main__":
    unittest.main()
