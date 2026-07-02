"""Tests for plaita.core.context — ExecutionContext."""

import os
import unittest
from unittest.mock import patch

from plaita.core.context import ExecutionContext, _SENSITIVE_ENV_PREFIXES


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
        self.assertEqual(_coerce_input_value(None, ({"a": 1},), {}), {"a": 1})

    def test_coerce_input_value_kwargs(self):
        from plaita.core.context import _coerce_input_value
        self.assertEqual(_coerce_input_value(None, (), {"a": 1}), {"a": 1})

    def test_coerce_input_value_merge_dict_and_kwargs(self):
        from plaita.core.context import _coerce_input_value
        self.assertEqual(
            _coerce_input_value(None, ({"a": 1},), {"b": 2}),
            {"a": 1, "b": 2},
        )

    def test_coerce_input_value_no_input_type_empty(self):
        from plaita.core.context import _coerce_input_value
        self.assertEqual(_coerce_input_value(None, (), {}), {})

    def test_get_or_create_event_bus_no_bus(self):
        ctx = ExecutionContext()
        bus = ctx.get_or_create_event_bus()
        # Without a configured bus, should return None or a default
        # Just verify it doesn't crash


class TestExecutionContextEnvFiltering(unittest.TestCase):
    """2026-07 安全模型重构：``$ENV`` 默认空（不再泄漏 os.environ），
    flow 通过 ``expose_env`` allowlist 显式声明需要的环境变量。"""

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

    def test_sensitive_prefix_still_blocked_even_when_allowlisted(self):
        """allowlist 命中敏感前缀时仍拒绝——深度防御层。"""
        with patch.dict(os.environ, {"AWS_SECRET_ACCESS_KEY": "k"}, clear=True):
            ctx = ExecutionContext(expose_env=["AWS_SECRET_ACCESS_KEY"])
            ctx.clean()
            env = ctx.get_state("$ENV")
            self.assertEqual(env, {})  # 被深度防御层拦下

    def test_sensitive_prefix_blacklist_covers_all_documented_prefixes(self):
        """黑名单本身保留——用作 allowlist 之上的二次过滤。"""
        for prefix in _SENSITIVE_ENV_PREFIXES:
            key = f"{prefix}_VAR"
            with patch.dict(os.environ, {key: "secret"}, clear=True):
                ctx = ExecutionContext(expose_env=[key])
                ctx.clean()
                env = ctx.get_state("$ENV")
                self.assertNotIn(key, env, f"Prefix {prefix} should be blocked by defense layer")

    def test_custom_express_prefix(self):
        ctx = ExecutionContext(express_prefix="#", expose_env=["HOME"])
        with patch.dict(os.environ, {"HOME": "/h"}, clear=True):
            ctx.clean()
            env = ctx.get_state("#ENV")
            self.assertEqual(env, {"HOME": "/h"})


if __name__ == "__main__":
    unittest.main()
