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
        from pydantic import BaseModel, Field
        from typing import Optional, Dict, List

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

    def test_get_or_create_event_bus_no_bus(self):
        ctx = ExecutionContext()
        bus = ctx.get_or_create_event_bus()
        # Without a configured bus, should return None or a default
        # Just verify it doesn't crash


class TestExecutionContextEnvFiltering(unittest.TestCase):
    """T038: Sensitive prefix filtering for environment variables."""

    def test_sensitive_env_vars_filtered(self):
        test_env = {
            "PATH": "/usr/bin",
            "HOME": "/home/user",
            "SECRET_KEY": "s3cret",
            "AWS_SECRET_ACCESS_KEY": "aws_secret",
            "DATABASE_URL": "postgres://...",
            "TOKEN_API": "tok123",
            "PASSWORD_DB": "pass123",
            "NORMAL_VAR": "normal",
        }
        with patch.dict(os.environ, test_env, clear=True):
            ctx = ExecutionContext()
            ctx.clean()
            env = ctx.get_state("$ENV")
            self.assertIn("PATH", env)
            self.assertIn("HOME", env)
            self.assertIn("NORMAL_VAR", env)
            self.assertNotIn("SECRET_KEY", env)
            self.assertNotIn("AWS_SECRET_ACCESS_KEY", env)
            self.assertNotIn("DATABASE_URL", env)
            self.assertNotIn("TOKEN_API", env)
            self.assertNotIn("PASSWORD_DB", env)

    def test_all_sensitive_prefixes_blocked(self):
        for prefix in _SENSITIVE_ENV_PREFIXES:
            key = f"{prefix}_TEST_VALUE"
            with patch.dict(os.environ, {key: "secret"}, clear=True):
                ctx = ExecutionContext()
                ctx.clean()
                env = ctx.get_state("$ENV")
                self.assertNotIn(key, env, f"Prefix {prefix} should be filtered")

    def test_custom_express_prefix(self):
        ctx = ExecutionContext(express_prefix="#")
        ctx.clean()
        env = ctx.get_state("#ENV")
        self.assertIsInstance(env, dict)


if __name__ == "__main__":
    unittest.main()
