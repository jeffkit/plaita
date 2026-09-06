"""Tests for plaita.core.executor — ExecutionStrategy and ExecutionMode."""

import asyncio
import json
import unittest

from plaita.core.executor import (
    ExecutionMode,
    FlowExecution,
    NormalStrategy,
    GeneratorStrategy,
    DistributedStrategy,
)
from plaita.core.flow import Flow


def make_simple_flow():
    return Flow.model_validate_json(json.dumps({
        "id": "test",
        "nodes": [
            {"type": "start", "id": "start", "next": "end"},
            {"type": "end", "id": "end", "resultType": "success", "output": "result_val"},
        ],
    }))


def make_multi_node_flow():
    return Flow.model_validate_json(json.dumps({
        "id": "multi",
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "assign"},
            {
                "type": "assignment",
                "id": "assign",
                "next": "end",
                "output": "$INPUT.x",
            },
            {"type": "end", "id": "end", "resultType": "success", "output": "$NODE.assign"},
        ],
    }))


class TestExecutionMode(unittest.TestCase):
    """T042: ExecutionMode enum replaces string-based dispatch."""

    def test_enum_values(self):
        self.assertEqual(ExecutionMode.NORMAL.value, "normal")
        self.assertEqual(ExecutionMode.GENERATOR.value, "generator")
        self.assertEqual(ExecutionMode.DISTRIBUTED.value, "distributed")

    def test_from_string(self):
        self.assertEqual(ExecutionMode.from_string("normal"), ExecutionMode.NORMAL)
        self.assertEqual(ExecutionMode.from_string("GENERATOR"), ExecutionMode.GENERATOR)
        self.assertEqual(ExecutionMode.from_string("distributed"), ExecutionMode.DISTRIBUTED)

    def test_from_string_invalid(self):
        # 0.5.0 回归修复: 非法 mode 现在抛带合法值清单的 ValueError（原来是裸 KeyError）
        with self.assertRaises(ValueError) as cm:
            ExecutionMode.from_string("invalid")
        self.assertIn("normal", str(cm.exception))
        self.assertIn("generator", str(cm.exception))
        self.assertIn("distributed", str(cm.exception))


class TestNormalStrategy(unittest.IsolatedAsyncioTestCase):
    """T041: NormalStrategy traversal."""

    async def test_normal_flow_execution(self):
        flow = make_simple_flow()
        result = FlowExecution.run(flow, mode="normal")
        self.assertEqual(result, "result_val")

    async def test_normal_with_params(self):
        flow = make_multi_node_flow()
        result = FlowExecution.run(flow, params={"x": 42}, mode="normal")
        self.assertEqual(result, 42)

    async def test_normal_via_flow_run(self):
        flow = make_simple_flow()
        result = flow.run()
        self.assertEqual(result, "result_val")


class TestGeneratorStrategy(unittest.IsolatedAsyncioTestCase):
    """T041: GeneratorStrategy yielding per-node output."""

    def test_generator_yields_per_node(self):
        flow = make_simple_flow()
        gen = FlowExecution.run(flow, mode="generator")
        outputs = list(gen)
        self.assertGreater(len(outputs), 0)
        for out in outputs:
            self.assertIn("id", out)
            self.assertIn("result", out)
            self.assertIn("context", out)

    def test_generator_end_node_present(self):
        flow = make_simple_flow()
        gen = FlowExecution.run(flow, mode="generator")
        outputs = list(gen)
        end_outputs = [o for o in outputs if o.get("type") == "end"]
        self.assertGreater(len(end_outputs), 0)

    def test_generator_flow_end_fires_only_after_consumption(self):
        """on_flow_end must NOT fire before the generator is consumed."""
        from unittest.mock import MagicMock
        from plaita.core.callback import FlowCallback
        handler = MagicMock(spec=FlowCallback)
        flow = make_simple_flow()
        gen = FlowExecution.run(flow, mode="generator", callback_handlers=[handler])
        # 创建生成器但未消费: 生命周期回调都不应触发 (lazy 语义, prepare 在首次 next 才跑)
        handler.on_flow_end.assert_not_called()
        # 消费完毕后 on_flow_start / on_flow_end 各触发一次
        list(gen)
        handler.on_flow_start.assert_called_once()
        handler.on_flow_end.assert_called_once()


class TestDistributedStrategy(unittest.TestCase):
    """T041: DistributedStrategy suspend/resume."""

    def test_distributed_first_call(self):
        flow = make_simple_flow()
        result = FlowExecution.run(flow, mode="distributed")
        self.assertIsInstance(result, dict)
        self.assertIn("context", result)
        self.assertIn("result", result)

    def test_distributed_step_by_step(self):
        flow = make_multi_node_flow()
        result1 = FlowExecution.run(flow, params={"x": 99}, mode="distributed")
        self.assertIsInstance(result1, dict)
        if not result1.get("is_end"):
            ctx = result1["context"]
            result2 = FlowExecution.run(flow, mode="distributed", context=ctx)
            self.assertIsInstance(result2, dict)


class TestFlowExecutionFacade(unittest.TestCase):
    """T056: FlowExecution as thin facade."""

    def test_classmethod_run(self):
        flow = make_simple_flow()
        result = FlowExecution.run(flow, mode="normal")
        self.assertEqual(result, "result_val")

    def test_instance_run_compatible(self):
        flow = make_simple_flow()
        result = FlowExecution().run_compatible(flow, False)
        self.assertEqual(result, "result_val")

    def test_instance_execute(self):
        flow = make_simple_flow()
        result = FlowExecution().execute(flow)
        self.assertEqual(result, "result_val")

    def test_execute_with_params(self):
        flow = make_multi_node_flow()
        result = FlowExecution().execute(flow, params={"x": 7})
        self.assertEqual(result, 7)

    def test_execute_rejects_non_dict_params(self):
        flow = make_simple_flow()
        with self.assertRaises(TypeError):
            FlowExecution().execute(flow, params="not_a_dict")

    def test_flow_debug_returns_generator(self):
        flow = make_simple_flow()
        gen = flow.debug()
        outputs = list(gen)
        self.assertGreater(len(outputs), 0)

    def test_context_property_backward_compat(self):
        fe = FlowExecution()
        fe.clean()
        # ``execution.context`` is now a ``CheckpointState`` (typed BaseModel)
        # that exposes a dict-like view over the prefixed storage keys. It is
        # NOT a ``dict`` subclass anymore — see MIGRATION.md / HANDOFF task #1.
        from plaita.core.state import CheckpointState
        self.assertIsInstance(fe.context, CheckpointState)
        # ...but it still behaves like the old dict for every call site:
        self.assertIn("$ENV", fe.context)
        self.assertEqual(fe.context, dict(fe.context))

    def test_evaluate_proxy(self):
        fe = FlowExecution()
        fe.clean()
        fe.set_state("$INPUT", {"val": 100})
        self.assertEqual(fe.evaluate("$INPUT.val"), 100)

    def test_get_child_execution(self):
        fe = FlowExecution()
        child = fe.get_child_execution()
        self.assertIsNotNone(child)
        self.assertIs(child.parent, fe)

    def test_execution_mode_enum_accepted(self):
        flow = make_simple_flow()
        result = FlowExecution.run(flow, mode=ExecutionMode.NORMAL)
        self.assertEqual(result, "result_val")


if __name__ == "__main__":
    unittest.main()
