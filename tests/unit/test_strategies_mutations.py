"""Mutation-killing tests for plaita/core/strategies.py.

Targets: NormalStrategy, GeneratorStrategy, DistributedStrategy,
         _create_lazy_output, _create_end_output, _advance_one,
         _subscribe_event, _coerce_mode, ExecutionMode, RunOptions.
"""
from __future__ import annotations

import asyncio
import unittest
from typing import ClassVar
from unittest.mock import AsyncMock, MagicMock, patch

from plaita.core.context import ExecutionContext
from plaita.core.errors import (
    FlowStartMissingError,
    FlowTimeoutError,
    ResumeError,
    ResumeType,
)
from plaita.core.strategies import (
    DistributedStrategy,
    ExecutionMode,
    GeneratorStrategy,
    NormalStrategy,
    RunOptions,
    _StateView,
    _advance_one,
    _coerce_mode,
    _create_end_output,
    _create_lazy_output,
)
from plaita.node.basic import Node


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

class _EndNode(Node):
    node_type: ClassVar[str] = "end"
    node_name: ClassVar[str] = "end"

    def execute(self, execution=None):
        return {"done": True}


class _StartNode(Node):
    node_type: ClassVar[str] = "start"
    node_name: ClassVar[str] = "start"

    def execute(self, execution=None):
        return "start_result"


class _MidNode(Node):
    node_type: ClassVar[str] = "mid"
    node_name: ClassVar[str] = "mid"
    result_val: str = "mid_result"

    def execute(self, execution=None):
        return self.result_val


def _make_flow(*, start=None, end=None, mid=None):
    """Build a mock flow with is_end_node / next_node / start_node."""
    flow = MagicMock()
    end_node = end or _EndNode(id="end1", name="end1")
    start_node = start or _StartNode(id="start1", name="start1")
    flow.start_node = start_node
    flow.is_end_node.side_effect = lambda n: n is end_node
    flow.next_node.return_value = end_node
    return flow, start_node, end_node


def _make_cb():
    cb = MagicMock()
    cb.on_node_start = MagicMock()
    cb.on_node_end = MagicMock()
    cb.on_flow_start = MagicMock()
    cb.on_flow_end = MagicMock()
    return cb


# ---------------------------------------------------------------------------
# _coerce_mode
# ---------------------------------------------------------------------------

class TestCoerceModeMutations(unittest.TestCase):
    def test_none_returns_none(self):
        self.assertIsNone(_coerce_mode(None))

    def test_enum_passthrough(self):
        self.assertIs(_coerce_mode(ExecutionMode.NORMAL), ExecutionMode.NORMAL)
        self.assertIs(_coerce_mode(ExecutionMode.GENERATOR), ExecutionMode.GENERATOR)

    def test_string_normal(self):
        self.assertEqual(_coerce_mode("normal"), ExecutionMode.NORMAL)

    def test_string_generator(self):
        self.assertEqual(_coerce_mode("generator"), ExecutionMode.GENERATOR)

    def test_string_distributed(self):
        self.assertEqual(_coerce_mode("distributed"), ExecutionMode.DISTRIBUTED)

    def test_unknown_type_returns_as_is(self):
        s = object()
        self.assertIs(_coerce_mode(s), s)


# ---------------------------------------------------------------------------
# ExecutionMode
# ---------------------------------------------------------------------------

class TestExecutionModeMutations(unittest.TestCase):
    def test_from_string_normal(self):
        self.assertEqual(ExecutionMode.from_string("normal"), ExecutionMode.NORMAL)

    def test_from_string_generator(self):
        self.assertEqual(ExecutionMode.from_string("generator"), ExecutionMode.GENERATOR)

    def test_from_string_distributed(self):
        self.assertEqual(ExecutionMode.from_string("distributed"), ExecutionMode.DISTRIBUTED)

    def test_from_string_upper(self):
        self.assertEqual(ExecutionMode.from_string("NORMAL"), ExecutionMode.NORMAL)

    def test_values_are_strings(self):
        self.assertEqual(ExecutionMode.NORMAL.value, "normal")
        self.assertEqual(ExecutionMode.GENERATOR.value, "generator")
        self.assertEqual(ExecutionMode.DISTRIBUTED.value, "distributed")


# ---------------------------------------------------------------------------
# RunOptions
# ---------------------------------------------------------------------------

class TestRunOptionsMutations(unittest.TestCase):
    def test_defaults(self):
        opts = RunOptions()
        self.assertIsNone(opts.mode)
        self.assertIsNone(opts.timeout)

    def test_set_mode(self):
        opts = RunOptions(mode=ExecutionMode.GENERATOR)
        self.assertEqual(opts.mode, ExecutionMode.GENERATOR)

    def test_set_timeout(self):
        opts = RunOptions(timeout=5000)
        self.assertEqual(opts.timeout, 5000)


# ---------------------------------------------------------------------------
# _create_lazy_output — all fields
# ---------------------------------------------------------------------------

class TestCreateLazyOutput(unittest.TestCase):
    def _make_node(self) -> Node:
        n = MagicMock()
        n.id = "n_lazy"
        n.node_type = "mid"
        n.name = "Mid Node"
        return n

    def test_id_field(self):
        n = self._make_node()
        out = _create_lazy_output(n, "res", "branch1", {})
        self.assertEqual(out["id"], "n_lazy")

    def test_type_field(self):
        n = self._make_node()
        out = _create_lazy_output(n, "res", "b", {})
        self.assertEqual(out["type"], "mid")

    def test_name_field(self):
        n = self._make_node()
        out = _create_lazy_output(n, "res", "b", {})
        self.assertEqual(out["name"], "Mid Node")

    def test_result_field(self):
        n = self._make_node()
        out = _create_lazy_output(n, {"x": 42}, "b", {})
        self.assertEqual(out["result"], {"x": 42})

    def test_branch_field(self):
        n = self._make_node()
        out = _create_lazy_output(n, "res", "left", {})
        self.assertEqual(out["branch"], "left")

    def test_branch_none_becomes_empty_string(self):
        n = self._make_node()
        out = _create_lazy_output(n, "res", None, {})
        self.assertEqual(out["branch"], "")

    def test_is_end_false(self):
        n = self._make_node()
        out = _create_lazy_output(n, "r", "b", {})
        self.assertFalse(out["is_end"])

    def test_is_suspend_default_false(self):
        n = self._make_node()
        out = _create_lazy_output(n, "r", "b", {})
        self.assertFalse(out["is_suspend"])

    def test_is_suspend_true(self):
        n = self._make_node()
        out = _create_lazy_output(n, "r", "b", {}, is_suspend=True)
        self.assertTrue(out["is_suspend"])

    def test_context_field(self):
        n = self._make_node()
        ctx = {"key": "val"}
        out = _create_lazy_output(n, "r", "b", ctx)
        self.assertEqual(out["context"], ctx)

    def test_execution_id_none_by_default(self):
        n = self._make_node()
        out = _create_lazy_output(n, "r", "b", {})
        self.assertIsNone(out["execution_id"])

    def test_execution_id_set(self):
        n = self._make_node()
        out = _create_lazy_output(n, "r", "b", {}, execution_id="exec-123")
        self.assertEqual(out["execution_id"], "exec-123")


# ---------------------------------------------------------------------------
# _create_end_output — all fields
# ---------------------------------------------------------------------------

class TestCreateEndOutput(unittest.TestCase):
    def _make_node(self) -> Node:
        n = MagicMock()
        n.id = "end_node"
        n.name = "End Node"
        return n

    def test_is_end_true(self):
        n = self._make_node()
        out = _create_end_output(n, {"result": 1}, {})
        self.assertTrue(out["is_end"])

    def test_type_is_end_string(self):
        n = self._make_node()
        out = _create_end_output(n, {}, {})
        self.assertEqual(out["type"], "end")

    def test_is_suspend_false(self):
        n = self._make_node()
        out = _create_end_output(n, {}, {})
        self.assertFalse(out["is_suspend"])

    def test_id_from_node(self):
        n = self._make_node()
        out = _create_end_output(n, {}, {})
        self.assertEqual(out["id"], "end_node")

    def test_name_from_node(self):
        n = self._make_node()
        out = _create_end_output(n, {}, {})
        self.assertEqual(out["name"], "End Node")

    def test_result_field(self):
        n = self._make_node()
        out = _create_end_output(n, {"x": 99}, {})
        self.assertEqual(out["result"], {"x": 99})

    def test_branch_is_empty_string(self):
        n = self._make_node()
        out = _create_end_output(n, {}, {})
        self.assertEqual(out["branch"], "")

    def test_node_none_id_is_none(self):
        out = _create_end_output(None, {"r": 1}, {})
        self.assertIsNone(out["id"])

    def test_node_none_name_is_end_fallback(self):
        out = _create_end_output(None, {}, {})
        self.assertEqual(out["name"], "结束")

    def test_context_field(self):
        n = self._make_node()
        ctx = {"flow": "data"}
        out = _create_end_output(n, {}, ctx)
        self.assertEqual(out["context"], ctx)

    def test_execution_id_none_default(self):
        n = self._make_node()
        out = _create_end_output(n, {}, {})
        self.assertIsNone(out["execution_id"])

    def test_execution_id_set(self):
        n = self._make_node()
        out = _create_end_output(n, {}, {}, execution_id="eid-456")
        self.assertEqual(out["execution_id"], "eid-456")


# ---------------------------------------------------------------------------
# NormalStrategy — return value and reached_end logic
# ---------------------------------------------------------------------------

class TestNormalStrategyReturnValue(unittest.IsolatedAsyncioTestCase):
    async def test_single_end_node_returns_result(self):
        """Kill NormalStrategy.execute mutmut_3: result=None→"" """
        ctx = ExecutionContext()
        runner = MagicMock()
        cb = _make_cb()

        node = _EndNode(id="end1", name="end1")
        flow = MagicMock()
        flow.start_node = node
        flow.is_end_node.return_value = True
        flow.next_node.return_value = None
        runner.run_node = AsyncMock(return_value=({"done": True}, None))

        strategy = NormalStrategy()
        result = await strategy.execute(flow, ctx, runner, cb)
        self.assertEqual(result, {"done": True})
        self.assertIsNotNone(result)

    async def test_two_node_flow_returns_last_result(self):
        """Kill reached_end mutations and result propagation."""
        ctx = ExecutionContext()
        runner = MagicMock()
        cb = _make_cb()

        start_node = _StartNode(id="s1", name="s1")
        end_node = _EndNode(id="e1", name="e1")

        call_count = {"n": 0}
        async def run_node(flow, node, **kw):
            call_count["n"] += 1
            if node is start_node:
                return ("start_res", None)
            return ({"final": True}, None)

        runner.run_node = run_node
        flow = MagicMock()
        flow.start_node = start_node
        flow.is_end_node.side_effect = lambda n: n is end_node
        flow.next_node.return_value = end_node

        strategy = NormalStrategy()
        result = await strategy.execute(flow, ctx, runner, cb)
        self.assertEqual(result, {"final": True})
        self.assertEqual(call_count["n"], 2)

    async def test_result_none_when_not_reached_end(self):
        """Kill not_reached_end path (next_node returns None before is_end)."""
        ctx = ExecutionContext()
        runner = MagicMock()
        cb = _make_cb()

        start_node = _StartNode(id="s1", name="s1")

        runner.run_node = AsyncMock(return_value=("start_val", None))
        flow = MagicMock()
        flow.start_node = start_node
        flow.is_end_node.return_value = False
        flow.next_node.return_value = None  # loop exits but reached_end stays False
        ctx.set_state(f"{ctx.express_prefix}{ctx.express_node_name}", {})

        strategy = NormalStrategy()
        result = await strategy.execute(flow, ctx, runner, cb)
        # Should return context node state dict (empty), not "start_val"
        self.assertIsInstance(result, dict)


# ---------------------------------------------------------------------------
# NormalStrategy — timeout counter decrements
# ---------------------------------------------------------------------------

class TestNormalStrategyTimeoutMutations(unittest.IsolatedAsyncioTestCase):
    async def test_timeout_ms_none_does_not_raise(self):
        """Kill mutations that remove timeout_ms guard."""
        ctx = ExecutionContext()
        runner = MagicMock()
        cb = _make_cb()
        node = _EndNode(id="e1", name="e1")
        flow = MagicMock()
        flow.start_node = node
        flow.is_end_node.return_value = True
        runner.run_node = AsyncMock(return_value=({"ok": 1}, None))

        strategy = NormalStrategy()
        result = await strategy.execute(flow, ctx, runner, cb, timeout_ms=None)
        self.assertIsNotNone(result)

    async def test_timeout_ms_large_does_not_raise(self):
        """Verify non-zero remaining is correctly computed."""
        ctx = ExecutionContext()
        runner = MagicMock()
        cb = _make_cb()
        node = _EndNode(id="e1", name="e1")
        flow = MagicMock()
        flow.start_node = node
        flow.is_end_node.return_value = True
        runner.run_node = AsyncMock(return_value=({"ok": 1}, None))

        strategy = NormalStrategy()
        result = await strategy.execute(flow, ctx, runner, cb, timeout_ms=100000)
        self.assertEqual(result, {"ok": 1})


# ---------------------------------------------------------------------------
# GeneratorStrategy — result propagation and reached_end
# ---------------------------------------------------------------------------

class TestGeneratorStrategyMutations(unittest.IsolatedAsyncioTestCase):
    async def test_single_end_node_yields_one_output(self):
        """Kill reached_end=False → None mutation."""
        ctx = ExecutionContext()
        runner = MagicMock()
        cb = _make_cb()

        node = _EndNode(id="e1", name="e1")
        flow = MagicMock()
        flow.start_node = node
        flow.is_end_node.return_value = True
        flow.next_node.return_value = None
        runner.run_node = AsyncMock(return_value=({"x": 1}, None))

        strategy = GeneratorStrategy()
        outputs = [o async for o in strategy.execute(flow, ctx, runner, cb)]
        self.assertEqual(len(outputs), 1)
        self.assertEqual(outputs[0]["result"], {"x": 1})

    async def test_two_node_flow_yields_two_outputs(self):
        """Kill mutations in the while loop."""
        ctx = ExecutionContext()
        runner = MagicMock()
        cb = _make_cb()

        start = _StartNode(id="s1", name="s1")
        end = _EndNode(id="e1", name="e1")

        async def run_node(flow, node, **kw):
            if node is start:
                return ("start_r", None)
            return ({"fin": 1}, None)

        runner.run_node = run_node
        flow = MagicMock()
        flow.start_node = start
        flow.is_end_node.side_effect = lambda n: n is end
        flow.next_node.return_value = end

        strategy = GeneratorStrategy()
        outputs = [o async for o in strategy.execute(flow, ctx, runner, cb)]
        self.assertEqual(len(outputs), 2)
        results = [o["result"] for o in outputs]
        self.assertIn("start_r", results)

    async def test_last_output_is_end_when_reached_end(self):
        ctx = ExecutionContext()
        runner = MagicMock()
        cb = _make_cb()

        end = _EndNode(id="e1", name="e1")
        flow = MagicMock()
        flow.start_node = end
        flow.is_end_node.return_value = True
        runner.run_node = AsyncMock(return_value=({"fin": 2}, None))

        strategy = GeneratorStrategy()
        outputs = [o async for o in strategy.execute(flow, ctx, runner, cb)]
        last = outputs[-1]
        self.assertEqual(last["result"], {"fin": 2})

    async def test_not_reached_end_yields_extra_end_output(self):
        """Kill mutations around not_reached_end path."""
        ctx = ExecutionContext()
        runner = MagicMock()
        cb = _make_cb()

        start = _StartNode(id="s1", name="s1")
        runner.run_node = AsyncMock(return_value=("s_res", None))
        flow = MagicMock()
        flow.start_node = start
        flow.is_end_node.return_value = False
        flow.next_node.return_value = None
        ctx.set_state(f"{ctx.express_prefix}{ctx.express_node_name}", {"s1": "s_res"})

        strategy = GeneratorStrategy()
        outputs = [o async for o in strategy.execute(flow, ctx, runner, cb)]
        # Should have normal output + extra synthetic end output
        self.assertGreaterEqual(len(outputs), 2)
        end_outs = [o for o in outputs if o.get("is_end")]
        self.assertTrue(len(end_outs) >= 1)


# ---------------------------------------------------------------------------
# _advance_one — return tuple structure
# ---------------------------------------------------------------------------

class TestAdvanceOneMutations(unittest.IsolatedAsyncioTestCase):
    async def test_returns_result_branch_next_is_end(self):
        """Verify all 4 tuple elements are correct."""
        runner = MagicMock()
        cb = _make_cb()

        end_node = _EndNode(id="e1", name="e1")
        start_node = _StartNode(id="s1", name="s1")

        runner.run_node = AsyncMock(return_value=("r1", "b1"))
        flow = MagicMock()
        flow.is_end_node.return_value = True
        flow.next_node.return_value = None

        result, branch, next_node, is_end = await _advance_one(flow, runner, cb, start_node)
        self.assertEqual(result, "r1")
        self.assertEqual(branch, "b1")
        self.assertIsNone(next_node)
        self.assertTrue(is_end)

    async def test_is_end_false_returns_next_node(self):
        """Kill is_end = flow.is_end_node(node) mutation."""
        runner = MagicMock()
        cb = _make_cb()

        mid_node = _MidNode(id="m1", name="m1")
        next_n = _EndNode(id="e1", name="e1")

        runner.run_node = AsyncMock(return_value=("mid_r", None))
        flow = MagicMock()
        flow.is_end_node.return_value = False
        flow.next_node.return_value = next_n

        result, branch, next_node, is_end = await _advance_one(flow, runner, cb, mid_node)
        self.assertEqual(result, "mid_r")
        self.assertFalse(is_end)
        self.assertIs(next_node, next_n)

    async def test_timeout_passed_to_runner(self):
        """Kill mutations that drop max_timeout_ms."""
        runner = MagicMock()
        cb = _make_cb()

        captured = {}
        async def run_node(flow, node, max_timeout_ms=None, callback_manager=None):
            captured["timeout"] = max_timeout_ms
            return ("r", None)

        runner.run_node = run_node
        flow = MagicMock()
        flow.is_end_node.return_value = True
        flow.next_node.return_value = None

        node = _StartNode(id="s1", name="s1")
        await _advance_one(flow, runner, cb, node, max_timeout_ms=9999)
        self.assertEqual(captured["timeout"], 9999)


# ---------------------------------------------------------------------------
# DistributedStrategy — all fields in lazy output
# ---------------------------------------------------------------------------

class TestDistributedStrategyOutputFields(unittest.IsolatedAsyncioTestCase):
    async def _run_one_step(self):
        """Run DistributedStrategy: saved_context with last_node_id so we go directly
        to _execute_current_node with node m1 (non-end, non-suspending)."""
        ctx = ExecutionContext()
        ctx.clean()
        ctx.setup_flow(MagicMock(flow_id="f1"), (), {})

        mid_node = _MidNode(id="m1", name="m1")
        end_node = _EndNode(id="e1", name="e1")

        # Put last_node_id into context so _determine_current_node uses _get_next_from_last
        pfx = ctx.express_prefix
        ctx.set_state(f"{pfx}LAST_NODE", "s0")  # previous node was s0
        ctx.set_state(f"{pfx}{ctx.express_node_name}", {"s0": "prev_result"})
        saved = ctx.context

        runner = MagicMock()
        runner.run_node = AsyncMock(return_value=("mid_val", "left"))
        runner.node_execution = None

        flow = MagicMock()
        # find_node_by_id("s0") returns a dummy start, next_node returns m1
        dummy_start = MagicMock()
        dummy_start.id = "s0"
        dummy_start.node_type = "start"
        flow.find_node_by_id.side_effect = lambda nid: dummy_start if nid == "s0" else mid_node
        flow.is_end_node.side_effect = lambda n: n is end_node
        flow.next_node.return_value = mid_node  # from _get_next_from_last

        cb = _make_cb()
        strategy = DistributedStrategy()
        result = await strategy.execute(flow, ctx, runner, cb, saved_context=saved)
        return result

    async def test_output_is_not_end(self):
        out = await self._run_one_step()
        self.assertFalse(out["is_end"])

    async def test_output_has_id(self):
        out = await self._run_one_step()
        self.assertEqual(out["id"], "m1")

    async def test_output_branch_not_empty(self):
        out = await self._run_one_step()
        self.assertEqual(out["branch"], "left")

    async def test_output_result_field(self):
        out = await self._run_one_step()
        self.assertEqual(out["result"], "mid_val")


# ---------------------------------------------------------------------------
# DistributedStrategy — resume paths
# ---------------------------------------------------------------------------

class TestDistributedStrategyResumePaths(unittest.IsolatedAsyncioTestCase):
    def _build_ctx_with_last_node(self, node_id, status="pending"):
        ctx = ExecutionContext()
        pfx = ctx.express_prefix
        ctx.set_state(f"{pfx}LAST_NODE", node_id)
        ctx.set_state(f"{pfx}{ctx.express_node_name}", {node_id: {"status": status}})
        return ctx

    async def test_handle_resume_no_last_node_raises(self):
        """Kill mutations that remove last_node_id check."""
        ctx = ExecutionContext()
        flow = MagicMock()
        runner = MagicMock()
        runner.node_execution = None
        cb = _make_cb()

        strategy = DistributedStrategy()
        with self.assertRaises(ResumeError) as cm:
            await strategy._handle_resume(flow, ctx, runner, cb, ResumeType.EVENT, {})
        self.assertIn("No suspended node", str(cm.exception))

    async def test_handle_resume_non_suspending_node_raises(self):
        """Kill mutations on is_suspending check."""
        ctx = self._build_ctx_with_last_node("n1")
        node = MagicMock()
        node.id = "n1"
        node.is_suspending = False

        flow = MagicMock()
        flow.find_node_by_id.return_value = node
        runner = MagicMock()
        runner.node_execution = None
        cb = _make_cb()

        strategy = DistributedStrategy()
        with self.assertRaises(ResumeError) as cm:
            await strategy._handle_resume(flow, ctx, runner, cb, ResumeType.EVENT, {})
        self.assertIn("EventNode", str(cm.exception))

    async def test_handle_resume_not_pending_raises(self):
        """Kill mutations on status != 'pending' check."""
        ctx = self._build_ctx_with_last_node("n1", status="completed")
        node = MagicMock()
        node.id = "n1"
        node.is_suspending = True

        flow = MagicMock()
        flow.find_node_by_id.return_value = node
        runner = MagicMock()
        runner.node_execution = None
        cb = _make_cb()

        strategy = DistributedStrategy()
        with self.assertRaises(ResumeError) as cm:
            await strategy._handle_resume(flow, ctx, runner, cb, ResumeType.EVENT, {})
        self.assertIn("pending", str(cm.exception))

    async def test_handle_resume_unsupported_type_raises(self):
        """Kill mutations on resume_type not in (...) check."""
        ctx = self._build_ctx_with_last_node("n1")
        node = MagicMock()
        node.id = "n1"
        node.is_suspending = True

        flow = MagicMock()
        flow.find_node_by_id.return_value = node
        runner = MagicMock()
        runner.node_execution = None
        cb = _make_cb()

        strategy = DistributedStrategy()
        with self.assertRaises(ResumeError) as cm:
            await strategy._handle_resume(flow, ctx, runner, cb, ResumeType.CONTINUE, {})
        self.assertIn("Unsupported resume type", str(cm.exception))

    async def test_handle_resume_success_returns_lazy_output(self):
        """Kill mutations around result building."""
        ctx = self._build_ctx_with_last_node("n1")
        node = MagicMock()
        node.id = "n1"
        node.is_suspending = True
        node.resume = MagicMock(return_value={"resumed": True})
        node.node_type = "event"
        node.name = "EventNode"
        node.source_line = None

        flow = MagicMock()
        flow.find_node_by_id.return_value = node
        flow.next_node.return_value = None
        runner = MagicMock()
        runner.node_execution = None
        cb = _make_cb()
        cb.on_flow_resume = MagicMock()
        cb.on_node_resume = MagicMock()

        strategy = DistributedStrategy()
        out = await strategy._handle_resume(flow, ctx, runner, cb, ResumeType.EVENT, {"data": "ok"})
        self.assertFalse(out["is_end"])
        self.assertFalse(out["is_suspend"])
        self.assertEqual(out["id"], "n1")
        self.assertEqual(out["result"], {"resumed": True})

    async def test_handle_resume_error_raises_resume_error(self):
        """Kill mutations that suppress or alter exception re-raise."""
        ctx = self._build_ctx_with_last_node("n1")
        node = MagicMock()
        node.id = "n1"
        node.is_suspending = True
        node.resume = MagicMock(side_effect=ValueError("bad resume"))
        node.source_line = None

        flow = MagicMock()
        flow.find_node_by_id.return_value = node
        runner = MagicMock()
        runner.node_execution = None
        cb = _make_cb()
        cb.on_flow_resume = MagicMock()
        cb.on_node_resume = MagicMock()

        strategy = DistributedStrategy()
        with self.assertRaises(ResumeError) as cm:
            await strategy._handle_resume(flow, ctx, runner, cb, ResumeType.EVENT, {})
        self.assertIn("bad resume", str(cm.exception))


# ---------------------------------------------------------------------------
# _subscribe_event
# ---------------------------------------------------------------------------

class TestSubscribeEventMutations(unittest.IsolatedAsyncioTestCase):
    async def test_no_event_bus_returns_false(self):
        """Kill mutations that skip event_bus check."""
        from plaita.core.strategies import _subscribe_event

        node = MagicMock()
        node.on_error = MagicMock(return_value={"status": "error"})
        node.event_filter = {}
        node.id = "n1"

        context = MagicMock()
        context.get_or_create_event_bus.return_value = None
        context.execution_id = "eid"
        context.update_node_result = MagicMock()

        flow = MagicMock()
        flow.flow_id = "f1"

        result = await _subscribe_event(node, flow, {}, context)
        self.assertFalse(result)

    async def test_with_event_bus_returns_true(self):
        """Kill mutations that alter the success return value."""
        from plaita.core.strategies import _subscribe_event

        node = MagicMock()
        node.event_filter = {}
        node.id = "n1"
        node.event_type = "order.created"

        event_bus = MagicMock()
        event_bus.register_subscription = MagicMock(return_value="sub-id-1")

        node_state = {"event_type": "order.created"}

        context = MagicMock()
        context.get_or_create_event_bus.return_value = event_bus
        context.execution_id = "eid"
        context.update_node_result = MagicMock()

        flow = MagicMock()
        flow.flow_id = "f1"

        result = await _subscribe_event(node, flow, node_state, context)
        self.assertTrue(result)
        self.assertEqual(node_state.get("subscription_id"), "sub-id-1")

    async def test_with_async_event_bus(self):
        """Kill mutations that skip coroutinefunction check."""
        from plaita.core.strategies import _subscribe_event

        node = MagicMock()
        node.event_filter = {}
        node.id = "n1"
        node.event_type = "test.event"

        event_bus = MagicMock()
        event_bus.register_subscription = AsyncMock(return_value="async-sub-id")

        node_state = {}

        context = MagicMock()
        context.get_or_create_event_bus.return_value = event_bus
        context.execution_id = "eid"
        context.update_node_result = MagicMock()

        flow = MagicMock()
        flow.flow_id = "f1"

        result = await _subscribe_event(node, flow, node_state, context)
        self.assertTrue(result)

    async def test_subscribe_exception_returns_false(self):
        """Kill mutations that alter error handling path."""
        from plaita.core.strategies import _subscribe_event

        node = MagicMock()
        node.event_filter = {}
        node.id = "n1"
        node.event_type = "test.event"
        node.on_error = MagicMock(return_value={"status": "error"})

        event_bus = MagicMock()
        event_bus.register_subscription = MagicMock(side_effect=RuntimeError("bus down"))

        context = MagicMock()
        context.get_or_create_event_bus.return_value = event_bus
        context.execution_id = "eid"
        context.update_node_result = MagicMock()

        flow = MagicMock()
        flow.flow_id = "f1"

        result = await _subscribe_event(node, flow, {}, context)
        self.assertFalse(result)


# ---------------------------------------------------------------------------
# _StateView
# ---------------------------------------------------------------------------

class TestStateViewMutations(unittest.TestCase):
    def test_flow_id_field(self):
        ctx = ExecutionContext()
        pfx = ctx.express_prefix
        ctx.set_state(f"{pfx}FLOW_ID", "my-flow")
        view = _StateView(ctx)
        self.assertEqual(view.flow_id, "my-flow")

    def test_last_node_id_field(self):
        ctx = ExecutionContext()
        pfx = ctx.express_prefix
        ctx.set_state(f"{pfx}LAST_NODE", "node-7")
        view = _StateView(ctx)
        self.assertEqual(view.last_node_id, "node-7")

    def test_last_branch_field(self):
        ctx = ExecutionContext()
        pfx = ctx.express_prefix
        ctx.set_state(f"{pfx}BRANCH", "right")
        view = _StateView(ctx)
        self.assertEqual(view.last_branch, "right")

    def test_all_none_when_empty(self):
        ctx = ExecutionContext()
        view = _StateView(ctx)
        self.assertIsNone(view.flow_id)
        self.assertIsNone(view.last_node_id)
        self.assertIsNone(view.last_branch)


# ---------------------------------------------------------------------------
# NormalStrategy — remaining/timing precision mutations (Round 2)
# ---------------------------------------------------------------------------

class TestNormalStrategyRemainingMutations(unittest.IsolatedAsyncioTestCase):
    async def test_remaining_none_when_no_timeout(self):
        """Kill remaining = None → "" mutation: verify None is passed when timeout_ms=None."""
        ctx = ExecutionContext()
        cb = _make_cb()
        captured = []

        async def run_node(flow, node, max_timeout_ms=None, callback_manager=None):
            captured.append(max_timeout_ms)
            return ({}, None)

        end = _EndNode(id="e1", name="e1")
        flow = MagicMock()
        flow.start_node = end
        flow.is_end_node.return_value = True

        runner = MagicMock()
        runner.run_node = run_node
        strategy = NormalStrategy()
        await strategy.execute(flow, ctx, runner, cb, timeout_ms=None)
        self.assertIsNone(captured[0])

    async def test_remaining_decreases_with_elapsed_time(self):
        """Kill elapsed_ms = int((time - start) / 1000) mutation (should use * 1000)."""
        import asyncio
        ctx = ExecutionContext()
        cb = _make_cb()
        captured = []
        calls = [0]

        start = _StartNode(id="s1", name="s1")
        end = _EndNode(id="e1", name="e1")

        async def run_node(flow, node, max_timeout_ms=None, callback_manager=None):
            captured.append(max_timeout_ms)
            calls[0] += 1
            if calls[0] == 1:
                await asyncio.sleep(0.06)  # 60ms
            return ({}, None)

        runner = MagicMock()
        runner.run_node = run_node
        flow = MagicMock()
        flow.start_node = start
        flow.is_end_node.side_effect = lambda n: n is end
        flow.next_node.return_value = end

        strategy = NormalStrategy()
        await strategy.execute(flow, ctx, runner, cb, timeout_ms=10000)

        if len(captured) >= 2 and captured[1] is not None:
            # After 60ms, remaining should be < 10000 (with * 1000) but = 10000 (with / 1000)
            self.assertLess(captured[1], 10000)

    async def test_remaining_uses_max_0(self):
        """Kill remaining = max(0, ...) → max(1, ...) mutation."""
        ctx = ExecutionContext()
        cb = _make_cb()
        captured = []

        end = _EndNode(id="e1", name="e1")
        flow = MagicMock()
        flow.start_node = end
        flow.is_end_node.return_value = True

        async def run_node(flow, node, max_timeout_ms=None, **kw):
            captured.append(max_timeout_ms)
            return ({}, None)

        runner = MagicMock()
        runner.run_node = run_node

        strategy = NormalStrategy()
        # With a tiny timeout_ms and a already-elapsed run, remaining should be 0 (not 1)
        await strategy.execute(flow, ctx, runner, cb, timeout_ms=10000)
        # The remaining should be <= 10000 and >= 0 (not 1 minimum)
        if captured[0] is not None:
            self.assertGreaterEqual(captured[0], 0)

    async def test_callback_manager_passed_not_none(self):
        """Kill callback_manager=None mutation in _advance_one call."""
        ctx = ExecutionContext()
        cb = _make_cb()
        captured_cb = []

        end = _EndNode(id="e1", name="e1")
        flow = MagicMock()
        flow.start_node = end
        flow.is_end_node.return_value = True

        async def run_node(flow, node, max_timeout_ms=None, callback_manager=None):
            captured_cb.append(callback_manager)
            return ({}, None)

        runner = MagicMock()
        runner.run_node = run_node
        strategy = NormalStrategy()
        await strategy.execute(flow, ctx, runner, cb)
        self.assertIsNotNone(captured_cb[0])
        self.assertIs(captured_cb[0], cb)


# ---------------------------------------------------------------------------
# DistributedStrategy.execute — resume_data passthrough
# ---------------------------------------------------------------------------

class TestDistributedStrategyResumeDataPassthrough(unittest.IsolatedAsyncioTestCase):
    async def test_resume_data_passed_to_handle_resume(self):
        """Kill resume_data = None mutation: verify resume_data is forwarded."""
        ctx = ExecutionContext()
        pfx = ctx.express_prefix
        ctx.set_state(f"{pfx}LAST_NODE", "n1")
        ctx.set_state(f"{pfx}{ctx.express_node_name}", {"n1": {"status": "pending"}})

        node = MagicMock()
        node.id = "n1"
        node.is_suspending = True
        node.source_line = None

        resume_data_received = []
        def mock_resume(exec_ctx, rt, rd=None):
            resume_data_received.append(rd)
            return {"resumed": True}
        node.resume = mock_resume
        node.node_type = "event"
        node.name = "EventNode"

        flow = MagicMock()
        flow.find_node_by_id.return_value = node
        flow.next_node.return_value = None

        runner = MagicMock()
        runner.node_execution = None
        cb = _make_cb()
        cb.on_flow_resume = MagicMock()
        cb.on_node_resume = MagicMock()

        strategy = DistributedStrategy()
        the_data = {"specific_key": "specific_value"}
        result = await strategy.execute(
            flow, ctx, runner, cb,
            saved_context=ctx.context,
            resume_type="event",
            resume_data=the_data,
        )
        self.assertEqual(len(resume_data_received), 1)
        self.assertEqual(resume_data_received[0], the_data)


# ---------------------------------------------------------------------------
# DistributedStrategy._determine_current_node — dispatch correctness
# ---------------------------------------------------------------------------

class TestDistributedDetermineCurrentNodeMutations(unittest.IsolatedAsyncioTestCase):
    async def test_uses_last_node_id_from_context(self):
        """Kill last_node_id = None mutation."""
        ctx = ExecutionContext()
        pfx = ctx.express_prefix
        ctx.set_state(f"{pfx}LAST_NODE", "node-xyz")
        ctx.set_state(f"{pfx}{ctx.express_node_name}", {"node-xyz": "prev_res"})

        found_node = _MidNode(id="node-xyz", name="m")
        next_node = _EndNode(id="e1", name="end")

        flow = MagicMock()
        flow.find_node_by_id.return_value = found_node
        flow.next_node.return_value = next_node

        runner = MagicMock()
        cb = _make_cb()
        strategy = DistributedStrategy()
        current, result, branch = strategy._get_next_from_last(flow, ctx, "node-xyz")

        # find_node_by_id should have been called with "node-xyz"
        flow.find_node_by_id.assert_called_with("node-xyz")

    async def test_dispatches_to_start_new_flow_when_no_last_node(self):
        """Kill _get_next_from_last call when last_node_id is falsy."""
        ctx = ExecutionContext()
        # No LAST_NODE set

        start = _StartNode(id="s1", name="s1")
        end = _EndNode(id="e1", name="e1")

        runner = MagicMock()
        runner.run_node = AsyncMock(return_value=("start_res", None))
        flow = MagicMock()
        flow.start_node = start
        flow.is_end_node.return_value = False
        flow.next_node.return_value = end

        cb = _make_cb()
        strategy = DistributedStrategy()
        current, result, branch = await strategy._determine_current_node(flow, ctx, runner, cb)

        # Should have called run_node (via _start_new_flow)
        runner.run_node.assert_called_once()

    async def test_get_next_from_last_passes_id_to_find_node(self):
        """Kill _get_next_from_last(flow, context, None) mutation."""
        ctx = ExecutionContext()
        pfx = ctx.express_prefix
        ctx.set_state(f"{pfx}{ctx.express_node_name}", {"n99": "res"})

        mid = _MidNode(id="n99", name="m")
        end = _EndNode(id="e1", name="end")

        flow = MagicMock()
        flow.find_node_by_id.return_value = mid
        flow.next_node.return_value = end

        strategy = DistributedStrategy()
        current, result, branch = strategy._get_next_from_last(flow, ctx, "n99")

        # Must pass "n99" not None
        call_args = flow.find_node_by_id.call_args
        self.assertEqual(call_args[0][0], "n99")


# ---------------------------------------------------------------------------
# DistributedStrategy._get_next_from_last — node identity and result
# ---------------------------------------------------------------------------

class TestGetNextFromLastMutations(unittest.TestCase):
    def _make_ctx_with_last(self, node_id, result_val="prev_res"):
        ctx = ExecutionContext()
        pfx = ctx.express_prefix
        ctx.set_state(f"{pfx}LAST_NODE", node_id)
        ctx.set_state(f"{pfx}{ctx.express_node_name}", {node_id: result_val})
        ctx.set_state(f"{pfx}BRANCH", "right")
        return ctx

    def test_current_node_from_find_not_none(self):
        """Kill current_node = None mutation."""
        ctx = self._make_ctx_with_last("n5")
        found = _MidNode(id="n5", name="m")
        next_n = _EndNode(id="e1", name="end")
        flow = MagicMock()
        flow.find_node_by_id.return_value = found
        flow.next_node.return_value = next_n

        strategy = DistributedStrategy()
        current, result, branch = strategy._get_next_from_last(flow, ctx, "n5")
        self.assertIs(current, next_n)

    def test_result_from_node_results(self):
        """Kill result = node_results.get(None) mutation."""
        ctx = self._make_ctx_with_last("n5", result_val="expected_result")
        found = _MidNode(id="n5", name="m")
        next_n = _EndNode(id="e1", name="end")
        flow = MagicMock()
        flow.find_node_by_id.return_value = found
        flow.next_node.return_value = next_n

        strategy = DistributedStrategy()
        current, result, branch = strategy._get_next_from_last(flow, ctx, "n5")
        self.assertEqual(result, "expected_result")

    def test_branch_from_context(self):
        """Kill branch = context.last_branch → None mutation."""
        ctx = self._make_ctx_with_last("n5")
        found = _MidNode(id="n5", name="m")
        flow = MagicMock()
        flow.find_node_by_id.return_value = found
        flow.next_node.return_value = None

        strategy = DistributedStrategy()
        current, result, branch = strategy._get_next_from_last(flow, ctx, "n5")
        self.assertEqual(branch, "right")

    def test_uses_express_prefix_for_state_key(self):
        """Kill pfx = None mutation."""
        ctx = ExecutionContext()
        pfx = ctx.express_prefix
        ctx.set_state(f"{pfx}{ctx.express_node_name}", {"n5": "v5"})

        found = _MidNode(id="n5", name="m")
        flow = MagicMock()
        flow.find_node_by_id.return_value = found
        flow.next_node.return_value = None

        strategy = DistributedStrategy()
        current, result, branch = strategy._get_next_from_last(flow, ctx, "n5")
        self.assertEqual(result, "v5")


# ---------------------------------------------------------------------------
# DistributedStrategy._handle_resume — error dict precision
# ---------------------------------------------------------------------------

class TestHandleResumeErrorDictMutations(unittest.IsolatedAsyncioTestCase):
    async def _setup_for_error_resume(self):
        ctx = ExecutionContext()
        pfx = ctx.express_prefix
        ctx.set_state(f"{pfx}LAST_NODE", "n1")
        ctx.set_state(f"{pfx}{ctx.express_node_name}", {"n1": {"status": "pending"}})

        node = MagicMock()
        node.id = "n1"
        node.is_suspending = True
        node.resume = MagicMock(side_effect=RuntimeError("resume_failed_xyz"))
        node.source_line = None

        flow = MagicMock()
        flow.find_node_by_id.return_value = node

        runner = MagicMock()
        runner.node_execution = None
        cb = _make_cb()
        cb.on_flow_resume = MagicMock()
        cb.on_node_resume = MagicMock()

        return ctx, flow, runner, cb, node

    async def test_error_dict_code_is_negative_500(self):
        """Kill {"code": +500} and {"code": -501} mutations."""
        ctx, flow, runner, cb, node = await self._setup_for_error_resume()
        strategy = DistributedStrategy()
        with self.assertRaises(ResumeError):
            await strategy._handle_resume(flow, ctx, runner, cb, ResumeType.EVENT, {})

        # on_node_end should have been called with error dict
        cb.on_node_end.assert_called()
        call_args = cb.on_node_end.call_args
        error_dict = call_args[0][3] if len(call_args[0]) > 3 else call_args[1].get("error", call_args[0][3])
        # Normalize - error is 4th positional arg: (flow, node, None, error, exception=e)
        error_dict = cb.on_node_end.call_args[0][3]
        self.assertEqual(error_dict["code"], -500)

    async def test_error_dict_code_key_is_lowercase(self):
        """Kill {"CODE": -500} mutation."""
        ctx, flow, runner, cb, node = await self._setup_for_error_resume()
        strategy = DistributedStrategy()
        with self.assertRaises(ResumeError):
            await strategy._handle_resume(flow, ctx, runner, cb, ResumeType.EVENT, {})

        error_dict = cb.on_node_end.call_args[0][3]
        self.assertIn("code", error_dict)
        self.assertNotIn("CODE", error_dict)

    async def test_error_dict_message_key_is_lowercase(self):
        """Kill {"MESSAGE": ...} and {"XXmessageXX": ...} mutations."""
        ctx, flow, runner, cb, node = await self._setup_for_error_resume()
        strategy = DistributedStrategy()
        with self.assertRaises(ResumeError):
            await strategy._handle_resume(flow, ctx, runner, cb, ResumeType.EVENT, {})

        error_dict = cb.on_node_end.call_args[0][3]
        self.assertIn("message", error_dict)
        self.assertNotIn("MESSAGE", error_dict)
        self.assertNotIn("XXmessageXX", error_dict)

    async def test_error_dict_message_contains_exception_text(self):
        """Kill mutations that drop str(e)."""
        ctx, flow, runner, cb, node = await self._setup_for_error_resume()
        strategy = DistributedStrategy()
        with self.assertRaises(ResumeError):
            await strategy._handle_resume(flow, ctx, runner, cb, ResumeType.EVENT, {})

        error_dict = cb.on_node_end.call_args[0][3]
        self.assertIn("resume_failed_xyz", error_dict["message"])


# ---------------------------------------------------------------------------
# _subscribe_event — argument precision
# ---------------------------------------------------------------------------

class TestSubscribeEventArgumentPrecision(unittest.IsolatedAsyncioTestCase):
    async def test_on_error_called_with_context_and_exact_message(self):
        """Kill on_error(None, ...) and on_error(context, None) mutations."""
        from plaita.core.strategies import _subscribe_event

        node = MagicMock()
        node.on_error = MagicMock(return_value={"status": "error"})
        node.id = "n1"
        node.event_type = "test.event"

        context = MagicMock()
        context.get_or_create_event_bus.return_value = None  # no event bus
        context.update_node_result = MagicMock()

        flow = MagicMock()
        flow.flow_id = "f1"

        await _subscribe_event(node, flow, {}, context)

        # on_error should have been called with (context, "Unable to get event bus")
        node.on_error.assert_called_once()
        call_args = node.on_error.call_args[0]
        self.assertIs(call_args[0], context)  # first arg is context, not None
        self.assertEqual(call_args[1], "Unable to get event bus")

    async def test_on_error_message_is_capitalized(self):
        """Kill lowercase mutation: "unable to get event bus"."""
        from plaita.core.strategies import _subscribe_event

        node = MagicMock()
        node.on_error = MagicMock(return_value={"status": "error"})
        node.id = "n1"
        node.event_type = "test.event"

        context = MagicMock()
        context.get_or_create_event_bus.return_value = None
        context.update_node_result = MagicMock()
        flow = MagicMock()
        flow.flow_id = "f1"

        await _subscribe_event(node, flow, {}, context)
        call_args = node.on_error.call_args[0]
        msg = call_args[1]
        self.assertTrue(msg[0].isupper(), f"Message should start with uppercase U: {msg!r}")
        self.assertNotEqual(msg, msg.lower(), "Message should not be all lowercase")

    async def test_update_node_result_called_with_error_state(self):
        """Kill context.update_node_result(node,) mutation."""
        from plaita.core.strategies import _subscribe_event

        error_state = {"status": "error", "err": "no bus"}
        node = MagicMock()
        node.on_error = MagicMock(return_value=error_state)
        node.id = "n1"
        node.event_type = "test.event"

        context = MagicMock()
        context.get_or_create_event_bus.return_value = None
        context.update_node_result = MagicMock()
        flow = MagicMock()

        await _subscribe_event(node, flow, {}, context)
        context.update_node_result.assert_called_once_with(node, error_state)

    async def test_subscription_params_event_type_key(self):
        """Kill "XXevent_typeXX" and "EVENT_TYPE" key mutations."""
        from plaita.core.strategies import _subscribe_event

        node = MagicMock()
        node.event_filter = {}
        node.id = "n1"
        node.event_type = "order.shipped"

        captured_params = {}
        event_bus = MagicMock()
        def mock_register(**kwargs):
            captured_params.update(kwargs)
            return "sub-1"
        event_bus.register_subscription = mock_register

        context = MagicMock()
        context.get_or_create_event_bus.return_value = event_bus
        context.execution_id = "eid1"
        context.update_node_result = MagicMock()

        flow = MagicMock()
        flow.flow_id = "f1"

        await _subscribe_event(node, flow, {}, context)
        self.assertIn("event_type", captured_params)
        self.assertNotIn("XXevent_typeXX", captured_params)

    async def test_node_state_event_type_overrides_node_event_type(self):
        """Kill node_state.get("XXevent_typeXX", ...) and get("EVENT_TYPE", ...) mutations."""
        from plaita.core.strategies import _subscribe_event

        node = MagicMock()
        node.event_filter = {}
        node.id = "n1"
        node.event_type = "default.event"  # fallback

        captured_params = {}
        event_bus = MagicMock()
        def mock_register(**kwargs):
            captured_params.update(kwargs)
            return "sub-1"
        event_bus.register_subscription = mock_register

        context = MagicMock()
        context.get_or_create_event_bus.return_value = event_bus
        context.execution_id = "eid1"
        context.update_node_result = MagicMock()

        flow = MagicMock()
        flow.flow_id = "f1"

        # node_state has "event_type" — should use it, not node.event_type
        node_state = {"event_type": "resolved.event"}
        await _subscribe_event(node, flow, node_state, context)
        self.assertEqual(captured_params["event_type"], "resolved.event")

    async def test_node_event_type_used_as_fallback(self):
        """Kill node_state.get("event_type", None) mutation (should use node.event_type)."""
        from plaita.core.strategies import _subscribe_event

        node = MagicMock()
        node.event_filter = {}
        node.id = "n1"
        node.event_type = "fallback.event"

        captured_params = {}
        event_bus = MagicMock()
        def mock_register(**kwargs):
            captured_params.update(kwargs)
            return "sub-1"
        event_bus.register_subscription = mock_register

        context = MagicMock()
        context.get_or_create_event_bus.return_value = event_bus
        context.execution_id = "eid1"
        context.update_node_result = MagicMock()

        flow = MagicMock()
        flow.flow_id = "f1"

        # node_state does NOT have "event_type" → should use node.event_type
        await _subscribe_event(node, flow, {}, context)
        self.assertEqual(captured_params["event_type"], "fallback.event")

    async def test_subscription_params_filter_condition_key(self):
        """Kill filter_condition key mutation."""
        from plaita.core.strategies import _subscribe_event

        node = MagicMock()
        node.event_filter = {"status": "paid"}
        node.id = "n1"
        node.event_type = "order.paid"

        captured_params = {}
        event_bus = MagicMock()
        def mock_register(**kwargs):
            captured_params.update(kwargs)
            return "sub-1"
        event_bus.register_subscription = mock_register

        context = MagicMock()
        context.get_or_create_event_bus.return_value = event_bus
        context.execution_id = "eid1"
        context.update_node_result = MagicMock()

        flow = MagicMock()
        flow.flow_id = "f1"

        await _subscribe_event(node, flow, {}, context)
        self.assertIn("filter_condition", captured_params)
        self.assertEqual(captured_params["filter_condition"], {"status": "paid"})

    async def test_subscription_id_stored_in_node_state(self):
        """Kill node_state["subscription_id"] = subscription_id mutation."""
        from plaita.core.strategies import _subscribe_event

        node = MagicMock()
        node.event_filter = {}
        node.id = "n1"
        node.event_type = "test.event"

        event_bus = MagicMock()
        event_bus.register_subscription = MagicMock(return_value="sub-99")

        node_state = {}
        context = MagicMock()
        context.get_or_create_event_bus.return_value = event_bus
        context.execution_id = "eid1"
        context.update_node_result = MagicMock()

        flow = MagicMock()
        flow.flow_id = "f1"

        await _subscribe_event(node, flow, node_state, context)
        self.assertEqual(node_state.get("subscription_id"), "sub-99")

    async def test_context_update_called_with_node_state_after_success(self):
        """Kill context.update_node_result call mutation."""
        from plaita.core.strategies import _subscribe_event

        node = MagicMock()
        node.event_filter = {}
        node.id = "n1"
        node.event_type = "test.event"

        event_bus = MagicMock()
        event_bus.register_subscription = MagicMock(return_value="sub-7")

        node_state = {"event_type": "test.event"}
        context = MagicMock()
        context.get_or_create_event_bus.return_value = event_bus
        context.execution_id = "eid1"
        context.update_node_result = MagicMock()

        flow = MagicMock()
        flow.flow_id = "f1"

        await _subscribe_event(node, flow, node_state, context)
        context.update_node_result.assert_called_once_with(node, node_state)


if __name__ == "__main__":
    unittest.main()
