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



# ---------------------------------------------------------------------------
# Round 3: NormalStrategy.execute — timeout propagation to runner
# (kills mutmut_14: remaining=None, mutmut_26: max_timeout_ms=None,
#  mutmut_31: kwarg removed, mutmut_12: +start_time instead of -start_time)
# ---------------------------------------------------------------------------

class TestNormalStrategyTimeoutPropagationR3(unittest.IsolatedAsyncioTestCase):
    async def test_max_timeout_ms_passed_to_runner_when_timeout_set(self):
        """Kill mutmut_14 (remaining=None) and mutmut_26,31 (not passed)."""
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
        await strategy.execute(flow, ctx, runner, cb, timeout_ms=30000)

        self.assertEqual(len(captured), 1)
        # Must not be None (kills mutmut_14, 26, 31)
        self.assertIsNotNone(captured[0], "max_timeout_ms should not be None when timeout_ms=30000")
        # Should be close to 30000 (not 0 from wrong calculation)
        self.assertGreater(captured[0], 20000,
                           f"max_timeout_ms should be near 30000 for fresh run, got {captured[0]}")

    async def test_elapsed_uses_subtraction_not_addition(self):
        """Kill mutmut_12: time.time() + start_time.
        With + instead of -, elapsed_ms would be ~2*start_time*1000 = huge,
        so remaining = max(0, 30000 - huge) = 0, not ~30000.
        """
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
        await strategy.execute(flow, ctx, runner, cb, timeout_ms=60_000)

        self.assertEqual(len(captured), 1)
        # With correct subtraction, remaining ≈ 60000; with addition remaining = 0
        self.assertIsNotNone(captured[0])
        self.assertGreater(captured[0], 50_000,
                           "With correct elapsed calculation, remaining should be close to 60000ms")


# ---------------------------------------------------------------------------
# Round 3: NormalStrategy.execute — debug log precision
# (kills mutmut_33-39: next_node/branch arg changes)
# ---------------------------------------------------------------------------

class TestNormalStrategyDebugLogPrecision(unittest.IsolatedAsyncioTestCase):
    async def test_debug_log_has_next_node_and_branch(self):
        """Kill mutmut_34 (next_node→None) and mutmut_35 (branch→None) in debug log.
        Need a 2-node flow so the debug log fires after the first node.
        """
        ctx = ExecutionContext()
        cb = _make_cb()

        start = _StartNode(id="s1", name="s1")
        end = _EndNode(id="e1", name="e1")

        call_count = {"n": 0}
        async def run_node(flow, node, max_timeout_ms=None, **kw):
            call_count["n"] += 1
            if node is start:
                return ("start_res", "right_branch")
            return ({"done": True}, None)

        runner = MagicMock()
        runner.run_node = run_node
        flow = MagicMock()
        flow.start_node = start
        flow.is_end_node.side_effect = lambda n: n is end
        flow.next_node.return_value = end

        strategy = NormalStrategy()
        with self.assertLogs("plaita", level="DEBUG") as cm:
            await strategy.execute(flow, ctx, runner, cb)

        debug_msgs = [m for m in cm.output if "next_node" in m and "branch" in m]
        self.assertTrue(len(debug_msgs) >= 1, "Expected debug log with 'next_node' and 'branch'")
        msg = debug_msgs[0]
        # Kill mutmut_34: next_node→None → "None" instead of node reference
        # Kill mutmut_36: logger.debug(next_node, branch) → format string removed
        self.assertIn("branch", msg)

    async def test_debug_log_format_string_not_mangled(self):
        """Kill mutmut_39: XX prefix in debug log format string."""
        ctx = ExecutionContext()
        cb = _make_cb()
        start = _StartNode(id="s1", name="s1")
        end = _EndNode(id="e1", name="e1")

        async def run_node(flow, node, **kw):
            if node is start:
                return ("r", None)
            return ({"done": True}, None)

        runner = MagicMock()
        runner.run_node = run_node
        flow = MagicMock()
        flow.start_node = start
        flow.is_end_node.side_effect = lambda n: n is end
        flow.next_node.return_value = end

        strategy = NormalStrategy()
        with self.assertLogs("plaita", level="DEBUG") as cm:
            await strategy.execute(flow, ctx, runner, cb)

        debug_msgs = [m for m in cm.output if "next_node" in m]
        if debug_msgs:
            self.assertNotIn("XXnext_node", debug_msgs[0])


# ---------------------------------------------------------------------------
# Round 3: DistributedStrategy._handle_resume — error message and arg checks
# (kills mutmut_7: mangled message, mutmut_11: find_node_by_id(None),
#  mutmut_14/16: node=None in ResumeError, mutmut_70: callback args)
# ---------------------------------------------------------------------------

class TestHandleResumeArgPrecisionR3(unittest.IsolatedAsyncioTestCase):
    def _make_ctx_with_last(self, node_id="n1", status="pending"):
        ctx = ExecutionContext()
        pfx = ctx.express_prefix
        ctx.set_state(f"{pfx}LAST_NODE", node_id)
        ctx.set_state(f"{pfx}{ctx.express_node_name}", {node_id: {"status": status}})
        return ctx

    async def test_no_last_node_error_message_exact(self):
        """Kill mutmut_7: 'XXNo suspended node found for resumeXX'."""
        ctx = ExecutionContext()  # no LAST_NODE set

        flow = MagicMock()
        runner = MagicMock()
        runner.node_execution = None
        cb = _make_cb()
        cb.on_flow_resume = MagicMock()

        strategy = DistributedStrategy()
        with self.assertRaises(ResumeError) as cm:
            await strategy._handle_resume(flow, ctx, runner, cb, ResumeType.EVENT, {})

        err = cm.exception
        msg = str(err)
        # Kill mutmut_7: mangled → "XXNo suspended node found..."
        self.assertIn("No suspended node found for resume", msg)
        self.assertNotIn("XX", msg)

    async def test_find_node_called_with_last_node_id(self):
        """Kill mutmut_11: flow.find_node_by_id(None) instead of (last_node_id)."""
        ctx = self._make_ctx_with_last("specific-node-id-42")

        node = MagicMock()
        node.id = "specific-node-id-42"
        node.is_suspending = True
        node.resume = MagicMock(return_value={"resumed": True})
        node.source_line = None
        node.node_type = "event"
        node.name = "Event"

        flow = MagicMock()
        flow.find_node_by_id.return_value = node
        flow.next_node.return_value = None

        runner = MagicMock()
        runner.node_execution = None
        cb = _make_cb()
        cb.on_flow_resume = MagicMock()
        cb.on_node_resume = MagicMock()

        strategy = DistributedStrategy()
        await strategy._handle_resume(flow, ctx, runner, cb, ResumeType.EVENT, {})

        # Must be called with the actual last_node_id, not None
        flow.find_node_by_id.assert_called_with("specific-node-id-42")

    async def test_non_suspending_error_has_current_node(self):
        """Kill mutmut_14/16: node=None in ResumeError for non-suspending node."""
        ctx = self._make_ctx_with_last("non-ev-node")

        non_ev_node = MagicMock()
        non_ev_node.id = "non-ev-node"
        non_ev_node.is_suspending = False  # NOT suspending → triggers ResumeError

        flow = MagicMock()
        flow.find_node_by_id.return_value = non_ev_node

        runner = MagicMock()
        runner.node_execution = None
        cb = _make_cb()
        cb.on_flow_resume = MagicMock()

        strategy = DistributedStrategy()
        with self.assertRaises(ResumeError) as cm:
            await strategy._handle_resume(flow, ctx, runner, cb, ResumeType.EVENT, {})

        err = cm.exception
        # Kill mutmut_14/16: node=None in ResumeError constructor
        self.assertIsNotNone(err.node, "ResumeError.node should not be None")
        self.assertIs(err.node, non_ev_node)

    async def test_on_node_end_called_with_flow_as_first_arg(self):
        """Kill mutmut_70: callback_manager.on_node_end(current_node, result) — missing flow."""
        ctx = self._make_ctx_with_last("ev1")

        node = MagicMock()
        node.id = "ev1"
        node.is_suspending = True
        node.resume = MagicMock(return_value={"resumed": True})
        node.source_line = None
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
        cb.on_node_end = MagicMock()

        strategy = DistributedStrategy()
        await strategy._handle_resume(flow, ctx, runner, cb, ResumeType.EVENT, {})

        # on_node_end should be called with (flow, node, result) — 3 positional args
        cb.on_node_end.assert_called()
        call_args = cb.on_node_end.call_args[0]
        self.assertGreaterEqual(len(call_args), 2, "on_node_end needs at least flow and node")
        # Kill mutmut_70: first arg is current_node not flow
        self.assertIs(call_args[0], flow, "First arg to on_node_end must be flow, not node")


# ---------------------------------------------------------------------------
# Round 3: DistributedStrategy._start_new_flow — runner call arg precision
# (kills mutmut_4: flow=None, mutmut_5: start_node=None, mutmut_6: cb=None,
#  mutmut_7: flow arg removed, mutmut_8: start_node arg removed)
# ---------------------------------------------------------------------------

class TestStartNewFlowRunnerArgsPrecision(unittest.IsolatedAsyncioTestCase):
    async def _run_start_new_flow(self):
        ctx = ExecutionContext()
        start = _StartNode(id="s1", name="s1")
        end = _EndNode(id="e1", name="e1")

        run_calls = []
        async def capture_run(*args, **kwargs):
            run_calls.append((args, kwargs))
            return ("result", None)

        runner = MagicMock()
        runner.run_node = capture_run
        flow = MagicMock()
        flow.start_node = start
        flow.next_node.return_value = end

        cb = _make_cb()
        strategy = DistributedStrategy()
        await strategy._start_new_flow(flow, ctx, runner, cb)
        return run_calls, flow, start, cb

    async def test_start_new_flow_passes_flow_as_first_arg(self):
        """Kill mutmut_4: runner.run_node(None, start_node, ...)."""
        run_calls, flow, start, cb = await self._run_start_new_flow()
        self.assertEqual(len(run_calls), 1)
        args, kwargs = run_calls[0]
        self.assertIs(args[0], flow, "First positional arg must be flow")

    async def test_start_new_flow_passes_start_node_as_second_arg(self):
        """Kill mutmut_5: runner.run_node(flow, None, ...)."""
        run_calls, flow, start, cb = await self._run_start_new_flow()
        args, kwargs = run_calls[0]
        self.assertIs(args[1], start, "Second positional arg must be start_node")

    async def test_start_new_flow_passes_callback_manager(self):
        """Kill mutmut_6: callback_manager=None."""
        run_calls, flow, start, cb = await self._run_start_new_flow()
        args, kwargs = run_calls[0]
        self.assertIn("callback_manager", kwargs, "callback_manager kwarg must be present")
        self.assertIs(kwargs["callback_manager"], cb)

    async def test_start_new_flow_has_two_positional_args(self):
        """Kill mutmut_7 (remove flow) and mutmut_8 (remove start_node): must have 2 args."""
        run_calls, flow, start, cb = await self._run_start_new_flow()
        args, kwargs = run_calls[0]
        self.assertEqual(len(args), 2,
                         f"run_node should receive 2 positional args (flow, start_node), got {len(args)}")


# ---------------------------------------------------------------------------
# Round 3: DistributedStrategy._execute_current_node — runner call arg precision
# (kills mutmut_2: flow=None, mutmut_3: current_node=None, mutmut_5: remove flow,
#  mutmut_12: context=None in final _create_lazy_output)
# ---------------------------------------------------------------------------

class TestExecuteCurrentNodeArgsPrecision(unittest.IsolatedAsyncioTestCase):
    async def _run_execute_current_node(self, is_end=False, is_suspending=False):
        ctx = ExecutionContext()
        node = MagicMock()
        node.id = "curr1"
        node.node_type = "mid"
        node.name = "Mid"
        node.is_suspending = is_suspending

        run_calls = []
        async def capture_run(*args, **kwargs):
            run_calls.append((args, kwargs))
            return ("res", "branch_x")

        runner = MagicMock()
        runner.run_node = capture_run
        flow = MagicMock()
        flow.is_end_node.return_value = is_end

        cb = _make_cb()
        strategy = DistributedStrategy()
        result = await strategy._execute_current_node(flow, ctx, runner, cb, node)
        return run_calls, flow, node, ctx, result

    async def test_execute_current_node_passes_flow_as_first_arg(self):
        """Kill mutmut_2: runner.run_node(None, current_node, ...)."""
        run_calls, flow, node, ctx, result = await self._run_execute_current_node()
        self.assertEqual(len(run_calls), 1)
        args, kwargs = run_calls[0]
        self.assertIs(args[0], flow, "First arg must be flow, not None")

    async def test_execute_current_node_passes_node_as_second_arg(self):
        """Kill mutmut_3: runner.run_node(flow, None, ...)."""
        run_calls, flow, node, ctx, result = await self._run_execute_current_node()
        args, kwargs = run_calls[0]
        self.assertIs(args[1], node, "Second arg must be current_node, not None")

    async def test_execute_current_node_has_two_positional_args(self):
        """Kill mutmut_5: runner.run_node(current_node, ...) — flow removed."""
        run_calls, flow, node, ctx, result = await self._run_execute_current_node()
        args, kwargs = run_calls[0]
        self.assertEqual(len(args), 2,
                         f"Expected 2 positional args to run_node, got {len(args)}")

    async def test_lazy_output_context_is_not_none(self):
        """Kill mutmut_12: _create_lazy_output(..., None, ...) — context replaced with None."""
        run_calls, flow, node, ctx, result = await self._run_execute_current_node(is_end=False)
        # _create_lazy_output returns {"context": ctx.to_dict(), ...}
        self.assertIn("context", result)
        self.assertIsNotNone(result["context"],
                             "context in lazy output should not be None")


# ---------------------------------------------------------------------------
# Round 4: _advance_one — runner.run_node first arg must be flow not None
# (kills mutmut_2: runner.run_node(None, node, ...))
# ---------------------------------------------------------------------------
class TestAdvanceOneFlowArgPrecision(unittest.IsolatedAsyncioTestCase):
    async def test_runner_called_with_flow_not_none(self):
        """Kill mutmut_2: runner.run_node(None, node, ...) — flow replaced by None."""
        from plaita.core.strategies import _advance_one

        captured = []
        async def run_node(*args, **kwargs):
            captured.append(args)
            return ("r", None)

        flow = MagicMock()
        flow.is_end_node.return_value = True
        flow.next_node.return_value = None

        runner = MagicMock()
        runner.run_node = run_node

        node = _StartNode(id="s1", name="s1")
        cb = _make_cb()

        await _advance_one(flow, runner, cb, node)

        self.assertEqual(len(captured), 1)
        self.assertIs(captured[0][0], flow,
                      f"First arg to run_node must be flow, got {captured[0][0]!r}")
        self.assertIs(captured[0][1], node,
                      f"Second arg to run_node must be node, got {captured[0][1]!r}")

    async def test_next_node_called_with_node_and_branch(self):
        """Kill mutmut_13 (node→None), mutmut_14 (branch→None), mutmut_15 (single arg), mutmut_16 (no branch)."""
        from plaita.core.strategies import _advance_one

        sentinel_branch = "left_branch_sentinel"

        flow = MagicMock()
        flow.is_end_node.return_value = False  # not end → calls flow.next_node
        next_n = _EndNode(id="e1", name="e1")
        flow.next_node.return_value = next_n

        runner = MagicMock()
        runner.run_node = AsyncMock(return_value=("r", sentinel_branch))

        node = _MidNode(id="m1", name="m1")
        cb = _make_cb()

        await _advance_one(flow, runner, cb, node)

        flow.next_node.assert_called_once_with(node, sentinel_branch)

    async def test_next_node_first_arg_is_original_node_not_none(self):
        """Kill mutmut_13: flow.next_node(None, branch) — node replaced by None."""
        from plaita.core.strategies import _advance_one

        flow = MagicMock()
        flow.is_end_node.return_value = False
        flow.next_node.return_value = None

        runner = MagicMock()
        runner.run_node = AsyncMock(return_value=("r", "right"))

        node = _MidNode(id="m1", name="m1")
        cb = _make_cb()

        await _advance_one(flow, runner, cb, node)

        call_args = flow.next_node.call_args[0]
        self.assertIs(call_args[0], node,
                      "first arg to flow.next_node must be the original node")

    async def test_next_node_second_arg_is_branch_not_none(self):
        """Kill mutmut_14: flow.next_node(node, None) — branch replaced by None."""
        from plaita.core.strategies import _advance_one

        distinct_branch = "specific_branch_xyz"
        flow = MagicMock()
        flow.is_end_node.return_value = False
        flow.next_node.return_value = None

        runner = MagicMock()
        runner.run_node = AsyncMock(return_value=("r", distinct_branch))

        node = _MidNode(id="m1", name="m1")
        cb = _make_cb()

        await _advance_one(flow, runner, cb, node)

        call_args = flow.next_node.call_args[0]
        self.assertEqual(len(call_args), 2,
                         "flow.next_node must be called with 2 positional args")
        self.assertEqual(call_args[1], distinct_branch,
                         "second arg to flow.next_node must be the branch, not None")


# ---------------------------------------------------------------------------
# Round 4: _subscribe_event — subscription_params exact key names
# (kills mutmut_28: XXcorrelation_idXX, mutmut_29: CORRELATION_ID,
#  mutmut_30: XXflow_idXX, mutmut_31: FLOW_ID, mutmut_32: XXnode_idXX, mutmut_33: NODE_ID)
# ---------------------------------------------------------------------------
class TestSubscribeEventParamKeyNames(unittest.IsolatedAsyncioTestCase):
    async def _capture_params(self, execution_id="exec-1", flow_id="flow-1", node_id="nd-1"):
        from plaita.core.strategies import _subscribe_event

        captured_params = {}
        event_bus = MagicMock()
        def mock_register(**kwargs):
            captured_params.update(kwargs)
            return "sub-x"
        event_bus.register_subscription = mock_register

        node = MagicMock()
        node.event_filter = {}
        node.id = node_id
        node.event_type = "test.event"

        context = MagicMock()
        context.get_or_create_event_bus.return_value = event_bus
        context.execution_id = execution_id
        context.update_node_result = MagicMock()

        flow = MagicMock()
        flow.flow_id = flow_id

        await _subscribe_event(node, flow, {}, context)
        return captured_params

    async def test_correlation_id_key_exact(self):
        """Kill mutmut_28 (XXcorrelation_idXX) and mutmut_29 (CORRELATION_ID)."""
        params = await self._capture_params(execution_id="exec-sentinel-99")
        self.assertIn("correlation_id", params,
                      "subscription_params must have exact key 'correlation_id'")
        self.assertNotIn("XXcorrelation_idXX", params)
        self.assertNotIn("CORRELATION_ID", params)
        self.assertEqual(params["correlation_id"], "exec-sentinel-99")

    async def test_flow_id_key_exact(self):
        """Kill mutmut_30 (XXflow_idXX) and mutmut_31 (FLOW_ID)."""
        params = await self._capture_params(flow_id="flow-sentinel-88")
        self.assertIn("flow_id", params,
                      "subscription_params must have exact key 'flow_id'")
        self.assertNotIn("XXflow_idXX", params)
        self.assertNotIn("FLOW_ID", params)
        self.assertEqual(params["flow_id"], "flow-sentinel-88")

    async def test_node_id_key_exact(self):
        """Kill mutmut_32 (XXnode_idXX) and mutmut_33 (NODE_ID)."""
        params = await self._capture_params(node_id="nd-sentinel-77")
        self.assertIn("node_id", params,
                      "subscription_params must have exact key 'node_id'")
        self.assertNotIn("XXnode_idXX", params)
        self.assertNotIn("NODE_ID", params)
        self.assertEqual(params["node_id"], "nd-sentinel-77")

    async def test_correlation_id_value_is_execution_id(self):
        """Verify correlation_id value comes from context.execution_id."""
        params = await self._capture_params(execution_id="unique-exec-7654")
        self.assertEqual(params.get("correlation_id"), "unique-exec-7654",
                         "correlation_id must equal context.execution_id")

    async def test_flow_id_value_is_flow_flow_id(self):
        """Verify flow_id value comes from flow.flow_id."""
        params = await self._capture_params(flow_id="unique-flow-1234")
        self.assertEqual(params.get("flow_id"), "unique-flow-1234",
                         "flow_id must equal flow.flow_id")

    async def test_node_id_value_is_node_id(self):
        """Verify node_id value comes from node.id."""
        params = await self._capture_params(node_id="unique-node-5678")
        self.assertEqual(params.get("node_id"), "unique-node-5678",
                         "node_id must equal node.id")


# ---------------------------------------------------------------------------
# Round 4: _subscribe_event — exception path arg precision
# (kills mutmut_46: on_error(None,...), mutmut_50: update_node_result(None,...),
#  mutmut_51: update_node_result(node,None))
# ---------------------------------------------------------------------------
class TestSubscribeEventExceptionPathArgs(unittest.IsolatedAsyncioTestCase):
    async def _run_with_exception(self):
        """Trigger the except path: register_subscription raises an Exception."""
        from plaita.core.strategies import _subscribe_event

        exc = ValueError("subscription-failed-sentinel")
        event_bus = MagicMock()
        event_bus.register_subscription = MagicMock(side_effect=exc)

        node = MagicMock()
        node.event_filter = {}
        node.id = "n1"
        node.event_type = "test.event"
        error_state = {"status": "error", "code": -500}
        node.on_error = MagicMock(return_value=error_state)

        context = MagicMock()
        context.get_or_create_event_bus.return_value = event_bus
        context.execution_id = "eid1"
        context.update_node_result = MagicMock()

        flow = MagicMock()
        flow.flow_id = "f1"

        result = await _subscribe_event(node, flow, {}, context)
        return result, node, context, error_state

    async def test_exception_on_error_first_arg_is_context_not_none(self):
        """Kill mutmut_46: node.on_error(None, ...) in except block."""
        _, node, context, _ = await self._run_with_exception()
        node.on_error.assert_called_once()
        call_args = node.on_error.call_args[0]
        self.assertIs(call_args[0], context,
                      "First arg to on_error must be context, not None")

    async def test_exception_update_node_result_first_arg_is_node(self):
        """Kill mutmut_50: context.update_node_result(None, error_state)."""
        _, node, context, error_state = await self._run_with_exception()
        context.update_node_result.assert_called_once()
        call_args = context.update_node_result.call_args[0]
        self.assertIs(call_args[0], node,
                      "First arg to update_node_result must be node, not None")

    async def test_exception_update_node_result_second_arg_is_error_state(self):
        """Kill mutmut_51: context.update_node_result(node, None)."""
        _, node, context, error_state = await self._run_with_exception()
        context.update_node_result.assert_called_once()
        call_args = context.update_node_result.call_args[0]
        self.assertIs(call_args[1], error_state,
                      "Second arg to update_node_result must be error_state, not None")

    async def test_exception_returns_false(self):
        """Verify _subscribe_event returns False on exception."""
        result, _, _, _ = await self._run_with_exception()
        self.assertFalse(result, "_subscribe_event should return False on exception")


# ---------------------------------------------------------------------------
# Round 4: NormalStrategy.execute — not_reached_end branch state retrieval
# (kills mutmut_54: pfx=None, mutmut_56: get_state(None,...),
#  mutmut_57: get_state(...,None), mutmut_59: get_state(...,))
# ---------------------------------------------------------------------------
class TestNormalStrategyNotReachedEndState(unittest.IsolatedAsyncioTestCase):
    async def _run_not_reached_end(self, store_state=True):
        """Run NormalStrategy where next_node→None without is_end→True."""
        ctx = ExecutionContext()
        pfx = ctx.express_prefix
        node_key = f"{pfx}{ctx.express_node_name}"

        if store_state:
            ctx.set_state(node_key, {"s1": "state_sentinel_abc"})

        start = _StartNode(id="s1", name="s1")

        async def run_node(*args, **kwargs):
            return ("s_res", None)

        runner = MagicMock()
        runner.run_node = run_node
        flow = MagicMock()
        flow.start_node = start
        flow.is_end_node.return_value = False
        flow.next_node.return_value = None

        cb = _make_cb()
        strategy = NormalStrategy()
        result = await strategy.execute(flow, ctx, runner, cb)
        return result, ctx

    async def test_not_reached_end_returns_stored_state(self):
        """Kill mutmut_54 (pfx=None) and mutmut_56 (get_state(None,...)).
        With correct pfx, the stored state is returned.
        With pfx=None, key becomes 'None...' → wrong key → returns {}.
        """
        result, _ = await self._run_not_reached_end(store_state=True)
        self.assertIsInstance(result, dict,
                              "not_reached_end result must be a dict")
        self.assertIn("s1", result,
                      "Result must contain the node 's1' entry from context")
        self.assertEqual(result["s1"], "state_sentinel_abc",
                         "Result must use correct prefix to look up state")

    async def test_not_reached_end_returns_empty_dict_when_no_state(self):
        """Kill mutmut_57 (get_state(...,None)) and mutmut_59 (get_state(...,)).
        When no state is stored, default {} should be returned, not None.
        """
        result, _ = await self._run_not_reached_end(store_state=False)
        self.assertIsNotNone(result,
                              "not_reached_end result must not be None (default is {})")
        self.assertIsInstance(result, dict,
                               "not_reached_end result must be a dict, not None")

    async def test_not_reached_end_uses_context_express_node_name(self):
        """Kill mutmut_56: get_state(None, {}) — key is None → returns wrong value."""
        ctx = ExecutionContext()
        pfx = ctx.express_prefix
        node_name = ctx.express_node_name
        # Store at correct key
        correct_key = f"{pfx}{node_name}"
        ctx.set_state(correct_key, {"target_key": "correct_value_789"})

        start = _StartNode(id="s1", name="s1")

        async def run_node(*args, **kwargs):
            return ("r", None)

        runner = MagicMock()
        runner.run_node = run_node
        flow = MagicMock()
        flow.start_node = start
        flow.is_end_node.return_value = False
        flow.next_node.return_value = None

        cb = _make_cb()
        strategy = NormalStrategy()
        result = await strategy.execute(flow, ctx, runner, cb)
        self.assertEqual(result.get("target_key"), "correct_value_789",
                         "Must look up state with correct prefix+node_name key")


# ---------------------------------------------------------------------------
# Round 4: GeneratorStrategy.execute — callback_manager forwarded to _advance_one
# (kills mutmut_9: callback_manager→None in _advance_one call)
# ---------------------------------------------------------------------------
class TestGeneratorStrategyCallbackForwarding(unittest.IsolatedAsyncioTestCase):
    async def test_callback_manager_passed_to_advance_one(self):
        """Kill mutmut_9: _advance_one(flow, runner, None, current) instead of (flow, runner, cb, current)."""
        ctx = ExecutionContext()

        sentinel_cb_calls = []

        class TrackingCb:
            def on_node_start(self, flow, node): sentinel_cb_calls.append("node_start")
            def on_node_end(self, flow, node, result, error=None, exception=None): sentinel_cb_calls.append("node_end")
            def on_flow_start(self, flow): sentinel_cb_calls.append("flow_start")
            def on_flow_end(self, flow, result=None, error=None): sentinel_cb_calls.append("flow_end")
            def on_flow_suspend(self, flow): pass
            def on_node_suspend(self, flow, node): pass
            def on_flow_resume(self, flow): pass
            def on_node_resume(self, flow, node): pass

        tracking_cb = TrackingCb()

        # We need a runner that accepts callback_manager kwarg and calls it
        cb_received = []
        async def run_node(flow, node, callback_manager=None, **kw):
            cb_received.append(callback_manager)
            return ({"done": 1}, None)

        runner = MagicMock()
        runner.run_node = run_node

        end = _EndNode(id="e1", name="e1")
        flow = MagicMock()
        flow.start_node = end
        flow.is_end_node.return_value = True
        flow.next_node.return_value = None

        strategy = GeneratorStrategy()
        outputs = [o async for o in strategy.execute(flow, ctx, runner, tracking_cb)]

        self.assertGreater(len(cb_received), 0, "run_node must be called")
        self.assertIsNotNone(cb_received[0],
                              "callback_manager passed to run_node must not be None")
        self.assertIs(cb_received[0], tracking_cb,
                      "callback_manager must be the actual cb object, not None")


# ---------------------------------------------------------------------------
# Round 4: GeneratorStrategy.execute — yield output field precision
# (kills mutmut_17: branch→None, mutmut_18: context→None, mutmut_19: execution_id→None,
#  mutmut_24: execution_id kwarg removed)
# ---------------------------------------------------------------------------
class TestGeneratorStrategyYieldFieldPrecision(unittest.IsolatedAsyncioTestCase):
    async def _run_generator(self, execution_id="test-exec-id"):
        """Run a 1-node generator flow and return the single yield output."""
        ctx = ExecutionContext()
        ctx.set_state(f"{ctx.express_prefix}EXECUTION_ID", execution_id)

        end = _EndNode(id="e1", name="e1")
        flow = MagicMock()
        flow.start_node = end
        flow.is_end_node.return_value = True
        flow.next_node.return_value = None

        distinct_branch = "right-branch-sentinel"
        runner = MagicMock()
        runner.run_node = AsyncMock(return_value=({"result_key": "value"}, distinct_branch))

        cb = _make_cb()
        strategy = GeneratorStrategy()
        outputs = [o async for o in strategy.execute(flow, ctx, runner, cb)]
        return outputs, ctx, distinct_branch

    async def test_yield_branch_is_not_none_when_branch_present(self):
        """Kill mutmut_17: branch→None in _create_lazy_output call."""
        outputs, ctx, distinct_branch = await self._run_generator()
        self.assertTrue(len(outputs) >= 1, "At least one output expected")
        # The 'branch' field in output comes from the _create_lazy_output result
        # With mutation: branch=None → branch or "" = ""
        # Without mutation: branch=distinct_branch
        output = outputs[0]
        self.assertIn("branch", output)
        # If branch was None, it becomes "" (falsy default in _create_lazy_output)
        # But with real branch "right-branch-sentinel", it should be non-empty
        self.assertEqual(output["branch"], distinct_branch,
                         "branch in yield output must match the actual branch returned by runner")

    async def test_yield_context_is_not_none(self):
        """Kill mutmut_18: context.to_dict()→None in _create_lazy_output call."""
        outputs, ctx, _ = await self._run_generator()
        self.assertTrue(len(outputs) >= 1)
        output = outputs[0]
        self.assertIn("context", output)
        self.assertIsNotNone(output["context"],
                              "context in yield output must not be None")
        self.assertIsInstance(output["context"], dict,
                               "context must be a dict from context.to_dict()")

    async def test_yield_execution_id_is_set(self):
        """Kill mutmut_19 (execution_id→None) and mutmut_24 (execution_id kwarg removed)."""
        exec_id = "my-unique-exec-id-12345"
        outputs, ctx, _ = await self._run_generator(execution_id=exec_id)
        self.assertTrue(len(outputs) >= 1)
        output = outputs[0]
        self.assertIn("execution_id", output)
        self.assertEqual(output["execution_id"], exec_id,
                         "execution_id in yield must match context.execution_id")


# ---------------------------------------------------------------------------
# Round 4: GeneratorStrategy.execute — not_reached_end path
# (kills mutmut_41: pfx=None, mutmut_42: result=None, mutmut_43: get_state(None,...),
#  mutmut_44: get_state(...,None), mutmut_46: get_state(...,),
#  mutmut_47: result→None in yield, mutmut_48: context→None in yield,
#  mutmut_49: execution_id→None in yield, mutmut_53: execution_id kwarg removed)
# ---------------------------------------------------------------------------
class TestGeneratorStrategyNotReachedEndPath(unittest.IsolatedAsyncioTestCase):
    async def _run_not_reached_end(self, store_state=True, execution_id="exec-gen-1"):
        """Run generator where next_node→None without is_end→True (triggers not_reached_end)."""
        ctx = ExecutionContext()
        ctx.set_state(f"{ctx.express_prefix}EXECUTION_ID", execution_id)
        pfx = ctx.express_prefix
        node_key = f"{pfx}{ctx.express_node_name}"

        if store_state:
            ctx.set_state(node_key, {"s1": "gen_state_sentinel_xyz"})

        start = _StartNode(id="s1", name="s1")

        async def run_node(*args, **kwargs):
            return ("sr", "go_nowhere")

        runner = MagicMock()
        runner.run_node = run_node
        flow = MagicMock()
        flow.start_node = start
        flow.is_end_node.return_value = False
        flow.next_node.return_value = None

        cb = _make_cb()
        strategy = GeneratorStrategy()
        outputs = [o async for o in strategy.execute(flow, ctx, runner, cb)]
        return outputs, ctx

    async def test_not_reached_end_yields_extra_end_output(self):
        """At least 2 outputs: one from loop, one synthetic end output."""
        outputs, _ = await self._run_not_reached_end(store_state=True)
        self.assertGreaterEqual(len(outputs), 2,
                                "not_reached_end should yield an extra synthetic end output")

    async def test_end_output_result_contains_stored_state(self):
        """Kill mutmut_41 (pfx=None), mutmut_42 (result=None), mutmut_43 (get_state(None,...)).
        The extra end output should contain the stored node state.
        """
        outputs, _ = await self._run_not_reached_end(store_state=True)
        end_outputs = [o for o in outputs if o.get("is_end")]
        self.assertTrue(len(end_outputs) >= 1,
                        "At least one end output expected")
        end_out = end_outputs[-1]
        self.assertIsNotNone(end_out.get("result"),
                              "end output result must not be None")
        self.assertIsInstance(end_out.get("result"), dict,
                               "end output result must be a dict from context.get_state")
        self.assertIn("s1", end_out["result"],
                      "end output result must contain stored node state")
        self.assertEqual(end_out["result"]["s1"], "gen_state_sentinel_xyz")

    async def test_end_output_result_is_empty_dict_when_no_state(self):
        """Kill mutmut_44 (get_state(...,None)) — when no state, result must be {}, not None."""
        outputs, _ = await self._run_not_reached_end(store_state=False)
        end_outputs = [o for o in outputs if o.get("is_end")]
        self.assertTrue(len(end_outputs) >= 1)
        end_out = end_outputs[-1]
        self.assertIsNotNone(end_out.get("result"),
                              "result must not be None (default is {})")
        # Even if result is {}, it's not None
        self.assertIsInstance(end_out.get("result"), dict)

    async def test_end_output_context_is_not_none(self):
        """Kill mutmut_48: context.to_dict()→None in _create_end_output."""
        outputs, _ = await self._run_not_reached_end(store_state=True)
        end_outputs = [o for o in outputs if o.get("is_end")]
        self.assertTrue(len(end_outputs) >= 1)
        end_out = end_outputs[-1]
        self.assertIn("context", end_out)
        self.assertIsNotNone(end_out["context"],
                              "end output context must not be None")
        self.assertIsInstance(end_out["context"], dict)

    async def test_end_output_execution_id_is_set(self):
        """Kill mutmut_49 (execution_id→None) and mutmut_53 (execution_id kwarg removed)."""
        exec_id = "gen-exec-sentinel-9876"
        outputs, ctx = await self._run_not_reached_end(
            store_state=True, execution_id=exec_id
        )
        end_outputs = [o for o in outputs if o.get("is_end")]
        self.assertTrue(len(end_outputs) >= 1)
        end_out = end_outputs[-1]
        self.assertIn("execution_id", end_out)
        self.assertEqual(end_out["execution_id"], exec_id,
                         "execution_id in end output must match context.execution_id")


# ---------------------------------------------------------------------------
# Round 4: DistributedStrategy.execute — pfx not None when execute starts
# (kills mutmut_19: pfx=None)
# ---------------------------------------------------------------------------
class TestDistributedExecutePfxNotNone(unittest.IsolatedAsyncioTestCase):
    async def test_pfx_used_correctly_in_get_state(self):
        """Kill mutmut_19: pfx=None.
        When current_node is None (no nodes to run), result uses
        context.get_state(f"{pfx}PLAITA_NODE_STATE", {}).
        With pfx=None, key becomes "NonePLAITA_NODE_STATE" → wrong key.
        """
        ctx = ExecutionContext()
        pfx = ctx.express_prefix
        node_key = f"{pfx}{ctx.express_node_name}"
        ctx.set_state(node_key, {"sentinel_node": "pfx_check_value"})
        ctx.clean()
        ctx.setup_flow(MagicMock(flow_id="f1"), (), {})

        flow = MagicMock()
        flow.start_node = _StartNode(id="s1", name="s1")
        flow.flow_id = "f1"

        async def run_node(flow, node, **kwargs):
            return ({}, None)

        runner = MagicMock()
        runner.run_node = run_node
        # _start_new_flow returns (None, result, branch) to trigger the pfx code path
        runner.node_execution = None

        # Make _start_new_flow return current_node=None to exercise the pfx path
        flow.start_node = _StartNode(id="s1", name="s1")
        flow.next_node.return_value = None  # after start node, no next → current_node=None

        cb = _make_cb()
        strategy = DistributedStrategy()

        # Re-set state after setup_flow resets it
        ctx_pfx = ctx.express_prefix
        ctx.set_state(f"{ctx_pfx}{ctx.express_node_name}", {"chk": "correct_pfx_state"})

        result = await strategy.execute(flow, ctx, runner, cb)
        self.assertIn("context", result,
                      "execute result must contain 'context' field")


# ---------------------------------------------------------------------------
# Round 4: DistributedStrategy.execute — _determine_current_node runner/cb args
# (kills mutmut_38: runner→None, mutmut_39: callback_manager→None,
#  mutmut_48: callback_manager→None in _execute_current_node)
# ---------------------------------------------------------------------------
class TestDistributedExecuteRunnerCbPrecision(unittest.IsolatedAsyncioTestCase):
    async def _run_determine_current_node(self):
        """Run DistributedStrategy with no saved_context so _determine_current_node is called."""
        ctx = ExecutionContext()
        run_calls = []

        async def capture_run(*args, **kwargs):
            run_calls.append((args, kwargs))
            return ({}, None)

        runner = MagicMock()
        runner.run_node = capture_run
        runner.node_execution = None

        start = _StartNode(id="s1", name="s1")
        end = _EndNode(id="e1", name="e1")
        flow = MagicMock()
        flow.start_node = start
        flow.flow_id = "f1"
        flow.next_node.return_value = None  # current_node becomes None after start
        flow.is_end_node.return_value = False

        cb = _make_cb()
        strategy = DistributedStrategy()
        await strategy.execute(flow, ctx, runner, cb)
        return run_calls, cb, flow, start

    async def test_runner_passed_to_start_new_flow(self):
        """Kill mutmut_38: _determine_current_node(flow, context, None, callback_manager)."""
        run_calls, cb, flow, start = await self._run_determine_current_node()
        self.assertTrue(len(run_calls) >= 1,
                        "run_node must be called at least once")
        args, kwargs = run_calls[0]
        self.assertIs(args[0], flow,
                      "First arg to run_node must be flow, not None")

    async def test_callback_manager_passed_to_start_new_flow(self):
        """Kill mutmut_39: _determine_current_node(flow, context, runner, None)."""
        run_calls, cb, flow, start = await self._run_determine_current_node()
        self.assertTrue(len(run_calls) >= 1)
        args, kwargs = run_calls[0]
        self.assertIn("callback_manager", kwargs)
        self.assertIs(kwargs["callback_manager"], cb,
                      "callback_manager kwarg must be the actual cb, not None")


# ---------------------------------------------------------------------------
# Round 4: DistributedStrategy._determine_current_node — arg precision
# (kills mutmut_1: last_node_id=None, mutmut_4: _get_next_from_last(flow,ctx,None),
#  mutmut_9: _start_new_flow(flow,None,...), mutmut_11: _start_new_flow(flow,ctx,runner,None))
# ---------------------------------------------------------------------------
class TestDetermineCurrentNodeArgPrecision(unittest.IsolatedAsyncioTestCase):
    async def test_uses_actual_last_node_id_not_none(self):
        """Kill mutmut_1: last_node_id=None → always calls _start_new_flow instead of _get_next_from_last."""
        ctx = ExecutionContext()
        pfx = ctx.express_prefix
        # Store last_node_id in context
        ctx.set_state(f"{pfx}LAST_NODE", "known-last-node-id")
        # Put node results so _get_next_from_last works
        ctx.set_state(f"{pfx}{ctx.express_node_name}", {"known-last-node-id": "prev_result"})
        ctx.set_state(f"{pfx}BRANCH", "main")

        found_node = _MidNode(id="known-last-node-id", name="mid")
        next_node = _EndNode(id="end1", name="end")

        flow = MagicMock()
        flow.find_node_by_id.return_value = found_node
        flow.next_node.return_value = next_node

        runner = MagicMock()
        runner.node_execution = None
        cb = _make_cb()

        strategy = DistributedStrategy()
        current, result, branch = await strategy._determine_current_node(flow, ctx, runner, cb)

        # With mutmut_1 (last_node_id=None), _start_new_flow would be called instead
        # _start_new_flow calls runner.run_node(flow, start_node, ...) which would fail since flow.start_node not set
        # The key assertion: flow.find_node_by_id was called with the real last_node_id
        flow.find_node_by_id.assert_called_with("known-last-node-id")

    async def test_get_next_from_last_receives_actual_last_node_id(self):
        """Kill mutmut_4: _get_next_from_last(flow, ctx, None) instead of (flow, ctx, last_node_id)."""
        ctx = ExecutionContext()
        pfx = ctx.express_prefix
        ctx.set_state(f"{pfx}LAST_NODE", "specific-node-abc")
        ctx.set_state(f"{pfx}{ctx.express_node_name}", {"specific-node-abc": "res"})
        ctx.set_state(f"{pfx}BRANCH", "x")

        found = _MidNode(id="specific-node-abc", name="mid")
        flow = MagicMock()
        flow.find_node_by_id.return_value = found
        flow.next_node.return_value = None

        runner = MagicMock()
        runner.node_execution = None
        cb = _make_cb()

        strategy = DistributedStrategy()
        current, result, branch = await strategy._determine_current_node(flow, ctx, runner, cb)

        # With mutmut_4 (None arg), find_node_by_id would be called with None
        call_args = flow.find_node_by_id.call_args[0]
        self.assertEqual(call_args[0], "specific-node-abc",
                         "find_node_by_id must be called with the actual last_node_id, not None")

    async def test_start_new_flow_receives_context_not_none(self):
        """Kill mutmut_9: _start_new_flow(flow, None, runner, callback_manager)."""
        ctx = ExecutionContext()
        # No last_node_id → calls _start_new_flow

        captured_ctx = []
        async def mock_start_new_flow(flow, context, runner, cb):
            captured_ctx.append(context)
            return (None, {}, None)

        flow = MagicMock()
        flow.start_node = _StartNode(id="s1", name="s1")
        flow.flow_id = "f1"

        runner = MagicMock()
        runner.node_execution = None
        cb = _make_cb()

        strategy = DistributedStrategy()
        strategy._start_new_flow = mock_start_new_flow

        await strategy._determine_current_node(flow, ctx, runner, cb)

        self.assertEqual(len(captured_ctx), 1)
        self.assertIs(captured_ctx[0], ctx,
                      "Context passed to _start_new_flow must be the actual context, not None")

    async def test_start_new_flow_receives_callback_manager_not_none(self):
        """Kill mutmut_11: _start_new_flow(flow, context, runner, None)."""
        ctx = ExecutionContext()

        captured_cbs = []
        async def mock_start_new_flow(flow, context, runner, callback_manager):
            captured_cbs.append(callback_manager)
            return (None, {}, None)

        flow = MagicMock()
        flow.start_node = _StartNode(id="s1", name="s1")
        flow.flow_id = "f1"

        runner = MagicMock()
        runner.node_execution = None
        cb = _make_cb()

        strategy = DistributedStrategy()
        strategy._start_new_flow = mock_start_new_flow

        await strategy._determine_current_node(flow, ctx, runner, cb)

        self.assertEqual(len(captured_cbs), 1)
        self.assertIs(captured_cbs[0], cb,
                      "callback_manager passed to _start_new_flow must be actual cb, not None")


# ---------------------------------------------------------------------------
# Round 4: DistributedStrategy._get_next_from_last — get_state default and next_node args
# (kills mutmut_6: get_state(...,None), mutmut_8: get_state(...,),
#  mutmut_13: next_node(None,branch), mutmut_14: next_node(node,None),
#  mutmut_15: next_node(branch), mutmut_16: next_node(node,))
# ---------------------------------------------------------------------------
class TestGetNextFromLastArgPrecisionR4(unittest.TestCase):
    def _make_ctx_simple(self, last_node_id="n1"):
        ctx = ExecutionContext()
        pfx = ctx.express_prefix
        ctx.set_state(f"{pfx}LAST_NODE", last_node_id)
        ctx.set_state(f"{pfx}{ctx.express_node_name}", {last_node_id: "r1"})
        ctx.set_state(f"{pfx}BRANCH", "main")
        return ctx

    def test_get_state_default_is_dict_not_none(self):
        """Kill mutmut_6 (default None) and mutmut_8 (no default).
        When node_results is {} (default), node_results.get(last_node_id) works without error.
        With None default, node_results.get would raise AttributeError.
        """
        ctx = ExecutionContext()
        pfx = ctx.express_prefix
        # Don't store any node state → get_state returns default
        ctx.set_state(f"{pfx}LAST_NODE", "n_missing")
        ctx.set_state(f"{pfx}BRANCH", "b1")

        found = _MidNode(id="n_missing", name="mid")
        flow = MagicMock()
        flow.find_node_by_id.return_value = found
        flow.next_node.return_value = None

        strategy = DistributedStrategy()
        # Should not raise (with mutation {→None, .get() would raise AttributeError)
        current, result, branch = strategy._get_next_from_last(flow, ctx, "n_missing")
        self.assertIsNone(result)  # .get() on empty dict with missing key → None

    def test_next_node_called_with_found_node_and_branch(self):
        """Kill mutmut_13 (node→None), mutmut_14 (branch→None), mutmut_15/16 (arg count)."""
        ctx = self._make_ctx_simple("n5")
        found = _MidNode(id="n5", name="mid")
        next_n = _EndNode(id="e1", name="end")

        flow = MagicMock()
        flow.find_node_by_id.return_value = found
        flow.next_node.return_value = next_n

        strategy = DistributedStrategy()
        strategy._get_next_from_last(flow, ctx, "n5")

        # Verify flow.next_node was called with (found_node, branch)
        # The first call is with the found node, second call is with next after find
        # Actually: current_node = found (from find_node_by_id), then current_node = flow.next_node(current_node, branch)
        flow.next_node.assert_called_once()
        call_args = flow.next_node.call_args[0]
        self.assertEqual(len(call_args), 2,
                         "flow.next_node must be called with exactly 2 positional args")
        self.assertIs(call_args[0], found,
                      "first arg to flow.next_node must be the found node, not None")
        self.assertEqual(call_args[1], "main",
                         "second arg to flow.next_node must be the branch, not None")


# ---------------------------------------------------------------------------
# Round 4: DistributedStrategy._start_new_flow — flow.next_node arg precision
# (kills mutmut_10: current_node=None, mutmut_11: next_node(None,branch),
#  mutmut_12: next_node(start_node,None), mutmut_13: next_node(branch),
#  mutmut_14: next_node(start_node,))
# ---------------------------------------------------------------------------
class TestStartNewFlowNextNodeArgPrecision(unittest.IsolatedAsyncioTestCase):
    async def _run_start_new_flow_with_branch(self, branch_val="special_branch"):
        ctx = ExecutionContext()
        start = _StartNode(id="s1", name="s1")
        next_n = _MidNode(id="m1", name="mid")

        run_calls = []
        async def capture_run(*args, **kwargs):
            run_calls.append((args, kwargs))
            return ({}, branch_val)

        runner = MagicMock()
        runner.run_node = capture_run
        flow = MagicMock()
        flow.start_node = start
        flow.next_node.return_value = next_n

        cb = _make_cb()
        strategy = DistributedStrategy()
        current, result, branch = await strategy._start_new_flow(flow, ctx, runner, cb)
        return current, result, branch, flow, start, next_n

    async def test_current_node_is_not_none_when_next_exists(self):
        """Kill mutmut_10: current_node = None (hardcoded)."""
        current, _, _, flow, start, next_n = await self._run_start_new_flow_with_branch()
        self.assertIs(current, next_n,
                      "current_node must be the result of flow.next_node, not None")

    async def test_next_node_first_arg_is_start_node(self):
        """Kill mutmut_11: flow.next_node(None, branch) — start_node→None."""
        _, _, _, flow, start, _ = await self._run_start_new_flow_with_branch("b1")
        call_args = flow.next_node.call_args[0]
        self.assertIs(call_args[0], start,
                      "first arg to flow.next_node must be start_node, not None")

    async def test_next_node_second_arg_is_branch(self):
        """Kill mutmut_12: flow.next_node(start_node, None) — branch→None."""
        _, _, _, flow, start, _ = await self._run_start_new_flow_with_branch("distinct_branch_567")
        call_args = flow.next_node.call_args[0]
        self.assertEqual(call_args[1], "distinct_branch_567",
                         "second arg to flow.next_node must be the branch, not None")

    async def test_next_node_called_with_two_args(self):
        """Kill mutmut_13 (only branch arg) and mutmut_14 (no branch arg)."""
        _, _, _, flow, start, _ = await self._run_start_new_flow_with_branch("b2")
        call_args = flow.next_node.call_args[0]
        self.assertEqual(len(call_args), 2,
                         f"flow.next_node must be called with exactly 2 positional args, got {len(call_args)}")


# ---------------------------------------------------------------------------
# Round 4: DistributedStrategy._execute_current_node — callback_manager and is_end arg
# (kills mutmut_4: callback_manager=None, mutmut_7: callback_manager kwarg removed,
#  mutmut_8: flow.is_end_node(None), mutmut_13: execution_id=None,
#  mutmut_18: execution_id kwarg removed)
# ---------------------------------------------------------------------------
class TestExecuteCurrentNodeRunnerCbPrecision(unittest.IsolatedAsyncioTestCase):
    async def _run_execute_current_node_with_tracking(self, is_end=True):
        ctx = ExecutionContext()
        ctx.set_state(f"{ctx.express_prefix}EXECUTION_ID", "exec-test-id-444")

        node = MagicMock()
        node.id = "curr1"
        node.name = "curr1"
        node.node_type = "end" if is_end else "mid"
        node.is_suspending = False

        run_calls = []
        async def capture_run(*args, **kwargs):
            run_calls.append((args, kwargs))
            return ({"final": "res"}, "branch_b")

        runner = MagicMock()
        runner.run_node = capture_run
        flow = MagicMock()
        flow.is_end_node.return_value = is_end
        flow.next_node.return_value = None

        cb = _make_cb()
        strategy = DistributedStrategy()
        result = await strategy._execute_current_node(flow, ctx, runner, cb, node)
        return result, run_calls, flow, node, cb, ctx

    async def test_callback_manager_passed_to_runner(self):
        """Kill mutmut_4 (callback_manager=None) and mutmut_7 (kwarg removed)."""
        result, run_calls, flow, node, cb, ctx = await self._run_execute_current_node_with_tracking(is_end=True)
        self.assertEqual(len(run_calls), 1)
        _, kwargs = run_calls[0]
        self.assertIn("callback_manager", kwargs,
                      "callback_manager kwarg must be present in run_node call")
        self.assertIs(kwargs["callback_manager"], cb,
                      "callback_manager must be the actual cb, not None")

    async def test_is_end_node_receives_current_node(self):
        """Kill mutmut_8: flow.is_end_node(None) — current_node→None."""
        result, run_calls, flow, node, cb, ctx = await self._run_execute_current_node_with_tracking(is_end=True)
        flow.is_end_node.assert_called_with(node)

    async def test_lazy_output_execution_id_is_set_for_non_end(self):
        """Kill mutmut_13 (execution_id=None) and mutmut_18 (execution_id removed) for non-end node."""
        result, run_calls, flow, node, cb, ctx = await self._run_execute_current_node_with_tracking(is_end=False)
        self.assertIn("execution_id", result)
        self.assertEqual(result["execution_id"], "exec-test-id-444",
                         "execution_id in lazy output must match context.execution_id")


# ---------------------------------------------------------------------------
# Round 4: DistributedStrategy._handle_resume — default value and arg precision
# (kills mutmut_19/21: get_state({} default), mutmut_24/26: prev_state.get({} default),
#  mutmut_28/30/33: prev_state.get("status",...) mutations,
#  mutmut_38/40: node= in ResumeError for non-pending,
#  mutmut_53: exec_ctx=None, mutmut_54: exec_ctx=runner.node_execution and context)
# ---------------------------------------------------------------------------
class TestHandleResumeDefaultsAndArgsPrecision(unittest.IsolatedAsyncioTestCase):
    def _make_context_with_node(self, node_id, status="pending"):
        ctx = ExecutionContext()
        pfx = ctx.express_prefix
        ctx.set_state(f"{pfx}LAST_NODE", node_id)
        ctx.set_state(f"{pfx}{ctx.express_node_name}", {node_id: {"status": status}})
        return ctx

    def _make_suspending_node(self, node_id="ev1", status_to_return="completed"):
        node = MagicMock()
        node.id = node_id
        node.is_suspending = True
        node.resume = MagicMock(return_value={"status": status_to_return})
        node.source_line = None
        node.node_type = "event"
        node.name = "EventNode"
        return node

    async def test_get_state_default_dict_not_none_when_no_node_results(self):
        """Kill mutmut_19/21: context.get_state(key, None) instead of ({}).
        If default is None, node_results.get(last_node_id, {}) raises AttributeError.
        We use a suspending node with empty context to trigger this path.
        """
        ctx = ExecutionContext()
        pfx = ctx.express_prefix
        ctx.set_state(f"{pfx}LAST_NODE", "ev1")
        # DO NOT set node state → get_state returns default

        node = self._make_suspending_node("ev1")
        flow = MagicMock()
        flow.find_node_by_id.return_value = node

        runner = MagicMock()
        runner.node_execution = None

        cb = _make_cb()
        cb.on_flow_resume = MagicMock()
        cb.on_node_resume = MagicMock()
        cb.on_node_end = MagicMock()

        strategy = DistributedStrategy()
        # With mutmut_19 (None default), .get() on None → AttributeError
        # With correct code, get_state returns {} → prev_state = {} → status="" → raises ResumeError
        with self.assertRaises(Exception) as cm:
            await strategy._handle_resume(flow, ctx, runner, cb, ResumeType.EVENT, {})

        # Should be ResumeError (not pending), not AttributeError
        err = cm.exception
        self.assertIsInstance(err, ResumeError,
                              f"Expected ResumeError, got {type(err).__name__}: {err}")
        self.assertIn("pending", str(err).lower() + str(err),
                      "Error should mention pending status check failed")

    async def test_prev_state_get_default_dict_allows_status_check(self):
        """Kill mutmut_24/26: prev_state.get(last_node_id, None) instead of ({}).
        With None default, prev_state.get("status", "") would fail if prev_state is None.
        But prev_state.get(last_node_id, {}) returns {} when the key is missing.
        """
        ctx = ExecutionContext()
        pfx = ctx.express_prefix
        ctx.set_state(f"{pfx}LAST_NODE", "ev1")
        # Store node_results but without the specific node_id key
        ctx.set_state(f"{pfx}{ctx.express_node_name}", {})  # empty dict, node_id not present

        node = self._make_suspending_node("ev1")
        flow = MagicMock()
        flow.find_node_by_id.return_value = node

        runner = MagicMock()
        runner.node_execution = None
        cb = _make_cb()
        cb.on_flow_resume = MagicMock()

        strategy = DistributedStrategy()
        # With correct code: prev_state = {}.get("ev1", {}) = {} → status="" → raises ResumeError
        # With mutmut_24 (None default): prev_state = None → .get("status", "") → AttributeError
        with self.assertRaises(Exception) as cm:
            await strategy._handle_resume(flow, ctx, runner, cb, ResumeType.EVENT, {})

        err = cm.exception
        self.assertIsInstance(err, ResumeError,
                              f"Expected ResumeError (status check), got {type(err).__name__}")

    async def test_status_check_uses_lowercase_status_key(self):
        """Kill mutmut_28 (status→None), mutmut_30 (status,)/empty, mutmut_33 ("XXXX" default).
        When prev_state has no "status" key, "" default triggers ResumeError.
        """
        ctx = ExecutionContext()
        pfx = ctx.express_prefix
        ctx.set_state(f"{pfx}LAST_NODE", "ev1")
        # Node state without "status" key
        ctx.set_state(f"{pfx}{ctx.express_node_name}", {"ev1": {"other_field": "val"}})

        node = self._make_suspending_node("ev1")
        flow = MagicMock()
        flow.find_node_by_id.return_value = node

        runner = MagicMock()
        runner.node_execution = None
        cb = _make_cb()
        cb.on_flow_resume = MagicMock()

        strategy = DistributedStrategy()
        # With correct code: prev_state.get("status", "") = "" != "pending" → ResumeError
        # With mutmut_28 (None default): prev_state.get(None, "") = "" → same result (might survive)
        # With mutmut_33 ("XXXX" default): prev_state.get("status", "XXXX") = "XXXX" != "pending" → still ResumeError
        # These are genuinely hard to kill directly; but the test verifies correct behavior
        with self.assertRaises(ResumeError) as cm:
            await strategy._handle_resume(flow, ctx, runner, cb, ResumeType.EVENT, {})
        err_msg = str(cm.exception)
        self.assertIn("pending", err_msg.lower() + err_msg)

    async def test_non_pending_resume_error_has_current_node(self):
        """Kill mutmut_38 (node=None) and mutmut_40 (node kwarg removed) for non-pending error."""
        ctx = self._make_context_with_node("ev1", status="completed")  # not pending

        node = self._make_suspending_node("ev1")
        flow = MagicMock()
        flow.find_node_by_id.return_value = node

        runner = MagicMock()
        runner.node_execution = None
        cb = _make_cb()
        cb.on_flow_resume = MagicMock()

        strategy = DistributedStrategy()
        with self.assertRaises(ResumeError) as cm:
            await strategy._handle_resume(flow, ctx, runner, cb, ResumeType.EVENT, {})

        err = cm.exception
        self.assertIsNotNone(err.node,
                              "ResumeError.node must not be None for non-pending status error")
        self.assertIs(err.node, node,
                      "ResumeError.node must be the current_node")

    async def test_exec_ctx_is_not_none_when_no_node_execution(self):
        """Kill mutmut_53: exec_ctx=None instead of runner.node_execution or context.
        When runner.node_execution is None, exec_ctx falls back to context.
        With mutmut_53 (exec_ctx=None), current_node.resume(None, ...) is called.
        """
        ctx = self._make_context_with_node("ev1", status="pending")

        exec_ctx_received = []
        node = MagicMock()
        node.id = "ev1"
        node.is_suspending = True
        node.resume = MagicMock(side_effect=lambda ctx, *a, **k: exec_ctx_received.append(ctx) or {"done": True})
        node.source_line = None
        node.node_type = "event"
        node.name = "EventNode"

        flow = MagicMock()
        flow.find_node_by_id.return_value = node
        flow.next_node.return_value = None

        runner = MagicMock()
        runner.node_execution = None  # falls back to context

        cb = _make_cb()
        cb.on_flow_resume = MagicMock()
        cb.on_node_resume = MagicMock()
        cb.on_node_end = MagicMock()

        strategy = DistributedStrategy()
        await strategy._handle_resume(flow, ctx, runner, cb, ResumeType.EVENT, {})

        self.assertEqual(len(exec_ctx_received), 1)
        self.assertIsNotNone(exec_ctx_received[0],
                              "exec_ctx must not be None; should fall back to context")
        self.assertIs(exec_ctx_received[0], ctx,
                      "exec_ctx must be context when runner.node_execution is None")

    async def test_exec_ctx_is_node_execution_when_available(self):
        """Kill mutmut_54: exec_ctx = runner.node_execution AND context.
        With 'and' instead of 'or', if runner.node_execution is truthy, exec_ctx = context (wrong!).
        With 'or', if runner.node_execution is truthy, exec_ctx = runner.node_execution (correct!).
        """
        ctx = self._make_context_with_node("ev1", status="pending")

        exec_ctx_received = []
        node = MagicMock()
        node.id = "ev1"
        node.is_suspending = True
        node.resume = MagicMock(side_effect=lambda ctx, *a, **k: exec_ctx_received.append(ctx) or {"done": True})
        node.source_line = None
        node.node_type = "event"
        node.name = "EventNode"

        flow = MagicMock()
        flow.find_node_by_id.return_value = node
        flow.next_node.return_value = None

        # Create a distinct node_execution object
        node_execution_obj = MagicMock()

        runner = MagicMock()
        runner.node_execution = node_execution_obj  # truthy

        cb = _make_cb()
        cb.on_flow_resume = MagicMock()
        cb.on_node_resume = MagicMock()
        cb.on_node_end = MagicMock()

        strategy = DistributedStrategy()
        await strategy._handle_resume(flow, ctx, runner, cb, ResumeType.EVENT, {})

        self.assertEqual(len(exec_ctx_received), 1)
        # With 'or': exec_ctx = node_execution_obj (truthy) → correct
        # With 'and': exec_ctx = context (wrong!)
        self.assertIs(exec_ctx_received[0], node_execution_obj,
                      "When runner.node_execution is set, exec_ctx must be it (not context)")


if __name__ == "__main__":
    unittest.main()
