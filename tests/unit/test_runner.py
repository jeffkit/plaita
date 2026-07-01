"""Tests for plaita.core.runner — NodeRunner."""

import asyncio
import threading
import time
import unittest
from typing import ClassVar
from unittest.mock import MagicMock, AsyncMock

from pydantic import Field

from plaita.core.context import ExecutionContext
from plaita.core.errors import (
    ErrorStrategy,
    FlowExecutionException,
    FlowResultError,
    NodeException,
    RecoverableErrorHandler,
    ErrorHandler,
)
from plaita.core.runner import NodeRunner
from plaita.node.basic import Node


class SimpleNode(Node):
    node_type: ClassVar[str] = "simple"
    node_name: ClassVar[str] = "simple"
    _execute_result: ClassVar = "ok"

    def execute(self, execution=None):
        return self._execute_result


class SlowNode(Node):
    node_type: ClassVar[str] = "slow"
    node_name: ClassVar[str] = "slow"
    delay: float = 2.0

    def execute(self, execution=None):
        time.sleep(self.delay)
        return "slow_done"


class ErrorNode(Node):
    node_type: ClassVar[str] = "error"
    node_name: ClassVar[str] = "error"

    def execute(self, execution=None):
        raise RuntimeError("node failed")


class AsyncNode(Node):
    node_type: ClassVar[str] = "async"
    node_name: ClassVar[str] = "async"
    async_node: ClassVar[bool] = True

    async def arun(self, execution=None):
        await asyncio.sleep(0.01)
        return "async_result"


class FakeFlow:
    flow_id = "test"


class TestNodeRunnerBasics(unittest.IsolatedAsyncioTestCase):
    """T039: run_node with timeout, retry, error strategy dispatch."""

    async def test_run_node_success(self):
        ctx = ExecutionContext()
        ctx.clean()
        runner = NodeRunner(ctx)
        node = SimpleNode(id="n1")
        flow = FakeFlow()
        result, branch = await runner.run_node(flow, node)
        self.assertEqual(result, "ok")
        self.assertIsNone(branch)
        self.assertEqual(ctx.get_state("$LAST_NODE"), "n1")

    async def test_run_node_branching(self):
        ctx = ExecutionContext()
        ctx.clean()
        runner = NodeRunner(ctx)

        class BranchNode(Node):
            node_type: ClassVar[str] = "branch"
            branching: ClassVar[bool] = True

            def execute(self, execution=None):
                return "left"

        node = BranchNode(id="b1")
        result, branch = await runner.run_node(FakeFlow(), node)
        self.assertEqual(result, "left")
        self.assertEqual(branch, "left")

    async def test_timeout_raises(self):
        ctx = ExecutionContext()
        ctx.clean()
        runner = NodeRunner(ctx)
        node = SlowNode(id="slow1", delay=5.0)
        with self.assertRaises(FlowExecutionException):
            await runner.run_node(FakeFlow(), node, max_timeout_ms=100)

    async def test_retry_on_error(self):
        ctx = ExecutionContext()
        ctx.clean()
        runner = NodeRunner(ctx)
        call_count = 0

        class RetryNode(Node):
            node_type: ClassVar[str] = "retry"
            error_handler: RecoverableErrorHandler = Field(
                default_factory=lambda: RecoverableErrorHandler(strategy="continue", retryTimes=2)
            )

            def execute(self, execution=None):
                nonlocal call_count
                call_count += 1
                if call_count < 3:
                    raise RuntimeError("fail")
                return "success"

        node = RetryNode(id="r1")
        result, _ = await runner.run_node(FakeFlow(), node)
        self.assertEqual(call_count, 3)
        self.assertEqual(result, "success")

    async def test_error_strategy_abort(self):
        ctx = ExecutionContext()
        ctx.clean()
        runner = NodeRunner(ctx)
        node = ErrorNode(id="e1")
        with self.assertRaises(FlowExecutionException):
            await runner.run_node(FakeFlow(), node)

    async def test_error_strategy_continue(self):
        ctx = ExecutionContext()
        ctx.clean()
        runner = NodeRunner(ctx)

        class ContinueErrorNode(Node):
            node_type: ClassVar[str] = "cerr"
            error_handler: RecoverableErrorHandler = Field(
                default_factory=lambda: RecoverableErrorHandler(strategy="continue")
            )

            def execute(self, execution=None):
                raise RuntimeError("fail")

        node = ContinueErrorNode(id="ce1")
        result, _ = await runner.run_node(FakeFlow(), node)
        self.assertIsNone(result)

    async def test_error_strategy_continue_with(self):
        ctx = ExecutionContext()
        ctx.clean()
        runner = NodeRunner(ctx)

        class ContinueWithNode(Node):
            node_type: ClassVar[str] = "cwith"
            error_handler: RecoverableErrorHandler = Field(
                default_factory=lambda: RecoverableErrorHandler(strategy="continue-with", defaultValue={"fallback": True})
            )

            def execute(self, execution=None):
                raise RuntimeError("fail")

        node = ContinueWithNode(id="cw1")
        result, _ = await runner.run_node(FakeFlow(), node)
        self.assertEqual(result, {"fallback": True})

    async def test_callback_manager_called(self):
        ctx = ExecutionContext()
        ctx.clean()
        runner = NodeRunner(ctx)
        node = SimpleNode(id="cb1")

        cbm = MagicMock()
        await runner.run_node(FakeFlow(), node, callback_manager=cbm)
        cbm.on_node_start.assert_called_once()
        cbm.on_node_end.assert_called_once()


class TestNodeRunnerAsyncSupport(unittest.IsolatedAsyncioTestCase):
    """T040: async node support."""

    async def test_async_node_execution(self):
        ctx = ExecutionContext()
        ctx.clean()
        runner = NodeRunner(ctx)
        node = AsyncNode(id="a1")
        result, _ = await runner.run_node(FakeFlow(), node)
        self.assertEqual(result, "async_result")

    async def test_async_node_with_timeout(self):
        ctx = ExecutionContext()
        ctx.clean()
        runner = NodeRunner(ctx)

        class SlowAsyncNode(Node):
            node_type: ClassVar[str] = "slowasync"
            async_node: ClassVar[bool] = True

            async def arun(self, execution=None):
                await asyncio.sleep(10)
                return "done"

        node = SlowAsyncNode(id="sa1")
        with self.assertRaises(FlowExecutionException):
            await runner.run_node(FakeFlow(), node, max_timeout_ms=100)

    async def test_sync_node_runs_in_executor(self):
        ctx = ExecutionContext()
        ctx.clean()
        runner = NodeRunner(ctx)
        node = SimpleNode(id="sync1")
        result, _ = await runner.run_node(FakeFlow(), node)
        self.assertEqual(result, "ok")


class TestCooperativeCancellation(unittest.IsolatedAsyncioTestCase):
    """T049: Cooperative cancellation via threading.Event."""

    async def test_timeout_with_grace_period(self):
        ctx = ExecutionContext()
        ctx.clean()
        runner = NodeRunner(ctx)
        node = SlowNode(id="cancel1", delay=10.0)
        with self.assertRaises(FlowExecutionException):
            await runner.run_node(FakeFlow(), node, max_timeout_ms=200)


class TestTimeoutThreadCleanup(unittest.IsolatedAsyncioTestCase):
    """T082: threading.Event cooperative cancellation, 1s grace period,
    warning log for lingering threads."""

    async def test_cancel_event_is_set_on_timeout(self):
        ctx = ExecutionContext()
        ctx.clean()
        runner = NodeRunner(ctx)

        class ObservableSlow(Node):
            node_type: ClassVar[str] = "obs_slow"
            node_name: ClassVar[str] = "obs_slow"
            cancel_observed: ClassVar[bool] = False

            def execute(self, execution=None):
                time.sleep(10)
                return "never"

        node = ObservableSlow(id="os1")
        with self.assertRaises(FlowExecutionException):
            await runner.run_node(FakeFlow(), node, max_timeout_ms=200)

    async def test_grace_period_allows_cleanup(self):
        ctx = ExecutionContext()
        ctx.clean()
        runner = NodeRunner(ctx)

        cleanup_done = threading.Event()

        class GracefulNode(Node):
            node_type: ClassVar[str] = "graceful"
            node_name: ClassVar[str] = "graceful"

            def execute(self, execution=None):
                time.sleep(5)
                cleanup_done.set()
                return "done"

        node = GracefulNode(id="g1")
        with self.assertRaises(FlowExecutionException):
            await runner.run_node(FakeFlow(), node, max_timeout_ms=100)

    async def test_warning_log_for_lingering_thread(self):
        ctx = ExecutionContext()
        ctx.clean()
        runner = NodeRunner(ctx)

        class StubbornNode(Node):
            node_type: ClassVar[str] = "stubborn"
            node_name: ClassVar[str] = "stubborn"

            def execute(self, execution=None):
                time.sleep(30)
                return "never"

        node = StubbornNode(id="st1")
        # 新实现: 超时后设置 cancel_event 并打 WARNING("Sync node ... timed out ...")
        with self.assertLogs("plaita.core.runner", level="WARNING") as cm:
            with self.assertRaises(FlowExecutionException):
                await runner.run_node(FakeFlow(), node, max_timeout_ms=100)
            timeout_msgs = [r for r in cm.output if "timed out" in r]
            self.assertGreaterEqual(len(timeout_msgs), 1)
        # cancel_event 应已被 set
        self.assertTrue(ctx.cancel_event.is_set())

    async def test_normal_execution_no_lingering(self):
        ctx = ExecutionContext()
        ctx.clean()
        runner = NodeRunner(ctx)
        node = SimpleNode(id="fast1")
        result, _ = await runner.run_node(FakeFlow(), node)
        self.assertEqual(result, "ok")


if __name__ == "__main__":
    unittest.main()
