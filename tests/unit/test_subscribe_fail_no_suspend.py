"""订阅失败时禁止挂起：避免 suspended + 无 subscription 的僵尸执行。"""
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from plaita.core.context import ExecutionContext
from plaita.core.errors import FlowExecutionException
from plaita.core.strategies import DistributedStrategy


def _make_cb():
    cb = MagicMock()
    cb.on_node_suspend = MagicMock()
    cb.on_flow_suspend = MagicMock()
    return cb


class TestSubscribeFailRefusesSuspend(unittest.IsolatedAsyncioTestCase):
    async def test_no_bus_raises_and_does_not_suspend(self):
        ctx = ExecutionContext()
        node = MagicMock()
        node.id = "evt1"
        node.node_type = "event"
        node.name = "E"
        node.is_suspending = True
        node.on_error = MagicMock(return_value={"status": "error"})

        async def run_node(*args, **kwargs):
            return ({"status": "pending", "event_type": "x"}, None)

        runner = MagicMock()
        runner.run_node = run_node
        flow = MagicMock()
        flow.is_end_node.return_value = False
        flow.flow_id = "f1"

        cb = _make_cb()
        strategy = DistributedStrategy()

        with self.assertRaises(FlowExecutionException) as cm:
            await strategy._execute_current_node(flow, ctx, runner, cb, node)

        self.assertIn("subscription failed", str(cm.exception).lower())
        cb.on_flow_suspend.assert_not_called()
        cb.on_node_suspend.assert_not_called()

    async def test_register_raises_refuses_suspend(self):
        bus = MagicMock()
        bus.register_subscription = MagicMock(side_effect=RuntimeError("bus down"))
        ctx = ExecutionContext(event_bus=bus)

        node = MagicMock()
        node.id = "evt1"
        node.node_type = "event"
        node.name = "E"
        node.is_suspending = True
        node.event_type = "t"
        node.event_filter = None
        node.on_error = MagicMock(return_value={"status": "error"})

        async def run_node(*args, **kwargs):
            return ({"status": "pending", "event_type": "t"}, None)

        runner = MagicMock()
        runner.run_node = run_node
        flow = MagicMock()
        flow.is_end_node.return_value = False
        flow.flow_id = "f1"

        cb = _make_cb()
        strategy = DistributedStrategy()

        with self.assertRaises(FlowExecutionException):
            await strategy._execute_current_node(flow, ctx, runner, cb, node)

        cb.on_flow_suspend.assert_not_called()

    async def test_successful_subscribe_still_suspends(self):
        bus = MagicMock()
        bus.register_subscription = MagicMock(return_value="sub-1")
        ctx = ExecutionContext(event_bus=bus)
        ctx.set_state(f"{ctx.express_prefix}EXECUTION_ID", "exec-1")

        node = MagicMock()
        node.id = "evt1"
        node.node_type = "event"
        node.name = "E"
        node.is_suspending = True
        node.event_type = "t"
        node.event_filter = None

        async def run_node(*args, **kwargs):
            return ({"status": "pending", "event_type": "t"}, None)

        runner = MagicMock()
        runner.run_node = run_node
        flow = MagicMock()
        flow.is_end_node.return_value = False
        flow.flow_id = "f1"

        cb = _make_cb()
        strategy = DistributedStrategy()
        out = await strategy._execute_current_node(flow, ctx, runner, cb, node)

        self.assertTrue(out["is_suspend"])
        cb.on_flow_suspend.assert_called_once()


if __name__ == "__main__":
    unittest.main()
