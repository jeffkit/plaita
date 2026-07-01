"""Tests for plaita.core.callback — FlowCallback, CallbackManager."""

import unittest
from unittest.mock import MagicMock

from plaita.core.callback import (
    FlowCallback,
    CallbackManager,
    LoggerCallback,
    FlowEvent,
    BaseCallbackManager,
)


class FakeFlow:
    flow_id = "test"


class FakeNode:
    id = "n1"


class TestFlowCallbackDefaults(unittest.TestCase):
    """T045: FlowCallback default no-op implementations."""

    def test_default_no_op_methods(self):
        cb = FlowCallback()
        flow = FakeFlow()
        node = FakeNode()
        cb.on_flow_start(flow)
        cb.on_flow_end(flow, result="ok")
        cb.on_flow_suspend(flow)
        cb.on_flow_resume(flow)
        cb.on_node_start(flow, node)
        cb.on_node_end(flow, node, result="ok")
        cb.on_node_suspend(flow, node)
        cb.on_node_resume(flow, node)

    def test_subclass_override_single_hook(self):
        class MyCallback(FlowCallback):
            def __init__(self):
                self.ended = False

            def on_flow_end(self, flow, result=None, error=None, exception=None, **kwargs):
                self.ended = True

        cb = MyCallback()
        cb.on_flow_start(FakeFlow())
        cb.on_flow_end(FakeFlow(), result="done")
        self.assertTrue(cb.ended)

    def test_not_abstract(self):
        cb = FlowCallback()
        self.assertIsInstance(cb, FlowCallback)


class TestCallbackManager(unittest.TestCase):
    """T045: CallbackManager multi-dispatch."""

    def test_dispatch_to_multiple_handlers(self):
        h1 = MagicMock(spec=FlowCallback)
        h2 = MagicMock(spec=FlowCallback)
        mgr = CallbackManager([h1, h2])
        mgr.on_flow_start(FakeFlow())
        h1.on_flow_start.assert_called_once()
        h2.on_flow_start.assert_called_once()

    def test_add_and_remove_handler(self):
        h = MagicMock(spec=FlowCallback)
        mgr = CallbackManager([])
        mgr.add_handler(h)
        self.assertEqual(len(mgr.handlers), 1)
        mgr.remove_handler(h)
        self.assertEqual(len(mgr.handlers), 0)

    def test_handler_error_does_not_break_dispatch(self):
        class BadHandler(FlowCallback):
            def on_flow_start(self, flow, **kwargs):
                raise RuntimeError("boom")

        class GoodHandler(FlowCallback):
            def __init__(self):
                self.called = False

            def on_flow_start(self, flow, **kwargs):
                self.called = True

        bad = BadHandler()
        good = GoodHandler()
        mgr = CallbackManager([bad, good])
        mgr.on_flow_start(FakeFlow())
        self.assertTrue(good.called)

    def test_child_inherits_handlers(self):
        h = MagicMock(spec=FlowCallback)
        mgr = CallbackManager([h])
        child = mgr.child()
        self.assertIn(h, child.handlers)

    def test_child_custom_handlers(self):
        parent_h = MagicMock(spec=FlowCallback)
        child_h = MagicMock(spec=FlowCallback)
        mgr = CallbackManager([parent_h])
        child = mgr.child([child_h])
        child.on_flow_start(FakeFlow())
        child_h.on_flow_start.assert_called_once()
        parent_h.on_flow_start.assert_called_once()

    def test_all_lifecycle_methods(self):
        h = MagicMock(spec=FlowCallback)
        mgr = CallbackManager([h])
        flow = FakeFlow()
        node = FakeNode()
        mgr.on_flow_start(flow)
        mgr.on_flow_end(flow, result="ok")
        mgr.on_flow_suspend(flow)
        mgr.on_flow_resume(flow)
        mgr.on_node_start(flow, node)
        mgr.on_node_end(flow, node, result="ok")
        mgr.on_node_suspend(flow, node)
        mgr.on_node_resume(flow, node)
        self.assertEqual(h.on_flow_start.call_count, 1)
        self.assertEqual(h.on_flow_end.call_count, 1)
        self.assertEqual(h.on_flow_suspend.call_count, 1)
        self.assertEqual(h.on_flow_resume.call_count, 1)
        self.assertEqual(h.on_node_start.call_count, 1)
        self.assertEqual(h.on_node_end.call_count, 1)
        self.assertEqual(h.on_node_suspend.call_count, 1)
        self.assertEqual(h.on_node_resume.call_count, 1)


class TestLoggerCallback(unittest.TestCase):
    """LoggerCallback smoke test."""

    def test_logger_callback_no_crash(self):
        cb = LoggerCallback()
        flow = FakeFlow()
        node = FakeNode()
        cb.on_flow_start(flow)
        cb.on_flow_end(flow, result="ok")
        cb.on_node_start(flow, node)
        cb.on_node_end(flow, node, result="ok")
        cb.on_flow_suspend(flow)
        cb.on_flow_resume(flow)
        cb.on_node_suspend(flow, node)
        cb.on_node_resume(flow, node)


class TestFlowEvent(unittest.TestCase):
    """FlowEvent enum."""

    def test_enum_values(self):
        self.assertEqual(FlowEvent.FLOW_START.value, "flow_start")
        self.assertEqual(FlowEvent.FLOW_END.value, "flow_end")
        self.assertEqual(FlowEvent.NODE_START.value, "node_start")
        self.assertEqual(FlowEvent.NODE_END.value, "node_end")


class TestFlowCallbackSubclassOverride(unittest.TestCase):
    """T081: Subclass overriding only on_flow_end works without implementing other hooks."""

    def test_subclass_only_on_flow_end(self):
        class EndOnlyCallback(FlowCallback):
            def __init__(self):
                self.end_result = None

            def on_flow_end(self, flow, result=None, error=None, exception=None, **kwargs):
                self.end_result = result

        cb = EndOnlyCallback()
        flow = FakeFlow()
        node = FakeNode()

        cb.on_flow_start(flow)
        cb.on_node_start(flow, node)
        cb.on_node_end(flow, node, result="node_ok")
        cb.on_flow_suspend(flow)
        cb.on_flow_resume(flow)
        cb.on_node_suspend(flow, node)
        cb.on_node_resume(flow, node)
        cb.on_flow_end(flow, result="final")

        self.assertEqual(cb.end_result, "final")

    def test_subclass_only_on_node_start(self):
        class NodeStartCallback(FlowCallback):
            def __init__(self):
                self.started_nodes = []

            def on_node_start(self, flow, node, **kwargs):
                self.started_nodes.append(node.id)

        cb = NodeStartCallback()
        flow = FakeFlow()
        node = FakeNode()

        cb.on_flow_start(flow)
        cb.on_node_start(flow, node)
        cb.on_node_end(flow, node, result="ok")
        cb.on_flow_end(flow, result="done")

        self.assertEqual(cb.started_nodes, ["n1"])

    def test_callback_manager_with_partial_subclass(self):
        class PartialCallback(FlowCallback):
            def __init__(self):
                self.flow_ended = False

            def on_flow_end(self, flow, result=None, error=None, exception=None, **kwargs):
                self.flow_ended = True

        cb = PartialCallback()
        mgr = CallbackManager([cb])
        flow = FakeFlow()
        node = FakeNode()

        mgr.on_flow_start(flow)
        mgr.on_node_start(flow, node)
        mgr.on_node_end(flow, node, result="ok")
        mgr.on_flow_end(flow, result="done")

        self.assertTrue(cb.flow_ended)


if __name__ == "__main__":
    unittest.main()
