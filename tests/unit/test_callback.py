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


class TestCallbackManagerDispatchArgs(unittest.TestCase):
    """断言每个生命周期方法把「正确的事件名 + 参数」原样分发到 handler。

    这一组断言用来杀死「改事件名字符串 / 丢弃或篡改 flow/node/result 参数」
    的变异点——只看 call_count 看不出这类缺陷。
    """

    def test_lifecycle_methods_dispatch_exact_args(self):
        h = MagicMock(spec=FlowCallback)
        mgr = CallbackManager([h])
        flow = FakeFlow()
        node = FakeNode()
        exc = ValueError("boom")
        mgr.on_flow_start(flow, extra="x")
        mgr.on_flow_end(flow, result="ok", error="err", exception=exc, tag="t")
        mgr.on_flow_suspend(flow, reason="pause")
        mgr.on_flow_resume(flow, reason="resume")
        mgr.on_node_start(flow, node, phase="start")
        mgr.on_node_end(flow, node, result="ok", error="err", exception=exc, tag="t")
        mgr.on_node_suspend(flow, node, reason="pause")
        mgr.on_node_resume(flow, node, reason="resume")

        h.on_flow_start.assert_called_once_with(flow, extra="x")
        h.on_flow_end.assert_called_once_with(flow, "ok", "err", exc, tag="t")
        h.on_flow_suspend.assert_called_once_with(flow, reason="pause")
        h.on_flow_resume.assert_called_once_with(flow, reason="resume")
        h.on_node_start.assert_called_once_with(flow, node, phase="start")
        h.on_node_end.assert_called_once_with(flow, node, "ok", "err", exc, tag="t")
        h.on_node_suspend.assert_called_once_with(flow, node, reason="pause")
        h.on_node_resume.assert_called_once_with(flow, node, reason="resume")


class TestCallbackManagerInit(unittest.TestCase):
    """断言 __init__ 的 parent 记录与 inherit_handlers 默认行为（False 时不继承）。"""

    def test_parent_recorded(self):
        parent = CallbackManager([MagicMock(spec=FlowCallback)])
        child = CallbackManager([], parent=parent)
        self.assertIs(child.parent, parent)

    def test_default_does_not_inherit_parent_handlers(self):
        parent_h = MagicMock(spec=FlowCallback)
        parent = CallbackManager([parent_h])
        own_h = MagicMock(spec=FlowCallback)
        # 不传 inherit_handlers：默认 False，不应把 parent 的 handler 并入。
        mgr = CallbackManager([own_h], parent=parent)
        self.assertEqual(mgr.handlers, [own_h])
        self.assertNotIn(parent_h, mgr.handlers)

    def test_inherit_handlers_true_extends_from_parent(self):
        parent_h = MagicMock(spec=FlowCallback)
        parent = CallbackManager([parent_h])
        own_h = MagicMock(spec=FlowCallback)
        mgr = CallbackManager([own_h], parent=parent, inherit_handlers=True)
        self.assertEqual(mgr.handlers, [own_h, parent_h])


class TestCallHandlersErrorLogging(unittest.TestCase):
    """断言 handler 抛错时，_call_handlers 写出的 warning 包含方法名、错误信息
    且带 traceback（exc_info）。用来杀死改 logger.warning 参数 / 丢 exc_info
    的变异点。"""

    def test_error_in_handler_logged_with_method_error_and_exc_info(self):
        class BadHandler(FlowCallback):
            def on_flow_start(self, flow, **kwargs):
                raise RuntimeError("boom")

        mgr = CallbackManager([BadHandler()])
        with self.assertLogs("plaita.core.callback", level="WARNING") as cm:
            mgr.on_flow_start(FakeFlow())  # 不得抛出

        self.assertEqual(len(cm.records), 1)
        record = cm.records[0]
        self.assertEqual(record.getMessage(), "Error in on_flow_start callback: boom")
        # exc_info=True 应得到 traceback 三元组；改 exc_info=False 的变异点会留下 False。
        self.assertTrue(record.exc_info)


class TestLoggerCallback(unittest.TestCase):
    """LoggerCallback smoke test + 断言日志内容（杀死改日志字符串/参数的变异点）。"""

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

    def test_log_messages_contain_event_flow_and_node_ids(self):
        cb = LoggerCallback()
        flow = FakeFlow()
        node = FakeNode()
        with self.assertLogs("plaita.core.callback", level="INFO") as cm:
            cb.on_flow_start(flow)
            cb.on_flow_end(flow, result="ok", error=None, exception=None)
            cb.on_node_start(flow, node)
            cb.on_node_end(flow, node, result="ok", error=None, exception=None)
            cb.on_flow_suspend(flow)
            cb.on_flow_resume(flow)
            cb.on_node_suspend(flow, node)
            cb.on_node_resume(flow, node)

        # 每个事件一条 INFO；逐条校验，避免 join 后的子串误判。
        messages = [r.getMessage() for r in cm.records]
        self.assertEqual(messages[0], "[flow start] test")
        self.assertEqual(messages[1], "[flow end] test with result: ok, error: None, exception: None")
        self.assertEqual(messages[2], "[node start] n1 @ flow test")
        self.assertEqual(
            messages[3],
            "[node end] n1 @ flow test with result: ok, error: None, exception: None",
        )
        self.assertEqual(messages[4], "[flow suspend] test")
        self.assertEqual(messages[5], "[flow resume] test")
        self.assertEqual(messages[6], "[node suspend] n1 @ flow test")
        self.assertEqual(messages[7], "[node resume] n1 @ flow test")


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
