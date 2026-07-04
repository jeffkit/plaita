"""Unit tests for plaita.node.event_node — EventNode.

Coverage target: plaita/node/event_node.py (64% → target 85%+)

Tests are purely unit-level and do not depend on EventBus or RedisEventBus.
An execution context is simulated with a simple dict-backed mock.
"""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock

from plaita.node.event_node import EventNode, EventNodeStatus


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_node(event_type: str = "order.created", event_filter: dict = None) -> EventNode:
    data = {
        "id": "evt_node",
        "event_type": event_type,
    }
    if event_filter is not None:
        data["event_filter"] = event_filter
    return EventNode.model_validate(data)


def _make_execution(context: dict = None, express_prefix: str = "$") -> MagicMock:
    """Minimal mock for the FlowExecution context."""
    mock = MagicMock()
    mock.express_prefix = express_prefix
    mock.context = context or {}
    mock.evaluate = lambda val: val  # pass-through
    # Return a real dict so _get_node_state result supports item assignment
    mock.get_node_state = MagicMock(return_value={})
    return mock


# ---------------------------------------------------------------------------
# EventNode.execute
# ---------------------------------------------------------------------------

class TestEventNodeExecute(unittest.TestCase):
    def test_execute_returns_pending_status(self):
        node = _make_node("order.created")
        execution = _make_execution()
        result = node.execute(execution)
        self.assertEqual(result["status"], EventNodeStatus.PENDING.value)

    def test_execute_sets_event_type_in_result(self):
        node = _make_node("user.signup")
        execution = _make_execution()
        result = node.execute(execution)
        self.assertEqual(result["event_type"], "user.signup")

    def test_execute_includes_event_id(self):
        node = _make_node()
        execution = _make_execution()
        result = node.execute(execution)
        self.assertIn("event_id", result)
        self.assertTrue(result["event_id"].startswith("event_evt_node_"))

    def test_execute_is_async_flag(self):
        node = _make_node()
        result = node.execute(_make_execution())
        self.assertTrue(result["is_async"])

    def test_execute_resolves_variable_event_type(self):
        """If event_type starts with $, it's evaluated via execution.evaluate."""
        node = _make_node("$INPUT.event_type")
        mock = MagicMock()
        mock.express_prefix = "$"
        mock.context = {"$INPUT": {"event_type": "resolved.type"}}
        mock.evaluate = lambda val: "resolved.type" if val == "$INPUT.event_type" else val
        result = node.execute(mock)
        self.assertEqual(result["event_type"], "resolved.type")

    def test_execute_handles_evaluate_failure_gracefully(self):
        """If evaluate raises, original event_type is preserved."""
        node = _make_node("$BROKEN.ref")
        mock = MagicMock()
        mock.express_prefix = "$"
        mock.context = {}
        mock.evaluate = MagicMock(side_effect=Exception("eval error"))
        result = node.execute(mock)
        self.assertEqual(result["event_type"], "$BROKEN.ref")

    def test_execute_no_evaluate_attr(self):
        """If execution has no 'evaluate' method, original event_type preserved."""
        node = _make_node("$NOEVAL.ref")
        mock = MagicMock(spec=[])  # no attributes
        result = node.execute(mock)
        self.assertEqual(result["event_type"], "$NOEVAL.ref")

    def test_execute_includes_filter(self):
        node = _make_node(event_filter={"order_id": "123"})
        result = node.execute(_make_execution())
        self.assertEqual(result["event_filter"], {"order_id": "123"})


# ---------------------------------------------------------------------------
# EventNode.on_event
# ---------------------------------------------------------------------------

class TestEventNodeOnEvent(unittest.TestCase):
    def test_on_event_updates_status_to_completed(self):
        node = _make_node()
        result = node.on_event(_make_execution(), {"data": "payload"})
        self.assertEqual(result["status"], EventNodeStatus.COMPLETED.value)

    def test_on_event_stores_event_data(self):
        node = _make_node()
        data = {"order_id": "xyz", "amount": 100}
        result = node.on_event(_make_execution(), data)
        self.assertEqual(result["event_data"], data)

    def test_on_event_empty_data_returns_existing_state(self):
        node = _make_node()
        result = node.on_event(_make_execution(), {})
        # empty data → returns state without modification
        self.assertIsInstance(result, dict)

    def test_on_event_none_data(self):
        node = _make_node()
        result = node.on_event(_make_execution(), None)
        self.assertIsInstance(result, dict)


# ---------------------------------------------------------------------------
# EventNode.on_timeout
# ---------------------------------------------------------------------------

class TestEventNodeOnTimeout(unittest.TestCase):
    def test_on_timeout_sets_timeout_status(self):
        node = _make_node()
        result = node.on_timeout(_make_execution())
        self.assertEqual(result["status"], EventNodeStatus.TIMEOUT.value)


# ---------------------------------------------------------------------------
# EventNode.on_error
# ---------------------------------------------------------------------------

class TestEventNodeOnError(unittest.TestCase):
    def test_on_error_sets_error_status(self):
        node = _make_node()
        result = node.on_error(_make_execution(), "connection refused")
        self.assertEqual(result["status"], EventNodeStatus.ERROR.value)

    def test_on_error_stores_message(self):
        node = _make_node()
        result = node.on_error(_make_execution(), "timeout occurred")
        self.assertEqual(result["error_message"], "timeout occurred")


# ---------------------------------------------------------------------------
# EventNode.on_cancel
# ---------------------------------------------------------------------------

class TestEventNodeOnCancel(unittest.TestCase):
    def test_on_cancel_sets_cancelled_status(self):
        node = _make_node()
        result = node.on_cancel(_make_execution())
        self.assertEqual(result["status"], EventNodeStatus.CANCELLED.value)


# ---------------------------------------------------------------------------
# EventNode.resume — dispatches to on_cancel / on_timeout / on_event
# ---------------------------------------------------------------------------

class TestEventNodeResume(unittest.TestCase):
    def test_resume_cancel(self):
        from plaita.core.errors import ResumeType
        node = _make_node()
        result = node.resume(_make_execution(), ResumeType.CANCEL)
        self.assertEqual(result["status"], EventNodeStatus.CANCELLED.value)

    def test_resume_timeout(self):
        from plaita.core.errors import ResumeType
        node = _make_node()
        result = node.resume(_make_execution(), ResumeType.TIMEOUT)
        self.assertEqual(result["status"], EventNodeStatus.TIMEOUT.value)

    def test_resume_event(self):
        from plaita.core.errors import ResumeType
        node = _make_node()
        data = {"value": 42}
        result = node.resume(_make_execution(), ResumeType.EVENT, resume_data=data)
        self.assertEqual(result["status"], EventNodeStatus.COMPLETED.value)
        self.assertEqual(result["event_data"], data)


# ---------------------------------------------------------------------------
# EventNode.can_handle_event
# ---------------------------------------------------------------------------

class TestEventNodeCanHandleEvent(unittest.TestCase):
    def test_same_type_no_filter(self):
        node = _make_node("order.created")
        self.assertTrue(node.can_handle_event("order.created", {}))

    def test_different_type_returns_false(self):
        node = _make_node("order.created")
        self.assertFalse(node.can_handle_event("order.updated", {}))

    def test_filter_matches(self):
        node = _make_node("order.created", event_filter={"status": "paid"})
        self.assertTrue(node.can_handle_event("order.created", {"status": "paid"}))

    def test_filter_no_match(self):
        node = _make_node("order.created", event_filter={"status": "paid"})
        self.assertFalse(node.can_handle_event("order.created", {"status": "pending"}))

    def test_nested_filter_matches(self):
        node = _make_node("order.created", event_filter={"user.role": "admin"})
        self.assertTrue(node.can_handle_event("order.created", {"user": {"role": "admin"}}))

    def test_nested_filter_key_missing(self):
        node = _make_node("order.created", event_filter={"user.role": "admin"})
        self.assertFalse(node.can_handle_event("order.created", {"user": {}}))

    def test_filter_missing_top_key(self):
        node = _make_node("order.created", event_filter={"order_id": "123"})
        self.assertFalse(node.can_handle_event("order.created", {}))


# ---------------------------------------------------------------------------
# EventNode._get_node_state — via mock execution
# ---------------------------------------------------------------------------

class TestEventNodeGetNodeState(unittest.TestCase):
    def test_via_get_node_state_method(self):
        node = _make_node()
        mock = MagicMock()
        mock.get_node_state = MagicMock(return_value={"custom": True})
        result = node._get_node_state(mock)
        mock.get_node_state.assert_called_once_with("evt_node")
        self.assertEqual(result, {"custom": True})

    def test_via_context_node_key(self):
        node = _make_node()
        mock = MagicMock(spec=["context", "express_prefix"])
        mock.express_prefix = "$"
        mock.context = {"$NODE": {"evt_node": {"my": "state"}}}
        result = node._get_node_state(mock)
        self.assertEqual(result, {"my": "state"})

    def test_fallback_on_exception(self):
        node = _make_node()
        mock = MagicMock()
        mock.get_node_state = MagicMock(side_effect=Exception("boom"))
        result = node._get_node_state(mock, default={"fallback": True})
        self.assertEqual(result, {"fallback": True})


if __name__ == "__main__":
    unittest.main()
