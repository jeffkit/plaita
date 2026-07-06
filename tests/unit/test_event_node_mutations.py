"""Mutation-killing tests for plaita/node/event_node.py.

Targets survived mutations in:
- EventNode._get_node_state: context path, get_node_state fallback
- EventNode.execute: status value, event_id, event_type, variable resolution
- EventNode.on_event / on_timeout / on_error / on_cancel: status values
- EventNode.can_handle_event: event_type check, filter traversal
- EventNode.resume: dispatch to on_cancel / on_timeout / on_event
- EventNode._create_result: key names (event_type, event_filter, is_async)
"""
from __future__ import annotations

import unittest
from unittest.mock import MagicMock

from plaita.node.event_node import EventNode, EventNodeStatus


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_node(event_type="order.created", event_filter=None):
    data = {"id": "evt1", "event_type": event_type}
    if event_filter is not None:
        data["event_filter"] = event_filter
    return EventNode.model_validate(data)


def _make_execution(context=None, prefix="$", get_node_state_return=None):
    mock = MagicMock()
    mock.express_prefix = prefix
    mock.context = context or {}
    mock.evaluate = lambda v: v
    mock.get_node_state = MagicMock(return_value=get_node_state_return or {})
    return mock


# ---------------------------------------------------------------------------
# EventNode._get_node_state — path selection mutations
# ---------------------------------------------------------------------------

class TestGetNodeStatePaths(unittest.TestCase):
    def test_uses_get_node_state_when_available(self):
        """Kill mutations that skip hasattr(execution, 'get_node_state')."""
        node = _make_node()
        expected = {"status": "pending"}
        execution = _make_execution(get_node_state_return=expected)
        state = node._get_node_state(execution)
        execution.get_node_state.assert_called_once_with("evt1")
        self.assertEqual(state, expected)

    def test_falls_back_to_context_when_no_get_node_state(self):
        """Kill mutations on the second fallback path."""
        node = _make_node()
        # Execution without get_node_state method
        execution = MagicMock(spec=[])
        execution.express_prefix = "$"
        execution.context = {"$NODE": {"evt1": {"status": "completed"}}}
        result = node._get_node_state(execution)
        self.assertEqual(result, {"status": "completed"})

    def test_returns_default_when_no_context(self):
        """Kill mutations that don't return fallback."""
        node = _make_node()
        # spec with neither get_node_state nor context
        execution = MagicMock(spec=[])
        result = node._get_node_state(execution, default={"k": "v"})
        self.assertEqual(result, {"k": "v"})

    def test_node_id_used_in_context_lookup(self):
        """Kill mutations that change the context key to wrong node id."""
        node = _make_node()
        node_state = {"status": "pending"}
        mock = MagicMock()
        mock.express_prefix = "$"
        # Only the correct node id should hit
        mock.context = {"$NODE": {"evt1": node_state, "other": {"x": 1}}}
        # Remove get_node_state to force context path
        del mock.get_node_state
        result = node._get_node_state(mock)
        self.assertEqual(result, node_state)

    def test_prefix_used_in_context_node_key(self):
        """Kill mutations on the f'{prefix}NODE' computation."""
        node = _make_node()
        mock = MagicMock(spec=["express_prefix", "context"])
        mock.express_prefix = "#"
        mock.context = {"#NODE": {"evt1": {"s": 1}}}
        result = node._get_node_state(mock)
        self.assertEqual(result, {"s": 1})


# ---------------------------------------------------------------------------
# EventNode.execute — field mutations
# ---------------------------------------------------------------------------

class TestEventNodeExecuteMutations(unittest.TestCase):
    def test_status_is_pending_not_completed(self):
        """Kill mutation: PENDING.value → COMPLETED.value."""
        node = _make_node()
        result = node.execute(_make_execution())
        self.assertEqual(result["status"], "pending")
        self.assertNotEqual(result["status"], "completed")

    def test_event_id_starts_with_event_prefix(self):
        """Kill mutations that strip 'event_' prefix."""
        node = _make_node()
        result = node.execute(_make_execution())
        self.assertIn("event_id", result)
        self.assertTrue(result["event_id"].startswith("event_evt1_"))

    def test_event_type_in_result(self):
        """Kill mutations that remove event_type from result."""
        node = _make_node("user.signup")
        result = node.execute(_make_execution())
        self.assertEqual(result["event_type"], "user.signup")

    def test_is_async_true(self):
        """Kill mutations that set is_async=False."""
        node = _make_node()
        result = node.execute(_make_execution())
        self.assertTrue(result["is_async"])

    def test_event_filter_in_result(self):
        """Kill mutations that strip event_filter."""
        node = _make_node(event_filter={"key": "val"})
        result = node.execute(_make_execution())
        self.assertEqual(result["event_filter"], {"key": "val"})

    def test_variable_reference_resolved(self):
        """Kill mutations on startswith('$') check."""
        node = _make_node("$order_type")
        execution = _make_execution()
        execution.evaluate = lambda v: "order.created" if v == "$order_type" else v
        result = node.execute(execution)
        self.assertEqual(result["event_type"], "order.created")

    def test_non_variable_event_type_not_evaluated(self):
        """Kill mutations that always call evaluate."""
        node = _make_node("literal.event")
        execution = _make_execution()
        execution.evaluate = MagicMock(return_value="wrong")
        result = node.execute(execution)
        # literal should not be evaluated
        self.assertEqual(result["event_type"], "literal.event")
        execution.evaluate.assert_not_called()

    def test_variable_resolve_empty_falls_back(self):
        """Kill mutations that replace resolved_event_type on falsy resolve."""
        node = _make_node("$empty")
        execution = _make_execution()
        execution.evaluate = lambda v: ""  # returns empty string
        result = node.execute(execution)
        # Should fall back to the original reference
        self.assertEqual(result["event_type"], "$empty")

    def test_variable_resolve_non_string_falls_back(self):
        """Kill mutations where non-str resolved value causes wrong assignment."""
        node = _make_node("$num")
        execution = _make_execution()
        execution.evaluate = lambda v: 42  # returns non-string
        result = node.execute(execution)
        self.assertEqual(result["event_type"], "$num")


# ---------------------------------------------------------------------------
# EventNode.on_event
# ---------------------------------------------------------------------------

class TestOnEventMutations(unittest.TestCase):
    def test_status_becomes_completed(self):
        """Kill mutation: COMPLETED → some other status."""
        node = _make_node()
        execution = _make_execution()
        result = node.on_event(execution, {"data": "x"})
        self.assertEqual(result["status"], "completed")

    def test_event_data_stored(self):
        """Kill mutations that don't store event_data."""
        node = _make_node()
        execution = _make_execution()
        event_data = {"order_id": "123"}
        result = node.on_event(execution, event_data)
        self.assertEqual(result["event_data"], event_data)

    def test_empty_event_data_returns_early(self):
        """Kill mutations on the `if not event_data` guard."""
        node = _make_node()
        execution = _make_execution(get_node_state_return={"status": "pending"})
        result = node.on_event(execution, None)
        self.assertNotIn("event_data", result)

    def test_empty_dict_event_data_returns_early(self):
        node = _make_node()
        execution = _make_execution()
        result = node.on_event(execution, {})
        self.assertNotIn("event_data", result)


# ---------------------------------------------------------------------------
# EventNode.on_timeout
# ---------------------------------------------------------------------------

class TestOnTimeoutMutations(unittest.TestCase):
    def test_status_becomes_timeout(self):
        """Kill mutation: TIMEOUT → other status."""
        node = _make_node()
        execution = _make_execution()
        result = node.on_timeout(execution)
        self.assertEqual(result["status"], "timeout")

    def test_status_not_completed_or_error(self):
        node = _make_node()
        result = node.on_timeout(_make_execution())
        self.assertNotEqual(result["status"], "completed")
        self.assertNotEqual(result["status"], "error")
        self.assertNotEqual(result["status"], "cancelled")


# ---------------------------------------------------------------------------
# EventNode.on_error
# ---------------------------------------------------------------------------

class TestOnErrorMutations(unittest.TestCase):
    def test_status_becomes_error(self):
        """Kill mutation: ERROR → other status."""
        node = _make_node()
        execution = _make_execution()
        result = node.on_error(execution, "something went wrong")
        self.assertEqual(result["status"], "error")

    def test_error_message_stored(self):
        """Kill mutations that don't store error_message."""
        node = _make_node()
        result = node.on_error(_make_execution(), "msg here")
        self.assertEqual(result["error_message"], "msg here")


# ---------------------------------------------------------------------------
# EventNode.on_cancel
# ---------------------------------------------------------------------------

class TestOnCancelMutations(unittest.TestCase):
    def test_status_becomes_cancelled(self):
        """Kill mutation: CANCELLED → other status."""
        node = _make_node()
        execution = _make_execution()
        result = node.on_cancel(execution)
        self.assertEqual(result["status"], "cancelled")

    def test_status_not_error_or_timeout(self):
        node = _make_node()
        result = node.on_cancel(_make_execution())
        self.assertNotEqual(result["status"], "error")
        self.assertNotEqual(result["status"], "timeout")


# ---------------------------------------------------------------------------
# EventNode.resume — dispatch
# ---------------------------------------------------------------------------

class TestResumeMutations(unittest.TestCase):
    def test_cancel_calls_on_cancel(self):
        """Kill mutations that dispatch CANCEL to wrong handler."""
        from plaita.core.errors import ResumeType
        node = _make_node()
        execution = _make_execution()
        result = node.resume(execution, ResumeType.CANCEL)
        self.assertEqual(result["status"], "cancelled")

    def test_timeout_calls_on_timeout(self):
        """Kill mutations that dispatch TIMEOUT to wrong handler."""
        from plaita.core.errors import ResumeType
        node = _make_node()
        execution = _make_execution()
        result = node.resume(execution, ResumeType.TIMEOUT)
        self.assertEqual(result["status"], "timeout")

    def test_event_calls_on_event(self):
        """Kill mutations that dispatch EVENT to wrong handler."""
        from plaita.core.errors import ResumeType
        node = _make_node()
        execution = _make_execution()
        result = node.resume(execution, ResumeType.EVENT, {"key": "val"})
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["event_data"], {"key": "val"})

    def test_continue_calls_on_event(self):
        """CONTINUE falls through to on_event."""
        from plaita.core.errors import ResumeType
        node = _make_node()
        result = node.resume(_make_execution(), ResumeType.CONTINUE, {"x": 1})
        self.assertEqual(result["status"], "completed")


# ---------------------------------------------------------------------------
# EventNode.can_handle_event
# ---------------------------------------------------------------------------

class TestCanHandleEventMutations(unittest.TestCase):
    def test_matching_event_type_no_filter(self):
        """Kill event_type != comparison mutation."""
        node = _make_node("order.created")
        self.assertTrue(node.can_handle_event("order.created", {}))

    def test_wrong_event_type_returns_false(self):
        """Kill mutations that skip event_type check."""
        node = _make_node("order.created")
        self.assertFalse(node.can_handle_event("user.signup", {}))

    def test_empty_filter_matches_all(self):
        """Kill mutations on `if not self.event_filter`."""
        node = _make_node("order.created", event_filter={})
        self.assertTrue(node.can_handle_event("order.created", {"any": "data"}))

    def test_filter_match(self):
        """Kill mutations in filter traversal."""
        node = _make_node("order.created", event_filter={"status": "paid"})
        self.assertTrue(node.can_handle_event("order.created", {"status": "paid"}))

    def test_filter_mismatch(self):
        """Kill mutations that invert value comparison."""
        node = _make_node("order.created", event_filter={"status": "paid"})
        self.assertFalse(node.can_handle_event("order.created", {"status": "pending"}))

    def test_nested_filter_match(self):
        """Kill mutations in key split / traversal."""
        node = _make_node("order", event_filter={"order.status": "paid"})
        self.assertTrue(node.can_handle_event("order", {"order": {"status": "paid"}}))

    def test_nested_filter_missing_key(self):
        """Kill mutations that skip missing-key return False."""
        node = _make_node("order", event_filter={"order.status": "paid"})
        self.assertFalse(node.can_handle_event("order", {"order": {}}))

    def test_filter_key_missing_in_data(self):
        """Kill mutations in isinstance dict check."""
        node = _make_node("order", event_filter={"status": "paid"})
        self.assertFalse(node.can_handle_event("order", {}))


# ---------------------------------------------------------------------------
# EventNode._create_result — output structure
# ---------------------------------------------------------------------------

class TestCreateResultMutations(unittest.TestCase):
    def test_event_type_field(self):
        """Kill mutations that change 'event_type' key."""
        node = _make_node("test.event")
        result = node._create_result({})
        self.assertIn("event_type", result)
        self.assertEqual(result["event_type"], "test.event")

    def test_event_filter_field(self):
        """Kill mutations that change 'event_filter' key."""
        node = _make_node(event_filter={"k": "v"})
        result = node._create_result({})
        self.assertIn("event_filter", result)
        self.assertEqual(result["event_filter"], {"k": "v"})

    def test_is_async_field_true(self):
        """Kill mutations that set is_async=False or change key name."""
        node = _make_node()
        result = node._create_result({})
        self.assertIn("is_async", result)
        self.assertTrue(result["is_async"])

    def test_state_merged_into_result(self):
        """Kill mutations that don't call result.update(state)."""
        node = _make_node()
        state = {"status": "pending", "extra": "val"}
        result = node._create_result(state)
        self.assertEqual(result["status"], "pending")
        self.assertEqual(result["extra"], "val")

    def test_state_overwrites_base_event_type(self):
        """Kill mutations on update call vs separate assignment."""
        node = _make_node("base.event")
        # After update(state), state values should be present.
        # The execute() method reassigns event_type after _create_result.
        result = node._create_result({"status": "pending"})
        # Base event_type should be there from _create_result
        self.assertIn("event_type", result)


# ---------------------------------------------------------------------------
# _get_node_state — default argument precision (Round 2)
# ---------------------------------------------------------------------------

class TestGetNodeStateDefaultsMutations(unittest.TestCase):
    def test_default_prefix_is_dollar_not_xx(self):
        """Kill getattr(execution, 'express_prefix', 'XX$XX') mutation."""
        node = _make_node()
        mock = MagicMock(spec=["context"])
        # express_prefix is NOT an attribute, so getattr uses default
        mock.context = {"$NODE": {"evt1": {"status": "pending"}}}
        result = node._get_node_state(mock)
        # With default '$', key="$NODE" is found
        self.assertEqual(result, {"status": "pending"})

    def test_context_get_with_empty_dict_default(self):
        """Kill context.get(node_key, None) mutation.
        If node_key exists but has empty value, {} default shouldn't matter.
        But if context.get is called without default... need to verify dict fallback."""
        node = _make_node()
        mock = MagicMock(spec=["context", "express_prefix"])
        mock.express_prefix = "$"
        # Simulated context where $NODE exists but returned as is
        node_data = {"evt1": {"s": 1}}
        mock.context = {"$NODE": node_data}
        # .get() should return the actual node_data, not crash
        result = node._get_node_state(mock)
        self.assertEqual(result, {"s": 1})

    def test_node_id_used_in_results_lookup(self):
        """Kill node_results.get(None, ...) mutation."""
        node = _make_node("order.created")  # node.id = "evt1"
        mock = MagicMock(spec=["context", "express_prefix"])
        mock.express_prefix = "$"
        mock.context = {"$NODE": {"evt1": {"detail": "found"}, "other": {"x": 1}}}
        result = node._get_node_state(mock)
        self.assertEqual(result, {"detail": "found"})

    def test_default_value_returned_when_not_in_node_results(self):
        """Kill default or {} mutation in results.get(self.id, default or {})."""
        node = _make_node()
        mock = MagicMock(spec=["context", "express_prefix"])
        mock.express_prefix = "$"
        mock.context = {"$NODE": {"other_node": {"x": 1}}}  # evt1 not in node results
        # Should return empty dict (or default) when id not found
        result = node._get_node_state(mock)
        self.assertIsInstance(result, dict)

    def test_exception_returns_default(self):
        """Kill mutations that suppress exceptions."""
        node = _make_node()
        mock = MagicMock()
        mock.get_node_state = MagicMock(side_effect=RuntimeError("fail"))
        result = node._get_node_state(mock, default={"fallback": True})
        self.assertEqual(result, {"fallback": True})


# ---------------------------------------------------------------------------
# execute — logging arg precision (Round 2)
# ---------------------------------------------------------------------------

class TestExecuteLoggingMutations(unittest.TestCase):
    def test_execute_logs_node_id(self):
        """Kill logger.info(None, self.id, ...) mutation."""
        node = _make_node("order.created")
        with self.assertLogs("plaita", level="INFO") as cm:
            node.execute(_make_execution())
        combined = " ".join(cm.output)
        self.assertIn("evt1", combined)

    def test_execute_logs_event_type(self):
        """Kill logger.info("...", self.id, self.event_filter) mutation (drops event_type)."""
        node = _make_node("user.signup")
        with self.assertLogs("plaita", level="INFO") as cm:
            node.execute(_make_execution())
        combined = " ".join(cm.output)
        self.assertIn("user.signup", combined)

    def test_execute_logs_event_filter(self):
        """Kill mutation that drops event_filter from log."""
        node = _make_node("order.created", event_filter={"status": "active"})
        with self.assertLogs("plaita", level="INFO") as cm:
            node.execute(_make_execution())
        combined = " ".join(cm.output)
        self.assertIn("status", combined)


class TestExecuteVariableResolutionLogging(unittest.TestCase):
    def test_successful_resolution_logged_with_original_and_resolved(self):
        """Kill logger.info(..., None, resolved) and logger.info(..., self.event_type, None) mutations."""
        node = _make_node("$order_type")
        execution = _make_execution()
        execution.evaluate = lambda v: "order.placed" if v == "$order_type" else v

        with self.assertLogs("plaita", level="INFO") as cm:
            result = node.execute(execution)
        combined = " ".join(cm.output)
        self.assertIn("$order_type", combined)
        self.assertIn("order.placed", combined)

    def test_hasattr_evaluate_is_exact_string(self):
        """Kill hasattr(execution, 'XXevaluateXX') mutation."""
        node = _make_node("$var_event")
        execution = _make_execution()
        # execution HAS 'evaluate' → should try to resolve
        resolution_attempted = []
        def capturing_evaluate(v):
            resolution_attempted.append(v)
            return "resolved.event"
        execution.evaluate = capturing_evaluate

        result = node.execute(execution)
        self.assertGreater(len(resolution_attempted), 0,
                           "evaluate should have been called since execution has 'evaluate'")
        self.assertEqual(result["event_type"], "resolved.event")

    def test_no_evaluate_attr_fallback_to_original(self):
        """Kill inverse: without 'evaluate', original event_type preserved."""
        node = _make_node("$var_event")
        # Create execution without 'evaluate' attribute
        execution = MagicMock(spec=["express_prefix", "context", "get_node_state"])
        execution.express_prefix = "$"
        execution.context = {}
        execution.get_node_state = MagicMock(return_value={})

        result = node.execute(execution)
        self.assertEqual(result["event_type"], "$var_event")

    def test_execution_id_in_event_id_format(self):
        """Kill mutations on event_id string formatting."""
        node = _make_node()
        result = node.execute(_make_execution())
        # event_id should contain node id
        self.assertIn("evt1", result["event_id"])
        # Must start with "event_"
        self.assertTrue(result["event_id"].startswith("event_evt1_"))


# ---------------------------------------------------------------------------
# can_handle_event — filter split and traversal precision (Round 2)
# ---------------------------------------------------------------------------

class TestCanHandleEventFilterPrecision(unittest.TestCase):
    def test_filter_empty_string_value_matches(self):
        """Kill mutations that alter empty filter logic."""
        node = _make_node("order", event_filter={})
        self.assertTrue(node.can_handle_event("order", {"any": "data"}))

    def test_deep_nested_filter_exact_key_traversal(self):
        """Kill key.split('.') mutation."""
        node = _make_node("ev", event_filter={"a.b.c": "deep_val"})
        self.assertTrue(node.can_handle_event("ev", {"a": {"b": {"c": "deep_val"}}}))
        self.assertFalse(node.can_handle_event("ev", {"a": {"b": {"c": "wrong"}}}))

    def test_isinstance_check_on_current_data(self):
        """Kill isinstance(current_data, dict) → isinstance(current_data, str) mutation."""
        node = _make_node("ev", event_filter={"key": "val"})
        # Non-dict event data should fail gracefully
        self.assertFalse(node.can_handle_event("ev", "not_a_dict"))

    def test_multi_filter_all_must_match(self):
        """Kill early return in filter loop."""
        node = _make_node("ev", event_filter={"a": "1", "b": "2"})
        self.assertTrue(node.can_handle_event("ev", {"a": "1", "b": "2"}))
        self.assertFalse(node.can_handle_event("ev", {"a": "1", "b": "wrong"}))

    def test_event_type_exact_comparison(self):
        """Kill event_type == → != mutation."""
        node = _make_node("exact.type")
        self.assertTrue(node.can_handle_event("exact.type", {}))
        self.assertFalse(node.can_handle_event("exact.typo", {}))
        self.assertFalse(node.can_handle_event("", {}))


if __name__ == "__main__":
    unittest.main()
