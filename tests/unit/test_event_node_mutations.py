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



# ---------------------------------------------------------------------------
# Round 3: execute — initial log precise (kills mutmut_2,3,4,9)
# ---------------------------------------------------------------------------

class TestExecuteInitialLogPrecision(unittest.TestCase):
    def test_initial_log_has_node_id_not_none(self):
        """Kill mutmut_2: self.id → None in initial logger.info call."""
        node = _make_node("order.created")
        with self.assertLogs("plaita", level="INFO") as cm:
            node.execute(_make_execution())
        start_msgs = [m for m in cm.output if "开始执行事件节点" in m]
        self.assertTrue(len(start_msgs) >= 1, "Expected initial execution log")
        # With mutmut_2, node id is None → "[None]" not "[evt1]"
        self.assertIn("evt1", start_msgs[0])

    def test_initial_log_has_event_type_not_none(self):
        """Kill mutmut_3: self.event_type → None in initial logger.info call."""
        node = _make_node("user.registered")
        with self.assertLogs("plaita", level="INFO") as cm:
            node.execute(_make_execution())
        start_msgs = [m for m in cm.output if "开始执行事件节点" in m]
        self.assertTrue(len(start_msgs) >= 1)
        self.assertIn("user.registered", start_msgs[0])

    def test_initial_log_has_event_filter_not_none(self):
        """Kill mutmut_4: self.event_filter → None in initial logger.info call."""
        node = _make_node("order.created", event_filter={"item_id": "42"})
        with self.assertLogs("plaita", level="INFO") as cm:
            node.execute(_make_execution())
        start_msgs = [m for m in cm.output if "开始执行事件节点" in m]
        self.assertTrue(len(start_msgs) >= 1)
        self.assertIn("item_id", start_msgs[0])

    def test_initial_log_message_is_not_mangled(self):
        """Kill mutmut_9: format string mangled with XX prefix/suffix."""
        node = _make_node("order.created")
        with self.assertLogs("plaita", level="INFO") as cm:
            node.execute(_make_execution())
        start_msgs = [m for m in cm.output if "开始执行事件节点" in m]
        self.assertTrue(len(start_msgs) >= 1)
        # Mangled version starts with "XX开始执行事件节点"
        self.assertNotIn("XX", start_msgs[0].split("INFO:")[1] if "INFO:" in start_msgs[0] else start_msgs[0])


# ---------------------------------------------------------------------------
# Round 3: execute — hasattr spec mock (kills mutmut_19,20)
# ---------------------------------------------------------------------------

class TestExecuteHasattrSpecMock(unittest.TestCase):
    def test_hasattr_checks_lowercase_evaluate(self):
        """Kill hasattr(execution, 'XXevaluateXX') and 'EVALUATE' mutations.

        Using spec= prevents MagicMock from auto-creating unknown attributes,
        so hasattr(mock, 'XXevaluateXX') returns False when spec doesn't include it.
        """
        node = _make_node("$var_event_type")
        # spec restricts to only declared attributes
        execution = MagicMock(spec=["evaluate", "express_prefix", "context", "get_node_state"])
        execution.express_prefix = "$"
        execution.context = {}
        execution.get_node_state = MagicMock(return_value={})

        resolved_vals = []
        def capture_eval(v):
            resolved_vals.append(v)
            return "resolved.specific.type"
        execution.evaluate = capture_eval

        result = node.execute(execution)
        # With mutmut_19/20: hasattr checks wrong attr name → spec mock returns False
        # → evaluate never called → event_type stays as "$var_event_type"
        self.assertGreater(len(resolved_vals), 0,
                           "evaluate must be called when execution has 'evaluate' attr")
        self.assertEqual(result["event_type"], "resolved.specific.type")

    def test_no_evaluate_on_spec_mock_preserves_original(self):
        """Confirm: spec mock without 'evaluate' correctly falls through to else branch."""
        node = _make_node("$var_event_type")
        execution = MagicMock(spec=["express_prefix", "context", "get_node_state"])
        execution.express_prefix = "$"
        execution.context = {}
        execution.get_node_state = MagicMock(return_value={})

        with self.assertLogs("plaita", level="WARNING") as cm:
            result = node.execute(execution)
        self.assertEqual(result["event_type"], "$var_event_type")
        # Should log the "不支持evaluate方法" warning
        combined = " ".join(cm.output)
        self.assertIn("evaluate", combined)


# ---------------------------------------------------------------------------
# Round 3: execute — success/failure/no-evaluate/exception log precision
# ---------------------------------------------------------------------------

class TestExecuteVariableResolutionLogPrecision(unittest.TestCase):
    def test_success_resolve_log_has_original_event_type(self):
        """Kill mutmut_26: self.event_type → None in success log."""
        node = _make_node("$orig_type")
        execution = _make_execution()
        execution.evaluate = lambda v: "concrete.type"
        with self.assertLogs("plaita", level="INFO") as cm:
            node.execute(execution)
        resolve_msgs = [m for m in cm.output if "成功解析" in m]
        self.assertTrue(len(resolve_msgs) >= 1)
        self.assertIn("$orig_type", resolve_msgs[0])

    def test_success_resolve_log_has_resolved_value(self):
        """Kill mutmut_27: resolved → None in success log."""
        node = _make_node("$orig_type")
        execution = _make_execution()
        execution.evaluate = lambda v: "concrete.type"
        with self.assertLogs("plaita", level="INFO") as cm:
            node.execute(execution)
        resolve_msgs = [m for m in cm.output if "成功解析" in m]
        self.assertTrue(len(resolve_msgs) >= 1)
        self.assertIn("concrete.type", resolve_msgs[0])

    def test_success_resolve_log_message_not_mangled(self):
        """Kill mutmut_31: format string mangled with XX."""
        node = _make_node("$orig_type")
        execution = _make_execution()
        execution.evaluate = lambda v: "concrete.type"
        with self.assertLogs("plaita", level="INFO") as cm:
            node.execute(execution)
        resolve_msgs = [m for m in cm.output if "成功解析" in m]
        self.assertTrue(len(resolve_msgs) >= 1)
        self.assertNotIn("XX成功", resolve_msgs[0])

    def test_failure_resolve_log_has_original_event_type(self):
        """Kill mutmut_34: self.event_type → None in failure log."""
        node = _make_node("$bad_type")
        execution = _make_execution()
        execution.evaluate = lambda v: None  # returns None → failure path
        with self.assertLogs("plaita", level="WARNING") as cm:
            node.execute(execution)
        fail_msgs = [m for m in cm.output if "解析事件类型变量引用失败" in m]
        self.assertTrue(len(fail_msgs) >= 1)
        self.assertIn("$bad_type", fail_msgs[0])

    def test_failure_resolve_log_has_resolved_value(self):
        """Kill mutmut_35: resolved → None in failure log."""
        node = _make_node("$bad_type")
        execution = _make_execution()
        execution.evaluate = lambda v: 999  # non-string → failure path
        with self.assertLogs("plaita", level="WARNING") as cm:
            node.execute(execution)
        fail_msgs = [m for m in cm.output if "解析事件类型变量引用失败" in m]
        self.assertTrue(len(fail_msgs) >= 1)
        self.assertIn("999", fail_msgs[0])

    def test_failure_resolve_log_message_not_mangled(self):
        """Kill mutmut_39,40: XX prefix or uppercase %S in format string."""
        node = _make_node("$bad_type")
        execution = _make_execution()
        execution.evaluate = lambda v: None
        with self.assertLogs("plaita", level="WARNING") as cm:
            node.execute(execution)
        fail_msgs = [m for m in cm.output if "解析事件类型变量引用失败" in m]
        self.assertTrue(len(fail_msgs) >= 1)
        self.assertNotIn("XX解析", fail_msgs[0])

    def test_no_evaluate_log_has_event_type(self):
        """Kill mutmut_43: self.event_type → None in no-evaluate warning."""
        node = _make_node("$no_eval_type")
        execution = MagicMock(spec=["express_prefix", "context", "get_node_state"])
        execution.express_prefix = "$"
        execution.context = {}
        execution.get_node_state = MagicMock(return_value={})
        with self.assertLogs("plaita", level="WARNING") as cm:
            node.execute(execution)
        no_eval_msgs = [m for m in cm.output if "不支持evaluate" in m]
        self.assertTrue(len(no_eval_msgs) >= 1)
        self.assertIn("$no_eval_type", no_eval_msgs[0])

    def test_no_evaluate_log_message_not_mangled(self):
        """Kill mutmut_46,47: mangled format string in no-evaluate warning."""
        node = _make_node("$no_eval_type")
        execution = MagicMock(spec=["express_prefix", "context", "get_node_state"])
        execution.express_prefix = "$"
        execution.context = {}
        execution.get_node_state = MagicMock(return_value={})
        with self.assertLogs("plaita", level="WARNING") as cm:
            node.execute(execution)
        no_eval_msgs = [m for m in cm.output if "不支持evaluate" in m]
        self.assertTrue(len(no_eval_msgs) >= 1)
        self.assertNotIn("XX执行", no_eval_msgs[0])

    def test_exception_error_log_has_exception_text(self):
        """Kill mutmut_50: e → None in exception error log."""
        node = _make_node("$exc_type")
        execution = _make_execution()
        execution.evaluate = MagicMock(side_effect=ValueError("eval-exception-text"))
        with self.assertLogs("plaita", level="ERROR") as cm:
            result = node.execute(execution)
        error_msgs = [m for m in cm.output if "ERROR" in m]
        self.assertTrue(len(error_msgs) >= 1)
        self.assertIn("eval-exception-text", error_msgs[0])

    def test_exception_error_log_has_format_string(self):
        """Kill mutmut_51: logger.error(e) → format string removed.
        With mutmut_51, logger.error(e) uses exception as format string,
        losing the '解析事件类型变量引用时出错' prefix.
        """
        node = _make_node("$exc_type")
        execution = _make_execution()
        execution.evaluate = MagicMock(side_effect=ValueError("exc-sentinel"))
        with self.assertLogs("plaita", level="ERROR") as cm:
            node.execute(execution)
        error_msgs = [m for m in cm.output if "ERROR" in m]
        self.assertTrue(len(error_msgs) >= 1)
        self.assertIn("解析事件类型变量引用时出错", error_msgs[0])

    def test_exception_error_log_message_not_mangled(self):
        """Kill mutmut_53: XX prefix in exception error log."""
        node = _make_node("$exc_type")
        execution = _make_execution()
        execution.evaluate = MagicMock(side_effect=ValueError("exc-msg"))
        with self.assertLogs("plaita", level="ERROR") as cm:
            node.execute(execution)
        error_msgs = [m for m in cm.output if "ERROR" in m]
        self.assertTrue(len(error_msgs) >= 1)
        self.assertNotIn("XX解析", error_msgs[0])


# ---------------------------------------------------------------------------
# Round 3: execute — event_id timestamp format (kills mutmut_61)
# ---------------------------------------------------------------------------

class TestExecuteEventIdTimestamp(unittest.TestCase):
    def test_event_id_timestamp_is_milliseconds(self):
        """Kill mutmut_61: time.time() / 1000 instead of * 1000.
        Millisecond timestamp (~1.7e12) vs second/1000 (~1.7e6).
        """
        node = _make_node()
        result = node.execute(_make_execution())
        event_id = result["event_id"]
        # Format: "event_evt1_<timestamp>"
        parts = event_id.split("_")
        self.assertGreaterEqual(len(parts), 3)
        timestamp = int(parts[-1])
        # Millisecond timestamp > 1e12; / 1000 would give ~1.7e6
        self.assertGreater(timestamp, 1_000_000_000_000,
                           f"event_id timestamp should be in milliseconds, got {timestamp}")

    def test_event_id_timestamp_uses_exact_millisecond_multiplier(self):
        """Kill mutmut_62: int(time.time() * 1001) instead of * 1000.
        With fixed mock time, *1000 and *1001 produce different integer timestamps.
        """
        from unittest.mock import patch
        node = _make_node()
        fixed_time = 1_700_000_000.5
        with patch("plaita.node.event_node.time.time", return_value=fixed_time):
            result = node.execute(_make_execution())
        event_id = result["event_id"]
        expected_ts = int(fixed_time * 1000)
        self.assertTrue(event_id.endswith(f"_{expected_ts}"),
                        f"event_id should end with _{expected_ts}, got {event_id}")
        wrong_ts = int(fixed_time * 1001)
        self.assertNotEqual(int(event_id.rsplit("_", 1)[-1]), wrong_ts,
                            "timestamp must use *1000 not *1001")


# ---------------------------------------------------------------------------
# Round 3: execute — final result log precision (kills mutmut_72,73,74,77)
# ---------------------------------------------------------------------------

class TestExecuteFinalLogPrecision(unittest.TestCase):
    def test_final_log_has_node_id(self):
        """Kill mutmut_72: self.id → None in final result logger.info call.
        Must check the bracket prefix, not bare 'evt1' which also appears in event_id.
        """
        node = _make_node("order.created")
        with self.assertLogs("plaita", level="INFO") as cm:
            node.execute(_make_execution())
        result_msgs = [m for m in cm.output if "执行结果" in m]
        self.assertTrue(len(result_msgs) >= 1)
        # Format: "事件节点 [evt1] 执行结果: ..." — mutmut_72 gives "[None]"
        self.assertIn("事件节点 [evt1]", result_msgs[0])
        self.assertNotIn("事件节点 [None]", result_msgs[0])

    def test_final_log_has_result_content(self):
        """Kill mutmut_73: result → None in final result logger.info call."""
        node = _make_node("order.created")
        with self.assertLogs("plaita", level="INFO") as cm:
            node.execute(_make_execution())
        result_msgs = [m for m in cm.output if "执行结果" in m]
        self.assertTrue(len(result_msgs) >= 1)
        # With mutmut_73: result is None → doesn't show event_type
        self.assertIn("event_type", result_msgs[0])

    def test_final_log_format_string_present(self):
        """Kill mutmut_74: logger.info(self.id, result) — format string removed.
        Without format string, '执行结果' won't appear in log output.
        """
        node = _make_node("order.created")
        with self.assertLogs("plaita", level="INFO") as cm:
            node.execute(_make_execution())
        result_msgs = [m for m in cm.output if "执行结果" in m]
        # mutmut_74: logger.info("evt1", result) → log has "evt1" but no "执行结果"
        self.assertTrue(len(result_msgs) >= 1,
                        "Log should contain '执行结果' format string")

    def test_final_log_message_not_mangled(self):
        """Kill mutmut_77: XX prefix/suffix in final log format string."""
        node = _make_node("order.created")
        with self.assertLogs("plaita", level="INFO") as cm:
            node.execute(_make_execution())
        result_msgs = [m for m in cm.output if "执行结果" in m]
        self.assertTrue(len(result_msgs) >= 1)
        # With mutmut_77: "XX事件节点 [%s] 执行结果: %sXX" — has "XX"
        self.assertNotIn("XX事件节点", result_msgs[0])


# ---------------------------------------------------------------------------
# Round 3: _get_node_state — exception log precision (kills mutmut_38-41)
# ---------------------------------------------------------------------------

class TestGetNodeStateExceptionLogPrecision(unittest.TestCase):
    def test_exception_warning_has_error_text(self):
        """Kill mutmut_38: e → None in warning arg; log would show 'None' not error text."""
        node = _make_node()
        execution = MagicMock()
        execution.get_node_state = MagicMock(
            side_effect=RuntimeError("distinctive-error-abc123")
        )
        with self.assertLogs("plaita", level="WARNING") as cm:
            node._get_node_state(execution)
        combined = " ".join(cm.output)
        self.assertIn("distinctive-error-abc123", combined)

    def test_exception_warning_has_format_string(self):
        """Kill mutmut_39: logger.warning(e) — format string removed.
        With mutmut_39, message is just the exception str, missing '获取节点状态失败'.
        """
        node = _make_node()
        execution = MagicMock()
        execution.get_node_state = MagicMock(side_effect=RuntimeError("err-xyz"))
        with self.assertLogs("plaita", level="WARNING") as cm:
            node._get_node_state(execution)
        combined = " ".join(cm.output)
        self.assertIn("获取节点状态失败", combined)

    def test_exception_warning_not_missing_error_arg(self):
        """Kill mutmut_40: logger.warning("...", ) — error arg dropped.
        With mutmut_40, log shows '获取节点状态失败: ' with empty %s substitution.
        """
        node = _make_node()
        execution = MagicMock()
        execution.get_node_state = MagicMock(
            side_effect=RuntimeError("specific-err-7890")
        )
        with self.assertLogs("plaita", level="WARNING") as cm:
            node._get_node_state(execution)
        combined = " ".join(cm.output)
        # Mutmut_40 drops the arg → error text not in log
        self.assertIn("specific-err-7890", combined)

    def test_exception_warning_message_not_mangled(self):
        """Kill mutmut_41: XX prefix/suffix in format string."""
        node = _make_node()
        execution = MagicMock()
        execution.get_node_state = MagicMock(side_effect=RuntimeError("err"))
        with self.assertLogs("plaita", level="WARNING") as cm:
            node._get_node_state(execution)
        warn_msgs = [m for m in cm.output if "获取节点状态失败" in m]
        self.assertTrue(len(warn_msgs) >= 1)
        # mutmut_41: "XX获取节点状态失败: %sXX" — mangled
        self.assertNotIn("XX获取", warn_msgs[0])


# ---------------------------------------------------------------------------
# Round 3: on_event/on_timeout/on_error/on_cancel — execution context usage
# (kills mutmut_2: execution→None, mutmut_4: _get_node_state({}) single arg)
# ---------------------------------------------------------------------------

class TestOnEventExecutionContextUsage(unittest.TestCase):
    def test_on_event_uses_execution_get_node_state(self):
        """Kill mutmut_2: execution→None; existing state won't be returned.
        Kill mutmut_4: _get_node_state({}) — execution replaced by {}.
        """
        node = _make_node()
        # get_node_state returns an existing event_id that should be preserved
        execution = _make_execution(
            get_node_state_return={"event_id": "preserved-event-id-999", "status": "pending"}
        )
        result = node.on_event(execution, {"data": "something"})
        # Original: state starts as {"event_id": "preserved-event-id-999", ...}
        # Mutant (execution→None or {}): state starts as {}; no event_id
        self.assertIn("event_id", result)
        self.assertEqual(result["event_id"], "preserved-event-id-999")

    def test_on_timeout_uses_execution_get_node_state(self):
        """Kill mutmut_2 and mutmut_4 for on_timeout."""
        node = _make_node()
        execution = _make_execution(
            get_node_state_return={"event_id": "timeout-event-777", "status": "pending"}
        )
        result = node.on_timeout(execution)
        self.assertIn("event_id", result)
        self.assertEqual(result["event_id"], "timeout-event-777")

    def test_on_error_uses_execution_get_node_state(self):
        """Kill mutmut_2 and mutmut_4 for on_error."""
        node = _make_node()
        execution = _make_execution(
            get_node_state_return={"event_id": "error-event-555", "status": "pending"}
        )
        result = node.on_error(execution, "something went wrong")
        self.assertIn("event_id", result)
        self.assertEqual(result["event_id"], "error-event-555")

    def test_on_cancel_uses_execution_get_node_state(self):
        """Kill mutmut_2 and mutmut_4 for on_cancel."""
        node = _make_node()
        execution = _make_execution(
            get_node_state_return={"event_id": "cancel-event-333", "status": "pending"}
        )
        result = node.on_cancel(execution)
        self.assertIn("event_id", result)
        self.assertEqual(result["event_id"], "cancel-event-333")


# ---------------------------------------------------------------------------
# Round 3: resume dispatch — execution context usage
# (kills mutmut_2: on_cancel(None), mutmut_4: on_timeout(None),
#  mutmut_5: on_event(None, resume_data))
# ---------------------------------------------------------------------------

class TestResumeDispatchExecutionContext(unittest.TestCase):
    def test_resume_cancel_passes_execution_not_none(self):
        """Kill mutmut_2: on_cancel(None) — execution state not forwarded."""
        from plaita.core.errors import ResumeType
        node = _make_node()
        execution = _make_execution(
            get_node_state_return={"event_id": "resume-cancel-888", "status": "pending"}
        )
        result = node.resume(execution, ResumeType.CANCEL)
        self.assertEqual(result["status"], "cancelled")
        # execution context should be forwarded — event_id preserved
        self.assertIn("event_id", result)
        self.assertEqual(result["event_id"], "resume-cancel-888")

    def test_resume_timeout_passes_execution_not_none(self):
        """Kill mutmut_4: on_timeout(None) — execution state not forwarded."""
        from plaita.core.errors import ResumeType
        node = _make_node()
        execution = _make_execution(
            get_node_state_return={"event_id": "resume-timeout-444", "status": "pending"}
        )
        result = node.resume(execution, ResumeType.TIMEOUT)
        self.assertEqual(result["status"], "timeout")
        self.assertIn("event_id", result)
        self.assertEqual(result["event_id"], "resume-timeout-444")

    def test_resume_event_passes_execution_not_none(self):
        """Kill mutmut_5: on_event(None, resume_data) — execution state not forwarded."""
        from plaita.core.errors import ResumeType
        node = _make_node()
        execution = _make_execution(
            get_node_state_return={"event_id": "resume-event-222", "status": "pending"}
        )
        result = node.resume(execution, ResumeType.EVENT, {"payload": "x"})
        self.assertEqual(result["status"], "completed")
        # execution context should be forwarded
        self.assertIn("event_id", result)
        self.assertEqual(result["event_id"], "resume-event-222")


if __name__ == "__main__":
    unittest.main()
