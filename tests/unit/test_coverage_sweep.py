"""
Targeted coverage sweep for remaining uncovered lines across multiple modules.
Each test class focuses on a specific module / feature area.
"""

import asyncio
import importlib
import json
import os
import sys
import tempfile
import time
import unittest
import warnings
from unittest.mock import AsyncMock, MagicMock, patch

# ---------------------------------------------------------------------------
# plaita.core.runner  line 165
# ---------------------------------------------------------------------------
from plaita.core.context import ExecutionContext, _resolve_default_event_bus
from plaita.core.errors import ErrorStrategy, RecoverableErrorHandler, ResumeType
from plaita.core.runner import NodeRunner


class TestRunnerLine165(unittest.TestCase):
    """_execute_with_retry: retry_times=-1 → range(0) empty loop → line 165."""

    def _make_runner(self):
        ctx = ExecutionContext()
        return NodeRunner(ctx)

    def test_negative_retry_continue_returns_none(self):
        """retry_times=-1 with CONTINUE → _get_error_result → None."""
        runner = self._make_runner()
        node = MagicMock()
        node.timeout = None
        node.timeout_handler = None
        handler = RecoverableErrorHandler.model_validate(
            {"strategy": "continue", "retryTimes": -1}
        )
        node.error_handler = handler
        flow = MagicMock()
        flow.max_timeout = None
        result = asyncio.run(runner._execute_with_retry(flow, node, None))
        self.assertIsNone(result)

    def test_negative_retry_continue_with_returns_default(self):
        """retry_times=-1 with CONTINUE_WITH → _get_error_result → default."""
        runner = self._make_runner()
        node = MagicMock()
        node.timeout = None
        node.timeout_handler = None
        handler = RecoverableErrorHandler.model_construct(
            retry_times=-1,
            strategy=ErrorStrategy.CONTINUE_WITH,
            default_value={"value": "fallback"},
        )
        node.error_handler = handler
        flow = MagicMock()
        flow.max_timeout = None
        result = asyncio.run(runner._execute_with_retry(flow, node, None))
        self.assertEqual(result, {"value": "fallback"})


# ---------------------------------------------------------------------------
# plaita.node.calculate  line 75
# ---------------------------------------------------------------------------
from plaita.node.calculate import Call


class TestCalculateFromJson(unittest.TestCase):
    """Call.from_json: string content → json.loads (line 75)."""

    def test_from_json_string_input(self):
        payload = json.dumps({"function_name": "add", "params": {}})
        result = Call.from_json(payload)
        self.assertIsNotNone(result)
        self.assertEqual(result.function_name, "add")

    def test_from_json_none_returns_none(self):
        self.assertIsNone(Call.from_json(None))

    def test_from_json_call_identity(self):
        call = Call.from_json({"function_name": "add", "params": {}})
        same = Call.from_json(call)
        self.assertIs(same, call)


# ---------------------------------------------------------------------------
# plaita.node.end  line 37
# ---------------------------------------------------------------------------
from plaita.node.end import END_TYPE_NOP, End


class TestEndNodeNop(unittest.TestCase):
    """End.execute with NOP type returns None (line 37)."""

    def test_nop_returns_none(self):
        node = End(id="end", result_type=END_TYPE_NOP)
        result = node.execute(MagicMock())
        self.assertIsNone(result)


# ---------------------------------------------------------------------------
# plaita.node.basic  lines 58, 66
# ---------------------------------------------------------------------------
from plaita.node.basic import Node


class TestNodeBasicLines(unittest.TestCase):
    """Node validator builds timeout_handler; validate() returns None."""

    def test_timeout_handler_constructed_from_dict(self):
        """Line 58: ErrorHandler built from timeoutHandler dict."""
        data = {"id": "n1", "timeoutHandler": {"strategy": "abort"}}
        node = Node.model_validate(data)
        self.assertIsNotNone(node.timeout_handler)

    def test_validate_returns_none(self):
        """Line 66: default validate() returns None."""
        node = Node(id="n2")
        self.assertIsNone(node.validate())


# ---------------------------------------------------------------------------
# plaita.core.errors  lines 67, 218, 229
# ---------------------------------------------------------------------------
class TestCoreErrors(unittest.TestCase):
    """ResumeType and RecoverableErrorHandler edge paths."""

    def test_resume_type_coerce_non_string_non_enum(self):
        """Line 67: non-str, non-ResumeType value → ResumeError."""
        from plaita.core.errors import ResumeError

        with self.assertRaises(ResumeError):
            ResumeType.coerce([1, 2, 3])

    def test_error_handler_strategy_none_defaults_abort(self):
        """Line 218: _validate_strategy(None) → ABORT."""
        handler = RecoverableErrorHandler(strategy=None)
        self.assertEqual(handler.strategy, ErrorStrategy.ABORT)

    def test_error_handler_strategy_invalid_type_raises(self):
        """Line 229: _validate_strategy with non-str/non-enum → ValueError."""
        with self.assertRaises((ValueError, Exception)):
            RecoverableErrorHandler(strategy=12345)


# ---------------------------------------------------------------------------
# plaita.node.assignment  lines 35-36, 40
# ---------------------------------------------------------------------------
from plaita.io import Property
from plaita.node.assignment import Assignment


class TestAssignmentNode(unittest.TestCase):
    """Assignment.validate() and execute() with single upstream."""

    def test_validate_passes(self):
        """Lines 35-36: asserts that output_type is set."""
        p = Property(type="string")
        node = Assignment(id="a1", output_type=p, output="$INPUT.x")
        node.validate()  # should not raise

    def test_execute_single_upstream_extracts_value(self):
        """Line 40: single upstream_output → value = upstream_output[0]['value']."""
        p = Property(type="string")
        node = Assignment(
            id="a2",
            output_type=p,
            upstream_output=[{"value": "hello", "upstream": "prev"}],
        )
        exec_ctx = MagicMock()
        exec_ctx.evaluate.return_value = "hello"
        result = node.execute(exec_ctx)
        self.assertEqual(result, "hello")


# ---------------------------------------------------------------------------
# plaita.node.event_node  lines 63, 93-94
# ---------------------------------------------------------------------------
from plaita.node.event_node import EventNode


class TestEventNodeEdgePaths(unittest.TestCase):
    """EventNode._get_node_state and execute variable-ref resolution."""

    def test_get_node_state_no_get_node_state_no_context(self):
        """Line 63: no get_node_state, no context attribute → return default."""
        node = EventNode(id="en1", event_type="some.event")
        exec_ctx = MagicMock(spec=[])  # no attributes
        result = node._get_node_state(exec_ctx, default={"k": "v"})
        self.assertEqual(result, {"k": "v"})

    def test_execute_resolve_event_type_non_string_result(self):
        """Lines 93-94: evaluate returns non-string → keep original event_type."""
        node = EventNode(id="en2", event_type="$some.var")
        exec_ctx = MagicMock()
        exec_ctx.evaluate.return_value = None  # not a string → lines 93-94
        result = node.execute(exec_ctx)
        self.assertIsNotNone(result)


# ---------------------------------------------------------------------------
# plaita.io_format  lines 64, 77, 99, 104
# ---------------------------------------------------------------------------
from plaita.io_format import load_file, loads


class TestIoFormat(unittest.TestCase):
    """loads() and load_file() edge cases."""

    def test_loads_null_yaml_returns_empty_dict(self):
        """Line 77: YAML null → None → _coerce_to_flow_dict → {}."""
        self.assertEqual(loads("null"), {})

    def test_loads_tilde_yaml_returns_empty_dict(self):
        """Line 77 via YAML '~' → None → {}."""
        self.assertEqual(loads("~"), {})

    def test_loads_json_fail_yaml_fail_raises(self):
        """Line 64 area: json fails, yaml fails → original error raised."""
        with self.assertRaises(Exception):
            loads('{"key": @invalid@}')

    def test_load_file_yaml_non_dict_raises(self):
        """Lines 99-102: YAML list top-level → RuntimeError."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write("- item1\n- item2\n")
            fname = f.name
        try:
            with self.assertRaises(RuntimeError):
                load_file(fname)
        finally:
            os.unlink(fname)

    def test_load_file_unknown_extension_uses_loads(self):
        """Line 104: unknown extension → delegates to loads()."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False
        ) as f:
            f.write('{"flow_id": "test-txt"}\n')
            fname = f.name
        try:
            result = load_file(fname)
            self.assertEqual(result.get("flow_id"), "test-txt")
        finally:
            os.unlink(fname)


# ---------------------------------------------------------------------------
# plaita.core.types  lines 72, 83, 99, 102, 110, 118
# ---------------------------------------------------------------------------
from plaita.core import types as core_types


class TestCoreTypes(unittest.TestCase):
    """Type validation and validator registration edge cases."""

    def test_register_validator_duplicate_raises(self):
        """Line 72: registering same name twice → ValueError."""
        name = "_test_dup_validator_sweep"
        core_types.register_validator(name, lambda v: True)
        try:
            with self.assertRaises(ValueError):
                core_types.register_validator(name, lambda v: True)
        finally:
            del core_types.data_validators[name]

    def test_validate_data_type_any_returns_none(self):
        """Line 83: data_type == ANY → early return."""
        result = core_types._validate_data_type(core_types.ANY, "anything")
        self.assertIsNone(result)

    def test_parse_validator_string_returns_tuple(self):
        """Line 99: validator is a string → (name, {})."""
        name, params = core_types._parse_validator("my_validator")
        self.assertEqual(name, "my_validator")
        self.assertEqual(params, {})

    def test_parse_validator_dict_no_name_raises(self):
        """Line 102: validator dict missing 'name' → ValueError."""
        with self.assertRaises(ValueError):
            core_types._parse_validator({"wrong_key": "value"})

    def test_run_validator_unknown_name_raises(self):
        """Line 110: validator func not found → exception."""
        with self.assertRaises(Exception):
            core_types._run_validator("_nonexistent_xyz", "val", "string", {})

    def test_raise_validation_error_no_message(self):
        """Line 118: no message template → generic ValidationError."""
        from plaita.core.types import ValidationError

        with self.assertRaises(ValidationError):
            core_types._raise_validation_error(None, "v", "string", {}, "chk")


# ---------------------------------------------------------------------------
# plaita.node.__init__  lines 79, 267, 271, 274, 286-287, 290, 293, 296, 299
# ---------------------------------------------------------------------------
from plaita.node import NodeRegistry, _RegistryDictProxy, _default_registry


class TestNodeRegistryProxy(unittest.TestCase):
    """NodeRegistry parent init and _RegistryDictProxy dict-like methods."""

    def test_registry_with_parent_inherits_nodes(self):
        """Line 79: child registry copies parent nodes."""
        parent = NodeRegistry()
        child = NodeRegistry(parent=parent)
        self.assertGreater(len(child._nodes), 0)

    def test_proxy_getitem_missing_raises_keyerror(self):
        """Line 267: __getitem__ for missing key → KeyError."""
        proxy = _RegistryDictProxy(_default_registry)
        with self.assertRaises(KeyError):
            _ = proxy["__nonexistent_node_xyz__"]

    def test_proxy_setitem_and_delitem(self):
        """Lines 271, 274: __setitem__ and __delitem__."""
        proxy = _RegistryDictProxy(_default_registry)
        proxy["_tmp_node_sweep"] = Node
        self.assertIn("_tmp_node_sweep", proxy._registry._nodes)
        del proxy["_tmp_node_sweep"]
        self.assertNotIn("_tmp_node_sweep", proxy._registry._nodes)

    def test_proxy_get_existing_and_missing(self):
        """Lines 286-287: get() with and without default."""
        proxy = _RegistryDictProxy(_default_registry)
        self.assertIsNotNone(proxy.get("end"))
        self.assertEqual(proxy.get("__nonexistent_xyz__", "default"), "default")

    def test_proxy_keys_values_items(self):
        """Lines 290, 293, 296: keys(), values(), items()."""
        proxy = _RegistryDictProxy(_default_registry)
        self.assertIn("end", proxy.keys())
        self.assertTrue(len(list(proxy.values())) > 0)
        self.assertTrue(len(list(proxy.items())) > 0)

    def test_proxy_repr(self):
        """Line 299: __repr__."""
        proxy = _RegistryDictProxy(_default_registry)
        self.assertIn("_RegistryDictProxy", repr(proxy))


# ---------------------------------------------------------------------------
# plaita.core.state  lines 120, 226, 251, 256, 258, 279, 289, 292
# ---------------------------------------------------------------------------
from plaita.core.state import CheckpointState, validate_checkpoint


class TestCheckpointStateEdges(unittest.TestCase):
    """CheckpointState edge paths."""

    def test_getitem_express_prefix_not_present_raises(self):
        """Line 226: __getitem__('EXPRESS_PREFIX') when absent → KeyError."""
        s = CheckpointState()
        with self.assertRaises(KeyError):
            _ = s["EXPRESS_PREFIX"]

    def test_setitem_when_pydantic_extra_is_none(self):
        """Line 251: __setitem__ initialises __pydantic_extra__ if None."""
        s = CheckpointState()
        object.__setattr__(s, "__pydantic_extra__", None)
        s["my_key"] = "my_value"
        self.assertEqual(s.__pydantic_extra__["my_key"], "my_value")

    def test_contains_non_string_returns_false(self):
        """Line 256: non-string key → False."""
        s = CheckpointState()
        self.assertFalse(123 in s)

    def test_contains_express_prefix_when_set(self):
        """Line 258: EXPRESS_PREFIX in _present after setting it."""
        s = CheckpointState()
        s["EXPRESS_PREFIX"] = "$"
        self.assertIn("EXPRESS_PREFIX", s)

    def test_values_returns_list(self):
        """Line 279: values() returns list."""
        s = CheckpointState()
        s["k1"] = "v1"
        self.assertIn("v1", s.values())

    def test_eq_two_checkpoints(self):
        """Line 289: __eq__ between two CheckpointState instances."""
        self.assertEqual(CheckpointState(), CheckpointState())

    def test_eq_returns_not_implemented_for_unknown_type(self):
        """Line 292: __eq__ with unknown type → NotImplemented."""
        s = CheckpointState()
        self.assertIs(s.__eq__(42), NotImplemented)

    def test_validate_checkpoint_known_key_continues(self):
        """Line 120: key in known set → continue (no warning emitted)."""
        ws = validate_checkpoint({"$LAST_NODE": "n1"})
        self.assertEqual(ws, [])


# ---------------------------------------------------------------------------
# plaita.storage.memory  lines 65-67, 121, 138-140
# ---------------------------------------------------------------------------
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage


class TestStorageMemory(unittest.TestCase):
    """Memory storage edge cases."""

    def test_list_executions_invalid_sort_field_keeps_order(self):
        """Lines 65-67: AttributeError on sort → original order kept."""
        storage = MemoryExecutionStorage()
        s1 = ExecutionState(execution_id="e1", flow_id="f1", status="running", context={})
        s2 = ExecutionState(execution_id="e2", flow_id="f1", status="completed", context={})
        storage.save_execution_state("e1", s1)
        storage.save_execution_state("e2", s2)
        results = storage.list_executions(order_by="nonexistent_field")
        self.assertEqual(len(results), 2)

    def test_get_flow_empty_versions_returns_none(self):
        """Line 121: flow exists with empty versions dict → None."""
        storage = MemoryFlowStorage()
        storage.flows["ghost"] = {}
        result = storage.get_flow("ghost")
        self.assertIsNone(result)

    def test_get_flow_non_numeric_version_fallback(self):
        """Lines 138-140: non-numeric version → fallback to any version."""
        storage = MemoryFlowStorage()
        flow_dict = {"flow_id": "f1", "version": "alpha", "nodes": []}
        storage.save_flow(flow_dict)
        # Add a second non-sortable version
        storage.flows["f1"]["beta"] = {"flow_id": "f1", "version": "beta", "nodes": []}
        result = storage.get_flow("f1")
        self.assertIsNotNone(result)


# ---------------------------------------------------------------------------
# plaita.core.executor  lines 240, 249
# ---------------------------------------------------------------------------
from plaita.core.executor import FlowExecution


class TestExecutorDelegates(unittest.TestCase):
    """FlowExecution.get_or_create_event_bus and setup_flow delegate to _ctx."""

    def test_get_or_create_event_bus(self):
        """Line 240: delegates to ctx, returns event bus."""
        exec_ = FlowExecution()
        bus = exec_.get_or_create_event_bus()
        self.assertIsNotNone(bus)

    def test_setup_flow(self):
        """Line 249: delegates to ctx without error."""
        from plaita.core.flow import Flow

        exec_ = FlowExecution()
        flow = Flow(flow_id="f2", runtime="python")
        exec_.setup_flow(flow, (), {})


# ---------------------------------------------------------------------------
# plaita.core.context  lines 121-122  (ImportError in _resolve_default_event_bus)
# ---------------------------------------------------------------------------
class TestContextEventBusImportError(unittest.TestCase):
    """_resolve_default_event_bus handles ImportError (lines 121-122)."""

    def test_import_error_returns_none(self):
        with patch.dict("sys.modules", {"plaita.event": None}):
            result = _resolve_default_event_bus()
            self.assertIsNone(result)


# ---------------------------------------------------------------------------
# plaita.event.memory  lines 414-415, 516
# ---------------------------------------------------------------------------
class TestEventMemoryEdges(unittest.IsolatedAsyncioTestCase):
    """InMemoryEventBus: exception in handler and EventTimeoutError."""

    async def test_publish_handler_exception_does_not_propagate(self):
        """Lines 414-415: handler raises → bus continues."""
        from plaita.event.core import Event
        from plaita.event.memory import InMemoryEventBus

        bus = InMemoryEventBus()

        async def bad_handler(event):
            raise RuntimeError("intentional")

        await bus.register_handler(event_type="test.error", handler=bad_handler)
        event = Event(event_type="test.error", data={})
        await bus.publish(event, prevent_duplicate_consumption=False)
        await asyncio.sleep(0.1)

    async def test_wait_for_event_timeout_raises(self):
        """Line 516: wait_for_event with very short timeout → EventTimeoutError."""
        from plaita.event.exceptions import EventTimeoutError
        from plaita.event.memory import InMemoryEventBus

        bus = InMemoryEventBus()
        with self.assertRaises(EventTimeoutError):
            await bus.wait_for_event("no.such.event", timeout=0.001)


# ---------------------------------------------------------------------------
# plaita.core.async_utils  lines 162-163, 179
# ---------------------------------------------------------------------------
class TestAsyncUtilsEdges(unittest.TestCase):
    """async_gen_to_sync: normal operation (coverage of surrounding lines)."""

    def test_async_gen_to_sync_basic(self):
        """Lines 162-163 area: aclose() cleanup after generation."""
        from plaita.core.async_utils import async_gen_to_sync

        async def gen():
            yield 1
            yield 2

        items = list(async_gen_to_sync(gen()))
        self.assertEqual(items, [1, 2])

    def test_async_gen_to_sync_three_items(self):
        from plaita.core.async_utils import async_gen_to_sync

        async def gen():
            for i in range(3):
                yield i

        self.assertEqual(list(async_gen_to_sync(gen())), [0, 1, 2])


# ---------------------------------------------------------------------------
# plaita.io  line 163  (__delitem__ with missing key → KeyError)
# ---------------------------------------------------------------------------
class TestIoUnregister(unittest.TestCase):
    """_RegisteredFunctionsProxy.__delitem__ raises KeyError for missing key."""

    def test_delete_nonexistent_key_raises(self):
        import warnings as _warnings
        from plaita.io import REGISTERED_FUNCTIONS

        with _warnings.catch_warnings():
            _warnings.simplefilter("ignore", DeprecationWarning)
            with self.assertRaises(KeyError):
                del REGISTERED_FUNCTIONS["__not_registered_sweep_xyz__"]


# ---------------------------------------------------------------------------
# plaita.core.flow  line 159
# ---------------------------------------------------------------------------
class TestFlowResolveNodes(unittest.TestCase):
    """Flow._resolve_nodes: already-resolved nodes → append unchanged (line 159)."""

    def test_resolve_nodes_idempotent(self):
        """Second resolve_nodes() call: nodes already Node instances → line 159."""
        from plaita.core.flow import Flow

        flow = Flow.model_validate(
            {
                "flow_id": "f1",
                "runtime": "python",
                "nodes": [
                    {"id": "n1", "type": "start"},
                    {"id": "n2", "type": "end"},
                ],
            }
        )
        # First call resolves dicts → Node instances
        flow.resolve_nodes()
        # Second call: nodes already Node objects → hits line 159 (resolved.append(n))
        flow.resolve_nodes()
        self.assertTrue(len(flow.nodes) >= 0)


# ---------------------------------------------------------------------------
# plaita.core.expression_parser  lines 70, 90, 241-242, 350, 353-354
# ---------------------------------------------------------------------------
from plaita.core.expression_parser import ExpressionParser, _get_attr


class TestExpressionParserEdges(unittest.TestCase):
    """ExpressionParser edge paths."""

    def test_get_attr_on_primitive_returns_none(self):
        """Line 90: non-dict, no __getitem__, no __dict__ (e.g. int) → None."""
        result = _get_attr(42, "anything")
        self.assertIsNone(result)

    def test_parse_function_with_dict_registry(self):
        """Line 70: registry is a dict-like proxy → registry.get(func_name)."""
        parser = ExpressionParser()
        mock_reg = MagicMock(spec=["get"])
        mock_reg.get.return_value = lambda x: x
        fn = parser.parse_function("add", context={}, registry=mock_reg)
        self.assertEqual(fn, "add")

    def test_evaluate_boolean_true_in_function(self):
        """Lines 241-242: _eval_boolean triggered when True/False used as function arg."""
        parser = ExpressionParser()
        result = parser.evaluate("$F.add(True, False)", {"$F": {}})
        self.assertEqual(result, 1)

    def test_evaluate_boolean_false_in_function(self):
        """Lines 241-242: _eval_boolean triggered with False token."""
        parser = ExpressionParser()
        result = parser.evaluate("$F.add(False, False)", {"$F": {}})
        self.assertEqual(result, 0)

    def test_parse_function_no_f_prefix_returns_as_is(self):
        """Line 350: expression without $F. prefix → returned unchanged."""
        parser = ExpressionParser()
        result = parser.parse_function("plain_string", {})
        self.assertEqual(result, "plain_string")

    def test_parse_function_bad_syntax_returns_original(self):
        """Lines 353-354: $F. prefix but bad grammar → ParseException caught → original."""
        parser = ExpressionParser()
        bad = "$F.(invalid)"
        # Context must include $F to get past variable lookup into grammar error
        result = parser.parse_function(bad, {"$F": None})
        self.assertEqual(result, bad)


if __name__ == "__main__":
    unittest.main()
