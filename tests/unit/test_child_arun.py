"""Tests for async (arun) paths of child.py - InlineFlow and ReferenceFlow.

Covers FlowNode.setup_child_flow validator (string / dict / missing),
InlineFlow.execute / arun, and ReferenceFlow behaviour.

Note: ReferenceFlow.setup_child_flow intentionally ignores childFlow in the
dict — child_flow must be injected by an orchestrator after parsing.
"""

from __future__ import annotations

import json
import unittest
from typing import Any

from plaita import Flow


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SIMPLE_CHILD = {
    "id": "cf", "version": "1", "runtime": "python",
    # ReferenceFlow.execute calls match(child_flow.input_property, ...) without
    # a None guard, so inputType must be provided.
    "inputType": {"dataType": "any"},
    "nodes": [
        {"type": "start", "id": "s", "next": "e"},
        {"type": "end", "id": "e", "resultType": "success", "output": "$INPUT"},
    ],
}


def _make_inline_flow(child_flow_value: Any) -> Flow:
    """Build a flow with an InlineFlow (type=child) node."""
    return Flow.model_validate({
        "flow_id": "test", "version": "1", "runtime": "python",
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "child_node"},
            {
                "type": "child", "id": "child_node",
                "input": "$INPUT.value",
                "childFlow": child_flow_value,
                "next": "end",
            },
            {"type": "end", "id": "end", "resultType": "success", "output": "$NODE.child_node"},
        ],
    })


def _make_reference_flow_with_injection() -> tuple[Flow, Any]:
    """Build a flow with a ReferenceFlow node (type=reference).

    ReferenceFlow ignores 'childFlow' in the dict; child_flow must be
    injected into the node after parsing. Returns (flow, ref_node).
    """
    flow = Flow.model_validate({
        "flow_id": "outer", "version": "1", "runtime": "python",
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "ref_node"},
            {
                "type": "reference", "id": "ref_node",
                "flowID": "inner_flow",
                "input": "$INPUT.value",
                "next": "end",
            },
            {"type": "end", "id": "end", "resultType": "success", "output": "$NODE.ref_node"},
        ],
    })
    ref_node = next(n for n in flow.nodes if getattr(n, "node_type", None) == "reference")
    child = Flow.model_validate(_SIMPLE_CHILD)
    ref_node.child_flow = child
    return flow, ref_node


# ---------------------------------------------------------------------------
# FlowNode.setup_child_flow validator paths
# ---------------------------------------------------------------------------

class TestFlowNodeSetupChildFlow(unittest.TestCase):
    def test_child_flow_as_dict(self):
        """childFlow passed as dict is parsed into a Flow object."""
        flow = _make_inline_flow(_SIMPLE_CHILD)
        result = flow.run(value="hello")
        self.assertEqual(result, "hello")

    def test_child_flow_as_json_string(self):
        """childFlow passed as JSON string is parsed via model_validate_json."""
        flow = _make_inline_flow(json.dumps(_SIMPLE_CHILD))
        result = flow.run(value=42)
        self.assertEqual(result, 42)

    def test_missing_child_flow_raises(self):
        """InlineFlow (type=child) without childFlow raises an error at parse time."""
        with self.assertRaises(Exception):
            Flow.model_validate({
                "flow_id": "test", "version": "1", "runtime": "python",
                "nodes": [
                    {"type": "start", "id": "start", "next": "child_node"},
                    {"type": "child", "id": "child_node", "next": "end"},
                    {"type": "end", "id": "end", "resultType": "success", "output": "x"},
                ],
            })


# ---------------------------------------------------------------------------
# InlineFlow.execute (synchronous)
# ---------------------------------------------------------------------------

class TestInlineFlowExecute(unittest.TestCase):
    def test_execute_returns_child_result(self):
        """InlineFlow.execute propagates the input to the child flow and returns its result."""
        flow = _make_inline_flow(_SIMPLE_CHILD)
        result = flow.run(value="world")
        self.assertEqual(result, "world")

    def test_execute_with_integer_input(self):
        flow = _make_inline_flow(_SIMPLE_CHILD)
        result = flow.run(value=99)
        self.assertEqual(result, 99)

    def test_execute_with_dict_input(self):
        flow = _make_inline_flow(_SIMPLE_CHILD)
        result = flow.run(value={"key": "val"})
        self.assertEqual(result, {"key": "val"})


# ---------------------------------------------------------------------------
# InlineFlow.arun (async)
# ---------------------------------------------------------------------------

class TestInlineFlowArun(unittest.IsolatedAsyncioTestCase):
    async def test_arun_returns_child_result(self):
        flow = _make_inline_flow(_SIMPLE_CHILD)
        result = await flow.arun(value="async_test")
        self.assertEqual(result, "async_test")

    async def test_arun_with_integer_input(self):
        flow = _make_inline_flow(_SIMPLE_CHILD)
        result = await flow.arun(value=123)
        self.assertEqual(result, 123)

    async def test_arun_with_dict_input(self):
        flow = _make_inline_flow(_SIMPLE_CHILD)
        result = await flow.arun(value={"a": 1})
        self.assertEqual(result, {"a": 1})


# ---------------------------------------------------------------------------
# ReferenceFlow — validator and execution
# ---------------------------------------------------------------------------

class TestReferenceFlowValidator(unittest.TestCase):
    def test_reference_parses_without_child_flow(self):
        """ReferenceFlow ignores childFlow — child_flow is None after parse."""
        flow = Flow.model_validate({
            "flow_id": "outer", "version": "1", "runtime": "python",
            "nodes": [
                {"type": "start", "id": "start", "next": "ref"},
                {
                    "type": "reference", "id": "ref",
                    "flowID": "my-flow",
                    "next": "end",
                },
                {"type": "end", "id": "end", "resultType": "success", "output": "$NODE.ref"},
            ],
        })
        ref_node = next(n for n in flow.nodes if getattr(n, "node_type", None) == "reference")
        self.assertIsNone(ref_node.child_flow)
        # flow_id is set in the validator via values["flow_id"] but may not be
        # a declared field; we just verify child_flow is None (not injected yet)
        self.assertIsNone(ref_node.child_flow)

    def test_reference_without_child_flow_raises_on_execute(self):
        """Executing ReferenceFlow without injected child_flow raises AssertionError."""
        flow = Flow.model_validate({
            "flow_id": "outer", "version": "1", "runtime": "python",
            "nodes": [
                {"type": "start", "id": "start", "next": "ref"},
                {
                    "type": "reference", "id": "ref",
                    "flowID": "my-flow",
                    "next": "end",
                },
                {"type": "end", "id": "end", "resultType": "success", "output": "$NODE.ref"},
            ],
        })
        with self.assertRaises(Exception):
            flow.run(value="test")


class TestReferenceFlowExecute(unittest.TestCase):
    def test_execute_after_child_flow_injection(self):
        """After injecting child_flow, ReferenceFlow.execute works."""
        flow, _ = _make_reference_flow_with_injection()
        result = flow.run(value="ref_test")
        self.assertEqual(result, "ref_test")

    def test_execute_with_integer_input(self):
        flow, _ = _make_reference_flow_with_injection()
        result = flow.run(value=777)
        self.assertEqual(result, 777)


class TestReferenceFlowArun(unittest.IsolatedAsyncioTestCase):
    async def test_arun_after_child_flow_injection(self):
        """After injecting child_flow, ReferenceFlow.arun works."""
        flow, _ = _make_reference_flow_with_injection()
        result = await flow.arun(value="async_ref")
        self.assertEqual(result, "async_ref")

    async def test_arun_with_integer_input(self):
        flow, _ = _make_reference_flow_with_injection()
        result = await flow.arun(value=999)
        self.assertEqual(result, 999)


if __name__ == "__main__":
    unittest.main()
