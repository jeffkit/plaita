"""Direct unit tests for the sync execute() paths of loop.py collection nodes.

In the current architecture, NodeRunner always calls node.arun() when the node
exposes an async arun() method. The sync execute() methods are therefore
dead code for the normal execution pipeline — but they remain part of the public
API and must stay regression-covered.

Strategy: call node.execute(mock_ctx) directly with a thin mock execution
context so we don't pull in the full async pipeline.
"""

from __future__ import annotations

import unittest
from copy import deepcopy
from typing import Any
from unittest.mock import MagicMock

from plaita import Flow


# ---------------------------------------------------------------------------
# Helpers — build a minimal mock execution context
# ---------------------------------------------------------------------------

def _make_ctx(collection, child_return_fn=None, express_prefix="$"):
    """Build a mock ExecutionContext.

    Args:
        collection: the list returned by ctx.evaluate(self.collection)
        child_return_fn: callable(item, index) → value for child run_compatible
        express_prefix: prefix for loop context keys
    """
    ctx = MagicMock()
    ctx.express_prefix = express_prefix
    ctx.context = {}
    ctx.evaluate.side_effect = lambda expr: collection

    def get_child():
        child = MagicMock()
        if child_return_fn is not None:
            child.run_compatible.side_effect = lambda flow, is_async, **kw: child_return_fn(
                kw.get("item"), kw.get("index")
            )
        else:
            child.run_compatible.return_value = None
        return child

    ctx.get_child_execution.side_effect = get_child
    return ctx


def _double(item, index):
    return item * 2


def _gt2(item, index):
    return item if item > 2 else None


def _sum(first=None, second=None):
    return first + second


def _child_flow_stub():
    """Minimal flow dict — the mock ctx ignores it anyway."""
    return {
        "id": "cf", "version": "1", "runtime": "python",
        "nodes": [
            {"type": "start", "id": "s", "next": "e"},
            {"type": "end", "id": "e", "resultType": "success", "output": "$INPUT"},
        ],
    }


def _make_node(node_type: str, extra: dict = None):
    """Parse a standalone node dict through Flow.model_validate to get a real node object."""
    node_dict = {"type": node_type, "id": "n", "collection": "$INPUT.items",
                 "childFlow": _child_flow_stub(), "next": "end"}
    if extra:
        node_dict.update(extra)
    flow = Flow.model_validate({
        "flow_id": "f", "version": "1", "runtime": "python",
        "nodes": [
            {"type": "start", "id": "start", "next": "n"},
            node_dict,
            {"type": "end", "id": "end", "resultType": "success", "output": "$NODE.n"},
        ],
    })
    return next(n for n in flow.nodes if getattr(n, "id", None) == "n")


# ---------------------------------------------------------------------------
# Loop.execute (sync path)
# ---------------------------------------------------------------------------

class TestLoopExecuteSync(unittest.TestCase):
    def test_basic_loop(self):
        """Lines 54-70: Loop.execute iterates and returns last result."""
        node = _make_node("loop")
        ctx = _make_ctx([1, 2, 3], child_return_fn=_double)
        result = node.execute(ctx)
        self.assertEqual(result, 6)  # last item 3 → 3*2

    def test_empty_collection_returns_none(self):
        node = _make_node("loop")
        ctx = _make_ctx([], child_return_fn=_double)
        result = node.execute(ctx)
        self.assertIsNone(result)

    def test_single_item(self):
        node = _make_node("loop")
        ctx = _make_ctx([7], child_return_fn=_double)
        result = node.execute(ctx)
        self.assertEqual(result, 14)

    def test_condition_breaks_early(self):
        """Loop condition: continue while LOOP-RESULT < 8.
        items=[1,2,3,4,5]; doubled=[2,4,6,8,...]; breaks at result=8.
        """
        node = _make_node("loop", extra={
            "condition": {"field": "$LOOP-RESULT", "operator": "lt", "value": 8},
        })
        ctx = _make_ctx([1, 2, 3, 4, 5], child_return_fn=_double)
        result = node.execute(ctx)
        self.assertEqual(result, 8)


# ---------------------------------------------------------------------------
# Map.execute (sync path) + _build_executor
# ---------------------------------------------------------------------------

class TestMapExecuteSync(unittest.TestCase):
    def test_sequential_map(self):
        """Lines 125-140: Map.execute sequential (concurrent=False)."""
        node = _make_node("map")
        ctx = _make_ctx([1, 2, 3], child_return_fn=_double)
        result = node.execute(ctx)
        self.assertEqual(result, [2, 4, 6])

    def test_concurrent_map(self):
        """Lines 120-122: _build_executor returns ThreadParallelExecutor."""
        node = _make_node("map", extra={"concurrent": True})
        ctx = _make_ctx([5, 10], child_return_fn=_double)
        result = node.execute(ctx)
        self.assertEqual(sorted(result), [10, 20])

    def test_map_empty_collection(self):
        node = _make_node("map")
        ctx = _make_ctx([], child_return_fn=_double)
        result = node.execute(ctx)
        self.assertEqual(result, [])


# ---------------------------------------------------------------------------
# Filter.execute (sync path)
# ---------------------------------------------------------------------------

class TestFilterExecuteSync(unittest.TestCase):
    def test_filter_keeps_truthy(self):
        """Lines 180-187: Filter.execute keeps items where child returns truthy."""
        node = _make_node("filter")
        ctx = _make_ctx([1, 2, 3, 4, 5], child_return_fn=_gt2)
        result = node.execute(ctx)
        self.assertEqual(result, [3, 4, 5])

    def test_filter_empty(self):
        node = _make_node("filter")
        ctx = _make_ctx([], child_return_fn=_gt2)
        result = node.execute(ctx)
        self.assertEqual(result, [])

    def test_filter_none_pass(self):
        node = _make_node("filter")
        ctx = _make_ctx([0, 1, 2], child_return_fn=_gt2)
        result = node.execute(ctx)
        self.assertEqual(result, [])


# ---------------------------------------------------------------------------
# Find.execute (sync path)
# ---------------------------------------------------------------------------

class TestFindExecuteSync(unittest.TestCase):
    def test_find_returns_first_match(self):
        """Lines 212-218: Find.execute returns first truthy item."""
        node = _make_node("find")
        ctx = _make_ctx([1, 3, 4], child_return_fn=_gt2)
        result = node.execute(ctx)
        self.assertEqual(result, 3)

    def test_find_not_found_returns_none(self):
        node = _make_node("find")
        ctx = _make_ctx([0, 1, 2], child_return_fn=_gt2)
        result = node.execute(ctx)
        self.assertIsNone(result)

    def test_find_empty_returns_none(self):
        node = _make_node("find")
        ctx = _make_ctx([], child_return_fn=_gt2)
        result = node.execute(ctx)
        self.assertIsNone(result)


# ---------------------------------------------------------------------------
# Reduce.execute (sync path) + _child_is_array_input attr path
# ---------------------------------------------------------------------------

class TestReduceExecuteSync(unittest.TestCase):
    def _make_reduce_ctx(self, items, child_fn):
        """Build a reduce ctx where run_compatible uses positional args for array input."""
        ctx = MagicMock()
        ctx.express_prefix = "$"
        ctx.context = {}
        ctx.evaluate.side_effect = lambda expr: items

        def get_child():
            child = MagicMock()
            child.run_compatible.side_effect = lambda flow, is_async, *args, **kwargs: (
                child_fn(*args, **kwargs)
            )
            return child

        ctx.get_child_execution.side_effect = get_child
        return ctx

    def test_reduce_object_input(self):
        """Lines 253-256: Reduce.execute object input path."""
        node = _make_node("reduce")
        # child_flow has no inputType → object input mode
        ctx = self._make_reduce_ctx([1, 2, 3, 4], lambda first=0, second=0: first + second)
        result = node.execute(ctx)
        self.assertEqual(result, 10)

    def test_reduce_array_input(self):
        """Lines 249-251: Reduce.execute array input path (child_flow has inputType=array)."""
        node = _make_node("reduce", extra={
            "childFlow": {
                "id": "cf", "version": "1", "runtime": "python",
                "inputType": {"dataType": "array"},
                "nodes": [
                    {"type": "start", "id": "s", "next": "e"},
                    {"type": "end", "id": "e", "resultType": "success", "output": "$INPUT"},
                ],
            }
        })
        # Array input: run_compatible is called with positional [acc, item]
        ctx = self._make_reduce_ctx([1, 2, 3], lambda args: args[0] + args[1])
        result = node.execute(ctx)
        self.assertEqual(result, 6)

    def test_reduce_single_item_no_initial(self):
        """Single item → no loop body, returns first item."""
        node = _make_node("reduce")
        ctx = self._make_reduce_ctx([42], lambda first=0, second=0: first + second)
        result = node.execute(ctx)
        self.assertEqual(result, 42)

    def test_reduce_with_initial(self):
        """Reduce with initial value."""
        node = _make_node("reduce", extra={"initial": 100})
        ctx = MagicMock()
        ctx.express_prefix = "$"
        ctx.context = {}
        # evaluate is called for both self.collection and self.initial
        ctx.evaluate.side_effect = lambda expr: [1, 2, 3] if "items" in str(expr) else 100

        def get_child():
            child = MagicMock()
            child.run_compatible.side_effect = lambda flow, is_async, **kw: kw["first"] + kw["second"]
            return child

        ctx.get_child_execution.side_effect = get_child
        result = node.execute(ctx)
        self.assertEqual(result, 106)

    def test_child_is_array_input_via_flow_attribute(self):
        """Lines 285-288: _child_is_array_input reads data_type via getattr on Flow object."""
        from plaita.node.loop import Reduce
        from plaita import Flow

        array_flow = Flow.model_validate({
            "flow_id": "cf", "version": "1", "runtime": "python",
            "inputType": {"dataType": "array"},
            "nodes": [
                {"type": "start", "id": "s", "next": "e"},
                {"type": "end", "id": "e", "resultType": "success", "output": "$INPUT"},
            ],
        })
        r = Reduce.model_validate({
            "id": "r", "collection": "$INPUT.x",
            "childFlow": _child_flow_stub(),
        })
        r.child_flow = array_flow
        # Flow.inputType is a Pydantic Property model (not a dict), so
        # _child_is_array_input goes through the getattr path (line 285+).
        self.assertTrue(r._child_is_array_input())

    def test_child_is_array_input_dict_input_type(self):
        """Line 284: input_type is a raw dict (e.g. a mock child_flow)."""
        from plaita.node.loop import Reduce

        r = Reduce.model_validate({
            "id": "r", "collection": "$INPUT.x",
            "childFlow": _child_flow_stub(),
        })
        # Override with a mock that exposes inputType as a plain dict
        mock_flow = MagicMock()
        mock_flow.inputType = {"dataType": "array"}
        r.child_flow = mock_flow
        # isinstance(input_type, dict) → True, covers line 284
        self.assertTrue(r._child_is_array_input())


if __name__ == "__main__":
    unittest.main()
