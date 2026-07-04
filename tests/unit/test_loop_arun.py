"""Tests for async (arun) paths of loop.py collection nodes.

Covers Loop.arun, Map.arun (sequential + concurrent), Filter.arun,
Find.arun, Reduce.arun (array input + object input + initial), and
Reduce._child_is_array_input edge cases.

Implementation notes:
- express_prefix is "$" by default, so loop context keys are "$LOOP-ITEM",
  "$LOOP-RESULT", etc. Use these in condition field.
- Filter/Find child flows use the "if" (Bool) branching node — node_type is
  "if" not "bool". Truthy path → End("true"), falsy → End("false").
- $F.eq is NOT registered; use Condition-based Bool node for true/false logic.
- Reduce does NOT handle empty collections (collection[0] would IndexError).
"""

from __future__ import annotations

import unittest
from typing import Any, Optional

from plaita import Flow


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _end(id_: str, output: Any = None) -> dict:
    d: dict = {"type": "end", "id": id_, "resultType": "success"}
    if output is not None:
        d["output"] = output
    return d


def _child_flow(nodes: list, version: str = "1", input_type: Optional[dict] = None) -> dict:
    d: dict = {"id": "cf", "version": version, "runtime": "python", "nodes": nodes}
    if input_type:
        d["inputType"] = input_type
    return d


def _double_item_flow():
    """Child flow: returns INPUT.item * 2."""
    return _child_flow([
        {"type": "start", "id": "s", "next": "e"},
        _end("e", "$F.mul($INPUT.item, 2)"),
    ])


def _gt2_filter_flow():
    """Child flow for Filter/Find: True when item > 2, False otherwise.

    Uses Bool (node_type="if") to route to a "true" or "false" End node.
    Filter keeps items whose child-flow returns a truthy result.
    """
    return _child_flow([
        {"type": "start", "id": "s", "next": "b"},
        {
            "type": "if", "id": "b",
            "condition": {"field": "$INPUT.item", "operator": "gt", "value": 2},
            # Bool defaults: next="true", else_next="false"
        },
        _end("true", "$INPUT.item"),  # truthy: return the item itself
        _end("false"),                 # falsy: return None
    ])


def _sum_flow_array_input():
    """Reduce child flow: array input $INPUT[0] + $INPUT[1]."""
    return {
        "id": "cf-sum",
        "version": "1",
        "runtime": "python",
        "inputType": {"dataType": "array"},
        "nodes": [
            {"type": "start", "id": "s", "next": "e"},
            _end("e", "$F.add($INPUT[0], $INPUT[1])"),
        ],
    }


def _sum_flow_object_input():
    """Reduce child flow: object input $INPUT.first + $INPUT.second."""
    return _child_flow([
        {"type": "start", "id": "s", "next": "e"},
        _end("e", "$F.add($INPUT.first, $INPUT.second)"),
    ])


# ---------------------------------------------------------------------------
# Loop.arun
# ---------------------------------------------------------------------------

class TestLoopArun(unittest.IsolatedAsyncioTestCase):
    async def test_loop_arun_basic(self):
        """Loop doubles each item; final result is the last item doubled."""
        flow = Flow.model_validate({
            "flow_id": "test", "version": "1", "runtime": "python",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "loop"},
                {
                    "type": "loop", "id": "loop",
                    "collection": "$INPUT.items",
                    "childFlow": _double_item_flow(),
                    "next": "end",
                },
                _end("end", "$NODE.loop"),
            ],
        })
        result = await flow.arun(items=[1, 2, 3])
        self.assertEqual(result, 6)  # last item (3) doubled

    async def test_loop_arun_empty_collection(self):
        """Loop over empty collection returns None."""
        flow = Flow.model_validate({
            "flow_id": "test", "version": "1", "runtime": "python",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "loop"},
                {
                    "type": "loop", "id": "loop",
                    "collection": "$INPUT.items",
                    "childFlow": _double_item_flow(),
                    "next": "end",
                },
                _end("end", "$NODE.loop"),
            ],
        })
        result = await flow.arun(items=[])
        self.assertIsNone(result)

    async def test_loop_arun_single_item(self):
        """Loop with single item returns that item's child-flow result."""
        flow = Flow.model_validate({
            "flow_id": "test", "version": "1", "runtime": "python",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "loop"},
                {
                    "type": "loop", "id": "loop",
                    "collection": "$INPUT.items",
                    "childFlow": _double_item_flow(),
                    "next": "end",
                },
                _end("end", "$NODE.loop"),
            ],
        })
        result = await flow.arun(items=[7])
        self.assertEqual(result, 14)

    async def test_loop_arun_with_condition_break(self):
        """Loop condition uses $LOOP-RESULT (express_prefix=$).

        express_prefix is "$" by default, so Loop stores the result as
        "$LOOP-RESULT" in the context. Condition field "$LOOP-RESULT" with
        operator "lt" and value 8 means: continue while result < 8.

        items=[1,2,3,4,5], child doubles each:
          item=1 → result=2, 2<8=True → continue
          item=2 → result=4, 4<8=True → continue
          item=3 → result=6, 6<8=True → continue
          item=4 → result=8, 8<8=False → BREAK
        Final result: 8 (item=4 was processed before break).
        """
        flow = Flow.model_validate({
            "flow_id": "test", "version": "1", "runtime": "python",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "loop"},
                {
                    "type": "loop", "id": "loop",
                    "collection": "$INPUT.items",
                    "childFlow": _double_item_flow(),
                    "condition": {
                        "field": "$LOOP-RESULT",
                        "operator": "lt",
                        "value": 8,
                    },
                    "next": "end",
                },
                _end("end", "$NODE.loop"),
            ],
        })
        result = await flow.arun(items=[1, 2, 3, 4, 5])
        self.assertEqual(result, 8)


# ---------------------------------------------------------------------------
# Map.arun
# ---------------------------------------------------------------------------

class TestMapArun(unittest.IsolatedAsyncioTestCase):
    async def test_map_arun_sequential(self):
        """Map doubles each item in sequence."""
        flow = Flow.model_validate({
            "flow_id": "test", "version": "1", "runtime": "python",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "map"},
                {
                    "type": "map", "id": "map",
                    "collection": "$INPUT.items",
                    "childFlow": _double_item_flow(),
                    "next": "end",
                },
                _end("end", "$NODE.map"),
            ],
        })
        result = await flow.arun(items=[1, 2, 3])
        self.assertEqual(result, [2, 4, 6])

    async def test_map_arun_concurrent(self):
        """Concurrent map doubles each item."""
        flow = Flow.model_validate({
            "flow_id": "test", "version": "1", "runtime": "python",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "map"},
                {
                    "type": "map", "id": "map",
                    "collection": "$INPUT.items",
                    "concurrent": True,
                    "childFlow": _double_item_flow(),
                    "next": "end",
                },
                _end("end", "$NODE.map"),
            ],
        })
        result = await flow.arun(items=[5, 10, 15])
        self.assertEqual(sorted(result), [10, 20, 30])

    async def test_map_arun_with_max_concurrent(self):
        """Concurrent map with concurrency limit."""
        flow = Flow.model_validate({
            "flow_id": "test", "version": "1", "runtime": "python",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "map"},
                {
                    "type": "map", "id": "map",
                    "collection": "$INPUT.items",
                    "concurrent": True,
                    "max_concurrent": 2,
                    "childFlow": _double_item_flow(),
                    "next": "end",
                },
                _end("end", "$NODE.map"),
            ],
        })
        result = await flow.arun(items=[1, 2, 3, 4])
        self.assertEqual(sorted(result), [2, 4, 6, 8])

    async def test_map_arun_empty(self):
        """Map over empty collection returns empty list."""
        flow = Flow.model_validate({
            "flow_id": "test", "version": "1", "runtime": "python",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "map"},
                {
                    "type": "map", "id": "map",
                    "collection": "$INPUT.items",
                    "childFlow": _double_item_flow(),
                    "next": "end",
                },
                _end("end", "$NODE.map"),
            ],
        })
        result = await flow.arun(items=[])
        self.assertEqual(result, [])


# ---------------------------------------------------------------------------
# Filter.arun — child flow uses "if" (Bool) node to return truthy/falsy
# ---------------------------------------------------------------------------

class TestFilterArun(unittest.IsolatedAsyncioTestCase):
    def _make_filter_flow(self):
        return Flow.model_validate({
            "flow_id": "test", "version": "1", "runtime": "python",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "fil"},
                {
                    "type": "filter", "id": "fil",
                    "collection": "$INPUT.items",
                    "childFlow": _gt2_filter_flow(),
                    "next": "end",
                },
                _end("end", "$NODE.fil"),
            ],
        })

    async def test_filter_arun_keeps_matching(self):
        """Filter keeps items > 2: [1,2,3,4,5,6] → [3,4,5,6]."""
        flow = self._make_filter_flow()
        result = await flow.arun(items=[1, 2, 3, 4, 5, 6])
        self.assertEqual(result, [3, 4, 5, 6])

    async def test_filter_arun_none_pass(self):
        """All items <= 2 → empty result."""
        flow = self._make_filter_flow()
        result = await flow.arun(items=[0, 1, 2])
        self.assertEqual(result, [])

    async def test_filter_arun_all_pass(self):
        """All items > 2 → all kept."""
        flow = self._make_filter_flow()
        result = await flow.arun(items=[3, 4, 5])
        self.assertEqual(result, [3, 4, 5])

    async def test_filter_arun_empty_input(self):
        """Empty input collection → empty result."""
        flow = self._make_filter_flow()
        result = await flow.arun(items=[])
        self.assertEqual(result, [])


# ---------------------------------------------------------------------------
# Find.arun
# ---------------------------------------------------------------------------

class TestFindArun(unittest.IsolatedAsyncioTestCase):
    def _make_find_flow(self):
        return Flow.model_validate({
            "flow_id": "test", "version": "1", "runtime": "python",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "find"},
                {
                    "type": "find", "id": "find",
                    "collection": "$INPUT.items",
                    "childFlow": _gt2_filter_flow(),
                    "next": "end",
                },
                _end("end", "$NODE.find"),
            ],
        })

    async def test_find_arun_returns_first_match(self):
        """Find returns first item where item > 2."""
        flow = self._make_find_flow()
        result = await flow.arun(items=[1, 3, 4, 5])
        self.assertEqual(result, 3)

    async def test_find_arun_not_found(self):
        """Find returns None when no item matches."""
        flow = self._make_find_flow()
        result = await flow.arun(items=[0, 1, 2])
        self.assertIsNone(result)

    async def test_find_arun_empty_input(self):
        """Find over empty collection returns None."""
        flow = self._make_find_flow()
        result = await flow.arun(items=[])
        self.assertIsNone(result)

    async def test_find_arun_skips_non_matching(self):
        """Find correctly skips non-matching items."""
        flow = self._make_find_flow()
        result = await flow.arun(items=[1, 2, 10, 20])
        self.assertEqual(result, 10)


# ---------------------------------------------------------------------------
# Reduce (sync + async)
# ---------------------------------------------------------------------------

class TestReduceExecute(unittest.TestCase):
    def _run_reduce(self, child_flow: dict, items: list, initial=None) -> Any:
        node_def: dict = {
            "type": "reduce", "id": "red",
            "collection": "$INPUT.items",
            "childFlow": child_flow,
            "next": "end",
        }
        if initial is not None:
            node_def["initial"] = initial
        flow = Flow.model_validate({
            "flow_id": "test", "version": "1", "runtime": "python",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "red"},
                node_def,
                _end("end", "$NODE.red"),
            ],
        })
        return flow.run(items=items)

    def test_reduce_array_input(self):
        result = self._run_reduce(_sum_flow_array_input(), [1, 2, 3, 4])
        self.assertEqual(result, 10)

    def test_reduce_object_input(self):
        result = self._run_reduce(_sum_flow_object_input(), [1, 2, 3, 4])
        self.assertEqual(result, 10)

    def test_reduce_with_initial(self):
        result = self._run_reduce(_sum_flow_array_input(), [1, 2, 3], initial=100)
        self.assertEqual(result, 106)

    def test_reduce_single_item_no_initial(self):
        """Single item, no initial: no loop body runs, returns the first item."""
        result = self._run_reduce(_sum_flow_array_input(), [42])
        self.assertEqual(result, 42)


class TestReduceArun(unittest.IsolatedAsyncioTestCase):
    async def _arun_reduce(self, child_flow: dict, items: list, initial=None) -> Any:
        node_def: dict = {
            "type": "reduce", "id": "red",
            "collection": "$INPUT.items",
            "childFlow": child_flow,
            "next": "end",
        }
        if initial is not None:
            node_def["initial"] = initial
        flow = Flow.model_validate({
            "flow_id": "test", "version": "1", "runtime": "python",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "red"},
                node_def,
                _end("end", "$NODE.red"),
            ],
        })
        return await flow.arun(items=items)

    async def test_reduce_arun_array_input(self):
        result = await self._arun_reduce(_sum_flow_array_input(), [1, 2, 3, 4])
        self.assertEqual(result, 10)

    async def test_reduce_arun_object_input(self):
        result = await self._arun_reduce(_sum_flow_object_input(), [1, 2, 3, 4])
        self.assertEqual(result, 10)

    async def test_reduce_arun_with_initial(self):
        result = await self._arun_reduce(_sum_flow_array_input(), [5, 5], initial=90)
        self.assertEqual(result, 100)

    async def test_reduce_arun_single_item(self):
        """Single item, no initial: returns the item directly (no child flow)."""
        result = await self._arun_reduce(_sum_flow_array_input(), [99])
        self.assertEqual(result, 99)


# ---------------------------------------------------------------------------
# Reduce._child_is_array_input edge cases
# ---------------------------------------------------------------------------

class TestChildIsArrayInput(unittest.TestCase):
    def _make_reduce(self, child_flow_dict: Optional[dict]) -> Any:
        from plaita.node.loop import Reduce
        r = Reduce.model_validate({
            "id": "r", "collection": "$INPUT.x",
            "childFlow": _sum_flow_object_input(),
        })
        if child_flow_dict is None:
            r.child_flow = None
            return r
        r.child_flow = Flow.model_validate(child_flow_dict)
        return r

    def test_child_is_array_input_none_flow(self):
        r = self._make_reduce(None)
        self.assertFalse(r._child_is_array_input())

    def test_child_is_array_input_no_input_type(self):
        """Child flow without inputType → not array input."""
        r = self._make_reduce(_sum_flow_object_input())
        self.assertFalse(r._child_is_array_input())

    def test_child_is_array_input_object_type(self):
        child = {**_sum_flow_object_input(), "inputType": {"dataType": "object"}}
        r = self._make_reduce(child)
        self.assertFalse(r._child_is_array_input())

    def test_child_is_array_input_array_type_via_dict(self):
        child = {**_sum_flow_object_input(), "inputType": {"dataType": "array"}}
        r = self._make_reduce(child)
        self.assertTrue(r._child_is_array_input())

    def test_child_is_array_input_array_type_via_flow_object(self):
        """Inject a Flow object with input_property.data_type == 'array'."""
        from plaita.node.loop import Reduce
        flow_data = {
            "flow_id": "cf", "version": "1", "runtime": "python",
            "inputType": {"dataType": "array"},
            "nodes": [
                {"type": "start", "id": "s", "next": "e"},
                _end("e", "$INPUT"),
            ],
        }
        child = Flow.model_validate(flow_data)
        r = Reduce.model_validate({
            "id": "r", "collection": "$INPUT.x",
            "childFlow": _sum_flow_array_input(),
        })
        r.child_flow = child
        self.assertTrue(r._child_is_array_input())


if __name__ == "__main__":
    unittest.main()
