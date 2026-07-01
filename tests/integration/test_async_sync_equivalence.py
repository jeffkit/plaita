"""Tests for sync/async execution equivalence (T043, T044)."""

import asyncio
import json
import unittest

from plaita.core.executor import FlowExecution
from plaita.core.flow import Flow


def make_flow():
    return Flow.model_validate_json(json.dumps({
        "id": "equiv_test",
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "assign"},
            {"type": "assignment", "id": "assign", "next": "end", "output": "$INPUT.val"},
            {"type": "end", "id": "end", "resultType": "success", "output": "$NODE.assign"},
        ],
    }))


class TestSyncAsyncEquivalence(unittest.IsolatedAsyncioTestCase):
    """T043: Sync and async execution produce identical results."""

    def test_sync_execution(self):
        flow = make_flow()
        result = FlowExecution().run_compatible(flow, False, val=42)
        self.assertEqual(result, 42)

    async def test_async_execution(self):
        flow = make_flow()
        result = await FlowExecution().arun_compatible(flow, False, val=42)
        self.assertEqual(result, 42)

    async def test_sync_async_identical_result(self):
        flow = make_flow()
        sync_result = FlowExecution().run_compatible(flow, False, val="hello")
        async_result = await FlowExecution().arun_compatible(flow, False, val="hello")
        self.assertEqual(sync_result, async_result)

    async def test_flow_run_and_arun_identical(self):
        flow = make_flow()
        sync_result = flow.run(val=99)
        async_result = await flow.arun(val=99)
        self.assertEqual(sync_result, async_result)


class TestSyncWrapperNestedLoop(unittest.TestCase):
    """T044: Sync wrapper handles nested event loop via thread pool fallback."""

    def test_run_compatible_outside_loop(self):
        flow = make_flow()
        result = FlowExecution().run_compatible(flow, False, val=7)
        self.assertEqual(result, 7)

    def test_flow_run_outside_loop(self):
        flow = make_flow()
        result = flow.run(val=7)
        self.assertEqual(result, 7)


if __name__ == "__main__":
    unittest.main()
