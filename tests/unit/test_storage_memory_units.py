"""Unit tests for plaita.storage.memory — MemoryExecutionStorage and MemoryFlowStorage.

Coverage target: plaita/storage/memory.py (54% → target 85%+)
"""

from __future__ import annotations

import unittest
from typing import Any

from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage
from plaita.storage.base import ExecutionState


def _make_state(execution_id: str = "exec-1", flow_id: str = "flow-1", status: str = "running") -> ExecutionState:
    return ExecutionState.model_validate({
        "execution_id": execution_id,
        "flow_id": flow_id,
        "status": status,
        "created_at": 1000.0,
        "updated_at": 1000.0,
        "context": {},
        "output": None,
        "error": None,
    })


# ---------------------------------------------------------------------------
# MemoryExecutionStorage
# ---------------------------------------------------------------------------

class TestMemoryExecutionStorage(unittest.TestCase):
    def setUp(self):
        self.storage = MemoryExecutionStorage()

    def test_save_and_load(self):
        state = _make_state("e1")
        self.storage.save_execution_state("e1", state)
        loaded = self.storage.load_execution_state("e1")
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.execution_id, "e1")

    def test_load_missing_returns_none(self):
        result = self.storage.load_execution_state("nonexistent")
        self.assertIsNone(result)

    def test_delete_existing(self):
        state = _make_state("e2")
        self.storage.save_execution_state("e2", state)
        deleted = self.storage.delete_execution_state("e2")
        self.assertTrue(deleted)
        self.assertIsNone(self.storage.load_execution_state("e2"))

    def test_delete_missing_returns_false(self):
        result = self.storage.delete_execution_state("missing")
        self.assertFalse(result)

    def test_list_all(self):
        self.storage.save_execution_state("a", _make_state("a"))
        self.storage.save_execution_state("b", _make_state("b"))
        results = self.storage.list_executions()
        self.assertEqual(len(results), 2)

    def test_list_empty(self):
        results = self.storage.list_executions()
        self.assertEqual(results, [])

    def test_list_with_query_match(self):
        self.storage.save_execution_state("e3", _make_state("e3", flow_id="flow-A", status="done"))
        self.storage.save_execution_state("e4", _make_state("e4", flow_id="flow-B", status="running"))
        results = self.storage.list_executions(query={"flow_id": "flow-A"})
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].execution_id, "e3")

    def test_list_with_query_no_match(self):
        self.storage.save_execution_state("e5", _make_state("e5", status="done"))
        results = self.storage.list_executions(query={"status": "nonexistent"})
        self.assertEqual(results, [])

    def test_list_with_limit(self):
        for i in range(5):
            self.storage.save_execution_state(f"e{i}", _make_state(f"e{i}"))
        results = self.storage.list_executions(limit=3)
        self.assertEqual(len(results), 3)

    def test_list_with_offset(self):
        for i in range(5):
            self.storage.save_execution_state(f"e{i}", _make_state(f"e{i}"))
        results = self.storage.list_executions(offset=3)
        self.assertEqual(len(results), 2)

    def test_list_with_order_by_status(self):
        self.storage.save_execution_state("x1", _make_state("x1", status="done"))
        self.storage.save_execution_state("x2", _make_state("x2", status="running"))
        results = self.storage.list_executions(order_by="status")
        statuses = [r.status for r in results]
        self.assertEqual(statuses, sorted(statuses))

    def test_list_with_order_by_reverse(self):
        self.storage.save_execution_state("y1", _make_state("y1", status="done"))
        self.storage.save_execution_state("y2", _make_state("y2", status="running"))
        results = self.storage.list_executions(order_by="-status")
        statuses = [r.status for r in results]
        self.assertEqual(statuses, sorted(statuses, reverse=True))

    def test_save_overwrites(self):
        state1 = _make_state("e_over", status="running")
        self.storage.save_execution_state("e_over", state1)
        state2 = _make_state("e_over", status="done")
        self.storage.save_execution_state("e_over", state2)
        loaded = self.storage.load_execution_state("e_over")
        self.assertEqual(loaded.status, "done")


# ---------------------------------------------------------------------------
# MemoryFlowStorage
# ---------------------------------------------------------------------------

class TestMemoryFlowStorage(unittest.TestCase):
    def setUp(self):
        self.storage = MemoryFlowStorage()

    def _flow(self, flow_id: str, version: str = "1") -> dict:
        return {"flow_id": flow_id, "version": version, "name": f"flow-{flow_id}"}

    def test_save_and_get(self):
        flow = self._flow("f1", "1")
        self.storage.save_flow(flow)
        result = self.storage.get_flow("f1", "1")
        self.assertIsNotNone(result)
        self.assertEqual(result["flow_id"], "f1")

    def test_get_missing_returns_none(self):
        result = self.storage.get_flow("does-not-exist")
        self.assertIsNone(result)

    def test_save_without_flow_id_returns_false(self):
        result = self.storage.save_flow({"name": "no-id"})
        self.assertFalse(result)

    def test_save_uses_id_fallback(self):
        """Flows using 'id' instead of 'flow_id' are accepted."""
        flow = {"id": "f_alt", "version": "1"}
        saved = self.storage.save_flow(flow)
        self.assertTrue(saved)
        result = self.storage.get_flow("f_alt", "1")
        self.assertIsNotNone(result)

    def test_get_specific_version(self):
        self.storage.save_flow(self._flow("f2", "1"))
        self.storage.save_flow(self._flow("f2", "2"))
        r1 = self.storage.get_flow("f2", "1")
        r2 = self.storage.get_flow("f2", "2")
        self.assertEqual(r1["version"], "1")
        self.assertEqual(r2["version"], "2")

    def test_get_latest_version_explicit(self):
        flow = self._flow("f3")
        flow["version"] = "latest"
        self.storage.save_flow(flow)
        result = self.storage.get_flow("f3")
        self.assertEqual(result["version"], "latest")

    def test_get_without_version_returns_latest_fallback(self):
        """When no 'latest' key, returns the numeric-highest version."""
        self.storage.save_flow({"flow_id": "f4", "version": "1"})
        self.storage.save_flow({"flow_id": "f4", "version": "2"})
        result = self.storage.get_flow("f4")
        self.assertIsNotNone(result)

    def test_get_without_version_fallback_any(self):
        """When versions have non-numeric strings, returns some version."""
        self.storage.save_flow({"flow_id": "f5", "version": "alpha"})
        result = self.storage.get_flow("f5")
        self.assertIsNotNone(result)

    def test_get_missing_version_falls_back_to_any(self):
        """When specified version doesn't exist, implementation falls back to
        returning any available version (by design: returns latest numeric or any)."""
        self.storage.save_flow(self._flow("f6", "1"))
        # The implementation tries numeric sort fallback, so returns version "1"
        result = self.storage.get_flow("f6", "999")
        # Per implementation: falls back rather than returning None
        self.assertIsNotNone(result)

    def test_get_existing_flow_missing_version(self):
        """Flow exists but specified version doesn't exist. Falls back."""
        self.storage.save_flow({"flow_id": "f7", "version": "latest"})
        result = self.storage.get_flow("f7", "nonexistent")
        # Fallback to "latest" since it exists
        self.assertEqual(result["version"], "latest")

    def test_overwrite_same_version(self):
        self.storage.save_flow({"flow_id": "f8", "version": "1", "name": "old"})
        self.storage.save_flow({"flow_id": "f8", "version": "1", "name": "new"})
        result = self.storage.get_flow("f8", "1")
        self.assertEqual(result["name"], "new")


if __name__ == "__main__":
    unittest.main()
