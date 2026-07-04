"""Extended tests for plaita/node/concurrent.py — covers previously uncovered branches.

Target coverage gaps (lines 99, 103, 115, 147-150, 168, 210-212, 232, 251,
273-276, 283, 297, 310, 319-328, 351-354, 357, 364-366).
"""
from __future__ import annotations

import asyncio
import threading
import unittest
from concurrent.futures import Future
from typing import Any
from unittest.mock import MagicMock, patch

from plaita.node.concurrent import (
    COROUTINE,
    PROCESS,
    THREAD,
    Parallel,
    ParallelBranch,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _simple_branch_dict(name: str, val: Any = 1) -> dict:
    """Build a minimal ParallelBranch dict with an echo flow."""
    return {
        "name": name,
        "input": val,
        "flow": {
            "id": f"f_{name}",
            "version": "0.1",
            "runtime": "python",
            "nodes": [
                {"type": "start", "id": "s", "next": "e"},
                {"type": "end", "id": "e", "resultType": "success", "output": "$INPUT"},
            ],
        },
    }


def _make_parallel(mode: str = THREAD, branches=None, join_branches=None) -> Parallel:
    b = branches or [_simple_branch_dict("b1", 1)]
    jb = join_branches or [bd["name"] for bd in b]
    return Parallel.model_validate({
        "id": "p1",
        "mode": mode,
        "branches": b,
        "join_branches": jb,
    })


def _make_mock_execution(input_val: Any = 42) -> MagicMock:
    """Create a minimal mock ExecutionContext."""
    m = MagicMock()
    m.mode = MagicMock()
    m.get_child_execution.return_value = m
    m.evaluate.return_value = input_val
    m.arun_compatible = MagicMock(return_value=asyncio.coroutine(lambda *a, **kw: input_val)())
    # Make run_compatible synchronous
    m.run_compatible.return_value = input_val
    m.execution_id = "test-exec-1"
    return m


# ---------------------------------------------------------------------------
# setup_branches validator
# ---------------------------------------------------------------------------

class TestSetupBranches(unittest.TestCase):
    def test_already_parallel_branch_object(self):
        """Line 99: branch that is already a ParallelBranch is appended as-is."""
        pb = ParallelBranch.model_validate(_simple_branch_dict("b1", 10))
        p = Parallel.model_validate({
            "id": "p1", "mode": THREAD,
            "branches": [pb],  # pass pre-constructed ParallelBranch
            "join_branches": ["b1"],
        })
        self.assertEqual(len(p.branches), 1)
        self.assertIsInstance(p.branches[0], ParallelBranch)

    def test_unknown_branch_type_raises(self):
        """Line 103: passing an unknown branch type raises ValueError."""
        with self.assertRaises(Exception):
            Parallel.model_validate({
                "id": "p1", "mode": THREAD,
                "branches": [42],  # not a dict or ParallelBranch
                "join_branches": [],
            })


# ---------------------------------------------------------------------------
# validate() method
# ---------------------------------------------------------------------------

class TestValidateMethod(unittest.TestCase):
    def test_validate_is_no_op(self):
        """Line 115: validate() does nothing (pass)."""
        p = _make_parallel()
        p.validate()  # should not raise


# ---------------------------------------------------------------------------
# _build_executor with cancel event
# ---------------------------------------------------------------------------

class TestBuildExecutorCancel(unittest.TestCase):
    def test_returns_none_when_cancel_event_set_process_mode(self):
        """Lines 147-150: PROCESS mode with cancel_event set → returns None."""
        p = _make_parallel(mode=PROCESS)
        execution = MagicMock()
        execution.cancel_event = threading.Event()
        execution.cancel_event.set()  # signal already cancelled
        result = p._build_executor(PROCESS, execution)
        self.assertIsNone(result)

    def test_returns_executor_when_cancel_event_not_set(self):
        """Normal PROCESS mode when cancel_event not set → returns executor."""
        p = _make_parallel(mode=PROCESS)
        execution = MagicMock()
        execution.cancel_event = threading.Event()
        # NOT set
        result = p._build_executor(PROCESS, execution)
        self.assertIsNotNone(result)

    def test_returns_executor_for_thread_mode(self):
        """THREAD mode never checks cancel_event."""
        p = _make_parallel(mode=THREAD)
        execution = MagicMock()
        execution.cancel_event = threading.Event()
        execution.cancel_event.set()
        result = p._build_executor(THREAD, execution)
        self.assertIsNotNone(result)


# ---------------------------------------------------------------------------
# pool_execute with None executor (line 168)
# ---------------------------------------------------------------------------

class TestPoolExecuteNoneExecutor(unittest.TestCase):
    def test_pool_execute_returns_empty_when_executor_is_none(self):
        """Line 168: if _build_executor returns None, pool_execute returns {}."""
        p = _make_parallel(mode=PROCESS)
        with patch.object(p, "_build_executor", return_value=None):
            execution = MagicMock()
            execution.is_conditional = False
            result = p.pool_execute(PROCESS, execution)
        self.assertEqual(result, {})


# ---------------------------------------------------------------------------
# _make_background_done_callback exception path
# ---------------------------------------------------------------------------

class TestBackgroundDoneCallbackException(unittest.TestCase):
    def test_callback_when_future_exception_raises(self):
        """Lines 210-212: fut.exception() itself raises → exc captured."""
        branch = MagicMock()
        branch.name = "bg_branch"
        errors = []

        callback = Parallel._make_background_done_callback(branch, errors)

        # future.exception() raises (e.g., CancelledFuture)
        fut = MagicMock()
        fut.exception.side_effect = RuntimeError("future was cancelled")

        callback(fut)  # should not raise
        # In this path, exc != None so it gets appended
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0]["branch"], "bg_branch")

    def test_callback_no_error_no_append(self):
        """No exception → errors list unchanged."""
        branch = MagicMock()
        branch.name = "ok_branch"
        errors = []
        callback = Parallel._make_background_done_callback(branch, errors)
        fut = MagicMock()
        fut.exception.return_value = None
        callback(fut)
        self.assertEqual(len(errors), 0)

    def test_callback_with_actual_exception(self):
        """Normal exception case: error appended to errors list."""
        branch = MagicMock()
        branch.name = "failing_branch"
        errors = []
        callback = Parallel._make_background_done_callback(branch, errors)
        fut = MagicMock()
        fut.exception.return_value = ValueError("boom")
        callback(fut)
        self.assertEqual(len(errors), 1)
        self.assertIn("boom", errors[0]["error"])


# ---------------------------------------------------------------------------
# wait_background_branches with no futures
# ---------------------------------------------------------------------------

class TestWaitBackgroundBranches(unittest.TestCase):
    def test_returns_zeros_when_no_futures(self):
        """Line 232: no futures → {'done': 0, 'not_done': 0}."""
        p = _make_parallel()
        execution = MagicMock()
        execution.execution_id = "empty-exec"
        # Ensure state is empty
        from plaita.node.concurrent import _BG_STATE
        _BG_STATE.pop("empty-exec", None)

        result = p.wait_background_branches(execution)
        self.assertEqual(result, {"done": 0, "not_done": 0})


# ---------------------------------------------------------------------------
# _execute_join_branches with empty list
# ---------------------------------------------------------------------------

class TestExecuteJoinBranchesEmpty(unittest.TestCase):
    def test_empty_join_branches_returns_empty_dict(self):
        """Line 251: no join branches → returns empty {}."""
        p = _make_parallel()
        executor = MagicMock()
        execution = MagicMock()
        result = p._execute_join_branches([], executor, execution)
        self.assertEqual(result, {})


# ---------------------------------------------------------------------------
# _process_future_result exception path
# ---------------------------------------------------------------------------

class TestProcessFutureResultException(unittest.TestCase):
    def test_exception_branch_stored_as_error_sentinel(self):
        """Lines 273-276: future.result() raises → error sentinel stored."""
        p = _make_parallel()
        lock = threading.Lock()

        fut = MagicMock()
        fut.result.side_effect = RuntimeError("branch crashed")
        branch = MagicMock()
        branch.name = "crashed_branch"
        results = {}

        p._process_future_result(fut, branch, results, lock)

        self.assertIn("crashed_branch", results)
        self.assertIn("__parallel_error__", results["crashed_branch"])
        self.assertIn("branch crashed", results["crashed_branch"]["__parallel_error__"])

    def test_success_branch_stored_normally(self):
        """Happy path: result stored directly."""
        p = _make_parallel()
        lock = threading.Lock()
        fut = MagicMock()
        fut.result.return_value = 99
        branch = MagicMock()
        branch.name = "ok_branch"
        results = {}

        p._process_future_result(fut, branch, results, lock)
        self.assertEqual(results["ok_branch"], 99)


# ---------------------------------------------------------------------------
# coroutine_execute raises ValueError (line 310)
# execute() method dispatching (lines 319-328)
# ---------------------------------------------------------------------------

class TestSyncExecuteMethods(unittest.TestCase):
    def test_coroutine_execute_raises_value_error(self):
        """Line 310: coroutine_execute no longer supported, always raises."""
        p = _make_parallel(mode=COROUTINE)
        with self.assertRaises(ValueError):
            p.coroutine_execute(MagicMock())

    def test_execute_coroutine_mode_raises(self):
        """Lines 319-321: execute() with coroutine mode calls coroutine_execute → raises."""
        p = _make_parallel(mode=COROUTINE)
        with self.assertRaises(ValueError):
            p.execute(MagicMock())

    def test_execute_unknown_mode_raises(self):
        """Lines 325-326: execute() with unknown mode raises ValueError."""
        p = _make_parallel()
        p.mode = "unknown_mode"
        with self.assertRaises(ValueError) as ctx:
            p.execute(MagicMock())
        self.assertIn("Unknown mode", str(ctx.exception))

    def test_execute_thread_mode(self):
        """Lines 322-328: execute() with THREAD mode returns results from pool_execute."""
        p = _make_parallel(mode=THREAD)
        with patch.object(Parallel, "pool_execute", return_value={"b1": 5}):
            result = p.execute(MagicMock())
        self.assertEqual(result, {"b1": 5})

    def test_execute_process_mode(self):
        """Lines 323-328: execute() with PROCESS mode returns results from pool_execute."""
        p = _make_parallel(mode=PROCESS)
        with patch.object(Parallel, "pool_execute", return_value={"b1": 6}):
            result = p.execute(MagicMock())
        self.assertEqual(result, {"b1": 6})

    def test_thread_execute_delegates_to_pool(self):
        """Line 283: thread_execute calls pool_execute with THREAD."""
        p = _make_parallel(mode=THREAD)
        # Pydantic v2 models block instance attribute patching; patch at class level
        with patch.object(Parallel, "pool_execute", return_value={"b1": 1}) as mock_pool:
            result = p.thread_execute(MagicMock())
        mock_pool.assert_called_once()
        self.assertEqual(result, {"b1": 1})

    def test_process_execute_delegates_to_pool(self):
        """Line 297: process_execute calls pool_execute with PROCESS."""
        p = _make_parallel(mode=PROCESS)
        with patch.object(Parallel, "pool_execute", return_value={"b1": 2}) as mock_pool:
            result = p.process_execute(MagicMock())
        mock_pool.assert_called_once()
        self.assertEqual(result, {"b1": 2})


# ---------------------------------------------------------------------------
# Parallel.arun — background branches + error paths
# ---------------------------------------------------------------------------

def _failing_flow() -> dict:
    """A flow whose end node raises by dividing by zero in expression."""
    return {
        "id": "failing",
        "version": "0.1",
        "runtime": "python",
        "nodes": [
            {"type": "start", "id": "s", "next": "e"},
            {
                "type": "end", "id": "e", "resultType": "success",
                "output": {"$calc": {"left": 1, "op": "/", "right": 0}},
            },
        ],
    }


class TestParallelArunBackgroundAndErrors(unittest.IsolatedAsyncioTestCase):
    async def test_background_branches_fire_and_forget(self):
        """Line 357: background branches (not in join_branches) use ensure_future."""
        from plaita.core.flow import Flow
        import json

        flow = Flow.from_string(json.dumps({
            "id": "outer",
            "version": "0.1",
            "runtime": "python",
            "nodes": [
                {"type": "start", "id": "start", "next": "par1"},
                {
                    "type": "parallel",
                    "id": "par1",
                    "mode": COROUTINE,
                    # Only b1 is a join branch; b2 is background (fire-and-forget)
                    "join_branches": ["b1"],
                    "branches": [
                        _simple_branch_dict("b1", 10),
                        _simple_branch_dict("b2", 20),  # background
                    ],
                    "next": "end",
                },
                {"type": "end", "id": "end", "resultType": "success",
                 "output": "$NODE.par1"},
            ],
        }))
        result = await flow.arun()
        # Only join branch is returned
        self.assertEqual(result, {"b1": 10})
        # Give fire-and-forget tasks a tick to complete
        await asyncio.sleep(0.05)

    async def test_arun_coroutine_join_branch_error_stored(self):
        """Lines 364-366: join branch error → error sentinel stored in result."""
        from plaita.core.flow import Flow
        import json

        flow = Flow.from_string(json.dumps({
            "id": "outer",
            "version": "0.1",
            "runtime": "python",
            "nodes": [
                {"type": "start", "id": "start", "next": "par1"},
                {
                    "type": "parallel",
                    "id": "par1",
                    "mode": COROUTINE,
                    "join_branches": ["failing_branch"],
                    "branches": [_simple_branch_dict("failing_branch", 42)],
                    "next": "end",
                },
                {"type": "end", "id": "end", "resultType": "success",
                 "output": "$NODE.par1"},
            ],
        }))
        # Patch exec_branch_async to raise for this test
        async def _raise(pb, execution):
            raise RuntimeError("branch exploded")

        par_node = next(n for n in flow.nodes if getattr(n, "node_type", None) == "parallel")
        with patch.object(Parallel, "exec_branch_async", side_effect=_raise):
            result = await flow.arun()
        # The result dict for the failing branch should be an error sentinel
        self.assertIn("failing_branch", result)
        self.assertIn("__parallel_error__", result["failing_branch"])

    async def test_arun_coroutine_background_branch_error_logged(self):
        """Lines 351-354: background branch failure is logged but doesn't propagate."""
        from plaita.core.flow import Flow
        import json

        flow = Flow.from_string(json.dumps({
            "id": "outer",
            "version": "0.1",
            "runtime": "python",
            "nodes": [
                {"type": "start", "id": "start", "next": "par1"},
                {
                    "type": "parallel",
                    "id": "par1",
                    "mode": COROUTINE,
                    "join_branches": ["ok_branch"],  # only ok_branch is joined
                    "branches": [
                        _simple_branch_dict("ok_branch", 99),
                        _simple_branch_dict("bg_fail", 0),  # background branch
                    ],
                    "next": "end",
                },
                {"type": "end", "id": "end", "resultType": "success",
                 "output": "$NODE.par1"},
            ],
        }))

        # Make bg_fail branch raise only when processing that branch
        original_exec = Parallel.exec_branch_async.__wrapped__ if hasattr(
            Parallel.exec_branch_async, "__wrapped__") else None

        call_count = [0]

        async def _selective_fail(self_node, pb, execution):
            call_count[0] += 1
            if pb.name == "bg_fail":
                raise RuntimeError("background fail")
            # ok_branch passes normally
            child_execution = execution.get_child_execution()
            return await child_execution.arun_compatible(pb.flow, False, execution.evaluate(pb.input))

        with patch.object(Parallel, "exec_branch_async", _selective_fail):
            result = await flow.arun()

        # ok_branch succeeded; bg_fail is fire-and-forget and must not propagate
        self.assertEqual(result, {"ok_branch": 99})
        # Give bg task time to finish
        await asyncio.sleep(0.05)

    async def test_arun_thread_mode_runs_via_executor(self):
        """Lines 373-376: thread/process mode in arun delegates to run_in_executor."""
        from plaita.core.flow import Flow
        import json

        flow = Flow.from_string(json.dumps({
            "id": "outer",
            "version": "0.1",
            "runtime": "python",
            "nodes": [
                {"type": "start", "id": "start", "next": "par1"},
                {
                    "type": "parallel",
                    "id": "par1",
                    "mode": THREAD,
                    "join_branches": ["b1"],
                    "branches": [_simple_branch_dict("b1", 77)],
                    "next": "end",
                },
                {"type": "end", "id": "end", "resultType": "success",
                 "output": "$NODE.par1"},
            ],
        }))
        result = await flow.arun()
        self.assertEqual(result, {"b1": 77})


if __name__ == "__main__":
    unittest.main()
