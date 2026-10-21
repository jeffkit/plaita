"""#280 (r55) P2: cancel between the step-boundary check and running persist.

The step loop checks the cancel flag at the top, then executes a node and writes
``state.status="running"`` at the step-persist point. A CANCEL landing in that
window could be overwritten by the running write (console already CAS'ed
cancelled and released quota), losing the cancellation until a later step.

Fix: re-check the cancel flag immediately before the running step persist; if
set, terminalize ``cancelled`` instead of writing running.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.core.flow import Flow
from plaita.server.flow_worker import RedisFlowWorker
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

EXEC = "exec-280"
FLOW_DEF = {
    "flow_id": "f1",
    "version": "1",
    "nodes": [
        {"id": "start", "type": "start", "next": "a"},
        {"id": "a", "type": "assignment", "result": {"step": 1}, "next": "end"},
        {"id": "end", "type": "end", "output": "ok"},
    ],
}


def _worker(fake, storage):
    flow_storage = MemoryFlowStorage()
    flow_storage.save_flow(FLOW_DEF)
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:issue280-cancel-window",
        execution_storage=storage,
        flow_storage=flow_storage,
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
    )


def test_cancel_between_check_and_running_persist_wins():
    fake = fakeredis.FakeRedis(decode_responses=True)
    storage = MemoryExecutionStorage()
    worker = _worker(fake, storage)
    state = ExecutionState(
        execution_id=EXEC,
        flow_id="f1",
        flow_version="1",
        status="running",
        context={"$LAST_NODE": "start", "$NODE": {"start": {}}},
    )
    storage.save_execution_state(EXEC, state)

    execution = MagicMock()

    def step(flow, **kwargs):
        # CANCEL 恰落在顶部 _cancel_requested() 之后、running 落盘之前。
        fake.set(worker._cancel_flag_key(EXEC), "now")
        return {
            "execution_id": EXEC,
            "id": "a",
            "is_end": False,
            "is_suspend": False,
            "context": {"$LAST_NODE": "a", "$NODE": {"start": {}, "a": {"step": 1}}},
        }

    execution.run_distributed.side_effect = step
    with patch.object(
        worker, "_persist_state_or_raise",
        wraps=worker._persist_state_or_raise,
    ) as persist:
        worker._process_execution_result(
            Flow.model_validate(FLOW_DEF),
            {
                "execution_id": EXEC,
                "id": "start",
                "is_end": False,
                "is_suspend": False,
                "context": {"$LAST_NODE": "start", "$NODE": {"start": {}}},
            },
            state,
            execution=execution,
        )
    phases = [call.args[2] for call in persist.call_args_list]
    assert "step_persist" not in phases, (
        f"cancel 已置位后仍写 running step_persist: {phases}"
    )
    assert "cancelled_before_step_persist" in phases, phases

    saved = storage.load_execution_state(EXEC)
    assert saved.status == "cancelled", (
        f"cancel between check and running persist must not be overwritten; "
        f"got {saved.status}"
    )
    assert saved.end_time is not None
    assert saved.context.get("$LAST_NODE") == "a"
