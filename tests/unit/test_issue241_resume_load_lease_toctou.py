"""#241 回归：resume 的「load 状态 → acquire 租约」窗口内他 worker 终态化。

缺陷形态（P1）：``resume_flow`` 先 ``load_execution_state`` 再取执行租约，取到
租约后不重读/不校验状态。窗口内若另一 worker 把执行推到终态并释放租约，本
worker 拿到的是**新一代**租约——fencing 只拒绝旧世代写，不会拦它；于是它拿
旧 running 快照继续 ``run_distributed``，重放节点副作用并二次改写终态行
（completed/error/cancelled 互相覆盖）。

修复契约：租约内必须**重读并以盘上状态重跑前置判定**（#33 的
「suspended+continue 不取租约」前置短路保持不变）。本用例用两个真实
``FlowWorker`` + ``RedisExecutionLease``（fakeredis）交错：

    B: load(running) → A: 终态化 completed + release → B: acquire(gen=2)
      → B: load(completed) → 具名拒绝（rejected_after_lease），不跑引擎

负控：若恢复「只读一次」的旧顺序，本用例必红——``run_distributed`` 被调用且
终态行被 B 二次改写（见交付台账的负控证据）。
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")

import fakeredis

from plaita.server.execution_lease import RedisExecutionLease
from plaita.server.flow_worker import FlowWorker
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

FLOW = {
    "flow_id": "f1",
    "version": "1",
    "nodes": [
        {"id": "start", "type": "start", "next": "end"},
        {"id": "end", "type": "end", "output": "ok"},
    ],
}

EXEC = "exec-241"
LEASE_KEY = f"plaita:execution:lease:{EXEC}"


def _state(status: str, last_node: str, node: dict, end_time: str | None = None) -> ExecutionState:
    return ExecutionState(
        execution_id=EXEC,
        flow_id="f1",
        flow_version="1",
        status=status,
        context={"$LAST_NODE": last_node, "$NODE": node},
        end_time=end_time,
    )


def _two_workers() -> tuple:
    redis = fakeredis.FakeRedis(decode_responses=True)
    lease = RedisExecutionLease(redis)
    storage = MemoryExecutionStorage()
    flow_storage = MemoryFlowStorage()
    flow_storage.save_flow(FLOW)
    # 两个独立 worker 实例共享同一状态存储与租约后端（多 worker 池形态）
    worker_a = FlowWorker(storage, flow_storage, execution_lease=lease, lease_ttl_seconds=60)
    worker_b = FlowWorker(storage, flow_storage, execution_lease=lease, lease_ttl_seconds=60)
    return redis, lease, storage, worker_a, worker_b


def test_terminalized_in_acquire_window_is_rejected():
    """A 在 B 的 load→acquire 窗口内终态化并释放：B 必须具名拒绝且不推进。"""
    redis, lease, storage, _worker_a, worker_b = _two_workers()
    storage.save_execution_state(EXEC, _state("running", "start", {}))

    # A 先持租约推进（B 的入口预读看到的仍是 running）
    assert lease.try_acquire_fenced(EXEC, "holder-a", 60) == 1

    events: list[tuple[str, object]] = []

    original_load = storage.load_execution_state

    def recording_load(execution_id: str):
        st = original_load(execution_id)
        events.append(("load", None if st is None else st.status))
        return st

    storage.load_execution_state = recording_load  # type: ignore[method-assign]

    finished: list[bool] = []

    def a_finishes() -> None:
        """A 的推进完成：终态化 completed 并释放租约（发生在 B 的 acquire 前）。"""
        if finished:
            return
        finished.append(True)
        storage.save_execution_state(
            EXEC,
            _state("completed", "end", {"end": "ok"}, end_time="2026-01-01T00:00:00"),
        )
        assert lease.release(EXEC, "holder-a:1")
        events.append(("a_finished", None))

    original_acquire = worker_b._acquire_lease
    acquired: list[tuple] = []

    def acquire_during_race(execution_id: str, holder: str):
        a_finishes()
        value = original_acquire(execution_id, holder)
        acquired.append(value)
        events.append(("acquire", "ok" if value[0] is not None else "failed"))
        return value

    worker_b._acquire_lease = acquire_during_race  # type: ignore[method-assign]

    with patch("plaita.server.flow_worker.FlowExecution") as FE:
        # 旧实现会跑这一次并把 A 的 checkpoint 覆写成这个「旧快照」结果——
        # 负控（对 pre-fix HEAD 跑）据此可判「run 发生 + 终态行被二次改写」。
        FE.return_value.run_distributed.return_value = {
            "execution_id": EXEC,
            "is_end": True,
            "is_suspend": False,
            "context": {"$LAST_NODE": "start", "$NODE": {}},
            "result": "stale-rerun",
        }
        result = worker_b.resume_flow("f1", EXEC, "continue")

    # 引擎绝不启动（旧实现会跑一次并重放节点）
    FE.assert_not_called()

    # 精确交错：B 先读到 running；A 终态化并释放；B 抢到**新一代**租约；
    # B 在租约内重读，看到 completed 后才拒绝。
    assert events == [
        ("load", "running"),
        ("a_finished", None),
        ("acquire", "ok"),
        ("load", "completed"),
    ], events
    assert acquired and acquired[0][1] == 2, (
        "B 必须取得新一代租约（gen=2）——旧实现正是凭新世代绕过 fencing"
    )

    # 具名拒绝：不是普通重复投递的 already_terminal，而是租约内复核发现变化
    assert result["already_terminal"] is True
    assert result["status"] == "completed"
    assert result["rejected_after_lease"] is True

    # A 的终态行原样保留（不被旧 state 二次改写）
    final = storage.load_execution_state(EXEC)
    assert final.status == "completed"
    assert final.context["$LAST_NODE"] == "end"
    assert final.end_time == "2026-01-01T00:00:00"

    # B 的租约在 finally 释放（键无主）
    assert redis.get(LEASE_KEY) is None


def test_suspended_in_acquire_window_is_rejected():
    """窗口内改为挂起：同样以盘上状态为准幂等拒绝（不跑引擎、不改写状态）。"""
    redis, lease, storage, _worker_a, worker_b = _two_workers()
    storage.save_execution_state(EXEC, _state("running", "start", {}))
    assert lease.try_acquire_fenced(EXEC, "holder-a", 60) == 1

    finished: list[bool] = []

    def a_suspends() -> None:
        if finished:
            return
        finished.append(True)
        storage.save_execution_state(EXEC, _state("suspended", "wait", {}))
        assert lease.release(EXEC, "holder-a:1")

    original_acquire = worker_b._acquire_lease

    def acquire_during_race(execution_id: str, holder: str):
        a_suspends()
        return original_acquire(execution_id, holder)

    worker_b._acquire_lease = acquire_during_race  # type: ignore[method-assign]

    with patch("plaita.server.flow_worker.FlowExecution") as FE:
        FE.return_value.run_distributed.return_value = {
            "execution_id": EXEC,
            "is_end": True,
            "is_suspend": False,
            "context": {"$LAST_NODE": "start", "$NODE": {}},
            "result": "stale-rerun",
        }
        result = worker_b.resume_flow("f1", EXEC, "continue")

    FE.assert_not_called()
    assert result["already_suspended"] is True
    assert result["status"] == "suspended"
    assert result["rejected_after_lease"] is True
    assert storage.load_execution_state(EXEC).status == "suspended"
    assert redis.get(LEASE_KEY) is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
