"""#243 回归：#241 租约内 suspended 复核自撞租约——pending 派发被跳过。

缺陷形态（P1）：``resume_flow`` 取到租约后调用
``_load_and_guard_resume_state`` 复核；若窗口内执行已被上一持有者写成
``suspended``（带 ``dispatch_pending``），复核分支调用
``_compensate_suspend_dispatch``，后者**再次 acquire 同一 execution 的租约**
——外层已持有 ⇒ 内层必然失败 ⇒ 补派跳过；随后复核返回
``already_suspended`` + ``rejected_after_lease``，``run()`` 对正常返回 ack，
消息不再重投，delay/approval 任务永久不投递。

修复契约：外层已持租约即所有权凭据，补派走进程内直接投递（``lease_held``
路径），不重新 acquire；仍持锁重读盘上状态，投递成功后清凭据。独立载体
（start dedup / 租约前短路）保持原有「短期租约 + 重读」语义（#233/#195 不回退）。

本用例用真实 ``RedisFlowWorker`` + ``RedisExecutionLease``（fakeredis）交错：

    B: load(running) → A: 挂起 + 写 dispatch_pending + release
      → B: acquire(gen=2) → B: 复核(suspended) → B: 用外层租约补派
      → 队列恰 1 条 delay 任务 + 凭据清空 + 租约释放

负控：恢复「复核分支再次 acquire」后本用例必红（pending 仍 True / 队列 0 条）。
"""
from __future__ import annotations

import json
from unittest.mock import patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.server.flow_worker import RedisFlowWorker
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
EXEC = "exec-243"
QUEUE = "test:issue243"
LEASE_KEY = f"plaita:execution:lease:{EXEC}"
DELAY_QUEUE = "plaita:delay:queue"


def _state(status: str, last_node: str, node: dict) -> ExecutionState:
    return ExecutionState(
        execution_id=EXEC,
        flow_id="f1",
        flow_version="1",
        status=status,
        context={"$LAST_NODE": last_node, "$NODE": node},
    )


def _pending_marker() -> dict:
    return {
        "pending": True,
        "service_type": "delay",
        "service_config": {"type": "delay", "seconds": 5},
        "node_id": "wait",
        "recorded_at": "2026-01-01T00:00:00",
    }


def _worker(fake) -> RedisFlowWorker:
    storage = MemoryExecutionStorage()
    flow_storage = MemoryFlowStorage()
    flow_storage.save_flow(FLOW)
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name=QUEUE,
        execution_storage=storage,
        flow_storage=flow_storage,
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
    )


def test_suspended_in_acquire_window_compensates_with_held_lease():
    """外层租约内复核 = 进程内直接投递；补派成功且只投一次。"""
    fake = fakeredis.FakeRedis(decode_responses=True)
    worker = _worker(fake)
    storage = worker.execution_storage
    storage.save_execution_state(EXEC, _state("running", "start", {}))

    # A 先在 B 的 load→acquire 窗口持有租约（gen=1）
    assert worker.execution_lease.try_acquire_fenced(EXEC, "holder-a", 60) == 1

    original_acquire = worker._acquire_lease
    injected: list[str] = []

    def acquire_during_race(execution_id: str, holder: str):
        if not injected:
            injected.append("window")
            # 模拟 A 在窗口内挂起：落盘 suspended + 未派发凭据，然后释放租约。
            suspended = _state(
                "suspended",
                "wait",
                {"wait": {"service_config": _pending_marker()["service_config"]}},
            )
            suspended.dispatch_pending = _pending_marker()
            storage.save_execution_state(EXEC, suspended)
            assert worker.execution_lease.release(EXEC, "holder-a:1")
        return original_acquire(execution_id, holder)

    worker._acquire_lease = acquire_during_race  # type: ignore[method-assign]

    with patch("plaita.server.flow_worker.FlowExecution") as FE:
        result = worker.resume_flow("f1", EXEC, "continue")

    # 精确交错：窗口注入发生一次；引擎不启动（不重跑挂起节点，#33 契约）。
    assert injected == ["window"]
    FE.assert_not_called()

    # 负控主判据先断言：自撞时 pending 仍为 True、队列为空。
    saved = storage.load_execution_state(EXEC)
    assert saved.status == "suspended"
    assert saved.dispatch_pending is None, (
        f"补派后凭据必须清空，实际 {saved.dispatch_pending!r}（自撞路径会留下 pending=True）"
    )
    tasks = fake.lrange(DELAY_QUEUE, 0, -1)
    assert len(tasks) == 1, (
        f"expected exactly one delay task, got {len(tasks)}（自撞路径队列为空）"
    )
    task = json.loads(tasks[0])
    assert task["type"] == "delay"
    assert task["execution_id"] == EXEC

    # 正控：复核仍拒绝推进，但补派已完成。
    assert result["status"] == "suspended"
    assert result["already_suspended"] is True
    assert result["rejected_after_lease"] is True
    assert result["dispatch_compensated"] is True

    # 第二次 continue：凭据已清 ⇒ 不再补派（同一执行只投一次）。
    second = worker.resume_flow("f1", EXEC, "continue")
    assert second["already_suspended"] is True
    assert "dispatch_compensated" not in second
    assert len(fake.lrange(DELAY_QUEUE, 0, -1)) == 1

    # 外层租约在 finally 释放。
    assert fake.get(LEASE_KEY) is None


def test_review_without_pending_still_does_not_acquire_extra_lease():
    """#33 契约回归：suspended 复核且无 pending ⇒ 不补派、不额外取租约。

    入口预读在租约前就会短路（根本不进入 acquire）；这里直接验证
    ``_compensate_suspend_dispatch(lease_held=True)`` 在无凭据时是纯 no-op。
    """
    fake = fakeredis.FakeRedis(decode_responses=True)
    worker = _worker(fake)
    storage = worker.execution_storage
    state = _state("suspended", "wait", {})
    storage.save_execution_state(EXEC, state)

    compensated = worker._compensate_suspend_dispatch(
        state, EXEC, source="test", lease_held=True
    )

    assert compensated is False
    assert fake.exists(LEASE_KEY) == 0
    assert fake.llen(DELAY_QUEUE) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
