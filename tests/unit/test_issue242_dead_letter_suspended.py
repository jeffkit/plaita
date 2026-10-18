"""plaita#242：死信守卫把 suspended 按重试兜底计数终态化 error。

## 缺陷

``_dead_letter_guard`` 对「非终态 + 租约空 + 重试计数达预算」一律写
``status=error`` + ``end_time``。它不区分状态：``suspended`` 在等外部事件/
审批/delay/超时，重试计数达限并不代表执行搁浅——被终态化后，后续
``resume(event/cancel/timeout)`` 全部命中 ``already_terminal``，执行被永久
打封。

第二条缺陷：``{ns}:execution:noderefetch:{id}`` 只在失败时 INCR，成功推进
不清零 ⇒ 长寿命执行的累计失败必然在某天达到 ``NODE_RETRY_TOTAL_MAX=100``，
此后任意一条超投递消息都会触发上面的误杀。计数语义应是「连续失败」。

## 修复契约

1. 终态化只允许 ``status == "running"``；``suspended`` 保持原状，走既有的
   「重入队一份同体消息后放行原死信」恢复路径（消息不丢）。
2. 任一节点**成功推进**时，与 ``noderetry`` / ``nofail`` 同一个收口清零
   ``noderefetch``；G1 retry 唤醒不清（唤醒后仍连续撞墙必须能收敛）。
3. #123 的节点预算语义不动：running + 单节点计数达预算仍终态化。

## 反证

- 删掉状态判断（恢复「不区分状态」）：``TestSuspendedIsNotTerminalized``
  变红，精确 ``expected suspended, got error``；
- 删掉成功推进处的 ``_reset_node_retry_total``：``TestTotalCounterReset``
  变红（键仍为 ``100``）。
"""
from unittest.mock import MagicMock

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.core.flow import Flow
from plaita.server.flow_worker import RedisFlowWorker
from plaita.server.task_queue import StreamTask
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

EXEC = "exec-242"
QUEUE = "test:issue242-dlq-suspended"
TOTAL_KEY = f"plaita:execution:noderefetch:{EXEC}"
NODERETRY_KEY = f"plaita:execution:noderetry:{EXEC}:a"
CHECKPOINT = {"$LAST_NODE": "start", "$NODE": {"start": {}}}

FLOW_DEF = {
    "flow_id": "f1",
    "version": "1",
    "nodes": [
        {"id": "start", "type": "start", "next": "a"},
        {"id": "a", "type": "assignment", "result": {"step": 1}, "next": "end"},
        {"id": "end", "type": "end", "output": "ok"},
    ],
}


def _worker(fake, storage=None) -> RedisFlowWorker:
    fs = MemoryFlowStorage()
    fs.save_flow(FLOW_DEF)
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name=QUEUE,
        execution_storage=storage if storage is not None else MemoryExecutionStorage(),
        flow_storage=fs,
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
    )


def _state(status="suspended", error=None, end_time=None) -> ExecutionState:
    return ExecutionState(
        execution_id=EXEC,
        flow_id="f1",
        flow_version="1",
        status=status,
        context=dict(CHECKPOINT),
        error=error,
        end_time=end_time,
    )


def _task() -> StreamTask:
    return StreamTask(
        message_id="1-0",
        body={
            "type": "resume",
            "execution_id": EXEC,
            "tenant_id": "default",
            "resume_type": "event",
        },
        delivery_count=5,
    )


class TestSuspendedIsNotTerminalized:
    @pytest.mark.parametrize("cap", ["total", "node"])
    def test_suspended_budget_exhausted_stays_suspended(self, cap):
        """suspended + 任一重试判据达限 + 超投递消息 ⇒ 守卫不得改状态。

        总计数（noderefetch）与单节点预算（noderetry）两条判据都要覆盖：
        守卫的终态化分支对两者共用，状态门必须同时挡住。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        worker = _worker(fake, storage)
        storage.save_execution_state(EXEC, _state("suspended"))

        if cap == "total":
            fake.set(TOTAL_KEY, str(worker.NODE_RETRY_TOTAL_MAX))
        else:
            fake.set(NODERETRY_KEY, str(worker._node_retry_budget()))

        assert worker._dead_letter_guard(_task()) is True

        saved = storage.load_execution_state(EXEC)
        assert saved.status == "suspended", f"expected suspended, got {saved.status}"
        assert saved.error is None
        assert saved.end_time is None
        # 消息不丢：原消息放行死信，同时重入队一份同体消息（delivery 归 1）。
        assert fake.xlen(QUEUE) == 1

    def test_suspended_error_and_end_time_preserved(self):
        """即使 suspended 记录上已有 error/end_time（历史数据/外部写入），
        守卫也不得覆盖——「保持原状」是逐字段的。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        worker = _worker(fake, storage)
        seeded_error = {"message": "waiting approval", "code": 7}
        seeded_end = "2026-10-01T00:00:00"
        storage.save_execution_state(
            EXEC, _state("suspended", error=seeded_error, end_time=seeded_end)
        )
        fake.set(TOTAL_KEY, str(worker.NODE_RETRY_TOTAL_MAX))

        assert worker._dead_letter_guard(_task()) is True

        saved = storage.load_execution_state(EXEC)
        assert saved.status == "suspended", f"expected suspended, got {saved.status}"
        assert saved.error == seeded_error
        assert saved.end_time == seeded_end
        assert fake.xlen(QUEUE) == 1

    def test_lease_held_suspended_still_skips_dead_letter(self):
        """既有语义不回归：suspended + 租约仍在 ⇒ 跳过死信（不终态化、
        不重入队），消息留 pending 由活 worker 处理。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        worker = _worker(fake, storage)
        storage.save_execution_state(EXEC, _state("suspended"))
        fake.set(f"plaita:execution:lease:{EXEC}", "holder")
        fake.set(TOTAL_KEY, str(worker.NODE_RETRY_TOTAL_MAX))

        assert worker._dead_letter_guard(_task()) is False

        saved = storage.load_execution_state(EXEC)
        assert saved.status == "suspended", f"expected suspended, got {saved.status}"
        assert fake.xlen(QUEUE) == 0


class TestRunningStillTerminalized:
    @pytest.mark.parametrize("cap", ["total", "node"])
    def test_running_budget_exhausted_terminalizes_error(self, cap):
        """回归：#123 的收尸语义不变——running（真正卡住的推进态）在重试
        预算/兜底上限达限时仍必须终态化 error 并放行死信（不重入队）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        worker = _worker(fake, storage)
        storage.save_execution_state(EXEC, _state("running"))

        if cap == "total":
            fake.set(TOTAL_KEY, str(worker.NODE_RETRY_TOTAL_MAX))
        else:
            fake.set(NODERETRY_KEY, str(worker._node_retry_budget()))

        assert worker._dead_letter_guard(_task()) is True

        saved = storage.load_execution_state(EXEC)
        assert saved.status == "error"
        assert saved.end_time is not None
        assert saved.error is not None
        assert saved.error.get("error_type") == "node_retry_budget_exhausted"
        assert fake.xlen(QUEUE) == 0, "终态化分支不得重入队"


class TestTotalCounterReset:
    def test_successful_advance_clears_total_counter(self):
        """成功推进必须清零 noderefetch——否则长寿命执行累计到 100 后，
        任意超投递消息都会把 suspended 执行误终态化（#242 第二根因）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        worker = _worker(fake, storage)
        state = _state("running")
        storage.save_execution_state(EXEC, state)
        fake.set(TOTAL_KEY, str(worker.NODE_RETRY_TOTAL_MAX))
        fake.set(NODERETRY_KEY, "3")

        execution = MagicMock()
        execution.run_distributed.return_value = {
            "execution_id": EXEC,
            "id": "end",
            "is_end": True,
            "context": {"$LAST_NODE": "end", "$NODE": {"start": {}, "a": {}, "end": {}}},
        }
        worker._process_execution_result(
            Flow.model_validate(FLOW_DEF),
            {
                "execution_id": EXEC,
                "id": "start",
                "is_end": False,
                "is_suspend": False,
                "context": dict(CHECKPOINT),
            },
            state,
            execution=execution,
        )

        assert fake.get(TOTAL_KEY) is None, (
            "成功推进必须清零兜底总重试计数——否则长寿命执行必然累计到 "
            f"{worker.NODE_RETRY_TOTAL_MAX} 后误杀 suspended（plaita#242）"
        )
        assert storage.load_execution_state(EXEC).status == "completed"
