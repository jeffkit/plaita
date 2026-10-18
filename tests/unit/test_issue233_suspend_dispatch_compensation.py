"""plaita#233 回归：挂起「落盘 → 派发」窗口的补偿载体。

基线缺陷（P1）：`_process_execution_result` 的挂起分支是两段非原子操作——

    state.status = "suspended"; self._persist_state_or_raise(...)   # 先落盘
    self._dispatch_service_task(...)                                # 再派发

派发瞬断（Redis 抖动）或进程在两步之间被杀（滚动重启/OOM）时，执行永久停在
``suspended``（``error=None``、``end_time=None``），且三条载体全部落空：

1. resume（continue/retry）被 #33 的挂起幂等短路吞掉（不重跑节点、不派发）；
2. start 只回报现状（dedup 路径）/ 整条重放（非 dedup 路径，见 #234）；
3. 全仓没有从 service_config 重建任务的补偿巡检。

修复契约（本测试钉住）：

- 挂起落盘**同一次写**带上待派发凭据 ``dispatch_pending``；派发成功后清；
- 重投载体（resume continue/retry、start 载体）见凭据 ⇒ **幂等补派一次**
  （不重跑挂起节点、状态仍 ``suspended``），随后清凭据；
- 再投一次不产生第二条任务（幂等）；
- 补派失败不吞：抛 ``ServiceDispatchError``，凭据保留，消息不 ack 继续重投；
- 正常（派发成功）路径行为零变化。
"""
import json
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.server.flow_worker import (
    RedisFlowWorker,
    ServiceDispatchError,
)
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

TEST_FLOW = {
    "flow_id": "f1",
    "version": "1",
    "nodes": [
        {"id": "start", "type": "start", "next": "a"},
        {"id": "a", "type": "assignment", "result": {"step": 1}, "next": "end"},
        {"id": "end", "type": "end", "output": "ok"},
    ],
}
QUEUE = "plaita:delay:queue"
EXEC = "exec-dispatch-retry"
SERVICE_CONFIG = {"type": "delay", "delay_ms": 1000}
CHECKPOINT = {"$LAST_NODE": "a", "$NODE": {}}


def _worker():
    fake = fakeredis.FakeRedis(decode_responses=True)
    storage = MemoryExecutionStorage()
    flow_storage = MemoryFlowStorage()
    flow_storage.save_flow(TEST_FLOW)
    worker = RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:issue233-dispatch",
        execution_storage=storage,
        flow_storage=flow_storage,
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
    )
    return worker, storage, fake


def _seed_running(storage, execution_id=EXEC):
    storage.save_execution_state(
        execution_id,
        ExecutionState(
            execution_id=execution_id,
            flow_id="f1",
            flow_version="1",
            status="running",
            context=dict(CHECKPOINT),
        ),
    )


def _seed_suspended_with_pending(storage, execution_id=EXEC):
    """模拟「suspended 已落盘、派发失败/进程在派发前被杀」的盘上现场。"""
    storage.save_execution_state(
        execution_id,
        ExecutionState(
            execution_id=execution_id,
            flow_id="f1",
            flow_version="1",
            status="suspended",
            context=dict(CHECKPOINT),
            dispatch_pending={
                "pending": True,
                "service_type": "delay",
                "service_config": dict(SERVICE_CONFIG),
                "node_id": "a",
                "recorded_at": "2026-10-18T00:00:00",
            },
        ),
    )


def _suspend_result(execution_id=EXEC):
    return {
        "execution_id": execution_id,
        "is_suspend": True,
        "service_config": dict(SERVICE_CONFIG),
        "context": dict(CHECKPOINT),
    }


def _queued(fake):
    return [json.loads(item) for item in fake.lrange(QUEUE, 0, -1)]


class TestSuspendPersistsDispatchMarker:
    """落盘凭据：先落盘（带凭据）→ 派发 → 派发成功后清凭据。"""

    def test_suspend_persists_marker_before_dispatch(self):
        """派发失败：凭据必须已经在盘上（否则这段窗口不可补偿）。"""
        worker, storage, fake = _worker()
        _seed_running(storage)
        fake.rpush = MagicMock(side_effect=ConnectionError("redis down during rpush"))

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            FE.return_value.run_distributed.return_value = _suspend_result()
            with pytest.raises(ServiceDispatchError):
                worker.resume_flow("f1", EXEC, "continue")

        state = storage.load_execution_state(EXEC)
        assert state.status == "suspended"
        assert state.end_time is None and state.error is None
        marker = state.dispatch_pending
        assert isinstance(marker, dict) and marker.get("pending") is True, (
            "派发失败时盘上必须留有派发凭据，否则重投载体无从补派（本单缺陷）"
        )
        assert marker["service_config"] == SERVICE_CONFIG
        assert marker["service_type"] == "delay"

    def test_successful_dispatch_clears_marker(self):
        """正常路径零变化：派发成功 → 恰 1 条任务、凭据清空、状态 suspended。"""
        worker, storage, fake = _worker()
        _seed_running(storage)

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            FE.return_value.run_distributed.return_value = _suspend_result()
            result = worker.resume_flow("f1", EXEC, "continue")

        assert result["is_suspend"] is True
        state = storage.load_execution_state(EXEC)
        assert state.status == "suspended"
        assert state.dispatch_pending is None
        assert len(_queued(fake)) == 1
        assert _queued(fake)[0]["execution_id"] == EXEC

    def test_non_service_suspend_writes_no_marker(self):
        """非服务节点挂起（无 service_config）不留凭据、不派发。"""
        worker, storage, fake = _worker()
        _seed_running(storage)
        result_no_service = {
            "execution_id": EXEC,
            "is_suspend": True,
            "context": dict(CHECKPOINT),
        }
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            FE.return_value.run_distributed.return_value = result_no_service
            worker.resume_flow("f1", EXEC, "continue")

        assert storage.load_execution_state(EXEC).dispatch_pending is None
        assert fake.llen(QUEUE) == 0


class TestResumeCarrierCompensation:
    """载体 1：resume(continue/retry) 的 #33 幂等短路必须补派（不重跑节点）。"""

    def _failed_then_redeliver(self):
        worker, storage, fake = _worker()
        _seed_running(storage)
        orig_rpush = fake.rpush
        fake.rpush = MagicMock(side_effect=ConnectionError("redis down"))
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            FE.return_value.run_distributed.return_value = _suspend_result()
            with pytest.raises(ServiceDispatchError):
                worker.resume_flow("f1", EXEC, "continue")
        assert fake.llen(QUEUE) == 0
        fake.rpush = orig_rpush
        return worker, storage, fake

    @pytest.mark.parametrize("resume_type", ["continue", "retry"])
    def test_redelivered_resume_dispatches_exactly_once(self, resume_type):
        worker, storage, fake = self._failed_then_redeliver()

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            result = worker.resume_flow("f1", EXEC, resume_type)

        # #33 契约不变：不重跑挂起节点、不造 FlowExecution、状态原样
        FE.assert_not_called()
        assert result["already_suspended"] is True
        assert result["dispatch_compensated"] is True
        state = storage.load_execution_state(EXEC)
        assert state.status == "suspended"
        assert state.error is None and state.end_time is None
        assert state.dispatch_pending is None, "补派成功后必须清凭据（幂等前提）"
        queued = _queued(fake)
        assert len(queued) == 1, "重投后必须恰有 1 条延迟任务"
        assert queued[0]["type"] == "delay"
        assert queued[0]["execution_id"] == EXEC

    def test_second_redelivery_adds_no_second_task(self):
        worker, storage, fake = self._failed_then_redeliver()

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            worker.resume_flow("f1", EXEC, "continue")
            again = worker.resume_flow("f1", EXEC, "continue")

        FE.assert_not_called()
        assert len(_queued(fake)) == 1, "清凭据后再投一次不得产生第 2 条任务"
        assert "dispatch_compensated" not in again
        assert storage.load_execution_state(EXEC).status == "suspended"

    def test_compensation_failure_keeps_marker_and_raises(self):
        """补派仍失败：不吞（ServiceDispatchError），凭据保留，下轮还能再补。"""
        worker, storage, fake = _worker()
        _seed_suspended_with_pending(storage)
        orig_rpush = fake.rpush
        fake.rpush = MagicMock(side_effect=ConnectionError("still down"))

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            with pytest.raises(ServiceDispatchError):
                worker.resume_flow("f1", EXEC, "continue")
        FE.assert_not_called()
        state = storage.load_execution_state(EXEC)
        assert state.status == "suspended"
        assert state.dispatch_pending is not None and state.dispatch_pending["pending"] is True
        assert fake.llen(QUEUE) == 0

        # 恢复后同一载体补派成功（at-least-once 收敛）
        fake.rpush = orig_rpush
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            worker.resume_flow("f1", EXEC, "continue")
        FE.assert_not_called()
        assert len(_queued(fake)) == 1


class TestStartCarrierCompensation:
    """载体 2/3：start 重投（dedup 命中 + 非 dedup）同样补派一次。"""

    def test_start_dedup_hit_compensates_pending_dispatch(self):
        worker, storage, fake = _worker()
        _seed_suspended_with_pending(storage)
        fake.set("plaita:start-dedup:k1", EXEC)

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            result = worker.start_flow("f1", {}, version="1", dedup_key="k1")

        # start 路径会先构造执行器（dedup/载体判定在此之前），要求的是"不跑"
        FE.return_value.run_distributed.assert_not_called()
        assert result["deduplicated"] is True
        assert result["dispatch_compensated"] is True
        assert len(_queued(fake)) == 1
        assert storage.load_execution_state(EXEC).dispatch_pending is None


class TestCompensationLeaseGuard:
    """补派必须持租约 + 持锁重读：不许用旧快照倒灌正在推进/已终态的执行。

    「补派成功后清凭据」是一次**状态行写**。无租约的写是 last-writer-wins：
    若另一 worker 正持租约把执行推向终态，旧 ``suspended`` 快照倒灌会把
    已终态执行唤醒。故补派这一段短暂持租约；持锁后重读，盘上已无凭据/已
    非 suspended 就跳过。租约被活跃持有者占着也只是跳过（对方会推进/重派）。
    """

    def test_skips_when_lease_held_by_active_worker(self):
        worker, storage, fake = _worker()
        _seed_suspended_with_pending(storage)
        held = worker.execution_lease.try_acquire(EXEC, "live-holder", 120)
        assert held is True

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            result = worker.resume_flow("f1", EXEC, "continue")

        FE.assert_not_called()
        assert result["already_suspended"] is True
        assert "dispatch_compensated" not in result
        assert fake.llen(QUEUE) == 0, "持有者活跃时不得并发补派"
        state = storage.load_execution_state(EXEC)
        assert state.status == "suspended"
        assert state.dispatch_pending is not None, "跳过时凭据必须原样保留"
        # 持有者租约未被顶掉
        assert worker.execution_lease.get_holder(EXEC) == "live-holder"

    def test_no_stale_write_when_disk_state_progressed(self):
        """入口快照还带凭据、盘上却已被推进成终态 ⇒ 不得派发、不得覆写。"""
        worker, storage, fake = _worker()
        stale = ExecutionState(
            execution_id=EXEC,
            flow_id="f1",
            flow_version="1",
            status="suspended",
            context=dict(CHECKPOINT),
            dispatch_pending={
                "pending": True,
                "service_type": "delay",
                "service_config": dict(SERVICE_CONFIG),
                "node_id": "a",
            },
        )
        storage.save_execution_state(
            EXEC,
            ExecutionState(
                execution_id=EXEC,
                flow_id="f1",
                flow_version="1",
                status="completed",
                context={"$LAST_NODE": "end"},
            ),
        )

        compensated = worker._compensate_suspend_dispatch(stale, EXEC, source="test")

        assert compensated is False
        assert fake.llen(QUEUE) == 0
        state = storage.load_execution_state(EXEC)
        assert state.status == "completed", "旧 suspended 快照绝不能倒灌覆盖终态"
        assert state.context == {"$LAST_NODE": "end"}
