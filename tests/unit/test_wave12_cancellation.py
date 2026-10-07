"""波次①（取消可达）+ 波次②（看门狗）单测——设计稿 §3.1/§3.3/§4.1、测试计划 T3/T4/T5。

覆盖：
- T3  步间取消（集群档）：循环中途回写标志键 → worker 下一步界终态化为
  cancelled、context 为上一步 checkpoint、不再翻回 running；
  resume 入口（XCLAIM 恢复路径）命中标志键直接终态化；
- T4  取消后 resume 被拒：cancelled 终态短路 already_terminal；
- T5  看门狗失效恢复：renew 失败 → execution.cancel() 被调（鸭子调用，
  波次③的 FlowExecution.cancel 已就位）、lease_lost 标记、worker 以
  ExecutionLeaseError 退出且不写 state；renew 抛异常（Redis 瞬断）不判死；
- 回滚开关：PLAITA_DISABLE_CANCEL_CHECKPOINT / PLAITA_DISABLE_LEASE_WATCHDOG；
- 兼容红线：无 redis 客户端的内存 worker 取消检查降级 no-op（§3.5）；
- 取消标志键租户路由。
"""
import time
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.server.flow_worker import FlowWorker, RedisFlowWorker
from plaita.server.execution_lease import ExecutionLeaseError
from plaita.server.tenant_context import reset_current_tenant, set_current_tenant
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage


TEST_FLOW = {
    "flow_id": "f1",
    "version": "1",
    "nodes": [
        {"id": "start", "type": "start", "next": "a"},
        {"id": "a", "type": "assignment", "result": {"step": 1}, "next": "b"},
        {"id": "b", "type": "assignment", "result": {"step": 2}, "next": "end"},
        {"id": "end", "type": "end", "output": "ok"},
    ],
}


def _make_worker(fake_redis, storage=None, flow_storage=None, **kwargs):
    kwargs.setdefault("lease_ttl_seconds", 60)
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:wave12-cancel",
        execution_storage=storage if storage is not None else MemoryExecutionStorage(),
        flow_storage=flow_storage if flow_storage is not None else MemoryFlowStorage(),
        redis_client=fake_redis,
        enable_registry=False,
        enable_redis_logging=False,
        **kwargs,
    )


def _save_running_state(storage, execution_id="exec-1", context=None):
    state = ExecutionState(
        execution_id=execution_id,
        flow_id="f1",
        flow_version="1",
        context=context if context is not None else {"$LAST_NODE": "start", "$NODE": {}},
        status="running",
    )
    storage.save_execution_state(execution_id, state)
    return state


class TestStepBoundaryCancel:
    """T3：步间取消（集群档）。"""

    def test_t3_cancel_flag_between_steps_terminalizes_at_boundary(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        flow_storage = MemoryFlowStorage()
        flow_storage.save_flow(TEST_FLOW)
        worker = _make_worker(fake, storage, flow_storage)
        _save_running_state(storage)

        step1 = {
            "execution_id": "exec-1",
            "is_end": False,
            "is_suspend": False,
            "context": {"$LAST_NODE": "a", "step": 1},
        }

        def run_step(flow, **kwargs):
            # BFF 语义等价：运行中执行取消 → 只写标志键（7 天 TTL）
            fake.set(
                "plaita:execution:cancel:exec-1",
                "2026-10-02T00:00:00",
                ex=7 * 86400,
            )
            return step1

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.run_distributed.side_effect = run_step
            result = worker.resume_flow("f1", "exec-1", "continue")

        # 步界终态化，不再推进（run_distributed 只被 resume 入口调了一次）
        assert inst.run_distributed.call_count == 1
        state = storage.load_execution_state("exec-1")
        assert state.status == "cancelled"
        # context = 取消点前一步的完整 checkpoint（§3.3）
        assert state.context == {"$LAST_NODE": "a", "step": 1}
        assert state.end_time is not None

    def test_t3_cancel_flag_at_resume_entry_terminalizes_without_advance(self):
        """XCLAIM 恢复路径同享检查点：crash 后消息重投，标志在 → 直接终态化。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        flow_storage = MemoryFlowStorage()
        flow_storage.save_flow(TEST_FLOW)
        worker = _make_worker(fake, storage, flow_storage)
        _save_running_state(storage)
        fake.set("plaita:execution:cancel:exec-1", "ts", ex=7 * 86400)

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            result = worker.resume_flow("f1", "exec-1", "continue")

        # 未创建/未推进 FlowExecution
        FE.assert_not_called()
        assert result["status"] == "cancelled"
        assert result["cancelled_at_resume"] is True
        assert storage.load_execution_state("exec-1").status == "cancelled"
        # 租约已释放（finally）
        assert fake.get("plaita:execution:lease:exec-1") is None

    def test_t3_lease_released_after_boundary_cancel(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        flow_storage = MemoryFlowStorage()
        flow_storage.save_flow(TEST_FLOW)
        worker = _make_worker(fake, storage, flow_storage)
        _save_running_state(storage)

        def run_step(flow, **kwargs):
            fake.set("plaita:execution:cancel:exec-1", "ts", ex=7 * 86400)
            return {
                "execution_id": "exec-1",
                "is_end": False,
                "is_suspend": False,
                "context": {"step": 1},
            }

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.run_distributed.side_effect = run_step
            worker.resume_flow("f1", "exec-1", "continue")

        assert storage.load_execution_state("exec-1").status == "cancelled"
        assert fake.get("plaita:execution:lease:exec-1") is None


class TestCancelledResumeRefused:
    """T4：取消后 resume 被拒（现有 E2E 终态短路语义的集群档延伸）。"""

    def test_t4_resume_on_cancelled_is_already_terminal(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        flow_storage = MemoryFlowStorage()
        flow_storage.save_flow(TEST_FLOW)
        worker = _make_worker(fake, storage, flow_storage)
        _save_running_state(storage)
        # 挂起取消路径（保持现状）：控制面直写 cancelled
        state = storage.load_execution_state("exec-1")
        state.status = "cancelled"
        storage.save_execution_state("exec-1", state)

        result = worker.resume_flow("f1", "exec-1", "cancel", {})
        assert result["already_terminal"] is True
        assert result["status"] == "cancelled"
        assert storage.load_execution_state("exec-1").status == "cancelled"


class TestWatchdogLeaseLost:
    """T5：看门狗失效恢复。"""

    def _worker_with_watch(self, fake, storage, flow_storage):
        worker = _make_worker(
            fake, storage, flow_storage, watchdog_interval_seconds=0.05
        )
        _save_running_state(storage, "exec-t5")
        execution = MagicMock()
        worker._register_lease_watch("exec-t5", "holder-x:1", execution)
        return worker, execution

    def test_t5_renew_failure_marks_lost_and_cancels_execution(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        flow_storage = MemoryFlowStorage()
        flow_storage.save_flow(TEST_FLOW)
        worker, execution = self._worker_with_watch(fake, storage, flow_storage)
        try:
            with patch.object(worker.execution_lease, "renew", return_value=False):
                worker._watchdog_renew_once()

            # 鸭子调 execution.cancel() 被触发（波次③ FlowExecution.cancel 已就位）
            execution.cancel.assert_called_once()
            # lease_lost 标记 → 步界续租与落盘前检查都自爆，不写状态
            with pytest.raises(ExecutionLeaseError):
                worker._renew_lease_if_held("exec-t5", "holder-x:1")
            with pytest.raises(ExecutionLeaseError):
                worker._raise_if_lease_lost("exec-t5")
            assert storage.load_execution_state("exec-t5").status == "running"
        finally:
            worker._stop_lease_watchdog()

    def test_t5_watchdog_thread_flags_lease_lost(self):
        """线程模式：看门狗线程注入 renew 连续失败 → 失租标记生效。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        flow_storage = MemoryFlowStorage()
        flow_storage.save_flow(TEST_FLOW)
        worker, execution = self._worker_with_watch(fake, storage, flow_storage)
        try:
            worker._start_lease_watchdog()
            with patch.object(worker.execution_lease, "renew", return_value=False):
                deadline = time.monotonic() + 3.0
                while time.monotonic() < deadline:
                    try:
                        worker._raise_if_lease_lost("exec-t5")
                    except ExecutionLeaseError:
                        execution.cancel.assert_called_once()
                        break
                    time.sleep(0.02)
                else:
                    pytest.fail("看门狗线程未在 3s 内标记 lease_lost")
        finally:
            worker._stop_lease_watchdog()
            worker._unregister_lease_watch("exec-t5", "holder-x:1")

    def test_t5_transient_renew_error_does_not_flag_lost(self):
        """renew 抛异常（Redis 瞬断）不判死，下周期重试。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        flow_storage = MemoryFlowStorage()
        flow_storage.save_flow(TEST_FLOW)
        worker, execution = self._worker_with_watch(fake, storage, flow_storage)
        try:
            with patch.object(
                worker.execution_lease, "renew", side_effect=RuntimeError("boom")
            ):
                worker._watchdog_renew_once()  # 不抛
            worker._raise_if_lease_lost("exec-t5")  # 未标记 → 不抛
            execution.cancel.assert_not_called()
        finally:
            worker._stop_lease_watchdog()

    def test_re_registration_clears_stale_lease_lost(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        flow_storage = MemoryFlowStorage()
        worker = _make_worker(fake, storage, flow_storage)
        execution = MagicMock()
        worker._register_lease_watch("e1", "h:1", execution)
        with patch.object(worker.execution_lease, "renew", return_value=False):
            worker._watchdog_renew_once()
        with pytest.raises(ExecutionLeaseError):
            worker._raise_if_lease_lost("e1")
        # 重投的 resume 取得新世代租约 → 陈旧失租标记清除
        worker._register_lease_watch("e1", "h:2", execution)
        worker._raise_if_lease_lost("e1")  # 不抛
        worker._stop_lease_watchdog()

    def test_watchdog_renews_under_registered_tenant(self):
        """看门狗 renew 在登记时的租户上下文内执行（TenantRoutingLease 路由依赖）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        flow_storage = MemoryFlowStorage()
        worker = _make_worker(fake, storage, flow_storage)
        execution = MagicMock()
        token = set_current_tenant("tenant-a")
        try:
            worker._register_lease_watch("e1", "h:1", execution)
        finally:
            reset_current_tenant(token)

        seen_tenants = []

        def fake_renew(execution_id, holder, ttl):
            from plaita.server.tenant_context import current_tenant

            seen_tenants.append(current_tenant())
            return True

        try:
            with patch.object(worker.execution_lease, "renew", side_effect=fake_renew):
                worker._watchdog_renew_once()
            assert seen_tenants == ["tenant-a"]
        finally:
            worker._unregister_lease_watch("e1", "h:1")


class TestRollbackSwitches:
    """波次①②回滚开关（设计稿 §6）。"""

    def test_disable_cancel_checkpoint_skips_flag_check(self, monkeypatch):
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        flow_storage = MemoryFlowStorage()
        flow_storage.save_flow(TEST_FLOW)
        worker = _make_worker(fake, storage, flow_storage)
        fake.set("plaita:execution:cancel:exec-1", "ts", ex=7 * 86400)
        assert worker._cancel_requested("exec-1") is True

        monkeypatch.setenv("PLAITA_DISABLE_CANCEL_CHECKPOINT", "1")
        assert worker._cancel_requested("exec-1") is False

    def test_disable_fencing_falls_back_to_plain_acquire(self, monkeypatch):
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        monkeypatch.setenv("PLAITA_DISABLE_FENCING", "1")
        lease_value, fence_token = worker._acquire_lease("e1", "holder-a")
        assert lease_value == "holder-a"
        assert fence_token is None
        # 普通档租约值串即 holder
        assert fake.get("plaita:execution:lease:e1") == "holder-a"

    def test_disable_watchdog_does_not_start_thread(self, monkeypatch):
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        monkeypatch.setenv("PLAITA_DISABLE_LEASE_WATCHDOG", "1")
        worker._start_lease_watchdog()
        assert worker._watchdog_thread is None
        worker._stop_lease_watchdog()

    def test_watchdog_starts_and_stops(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        worker._start_lease_watchdog()
        assert worker._watchdog_thread is not None
        assert worker._watchdog_thread.is_alive()
        worker._stop_lease_watchdog()
        assert worker._watchdog_thread is None


class TestGracefulDegradation:
    """§3.5 兼容红线：无 Redis 的内存 worker / 单测路径容错降级。"""

    def test_flow_worker_without_redis_client_never_cancels(self):
        worker = FlowWorker(MemoryExecutionStorage(), MemoryFlowStorage())
        assert worker._cancel_requested("e1") is False

    def test_redis_error_on_flag_check_degrades_to_not_cancelled(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)

        class Boom:
            def exists(self, key):
                raise RuntimeError("redis down")

        worker.redis_client = Boom()
        assert worker._cancel_requested("e1") is False

    def test_cancel_flag_key_follows_tenant_namespace(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        token = set_current_tenant("tenant-x")
        try:
            assert worker._cancel_flag_key("e1") == "plaita:tenant-x:execution:cancel:e1"
        finally:
            reset_current_tenant(token)
        assert worker._cancel_flag_key("e1") == "plaita:execution:cancel:e1"
