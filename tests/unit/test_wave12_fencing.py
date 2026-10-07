"""波次②（fencing）单测——设计稿 §4.1/§4.2、测试计划 T1/T2/T10。

覆盖：
- T1  双 worker 竞争同一执行：A 持租约（fenced/普通两档），B resume_flow
  抛 ExecutionLeaseError 且消息留 pending（at-least-once 回收语义）；
- T2  单步超 TTL：无看门狗时 B 接管、A 步界自爆不写状态（state 非 error）；
  有看门狗时 A renew 成功、B try_acquire 失败；
- T10 fencing 世代号 CAS：旧 gen 写被拒、新 gen 写成功；fence 键缺失
  （旧数据）首 acquire 后可写；未持 fenced 租约（start 路径/单测）退化普通写；
- tenant_context._storage_cls 同位注入接线 + PLAITA_DISABLE_FENCING 回滚。

模拟基建：fakeredis（eval 需要 lupa）+ lease_ttl_seconds/watchdog_interval_seconds
参数化缩短窗口。
"""
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.server.execution_lease import (
    ExecutionLeaseError,
    FENCE_KEY_TTL_SECONDS,
    RedisExecutionLease,
)
from plaita.server.flow_worker import RedisFlowWorker
from plaita.storage.base import ExecutionState
from plaita.storage.fenced import (
    FencedExecutionStorage,
    current_fence_token,
    reset_current_fence_token,
    set_current_fence_token,
)
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage
from plaita.storage.redis import (
    DEFAULT_EXECUTION_STATE_TTL_DAYS,
    RedisExecutionStorage,
)

TEST_FLOW = {
    "flow_id": "f1",
    "version": "1",
    "nodes": [
        {"id": "start", "type": "start", "next": "end"},
        {"id": "end", "type": "end", "output": "ok"},
    ],
}

STATE = dict(
    execution_id="exec-1",
    flow_id="f1",
    flow_version="1",
    context={"$LAST_NODE": "start", "$NODE": {}},
    status="suspended",
)


def _make_worker(fake_redis, storage=None, flow_storage=None, **kwargs):
    kwargs.setdefault("lease_ttl_seconds", 60)
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:wave12-fencing",
        execution_storage=storage if storage is not None else MemoryExecutionStorage(),
        flow_storage=flow_storage if flow_storage is not None else MemoryFlowStorage(),
        redis_client=fake_redis,
        enable_registry=False,
        enable_redis_logging=False,
        **kwargs,
    )


class TestFencedLeasePrimitives:
    def test_fenced_acquire_exclusive_and_generation_increases(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        lease = RedisExecutionLease(fake)
        gen1 = lease.try_acquire_fenced("e1", "h1", 60)
        assert gen1 == 1
        # NX 语义保持：A 持租约期间 B acquire 失败（T1 契约）
        assert lease.try_acquire_fenced("e1", "h2", 60) is None
        # 普通 try_acquire 同样互斥
        assert lease.try_acquire("e1", "h2", 60) is False

    def test_lease_value_is_holder_colon_gen_and_renew_release_compare_full_value(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        lease = RedisExecutionLease(fake)
        gen = lease.try_acquire_fenced("e1", "h1", 60)
        assert fake.get("plaita:execution:lease:e1") == f"h1:{gen}"
        # renew/release 必须用完整 value 串
        assert lease.renew("e1", "h1", 60) is False
        assert lease.release("e1", "h1") is False
        assert lease.renew("e1", f"h1:{gen}", 60) is True
        assert lease.release("e1", f"h1:{gen}") is True
        assert fake.get("plaita:execution:lease:e1") is None

    def test_fence_key_created_with_ttl(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        lease = RedisExecutionLease(fake)
        lease.try_acquire_fenced("e1", "h1", 60)
        assert fake.get("plaita:execution:fence:e1") == "1"
        ttl = fake.ttl("plaita:execution:fence:e1")
        assert 0 < ttl <= FENCE_KEY_TTL_SECONDS

    def test_generation_bumps_on_takeover_after_expiry(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        lease = RedisExecutionLease(fake)
        gen1 = lease.try_acquire_fenced("e1", "h1", 60)
        fake.delete("plaita:execution:lease:e1")  # 模拟租约过期
        gen2 = lease.try_acquire_fenced("e1", "h2", 60)
        assert gen2 == gen1 + 1


class TestFencedStorageCAS:
    """T10：fencing 世代号 CAS 写。"""

    def _storage(self, fake):
        return FencedExecutionStorage(RedisExecutionStorage(client=fake))

    def _state(self):
        return ExecutionState(**STATE)

    def test_t10_new_gen_writes_old_gen_refused(self):
        fake = fakeredis.FakeStrictRedis(decode_responses=True)
        fenced = self._storage(fake)
        lease = RedisExecutionLease(fake)

        gen1 = lease.try_acquire_fenced("exec-1", "h1", 60)
        token1 = set_current_fence_token(gen1)
        try:
            assert fenced.save_execution_state("exec-1", self._state()) is True
        finally:
            reset_current_fence_token(token1)

        # 新世代接管（旧租约过期后 B 抢到）
        fake.delete("plaita:execution:lease:exec-1")
        gen2 = lease.try_acquire_fenced("exec-1", "h2", 60)
        assert gen2 == gen1 + 1

        # 旧世代写被拒并抛 ExecutionLeaseError
        token1b = set_current_fence_token(gen1)
        try:
            with pytest.raises(ExecutionLeaseError):
                fenced.save_execution_state("exec-1", self._state())
        finally:
            reset_current_fence_token(token1b)

        # 新世代写成功
        token2 = set_current_fence_token(gen2)
        try:
            assert fenced.save_execution_state("exec-1", self._state()) is True
        finally:
            reset_current_fence_token(token2)

    def test_t10_missing_fence_key_writable_after_first_acquire(self):
        """旧数据兼容：fence 键缺失 → 首 acquire 创建（INCR 建键）后可写。"""
        fake = fakeredis.FakeStrictRedis(decode_responses=True)
        fenced = self._storage(fake)
        lease = RedisExecutionLease(fake)
        # 模拟旧 worker 世界的存量租约/无 fence 键
        fake.set("plaita:execution:lease:exec-1", "old-worker", ex=60)
        assert fake.exists("plaita:execution:fence:exec-1") == 0

        fake.delete("plaita:execution:lease:exec-1")
        gen = lease.try_acquire_fenced("exec-1", "new-worker", 60)
        assert gen == 1
        token = set_current_fence_token(gen)
        try:
            assert fenced.save_execution_state("exec-1", self._state()) is True
        finally:
            reset_current_fence_token(token)

    def test_no_fence_token_degrades_to_plain_write(self):
        """未持 fenced 租约（start 路径/单测）→ NullLease 同款 no-op 普通写。"""
        fake = fakeredis.FakeStrictRedis(decode_responses=True)
        fenced = self._storage(fake)
        assert current_fence_token() is None
        assert fenced.save_execution_state("exec-1", self._state()) is True
        assert fenced.load_execution_state("exec-1").status == "suspended"

    def test_missing_fence_key_with_token_set_refuses_write(self):
        """持有世代但 fence 键被清 → CAS 不符拒绝（保守侧失败）。"""
        fake = fakeredis.FakeStrictRedis(decode_responses=True)
        fenced = self._storage(fake)
        token = set_current_fence_token(7)
        try:
            with pytest.raises(ExecutionLeaseError):
                fenced.save_execution_state("exec-1", self._state())
        finally:
            reset_current_fence_token(token)

    def test_delegates_non_save_methods(self):
        fake = fakeredis.FakeStrictRedis(decode_responses=True)
        fenced = self._storage(fake)
        fenced.save_execution_state("exec-1", self._state())
        assert fenced.load_execution_state("exec-1").execution_id == "exec-1"
        assert fenced.delete_execution_state("exec-1") is True
        assert fenced.load_execution_state("exec-1") is None
        assert fenced.get_namespace_key("execution", "x") == "plaita:execution:x"


class TestTenantRoutingInjection:
    def test_storage_cls_injects_fenced_wrapper(self):
        from plaita.server.tenant_context import (
            TenantRoutingExecutionStorage,
            tenant_namespace,
        )

        assert TenantRoutingExecutionStorage._storage_cls is not RedisExecutionStorage
        storage = TenantRoutingExecutionStorage(host="localhost")
        inner = storage._storage_for("default")
        assert isinstance(inner, FencedExecutionStorage)
        assert isinstance(inner._inner, RedisExecutionStorage)
        assert inner._inner.namespace == tenant_namespace("default")

    def test_disable_fencing_rollback_unwraps_storage(self, monkeypatch):
        from plaita.server.tenant_context import TenantRoutingExecutionStorage

        monkeypatch.setenv("PLAITA_DISABLE_FENCING", "1")
        storage = TenantRoutingExecutionStorage(host="localhost")
        inner = storage._storage_for("default")
        assert isinstance(inner, RedisExecutionStorage)
        assert not isinstance(inner, FencedExecutionStorage)


class TestFencedTerminalStateTTL:
    """Track B 任务2（2026-10-02 评审）：fenced 落盘的终态键没有 TTL。

    普通路径 ``RedisExecutionStorage.save_execution_state`` 对终态
    （completed/error/cancelled）写 TTL（默认 30 天，env
    ``PLAITA_EXECUTION_STATE_TTL_DAYS`` 可调，<=0 关）；fenced Lua 只 SET
    不带 EX——经 worker resume 路径（fenced）落盘的终态键永不过期，只能靠
    console 读时补偿。修复后 fenced 与普通路径 TTL 语义对齐（常量/env 复用
    storage.redis，不抄一份），非终态 ttl=0 保持无 TTL 现状。
    """

    def _storage(self, fake):
        return FencedExecutionStorage(RedisExecutionStorage(client=fake))

    def _state(self, status="completed"):
        return ExecutionState(**{**STATE, "status": status})

    def _save_with_fenced_token(self, fake, status) -> None:
        fenced = self._storage(fake)
        lease = RedisExecutionLease(fake)
        gen = lease.try_acquire_fenced("exec-1", "h1", 60)
        assert gen is not None
        token = set_current_fence_token(gen)
        try:
            assert fenced.save_execution_state("exec-1", self._state(status)) is True
        finally:
            reset_current_fence_token(token)

    @pytest.mark.parametrize("status", ["completed", "error", "cancelled"])
    def test_terminal_state_via_fenced_gets_ttl(self, status):
        fake = fakeredis.FakeStrictRedis(decode_responses=True)
        self._save_with_fenced_token(fake, status)
        ttl = fake.ttl("plaita:execution:exec-1")
        assert 0 < ttl <= DEFAULT_EXECUTION_STATE_TTL_DAYS * 86400

    @pytest.mark.parametrize("status", ["running", "suspended"])
    def test_non_terminal_state_via_fenced_no_ttl(self, status):
        fake = fakeredis.FakeStrictRedis(decode_responses=True)
        self._save_with_fenced_token(fake, status)
        assert fake.ttl("plaita:execution:exec-1") == -1

    def test_fenced_terminal_ttl_env_override(self):
        """env 语义经复用自动继承：7 天 → TTL ≤ 7 天。"""
        fake = fakeredis.FakeStrictRedis(decode_responses=True)
        with pytest.MonkeyPatch.context() as mp:
            mp.setenv("PLAITA_EXECUTION_STATE_TTL_DAYS", "7")
            self._save_with_fenced_token(fake, "completed")
        ttl = fake.ttl("plaita:execution:exec-1")
        assert 0 < ttl <= 7 * 86400

    def test_fenced_terminal_ttl_env_disable(self):
        """env <=0 关 TTL：fenced 终态同样不带 EX。"""
        fake = fakeredis.FakeStrictRedis(decode_responses=True)
        with pytest.MonkeyPatch.context() as mp:
            mp.setenv("PLAITA_EXECUTION_STATE_TTL_DAYS", "0")
            self._save_with_fenced_token(fake, "completed")
        assert fake.ttl("plaita:execution:exec-1") == -1


class TestT1ConcurrentResume:
    def test_t1_second_worker_refused_and_message_stays_pending(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        flow_storage = MemoryFlowStorage()
        flow_storage.save_flow(TEST_FLOW)
        storage.save_execution_state("exec-1", ExecutionState(**STATE))

        lease = RedisExecutionLease(fake)
        worker_a = _make_worker(fake, storage, flow_storage, execution_lease=lease)
        worker_b = _make_worker(fake, storage, flow_storage, execution_lease=lease)

        # A 持租约不 renew
        assert lease.try_acquire("exec-1", "holder-a", 60) is True

        from plaita.server.task_queue import RedisStreamTaskQueue

        queue = RedisStreamTaskQueue(fake, "test:wave12-t1", consumer_name="w-b")
        queue.ensure_group()
        queue.enqueue(
            {
                "type": "resume",
                "flow_id": "f1",
                "execution_id": "exec-1",
                "resume_type": "continue",
                "tenant_id": "default",
            }
        )
        task = queue.read(block_ms=100)
        assert task is not None

        with pytest.raises(ExecutionLeaseError):
            worker_b._dispatch_task(task.body)

        # run() 现路径：ExecutionLeaseError → 不 ack + note_lease_conflict
        try:
            worker_b._dispatch_task(task.body)
            queue.ack(task.message_id)
        except ExecutionLeaseError:
            queue.note_lease_conflict()
        summary = fake.xpending("test:wave12-t1", queue.group_name)
        pending = summary["pending"] if isinstance(summary, dict) else summary[0]
        assert int(pending) == 1  # 消息留 pending，待租约过期回收
        assert queue.stats()["lease_conflicts"] == 1
        # 状态未被 B 改写
        assert storage.load_execution_state("exec-1").status == "suspended"

    def test_t1_fenced_acquire_refuses_concurrent_resume(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        flow_storage = MemoryFlowStorage()
        flow_storage.save_flow(TEST_FLOW)
        storage.save_execution_state("exec-1", ExecutionState(**STATE))

        lease = RedisExecutionLease(fake)
        worker_b = _make_worker(fake, storage, flow_storage, execution_lease=lease)
        gen = lease.try_acquire_fenced("exec-1", "holder-a", 60)
        assert gen == 1
        with pytest.raises(ExecutionLeaseError):
            worker_b.resume_flow("f1", "exec-1", "continue")


class TestT2StepExceedsTtl:
    def test_t2_without_watchdog_b_takes_over_and_a_self_aborts_clean(self):
        """无看门狗：步 > TTL → 租约过期 → A 步界 renew 失败自爆且不写状态。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        flow_storage = MemoryFlowStorage()
        flow_storage.save_flow(TEST_FLOW)
        storage.save_execution_state("exec-1", ExecutionState(**STATE))
        lease = RedisExecutionLease(fake)
        worker_a = _make_worker(
            fake, storage, flow_storage, execution_lease=lease, lease_ttl_seconds=1
        )

        def slow_step(flow, **kwargs):
            time.sleep(1.4)  # > ttl=1：步内租约过期
            return {
                "execution_id": "exec-1",
                "is_end": False,
                "is_suspend": False,
                "context": {"step": 1},
            }

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.run_distributed.side_effect = slow_step
            with pytest.raises(ExecutionLeaseError):
                worker_a.resume_flow("f1", "exec-1", "continue")

        # A 自爆：状态非 error、未被改写
        assert storage.load_execution_state("exec-1").status == "suspended"
        # B 可接管（世代递增 = fencing 新纪元）
        gen = lease.try_acquire_fenced("exec-1", "worker-b", 60)
        assert gen is not None

    def test_t2_with_watchdog_a_renews_and_b_acquire_fails(self):
        """有看门狗：长步期间租约被持续续期，B 接管失败。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        flow_storage = MemoryFlowStorage()
        flow_storage.save_flow(TEST_FLOW)
        storage.save_execution_state("exec-1", ExecutionState(**STATE))
        lease = RedisExecutionLease(fake)
        worker_a = _make_worker(
            fake,
            storage,
            flow_storage,
            execution_lease=lease,
            lease_ttl_seconds=2,
            watchdog_interval_seconds=0.2,
        )
        worker_b = _make_worker(
            fake, storage, flow_storage, execution_lease=lease, lease_ttl_seconds=2
        )
        worker_a._start_lease_watchdog()
        try:
            step_started = threading.Event()
            takeover_done = threading.Event()
            step_calls = {"n": 0}

            def long_step(flow, **kwargs):
                # 第一步长跑：等全部接管尝试落定（由接管线程显式收尾）且至少
                # 跑满 1.5s（> 无看门狗时的安全窗口），看门狗每 0.2s 续租；
                # 第二步立即终态，保证推进循环退出。
                # 不用固定墙钟 + 固定尝试次数：单次接管尝试约 0.23s，8 次
                # 约 1.8s > 1.5s，最后一次尝试会落到 A 释放租约**之后**而合法
                # 抢到租约——环境越慢越必现的假红。
                step_calls["n"] += 1
                if step_calls["n"] > 1:
                    return {
                        "execution_id": "exec-1",
                        "is_end": True,
                        "is_suspend": False,
                        "context": {"step": 2},
                        "result": "done",
                    }
                step_started.set()
                deadline = time.monotonic() + 1.5
                takeover_done.wait(timeout=10.0)
                while time.monotonic() < deadline:
                    time.sleep(0.05)
                return {
                    "execution_id": "exec-1",
                    "is_end": False,
                    "is_suspend": False,
                    "context": {"step": 1},
                }

            takeover_errors = []

            def try_takeover():
                # 等 A 真持租约再开始（否则首次尝试可能抢在 A 的 acquire
                # 之前，把「持租约期间拒绝接管」测成裸抢锁竞速）
                step_started.wait(timeout=10.0)
                try:
                    # A 的步进行中反复尝试接管
                    for _ in range(8):
                        time.sleep(0.15)
                        try:
                            worker_b.resume_flow("f1", "exec-1", "continue")
                            takeover_errors.append(None)  # 不应发生
                        except ExecutionLeaseError:
                            takeover_errors.append("refused")
                        except Exception as exc:  # noqa: BLE001
                            takeover_errors.append(f"{type(exc).__name__}: {exc}")
                finally:
                    takeover_done.set()

            with patch("plaita.server.flow_worker.FlowExecution") as FE:
                inst = MagicMock()
                FE.return_value = inst
                inst.run_distributed.side_effect = long_step
                th = threading.Thread(target=try_takeover)
                th.start()
                result = worker_a.resume_flow("f1", "exec-1", "continue")
                th.join(timeout=10)

            # B 的所有接管尝试都被拒
            assert takeover_errors and all(e == "refused" for e in takeover_errors)
            # A 长步全程续租，正常推进到终态（未被自爆、状态未被拒绝写）
            assert result["is_end"] is True
            assert storage.load_execution_state("exec-1").status == "completed"
            worker_a._stop_lease_watchdog()
        finally:
            worker_a._stop_lease_watchdog()
