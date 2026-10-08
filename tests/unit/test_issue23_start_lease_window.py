"""#23：start 消息租约窗口——多 worker 池下长任务不被 XCLAIM 重派双跑。

场景（okguitar 提报）：`_acquire_lease` 在 A′（2026-10-06）后仍只覆盖
「落 running 行**之后**」的窗口，且 schedule_service 派发的 start 消息
无 execution_id 也无 dedup_key——数小时 run 消费 60s（claim_min_idle_ms）
后即被空闲 worker XCLAIM 重派：

- 有 dedup_key：重派者命中 running 分支重入队 resume，resume 取得租约从
  checkpoint 推进，与原 start 持有者并发推进同一 execution；
- 无 dedup_key（schedule 消息原状）：重派者就地铸造新 id，整条 flow 从头
  双跑双烧。

修复契约（本文件锁定）：
- start 租约窗口前移到**落 running 行之前**：重派者拿不到租约 →
  ExecutionLeaseError，原持有者的 running 行不被抢占者覆写；
- 验收臂 1（场景级）：双 worker 池 + 长任务——start 消息消费期间被另一
  worker XCLAIM，重派处理不推进、不落新行、不双跑；
- 验收臂 2：kill 掉持有者（释放租约模拟死亡 + TTL 过期）后，重派消息能
  取得租约接管（start 从头重跑语义不变）；
- schedule 消息带确定性 dedup_key（sched:{id}:{时点秒}），重派命中 dedup
  也不再产生平行执行。
"""
import json
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.server.execution_lease import ExecutionLeaseError, RedisExecutionLease
from plaita.server.flow_worker import FlowWorker, RedisFlowWorker
from plaita.server.services.schedule_service import fire_schedule
from plaita.server.task_queue import RedisStreamTaskQueue
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

TEST_FLOW = {
    "flow_id": "f1",
    "version": "1",
    "nodes": [
        {"id": "start", "type": "start", "next": "end"},
        {"id": "end", "type": "end", "output": "ok"},
    ],
}

QUEUE = "test:issue23-start-lease"


def _flow_storage():
    fs = MemoryFlowStorage()
    fs.save_flow(TEST_FLOW)
    return fs


def _redis_worker(fake, storage=None, flow_storage=None, **kwargs) -> RedisFlowWorker:
    kwargs.setdefault("enable_registry", False)
    kwargs.setdefault("enable_redis_logging", False)
    kwargs.setdefault("lease_ttl_seconds", 120)
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name=QUEUE,
        execution_storage=storage if storage is not None else MemoryExecutionStorage(),
        flow_storage=flow_storage if flow_storage is not None else _flow_storage(),
        redis_client=fake,
        **kwargs,
    )


def _lease_key(fake, execution_id: str) -> str:
    """worker 默认租约键（default 租户 = 历史 plaita 前缀）。"""
    key = f"plaita:execution:lease:{execution_id}"
    assert fake.exists(key), f"租约键 {key} 不存在"
    return key


def _reclaim_as_other_worker(fake, consumer: str = "worker-B", min_idle_ms: int = 1):
    """模拟空闲 worker 的回收循环：XCLAIM 一条 idle 超阈的 pending 消息。"""
    q = RedisStreamTaskQueue(
        fake, QUEUE, group_name="plaita-workers", consumer_name=consumer,
        claim_min_idle_ms=min_idle_ms,
    )
    q.ensure_group()
    task = q.read(block_ms=50)
    if task is None:
        return None
    # read 内部已对 idle 超阈条目 XCLAIM（ownership 转移到 worker-B）
    return q, task


class TestStartLeaseCoversWholeWindow:
    """租约窗口前移到落 running 行之前（#23 核心）。"""

    def test_lease_acquired_before_running_row_persisted(self):
        """acquire 严格先于先行落行：acquire 失败时**不落** running 行。

        A′ 的顺序是「落行 → acquire」，重派者在两步之间抢到租约后，原持有者
        已落的行会被抢占者的推进覆写。前移后失败路径零落盘。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        worker = _redis_worker(fake, storage)
        # 他人先持租约：start 在任何落盘动作之前就该被拒
        assert worker.execution_lease.try_acquire("exec-x", "other-worker", 120)
        with pytest.raises(ExecutionLeaseError):
            worker.start_flow("f1", {}, version="1", execution_id="exec-x")
        assert storage.load_execution_state("exec-x") is None, (
            "start 被拒后不得留下 running 行（落行必须发生在 acquire 之后）"
        )

    def test_running_row_persisted_while_lease_held(self):
        """正常路径：落行时租约已在手（行与后续写同属一个持租约窗口）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        worker = _redis_worker(fake, storage)
        worker.start_flow("f1", {}, version="1", execution_id="exec-y")
        assert storage.load_execution_state("exec-y").status == "completed"
        # 处理结束租约已释放（finally 语义，A′ 契约保留）
        assert not fake.exists("plaita:execution:lease:exec-y")


class TestReclaimedStartDoesNotDoubleRun:
    """验收臂 1：双 worker 池 + 长任务，start 消费期间不被重派双跑。"""

    def _long_running_start_scene(self, *, dedup_key=None):
        """构造「worker-A 正在处理长 start 消息」的场景。

        消息带预铸 execution_id（schedule 消息补 dedup_key 后与 BFF 形态对齐；
        无预铸 id 时 worker 就地铸造，断言以落盘行为准）。FlowExecution.
        run_distributed 挂起在 barrier 上模拟数小时 AGENTRUN。
        返回 (fake, storage, barrier, release, queue_a, task, execution_id)。
        """
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        worker_a = _redis_worker(fake, storage)

        queue_a = RedisStreamTaskQueue(
            fake, QUEUE, group_name="plaita-workers", consumer_name="worker-A",
            claim_min_idle_ms=60_000,
        )
        queue_a.ensure_group()

        message = {
            "type": "start", "flow_id": "f1", "version": "1",
            "params": {}, "tenant_id": "default", "execution_id": "exec-a",
        }
        if dedup_key:
            message["dedup_key"] = dedup_key
        queue_a.enqueue(message)

        task = queue_a.read(block_ms=100)
        assert task is not None

        barrier = threading.Event()
        release = threading.Event()

        def blocked_run_distributed(*args, **kwargs):
            barrier.set()
            assert release.wait(10), "test harness: release 未到"
            return {"execution_id": "exec-a", "is_end": True, "context": {}}

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.execution_id = "exec-a"
            inst.run_distributed.side_effect = blocked_run_distributed

            def process():
                try:
                    worker_a._dispatch_task(task.body, delivery_count=task.delivery_count)
                    queue_a.ack(task.message_id)
                except ExecutionLeaseError:
                    pass  # run() 的冲突语义（本用例不应触达）

            t = threading.Thread(target=process, daemon=True)
            t.start()
            assert barrier.wait(10), "worker-A 未进入首节点执行"

        return fake, storage, barrier, release, queue_a, task

    def test_long_start_not_reclaimed_into_parallel_run(self):
        """>claim_min_idle 后消息被 worker-B XCLAIM：不双跑、不落新执行。

        重派者（worker-B 处理同一条消息）拿不到租约 → ExecutionLeaseError →
        run() 对该分支 ack 释放；期间 worker-A 的执行仍持租约独占推进。
        """
        fake, storage, barrier, release, queue_a, task = (
            self._long_running_start_scene()
        )
        try:
            # 消息 idle 超过 claim_min_idle（老 worker 持有 60s+），空闲 worker-B 回收
            time.sleep(0.02)
            stolen = _reclaim_as_other_worker(fake, consumer="worker-B", min_idle_ms=1)
            assert stolen is not None, "worker-B 未能回收消息（场景未成立）"
            queue_b, task_b = stolen
            assert task_b.message_id == task.message_id

            worker_b = _redis_worker(fake)
            with patch("plaita.server.flow_worker.FlowExecution") as FE_B:
                inst_b = MagicMock()
                FE_B.return_value = inst_b
                with pytest.raises(ExecutionLeaseError):
                    worker_b._dispatch_task(
                        task_b.body, delivery_count=task_b.delivery_count
                    )
                # 双跑根除：重派者从未推进任何节点
                inst_b.run_distributed.assert_not_called()

            # 原持有者仍在独占窗口内：租约在、执行行状态健康
            _lease_key(fake, "exec-a")
        finally:
            release.set()

        # worker-A 正常收尾后：执行 completed、消息被 ack（消费组内无 pending）
        deadline = time.time() + 5
        while time.time() < deadline:
            state = storage.load_execution_state("exec-a")
            if state is not None and state.status == "completed":
                break
            time.sleep(0.05)
        state = storage.load_execution_state("exec-a")
        assert state is not None and state.status == "completed"
        pending = fake.xpending(QUEUE, "plaita-workers")
        assert (pending["pending"] if isinstance(pending, dict) else pending[0]) == 0

    def test_reclaimed_start_with_dedup_key_never_restarts(self):
        """带 dedup_key 的重派：先撞租约拒绝（不二次 start），即撞 running
        分支也是重入队 resume 而非新执行——两条路都不产生平行执行。"""
        fake, storage, barrier, release, queue_a, task = (
            self._long_running_start_scene(dedup_key="sched:s9:1770000000")
        )
        try:
            time.sleep(0.02)
            stolen = _reclaim_as_other_worker(fake, consumer="worker-B", min_idle_ms=1)
            queue_b, task_b = stolen

            worker_b = _redis_worker(fake)
            with patch("plaita.server.flow_worker.FlowExecution") as FE_B:
                inst_b = MagicMock()
                FE_B.return_value = inst_b
                inst_b.execution_id = "exec-b"
                # 重派者先撞租约（dedup 未命中：原执行尚未认领——认领发生在
                # start_flow 租约窗口内）。ExecutionLeaseError 直接退出。
                with pytest.raises(ExecutionLeaseError):
                    worker_b._dispatch_task(
                        task_b.body, delivery_count=task_b.delivery_count
                    )
                inst_b.run_distributed.assert_not_called()
            # 没有任何新执行行被落下
            assert storage.load_execution_state("exec-b") is None
        finally:
            release.set()


class TestTakeoverAfterHolderDeath:
    """验收臂 2：持有者死后租约过期，重派消息可接管。"""

    def test_reclaim_succeeds_after_lease_expiry(self):
        """持有者消亡（模拟：租约 TTL 30ms 过期 + 释放路径不再续期）→
        worker-B 回收同一条 start 消息并成功取得租约推进。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        # 短 TTL 模拟持有者死亡后租约过期窗口
        worker_a = _redis_worker(fake, storage, lease_ttl_seconds=30)

        queue_a = RedisStreamTaskQueue(
            fake, QUEUE, group_name="plaita-workers", consumer_name="worker-A",
            claim_min_idle_ms=1,
        )
        queue_a.ensure_group()
        queue_a.enqueue({"type": "start", "flow_id": "f1", "version": "1",
                         "params": {}, "tenant_id": "default"})
        task = queue_a.read(block_ms=100)
        assert task is not None

        # 模拟「持有者跑首节点中途死亡」：先抢到租约再消失（不 release）
        holder = "start:holder-gone"
        assert worker_a.execution_lease.try_acquire("exec-z", holder, 30) is True
        assert fake.exists("plaita:execution:lease:exec-z")
        # 等租约 TTL 过期 = 持有者死后租约尾巴烧完
        time.sleep(0.05)

        worker_b = _redis_worker(fake, storage)
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.execution_id = "exec-z"
            inst.run_distributed.return_value = {
                "execution_id": "exec-z", "is_end": True, "context": {},
            }
            worker_b._dispatch_task(task.body, delivery_count=task.delivery_count)

        state = storage.load_execution_state("exec-z")
        assert state is not None and state.status == "completed"

    def test_requeue_resume_after_holder_death_takes_over_from_checkpoint(self):
        """dedup 命中 running 分支的重入队 resume：持有者活着 → 被租约拦下
        ack 释放；持有者死（租约过期）→ resume 取得租约从 checkpoint 接管。
        这补上 A′ 时代「重入队 resume 与原持有者并发推进」的缺口。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        worker = _redis_worker(fake, storage)
        from plaita.storage.base import ExecutionState

        storage.save_execution_state("exec-r", ExecutionState(
            execution_id="exec-r", flow_id="f1", status="running",
            context={"$LAST_NODE": "start", "$NODE": {}},
        ))
        fake.set("plaita:start-dedup:sched:s1:1770000000", "exec-r")

        # 持有者活着：重入队 resume 只回报现状（run_distributed 未被调用）
        assert worker.execution_lease.try_acquire("exec-r", "live-holder", 120)
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.execution_id = "exec-other"
            result = worker.start_flow(
                "f1", {}, version="1",
                dedup_key="sched:s1:1770000000", execution_id="exec-other",
            )
        assert result["deduplicated"] is True
        assert result["resume_requeued"] is True
        inst.run_distributed.assert_not_called()
        assert storage.load_execution_state("exec-other") is None

        entries = fake.xrange(QUEUE)
        assert len(entries) == 1
        payload = entries[0][1]["payload"]
        msg = json.loads(payload if isinstance(payload, str) else payload.decode())
        assert msg["type"] == "resume" and msg["execution_id"] == "exec-r"

        # 持有者死：租约过期后重入队的 resume 接管（不再被拦）。
        # 显式释放持有者租约（等价 TTL 过期 + 加速用例）。
        worker.execution_lease.release("exec-r", "live-holder")
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.execution_id = "exec-r"
            inst.run_distributed.return_value = {
                "execution_id": "exec-r", "is_end": True, "context": {},
            }
            worker._dispatch_task(msg, delivery_count=1)
        assert storage.load_execution_state("exec-r").status == "completed"


class TestScheduleFireCarriesDedupKey:
    """schedule_service 派发的 start 消息带确定性幂等键（#23 建议）。"""

    def _schedule(self, **overrides):
        s = {
            "schedule_id": "s9", "name": "n", "flow_id": "f1",
            "cron": "* * * * *", "params": {}, "tenant_id": "acme",
            "version": "1",
        }
        s.update(overrides)
        return s

    def test_message_has_deterministic_dedup_key(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        msg_id = fire_schedule(fake, self._schedule(), QUEUE, trigger_kind="cron")
        assert msg_id is not None
        entries = fake.xrange(QUEUE)
        payload = entries[0][1]["payload"]
        msg = json.loads(payload if isinstance(payload, str) else payload.decode())
        assert msg["dedup_key"]
        assert msg["dedup_key"].startswith("sched:s9:")
        # 键值 = schedule_id + 触发时点（秒级 epoch）：确定性、跨周期不同
        suffix = msg["dedup_key"].rsplit(":", 1)[-1]
        assert suffix.isdigit()

    def test_dedup_key_follows_fired_at_not_wall_clock_drift(self):
        """两次触发时间不同 → 键不同（下一周期不被误吞）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        fire_schedule(fake, self._schedule(), QUEUE, trigger_kind="cron")
        time.sleep(1.1)
        fire_schedule(fake, self._schedule(), QUEUE, trigger_kind="cron")
        entries = fake.xrange(QUEUE)
        keys = {
            json.loads(
                f["payload"] if isinstance(f["payload"], str) else f["payload"].decode()
            )["dedup_key"]
            for _, f in entries
        }
        assert len(keys) == 2

    def test_manual_trigger_also_keyed(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        fire_schedule(fake, self._schedule(), QUEUE, trigger_kind="manual")
        entries = fake.xrange(QUEUE)
        payload = entries[0][1]["payload"]
        msg = json.loads(payload if isinstance(payload, str) else payload.decode())
        assert msg["dedup_key"].startswith("sched:s9:")

    def test_keyed_message_converges_on_requeue(self):
        """同键重投：第二次 start 命中 dedup，绝不二次 start（端到端）。

        首条消息带预铸 execution_id（BFF/keeper 派发形态），重投同键收敛到
        同一执行。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        worker = _redis_worker(fake, storage)
        fire_schedule(fake, self._schedule(), QUEUE, trigger_kind="cron")
        entries = fake.xrange(QUEUE)
        payload = entries[0][1]["payload"]
        msg = json.loads(payload if isinstance(payload, str) else payload.decode())
        msg["execution_id"] = "exec-first"

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.execution_id = "exec-first"
            inst.run_distributed.return_value = {
                "execution_id": "exec-first", "is_end": True, "context": {},
            }
            worker._dispatch_task(msg, delivery_count=1)

        # 消息重派/重投（同键）→ dedup 命中已完成执行，不重启。
        # 调度消息归属租户 acme：重派处理同样在消息租户上下文里
        # （run() 的 _dispatch_task 会 set_current_tenant）。
        from plaita.server.tenant_context import reset_current_tenant, set_current_tenant

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.execution_id = "exec-second"
            token = set_current_tenant(msg["tenant_id"])
            try:
                result = worker.start_flow(
                    msg["flow_id"], msg["params"], msg.get("version"),
                    dedup_key=msg["dedup_key"], execution_id="exec-second",
                )
            finally:
                reset_current_tenant(token)
        assert result["already_terminal"] is True
        assert result["execution_id"] == "exec-first"
        assert storage.load_execution_state("exec-second") is None


class TestBaseWorkerZeroChange:
    """无 redis 客户端（内存 worker / 单测基类）行为零变化。"""

    def test_memory_worker_start_still_works(self):
        worker = FlowWorker(
            execution_storage=MemoryExecutionStorage(),
            flow_storage=_flow_storage(),
        )
        result = worker.start_flow("f1", {}, version="1")
        assert result.get("is_end") is True
