"""plaita#47：回收前查活跃租约 + 恢复副本重入队冷却（队列 churn / DLQ 污染）。

基线现象（2026-10-10 实测）：``plaita:flow:queue:v2:dlq`` 三条同一 start 任务
的 ``reason=max_deliveries=5``，``source_id`` 互为链条（＝「死信 → 重入队新副本
→ 再烧满 → 再死信」循环，周期 ≈6.4min）；同窗口 XPENDING 全压在单个 consumer
且 XLEN 虚增。两条成因：

1. ``_reclaim_one`` 只按 idle 判空闲就 XCLAIM——**不查该 pending 对应的执行是否
   还有活持有者**：租约活着的执行，其队列消息每 ``claim_min_idle_ms`` 被同伴抢
   一次（delivery_count 虚增、idle 归零刷新、日志刷屏），触顶后即便死信守卫拦下
   也照抢不误。修法：``claim_guard``（XCLAIM 前闸门，False = 不抢、不计数）。
2. 死信守卫的「非终态 + 租约空」分支无条件重入队恢复副本 → 新副本又被回收烧满
   → 再重入队，链条自持。修法：同执行每 ``DLQ_REQUEUE_COOLDOWN_SECONDS`` 只
   重生一次（``_dlq_requeue_allowed``），窗外照常重入队（真孤儿恢复不切断）。
"""
import time

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

import plaita.server.flow_worker as flow_worker_module
from plaita.server.flow_worker import RedisFlowWorker
from plaita.server.task_queue import RedisStreamTaskQueue, StreamTask
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage


STREAM = "plaita:flow:queue:issue47"
GROUP = "plaita-workers"


def _redis_worker(fake_redis, storage=None) -> RedisFlowWorker:
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name=STREAM,
        execution_storage=storage if storage is not None else MemoryExecutionStorage(),
        flow_storage=MemoryFlowStorage(),
        redis_client=fake_redis,
        enable_registry=False,
        enable_redis_logging=False,
    )


def _running_state(worker, execution_id="exec-1") -> None:
    worker.execution_storage.save_execution_state(
        execution_id,
        ExecutionState(
            execution_id=execution_id,
            flow_id="f1",
            flow_version="1",
            status="running",
            context={"$LAST_NODE": "start", "$NODE": {}},
        ),
    )


def _terminal_state(worker, execution_id="exec-1") -> None:
    worker.execution_storage.save_execution_state(
        execution_id,
        ExecutionState(
            execution_id=execution_id,
            flow_id="f1",
            flow_version="1",
            status="error",
            context={"$LAST_NODE": "start", "$NODE": {}},
        ),
    )


def _queue(fake_redis, **kwargs) -> RedisStreamTaskQueue:
    kwargs.setdefault("claim_min_idle_ms", 1)
    kwargs.setdefault("max_deliveries", 3)
    kwargs.setdefault("consumer_name", "reader")
    return RedisStreamTaskQueue(fake_redis, STREAM, group_name=GROUP, **kwargs)


def _enqueue_over_delivered(fake_redis, body, *, deliveries: int, max_deliveries: int) -> str:
    """造一条 ``times_delivered == deliveries`` 且 idle 已超阈的 pending 消息。"""
    writer = _queue(fake_redis, consumer_name="c1", max_deliveries=max_deliveries)
    mid = writer.enqueue(body)
    first = writer.read(block_ms=100)
    assert first is not None and first.message_id == mid
    for _ in range(deliveries - 1):
        time.sleep(0.005)
        claimed = fake_redis.xclaim(STREAM, GROUP, "c2", min_idle_time=1, message_ids=[mid])
        assert claimed
    time.sleep(0.01)  # 留出 > claim_min_idle_ms 的 idle 窗口
    return mid


def _times_delivered(fake_redis, mid: str) -> int:
    pending = fake_redis.xpending_range(STREAM, GROUP, min=mid, max=mid, count=1)
    assert pending, "消息应仍在 pending"
    entry = pending[0]
    return int(entry["times_delivered"] if isinstance(entry, dict) else entry[3])


class TestClaimGuardInQueue:
    """claim_guard：XCLAIM 前闸门（False = 不抢、不递增投递数、不刷新 idle）。"""

    def test_guard_false_freezes_delivery_count_and_dlq(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        mid = _enqueue_over_delivered(
            fake, {"type": "start", "execution_id": "e1"}, deliveries=3, max_deliveries=3
        )
        seen = []

        def refuse(task):
            seen.append((task.message_id, task.body["execution_id"]))
            return False

        q = _queue(fake, claim_guard=refuse, max_deliveries=3)
        assert q.read(block_ms=20) is None
        assert seen == [(mid, "e1")], "闸门应拿到消息体（XRANGE 窥视）"
        assert _times_delivered(fake, mid) == 3, "被闸门拒绝的条目不得烧 delivery"
        assert fake.xlen(q.dlq_key) == 0, "被闸门拒绝 ≠ 死信"
        assert q.stats()["claim_guard_skipped"] == 1
        assert q.stats()["reclaimed"] == 0

    def test_guard_refusal_keeps_idle_growing(self):
        """被拒条目不得被 XCLAIM ⇒ idle 只增不减（XPENDING 读数稳定）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        mid = _enqueue_over_delivered(
            fake, {"type": "resume", "execution_id": "e1"}, deliveries=1, max_deliveries=3
        )

        q = _queue(fake, claim_guard=lambda task: False)
        assert q.read(block_ms=20) is None
        first_idle = fake.xpending_range(STREAM, GROUP, min=mid, max=mid, count=1)[0][
            "time_since_delivered"
        ]
        time.sleep(0.02)
        assert q.read(block_ms=20) is None
        second_idle = fake.xpending_range(STREAM, GROUP, min=mid, max=mid, count=1)[0][
            "time_since_delivered"
        ]
        assert second_idle > first_idle, "idle 应继续累积（未被 XCLAIM 归零）"
        assert _times_delivered(fake, mid) == 1

    def test_guard_true_reclaims_normally(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        mid = _enqueue_over_delivered(
            fake, {"type": "resume", "execution_id": "e1"}, deliveries=2, max_deliveries=3
        )
        q = _queue(fake, claim_guard=lambda task: True, max_deliveries=3)
        task = q.read(block_ms=50)
        assert task is not None and task.message_id == mid
        assert task.delivery_count == 2, "delivery_count 语义不变（XCLAIM 前的投递数）"
        assert q.stats()["reclaimed"] == 1
        assert q.stats()["claim_guard_skipped"] == 0

    def test_guard_raising_skips_claim(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        mid = _enqueue_over_delivered(
            fake, {"type": "resume", "execution_id": "e1"}, deliveries=2, max_deliveries=3
        )

        def boom(task):
            raise RuntimeError("lease probe failed")

        q = _queue(fake, claim_guard=boom, max_deliveries=3)
        assert q.read(block_ms=20) is None
        assert _times_delivered(fake, mid) == 2
        assert q.stats()["claim_guard_skipped"] == 1
        assert fake.xlen(q.dlq_key) == 0

    def test_guard_not_invoked_for_fresh_pending(self):
        """idle 未超阈的 pending 不该惊动闸门（也不该被抢）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        writer = _queue(fake, consumer_name="c1", claim_min_idle_ms=60_000)
        mid = writer.enqueue({"type": "resume", "execution_id": "e1"})
        assert writer.read(block_ms=100) is not None
        calls = []

        q = _queue(
            fake,
            claim_min_idle_ms=60_000,
            claim_guard=lambda t: calls.append(t) or False,
        )
        assert q.read(block_ms=20) is None
        assert calls == [], "idle 未超阈不该调闸门"
        assert _times_delivered(fake, mid) == 1

    def test_peek_failure_falls_back_to_existing_reclaim(self):
        """窥视失败（XRANGE 瞬断）不得改变存量处置：照常 XCLAIM。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        mid = _enqueue_over_delivered(
            fake, {"type": "resume", "execution_id": "e1"}, deliveries=2, max_deliveries=3
        )
        q = _queue(fake, claim_guard=lambda task: False, max_deliveries=3)

        def blip(*args, **kwargs):
            raise ConnectionError("connection reset")

        fake.xrange = blip
        task = q.read(block_ms=50)
        assert task is not None and task.message_id == mid, "窥视失败 → 存量回收路径"

    def test_refused_entry_still_dead_lettered_after_guard_allows(self):
        """被闸门拒过的超限条目不是僵尸：闸门放行后照旧走死信判定。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        mid = _enqueue_over_delivered(
            fake, {"type": "resume", "execution_id": "e1"}, deliveries=3, max_deliveries=3
        )
        allow = {"v": False}
        q = _queue(fake, claim_guard=lambda task: allow["v"], max_deliveries=3)
        assert q.read(block_ms=20) is None
        assert fake.xlen(q.dlq_key) == 0
        allow["v"] = True
        assert q.read(block_ms=20) is None
        assert fake.xlen(q.dlq_key) == 1, "闸门放行后超限条目照常死信"
        assert fake.xpending_range(STREAM, GROUP, min=mid, max=mid, count=1) == []


class TestClaimGuardJudgement:
    """FlowWorker._claim_guard 判据：只有「租约存在」才拒绝回收。"""

    def _task(self, body, message_id="msg-1"):
        return StreamTask(message_id=message_id, body=body, delivery_count=2)

    def test_skips_when_lease_held(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _redis_worker(fake)
        fake.set("plaita:execution:lease:exec-1", "start:abc:3", ex=60)
        assert (
            worker._claim_guard(
                self._task({"type": "start", "execution_id": "exec-1", "tenant_id": "default"})
            )
            is False
        )

    def test_allows_when_no_lease(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _redis_worker(fake)
        _running_state(worker)  # 非终态但租约空（孤儿）→ 必须可回收重投
        assert (
            worker._claim_guard(
                self._task({"type": "resume", "execution_id": "exec-1", "tenant_id": "default"})
            )
            is True
        )

    def test_allows_when_state_missing_or_terminal(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _redis_worker(fake)
        assert worker._claim_guard(self._task({"type": "start", "execution_id": "e-x"})) is True
        worker.execution_storage.save_execution_state(
            "exec-t",
            ExecutionState(
                execution_id="exec-t",
                flow_id="f1",
                flow_version="1",
                status="completed",
                context={},
            ),
        )
        assert (
            worker._claim_guard(self._task({"type": "resume", "execution_id": "exec-t"})) is True
        ), "终态执行的挂尾消息要被回收后经 already_terminal 短路 ack"

    def test_allows_without_execution_id_or_dict_body(self):
        worker = _redis_worker(fakeredis.FakeRedis(decode_responses=True))
        assert worker._claim_guard(self._task({"type": "start", "flow_id": "f1"})) is True
        assert worker._claim_guard(StreamTask(message_id="m", body=None)) is True

    def test_lease_key_routed_by_tenant(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _redis_worker(fake)
        fake.set("plaita:acme:execution:lease:exec-9", "resume:abc:3", ex=60)
        assert (
            worker._claim_guard(
                self._task({"type": "resume", "execution_id": "exec-9", "tenant_id": "acme"})
            )
            is False
        )
        # default namespace 无租约 → 可回收（同一消息体换租户 routing）
        assert (
            worker._claim_guard(
                self._task({"type": "resume", "execution_id": "exec-9", "tenant_id": "default"})
            )
            is True
        )

    def test_skips_conservatively_when_lease_probe_fails(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _redis_worker(fake)

        def blip(key):
            raise ConnectionError("connection reset")

        fake.exists = blip
        assert (
            worker._claim_guard(self._task({"type": "resume", "execution_id": "exec-1"})) is False
        )


class TestDlqRequeueCooldown:
    """恢复副本重入队冷却（#47）：同一执行每冷却窗只重生一次。"""

    def _task(self, message_id="msg-1", execution_id="exec-1"):
        return StreamTask(
            message_id=message_id,
            body={"type": "start", "execution_id": execution_id, "tenant_id": "default"},
            delivery_count=5,
        )

    def test_first_hit_requeues_and_arms_cooldown(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _redis_worker(fake)
        _running_state(worker)
        assert worker._dead_letter_guard(self._task()) is True
        assert fake.xlen(STREAM) == 1, "首次命中重入队一份恢复副本"
        marker = "plaita:execution:dlq_requeue:exec-1"
        assert fake.exists(marker) == 1
        ttl = fake.ttl(marker)
        assert 0 < ttl <= worker.DLQ_REQUEUE_COOLDOWN_SECONDS

    def test_second_hit_within_cooldown_skips_requeue(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _redis_worker(fake)
        _running_state(worker)
        assert worker._dead_letter_guard(self._task(message_id="msg-1")) is True
        assert worker._dead_letter_guard(self._task(message_id="msg-2")) is False
        assert fake.xlen(STREAM) == 1, "冷却窗内不得再重入队副本（链条止于此）"

    def test_requeues_again_after_cooldown_expires(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _redis_worker(fake)
        _running_state(worker)
        assert worker._dead_letter_guard(self._task(message_id="msg-1")) is True
        fake.delete("plaita:execution:dlq_requeue:exec-1")  # 模拟冷却窗过期（TTL 到）
        assert worker._dead_letter_guard(self._task(message_id="msg-2")) is True
        assert fake.xlen(STREAM) == 2, "窗外照常重入队：真孤儿恢复路径不被切断"

    def test_terminal_state_still_allowed_despite_marker(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _redis_worker(fake)
        fake.set("plaita:execution:dlq_requeue:exec-1", "1", ex=600)
        worker.execution_storage.save_execution_state(
            "exec-1",
            ExecutionState(
                execution_id="exec-1",
                flow_id="f1",
                flow_version="1",
                status="completed",
                context={},
            ),
        )
        assert worker._dead_letter_guard(self._task()) is True, "终态放行不受冷却闸影响"

    def test_marker_write_failure_skips_dead_letter(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _redis_worker(fake)
        _running_state(worker)

        def blip(*args, **kwargs):
            raise ConnectionError("connection reset")

        fake.set = blip
        assert worker._dead_letter_guard(self._task()) is False
        assert fake.xlen(STREAM) == 0, "抢不到冷却闸不得重入队（保守留 pending）"

    def test_chain_is_bounded_end_to_end(self):
        """端到端：同一条 start 任务反复烧满 → 只产生一份恢复副本、一条死信。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _redis_worker(fake)
        _running_state(worker)
        q = _queue(
            fake, claim_guard=worker._claim_guard, dead_letter_guard=worker._dead_letter_guard
        )
        worker._max_deliveries = 3  # 与队列同一旋钮（生产中同源配置）

        mid = _enqueue_over_delivered(
            fake,
            {"type": "start", "execution_id": "exec-1", "tenant_id": "default"},
            deliveries=3,
            max_deliveries=3,
        )
        # 第一轮：超限原消息 → 重入队恢复副本 + 死信；同一次 read 随后把恢复
        # 副本正常投递给本 worker（XREADGROUP）。
        copy_task = q.read(block_ms=20)
        assert copy_task is not None and copy_task.message_id != mid
        assert fake.xlen(q.dlq_key) == 1
        # 死信会 ack 原消息（并 XDEL 原条目）→ stream 里只剩重入队的恢复副本
        assert fake.xlen(STREAM) == 1, "只该有一份恢复副本"
        copy_id = copy_task.message_id

        # 恢复副本同样被回收烧满（第二代）：链条必须止于此
        for _ in range(2):
            time.sleep(0.005)
            fake.xclaim(STREAM, GROUP, "c3", min_idle_time=1, message_ids=[copy_id])
        assert _times_delivered(fake, copy_id) == 3
        time.sleep(0.01)
        assert q.read(block_ms=20) is None
        assert fake.xlen(q.dlq_key) == 1, "冷却窗内不得再死信（链条被斩断）"
        assert _times_delivered(fake, copy_id) == 3, "被拒条目连 XCLAIM 都不做（计数冻结）"

        # 执行终态化后（reaper/人工）守卫放行 → 超限条目清零出队
        _terminal_state(worker)
        assert q.read(block_ms=20) is None
        assert fake.xlen(q.dlq_key) == 2, "终态执行的挂尾消息照常死信清出"
        assert fake.xpending_range(STREAM, GROUP, min=copy_id, max=copy_id, count=1) == []


class TestClaimGuardWiring:
    def test_get_task_queue_passes_claim_guard_when_supported(self, monkeypatch):
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _redis_worker(fake)
        captured = {}

        class GuardAcceptingQueue:
            def __init__(
                self,
                redis_client,
                stream_key,
                *,
                group_name=None,
                consumer_name=None,
                claim_min_idle_ms=None,
                max_deliveries=None,
                dlq_key=None,
                claim_guard=None,
                dead_letter_guard=None,
            ):
                captured.update(
                    {"claim_guard": claim_guard, "dead_letter_guard": dead_letter_guard}
                )
                self.consumer_name = consumer_name or "c"

        monkeypatch.setattr(flow_worker_module, "RedisStreamTaskQueue", GuardAcceptingQueue)
        worker._get_task_queue()
        assert captured.get("claim_guard") == worker._claim_guard
        assert callable(captured.get("claim_guard"))
        assert captured.get("dead_letter_guard") == worker._dead_letter_guard

    def test_get_task_queue_compatible_with_queue_class_without_claim_guard(self, monkeypatch):
        """旧队列类（无 claim_guard 具名参数）不得让 _get_task_queue 炸掉。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _redis_worker(fake)

        class LegacyQueue:
            def __init__(
                self,
                redis_client,
                stream_key,
                *,
                group_name=None,
                consumer_name=None,
                claim_min_idle_ms=None,
                max_deliveries=None,
                dlq_key=None,
            ):
                self.consumer_name = consumer_name or "c"

        monkeypatch.setattr(flow_worker_module, "RedisStreamTaskQueue", LegacyQueue)
        assert worker._get_task_queue() is not None


class TestClaimGuardMetrics:
    """回收闸门的可观测出口：被拦次数必须能在 /metrics 上看见。"""

    def test_prometheus_exposes_claim_guard_skipped(self):
        from plaita.server.metrics import render_queue_metrics

        text = render_queue_metrics({"stream_key": "q", "claim_guard_skipped": 3})
        assert "plaita_queue_claim_guard_skipped_total" in text
