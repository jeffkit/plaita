"""plaita#252：run() 兜底死信绕过 dead_letter_guard。

## 缺陷

``_consume_loop`` 的兜底异常分支在 ``delivery_count >= max_deliveries`` 时
直接 ``queue.dead_letter(...)``，不经过 ``_read_once`` / ``_reclaim_one``
都遵守的 ``dead_letter_guard``。该分支正是 ``ExecutionStateLoadError`` /
``StatePersistError`` 等**瞬态**失败的落点：守卫判 False（running + 活
租约）或状态读取瞬断时，原消息仍被 ack 出 PEL、DLQ 增加一条，执行失去
唯一恢复载体。

## 修复契约

1. 兜底死信走队列层 ``dead_letter_if_allowed``（与另两条路径同一守卫）；
2. 守卫拒绝/抛异常 → 不 ack、消息留 pending、``dlq_guard_skipped`` 递增；
3. 守卫 None 恒放行——存量行为零变化；
4. 守卫放行时维持既有语义（running + 预算耗尽先终态化 error；非终态 +
   租约空先重入队一份 recovery 副本）。

## 捆绑单（A1 r37 a/P2，同域 task_queue.py）

``_read_once`` 对 ``xreadgroup(">")`` 交付的**首投**消息（delivery_count=1）
也套用 ``>= max_deliveries``，``max_deliveries=1`` 时合法任务在零处理尝试前
即进 DLQ。本文件同时钉死正确语义：``max_deliveries`` 计的是**处理尝试
次数**，首投必须先获得这一次尝试；失败/未 ack 之后才由 worker 兜底与
reclaim 路径死信。

## 反证

- 还原 worker 兜底为直接 ``queue.dead_letter(...)``：``TestFallbackGuard``
  的活租约/瞬断两条精确红（DLQ 1 ≠ 0、pending 0 ≠ 1）；
- 还原 ``_read_once`` 的首投死信判定：``TestMaxDeliveriesOne`` 两条精确红
  （首投返回 None、DLQ 1 ≠ 0）。
"""
import threading
import time

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.server.flow_worker import RedisFlowWorker
from plaita.server.task_queue import RedisStreamTaskQueue, StreamTask
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

EXEC = "exec-252"
QUEUE = "test:issue252-dlq-guard-fallback"
BODY = {
    "type": "resume",
    "execution_id": EXEC,
    "tenant_id": "default",
    "resume_type": "event",
}


def _state(status="running", error=None, end_time=None) -> ExecutionState:
    return ExecutionState(
        execution_id=EXEC,
        flow_id="f1",
        flow_version="1",
        status=status,
        context={"$LAST_NODE": "start", "$NODE": {"start": {}}},
        error=error,
        end_time=end_time,
    )


def _worker(fake, storage=None, max_deliveries=5) -> RedisFlowWorker:
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name=QUEUE,
        execution_storage=storage if storage is not None else MemoryExecutionStorage(),
        flow_storage=MemoryFlowStorage(),
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
        max_deliveries=max_deliveries,
        claim_min_idle_ms=1,
        read_block_ms=100,
    )


def _pending_ids(fake, group="g"):
    summary = fake.xpending(QUEUE, group)
    count = int(summary["pending"] if isinstance(summary, dict) else summary[0])
    if not count:
        return []
    entries = fake.xpending_range(QUEUE, group, min="-", max="+", count=count)
    return [
        entry.get("message_id") if isinstance(entry, dict) else entry[0]
        for entry in entries
    ]


def _seed_pending(fake, *, deliveries, max_deliveries=5):
    """造一条真实 pending 消息，并把 ``times_delivered`` 刷到 ``deliveries``。"""
    seed = RedisStreamTaskQueue(
        fake,
        QUEUE,
        group_name="g",
        consumer_name="seed",
        claim_min_idle_ms=1,
        max_deliveries=max_deliveries,
    )
    mid = seed.enqueue(dict(BODY), max_deliveries=None)  # legacy 载体：预算由队列本地值裁判
    first = seed.read(block_ms=100)
    assert first is not None and first.message_id == mid, "首投必须可被派发"
    for _ in range(deliveries - 1):
        time.sleep(0.005)
        claimed = fake.xclaim(QUEUE, "g", "bump", min_idle_time=1, message_ids=[mid])
        assert claimed, "XCLAIM 刷投递次数失败"
    assert _pending_ids(fake) == [mid]
    return mid


class _InjectedQueue(RedisStreamTaskQueue):
    """把预置任务直接交给消费循环（模拟 reclaim 后 delivery_count 已触顶）。

    第二次 ``read`` 触发停机回调，避免依赖 block 超时；其余方法全部走真实
    队列实现（含守卫、DLQ、ack）。
    """

    def __init__(self, *args, injected, on_exhausted=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._injected = list(injected)
        self._on_exhausted = on_exhausted
        self.acked_ids = []

    def read(self, block_ms=0):
        if self._injected:
            return self._injected.pop(0)
        if self._on_exhausted is not None:
            callback, self._on_exhausted = self._on_exhausted, None
            callback()
        return None

    def ack(self, message_id):
        self.acked_ids.append(message_id)
        super().ack(message_id)


def _run_until_stopped(worker, queue, monkeypatch, exc, timeout=10.0):
    monkeypatch.setattr(worker, "_dispatch_task", _raise(exc))
    worker._task_queue = queue
    thread = threading.Thread(target=worker.run, daemon=True)
    thread.start()
    thread.join(timeout=timeout)
    assert not thread.is_alive(), "run() 未在队列停机回调后退出"


def _raise(exc):
    def _boom(*args, **kwargs):
        raise exc

    return _boom


def _injected_task(mid):
    return StreamTask(message_id=mid, body=dict(BODY), delivery_count=5)


class TestFallbackGuard:
    def test_running_with_live_lease_skips_fallback_dead_letter(self, monkeypatch):
        """守卫拒绝（running + 活租约）：兜底分支不得死信/ack，消息留 PEL。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        worker = _worker(fake, storage)
        storage.save_execution_state(EXEC, _state("running"))
        fake.set(f"plaita:execution:lease:{EXEC}", "worker-a:1", ex=120)
        mid = _seed_pending(fake, deliveries=5)

        guard_calls = []

        def guard(task):
            guard_calls.append(task.message_id)
            return worker._dead_letter_guard(task)

        queue = _InjectedQueue(
            fake,
            QUEUE,
            group_name="g",
            consumer_name="worker",
            claim_min_idle_ms=1,
            max_deliveries=5,
            dead_letter_guard=guard,
            injected=[_injected_task(mid)],
            on_exhausted=worker.stop,
        )
        _run_until_stopped(worker, queue, monkeypatch, RuntimeError("dispatch boom"))

        assert fake.xlen(queue.dlq_key) == 0, "守卫拒绝不得死信"
        assert guard_calls == [mid], "兜底死信前必须调用守卫"
        assert _pending_ids(fake) == [mid], "原消息必须留在 PEL"
        assert queue.acked_ids == [], "守卫拒绝不得 ack"
        assert queue.stats()["dead_lettered"] == 0
        assert queue.stats()["dlq_guard_skipped"] == 1
        saved = storage.load_execution_state(EXEC)
        assert saved.status == "running", "守卫拒绝不得改执行状态"
        assert saved.error is None and saved.end_time is None

    def test_transient_state_load_failure_skips_fallback_dead_letter(self, monkeypatch):
        """状态读取瞬断：守卫抛异常按拒绝处理，消息留 pending 等下一轮。"""
        fake = fakeredis.FakeRedis(decode_responses=True)

        class BlipStorage(MemoryExecutionStorage):
            def __init__(self):
                super().__init__()
                self.load_calls = 0
                self.save_calls = 0

            def load_execution_state(self, execution_id):
                self.load_calls += 1
                raise RuntimeError("redis down")

            def save_execution_state(self, execution_id, state):
                self.save_calls += 1
                return super().save_execution_state(execution_id, state)

        storage = BlipStorage()
        worker = _worker(fake, storage)
        mid = _seed_pending(fake, deliveries=5)

        guard_calls = []

        def guard(task):
            guard_calls.append(task.message_id)
            return worker._dead_letter_guard(task)

        queue = _InjectedQueue(
            fake,
            QUEUE,
            group_name="g",
            consumer_name="worker",
            claim_min_idle_ms=1,
            max_deliveries=5,
            dead_letter_guard=guard,
            injected=[_injected_task(mid)],
            on_exhausted=worker.stop,
        )
        _run_until_stopped(worker, queue, monkeypatch, RuntimeError("transient"))

        assert fake.xlen(queue.dlq_key) == 0, "瞬断不得死信"
        assert guard_calls == [mid], "瞬断场景守卫至少被调用一次"
        assert storage.load_calls == 1
        assert storage.save_calls == 0, "拒死信路径不得改写执行状态"
        assert _pending_ids(fake) == [mid], "瞬断消息必须留在 PEL"
        assert queue.stats()["dead_lettered"] == 0
        assert queue.stats()["dlq_guard_skipped"] == 1

    @pytest.mark.parametrize(
        "max_deliveries,delivery_count",
        [(5, 5), (1, 1)],
        ids=["default-budget", "single-attempt"],
    )
    def test_guard_none_fallback_dead_letter_unchanged(
        self, monkeypatch, max_deliveries, delivery_count
    ):
        """守卫 None 恒放行：兜底分支的存量死信行为零变化。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _worker(fake, max_deliveries=max_deliveries)
        mid = _seed_pending(
            fake, deliveries=delivery_count, max_deliveries=max_deliveries
        )
        queue = _InjectedQueue(
            fake,
            QUEUE,
            group_name="g",
            consumer_name="worker",
            claim_min_idle_ms=1,
            max_deliveries=max_deliveries,
            dead_letter_guard=None,
            injected=[
                StreamTask(
                    message_id=mid,
                    body=dict(BODY),
                    delivery_count=delivery_count,
                )
            ],
            on_exhausted=worker.stop,
        )
        _run_until_stopped(worker, queue, monkeypatch, RuntimeError("dispatch boom"))

        assert fake.xlen(queue.dlq_key) == 1, "无守卫时照旧死信"
        assert _pending_ids(fake) == [], "死信后原消息应被 ack 出 PEL"
        assert queue.stats()["dead_lettered"] == 1
        assert queue.stats()["dlq_guard_skipped"] == 0

    def test_guard_allows_running_budget_exhausted_terminalizes_then_dead_letters(
        self, monkeypatch
    ):
        """守卫放行（running + 预算耗尽）：先终态化 error，再死信恰 1 条。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        worker = _worker(fake, storage)
        storage.save_execution_state(EXEC, _state("running"))
        fake.set(
            f"plaita:execution:noderetry:{EXEC}:a",
            str(worker._node_retry_budget()),
        )
        mid = _seed_pending(fake, deliveries=5)
        # 按 #242 的 tenant 路由键确认：守卫读的是 default namespace
        queue = _InjectedQueue(
            fake,
            QUEUE,
            group_name="g",
            consumer_name="worker",
            claim_min_idle_ms=1,
            max_deliveries=5,
            dead_letter_guard=worker._dead_letter_guard,
            injected=[_injected_task(mid)],
            on_exhausted=worker.stop,
        )
        _run_until_stopped(worker, queue, monkeypatch, RuntimeError("dispatch boom"))

        saved = storage.load_execution_state(EXEC)
        assert saved.status == "error"
        assert saved.end_time is not None
        assert saved.error.get("error_type") == "node_retry_budget_exhausted"
        assert fake.xlen(queue.dlq_key) == 1, "守卫放行后 DLQ 恰 1 条"
        assert queue.stats()["dead_lettered"] == 1
        assert _pending_ids(fake) == []

    def test_guard_allows_orphan_reenqueues_before_dead_letter(self, monkeypatch):
        """守卫放行（非终态 + 租约空）：先重入队 recovery 副本，再死信原消息。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        worker = _worker(fake, storage)
        storage.save_execution_state(EXEC, _state("running"))
        mid = _seed_pending(fake, deliveries=5)
        queue = _InjectedQueue(
            fake,
            QUEUE,
            group_name="g",
            consumer_name="worker",
            claim_min_idle_ms=1,
            max_deliveries=5,
            dead_letter_guard=worker._dead_letter_guard,
            injected=[_injected_task(mid)],
            on_exhausted=worker.stop,
        )
        _run_until_stopped(worker, queue, monkeypatch, RuntimeError("dispatch boom"))

        assert fake.xlen(queue.dlq_key) == 1, "原消息进 DLQ"
        assert _pending_ids(fake) == [], "原消息已 ack"
        assert fake.xlen(QUEUE) == 1, "stream 尾部出现同体 recovery 副本"
        assert storage.load_execution_state(EXEC).status == "running", (
            "租约空且预算未耗尽：只重入队，不终态化"
        )


class TestMaxDeliveriesOne:
    """捆绑单：max_deliveries 计的是处理尝试次数，首投必须先获得一次尝试。"""

    def _queue(self, fake, **kwargs):
        return RedisStreamTaskQueue(
            fake,
            QUEUE,
            group_name="g",
            consumer_name="c1",
            claim_min_idle_ms=1,
            **kwargs,
        )

    def test_first_delivery_with_max_deliveries_one_is_not_dead_lettered(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        q = self._queue(fake, max_deliveries=1)
        mid = q.enqueue(dict(BODY))

        task = q.read(block_ms=100)

        assert task is not None, "max_deliveries=1 的首投不得在零处理尝试前死信"
        assert task.message_id == mid
        assert task.delivery_count == 1
        assert fake.xlen(q.dlq_key) == 0
        assert q.stats()["dead_lettered"] == 0
        assert _pending_ids(fake) == [mid], "首投后消息在 PEL，等 worker ack/失败处置"

    def test_max_deliveries_one_dead_letters_after_unacked_delivery(self):
        """首投之后（未 ack = 这次尝试没成功）才允许死信。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        writer = self._queue(fake, max_deliveries=1)
        mid = writer.enqueue(dict(BODY), max_deliveries=None)  # legacy 载体：预算=队列本地 1
        first = writer.read(block_ms=100)
        assert first is not None and first.message_id == mid

        time.sleep(0.01)
        reader = RedisStreamTaskQueue(
            fake,
            QUEUE,
            group_name="g",
            consumer_name="c2",
            claim_min_idle_ms=1,
            max_deliveries=1,
        )
        assert reader.read(block_ms=50) is None, "第二次投递不得发生"
        assert fake.xlen(reader.dlq_key) == 1, "未 ack 的首次投递后才死信"
        assert _pending_ids(fake) == []
        assert reader.stats()["dead_lettered"] == 1

    def test_max_deliveries_one_survives_guard_refusal(self):
        """守卫拒绝时首投之后的 reclaim 也不得死信（消息留 pending）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        writer = self._queue(fake, max_deliveries=1)
        mid = writer.enqueue(dict(BODY), max_deliveries=None)  # legacy 载体：预算=队列本地 1
        assert writer.read(block_ms=100) is not None

        time.sleep(0.01)
        reader = RedisStreamTaskQueue(
            fake,
            QUEUE,
            group_name="g",
            consumer_name="c2",
            claim_min_idle_ms=1,
            max_deliveries=1,
            dead_letter_guard=lambda task: False,
        )
        assert reader.read(block_ms=50) is None
        assert fake.xlen(reader.dlq_key) == 0
        assert _pending_ids(fake) == [mid]
        assert reader.stats()["dead_lettered"] == 0
        assert reader.stats()["dlq_guard_skipped"] == 1
