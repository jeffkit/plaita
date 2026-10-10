"""#47：reclaim 抢单前查执行租约——排队/在跑的长任务不被「死信→重入队」循环冲洗。

现象（实测，2026-10-10）：``plaita:flow:queue:v2:dlq`` 三条
``reason=max_deliveries=5``，payload 全是同一条 ``start`` 任务，``source_id``
互为链条（=「死信 → 重入队新副本 → 再烧 5 次投递 → 再死信」循环，周期
≈6.4min）；同期 ``XPENDING`` 全部压在同一个 consumer 上、执行租约其实**健在**。

根因：``RedisStreamTaskQueue._reclaim_one`` 只看 idle 判「该回收了」，不看该
消息对应的执行是否仍被活 worker 持有；而 XCLAIM 每次让 ``times_delivered``
+1——「执行正在跑长节点、消息却被同伴每 60s 抢一次」把投递预算烧在纯采样上，
5 轮触顶即队列层死信 + 重入队。

修复契约（本文件锁定）：
- ``claim_guard`` 在 XCLAIM **之前**判：拒绝 → 本条原样跳过（不抢、不计数、
  不刷新 idle），消息留 pending；
- FlowWorker 接线 ``_claim_guard``：租约键在（活 worker 推进中）→ 拒绝；
  租约空 → 放行（孤儿/排队的正确动作都是抢回来重派，不因猜「还在排队」扣住
  消息而切断恢复路径）；
- 验收臂：长任务消费期间，同伴的每一轮 reclaim 都不动该条（``times_delivered``
  恒为 1、owner 不变、DLQ 零新增）；持有者消失（租约释放/过期）后同一轮
  reclaim 照旧接管。
"""
import json
import time

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.server.flow_worker import RedisFlowWorker
from plaita.server.task_queue import RedisStreamTaskQueue
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

QUEUE = "test:issue47-reclaim-lease"
GROUP = "plaita-workers"


def _queue(fake, consumer, **kwargs):
    kwargs.setdefault("group_name", GROUP)
    kwargs.setdefault("claim_min_idle_ms", 1)
    return RedisStreamTaskQueue(fake, QUEUE, consumer_name=consumer, **kwargs)


def _redis_worker(fake, **kwargs) -> RedisFlowWorker:
    kwargs.setdefault("enable_registry", False)
    kwargs.setdefault("enable_redis_logging", False)
    kwargs.setdefault("claim_min_idle_ms", 1)
    kwargs.setdefault("lease_ttl_seconds", 120)
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name=QUEUE,
        execution_storage=MemoryExecutionStorage(),
        flow_storage=MemoryFlowStorage(),
        redis_client=fake,
        **kwargs,
    )


def _pending_entry(fake, message_id):
    entries = fake.xpending_range(QUEUE, GROUP, min=message_id, max=message_id, count=1)
    assert entries, f"消息 {message_id} 不在 PEL"
    return entries[0]


def _seed_pending(fake, body=None, consumer="worker-A"):
    """造一条 idle 超阈的 pending 消息（worker-A 持有、未 ack）。"""
    writer = _queue(fake, consumer)
    writer.ensure_group()
    body = body if body is not None else {
        "type": "start", "flow_id": "f1", "version": "1",
        "execution_id": "exec-47", "tenant_id": "default",
    }
    mid = writer.enqueue(body)
    task = writer.read(block_ms=100)
    assert task is not None and task.message_id == mid
    time.sleep(0.01)  # 越过 claim_min_idle_ms=1
    return mid, task.body


# ---------- 队列层：守卫在 XCLAIM 之前生效 ----------


class TestClaimGuardPrecedesXClaim:
    def test_refusal_keeps_owner_and_delivery_count(self):
        """守卫拒绝 → 不 XCLAIM：owner 不变、times_delivered 不增、无 DLQ。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        mid, _body = _seed_pending(fake)
        q = _queue(fake, "worker-B", claim_guard=lambda task: False)
        assert q.read(block_ms=50) is None
        entry = _pending_entry(fake, mid)
        assert entry["consumer"] == "worker-A", "被拒的消息不得改换持有者"
        assert int(entry["times_delivered"]) == 1, "被拒不得烧投递次数（#47 核心）"
        assert fake.xlen(q.dlq_key) == 0
        assert q.stats()["claim_guard_skipped"] == 1
        assert q.stats()["reclaimed"] == 0

    def test_repeated_reads_do_not_grow_delivery_count(self):
        """多轮 reclaim 都拒绝 → 计数恒 1、idle 单调增长（不再被刷新归零）。

        验收条件（#47）：「XPENDING 在饱和窗内条目数稳定，不出现同一条目
        反复刷新 idle」「该消息 times_delivered 不再增长」。
        """
        fake = fakeredis.FakeRedis(decode_responses=True)
        mid, _body = _seed_pending(fake)
        q = _queue(fake, "worker-B", claim_guard=lambda task: False)
        idles = []
        for _ in range(3):
            assert q.read(block_ms=20) is None
            idles.append(int(_pending_entry(fake, mid)["time_since_delivered"]))
            time.sleep(0.01)
        assert int(_pending_entry(fake, mid)["times_delivered"]) == 1
        assert idles == sorted(idles), f"idle 必须单调增长（未被 XCLAIM 归零）: {idles}"
        assert fake.xlen(q.dlq_key) == 0
        assert q.stats()["claim_guard_skipped"] == 3

    def test_exception_in_guard_skips_claim(self):
        """守卫抛异常（租约查询瞬断）→ 按拒绝处理，消息留 pending。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        mid, _body = _seed_pending(fake)

        def boom(task):
            raise ConnectionError("lease probe failed")

        q = _queue(fake, "worker-B", claim_guard=boom)
        assert q.read(block_ms=50) is None
        assert int(_pending_entry(fake, mid)["times_delivered"]) == 1
        assert q.stats()["claim_guard_skipped"] == 1

    def test_refusal_does_not_block_next_pending(self):
        """被拒的条目不阻塞扫描：同批下一条健康消息照常被服务。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        # claim_min_idle 拉高：只读不回收，两条 pending 都留在 PEL 里
        writer = _queue(fake, "worker-A", claim_min_idle_ms=60_000)
        writer.ensure_group()
        writer.enqueue({"type": "resume", "execution_id": "held-1"})
        writer.enqueue({"type": "resume", "execution_id": "free-2"})
        assert writer.read(block_ms=100) is not None
        assert writer.read(block_ms=100) is not None
        time.sleep(0.01)
        q = _queue(
            fake, "worker-B",
            claim_guard=lambda task: task.body.get("execution_id") != "held-1",
        )
        task = q.read(block_ms=100)
        assert task is not None and task.body["execution_id"] == "free-2"
        q.ack(task.message_id)
        assert q.stats()["claim_guard_skipped"] == 1

    def test_over_limit_pending_not_dead_lettered_when_guard_refuses(self):
        """超限条目同样先过抢单守卫：拒绝 → 既不死信也不重入队（DLQ 零新增）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        mid, _body = _seed_pending(fake, body={"type": "start", "flow_id": "f1"})
        assert fake.xclaim(QUEUE, GROUP, "worker-A", min_idle_time=1, message_ids=[mid])
        assert int(_pending_entry(fake, mid)["times_delivered"]) == 2

        q = _queue(
            fake, "worker-B",
            max_deliveries=2,
            claim_guard=lambda task: False,
            dead_letter_guard=lambda task: True,
        )
        assert q.read(block_ms=50) is None
        assert fake.xlen(q.dlq_key) == 0, "守卫拒绝不得死信"
        assert fake.xlen(QUEUE) == 1, "不得重入队新副本（会被再烧一轮）"
        assert q.stats()["dead_lettered"] == 0

    def test_claim_resumes_when_guard_allows(self):
        """守卫改放行（持有者已死/租约过期）→ 同一轮 reclaim 正常接管。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        mid, body = _seed_pending(fake)
        allow = {"v": False}
        q = _queue(fake, "worker-B", claim_guard=lambda task: allow["v"])
        assert q.read(block_ms=20) is None
        allow["v"] = True
        time.sleep(0.01)
        task = q.read(block_ms=100)
        assert task is not None and task.message_id == mid
        assert task.body == body, "重派必须带原始消息体"
        q.ack(task.message_id)
        assert q.stats()["reclaimed"] == 1


# ---------- worker 层：租约判据与接线 ----------


class TestWorkerClaimGuard:
    def _task(self, body):
        from plaita.server.task_queue import StreamTask

        return StreamTask(message_id="m-1", body=body, delivery_count=1)

    def test_refuses_while_lease_held(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _redis_worker(fake)
        fake.set("plaita:execution:lease:exec-47", "start:abc:1", ex=120)
        task = self._task({"type": "start", "execution_id": "exec-47", "tenant_id": "default"})
        assert worker._claim_guard(task) is False

    def test_allows_when_lease_absent(self):
        """租约空 → 放行：孤儿恢复与排队消息都靠这条路径重派（不得扣住）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _redis_worker(fake)
        task = self._task({"type": "resume", "execution_id": "exec-47", "tenant_id": "default"})
        assert worker._claim_guard(task) is True

    def test_routes_lease_key_by_tenant(self):
        """租约键按消息体 tenant_id 路由（守卫在 ContextVar 复位后运行）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _redis_worker(fake)
        fake.set("plaita:acme:execution:lease:exec-9", "start:abc:1", ex=120)
        acme = self._task({"type": "start", "execution_id": "exec-9", "tenant_id": "acme"})
        default = self._task({"type": "start", "execution_id": "exec-9", "tenant_id": "default"})
        assert worker._claim_guard(acme) is False
        assert worker._claim_guard(default) is True

    def test_allows_message_without_execution_id(self):
        """无 execution_id 的消息（schedule 裸 start）无租约可查 → 放行。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _redis_worker(fake)
        assert worker._claim_guard(self._task({"type": "start", "flow_id": "f1"})) is True

    def test_allows_non_dict_body(self):
        from plaita.server.task_queue import StreamTask

        worker = _redis_worker(fakeredis.FakeRedis(decode_responses=True))
        assert worker._claim_guard(StreamTask(message_id="m", body=None)) is True

    def test_query_failure_conservatively_skips(self):
        """租约查询瞬断 → 本轮不抢（抢单可延迟，判据不可信时不烧投递次数）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _redis_worker(fake)

        def blip(key):
            raise ConnectionError("redis blip")

        fake.exists = blip
        task = self._task({"type": "start", "execution_id": "exec-47", "tenant_id": "default"})
        assert worker._claim_guard(task) is False


class TestClaimGuardWiring:
    def test_get_task_queue_passes_claim_guard(self):
        """worker 构造的真实队列带上了抢单守卫（绑定到本 worker 实例）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _redis_worker(fake)
        queue = worker._get_task_queue()
        assert queue.claim_guard == worker._claim_guard
        assert callable(queue.claim_guard)


# ---------- 场景：长任务在跑，同伴的 reclaim 不动它；持有者消失即接管 ----------


class TestLongRunningExecutionNotFlushed:
    def test_reclaim_skips_while_holder_alive_then_takes_over(self):
        """验收臂 1+2：租约健在时多轮 reclaim 零动作；租约释放后照旧接管。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker_a = _redis_worker(fake, consumer_name="worker-A")
        worker_b = _redis_worker(fake, consumer_name="worker-B")

        body = {
            "type": "start", "flow_id": "f1", "version": "1",
            "execution_id": "exec-47", "tenant_id": "default",
        }
        queue_a = worker_a._get_task_queue()
        mid = queue_a.enqueue(body)
        task = queue_a.read(block_ms=100)
        assert task is not None and task.message_id == mid

        # worker-A 正在跑长节点：租约在手、消息未 ack（at-least-once 常态）
        holder = "start:worker-a:1"
        assert worker_a.execution_lease.try_acquire("exec-47", holder, 120)

        queue_b = worker_b._get_task_queue()
        for _ in range(3):
            time.sleep(0.01)
            assert queue_b.read(block_ms=20) is None, "持有者活着时不得抢走"
        entry = _pending_entry(fake, mid)
        assert entry["consumer"] == "worker-A"
        assert int(entry["times_delivered"]) == 1
        assert fake.xlen(queue_b.dlq_key) == 0, "DLQ 零新增"
        assert queue_b.stats()["claim_guard_skipped"] == 3
        assert queue_b.stats()["reclaimed"] == 0

        # 持有者消失（崩溃 → 租约释放/过期）→ 恢复路径不被切断
        assert worker_a.execution_lease.release("exec-47", holder)
        time.sleep(0.01)
        recovered = queue_b.read(block_ms=100)
        assert recovered is not None and recovered.message_id == mid
        assert recovered.body == body
        queue_b.ack(recovered.message_id)


class TestOrphanRecoveryContractIntact:
    def test_over_limit_orphan_still_requeued_and_dead_lettered(self):
        """对照臂：真孤儿（租约空 + 非终态）照旧「重入队副本 + 原消息进 DLQ」。

        这条是 CI 钉住的收容契约（``plaita-console/scripts/e2e-chaos-dlq.sh``：
        挂起派发持续失败 → 不 ack 留 pending → 清道夫回收 → 守卫重入队恢复
        副本 + 原消息进 DLQ），抢单闸不得把它一起拦掉。
        """
        from plaita.storage.base import ExecutionState

        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _redis_worker(fake, consumer_name="worker-B", max_deliveries=2)
        worker.execution_storage.save_execution_state(
            "exec-47",
            ExecutionState(
                execution_id="exec-47", flow_id="f1", status="suspended", context={}
            ),
        )
        body = {"type": "resume", "flow_id": "f1", "execution_id": "exec-47",
                "tenant_id": "default", "resume_type": "event"}
        # 前置：主 worker 已投递该消息、不 ack（in-flight），消息留在 pending 且
        # 投递次数已到上限（清道夫回收时应就地收容）
        producer = _queue(fake, "worker-A", max_deliveries=5)
        producer.ensure_group()
        mid = producer.enqueue(body)
        assert producer.read(block_ms=100) is not None
        time.sleep(0.01)
        assert fake.xclaim(QUEUE, GROUP, "worker-A", min_idle_time=1, message_ids=[mid])

        # 清道夫（worker-B）回收：抢单闸放行（租约空）→ 死信守卫重入队恢复副本
        # + 原消息死信；同一次 read 里重入队的副本经 XREADGROUP 正常取回
        queue = worker._get_task_queue()
        recovered = queue.read(block_ms=50)
        assert recovered is not None and recovered.body == body, "副本应可正常派发"
        dlq_reasons = [
            json.loads(
                fields["payload"] if isinstance(fields, dict) else fields[b"payload"]
            )["reason"]
            for _id, fields in fake.xrange(queue.dlq_key)
        ]
        assert dlq_reasons == ["max_deliveries=2"], "真孤儿照旧收容进 DLQ"
        assert fake.xlen(queue.stream_key) == 1, "恢复副本已重入队（delivery 归 1）"
        assert fake.xpending_range(queue.stream_key, queue.group_name, min=mid, max=mid, count=1) == [], (
            "原消息死信后应被 ack（退出 PEL）"
        )

