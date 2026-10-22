"""#282/#283 (r56): tenant-write rejection must not kill reclaim; generation travels with carriers.

#282: ``_reclaim_one``'s max_deliveries branch called ``dead_letter`` directly.
For a deleted/old-generation tenant, ``dead_letter`` raises TenantWriteRejected,
which escaped ``read()`` and killed the consume loop (crash/restart cycle). The
branch now goes through ``dead_letter_if_allowed`` (returns False, pending), and
the consume loop has a backstop catch.

#283: ``StreamTask.tenant_generation`` was sampled from the *current* meta at
read time. A requeue copy produced by an old worker after tenant recreation was
therefore read as the new generation and allowed into the new tenant's DLQ.
Now the generation is persisted in the message envelope; requeue/handover copies
inherit the source value; the worker dispatch gate compares source vs current.
Subscriptions without a generation context no longer auto-adopt the current one.
"""
from __future__ import annotations

import asyncio
import json
import threading

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.event.core import EventSubscription
from plaita.event.redis import RedisEventSubscriptionStorage
from plaita.server.flow_worker import RedisFlowWorker, _requeue_envelope_kwargs
from plaita.server.task_queue import (
    RedisStreamTaskQueue,
    StreamTask,
    enqueue_task,
)
from plaita.tenant_context import (
    TenantWriteRejected,
    reset_current_tenant_generation,
    set_current_tenant_generation,
)

STREAM = "test:r56-generation-stream"
DLQ = "test:r56-generation-dlq"
TENANT = "acme"
STATUS_KEY = "plaita:tenant_status"
META_KEY = "plaita:tenant_status:meta"


def _seed(fake, *, status="active", generation="gen-1"):
    fake.hset(STATUS_KEY, TENANT, status)
    if generation is None:
        fake.hdel(META_KEY, TENANT)
    else:
        fake.hset(META_KEY, TENANT, json.dumps({"tenant_generation": generation}))


def _queue(fake, *, max_deliveries=5, claim_min_idle_ms=0):
    return RedisStreamTaskQueue(
        fake,
        STREAM,
        group_name="g",
        consumer_name="c1",
        dlq_key=DLQ,
        claim_min_idle_ms=claim_min_idle_ms,
        max_deliveries=max_deliveries,
    )


class TestReclaimTenantRejection:
    def test_deleted_tenant_over_delivery_does_not_raise(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        _seed(fake, generation="gen-1")
        enqueue_task(
            fake,
            STREAM,
            {"type": "start", "tenant_id": TENANT, "execution_id": "e"},
            tenant_generation="gen-1",
        )
        queue = _queue(fake, max_deliveries=1)
        queue.ensure_group()
        assert queue.read(block_ms=1) is not None  # first delivery -> PEL

        _seed(fake, status="disabled", generation="gen-1")
        # 超投递 reclaim：tenant 写闸拒绝必须在 dead_letter_if_allowed 内被吸收。
        # A125-01：拒绝后不再留 pending 永久滞留——收容终局（隔离流 + ack）。
        assert queue.read(block_ms=1) is None
        assert fake.xlen(DLQ) == 0, "平台 DLQ 纯净性不变（#277）"
        assert queue.stats()["tenant_write_rejected"] >= 1
        assert queue.stats()["tenant_quarantined"] == 1
        pending = fake.xpending_range(STREAM, "g", "-", "+", 10)
        assert not pending, "原消息已收容 ack（终局），不再永滞 PEL"
        assert int(fake.xlen(f"{DLQ}:quarantine:{TENANT}")) == 1


class _LoopHost:
    """最小 consume-loop 宿主（只实现循环需要的接口）。"""

    read_block_ms = 1

    def __init__(self):
        self._running = True
        self._drain_event = threading.Event()
        self.seen: list[str] = []

    def _disk_guard_blocks_claim(self):
        return False

    def _wait_disk_guard(self):
        pass

    def _sweep_residue_if_due(self, queue):
        pass

    def _bump_active(self, delta):
        pass

    def _dispatch_task(self, body, delivery_count=None, schema_version=None,
                       stream_message_id=None):
        if body.get("execution_id") == "bad":
            raise TenantWriteRejected(TENANT, "tenant_disabled")
        self.seen.append(body["execution_id"])
        self._running = False


class TestConsumeLoopSurvivesRejection:
    def test_rejection_leaves_pending_counts_and_continues(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        _seed(fake, generation="gen-1")
        enqueue_task(
            fake, STREAM,
            {"type": "start", "tenant_id": TENANT, "execution_id": "bad"},
            tenant_generation="gen-1",
        )
        enqueue_task(
            fake, STREAM,
            {"type": "start", "tenant_id": TENANT, "execution_id": "good"},
            tenant_generation="gen-1",
        )
        queue = _queue(fake, claim_min_idle_ms=60_000)
        queue.ensure_group()

        host = _LoopHost()
        RedisFlowWorker._consume_loop(host, queue)

        # 拒写未杀死循环：后续正常消息照常处理；坏消息留 pending 且已计数。
        assert host.seen == ["good"]
        assert queue.stats()["tenant_write_rejected"] == 1
        pending = fake.xpending_range(STREAM, "g", "-", "+", 10)
        assert len(pending) == 1
        assert fake.xlen(STREAM) == 1


class TestGenerationTravels:
    def test_requeue_copy_preserves_source_generation(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        _seed(fake, generation="gen-1")
        task = StreamTask(
            message_id="1-0",
            body={"type": "start", "tenant_id": TENANT, "execution_id": "e"},
            tenant_generation="gen-1",
        )
        kwargs = _requeue_envelope_kwargs(task)
        assert kwargs["tenant_generation"] == "gen-1"

        # 同名重建到 gen2 后，旧进程写出的副本仍带 gen1；读取端不得采样成 gen2。
        _seed(fake, generation="gen-2")
        enqueue_task(fake, STREAM, dict(task.body), **kwargs)
        queue = _queue(fake)
        queue.ensure_group()
        copy = queue.read(block_ms=1)
        assert copy is not None
        assert copy.tenant_generation == "gen-1"

    def test_dispatch_rejects_old_generation_copy(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        _seed(fake, generation="gen-2")
        from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

        worker = RedisFlowWorker(
            redis_url="redis://localhost:6379/15",
            queue_name="test:r56-dispatch",
            execution_storage=MemoryExecutionStorage(),
            flow_storage=MemoryFlowStorage(),
            redis_client=fake,
            enable_registry=False,
            enable_redis_logging=False,
        )
        worker.start_flow = lambda *a, **k: (_ for _ in ()).throw(
            AssertionError("old-generation carrier must not start")
        )
        from datetime import datetime

        # 消费循环在 dispatch 前按载体源世代设置 ContextVar（#283）。
        token = set_current_tenant_generation("gen-1")
        try:
            worker._dispatch_task(
                {"type": "start", "tenant_id": TENANT, "execution_id": "e",
                 "flow_id": "f1", "timestamp": datetime.now().isoformat()},
            )
        finally:
            reset_current_tenant_generation(token)

    def test_subscription_without_generation_is_rejected(self):
        async def scenario():
            fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
            await fake.hset(STATUS_KEY, TENANT, "active")
            await fake.hset(
                META_KEY, TENANT, json.dumps({"tenant_generation": "gen-2"})
            )
            storage = RedisEventSubscriptionStorage(
                "redis://localhost:6379/0", key_prefix="test:r56:subscription:"
            )
            storage.redis = fake
            legacy = EventSubscription(event_type="test.event", tenant_id=TENANT)
            with pytest.raises(TenantWriteRejected):
                await storage.store_subscription(legacy)

            explicit = EventSubscription(
                event_type="test.event", tenant_id=TENANT,
                tenant_generation="gen-2",
            )
            sub_id = await storage.store_subscription(explicit)
            assert await storage.get_subscription(sub_id) is not None

        asyncio.run(scenario())
