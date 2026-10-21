"""#277 (r54): DLQ/subscription writes must reject deleted/stale tenant generations.

``delete_tenant`` purges the tenant namespace, DLQ and subscriptions, but an
in-flight worker that already took a task can still fail afterwards and write a
dead letter / re-register a subscription. Those writes are not protected by
execution fencing; the same DLQ key is platform-level, so the old payload
(reported probe: ``OLD-SECRET``) becomes readable by a recreated tenant.

Fix: console publishes the tenant generation in Redis on create; workers capture
it when reading/dispatching a carrier (``StreamTask.tenant_generation`` and a
ContextVar for subscriptions); DLQ and subscription writes compare the current
status/generation and raise the named ``TenantWriteRejected`` (dead-letter guard
returns False, leaving the message pending; nothing is written/acked).
"""
from __future__ import annotations

import asyncio
import json

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.event.core import EventSubscription
from plaita.event.redis import RedisEventSubscriptionStorage
from plaita.server.task_queue import RedisStreamTaskQueue, StreamTask, enqueue_task
from plaita.tenant_context import TenantWriteRejected

STREAM = "test:r54-tenant-stream"
DLQ = "test:r54-tenant-dlq"
TENANT = "acme"
STATUS_KEY = "plaita:tenant_status"
META_KEY = "plaita:tenant_status:meta"


def _seed(fake, *, status="active", generation="gen-1"):
    fake.hset(STATUS_KEY, TENANT, status)
    if generation is None:
        fake.hdel(META_KEY, TENANT)
    else:
        fake.hset(
            META_KEY,
            TENANT,
            json.dumps({"tenant_generation": generation}),
        )


def _queue(fake):
    return RedisStreamTaskQueue(
        fake,
        STREAM,
        group_name="g",
        consumer_name="c",
        dlq_key=DLQ,
        claim_min_idle_ms=0,
    )


def _task(generation, *, tenant=TENANT, secret="OLD-SECRET"):
    return StreamTask(
        message_id="1-0",
        body={
            "type": "start",
            "tenant_id": tenant,
            "execution_id": "e",
            "params": {"secret": secret},
        },
        tenant_generation=generation,
    )


class TestDlqWriteGate:
    def test_deleted_tenant_dlq_write_rejected(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        _seed(fake)
        queue = _queue(fake)
        assert queue.dead_letter_if_allowed(_task("gen-1"), reason="probe") is True
        assert fake.xlen(DLQ) == 1

        _seed(fake, status="disabled")
        assert queue.dead_letter_if_allowed(_task("gen-1"), reason="probe") is False
        assert fake.xlen(DLQ) == 1, "已删租户的迟到死信不得写回平台 DLQ"
        with pytest.raises(TenantWriteRejected) as excinfo:
            queue.dead_letter(_task("gen-1"), reason="probe")
        assert excinfo.value.tenant_id == TENANT
        assert excinfo.value.reason == "tenant_disabled"

    def test_recreated_generation_rejects_old_task(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        _seed(fake, generation="gen-2")  # 同名重建后的新世代
        queue = _queue(fake)

        old_payload = json.dumps(_task("gen-1").body)
        assert queue.dead_letter_if_allowed(_task("gen-1"), reason="probe") is False
        assert fake.xlen(DLQ) == 0
        assert old_payload not in (fake.xrange(DLQ) or [])

        # 新世代自己的载体可以正常死信
        assert queue.dead_letter_if_allowed(
            _task("gen-2", secret="NEW"), reason="probe"
        ) is True
        raw = fake.xrange(DLQ)[0][1]["payload"]
        assert "OLD-SECRET" not in raw
        assert "NEW" in raw

    def test_missing_generation_fails_closed_when_meta_exists(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        _seed(fake, generation="gen-1")
        queue = _queue(fake)
        assert queue.dead_letter_if_allowed(_task(None), reason="probe") is False

    def test_default_tenant_keeps_compatibility(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        queue = _queue(fake)
        assert queue.dead_letter_if_allowed(
            _task(None, tenant="default"), reason="probe"
        ) is True

    def test_queue_read_captures_current_generation(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        _seed(fake, generation="gen-1")
        enqueue_task(
            fake,
            STREAM,
            {"type": "start", "tenant_id": TENANT, "execution_id": "e"},
            tenant_generation="gen-1",
        )
        queue = _queue(fake)
        queue.ensure_group()
        task = queue.read(block_ms=1)
        assert task is not None
        assert task.tenant_generation == "gen-1"


class TestSubscriptionWriteGate:
    def _storage(self, fake):
        storage = RedisEventSubscriptionStorage(
            "redis://localhost:6379/0", key_prefix="test:r54:subscription:"
        )
        storage.redis = fake
        return storage

    def test_deleted_tenant_subscription_rejected(self):
        async def scenario():
            fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
            await fake.hset(STATUS_KEY, TENANT, "disabled")
            await fake.hset(
                META_KEY, TENANT, json.dumps({"tenant_generation": "gen-1"})
            )
            storage = self._storage(fake)
            sub = EventSubscription(
                event_type="test.event",
                tenant_id=TENANT,
                tenant_generation="gen-1",
            )
            with pytest.raises(TenantWriteRejected):
                await storage.store_subscription(sub)
            assert await fake.keys("test:r54:subscription:*") == []

        asyncio.run(scenario())

    def test_recreated_generation_rejects_old_subscription(self):
        async def scenario():
            fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
            await fake.hset(STATUS_KEY, TENANT, "active")
            await fake.hset(
                META_KEY, TENANT, json.dumps({"tenant_generation": "gen-2"})
            )
            storage = self._storage(fake)
            old = EventSubscription(
                event_type="test.event",
                tenant_id=TENANT,
                tenant_generation="gen-1",
            )
            with pytest.raises(TenantWriteRejected):
                await storage.store_subscription(old)

            new = EventSubscription(
                event_type="test.event",
                tenant_id=TENANT,
                tenant_generation="gen-2",
            )
            sub_id = await storage.store_subscription(new)
            assert await storage.get_subscription(sub_id) is not None

            # 世代再次前移后，旧订阅的 mark 也必须拒绝。
            await fake.hset(
                META_KEY, TENANT, json.dumps({"tenant_generation": "gen-3"})
            )
            with pytest.raises(TenantWriteRejected):
                await storage.mark_event_processed(sub_id, "evt-1")

        asyncio.run(scenario())
