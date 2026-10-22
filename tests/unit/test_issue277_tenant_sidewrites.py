"""#277 (r54): DLQ/subscription writes must reject deleted/stale tenant generations.

``delete_tenant`` purges the tenant namespace, DLQ and subscriptions, but an
in-flight worker that already took a task can still fail afterwards and write a
dead letter / re-register a subscription. Those writes are not protected by
execution fencing; the same DLQ key is platform-level, so the old payload
(reported probe: ``OLD-SECRET``) becomes readable by a recreated tenant.

Fix: console publishes the tenant generation in Redis on create; workers capture
it when reading/dispatching a carrier (``StreamTask.tenant_generation`` and a
ContextVar for subscriptions); DLQ and subscription writes compare the current
status/generation and raise the named ``TenantWriteRejected``.

A125-01/02 (r83) 契约演进：「拒绝后留 pending 永久滞留」改为**收容终局**——
``dead_letter_if_allowed`` 捕拒绝后把信封写租户隔离流
``{dlq}:quarantine:{tenant}`` 并 ack 原消息（平台 DLQ 纯净性不变；直调
``dead_letter`` 仍抛具名 ``TenantWriteRejected``，写闸本身 fail-closed 不变）。
本文件同步升级：原来的「returns False / stays pending」断言改为「returns True
+ 平台 DLQ 不受污染 + 证据落在隔离流」。
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
from plaita.server.tenant_status import tenant_write_allowed
from plaita.tenant_context import TenantWriteRejected

STREAM = "test:r54-tenant-stream"
DLQ = "test:r54-tenant-dlq"
TENANT = "acme"
STATUS_KEY = "plaita:tenant_status"
META_KEY = "plaita:tenant_status:meta"


def _quarantine_len(fake):
    return int(fake.xlen(f"{DLQ}:quarantine:{TENANT}") or 0)


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
        # A125：写闸仍 fail-closed，但调用面收容终局（隔离流 + ack），不再
        # 「留 pending 永久滞留」；平台 DLQ 不受污染。
        assert queue.dead_letter_if_allowed(_task("gen-1"), reason="probe") is True
        assert fake.xlen(DLQ) == 1, "已删租户的迟到死信不得写回平台 DLQ"
        assert _quarantine_len(fake) == 1
        with pytest.raises(TenantWriteRejected) as excinfo:
            queue.dead_letter(_task("gen-1"), reason="probe")
        assert excinfo.value.tenant_id == TENANT
        assert excinfo.value.reason == "tenant_disabled"

    def test_recreated_generation_rejects_old_task(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        _seed(fake, generation="gen-2")  # 同名重建后的新世代
        queue = _queue(fake)

        old_payload = json.dumps(_task("gen-1").body)
        # A125：旧世代死信写被拒后收容终局——平台 DLQ 不收，证据在隔离流。
        assert queue.dead_letter_if_allowed(_task("gen-1"), reason="probe") is True
        assert fake.xlen(DLQ) == 0
        assert old_payload not in (fake.xrange(DLQ) or [])
        assert _quarantine_len(fake) == 1
        quarantined = json.loads(
            fake.xrange(f"{DLQ}:quarantine:{TENANT}")[0][1]["payload"]
        )
        assert quarantined["payload"]["params"]["secret"] == "OLD-SECRET"
        assert quarantined["tenant_write_rejected"] == "generation_changed"
        assert quarantined["tenant_generation"] == "gen-1"

        # 新世代自己的载体可以正常死信（进平台 DLQ，不进收容）
        assert queue.dead_letter_if_allowed(
            _task("gen-2", secret="NEW"), reason="probe"
        ) is True
        raw = fake.xrange(DLQ)[0][1]["payload"]
        assert "OLD-SECRET" not in raw
        assert "NEW" in raw
        assert _quarantine_len(fake) == 1

    def test_missing_generation_gate_still_fails_closed_contained(self):
        """A125-02：legacy 载体×已发布世代——写闸 fail-closed 不变，调用面收容。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        _seed(fake, generation="gen-1")
        queue = _queue(fake)

        allowed, reason = tenant_write_allowed(fake, TENANT, None)
        assert (allowed, reason) == (False, "generation_missing"), "写闸本身不变"

        assert queue.dead_letter_if_allowed(_task(None), reason="probe") is True
        assert fake.xlen(DLQ) == 0
        assert _quarantine_len(fake) == 1

    def test_default_tenant_keeps_compatibility(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        queue = _queue(fake)
        assert queue.dead_letter_if_allowed(
            _task(None, tenant="default"), reason="probe"
        ) is True
        assert _quarantine_len(fake) == 0

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
