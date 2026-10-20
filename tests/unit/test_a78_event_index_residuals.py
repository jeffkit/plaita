"""A69-02/03 修后复核残面（A1 第七十八轮：A78-01/04/05 + B78-01）。

覆盖四条修后缺陷：

* **A78-01**：回填/短 TTL 写入不得把同类型 canonical 索引桶 TTL 砍短——
  桶先过期后，同类型仍存活的 7d 记录对 ``list_events`` 全盲，重跑回填因
  ``skipped_existing`` 也无法修复；
* **A78-04**：janitor 的 ``PLAITA_EVENT_INDEX_SWEEP_INTERVAL=nan/inf`` 通过
  ``float()`` 但 ``asyncio.wait_for`` 永不超时 → 周期清扫静默停摆；
* **A78-05**：读侧/janitor 的 ``EXISTS/MGET → ZREM/SREM`` 两段往返，窗口内
  同 id 重写时活成员被删出索引（记录在、``list_*`` 盲）；
* **B78-01**：桶轮转每 tick 只前进 1，窗口内其余桶被重复检查，首轮覆盖需要
  N tick 而不是 N/budget。

负控（clean pre-fix 行）：
- A78-01：桶 TTL 被回填记录的 5s 覆盖 → ``ttl < 600``（本组 A78-01 红）；
- A78-04：``interval_seconds=nan`` → 只有启动首轮 sweep；
- A78-05：清理走 ``pipeline.exists → zrem``，代理在 ``eval`` 注入的重写不会
  发生 → 活成员被删（断言 zcard/sismember 保留即红）；
- B78-01：20 桶 × (1 live + 1 missing)、budget=4，最后一个桶约第 19 tick
  才被检查 → 12 tick 内未全部收敛即红。
"""
from __future__ import annotations

import asyncio
import importlib.util
import json
import logging
import time
from pathlib import Path

import fakeredis
import fakeredis.aioredis
import pytest

from plaita.event.core import Event, EventSubscription
from plaita.event.index_janitor import IndexJanitor
from plaita.event.redis import (
    RedisEventStorage,
    RedisEventSubscriptionStorage,
    event_record_key,
    event_type_index_key,
    store_event_sync,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "migrate_console_events",
    REPO_ROOT / "scripts" / "migrate_console_events.py",
)
backfill = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(backfill)

LEGACY_PREFIX = "plaita:event:"
CANONICAL_TTL = 60 * 60 * 24 * 7


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _event(event_id: str, event_type: str = "shared.type", ts: float | None = None) -> Event:
    return Event(
        event_id=event_id,
        event_type=event_type,
        data={},
        timestamp=ts if ts is not None else time.time(),
    )


def _event_storage(server, ttl: int = CANONICAL_TTL) -> RedisEventStorage:
    storage = RedisEventStorage(redis_url="redis://localhost:6379/15", ttl=ttl)
    storage.redis = fakeredis.aioredis.FakeRedis(
        server=server, decode_responses=True
    )
    return storage


def _sub_storage(server, ttl: int = CANONICAL_TTL) -> RedisEventSubscriptionStorage:
    storage = RedisEventSubscriptionStorage(
        redis_url="redis://localhost:6379/15", ttl=ttl
    )
    storage.redis = fakeredis.aioredis.FakeRedis(
        server=server, decode_responses=True
    )
    return storage


# ------------------------------------------------------------------ A78-01


def test_backfill_short_legacy_ttl_does_not_shorten_index_bucket():
    """回填一条剩余 5s 的 legacy：记录可短，类型桶必须保持 7d。

    pre-fix：``store_event_sync`` 无条件 ``EXPIRE type_key ttl`` → 桶 TTL=5；
    桶到期后同类型 canonical 记录仍在但 ``list_events`` 全盲。
    """
    server = fakeredis.FakeServer()
    sync_client = fakeredis.FakeRedis(server=server, decode_responses=True)
    type_key = event_type_index_key("shared.type")

    for event_id in ("canon-a", "canon-b"):
        store_event_sync(sync_client, _event(event_id), ttl=CANONICAL_TTL)
    assert sync_client.ttl(type_key) > CANONICAL_TTL - 60

    legacy = {
        "event_id": "legacy-1",
        "event_type": "shared.type",
        "timestamp": time.time(),
        "data": {},
    }
    sync_client.set(f"{LEGACY_PREFIX}legacy-1", json.dumps(legacy), ex=5)

    stats = backfill.migrate(sync_client, apply=True)
    assert stats["migrated"] == 1

    bucket_ttl = sync_client.ttl(type_key)
    assert bucket_ttl > CANONICAL_TTL - 60, (
        f"类型桶 TTL 被短成员砍到 {bucket_ttl}s——桶到期后 canonical 记录失明"
    )
    assert sync_client.zcard(type_key) == 3

    events = _run(_event_storage(server).list_events(limit=50))
    assert {event.event_id for event in events} == {"canon-a", "canon-b", "legacy-1"}


def test_smaller_event_ttl_does_not_shorten_bucket_of_older_members():
    """``PLAITA_EVENT_TTL_SECONDS`` 调小后，已有桶不被新写砍短（同根因）。"""
    server = fakeredis.FakeServer()
    sync_client = fakeredis.FakeRedis(server=server, decode_responses=True)
    type_key = event_type_index_key("shared.type")

    store_event_sync(sync_client, _event("long-lived"), ttl=CANONICAL_TTL)
    store_event_sync(sync_client, _event("short-lived"), ttl=30)

    assert sync_client.ttl(type_key) > CANONICAL_TTL - 60


# ------------------------------------------------------------------ A78-04


class _CountingStorage:
    def __init__(self):
        self.calls = 0

    async def prune_indexes(self, budget: int = 500):
        self.calls += 1
        return 0, 0, 0


def test_janitor_rejects_non_finite_interval(monkeypatch, caplog):
    """``nan``/``inf`` 周期必须回退默认（否则首轮后静默停摆）。"""
    import plaita.event.index_janitor as janitor_module

    monkeypatch.setattr(janitor_module, "DEFAULT_SWEEP_INTERVAL_SECONDS", 0.02)
    for raw in ("nan", "inf", "-inf"):
        with caplog.at_level(logging.WARNING):
            janitor = IndexJanitor(event_storage=_CountingStorage(), interval_seconds=raw)
        assert janitor.interval_seconds == 0.02, raw
        assert any("非法" in record.message for record in caplog.records), raw


def test_janitor_with_nan_interval_still_sweeps_periodically(monkeypatch):
    import plaita.event.index_janitor as janitor_module

    monkeypatch.setattr(janitor_module, "DEFAULT_SWEEP_INTERVAL_SECONDS", 0.02)
    storage = _CountingStorage()
    janitor = IndexJanitor(event_storage=storage, interval_seconds="nan")

    async def _exercise():
        await janitor.start()
        await asyncio.sleep(0.15)
        await janitor.stop()

    _run(_exercise())
    # pre-fix：wait_for(timeout=nan) 永不触发 → calls == 1。
    assert storage.calls >= 3, f"nan 间隔下只发生 {storage.calls} 次 sweep"


# ------------------------------------------------------------------ A78-05


class _RewriteOnEvalClient:
    """代理：在第一次 ``eval`` 前重写同 id 记录（模拟清理窗口内的并发写）。

    pre-fix 的清理走 ``pipeline.exists → zrem``，不会调用 ``eval``，因此重写
    不发生、活成员被删；post-fix 的判死+删除在 Lua 内，重写先于脚本 → 保留。
    """

    def __init__(self, inner, rewrite):
        self._inner = inner
        self._rewrite = rewrite
        self.rewritten = 0

    def __getattr__(self, name):
        return getattr(self._inner, name)

    async def eval(self, script, numkeys, *args):
        if not self.rewritten:
            self.rewritten += 1
            await self._rewrite()
        return await self._inner.eval(script, numkeys, *args)


def test_event_prune_keeps_member_rewritten_in_cleanup_window():
    server = fakeredis.FakeServer()
    storage = _event_storage(server)
    client = storage.redis
    event = _event("race-evt", ts=time.time())
    record_key = event_record_key(event.event_id)
    type_key = event_type_index_key(event.event_type)

    async def _scenario():
        # 索引里有成员、payload 缺失（过期/逐出）——读路径会判死并清理。
        await client.zadd(type_key, {event.event_id: event.timestamp})

        async def rewrite():
            # 并发写回同 id：记录 + 索引成员都重新落地。
            await client.set(record_key, event.model_dump_json(), ex=CANONICAL_TTL)
            await client.zadd(type_key, {event.event_id: event.timestamp})

        storage.redis = _RewriteOnEvalClient(client, rewrite)
        try:
            await storage.list_events(limit=10)
        finally:
            storage.redis = client
        return await client.exists(record_key), await client.zcard(type_key)

    record_exists, zcard = _run(_scenario())

    # pre-fix：无 eval → rewrite 未发生 → zrem 删掉活成员（zcard=0）→ 红。
    assert record_exists == 1
    assert zcard == 1, (
        "清理窗口内重写的活成员被删出索引（记录在、list_events 盲）"
    )


def test_subscription_prune_keeps_member_rewritten_in_cleanup_window():
    server = fakeredis.FakeServer()
    storage = _sub_storage(server)
    client = storage.redis
    sub = EventSubscription(
        subscription_id="race-sub", event_type="shared.type", flow_id="f1"
    )
    data_key = f"{storage.key_prefix}data:{sub.subscription_id}"
    index_key = f"{storage.key_prefix}type:{sub.event_type}"

    async def _scenario():
        await client.sadd(index_key, sub.subscription_id)

        async def rewrite():
            await client.set(data_key, sub.model_dump_json(), ex=CANONICAL_TTL)
            await client.sadd(index_key, sub.subscription_id)

        storage.redis = _RewriteOnEvalClient(client, rewrite)
        try:
            await storage.list_subscriptions(event_type="shared.type")
        finally:
            storage.redis = client
        return await client.exists(data_key), await client.sismember(
            index_key, sub.subscription_id
        )

    data_exists, indexed = _run(_scenario())

    assert data_exists == 1
    assert indexed == 1, (
        "清理窗口内重写的活订阅被 SREM 出索引（数据在、list 盲）"
    )


# ------------------------------------------------------------------ B78-01


def test_janitor_rotation_covers_buckets_within_budget_slices():
    """20 桶 × (1 live + 1 missing)、budget=4：12 tick 内必须全部收敛。

    pre-fix 轮转每 tick 只 +1，最后几个桶约第 19 tick 才被检查 → 12 tick 时
    仍有 missing 成员 → 红。
    """
    server = fakeredis.FakeServer()
    storage = _event_storage(server)
    client = storage.redis
    base_ts = 1_700_000_000.0

    async def _seed():
        for i in range(20):
            type_key = event_type_index_key(f"type-{i:02d}")
            live = _event(f"live-{i}", event_type=f"type-{i:02d}", ts=base_ts + i)
            await client.set(event_record_key(live.event_id), live.model_dump_json())
            await client.zadd(
                type_key,
                {live.event_id: live.timestamp, f"missing-{i}": base_ts + i},
            )

    _run(_seed())

    async def _sweep():
        for _ in range(12):
            await storage.prune_indexes(budget=4)
            remaining = 0
            for i in range(20):
                remaining += int(
                    await client.zcard(event_type_index_key(f"type-{i:02d}"))
                ) - 1
            if remaining == 0:
                return True
        return False

    assert _run(_sweep()) is True, "12 tick 内未覆盖全部桶（轮转被重复检查拖慢）"
