"""A69-02/03 修后对抗复核残面（A1 第八十三轮：A83-02/03/05）。

覆盖三条 A69 事件族残面：

* **A83-02**：已缩短/已过期的类型桶没有修复路径。回填只扫 legacy 键，
  canonical 记录已存在时 ``skipped_existing`` 直接跳过，不补索引成员；
  canonical 记录（7d）对 ``list_events``/回扫永久失明。修复=回填
  ``--apply`` 顺带补索引 + 新增 ``--reindex`` 扫描正典记录修复任意损坏桶。
* **A83-03**：janitor 预算调大（>~8000）时单条 Lua ``unpack`` 溢出
  （``too many results to unpack``），该桶每 tick 都失败、永不收敛。
  修复=按 :data:`plaita.event.redis._PRUNE_EVAL_CHUNK_SIZE` 分批 EVAL。
* **A83-05**：订阅四索引仍无条件 ``EXPIRE``——短 TTL storage 写入会把旧订阅
  桶砍短，桶到期后旧订阅 data 仍在但 ``list_subscriptions``/EventFilter 全盲。
  修复=与事件 ``types:`` 同款「只延长不缩短」。

负控形态：A83-03 用代理客户端模拟真实 Redis 的 8000 参数上限（pre-fix 单条
EVAL 载 8200 成员 → ``ResponseError``，pruned=0）；A83-05 断言短 TTL 写入后
桶 TTL 仍接近 7d（pre-fix 变成 2s）；A83-02 断言修复后成员回到索引且
``list_events`` 可见（pre-fix 桶保持缺失、列表为空）。
"""
from __future__ import annotations

import asyncio
import importlib.util
import time
from pathlib import Path

import fakeredis
import fakeredis.aioredis
import pytest
from redis.exceptions import ResponseError

from plaita.event.core import Event, EventSubscription
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

CANONICAL_TTL = 60 * 60 * 24 * 7
BASE_TS = 1_700_000_000.0
#: 真实 Redis（Lua 5.1）``unpack`` 的经验上限；代理按此模拟溢出。
_REAL_UNPACK_LIMIT = 8000
#: 与实现同步的批大小（断言单条 EVAL 不超过它）。
_EVAL_CHUNK_SIZE = 500


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


# ------------------------------------------------------------------ A83-03


class _UnpackLimitClient:
    """代理：模拟 Redis Lua ``unpack`` 的参数上限，并记录单条 EVAL 的规模。

    pre-fix 的 ``_prune_index_members_atomic`` 把全部成员塞进一条 EVAL，预算
    8200 时超过真实 Redis 的 8000 参数上限 → ``ResponseError``，本 tick 零
    删除；post-fix 按 500 分批，每条 EVAL 都在上限内。
    """

    def __init__(self, inner, limit: int = _REAL_UNPACK_LIMIT):
        self._inner = inner
        self._limit = limit
        self.max_args = 0
        self.calls = 0

    def __getattr__(self, name):
        return getattr(self._inner, name)

    async def eval(self, script, numkeys, *args):
        self.calls += 1
        # 脚本参数 = 1 个索引键 + N 个记录键 + N 个成员；成员数 = (len(args)-1)/2
        member_count = (len(args) - 1) // 2
        self.max_args = max(self.max_args, member_count)
        if member_count > self._limit:
            raise ResponseError(
                "ERR Error running script: too many results to unpack"
            )
        return await self._inner.eval(script, numkeys, *args)


def test_event_prune_batches_large_budget_within_lua_limit():
    """budget=9000、单桶 8200 个死成员：不得整条 EVAL 溢出。

    pre-fix：单条 EVAL 载 8200 个成员 → 代理抛 ``ResponseError`` →
    ``errors=1``、``pruned=0``、桶保持 8200（janitor 永不收敛）。
    """
    server = fakeredis.FakeServer()
    storage = _event_storage(server)
    type_key = event_type_index_key("shared.type")

    async def _seed():
        await storage.redis.zadd(
            type_key, {f"dead-{i}": BASE_TS + i for i in range(8200)}
        )

    _run(_seed())
    proxy = _UnpackLimitClient(storage.redis)
    storage.redis = proxy

    async def _sweep():
        examined, pruned, errors = await storage.prune_indexes(budget=9000)
        remaining = await storage.redis.zcard(type_key)
        return examined, pruned, errors, remaining

    examined, pruned, errors, remaining = _run(_sweep())

    assert errors == 0, "大预算下 EVAL 溢出（too many results to unpack）"
    assert examined == 8200 and pruned == 8200
    assert remaining == 0, f"janitor 未收敛（剩余 {remaining}）"
    assert proxy.max_args <= _EVAL_CHUNK_SIZE, (
        f"单条 EVAL 载了 {proxy.max_args} 个成员（批上限 {_EVAL_CHUNK_SIZE}）"
    )
    assert proxy.calls >= 8200 // _EVAL_CHUNK_SIZE


def test_subscription_prune_batches_large_budget_within_lua_limit():
    """订阅侧走同一 helper：大预算同样不得整条 EVAL 溢出。"""
    server = fakeredis.FakeServer()
    storage = _sub_storage(server)
    bucket = f"{storage.key_prefix}type:shared.type"

    async def _seed():
        await storage.redis.sadd(bucket, *[f"ghost-{i}" for i in range(1200)])

    _run(_seed())
    proxy = _UnpackLimitClient(storage.redis)
    storage.redis = proxy

    async def _sweep():
        examined, pruned, errors = await storage.prune_indexes(budget=1500)
        return examined, pruned, errors, await storage.redis.scard(bucket)

    examined, pruned, errors, remaining = _run(_sweep())

    assert errors == 0
    assert examined == 1200 and pruned == 1200
    assert remaining == 0
    assert proxy.max_args <= _EVAL_CHUNK_SIZE


# ------------------------------------------------------------------ A83-05


def test_short_ttl_subscription_write_does_not_shorten_bucket():
    """短 TTL storage 写入不得把同类型旧订阅桶砍短（A78-01 订阅半）。

    pre-fix：``store_subscription`` 无条件 ``EXPIRE`` → 桶 TTL=2s；桶到期后
    旧订阅 data（7d）仍在，但 ``list_subscriptions(event_type=...)`` 返回空。
    """
    server = fakeredis.FakeServer()
    long_storage = _sub_storage(server, ttl=CANONICAL_TTL)
    short_storage = _sub_storage(server, ttl=2)
    type_key = f"{long_storage.key_prefix}type:shared.type"

    old = EventSubscription(
        subscription_id="old-sub", event_type="shared.type", flow_id="flow-1"
    )
    fresh = EventSubscription(
        subscription_id="fresh-sub", event_type="shared.type", flow_id="flow-1"
    )

    async def _scenario():
        await long_storage.store_subscription(old)
        before = await long_storage.redis.ttl(type_key)
        await short_storage.store_subscription(fresh)
        after = await long_storage.redis.ttl(type_key)
        rows = await long_storage.list_subscriptions(event_type="shared.type")
        return before, after, {row.subscription_id for row in rows}

    before, after, ids = _run(_scenario())

    assert before > CANONICAL_TTL - 60
    assert after > CANONICAL_TTL - 60, (
        f"短 TTL 写入把订阅桶砍到 {after}s——桶到期后旧订阅对匹配失明"
    )
    assert ids == {"old-sub", "fresh-sub"}


# ------------------------------------------------------------------ A83-02


def _seed_legacy(client, event_id: str = "legacy-1", ttl: int = 3600) -> None:
    client.set(
        f"plaita:event:{event_id}",
        (
            '{"event_id": "%s", "event_type": "shared.type", '
            '"timestamp": %.6f, "data": {}}' % (event_id, time.time())
        ),
        ex=ttl,
    )


def test_backfill_repairs_index_when_canonical_already_exists():
    """回填重跑遇到 ``skipped_existing`` 时补回类型索引成员。

    pre-fix：canonical 记录在、桶被删 → ``skipped_existing`` 跳过，zcard=0、
    ``list_events`` 空（A83-02 的永久盲区）。
    """
    server = fakeredis.FakeServer()
    client = fakeredis.FakeRedis(server=server, decode_responses=True)
    _seed_legacy(client)
    type_key = event_type_index_key("shared.type")

    first = backfill.migrate(client, apply=True)
    assert first["migrated"] == 1
    assert client.zcard(type_key) == 1

    # 模拟「桶已被砍短并过期」：记录仍在，索引桶消失。
    client.delete(type_key)
    assert client.exists(event_record_key("legacy-1")) == 1

    dry = backfill.migrate(client, apply=False)
    assert client.zcard(type_key) == 0, "dry-run 不得写入"

    applied = backfill.migrate(client, apply=True)
    assert client.zcard(type_key) == 1, "重跑回填仍未把成员补回索引"
    assert dry["skipped_existing"] == 1
    assert dry["index_repaired"] == 1, "dry-run 未报告可修复的缺失成员"
    assert applied["skipped_existing"] == 1
    assert applied["index_repaired"] == 1

    events = _run(_event_storage(server).list_events(event_type="shared.type", limit=10))
    assert [event.event_id for event in events] == ["legacy-1"]


def test_reindex_repairs_bucket_without_legacy_record():
    """``--reindex`` 扫描正典记录修复任意损坏桶（不依赖 legacy 键）。"""
    server = fakeredis.FakeServer()
    client = fakeredis.FakeRedis(server=server, decode_responses=True)
    event = _event("canon-only", ts=BASE_TS)
    store_event_sync(client, event, ttl=CANONICAL_TTL)
    type_key = event_type_index_key(event.event_type)

    client.delete(type_key)

    dry = backfill.reindex(client, apply=False)
    assert dry["scanned"] == 1
    assert dry["reindexed"] == 1
    assert client.zcard(type_key) == 0, "dry-run 不得写入"

    applied = backfill.reindex(client, apply=True)
    assert applied["reindexed"] == 1
    assert client.zcard(type_key) == 1
    assert client.ttl(type_key) > CANONICAL_TTL - 60, "修复动作未给桶续期"
    assert client.ttl(event_record_key(event.event_id)) > 0, "不得改动记录 TTL"

    again = backfill.reindex(client, apply=True)
    assert again["reindexed"] == 0
    assert again["already_indexed"] == 1, "重复 reindex 必须幂等"

    events = _run(_event_storage(server).list_events(event_type="shared.type", limit=10))
    assert [e.event_id for e in events] == ["canon-only"]


def test_reindex_extends_short_bucket_ttl_without_shortening():
    """``--reindex`` 只延长桶 TTL：短桶被续期，长桶不被砍短。"""
    server = fakeredis.FakeServer()
    client = fakeredis.FakeRedis(server=server, decode_responses=True)
    event = _event("canon-ttl", ts=BASE_TS)
    store_event_sync(client, event, ttl=CANONICAL_TTL)
    type_key = event_type_index_key(event.event_type)

    client.expire(type_key, 5)
    assert client.ttl(type_key) <= 5

    backfill.reindex(client, apply=True)
    assert client.ttl(type_key) > CANONICAL_TTL - 60


def test_reindex_repairs_subscription_indexes():
    """订阅桶过期后 data 仍在：``--reindex`` 重建四索引成员。

    与 A83-02 事件半同源——没有这条路径时，``list_subscriptions`` 与
    EventFilter 对旧订阅永久失明（只有重写订阅才会重建索引）。
    """
    server = fakeredis.FakeServer()
    client = fakeredis.FakeRedis(server=server, decode_responses=True)
    sub = EventSubscription(
        subscription_id="sub-repair",
        event_type="shared.type",
        correlation_id="corr-1",
        flow_id="flow-1",
        node_id="node-1",
    )

    async def _seed():
        storage = _sub_storage(server)
        await storage.store_subscription(sub)

    _run(_seed())
    buckets = [
        "plaita:subscription:type:shared.type",
        "plaita:subscription:correlation:corr-1",
        "plaita:subscription:flow:flow-1",
        "plaita:subscription:node:node-1",
    ]
    for bucket in buckets:
        client.delete(bucket)

    dry = backfill.reindex_subscriptions(client, apply=False)
    assert dry["scanned"] == 1
    assert dry["reindexed"] == 1
    assert all(client.scard(b) == 0 for b in buckets), "dry-run 不得写入"

    applied = backfill.reindex_subscriptions(client, apply=True)
    assert applied["reindexed"] == 1
    for bucket in buckets:
        assert client.sismember(bucket, "sub-repair") == 1, f"索引未修复: {bucket}"
        assert client.ttl(bucket) > CANONICAL_TTL - 60, f"索引桶未续期: {bucket}"
    assert client.ttl(f"plaita:subscription:data:sub-repair") > 0, (
        "不得改动订阅 data TTL"
    )

    again = backfill.reindex_subscriptions(client, apply=True)
    assert again["reindexed"] == 0
    assert again["already_indexed"] == 1, "重复 reindex 必须幂等"

    rows = _run(
        _sub_storage(server).list_subscriptions(event_type="shared.type")
    )
    assert [row.subscription_id for row in rows] == ["sub-repair"]


def test_reindex_cli_wires_flag(monkeypatch):
    """CLI ``--reindex --apply`` 走事件+订阅修复路径并返回 0。"""
    server = fakeredis.FakeServer()
    client = fakeredis.FakeRedis(server=server, decode_responses=True)
    event = _event("cli-evt", ts=BASE_TS)
    store_event_sync(client, event, ttl=CANONICAL_TTL)
    type_key = event_type_index_key(event.event_type)
    client.delete(type_key)

    sub = EventSubscription(
        subscription_id="cli-sub", event_type="shared.type", flow_id="flow-1"
    )

    async def _seed_sub():
        await _sub_storage(server).store_subscription(sub)

    _run(_seed_sub())
    client.delete("plaita:subscription:type:shared.type")

    monkeypatch.setattr(backfill, "make_redis_client", lambda *a, **kw: client)
    assert backfill.main(["--reindex", "--apply"]) == 0
    assert client.zcard(type_key) == 1
    assert client.sismember("plaita:subscription:type:shared.type", "cli-sub") == 1
