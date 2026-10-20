"""A69-03 §3.3：``event_index_pruned_total{kind=...}`` 按 kind 的删除计数。

规格要求读时惰性清理、写侧兜底清理与 janitor 三条路径的删除都可见，并按
``event_type`` / ``sub_type`` / ``sub_correlation`` / ``sub_flow`` / ``sub_node``
分组。此前实现只有 INFO 日志和 janitor 的聚合 ``event_index_sweep_pruned_total``，
读时清理没有指标、冷桶也分不出是哪个订阅索引在删。

本组用例逐路径钉住计数（不改变删除行为本身，行为由
``test_a69_03_index_cleanup.py`` / ``test_a69_03_index_write_prune.py`` 覆盖）。
"""
from __future__ import annotations

import asyncio
from typing import Dict

import fakeredis
import fakeredis.aioredis
from fakeredis import FakeRedis

from plaita.event.core import Event, EventSubscription
from plaita.event.index_janitor import IndexJanitor
from plaita.event.redis import (
    RedisEventStorage,
    RedisEventSubscriptionStorage,
    event_index_pruned_metrics,
    reset_event_index_pruned_metrics,
    store_event_sync,
)

EVENT_PREFIX = "plaita:event:"
SUB_PREFIX = "plaita:subscription:"
BASE_TS = 1_700_000_000.0


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _event_storage(server, ttl: int = 3600) -> RedisEventStorage:
    storage = RedisEventStorage(redis_url="redis://localhost:6379/15", ttl=ttl)
    storage.redis = fakeredis.aioredis.FakeRedis(server=server, decode_responses=True)
    return storage


def _sub_storage(server, ttl: int = 3600) -> RedisEventSubscriptionStorage:
    storage = RedisEventSubscriptionStorage(
        redis_url="redis://localhost:6379/15", ttl=ttl
    )
    storage.redis = fakeredis.aioredis.FakeRedis(server=server, decode_responses=True)
    return storage


def _non_zero() -> Dict[str, int]:
    return {kind: count for kind, count in event_index_pruned_metrics().items() if count}


# ---------------------------------------------------------------- 读时事件


def test_event_read_prune_counts_by_kind():
    reset_event_index_pruned_metrics()
    server = fakeredis.FakeServer()

    async def _t():
        storage = _event_storage(server)
        for i in range(2):
            await storage.store_event(
                Event(
                    event_type="metrics-read",
                    data={},
                    event_id=f"live-{i}",
                    timestamp=BASE_TS + i,
                )
            )
        await storage.redis.zadd(
            f"{EVENT_PREFIX}types:metrics-read",
            {"dead-0": BASE_TS + 10, "dead-1": BASE_TS + 11},
        )
        return await storage.list_events(event_type="metrics-read", limit=10)

    assert len(_run(_t())) == 2
    assert _non_zero() == {"event_type": 2}


# ---------------------------------------------------------------- 读时订阅


def test_subscription_read_prune_counts_selected_kind():
    cases = [
        ({"event_type": "metrics-type"}, "sub_type", f"{SUB_PREFIX}type:metrics-type"),
        ({"correlation_id": "mc-1"}, "sub_correlation", f"{SUB_PREFIX}correlation:mc-1"),
        ({"flow_id": "mf-1"}, "sub_flow", f"{SUB_PREFIX}flow:mf-1"),
        ({"node_id": "mn-1"}, "sub_node", f"{SUB_PREFIX}node:mn-1"),
    ]
    for kwargs, kind, bucket in cases:
        reset_event_index_pruned_metrics()
        server = fakeredis.FakeServer()

        async def _t(bucket=bucket, kwargs=kwargs):
            storage = _sub_storage(server)
            await storage.store_subscription(
                EventSubscription(
                    event_type="metrics-type",
                    correlation_id="mc-1",
                    flow_id="mf-1",
                    node_id="mn-1",
                )
            )
            await storage.redis.sadd(bucket, "ghost")
            return await storage.list_subscriptions(**kwargs)

        assert len(_run(_t())) == 1
        assert _non_zero() == {kind: 1}, f"kind={kind} kwargs={kwargs}"


def test_subscription_read_prune_full_scan_path_is_not_counted():
    """全量 SCAN 路径没有桶信息，不清理也不计数（交给 janitor）。"""
    reset_event_index_pruned_metrics()
    server = fakeredis.FakeServer()

    async def _t():
        storage = _sub_storage(server)
        await storage.store_subscription(
            EventSubscription(event_type="metrics-scan")
        )
        await storage.redis.sadd(f"{SUB_PREFIX}type:metrics-scan", "ghost")
        return await storage.list_subscriptions()

    assert len(_run(_t())) == 1
    assert _non_zero() == {}


# ---------------------------------------------------------------- janitor


def test_event_janitor_prune_counts_by_kind():
    reset_event_index_pruned_metrics()
    server = fakeredis.FakeServer()

    async def _t():
        storage = _event_storage(server)
        await storage.redis.zadd(
            f"{EVENT_PREFIX}types:metrics-cold", {"ghost": BASE_TS}
        )
        janitor = IndexJanitor(event_storage=storage, budget=10)
        return await janitor.sweep_once()

    examined, pruned, errors = _run(_t())
    assert (examined, pruned, errors) == (1, 1, 0)
    assert _non_zero() == {"event_type": 1}


def test_subscription_janitor_prune_counts_each_kind():
    reset_event_index_pruned_metrics()
    server = fakeredis.FakeServer()

    async def _t():
        storage = _sub_storage(server)
        for bucket in (
            f"{SUB_PREFIX}type:mt",
            f"{SUB_PREFIX}correlation:mc",
            f"{SUB_PREFIX}flow:mf",
            f"{SUB_PREFIX}node:mn",
        ):
            await storage.redis.sadd(bucket, "ghost")
        janitor = IndexJanitor(subscription_storage=storage, budget=10)
        return await janitor.sweep_once()

    examined, pruned, errors = _run(_t())
    assert (examined, pruned, errors) == (4, 4, 0)
    assert _non_zero() == {
        "sub_type": 1,
        "sub_correlation": 1,
        "sub_flow": 1,
        "sub_node": 1,
    }


# ---------------------------------------------------------------- 写侧兜底


def test_write_prune_counts_event_type():
    reset_event_index_pruned_metrics()
    sync = FakeRedis(decode_responses=True)
    type_key = f"{EVENT_PREFIX}types:metrics-write"
    sync.zadd(type_key, {"ghost": BASE_TS})

    store_event_sync(
        sync,
        Event(
            event_type="metrics-write",
            data={},
            event_id="live-write",
            timestamp=BASE_TS + 1,
        ),
        ttl=3600,
    )

    assert sync.zscore(type_key, "ghost") is None
    assert _non_zero() == {"event_type": 1}


# ---------------------------------------------------------------- 读数语义


def test_metrics_returns_copy_and_reset_clears():
    from plaita.event.redis import _record_event_index_pruned

    reset_event_index_pruned_metrics()
    _record_event_index_pruned("event_type", 3)
    snapshot = event_index_pruned_metrics()
    assert set(snapshot) == {
        "event_type",
        "sub_type",
        "sub_correlation",
        "sub_flow",
        "sub_node",
    }
    assert snapshot["event_type"] == 3

    snapshot["event_type"] = 999
    assert event_index_pruned_metrics()["event_type"] == 3, "返回的不是副本"

    reset_event_index_pruned_metrics()
    assert all(value == 0 for value in event_index_pruned_metrics().values())
