"""A69-03（台账 r70 / PR2+PR3）：事件/订阅索引的续期 + 惰性清理 + 有界 janitor。

背景：事件类型索引（ZSET）与订阅四索引（SET）每次写入都续期整键 TTL，但
成员不随数据键过期：活跃流量下桶永不整体过期，``zcard``/``scard`` 持续高于
live 数；``list_events``/``list_subscriptions`` 的 N+1 与「载荷缺失」ERROR
随 churn 放大。只停续期会把仍存活成员的桶一起放死，所以方案是
**继续续期 + 读时惰性清理 + EventFilter 挂载的有界 janitor**。

本组用例对应 duo-a-ledger r70 §3.5 的负控表：

1. ``list_events`` 读后把载荷缺失成员 ZREM（热桶立即收敛）；
2. ``list_subscriptions`` 读后 SREM 选中桶里的缺失成员；
3. janitor 清理冷类型桶（惰性清理触达不到）；
4. janitor 覆盖订阅 ``flow:``/``node:`` 冷桶；
5. budget 是「检查成员数」，单 tick 有界、多 tick 收敛；
6. 数据仍存在（哪怕 TTL 将到期）的成员保留；
7. 时间戳很老但载荷仍活的事件保留（不按 score 误删）；
8. console 删除订阅补齐 ``node:`` 索引；
9. data 已消失时 ``delete_subscription`` 仍返回 False，但 janitor 清索引；
10. 单桶 WRONGTYPE 只 WARNING，其余桶照常清理、下一 tick 继续；
11. 全流程只用 SCAN/ZSCAN/SSCAN（#202 有界扫描政策），不调用 KEYS。
"""
from __future__ import annotations

import asyncio
import json
import logging
import sys
import time
from pathlib import Path
from typing import List

import fakeredis
import fakeredis.aioredis
from fakeredis import FakeRedis
from fastapi import Depends, FastAPI, Request
from starlette.testclient import TestClient

from plaita.event.core import Event, EventSubscription
from plaita.event.index_janitor import IndexJanitor
from plaita.event.redis import RedisEventStorage, RedisEventSubscriptionStorage

EVENT_PREFIX = "plaita:event:"
SUB_PREFIX = "plaita:subscription:"
BASE_TS = 1_700_000_000.0
TENANT = "acme"

BACKEND_DIR = Path(__file__).resolve().parents[2] / "plaita-console" / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))


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


# ---------------------------------------------------------------- 1


def test_event_read_prunes_expired_members():
    server = fakeredis.FakeServer()

    async def _t():
        storage = _event_storage(server)
        for i in range(3):
            await storage.store_event(
                Event(
                    event_type="approval",
                    data={},
                    event_id=f"live-{i}",
                    timestamp=BASE_TS + i,
                )
            )
        type_key = f"{EVENT_PREFIX}types:approval"
        await storage.redis.zadd(
            type_key, {f"dead-{i}": BASE_TS + 10 + i for i in range(3)}
        )
        before = await storage.redis.zcard(type_key)
        got = await storage.list_events(event_type="approval", limit=10)
        after = await storage.redis.zcard(type_key)
        return before, len(got), after

    before, returned, after = _run(_t())
    assert before == 6
    assert returned == 3, "缺失载荷的成员不得计入返回"
    assert after == 3, f"读时惰性清理未收敛（zcard={after}，期望 3）"


# ---------------------------------------------------------------- 2


def test_subscription_read_prunes_selected_bucket():
    server = fakeredis.FakeServer()

    async def _t():
        storage = _sub_storage(server)
        for i in range(3):
            await storage.store_subscription(
                EventSubscription(event_type="approval", correlation_id=f"c-{i}")
            )
        type_key = f"{SUB_PREFIX}type:approval"
        await storage.redis.sadd(type_key, "ghost-0", "ghost-1", "ghost-2")
        before = await storage.redis.scard(type_key)
        got = await storage.list_subscriptions(event_type="approval")
        after = await storage.redis.scard(type_key)
        return before, len(got), after

    before, returned, after = _run(_t())
    assert before == 6
    assert returned == 3
    assert after == 3, f"选中桶的缺失成员未 SREM（scard={after}）"


# ---------------------------------------------------------------- 3


def test_event_janitor_prunes_cold_type_bucket():
    server = fakeredis.FakeServer()

    async def _t():
        storage = _event_storage(server)
        cold_key = f"{EVENT_PREFIX}types:cold"
        await storage.redis.zadd(
            cold_key, {f"cold-{i}": BASE_TS + i for i in range(3)}
        )
        janitor = IndexJanitor(event_storage=storage, budget=10)
        examined, pruned, errors = await janitor.sweep_once()
        return (
            await storage.redis.zcard(cold_key),
            examined,
            pruned,
            errors,
        )

    remaining, examined, pruned, errors = _run(_t())
    assert remaining == 0, "冷类型桶的孤儿成员未被 janitor 清理"
    assert examined == 3 and pruned == 3
    assert errors == 0


# ---------------------------------------------------------------- 4


def test_subscription_janitor_prunes_flow_node_buckets():
    server = fakeredis.FakeServer()

    async def _t():
        storage = _sub_storage(server)
        flow_key = f"{SUB_PREFIX}flow:flow-1"
        node_key = f"{SUB_PREFIX}node:node-1"
        await storage.redis.sadd(flow_key, "ghost-a", "ghost-b")
        await storage.redis.sadd(node_key, "ghost-c")
        janitor = IndexJanitor(subscription_storage=storage, budget=10)
        examined, pruned, errors = await janitor.sweep_once()
        return (
            await storage.redis.scard(flow_key),
            await storage.redis.scard(node_key),
            examined,
            pruned,
            errors,
        )

    flow_left, node_left, examined, pruned, errors = _run(_t())
    assert flow_left == 0 and node_left == 0, "冷桶 flow:/node: 未被 janitor 覆盖"
    assert examined == 3 and pruned == 3
    assert errors == 0


# ---------------------------------------------------------------- 4b


def test_event_filter_mounts_and_stops_index_janitor(monkeypatch):
    """EventFilter.start/stop 与 janitor 同生命周期（不依赖回扫器）。"""
    monkeypatch.setenv("PLAITA_DISABLE_EVENT_RECONCILE", "1")

    from plaita.event.memory import InMemoryEventBus
    from plaita.server.event_filter import EventFilter
    from plaita.storage.memory import MemoryExecutionStorage

    server = fakeredis.FakeServer()
    sync = FakeRedis(server=server, decode_responses=True)

    async def _t():
        storage = _event_storage(server)
        event_filter = EventFilter(
            execution_storage=MemoryExecutionStorage(),
            subscription_storage=_sub_storage(server),
            redis_client=sync,
            event_bus=InMemoryEventBus(),
            queue_name="plaita:a69-03:queue",
            enable_subscription_timeout_checker=False,
            event_storage=storage,
        )
        task = asyncio.create_task(event_filter.start())
        for _ in range(100):
            await asyncio.sleep(0.01)
            if event_filter._index_janitor is not None:
                break
        mounted = (
            event_filter._index_janitor is not None
            and event_filter._index_janitor._running
        )
        await event_filter.stop()
        try:
            await asyncio.wait_for(task, timeout=2.0)
        except asyncio.TimeoutError:  # pragma: no cover — start 未退出即失败
            task.cancel()
        return mounted, event_filter._index_janitor

    mounted, janitor_after_stop = _run(_t())
    assert mounted, "EventFilter.start 未挂载 IndexJanitor"
    assert janitor_after_stop is None, "EventFilter.stop 未停止/清理 IndexJanitor"


# ---------------------------------------------------------------- 5


def test_janitor_budget_bounds_examined():
    server = fakeredis.FakeServer()

    async def _t():
        storage = _event_storage(server)
        cold_key = f"{EVENT_PREFIX}types:cold"
        await storage.redis.zadd(
            cold_key, {f"cold-{i}": BASE_TS + i for i in range(5)}
        )
        janitor = IndexJanitor(event_storage=storage, budget=2)
        ticks: List = []
        for _ in range(10):
            ticks.append(await janitor.sweep_once())
            if await storage.redis.zcard(cold_key) == 0:
                break
        return ticks, await storage.redis.zcard(cold_key)

    ticks, remaining = _run(_t())
    assert all(examined <= 2 for examined, _p, _e in ticks), (
        f"单 tick 检查数超过 budget=2: {ticks}"
    )
    assert sum(examined for examined, _p, _e in ticks) >= 5, (
        f"多 tick 未覆盖全部 5 个成员: {ticks}"
    )
    assert remaining == 0, "多 tick 后未收敛到 0"


# ---------------------------------------------------------------- 6


def test_janitor_keeps_live_member():
    server = fakeredis.FakeServer()

    async def _t():
        storage = _event_storage(server, ttl=2)
        event = Event(
            event_type="approval", data={"ok": True}, event_id="live", timestamp=BASE_TS
        )
        await storage.store_event(event)
        janitor = IndexJanitor(event_storage=storage, budget=10)
        await janitor.sweep_once()
        return await storage.redis.zcard(f"{EVENT_PREFIX}types:approval")

    assert _run(_t()) == 1, "载荷仍存在的成员被误删（TTL 将到期也不例外）"


# ---------------------------------------------------------------- 7


def test_event_janitor_keeps_old_timestamp_live_event():
    server = fakeredis.FakeServer()

    async def _t():
        storage = _event_storage(server)
        old_ts = time.time() - 40 * 86400
        await storage.store_event(
            Event(
                event_type="approval",
                data={"old": True},
                event_id="old-but-live",
                timestamp=old_ts,
            )
        )
        janitor = IndexJanitor(event_storage=storage, budget=10)
        await janitor.sweep_once()
        return await storage.redis.zcard(f"{EVENT_PREFIX}types:approval")

    assert _run(_t()) == 1, "按 score 裁剪误删了「时间戳老但载荷仍活」的事件"


# ---------------------------------------------------------------- 8


def _tenant_client(redis_client, tenant: str = TENANT):
    from api import events as events_api

    app = FastAPI()
    app.state.redis = redis_client

    async def _auth(request: Request):
        request.state.tenant_id = tenant
        request.state.role = "editor"
        request.state.actor = f"editor@{tenant}"
        request.state.platform_admin = False
        return {"tenant_id": tenant}

    app.include_router(events_api.router, prefix="/api", dependencies=[Depends(_auth)])
    return TestClient(app)


def test_console_delete_removes_node_index():
    redis_client = FakeRedis(decode_responses=True)
    subscription_id = "sub-node"
    redis_client.set(
        f"{SUB_PREFIX}data:{subscription_id}",
        json.dumps({
            "subscription_id": subscription_id,
            "event_type": "approval",
            "correlation_id": "c-1",
            "flow_id": "f-1",
            "node_id": "node-9",
            "tenant_id": TENANT,
        }),
    )
    buckets = [
        f"{SUB_PREFIX}type:approval",
        f"{SUB_PREFIX}correlation:c-1",
        f"{SUB_PREFIX}flow:f-1",
        f"{SUB_PREFIX}node:node-9",
    ]
    for bucket in buckets:
        redis_client.sadd(bucket, subscription_id)

    client = _tenant_client(redis_client)
    r = client.delete(f"/api/events/subscriptions/{subscription_id}")
    assert r.status_code == 200, r.text
    for bucket in buckets:
        assert redis_client.sismember(bucket, subscription_id) == 0, (
            f"删除订阅后索引残留: {bucket}"
        )


# ---------------------------------------------------------------- 9


def test_delete_after_data_expiry_returns_false_but_janitor_cleans():
    server = fakeredis.FakeServer()

    async def _t():
        storage = _sub_storage(server)
        type_key = f"{SUB_PREFIX}type:approval"
        await storage.redis.sadd(type_key, "ghost-sub")
        deleted = await storage.delete_subscription("ghost-sub")
        before = await storage.redis.scard(type_key)
        janitor = IndexJanitor(subscription_storage=storage, budget=10)
        await janitor.sweep_once()
        return deleted, before, await storage.redis.scard(type_key)

    deleted, before, after = _run(_t())
    assert deleted is False, "data 缺失时 delete_subscription 必须仍返回 False"
    assert before == 1
    assert after == 0, "janitor 未清理 data 已过期的索引成员"


# ---------------------------------------------------------------- 10


def test_janitor_error_is_observable_and_loop_survives(caplog):
    server = fakeredis.FakeServer()

    async def _t():
        storage = _event_storage(server)
        await storage.redis.set(f"{EVENT_PREFIX}types:bad", "not-a-zset")
        cold_key = f"{EVENT_PREFIX}types:cold"
        await storage.redis.zadd(cold_key, {"ghost": BASE_TS})
        janitor = IndexJanitor(event_storage=storage, budget=10)
        return await janitor.sweep_once(), await storage.redis.zcard(cold_key)

    with caplog.at_level(logging.WARNING, logger="plaita.event.redis"):
        (examined, pruned, errors), cold_left = _run(_t())

    assert errors == 1, f"坏桶未计入 errors: {(examined, pruned, errors)}"
    assert cold_left == 0, "一个坏桶让其余桶停摆"
    assert any(
        "失败" in record.getMessage() and "types:bad" in record.getMessage()
        for record in caplog.records
    ), f"坏桶失败没有 WARNING: {[r.getMessage() for r in caplog.records]}"


# ---------------------------------------------------------------- 11


def test_no_keys_command():
    server = fakeredis.FakeServer()

    async def _t():
        event_storage = _event_storage(server)
        sub_storage = _sub_storage(server)
        await event_storage.store_event(
            Event(event_type="approval", data={}, event_id="e-1", timestamp=BASE_TS)
        )
        await sub_storage.store_subscription(
            EventSubscription(event_type="approval", correlation_id="c-1")
        )

        async def _forbidden(*args, **kwargs):
            raise AssertionError("清扫/读路径不得使用 KEYS 命令（#202 有界扫描政策）")

        event_storage.redis.keys = _forbidden
        sub_storage.redis.keys = _forbidden
        await event_storage.list_events(limit=10)
        await event_storage.prune_indexes(budget=10)
        await sub_storage.list_subscriptions(event_type="approval")
        await sub_storage.prune_indexes(budget=10)

    _run(_t())
