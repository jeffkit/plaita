"""plaita#79 回归：``RedisEventStorage.list_events`` 的返回完整性。

生产建连方式（``aioredis.from_url`` 未传 ``decode_responses``，redis-py 默认
``False``）下 ``zrangebyscore`` 的 member 是 ``bytes``，f-string 拼出的键变成
``plaita:event:events:b'<uuid>'``——与 ``store_event`` 写入的 str 键不符，
``mget`` 全部 miss ⇒ ``list_events`` 恒返回 ``[]``。它是 ``EventReconciler``
（事件→resume 兜底回扫）的唯一读入口，恒空即整条补偿链为死代码。

即便修好解码，无类型过滤分支仍被 ``type_keys[:10]`` 截断：事件类型数 > 10 时
第 11 个类型起整类事件不进结果集；``sub_limit = limit // len(type_keys)`` 又按
「全部类型数」当分母，配额进一步缩水。

本文件断言三件事（均对应 issue 验收条款）：
- 生产建连显式 ``decode_responses=True``，且外部注入的 bytes 客户端也能读回；
- 类型数 > 10 时全部类型可见（``limit`` 足够时），且 ``limit`` 用到刀刃上；
- 无类型列表返回窗口内**完整**且**全局按时间有序**的前 ``limit`` 条——这是
  回扫游标「推进到本批最晚时间戳」语义（plaita#85）的前提。
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis
import fakeredis.aioredis

from plaita.event.core import Event
from plaita.event.redis import RedisEventStorage

BASE_TS = 1_700_000_000.0


def run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _storage(decode_responses: bool) -> RedisEventStorage:
    server = fakeredis.FakeServer()
    storage = RedisEventStorage(redis_url="redis://localhost:6379/15", ttl=3600)
    storage.redis = fakeredis.aioredis.FakeRedis(
        server=server, decode_responses=decode_responses
    )
    return storage


async def _seed(storage: RedisEventStorage, pairs):
    events = []
    for event_type, ts in pairs:
        event = Event(
            event_type=event_type,
            data={},
            event_id=f"{event_type}:{ts}",
            timestamp=ts,
        )
        await storage.store_event(event)
        events.append(event)
    return events


def test_bytes_client_returns_stored_event():
    """(a) ``decode_responses=False`` 的客户端：已存事件必须真被读回。

    修复前：bytes member 拼出的键与写入键不符，``list_events`` 恒返回 ``[]``
    （无类型与单类型两条分支都空）。
    """

    async def _t():
        storage = _storage(False)
        events = await _seed(storage, [("approval", BASE_TS + 1)])

        # 先证伪夹具前提：底层客户端确实是 bytes 模式，否则本用例测的不是
        # bytes 拼键路径（"证明这道守卫能失败"）。
        raw = await storage.redis.zrange(
            f"{storage.key_prefix}types:approval", 0, -1
        )
        assert raw and isinstance(raw[0], bytes), (
            f"夹具不是 bytes 模式（{type(raw[0]).__name__}），bytes 拼键路径未被覆盖"
        )

        got = await storage.list_events(
            start_time=BASE_TS, end_time=BASE_TS + 10, limit=10
        )
        assert [e.event_id for e in got] == [events[0].event_id], (
            f"bytes 客户端下 list_events(无类型) 未读回已存事件: {got!r}"
        )

        got_typed = await storage.list_events(event_type="approval", limit=10)
        assert [e.event_id for e in got_typed] == [events[0].event_id], (
            f"bytes 客户端下 list_events(event_type=...) 未读回已存事件: {got_typed!r}"
        )

    run(_t())


def test_initialize_builds_client_with_decode_responses():
    """自建生产客户端必须开 ``decode_responses``：治本一半（外部注入的
    bytes 客户端由 ``_as_str`` 兜底，治标一半，两者都要）。"""
    from unittest.mock import MagicMock

    async def _t():
        storage = RedisEventStorage(redis_url="redis://localhost:6379/15", ttl=3600)
        assert storage.redis is None
        fake_client = MagicMock()
        with patch(
            "plaita.event.redis.aioredis.from_url",
            new=AsyncMock(return_value=fake_client),
        ) as from_url:
            await storage.initialize()
        assert from_url.call_args.args[0] == "redis://localhost:6379/15"
        assert from_url.call_args.kwargs.get("decode_responses") is True, (
            f"生产建连未开 decode_responses: {from_url.call_args!r}"
        )

    run(_t())


def test_more_than_ten_event_types_all_returned():
    """(b) 12 个事件类型各 1 条：无类型列表必须返回全部 12 条。

    修复前（decode 已修）仍只返回前 10 个类型，``t10``/``t11`` 整类丢失。
    """

    async def _t():
        storage = _storage(True)
        pairs = [(f"t{i:02d}", BASE_TS + i) for i in range(12)]
        await _seed(storage, pairs)

        got = await storage.list_events(limit=200)
        assert len(got) == 12, f"类型数 12、limit 200 只返回 {len(got)} 条（前 10 类型截断）"
        assert {e.event_type for e in got} == {f"t{i:02d}" for i in range(12)}, (
            f"整类事件被跳过: {sorted({e.event_type for e in got})}"
        )

    run(_t())


def test_eleven_types_with_limit_eleven_all_returned():
    """11 个类型、``limit=11``：配额必须用到刀刃上（修复前 sub_limit 分母含
    未遍历类型，只返回 10 条）。"""

    async def _t():
        storage = _storage(True)
        await _seed(storage, [(f"t{i:02d}", BASE_TS + i) for i in range(11)])

        got = await storage.list_events(limit=11)
        assert len(got) == 11, f"limit=11 只返回 {len(got)} 条（分母含未遍历类型）"
        assert len({e.event_type for e in got}) == 11

    run(_t())


def test_no_type_listing_is_globally_time_ordered():
    """无类型列表必须全局按时间有序——回扫游标依赖这个顺序。"""

    async def _t():
        storage = _storage(True)
        # 同一类型的时间戳交错：按类型分块遍历会得到 [1,3,2,4]
        await _seed(
            storage,
            [("a", BASE_TS + 1), ("a", BASE_TS + 3),
             ("b", BASE_TS + 2), ("b", BASE_TS + 4)],
        )

        got = await storage.list_events(start_time=BASE_TS, limit=10)
        seq = [round(e.timestamp - BASE_TS, 3) for e in got]
        assert seq == [1.0, 2.0, 3.0, 4.0], f"未全局按时间归并: {seq}"
        assert [e.timestamp for e in got] == sorted(e.timestamp for e in got)

    run(_t())


def test_limit_selects_globally_earliest_events():
    """``limit`` 必须命中全局最早的事件，而不是「每类型分摊配额」的结果。"""

    async def _t():
        storage = _storage(True)
        await _seed(
            storage,
            [("a", BASE_TS + 1), ("a", BASE_TS + 2),
             ("b", BASE_TS + 3), ("b", BASE_TS + 4)],
        )

        got = await storage.list_events(start_time=BASE_TS, limit=2)
        seq = [round(e.timestamp - BASE_TS, 3) for e in got]
        assert seq == [1.0, 2.0], f"limit=2 未取全局最早两条: {seq}"

    run(_t())
