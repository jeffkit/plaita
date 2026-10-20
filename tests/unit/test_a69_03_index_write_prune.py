"""A69-03 §3.3-3（台账 r70 / PR2）：写侧节流兜底清理。

``store_event_sync`` 是 console-only / 无 worker 部署的唯一写者：janitor 不跑、
``list_events`` 也没人读时，过期索引成员只能靠写时兜底收敛。本组用例钉住
规格里的三条约束：

1. 每次写入按 type 桶的有界窗口（默认 8 成员）清理载荷缺失成员，存活成员
   与本次新写的成员都保留；窗口按 offset 轮转，多次写入能覆盖整桶；
2. ``PLAITA_DISABLE_EVENT_INDEX_PRUNE_ON_WRITE=1`` 时完全不碰索引（回到
   现状，供回滚/对照）；
3. 兜底清理自身失败（客户端能力异常等）不得影响事件落盘主路径，只留
   WARNING——清理是降载手段，不是写入契约。
"""
from __future__ import annotations

import logging
import os
import uuid

import fakeredis

from plaita.event import redis as er
from plaita.event.core import Event

EVENT_PREFIX = "plaita:event:"


def _client() -> fakeredis.FakeRedis:
    return fakeredis.FakeRedis(decode_responses=True)


def _event(event_type: str, timestamp: float = 100.0) -> Event:
    return Event(
        event_id=str(uuid.uuid4()),
        event_type=event_type,
        data={},
        timestamp=timestamp,
    )


def _reset_write_prune_state() -> None:
    er._event_index_write_prune_state.clear()


def test_store_event_sync_prunes_missing_members_and_keeps_live():
    _reset_write_prune_state()
    client = _client()
    type_key = er.event_type_index_key("probe.write_prune")

    # 一个存活成员（载荷仍存在）+ 两个孤儿（只有索引成员）。
    live = _event("probe.write_prune", timestamp=1.0)
    client.set(er.event_record_key(live.event_id), live.model_dump_json())
    client.zadd(type_key, {live.event_id: live.timestamp, "orphan-1": 2.0, "orphan-2": 3.0})

    fresh = _event("probe.write_prune", timestamp=4.0)
    er.store_event_sync(client, fresh, ttl=60)

    # 孤儿被 ZREM；存活成员与本次新写成员保留；记录键照常落盘。
    assert client.zrange(type_key, 0, -1) == [live.event_id, fresh.event_id]
    assert client.exists(er.event_record_key("orphan-1")) == 0
    assert client.exists(er.event_record_key("orphan-2")) == 0
    assert client.exists(er.event_record_key(fresh.event_id)) == 1
    assert client.ttl(er.event_record_key(fresh.event_id)) > 0


def test_write_prune_window_is_bounded_and_rotates(monkeypatch):
    """单次最多检查 8 个成员；offset 轮转让后续写入覆盖桶尾。"""
    _reset_write_prune_state()
    monkeypatch.setattr(er, "_EVENT_INDEX_PRUNE_ON_WRITE_INTERVAL_SECONDS", 0.0)
    client = _client()
    type_key = er.event_type_index_key("probe.write_prune_window")
    orphans = {f"orphan-{i}": float(i) for i in range(1, 13)}
    client.zadd(type_key, orphans)

    first = _event("probe.write_prune_window", timestamp=100.0)
    er.store_event_sync(client, first, ttl=60)
    # 第一次窗口：score 最小的 8 个孤儿；剩余 4 个孤儿 + 新成员。
    assert client.zcard(type_key) == 5
    assert client.zscore(type_key, "orphan-9") is not None
    assert client.zscore(type_key, "orphan-8") is None

    second = _event("probe.write_prune_window", timestamp=101.0)
    er.store_event_sync(client, second, ttl=60)
    # 第二次窗口：桶已缩小，offset 越过桶尾后回到起点，清掉最后 4 个。
    assert client.zrange(type_key, 0, -1) == [first.event_id, second.event_id]


def test_write_prune_disabled_by_env_keeps_members(monkeypatch):
    _reset_write_prune_state()
    monkeypatch.setenv(er.EVENT_INDEX_PRUNE_ON_WRITE_DISABLE_ENV, "1")
    client = _client()
    type_key = er.event_type_index_key("probe.write_prune_disabled")
    client.zadd(type_key, {"orphan-disabled": 1.0})

    fresh = _event("probe.write_prune_disabled", timestamp=2.0)
    er.store_event_sync(client, fresh, ttl=60)

    assert client.zscore(type_key, "orphan-disabled") is not None
    assert client.zscore(type_key, fresh.event_id) is not None
    assert client.exists(er.event_record_key(fresh.event_id)) == 1


def test_write_prune_failure_does_not_break_store(caplog):
    """兜底清理抛异常时，事件记录/索引写入仍然成功且只留 WARNING。"""
    _reset_write_prune_state()

    class _ZcardBoom(fakeredis.FakeRedis):
        def zcard(self, *args, **kwargs):
            raise RuntimeError("zcard unavailable")

    client = _ZcardBoom(decode_responses=True)
    fresh = _event("probe.write_prune_failure", timestamp=1.0)
    with caplog.at_level(logging.WARNING, logger="plaita.event.redis"):
        er.store_event_sync(client, fresh, ttl=60)

    assert client.exists(er.event_record_key(fresh.event_id)) == 1
    assert client.zscore(er.event_type_index_key("probe.write_prune_failure"), fresh.event_id) is not None
    assert any(
        "写侧兜底清理失败" in record.getMessage()
        for record in caplog.records
    )
