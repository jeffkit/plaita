"""plaita#196：reclaim 扫描窗口必须轮转，不能被活跃租约占死前 8 条。

基线缺陷：``_reclaim_one`` 每轮固定 ``xpending_range(count=8, min="-")``，
只扫 PEL **头 8 条**；前 8 条都是活跃租约（本就不该抢）时，``continue``
之后直接 ``return None``——第 9 条起的真孤儿永远不会被看到，XCLAIM 恢复
路径被固定窗口饿死。

修复契约：实例级游标从上一轮扫到的 id 继续，到尾部回绕；任意 pending
条目在有限轮内必被看到。活跃条目仍不 XCLAIM（#47 闸门语义不变）。
"""
from __future__ import annotations

import json

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis

from plaita.server.task_queue import PAYLOAD_FIELD, RedisStreamTaskQueue

STREAM = "plaita:flow:queue:t196"
GROUP = "g"


def _seed_pending(redis, count):
    """把 ``count`` 条消息投递给 dead consumer（不 ack）→ 全部进 PEL。"""
    writer = RedisStreamTaskQueue(
        redis, STREAM, group_name=GROUP, consumer_name="dead", claim_min_idle_ms=0
    )
    writer.ensure_group()
    for i in range(count):
        redis.xadd(STREAM, {PAYLOAD_FIELD: json.dumps({"type": "start", "execution_id": f"e{i}"})})
    redis.xreadgroup(GROUP, "dead", {STREAM: ">"}, count=count)
    assert int(redis.xpending(STREAM, GROUP)["pending"]) == count


def _reader(redis, active_before):
    """前 ``active_before`` 条租约活跃（guard True），其余视为真孤儿。"""
    return RedisStreamTaskQueue(
        redis,
        STREAM,
        group_name=GROUP,
        consumer_name="c1",
        claim_min_idle_ms=0,
        reclaim_lease_guard=(
            lambda body: int(str(body["execution_id"]).lstrip("e")) < active_before
        ),
    )


def _times_delivered(redis, mid):
    for entry in redis.xpending_range(STREAM, GROUP, min=mid, max=mid, count=1):
        return int(entry["times_delivered"])
    return 0


class TestReclaimRotation:
    def test_issue_repro_ninth_orphan_is_reclaimed(self):
        """正文复现：前 8 条活跃，第 9 条 e8 是唯一真孤儿。"""
        redis = fakeredis.FakeRedis(decode_responses=True)
        _seed_pending(redis, 9)
        q = _reader(redis, active_before=8)

        reclaimed = []
        for _round in range(3):  # 上界：3 轮内必须看到第 9 条
            task = q._reclaim_one()
            if task is not None:
                reclaimed.append(task.body["execution_id"])

        assert reclaimed == ["e8"], f"第 9 条孤儿未被回收：{reclaimed}"

    def test_twenty_pending_fifteen_active_five_orphans_all_reclaimed(self):
        """验收：20 条 pending、前 15 条活跃、后 5 条真孤儿，有限轮内全部回收。"""
        redis = fakeredis.FakeRedis(decode_responses=True)
        _seed_pending(redis, 20)
        q = _reader(redis, active_before=15)

        reclaimed = []
        rounds = 0
        for rounds in range(1, 11):  # 上界 10 轮（窗口 8 + 回绕）
            task = q._reclaim_one()
            if task is not None:
                reclaimed.append(task.body["execution_id"])
            if len(reclaimed) == 5:
                break

        assert sorted(reclaimed) == [f"e{i}" for i in range(15, 20)], (
            f"孤儿未被全部回收：{reclaimed}（rounds={rounds}）"
        )

    def test_active_entries_are_never_xclaimed(self):
        """#47 闸门语义不变：租约活跃条目的 times_delivered 不得增长。"""
        redis = fakeredis.FakeRedis(decode_responses=True)
        _seed_pending(redis, 20)
        q = _reader(redis, active_before=15)

        mids = [entry["message_id"] for entry in redis.xpending_range(
            STREAM, GROUP, min="-", max="+", count=20)]
        active = mids[:15]

        for _ in range(10):
            q._reclaim_one()

        for mid in active:
            assert _times_delivered(redis, mid) == 1, f"活跃条目 {mid} 被 XCLAIM 了"

    def test_single_active_pending_stays_untouched_across_rounds(self):
        """只有一条 pending 且租约活跃：游标回绕也不得把它再抢一遍。"""
        redis = fakeredis.FakeRedis(decode_responses=True)
        _seed_pending(redis, 1)
        q = _reader(redis, active_before=1)

        for _ in range(10):
            assert q._reclaim_one() is None

        mid = redis.xpending_range(STREAM, GROUP, min="-", max="+", count=1)[0]["message_id"]
        assert _times_delivered(redis, mid) == 1
        assert redis.xlen(q.dlq_key) == 0

    def test_reclaim_window_is_configurable(self):
        """窗口大小可配置（默认 8）；窗口=2 时游标仍按该窗口轮转。"""
        redis = fakeredis.FakeRedis(decode_responses=True)
        _seed_pending(redis, 6)
        q = RedisStreamTaskQueue(
            redis,
            STREAM,
            group_name=GROUP,
            consumer_name="c1",
            claim_min_idle_ms=0,
            reclaim_window=2,
            reclaim_lease_guard=(
                lambda body: int(str(body["execution_id"]).lstrip("e")) < 3
            ),
        )
        assert q.reclaim_window == 2

        reclaimed = []
        for _ in range(10):
            task = q._reclaim_one()
            if task is not None:
                reclaimed.append(task.body["execution_id"])
            if len(reclaimed) == 3:
                break
        assert sorted(reclaimed) == ["e3", "e4", "e5"]
