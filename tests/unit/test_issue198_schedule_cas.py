"""plaita#198：fire_schedule 与 console PUT 不得用旧快照整份回写调度定义。

历史缺陷：
- ``fire_schedule`` 用调用方传入的扫描快照 ``_merge(schedule, updates)`` 后
  整份 HSET——期间 console PUT 停用/编辑会被旧快照翻回去；
- console PUT 读-改-写整份 JSON，无乐观锁，并发编辑静默覆盖。

契约：
- 触发回写用 WATCH/MULTI **合并到当前值**，只碰
  ``last_fired_at`` / ``last_enqueue_ok`` / ``next_run_at``；``enabled`` /
  ``cron`` / ``params`` / ``version`` 不被旧快照覆盖；
- console PUT 以读取时的 ``updated_at`` 为乐观锁基准，冲突抛
  ``ScheduleConflictError``（API 层转结构化 409）；
- #118 的 dedup_key / cron 到期语义不变。
"""
from __future__ import annotations

import json
import time

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis

from plaita.server.services.schedule_service import (
    SCHEDULES_KEY,
    ScheduleConflictError,
    cas_update_schedule,
    fire_schedule,
)

QUEUE = "plaita:flow:queue"


def _stale(**overrides):
    base = {
        "schedule_id": "s1",
        "tenant_id": "default",
        "name": "n",
        "flow_id": "f",
        "version": None,
        "cron": "* * * * *",
        "params": {"x": 1},
        "enabled": True,
        "created_at": "t0",
        "updated_at": "t0",
        "next_run_at": "0",
    }
    base.update(overrides)
    return base


def _get(redis):
    return json.loads(redis.hget(SCHEDULES_KEY, "s1"))


def _put(redis, record):
    redis.hset(SCHEDULES_KEY, "s1", json.dumps(record, ensure_ascii=False))


def test_interleaved_disable_is_not_flipped_back():
    """验收 #2：读旧快照 → PUT 停用 → fire 回写 → 最终 enabled=false。"""
    redis = fakeredis.FakeRedis(decode_responses=True)
    stale = _stale()
    _put(redis, stale)

    current = _get(redis)
    current["enabled"] = False
    current["updated_at"] = "t1"
    _put(redis, current)
    assert _get(redis)["enabled"] is False

    fire_schedule(redis, stale, QUEUE, trigger_kind="cron", due_at=0)

    final = _get(redis)
    assert final["enabled"] is False, "fire 用旧快照把停用翻回了 true（#198）"
    assert final["last_fired_at"], "触发状态未回写"
    assert final["last_enqueue_ok"] == "1"
    assert final["cron"] == "* * * * *"
    assert final["params"] == {"x": 1}


def test_fire_recomputes_next_run_from_current_cron():
    """并发改 cron 后，fire 的 next_run_at 必须按**当前** cron 重算。"""
    redis = fakeredis.FakeRedis(decode_responses=True)
    stale = _stale(cron="* * * * *")
    _put(redis, stale)

    current = _get(redis)
    current["cron"] = "0 0 1 1 *"  # 每年 1 月 1 日
    current["updated_at"] = "t1"
    _put(redis, current)

    fire_schedule(redis, stale, QUEUE, trigger_kind="cron", due_at=0)

    final = _get(redis)
    assert final["cron"] == "0 0 1 1 *", "并发编辑的 cron 被旧快照覆盖"
    # 当前 cron 的下一次触发在明年；旧 cron 的下一次在一分钟内。
    assert int(final["next_run_at"]) > int(time.time() * 1000) + 3600_000, (
        "next_run_at 仍按旧 cron 计算（应使用当前 cron）"
    )


def test_fire_preserves_version_and_params_edited_concurrently():
    redis = fakeredis.FakeRedis(decode_responses=True)
    stale = _stale(version="v1", params={"old": True})
    _put(redis, stale)
    current = _get(redis)
    current.update({"version": "v2", "params": {"new": True}, "updated_at": "t1"})
    _put(redis, current)

    fire_schedule(redis, stale, QUEUE, trigger_kind="cron", due_at=0)

    final = _get(redis)
    assert final["version"] == "v2"
    assert final["params"] == {"new": True}


def test_manual_fire_does_not_touch_next_run_at_or_enabled():
    redis = fakeredis.FakeRedis(decode_responses=True)
    stale = _stale(next_run_at="123", enabled=True)
    _put(redis, stale)
    current = _get(redis)
    current["enabled"] = False
    current["updated_at"] = "t1"
    _put(redis, current)

    fire_schedule(redis, stale, QUEUE, trigger_kind="manual")

    final = _get(redis)
    assert final["enabled"] is False
    assert final["next_run_at"] == "123", "manual 触发不应改写 next_run_at"
    assert final["last_fired_at"]


def test_cas_conflict_reports_latest_and_does_not_write():
    redis = fakeredis.FakeRedis(decode_responses=True)
    _put(redis, _stale(updated_at="t2"))

    with pytest.raises(ScheduleConflictError) as excinfo:
        cas_update_schedule(
            redis, "s1", lambda cur: cur.update({"enabled": False}),
            expected_updated_at="t1", check_updated_at=True,
        )
    assert excinfo.value.latest_updated_at == "t2"
    assert _get(redis)["enabled"] is True, "冲突时不得写入"


def test_fire_after_delete_does_not_recreate():
    redis = fakeredis.FakeRedis(decode_responses=True)
    stale = _stale()
    _put(redis, stale)
    redis.hdel(SCHEDULES_KEY, "s1")

    fire_schedule(redis, stale, QUEUE, trigger_kind="cron", due_at=0)

    assert redis.hget(SCHEDULES_KEY, "s1") is None, "fire 不应把已删除的调度写回"
