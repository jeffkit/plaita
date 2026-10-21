"""第七十二轮任务②：触发拒绝零痕（B44-01，duo-a2 台账第四十四轮）。

## 缺陷

租户停用（plaita#27 闸）与 start 配额满（plaita#28 闸）的触发拒绝在
fire_record 写入**之前** return——console 触发历史对「到期被拒」零痕迹，
与「未到期」不可区分；调度面表现为静默停走（停用租户/配额长期打满时，
调度到底还活没活着无从判断）。

## 修复契约

- 拒绝也落 fire_record：``enqueue="rejected"`` + ``rejection_reason``
  （``tenant_disabled`` / ``tenant_quota_exhausted``），fired_at 如实记
  拒绝时刻；
- 拒绝路径**不推进** next_run_at / last_fired_at / last_enqueue_ok（既有
  语义：rejected-but-due 每 30s 锁窗重试，恢复后同一 due 正常受理）；
- rejected 记录不带 execution_id（预铸 id 未落任何行/消息，映射已回滚——
  留 id 只会链到 404；诊断值在原因，不在 id）；
- 恢复后正常记录与拒绝记录**并存**（历史即调度面健康证据）。

负控（先红后绿）：停用租户/配额满触发——pre-fix 历史零新增（红）；
post-fix rejected+原因可见，恢复后 ok 记录并存。
"""
import json

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis

from plaita.server.services.schedule_service import (
    SCHEDULES_KEY,
    SCHEDULE_FIRES_KEY,
    fire_schedule,
)
from plaita.server.tenant_quota import (
    publish_tenant_quota,
    release_tenant_start,
    try_admit_tenant_start,
)
from plaita.server.tenant_status import publish_tenant_status

QUEUE = "plaita:flow:queue"
DUE_MS = 1791530071848


def _schedule(**overrides):
    base = {
        "schedule_id": "s1",
        "tenant_id": "default",
        "name": "n",
        "flow_id": "f",
        "version": "7",
        "cron": "* * * * *",
        "params": {"x": 1},
        "enabled": True,
        "created_at": "t0",
        "updated_at": "t0",
        "next_run_at": str(DUE_MS),
    }
    base.update(overrides)
    return base


def _records(redis, schedule_id="s1"):
    raw = redis.lrange(SCHEDULE_FIRES_KEY.format(schedule_id=schedule_id), 0, -1)
    return [json.loads(item) for item in raw]


def _stream_start_count(redis):
    return sum(
        1
        for entry in redis.xrange(QUEUE)
        if json.loads(entry[1]["payload"]).get("type") == "start"
    )


class TestDisabledTenantRejectionTrace:
    def test_rejected_with_reason_and_no_enqueue(self):
        """负控：停用租户触发——历史记 rejected+原因，不入队不推进状态。

        pre-fix：历史零新增（首个断言红）；post-fix：rejected 可见。
        """
        fake = fakeredis.FakeRedis(decode_responses=True)
        fake.hset(SCHEDULES_KEY, "s1", json.dumps(_schedule()))
        publish_tenant_status(fake, "default", "disabled")

        ret = fire_schedule(fake, _schedule(), QUEUE, trigger_kind="cron", due_at=DUE_MS)

        assert ret is None
        records = _records(fake)
        assert len(records) == 1, "pre-fix 这里是 0——拒绝零痕（静默停走）"
        record = records[0]
        assert record["enqueue"] == "rejected"
        assert record["rejection_reason"] == "tenant_disabled"
        assert record["fired_at"], "拒绝时刻必须如实记录"
        assert record["trigger_kind"] == "cron"
        assert not record.get("execution_id"), (
            "rejected 不带 execution_id（未落任何行/消息，留 id 只会链到 404）"
        )
        assert _stream_start_count(fake) == 0

    def test_rejection_does_not_advance_schedule_state(self):
        """拒绝不推进 next_run_at/last_fired_at：恢复后同一 due 立即正常受理。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        fake.hset(SCHEDULES_KEY, "s1", json.dumps(_schedule()))
        publish_tenant_status(fake, "default", "disabled")
        fire_schedule(fake, _schedule(), QUEUE, trigger_kind="cron", due_at=DUE_MS)

        current = json.loads(fake.hget(SCHEDULES_KEY, "s1"))
        assert current["next_run_at"] == str(DUE_MS), (
            "被拒触发不得推进 next_run_at（恢复后同一 due 重试既有语义）"
        )
        assert "last_fired_at" not in current
        assert "last_enqueue_ok" not in current

    def test_ok_and_rejected_records_coexist_after_recovery(self):
        """恢复后正常记录与拒绝记录并存——历史即调度面健康证据。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        fake.hset(SCHEDULES_KEY, "s1", json.dumps(_schedule()))
        publish_tenant_status(fake, "default", "disabled")
        fire_schedule(fake, _schedule(), QUEUE, trigger_kind="cron", due_at=DUE_MS)

        publish_tenant_status(fake, "default", "active")
        msg_id = fire_schedule(
            fake, _schedule(), QUEUE, trigger_kind="cron", due_at=DUE_MS
        )
        assert msg_id is not None

        records = _records(fake)
        assert [r["enqueue"] for r in records] == ["ok", "rejected"]
        assert records[1]["rejection_reason"] == "tenant_disabled"
        assert records[0].get("execution_id"), "恢复后的受理照常记 execution_id"


class TestQuotaExhaustedRejectionTrace:
    def test_rejected_with_reason_and_claims_rolled_back(self):
        """负控：配额满触发——历史记 rejected+原因，幂等映射回滚、不入队。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        fake.hset(SCHEDULES_KEY, "s1", json.dumps(_schedule()))
        publish_tenant_quota(fake, "default", 1)
        assert try_admit_tenant_start(fake, "default", "occupant-1"), "先占满唯一名额"

        ret = fire_schedule(fake, _schedule(), QUEUE, trigger_kind="cron", due_at=DUE_MS)

        assert ret is None
        records = _records(fake)
        assert len(records) == 1, "pre-fix 这里是 0——配额拒绝零痕"
        record = records[0]
        assert record["enqueue"] == "rejected"
        assert record["rejection_reason"] == "tenant_quota_exhausted"
        assert not record.get("execution_id")
        assert _stream_start_count(fake) == 0
        from plaita.server.task_queue import start_dedup_map_key
        from plaita.server.tenant_context import tenant_namespace

        mapping = start_dedup_map_key(
            tenant_namespace("default"), f"sched:s1:due:{DUE_MS}"
        )
        assert fake.get(mapping) is None, "被拒触发的幂等预认领必须回滚"

    def test_ok_after_quota_freed_coexists_with_rejection(self):
        """配额释放后同一 due 正常受理，ok 与 rejected 并存。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        fake.hset(SCHEDULES_KEY, "s1", json.dumps(_schedule()))
        publish_tenant_quota(fake, "default", 1)
        try_admit_tenant_start(fake, "default", "occupant-1")
        fire_schedule(fake, _schedule(), QUEUE, trigger_kind="cron", due_at=DUE_MS)

        release_tenant_start(fake, "default", "occupant-1")
        msg_id = fire_schedule(
            fake, _schedule(), QUEUE, trigger_kind="cron", due_at=DUE_MS
        )
        assert msg_id is not None

        records = _records(fake)
        assert [r["enqueue"] for r in records] == ["ok", "rejected"]
        assert records[1]["rejection_reason"] == "tenant_quota_exhausted"
