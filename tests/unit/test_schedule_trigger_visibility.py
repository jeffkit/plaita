"""第七十一轮任务②：调度触发的受理窗口可见性（A43-04）。

## 缺陷（duo-a2 台账第四十三轮 A43-04，a/P3）

``fire_schedule``（cron 与 console「立即触发」manual 共用的唯一触发路径）
预铸 execution_id + 入口占租户 start 配额后**直接 enqueue**——无占位状态行、
fire_record 不记 execution_id。#273 修复后 POST /executions 的「已受理未接
管」窗口可查可取消（queued 占位行 + CANCEL 直取消），而调度触发的同一窗口
**凭任何返回值/历史行都定位不到 execution_id**：worker 停机/积压期，GET/
列表/CANCEL 全不可达、配额已占——可见性双标。

## 修复契约（复用 #273/#274 受理语义，消息形状零变化）

- **幂等预认领**（#274 同款 worker 同源键）：入队前以
  ``{ns}:start-dedup:{dedup_key}`` SET NX 认领；命中 ⇒ 同键已受理，收敛到
  既有 execution_id——不再入队、不重复占配额（否则第二条消息被 worker dedup
  吞掉后，其占位行/配额成幽灵——预认领是占位行的**前置正确性条件**，不是
  附加装饰）；
- **queued 占位行**（#273 同款）：入队前经 ``RedisExecutionStorage`` 写路径
  同步落 ``status=queued``、空 context、``invoker="schedule"`` 的状态行（索引
  同步维护）；worker 消费时以 running 行覆盖接管（``_existing_start_checkpoint``
  按首投处理，不会误转 resume）；
- **失败逐级回滚**（具名 ERROR，不带病入队）：占位行落盘失败 ⇒ 不入队 +
  释放配额 + 释放映射，返回 None；入队失败 ⇒ 清占位行 + 释放配额 + 释放映射；
- **fire_record 记 execution_id**：console history 端点的
  ``ScheduleFireRecord.execution_id`` 字段已就绪，集群档记录补上即透出。

负控（先红后绿）：worker 停机下触发调度——pre-fix 无占位行、fire_record
无 execution_id（无从定位）；post-fix 占位行 queued 可查、fire_record 可定
位、取消语义可达（console 端到端见
``plaita-console/backend/tests/console/test_schedule_trigger_visibility.py``）。
"""
from __future__ import annotations

import json

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis

from plaita.server.services.schedule_service import (
    SCHEDULE_FIRES_KEY,
    fire_schedule,
)
from plaita.server.task_queue import start_dedup_map_key
from plaita.server.tenant_context import tenant_namespace
from plaita.storage.redis import RedisExecutionStorage

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
        "next_run_at": "0",
    }
    base.update(overrides)
    return base


def _stream_start_messages(redis):
    out = []
    for entry in redis.xrange(QUEUE):
        payload = json.loads(entry[1]["payload"])
        if payload.get("type") == "start":
            out.append(payload)
    return out


def _fire_record(redis, schedule_id="s1"):
    raw = redis.lrange(SCHEDULE_FIRES_KEY.format(schedule_id=schedule_id), 0, 0)
    if not raw:
        return None
    return json.loads(raw[0])


def _load_row(redis, execution_id):
    return RedisExecutionStorage(
        client=redis, namespace=tenant_namespace("default")
    ).load_execution_state(execution_id)


class TestScheduleTriggerVisibility:
    def test_fire_writes_queued_row_and_records_execution_id(self):
        """核心负控：worker 停机下触发调度——占位行可查、fire_record 可定位。

        pre-fix：无占位行（load 返回 None）且 fire_record 无 execution_id
        字段——双断言皆红；post-fix：queued 行 + 记录带同一 execution_id。
        """
        fake = fakeredis.FakeRedis(decode_responses=True)
        msg_id = fire_schedule(
            fake, _schedule(), QUEUE, trigger_kind="cron", due_at=DUE_MS
        )
        assert msg_id is not None

        messages = _stream_start_messages(fake)
        assert len(messages) == 1
        eid = messages[0]["execution_id"]

        record = _fire_record(fake)
        assert record.get("execution_id") == eid, (
            "fire_record 必须记 execution_id（受理窗口唯一可定位凭据）"
        )

        row = _load_row(fake, eid)
        assert row is not None, "受理窗口内必须有占位状态行（GET 不可 404）"
        assert row.status == "queued"
        assert row.flow_id == "f"
        assert row.flow_version == "7"
        assert row.tenant_id == "default"
        assert not row.context, "占位行 context 必须为空（worker 按首投接管）"

    def test_placeholder_row_in_execution_index(self):
        """占位行同步进执行列表索引（#273 同款：列表/状态过滤立即可见）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        fire_schedule(fake, _schedule(), QUEUE, trigger_kind="cron", due_at=DUE_MS)
        eid = _stream_start_messages(fake)[0]["execution_id"]

        members = fake.zrange(
            f"{tenant_namespace('default')}:execution:index", 0, -1
        )
        assert eid in members

    def test_manual_trigger_equally_visible(self):
        """manual（console 立即触发）与 cron 同一受理语义，可见性无双标。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        fire_schedule(fake, _schedule(), QUEUE, trigger_kind="manual")

        eid = _stream_start_messages(fake)[0]["execution_id"]
        assert _fire_record(fake).get("execution_id") == eid
        assert _load_row(fake, eid).status == "queued"

    def test_same_due_retrigger_converges_to_first_execution(self):
        """同 due 重触发（状态回写失败+锁过期）收敛到既有 id：不入队、
        不重复占配额——否则第二条消息被 worker dedup 吞掉后，其占位行/配额
        成幽灵（预认领是占位行的前置正确性条件）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        from plaita.server.tenant_quota import publish_tenant_quota, tenant_running_count

        publish_tenant_quota(fake, "default", 5)  # 未配额租户 fail-open 不记账
        fire_schedule(fake, _schedule(), QUEUE, trigger_kind="cron", due_at=DUE_MS)
        eid1 = _stream_start_messages(fake)[0]["execution_id"]

        ret = fire_schedule(
            fake, _schedule(), QUEUE, trigger_kind="cron", due_at=DUE_MS
        )

        assert ret is None, "同键重触发不得再次入队"
        assert len(_stream_start_messages(fake)) == 1
        assert tenant_running_count(fake, "default") == 1, (
            "重触发不得重复占租户 start 配额"
        )
        record = _fire_record(fake)
        assert record.get("execution_id") == eid1, "重触发记录必须收敛到既有 id"
        assert record.get("enqueue") == "dedup"

    def test_placeholder_persist_failure_aborts_fire_and_rolls_back(self):
        """占位行落盘失败：不入队（绝不带病受理）+ 配额/映射回滚 + 返回 None。

        与 #273 console POST 的 5xx+回滚同语义；调度入口无 HTTP 响应，
        「不入队 + 返回 None」即其形态（cron 等下一次到期，manual 显示未触发）。
        """
        fake = fakeredis.FakeRedis(decode_responses=True)
        from plaita.server.tenant_quota import publish_tenant_quota, tenant_running_count

        publish_tenant_quota(fake, "default", 5)  # 未配额租户 fail-open 不记账
        original = RedisExecutionStorage.save_execution_state

        def _broken(self, execution_id, state):
            return False

        try:
            RedisExecutionStorage.save_execution_state = _broken
            ret = fire_schedule(
                fake, _schedule(), QUEUE, trigger_kind="cron", due_at=DUE_MS
            )
        finally:
            RedisExecutionStorage.save_execution_state = original

        assert ret is None
        assert _stream_start_messages(fake) == [], "占位行落盘失败绝不能入队"
        assert tenant_running_count(fake, "default") == 0, "配额必须回滚"
        mapping = start_dedup_map_key(tenant_namespace("default"),
                                      f"sched:s1:due:{DUE_MS}")
        assert fake.get(mapping) is None, "幂等映射必须回滚（防同键重触发永久哑火）"

    def test_enqueue_failure_discards_row_and_releases_quota(self):
        """入队失败：清占位行（不留无人消费的幽灵 queued 行）+ 释放配额 +
        释放映射；fire_record 照记（enqueue=failed + execution_id）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        from plaita.server.tenant_quota import publish_tenant_quota, tenant_running_count
        import plaita.server.services.schedule_service as ss

        publish_tenant_quota(fake, "default", 5)  # 未配额租户 fail-open 不记账
        original = ss.enqueue_task

        def _boom(*args, **kwargs):
            raise ConnectionError("simulated enqueue failure")

        try:
            ss.enqueue_task = _boom
            ret = fire_schedule(
                fake, _schedule(), QUEUE, trigger_kind="cron", due_at=DUE_MS
            )
        finally:
            ss.enqueue_task = original

        assert ret is None
        assert tenant_running_count(fake, "default") == 0
        mapping = start_dedup_map_key(tenant_namespace("default"),
                                      f"sched:s1:due:{DUE_MS}")
        assert fake.get(mapping) is None

        record = _fire_record(fake)
        assert record.get("enqueue") == "failed"
        assert record.get("execution_id"), "失败触发的记录仍须带预铸 id（可观测）"
        eid = record["execution_id"]
        assert _load_row(fake, eid) is None, "入队失败必须清掉占位行"
