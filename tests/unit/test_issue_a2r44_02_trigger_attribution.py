"""第七十二轮任务①：触发归因反向断链（A2R44-02，duo-a2 台账第四十四轮）。

## 缺陷

``fire_schedule`` 的 start 消息带 ``trigger: {kind, schedule_id, schedule_name}``，
但 ``ExecutionState`` 全模型无 trigger 字段、flow_worker 无一处读取、接管落行
恒 ``invoker="worker"``——调度触发的执行一旦被 worker 接管，归因即丢失：
列表/详情只见 invoker=worker，与手动启动不可区分，执行无法归因到调度。

## 修复契约

- ``ExecutionState`` 增可选 ``trigger`` 字段（{kind, schedule_id, schedule_name}；
  None = 手动启动/老执行，兼容缺省）；
- **queued 占位行透传**（schedule_service 侧）：触发即带归因，受理窗口内
  list/detail 已可见 invoker=schedule + schedule_id；
- **worker 接管写入**：``start_flow`` 增 ``trigger`` 形参（``_dispatch_task``
  从消息体透传），落 running 行 ``invoker=invoker_for_trigger(trigger)``
  （带 schedule_id 的调度触发 → "schedule"；无/畸形 trigger → "worker"
  存量语义），trigger 原样随行并贯穿终态写（同一 state 对象）；
- console 列表（服务端投影）与详情透出。

负控（先红后绿）：pre-fix 占位行/接管行无 trigger、start_flow 无 trigger
形参（TypeError）、分派不透传；post-fix 全链路归因可达，手动仍 worker。
"""
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.server.flow_worker import RedisFlowWorker, invoker_for_trigger
from plaita.server.services.schedule_service import (
    SCHEDULE_FIRES_KEY,
    fire_schedule,
)
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

QUEUE = "plaita:flow:queue"
DUE_MS = 1791530071848
TRIGGER = {"kind": "cron", "schedule_id": "s1", "schedule_name": "夜间对账"}


def _schedule(**overrides):
    base = {
        "schedule_id": "s1",
        "tenant_id": "default",
        "name": "夜间对账",
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
        payload = json_payload(entry)
        if payload.get("type") == "start":
            out.append(payload)
    return out


def json_payload(entry):
    import json

    return json.loads(entry[1]["payload"])


def _placeholder_row(redis, execution_id):
    from plaita.server.tenant_context import tenant_namespace
    from plaita.storage.redis import RedisExecutionStorage

    return RedisExecutionStorage(
        client=redis, namespace=tenant_namespace("default")
    ).load_execution_state(execution_id)


WORKER_TEST_FLOW = {
    "flow_id": "f1",
    "version": "1",
    "nodes": [
        {"id": "start", "type": "start", "next": "a"},
        {"id": "a", "type": "assignment", "result": {"step": 1}, "next": "end"},
        {"id": "end", "type": "end", "output": "ok"},
    ],
}


def _make_worker(fake_redis, storage=None, flow_storage=None):
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:a2r44-02",
        execution_storage=storage if storage is not None else MemoryExecutionStorage(),
        flow_storage=flow_storage if flow_storage is not None else MemoryFlowStorage(),
        redis_client=fake_redis,
        enable_registry=False,
        enable_redis_logging=False,
        lease_ttl_seconds=60,
    )


class TestPlaceholderAndMessageCarryTrigger:
    def test_fired_message_carries_trigger(self):
        """消息形状（既有）：trigger 三元组随 start 消息入队（回归锚）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        fire_schedule(fake, _schedule(), QUEUE, trigger_kind="cron", due_at=DUE_MS)
        messages = _stream_start_messages(fake)
        assert len(messages) == 1
        assert messages[0]["trigger"] == TRIGGER

    def test_placeholder_row_carries_trigger(self):
        """负控：占位行透传 trigger——受理窗口内归因即可查，不等 worker 接管。

        pre-fix：占位行无 trigger（None）；post-fix：trigger == 消息同款三元组。
        """
        fake = fakeredis.FakeRedis(decode_responses=True)
        fire_schedule(fake, _schedule(), QUEUE, trigger_kind="cron", due_at=DUE_MS)
        eid = _stream_start_messages(fake)[0]["execution_id"]

        row = _placeholder_row(fake, eid)
        assert row is not None
        assert row.invoker == "schedule"
        assert row.trigger == TRIGGER, (
            "占位行必须透传 trigger（受理窗口内归因即可查）"
        )

    def test_manual_trigger_kind_preserved(self):
        """console「立即触发」：kind=manual 原样随行（周期/手动可区分）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        fire_schedule(fake, _schedule(), QUEUE, trigger_kind="manual")
        eid = _stream_start_messages(fake)[0]["execution_id"]
        row = _placeholder_row(fake, eid)
        assert row.trigger == {
            "kind": "manual", "schedule_id": "s1", "schedule_name": "夜间对账",
        }


class TestInvokerForTrigger:
    def test_schedule_trigger_kinds_land_schedule(self):
        assert invoker_for_trigger(TRIGGER) == "schedule"
        assert invoker_for_trigger(
            {"kind": "manual", "schedule_id": "s1", "schedule_name": "n"}
        ) == "schedule"

    def test_missing_or_malformed_trigger_lands_worker(self):
        """手动启动/旧消息无 trigger、畸形 trigger ⇒ invoker=worker 存量语义。"""
        assert invoker_for_trigger(None) == "worker"
        assert invoker_for_trigger({}) == "worker"
        assert invoker_for_trigger({"kind": "cron"}) == "worker"
        assert invoker_for_trigger("cron") == "worker"


class TestWorkerTakeoverAttribution:
    def _worker(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        flow_storage = MemoryFlowStorage()
        flow_storage.save_flow(WORKER_TEST_FLOW)
        return _make_worker(fake, storage, flow_storage), storage

    def test_start_row_carries_trigger_and_schedule_invoker(self):
        """负控（worker 侧）：接管落行写 trigger + invoker=schedule，贯穿终态。

        pre-fix：start_flow 无 trigger 形参（TypeError 红）；post-fix：
        running → 终态行 trigger 原样、invoker=schedule。
        """
        worker, storage = self._worker()
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.run_distributed.return_value = {
                "execution_id": "sched-1", "is_end": True, "is_suspend": False,
                "result": "ok", "context": {"$LAST_NODE": "end", "$NODE": {}},
            }
            worker.start_flow("f1", {}, execution_id="sched-1", trigger=TRIGGER)
        state = storage.load_execution_state("sched-1")
        assert state.invoker == "schedule", (
            "调度触发的执行接管后 invoker 必须是 schedule（而非恒 worker）"
        )
        assert state.trigger == TRIGGER

    def test_manual_start_row_still_worker_without_trigger(self):
        """手动启动（无 trigger）：invoker=worker、trigger=None——存量零变化。"""
        worker, storage = self._worker()
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.run_distributed.return_value = {
                "execution_id": "manual-1", "is_end": True, "is_suspend": False,
                "result": "ok", "context": {"$LAST_NODE": "end", "$NODE": {}},
            }
            worker.start_flow("f1", {}, execution_id="manual-1")
        state = storage.load_execution_state("manual-1")
        assert state.invoker == "worker"
        assert state.trigger is None


class TestDispatchPassesTrigger:
    def _fresh_stream_id(self):
        """当前墙钟 Stream ID（存量 ID 会被载体年龄闸当历史丢弃）。"""
        import time

        return f"{int(time.time() * 1000)}-0"

    def test_dispatch_task_forwards_trigger_to_start_flow(self):
        """消费分派把消息体 trigger 透传给 start_flow（断链修复的接线点）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        body = {
            "type": "start",
            "tenant_id": "default",
            "execution_id": "disp-1",
            "flow_id": "f1",
            "params": {},
            "trigger": TRIGGER,
        }
        with patch.object(worker, "start_flow", MagicMock()) as sf:
            worker._dispatch_task(
                body, stream_message_id=self._fresh_stream_id()
            )
        assert sf.call_args.kwargs.get("trigger") == TRIGGER

    def test_dispatch_task_without_trigger_passes_none(self):
        """旧消息无 trigger 键：透传 None（不炸、语义同存量）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        body = {
            "type": "start",
            "tenant_id": "default",
            "execution_id": "disp-2",
            "flow_id": "f1",
            "params": {},
        }
        with patch.object(worker, "start_flow", MagicMock()) as sf:
            worker._dispatch_task(
                body, stream_message_id=self._fresh_stream_id()
            )
        assert "trigger" not in sf.call_args.kwargs or (
            sf.call_args.kwargs.get("trigger") is None
        )


class TestFireRecordAnchors:
    def test_fire_record_shape_unchanged_on_ok_path(self):
        """ok 路径 fire_record 既有字段不回归（r71 契约锚）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        fire_schedule(fake, _schedule(), QUEUE, trigger_kind="cron", due_at=DUE_MS)
        raw = fake.lrange(SCHEDULE_FIRES_KEY.format(schedule_id="s1"), 0, 0)
        import json

        record = json.loads(raw[0])
        assert record["enqueue"] == "ok"
        assert record["execution_id"]
