"""#300 触发面池路由收口（第七十五轮）：调度触发（cron/manual）按 runtime 选池。

r71 已知缺口（台账留痕）：``fire_schedule`` 的队列由调用方传——cron 循环是
独立 plaita server 进程（无 console store），恒落默认 python 池；console
manual 触发也恒传 ``_task_queue_name()``。go-runtime flow 的调度触发会串池。

收口语义：
- 调度行冗余 ``runtime``（console create/update/enable 按 start 同源的版本行
  解析盖章）→ cron 派发据此选池（跨进程只能靠行上冗余）；
- ``fire_schedule(queue_name=None)`` 自解析：显式参 > 行 runtime > 历史派发链
  （旧行无 runtime 键，行为零变化）；
- 行 runtime 非法 → 具名 WARNING 后回落历史链（降级可观测，绝不静默换道）。

负控先红：基线（1dd62a3）无 ``_resolve_schedule_queue``，``fire_schedule``
不解析行 runtime——带「先红」标注的用例在基线上必红。
"""
from __future__ import annotations

import json

import pytest

pytest.importorskip("fakeredis")

import fakeredis

import plaita.server.services.schedule_service as sched
from plaita.server.services.schedule_service import (
    SCHEDULE_FIRES_KEY,
    DEFAULT_QUEUE_NAME,
    ScheduleService,
    fire_schedule,
    resolve_dispatch_queue_name,
)

GO_QUEUE = "plaita:flow:queue:go"


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.setattr(sched, "_queue_split_warned", set())
    for name in (
        "PLAITA_CONSOLE_TASK_QUEUE",
        "PLAITA_QUEUE_NAME",
        "QUEUE_NAME",
        "PLAITA_CONSOLE_GO_TASK_QUEUE",
        "PLAITA_CONSOLE_RUNTIME_QUEUES",
    ):
        monkeypatch.delenv(name, raising=False)


def _schedule(**overrides):
    base = {
        "schedule_id": "s-go",
        "tenant_id": "default",
        "name": "n",
        "flow_id": "f1",
        "version": "1.0.0",
        "cron": "* * * * *",
        "params": {},
        "enabled": True,
        "created_at": "t0",
        "updated_at": "t0",
        "next_run_at": "0",
    }
    base.update(overrides)
    return base


def _start_messages(redis, queue):
    out = []
    for entry in redis.xrange(queue):
        payload = json.loads(entry[1]["payload"])
        if payload.get("type") == "start":
            out.append(payload)
    return out


class TestResolveScheduleQueue:
    def test_row_runtime_selects_pool(self):
        """先红：基线无 _resolve_schedule_queue（ModuleNotFoundError 级缺面）。"""
        assert sched._resolve_schedule_queue(_schedule(runtime="go")) == GO_QUEUE
        assert sched._resolve_schedule_queue(_schedule(runtime="python")) == (
            DEFAULT_QUEUE_NAME
        )

    def test_row_without_runtime_legacy_chain(self):
        # 旧行零行为变化：无 runtime 键 → 历史派发链（正控）
        assert sched._resolve_schedule_queue(_schedule()) == (
            resolve_dispatch_queue_name()
        )

    def test_explicit_config_beats_row_runtime(self):
        assert sched._resolve_schedule_queue(
            _schedule(runtime="go"), "plaita:flow:queue:explicit"
        ) == "plaita:flow:queue:explicit"

    def test_invalid_runtime_warns_and_falls_back(self, caplog):
        with caplog.at_level("WARNING", logger="plaita.server.services.schedule_service"):
            queue = sched._resolve_schedule_queue(_schedule(runtime="banana"))
        assert queue == resolve_dispatch_queue_name()
        assert sched.SCHEDULE_RUNTIME_INVALID_WARNING in caplog.text


class TestFireScheduleRowRuntimeRouting:
    def test_go_row_lands_in_go_queue(self):
        """先红：基线不解析行 runtime——queue=None 直接把 None 当队列名。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        msg_id = fire_schedule(
            fake, _schedule(runtime="go"), None,
            trigger_kind="cron", due_at=1792700000000,
        )
        assert msg_id is not None
        assert len(_start_messages(fake, GO_QUEUE)) == 1
        assert fake.xlen(DEFAULT_QUEUE_NAME) == 0

    def test_python_row_and_legacy_row_land_in_default(self):
        # 正控：python 行与无 runtime 旧行都落默认池（历史行为保持）
        fake = fakeredis.FakeRedis(decode_responses=True)
        assert fire_schedule(
            fake, _schedule(runtime="python"), None,
            trigger_kind="cron", due_at=1792700000000,
        ) is not None
        assert fire_schedule(
            fake, _schedule(schedule_id="s-old"), None,
            trigger_kind="cron", due_at=1792700000001,
        ) is not None
        assert fake.xlen(GO_QUEUE) == 0
        assert len(_start_messages(fake, DEFAULT_QUEUE_NAME)) == 2

    def test_explicit_queue_still_wins_over_row(self):
        # console manual 触发显式传按版本行现算的队列——行 runtime 不抢优先级
        fake = fakeredis.FakeRedis(decode_responses=True)
        assert fire_schedule(
            fake, _schedule(runtime="go"), DEFAULT_QUEUE_NAME,
            trigger_kind="manual",
        ) is not None
        assert fake.xlen(GO_QUEUE) == 0
        assert len(_start_messages(fake, DEFAULT_QUEUE_NAME)) == 1

    def test_cron_fire_record_and_placeholder_follow_routing(self):
        """go 行触发的受理窗口证据（fire_record + queued 占位行）不因换池丢失。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        assert fire_schedule(
            fake, _schedule(runtime="go"), None,
            trigger_kind="cron", due_at=1792700000000,
        ) is not None
        record = json.loads(
            fake.lrange(SCHEDULE_FIRES_KEY.format(schedule_id="s-go"), 0, 0)[0]
        )
        assert record["enqueue"] == "ok"
        execution_id = record["execution_id"]
        assert execution_id
        from plaita.server.tenant_context import tenant_namespace
        from plaita.storage.redis import RedisExecutionStorage

        row = RedisExecutionStorage(
            client=fake, namespace=tenant_namespace("default")
        ).load_execution_state(execution_id)
        assert row is not None and row.status == "queued"


class TestCronLoopRouting:
    def test_fire_if_due_routes_by_row_runtime(self):
        """先红：基线 _fire_if_due 恒走历史派发链（go 行也落默认池）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        svc = ScheduleService(
            event_bus=object(), redis_client=fake, service_config={}
        )
        schedule = _schedule(runtime="go", next_run_at="1")  # 已到期
        svc._fire_if_due(schedule, 1792700000000)
        assert len(_start_messages(fake, GO_QUEUE)) == 1
        assert fake.xlen(DEFAULT_QUEUE_NAME) == 0
