"""A56-02 查漏：plaita 服务端调度派发必须与 console 手动启动同源。

console 侧（B3 r54）已把 ``POST /api/schedules/{id}/trigger`` 改为
``executions._task_queue_name()``（``PLAITA_CONSOLE_TASK_QUEUE``）。本仓
``ScheduleService`` 仍把队列名在构造期固化为 ``PLAITA_QUEUE_NAME``——同一
部署里“立即触发”与 cron 会落到不同 Stream。修复后每次派发时解析，
console env 优先，且 env 改动无需重启。
"""
from __future__ import annotations

import pytest

pytest.importorskip("fakeredis")

import fakeredis

import plaita.server.services.schedule_service as sched
from plaita.server.services.schedule_service import (
    DEFAULT_QUEUE_NAME,
    ScheduleService,
    resolve_dispatch_queue_name,
)


@pytest.fixture(autouse=True)
def _clear_warned(monkeypatch):
    monkeypatch.setattr(sched, "_queue_split_warned", set())
    for name in ("PLAITA_CONSOLE_TASK_QUEUE", "PLAITA_QUEUE_NAME", "QUEUE_NAME"):
        monkeypatch.delenv(name, raising=False)


def _service(redis=None, config=None):
    return ScheduleService(
        event_bus=object(),
        redis_client=redis or fakeredis.FakeRedis(decode_responses=True),
        service_config=config or {},
    )


def test_resolver_default(monkeypatch):
    assert resolve_dispatch_queue_name() == DEFAULT_QUEUE_NAME


def test_resolver_console_env_wins_over_worker_env(monkeypatch):
    monkeypatch.setenv("PLAITA_QUEUE_NAME", "plaita:flow:queue")
    monkeypatch.setenv("PLAITA_CONSOLE_TASK_QUEUE", "plaita:flow:queue:v2")
    assert resolve_dispatch_queue_name() == "plaita:flow:queue:v2"


def test_resolver_worker_env_still_compatible(monkeypatch):
    monkeypatch.setenv("PLAITA_QUEUE_NAME", "plaita:flow:queue:v2")
    assert resolve_dispatch_queue_name() == "plaita:flow:queue:v2"


def test_resolver_explicit_config_wins(monkeypatch):
    monkeypatch.setenv("PLAITA_CONSOLE_TASK_QUEUE", "plaita:flow:queue:v2")
    assert resolve_dispatch_queue_name("configured") == "configured"


def test_resolver_warns_once_on_split_queue(monkeypatch, caplog):
    monkeypatch.setenv("PLAITA_QUEUE_NAME", "plaita:flow:queue")
    monkeypatch.setenv("PLAITA_CONSOLE_TASK_QUEUE", "plaita:flow:queue:v2")
    # B2R31-02：分裂告警收敛到单一 resolver（plaita.runtime_config），
    # 进程级去重表需按用例清空才能验证「只打一次」。
    from plaita import runtime_config as rc

    rc._reset_warnings_for_tests()
    with caplog.at_level("WARNING", logger="plaita.runtime_config"):
        assert resolve_dispatch_queue_name() == "plaita:flow:queue:v2"
        assert resolve_dispatch_queue_name() == "plaita:flow:queue:v2"
    messages = [r.message for r in caplog.records if "不同源" in r.message]
    assert len(messages) == 1, "分裂队列告警只应打一次（同一 env 组合）"


def _due_schedule():
    return {
        "schedule_id": "s1",
        "name": "s1",
        "flow_id": "f1",
        "cron": "* * * * *",
        "enabled": True,
        "next_run_at": "0",
        "tenant_id": "default",
    }


def _capture_fire(monkeypatch):
    calls = []
    monkeypatch.setattr(
        sched, "fire_schedule",
        lambda redis, schedule, queue, **kwargs: calls.append(queue) or "1-0",
    )
    return calls


def test_fire_if_due_uses_console_env(monkeypatch):
    """负控主场景：env v2 时 cron 触发必须入 v2（pre-fix 落 worker env/默认）。"""
    monkeypatch.setenv("PLAITA_QUEUE_NAME", "plaita:flow:queue")
    monkeypatch.setenv("PLAITA_CONSOLE_TASK_QUEUE", "plaita:flow:queue:v2")
    calls = _capture_fire(monkeypatch)
    service = _service()

    service._fire_if_due(_due_schedule(), now_ms=10_000)

    assert calls == ["plaita:flow:queue:v2"]


def test_fire_if_due_reads_env_at_dispatch_not_construction(monkeypatch):
    """动态解析：构造后再改 env，下一次派发即生效（不重启服务）。"""
    calls = _capture_fire(monkeypatch)
    service = _service()
    monkeypatch.setenv("PLAITA_CONSOLE_TASK_QUEUE", "plaita:flow:queue:v2")

    service._fire_if_due(_due_schedule(), now_ms=10_000)

    assert calls == ["plaita:flow:queue:v2"]


def test_explicit_service_config_beats_env(monkeypatch):
    monkeypatch.setenv("PLAITA_CONSOLE_TASK_QUEUE", "plaita:flow:queue:v2")
    calls = _capture_fire(monkeypatch)
    service = _service(config={"queue_name": "configured-queue"})

    service._fire_if_due(_due_schedule(), now_ms=10_000)

    assert calls == ["configured-queue"]
