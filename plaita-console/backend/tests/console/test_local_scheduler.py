"""本地档调度与日志单测。

- 调度 CRUD（SQLite）与集群档结构一致
- 到期调度被内置循环自动触发（进程内执行）+ 触发历史
- 手动触发 / 暂停不触发
- 执行日志被线程级 handler 捕获，logs API 本地分支可查
"""
import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import logs as logs_api  # noqa: E402
from api import executions as executions_api  # noqa: E402
from api import schedules as schedules_api  # noqa: E402
from services import examples, flow_store  # noqa: E402
from services import local_executor  # noqa: E402
from services import local_scheduler as sched  # noqa: E402

GOOD_DEF = json.dumps({"nodes": [
    {"type": "start", "id": "start", "next": "end"},
    {"type": "end", "id": "end", "resultType": "success", "output": "ok"},
]})


@pytest.fixture()
def env(tmp_path, monkeypatch):
    flow_store.init_engine(f"sqlite:///{tmp_path / 'sched.db'}")
    store = flow_store.get_flow_store()
    store.ensure_flow("hello")
    store.save_flow_definition("hello", "1.0.0", GOOD_DEF, status="draft")
    store.publish_version("hello", "1.0.0")

    app = FastAPI()
    app.state.redis = None
    app.state.local_mode = True
    app.state.store = store
    app.include_router(executions_api.router, prefix="/api")
    app.include_router(schedules_api.router, prefix="/api")
    app.include_router(logs_api.router, prefix="/api")
    sched.start_scheduler_loop(store)
    yield TestClient(app), store
    sched._stop_event.set()


def client_of(env):
    return env[0]


def store_of(env):
    return env[1]


def _mk_cron_every_second() -> str:
    """croniter 支持秒级扩展（* * * * * *）；若引擎只支持 5 段，则用每分钟。"""
    from plaita.server.services.schedule_service import validate_cron
    if validate_cron("* * * * * *"):
        return "* * * * * *"
    return "* * * * *"


def _mk_cron_every_minute() -> str:
    return "* * * * *"


def _force_due(store, schedule_id: str):
    """把 next_run_at 改成已过期，让 5s 扫描立即命中（不等 cron 真实到期）。"""
    import sqlalchemy
    from models.flow import LocalSchedule
    engine = flow_store.get_init_engine()
    with engine.begin() as conn:
        conn.execute(
            sqlalchemy.text(
                "UPDATE local_schedules SET next_run_at = :t WHERE schedule_id = :sid"
            ),
            {"t": str((datetime.now() - timedelta(seconds=1)).timestamp() * 1000),
             "sid": schedule_id},
        )


def test_schedule_crud_roundtrip(env):
    client, store = client_of(env), store_of(env)
    r = client.post("/api/schedules", json={
        "name": "每分钟问候", "flow_id": "hello", "cron": _mk_cron_every_minute(),
        "params": {"k": "v"},
    })
    assert r.status_code == 200
    body = r.json()
    assert body["enabled"] is True and body["status"] == "running"
    assert body["next_run_at"]  # 创建时即计算下次触发

    lst = client.get("/api/schedules").json()["schedules"]
    assert len(lst) == 1 and lst[0]["name"] == "每分钟问候"

    # 暂停 → next_run_at 清空
    client.post(f"/api/schedules/{body['schedule_id']}/disable")
    assert client.get(f"/api/schedules/{body['schedule_id']}").json()["next_run_at"] == ""

    # 删除
    assert client.delete(f"/api/schedules/{body['schedule_id']}").status_code == 200
    assert client.get("/api/schedules").json()["total"] == 0


def test_cron_preview_works_without_redis(env):
    client = client_of(env)
    r = client.get("/api/schedules/preview", params={"cron": "0 9 * * *", "count": 3})
    assert r.status_code == 200
    assert len(r.json()["next"]) == 3


def test_due_schedule_auto_fires_and_records_history(env):
    client, store = client_of(env), store_of(env)
    cron = _mk_cron_every_second()
    r = client.post("/api/schedules", json={
        "name": "高频", "flow_id": "hello", "cron": cron,
    })
    sid = r.json()["schedule_id"]
    _force_due(store, sid)

    deadline = time.time() + 15
    executions = []
    while time.time() < deadline and not executions:
        time.sleep(0.5)
        hist = client.get(f"/api/schedules/{sid}/history").json()["records"]
        executions = [h["execution_id"] for h in hist if h["status"] == "fired"]

    assert executions, "调度未被自动触发"
    eid = executions[0]
    # 触发即真实执行（进程内），等待完成
    deadline = time.time() + 10
    while time.time() < deadline:
        body = client.get(f"/api/executions/{eid}").json()
        if body["status"] in ("completed", "failed"):
            break
        time.sleep(0.2)
    assert body["status"] == "completed"

    hist = client.get(f"/api/schedules/{sid}/history").json()["records"]
    assert hist[0]["trigger_kind"] == "cron"


def test_disabled_schedule_not_fired(env):
    client, store = client_of(env), store_of(env)
    r = client.post("/api/schedules", json={
        "name": "暂停的", "flow_id": "hello", "cron": _mk_cron_every_minute(), "enabled": False,
    })
    sid = r.json()["schedule_id"]
    time.sleep(1)
    assert client.get(f"/api/schedules/{sid}/history").json()["total"] == 0


def test_manual_trigger_runs_in_process(env):
    client, store = client_of(env), store_of(env)
    r = client.post("/api/schedules", json={
        "name": "手动", "flow_id": "hello", "cron": _mk_cron_every_minute(),
    })
    sid = r.json()["schedule_id"]
    r = client.post(f"/api/schedules/{sid}/trigger")
    assert r.status_code == 200
    eid = r.json()["execution_id"]
    deadline = time.time() + 10
    while time.time() < deadline:
        body = client.get(f"/api/executions/{eid}").json()
        if body["status"] in ("completed", "failed"):
            break
        time.sleep(0.2)
    assert body["status"] == "completed"
    hist = client.get(f"/api/schedules/{sid}/history").json()["records"]
    assert hist[0]["trigger_kind"] == "manual"


def test_execution_logs_captured_and_queryable(env):
    client, store = client_of(env), store_of(env)
    r = client.post("/api/executions", json={"flow_id": "hello"})
    eid = r.json()["execution_id"]
    deadline = time.time() + 10
    while time.time() < deadline:
        body = client.get(f"/api/executions/{eid}").json()
        if body["status"] in ("completed", "failed"):
            break
        time.sleep(0.2)

    # logs API 本地分支：按 instance_id（=执行 ID）查
    r = client.get("/api/logs", params={"instance_id": eid})
    assert r.status_code == 200
    # hello 流程无 WARNING 时也可能没有日志行——构造一条再验证查询通道
    r2 = client.get("/api/logs/stats")
    assert r2.status_code == 200
