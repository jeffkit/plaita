"""A2R24-05 / 契约族余项：``/schedules/{id}/history`` 两档统一。

原状（r24 清单点名但 r48 只修了 logs/events）：

- 两档都返回 ``total=len(records)``（本页条数），limit 以上的历史不可达；
- 本地档记录是 ``{triggered_at,status,error,execution_id}``，集群档与前端
  ``ScheduleFireRecord`` 是 ``{fired_at,flow_id,version,enqueue,msg_id}``——
  本地档对话框时间恒空、状态恒显示「入队失败」。

本文件钉住：同一字段集、``total``=匹配总数、``offset/next_offset`` 可翻到
全部历史、limit/offset 越界 422。
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis
from fastapi import FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import schedules as schedules_api  # noqa: E402
from models.flow import LocalScheduleFire  # noqa: E402
from plaita.server.services.schedule_service import (  # noqa: E402
    SCHEDULE_FIRES_KEY,
    SCHEDULES_KEY,
)
from services import flow_store  # noqa: E402
from services import local_scheduler as sched  # noqa: E402

RECORD_KEYS = {"fired_at", "trigger_kind", "flow_id", "version", "enqueue", "msg_id"}


def _schedule(schedule_id: str = "s1", **overrides):
    base = {
        "schedule_id": schedule_id,
        "tenant_id": "default",
        "name": "n",
        "flow_id": "f",
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


@pytest.fixture()
def cluster_client():
    redis = fakeredis.FakeRedis(decode_responses=True)
    redis.hset(SCHEDULES_KEY, "s1", json.dumps(_schedule()))
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.include_router(schedules_api.router, prefix="/api")
    return TestClient(app), redis


@pytest.fixture()
def local_client(tmp_path):
    flow_store.init_engine(f"sqlite:///{tmp_path / 'sched.db'}")
    store = flow_store.get_flow_store()
    sched.create_schedule(store, _schedule())
    app = FastAPI()
    app.state.redis = None
    app.state.local_mode = True
    app.state.store = store
    app.include_router(schedules_api.router, prefix="/api")
    return TestClient(app), store


def _push_cluster_records(redis, count: int = 5) -> None:
    # lpush 使最后写入的在最前；记录本身按 fired_at 倒序展示
    for i in range(count):
        redis.lpush(
            SCHEDULE_FIRES_KEY.format(schedule_id="s1"),
            json.dumps({
                "fired_at": f"2026-10-2{i}T00:00:00",
                "trigger_kind": "cron",
                "flow_id": "f",
                "version": "1.0.0",
                "dedup_key": f"d{i}",
                "enqueue": "ok",
                "msg_id": f"m{i}",
            }),
        )


def _insert_local_records(store, count: int = 5) -> None:
    base = datetime(2026, 10, 20, 12, 0, 0)
    with store._session_local() as session:
        for i in range(count):
            session.add(LocalScheduleFire(
                schedule_id="s1",
                execution_id=f"e{i}",
                trigger_kind="cron",
                status="fired" if i % 2 == 0 else "failed",
                error="" if i % 2 == 0 else f"boom-{i}",
                created_at=base + timedelta(seconds=i),
            ))
        session.commit()


def test_cluster_history_total_and_offset_pages(cluster_client):
    client, redis = cluster_client
    _push_cluster_records(redis, 5)

    page1 = client.get("/api/schedules/s1/history?limit=2&offset=0").json()
    assert page1["total"] == 5, page1
    assert page1["returned"] == 2
    assert page1["truncated"] is True
    assert page1["next_offset"] == 2
    assert [r["msg_id"] for r in page1["records"]] == ["m4", "m3"]
    for record in page1["records"]:
        assert RECORD_KEYS <= set(record), record

    page2 = client.get("/api/schedules/s1/history?limit=2&offset=2").json()
    assert [r["msg_id"] for r in page2["records"]] == ["m2", "m1"]
    assert page2["next_offset"] == 4

    page3 = client.get("/api/schedules/s1/history?limit=2&offset=4").json()
    assert [r["msg_id"] for r in page3["records"]] == ["m0"]
    assert page3["total"] == 5
    assert page3["truncated"] is False
    assert page3["next_offset"] is None


def test_cluster_history_rejects_bad_limit_and_offset(cluster_client):
    client, _ = cluster_client
    for query in ("limit=0", "limit=51", "offset=-1", "offset=10001"):
        resp = client.get(f"/api/schedules/s1/history?{query}")
        assert resp.status_code == 422, (query, resp.text)


def test_local_history_uses_cluster_record_shape(local_client):
    client, store = local_client
    _insert_local_records(store, 5)

    page = client.get("/api/schedules/s1/history?limit=2").json()
    assert page["total"] == 5, page
    assert page["returned"] == 2
    assert page["truncated"] is True
    assert page["next_offset"] == 2
    # 最新在前（created_at desc）
    assert [r["execution_id"] for r in page["records"]] == ["e4", "e3"]

    newest = page["records"][0]
    assert newest["fired_at"], "本地档 fired_at 不能为空（前端时间列依赖它）"
    assert newest["flow_id"] == "f"
    assert newest["version"] == "1.0.0"
    assert newest["enqueue"] in ("ok", "failed")
    assert newest["msg_id"] is None
    assert newest["status"] == "fired"
    assert newest["enqueue"] == "ok"
    assert newest["error"] is None
    assert RECORD_KEYS <= set(newest), newest

    last = client.get("/api/schedules/s1/history?limit=2&offset=4").json()
    assert [r["execution_id"] for r in last["records"]] == ["e0"]
    assert last["truncated"] is False
    assert last["next_offset"] is None


def test_local_history_enqueue_maps_fired_status(local_client):
    client, store = local_client
    _insert_local_records(store, 2)

    body = client.get("/api/schedules/s1/history").json()
    by_exec = {r["execution_id"]: r for r in body["records"]}
    assert by_exec["e0"]["enqueue"] == "ok"
    assert by_exec["e1"]["enqueue"] == "failed"
