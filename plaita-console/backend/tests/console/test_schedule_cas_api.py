"""plaita#198：console PUT /schedules/{id} 的乐观锁与 fire 回写的交错语义。

- PUT 带 ``expected_updated_at``，与在库 ``updated_at`` 不一致 → 结构化 409
  （conflict=schedule_stale，带 latest_updated_at）；不静默覆盖。
- 交错：读旧快照 → PUT 停用（enabled=false）→ engine ``fire_schedule`` 用旧
  快照回写 → 最终 enabled 仍为 false（fire 只合并触发字段）。
- fire 写入的 last_fired_at 不被后续 PUT 的整份/字段变更丢掉。
"""
from __future__ import annotations

import json
import sys
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
from plaita.server.services.schedule_service import (  # noqa: E402
    SCHEDULES_KEY,
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
        "params": {},
        "enabled": True,
        "created_at": "t0",
        "updated_at": "t0",
        "next_run_at": "0",
    }
    base.update(overrides)
    return base


def _stored(redis):
    return json.loads(redis.hget(SCHEDULES_KEY, "s1"))


@pytest.fixture()
def client():
    redis = fakeredis.FakeRedis(decode_responses=True)
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.include_router(schedules_api.router, prefix="/api")
    return TestClient(app), redis


def test_stale_expected_updated_at_returns_structured_409(client):
    c, redis = client
    redis.hset(SCHEDULES_KEY, "s1", json.dumps(_stale()))

    ok = c.put("/api/schedules/s1", json={
        "enabled": False, "expected_updated_at": "t0",
    })
    assert ok.status_code == 200, ok.text
    assert ok.json()["enabled"] is False

    stale = c.put("/api/schedules/s1", json={
        "enabled": True, "expected_updated_at": "t0",
    })
    assert stale.status_code == 409, stale.text
    detail = stale.json()["detail"]
    assert detail["conflict"] == "schedule_stale"
    assert detail["latest_updated_at"] == ok.json()["updated_at"]
    assert _stored(redis)["enabled"] is False, "409 时不得写入"


def test_interleaved_put_disable_then_fire_keeps_disabled(client):
    c, redis = client
    stale = _stale()
    redis.hset(SCHEDULES_KEY, "s1", json.dumps(stale))

    put = c.put("/api/schedules/s1", json={
        "enabled": False, "expected_updated_at": "t0",
    })
    assert put.status_code == 200, put.text

    fire_schedule(redis, stale, QUEUE, trigger_kind="cron", due_at=0)

    final = _stored(redis)
    assert final["enabled"] is False, "fire 用旧快照把停用翻回了 true"
    assert final["last_fired_at"], "触发状态未回写"
    assert final["cron"] == stale["cron"]


def test_update_after_fire_keeps_last_fired_at_and_current_updated_at(client):
    c, redis = client
    stale = _stale()
    redis.hset(SCHEDULES_KEY, "s1", json.dumps(stale))

    fire_schedule(redis, stale, QUEUE, trigger_kind="cron", due_at=0)
    after_fire = _stored(redis)
    assert after_fire["last_fired_at"]
    # fire 不推进 updated_at：console 的乐观锁基准仍是 t0。
    assert after_fire["updated_at"] == "t0"

    put = c.put("/api/schedules/s1", json={
        "name": "renamed", "expected_updated_at": "t0",
    })
    assert put.status_code == 200, put.text
    final = _stored(redis)
    assert final["name"] == "renamed"
    assert final["last_fired_at"] == after_fire["last_fired_at"]
