"""A43-04 端到端（负控）：调度触发的受理窗口在 console 可查可取消。

worker 全停形态（无消费者）下的完整链路：

    POST /api/schedules/{id}/trigger（集群档 manual，复用 engine ``fire_schedule``）
      → fire_record 记 execution_id（GET /api/schedules/{id}/history 可定位）
      → GET /api/executions/{id} → 200 / status=queued（占位行）
      → POST /api/executions/{id}/cancel → cancelled + 租户 start 配额立即释放

pre-fix：触发只返回 msg_id，fire_record 无 execution_id、无占位行——
history 定位不到、GET/CANCEL 双 404、配额占着（可见性双标）。本文件负控
pre-fix 必红（404 面）；占位行/记录形状的细粒度断言在引擎侧
``tests/unit/test_schedule_trigger_visibility.py``。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from fastapi import Depends, FastAPI
from fakeredis import FakeRedis
from starlette.testclient import TestClient

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import executions as executions_api  # noqa: E402
from api import schedules as schedules_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import flow_store  # noqa: E402
from plaita.server.services.schedule_service import (  # noqa: E402
    SCHEDULES_KEY,
)
from plaita.server.tenant_quota import (  # noqa: E402
    publish_tenant_quota,
    tenant_running_count,
)

ADMIN_KEY = "cluster-admin-key"
QUEUE = "plaita:flow:queue"


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


@pytest.fixture()
def env(tmp_path, monkeypatch):
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    flow_store.init_engine(f"sqlite:///{tmp_path}/a4304-schedule-visibility.db")
    return FakeRedis(decode_responses=True)


def _client(redis) -> TestClient:
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.state.store = flow_store.get_flow_store()
    auth = [Depends(require_auth)]
    app.include_router(executions_api.router, prefix="/api", dependencies=auth)
    app.include_router(schedules_api.router, prefix="/api", dependencies=auth)
    return TestClient(app)


def _headers():
    return {"X-Admin-API-Key": ADMIN_KEY}


def _schedule(**overrides):
    base = {
        "schedule_id": "s-vis",
        "tenant_id": "default",
        "name": "n",
        "flow_id": "f1",
        "version": "1",
        "cron": "* * * * *",
        "params": {},
        "enabled": True,
        "created_at": "t0",
        "updated_at": "t0",
        "next_run_at": "0",
    }
    base.update(overrides)
    return base


class TestScheduleTriggerVisibilityEndToEnd:
    def test_triggered_execution_locatable_and_cancellable_while_worker_down(self, env):
        """worker 停机：trigger → history 定位 id → GET 200/queued → CANCEL
        置终态 + 配额立即回收。pre-fix：history 无 id、GET/CANCEL 双 404。"""
        redis = env
        redis.hset(SCHEDULES_KEY, "s-vis", json.dumps(_schedule()))
        publish_tenant_quota(redis, "default", 5)  # 未配额租户 fail-open 不记账
        client = _client(redis)

        trig = client.post(
            "/api/schedules/s-vis/trigger", headers=_headers()
        )
        assert trig.status_code == 200, trig.text
        assert trig.json()["mode"] == "cluster"
        assert trig.json()["msg_id"], "manual 触发必须返回 msg_id"

        # 受理窗口唯一可定位凭据：fire_record 的 execution_id（history 透出）
        hist = client.get("/api/schedules/s-vis/history", headers=_headers())
        assert hist.status_code == 200, hist.text
        records = hist.json()["records"]
        assert records and records[0]["enqueue"] == "ok"
        eid = records[0]["execution_id"]
        assert eid, "pre-fix 这里是 None——历史行定位不到执行（可见性双标）"

        assert tenant_running_count(redis, "default") == 1, "受理即占配额"

        got = client.get(f"/api/executions/{eid}", headers=_headers())
        assert got.status_code == 200, got.text
        assert got.json()["status"] == "queued"
        assert got.json()["flow_id"] == "f1"

        cancel = client.post(
            f"/api/executions/{eid}/cancel", headers=_headers()
        )
        assert cancel.status_code == 200, cancel.text
        assert cancel.json()["status"] == "cancelled"

        got = client.get(f"/api/executions/{eid}", headers=_headers())
        assert got.status_code == 200
        assert got.json()["status"] == "cancelled"
        assert tenant_running_count(redis, "default") == 0, (
            "queued 行取消必须立即释放租户 start 配额（#273 同语义）"
        )

    def test_placeholder_row_visible_in_executions_list(self, env):
        """调度触发的占位行同步进执行列表（与手动 start 同一受理面）。"""
        redis = env
        redis.hset(SCHEDULES_KEY, "s-vis", json.dumps(_schedule()))
        client = _client(redis)

        trig = client.post("/api/schedules/s-vis/trigger", headers=_headers())
        assert trig.status_code == 200, trig.text
        hist = client.get("/api/schedules/s-vis/history", headers=_headers())
        eid = hist.json()["records"][0]["execution_id"]

        listed = client.get("/api/executions", headers=_headers())
        assert listed.status_code == 200, listed.text
        ids = [row["execution_id"] for row in listed.json().get("executions", [])]
        assert eid in ids, "占位行必须经索引进入执行列表"
