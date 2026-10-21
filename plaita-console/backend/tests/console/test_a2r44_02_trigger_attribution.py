"""第七十二轮负控（console 端到端）：触发归因 + 拒绝留痕（A2R44-02 / B44-01）。

链路一（归因，A2R44-02）：

    POST /api/schedules/{id}/trigger（集群档 manual，复用 engine ``fire_schedule``）
      → queued 占位行带 trigger + invoker=schedule
      → GET /api/executions/{id} 透出 invoker=schedule + trigger.schedule_id
      → GET /api/executions（列表服务端投影）同行透出

    pre-fix：ExecutionInfo 无 trigger 字段、接管后 invoker 恒 worker——执行
    无法归因到调度。

链路二（拒绝留痕，B44-01）：

    engine fire_schedule 遇停用租户/配额满 → fire_record 落 rejected+原因
      → GET /api/schedules/{id}/history 透出 rejected 记录
      → 恢复后再次触发 → ok 记录与 rejected 记录并存

    pre-fix：拒绝在 fire_record 写入前 return——历史零新增，「到期被拒」与
    「未到期」不可区分（调度面静默停走）。

（console POST /trigger 对停用租户另有 403 前置闸——本文件拒绝链路经 engine
层触发，与 cron 循环同一入口，历史面即其可见性形态。）
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
    fire_schedule,
)
from plaita.server.tenant_quota import publish_tenant_quota  # noqa: E402
from plaita.server.tenant_status import publish_tenant_status  # noqa: E402

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
    flow_store.init_engine(f"sqlite:///{tmp_path}/a2r44b4401-trigger-attribution.db")
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
        "schedule_id": "s-attr",
        "tenant_id": "default",
        "name": "夜间对账",
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


class TestTriggerAttributionVisibility:
    def test_detail_and_list_show_schedule_attribution(self, env):
        """负控：调度触发的执行在详情/列表可见 invoker=schedule + schedule_id。

        pre-fix：GET 的 trigger 为 None（模型无字段）；post-fix：占位行归因
        经详情与列表服务端投影双透出。
        """
        redis = env
        redis.hset(SCHEDULES_KEY, "s-attr", json.dumps(_schedule()))
        publish_tenant_quota(redis, "default", 5)  # 未配额租户 fail-open 不记账
        client = _client(redis)

        trig = client.post("/api/schedules/s-attr/trigger", headers=_headers())
        assert trig.status_code == 200, trig.text
        hist = client.get("/api/schedules/s-attr/history", headers=_headers())
        eid = hist.json()["records"][0]["execution_id"]
        assert eid

        got = client.get(f"/api/executions/{eid}", headers=_headers())
        assert got.status_code == 200, got.text
        body = got.json()
        assert body["invoker"] == "schedule"
        assert body["trigger"] == {
            "kind": "manual",
            "schedule_id": "s-attr",
            "schedule_name": "夜间对账",
        }

        listed = client.get("/api/executions", headers=_headers())
        assert listed.status_code == 200, listed.text
        rows = [
            row for row in listed.json().get("executions", [])
            if row["execution_id"] == eid
        ]
        assert rows, "占位行必须进列表"
        assert rows[0]["invoker"] == "schedule"
        assert rows[0]["trigger"]["schedule_id"] == "s-attr"


class TestRejectionTraceVisibility:
    def test_history_surfaces_rejected_with_reason_then_coexists(self, env):
        """负控：停用租户触发 → history 记 rejected+原因；恢复后 ok 并存。

        pre-fix：拒绝零痕（records 为空）；post-fix：rejected 可见且与 ok
        并存——「未到期」与「到期被拒」在调度面可区分。
        """
        redis = env
        redis.hset(SCHEDULES_KEY, "s-attr", json.dumps(_schedule()))
        publish_tenant_quota(redis, "default", 5)
        publish_tenant_status(redis, "default", "disabled")

        # cron 循环同入口（console POST 对停用租户另有 403 前置闸）
        ret = fire_schedule(
            redis, _schedule(), QUEUE, trigger_kind="cron", due_at=1791530071848
        )
        assert ret is None

        client = _client(redis)
        hist = client.get("/api/schedules/s-attr/history", headers=_headers())
        assert hist.status_code == 200, hist.text
        records = hist.json()["records"]
        assert len(records) == 1, "pre-fix 这里是 0——拒绝零痕"
        assert records[0]["enqueue"] == "rejected"
        assert records[0]["rejection_reason"] == "tenant_disabled"

        # 恢复后再次触发：ok 记录与 rejected 记录并存
        publish_tenant_status(redis, "default", "active")
        msg_id = fire_schedule(
            redis, _schedule(), QUEUE, trigger_kind="cron", due_at=1791530071848
        )
        assert msg_id is not None
        hist = client.get("/api/schedules/s-attr/history", headers=_headers())
        records = hist.json()["records"]
        assert [r["enqueue"] for r in records] == ["ok", "rejected"]
        assert records[1]["rejection_reason"] == "tenant_disabled"

    def test_quota_rejection_visible_in_history(self, env):
        """配额满拒绝同样留痕（quota_exhausted），恢复配额后 ok 并存。"""
        redis = env
        redis.hset(SCHEDULES_KEY, "s-attr", json.dumps(_schedule()))
        publish_tenant_quota(redis, "default", 1)
        from plaita.server.tenant_quota import try_admit_tenant_start

        assert try_admit_tenant_start(redis, "default", "occupant-1")
        fire_schedule(
            redis, _schedule(), QUEUE, trigger_kind="cron", due_at=1791530071848
        )

        client = _client(redis)
        hist = client.get("/api/schedules/s-attr/history", headers=_headers())
        records = hist.json()["records"]
        assert len(records) == 1
        assert records[0]["enqueue"] == "rejected"
        assert records[0]["rejection_reason"] == "tenant_quota_exhausted"

        publish_tenant_quota(redis, "default", 5)
        msg_id = fire_schedule(
            redis, _schedule(), QUEUE, trigger_kind="cron", due_at=1791530071848
        )
        assert msg_id is not None
        hist = client.get("/api/schedules/s-attr/history", headers=_headers())
        records = hist.json()["records"]
        assert [r["enqueue"] for r in records] == ["ok", "rejected"]
