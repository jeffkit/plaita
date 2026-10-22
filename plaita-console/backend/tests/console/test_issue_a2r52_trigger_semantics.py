"""A2R52-03/04：手动触发语义随模式翻转 + 配额满冒充基础设施故障。

机制（A2 台账第五十二轮，r84 核销红）：

- A2R52-03：暂停调度手动触发双档分裂——集群档端点不查 ``enabled``，
  ``fire_schedule`` 照常入队 200；本地档走 ``local.fire()`` 的 A48-04 闸
  返回 None → 502「触发失败，请查看 console 日志」（把用户配置态误导成
  基础设施故障，且语义随部署模式翻转）。修法=端点两档统一前置检查，显式
  ``enabled=False`` → 409 ``schedule_paused``（受理前拒绝，无 fire
  record；行缺键的最小形状/畸形行不拦，行为零变化；cron 面不经端点）。
- A2R52-04：manual 触发配额满——``fire_schedule`` 返回 None 多义（配额满/
  租户停用/入队失败/dedup 收敛），端点一律 502「入队失败，请检查 Redis 与
  任务队列配置」，真因只在 fire history ``rejection_reason``。修法=
  ``fire_schedule`` 加 keyword-only ``rejection`` 出参回写 ``REJECTION_*``，
  端点把配额满映射为与 POST /executions 同契约（#28 同一准入闸）的 429
  ``tenant_quota_exceeded``，其余 None 仍 502。

负控（修复前红）：A2 r56 探针 01d 双面 / 01e
（``~/.issue-keeper/duty/a2-r56-scripts/test_a2r52_axis1_schedule_lifecycle-r56.py``，
本仓 pre 证据 ``docs/proofs/data/r84-a2r52-trigger-semantics-pre.txt``）。
"""
import json
import sys
from pathlib import Path

import pytest
from fastapi import Depends, FastAPI
from fakeredis import FakeRedis
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import flows as flows_api  # noqa: E402
from api import schedules as schedules_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import flow_store  # noqa: E402

ADMIN_KEY = "a2r52-trigger-admin"

FLOW_ID = "trigger-flow"
VER_PUB = "1.0.0"
CRON = "0 9 * * *"

FLOW_DEF = json.dumps(
    {
        "flow_id": FLOW_ID,
        "desc": "a2r52 trigger semantics",
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "end"},
            {"type": "end", "id": "end", "output": "$INPUT.name",
             "resultType": "success"},
        ],
    }
)


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


@pytest.fixture()
def redis(tmp_path, monkeypatch) -> FakeRedis:
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    flow_store.init_engine(f"sqlite:///{tmp_path / 'a2r52_trigger.db'}")
    return FakeRedis(decode_responses=True)


def _client(redis, *, local_mode: bool = False) -> TestClient:
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = local_mode
    app.state.store = flow_store.get_flow_store()
    deps = [Depends(require_auth)]
    app.include_router(flows_api.router, prefix="/api", dependencies=deps)
    app.include_router(schedules_api.router, prefix="/api", dependencies=deps)
    return TestClient(app)


def _headers() -> dict:
    return {"X-Admin-API-Key": ADMIN_KEY}


def _seed_flow(client: TestClient) -> None:
    client.post("/api/flows", json={"flow_id": FLOW_ID}, headers=_headers())
    r = client.put(f"/api/flows/{FLOW_ID}/versions/{VER_PUB}",
                   json={"definition": FLOW_DEF}, headers=_headers())
    assert r.status_code == 200, r.text
    r = client.post(f"/api/flows/{FLOW_ID}/publish",
                    json={"version": VER_PUB}, headers=_headers())
    assert r.status_code == 200, r.text


def _mk_sched(client: TestClient, *, enabled: bool = True, name: str = "s1"):
    return client.post("/api/schedules", json={
        "name": name, "flow_id": FLOW_ID, "version": VER_PUB,
        "cron": CRON, "enabled": enabled,
    }, headers=_headers())


def _detail(r) -> dict:
    body = r.json() or {}
    return body.get("detail") if isinstance(body.get("detail"), dict) else {}


# ============ A2R52-03：暂停调度手动触发，两档统一 409 ============

def test_cluster_paused_manual_trigger_409_schedule_paused(redis):
    """集群档：显式 paused 调度手动触发 → 409 schedule_paused（原 200 入队）。"""
    client = _client(redis)
    _seed_flow(client)
    r = _mk_sched(client, enabled=False, name="paused")
    assert r.status_code == 200, r.text
    sid = r.json()["schedule_id"]

    r2 = client.post(f"/api/schedules/{sid}/trigger", headers=_headers())
    assert r2.status_code == 409, r2.text
    detail = _detail(r2)
    assert detail.get("code") == "schedule_paused", detail
    assert detail.get("schedule_id") == sid

    # 拒绝在受理之前：零入队、零 fire record、调度行原样
    queue_name = schedules_api._task_queue_name()
    assert redis.xlen(queue_name) == 0
    hist = client.get(f"/api/schedules/{sid}/history",
                      headers=_headers()).json()
    assert hist.get("total") == 0, hist
    got = client.get(f"/api/schedules/{sid}", headers=_headers()).json()
    assert got["enabled"] is False


def test_local_paused_manual_trigger_409_schedule_paused(redis):
    """本地档：显式 paused 调度手动触发 → 409 schedule_paused（原 502 误导）。"""
    client = _client(redis, local_mode=True)
    _seed_flow(client)
    r = _mk_sched(client, enabled=True, name="local")
    assert r.status_code == 200, r.text
    sid = r.json()["schedule_id"]
    r2 = client.post(f"/api/schedules/{sid}/disable", headers=_headers())
    assert r2.status_code == 200, r2.text

    r3 = client.post(f"/api/schedules/{sid}/trigger", headers=_headers())
    assert r3.status_code == 409, r3.text
    assert _detail(r3).get("code") == "schedule_paused"

    # 无 fire record（拒绝发生在 local.fire 之前）
    hist = client.get(f"/api/schedules/{sid}/history",
                      headers=_headers()).json()
    assert hist.get("total") == 0, hist


def test_cluster_enabled_manual_trigger_still_200(redis):
    """守恒格：启用调度手动触发照常 200 入队（闸不误伤）。"""
    client = _client(redis)
    _seed_flow(client)
    r = _mk_sched(client, name="live")
    assert r.status_code == 200, r.text
    sid = r.json()["schedule_id"]

    r2 = client.post(f"/api/schedules/{sid}/trigger", headers=_headers())
    assert r2.status_code == 200, r2.text
    body = r2.json()
    assert body["success"] is True and body["mode"] == "cluster"
    assert body["msg_id"], body
    assert redis.xlen(schedules_api._task_queue_name()) == 1


def test_row_without_enabled_key_not_blocked(redis):
    """行缺 enabled 键（最小形状/畸形行）不在此拦——零行为变化。"""
    client = _client(redis)
    raw_sid = "sched-raw-no-enabled"
    redis.hset(
        schedules_api.SCHEDULES_KEY, raw_sid,
        json.dumps({
            "schedule_id": raw_sid, "tenant_id": "default", "name": "raw",
            "flow_id": FLOW_ID, "version": VER_PUB, "cron": CRON,
            "params": {}, "created_at": "t0", "updated_at": "t0",
            "next_run_at": "",
        }),
    )
    r = client.post(f"/api/schedules/{raw_sid}/trigger", headers=_headers())
    assert r.status_code != 409, r.text
    assert r.status_code == 200, r.text


# ============ A2R52-04：配额满 429 具名，其余 None 仍 502 ============

def test_quota_full_manual_trigger_429_tenant_quota_exceeded(redis, monkeypatch):
    """配额满 → 429 tenant_quota_exceeded（与 POST /executions 同契约），
    fire history 留 rejection_reason（真因双通道可见）。"""
    import plaita.server.services.schedule_service as ss

    monkeypatch.setattr(ss, "try_admit_tenant_start",
                        lambda *a, **k: False)
    client = _client(redis)
    _seed_flow(client)
    r = _mk_sched(client, name="quota")
    assert r.status_code == 200, r.text
    sid = r.json()["schedule_id"]

    r2 = client.post(f"/api/schedules/{sid}/trigger", headers=_headers())
    assert r2.status_code == 429, r2.text
    detail = _detail(r2)
    assert detail.get("code") == "tenant_quota_exceeded", detail
    assert detail.get("error") == "tenant_quota_exceeded", detail
    assert detail.get("tenant_id") == "default", detail
    assert "上限" in detail.get("message", ""), detail
    assert "Redis" not in detail.get("message", ""), detail

    hist = client.get(f"/api/schedules/{sid}/history",
                      headers=_headers()).json()
    assert hist.get("total") == 1, hist
    head = hist["records"][0]
    assert head.get("enqueue") == "rejected", head
    assert head.get("rejection_reason") == "tenant_quota_exhausted", head
    # 配额名额与幂等认领已被 fire_schedule 回滚，队列零残留
    assert redis.xlen(schedules_api._task_queue_name()) == 0


def test_local_paused_gate_skips_local_fire(monkeypatch):
    """端点闸拦截时不再触达 local.fire（502 通道死代码化）。"""
    calls = []

    class _Local:
        def get_schedule(self, store, schedule_id, tenant_id=None):
            return {"schedule_id": schedule_id, "tenant_id": "default",
                    "enabled": False}

        def trigger_now(self, store, schedule):
            calls.append(schedule)
            return "exec-should-not-happen"

    monkeypatch.setattr(schedules_api, "_get_local", lambda request: _Local())
    monkeypatch.setattr(schedules_api, "_tenant_is_active",
                        lambda request, tenant: True)
    app = FastAPI()
    app.state.redis = None
    app.state.local_mode = True
    app.include_router(schedules_api.router, prefix="/api")
    client = TestClient(app)

    r = client.post("/api/schedules/s1/trigger")
    assert r.status_code == 409, r.text
    assert calls == []


def test_local_enabled_row_reaches_local_fire(monkeypatch):
    """守恒格：enabled=True 行通过端点闸，照常触达 local.trigger_now。"""
    seen = []

    class _Local:
        def get_schedule(self, store, schedule_id, tenant_id=None):
            return {"schedule_id": schedule_id, "tenant_id": "default",
                    "enabled": True}

        def trigger_now(self, store, schedule):
            seen.append(schedule)
            return "exec-local-1"

    monkeypatch.setattr(schedules_api, "_get_local", lambda request: _Local())
    monkeypatch.setattr(schedules_api, "_tenant_is_active",
                        lambda request, tenant: True)
    app = FastAPI()
    app.state.redis = None
    app.state.local_mode = True
    app.include_router(schedules_api.router, prefix="/api")
    client = TestClient(app)

    body = client.post("/api/schedules/s1/trigger").json()
    assert body["execution_id"] == "exec-local-1", body
    assert len(seen) == 1
