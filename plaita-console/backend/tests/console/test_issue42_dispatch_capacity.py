"""#42（W10-a）console 派发决策面：容量只读列表 + 影子派单计划。

## 红先证

基线（origin/main 87b8ad0）console 没有任何派发决策面：`api/` 下无
``dispatch/capacity`` / ``dispatch/plan`` 路由，``services/`` 下无
``dispatcher.py``；`services.py` 的 ``/services`` 只透出 registry 原样的
``active_tasks`` / ``metadata``——**没有可派性判定，也没有拒绝原因**。
故本组首个断言（低盘 worker 被标不可派）在基线必然失败。

## 断言要点（对应验收 2 / 5）

- 低盘 worker：``dispatchable=false`` + ``reject_reason`` 含 ``disk``；
- 影子计划：``enqueued == 0``、``shadow == true``、目标落在健康 worker 上；
- 生产队列零新增：调用前后 Redis 键集合完全一致（**只算不发**）；
- 负向对照：未上报容量的存量 worker 仍可派（红线：不因上报缺失停摆）。
"""
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi import Depends, FastAPI
from fakeredis import FakeRedis
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
PLAITA_ROOT = str(BACKEND_DIR.parents[1])
for _p in (str(BACKEND_DIR), PLAITA_ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from api import capacity as capacity_api  # noqa: E402
from auth import require_auth  # noqa: E402

from plaita.server.capacity import CAPACITY_METADATA_KEY  # noqa: E402
from plaita.server.registry import ServiceInfo, ServiceRegistry  # noqa: E402

ADMIN_KEY = "w10a-admin-key"
JOBS_KEY = "plaita:jobs"
QUEUE_KEY = "plaita:flow:queue"


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


def _capacity_block(**over):
    block = {
        "schema_version": 1,
        "free_disk_gib": 120.0,
        "min_free_disk_gib": 20.0,
        "disk_blocked": False,
        "active_tasks": 0,
        "concurrency": 1,
        "sampled_at": datetime.now(timezone.utc).isoformat(),
    }
    block.update(over)
    return block


def _register_worker(redis, instance_id, *, block=None, status="running"):
    reg = ServiceRegistry(redis)
    info = ServiceInfo(
        instance_id=instance_id,
        service_type="flow_worker",
        host=f"{instance_id}.host",
        metadata=(
            {"queue_name": QUEUE_KEY}
            if block is None
            else {"queue_name": QUEUE_KEY, CAPACITY_METADATA_KEY: json.dumps(block)}
        ),
    )
    reg.register(info)
    if status != "running":
        # register() 强制 status=running；直接改盘上 JSON 以造 draining 态
        key = f"plaita:registry:flow_worker:{instance_id}"
        data = json.loads(redis.get(key))
        data["status"] = status
        redis.set(key, json.dumps(data))
    return info


@pytest.fixture()
def redis():
    return FakeRedis(decode_responses=True)


@pytest.fixture()
def client(redis, monkeypatch):
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.include_router(
        capacity_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    return TestClient(app)


def _headers():
    return {"X-Admin-API-Key": ADMIN_KEY}


class TestCapacityList:
    def test_low_disk_worker_reported_as_not_dispatchable(self, client, redis):
        """验收 2 的可见面：低盘 worker 必须被判不可派并给出原因。"""
        _register_worker(redis, "w-low", block=_capacity_block(
            free_disk_gib=3.2, min_free_disk_gib=20.0, disk_blocked=True,
        ))
        _register_worker(redis, "w-ok", block=_capacity_block(free_disk_gib=120.0))

        resp = client.get("/api/dispatch/capacity", headers=_headers())
        assert resp.status_code == 200, resp.text
        rows = {r["instance_id"]: r for r in resp.json()}
        assert set(rows) == {"w-low", "w-ok"}
        assert rows["w-low"]["dispatchable"] is False
        assert "disk" in rows["w-low"]["reject_reason"]
        assert rows["w-ok"]["dispatchable"] is True
        assert rows["w-ok"]["reject_reason"] is None

    def test_legacy_worker_without_capacity_is_dispatchable(self, client, redis):
        """负向对照（红线）：存量 worker 未上报容量 → 仍可派，不被停摆。"""
        _register_worker(redis, "w-legacy", block=None)
        resp = client.get("/api/dispatch/capacity", headers=_headers())
        rows = resp.json()
        assert len(rows) == 1
        assert rows[0]["capacity"] is None
        assert rows[0]["dispatchable"] is True

    def test_stale_snapshot_is_flagged(self, client, redis):
        old = (datetime.now(timezone.utc) - timedelta(seconds=3600)).isoformat()
        _register_worker(redis, "w-stale", block=_capacity_block(sampled_at=old))
        resp = client.get("/api/dispatch/capacity", headers=_headers())
        row = resp.json()[0]
        assert row["capacity_stale"] is True
        assert row["capacity_age_seconds"] > 300
        assert row["dispatchable"] is True  # 陈旧不采信内容，但不踢出池

    def test_draining_worker_not_dispatchable(self, client, redis):
        _register_worker(redis, "w-drain", block=_capacity_block(), status="draining")
        resp = client.get("/api/dispatch/capacity", headers=_headers())
        row = resp.json()[0]
        assert row["dispatchable"] is False
        assert row["reject_reason"] == "draining"

    def test_empty_registry_returns_empty_list_not_500(self, client):
        resp = client.get("/api/dispatch/capacity", headers=_headers())
        assert resp.status_code == 200
        assert resp.json() == []

    def test_local_mode_without_redis_returns_503(self, monkeypatch):
        monkeypatch.setattr("config.get_settings", lambda: _settings())
        monkeypatch.setattr("auth.get_settings", lambda: _settings())
        app = FastAPI()
        app.state.redis = None
        app.state.local_mode = True
        app.include_router(
            capacity_api.router, prefix="/api", dependencies=[Depends(require_auth)]
        )
        resp = TestClient(app).get("/api/dispatch/capacity", headers=_headers())
        assert resp.status_code == 503

    def test_requires_admin_key(self, client):
        assert client.get("/api/dispatch/capacity").status_code == 401


class TestShadowPlan:
    def test_plan_skips_low_disk_and_picks_healthy(self, client, redis):
        _register_worker(redis, "w-low", block=_capacity_block(
            free_disk_gib=2.0, min_free_disk_gib=20.0, disk_blocked=True,
        ))
        _register_worker(redis, "w-ok", block=_capacity_block(free_disk_gib=120.0))

        resp = client.post(
            "/api/dispatch/plan",
            headers=_headers(),
            json={"issue_ids": ["issue-1", "issue-2"]},
        )
        assert resp.status_code == 200, resp.text
        plan = resp.json()
        assert plan["target"] == "w-ok"
        assert plan["allowed"] == ["w-ok"]
        assert plan["rejected"] == [
            {"instance_id": "w-low", "host": "w-low.host", "reason": plan["rejected"][0]["reason"]}
        ]
        assert "disk" in plan["rejected"][0]["reason"]

    def test_shadow_plan_never_enqueues(self, client, redis):
        """验收 5：影子模式只算不发——生产队列与派发台账零新增。"""
        _register_worker(redis, "w-ok", block=_capacity_block(free_disk_gib=120.0))
        redis.xadd(JOBS_KEY, {"payload": "pre-existing"})
        before = {k for k in redis.scan_iter()}

        resp = client.post(
            "/api/dispatch/plan",
            headers=_headers(),
            json={"issue_ids": ["issue-1"]},
        )
        assert resp.status_code == 200, resp.text
        plan = resp.json()
        after = {k for k in redis.scan_iter()}

        assert plan["shadow"] is True
        assert plan["enqueued"] == 0
        assert before == after, "影子决策不得新增任何 Redis 键"
        assert redis.xlen(QUEUE_KEY) == 0, "生产队列零新增"
        assert redis.xlen(JOBS_KEY) == 1, "台账队列零新增"

    def test_plan_is_deterministic_for_same_input(self, client, redis):
        """验收 1 的 console 侧内核：同输入同输出。"""
        _register_worker(redis, "w-a", block=_capacity_block(free_disk_gib=40.0))
        _register_worker(redis, "w-b", block=_capacity_block(free_disk_gib=40.0))
        first = client.post(
            "/api/dispatch/plan", headers=_headers(), json={"issue_ids": ["i-1"]}
        ).json()
        second = client.post(
            "/api/dispatch/plan", headers=_headers(), json={"issue_ids": ["i-1"]}
        ).json()
        assert first["target"] == second["target"] == "w-a"
        assert first["allowed"] == second["allowed"] == ["w-a", "w-b"]

    def test_plan_reports_none_target_when_every_worker_blocked(self, client, redis):
        _register_worker(redis, "w-only", block=_capacity_block(
            free_disk_gib=0.5, min_free_disk_gib=20.0, disk_blocked=True,
        ))
        plan = client.post(
            "/api/dispatch/plan", headers=_headers(), json={"issue_ids": ["i-1"]}
        ).json()
        assert plan["target"] is None
        assert plan["allowed"] == []
        assert plan["enqueued"] == 0

    def test_plan_refuses_no_queue_override(self, client, redis, monkeypatch):
        monkeypatch.setenv("PLAITA_CONSOLE_TASK_QUEUE", "plaita:flow:queue:v2")
        _register_worker(redis, "w-ok", block=_capacity_block())
        plan = client.post(
            "/api/dispatch/plan", headers=_headers(), json={"issue_ids": []}
        ).json()
        assert plan["queue"] == "plaita:flow:queue:v2"

    def test_plan_queue_override_in_body_wins(self, client, redis, monkeypatch):
        monkeypatch.setenv("PLAITA_CONSOLE_TASK_QUEUE", "plaita:flow:queue")
        _register_worker(redis, "w-ok", block=_capacity_block())
        plan = client.post(
            "/api/dispatch/plan",
            headers=_headers(),
            json={"issue_ids": [], "queue_name": "plaita:flow:queue:canary"},
        ).json()
        assert plan["queue"] == "plaita:flow:queue:canary"

    def test_plan_shadow_can_be_overridden_explicitly(self, client, redis):
        """显式 shadow=false 也只是「算得完整」——本接口仍然不发单。"""
        _register_worker(redis, "w-ok", block=_capacity_block())
        resp = client.post(
            "/api/dispatch/plan",
            headers=_headers(),
            json={"issue_ids": ["i-1"], "shadow": False},
        )
        plan = resp.json()
        assert plan["shadow"] is False
        assert plan["enqueued"] == 0

    def test_saturated_worker_excluded_from_plan(self, client, redis):
        _register_worker(redis, "w-full", block=_capacity_block(active_tasks=1, concurrency=1))
        _register_worker(redis, "w-free", block=_capacity_block(active_tasks=0, concurrency=1))
        plan = client.post(
            "/api/dispatch/plan", headers=_headers(), json={"issue_ids": ["i-1"]}
        ).json()
        assert plan["target"] == "w-free"
        assert plan["rejected"][0]["instance_id"] == "w-full"
