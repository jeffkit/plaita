"""「导入本地定时」迁移刀（2026-10-25）：POST /api/schedules/import-local。

钉五件事：
1. **集群档专用**：本地单机档（redis=None / local_mode=True）409 可操作错误；
2. **正路径**：SQLite 本地条目 → Redis hash（schedule_service 同一视图），
   保留原 schedule_id、next_run_at 重算、runtime 与 start 同源盖章（#300）；
3. **原地停用**：导入成功后本地行 enabled=False（保行保历史，切回本地档
   仍可见——不静默丢排班）；
4. **幂等**：重跑只看 enabled 行 → 已导入条目走 skipped_existing（hsetnx
   兜底不覆盖）；
5. **坏条目点名不挡批**：cron 非法/流程不可用逐条 skipped_invalid。

本地行的手工重建账单出处：deployment.md「以集群档启动时不启动本地调度循环
……请在集群档按相同 cron 重建后删除本地条目」——本端点即该账单的自动化。
"""
import sys
from pathlib import Path

import pytest
from fakeredis import FakeRedis
from fastapi import Depends, FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import schedules as schedules_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import flow_store  # noqa: E402
from services import schedule_store as local_schedule_store  # noqa: E402
from plaita.server.services.schedule_service import SCHEDULES_KEY  # noqa: E402

ADMIN_KEY = "import-local-admin"


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


GOOD_DEF = __import__("json").dumps({
    "flow_id": "hello",
    "nodes": [
        {"type": "start", "id": "start", "next": "end"},
        {"type": "end", "id": "end", "resultType": "success", "output": "ok"},
    ],
})


@pytest.fixture()
def env(tmp_path, monkeypatch):
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    flow_store.init_engine(f"sqlite:///{tmp_path / 'import-local.db'}")
    store = flow_store.get_flow_store()
    store.ensure_flow("hello", tenant_id="default")
    store.save_flow_definition("hello", "1.0.0", GOOD_DEF, status="draft", tenant_id="default")
    store.publish_version("hello", "1.0.0", tenant_id="default")

    redis = FakeRedis(decode_responses=True)
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False  # 集群档
    app.state.store = store
    app.include_router(schedules_api.router, prefix="/api", dependencies=[Depends(require_auth)])
    client = TestClient(app)
    headers = {"X-Admin-API-Key": ADMIN_KEY}
    return client, redis, store, headers


def _mk_local(store, schedule_id, *, enabled=True, cron="* * * * *", flow_id="hello"):
    return local_schedule_store.create_schedule(store, {
        "schedule_id": schedule_id,
        "tenant_id": "default",
        "name": f"本地-{schedule_id}",
        "flow_id": flow_id,
        "version": "1.0.0",
        "cron": cron,
        "params": {"k": "v"},
        "enabled": enabled,
        "created_by": "local-user",
        "next_run_at": "1791530071848" if enabled else "",
    })


def test_local_mode_rejected_with_actionable_error(tmp_path, monkeypatch):
    """本地单机档（redis=None）：409 + 指路文案（本地调度本就在本进程跑）。"""
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    flow_store.init_engine(f"sqlite:///{tmp_path / 'local-mode.db'}")
    app = FastAPI()
    app.state.redis = None
    app.state.local_mode = True
    app.include_router(schedules_api.router, prefix="/api", dependencies=[Depends(require_auth)])
    client = TestClient(app)
    r = client.post(
        "/api/schedules/import-local",
        json={},
        headers={"X-Admin-API-Key": ADMIN_KEY},
    )
    assert r.status_code == 409, r.text
    assert r.json()["detail"]["code"] == "local_mode"


def test_import_local_schedules_end_to_end(env):
    """正路径：本地条目迁入 Redis（保留 id/runtime 盖章/next_run_at 重算），
    本地行原地停用；集群调度列表可见。"""
    client, redis, store, headers = env
    _mk_local(store, "sched-local-1")
    _mk_local(store, "sched-local-2", cron="0 9 * * 1")

    r = client.post("/api/schedules/import-local", json={}, headers=headers)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["imported_count"] == 2
    assert sorted(body["imported"]) == ["sched-local-1", "sched-local-2"]
    assert body["candidates"] == 2

    # 集群侧：schedule_service 同一 hash 视图
    for sid in ("sched-local-1", "sched-local-2"):
        raw = redis.hget(SCHEDULES_KEY, sid)
        assert raw, f"{sid} 未写入集群 hash"
        record = __import__("json").loads(raw)
        assert record["flow_id"] == "hello"
        assert record["version"] == "1.0.0"
        assert record["params"] == {"k": "v"}
        assert record["enabled"] is True
        assert record["tenant_id"] == "default"
        assert record["runtime"] == "python"  # #300 池路由盖章
        assert record["next_run_at"] and float(record["next_run_at"]) > 0  # 重算非本地旧值

    # 本地侧：原地停用（保行，切回本地档仍可见）
    row = local_schedule_store.get_schedule(store, "sched-local-1", tenant_id="default")
    assert row is not None, "本地行被删了（应保留停用）"
    assert row["enabled"] is False

    # 集群调度列表可见（与手建同视图）
    listing = client.get("/api/schedules", headers=headers).json()
    ids = {s["schedule_id"] for s in listing} if isinstance(listing, list) else {
        s["schedule_id"] for s in listing.get("items", listing.get("schedules", []))
    }
    assert {"sched-local-1", "sched-local-2"} <= ids


def test_import_is_idempotent_rerun_skips_existing(env):
    """幂等：重跑只看 enabled 行 → 已导入（已停用）条目不再出现；hsetnx
    兜底不覆盖集群侧已有同 id 条目。"""
    client, redis, store, headers = env
    _mk_local(store, "sched-idem")
    r1 = client.post("/api/schedules/import-local", json={}, headers=headers)
    assert r1.json()["imported_count"] == 1

    # 模拟运维在集群侧改过导入条目
    raw = __import__("json").loads(redis.hget(SCHEDULES_KEY, "sched-idem"))
    raw["name"] = "集群侧改过名"
    redis.hset(SCHEDULES_KEY, "sched-idem", __import__("json").dumps(raw, ensure_ascii=False))

    r2 = client.post("/api/schedules/import-local", json={}, headers=headers)
    body = r2.json()
    assert body["imported_count"] == 0
    assert body["candidates"] == 0  # 本地行已停用 → 不再是候选
    assert __import__("json").loads(redis.hget(SCHEDULES_KEY, "sched-idem"))["name"] == "集群侧改过名"


def test_import_invalid_entries_named_not_blocking(env):
    """坏条目逐条点名（cron 非法/流程不存在），好条目照常导入。"""
    client, redis, store, headers = env
    _mk_local(store, "sched-bad-cron", cron="not a cron")
    _mk_local(store, "sched-missing-flow", flow_id="ghost")
    _mk_local(store, "sched-good")

    r = client.post("/api/schedules/import-local", json={}, headers=headers)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["imported"] == ["sched-good"]
    reasons = {item["schedule_id"]: item["reason"] for item in body["skipped_invalid"]}
    assert reasons["sched-bad-cron"] == "invalid_cron"
    assert reasons["sched-missing-flow"] == "flow_unavailable"
    # 坏条目保持本地启用原状（不吞不删，运维自行处置）
    assert local_schedule_store.get_schedule(store, "sched-bad-cron", tenant_id="default")["enabled"] is True
