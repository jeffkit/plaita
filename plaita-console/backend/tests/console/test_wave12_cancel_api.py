"""波次① BFF cancel 端点改造契约测试（设计稿 §3.1，集群档）。

- 运行中执行：只写取消标志键（7 天 TTL）+ 入队 cancel 消息，**不再直写**
  status=cancelled；
- 挂起执行：保持现状——直接写 cancelled + 入队（已验证路径，§3.5 红线）；
- 终态幂等保持；取消标志键不出现在执行列表/详情扫描（机制键排除）。
"""
import json
import sys
import time
from datetime import datetime
from pathlib import Path

import pytest
from fastapi import Depends, FastAPI
from fakeredis import FakeRedis
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import executions as executions_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import flow_store  # noqa: E402

ADMIN_KEY = "cluster-admin-key"


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
    flow_store.init_engine(f"sqlite:///{tmp_path}/wave12-cancel.db")
    return FakeRedis(decode_responses=True)


def _client(redis) -> TestClient:
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.state.store = flow_store.get_flow_store()
    app.include_router(
        executions_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    return TestClient(app)


def _headers():
    return {"X-Admin-API-Key": ADMIN_KEY}


def _put_execution(redis, execution_id, status):
    info = {
        "execution_id": execution_id,
        "flow_id": "f1",
        "flow_version": "1",
        "status": status,
        "tenant_id": "default",
        "start_time": datetime.utcnow().isoformat(),
        "context": {},
    }
    redis.set(f"plaita:execution:{execution_id}", json.dumps(info))
    return info


def _queued_messages(redis):
    out = []
    for _id, fields in redis.xrange("plaita:flow:queue"):
        payload = fields.get("payload")
        if isinstance(payload, bytes):
            payload = payload.decode()
        out.append(json.loads(payload))
    return out


def test_running_execution_cancel_writes_flag_not_status(env):
    redis = env
    client = _client(redis)
    _put_execution(redis, "exec-run", "running")

    r = client.post("/api/executions/exec-run/cancel", headers=_headers())
    assert r.status_code == 200
    assert r.json()["success"] is True

    # 意图键落盘，带 TTL（7 天自清理）
    flag = redis.get("plaita:execution:cancel:exec-run")
    assert flag is not None
    ttl = redis.ttl("plaita:execution:cancel:exec-run")
    assert 0 < ttl <= 7 * 86400

    # 状态不再被直写 cancelled（覆写战争起点移除）
    assert json.loads(redis.get("plaita:execution:exec-run"))["status"] == "running"

    # cancel 消息仍入队（worker 全灭后的死人开关）
    messages = _queued_messages(redis)
    assert any(
        m.get("type") == "resume" and m.get("resume_type") == "cancel"
        and m.get("execution_id") == "exec-run"
        for m in messages
    )


def test_suspended_execution_cancel_keeps_direct_write(env):
    """挂起执行取消保持现状（已验证路径）：直写 cancelled + 入队，无标志键。"""
    redis = env
    client = _client(redis)
    _put_execution(redis, "exec-susp", "suspended")

    r = client.post("/api/executions/exec-susp/cancel", headers=_headers())
    assert r.status_code == 200

    assert json.loads(redis.get("plaita:execution:exec-susp"))["status"] == "cancelled"
    assert json.loads(redis.get("plaita:execution:exec-susp"))["end_time"] is not None
    assert redis.get("plaita:execution:cancel:exec-susp") is None
    assert any(
        m.get("resume_type") == "cancel" and m.get("execution_id") == "exec-susp"
        for m in _queued_messages(redis)
    )


def test_terminal_execution_cancel_is_idempotent(env):
    redis = env
    client = _client(redis)
    _put_execution(redis, "exec-done", "completed")

    r = client.post("/api/executions/exec-done/cancel", headers=_headers())
    assert r.status_code == 200
    assert "忽略取消" in r.json()["message"]
    assert redis.get("plaita:execution:cancel:exec-done") is None
    assert _queued_messages(redis) == []


def test_cancel_flag_keys_excluded_from_list_and_find(env):
    """取消标志键是机制键：列表/跨租户定位不得把它当执行记录（防 500）。"""
    redis = env
    client = _client(redis)
    _put_execution(redis, "exec-x", "running")
    redis.set("plaita:execution:cancel:exec-x", "2026-10-02T00:00:00", ex=604800)

    r = client.get("/api/executions", headers=_headers())
    assert r.status_code == 200
    ids = [e["execution_id"] for e in r.json()["executions"]]
    assert ids == ["exec-x"]  # 只有执行记录，没有 cancel 键

    # 平台视角 _find_execution 的 scan 分支同样不命中 cancel 键
    tenant, data = executions_api._find_execution(None, redis, "exec-x")
    assert data["status"] == "running"


def test_tenant_scoped_cancel_writes_tenant_namespace_flag(env):
    redis = env
    client = _client(redis)
    info = _put_execution(redis, "exec-t", "running")
    redis.delete(f"plaita:execution:exec-t")
    redis.set("plaita:tenant-a:execution:exec-t", json.dumps(info))
    redis.set("plaita:tenant-a:execution:cancel:exec-t", "old", ex=60)

    r = client.post(
        "/api/executions/exec-t/cancel",
        headers={**_headers(), "X-Tenant-ID": "tenant-a"},
    )
    assert r.status_code == 200
    assert redis.get("plaita:tenant-a:execution:cancel:exec-t") is not None
    assert redis.get("plaita:tenant-a:execution:cancel:exec-t") != "old"
    # default namespace 未被误写
    assert redis.get("plaita:execution:cancel:exec-t") is None


def test_delete_execution_also_clears_cancel_flag(env):
    redis = env
    client = _client(redis)
    _put_execution(redis, "exec-del", "cancelled")
    redis.set("plaita:execution:cancel:exec-del", "ts", ex=604800)

    r = client.delete("/api/executions/exec-del", headers=_headers())
    assert r.status_code == 200
    assert redis.get("plaita:execution:cancel:exec-del") is None
