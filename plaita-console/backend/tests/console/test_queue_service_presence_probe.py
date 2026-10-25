"""队列服务点名（2026-10-25「现场盘点」）：managed:queue 台阶的环境侧真查。

钉四格：
1. 集群档 start：flow 含 delay 且注册表**无** delay 活实例 → 422
   ``queue_service_absent``（修「集群档假设服务在岗」的洞——此前只查
   节点集支持性，服务死透也照单入队 → 投递成功无人消费 → 静默搁浅）；
2. 注册表**有**活实例（TTL 内键）→ 放行（正控，入队照常）；
3. event flow（managed 台阶，不依赖队列服务）→ 点名不触发、放行——
   不误伤事件挂起面；
4. 本地档：静态拒（_gate_local_execution_profile 既有格）不变——点名
   只接管集群档，两档行为各自稳定。

fail-open（probe 查不动放行 + WARNING）由单元级直接断言；注册键 TTL 30s
滞后为已知精度（docstring 声明）。
"""
import json
import sys
from pathlib import Path

import pytest
from fakeredis import FakeRedis
from fastapi import Depends, FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import executions as executions_api  # noqa: E402
from api import flows as flows_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import flow_store  # noqa: E402

ADMIN_KEY = "queue-probe-admin"


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


def _def(flow_id, node_type):
    return json.dumps({
        "flow_id": flow_id,
        "nodes": [
            {"type": "start", "id": "start", "next": "x"},
            {"type": node_type, "id": "x",
             **({"delay_seconds": 30} if node_type == "delay" else {}),
             **({"event_type": "go"} if node_type == "event" else {}),
             "next": "end"},
            {"type": "end", "id": "end", "output": "x"},
        ],
    })


@pytest.fixture()
def env(tmp_path, monkeypatch):
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    flow_store.init_engine(f"sqlite:///{tmp_path / 'queue-probe.db'}")
    store = flow_store.get_flow_store()
    for fid, nt in (("delayed", "delay"), ("waiter", "event")):
        store.ensure_flow(fid, tenant_id="default")
        store.save_flow_definition(fid, "1.0.0", _def(fid, nt), status="published",
                                   tenant_id="default")
    redis = FakeRedis(decode_responses=True)
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.state.store = store
    app.include_router(flows_api.router, prefix="/api", dependencies=[Depends(require_auth)])
    app.include_router(executions_api.router, prefix="/api", dependencies=[Depends(require_auth)])
    return client_of(TestClient(app)), redis, headers_of()


def client_of(client):
    return client


def headers_of():
    return {"X-Admin-API-Key": ADMIN_KEY}


def _register(redis, service_type):
    """伪造一个活实例注册键（TTL 内 = 活着）。"""
    redis.set(f"plaita:registry:{service_type}:i1", "{}", ex=30)


def test_cluster_start_delay_rejected_when_service_absent(env):
    """负格：delay flow × 注册表无 delay 实例 → 422 queue_service_absent。"""
    client, redis, headers = env
    r = client.post("/api/executions", json={"flow_id": "delayed"}, headers=headers)
    assert r.status_code == 422, r.text
    detail = r.json()["detail"]
    assert detail["code"] == "queue_service_absent"
    assert detail["node_type"] == "delay"
    assert "无人消费" in detail["message"]


def test_cluster_start_delay_passes_when_service_registered(env):
    """正格：注册表有 delay 活实例 → 放行（入队/受理照常）。"""
    client, redis, headers = env
    _register(redis, "delay")
    r = client.post("/api/executions", json={"flow_id": "delayed"}, headers=headers)
    assert r.status_code == 200, r.text


def test_event_flow_not_blocked_by_probe(env):
    """event flow（managed 台阶）：点名不触发、放行——不误伤事件挂起面。"""
    client, redis, headers = env
    r = client.post("/api/executions", json={"flow_id": "waiter"}, headers=headers)
    assert r.status_code == 200, r.text


def test_probe_fail_open_on_redis_error(env, monkeypatch):
    """查不动 fail-open：probe 抛异常 → 放行 + WARNING（降级可观测红线）。"""
    client, redis, headers = env

    def boom(pattern, count=100):
        raise ConnectionError("redis down")

    monkeypatch.setattr(redis, "scan_iter", boom)
    r = client.post("/api/executions", json={"flow_id": "delayed"}, headers=headers)
    assert r.status_code == 200, r.text  # fail-open 放行


def test_local_tier_static_rejection_unchanged(tmp_path, monkeypatch):
    """本地档静态拒不变：delay flow 在 redis=None 下仍走 local 档具名拒。"""
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    flow_store.init_engine(f"sqlite:///{tmp_path / 'local-unchanged.db'}")
    store = flow_store.get_flow_store()
    store.ensure_flow("delayed", tenant_id="default")
    store.save_flow_definition("delayed", "1.0.0", _def("delayed", "delay"),
                               status="published", tenant_id="default")
    app = FastAPI()
    app.state.redis = None
    app.state.local_mode = True
    app.state.store = store
    app.include_router(executions_api.router, prefix="/api", dependencies=[Depends(require_auth)])
    client = TestClient(app)
    r = client.post("/api/executions", json={"flow_id": "delayed"},
                    headers={"X-Admin-API-Key": ADMIN_KEY})
    assert r.status_code == 422, r.text
    assert r.json()["detail"]["code"] == "suspend_not_executable"
