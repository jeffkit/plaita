"""plaita#265：``GET /api/services`` 读面回显基础设施连接串口令。

缺口（基线 ``5ccaf6a``）：``list_services`` 把 ``manager.config.infrastructure``
的 ``url`` 原样塞进合成条目 ``host``（``infra:redis``），内部组件条目同样
回显 ``redis://…`` / DB URL；``/api/services`` 只挂 ``require_auth``，任意
租户 **viewer** 即可读到含口令的连接串——可直接连共享 Redis，绕过租户隔离。

本文件钉住：
1. viewer GET 200，但响应任何字符串都不含口令（``host`` 保留
   scheme/host/port/path/db，userinfo 口令换成 ``***``）；
2. 非敏感服务状态不回归：worker registry 条目的 host/status/metadata 原样
   可见，基础设施条目仍在列表里且 ``status=running``；
3. 详情与拓扑读面走同一序列化层（registry 里若带凭据 URL 同样脱敏）；
4. 平台管理员视角同样脱敏（读面响应脱敏与身份无关，属纵深防御）。

负控（验收时手工执行）：仅 stash ``api/services.py``（保留本文件与
``services/read_redaction.py``）⇒ 第 1/3/4 组断言精确红（口令原样出网），
第 2 组「非敏感状态」仍绿。
"""
from __future__ import annotations

import fnmatch
import json
import sys
from pathlib import Path

import pytest
from fastapi import Depends, FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import auth_users  # noqa: E402
from api import services as services_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import flow_store, tenants_svc, users_svc  # noqa: E402

TENANT = "acme"
VIEWER_PW = "viewer-password-1"
ADMIN_PW = "admin-password-1"
INFRA_PW = "s3cret-pass"
DB_PW = "db-pass"
REGISTRY_PW = "reg-pass"

INFRA_URL = f"redis://:{INFRA_PW}@10.0.0.5:6379/0"
STORAGE_URL = f"postgresql://plaita:{DB_PW}@10.0.0.7:5432/plaita"
REGISTRY_URL = f"redis://:{REGISTRY_PW}@10.0.0.8:6379/0"
WORKER_HOST = "worker-1.internal:9000"


class FakeRedis:
    """最小 Redis 替身：registry 读面只用到 scan_iter/get/keys。"""

    def __init__(self, data=None):
        self.data = dict(data or {})

    def scan_iter(self, match=None, count=None):  # noqa: ARG002
        for key in list(self.data):
            if match is None or fnmatch.fnmatch(key, match):
                yield key

    def keys(self, pattern):
        return [
            key for key in self.data
            if fnmatch.fnmatch(key, pattern)
        ]

    def get(self, key):
        return self.data.get(key)


class _InfraConfig:
    enabled = True
    url = INFRA_URL
    type = "redis"
    display_name = "Redis"


class _StorageConfig:
    type = "db"
    url = STORAGE_URL


class _Config:
    infrastructure = {"redis": _InfraConfig()}
    eventbus = None
    queue = None
    storage = _StorageConfig()
    mode = "process"


class FakeManager:
    config = _Config()

    def list_instances(self):
        return []


def _worker_registry(instance_id: str = "worker-1", host: str = WORKER_HOST) -> str:
    return json.dumps({
        "instance_id": instance_id,
        "service_type": "flow_worker",
        "host": host,
        "status": "running",
        "start_time": "2026-10-19T00:00:00",
        "metadata": {"queue_name": "plaita:flow:queue"},
        "active_tasks": 0,
        "last_heartbeat": None,
    })


def _settings(admin_key: str = ""):
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = admin_key
    s.allow_insecure_admin = False
    return s


def _build_app(tmp_path, monkeypatch) -> FastAPI:
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{tmp_path}/issue265.db")
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(tmp_path / "creds.json"))
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(tmp_path / "creds.key"))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())

    flow_store.init_engine(f"sqlite:///{tmp_path}/issue265.db")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()
    tenants_svc.create_tenant(store, TENANT, "Acme")
    users_svc.create_user(store, "viewer1", VIEWER_PW, "viewer")
    users_svc.add_member(store, TENANT, "viewer1", "viewer")
    users_svc.create_user(store, "root", ADMIN_PW, "admin", platform_admin=True)
    users_svc.add_member(store, "default", "root", "admin")

    redis = FakeRedis({
        "plaita:registry:flow_worker:worker-1": _worker_registry(),
        "plaita:registry:flow_worker:worker-2": _worker_registry(
            "worker-2", REGISTRY_URL
        ),
    })
    monkeypatch.setattr(
        "services.service_manager.get_service_manager", lambda: FakeManager()
    )

    app = FastAPI()
    app.state.store = store
    app.state.local_mode = False
    app.state.redis = redis
    app.include_router(auth_users.router, prefix="/api")
    app.include_router(
        services_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    return app


@pytest.fixture()
def client(tmp_path, monkeypatch) -> TestClient:
    return TestClient(_build_app(tmp_path, monkeypatch))


def _login(client: TestClient, username: str, password: str) -> str:
    r = client.post(
        "/api/auth/login", json={"username": username, "password": password}
    )
    assert r.status_code == 200, r.text
    return r.json()["token"]


def _auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _by_id(services, instance_id):
    return next(s for s in services if s["instance_id"] == instance_id)


def test_viewer_list_redacts_infra_and_component_passwords(client):
    token = _login(client, "viewer1", VIEWER_PW)
    r = client.get("/api/services", headers=_auth(token))
    assert r.status_code == 200, r.text
    body = r.text
    for secret in (INFRA_PW, DB_PW, REGISTRY_PW):
        assert secret not in body

    services = r.json()["services"]
    infra = _by_id(services, "infra:redis")
    assert infra["host"] == "redis://:***@10.0.0.5:6379/0"
    assert infra["status"] == "running"

    storage = _by_id(services, "component:storage")
    assert storage["host"] == "postgresql://plaita:***@10.0.0.7:5432/plaita"
    assert storage["status"] == "running"


def test_viewer_non_sensitive_service_state_not_regressed(client):
    token = _login(client, "viewer1", VIEWER_PW)
    r = client.get("/api/services", headers=_auth(token))
    assert r.status_code == 200, r.text
    services = r.json()["services"]

    worker = _by_id(services, "worker-1")
    assert worker["host"] == WORKER_HOST
    assert worker["status"] == "running"
    assert worker["metadata"] == {"queue_name": "plaita:flow:queue"}


def test_detail_read_surface_redacts_registry_credentials(client):
    token = _login(client, "viewer1", VIEWER_PW)
    r = client.get("/api/services/worker-2", headers=_auth(token))
    assert r.status_code == 200, r.text
    assert REGISTRY_PW not in r.text
    assert r.json()["host"] == "redis://:***@10.0.0.8:6379/0"
    assert r.json()["status"] == "running"


def test_topology_read_surface_redacts_registry_credentials(client):
    token = _login(client, "viewer1", VIEWER_PW)
    r = client.get("/api/services/topology", headers=_auth(token))
    assert r.status_code == 200, r.text
    assert REGISTRY_PW not in r.text
    nodes = {n["instance_id"]: n for n in r.json()["nodes"]}
    assert nodes["worker-2"]["host"] == "redis://:***@10.0.0.8:6379/0"
    assert nodes["worker-1"]["host"] == WORKER_HOST


def test_platform_admin_list_is_redacted_too(client):
    token = _login(client, "root", ADMIN_PW)
    r = client.get("/api/services", headers=_auth(token))
    assert r.status_code == 200, r.text
    assert INFRA_PW not in r.text
    assert DB_PW not in r.text
    infra = _by_id(r.json()["services"], "infra:redis")
    assert infra["host"] == "redis://:***@10.0.0.5:6379/0"
