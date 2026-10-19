"""A55-S1：cluster 19 个非 GET 端点的**成功路径**全部写审计。

此前只有 ``ensure_platform_admin`` 的 ``*.denied``；真正做成的全局操作
（启停服务、切模式、改基础设施/集群配置、切换数据面）零留痕。

验收：19 个端点各一条 ``cluster.<action>``；tenant_id=""（全局）；detail
只含元数据，URL/secret 永不入库。
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import Depends, FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import auth_users  # noqa: E402
from api import cluster as cluster_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import audit as audit_svc  # noqa: E402
from services import cluster_registry as cluster_registry_module  # noqa: E402
from services import flow_store, users_svc  # noqa: E402
from services.service_manager import InfrastructureConfig  # noqa: E402

ROOT_PW = "root-password-1"
ADMIN_KEY = "r55-cluster-key"
SECRET = "R55-SECRET-PASSWORD"
NEW_SECRET = "R55-NEW-SECRET"


class FakeManager:
    def __init__(self, tmp_path: Path):
        self.config = SimpleNamespace(mode="process")
        self.config_path = str(tmp_path / "cluster_config.yaml")
        self.calls: list = []
        self.infrastructure = {
            "redis": InfrastructureConfig(
                name="redis", display_name="Redis", type="redis", enabled=True,
                url=f"redis://:{SECRET}@10.0.0.9:6379/0",
            ),
        }
        self.removed: list = []

    async def start_service(self, service_type):
        return SimpleNamespace(
            status="running", instance_id=f"{service_type}-1", error_message=None)

    async def stop_service(self, instance_id, graceful):
        return True

    async def stop_all(self, graceful):
        return {"inst-1": True}

    def get_infrastructure(self):
        return dict(self.infrastructure)

    def load_config(self):
        pass

    async def check_infrastructure_health(self):
        return {name: {"status": "healthy", "details": None}
                for name in self.infrastructure}

    async def refresh_status(self):
        return None

    async def add_infrastructure(self, **kwargs):
        self.infrastructure[kwargs["name"]] = InfrastructureConfig(
            name=kwargs["name"], display_name=kwargs["display_name"],
            type=kwargs["type"], enabled=kwargs.get("enabled", True),
            url=kwargs.get("url"), bootstrap_servers=kwargs.get("bootstrap_servers"),
            docker=kwargs.get("docker") or {},
        )
        return True

    async def update_infrastructure(self, **kwargs):
        config = self.infrastructure[kwargs["name"]]
        for field in ("display_name", "enabled", "url", "bootstrap_servers", "docker"):
            if kwargs.get(field) is not None:
                setattr(config, field, kwargs[field])
        return True

    async def delete_infrastructure(self, name):
        self.infrastructure.pop(name, None)
        return True

    async def start_infrastructure(self, name):
        return {"success": True, "name": name, "status": "running"}

    async def stop_infrastructure(self, name):
        return {"success": True, "name": name, "status": "stopped"}

    def remove_instance(self, instance_id):
        self.removed.append(instance_id)
        return True

    def clear_failed_instances(self):
        return 3


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


async def _noop_rebind(app, redis_url):  # noqa: ARG001
    return None


@pytest.fixture()
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{tmp_path}/r55c.db")
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    flow_store.init_engine(f"sqlite:///{tmp_path}/r55c.db")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()
    users_svc.create_user(store, "root", ROOT_PW, "admin", platform_admin=True)
    users_svc.add_member(store, "default", "root", "admin")

    registry = cluster_registry_module.ClusterRegistry(base_dir=str(tmp_path / "clusters"))
    registry.create_cluster("alpha", "Alpha", redis_url="redis://alpha:6379/0")
    registry.create_cluster("beta", "Beta", redis_url="redis://beta:6379/0")
    registry.switch_cluster("alpha")
    monkeypatch.setattr(cluster_registry_module, "_cluster_registry", registry)

    manager = FakeManager(tmp_path)
    monkeypatch.setattr(cluster_api, "get_service_manager", lambda: manager)
    monkeypatch.setattr(cluster_api, "_rebind_data_plane", _noop_rebind)
    # 保存活动集群配置前的探活：测试不连真实 Redis。
    monkeypatch.setattr(
        cluster_api, "_connect_redis_or_502",
        lambda url: SimpleNamespace(close=lambda: None),
    )
    monkeypatch.setattr(
        "services.service_manager.reset_service_manager", lambda: None)
    monkeypatch.setattr(
        cluster_api, "_execute_quick_test",
        lambda request: _quick_test_response(request),
    )

    app = FastAPI()
    app.state.store = store
    app.state.local_mode = True
    app.state.redis = None
    app.include_router(auth_users.router, prefix="/api")
    app.include_router(cluster_api.router, prefix="/api",
                       dependencies=[Depends(require_auth)])
    return TestClient(app)


async def _quick_test_response(request):
    return cluster_api.QuickTestResponse(
        success=True, message="ok", execution_id="exec-quick")


def _login(client: TestClient) -> dict:
    r = client.post("/api/auth/login", json={"username": "root", "password": ROOT_PW})
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['token']}"}


def test_all_cluster_write_success_paths_are_audited(env: TestClient):
    root = _login(env)
    calls = [
        ("POST", "/api/cluster/start", {"service_type": "flow_worker"}),
        ("POST", "/api/cluster/stop/inst-1", {"graceful": True}),
        ("POST", "/api/cluster/stop-all", {"graceful": True}),
        ("POST", "/api/cluster/mode", {"mode": "docker"}),
        ("POST", "/api/cluster/refresh", None),
        ("POST", "/api/cluster/infrastructure/redis/check", None),
        ("POST", "/api/cluster/infrastructure/redis/start", None),
        ("POST", "/api/cluster/infrastructure/redis/stop", None),
        ("POST", "/api/cluster/infrastructure",
         {"name": "evil2", "display_name": "Evil", "type": "redis",
          "url": f"redis://:{SECRET}@evil:6379/0"}),
        ("PUT", "/api/cluster/infrastructure/evil2",
         {"url": f"redis://:{NEW_SECRET}@evil2:6379/0"}),
        ("DELETE", "/api/cluster/infrastructure/evil2", None),
        ("POST", "/api/cluster/quick-test", {"test_type": "simple"}),
        ("DELETE", "/api/cluster/instances/inst-old", None),
        ("DELETE", "/api/cluster/instances", None),
        ("POST", "/api/clusters", {"id": "gamma", "name": "Gamma"}),
        ("PUT", "/api/clusters/gamma", {"name": "Gamma2"}),
        ("DELETE", "/api/clusters/gamma", None),
        ("POST", "/api/clusters/switch", {"cluster_id": "beta"}),
        ("PUT", "/api/clusters/beta/config",
         {"config": {"mode": "process", "services": {},
                     "redis": {"url": f"redis://:{SECRET}@beta:6379/0"}}}),
    ]
    for method, path, body in calls:
        resp = env.request(method, path, json=body, headers=root)
        assert resp.status_code == 200, (method, path, resp.status_code, resp.text)

    rows = audit_svc.list_audit(limit=1000)
    cluster_rows = [r for r in rows if r["action"].startswith("cluster.")]
    actions = {r["action"] for r in cluster_rows}
    expected = {
        "cluster.start", "cluster.stop", "cluster.stop_all",
        "cluster.switch_mode", "cluster.refresh",
        "cluster.infrastructure.check", "cluster.infrastructure.start",
        "cluster.infrastructure.stop", "cluster.infrastructure.create",
        "cluster.infrastructure.update", "cluster.infrastructure.delete",
        "cluster.quick_test", "cluster.instances.remove",
        "cluster.instances.clear", "cluster.registry.create",
        "cluster.registry.update", "cluster.registry.delete",
        "cluster.registry.switch", "cluster.registry.save_config",
    }
    assert expected <= actions, sorted(expected - actions)
    assert all(r["tenant_id"] == "" for r in cluster_rows)
    assert all(r["actor"] == "root" for r in cluster_rows)

    details = repr([r["detail"] for r in cluster_rows])
    assert SECRET not in details
    assert NEW_SECRET not in details
    assert "redis://" not in details
