"""plaita#261（P1 security）：``/api/cluster`` / ``/api/clusters`` 全局生命周期与
基础设施面收 ``platform_admin``——租户 admin 不再是平台身份。

缺口（基线 ``c0bffe0``）：``cluster.py`` 全文件没有 ``ensure_platform_admin``
调用。``_ADMIN_PREFIXES`` 只按租户角色 ``role == "admin"`` 放行，而租户 admin
的角色恰好是 admin ⇒ 它可启停全局服务、切换 process/docker 模式、改基础设施
``url``（把 Redis 指向攻击者主机 = 配置接管）、登记/切换/删除集群并改集群
配置（同样含 ``redis.url`` 与进程启动参数）。同义的
``POST /api/services/{id}/stop`` 在 #260 已闸，本文件把漏掉的另一半钉住。

验收：
1. 负向：租户 admin 对全部受闸操作 403 + 具名文案，且拒绝写入 ``*.denied``
   审计（actor=tadmin, denied=True）；前置副作用为零（manager 零调用、
   registry 配置不变）。
2. 正向：平台管理员（session）与平台级 API-Key 的同一批操作仍 200。
3. 读面（#263 更新）：租户 admin 读 cluster config / clusters 列表同样 403；
   平台管理员读面仍 200（正控）。

负控（验收时手工执行）：删除任一 handler 的 ``ensure_platform_admin`` 调用
⇒ 对应用例回到 pre-fix 200/副作用形态而精确红；其余用例保持绿。
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
from auth import PLATFORM_ADMIN_REQUIRED_DETAIL, require_auth  # noqa: E402
from services import audit as audit_svc  # noqa: E402
from services import cluster_registry as cluster_registry_module  # noqa: E402
from services import flow_store, tenants_svc, users_svc  # noqa: E402
from services.service_manager import InfrastructureConfig  # noqa: E402

ROOT_PW = "root-password-1"
TADMIN_PW = "tadmin-password-1"
TENANT_A = "tenant-a"
ADMIN_KEY = "issue261-cluster-key"


class FakeManager:
    """最小 ServiceManager 替身：只实现受闸端点会用到的接口并记录调用。"""

    def __init__(self, tmp_path: Path):
        self.config = SimpleNamespace(
            mode="process", redis={"url": "redis://localhost:6379/0"})
        self.config_path = str(tmp_path / "cluster_config.yaml")
        self.calls: list = []
        self.infrastructure = {
            "redis": InfrastructureConfig(
                name="redis", display_name="Redis", type="redis", enabled=True,
                url="redis://localhost:6379/0",
            ),
        }

    async def start_service(self, service_type):
        self.calls.append(("start_service", service_type))
        return SimpleNamespace(
            status="running", instance_id=f"{service_type}-1", error_message=None)

    async def stop_service(self, instance_id, graceful):
        self.calls.append(("stop_service", instance_id, graceful))
        return True

    async def stop_all(self, graceful):
        self.calls.append(("stop_all", graceful))
        return {"inst-1": True}

    def get_infrastructure(self):
        return dict(self.infrastructure)

    def load_config(self):
        self.calls.append(("load_config",))

    async def check_infrastructure_health(self):
        return {name: {"status": "healthy", "details": None}
                for name in self.infrastructure}

    async def add_infrastructure(self, **kwargs):
        self.calls.append(("add_infrastructure", kwargs))
        self.infrastructure[kwargs["name"]] = InfrastructureConfig(
            name=kwargs["name"],
            display_name=kwargs["display_name"],
            type=kwargs["type"],
            enabled=kwargs.get("enabled", True),
            url=kwargs.get("url"),
            bootstrap_servers=kwargs.get("bootstrap_servers"),
            docker=kwargs.get("docker") or {},
        )
        return True

    async def update_infrastructure(self, **kwargs):
        self.calls.append(("update_infrastructure", kwargs))
        config = self.infrastructure[kwargs["name"]]
        for field in ("display_name", "enabled", "url", "bootstrap_servers", "docker"):
            if kwargs.get(field) is not None:
                setattr(config, field, kwargs[field])
        return True

    async def delete_infrastructure(self, name):
        self.calls.append(("delete_infrastructure", name))
        self.infrastructure.pop(name, None)
        return True

    async def start_infrastructure(self, name):
        self.calls.append(("start_infrastructure", name))
        return {"success": True, "name": name, "status": "running"}

    async def stop_infrastructure(self, name):
        self.calls.append(("stop_infrastructure", name))
        return {"success": True, "name": name, "status": "stopped"}


class Env:
    def __init__(self, client, registry, manager):
        self.client = client
        self.registry = registry
        self.manager = manager


def _settings(admin_key: str = ADMIN_KEY):
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = admin_key
    s.allow_insecure_admin = False
    return s


async def _noop_rebind(app, redis_url):  # noqa: ARG001
    return None


def _build_env(tmp_path, monkeypatch) -> Env:
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{tmp_path}/issue261.db")
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(tmp_path / "creds.json"))
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(tmp_path / "creds.key"))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())

    flow_store.init_engine(f"sqlite:///{tmp_path}/issue261.db")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()
    tenants_svc.create_tenant(store, TENANT_A, "Tenant A")

    # root = 平台管理员；tadmin = tenant-a 的 admin（platform_admin=False）——攻击者身份
    users_svc.create_user(store, "root", ROOT_PW, "admin", platform_admin=True)
    users_svc.add_member(store, "default", "root", "admin")
    users_svc.create_user(store, "tadmin", TADMIN_PW, "admin")
    users_svc.add_member(store, TENANT_A, "tadmin", "admin")

    base = tmp_path / "clusters"
    registry = cluster_registry_module.ClusterRegistry(base_dir=str(base))
    registry.create_cluster("alpha", "Alpha", redis_url="redis://alpha:6379/0")
    registry.create_cluster("beta", "Beta", redis_url="redis://beta:6379/0")
    registry.switch_cluster("alpha")
    monkeypatch.setattr(cluster_registry_module, "_cluster_registry", registry)

    manager = FakeManager(tmp_path)
    monkeypatch.setattr(cluster_api, "get_service_manager", lambda: manager)
    # switch / save 的数据面重绑在负向用例里不应发生；若闸被删，这些替身让
    # pre-fix 形态仍返回 200（而不是被环境错误搅成 500）。
    monkeypatch.setattr(cluster_api, "_rebind_data_plane", _noop_rebind)
    monkeypatch.setattr(
        "services.service_manager.reset_service_manager",
        lambda: {"instances": 0, "processes": 0, "containers": 0},
    )

    app = FastAPI()
    app.state.store = store
    app.state.local_mode = True
    app.state.redis = None
    app.include_router(auth_users.router, prefix="/api")
    app.include_router(
        cluster_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    return Env(TestClient(app), registry, manager)


@pytest.fixture()
def env(tmp_path, monkeypatch) -> Env:
    return _build_env(tmp_path, monkeypatch)


def _login(client: TestClient, username: str, password: str) -> dict:
    r = client.post("/api/auth/login", json={"username": username, "password": password})
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['token']}"}


def _admin_key_headers() -> dict:
    return {"X-Admin-API-Key": ADMIN_KEY}


def _snapshot(registry) -> tuple:
    clusters = tuple(sorted(
        (c.id, c.name, c.redis_url) for c in registry.list_clusters()))
    active = registry.get_active_cluster()
    configs = tuple(sorted(
        (c.id, Path(c.config_path).read_bytes() if Path(c.config_path).is_file() else None)
        for c in registry.list_clusters()
    ))
    return clusters, active.id if active else None, configs


# (id, method, path, body, audit action)
PROTECTED_CASES = [
    pytest.param("start", "POST", "/api/cluster/start",
                 {"service_type": "flow_worker"}, "cluster.start", id="start"),
    pytest.param("stop", "POST", "/api/cluster/stop/inst-1",
                 {"graceful": True}, "cluster.stop", id="stop"),
    pytest.param("stop-all", "POST", "/api/cluster/stop-all",
                 {"graceful": True}, "cluster.stop_all", id="stop-all"),
    pytest.param("mode", "POST", "/api/cluster/mode",
                 {"mode": "docker"}, "cluster.switch_mode", id="mode"),
    pytest.param("infra-create", "POST", "/api/cluster/infrastructure",
                 {"name": "evil", "display_name": "Evil", "type": "redis",
                  "url": "redis://evil:6379/0"},
                 "cluster.infrastructure.create", id="infra-create"),
    # P1 本体：改 URL = 把数据面指向攻击者主机（配置接管）
    pytest.param("infra-update", "PUT", "/api/cluster/infrastructure/redis",
                 {"url": "redis://evil:6379/0"},
                 "cluster.infrastructure.update", id="infra-update-url"),
    pytest.param("infra-delete", "DELETE", "/api/cluster/infrastructure/redis",
                 None, "cluster.infrastructure.delete", id="infra-delete"),
    pytest.param("infra-start", "POST", "/api/cluster/infrastructure/redis/start",
                 None, "cluster.infrastructure.start", id="infra-start"),
    pytest.param("infra-stop", "POST", "/api/cluster/infrastructure/redis/stop",
                 None, "cluster.infrastructure.stop", id="infra-stop"),
    pytest.param("cluster-create", "POST", "/api/clusters",
                 {"id": "evil", "name": "Evil"}, "cluster.registry.create",
                 id="cluster-create"),
    pytest.param("cluster-update", "PUT", "/api/clusters/alpha",
                 {"name": "pwned"}, "cluster.registry.update",
                 id="cluster-update"),
    # 删非活动集群（beta）：pre-fix 形态是 200 删除，而非活动集群保护给的 400。
    pytest.param("cluster-delete", "DELETE", "/api/clusters/beta",
                 None, "cluster.registry.delete", id="cluster-delete"),
    pytest.param("cluster-switch", "POST", "/api/clusters/switch",
                 {"cluster_id": "beta"}, "cluster.registry.switch",
                 id="cluster-switch"),
    pytest.param("cluster-save-config", "PUT", "/api/clusters/beta/config",
                 {"config": {"mode": "process", "services": {},
                             "redis": {"url": "redis://evil:6379/0"}}},
                 "cluster.registry.save_config", id="cluster-save-config"),
]


@pytest.mark.parametrize("case_id,method,path,body,action", PROTECTED_CASES)
def test_tenant_admin_denied_for_every_global_surface(
    env: Env, case_id, method, path, body, action,
):
    """租户 admin 六个以上全局操作一律 403 + denied 审计 + 零副作用。

    负控：删除对应 handler 的 ``ensure_platform_admin`` ⇒ 本条 pre-fix 为
    200 且 registry/manager 出现副作用（精确红）。
    """
    tadmin = _login(env.client, "tadmin", TADMIN_PW)
    before_registry = _snapshot(env.registry)
    before_calls = list(env.manager.calls)

    resp = env.client.request(method, path, json=body, headers=tadmin)

    assert resp.status_code == 403, (case_id, resp.status_code, resp.text)
    assert resp.json().get("detail") == PLATFORM_ADMIN_REQUIRED_DETAIL, resp.json()
    assert _snapshot(env.registry) == before_registry, (
        f"{case_id}: 拒绝后 registry 仍被修改")
    assert env.manager.calls == before_calls, (
        f"{case_id}: 拒绝后仍调用了 ServiceManager: {env.manager.calls}")

    rows = audit_svc.list_audit(action=f"{action}.denied", limit=500)
    assert any(
        row["actor"] == "tadmin" and row["detail"].get("denied") is True
        and row["detail"].get("reason") == "platform_admin_required"
        for row in rows
    ), (case_id, rows)


def test_tenant_admin_read_surface_gated(env: Env):
    """#263：读面与写面同闸——租户 admin 读 cluster config / 集群列表 403。

    平台管理员正控与脱敏断言在
    ``test_issue263_cluster_read_surface_gate.py``。
    """
    tadmin = _login(env.client, "tadmin", TADMIN_PW)

    for path in ("/api/cluster/config", "/api/clusters"):
        response = env.client.get(path, headers=tadmin)
        assert response.status_code == 403, (path, response.text)
        assert response.json().get("detail") == PLATFORM_ADMIN_REQUIRED_DETAIL

    root = _login(env.client, "root", ROOT_PW)
    config = env.client.get("/api/cluster/config", headers=root)
    assert config.status_code == 200, config.text
    assert config.json()["mode"] == "process"

    clusters = env.client.get("/api/clusters", headers=root)
    assert clusters.status_code == 200, clusters.text
    # registry 构造时会自动登记 default 集群；alpha/beta 是 fixture 显式创建的。
    assert {c["id"] for c in clusters.json()["clusters"]} >= {"alpha", "beta"}


def test_platform_admin_session_can_manage_global_surface(env: Env):
    """正控：平台管理员 session 的启停 / 基础设施改 URL / 集群登记仍可用。"""
    root = _login(env.client, "root", ROOT_PW)

    r = env.client.post("/api/cluster/stop/inst-1",
                        json={"graceful": False}, headers=root)
    assert r.status_code == 200, r.text
    assert ("stop_service", "inst-1", False) in env.manager.calls

    r = env.client.put(
        "/api/cluster/infrastructure/redis",
        json={"url": "redis://platform-managed:6379/0"}, headers=root,
    )
    assert r.status_code == 200, r.text
    assert r.json()["url"] == "redis://platform-managed:6379/0"

    r = env.client.post("/api/clusters", json={"id": "newc", "name": "New"},
                        headers=root)
    assert r.status_code == 200, r.text
    assert env.registry.get_cluster("newc") is not None
    assert env.client.delete("/api/clusters/newc", headers=root).status_code == 200


def test_api_key_can_control_global_lifecycle(env: Env):
    """正控：平台级 API-Key 不受影响（stop-all / mode 仍 200）。"""
    headers = _admin_key_headers()

    r = env.client.post("/api/cluster/stop-all", json={"graceful": True},
                        headers=headers)
    assert r.status_code == 200, r.text
    assert r.json()["success"] is True
    assert ("stop_all", True) in env.manager.calls

    r = env.client.post("/api/cluster/mode", json={"mode": "docker"},
                        headers=headers)
    assert r.status_code == 200, r.text
    assert env.manager.config.mode == "docker"
