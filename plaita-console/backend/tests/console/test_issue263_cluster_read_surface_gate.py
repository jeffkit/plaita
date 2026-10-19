"""plaita#263（P1 security）：cluster/clusters **读面**收 platform_admin + 响应脱敏。

缺口（基线 ``37434a0``）：``cluster.py`` 34 个端点只有写面挂了
``ensure_platform_admin``（#261）。租户 admin（``role=admin``、
``platform_admin=False``）可读：

- ``GET /api/cluster/infrastructure`` / ``/{name}`` → ``redis://:password@…``；
- ``GET /api/clusters`` / ``/active`` / ``/{id}`` → ``redis_url``；
- ``GET /api/clusters/{id}/config`` → 完整 YAML（``redis.url`` +
  ``services.*.env`` + ``infrastructure.*.docker.env``，样例含
  ``POSTGRES_PASSWORD``）。

读到数据面连接串 = 绕过 tenant_scope 直连共享 Redis，读/写**所有租户**的
执行状态与队列；env 里的数据库口令同理。修法（主控确认帖判据 1/2）：

1. cluster/clusters **全部**端点统一挂 ``ensure_platform_admin``（含 GET，
   不留「读是低危」例外）；
2. 响应序列化层再脱敏一次（URL 口令段 + 凭据键值），新端点漏闸也不回明文；
3. 同族顺扫：``DELETE /cluster/instances*``、``POST /cluster/refresh``、
   ``POST /cluster/quick-test``（宿主真实执行流程，simple 输出 42）。

负控（验收时手工执行）：仅删除某 handler 的 ``ensure_platform_admin`` /
序列化脱敏 ⇒ 对应用例回到 pre-fix 200 + 明文形态而精确红。
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
ADMIN_KEY = "issue263-cluster-key"
SECRET = "s3cret-pass"
SECRET_URL = f"redis://:{SECRET}@10.0.0.5:6379/0"


class FakeManager:
    """最小 ServiceManager 替身：记录调用，返回带口令的 URL 供脱敏断言。"""

    def __init__(self, tmp_path: Path):
        self.config = SimpleNamespace(mode="process", redis={"url": SECRET_URL})
        self.config_path = str(tmp_path / "cluster_config.yaml")
        self.calls: list = []
        self.infrastructure = {
            "redis": InfrastructureConfig(
                name="redis", display_name="Redis", type="redis", enabled=True,
                url=SECRET_URL, docker={"env": {"REDIS_PASSWORD": SECRET}},
            ),
        }

    def get_available_services(self):
        self.calls.append(("get_available_services",))
        return {
            "flow_worker": SimpleNamespace(
                display_name="流程执行器", default_instances=1, max_instances=3,
            )
        }

    def list_instances(self, service_type=None):
        self.calls.append(("list_instances", service_type))
        return []

    async def refresh_status(self):
        self.calls.append(("refresh_status",))
        return None

    def get_infrastructure(self):
        self.calls.append(("get_infrastructure",))
        return dict(self.infrastructure)

    async def check_infrastructure_health(self):
        self.calls.append(("check_infrastructure_health",))
        return {
            "redis": {
                "status": "healthy",
                "details": f"connected to {SECRET_URL}",
            }
        }

    def remove_instance(self, instance_id):
        self.calls.append(("remove_instance", instance_id))
        return True

    def clear_failed_instances(self):
        self.calls.append(("clear_failed_instances",))
        return 2


class FakeRedis:
    """``service-types`` / ``instances`` 的外部注册表读取替身（无键）。"""

    def keys(self, pattern):  # noqa: ARG002
        return []


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


_CLUSTER_CONFIG = {
    "mode": "process",
    "redis": {"url": SECRET_URL},
    "services": {
        "flow_worker": {
            "display_name": "流程执行器",
            "env": {"DB_PASSWORD": SECRET, "LOG_LEVEL": "INFO"},
        }
    },
    "infrastructure": {
        "postgresql": {
            "docker": {
                "env": {
                    "POSTGRES_PASSWORD": SECRET,
                    "POSTGRES_USER": "plaita",
                }
            }
        }
    },
}


def _build_env(tmp_path, monkeypatch) -> Env:
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{tmp_path}/issue263.db")
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(tmp_path / "creds.json"))
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(tmp_path / "creds.key"))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())

    flow_store.init_engine(f"sqlite:///{tmp_path}/issue263.db")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()
    tenants_svc.create_tenant(store, TENANT_A, "Tenant A")

    # root = 平台管理员；tadmin = tenant-a 的 admin（platform_admin=False）。
    users_svc.create_user(store, "root", ROOT_PW, "admin", platform_admin=True)
    users_svc.add_member(store, "default", "root", "admin")
    users_svc.create_user(store, "tadmin", TADMIN_PW, "admin")
    users_svc.add_member(store, TENANT_A, "tadmin", "admin")

    base = tmp_path / "clusters"
    registry = cluster_registry_module.ClusterRegistry(base_dir=str(base))
    registry.create_cluster("alpha", "Alpha", redis_url=SECRET_URL)
    registry.create_cluster("beta", "Beta", redis_url="redis://beta:6379/0")
    alpha_config = Path(registry.get_cluster("alpha").config_path)
    alpha_config.write_text(
        cluster_registry_module.yaml.safe_dump(_CLUSTER_CONFIG), encoding="utf-8")
    registry.switch_cluster("alpha")
    monkeypatch.setattr(cluster_registry_module, "_cluster_registry", registry)

    manager = FakeManager(tmp_path)
    monkeypatch.setattr(cluster_api, "get_service_manager", lambda: manager)
    monkeypatch.setattr(cluster_api, "_rebind_data_plane", _noop_rebind)
    # 外部注册表读取不触网；闸前不应走到这里（负控时也保持 pre-fix 200 形态）。
    import redis as redis_module

    monkeypatch.setattr(redis_module.Redis, "from_url", lambda *a, **kw: FakeRedis())
    # quick-test 的宿主真实执行用替身：simple 流程返回 42（负控 pre-fix 形态）。
    monkeypatch.setattr(
        "plaita.Flow.model_validate",
        classmethod(lambda cls, data: SimpleNamespace(run=lambda **kw: 42)),
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


# (id, method, path, audit action) —— 全部读面 + A51-05 同族三处。
READ_CASES = [
    pytest.param("service-types", "GET", "/api/cluster/service-types",
                 "cluster.service_types.read", id="service-types"),
    pytest.param("instances", "GET", "/api/cluster/instances",
                 "cluster.instances.read", id="instances"),
    pytest.param("cluster-config", "GET", "/api/cluster/config",
                 "cluster.config.read", id="cluster-config"),
    pytest.param("infrastructure", "GET", "/api/cluster/infrastructure",
                 "cluster.infrastructure.read", id="infrastructure"),
    pytest.param("infra-detail", "GET", "/api/cluster/infrastructure/redis",
                 "cluster.infrastructure.read", id="infra-detail"),
    pytest.param("infra-templates", "GET", "/api/cluster/infrastructure-templates",
                 "cluster.infrastructure.templates.read", id="infra-templates"),
    pytest.param("test-templates", "GET", "/api/cluster/test-templates",
                 "cluster.test_templates.read", id="test-templates"),
    pytest.param("clusters", "GET", "/api/clusters",
                 "cluster.registry.read", id="clusters"),
    pytest.param("clusters-active", "GET", "/api/clusters/active",
                 "cluster.registry.read", id="clusters-active"),
    pytest.param("cluster-detail", "GET", "/api/clusters/alpha",
                 "cluster.registry.read", id="cluster-detail"),
    pytest.param("cluster-full-config", "GET", "/api/clusters/alpha/config",
                 "cluster.registry.config.read", id="cluster-full-config"),
]

SWEEP_CASES = [
    pytest.param("refresh", "POST", "/api/cluster/refresh",
                 "cluster.refresh", id="refresh"),
    pytest.param("quick-test", "POST", "/api/cluster/quick-test",
                 "cluster.quick_test", id="quick-test"),
    pytest.param("infra-check", "POST", "/api/cluster/infrastructure/redis/check",
                 "cluster.infrastructure.check", id="infra-check"),
    pytest.param("instance-remove", "DELETE", "/api/cluster/instances/inst-1",
                 "cluster.instances.remove", id="instance-remove"),
    pytest.param("instances-clear", "DELETE", "/api/cluster/instances",
                 "cluster.instances.clear", id="instances-clear"),
]


@pytest.mark.parametrize("case_id,method,path,action", READ_CASES + SWEEP_CASES)
def test_tenant_admin_denied_and_secret_never_echoed(
    env: Env, case_id, method, path, action,
):
    """租户 admin 对全部读面/同族端点 403 + denied 审计 + 零副作用。

    负控：仅删除对应 handler 的 ``ensure_platform_admin`` ⇒ 本条回到
    pre-fix 200（读面含 ``SECRET`` 明文）而精确红。
    """
    tadmin = _login(env.client, "tadmin", TADMIN_PW)
    before_registry = _snapshot(env.registry)
    before_calls = list(env.manager.calls)

    body = {"test_type": "simple"} if case_id == "quick-test" else None
    resp = env.client.request(method, path, json=body, headers=tadmin)

    assert resp.status_code == 403, (case_id, resp.status_code, resp.text)
    assert resp.json().get("detail") == PLATFORM_ADMIN_REQUIRED_DETAIL, resp.json()
    assert SECRET not in resp.text, f"{case_id}: 拒绝响应仍回显凭据"
    assert _snapshot(env.registry) == before_registry, (
        f"{case_id}: 拒绝后 registry 仍被修改")
    assert env.manager.calls == before_calls, (
        f"{case_id}: 拒绝后仍调用了 ServiceManager: {env.manager.calls}")

    rows = audit_svc.list_audit(action=f"{action}.denied", limit=500)
    assert any(
        row["actor"] == "tadmin" and row["detail"].get("denied") is True
        and row["detail"].get("reason") == "platform_admin_required"
        for row in rows
    ), (case_id, action, rows)


@pytest.mark.parametrize("case_id,method,path,action", READ_CASES)
def test_platform_admin_reads_all_surfaces(env: Env, case_id, method, path, action):
    """正控：平台管理员 session 读全部端点 200；秘密已脱敏。"""
    root = _login(env.client, "root", ROOT_PW)
    resp = env.client.request(method, path, headers=root)
    assert resp.status_code == 200, (case_id, resp.status_code, resp.text)
    assert SECRET not in resp.text, f"{case_id}: 响应仍含明文凭据"


def test_platform_api_key_reads_all_surfaces(env: Env):
    """正控：平台级 API-Key 与 session 同权（读面 200）。"""
    headers = _admin_key_headers()
    for path in ("/api/cluster/infrastructure", "/api/clusters",
                 "/api/clusters/alpha/config"):
        resp = env.client.get(path, headers=headers)
        assert resp.status_code == 200, (path, resp.text)
        assert SECRET not in resp.text, path


def test_redaction_masks_url_password_and_env_secrets(env: Env):
    """序列化层兜底：URL 口令段与 env 凭据键值一律 ``***``。

    负控：删除 ``_RedactedResponseModel`` 的 serializer（或整段脱敏）⇒
    本用例看到原文 ``SECRET`` 而精确红。
    """
    root = _login(env.client, "root", ROOT_PW)

    infra = env.client.get("/api/cluster/infrastructure", headers=root)
    assert infra.status_code == 200, infra.text
    assert infra.json()["infrastructure"][0]["url"] == (
        "redis://:***@10.0.0.5:6379/0"
    )

    detail = env.client.get("/api/cluster/infrastructure/redis", headers=root)
    assert detail.status_code == 200, detail.text
    assert detail.json()["url"] == "redis://:***@10.0.0.5:6379/0"
    assert SECRET not in str(detail.json().get("details"))

    clusters = env.client.get("/api/clusters", headers=root)
    assert clusters.status_code == 200, clusters.text
    alpha = next(c for c in clusters.json()["clusters"] if c["id"] == "alpha")
    assert alpha["redis_url"] == "redis://:***@10.0.0.5:6379/0"

    full = env.client.get("/api/clusters/alpha/config", headers=root)
    assert full.status_code == 200, full.text
    config = full.json()["config"]
    assert config["redis"]["url"] == "redis://:***@10.0.0.5:6379/0"
    assert config["services"]["flow_worker"]["env"]["DB_PASSWORD"] == "***"
    assert config["services"]["flow_worker"]["env"]["LOG_LEVEL"] == "INFO"
    assert (
        config["infrastructure"]["postgresql"]["docker"]["env"]
        ["POSTGRES_PASSWORD"] == "***"
    )
    assert (
        config["infrastructure"]["postgresql"]["docker"]["env"]
        ["POSTGRES_USER"] == "plaita"
    )
    assert SECRET not in full.text

    templates = env.client.get("/api/cluster/infrastructure-templates", headers=root)
    assert templates.status_code == 200, templates.text
    env_keys = {
        key
        for template in templates.json()["templates"]
        for key in (template.get("docker") or {}).get("env", {})
        if "PASSWORD" in key
    }
    assert env_keys, "模板用例失效：没有带 PASSWORD 的 env 键"
    for template in templates.json()["templates"]:
        for key, value in (template.get("docker") or {}).get("env", {}).items():
            if "PASSWORD" in key:
                # B59-01：模板不再内置固定口令，改成纯 ${VAR} 引用（启动时
                # 从 console 进程环境取值）。引用不含秘密，允许原样回读；
                # 其他任何值仍必须是 ***。
                if value.startswith("${") and value.endswith("}"):
                    continue
                assert value == "***", (key, value)


def test_quick_test_still_executes_for_platform_admin(env: Env):
    """同族顺扫正控：平台管理员仍可 quick-test（simple 输出 42）。"""
    root = _login(env.client, "root", ROOT_PW)
    resp = env.client.post(
        "/api/cluster/quick-test", json={"test_type": "simple"}, headers=root,
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["success"] is True, resp.json()
    assert resp.json()["result"]["output"] == 42, resp.json()
