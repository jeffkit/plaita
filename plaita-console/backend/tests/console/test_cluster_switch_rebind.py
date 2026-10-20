"""切换集群必须把 console 数据面与服务管理面重绑到活动集群的 Redis（plaita#65）。

修复前：switch_cluster 只改注册表活动指针——app.state.redis 仍是启动时由
PLAITA_CONSOLE_REDIS_URL 建的那套，切换后新执行投进旧集群；管理面读数也被同一
环境变量无条件覆盖。本测试用多套相互隔离的 fakeredis 代表各集群 Redis。
"""
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from fakeredis import FakeRedis
from fastapi import Depends, FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import main as main_module  # noqa: E402
from api import cluster as cluster_api  # noqa: E402
from api import executions as executions_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import cluster_registry as cluster_registry_module  # noqa: E402
from services import flow_store  # noqa: E402
from services import service_manager as service_manager_module  # noqa: E402

ADMIN_KEY = "cluster-switch-admin-key"
URL_A = "redis://cluster-a:6379/0"
URL_B = "redis://cluster-b:6379/0"
URL_C = "redis://cluster-c:6379/0"
# 注册表里自动创建的默认集群写死这个占位（Docker 里 console 容器连不上它）
PLACEHOLDER_URL = "redis://localhost:6379/0"
QUEUE = "plaita:flow:queue"
HEADERS = {"X-Admin-API-Key": ADMIN_KEY}


def _settings():
    from config import Settings
    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


class _FakeRedisByUrl:
    """按 URL 发放 fakeredis——模拟多套相互隔离的集群 Redis。"""

    def __init__(self, unreachable=()):
        self.clients = {}
        self.unreachable = set(unreachable)

    def __call__(self, url: str) -> FakeRedis:
        if url in self.unreachable:
            raise ConnectionError(f"cannot reach {url}")
        client = self.clients.get(url)
        if client is None:
            client = FakeRedis(decode_responses=True)
            self.clients[url] = client
        return client


@pytest.fixture()
def env(tmp_path, monkeypatch):
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    # console 数据面启动时连的是集群 A（env 只描述启动默认值）
    monkeypatch.setenv("PLAITA_CONSOLE_REDIS_URL", URL_A)

    flow_store.init_engine(f"sqlite:///{tmp_path}/switch.db")

    factory = _FakeRedisByUrl()
    monkeypatch.setattr(main_module, "connect_redis", factory)

    registry = cluster_registry_module.ClusterRegistry(
        base_dir=str(tmp_path / "clusters"))
    registry.create_cluster("A", "集群A", redis_url=URL_A)
    registry.create_cluster("B", "集群B", redis_url=URL_B)
    registry.switch_cluster("A")
    monkeypatch.setattr(cluster_registry_module, "_cluster_registry", registry)
    monkeypatch.setattr(service_manager_module, "_manager", None)

    app = FastAPI()
    app.state.redis = factory(URL_A)
    app.state.redis_url = URL_A
    app.state.local_mode = False
    app.state.store = flow_store.get_flow_store()
    app.include_router(cluster_api.router, prefix="/api",
                       dependencies=[Depends(require_auth)])
    app.include_router(executions_api.router, prefix="/api",
                       dependencies=[Depends(require_auth)])

    return factory, app


def _switch(client: TestClient, cluster_id: str, expected_url: str) -> None:
    r = client.post("/api/clusters/switch", json={"cluster_id": cluster_id},
                    headers=HEADERS)
    assert r.status_code == 200, r.text
    assert r.json()["redis_url"] == expected_url
    assert r.json()["is_active"] is True


def _assert_plane_on(app: FastAPI, factory: _FakeRedisByUrl, url: str) -> None:
    assert app.state.redis_url == url
    assert app.state.redis is factory.clients[url]


def test_switch_rebinds_data_plane_and_service_manager(env):
    factory, app = env
    client = TestClient(app)

    client_after_switch = factory.clients
    _switch(client, "B", URL_B)

    # (a) 数据面已重绑到集群 B，SSE 的 asyncio 客户端缓存被清掉（按 URL 惰性重建）
    _assert_plane_on(app, factory, URL_B)
    assert app.state.execution_sse_aioredis is None

    # (b) 切换后新发起的执行投进 B 的队列，A 上一条都没有
    r = client.post("/api/executions", json={"flow_id": "f1", "params": {}},
                    headers=HEADERS)
    assert r.status_code == 200, r.text
    assert client_after_switch[URL_B].xlen(QUEUE) == 1
    assert client_after_switch[URL_A].xlen(QUEUE) == 0

    # (c) 服务管理面读数跟随活动集群（不再被 PLAITA_CONSOLE_REDIS_URL 拉回 A）
    assert service_manager_module.get_service_manager().config.redis["url"] == URL_B

    # 切回 A：三条断言反向成立
    _switch(client, "A", URL_A)
    _assert_plane_on(app, factory, URL_A)
    r = client.post("/api/executions", json={"flow_id": "f2", "params": {}},
                    headers=HEADERS)
    assert r.status_code == 200, r.text
    assert factory.clients[URL_A].xlen(QUEUE) == 1
    assert factory.clients[URL_B].xlen(QUEUE) == 1
    assert service_manager_module.get_service_manager().config.redis["url"] == URL_A


def test_switch_to_same_url_keeps_client(env):
    factory, app = env
    client = TestClient(app)
    before = app.state.redis

    _switch(client, "A", URL_A)

    assert app.state.redis is before
    assert app.state.redis_url == URL_A


def test_switch_back_to_placeholder_default_cluster_uses_env(env):
    """Docker 部署：默认集群的注册表 URL 是 localhost 占位，切回它不能 502。

    只认注册表元数据会把默认集群判成不可达——切到任何具体集群后永远回不来。
    占位 URL 须与 load_config 同约定让位给 PLAITA_CONSOLE_REDIS_URL。
    """
    factory, app = env
    client = TestClient(app)
    registry = cluster_registry_module.get_cluster_registry()
    assert registry.get_cluster("default").redis_url == PLACEHOLDER_URL
    # console 容器里 localhost:6379 就是连不上（redis 在别的容器）
    factory.unreachable.add(PLACEHOLDER_URL)

    _switch(client, "B", URL_B)
    _assert_plane_on(app, factory, URL_B)

    # 切回默认集群：占位 URL 让位 env（= A），不是 502
    _switch(client, "default", PLACEHOLDER_URL)
    _assert_plane_on(app, factory, URL_A)
    assert registry.get_active_cluster().id == "default"


def test_switch_to_unreachable_cluster_keeps_registry_and_plane(env):
    """先验证后落元数据：目标 Redis 连不上就不切换，两个面都不动。"""
    factory, app = env
    client = TestClient(app)
    registry = cluster_registry_module.get_cluster_registry()
    registry.create_cluster("C", "集群C", redis_url=URL_C)
    factory.unreachable.add(URL_C)
    before = app.state.redis

    r = client.post("/api/clusters/switch", json={"cluster_id": "C"}, headers=HEADERS)

    assert r.status_code == 502, r.text
    assert URL_C in r.json()["detail"]["message"]
    assert registry.get_active_cluster().id == "A"
    assert app.state.redis is before
    assert app.state.redis_url == URL_A
    assert app.state.local_mode is False


def test_switch_from_local_mode_binds_cluster_and_leaves_local_mode(env):
    """本地单机模式（启动时 Redis 不可达）下切到可达集群：数据面接上、退出本地分支。"""
    factory, app = env
    client = TestClient(app)
    app.state.redis = None
    app.state.redis_url = None
    app.state.local_mode = True

    _switch(client, "B", URL_B)

    _assert_plane_on(app, factory, URL_B)
    assert app.state.local_mode is False
    r = client.post("/api/executions", json={"flow_id": "f1", "params": {}},
                    headers=HEADERS)
    assert r.status_code == 200, r.text
    assert factory.clients[URL_B].xlen(QUEUE) == 1


def test_startup_redis_url_follows_active_cluster(env):
    """重启后数据面仍要跟活动集群走（plaita#65 的可持久性）。

    lifespan 取启动 URL 的助手读注册表（配置文件 → 元数据 → env 同约定），
    而不是无条件用 env——否则「已切到集群 B」重启即打回原形。
    """
    _factory, _app = env
    assert main_module._active_cluster_redis_url() == URL_A

    registry = cluster_registry_module.get_cluster_registry()
    registry.switch_cluster("B")
    assert main_module._active_cluster_redis_url() == URL_B

    # 默认集群的配置/元数据都是 localhost 占位 → 让位 env
    registry.switch_cluster("default")
    assert main_module._active_cluster_redis_url() == URL_A


def _save_config(client: TestClient, cluster_id: str, config: dict):
    return client.put(f"/api/clusters/{cluster_id}/config", json={"config": config},
                      headers=HEADERS)


def test_save_active_cluster_config_rebinds_data_plane(env):
    """活动集群配置的 redis.url 就是数据面真相源：改它必须把数据面一起搬过去。"""
    factory, app = env
    client = TestClient(app)
    registry = cluster_registry_module.get_cluster_registry()
    config = registry.get_cluster_config("A")
    config["redis"]["url"] = URL_B

    r = _save_config(client, "A", config)

    assert r.status_code == 200, r.text
    _assert_plane_on(app, factory, URL_B)
    assert service_manager_module.get_service_manager().config.redis["url"] == URL_B


def test_save_active_cluster_config_unreachable_redis_writes_nothing(env):
    """探活先行：目标 Redis 连不上时配置文件不落盘、数据面不动。"""
    factory, app = env
    client = TestClient(app)
    registry = cluster_registry_module.get_cluster_registry()
    config = registry.get_cluster_config("A")
    config["redis"]["url"] = URL_C
    factory.unreachable.add(URL_C)

    r = _save_config(client, "A", config)

    assert r.status_code == 502, r.text
    assert registry.get_cluster_config("A")["redis"]["url"] == URL_A
    assert app.state.redis_url == URL_A


def test_save_inactive_cluster_config_keeps_data_plane(env):
    """非活动集群改配置只是改文件：数据面仍跟着活动集群 A。"""
    factory, app = env
    client = TestClient(app)
    registry = cluster_registry_module.get_cluster_registry()
    config = registry.get_cluster_config("B")
    config["redis"]["url"] = URL_C
    before = app.state.redis

    r = _save_config(client, "B", config)

    assert r.status_code == 200, r.text
    assert app.state.redis is before
    assert app.state.redis_url == URL_A


def test_sse_async_client_prefers_bound_url_over_env(monkeypatch):
    """SSE 的 redis.asyncio 客户端：数据面已绑集群时用绑定的 URL，未绑时退回 env。"""
    settings = _settings()
    settings.redis_url = URL_A
    monkeypatch.setattr("config.get_settings", lambda: settings)
    seen = []
    monkeypatch.setattr(
        "redis.asyncio.from_url", lambda url, **kw: seen.append(url) or object())

    app = FastAPI()
    request = SimpleNamespace(app=app)

    app.state.redis_url = URL_B
    bound = executions_api._execution_stream_async_redis(request)
    assert app.state.execution_sse_aioredis is bound

    # 清掉缓存（= 集群切换后的状态）才走惰性重建分支
    app.state.redis_url = None
    app.state.execution_sse_aioredis = None
    executions_api._execution_stream_async_redis(request)

    assert seen == [URL_B, URL_A]
