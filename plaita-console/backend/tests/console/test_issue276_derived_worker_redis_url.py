"""#276：``${redis.url}`` 占位与派生 worker 的 Redis URL 不得与数据面分叉。

A2R40-01/A2R40-03 修后残留面：``ServiceManager._resolve_env`` 只从集群配置
文件 ``redis.url`` 替换 ``${redis.url}``，不读注册表生效 URL——metadata 生效
档（配置文件无 redis 键、console env 未设）下占位解析成默认
``redis://localhost:6379/0``，派生 worker 重启后静默连错实例，而 console 读面
``data_plane_redis_url`` 报的是元数据 URL；``frozen_service_redis_urls`` 只扫
具体字面量、``${redis.url}`` 被显式跳过，warnings 为空（A2 r43 探针留档）。

修法（方案①，最小惊讶）：
- ``_resolve_env`` 解析 ``${redis.url}`` 时回退链与读面同源（plaita#65 序
  不变）：配置文件 redis.url > PLAITA_CONSOLE_REDIS_URL > 注册表元数据 >
  localhost 兜底；
- warnings 覆盖派生面：``${redis.url}`` 解析结果、无显式 Redis env 的继承
  （进程 ``PLAITA_REDIS_URL``/``REDIS_URL``）与 worker 默认面——与数据面生效
  URL 不一致时响应 warnings + 服务端 WARNING（读/写面 + 启动门可观测）。

边界（#276 明示）：显式具体 URL 的逐服务覆盖语义不动（冻结字面量仍原样
透传，由 A2R40-03 warnings 承载）；本组只修 ``${redis.url}``/派生默认路径。
"""
import asyncio
import logging
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from fakeredis import FakeRedis
from fastapi import Depends, FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import cluster as cluster_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import cluster_registry as cluster_registry_module  # noqa: E402
from services import service_manager as service_manager_module  # noqa: E402
from services.service_manager import (  # noqa: E402
    ServiceManager,
    config_redis_url,
    effective_redis_url,
)

ADMIN_KEY = "issue276-admin-key"
HEADERS = {"X-Admin-API-Key": ADMIN_KEY}

URL_PIN = "redis://pin-redis:6379/0"        # config_file 档：配置文件具体地址
URL_ENV = "redis://env-redis:6379/0"        # console_env 档：PLAITA_CONSOLE_REDIS_URL
URL_META = "redis://meta-redis:6379/0"      # registry_metadata 档：注册表元数据
URL_OLD = "redis://old-redis:6379/0"        # 冻结/继承面上的旧实例
PLACEHOLDER = "redis://localhost:6379/0"


def _settings():
    from config import Settings
    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


class _FakeRedisByUrl:
    """按 URL 发放 fakeredis——模拟相互隔离的集群 Redis。"""

    def __init__(self):
        self.clients = {}

    def __call__(self, url: str) -> FakeRedis:
        client = self.clients.get(url)
        if client is None:
            client = FakeRedis(decode_responses=True)
            self.clients[url] = client
        return client


class _CaptureLauncher:
    """捕获 start_service 实际下发的 env——派生 worker「实连 URL」断言面。"""

    def __init__(self):
        self.started = []

    async def start(self, service_type, config, env):
        self.started.append((service_type, dict(env)))
        return SimpleNamespace(
            instance_id=f"{service_type}-1",
            service_type=service_type,
            status="running",
            error_message=None,
        )


def _write_cluster_config(tmp_path: Path, name: str, config: dict) -> str:
    cfg_dir = tmp_path / name
    cfg_dir.mkdir(parents=True, exist_ok=True)
    cfg_path = cfg_dir / "cluster_config.yaml"
    cfg_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return str(cfg_path)


def _meta_mode_config(worker_env: dict) -> dict:
    """metadata 生效档：配置文件没有 redis 键——优先级序里只剩元数据。"""
    return {
        "profile": "dev",
        "mode": "process",
        "services": {
            "flow_worker": {
                "display_name": "流程执行器",
                "process": {"module": "plaita.server.flow_worker"},
                "env": worker_env,
            },
        },
    }


@pytest.fixture()
def env(tmp_path, monkeypatch):
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    # 三档裁决依赖环境变量形态：env 逐测试自行 set/del，不在 fixture 一刀切。
    for var in ("PLAITA_CONSOLE_REDIS_URL", "PLAITA_REDIS_URL", "REDIS_URL"):
        monkeypatch.delenv(var, raising=False)

    factory = _FakeRedisByUrl()
    registry = cluster_registry_module.ClusterRegistry(
        base_dir=str(tmp_path / "clusters"))

    # ① metadata 生效档 + ${redis.url} 派生 worker（#276 主现场）
    meta_cfg_path = _write_cluster_config(
        tmp_path, "meta", _meta_mode_config({"PLAITA_REDIS_URL": "${redis.url}"}))
    registry.create_cluster(
        "meta", "元数据档", config_path=meta_cfg_path, redis_url=URL_META)

    # ② metadata 生效档 + 无显式 Redis env（继承/默认派生面）
    inherit_cfg_path = _write_cluster_config(
        tmp_path, "meta-inherit", _meta_mode_config({}))
    registry.create_cluster(
        "meta-inherit", "继承档", config_path=inherit_cfg_path, redis_url=URL_META)

    # ③ config_file 档：redis.url 具体地址 + ${redis.url} 派生
    pin_cfg_path = _write_cluster_config(tmp_path, "pinned", {
        "profile": "dev",
        "mode": "process",
        "redis": {"url": URL_PIN},
        "services": {
            "flow_worker": {
                "display_name": "流程执行器",
                "process": {"module": "plaita.server.flow_worker"},
                "env": {"PLAITA_REDIS_URL": "${redis.url}"},
            },
        },
    })
    registry.create_cluster(
        "pinned", "钉住档", config_path=pin_cfg_path, redis_url=URL_PIN)

    # ④ console_env 档：redis.url 是 localhost 占位，让位 PLAITA_CONSOLE_REDIS_URL
    envpin_cfg_path = _write_cluster_config(tmp_path, "envpin", {
        "profile": "dev",
        "mode": "process",
        "redis": {"url": PLACEHOLDER},
        "services": {
            "flow_worker": {
                "display_name": "流程执行器",
                "process": {"module": "plaita.server.flow_worker"},
                "env": {"PLAITA_REDIS_URL": "${redis.url}"},
            },
        },
    })
    registry.create_cluster(
        "envpin", "占位档", config_path=envpin_cfg_path, redis_url=PLACEHOLDER)

    registry.switch_cluster("meta")
    monkeypatch.setattr(cluster_registry_module, "_cluster_registry", registry)
    monkeypatch.setattr(service_manager_module, "_manager", None)

    app = FastAPI()
    app.state.redis = factory(URL_META)
    app.state.redis_url = URL_META
    app.state.local_mode = False
    app.include_router(cluster_api.router, prefix="/api",
                       dependencies=[Depends(require_auth)])

    return factory, app, registry, monkeypatch, tmp_path


def _manager_for(cfg_path: str) -> ServiceManager:
    """真实 load_config 路径的 ServiceManager（不经 API 单例）。"""
    return ServiceManager(config_path=cfg_path)


def _start_worker(manager: ServiceManager):
    launcher = _CaptureLauncher()
    manager._process_launcher = launcher
    instance = asyncio.run(manager.start_service("flow_worker"))
    assert instance.status == "running"
    assert launcher.started, "launcher 未被调用"
    return launcher.started[-1][1]


def _effective_of(registry, cluster_id: str):
    info = registry.get_cluster(cluster_id)
    return effective_redis_url(
        config_redis_url(registry.get_cluster_config(cluster_id)),
        info.redis_url,
    )


# ---------------------------------------------------------------------------
# 负控核心：metadata 档派生 worker 实连 URL（先红后绿）
# ---------------------------------------------------------------------------

def test_metadata_mode_placeholder_resolves_to_registry_effective_url(env):
    """metadata 档 + ``${redis.url}``：派生 worker 实连元数据生效 URL。

    修复前：``_resolve_env`` 只读配置文件（无 redis 键）⇒ 解析成
    ``redis://localhost:6379/0``，worker 静默连错实例而读面报元数据 URL。
    """
    factory, app, registry, monkeypatch, tmp_path = env
    manager = _manager_for(registry.get_cluster("meta").config_path)
    manager.load_config()

    resolved = manager._resolve_env({"PLAITA_REDIS_URL": "${redis.url}"})
    assert resolved["PLAITA_REDIS_URL"] == URL_META, (
        f"${{redis.url}} 应解析成数据面生效 URL {URL_META}，"
        f"实得 {resolved['PLAITA_REDIS_URL']}"
    )

    # 启动门实弹：launcher 实际收到的 env（worker 进程将看到）同 URL
    started_env = _start_worker(manager)
    assert started_env["PLAITA_REDIS_URL"] == URL_META


def test_metadata_mode_read_and_start_agree_no_warnings(env):
    """metadata 档一致面：``${redis.url}`` 解析 == 生效 URL，warnings 为空。"""
    factory, app, registry, monkeypatch, tmp_path = env
    client = TestClient(app)

    body = client.get("/api/clusters/meta", headers=HEADERS).json()
    assert body["data_plane_redis_url"] == URL_META
    assert body["data_plane_redis_source"] == "registry_metadata"
    assert body["warnings"] == [], (
        f"一致面不应有告警：{body['warnings']}"
    )

    manager = _manager_for(registry.get_cluster("meta").config_path)
    manager.load_config()
    assert manager._resolve_env(
        {"PLAITA_REDIS_URL": "${redis.url}"})["PLAITA_REDIS_URL"] == URL_META


@pytest.mark.parametrize("cluster_id,setup_env", [
    ("pinned", {}),                       # config_file 档
    ("envpin", {"PLAITA_CONSOLE_REDIS_URL": URL_ENV}),  # console_env 档
    ("meta", {}),                         # registry_metadata 档
])
def test_placeholder_equals_data_plane_url_across_three_sources(
        env, monkeypatch, cluster_id, setup_env):
    """三档各跑一次：``${redis.url}`` 解析始终等于 ``data_plane_redis_url``。"""
    factory, app, registry, monkeypatch, tmp_path = env
    for var, value in setup_env.items():
        monkeypatch.setenv(var, value)

    manager = _manager_for(registry.get_cluster(cluster_id).config_path)
    manager.load_config()
    resolved = manager._resolve_env({"PLAITA_REDIS_URL": "${redis.url}"})
    assert resolved["PLAITA_REDIS_URL"] == _effective_of(registry, cluster_id)

    # console_env 档读面同样报 env URL（配置占位让位）
    if cluster_id == "envpin":
        client = TestClient(app)
        body = client.get("/api/clusters/envpin", headers=HEADERS).json()
        assert body["data_plane_redis_url"] == URL_ENV
        assert body["data_plane_redis_source"] == "console_env"


def test_localhost_placeholder_env_is_rewritten_to_effective(env):
    """localhost 占位字面量仍被改写为生效 URL（既有语义不回退）。"""
    factory, app, registry, monkeypatch, tmp_path = env
    manager = _manager_for(registry.get_cluster("pinned").config_path)
    manager.load_config()
    resolved = manager._resolve_env(
        {"PLAITA_REDIS_URL": "redis://localhost:6379/0"})
    assert resolved["PLAITA_REDIS_URL"] == URL_PIN


# ---------------------------------------------------------------------------
# warnings 派生面（继承 / worker 默认 / 启动门）
# ---------------------------------------------------------------------------

def test_inherited_env_divergence_is_warned_on_read_face(env, monkeypatch):
    """无显式 Redis env 的派生 worker 继承进程旧 URL ⇒ 读面 warnings 点名。"""
    factory, app, registry, monkeypatch, tmp_path = env
    monkeypatch.setenv("PLAITA_REDIS_URL", URL_OLD)
    client = TestClient(app)

    body = client.get("/api/clusters/meta-inherit", headers=HEADERS).json()
    warnings = body["warnings"]
    assert any(
        "flow_worker" in w and URL_OLD in w and URL_META in w for w in warnings
    ), f"继承面告警应点名服务与两端 URL：{warnings}"


def test_inherited_env_consistent_no_warnings(env, monkeypatch):
    """继承 URL 与生效一致 ⇒ 不告警（不制造噪音）。"""
    factory, app, registry, monkeypatch, tmp_path = env
    monkeypatch.setenv("PLAITA_REDIS_URL", URL_META)
    client = TestClient(app)

    body = client.get("/api/clusters/meta-inherit", headers=HEADERS).json()
    assert body["warnings"] == []


def test_worker_default_localhost_divergence_is_warned(env, monkeypatch):
    """无显式 env 且进程无 Redis 变量 ⇒ worker 默认 localhost 面 warnings。"""
    factory, app, registry, monkeypatch, tmp_path = env
    client = TestClient(app)

    body = client.get("/api/clusters/meta-inherit", headers=HEADERS).json()
    warnings = body["warnings"]
    assert any(
        "flow_worker" in w and "localhost" in w and URL_META in w
        for w in warnings
    ), f"worker 默认面告警应点名 localhost 与生效 URL：{warnings}"


def test_metadata_put_face_covers_inherited_divergence(env, monkeypatch,
                                                       caplog):
    """元数据 PUT 面的 warnings + 服务端 WARNING 覆盖继承派生面。"""
    factory, app, registry, monkeypatch, tmp_path = env
    monkeypatch.setenv("PLAITA_REDIS_URL", URL_OLD)
    client = TestClient(app)

    with caplog.at_level(logging.WARNING, logger="api.cluster"):
        r = client.put("/api/clusters/meta-inherit",
                       json={"redis_url": URL_META}, headers=HEADERS)
    assert r.status_code == 200, r.text
    warnings = r.json().get("warnings") or []
    assert any(
        "flow_worker" in w and URL_OLD in w for w in warnings
    ), f"PUT 面告警应覆盖继承派生面：{warnings}"
    # 服务端也留 WARNING 痕迹（任何降级必须可观测）
    assert any("flow_worker" in rec.message for rec in caplog.records), (
        [rec.message for rec in caplog.records]
    )


def test_start_gate_warns_when_frozen_env_diverges(env, caplog):
    """启动门：冻结具体 URL ≠ 生效 URL ⇒ 服务端 WARNING（实连仍按冻结语义）。"""
    factory, app, registry, monkeypatch, tmp_path = env
    cfg_path = _write_cluster_config(tmp_path, "meta-frozen", _meta_mode_config(
        {"PLAITA_REDIS_URL": URL_OLD}))
    registry.create_cluster(
        "meta-frozen", "冻结档", config_path=cfg_path, redis_url=URL_META)

    manager = _manager_for(cfg_path)
    manager.load_config()
    with caplog.at_level(logging.WARNING, logger="services.service_manager"):
        started_env = _start_worker(manager)

    # 边界：显式具体 URL 的覆盖语义不动（A2R40-03 冻结面）
    assert started_env["PLAITA_REDIS_URL"] == URL_OLD
    assert any(
        "flow_worker" in rec.message and URL_OLD in rec.getMessage()
        for rec in caplog.records
    ), [rec.getMessage() for rec in caplog.records]
