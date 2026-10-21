"""PUT /api/clusters/{id} 改 redis_url 不得对数据面静默 no-op（A2R40-01）。

plaita#65 优先级序：集群配置文件具体地址 > CONSOLE env > 配置占位 > 注册表
元数据。PUT 只写元数据（末位）——被高优先级来源钉住时是纯 no-op，而
PUT→GET→switch 三响应都回显元数据，操作员拿三重「确认」后数据面仍钉在
退役实例（Redis 迁移场景实测 CLIENT LIST 不变、零 WARNING）。

修法（方案①+读面可观测）：
- PUT 带 redis_url 且生效来源高于元数据、与生效 URL 不同 ⇒ 具名 409
  ``cluster_redis_url_pinned``，文案给出生效来源与（脱敏）URL；
- GET/list/active/create/update/switch 响应统一携带
  ``data_plane_redis_url`` + ``data_plane_redis_source``，元数据与数据面
  错位时可观测而非撒谎；
- A2R40-03（services.*.env 创建期冻结具体 URL，``_resolve_env`` 只改写
  localhost 占位）经 config 保存面与元数据 PUT 面可达 ⇒ 冻结 env 与生效
  URL 不一致时响应 ``warnings`` + 服务端 WARNING。
"""
import logging
import sys
from pathlib import Path

import pytest
import yaml
from fakeredis import FakeRedis
from fastapi import Depends, FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import main as main_module  # noqa: E402
from api import cluster as cluster_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import cluster_registry as cluster_registry_module  # noqa: E402
from services import flow_store  # noqa: E402
from services import service_manager as service_manager_module  # noqa: E402

ADMIN_KEY = "a2r40-admin-key"
HEADERS = {"X-Admin-API-Key": ADMIN_KEY}

URL_PIN = "redis://pin-redis:6379/0"        # 配置文件钉住的具体地址
URL_NEW = "redis://new-redis:6379/0"        # 操作员想迁去的目标
URL_ENV = "redis://env-redis:6379/0"        # PLAITA_CONSOLE_REDIS_URL
URL_META = "redis://meta-redis:6379/0"      # 元数据生效档的初始值
URL_META2 = "redis://meta2-redis:6379/0"    # 元数据生效档的迁移目标
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


@pytest.fixture()
def env(tmp_path, monkeypatch):
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    # 环境变量逐测试自行 set/del：config_file 档要求 env 未设，console_env
    # 档要求 env 已设，不能在 fixture 里一刀切。
    monkeypatch.delenv("PLAITA_CONSOLE_REDIS_URL", raising=False)

    flow_store.init_engine(f"sqlite:///{tmp_path}/a2r40.db")

    factory = _FakeRedisByUrl()
    monkeypatch.setattr(main_module, "connect_redis", factory)

    registry = cluster_registry_module.ClusterRegistry(
        base_dir=str(tmp_path / "clusters"))

    # ① 配置文件钉住档：preset 把具体地址写进 redis.url 与 services env
    registry.create_cluster("pinned", "钉住档", redis_url=URL_PIN)

    # ② 元数据生效档：配置文件没有 redis 键——优先级序里只剩元数据
    meta_cfg = {
        "profile": "dev",
        "mode": "process",
        "services": {
            "flow_worker": {
                "display_name": "流程执行器",
                "process": {"module": "plaita.server.flow_worker"},
                "env": {"PLAITA_REDIS_URL": URL_PIN},  # 冻结的具体 URL（A2R40-03）
            },
        },
    }
    meta_dir = tmp_path / "meta-cluster"
    meta_dir.mkdir()
    meta_cfg_path = meta_dir / "cluster_config.yaml"
    meta_cfg_path.write_text(yaml.safe_dump(meta_cfg), encoding="utf-8")
    registry.create_cluster(
        "meta", "元数据档", config_path=str(meta_cfg_path), redis_url=URL_META)

    # ③ 配置占位档：redis.url 是 localhost 占位（给 console_env 测试用）
    registry.create_cluster("envpin", "占位档", redis_url=PLACEHOLDER)

    registry.switch_cluster("pinned")
    monkeypatch.setattr(cluster_registry_module, "_cluster_registry", registry)
    monkeypatch.setattr(service_manager_module, "_manager", None)

    app = FastAPI()
    app.state.redis = factory(URL_PIN)
    app.state.redis_url = URL_PIN
    app.state.local_mode = False
    app.state.store = flow_store.get_flow_store()
    app.include_router(cluster_api.router, prefix="/api",
                       dependencies=[Depends(require_auth)])

    return factory, app, registry, monkeypatch


def test_put_redis_url_pinned_by_config_file_is_named_rejected(env):
    """配置文件钉住时 PUT 改 redis_url ⇒ 具名 409，不再三重假确认。

    修复前：PUT 200、GET/switch 全部回显新 URL（元数据），数据面仍连旧实例。
    """
    factory, app, registry, monkeypatch = env
    client = TestClient(app)

    r = client.put("/api/clusters/pinned", json={"redis_url": URL_NEW},
                   headers=HEADERS)
    assert r.status_code == 409, r.text
    detail = r.json()["detail"]
    assert detail["code"] == "cluster_redis_url_pinned"
    # 文案给出当前生效来源与 URL（无口令 URL 脱敏后原样保留）
    assert "配置文件" in detail["message"]
    assert URL_PIN in detail["message"]
    assert URL_NEW not in detail["message"]
    assert detail["effective_source"] == "config_file"
    assert detail["effective_redis_url"] == URL_PIN

    # 拒绝即整体拒绝：元数据不动、数据面来源可观测
    g = client.get("/api/clusters/pinned", headers=HEADERS)
    assert g.status_code == 200, g.text
    body = g.json()
    assert body["redis_url"] == URL_PIN
    assert body["data_plane_redis_url"] == URL_PIN
    assert body["data_plane_redis_source"] == "config_file"


def test_read_and_switch_faces_report_data_plane_source_not_bare_metadata(env):
    """元数据与数据面错位（历史遗留数据）时，读/切面显式给出生效 URL 与来源。

    直接改注册表模拟 pre-fix 时代的存量错位：元数据 NEW、配置文件钉 PIN。
    """
    factory, app, registry, monkeypatch = env
    registry.update_cluster("pinned", redis_url=URL_NEW)  # 绕过 API 的存量错位
    client = TestClient(app)

    g = client.get("/api/clusters/pinned", headers=HEADERS)
    body = g.json()
    assert body["redis_url"] == URL_NEW  # 元数据原样
    assert body["data_plane_redis_url"] == URL_PIN  # 数据面真相
    assert body["data_plane_redis_source"] == "config_file"

    lst = client.get("/api/clusters", headers=HEADERS).json()
    entry = next(c for c in lst["clusters"] if c["id"] == "pinned")
    assert entry["data_plane_redis_url"] == URL_PIN
    assert entry["data_plane_redis_source"] == "config_file"

    active = client.get("/api/clusters/active", headers=HEADERS).json()
    assert active["data_plane_redis_url"] == URL_PIN
    assert active["data_plane_redis_source"] == "config_file"

    # switch 实连的就是生效 URL，响应不得只报元数据
    s = client.post("/api/clusters/switch", json={"cluster_id": "pinned"},
                    headers=HEADERS)
    assert s.status_code == 200, s.text
    sbody = s.json()
    assert sbody["data_plane_redis_url"] == URL_PIN
    assert sbody["data_plane_redis_source"] == "config_file"
    assert app.state.redis_url == URL_PIN


def test_put_redis_url_pinned_by_console_env_is_named_rejected(env):
    """CONSOLE env 钉住（配置文件只有 localhost 占位）时同样具名拒绝。"""
    factory, app, registry, monkeypatch = env
    monkeypatch.setenv("PLAITA_CONSOLE_REDIS_URL", URL_ENV)
    client = TestClient(app)

    r = client.put("/api/clusters/envpin", json={"redis_url": URL_NEW},
                   headers=HEADERS)
    assert r.status_code == 409, r.text
    detail = r.json()["detail"]
    assert detail["code"] == "cluster_redis_url_pinned"
    assert detail["effective_source"] == "console_env"
    assert detail["effective_redis_url"] == URL_ENV
    assert URL_ENV in detail["message"]

    # 读面：元数据是占位、数据面真相是 env URL——错位可观测
    body = client.get("/api/clusters/envpin", headers=HEADERS).json()
    assert body["redis_url"] == PLACEHOLDER
    assert body["data_plane_redis_url"] == URL_ENV
    assert body["data_plane_redis_source"] == "console_env"


def test_put_redis_url_takes_effect_when_metadata_is_top_priority(env):
    """元数据本就是最高优先级（配置文件无 redis 键、env 未设）⇒ 正常生效。"""
    factory, app, registry, monkeypatch = env
    client = TestClient(app)

    r = client.put("/api/clusters/meta", json={"redis_url": URL_META2},
                   headers=HEADERS)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["redis_url"] == URL_META2
    assert body["data_plane_redis_url"] == URL_META2
    assert body["data_plane_redis_source"] == "registry_metadata"

    g = client.get("/api/clusters/meta", headers=HEADERS).json()
    assert g["data_plane_redis_url"] == URL_META2
    assert g["data_plane_redis_source"] == "registry_metadata"

    # switch 按元数据重绑——正常路径不被误伤
    s = client.post("/api/clusters/switch", json={"cluster_id": "meta"},
                    headers=HEADERS)
    assert s.status_code == 200, s.text
    assert app.state.redis_url == URL_META2
    assert app.state.redis is factory.clients[URL_META2]


def test_put_redis_url_equal_to_effective_is_allowed(env):
    """PUT 与生效 URL 相同 ⇒ 放行（把元数据对齐数据面真相是合法操作）。"""
    factory, app, registry, monkeypatch = env
    client = TestClient(app)

    r = client.put("/api/clusters/pinned", json={"redis_url": URL_PIN},
                   headers=HEADERS)
    assert r.status_code == 200, r.text
    assert r.json()["redis_url"] == URL_PIN
    assert r.json()["data_plane_redis_source"] == "config_file"


def test_put_name_only_still_works_when_pinned(env):
    """钉住档只改名称/描述（不带 redis_url）⇒ 不受 409 闸影响。"""
    factory, app, registry, monkeypatch = env
    client = TestClient(app)

    r = client.put("/api/clusters/pinned",
                   json={"name": "改名后的钉住档", "description": "d"},
                   headers=HEADERS)
    assert r.status_code == 200, r.text
    assert r.json()["name"] == "改名后的钉住档"
    assert r.json()["redis_url"] == URL_PIN


def test_config_save_warns_when_service_env_freezes_old_url(env, caplog):
    """A2R40-03 经 config 保存面可达：改 redis.url 后冻结 env 显式告警。"""
    factory, app, registry, monkeypatch = env
    client = TestClient(app)

    cfg = registry.get_cluster_config("pinned")
    assert cfg["redis"]["url"] == URL_PIN
    cfg["redis"]["url"] = URL_NEW  # services.*.env.PLAITA_REDIS_URL 仍冻结 URL_PIN

    with caplog.at_level(logging.WARNING, logger="api.cluster"):
        r = client.put("/api/clusters/pinned/config", json={"config": cfg},
                       headers=HEADERS)
    assert r.status_code == 200, r.text
    body = r.json()
    warnings = body.get("warnings") or []
    assert any("flow_worker" in w and URL_PIN in w and URL_NEW in w
               for w in warnings)
    # 服务端也留 WARNING 痕迹（任何降级必须可观测）
    assert any("冻结" in rec.message for rec in caplog.records)

    # 数据面重绑到新 URL 的既有行为不变；冻结风险由 warnings 承载
    assert app.state.redis_url == URL_NEW


def test_metadata_put_warns_when_service_env_frozen(env, caplog):
    """A2R40-03 经元数据 PUT 面可达：元数据生效档改 URL 后冻结 env 告警。"""
    factory, app, registry, monkeypatch = env
    client = TestClient(app)

    with caplog.at_level(logging.WARNING, logger="api.cluster"):
        r = client.put("/api/clusters/meta", json={"redis_url": URL_META2},
                       headers=HEADERS)
    assert r.status_code == 200, r.text
    warnings = r.json().get("warnings") or []
    assert any("flow_worker" in w and URL_PIN in w for w in warnings)
    assert any("冻结" in rec.message for rec in caplog.records)
