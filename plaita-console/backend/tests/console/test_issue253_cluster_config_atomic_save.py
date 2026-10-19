"""plaita#253：cluster_config.yaml 两条保存路径原子写 + 载入失败具名报错。

负控手法：
- monkeypatch ``yaml.dump``：先往调用方传入的 stream 写半截再抛
  ``OSError(ENOSPC)``。旧实现把真实文件句柄交给 dump，原文件当场被截断成
  半截；新实现先序列化到字符串、目标文件从不交给 dump，保存失败后旧文件
  逐字节不变，且两处失败分支都打 WARNING（不再 ``return False``/``print``
  静默）。
- 预置半截 YAML：``GET /api/cluster/config`` 必须 503（非 200 standalone），
  ``/api/cluster/service-types`` 的错误信息指向损坏文件；连续两次请求都
  失败，证明没有把 ``config=None`` 的坏单例登记下来、也没有回退默认配置。
"""
import logging
import sys
from pathlib import Path

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import cluster as cluster_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import cluster_registry as cluster_registry_module  # noqa: E402
from services import service_manager as service_manager_module  # noqa: E402

CORRUPT_CONFIG = b"mode: process\nservices:\n  flow_work"


def _write_config(path: Path, services=None) -> bytes:
    """写一份合法的 cluster_config.yaml，返回落盘字节。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "mode": "process",
        "redis": {"url": "redis://localhost:6379/0"},
        "services": services or {},
        "infrastructure": {
            "redis": {
                "display_name": "Redis",
                "type": "redis",
                "enabled": True,
                "url": "redis://localhost:6379/0",
            },
        },
    }
    text = cluster_registry_module.yaml.dump(
        payload, allow_unicode=True, default_flow_style=False)
    path.write_text(text, encoding="utf-8")
    return path.read_bytes()


def _exploding_dump(data, stream=None, **kwargs):
    """复刻旧实现的中途失败：先写半截到目标流，再报磁盘满。"""
    if stream is not None:
        stream.write("mode: process\nservices:\n  flow_work")
        stream.flush()
    raise OSError(28, "No space left on device")


def _temp_files(directory: Path) -> list:
    return list(directory.glob(".*.tmp"))


def _client() -> TestClient:
    app = FastAPI()
    # 本文件只测保存/载入原子性；#261 的 platform_admin 闸用平台身份绕过。
    app.dependency_overrides[require_auth] = lambda: {
        "actor": "test", "role": "admin", "tenant_id": None, "platform_admin": True,
    }
    app.include_router(cluster_api.router, prefix="/api")
    return TestClient(app)


@pytest.fixture()
def registry(tmp_path, monkeypatch):
    base = tmp_path / "clusters"
    reg = cluster_registry_module.ClusterRegistry(base_dir=str(base))
    reg.create_cluster(
        "alpha", "集群 Alpha", config_path=str(base / "alpha" / "cluster_config.yaml"))
    _write_config(
        Path(reg.get_cluster("alpha").config_path),
        {"flow_worker": {
            "display_name": "流程执行器",
            "process": {"module": "plaita.server.flow_worker"},
            "max_instances": 10,
        }},
    )
    monkeypatch.setattr(cluster_registry_module, "_cluster_registry", reg)
    monkeypatch.setattr(service_manager_module, "_manager", None)
    return reg


@pytest.fixture()
def manager(tmp_path, monkeypatch):
    config_path = tmp_path / "cluster_config.yaml"
    before = _write_config(config_path)
    sm = service_manager_module.ServiceManager(config_path=str(config_path))
    sm.load_config()
    monkeypatch.setattr(service_manager_module, "_manager", sm)
    return sm, config_path, before


@pytest.fixture()
def broken_env(tmp_path, monkeypatch):
    """活动集群指向一份半截 YAML，且没有可静默回退的默认配置。"""
    base = tmp_path / "clusters"
    reg = cluster_registry_module.ClusterRegistry(base_dir=str(base))
    config_path = base / "broken" / "cluster_config.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_bytes(CORRUPT_CONFIG)
    reg.create_cluster("broken", "损坏集群", config_path=str(config_path))
    reg.switch_cluster("broken")
    monkeypatch.setattr(cluster_registry_module, "_cluster_registry", reg)
    monkeypatch.setattr(service_manager_module, "_manager", None)
    return config_path


# ---------------- 保存路径 1：ClusterRegistry.save_cluster_config ----------------

def test_registry_save_enospc_keeps_original_and_warns(registry, caplog, monkeypatch):
    info = registry.get_cluster("alpha")
    before = Path(info.config_path).read_bytes()
    monkeypatch.setattr(cluster_registry_module.yaml, "dump", _exploding_dump)

    with caplog.at_level(logging.WARNING, logger="services.cluster_registry"):
        assert registry.save_cluster_config(
            "alpha", {"mode": "process", "services": {}}) is False

    assert Path(info.config_path).read_bytes() == before, "失败保存毁掉了上一份完好配置"
    assert "保存集群 alpha 配置失败（原文件保留）" in caplog.text
    assert info.config_path in caplog.text
    assert _temp_files(Path(info.config_path).parent) == []


def test_put_cluster_config_enospc_returns_4xx_and_keeps_original(
        registry, caplog, monkeypatch):
    info = registry.get_cluster("alpha")
    before = Path(info.config_path).read_bytes()
    client = _client()
    monkeypatch.setattr(cluster_registry_module.yaml, "dump", _exploding_dump)

    with caplog.at_level(logging.WARNING, logger="services.cluster_registry"):
        response = client.put(
            "/api/clusters/alpha/config",
            json={"config": {"mode": "process", "services": {}}},
        )

    assert response.status_code == 400, response.text
    assert Path(info.config_path).read_bytes() == before
    assert "保存集群 alpha 配置失败（原文件保留）" in caplog.text
    assert _temp_files(Path(info.config_path).parent) == []


# ---------------- 保存路径 2：ServiceManager._save_config ----------------

def test_service_manager_save_enospc_keeps_original_and_warns(
        manager, caplog, monkeypatch):
    sm, config_path, before = manager
    monkeypatch.setattr(service_manager_module.yaml, "dump", _exploding_dump)

    with caplog.at_level(logging.WARNING, logger="services.service_manager"):
        assert sm._save_config() is False

    assert config_path.read_bytes() == before, "失败保存毁掉了上一份完好配置"
    assert "保存配置失败（原文件保留）" in caplog.text
    assert str(config_path) in caplog.text
    assert _temp_files(config_path.parent) == []


def test_post_infrastructure_enospc_returns_5xx_and_keeps_original(
        manager, caplog, monkeypatch):
    sm, config_path, before = manager
    client = _client()
    monkeypatch.setattr(service_manager_module.yaml, "dump", _exploding_dump)

    with caplog.at_level(logging.WARNING, logger="services.service_manager"):
        response = client.post(
            "/api/cluster/infrastructure",
            json={
                "name": "kafka",
                "display_name": "Kafka",
                "type": "kafka",
                "bootstrap_servers": "localhost:9092",
            },
        )

    assert response.status_code == 500, response.text
    assert config_path.read_bytes() == before
    assert "保存配置失败（原文件保留）" in caplog.text
    assert _temp_files(config_path.parent) == []


# ---------------- 载入路径：损坏/截断必须具名失败，不伪装健康 ----------------

def test_corrupt_config_returns_named_503_and_stays_failed(broken_env, caplog):
    config_path = broken_env
    client = _client()

    with caplog.at_level(logging.WARNING, logger="services.service_manager"):
        first = client.get("/api/cluster/config")
        second = client.get("/api/cluster/config")
        service_types = client.get("/api/cluster/service-types")
        infrastructure = client.get("/api/cluster/infrastructure")

    for response in (first, second, service_types, infrastructure):
        assert response.status_code == 503, response.text
        detail = response.json()["detail"]
        assert "集群配置载入失败" in detail
        assert str(config_path) in detail

    # 半截文件原样保留：改名会让下一次加载把它当「路径失效」回退到别的
    # checkout，故障再次被伪装成健康。
    assert config_path.read_bytes() == CORRUPT_CONFIG
    assert "集群配置载入失败" in caplog.text
    assert str(config_path) in caplog.text
    # 失败不得登记半初始化单例：下一次请求仍要重新尝试。
    assert service_manager_module._manager is None


@pytest.mark.parametrize(
    "payload",
    [
        b"",                        # 空文件（写第一个字节前就失败）
        b"mode: [\n",               # YAML 语法损坏
        b"services: []\n",          # services 被截成列表
        b"- alpha\n",               # 顶层不是映射
    ],
    ids=["empty", "bad-yaml", "services-list", "top-level-list"],
)
def test_all_corrupt_shapes_raise_named_error(tmp_path, payload):
    config_path = tmp_path / "cluster_config.yaml"
    config_path.write_bytes(payload)
    sm = service_manager_module.ServiceManager(config_path=str(config_path))

    with pytest.raises(service_manager_module.ClusterConfigError) as excinfo:
        sm.load_config()

    assert str(config_path) in str(excinfo.value)
    assert sm.config is None
    assert config_path.read_bytes() == payload


# ---------------- 正常路径回归 ----------------

def test_successful_saves_keep_complete_content(registry, manager):
    sm, config_path, _ = manager
    assert sm._save_config() is True
    saved = cluster_registry_module.yaml.safe_load(
        config_path.read_text(encoding="utf-8"))
    assert saved["mode"] == "process"
    assert "redis" in saved["infrastructure"]
    assert _temp_files(config_path.parent) == []

    info = registry.get_cluster("alpha")
    assert registry.save_cluster_config(
        "alpha", {"mode": "process", "services": {}}) is True
    saved = cluster_registry_module.yaml.safe_load(
        Path(info.config_path).read_text(encoding="utf-8"))
    assert saved == {"mode": "process", "services": {}}
    assert _temp_files(Path(info.config_path).parent) == []


def test_healthy_config_endpoint_still_200(manager):
    client = _client()
    response = client.get("/api/cluster/config")
    assert response.status_code == 200, response.text
    assert response.json()["mode"] == "process"
    assert response.json()["config_path"] == str(manager[1])
