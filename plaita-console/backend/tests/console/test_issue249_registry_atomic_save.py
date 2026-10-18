"""plaita#249：集群注册表原子保存 + 载入失败隔离——一次失败不得抹掉全部登记。

负控手法：
- monkeypatch ``yaml.dump`` 模拟写到一半 ENOSPC：旧实现先 ``open(..., 'w')``
  截断 registry.yaml，再写半截后抛错，原文件必被改动；原子实现下旧文件逐字节
  不变，重启（注入仍在）仍能载入全部集群、不会重建 default 覆盖。
- 预置截断 / 非映射 registry.yaml：断言具名 WARNING、``.corrupt.<ts>`` 备份
  保留原字节、本次启动不重建 default。
"""
import logging
import sys
from pathlib import Path

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services import cluster_registry as cluster_registry_module  # noqa: E402


@pytest.fixture()
def base(tmp_path):
    return tmp_path / "clusters"


@pytest.fixture()
def registry(base):
    reg = cluster_registry_module.ClusterRegistry(base_dir=str(base))
    reg.create_cluster(
        "alpha", "集群 Alpha", config_path=str(base / "alpha" / "cluster_config.yaml"))
    reg.create_cluster(
        "keepme", "集群 Keep", config_path=str(base / "keepme" / "cluster_config.yaml"))
    return reg


def _ids(reg) -> list:
    return sorted(info.id for info in reg.list_clusters())


def _assert_no_temp_files(base: Path) -> None:
    assert list(base.glob(".registry.yaml.*.tmp")) == []


def test_dump_enospc_keeps_previous_registry_and_restart_loads_all(
        registry, base, monkeypatch):
    before = registry.registry_file.read_bytes()
    before_ids = _ids(registry)
    assert before_ids == ["alpha", "default", "keepme"]

    def exploding_dump(data, stream=None, **kwargs):
        # 复刻旧实现的中途失败：先写半截到目标流，再报磁盘满。
        if stream is not None:
            stream.write("clusters:\n  default: {name: truncat")
            stream.flush()
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(cluster_registry_module.yaml, "dump", exploding_dump)

    with pytest.raises(OSError, match="No space left on device"):
        registry.create_cluster(
            "beta", "集群 Beta", config_path=str(base / "beta" / "cluster_config.yaml"))

    assert registry.registry_file.read_bytes() == before, "失败的保存改动了上一份完好文件"
    _assert_no_temp_files(base)

    # 模拟重启：yaml.dump 注入仍在。旧实现此时文件已被截断并会重建 default。
    restarted = cluster_registry_module.ClusterRegistry(base_dir=str(base))
    assert _ids(restarted) == before_ids
    assert restarted.get_active_cluster().id == "default"
    assert registry.registry_file.read_bytes() == before


def test_fsync_failure_keeps_previous_registry(registry, base, monkeypatch):
    before = registry.registry_file.read_bytes()
    hits = []

    def boom(fd):
        hits.append(True)
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(cluster_registry_module.os, "fsync", boom)

    with pytest.raises(OSError, match="No space left on device"):
        registry.update_cluster("alpha", name="改名")

    assert hits
    assert registry.registry_file.read_bytes() == before
    _assert_no_temp_files(base)


def test_replace_failure_keeps_previous_registry(registry, base, monkeypatch):
    before = registry.registry_file.read_bytes()
    hits = []

    def boom(src, dst):
        hits.append(True)
        raise OSError(5, "Input/output error")

    monkeypatch.setattr(cluster_registry_module.os, "replace", boom)

    with pytest.raises(OSError, match="Input/output error"):
        registry.update_cluster("alpha", name="改名")

    assert hits
    assert registry.registry_file.read_bytes() == before
    _assert_no_temp_files(base)


def test_successful_save_leaves_no_temp_and_complete_content(registry, base):
    registry.create_cluster(
        "beta", "集群 Beta", config_path=str(base / "beta" / "cluster_config.yaml"))

    data = cluster_registry_module.yaml.safe_load(
        registry.registry_file.read_text(encoding="utf-8"))
    assert sorted(data["clusters"]) == ["alpha", "beta", "default", "keepme"]
    assert data["active_cluster"] == "default"
    _assert_no_temp_files(base)


def test_corrupt_registry_is_quarantined_and_not_rebuilt(registry, base, caplog):
    corrupt = b"clusters:\n  default: {name: truncat"
    registry.registry_file.write_bytes(corrupt)

    with caplog.at_level(logging.WARNING, logger="services.cluster_registry"):
        restarted = cluster_registry_module.ClusterRegistry(base_dir=str(base))

    warnings = [
        record for record in caplog.records
        if record.name == "services.cluster_registry"
        and record.levelno == logging.WARNING
    ]
    assert warnings, "载入失败必须打具名 WARNING"
    assert "集群注册表载入失败" in caplog.text
    assert "registry.yaml.corrupt." in caplog.text

    backups = list(base.glob("registry.yaml.corrupt.*"))
    assert len(backups) == 1
    assert backups[0].read_bytes() == corrupt, "损坏残文件必须原样保留"
    assert not registry.registry_file.exists(), "载入失败不得重建 default 覆盖原文件"
    assert restarted.list_clusters() == []
    assert restarted.get_active_cluster() is None


@pytest.mark.parametrize("payload", [b"clusters: []\n", b"- alpha\n"])
def test_non_mapping_registry_is_quarantined(registry, base, caplog, payload):
    registry.registry_file.write_bytes(payload)

    with caplog.at_level(logging.WARNING, logger="services.cluster_registry"):
        restarted = cluster_registry_module.ClusterRegistry(base_dir=str(base))

    assert "集群注册表载入失败" in caplog.text
    backups = list(base.glob("registry.yaml.corrupt.*"))
    assert len(backups) == 1
    assert backups[0].read_bytes() == payload
    assert not registry.registry_file.exists()
    assert restarted.list_clusters() == []
