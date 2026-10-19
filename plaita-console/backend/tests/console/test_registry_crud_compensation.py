"""集群注册表 create/delete 多资源写盘的补偿回归（duo-a 第三十六轮 ②）。

## 缺陷

``ClusterRegistry.create_cluster`` / ``delete_cluster`` 各自要动两处资源：

- create：``<base>/<id>/cluster_config.yaml`` + ``registry.yaml``；
- delete：目录 ``rmtree`` + ``registry.yaml``。

旧实现没有失败补偿，两处不一致时留下不可重试的残骸：

1. **create save 失败 → 残留目录**：目录和配置已写下，``_save_registry``
   抛错（ENOSPC 等）后内存登记回滚，但磁盘目录还在；重试 create 会先命中
   「集群已存在」永远不会被调用（登记已回滚），于是目录成为无主残骸。
2. **delete save 失败 → ghost 条目**：先 ``shutil.rmtree`` 再删内存再保存；
   保存失败时磁盘 ``registry.yaml`` 仍登记该集群（#249 原子写保证旧文件
   完好），但目录已毁、内存登记已删——列表看得到、点开配置全空，且目录
   无法恢复、重试也删不掉 ghost（内存里没有它）。

## 修法

- create：目录 / 配置 / registry 三段写盘按补偿式事务处理；任一失败只回滚
  **本次新建**的路径（显式传入的 ``config_path`` 或既有目录不动）。
- delete：先把目录 ``os.replace`` 到同目录暂存名（同文件系统原子），再删
  内存并保存；保存失败把目录改回来、内存登记复原；保存成功才清理暂存目录。

## 负控

仅回退 ``cluster_registry.py``（保留本文件）：

- create save 失败用例断言「目录不存在」→ 旧实现红（残留目录）；
- delete save 失败用例断言「目录仍在 + 登记仍在 + 暂存名已改回」→ 旧实现
  红（目录已被 rmtree，内存登记已删）。

证据见 ``docs/proofs/data/duo36-registry-compensation-negative-control.txt``。
"""
from __future__ import annotations

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
    return cluster_registry_module.ClusterRegistry(base_dir=str(base))


def _boom_save():
    raise OSError(28, "No space left on device")


def _ids(reg) -> list:
    return sorted(info.id for info in reg.list_clusters())


def test_create_save_failure_rolls_back_directory_and_entry(registry, base, monkeypatch):
    """save 失败后不得留下 <base>/<id>/ 残骸，恢复后可正常重试。"""
    before = registry.registry_file.read_bytes()

    with monkeypatch.context() as m:
        m.setattr(registry, "_save_registry", _boom_save)
        with pytest.raises(OSError, match="No space left on device"):
            registry.create_cluster("beta", "集群 Beta")

    assert not (base / "beta").exists(), "create 失败留下了无主目录"
    assert registry.get_cluster("beta") is None, "内存登记未回滚"
    assert registry.registry_file.read_bytes() == before
    assert _ids(registry) == ["default"]

    # 登记与目录都干净：重试不会被「已存在」误拦。
    info = registry.create_cluster("beta", "集群 Beta")
    assert info.id == "beta"
    assert (base / "beta" / "cluster_config.yaml").is_file()
    assert _ids(registry) == ["beta", "default"]


def test_create_config_failure_rolls_back_directory(registry, base, monkeypatch):
    """配置写盘本身失败（而非 registry）同样回收刚建的目录。"""

    def exploding_config(self, config_path, preset="dev", redis_url="redis://localhost:6379/0"):
        Path(config_path).write_text("profile: partial", encoding="utf-8")
        raise OSError(28, "No space left on device")

    with monkeypatch.context() as m:
        m.setattr(
            cluster_registry_module.ClusterRegistry,
            "_create_default_config",
            exploding_config,
        )
        with pytest.raises(OSError, match="No space left on device"):
            registry.create_cluster("gamma", "集群 Gamma")

    assert not (base / "gamma").exists(), "配置写失败留下了半成品目录"
    assert registry.get_cluster("gamma") is None


def test_create_save_failure_keeps_caller_supplied_config(registry, base, monkeypatch):
    """显式 config_path 是调用方资源：save 失败只回滚登记，不删文件。"""
    external = base / "external" / "cfg.yaml"
    external.parent.mkdir(parents=True)
    external.write_text("profile: dev\n", encoding="utf-8")

    with monkeypatch.context() as m:
        m.setattr(registry, "_save_registry", _boom_save)
        with pytest.raises(OSError, match="No space left on device"):
            registry.create_cluster("delta", "集群 Delta", config_path=str(external))

    assert external.is_file(), "回滚误删了调用方提供的配置文件"
    assert registry.get_cluster("delta") is None


def test_delete_save_failure_keeps_directory_and_entry(registry, base, monkeypatch):
    """delete save 失败时目录 / 内存登记 / 磁盘 registry 三者保持一致。"""
    registry.create_cluster("beta", "集群 Beta")
    beta_dir = base / "beta"
    config_file = beta_dir / "cluster_config.yaml"
    assert config_file.is_file()
    before = registry.registry_file.read_bytes()

    with monkeypatch.context() as m:
        m.setattr(registry, "_save_registry", _boom_save)
        with pytest.raises(OSError, match="No space left on device"):
            registry.delete_cluster("beta")

    assert beta_dir.is_dir(), "delete 保存失败后目录已被 rmtree（ghost 条目）"
    assert config_file.is_file(), "集群配置已丢，登记却还在"
    assert registry.get_cluster("beta") is not None, "内存登记未复原"
    assert registry.registry_file.read_bytes() == before, "失败的 delete 改动了注册表"
    assert list(base.glob(".beta.deleting.*")) == [], "暂存目录未改回原路径"

    # 磁盘登记仍带 beta：重启后可正常服务（不是列表有、文件无的 ghost）。
    restarted = cluster_registry_module.ClusterRegistry(base_dir=str(base))
    assert _ids(restarted) == ["beta", "default"]
    assert restarted.get_cluster_config("beta") is not None

    # 恢复后重试删除成功，目录与登记同时消失。
    assert registry.delete_cluster("beta") is True
    assert not beta_dir.exists()
    assert registry.get_cluster("beta") is None
    assert list(base.glob(".beta.deleting.*")) == []


def test_delete_success_still_removes_directory(registry, base):
    """成功路径不回归：目录与登记一起消失，无暂存残骸。"""
    registry.create_cluster("beta", "集群 Beta")
    assert (base / "beta" / "cluster_config.yaml").is_file()

    assert registry.delete_cluster("beta") is True

    assert not (base / "beta").exists()
    assert registry.get_cluster("beta") is None
    assert list(base.glob(".beta.deleting.*")) == []
    assert cluster_registry_module.ClusterRegistry(base_dir=str(base)).get_cluster("beta") is None
