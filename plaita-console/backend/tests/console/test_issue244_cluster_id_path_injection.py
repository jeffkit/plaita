"""plaita#244（P1 security）：``POST /api/clusters`` 的 ``id`` 未校验——路径注入三把刀。

修复前 ``cluster_id`` 原样拼进 ``base_dir / cluster_id``：

1. **覆盖写**：``POST {"id": "../outside"}`` 的配置落到 ``base_dir/../outside/
   cluster_config.yaml``，覆盖任意可写 YAML 文件；
2. **rmtree**：先创建该条目再 ``DELETE``，``shutil.rmtree(base_dir/../outside)``
   删除任意目录；
3. **registry 持久化**：越界 id 被写进 ``registry.yaml``，重启后仍在册。

两道闸：

* API schema：``CreateClusterRequest.id`` 的 ``pattern`` 只放行
  ``^[a-z0-9][a-z0-9-]*$``（路径分隔符 / ``..`` / 绝对路径 / 大写与下划线均 422）；
* registry：``_cluster_dir`` 对 ``(base_dir / id).resolve()`` 做 ``is_relative_to``
  归属复核——创建写盘与删除 ``rmtree`` 同闸，schema 被绕过（直调 registry /
  符号链接 / 修复前遗留的 registry.yaml）也逃不出去。

负控：分别还原两处校验，对应用例必须变红（见交付台账）。
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import NamedTuple

import pytest
import yaml
from fastapi import FastAPI, HTTPException
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import cluster as cluster_api  # noqa: E402
from services import cluster_registry as cluster_registry_module  # noqa: E402


class Env(NamedTuple):
    registry: cluster_registry_module.ClusterRegistry
    client: TestClient
    base: Path
    outside: Path
    victim_config: Path
    victim_dir: Path


@pytest.fixture()
def env(tmp_path, monkeypatch) -> Env:
    base = tmp_path / "clusters"
    # 越界目标：同级的 outside/，内含可被覆盖的 config 与可被 rmtree 的目录。
    outside = tmp_path / "outside"
    outside.mkdir()
    victim_config = outside / "cluster_config.yaml"
    victim_config.write_text("owned: before\n", encoding="utf-8")
    victim_dir = outside / "victim"
    victim_dir.mkdir()
    (victim_dir / "keep.txt").write_text("keep\n", encoding="utf-8")

    registry = cluster_registry_module.ClusterRegistry(base_dir=str(base))
    monkeypatch.setattr(cluster_registry_module, "_cluster_registry", registry)

    app = FastAPI()
    app.include_router(cluster_api.router, prefix="/api")
    return Env(
        registry=registry,
        client=TestClient(app),
        base=base,
        outside=outside,
        victim_config=victim_config,
        victim_dir=victim_dir,
    )


def _create(client: TestClient, cluster_id: str):
    return client.post("/api/clusters", json={"id": cluster_id, "name": "越界集群"})


def _inject_legacy_entry(env: Env, legacy_id: str) -> None:
    """模拟修复前已落盘的恶意条目：registry.yaml 重启后仍会加载到内存。"""
    env.registry._clusters[legacy_id] = cluster_registry_module.ClusterInfo(
        id=legacy_id,
        name="legacy",
        config_path=str(env.victim_dir / "cluster_config.yaml"),
    )


@pytest.mark.parametrize(
    "cluster_id",
    [
        "../outside",             # 覆盖写：base_dir/../outside/cluster_config.yaml
        "../clusters-evil",       # 同前缀兄弟目录
        "../../escape",           # 多级逃逸
        "a/b",                    # 路径分隔符
        "..",                     # 父目录
        ".",                      # base_dir 本身
        "/tmp/absolute-cluster",  # 绝对路径（Path 拼接会丢弃 base_dir）
        "A",                      # 大写不在白名单
        "a_b",                    # 下划线不在白名单
        "-leading-dash",          # 首字符非字母/数字
        "",                       # 空 id
    ],
)
def test_api_create_rejects_non_member_ids(env: Env, cluster_id: str) -> None:
    before = env.victim_config.read_text(encoding="utf-8")
    r = _create(env.client, cluster_id)

    assert r.status_code == 422, (cluster_id, r.status_code, r.text)
    # 覆盖写未发生，且越界 id 没有进入注册表 / registry.yaml。
    assert env.victim_config.read_text(encoding="utf-8") == before
    assert set(env.registry._clusters) == {"default"}
    saved = yaml.safe_load(env.registry.registry_file.read_text(encoding="utf-8"))
    assert set(saved["clusters"]) == {"default"}


@pytest.mark.parametrize(
    "cluster_id",
    ["../outside", "../clusters-evil", "../../escape", "/tmp/abs"],
)
def test_registry_create_blocks_traversal_without_schema(env: Env, cluster_id: str) -> None:
    """schema 被绕过（直调 registry）时，写盘前的归属复核仍然拒绝。"""
    before = env.victim_config.read_text(encoding="utf-8")

    with pytest.raises(ValueError, match="集群 ID 非法"):
        env.registry.create_cluster(cluster_id, "越界集群")

    assert env.victim_config.read_text(encoding="utf-8") == before
    assert set(env.registry._clusters) == {"default"}
    saved = yaml.safe_load(env.registry.registry_file.read_text(encoding="utf-8"))
    assert set(saved["clusters"]) == {"default"}


def test_registry_create_blocks_symlinked_cluster_dir(env: Env) -> None:
    """id 本身合法但目录是指向 base_dir 外的符号链接：resolve 后归属复核拒绝。"""
    (env.base / "link").symlink_to(env.outside, target_is_directory=True)
    before = env.victim_config.read_text(encoding="utf-8")

    with pytest.raises(ValueError, match="集群 ID 非法"):
        env.registry.create_cluster("link", "越界集群")

    assert "link" not in env.registry._clusters
    assert env.victim_config.read_text(encoding="utf-8") == before


def test_registry_delete_never_rmtrees_outside_base(env: Env) -> None:
    """修复前遗留的越界 id 在册：delete 与 create 同闸，拒绝 rmtree。"""
    legacy_id = "../outside/victim"
    _inject_legacy_entry(env, legacy_id)

    with pytest.raises(ValueError, match="集群 ID 非法"):
        env.registry.delete_cluster(legacy_id)

    assert (env.victim_dir / "keep.txt").is_file()
    assert legacy_id in env.registry._clusters


def test_api_delete_maps_out_of_base_id_to_400(env: Env) -> None:
    """DELETE 端点把归属拒绝映射为 400，越界目录原样保留。"""
    legacy_id = "../outside/victim"
    _inject_legacy_entry(env, legacy_id)

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(cluster_api.delete_cluster(legacy_id))

    assert excinfo.value.status_code == 400
    assert "集群 ID 非法" in str(excinfo.value.detail)
    assert (env.victim_dir / "keep.txt").is_file()


def test_normal_id_create_and_delete_roundtrip(env: Env) -> None:
    """正常 id 路径回归：仍落在 base_dir/<id>/，创建与删除行为不变。"""
    r = _create(env.client, "prod-2")
    assert r.status_code == 200, r.text
    body = r.json()
    target = env.base / "prod-2" / "cluster_config.yaml"
    assert Path(body["config_path"]).resolve() == target.resolve()
    assert target.is_file()
    assert "prod-2" in env.registry.registry_file.read_text(encoding="utf-8")

    r = env.client.delete("/api/clusters/prod-2")
    assert r.status_code == 200, r.text
    assert not (env.base / "prod-2").exists()
    assert env.base.is_dir()  # base_dir 本身不在删除面
    assert "prod-2" not in env.registry.registry_file.read_text(encoding="utf-8")
