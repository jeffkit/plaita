"""回归：writefile 写入 jail 的部署默认（plaita#39）。

writefile 节点（plaita-nodes）读 ``PLAITA_NODES_WORKSPACE_ROOT`` 作为写入根，
未设时任意路径可写（含绝对路径与 ``../`` 穿越）。部署入口此前从不设置它——
本组用例看护 worker/console 启动注入的 fail-closed 默认。
"""
import os
from pathlib import Path

import pytest

from plaita import writefile_jail as wj


@pytest.fixture(autouse=True)
def _clean_jail_env(monkeypatch):
    for name in (wj.ROOT_ENV, wj.OPT_OUT_ENV, wj._PROJECT_ROOT_ENV):
        monkeypatch.delenv(name, raising=False)
    yield


def test_explicit_root_respected(tmp_path, monkeypatch):
    root = str(tmp_path.resolve())
    monkeypatch.setenv(wj.ROOT_ENV, root)
    assert wj.apply_writefile_jail("test") == root
    assert os.environ[wj.ROOT_ENV] == root


def test_opt_out_leaves_env_unset(monkeypatch):
    monkeypatch.setenv(wj.OPT_OUT_ENV, "1")
    assert wj.apply_writefile_jail("test") is None
    assert wj.ROOT_ENV not in os.environ


def test_default_root_prefers_project_root(tmp_path, monkeypatch):
    root = str(tmp_path.resolve())
    monkeypatch.setenv(wj._PROJECT_ROOT_ENV, root)
    assert wj.apply_writefile_jail("test") == root
    assert os.environ[wj.ROOT_ENV] == root


def test_default_root_falls_back_to_cwd(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    root = str(Path.cwd())
    assert wj.apply_writefile_jail("test") == root
    assert os.environ[wj.ROOT_ENV] == root


def test_default_root_skips_filesystem_root(monkeypatch):
    """``/`` 不构成边界：跳过并退到家目录（此处家目录也置 /，故无根可用）。"""
    monkeypatch.setenv(wj._PROJECT_ROOT_ENV, os.sep)
    monkeypatch.chdir(os.sep)
    monkeypatch.setenv("HOME", os.sep)
    assert wj.apply_writefile_jail("test") is None
    assert wj.ROOT_ENV not in os.environ


def _make_engine_checkout(base):
    """造一个形如 ``<base>/<dir>/plaita/__init__.py`` 的引擎 checkout。"""
    checkout = base / "plaita"
    (checkout / "plaita").mkdir(parents=True)
    (checkout / "plaita" / "__init__.py").write_text("")
    return checkout


def test_default_root_climbs_out_of_engine_checkout(tmp_path, monkeypatch):
    """cwd 落在引擎自身 checkout 内 → 默认根上溯到部署根（plaita#51）。

    Mac worker 从自己的 plaita clone 启动，旧实现把该 clone 当 jail 根；目标仓
    （clone 的兄弟目录）的 run_dir 写不出去，land-failure.log 落盘必抛。
    """
    deployment = tmp_path / "deployment"
    checkout = _make_engine_checkout(deployment)
    monkeypatch.chdir(checkout)
    root = str(deployment.resolve())
    assert wj.apply_writefile_jail("test") == root
    assert os.environ[wj.ROOT_ENV] == root


def test_default_root_climbs_from_subdir_of_engine_checkout(tmp_path, monkeypatch):
    """cwd 在 checkout 的子目录里（console 从 ``plaita-console/backend`` 启动）同理。"""
    deployment = tmp_path / "deployment"
    checkout = _make_engine_checkout(deployment)
    nested = checkout / "plaita-console" / "backend"
    nested.mkdir(parents=True)
    monkeypatch.chdir(nested)
    assert wj.apply_writefile_jail("test") == str(deployment.resolve())


def test_cross_repo_run_dir_falls_inside_default_root(tmp_path, monkeypatch):
    """验收（plaita#51）：跨仓 run 的 run_dir（引擎 clone 的兄弟仓）在默认根内。

    同仓 run 不受影响：run_dir 在 clone 里，父目录自然是它的超集。
    """
    deployment = tmp_path / "deployment"
    checkout = _make_engine_checkout(deployment)
    monkeypatch.chdir(checkout)
    root = Path(wj.apply_writefile_jail("test"))
    for repo in ("agentproc", "plaita"):
        run_dir = (deployment / repo / ".flowcast" / "runs" / "pipeline-17").resolve()
        assert run_dir.is_relative_to(root)
        assert (run_dir / "land-failure.log").parent.is_relative_to(root)


def test_non_engine_cwd_root_unchanged(tmp_path, monkeypatch):
    """普通工作目录（含引擎 clone 的兄弟目录）不被上溯：默认根就是它自己。"""
    deployment = tmp_path / "deployment"
    _make_engine_checkout(deployment)
    sibling = deployment / "agentproc"
    sibling.mkdir()
    monkeypatch.chdir(sibling)
    assert wj.apply_writefile_jail("test") == str(sibling.resolve())


def test_worker_cli_applies_jail(monkeypatch):
    """worker 启动入口必须注入 jail（否则机制形同虚设）。"""
    import sys

    pytest.importorskip("cachetools")
    pytest.importorskip("redis")
    from plaita.server import flow_worker as fw

    calls = []
    monkeypatch.setattr(fw, "apply_writefile_jail", lambda component: calls.append(component))

    def _stop(**kwargs):
        raise RuntimeError("stop here")

    monkeypatch.setattr(fw, "RedisFlowWorker", _stop)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "flow_worker",
            "--execution-storage-type", "memory",
            "--flow-storage-type", "memory",
            "--no-event-bus",
        ],
    )
    with pytest.raises(SystemExit):
        fw.main()
    assert calls == ["flow-worker"]


def test_writefile_rejects_absolute_path_outside_jail(tmp_path, monkeypatch):
    """验收：未显式配置 jail 的进程注入默认根后，绝对路径写入被拒。"""
    write_file = pytest.importorskip("plaita_nodes.write_file")

    root = str(tmp_path.resolve())
    monkeypatch.setenv(wj._PROJECT_ROOT_ENV, root)
    assert wj.apply_writefile_jail("test") == root

    class _Exec:
        def evaluate(self, value):
            return value

    node = write_file.WriteFileNode(id="w1", path="/tmp/pwn", content="x")
    with pytest.raises(ValueError, match="escapes workspace_root"):
        node.execute(_Exec())

    inside = write_file.WriteFileNode(id="w2", path="artifact.txt", content="x")
    assert inside.execute(_Exec())["path"] == str(Path(root) / "artifact.txt")


def test_engine_checkout_cwd_allows_cross_repo_write(tmp_path, monkeypatch):
    """验收（plaita#51，真实节点）：Mac worker 布局下跨仓 run_dir 写得进去。

    进程 cwd = 引擎 clone，run_dir 在兄弟仓（目标仓）——修复前 WRITEFILE 报
    ``escapes workspace_root``；修复后落盘成功，越界写仍被拒。
    """
    write_file = pytest.importorskip("plaita_nodes.write_file")

    deployment = tmp_path / "deployment"
    checkout = _make_engine_checkout(deployment)
    monkeypatch.chdir(checkout)
    assert wj.apply_writefile_jail("test") == str(deployment.resolve())

    class _Exec:
        def evaluate(self, value):
            return value

    target = (
        deployment / "agentproc" / ".flowcast" / "runs" / "pipeline-17" / "land-failure.log"
    )
    node = write_file.WriteFileNode(id="w1", path=str(target), content="land failed")
    assert node.execute(_Exec())["path"] == str(target.resolve())
    assert target.read_text() == "land failed"

    outside = write_file.WriteFileNode(id="w2", path="/etc/cron.d/pwn", content="x")
    with pytest.raises(ValueError, match="escapes workspace_root"):
        outside.execute(_Exec())
