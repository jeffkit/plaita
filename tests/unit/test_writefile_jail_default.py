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
