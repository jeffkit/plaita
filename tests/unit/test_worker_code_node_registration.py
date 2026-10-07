"""回归：worker 启动注册 CodeNode —— 含 code 节点的流程须可解析（否则整单被丢弃）。"""
import os
import pytest

pytest.importorskip("cachetools")
pytest.importorskip("redis")


def test_register_code_node_for_worker_enables_code(monkeypatch):
    from plaita.server import flow_worker as fw
    from plaita.node import NodeRegistry

    reg = NodeRegistry()  # 干净注册表：无 code（对齐 0.4.0 默认）
    assert reg.get("code") is None

    monkeypatch.setenv("PLAITA_CODE_BACKEND", "subprocess")
    # 用干净注册表验证 helper 的核心行为（默认注册表已被其它用例污染）
    from plaita.node import register_code_node
    assert fw._code_node_enabled() is True
    assert fw._code_backend_for_worker() == "subprocess"
    register_code_node(registry=reg, default_backend="subprocess")
    assert reg.get("code") is not None
    # 端到端：含 code 节点的定义能解析成图
    node = reg.parse_node({"type": "code", "id": "c1", "code": "def run(inp):\n    return inp\n"})
    assert node is not None


def test_disable_code_node_switch(monkeypatch):
    from plaita.server import flow_worker as fw
    monkeypatch.setenv("PLAITA_DISABLE_CODE_NODE", "1")
    assert fw._code_node_enabled() is False


def test_code_backend_default_subprocess(monkeypatch):
    from plaita.server import flow_worker as fw
    monkeypatch.delenv("PLAITA_CODE_BACKEND", raising=False)
    assert fw._code_backend_for_worker() == "subprocess"


def test_register_code_node_falls_back_when_backend_unavailable(monkeypatch):
    """backend=docker 但 daemon 不可用 → 降级 subprocess，不抛。"""
    from plaita.server import flow_worker as fw
    monkeypatch.setenv("PLAITA_CODE_BACKEND", "docker")
    calls = []

    def fake_register(**kw):
        calls.append(kw["default_backend"])
        if kw["default_backend"] == "docker":
            raise RuntimeError("docker daemon not available")

    import plaita.node as pn
    monkeypatch.setattr(pn, "register_code_node", fake_register)
    fw._register_code_node_for_worker()
    assert calls == ["docker", "subprocess"]
