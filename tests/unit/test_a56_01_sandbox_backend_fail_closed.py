"""A56-01 回归：显式 docker 后端不可用时 worker 必须 fail-closed。

README/runbook 的契约是"Docker daemon 不可用时拒绝注册，不允许静默降级"。修复前
``_register_code_node_for_worker`` 捕获 ``RuntimeError`` 后无条件以 subprocess
重注册，并把显式 ``PLAITA_SANDBOX_ALLOWED_BACKENDS=docker`` 扩成
``{docker, subprocess}``——一次 daemon 抖动就把运维的安全意图（流程作者写
``sandbox_backend: subprocess`` 必须解析期被拒）改成 subprocess 可用。

本文件钉三层：
1. 负控：显式 docker + daemon 不可用 → 具名拒绝，不调用 subprocess 注册；
2. 降级只能经显式开关 + 显式白名单（``PLAITA_ALLOW_SUBPROCESS_FALLBACK=1`` 且
   ``subprocess`` 在 ``PLAITA_SANDBOX_ALLOWED_BACKENDS`` 中），且白名单不扩集；
3. 正控/默认语义：docker 可用时照常注册 docker；默认 subprocess 档位零变化。
"""
from __future__ import annotations

import logging

import pytest

pytest.importorskip("cachetools")
pytest.importorskip("redis")


@pytest.fixture()
def sandbox_state(monkeypatch):
    """隔离 env + 保存/恢复 code 模块的三个沙箱全局。"""
    import plaita.node.code as code_mod

    saved = (
        code_mod._DEFAULT_SANDBOX_BACKEND,
        code_mod._ALLOWED_SANDBOX_BACKENDS,
        code_mod._ALLOWED_LANGUAGES,
    )
    for name in ("PLAITA_CODE_BACKEND",
                 "PLAITA_SANDBOX_ALLOWED_BACKENDS",
                 "PLAITA_SANDBOX_ALLOWED_LANGUAGES",
                 "PLAITA_ALLOW_SUBPROCESS_FALLBACK"):
        monkeypatch.delenv(name, raising=False)
    # 让"未变更"断言有确定的起点。
    code_mod._DEFAULT_SANDBOX_BACKEND = "docker"
    code_mod._ALLOWED_SANDBOX_BACKENDS = None
    code_mod._ALLOWED_LANGUAGES = frozenset({"python"})
    yield code_mod
    (code_mod._DEFAULT_SANDBOX_BACKEND,
     code_mod._ALLOWED_SANDBOX_BACKENDS,
     code_mod._ALLOWED_LANGUAGES) = saved


def _refusal_records(caplog):
    return [r for r in caplog.records
            if getattr(r, "event", None) == "code_node.registration_refused"]


def test_docker_unavailable_explicit_docker_refuses_and_keeps_state(
        sandbox_state, monkeypatch, caplog):
    """负控（真实注册路径）：拒绝且不把默认后端/白名单改成 subprocess。"""
    from plaita.server import flow_worker as fw

    monkeypatch.setenv("PLAITA_CODE_BACKEND", "docker")
    monkeypatch.setenv("PLAITA_SANDBOX_ALLOWED_BACKENDS", "docker")
    monkeypatch.setattr(sandbox_state, "_docker_available", lambda: False)

    with caplog.at_level(logging.ERROR, logger="plaita"):
        with pytest.raises(fw.CodeNodeRegistrationError, match="不会自动降级"):
            fw._register_code_node_for_worker()

    assert sandbox_state._DEFAULT_SANDBOX_BACKEND == "docker"
    assert sandbox_state._ALLOWED_SANDBOX_BACKENDS is None
    assert sandbox_state._ALLOWED_LANGUAGES == frozenset({"python"})
    records = _refusal_records(caplog)
    assert records, [r.getMessage() for r in caplog.records]
    assert records[-1].code_backend == "docker"
    assert records[-1].allowed_backends == ["docker"]
    assert records[-1].subprocess_fallback is False


def test_negative_control_never_registers_subprocess(monkeypatch, caplog):
    """负控：注册调用序列只有 docker，没有修复前的 ``['docker','subprocess']``。"""
    import plaita.node as pn
    from plaita.server import flow_worker as fw

    monkeypatch.setenv("PLAITA_CODE_BACKEND", "docker")
    monkeypatch.setenv("PLAITA_SANDBOX_ALLOWED_BACKENDS", "docker")
    monkeypatch.delenv("PLAITA_ALLOW_SUBPROCESS_FALLBACK", raising=False)
    calls = []

    def fake_register(**kw):
        calls.append(kw)
        raise RuntimeError("docker daemon not available")

    monkeypatch.setattr(pn, "register_code_node", fake_register)
    with caplog.at_level(logging.ERROR, logger="plaita"):
        with pytest.raises(fw.CodeNodeRegistrationError, match="PLAITA_ALLOW_SUBPROCESS_FALLBACK"):
            fw._register_code_node_for_worker()

    assert [c["default_backend"] for c in calls] == ["docker"]
    assert calls[0]["allowed_backends"] == ["docker"]
    assert _refusal_records(caplog)


def test_fallback_switch_alone_is_not_enough(monkeypatch, caplog):
    """逃生开关打开但 subprocess 不在运营者白名单 → 仍拒绝（白名单不扩集）。"""
    import plaita.node as pn
    from plaita.server import flow_worker as fw

    monkeypatch.setenv("PLAITA_CODE_BACKEND", "docker")
    monkeypatch.setenv("PLAITA_SANDBOX_ALLOWED_BACKENDS", "docker")
    monkeypatch.setenv("PLAITA_ALLOW_SUBPROCESS_FALLBACK", "1")
    calls = []

    def fake_register(**kw):
        calls.append(kw)
        raise RuntimeError("docker daemon not available")

    monkeypatch.setattr(pn, "register_code_node", fake_register)
    with caplog.at_level(logging.ERROR, logger="plaita"):
        with pytest.raises(fw.CodeNodeRegistrationError, match="未包含 'subprocess'"):
            fw._register_code_node_for_worker()

    assert [c["default_backend"] for c in calls] == ["docker"]
    records = _refusal_records(caplog)
    assert records and records[-1].subprocess_fallback is True


def test_explicit_fallback_requires_both_switch_and_allowlist(monkeypatch, caplog):
    """开关 + 白名单都显式给出 → 允许降级，且沿用原白名单（不重新扩集）。"""
    import plaita.node as pn
    from plaita.server import flow_worker as fw

    monkeypatch.setenv("PLAITA_CODE_BACKEND", "docker")
    monkeypatch.setenv("PLAITA_SANDBOX_ALLOWED_BACKENDS", "docker,subprocess")
    monkeypatch.setenv("PLAITA_ALLOW_SUBPROCESS_FALLBACK", "1")
    calls = []

    def fake_register(**kw):
        calls.append(kw)
        if kw["default_backend"] == "docker":
            raise RuntimeError("docker daemon not available")

    monkeypatch.setattr(pn, "register_code_node", fake_register)
    with caplog.at_level(logging.WARNING, logger="plaita"):
        fw._register_code_node_for_worker()

    assert [c["default_backend"] for c in calls] == ["docker", "subprocess"]
    assert calls[0]["allowed_backends"] == ["docker", "subprocess"]
    assert calls[1]["allowed_backends"] == ["docker", "subprocess"]
    fallback = [r for r in caplog.records
                if getattr(r, "event", None) == "code_node.subprocess_fallback"]
    assert fallback and fallback[-1].levelno == logging.WARNING


def test_docker_available_positive_control(sandbox_state, monkeypatch):
    """正控：daemon 可用时注册 docker，白名单保持运维显式值。"""
    from plaita.server import flow_worker as fw

    monkeypatch.setenv("PLAITA_CODE_BACKEND", "docker")
    monkeypatch.setenv("PLAITA_SANDBOX_ALLOWED_BACKENDS", "docker")
    monkeypatch.setattr(sandbox_state, "_docker_available", lambda: True)

    fw._register_code_node_for_worker()

    assert sandbox_state._DEFAULT_SANDBOX_BACKEND == "docker"
    assert sandbox_state._ALLOWED_SANDBOX_BACKENDS == frozenset({"docker"})


def test_default_subprocess_semantics_unchanged(sandbox_state, monkeypatch):
    """默认档（未设 PLAITA_CODE_BACKEND）仍是 subprocess，白名单 docker ∪ subprocess。"""
    from plaita.server import flow_worker as fw

    monkeypatch.delenv("PLAITA_CODE_BACKEND", raising=False)
    fw._register_code_node_for_worker()

    assert sandbox_state._DEFAULT_SANDBOX_BACKEND == "subprocess"
    assert sandbox_state._ALLOWED_SANDBOX_BACKENDS == frozenset({"docker", "subprocess"})
    assert sandbox_state._ALLOWED_LANGUAGES == frozenset({"python"})
