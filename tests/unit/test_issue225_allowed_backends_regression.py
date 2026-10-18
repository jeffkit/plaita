"""plaita#225 回归：``register_code_node(allowed_backends=...)`` 的解析期硬闸。

运营者的 ``allowed_backends`` 是沙箱安全边界：流程 JSON 逐节点声明的
``sandbox_backend`` 必须在 ``CodeNode.validate_code_node`` 解析期被拒，否则
流程作者可把运营者选定的后端覆盖成 ``subprocess``（无 FS/网络隔离）或
``unsafe``（raw exec）。该检查只在 ``plaita/node/code.py`` 的
``if allowed is not None and ... not in allowed`` 一行；执行路径不再复查。

本文件钉**直接 API 面**（此前未被单独覆盖）：``register_code_node(
allowed_backends=("restricted",))`` 后，直接构造 ``CodeNode`` 的
``subprocess`` / ``unsafe`` 必须 ``ValidationError``；默认后端落在白名单外
同样被拒；白名单内的 ``restricted`` 与「未设置白名单」的历史行为不变。

注：既有 ``tests/unit/test_issue22_sandbox_allowlist.py`` 从 worker / CLI 入口
间接覆盖了 ``unsafe``（负控下也会红）；但 ``register_code_node`` 直接 API +
``subprocess`` 档此前无断言，本文件补的是这个面。

负控：把 ``code.py`` 的白名单条件改成 ``if False:`` ⇒ 本文件所有
"must be rejected" 用例必红；``restricted``/无白名单正控与既有
``test_code*.py`` 保持绿。证据见 ``docs/proofs/225-allowed-backends.md`` 与
``docs/proofs/data/225-negative-control.txt``。
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from plaita.node import NodeRegistry, register_code_node
from plaita.node.code import CodeNode

_BAD = "def run(a):\n    return a"


@pytest.fixture()
def code_globals(monkeypatch):
    """快照/恢复沙箱模块级全局，避免白名单跨用例污染（#225 卡片的硬要求）。"""
    import plaita.node.code as code_mod

    saved = (
        code_mod._ALLOWED_SANDBOX_BACKENDS,
        code_mod._DEFAULT_SANDBOX_BACKEND,
        code_mod._ALLOWED_LANGUAGES,
    )
    monkeypatch.delenv("PLAITA_SANDBOX_ALLOWED_BACKENDS", raising=False)
    monkeypatch.delenv("PLAITA_CODE_BACKEND", raising=False)
    try:
        yield code_mod
    finally:
        (
            code_mod._ALLOWED_SANDBOX_BACKENDS,
            code_mod._DEFAULT_SANDBOX_BACKEND,
            code_mod._ALLOWED_LANGUAGES,
        ) = saved


def _register_restricted_only(default_backend: str = "restricted") -> None:
    # 独立 registry：不污染进程级默认 registry，只测模块级安全全局。
    register_code_node(
        registry=NodeRegistry(),
        default_backend=default_backend,
        allowed_backends=("restricted",),
    )


@pytest.mark.parametrize("backend", ["subprocess", "unsafe"])
def test_operator_allowlist_rejects_flow_declared_backend(code_globals, backend):
    _register_restricted_only()
    with pytest.raises(ValidationError, match="not allowed by the operator") as exc:
        CodeNode(id="c", code=_BAD, language="python", sandbox_backend=backend)
    # 精确到安全闸的文案：点名被拒后端 + 运营者白名单 + 设置入口
    message = str(exc.value)
    assert repr(backend) in message
    assert "restricted" in message
    assert "register_code_node(allowed_backends=" in message


def test_default_backend_outside_allowlist_is_rejected(code_globals):
    """未显式声明 sandbox_backend 时，默认后端同样要过运营者白名单。"""
    _register_restricted_only(default_backend="subprocess")
    with pytest.raises(ValidationError, match="not allowed by the operator"):
        CodeNode(id="c", code=_BAD, language="python")


def test_allowed_backend_still_constructs(code_globals):
    """正控：白名单内的 restricted 显式/默认两种写法都可用。"""
    _register_restricted_only()
    explicit = CodeNode(id="c1", code=_BAD, language="python",
                        sandbox_backend="restricted")
    implicit = CodeNode(id="c2", code=_BAD, language="python")
    assert explicit.sandbox_backend == "restricted"
    assert implicit.sandbox_backend == "restricted"


def test_no_allowlist_keeps_historical_behaviour(code_globals):
    """正控：未设置 allowed_backends 时不做运营者拦截（历史行为不变）。"""
    code_globals._ALLOWED_SANDBOX_BACKENDS = None
    node = CodeNode(id="c", code=_BAD, language="python",
                    sandbox_backend="unsafe")
    assert node.sandbox_backend == "unsafe"
