"""plaita#199 回归：``run_async_from_sync`` 的 running-loop 桥必须复制 ContextVar。

## 缺陷

``run_async_from_sync`` 在已有 running loop 时用
``ThreadPoolExecutor.submit(asyncio.run, coro)`` 把协程丢到新线程；``submit``
**不传播 contextvars**（新线程只有默认上下文）。``current_tenant()`` 因此退回
``default``，``credentials_file()`` 指向历史凭据文件——异步请求内跑 sync flow
时整条 flow 读 default 租户的凭据（#58 只修了节点线程，没覆盖这条整链桥）。

## 契约

running-loop 分支必须在**调用线程** ``copy_context()``，worker 内用
``ctx.run(asyncio.run, coro)`` 驱动；协程树继承调用方的租户 / fence ContextVar。
无 loop 分支本就在当前线程，行为不变。``copy_context`` 是隔离副本：协程内对
ContextVar 的修改不回写调用方。

负控：把实现还原成裸 ``pool.submit(asyncio.run, coro)``，本文件前四类断言必红；
证据见 ``docs/proofs/199-async-bridge-context.md`` 与
``docs/proofs/data/199-negative-control.*``。
"""
from __future__ import annotations

import asyncio
import json
from contextvars import ContextVar
from pathlib import Path

import pytest
from cryptography.fernet import Fernet

from plaita.core.async_utils import run_async_from_sync
from plaita.credentials import CredentialError, credentials_file, get_credential
from plaita.tenant_context import (
    current_tenant,
    reset_current_tenant,
    set_current_tenant,
)

#: 第二个 ContextVar（fence）：证明不是只对 tenant 做特判而是复制整个 context。
_FENCE: ContextVar[str] = ContextVar("issue199_fence", default="no-fence")


def _write(path: Path, key: bytes, entries: dict) -> None:
    store = {
        name: {
            "type": "generic",
            "data": Fernet(key).encrypt(json.dumps(data).encode()).decode(),
        }
        for name, data in entries.items()
    }
    path.write_text(json.dumps(store))


@pytest.fixture()
def cred_env(tmp_path, monkeypatch):
    """default 基础文件（shared=default.example + default-only）与 tenant-b
    旁文件（shared=acme.example），命名同 console 按租户导出的旁文件规则。"""
    base = tmp_path / "creds.json"
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(base))
    key = Fernet.generate_key()
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY", key.decode())
    _write(base.with_name("creds.tenant-b.json"), key, {
        "shared": {"url": "https://acme.example"},
    })
    _write(base, key, {
        "shared": {"url": "https://default.example"},
        "default-only": {"url": "https://default.example"},
    })
    return base


def _run_bridge_probe(tenant: str = "tenant-b", fence: str = "fence-b") -> dict:
    """在调用线程的 ``asyncio.run(main())`` 里 set ContextVar，再过桥读回。"""
    box: dict = {}

    async def main():
        tenant_token = set_current_tenant(tenant)
        fence_token = _FENCE.set(fence)
        try:
            async def probe():
                default_only = "READ_OK"
                try:
                    get_credential("default-only")
                except CredentialError:
                    default_only = "CredentialError"
                return {
                    "tenant": current_tenant(),
                    "fence": _FENCE.get(),
                    "file": credentials_file().name,
                    "shared": get_credential("shared")["url"],
                    "default_only": default_only,
                }

            box["bridged"] = run_async_from_sync(probe())
            # 桥后调用方上下文不受影响（copy_context 是隔离副本）。
            box["caller_after"] = (current_tenant(), _FENCE.get())
        finally:
            _FENCE.reset(fence_token)
            reset_current_tenant(tenant_token)

    asyncio.run(main())
    return box


def test_bridge_propagates_tenant_and_fence_contextvars(cred_env):
    """核心性质：调用线程 set 的租户/fence 必须在协程内可见。"""
    box = _run_bridge_probe()
    bridged = box["bridged"]

    assert bridged["tenant"] == "tenant-b", (
        "跨线程桥丢了租户 ContextVar（#199）；读到 default 说明 submit 未复制 context"
    )
    assert bridged["fence"] == "fence-b", "跨线程桥丢了第二个 ContextVar（fence）"
    assert box["caller_after"] == ("tenant-b", "fence-b"), (
        "copy_context 是隔离副本，桥后调用方 ContextVar 不得被协程改写"
    )


def test_bridge_credentials_read_tenant_side_file(cred_env):
    """验收双租户探针：桥内 get_credential 读 tenant-b 旁文件。"""
    bridged = _run_bridge_probe("tenant-b", "fence-b")["bridged"]

    assert bridged["file"] == "creds.tenant-b.json"
    assert bridged["shared"] == "https://acme.example"
    assert bridged["default_only"] == "CredentialError", (
        "default 文件独有的凭据对 tenant-b 必须不可见（#24 验收条款）"
    )


def test_bridge_default_tenant_unchanged(cred_env):
    """default 租户行为不变：base 文件 + default-only 可读。"""
    bridged = _run_bridge_probe("default", "fence-default")["bridged"]

    assert bridged["tenant"] == "default"
    assert bridged["fence"] == "fence-default"
    assert bridged["file"] == "creds.json"
    assert bridged["shared"] == "https://default.example"
    assert bridged["default_only"] == "READ_OK"


def test_bridge_context_is_isolated_from_coroutine_mutations(cred_env):
    """协程内改 ContextVar 不回写调用方（副本语义，不是共享）。"""

    async def main():
        token = set_current_tenant("tenant-b")
        fence_token = _FENCE.set("fence-b")
        try:
            async def mutate():
                set_current_tenant("tenant-x")
                _FENCE.set("fence-x")
                return current_tenant(), _FENCE.get()

            assert run_async_from_sync(mutate()) == ("tenant-x", "fence-x")
            assert current_tenant() == "tenant-b"
            assert _FENCE.get() == "fence-b"
        finally:
            _FENCE.reset(fence_token)
            reset_current_tenant(token)

    asyncio.run(main())


def test_no_loop_path_keeps_context():
    """无 running loop 时 asyncio.run 本就在当前线程，context 不变。"""
    token = set_current_tenant("tenant-no-loop")
    fence_token = _FENCE.set("fence-no-loop")
    try:
        async def read():
            return current_tenant(), _FENCE.get()

        assert run_async_from_sync(read()) == ("tenant-no-loop", "fence-no-loop")
    finally:
        _FENCE.reset(fence_token)
        reset_current_tenant(token)


def test_bridge_still_propagates_exceptions():
    """修 context 不改变异常传播契约。"""
    async def main():
        async def boom():
            raise ValueError("bridge boom")

        with pytest.raises(ValueError, match="bridge boom"):
            run_async_from_sync(boom())

    asyncio.run(main())
