"""plaita#212 回归：``async_gen_to_sync`` 的 worker 路径必须复制 ContextVar。

## 缺陷

``async_gen_to_sync`` 在已有 running loop 时把 async generator 交给 worker 线程
``_drive``；``threading.Thread(target=_drive)`` 不传播 contextvars。lazy/sync
路径（``drive_strategy(lazy=True, sync=True)`` → ``_drive_lazy_sync`` →
``async_gen_to_sync``）因此把租户/fence ContextVar 丢在 worker 外：
``current_tenant()`` 退回 ``default``，凭据路由读 default 文件。

#199 修的是 eager 桥 ``run_async_from_sync``；本文件覆盖 lazy worker 这条独立
路径（同一 ``async_utils`` 桥接面，不同函数）。

## 契约

running-loop 分支在**调用线程** ``copy_context()``，整个 ``_drive`` 在
``ctx.run`` 下执行；每个 ``run_until_complete`` task 继承调用方的租户 / fence。
无 loop 分支本就在当前线程，语义不变。``copy_context`` 是隔离副本：生成器内改
ContextVar 不回写调用方。

负控：还原裸 ``threading.Thread(target=_drive)``，本文件前四类断言必红；证据见
``docs/proofs/212-async-gen-bridge-context.md`` 与
``docs/proofs/data/212-negative-control.*``。
"""
from __future__ import annotations

import asyncio
import json
from contextvars import ContextVar
from pathlib import Path

import pytest
from cryptography.fernet import Fernet

from plaita.core.async_utils import async_gen_to_sync
from plaita.credentials import CredentialError, credentials_file, get_credential
from plaita.tenant_context import (
    current_tenant,
    reset_current_tenant,
    set_current_tenant,
)

_FENCE: ContextVar[str] = ContextVar("issue212_fence", default="no-fence")


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
    旁文件（shared=acme.example）。"""
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


def _run_lazy_probe(tenant: str = "tenant-b", fence: str = "fence-b") -> dict:
    """在调用线程的 ``asyncio.run(main())`` 里 set ContextVar，再走懒桥。"""
    box: dict = {}

    async def main():
        tenant_token = set_current_tenant(tenant)
        fence_token = _FENCE.set(fence)
        try:
            async def gen():
                default_only = "READ_OK"
                try:
                    get_credential("default-only")
                except CredentialError:
                    default_only = "CredentialError"
                yield {
                    "tenant": current_tenant(),
                    "fence": _FENCE.get(),
                    "file": credentials_file().name,
                    "shared": get_credential("shared")["url"],
                    "default_only": default_only,
                }

            box["items"] = list(async_gen_to_sync(gen()))
            box["caller_after"] = (current_tenant(), _FENCE.get())
        finally:
            _FENCE.reset(fence_token)
            reset_current_tenant(tenant_token)

    asyncio.run(main())
    return box


def test_lazy_bridge_propagates_tenant_and_fence_contextvars(cred_env):
    """核心性质：worker 路径必须看到调用线程的租户/fence。"""
    box = _run_lazy_probe()
    item = box["items"][0]

    assert item["tenant"] == "tenant-b", (
        "async_gen_to_sync worker 丢了租户 ContextVar（#212）；读到 default 说明"
        " _drive 未在 copy_context 下运行"
    )
    assert item["fence"] == "fence-b", "worker 路径丢了第二个 ContextVar（fence）"
    assert box["caller_after"] == ("tenant-b", "fence-b"), (
        "copy_context 是隔离副本，生成器不得改写调用方 ContextVar"
    )


def test_lazy_bridge_credentials_read_tenant_side_file(cred_env):
    """验收双租户探针：懒桥内 get_credential 读 tenant-b 旁文件。"""
    item = _run_lazy_probe("tenant-b", "fence-b")["items"][0]

    assert item["file"] == "creds.tenant-b.json"
    assert item["shared"] == "https://acme.example"
    assert item["default_only"] == "CredentialError", (
        "default 文件独有的凭据对 tenant-b 必须不可见（#24 验收条款）"
    )


def test_lazy_bridge_default_tenant_unchanged(cred_env):
    """default 租户行为不变：base 文件 + default-only 可读。"""
    item = _run_lazy_probe("default", "fence-default")["items"][0]

    assert item["tenant"] == "default"
    assert item["fence"] == "fence-default"
    assert item["file"] == "creds.json"
    assert item["shared"] == "https://default.example"
    assert item["default_only"] == "READ_OK"


def test_lazy_bridge_context_is_isolated_from_generator_mutations():
    """生成器内改 ContextVar 不回写调用方（副本语义，不是共享）。"""

    async def main():
        token = set_current_tenant("tenant-b")
        fence_token = _FENCE.set("fence-b")
        try:
            async def gen():
                set_current_tenant("tenant-x")
                _FENCE.set("fence-x")
                yield current_tenant(), _FENCE.get()

            assert list(async_gen_to_sync(gen())) == [("tenant-x", "fence-x")]
            assert current_tenant() == "tenant-b"
            assert _FENCE.get() == "fence-b"
        finally:
            _FENCE.reset(fence_token)
            reset_current_tenant(token)

    asyncio.run(main())


def test_no_loop_lazy_path_keeps_context():
    """无 running loop 时懒桥在当前线程跑，context 不变。"""
    token = set_current_tenant("tenant-no-loop")
    fence_token = _FENCE.set("fence-no-loop")
    try:
        async def gen():
            yield current_tenant(), _FENCE.get()

        assert list(async_gen_to_sync(gen())) == [
            ("tenant-no-loop", "fence-no-loop"),
        ]
    finally:
        _FENCE.reset(fence_token)
        reset_current_tenant(token)
