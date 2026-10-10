"""plaita#58 回归：租户 ContextVar 必须跨 runner 的执行线程边界。

## 缺陷

``plaita/core/runner.py:_run_sync_node`` 把同步节点丢进**不带调用方 context 的
新线程**——无超时走共享池 ``run_in_executor``，有超时走裸 daemon 线程。而
``contextvars`` 的取值**不跨线程继承**（新线程只有默认值），于是节点里
``current_tenant()`` 恒为 ``default``：

- 非 default 租户的凭据节点实际读 default 凭据文件——plaita#24 的验收条款
  「非 default 租户 flow 里的凭据节点取到的是自己租户的值；default 租户凭据
  对它不可见」在生产（console 集群档 worker）上不成立；
- 报错文案把 default 文件的凭据名报给别的租户。

## 契约

两条跨线程路径都必须在**调用方 context 的副本**里跑节点（``copy_context``）：
节点内 ``current_tenant()`` 与调度线程一致、``credentials_file()`` 指向本租户
旁文件、default 独有的凭据读取抛 ``CredentialError``。default 租户行为不变。

反证：去掉 ``_run_sync_node`` 里的 ``copy_context`` 包装，本文件前四个用例必红。
"""
import asyncio
import json
import threading
from pathlib import Path
from typing import ClassVar

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis
from cryptography.fernet import Fernet

from plaita.core.context import ExecutionContext
from plaita.core.runner import NodeRunner
from plaita.credentials import CredentialError, credentials_file, get_credential
from plaita.node import Node, get_default_registry
from plaita.server.flow_worker import RedisFlowWorker
from plaita.server.task_queue import enqueue_task
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage
from plaita.tenant_context import (
    current_tenant,
    reset_current_tenant,
    set_current_tenant,
)

# 节点线程内的观测只能由节点自己登记——跨线程后主线程看不到局部量。
_OBS: list = []
_PROBED = threading.Event()


class TenantProbeNode(Node):
    """读凭据并把「租户上下文 + 凭据文件 + 取值」记进 ``_OBS``。"""

    node_type: ClassVar[str] = "issue58_probe"
    node_name: ClassVar[str] = "issue58_probe"

    def execute(self, execution=None):
        entry = {
            "thread": threading.current_thread().name,
            "tenant": current_tenant(),
            "file": credentials_file().name,
            "shared": get_credential("shared")["url"],
        }
        try:
            get_credential("default-only")
            entry["default_only"] = "READ_OK"
        except CredentialError:
            entry["default_only"] = "CredentialError"
        _OBS.append(entry)
        _PROBED.set()
        return {"tenant": entry["tenant"]}


class _FakeFlow:
    flow_id = "f58"


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
    """default 基础文件（shared=default.example + default-only）与 ``acme``
    旁文件（shared=acme.example），命名同 console 按租户导出的旁文件规则。"""
    base = tmp_path / "creds.json"
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(base))
    key = Fernet.generate_key()
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY", key.decode())
    _write(base.with_name("creds.acme.json"), key, {
        "shared": {"url": "https://acme.example"},
    })
    _write(base, key, {
        "shared": {"url": "https://default.example"},
        "default-only": {"url": "https://default.example"},
    })
    _OBS.clear()
    _PROBED.clear()
    return base


async def _run_probe_node(timeout_ms):
    ctx = ExecutionContext()
    ctx.clean()
    runner = NodeRunner(ctx)
    node = TenantProbeNode(id="probe", name="probe")
    await runner.run_node(_FakeFlow(), node, max_timeout_ms=timeout_ms)


def _probe_in_tenant(tenant, timeout_ms):
    """在**已 set 租户的线程**内跑真实 runner（复刻 worker 消费线程形态）。"""
    box = {}

    def _drive():
        token = set_current_tenant(tenant)
        try:
            asyncio.run(_run_probe_node(timeout_ms))
            box["ok"] = True
        except BaseException as exc:  # noqa: BLE001 - 原样带回主线程断言
            box["error"] = exc
        finally:
            reset_current_tenant(token)

    thread = threading.Thread(target=_drive, name="plaita-consume-sim")
    thread.start()
    thread.join(timeout=30)
    assert not thread.is_alive(), "节点执行未在 30s 内结束"
    assert "error" not in box, box.get("error")
    assert len(_OBS) == 1, f"节点应恰好观测一次，实际 {_OBS}"
    return _OBS[0]


class TestSyncNodeKeepsCallerContext:
    """两条线程路径：无超时 = 共享池；有超时 = 裸 daemon 线程。"""

    @pytest.mark.parametrize("timeout_ms", [None, 5_000], ids=["no-timeout", "timeout"])
    def test_named_tenant_node_sees_own_tenant(self, cred_env, timeout_ms):
        entry = _probe_in_tenant("acme", timeout_ms)

        assert entry["tenant"] == "acme", "节点线程读到的租户上下文须与调度线程一致"
        assert entry["file"] == "creds.acme.json"
        assert entry["shared"] == "https://acme.example"
        assert entry["default_only"] == "CredentialError", (
            "default 文件的凭据对 acme 必须不可见（#24 验收条款）"
        )

    @pytest.mark.parametrize("timeout_ms", [None, 5_000], ids=["no-timeout", "timeout"])
    def test_default_tenant_node_unchanged(self, cred_env, timeout_ms):
        entry = _probe_in_tenant("default", timeout_ms)

        assert entry["tenant"] == "default"
        assert entry["file"] == "creds.json"
        assert entry["shared"] == "https://default.example"
        assert entry["default_only"] == "READ_OK"


FLOW_DEF = {
    "flow_id": "f58",
    "version": "1",
    "runtime": "python",
    "inputType": {"dataType": "object"},
    "nodes": [
        {"type": "start", "id": "start", "next": "probe"},
        {"type": "issue58_probe", "id": "probe", "next": "end"},
        {"type": "end", "id": "end", "output": {"done": True}},
    ],
}

QUEUE_NAME = "test:issue58-tenant-context"


@pytest.fixture()
def probe_node_registered():
    """用前登记探针节点——默认 registry 单例会被 ``init_default_registry()``
    清空重建（别的用例会调它），import 期注册靠不住。用完摘掉。"""
    get_default_registry().register(TenantProbeNode)
    yield
    get_default_registry().unregister(TenantProbeNode.node_type)


class TestWorkerConsumptionKeepsTenantContext:
    """端到端：真实 ``_consume_loop`` → ``_dispatch_task``（tenant_id=acme）。"""

    def test_cluster_shaped_consumption_reads_tenant_side_file(
        self, cred_env, probe_node_registered,
    ):
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        flow_storage = MemoryFlowStorage()
        flow_storage.save_flow(FLOW_DEF)
        worker = RedisFlowWorker(
            redis_url="redis://localhost:6379/15",
            queue_name=QUEUE_NAME,
            execution_storage=storage,
            flow_storage=flow_storage,
            redis_client=fake,
            enable_registry=False,
            enable_redis_logging=False,
        )
        queue = worker._get_task_queue()
        queue.ensure_group()
        enqueue_task(fake, QUEUE_NAME, {
            "type": "start",
            "flow_id": "f58",
            "version": "1",
            "params": {},
            "execution_id": "exec-58",
            "tenant_id": "acme",
        })

        worker._running = True
        consumer = threading.Thread(
            target=worker._consume_loop, args=(queue,), daemon=True,
        )
        consumer.start()
        try:
            assert _PROBED.wait(30), "节点未在 30s 内被执行"
        finally:
            worker._running = False
            consumer.join(timeout=10)

        assert len(_OBS) == 1, f"节点应恰好观测一次，实际 {_OBS}"
        entry = _OBS[0]
        assert entry["tenant"] == "acme"
        assert entry["file"] == "creds.acme.json"
        assert entry["shared"] == "https://acme.example"
        assert entry["default_only"] == "CredentialError"

        state = storage.load_execution_state("exec-58")
        assert state.status == "completed"
        assert state.tenant_id == "acme"
