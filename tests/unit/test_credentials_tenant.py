"""凭据租户路由单测（plaita#24）。

覆盖三件事：
1. ``plaita.credentials`` 按 ``current_tenant()`` 选文件——default 用
   ``PLAITA_CREDENTIALS_FILE``，其余租户用同目录旁文件；租户文件缺失时
   **不回落** default 文件（缺凭据报错，而不是串用别家密钥）。
2. 租户上下文能穿过节点执行线程（同步节点池 / 超时裸线程 / Parallel·Map
   分支池 / 进程池 / 惰性模式的驱动线程）到达凭据解析点——worker 里 set 的
   租户就是节点内看到的租户。
3. console 导出侧（``services/credentials_svc``）与引擎读取侧落到同一路径，
   并跑通「console 保存 → worker 按租户读回」的完整链路。
"""
from __future__ import annotations

import asyncio
import json
import sys
import threading
from pathlib import Path
from typing import ClassVar

import pytest

pytest.importorskip("cryptography")  # credentials extra

from cryptography.fernet import Fernet  # noqa: E402

from plaita.credentials import CredentialError, credentials_file, get_credential  # noqa: E402
from plaita.core.executor import ExecutionMode, FlowExecution  # noqa: E402
from plaita.core.flow import Flow  # noqa: E402
from plaita.node import Node, get_default_registry  # noqa: E402
from plaita.tenant_context import (  # noqa: E402
    current_tenant,
    reset_current_tenant,
    set_current_tenant,
)

ACME = "acme"
CONSOLE_BACKEND = Path(__file__).resolve().parents[2] / "plaita-console" / "backend"


def _encrypted(key: str, payload: dict) -> str:
    return Fernet(key.encode()).encrypt(json.dumps(payload).encode()).decode()


@pytest.fixture()
def cred_store(tmp_path, monkeypatch):
    """default 文件与 acme 旁文件各写一份同名凭据（值不同）+ 各一份独有凭据。"""
    key = Fernet.generate_key().decode()
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY", key)
    base = tmp_path / "creds.json"
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(base))
    base.write_text(json.dumps({
        "feishu-bot": {"type": "webhook", "data": _encrypted(key, {"url": "default-url"})},
        "default-only": {"type": "webhook", "data": _encrypted(key, {"url": "d"})},
    }))
    side = tmp_path / "creds.acme.json"
    side.write_text(json.dumps({
        "feishu-bot": {"type": "webhook", "data": _encrypted(key, {"url": "acme-url"})},
        "acme-only": {"type": "webhook", "data": _encrypted(key, {"url": "a"})},
    }))
    return {"base": base, "side": side}


class TestCredentialsFileRouting:
    def test_default_tenant_uses_env_file(self, cred_store):
        assert credentials_file() == cred_store["base"]

    def test_named_tenant_uses_side_file_matching_console_naming(self, cred_store):
        token = set_current_tenant(ACME)
        try:
            assert credentials_file() == cred_store["base"].with_name("creds.acme.json")
            assert credentials_file() == cred_store["side"]
        finally:
            reset_current_tenant(token)

    def test_explicit_tenant_arg_overrides_context(self, cred_store):
        token = set_current_tenant(ACME)
        try:
            assert credentials_file("default") == cred_store["base"]
        finally:
            reset_current_tenant(token)

    def test_empty_tenant_falls_back_to_env_file(self, cred_store):
        assert credentials_file("") == cred_store["base"]


class TestCredentialIsolation:
    def test_default_tenant_reads_default_file(self, cred_store):
        assert get_credential("feishu-bot")["url"] == "default-url"

    def test_named_tenant_reads_own_value_for_same_name(self, cred_store):
        token = set_current_tenant(ACME)
        try:
            assert get_credential("feishu-bot")["url"] == "acme-url"
        finally:
            reset_current_tenant(token)

    def test_default_only_credential_invisible_and_not_listed(self, cred_store):
        token = set_current_tenant(ACME)
        try:
            with pytest.raises(CredentialError) as exc:
                get_credential("default-only")
        finally:
            reset_current_tenant(token)
        message = str(exc.value)
        # 报错只列本租户可用凭据名（acme-only），不泄露 default 租户的清单
        assert "acme-only" in message
        assert message.count("default-only") == 1  # 仅出现在被请求的名字里

    def test_missing_tenant_file_does_not_fall_back_to_default(self, cred_store):
        cred_store["side"].unlink()
        token = set_current_tenant(ACME)
        try:
            with pytest.raises(CredentialError):
                get_credential("feishu-bot")
        finally:
            reset_current_tenant(token)


class _CredProbe(Node):
    """节点内解析凭据——与 plaita-nodes 连接器节点同一条路径。"""

    node_type: ClassVar[str] = "cred_probe_test"
    node_name: ClassVar[str] = "cred probe"

    def run(self, execution):
        return get_credential("feishu-bot")["url"]


class _TenantProbe(Node):
    node_type: ClassVar[str] = "tenant_probe_test"
    node_name: ClassVar[str] = "tenant probe"

    def run(self, execution):
        return current_tenant()


@pytest.fixture(autouse=True)
def probe_nodes():
    """探测节点注册进默认注册表：``childFlow`` 由 child.py 用默认注册表解析，
    局部注册表进不去子流程。用完摘掉，不污染其他测试。"""
    reg = get_default_registry()
    reg.register(_CredProbe)
    reg.register(_TenantProbe)
    yield
    reg.unregister(_CredProbe.node_type)
    reg.unregister(_TenantProbe.node_type)


def _simple_flow(node_type: str, *, timeout: str = "") -> Flow:
    node = {"type": node_type, "id": "p", "next": "e"}
    if timeout:
        node["timeout"] = timeout
    return Flow.from_string(json.dumps({
        "flow_id": "cred-tenant",
        "nodes": [
            {"type": "start", "id": "s", "next": "p"},
            node,
            {"type": "end", "id": "e", "output": "$NODE.p", "resultType": "success"},
        ],
    }))


def _map_flow(node_type: str) -> Flow:
    return Flow.from_string(json.dumps({
        "flow_id": "cred-tenant-map",
        "nodes": [
            {"type": "start", "id": "s", "next": "m"},
            {"type": "map", "id": "m", "collection": "$INPUT.items", "concurrent": True,
             "itemName": "item", "next": "e",
             "childFlow": {"flow_id": "c", "nodes": [
                 {"type": "start", "id": "cs", "next": "cp"},
                 {"type": node_type, "id": "cp", "next": "ce"},
                 {"type": "end", "id": "ce", "output": "$NODE.cp", "resultType": "success"}]}},
            {"type": "end", "id": "e", "output": "$NODE.m", "resultType": "success"},
        ],
    }))


def _parallel_flow(node_type: str, *, mode: str = "thread") -> Flow:
    return Flow.from_string(json.dumps({
        "flow_id": f"cred-tenant-parallel-{mode}",
        "nodes": [
            {"type": "start", "id": "s", "next": "p"},
            {"type": "parallel", "id": "p", "mode": mode, "join_branches": ["b1"],
             "branches": [{"name": "b1", "flow": {"flow_id": "c", "nodes": [
                 {"type": "start", "id": "cs", "next": "cp"},
                 {"type": node_type, "id": "cp", "next": "ce"},
                 {"type": "end", "id": "ce", "output": "$NODE.cp", "resultType": "success"}]}}],
             "next": "e"},
            {"type": "end", "id": "e", "output": "$NODE.p", "resultType": "success"},
        ],
    }))


def _run_in_tenant_thread(flow: Flow, tenant: str, **params):
    """模拟集群 worker 的租户注入：消费线程 set 租户后驱动一次分布式执行。"""
    out = {}

    def _worker():
        token = set_current_tenant(tenant)
        try:
            execution = FlowExecution()
            execution.mode = ExecutionMode.DISTRIBUTED
            step = execution.run_distributed(flow, params or None)
            out["result"] = step["result"]
            out["tenant"] = current_tenant()
        finally:
            reset_current_tenant(token)

    thread = threading.Thread(target=_worker)
    thread.start()
    thread.join(timeout=30)
    assert not thread.is_alive(), "worker 线程未在超时内结束"
    return out


class TestTenantContextReachesNodes:
    def test_sync_node_resolves_own_tenant_credential(self, cred_store):
        out = _run_in_tenant_thread(_simple_flow("cred_probe_test"), ACME)
        assert out["result"] == "acme-url"

    def test_sync_node_with_timeout_resolves_own_tenant(self, cred_store):
        out = _run_in_tenant_thread(_simple_flow("cred_probe_test", timeout="5000"), ACME)
        assert out["result"] == "acme-url"

    def test_sync_node_sees_tenant(self, cred_store):
        out = _run_in_tenant_thread(_simple_flow("tenant_probe_test"), ACME)
        assert out["result"] == ACME

    def test_map_branch_node_sees_tenant(self, cred_store):
        out = _run_in_tenant_thread(_map_flow("tenant_probe_test"), ACME, items=[1, 2])
        assert out["result"] == [ACME, ACME]

    def test_map_branch_node_resolves_own_tenant_credential(self, cred_store):
        out = _run_in_tenant_thread(_map_flow("cred_probe_test"), ACME, items=[1, 2])
        assert out["result"] == ["acme-url", "acme-url"]

    def test_parallel_branch_node_sees_tenant(self, cred_store):
        out = _run_in_tenant_thread(_parallel_flow("tenant_probe_test"), ACME)
        assert out["result"] == {"b1": ACME}

    def test_parallel_branch_node_resolves_own_tenant_credential(self, cred_store):
        out = _run_in_tenant_thread(_parallel_flow("cred_probe_test"), ACME)
        assert out["result"] == {"b1": "acme-url"}

    def test_parallel_process_branch_node_sees_tenant(self, cred_store):
        out = _run_in_tenant_thread(_parallel_flow("tenant_probe_test", mode="process"), ACME)
        assert out["result"] == {"b1": ACME}

    def test_default_tenant_thread_keeps_default_file(self, cred_store):
        out = _run_in_tenant_thread(_simple_flow("cred_probe_test"), "default")
        assert out["result"] == "default-url"


class TestTenantContextInLazyMode:
    """惰性（generator）模式：协程已在事件循环里时由驱动线程拉取，环境态同链。"""

    def test_async_gen_bridge_worker_thread_sees_tenant(self):
        from plaita.core.async_utils import async_gen_to_sync

        async def agen():
            yield current_tenant()

        async def drive():
            return list(async_gen_to_sync(agen()))

        token = set_current_tenant(ACME)
        try:
            assert asyncio.run(drive()) == [ACME]
        finally:
            reset_current_tenant(token)


class _RecordingPool:
    """记录提交内容的假池（不真起子进程）。"""

    def __init__(self):
        self.calls = []

    def submit(self, fn, *args, **kwargs):
        self.calls.append((fn, args))
        return None


class TestProcessPoolTenantTransfer:
    """Process(mode=process) 分支：ContextVar 不跨进程，环境态要显式带过去。"""

    def test_spawn_forwards_env_snapshot(self):
        from plaita.core.parallel_executor import ProcessParallelExecutor
        from plaita.env_context import run_with_env

        pool = _RecordingPool()
        token = set_current_tenant(ACME)
        try:
            ProcessParallelExecutor(pool=pool).submit(lambda: None)
        finally:
            reset_current_tenant(token)
        fn, args = pool.calls[0]
        assert fn is run_with_env
        # 快照里可能还有 console 本地档的凭据文件覆盖（取决于导入序），只钉租户。
        assert args[0]["plaita_tenant"] == ACME

    def test_child_entry_applies_snapshot_in_fresh_thread(self):
        from plaita.env_context import run_with_env

        out = {}

        def _child():
            out["tenant"] = run_with_env({"plaita_tenant": ACME}, current_tenant)

        thread = threading.Thread(target=_child)
        thread.start()
        thread.join(timeout=10)
        assert out["tenant"] == ACME


def _console_local_executor():
    """导入 console 本地档执行器（源码不在本工作区时跳过）。"""
    if not (CONSOLE_BACKEND / "services" / "local_executor.py").is_file():
        pytest.skip("plaita-console 源码不在本工作区")
    if str(CONSOLE_BACKEND) not in sys.path:
        sys.path.insert(0, str(CONSOLE_BACKEND))
    pytest.importorskip("sqlalchemy")
    from services import local_executor

    return local_executor


class TestConsoleLocalModeCredentialOverride:
    """console 本地档的凭据文件覆盖是另一条环境态——同样要进分支线程。

    本地档在同一进程内并发跑多租户流程，凭据文件路径不能改 env（跨租户串
    文件），靠 ``local_executor._tenant_credentials_file`` 按执行线程注入。节点
    执行会离开该线程，覆盖值必须随环境态快照一起走——否则分支内节点回落到
    default 租户的凭据文件（跨租户串用）。
    """

    def test_override_reaches_parallel_branch_node(self, cred_store):
        local_executor = _console_local_executor()
        out = {}

        def _worker():
            token = local_executor._tenant_credentials_file.set(str(cred_store["side"]))
            try:
                execution = FlowExecution()
                execution.mode = ExecutionMode.DISTRIBUTED
                out["result"] = execution.run_distributed(
                    _parallel_flow("cred_probe_test")
                )["result"]
            finally:
                local_executor._tenant_credentials_file.reset(token)

        thread = threading.Thread(target=_worker)
        thread.start()
        thread.join(timeout=30)
        assert not thread.is_alive(), "worker 线程未在超时内结束"
        assert out["result"] == {"b1": "acme-url"}


def _console_credentials_svc():
    """导入 console 凭据服务（源码不在本工作区时跳过）。"""
    if not (CONSOLE_BACKEND / "services" / "credentials_svc.py").is_file():
        pytest.skip("plaita-console 源码不在本工作区")
    if str(CONSOLE_BACKEND) not in sys.path:
        sys.path.insert(0, str(CONSOLE_BACKEND))
    pytest.importorskip("cryptography")
    pytest.importorskip("sqlalchemy")
    from services import credentials_svc

    return credentials_svc


@pytest.fixture()
def console_store(tmp_path, monkeypatch):
    """console 侧凭据库 + 密钥文件（与 worker 同一份 env 契约）。"""
    credentials_svc = _console_credentials_svc()
    db_file = tmp_path / "cred.db"
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{db_file}")
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(tmp_path / "creds.json"))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(tmp_path / "creds.key"))
    from services import flow_store

    flow_store.init_engine(f"sqlite:///{db_file}")
    return credentials_svc


class TestConsoleExportAgreement:
    """导出侧与读取侧的路径/命名必须一致，否则 worker 上租户凭据整片读空。"""

    def test_console_export_path_equals_engine_read_path(self, tmp_path, monkeypatch):
        credentials_svc = _console_credentials_svc()
        base = tmp_path / "creds.json"
        monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(base))
        assert credentials_svc.credentials_file("default") == base
        token = set_current_tenant(ACME)
        try:
            assert credentials_svc.credentials_file(ACME) == credentials_file()
            assert credentials_file() == base.with_name("creds.acme.json")
        finally:
            reset_current_tenant(token)

    def test_console_saves_then_engine_reads_own_tenant(self, console_store, tmp_path):
        credentials_svc = console_store
        credentials_svc.save_credential(
            "feishu-bot", "webhook", {"url": "https://acme.example/hook"}, tenant_id=ACME
        )
        credentials_svc.save_credential(
            "feishu-bot", "webhook", {"url": "https://default.example/hook"}
        )
        credentials_svc.save_credential(
            "default-only", "webhook", {"url": "https://default.example/only"}
        )
        assert credentials_svc.credentials_file(ACME).is_file()

        token = set_current_tenant(ACME)
        try:
            assert get_credential("feishu-bot")["url"] == "https://acme.example/hook"
            with pytest.raises(CredentialError):
                get_credential("default-only")
        finally:
            reset_current_tenant(token)
        assert get_credential("feishu-bot")["url"] == "https://default.example/hook"

    def test_tenant_without_credentials_does_not_fall_back(self, console_store):
        """租户从未建凭据（无旁文件）→ 报缺失，不读 default 租户凭据。"""
        credentials_svc = console_store
        credentials_svc.save_credential(
            "feishu-bot", "webhook", {"url": "https://default.example/hook"}
        )
        assert not credentials_svc.credentials_file(ACME).exists()

        token = set_current_tenant(ACME)
        try:
            with pytest.raises(CredentialError):
                get_credential("feishu-bot")
        finally:
            reset_current_tenant(token)
