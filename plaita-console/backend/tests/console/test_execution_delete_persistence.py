"""执行删除持久性（duo-a-ledger 第四十三轮 A43-01 / duo-a2-ledger 第五轮）。

集群档：``DELETE /api/executions/{id}`` 必须**删除即吊销**——状态、索引、
取消标志、事件频道、fence 世代键、lease 租约键一并清掉。在途 fenced
worker 持旧世代写回时以 ``ExecutionLeaseError`` 具名拒绝，状态不得复活。

本地档：非终态删除默认 409；``force=true`` 先取消并等待线程收口，随后
持久删除执行行**与日志**；线程超时未退出时后续日志写入被丢弃（不得
「删了记录、日志还在长」）。

另钉住终态守卫（running/suspended 默认 409、显式 force 才删）与
``FencedExecutionStorage.delete_execution_state`` 的世代 CAS（A43-07）。
"""
import json
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

import pytest
from fastapi import Depends, FastAPI
from fakeredis import FakeRedis
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import executions as executions_api  # noqa: E402
from auth import require_auth  # noqa: E402
from plaita.server import flow_worker as fw  # noqa: E402
from services import flow_store  # noqa: E402
from services import local_executor as le  # noqa: E402
from services import engine_backend  # noqa: E402
from plaita.core.errors import ExecutionLeaseError  # noqa: E402
from plaita.storage.base import ExecutionState  # noqa: E402
from plaita.storage.fenced import (  # noqa: E402
    FencedExecutionStorage,
    reset_current_fence_token,
    set_current_fence_token,
)
from plaita.storage.redis import RedisExecutionStorage  # noqa: E402

ADMIN_KEY = "cluster-admin-key"

FLOW_DEF = json.dumps(
    {
        "nodes": [
            {"type": "start", "id": "start", "next": "end"},
            {"type": "end", "id": "end", "resultType": "success", "output": "ok"},
        ]
    }
)


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


@pytest.fixture()
def env(tmp_path, monkeypatch):
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    flow_store.init_engine(f"sqlite:///{tmp_path}/delete-persistence.db")
    return FakeRedis(decode_responses=True)


def _client(redis, *, local_mode: bool = False, auth: bool = True) -> TestClient:
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = local_mode
    app.state.store = flow_store.get_flow_store()
    dependencies = [Depends(require_auth)] if auth else []
    app.include_router(
        executions_api.router, prefix="/api", dependencies=dependencies
    )
    return TestClient(app)


def _headers(tenant: str = None):
    h = {"X-Admin-API-Key": ADMIN_KEY}
    if tenant:
        h["X-Tenant-ID"] = tenant
    return h


def _ns(tenant: str = None) -> str:
    return "plaita" if not tenant or tenant == "default" else f"plaita:{tenant}"


def _seed_execution(redis, execution_id: str, status: str = "running",
                    tenant: str = None) -> dict:
    ns = _ns(tenant)
    info = {
        "execution_id": execution_id,
        "flow_id": "demo-flow",
        "flow_version": "1",
        "status": status,
        "tenant_id": tenant or "default",
        "start_time": datetime.utcnow().isoformat(),
        "context": {},
    }
    redis.set(f"{ns}:execution:{execution_id}", json.dumps(info))
    redis.zadd(f"{ns}:execution:index", {execution_id: 1.0})
    # 在途 worker 的机制键：fence 世代 + resume 租约
    redis.set(f"{ns}:execution:fence:{execution_id}", "1")
    redis.set(f"{ns}:execution:lease:{execution_id}", "worker:1")
    return info


def _state(execution_id: str) -> ExecutionState:
    return ExecutionState(
        execution_id=execution_id,
        flow_id="demo-flow",
        flow_version="1",
        context={"$LAST_NODE": "start", "$NODE": {}},
        status="running",
    )


def _storage(redis, tenant: str = None) -> FencedExecutionStorage:
    return FencedExecutionStorage(
        RedisExecutionStorage(client=redis, namespace=_ns(tenant))
    )


# ---------------- 集群档：删除即吊销 ----------------

def test_running_delete_without_force_is_409_and_keeps_all_keys(env):
    redis = env
    _seed_execution(redis, "e-run", "running")
    client = _client(redis)

    r = client.delete("/api/executions/e-run", headers=_headers())

    assert r.status_code == 409
    # 该 mini-app 未挂统一 handler：源站 detail 仍是字符串（A2R24-01 只收敛
    # 异常文本出网点；这条是业务文案）。
    assert "force=true" in r.json()["detail"]
    # 状态与机制键原样保留，取消意图未被写入
    assert redis.exists("plaita:execution:e-run") == 1
    assert redis.get("plaita:execution:fence:e-run") == "1"
    assert redis.get("plaita:execution:lease:e-run") == "worker:1"
    assert redis.get("plaita:execution:cancel:e-run") is None


def test_force_delete_purges_state_index_fence_lease_cancel_events(env):
    redis = env
    _seed_execution(redis, "e-run", "running")
    redis.set("plaita:execution:cancel:e-run", "old", ex=60)
    redis.set("plaita:execution:events:e-run", "{}")
    client = _client(redis)

    r = client.delete("/api/executions/e-run?force=true", headers=_headers())

    assert r.status_code == 200, r.text
    # 删除即吊销：六个键全部清掉，索引成员同步移除
    assert redis.exists("plaita:execution:e-run") == 0
    assert redis.exists("plaita:execution:fence:e-run") == 0
    assert redis.exists("plaita:execution:lease:e-run") == 0
    assert redis.exists("plaita:execution:cancel:e-run") == 0
    assert redis.exists("plaita:execution:events:e-run") == 0
    assert redis.zscore("plaita:execution:index", "e-run") is None
    # 「先取消再删」：投递了 cancel 消息（worker 侧终态短路/毒丸 ack 均有据）
    msgs = [
        json.loads(fields["payload"])
        for _id, fields in redis.xrange("plaita:flow:queue")
    ]
    assert msgs and msgs[-1]["resume_type"] == "cancel"

    # 在途 worker 仍持旧世代 gen=1：下一次落盘必须被具名拒绝，状态不复活
    storage = _storage(redis)
    token = set_current_fence_token(1)
    try:
        with pytest.raises(ExecutionLeaseError):
            storage.save_execution_state("e-run", _state("e-run"))
    finally:
        reset_current_fence_token(token)
    assert redis.exists("plaita:execution:e-run") == 0
    assert redis.zscore("plaita:execution:index", "e-run") is None


def test_force_delete_tenant_namespace_revokes_tenant_fence_lease(env):
    redis = env
    _seed_execution(redis, "e-acme", "running", tenant="acme")
    redis.set("plaita:execution:fence:other", "9")
    client = _client(redis)

    r = client.delete(
        "/api/executions/e-acme?force=true", headers=_headers("acme")
    )

    assert r.status_code == 200, r.text
    assert redis.exists("plaita:acme:execution:e-acme") == 0
    assert redis.exists("plaita:acme:execution:fence:e-acme") == 0
    assert redis.exists("plaita:acme:execution:lease:e-acme") == 0
    assert redis.zscore("plaita:acme:execution:index", "e-acme") is None
    # 其他 namespace 的机制键不受影响
    assert redis.get("plaita:execution:fence:other") == "9"


def test_terminal_delete_purges_fence_and_lease(env):
    redis = env
    _seed_execution(redis, "e-done", "completed")
    client = _client(redis)

    r = client.delete("/api/executions/e-done", headers=_headers())

    assert r.status_code == 200, r.text
    assert redis.exists("plaita:execution:e-done") == 0
    assert redis.exists("plaita:execution:fence:e-done") == 0
    assert redis.exists("plaita:execution:lease:e-done") == 0


def test_fenced_delete_refuses_stale_generation(env):
    """A43-07：旧世代不得删除新世代已推进的状态（与 save 的 CAS 对称）。"""
    redis = env
    storage = _storage(redis)
    redis.set("plaita:execution:fence:e-stale", "2")
    redis.set("plaita:execution:lease:e-stale", "worker:2")
    redis.set("plaita:execution:e-stale", json.dumps({"status": "running"}))

    token = set_current_fence_token(1)
    try:
        with pytest.raises(ExecutionLeaseError):
            storage.delete_execution_state("e-stale")
    finally:
        reset_current_fence_token(token)
    assert redis.exists("plaita:execution:e-stale") == 1
    assert redis.get("plaita:execution:fence:e-stale") == "2"

    # 世代匹配的控制面删除（无 token）清掉全部
    assert storage.delete_execution_state("e-stale") is True
    assert redis.exists("plaita:execution:e-stale") == 0
    assert redis.exists("plaita:execution:fence:e-stale") == 0
    assert redis.exists("plaita:execution:lease:e-stale") == 0


# ---------------- 本地档：先停执行，再持久删除 ----------------

class _BlockingExecution:
    """首次 run_distributed 阻塞到 release，用于制造「运行中删除」窗口。"""

    started: threading.Event = threading.Event()
    release: threading.Event = threading.Event()

    def __init__(self, callback_handlers=None, event_bus=None):
        self.callback_handlers = callback_handlers or []
        self.mode = None

    def set_state(self, *args, **kwargs):
        pass

    def run_distributed(self, flow, params=None, saved_context=None,
                        resume_type=None, resume_data=None, **kwargs):
        _BlockingExecution.started.set()
        _BlockingExecution.release.wait(timeout=10)
        return {
            "is_end": True,
            "is_suspend": False,
            "context": {"$LAST_NODE": "end"},
            "result": "done",
        }


def _publish_flow(flow_id: str = "delete-demo") -> None:
    store = flow_store.get_flow_store()
    store.ensure_flow(flow_id, tenant_id="default")
    store.save_flow_definition(
        flow_id, "1.0.0", FLOW_DEF, status="draft", tenant_id="default"
    )
    store.publish_version(flow_id, "1.0.0", tenant_id="default")


def _start_blocking_execution(tmp_path, monkeypatch) -> str:
    flow_store.init_engine(f"sqlite:///{tmp_path}/local-delete.db")
    _publish_flow()
    _BlockingExecution.started = threading.Event()
    _BlockingExecution.release = threading.Event()
    # C1 阶段 3 起执行驱动经 engine backend，注入点在 flow_worker 命名空间。
    monkeypatch.setattr(fw, "FlowExecution", _BlockingExecution)
    info = engine_backend.start_execution(
        flow_store.get_flow_store(), "delete-demo", "1.0.0", {},
        tenant_id="default",
    )
    eid = info["execution_id"]
    assert _BlockingExecution.started.wait(timeout=10), "执行线程未进入运行态"
    return eid


def _wait_thread_exit(execution_id: str, timeout: float = 10.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        thread = le._threads.get(execution_id)
        if thread is None or not thread.is_alive():
            return True
        time.sleep(0.05)
    return False


def test_local_running_delete_without_force_is_409(tmp_path, monkeypatch):
    eid = _start_blocking_execution(tmp_path, monkeypatch)
    try:
        with pytest.raises(le.ExecutionActiveError) as excinfo:
            le.delete_local_execution(eid, tenant_id="default")
        assert "force=true" in str(excinfo.value)
        assert flow_store.get_local_execution(eid) is not None
    finally:
        _BlockingExecution.release.set()
        _wait_thread_exit(eid)


def test_local_force_delete_stops_log_growth_and_purges_logs(tmp_path, monkeypatch):
    eid = _start_blocking_execution(tmp_path, monkeypatch)
    # 强删等待窗口压到 0.2s：线程仍阻塞，走「墓碑 + 丢弃后续日志」分支
    monkeypatch.setattr(le, "LOCAL_DELETE_JOIN_TIMEOUT_SECONDS", 0.2)
    flow_store.insert_local_log(
        execution_id=eid, level="INFO", logger_name="t",
        message="before-delete", tenant_id="default",
    )
    # C1 阶段 3 起日志写入面 = engine backend 的 BackendLogHandler（同语义：
    # 线程过滤 + 强删墓碑抑制 + local_logs sink），旧 _ThreadLogHandler 已删。
    handler = engine_backend.BackendLogHandler(
        eid, threading.get_ident(), engine_backend._fs_log_sink, "default"
    )
    try:
        assert le.delete_local_execution(eid, tenant_id="default", force=True) is True

        # 记录与既有日志都不可查
        assert flow_store.get_local_execution(eid) is None
        assert flow_store.list_local_logs(execution_id=eid, tenant_id="default") == []

        # 线程仍在途：后续日志写入必须被丢弃（否则删除后日志还在长）
        thread = le._live_thread(eid)
        assert thread is not None and thread.is_alive()
        handler.emit(
            type(
                "Record",
                (),
                {
                    "thread": threading.get_ident(),
                    "levelname": "INFO",
                    "name": "t",
                    "getMessage": lambda self: "after-delete",
                },
            )()
        )
        assert flow_store.list_local_logs(execution_id=eid, tenant_id="default") == []
    finally:
        handler.close()
        _BlockingExecution.release.set()
        assert _wait_thread_exit(eid)
    # 线程退出后墓碑清除；线程收口不会把已删记录写回
    assert eid not in le._deleted_executions
    assert flow_store.get_local_execution(eid) is None


def test_local_delete_via_api_requires_force_then_purges_logs(tmp_path, monkeypatch):
    eid = _start_blocking_execution(tmp_path, monkeypatch)
    monkeypatch.setattr(le, "LOCAL_DELETE_JOIN_TIMEOUT_SECONDS", 0.2)
    flow_store.insert_local_log(
        execution_id=eid, level="INFO", logger_name="t",
        message="log-line", tenant_id="default",
    )
    client = _client(None, local_mode=True, auth=False)
    try:
        r = client.delete(f"/api/executions/{eid}")
        assert r.status_code == 409
        assert "force=true" in r.json()["detail"]["message"]

        r = client.delete(f"/api/executions/{eid}?force=true")
        assert r.status_code == 200, r.text
        assert r.json()["success"] is True
        assert client.get(f"/api/executions/{eid}").status_code == 404
        assert flow_store.list_local_logs(execution_id=eid, tenant_id="default") == []
    finally:
        _BlockingExecution.release.set()
        _wait_thread_exit(eid)


def test_local_terminal_delete_purges_logs(tmp_path):
    flow_store.init_engine(f"sqlite:///{tmp_path}/local-terminal.db")
    eid = "e-local-done"
    flow_store.insert_local_execution(
        execution_id=eid,
        flow_id="delete-demo",
        flow_version="1.0.0",
        status="completed",
        tenant_id="default",
    )
    flow_store.insert_local_log(
        execution_id=eid, level="INFO", logger_name="t",
        message="finished", tenant_id="default",
    )
    client = _client(None, local_mode=True, auth=False)

    r = client.delete(f"/api/executions/{eid}")

    assert r.status_code == 200, r.text
    assert flow_store.get_local_execution(eid) is None
    assert flow_store.list_local_logs(execution_id=eid, tenant_id="default") == []
