"""#335 / C1 阶段 3：executions API 手动 start/resume 切 engine backend 后删旧环。

旧环 = ``local_executor`` 的第二套执行器驱动（``start_local_execution`` /
``resume_local_execution`` / ``_run_flow`` 手搓 run_distributed 循环）。C1
阶段 2（r96）已切 ``local_scheduler.fire``；本阶段把 API 手动 start/resume
切到 ``engine_backend``（同一 ``FlowWorker`` 状态机 + ``resume_flow`` 驱动
链）后删除旧环。API 调用面零变化：``test_local_mode`` 的挂起-恢复三格原样
守恒即证。

本文件的格子：
- 宿主入口 ``engine_backend.resume_execution`` 的行契约（状态闸 / #33 拒绝
  / checkpoint 缺失 / 线程存活回滚）——与旧 ``resume_local_execution``
  逐项对齐（同异常类型、同报错文案、同 CAS 回滚语义）；
- 删环钉子：旧环符号自模块消失（防回潜）。
"""
import json
import logging
import sys
import threading
import time
import uuid
from pathlib import Path

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from plaita.server import flow_worker as fw  # noqa: E402

from services import flow_store as fs  # noqa: E402
from services import local_executor as le  # noqa: E402
from services import engine_backend  # noqa: E402

FLOW_ID = "r335-flow"
VERSION = "1.0.0"
DEF = json.dumps({
    "nodes": [
        {"type": "start", "id": "start", "next": "end"},
        {"type": "end", "id": "end", "resultType": "success", "output": "ok"},
    ]
})

TERMINAL = ("completed", "failed", "cancelled")


class StubExecution:
    """可编排假 FlowExecution（与 conformance 套件同形：本文件自含一份，
    避免 tests 包名在两布局下歧义）。"""

    behavior = "end"
    marker = ""
    call_log: list = []

    def __init__(self, callback_handlers=None, event_bus=None):
        self.callback_handlers = callback_handlers or []
        self.event_bus = event_bus
        self.mode = None
        self.calls = []

    def set_state(self, *args, **kwargs):
        pass

    def bind_execution(self, execution):
        pass

    def run_distributed(self, flow, **kwargs):
        cls = type(self)
        cls.call_log.append((cls.marker, kwargs.get("resume_type") or "start"))
        self.calls.append(kwargs.get("resume_type") or "start")
        if cls.behavior == "suspend":
            return {
                "is_end": False,
                "is_suspend": True,
                "context": {"step": 1, "$LAST_NODE": "a"},
            }
        return {
            "is_end": True,
            "is_suspend": False,
            "context": {"step": 2, "$LAST_NODE": "end"},
            "result": "done",
        }


def _wait_status(execution_id, statuses, timeout=10.0):
    deadline = time.time() + timeout
    status = None
    while time.time() < deadline:
        row = fs.get_local_execution(execution_id)
        status = None if row is None else row["status"]
        if status in statuses:
            return status
        time.sleep(0.05)
    return status


def _wait_thread_exit(execution_id, timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if le._live_thread(execution_id) is None:
            return True
        time.sleep(0.05)
    return False


@pytest.fixture()
def env(tmp_path, monkeypatch):
    """local 腿夹具：init_engine + 已发布定义 + StubExecution 注入。"""
    from config import Settings

    def _settings():
        s = Settings()
        s.console_env = "dev"
        s.admin_api_key = "r335-admin"
        s.allow_insecure_admin = False
        return s

    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    fs.init_engine(f"sqlite:///{tmp_path / 'issue335.db'}")
    store = fs.get_flow_store()
    store.ensure_flow(FLOW_ID, tenant_id="default")
    store.save_flow_definition(
        FLOW_ID, VERSION, DEF, status="draft", tenant_id="default"
    )
    store.publish_version(FLOW_ID, VERSION, tenant_id="default")

    StubExecution.behavior = "end"
    StubExecution.marker = uuid.uuid4().hex[:8]
    StubExecution.call_log = []
    monkeypatch.setattr(fw, "FlowExecution", StubExecution)
    monkeypatch.setattr(logging.getLogger("r335.probe"), "level", logging.INFO)
    monkeypatch.setattr(engine_backend.logger, "level", logging.INFO)

    yield
    with le._lock:
        le._threads.clear()
        le._cancel_events.clear()
        le._deleted_executions.clear()


def _seed_row(status: str, *, context: str = '{"step": 1}',
              nodes: str = "[]") -> str:
    eid = uuid.uuid4().hex[:16]
    fs.insert_local_execution(
        execution_id=eid, flow_id=FLOW_ID, flow_version=VERSION,
        status=status, tenant_id="default",
    )
    fs.update_local_execution(eid, context_json=context, nodes_json=nodes)
    return eid


# ---- 宿主入口契约（与旧 resume_local_execution 逐项对齐）----


def test_resume_unknown_execution_returns_false(env):
    assert engine_backend.resume_execution(
        fs.get_flow_store(), "no-such-exec", "event", {}
    ) is False


def test_resume_rejects_non_suspended(env):
    eid = _seed_row("running")
    with pytest.raises(ValueError) as ei:
        engine_backend.resume_execution(
            fs.get_flow_store(), eid, "event", {}
        )
    assert "仅挂起状态可恢复" in str(ei.value)


def test_resume_suspended_rejects_continue_and_retry(env):
    """#33 对齐 worker 契约：挂起执行只认 event/cancel/timeout 决议。"""
    for resume_type in ("continue", "retry"):
        eid = _seed_row("suspended")
        with pytest.raises(ValueError) as ei:
            engine_backend.resume_execution(
                fs.get_flow_store(), eid, resume_type, {}
            )
        assert resume_type in str(ei.value)
        assert fs.get_local_execution(eid)["status"] == "suspended"


def test_resume_missing_checkpoint_rejected(env):
    eid = _seed_row("suspended", context="null")
    with pytest.raises(ValueError) as ei:
        engine_backend.resume_execution(
            fs.get_flow_store(), eid, "event", {}
        )
    assert "checkpoint 缺失" in str(ei.value)
    assert fs.get_local_execution(eid)["status"] == "suspended"


def test_resume_thread_alive_rolls_back_status(env):
    """同 id 线程仍存活：拒绝 + 状态回滚 suspended（防双线程写同一行）。"""
    eid = _seed_row("suspended")
    blocker = threading.Thread(target=lambda: threading.Event().wait(30),
                               daemon=True)
    blocker.start()
    with le._lock:
        le._threads[eid] = blocker
    try:
        with pytest.raises(ValueError) as ei:
            engine_backend.resume_execution(
                fs.get_flow_store(), eid, "event", {}
            )
        assert "尚有线程在运行" in str(ei.value)
        assert fs.get_local_execution(eid)["status"] == "suspended"
    finally:
        with le._lock:
            le._threads.pop(eid, None)


def test_resume_happy_path_flips_running_then_completes(env):
    eid = _seed_row("suspended", context='{"step": 1, "$LAST_NODE": "start"}')
    assert engine_backend.resume_execution(
        fs.get_flow_store(), eid, "event", {"approved": True}
    ) is True
    # CAS 先行：入口返回即 running（API 面轮询立即可见）
    assert fs.get_local_execution(eid)["status"] == "running"
    StubExecution.behavior = "end"
    assert _wait_thread_exit(eid)
    assert _wait_status(eid, TERMINAL) == "completed"


# ---- 删环钉子：旧环符号自 local_executor 消失 ----


def test_old_loop_symbols_deleted(env):
    """C1 阶段 3 收口：第二套执行器驱动面已删（engine backend 是唯一执行
    语义）。符号回潜即红。"""
    for symbol in (
        "start_local_execution",
        "resume_local_execution",
        "_run_flow",
        "_load_definition",
        "_ThreadLogHandler",
    ):
        assert not hasattr(le, symbol), f"旧环符号未删: {symbol}"
