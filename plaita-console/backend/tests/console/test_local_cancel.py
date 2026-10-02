"""波次①本地档取消端到端单测（设计稿 §3.1 本地档 / 测试计划 T8）。

覆盖：
- 线程长跑中 ``cancel_local_execution``：置位取消 Event + 立即落 cancelled
  终态；执行线程在步界收口退出，cancelled **不被翻回** running/completed
  （每步状态落库改条件更新，覆写战争消失）；
- cancelled 收口补写取消点前 checkpoint context；
- 挂起执行取消保持直接终态；不存在的执行返回 False（BFF 404 语义）。
"""
import json
import sys
import threading
import time
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services import flow_store as fs  # noqa: E402
from services import local_executor as le  # noqa: E402

DEF = json.dumps(
    {
        "nodes": [
            {"type": "start", "id": "start", "next": "end"},
            {"type": "end", "id": "end", "resultType": "success", "output": "ok"},
        ]
    }
)


def _publish_flow():
    store = fs.get_flow_store()
    store.ensure_flow("cancel-demo", tenant_id="default")
    store.save_flow_definition(
        "cancel-demo", "1.0.0", DEF, status="draft", tenant_id="default"
    )
    store.publish_version("cancel-demo", "1.0.0", tenant_id="default")
    return store


class _StubExecution:
    """可编排的假 FlowExecution：首次 run_distributed 长跑阻塞，等待放行。"""

    release_first: threading.Event = threading.Event()
    first_started: threading.Event = threading.Event()

    def __init__(self, callback_handlers=None, event_bus=None):
        self.callback_handlers = callback_handlers or []
        self.mode = None
        self.calls = []

    def set_state(self, *args, **kwargs):
        pass

    def run_distributed(self, flow, params=None, saved_context=None,
                        resume_type=None, resume_data=None):
        self.calls.append(resume_type or "start")
        if len(self.calls) == 1:
            _StubExecution.first_started.set()
            _StubExecution.release_first.wait(timeout=10)
            return {
                "is_end": False,
                "is_suspend": False,
                "context": {"step": 1, "$LAST_NODE": "a"},
            }
        return {
            "is_end": True,
            "is_suspend": False,
            "context": {"step": 2, "$LAST_NODE": "end"},
            "result": "done",
        }


def _wait_thread_exit(execution_id: str, timeout: float = 10.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        thread = le._threads.get(execution_id)
        if thread is None or not thread.is_alive():
            return True
        time.sleep(0.05)
    return False


def test_t8_cancel_running_execution_thread_exits_at_boundary(tmp_path, monkeypatch):
    fs.init_engine(f"sqlite:///{tmp_path}/t8-running.db")
    _publish_flow()
    _StubExecution.release_first = threading.Event()
    _StubExecution.first_started = threading.Event()
    # patch 须覆盖执行线程整个生命周期（子线程在 spawn 后才构造 FlowExecution）
    monkeypatch.setattr(le, "FlowExecution", _StubExecution)

    info = le.start_local_execution(
        fs.get_flow_store(), "cancel-demo", "1.0.0", {}, tenant_id="default"
    )
    eid = info["execution_id"]
    try:
        assert _StubExecution.first_started.wait(timeout=10), "执行线程未进入长跑步"
        # 长跑中取消：控制面立即反馈 cancelled
        assert le.cancel_local_execution(eid, tenant_id="default") is True
        assert fs.get_local_execution(eid)["status"] == "cancelled"

        # 放行长跑步 → 执行线程应在步界收口，不再推进下一步
        _StubExecution.release_first.set()
        assert _wait_thread_exit(eid), "执行线程未在步界退出"

        row = fs.get_local_execution(eid)
        # 终态 cancelled 不被覆写（T8 核心断言）
        assert row["status"] == "cancelled"
        assert row["end_time"] is not None
        # 收口补写了取消点前 checkpoint context
        assert row["context"] == {"step": 1, "$LAST_NODE": "a"}
    finally:
        _StubExecution.release_first.set()
        le._pop_cancel_event(eid)
        with le._lock:
            le._threads.pop(eid, None)


def test_cancel_suspended_execution_finalizes_directly(tmp_path):
    fs.init_engine(f"sqlite:///{tmp_path}/t8-suspended.db")
    eid = "exec-susp"
    fs.insert_local_execution(
        execution_id=eid,
        flow_id="cancel-demo",
        flow_version="1.0.0",
        status="suspended",
        tenant_id="default",
    )
    assert le.cancel_local_execution(eid, tenant_id="default") is True
    assert fs.get_local_execution(eid)["status"] == "cancelled"


def test_cancel_unknown_execution_returns_false(tmp_path):
    fs.init_engine(f"sqlite:///{tmp_path}/t8-unknown.db")
    assert le.cancel_local_execution("no-such-exec", tenant_id="default") is False
