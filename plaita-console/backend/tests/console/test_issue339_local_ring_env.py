"""plaita#339：console local 档执行面（C1 阶段 3 起统一 engine backend）接入
$ENV strip/重注（#326 wave 2 语义在本地档手动面的对齐）。

## 缺陷（#326×#323 缺口）

C1 阶段 2 只切了 local_scheduler 运行面到 engine backend（strip 经 worker
咽喉 ``_persist_state_or_raise`` 自动继承）；executions API 的手动
start/resume 曾走 ``local_executor`` 旧环——四处 context_json 直写活 context
（``$ENV`` 明文进 SQLite）、resume 直喂 checkpoint 无重注。**C1 阶段 3
（#335）删除旧环后，手动 start/resume 同经 engine backend**——strip 与
resume 重注（``resume_flow._reinject_env_for_resume``）在 worker 侧单点生效，
本套用例注入点随之迁 ``flow_worker.FlowExecution``，语义面不变。

## 负控（还原修复后必须精确红）

- 落库点改回裸 ``json.dumps(_safe(context))``（worker 咽喉去 strip）：[E] 由
  「行无 $ENV（值与键名）」变红（明文进 SQLite）；
- 删 resume 分支重注：[F] 由「saved_context 带当前进程 env 新值 / 真引擎
  续跑 completed」变红（KeyError 崩溃落 failed 或静默为空）。
"""
import json
import sys
import threading
import time
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from plaita.server import flow_worker as fw  # noqa: E402

from services import flow_store as fs  # noqa: E402
from services import local_executor as le  # noqa: E402
from services import engine_backend  # noqa: E402

SECRET = "S339_SECRET"

SIMPLE_DEF = json.dumps(
    {
        "nodes": [
            {"type": "start", "id": "start", "next": "end"},
            {"type": "end", "id": "end", "resultType": "success", "output": "ok"},
        ]
    }
)

ENV_DEF = json.dumps(
    {
        "runtime": "python",
        "expose_env": [SECRET],
        "nodes": [
            {"type": "start", "id": "start", "next": "wait"},
            {"type": "event", "id": "wait", "event_type": "approval", "next": "end"},
            {
                "type": "end",
                "id": "end",
                "resultType": "success",
                "output": {"seen": f"$ENV.{SECRET}"},
            },
        ],
    }
)


def _publish(flow_id: str, definition: str):
    store = fs.get_flow_store()
    store.ensure_flow(flow_id, tenant_id="default")
    store.save_flow_definition(
        flow_id, "1.0.0", definition, status="draft", tenant_id="default"
    )
    store.publish_version(flow_id, "1.0.0", tenant_id="default")
    return store


def _wait_thread_exit(execution_id: str, timeout: float = 10.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        thread = le._threads.get(execution_id)
        if thread is None or not thread.is_alive():
            return True
        time.sleep(0.05)
    return False


class _SuspendWithEnvStub:
    """挂起即返回带 $ENV 明文的活 context（旧环修前原样写 SQLite）。"""

    def __init__(self, callback_handlers=None, event_bus=None):
        self.callback_handlers = callback_handlers or []

    def run_distributed(self, flow, params=None, saved_context=None, **kw):
        return {
            "is_end": False,
            "is_suspend": True,
            "context": {
                "$LAST_NODE": "wait",
                "$INPUT": {"k": 1},
                "$ENV": {SECRET: "sk-live-value"},
                # #320 pending fail-closed：event 挂起的 $NODE 状态须带
                # event_type（worker 按 $NODE[$LAST_NODE] 构造 pending 引用）。
                "$NODE": {"wait": {"subscription_id": "sub-1",
                                   "event_type": "approval"}},
            },
        }


class _CompleteWithEnvStub:
    """一步到 completed（覆盖完成写点同咽喉）。"""

    def __init__(self, callback_handlers=None, event_bus=None):
        self.callback_handlers = callback_handlers or []

    def run_distributed(self, flow, params=None, saved_context=None, **kw):
        return {
            "is_end": True,
            "is_suspend": False,
            "context": {"$LAST_NODE": "end", "$ENV": {SECRET: "sk-live-value"}},
            "result": "done",
        }


class _ResumeCaptureStub:
    """resume 调用捕获 saved_context（重注断言面）；首拍挂起。"""

    saved: list = []

    def __init__(self, callback_handlers=None, event_bus=None):
        self.callback_handlers = callback_handlers or []

    def run_distributed(self, flow, params=None, saved_context=None, **kw):
        if saved_context is None:
            return {
                "is_end": False,
                "is_suspend": True,
                "context": {"$LAST_NODE": "wait", "$INPUT": {}},
            }
        _ResumeCaptureStub.saved.append(saved_context)
        return {
            "is_end": True,
            "is_suspend": False,
            "context": dict(saved_context),
            "result": "done",
        }


def test_suspend_row_context_stripped(tmp_path, monkeypatch):
    """[E] 挂起写点：旧环落库行无 $ENV（值与键名）；引擎活 context 不动
    语义由 helper 的 copy-on-write 保证（序列化面剥离）。"""
    fs.init_engine(f"sqlite:///{tmp_path}/s339-suspend.db")
    # worker 挂起面按真实 flow 节点合成 pending（event_type 必填）——stub 的
    # $LAST_NODE=wait 须对应真实 event 节点（旧环无此校验，测试形制对齐）。
    _publish("s339-suspend", ENV_DEF)
    monkeypatch.setattr(fw, "FlowExecution", _SuspendWithEnvStub)

    info = engine_backend.start_execution(
        fs.get_flow_store(), "s339-suspend", "1.0.0", {}, tenant_id="default"
    )
    eid = info["execution_id"]
    assert _wait_thread_exit(eid)

    row = fs.get_local_execution(eid)
    assert row["status"] == "suspended"
    assert "$ENV" not in (row["context"] or {}), "[E] 挂起行不落 $ENV 键"
    assert "sk-live-value" not in json.dumps(row["context"]), "[E] 值不落盘"
    assert row["context"]["$LAST_NODE"] == "wait", "其余 checkpoint 原样"


def test_completed_row_context_stripped(tmp_path, monkeypatch):
    """[E] 完成写点：同一咽喉语义覆盖终态行。"""
    fs.init_engine(f"sqlite:///{tmp_path}/s339-completed.db")
    _publish("s339-simple", SIMPLE_DEF)
    monkeypatch.setattr(fw, "FlowExecution", _CompleteWithEnvStub)

    info = engine_backend.start_execution(
        fs.get_flow_store(), "s339-simple", "1.0.0", {}, tenant_id="default"
    )
    eid = info["execution_id"]
    assert _wait_thread_exit(eid)

    row = fs.get_local_execution(eid)
    assert row["status"] == "completed"
    assert "$ENV" not in (row["context"] or {}), "[E] 完成行不落 $ENV"
    assert "sk-live-value" not in json.dumps(row["context"])


def test_cancelled_finalize_context_stripped(tmp_path, monkeypatch):
    """[E] 取消收口：``_finalize_cancelled`` 补写的 checkpoint 同样无 $ENV
    （test_local_cancel T8 形制 + env 面）。"""
    fs.init_engine(f"sqlite:///{tmp_path}/s339-cancel.db")
    _publish("s339-simple", SIMPLE_DEF)

    class _BlockingThenEnvStub(_SuspendWithEnvStub):
        first_started: threading.Event = threading.Event()
        release: threading.Event = threading.Event()

        def run_distributed(self, flow, params=None, saved_context=None, **kw):
            if saved_context is None and not self.first_started.is_set():
                self.first_started.set()
                self.release.wait(timeout=10)
            return {
                "is_end": False,
                "is_suspend": False,
                "context": {
                    "$LAST_NODE": "a",
                    "$ENV": {SECRET: "sk-live-value"},
                },
            }

    _BlockingThenEnvStub.first_started = threading.Event()
    _BlockingThenEnvStub.release = threading.Event()
    monkeypatch.setattr(fw, "FlowExecution", _BlockingThenEnvStub)

    info = engine_backend.start_execution(
        fs.get_flow_store(), "s339-simple", "1.0.0", {}, tenant_id="default"
    )
    eid = info["execution_id"]
    try:
        assert _BlockingThenEnvStub.first_started.wait(timeout=10)
        assert le.cancel_local_execution(eid, tenant_id="default") is True
        _BlockingThenEnvStub.release.set()
        assert _wait_thread_exit(eid)
        row = fs.get_local_execution(eid)
        assert row["status"] == "cancelled"
        assert "$ENV" not in (row["context"] or {}), "[E] 取消收口行不落 $ENV"
        assert "sk-live-value" not in json.dumps(row["context"])
        assert row["context"]["$LAST_NODE"] == "a"
    finally:
        _BlockingThenEnvStub.release.set()
        le._pop_cancel_event(eid)
        with le._lock:
            le._threads.pop(eid, None)


def test_resume_reinjects_current_env_into_saved_context(tmp_path, monkeypatch):
    """[F] resume 写面：engine backend 已 strip 的挂起行经旧环恢复——
    saved_context 按当前进程 env 重注（不静默为空、不吃旧值）。"""
    monkeypatch.setenv(SECRET, "rotated-current-value")
    fs.init_engine(f"sqlite:///{tmp_path}/s339-resume.db")
    _publish("s339-env", ENV_DEF)
    _ResumeCaptureStub.saved = []
    monkeypatch.setattr(fw, "FlowExecution", _ResumeCaptureStub)

    eid = "exec-s339-resume"
    fs.insert_local_execution(
        execution_id=eid,
        flow_id="s339-env",
        flow_version="1.0.0",
        status="suspended",
        tenant_id="default",
    )
    fs.update_local_execution(
        eid, context_json=json.dumps({"$LAST_NODE": "wait", "$INPUT": {}})
    )
    assert engine_backend.resume_execution(
        fs.get_flow_store(), eid, "event", {"approved": True}, tenant_id="default"
    )
    assert _wait_thread_exit(eid)

    assert _ResumeCaptureStub.saved, "resume 拍未发生"
    saved = _ResumeCaptureStub.saved[0]
    assert saved["$ENV"] == {SECRET: "rotated-current-value"}, (
        "[F] saved_context 重注=当前进程 env"
    )
    row = fs.get_local_execution(eid)
    assert row["status"] == "completed"


def test_real_engine_resume_stripped_checkpoint_completes(tmp_path, monkeypatch):
    """[F] 真引擎格（issue 实证形状）：engine backend 落盘已 strip 的挂起行
    → 旧环手动 event resume 续跑求值 $ENV.X = 当前进程值，终态 completed。
    修前：下一个节点求值 KeyError 崩溃落 failed（retry 重投同样崩溃）。"""
    monkeypatch.setenv(SECRET, "current-worker-value")
    fs.init_engine(f"sqlite:///{tmp_path}/s339-real.db")
    _publish("s339-real", ENV_DEF)

    info = engine_backend.start_execution(
        fs.get_flow_store(), "s339-real", "1.0.0", {}, tenant_id="default"
    )
    eid = info["execution_id"]
    assert _wait_thread_exit(eid)

    row = fs.get_local_execution(eid)
    assert row["status"] == "suspended", f"真引擎应挂起在 event 节点: {row}"
    assert "$ENV" not in (row["context"] or {}), (
        "[E] 真挂起行不落 $ENV（混合部署 checkpoint 形状）"
    )

    assert engine_backend.resume_execution(
        fs.get_flow_store(), eid, "event", {"approved": True}, tenant_id="default"
    )
    assert _wait_thread_exit(eid)

    row2 = fs.get_local_execution(eid)
    assert row2["status"] == "completed", (
        f"[F] 修后续跑应完成（修前 KeyError 落 failed）: {row2.get('error')}"
    )
    output = row2.get("output") or {}
    assert output.get("seen") == "current-worker-value", (
        f"[F] 终点求值=当前进程 env（不静默为空/不吃旧值）: {output!r}"
    )
