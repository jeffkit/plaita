"""C1 阶段 2：engine backend conformance 双后端套件（plaita#323 / ADR-001 §1）。

**同一组行为用例跑两遍**：``memory``（MemoryExecutionStorage + DictFlowStorage，
纯进程内参照形态）与 ``local``（LocalExecutionStorage 适配 local_executions 行
+ ConsoleFlowStorage，local 降级形态本体）各跑一遍——这是「local 降级为
engine backend」的等价性证明基建（降级不是第二种执行语义）。

四域（与 r94 勘明的 test_local_* 钉死进程内语义逐一对应）：

- **取消 Event**：控制面置位 Event + 即时落 cancelled，worker 在步界消费，
  终态不被翻回（T8 契约；取消点 checkpoint context 补写）；
- **墓碑**：强删窗口内 preminted start 被拒、强删后日志写入被抑制、
  行/状态随删清空；
- **日志处理器**：线程级过滤（只捕获驱动线程记录）、随执行删除清理；
- **租约面**：NullExecutionLease（内存/单机语义）下同一状态机全链可跑。

注入点：``plaita.server.flow_worker.FlowExecution``（engine 驱动的执行器
构造点，与 test_local_cancel patch ``local_executor.FlowExecution`` 同一
手法，层级不同）。

已声明的分叉（不属四域，见 services/engine_backend.py 模块 docstring）：
worker 协议字段（flow_hash/generation/pending/next_step/timeline）本地行
无列不落；本地档 resume 走既有 API 旧环（阶段 3 收口）。
"""
import json
import logging
import sys
import threading
import time
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from plaita.server import flow_worker as fw  # noqa: E402
from plaita.server.execution_lease import NullExecutionLease  # noqa: E402
from plaita.storage.base import ExecutionState  # noqa: E402

from services import flow_store as fs  # noqa: E402
from services import local_executor as le  # noqa: E402
from services import engine_backend  # noqa: E402

FLOW_ID = "conform-hello"
VERSION = "1.0.0"
GOOD_DEF = json.dumps({
    "nodes": [
        {"type": "start", "id": "start", "next": "wait"},
        # 事件节点：resume 格的 $LAST_NODE 指向它（worker 的 #258 stale 载体
        # 复核要求挂起形状真实——$NODE status=pending 的 suspending 节点）。
        {"type": "event", "id": "wait", "event_type": "approval", "next": "end"},
        {"type": "end", "id": "end", "resultType": "success", "output": "ok"},
    ]
})

# 合法挂起 checkpoint 形状（event/timeout 决议有处可落）。
PENDING_CONTEXT = {"step": 1, "$LAST_NODE": "wait",
                   "$NODE": {"wait": {"status": "pending"}}}

TERMINAL = ("completed", "failed", "cancelled")


class StubExecution:
    """可编排假 FlowExecution：behavior 决定首拍形状（end/block/suspend）。"""

    behavior = "end"
    first_started: threading.Event = threading.Event()
    release_first: threading.Event = threading.Event()
    marker = ""
    # 类级调用台账（引擎是否被进入过；墓碑窗口内的日志取证不可用——
    # 同 eid 日志本就被抑制，故用调用台账直接取证）
    call_log: list = []

    def __init__(self, callback_handlers=None, event_bus=None):
        self.callback_handlers = callback_handlers or []
        self.event_bus = event_bus
        self.mode = None
        self.calls = []

    def set_state(self, *args, **kwargs):
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
        if cls.behavior == "block" and len(self.calls) == 1:
            cls.first_started.set()
            cls.release_first.wait(timeout=10)
            logging.getLogger("conformance.probe").info("in-flight %s", cls.marker)
            return {
                "is_end": False,
                "is_suspend": False,
                "context": {"step": 1, "$LAST_NODE": "a"},
            }
        logging.getLogger("conformance.probe").info("boundary %s", cls.marker)
        return {
            "is_end": True,
            "is_suspend": False,
            "context": {"step": 2, "$LAST_NODE": "end"},
            "result": "done",
        }


def _wait_status(backend, execution_id, statuses, timeout=10.0):
    deadline = time.time() + timeout
    status = None
    while time.time() < deadline:
        status = backend.read_status(execution_id)
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


@pytest.fixture(params=["local", "memory"])
def env(request, tmp_path, monkeypatch):
    """双后端夹具：同一组用例在 local / memory 两腿各跑一遍。

    ``env.start`` 是腿抽象的起跑入口（local 腿先按 start_execution 同形建档
    再 launch；memory 腿直接 launch）——用例体两腿共用。
    """
    StubExecution.behavior = "end"
    StubExecution.first_started = threading.Event()
    StubExecution.release_first = threading.Event()
    StubExecution.marker = uuid.uuid4().hex[:8]
    StubExecution.call_log = []
    monkeypatch.setattr(fw, "FlowExecution", StubExecution)
    # pytest 下 root logger 缺省 WARNING——探针 logger 与 engine backend
    # 模块 logger 都钉 INFO，线程过滤与「收口 status=deleted」的断言才有
    # 确定性输入（生产本地档 console 启动配置即 INFO）。
    monkeypatch.setattr(logging.getLogger("conformance.probe"), "level", logging.INFO)
    monkeypatch.setattr(engine_backend.logger, "level", logging.INFO)

    if request.param == "local":
        fs.init_engine(f"sqlite:///{tmp_path / 'conform.db'}")
        store = fs.get_flow_store()
        store.ensure_flow(FLOW_ID, tenant_id="default")
        store.save_flow_definition(
            FLOW_ID, VERSION, GOOD_DEF, status="draft", tenant_id="default"
        )
        store.publish_version(FLOW_ID, VERSION, tenant_id="default")
        backend = engine_backend.local_backend(store)

        def start(params=None):
            execution_id = uuid.uuid4().hex[:16]
            fs.insert_local_execution(
                execution_id=execution_id,
                flow_id=FLOW_ID,
                flow_version=VERSION,
                status="running",
                tenant_id="default",
            )
            backend.launch(execution_id, FLOW_ID, VERSION, params or {}, "default")
            return execution_id

        def seed_suspended():
            execution_id = uuid.uuid4().hex[:16]
            fs.insert_local_execution(
                execution_id=execution_id,
                flow_id=FLOW_ID,
                flow_version=VERSION,
                status="suspended",
                tenant_id="default",
            )
            fs.update_local_execution(
                execution_id, context_json='{"step": 1, "$LAST_NODE": "a"}'
            )
            return execution_id

    else:
        backend = engine_backend.memory_backend()
        definition = json.loads(GOOD_DEF)
        definition["flow_id"] = FLOW_ID
        definition["version"] = VERSION
        backend.flow_storage.put_flow(FLOW_ID, VERSION, definition)

        def start(params=None):
            return backend.start(FLOW_ID, VERSION, params or {}, tenant_id="default")

        def seed_suspended():
            execution_id = uuid.uuid4().hex[:16]
            backend.execution_storage.save_execution_state(
                execution_id,
                ExecutionState(
                    execution_id=execution_id,
                    flow_id=FLOW_ID,
                    flow_version=VERSION,
                    status="suspended",
                    context={"step": 1, "$LAST_NODE": "a"},
                ),
            )
            return execution_id

    yield SimpleNamespace(
        backend=backend, kind=request.param, start=start, seed_suspended=seed_suspended
    )

    # 两腿共用的进程内注册表（local_executor 模块表）逐用例清空
    with le._lock:
        le._threads.clear()
        le._cancel_events.clear()
        le._deleted_executions.clear()


# ---- 生命周期 + 日志处理器 + 租约面 ----


def test_start_completes_with_output_context_and_thread_logs(env):
    StubExecution.behavior = "end"
    eid = env.start({"k": "v"})
    status = _wait_status(env.backend, eid, TERMINAL)
    assert status == "completed", f"期望 completed，得到 {status}"

    state = env.backend.read_state(eid)
    # 终态镜像：output / 取消点前 checkpoint / end_time / usage（未采集=None 非 0）
    assert state["output"] == "done"
    assert state["context"] == {"step": 2, "$LAST_NODE": "end"}
    assert state["end_time"]
    assert state["usage"] is None

    # 租约面：engine backend 以 NullExecutionLease 跑完同一状态机
    probe = env.backend.build_worker("conform-lease-probe", "default")
    assert isinstance(probe.execution_lease, NullExecutionLease)

    # 日志处理器：驱动线程的记录被捕获（含 stub 注入 marker）
    messages = env.backend.recent_logs(eid)
    assert any(StubExecution.marker in m for m in messages), messages

    # 线程过滤：测试线程自己的记录不得串进该执行的日志
    own = logging.getLogger("conformance.probe")
    own.info("outside %s", StubExecution.marker)
    assert not any(f"outside {StubExecution.marker}" in m
                   for m in env.backend.recent_logs(eid))


# ---- 取消 Event（T8 契约的 engine 形态）----


def test_cancel_running_execution_thread_exits_at_boundary(env):
    StubExecution.behavior = "block"
    eid = env.start({})
    assert StubExecution.first_started.wait(timeout=10), "执行线程未进入长跑步"

    # 长跑中取消：控制面立即反馈 cancelled（置位 Event + 即时终态）
    assert env.backend.cancel(eid) is True
    assert env.backend.read_status(eid) == "cancelled"

    # 放行长跑步 → 执行线程在步界收口，cancelled 不被翻回 completed（T8 核心）
    StubExecution.release_first.set()
    assert _wait_thread_exit(eid), "执行线程未在步界退出"
    state = env.backend.read_state(eid)
    assert state["status"] == "cancelled"
    assert state["end_time"] is not None
    # 收口补写了取消点前 checkpoint context
    assert state["context"] == {"step": 1, "$LAST_NODE": "a"}
    # 迟到的 output 不写（非 completed 镜像闸）
    assert not state.get("output")


def test_cancel_suspended_execution_finalizes_directly(env):
    eid = env.seed_suspended()
    assert env.backend.cancel(eid) is True
    assert env.backend.read_status(eid) == "cancelled"


def test_cancel_unknown_execution_returns_false(env):
    assert env.backend.cancel("no-such-exec") is False


# ---- 挂起面（checkpoint 落行同形）----


def test_suspend_lands_with_checkpoint_context(env):
    StubExecution.behavior = "suspend"
    eid = env.start({})
    status = _wait_status(env.backend, eid, TERMINAL + ("suspended",))
    assert status == "suspended"
    state = env.backend.read_state(eid)
    assert state["context"] == {"step": 1, "$LAST_NODE": "a"}


# ---- 墓碑（强删窗口：日志抑制 / 行清空 / preminted 拒绝）----


def test_force_delete_suppresses_logs_and_clears_state(env):
    StubExecution.behavior = "block"
    eid = env.start({})
    assert StubExecution.first_started.wait(timeout=10)

    # 先放行再强删：0.2s 后放行 < force 删除 5s join 上限，线程在步界收口
    threading.Timer(0.2, StubExecution.release_first.set).start()
    assert env.backend.force_delete(eid) is True
    assert le._live_thread(eid) is None

    # 行/状态随删清空；日志随执行一并删除，强删窗口内两枚 marker 均被抑制
    time.sleep(0.2)
    assert env.backend.read_status(eid) is None
    assert env.backend.recent_logs(eid) == []


# ---- resume（C1 阶段 3：worker.resume_flow 驱动链双后端等价）----


def test_resume_suspended_completes_via_event(env):
    """挂起执行经 resume(event) 完成。local 腿走宿主入口
    ``engine_backend.resume_execution``（行读取/CAS/绑定增强/nodes 续栽在
    入口内）；memory 腿直接 ``launch_resume``——两条腿最终同一
    ``worker.resume_flow`` 驱动链（lease/终态幂等/#33 守卫沿用基类）。"""
    # 两腿都直接播种挂起态（不经 start 拍）——resume 拍的 stub 形状在
    # launch 之前钉死为 end，杜绝异步驱动线程与主线程改 behavior 的竞态。
    StubExecution.behavior = "end"
    if env.kind == "local":
        eid = uuid.uuid4().hex[:16]
        fs.insert_local_execution(
            execution_id=eid, flow_id=FLOW_ID, flow_version=VERSION,
            status="suspended", tenant_id="default",
        )
        fs.update_local_execution(
            eid, context_json=json.dumps(PENDING_CONTEXT, ensure_ascii=False)
        )
        assert engine_backend.resume_execution(
            fs.get_flow_store(), eid, "event", {"approved": True}
        ) is True
    else:
        eid = uuid.uuid4().hex[:16]
        env.backend.execution_storage.save_execution_state(
            eid,
            ExecutionState(
                execution_id=eid, flow_id=FLOW_ID, flow_version=VERSION,
                status="suspended", context=dict(PENDING_CONTEXT),
            ),
        )
        env.backend.launch_resume(eid, FLOW_ID, "event", {"approved": True})
    StubExecution.behavior = "end"
    assert _wait_thread_exit(eid), "resume 驱动线程未收口"
    assert _wait_status(env.backend, eid, TERMINAL) == "completed"
    state = env.backend.read_state(eid)
    assert state["context"] == {"step": 2, "$LAST_NODE": "end"}
    assert state["output"] == "done"
    # resume 载体以 resume_type=event 进入引擎（挂起形状合法，未被 #258
    # 归一化为 continue——该归一化是 running+非挂起形状的专用路径）
    assert StubExecution.call_log[-1] == (StubExecution.marker, "event"), \
        StubExecution.call_log


def test_resume_seeds_trace_callback_with_row_nodes(env, monkeypatch):
    """resume 把行上已累积 nodes_json 续栽进 trace 回调（本地行模型的面）。

    旧环 resume 传 ``initial_nodes=row.nodes``——缺失即恢复首拍把节点 trace
    清空重写（执行详情页丢恢复前节点）。钉住 wiring。"""
    if env.kind != "local":
        pytest.skip("nodes 续栽是 local 行模型的面")
    seeded = [{"id": "a", "status": "success", "output": None}]
    eid = uuid.uuid4().hex[:16]
    fs.insert_local_execution(
        execution_id=eid, flow_id=FLOW_ID, flow_version=VERSION,
        status="suspended", tenant_id="default",
    )
    fs.update_local_execution(
        eid,
        context_json=json.dumps(PENDING_CONTEXT, ensure_ascii=False),
        nodes_json=json.dumps(seeded, ensure_ascii=False),
    )
    captured: dict = {}
    orig_trace = le._LocalTraceCallback

    class SpyTrace(orig_trace):
        def __init__(self, execution_id, initial_nodes=None):
            captured["initial_nodes"] = initial_nodes
            super().__init__(execution_id, initial_nodes=initial_nodes)

    monkeypatch.setattr(le, "_LocalTraceCallback", SpyTrace)
    StubExecution.behavior = "end"
    assert engine_backend.resume_execution(
        fs.get_flow_store(), eid, "event", {}
    ) is True
    assert _wait_thread_exit(eid), "resume 驱动线程未收口"
    assert captured.get("initial_nodes") == seeded, captured


def test_preminted_start_rejected_under_tombstone_window(env):
    eid = f"conform-tomb-{uuid.uuid4().hex[:8]}"
    if env.kind == "local":
        # 本地墓碑 = 强删窗口（进程内），直接立窗口
        le._mark_deleted(eid)
    else:
        # memory 后端墓碑来自 delete_execution_state（须先有状态行）
        storage = env.backend.execution_storage
        storage.save_execution_state(
            eid,
            ExecutionState(execution_id=eid, flow_id=FLOW_ID, context={},
                           status="completed"),
        )
        storage.delete_execution_state(eid)
    try:
        assert env.backend.tombstone(eid) is not None

        # preminted start 直驱 worker（不经 launch——_spawn 对同 id 清墓碑
        # 是旧环「同 id 重启清窗口」的既定语义，不属于本用例面）：start_flow
        # 的 A51-01 复核必须拒绝重建（deleted/discarded），引擎从未被进入
        StubExecution.behavior = "end"
        worker = env.backend.build_worker(eid, "default")
        result = worker.start_flow(FLOW_ID, params={}, version=VERSION,
                                   execution_id=eid)
        assert result.get("status") == "deleted" and result.get("discarded") is True
        assert StubExecution.call_log == [], StubExecution.call_log
        assert env.backend.read_status(eid) is None
    finally:
        with le._lock:
            le._deleted_executions.discard(eid)
