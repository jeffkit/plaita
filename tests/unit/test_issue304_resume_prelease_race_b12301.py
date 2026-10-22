"""B123-01：resume 定义缺失终态化的租约前写覆写窗（A124 C124-② 三点放大）。

issue 机制（A123-01/A124-01 台账）：resume 入口（flow_worker.py:3882 附近）
在**取租约之前**取流程定义，缺失即以**租约前快照** state 调
``_terminalize_definition_missing`` 终态化——未持租约 ⇒ fenced 存储纯透传
普通写。窗口内持缓存定义的活 worker 经租约推进至 completed 终态后，陈旧
快照（suspended + 旧 context）的 error 写**无条件覆写终态**：状态回退 +
进度丢失（B123-01 RED）。

A124 升级三点（C124-②，同窗三点放大）：
  (a) ``_persist_state_or_raise`` **整行回写**陈旧 checkpoint/context——
      error hint 引导 ``resume_type=retry`` 从旧断点续跑＝已成功节点重放；
  (b) ``_release_sandboxes`` 按快照 context 跨进程 pause **活跑 worker
      正在用的沙箱**（error→keep_data=True 保现场）；
  (c) 假终态先行 ``run_end(final=true)`` 时间线事件，真终态再发一条
      （下游消费双 run_end）。

修法（一处收三点）：resume 租约前改传 ``state=None``（与 start :3275 同
款）——``_terminalize_definition_missing`` 现读盘：已终态一律不覆写（
:2408 防线，本文件 state=None 直格钉住）；非终态才终态化，且改的是现读
行（context 不倒灌）。in-lease 复核点（:3966）持租约 fenced，不受影响
（test_issue247_resume_version_refresh.py 同族对照）。

负控（修复前全红）：竞窗用「快照已读 → 活 worker 写终态 → 取定义失败」
序列化最坏序——monkeypatch ``get_flow_definition`` 在抛缺失前先把盘上
执行推进到目标状态，精确复现 load→取定义之间的竞窗。
"""
from __future__ import annotations

from datetime import datetime

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis

from plaita.server.execution_lease import RedisExecutionLease
from plaita.server.flow_worker import FlowDefinitionMissingError, FlowWorker
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

EXEC = "exec-b12301"

STALE_CONTEXT = {"$LAST_NODE": "node1", "$NODE": {"node1": {"stale": True}}}
FRESH_DONE_CONTEXT = {
    "$LAST_NODE": "end",
    "$NODE": {},
    "fresh_marker": "live-worker-completed",
}
FRESH_RUNNING_CONTEXT = {
    "$LAST_NODE": "node3",
    "$NODE": {"node2": {"checkpoint": "new"}},
}


def _worker_without_definitions(
    storage: MemoryExecutionStorage,
) -> FlowWorker:
    """不带任何 flow 定义的 worker（模拟版本被删/未同步的引擎存储）。"""
    fake = fakeredis.FakeRedis(decode_responses=True)
    return FlowWorker(
        storage,
        MemoryFlowStorage(),
        execution_lease=RedisExecutionLease(fake),
        lease_ttl_seconds=60,
    )


def _seed(
    storage: MemoryExecutionStorage,
    status: str = "suspended",
    context: dict | None = None,
) -> None:
    storage.save_execution_state(
        EXEC,
        ExecutionState(
            execution_id=EXEC,
            flow_id="f1",
            flow_version="1",
            status=status,
            tenant_id="",
            context=context
            if context is not None
            else dict(STALE_CONTEXT),
        ),
    )


def _inject_race(
    worker: FlowWorker,
    storage: MemoryExecutionStorage,
    *,
    status: str,
    context: dict,
) -> None:
    """序列化「快照已读 → 竞窗内活 worker 推进 → 取定义失败」的最坏序。

    resume 流程：``_load_and_guard_resume_state``（租约前快照）→
    ``get_flow_definition``。本桩在后者内部先把盘上执行推到指定状态
    （持缓存定义的活 worker 经租约推进），再抛定义缺失——resume worker
    随后以**陈旧快照**进入终态化，正好踩进覆写窗。
    """

    def racing_get_flow_definition(flow_id, version=None):
        storage.save_execution_state(
            EXEC,
            ExecutionState(
                execution_id=EXEC,
                flow_id="f1",
                flow_version="1",
                status=status,
                tenant_id="",
                context=dict(context),
                end_time=(
                    datetime.now().isoformat() if status in ("completed", "error", "cancelled") else None
                ),
            ),
        )
        raise FlowDefinitionMissingError(
            f"racing injection: flow {flow_id}@{version} missing"
        )

    worker.get_flow_definition = racing_get_flow_definition  # type: ignore[method-assign]


def test_resume_prelease_race_completed_not_clobbered():
    """RED 主格（B123-01）：竞窗内活 worker 写下 completed，不被陈旧 error 覆写。"""
    storage = MemoryExecutionStorage()
    _seed(storage, status="suspended")
    worker = _worker_without_definitions(storage)
    _inject_race(
        worker, storage, status="completed", context=FRESH_DONE_CONTEXT
    )

    with pytest.raises(FlowDefinitionMissingError):
        worker.resume_flow("f1", EXEC, "event")

    saved = storage.load_execution_state(EXEC)
    assert saved.status == "completed", (
        f"竞窗内活 worker 写下的 completed 终态必须存活，实际被覆写为 "
        f"{saved.status!r}（陈旧快照倒灌，B123-01）"
    )
    assert saved.error is None, "completed 终态不得被塞入陈旧 error"
    assert saved.context.get("fresh_marker") == "live-worker-completed", (
        "活 worker 写下的 fresh context 不得被快照旧 context 倒灌"
    )


def test_resume_race_nonterminal_terminates_on_fresh_context():
    """波及面(a)：真终态化场景（执行仍非终态）不倒灌旧断点 context。

    定义确实缺失、竞窗内活 worker 把执行推进到 running 新断点——终态化
    error 照常落（#304 语义保持），但必须改**现读行**：context 保持新断
    点，retry 从新断点续跑；修复前整行回写快照旧 context，retry 会从旧
    断点重放已成功节点。
    """
    storage = MemoryExecutionStorage()
    _seed(storage, status="suspended")
    worker = _worker_without_definitions(storage)
    _inject_race(
        worker, storage, status="running", context=FRESH_RUNNING_CONTEXT
    )

    with pytest.raises(FlowDefinitionMissingError):
        worker.resume_flow("f1", EXEC, "event")

    saved = storage.load_execution_state(EXEC)
    assert saved.status == "error", "#304 终态化语义保持：非终态行仍须终态化"
    assert saved.error is not None
    assert saved.error.get("error_type") == "flow_definition_missing"
    assert saved.context.get("$LAST_NODE") == "node3", (
        f"终态化必须落在现读行（新断点 node3），实际 context=$LAST_NODE="
        f"{saved.context.get('$LAST_NODE')!r}（快照旧断点倒灌 → retry 重放已成功节点）"
    )


def test_resume_race_no_sandbox_pause_from_stale_context():
    """波及面(b)：竞窗场景不得按快照 context 跨进程 pause 活跑沙箱。

    修复前：陈旧快照被终态化 error → ``_persist_state_or_raise`` →
    ``_release_sandboxes`` 按快照 context drain（error→keep_data=True=
    pause 保现场），pause 的却是活跑 worker 正在用的沙箱。修复后：现读
    已终态即返回，persist 收口整体不进，``_release_sandboxes`` 零调用。
    """
    storage = MemoryExecutionStorage()
    _seed(storage, status="suspended")
    worker = _worker_without_definitions(storage)
    _inject_race(
        worker, storage, status="completed", context=FRESH_DONE_CONTEXT
    )

    released: list[tuple[str, str]] = []
    worker._release_sandboxes = (  # type: ignore[method-assign]
        lambda execution_id, state: released.append(
            (execution_id, getattr(state, "status", "?"))
        )
    )

    with pytest.raises(FlowDefinitionMissingError):
        worker.resume_flow("f1", EXEC, "event")

    assert released == [], (
        f"竞窗场景（盘上已 completed）不得触发沙箱释放，实际 "
        f"{released!r}——按快照 context pause 活跑沙箱"
    )


def test_resume_race_no_false_terminal_run_end():
    """波及面(c)：竞窗场景不发假终态 run_end（真终态已由活 worker 发过）。

    修复前：陈旧快照终态化进 ``_persist_state_or_raise`` →
    ``_collect_execution_timeline`` 对终态发 ``run_end(final=true)``——
    假终态先行，活 worker 的真终态再发一条，下游双收。修复后：不进
    persist 收口，时间线零动作。
    """
    storage = MemoryExecutionStorage()
    _seed(storage, status="suspended")
    worker = _worker_without_definitions(storage)
    _inject_race(
        worker, storage, status="completed", context=FRESH_DONE_CONTEXT
    )

    timeline_calls: list[str] = []
    worker._collect_execution_timeline = (  # type: ignore[method-assign]
        lambda execution_id, state, attempt_end=None: timeline_calls.append(
            getattr(state, "status", "?")
        )
    )

    with pytest.raises(FlowDefinitionMissingError):
        worker.resume_flow("f1", EXEC, "event")

    assert timeline_calls == [], (
        f"竞窗场景不得经 persist 收口发时间线事件（假终态 run_end 双发面），"
        f"实际 {timeline_calls!r}"
    )


def test_terminalize_state_none_reread_respects_terminal():
    """state=None 现读直格：已终态（completed）一律不覆写（:2408 防线）。

    修法把 resume 租约前切到 state=None 现读路径——该路径的行为由本格
    与下格直接钉住，不依赖上层 resume 流程。
    """
    storage = MemoryExecutionStorage()
    _seed(storage, status="completed", context=FRESH_DONE_CONTEXT)
    worker = _worker_without_definitions(storage)

    worker._terminalize_definition_missing(EXEC, "f1", "1", None, phase="test")

    saved = storage.load_execution_state(EXEC)
    assert saved.status == "completed"
    assert saved.error is None
    assert saved.context.get("fresh_marker") == "live-worker-completed"


def test_terminalize_state_none_still_terminalizes_nonterminal():
    """state=None 现读直格：非终态（suspended）照常终态化（#304 语义保持）。"""
    storage = MemoryExecutionStorage()
    _seed(storage, status="suspended")
    worker = _worker_without_definitions(storage)

    worker._terminalize_definition_missing(EXEC, "f1", "1", None, phase="test")

    saved = storage.load_execution_state(EXEC)
    assert saved.status == "error"
    assert saved.error is not None
    assert saved.error.get("error_type") == "flow_definition_missing"
    assert saved.end_time
