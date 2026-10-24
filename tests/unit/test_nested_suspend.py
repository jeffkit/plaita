"""plaita#311 NS-M3：嵌套挂起 v1——child/reference 链内的挂起穿透与沿路径恢复。

语义契约（plaita-rs docs/PROGRAM.md 程式一 + docs/specs/nested-suspend-validation.md
Q6/修正清单 #5/#7）：挂起节点位于任意深度的 child/reference 调用链内部时，分布式
执行把流程停在该内层位置、checkpoint 逐帧持久化恢复路径、决策载体到达时沿调用
栈精确续跑；迭代帧（loop/map/...）与 parallel 分支帧上的挂起具名拒绝（fail-closed）；
normal/generator 模式的挂起（根流程或子执行内）从「静默假完成」变为具名失败
（缺陷修复半边，修正清单 #7：与子执行 fail-open 是同一个洞）。

与 rs/go 的对拍锚点（测试名后括号标注）：
- 传播压帧序 = rs advance_from push_frame（最内父帧先入，恢复最深帧先返回）；
- 转换点作用域闸 = rs suspension_carrier_or_reject（修正清单 #1）；
- 再挂起栈拼接 = rs inner.call_stack.extend(stack) / go ResumeSuspension；
- 返回步回填 = $NODE[child]=v + $LAST_NODE=child + 不写 $ERROR（程式 ③.2）。
"""

from __future__ import annotations

import asyncio

import pytest

from plaita.core.errors import (
    FlowSuspendRejectedError,
    FlowSuspendedCarrier,
    ResumeGuardError,
    SuspendNotExecutableError,
)
from plaita.core.executor import FlowExecution
from plaita.core.flow import Flow
from plaita.core.strategies import (
    DistributedStrategy,
    SUSPEND_FRAMES_KEY,
    flow_contains_suspending_node,
)

# ---------------------------------------------------------------------------
# 测试素材与替身
# ---------------------------------------------------------------------------

EVENT_CHILD = {
    "flow_id": "child1", "version": "1", "runtime": "python",
    "nodes": [
        {"type": "start", "id": "cstart", "next": "wait"},
        {"type": "event", "id": "wait", "event_type": "approval", "next": "cend"},
        {"type": "end", "id": "cend", "output": {"done": True}},
    ],
}

#: 孙代链：root -> A(call_b) -> B(event)
GRANDCHILD_B = {
    "flow_id": "B", "version": "1", "runtime": "python",
    "nodes": [
        {"type": "start", "id": "bstart", "next": "wait"},
        {"type": "event", "id": "wait", "event_type": "deep", "next": "bend"},
        {"type": "end", "id": "bend", "output": "b-done"},
    ],
}
GRANDCHILD_A = {
    "flow_id": "A", "version": "1", "runtime": "python",
    "nodes": [
        {"type": "start", "id": "astart", "next": "callb"},
        {"type": "child", "id": "callb", "childFlow": GRANDCHILD_B, "next": "aend"},
        {"type": "end", "id": "aend", "output": "a-done"},
    ],
}
GRANDCHILD_ROOT = {
    "flow_id": "R", "version": "1", "runtime": "python",
    "nodes": [
        {"type": "start", "id": "start", "next": "calla"},
        {"type": "child", "id": "calla", "childFlow": GRANDCHILD_A, "next": "end"},
        {"type": "end", "id": "end", "output": "root-done"},
    ],
}

#: 再挂起：child 内两个 event 串行
RESUSPEND_CHILD = {
    "flow_id": "C2", "version": "1", "runtime": "python",
    "nodes": [
        {"type": "start", "id": "s", "next": "w1"},
        {"type": "event", "id": "w1", "event_type": "first", "next": "mid"},
        {"type": "assignment", "id": "mid", "result": "stepped", "next": "w2"},
        {"type": "event", "id": "w2", "event_type": "second", "next": "e"},
        {"type": "end", "id": "e", "output": "child2-done"},
    ],
}
RESUSPEND_ROOT = {
    "flow_id": "R3", "version": "1", "runtime": "python",
    "nodes": [
        {"type": "start", "id": "start", "next": "call"},
        {"type": "child", "id": "call", "childFlow": RESUSPEND_CHILD, "next": "end"},
        {"type": "end", "id": "end", "output": "root3-done"},
    ],
}

#: reference 链（调度器注入 child_flow 的形状见 _make_reference_root）
REFERENCE_CHILD = {
    "flow_id": "RC", "version": "1", "runtime": "python",
    "inputType": {"dataType": "any"},
    "nodes": [
        {"type": "start", "id": "s", "next": "w"},
        {"type": "event", "id": "w", "event_type": "ref-evt", "next": "e"},
        {"type": "end", "id": "e", "output": {"who": "ref"}},
    ],
}
REFERENCE_ROOT = {
    "flow_id": "RR", "version": "1", "runtime": "python",
    "nodes": [
        {"type": "start", "id": "start", "next": "ref"},
        {"type": "reference", "id": "ref", "flowID": "RC", "input": "$INPUT", "next": "end"},
        {"type": "end", "id": "end", "output": "refroot-done"},
    ],
}


class RecordingBus:
    """带注册/注销台账的简化 event bus 替身（订阅生命周期断言用）。"""

    def __init__(self):
        self.subscriptions = {}
        self.unregistered = []
        self._seq = 0

    async def register_subscription(self, event_type=None, filter_condition=None,
                                    correlation_id=None, flow_id=None, node_id=None, **kw):
        self._seq += 1
        sid = f"sub-{self._seq}"
        self.subscriptions[sid] = {
            "event_type": event_type,
            "correlation_id": correlation_id,
            "flow_id": flow_id,
            "node_id": node_id,
        }
        return sid

    def unregister_subscription(self, subscription_id):
        self.unregistered.append(subscription_id)
        return self.subscriptions.pop(subscription_id, None)


def _root_flow():
    """对照探针场景的最小 flow：root -> child(event) -> end。"""
    return Flow.model_validate({
        "flow_id": "root1", "version": "1", "runtime": "python",
        "nodes": [
            {"type": "start", "id": "start", "next": "call"},
            {"type": "child", "id": "call", "childFlow": EVENT_CHILD, "next": "end"},
            {"type": "end", "id": "end", "output": "root-done"},
        ],
    })


def _distributed(event_bus=None):
    ex = FlowExecution(event_bus=event_bus) if event_bus else FlowExecution()
    ex.mode = "distributed"
    return ex


def _frame_path(context):
    frames = context.get("$SUSPEND_FRAMES")
    assert isinstance(frames, list) and frames, "checkpoint 应携带嵌套帧路径"
    return [(f["flow_id"], f["node_id"]) for f in frames]


# ---------------------------------------------------------------------------
# 1. 分布式：child 链内 event 挂起 → 事件恢复 → 沿路径续跑到 end
#    （对照探针场景：从「假完成」变「真挂起 + 真恢复」）
# ---------------------------------------------------------------------------


class TestNestedSuspendProbeScenario:
    def test_child_event_suspends_with_subscription(self):
        """child 内 event 从「假完成」变「真挂起」：is_suspend=True + 订阅注册。"""
        bus = RecordingBus()
        flow = _root_flow()
        ex = _distributed(bus)
        r1 = ex.run_distributed(flow, {})
        assert r1["id"] == "call"
        assert r1.get("is_suspend") is True
        assert r1.get("is_end") is not True
        # 订阅注册在**最内帧**节点上，correlation_id 路由回**根执行**（恢复消息
        # 要回到载帧路径的根 checkpoint，不能用子执行的临时 execution_id）。
        assert len(bus.subscriptions) == 1
        sub = next(iter(bus.subscriptions.values()))
        assert sub["node_id"] == "wait"
        assert sub["correlation_id"] == ex.execution_id
        # 订阅 id 写回最内帧的 $NODE（随帧 checkpoint 持久化）。
        frames = r1["context"][f"${SUSPEND_FRAMES_KEY}"]
        inner_state = frames[0]["context"]["$NODE"]["wait"]
        assert inner_state["status"] == "pending"
        assert inner_state["subscription_id"] in bus.subscriptions

    def test_frame_path_in_checkpoint(self):
        """checkpoint 逐帧化：帧路径 = 现场（最内）在前、根帧收尾（context=None）。"""
        flow = _root_flow()
        r1 = _distributed().run_distributed(flow, {})
        frames = r1["context"][f"${SUSPEND_FRAMES_KEY}"]
        assert [(f["flow_id"], f["node_id"]) for f in frames] == [
            ("child1", "wait"),   # 现场帧：挂起节点所在帧
            ("root1", "call"),    # 根帧：返回值回填目标（context=None）
        ]
        assert frames[-1]["context"] is None
        assert frames[0]["context"]["$FLOW_ID"] == "child1"

    def test_event_resume_walks_path_to_root_end(self):
        """事件恢复 → 沿路径续跑：child 完成 → 返回步回填 $NODE[call] → continue 到根完成。"""
        bus = RecordingBus()
        flow = _root_flow()
        r1 = _distributed(bus).run_distributed(flow, {})
        sid = next(iter(bus.subscriptions))

        r2 = _distributed(bus).run_distributed(
            flow, saved_context=r1["context"], resume_type="event",
            resume_data={"subscription_id": sid, "event_type": "approval"})
        # 返回步：child 流程在本步内跑完并回填父帧，输出 = 根帧子节点。
        assert r2.get("is_suspend") is not True
        assert r2["id"] == "call"
        assert r2["result"] == {"done": True}
        # 回填细则（程式 ③.2）：$NODE[call]=返回值、$LAST_NODE=call、帧路径已消费。
        assert r2["context"]["$NODE"]["call"] == {"done": True}
        assert r2["context"]["$LAST_NODE"] == "call"
        assert r2["context"][f"${SUSPEND_FRAMES_KEY}"] is None
        # 最内帧订阅已注销。
        assert sid not in bus.subscriptions

        r3 = _distributed(bus).run_distributed(
            flow, saved_context=r2["context"], resume_type="continue")
        assert r3["id"] == "end"
        assert r3["is_end"] is True
        assert r3["result"] == "root-done"

    def test_traversing_child_node_gets_error_node_end(self):
        """穿越型子节点挂起时 NodeEnd 携带 error 文本（rs Q2 两种 NodeEnd 形状）。"""
        from plaita.core.callback import CallbackManager

        events = []

        class _Recorder:
            def on_node_end(self, flow, node, result=None, error=None, exception=None, **kw):
                events.append(("node_end", node.id, error))

            def __getattr__(self, name):
                return lambda *a, **k: None

        flow = _root_flow()
        manager = CallbackManager([_Recorder()])
        ex = FlowExecution(callback_manager=manager)
        ex.mode = "distributed"
        r1 = ex.run_distributed(flow, {})
        assert r1.get("is_suspend") is True
        node_ends = [e for e in events if e[0] == "node_end"]
        # call（child 节点）以 error 形态收尾；wait（直接挂起节点）由边界
        # on_node_suspend 表达，不经 error NodeEnd。
        call_ends = [e for e in node_ends if e[1] == "call"]
        assert call_ends and call_ends[0][2] is not None


# ---------------------------------------------------------------------------
# 2. 孙代双帧；reference 链隔离保持 + 挂起穿透
# ---------------------------------------------------------------------------


class TestDeepChains:
    def test_grandchild_double_frame_park_and_resume(self):
        """root -> A -> B(event)：帧路径三层；恢复经两次返回步逐层上溯。"""
        bus = RecordingBus()
        flow = Flow.model_validate(GRANDCHILD_ROOT)
        r1 = _distributed(bus).run_distributed(flow, {})
        assert r1.get("is_suspend") is True
        assert _frame_path(r1["context"]) == [
            ("B", "wait"), ("A", "callb"), ("R", "calla"),
        ]
        sid = next(iter(bus.subscriptions))

        r2 = _distributed(bus).run_distributed(
            flow, saved_context=r1["context"], resume_type="event",
            resume_data={"subscription_id": sid, "event_type": "deep"})
        # 两次返回步（B→A、A→root）都在本步完成：$LAST_NODE 直达根帧子节点。
        # （callb 的回填落在 A 帧上下文——帧消费后即弃，只在再挂起的帧快照里
        # 可观测，见 test_resume_then_resuspend_merges_stack 的中间帧断言。）
        assert r2.get("is_suspend") is not True
        assert r2["id"] == "calla"
        assert r2["result"] == "a-done"
        assert r2["context"]["$NODE"]["calla"] == "a-done"
        assert r2["context"]["$LAST_NODE"] == "calla"

        r3 = _distributed(bus).run_distributed(
            flow, saved_context=r2["context"], resume_type="continue")
        assert r3["is_end"] is True and r3["result"] == "root-done"

    def test_grandchild_subscription_correlation_is_root_execution(self):
        """孙子链的订阅 correlation_id 仍是根执行 id（event bus 递归查找示波钉）。

        历史上 get_or_create_event_bus 只看一层 parent——孙子执行拿不到宿主注入
        在根执行上的 bus，订阅静默落到进程级默认，恢复消息路由不到根 checkpoint。
        """
        bus = RecordingBus()
        flow = Flow.model_validate(GRANDCHILD_ROOT)
        ex = _distributed(bus)
        ex.run_distributed(flow, {})
        assert len(bus.subscriptions) == 1
        sub = next(iter(bus.subscriptions.values()))
        assert sub["correlation_id"] == ex.execution_id
        assert sub["node_id"] == "wait"

    def test_reference_chain_isolation_and_suspend(self):
        """reference 链：$PARENT 为构造期独立快照（存疑 #14 逐帧显式另存）+ 挂起穿透。"""
        bus = RecordingBus()
        flow = Flow.model_validate(REFERENCE_ROOT)
        # 调度器职责：reference 节点的 child_flow 注入。
        flow.find_node_by_id("ref").child_flow = Flow.model_validate(REFERENCE_CHILD)

        r1 = _distributed(bus).run_distributed(flow, {"order": "O-1"})
        assert r1.get("is_suspend") is True
        assert _frame_path(r1["context"]) == [("RC", "w"), ("RR", "ref")]
        parent_snapshot = r1["context"][f"${SUSPEND_FRAMES_KEY}"][0]["context"]["$PARENT"]
        # $PARENT 是子上下文构造期的快照（含根的状态键），不指向父的活状态。
        assert isinstance(parent_snapshot, dict)
        assert parent_snapshot.get("$FLOW_ID") == "RR"

        sid = next(iter(bus.subscriptions))
        r2 = _distributed(bus).run_distributed(
            flow, saved_context=r1["context"], resume_type="event",
            resume_data={"subscription_id": sid, "event_type": "ref-evt"})
        assert r2.get("is_suspend") is not True
        assert r2["result"] == {"who": "ref"}
        assert r2["context"]["$LAST_NODE"] == "ref"

        r3 = _distributed(bus).run_distributed(
            flow, saved_context=r2["context"], resume_type="continue")
        assert r3["is_end"] is True and r3["result"] == "refroot-done"


# ---------------------------------------------------------------------------
# 3. 作用域闸：迭代帧具名拒（fail-closed）；errorHandler continue 不吞
# ---------------------------------------------------------------------------


LOOP_ROOT = {
    "flow_id": "loop_root", "version": "1", "runtime": "python",
    "nodes": [
        {"type": "start", "id": "start", "next": "lp"},
        {"type": "loop", "id": "lp", "collection": "$INPUT.items",
         "childFlow": EVENT_CHILD, "next": "end"},
        {"type": "end", "id": "end", "output": "x"},
    ],
}

LOOP_MID = {
    "flow_id": "mid", "version": "1", "runtime": "python",
    "nodes": [
        {"type": "start", "id": "mstart", "next": "lp"},
        {"type": "loop", "id": "lp", "collection": "$INPUT.items",
         "childFlow": EVENT_CHILD, "next": "mend"},
        {"type": "end", "id": "mend", "output": "mid-done"},
    ],
}
LOOP_VIA_CHILD_ROOT = {
    "flow_id": "loop_via_child", "version": "1", "runtime": "python",
    "nodes": [
        {"type": "start", "id": "start", "next": "call"},
        {"type": "child", "id": "call", "childFlow": LOOP_MID, "next": "end"},
        {"type": "end", "id": "end", "output": "x"},
    ],
}


class TestScopeGate:
    def test_loop_direct_suspension_named_rejection(self):
        """loop 直连（loop 体即 event）：具名拒，消息点名 loop 帧。"""
        flow = Flow.model_validate(LOOP_ROOT)
        with pytest.raises(Exception) as ei:
            _distributed().run_distributed(flow, {"items": [1]})
        # distributed 契约把一切异常归一为 FlowErrorException/-500，但拒绝语义
        # 与阻断帧类型保留在消息链里（具名可观测，非静默）。
        assert "frame boundary" in str(ei.value)
        assert "'loop'" in str(ei.value)

    def test_loop_via_child_suspension_named_rejection(self):
        """loop→child 旁路（rs 验证报告 Q4 的遮蔽组合）：转换点闸具名拒。"""
        flow = Flow.model_validate(LOOP_VIA_CHILD_ROOT)
        with pytest.raises(Exception) as ei:
            _distributed().run_distributed(flow, {"items": [1]})
        assert "frame boundary" in str(ei.value)
        assert "'loop'" in str(ei.value)

    def test_loop_error_handler_continue_does_not_swallow_carrier(self):
        """loop 节点 errorHandler=continue 不吞挂起载体（rs Q1 stringify 同病的
        plaita 防线：runner 逃逸分支先于 errorHandler 分发）。"""
        swallow_flow = dict(LOOP_ROOT)
        swallow_flow["nodes"] = [dict(n) for n in LOOP_ROOT["nodes"]]
        swallow_flow["nodes"][1] = dict(swallow_flow["nodes"][1],
                                        errorHandler={"strategy": "continue"})
        flow = Flow.model_validate(swallow_flow)
        with pytest.raises(Exception) as ei:
            _distributed().run_distributed(flow, {"items": [1]})
        assert "frame boundary" in str(ei.value)

    def test_parallel_branch_static_gate(self):
        """parallel 分支含挂起节点：派发前静态具名拒（修正清单 #4：后台分支的
        动态拒绝是异步的，无法同步上抛）。"""
        flow = Flow.model_validate({
            "flow_id": "par_root", "version": "1", "runtime": "python",
            "nodes": [
                {"type": "start", "id": "start", "next": "par"},
                {"type": "parallel", "id": "par", "joinBranches": ["b1"],
                 "branches": [{"name": "b1", "flow": EVENT_CHILD}], "next": "end"},
                {"type": "end", "id": "end", "output": "x"},
            ],
        })
        with pytest.raises(Exception) as ei:
            _distributed().run_distributed(flow, {})
        assert "parallel frames" in str(ei.value)

    def test_flow_contains_suspending_node_scans_nested(self):
        """静态扫描递归穿 child_flow（含挂起节点的嵌套图也判真）。"""
        assert flow_contains_suspending_node(Flow.model_validate(EVENT_CHILD))
        wrapper = Flow.model_validate({
            "flow_id": "w", "version": "1", "runtime": "python",
            "nodes": [
                {"type": "start", "id": "s", "next": "c"},
                {"type": "child", "id": "c", "childFlow": EVENT_CHILD, "next": "e"},
                {"type": "end", "id": "e", "output": "x"},
            ],
        })
        assert flow_contains_suspending_node(wrapper)

    def test_gate_rejection_is_typed_in_normal_mode(self):
        """normal 模式下闸拒绝保留具名类型（#330 后根出口呈现为
        SuspendNotExecutableError；闸点载体/闸具名拒经 __cause__ 与 fix 可辨）。"""
        flow = Flow.model_validate(LOOP_ROOT)
        ex = FlowExecution()
        ex.mode = "normal"
        with pytest.raises(SuspendNotExecutableError) as ei:
            ex.run_compatible(flow, False, items=[1])
        # 迭代帧挂起：即便分布式也救不了（作用域闸）——fix 点名 restructure。
        assert ei.value.to_dict()["fix"]["action"] == "restructure"


# ---------------------------------------------------------------------------
# 4. normal/generator 模式：挂起节点具名失败（缺陷修复的行为钉，修正清单 #7）
# ---------------------------------------------------------------------------


class TestNormalModeNamedFailure:
    def test_root_event_in_normal_mode_is_named_failure(self):
        """根流程 normal 模式的挂起节点：不再静默假完成。

        #330 起根出口呈现为 ``SuspendNotExecutableError``（独立异常类）；
        plaita#331 起入口静态闸先行——静态闭包看得见的挂起节点在任何节点执行
        前即被拒（``stage="entry"``，``__cause__`` 无 carrier），运行期根出口
        呈现降为兜底（``_advance_one`` 直抛 carrier 钉在分布式宿主格不变）。
        """
        flow = Flow.model_validate({
            "flow_id": "n1", "version": "1", "runtime": "python",
            "nodes": [
                {"type": "start", "id": "start", "next": "wait"},
                {"type": "event", "id": "wait", "event_type": "e", "next": "end"},
                {"type": "end", "id": "end", "output": "x"},
            ],
        })
        ex = FlowExecution()
        ex.mode = "normal"
        with pytest.raises(SuspendNotExecutableError) as ei:
            ex.run_compatible(flow, False)
        assert ei.value.stage == "entry"
        assert ei.value.__cause__ is None

    def test_child_event_in_normal_mode_is_named_failure(self):
        """child 内 event 在 normal 模式：pending 载荷不再被当数据（历史静默
        假完成的直接钉）。"""
        flow = _root_flow()
        ex = FlowExecution()
        ex.mode = "normal"
        with pytest.raises(SuspendNotExecutableError) as ei:
            ex.run_compatible(flow, False)
        # 消息点名挂起节点与所在 flow（fail-open 时代的静默零报错对照面）。
        assert "'wait'" in str(ei.value)
        assert "child1" in str(ei.value)

    def test_root_event_in_generator_mode_is_named_failure(self):
        """根流程 generator 模式：同一具名失败（A2 评审 §2 采 Throw——与 Go
        ExecuteGenerator 的 Step{Error} 决策分类对齐，不做 yield 挂起步）。"""
        flow = Flow.model_validate({
            "flow_id": "g1", "version": "1", "runtime": "python",
            "nodes": [
                {"type": "start", "id": "start", "next": "wait"},
                {"type": "event", "id": "wait", "event_type": "e", "next": "end"},
                {"type": "end", "id": "end", "output": "x"},
            ],
        })
        ex = FlowExecution()
        ex.mode = "generator"
        with pytest.raises(SuspendNotExecutableError):
            list(ex.run_compatible(flow, True))

    def test_carrier_escapes_normal_drive_of_suspended_checkpoint(self):
        """用 normal 语义驱动带嵌套挂起现场的 checkpoint：不得被当普通推进吞掉
        （策略层外的兜底面）。plaita#331 起 runner 兜底闸在挂起节点 execute
        **前**具名拒绝（``stage="runner"``，宿主身份取执行链根=normal）——
        比 #330 时代「先再订阅再在根出口呈现」更早终态、无重复订阅副作用；
        分布式宿主的载体穿透钉见 TestNestedSuspension 分布式格（不变）。"""
        from plaita.core.runner import NodeRunner
        from plaita.core.strategies import _advance_one

        flow = _root_flow()
        r1 = _distributed().run_distributed(flow, {})
        assert r1.get("is_suspend") is True

        fresh = FlowExecution()
        fresh.mode = "normal"
        fresh._ctx.context = r1["context"]
        fresh._ctx.setup_flow(flow, (), {})
        node = flow.find_node_by_id("call")
        # node_execution=FlowExecution facade：子节点 arun 需要
        # get_child_execution（裸 ExecutionContext 没有该方法）。
        runner = NodeRunner(fresh._ctx, node_execution=fresh)
        with pytest.raises(SuspendNotExecutableError) as ei:
            asyncio.run(_advance_one(flow, runner, fresh.callback_manager, node))
        assert ei.value.stage == "runner"
        assert ei.value.node_id == "wait"


# ---------------------------------------------------------------------------
# 5. 恢复中再挂起：帧路径拼接、二次恢复到根完成
# ---------------------------------------------------------------------------


class TestReSuspension:
    def test_resume_then_resuspend_merges_stack(self):
        """恢复 → 续跑中再挂起：新现场更深在前 + 会话剩余栈在后（rs
        ``inner.call_stack.extend(stack)``）；旧订阅先注销、新订阅后注册。"""
        bus = RecordingBus()
        flow = Flow.model_validate(RESUSPEND_ROOT)
        r1 = _distributed(bus).run_distributed(flow, {})
        assert _frame_path(r1["context"]) == [("C2", "w1"), ("R3", "call")]
        sid1 = next(iter(bus.subscriptions))

        r2 = _distributed(bus).run_distributed(
            flow, saved_context=r1["context"], resume_type="event",
            resume_data={"subscription_id": sid1, "event_type": "first"})
        # 再挂起：帧路径拼接为 w2 的现场 + 根帧；旧订阅已注销、新订阅已注册。
        assert r2.get("is_suspend") is True
        assert _frame_path(r2["context"]) == [("C2", "w2"), ("R3", "call")]
        assert sid1 not in bus.subscriptions
        assert len(bus.subscriptions) == 1
        sid2 = next(iter(bus.subscriptions))
        assert bus.subscriptions[sid2]["node_id"] == "w2"

        r3 = _distributed(bus).run_distributed(
            flow, saved_context=r2["context"], resume_type="event",
            resume_data={"subscription_id": sid2, "event_type": "second"})
        assert r3.get("is_suspend") is not True
        assert r3["result"] == "child2-done"
        assert r3["context"]["$NODE"]["call"] == "child2-done"

        r4 = _distributed(bus).run_distributed(
            flow, saved_context=r3["context"], resume_type="continue")
        assert r4["is_end"] is True and r4["result"] == "root3-done"

    def test_suspension_inside_middle_frame_of_resume_session(self):
        """恢复会话的中间父帧续跑中再挂起（root→A→B 恢复后 A 内新 event）：
        三层帧路径重建。"""
        bus = RecordingBus()
        mid_with_two = {
            "flow_id": "A2", "version": "1", "runtime": "python",
            "nodes": [
                {"type": "start", "id": "astart", "next": "callb"},
                {"type": "child", "id": "callb", "childFlow": GRANDCHILD_B, "next": "w2"},
                {"type": "event", "id": "w2", "event_type": "mid-evt", "next": "aend"},
                {"type": "end", "id": "aend", "output": "a2-done"},
            ],
        }
        root = {
            "flow_id": "R2", "version": "1", "runtime": "python",
            "nodes": [
                {"type": "start", "id": "start", "next": "calla"},
                {"type": "child", "id": "calla", "childFlow": mid_with_two, "next": "end"},
                {"type": "end", "id": "end", "output": "root2-done"},
            ],
        }
        flow = Flow.model_validate(root)
        r1 = _distributed(bus).run_distributed(flow, {})
        assert _frame_path(r1["context"]) == [("B", "wait"), ("A2", "callb"), ("R2", "calla")]
        sid = next(iter(bus.subscriptions))
        r2 = _distributed(bus).run_distributed(
            flow, saved_context=r1["context"], resume_type="event",
            resume_data={"subscription_id": sid, "event_type": "deep"})
        # B 完成返回 A2 后，A2 的 w2 再挂起：帧路径 = 新现场 + 剩余根帧。
        assert r2.get("is_suspend") is True
        assert _frame_path(r2["context"]) == [("A2", "w2"), ("R2", "calla")]
        # 中间帧回填已落盘：callb 的返回值在 A2 帧状态里。
        frames = r2["context"][f"${SUSPEND_FRAMES_KEY}"]
        assert frames[0]["context"]["$NODE"]["callb"] == "b-done"
        sid2 = next(iter(bus.subscriptions))
        r3 = _distributed(bus).run_distributed(
            flow, saved_context=r2["context"], resume_type="event",
            resume_data={"subscription_id": sid2, "event_type": "mid-evt"})
        assert r3.get("is_suspend") is not True
        assert r3["result"] == "a2-done"
        r4 = _distributed(bus).run_distributed(
            flow, saved_context=r3["context"], resume_type="continue")
        assert r4["is_end"] is True and r4["result"] == "root2-done"


# ---------------------------------------------------------------------------
# 6. 守卫与向后兼容
# ---------------------------------------------------------------------------


class TestGuardsAndBackwardCompat:
    @staticmethod
    def _strategy_step(flow, context, resume_type, resume_data=None, event_bus=None):
        """策略层单步驱动（绕开 run_distributed 的 FlowErrorException 归一化），
        直接断言守卫的具名类型。"""
        ex = _distributed(event_bus)
        return asyncio.run(DistributedStrategy().execute(
            flow, ex._ctx, ex._runner, ex.callback_manager, None, None,
            saved_context=context, resume_type=resume_type, resume_data=resume_data))

    def test_continue_on_nested_suspension_is_rejected(self):
        """continue/retry 打在嵌套挂起上：#33 守卫的嵌套半边，帧路径入消息。"""
        flow = _root_flow()
        r1 = _distributed().run_distributed(flow, {})
        with pytest.raises(ResumeGuardError) as ei:
            self._strategy_step(flow, r1["context"], "continue")
        assert "nested child/reference call chain" in str(ei.value)
        assert "child1" in str(ei.value)

    def test_stale_event_binding_rejected_for_nested(self):
        """#221 订阅绑定在嵌套路径同样生效（准入判据读最内帧的挂起态）。"""
        bus = RecordingBus()
        flow = _root_flow()
        r1 = _distributed(bus).run_distributed(flow, {})
        with pytest.raises(ResumeGuardError) as ei:
            self._strategy_step(flow, r1["context"], "event",
                                resume_data={"subscription_id": "sub-other",
                                             "event_type": "approval"},
                                event_bus=bus)
        assert "Stale" in str(ei.value)

    def test_legacy_checkpoint_without_frames_resume_unchanged(self):
        """旧 checkpoint（无帧路径）：根流程挂起/恢复行为逐字不变（向后兼容钉）。"""
        legacy_flow = Flow.model_validate({
            "flow_id": "lg", "version": "1", "runtime": "python",
            "nodes": [
                {"type": "start", "id": "start", "next": "a"},
                {"type": "assignment", "id": "a", "result": 1, "next": "w"},
                {"type": "event", "id": "w", "event_type": "legacy", "next": "end"},
                {"type": "end", "id": "end", "output": "legacy-done"},
            ],
        })
        bus = RecordingBus()
        ex = _distributed(bus)
        r1 = ex.run_distributed(legacy_flow, {})
        r2 = ex.run_distributed(legacy_flow, saved_context=r1["context"],
                                resume_type="continue")
        assert r2["id"] == "w" and r2.get("is_suspend") is True
        # 根流程直挂不写帧键。
        assert f"${SUSPEND_FRAMES_KEY}" not in r2["context"]

        sid = next(iter(bus.subscriptions))
        r3 = ex.run_distributed(legacy_flow, saved_context=r2["context"],
                                resume_type="event",
                                resume_data={"subscription_id": sid, "event_type": "legacy"})
        assert r3.get("is_suspend") is not True

        r4 = ex.run_distributed(legacy_flow, saved_context=r3["context"],
                                resume_type="continue")
        assert r4["is_end"] is True and r4["result"] == "legacy-done"

    def test_handcrafted_frameless_checkpoint_still_advances(self):
        """手工构造的旧格式 checkpoint（无 $SUSPEND_FRAMES 键）照常 continue。"""
        legacy_flow = Flow.model_validate({
            "flow_id": "lg2", "version": "1", "runtime": "python",
            "nodes": [
                {"type": "start", "id": "start", "next": "a"},
                {"type": "assignment", "id": "a", "result": 1, "next": "end"},
                {"type": "end", "id": "end", "output": "ok"},
            ],
        })
        legacy_ctx = {
            "$LAST_NODE": "start", "$NODE": {}, "$INPUT": {}, "$ENV": {},
            "EXPRESS_PREFIX": "$",
        }
        r = _distributed().run_distributed(legacy_flow, saved_context=legacy_ctx,
                                           resume_type="continue")
        assert r["id"] == "a"

    def test_cancel_resume_resolves_innermost_and_continues(self):
        """cancel 载体打在最内帧：节点落 cancelled、child 流程续跑至返回。"""
        bus = RecordingBus()
        flow = _root_flow()
        r1 = _distributed(bus).run_distributed(flow, {})
        sid = next(iter(bus.subscriptions))
        r2 = _distributed(bus).run_distributed(
            flow, saved_context=r1["context"], resume_type="cancel")
        # 与根流程 cancel 语义一致：决议即继续——child 续跑到完成并回填父帧。
        assert r2.get("is_suspend") is not True
        assert r2["id"] == "call"
        assert r2["result"] == {"done": True}
        # 最内帧订阅已注销。
        assert sid not in bus.subscriptions
        assert not bus.subscriptions

    def test_per_frame_fresh_budget_pin(self):
        """预算（程式一④）：分布式逐步 per-step 新鲜发放，嵌套恢复的每帧推进
        同样走 execution_deadline_ms——恢复会话在步预算内完成多帧推进。"""
        bus = RecordingBus()
        flow = _root_flow()
        r1 = _distributed(bus).run_distributed(flow, {}, timeout=60000)
        sid = next(iter(bus.subscriptions))
        r2 = _distributed(bus).run_distributed(
            flow, saved_context=r1["context"], resume_type="event",
            resume_data={"subscription_id": sid, "event_type": "approval"},
            timeout=60000)
        # 60s 步预算内完成「决策步 + child 续跑 + 返回步」三段，未触发预算终态。
        assert r2.get("is_suspend") is not True
        assert r2["id"] == "call"


# ---------------------------------------------------------------------------
# 7. 可观测口径（程式 ③.1 的 plaita 对拍面）
# ---------------------------------------------------------------------------


class TestObservability:
    def test_suspend_direction_callbacks_innermost_first(self):
        """挂起方向：最内帧 on_node_suspend + 逐帧 on_flow_suspend（最内→根）。"""
        from plaita.core.callback import CallbackManager

        events = []

        class _Recorder:
            def on_node_suspend(self, flow, node, **kw):
                events.append(("node_suspend", getattr(flow, "flow_id", "?"), node.id))

            def on_flow_suspend(self, flow, **kw):
                events.append(("flow_suspend", getattr(flow, "flow_id", "?")))

            def __getattr__(self, name):
                return lambda *a, **k: None

        flow = _root_flow()
        ex = FlowExecution(callback_manager=CallbackManager([_Recorder()]))
        ex.mode = "distributed"
        r1 = ex.run_distributed(flow, {})
        assert r1.get("is_suspend") is True
        assert ("node_suspend", "child1", "wait") in events
        flows = [e[1] for e in events if e[0] == "flow_suspend"]
        assert flows == ["child1", "root1"]  # 最内→根

    def test_resume_direction_flow_end_per_completed_frame(self):
        """恢复方向：每完成帧发 on_flow_end（最内→根）；根帧由既有机制负责。"""
        from plaita.core.callback import CallbackManager

        events = []

        class _Recorder:
            def on_flow_end(self, flow, result=None, error=None, exception=None, **kw):
                events.append(("flow_end", getattr(flow, "flow_id", "?")))

            def on_node_end(self, flow, node, result=None, error=None, exception=None, **kw):
                if error is None and exception is None:
                    events.append(("node_end", getattr(flow, "flow_id", "?"), node.id))

            def __getattr__(self, name):
                return lambda *a, **k: None

        flow = Flow.model_validate(GRANDCHILD_ROOT)
        bus = RecordingBus()
        manager = CallbackManager([_Recorder()])
        ex = FlowExecution(event_bus=bus, callback_manager=manager)
        ex.mode = "distributed"
        r1 = ex.run_distributed(flow, {})
        events.clear()
        sid = next(iter(bus.subscriptions))
        r2 = FlowExecution(event_bus=bus, callback_manager=CallbackManager([_Recorder()]))
        r2.mode = "distributed"
        r2.run_distributed(flow, saved_context=r1["context"], resume_type="event",
                           resume_data={"subscription_id": sid, "event_type": "deep"})
        flow_ends = [e[1] for e in events if e[0] == "flow_end"]
        assert flow_ends == ["B", "A"]  # 最内→根；根帧 FlowEnd 不在本步
        # 返回步 NodeEnd：B 的 wait 决议、A2 的 callb 回填、根帧 calla 回填。
        node_ends = [(e[1], e[2]) for e in events if e[0] == "node_end"]
        assert ("A", "callb") in node_ends
        assert ("R", "calla") in node_ends


class TestResumeContinuationIntoChild:
    """恢复续跑途中进入 child 节点（NS-M4 对拍 case 09 的回归钉）。

    缺陷形态：_advance_frame_to_end 构造 NodeRunner 未接 node_execution
    facade——内层续跑进入 child 时 ``execution.get_child_execution()``
    拿到裸 ExecutionContext 直接 AttributeError。修复=恢复会话帧推进
    复用 facade（与 executor.py 正典构造同形）。
    """

    def test_resume_continuation_enters_child_and_completes(self):
        bus = RecordingBus()
        flow_doc = {
            "flow_id": "root",
            "nodes": [
                {"type": "start", "id": "s", "next": "evA"},
                {"type": "event", "id": "evA", "event_type": "first", "next": "c2"},
                {"type": "child", "id": "c2", "next": "o", "childFlow": {"nodes": [
                    {"type": "start", "id": "b", "next": "bend"},
                    {"type": "end", "id": "bend", "output": "deep-ok", "resultType": "success"},
                ]}},
                {"type": "end", "id": "o", "output": "$NODE.c2", "resultType": "success"},
            ],
        }
        flow = Flow.model_validate(flow_doc)
        execution = _distributed(bus)
        # step1：根级 evA 挂起（root 路径：resume 只做决策，不续跑）。
        out = execution.run_distributed(flow, {"x": 1})
        assert out["is_suspend"] is True and out["id"] == "evA"
        sid = next(iter(bus.subscriptions))
        # step2：决策步（is_suspend=False，无续跑——plaita 分布式逐步契约）。
        out2 = execution.run_distributed(
            flow, saved_context=out["context"], resume_type="event",
            resume_data={"subscription_id": sid, "event_type": "first", "signal": "go"},
        )
        assert out2["id"] == "evA" and out2["result"]["status"] == "completed"
        # step3：continue 步进入 child c2 并完成内层 → 回填 $NODE[c2] → 续到根 end。
        out3 = execution.run_distributed(
            flow, saved_context=out2["context"], resume_type="continue")
        assert out3["result"] == "deep-ok"

    def test_resume_continuation_re_suspends_in_nested_child(self):
        """恢复续跑途中进入 child，child 内 evB 再挂起（NS-M4 对拍 case 09 的
        回归钉：_advance_frame_to_end 必须接 node_execution facade——漏接时
        内层 child 的 get_child_execution 直接 AttributeError）。"""
        bus = RecordingBus()
        flow_doc = {
            "flow_id": "root",
            "nodes": [
                {"type": "start", "id": "s", "next": "evA"},
                {"type": "event", "id": "evA", "event_type": "first", "next": "c2"},
                {"type": "child", "id": "c2", "next": "o", "childFlow": {"nodes": [
                    {"type": "start", "id": "b", "next": "evB"},
                    {"type": "event", "id": "evB", "event_type": "second", "next": "bend"},
                    {"type": "end", "id": "bend", "output": "$NODE.evB", "resultType": "success"},
                ]}},
                {"type": "end", "id": "o", "output": "$NODE.c2", "resultType": "success"},
            ],
        }
        flow = Flow.model_validate(flow_doc)
        execution = _distributed(bus)
        # step1：根级 evA 挂起。
        out = execution.run_distributed(flow, {"x": 1})
        assert out["is_suspend"] is True and out["id"] == "evA"
        sid1 = next(iter(bus.subscriptions))
        # step2：evA 决策步（根路径，无续跑）。
        out2 = execution.run_distributed(
            flow, saved_context=out["context"], resume_type="event",
            resume_data={"subscription_id": sid1, "event_type": "first", "signal": "1"},
        )
        assert out2["id"] == "evA" and out2["result"]["status"] == "completed"
        # step3：continue 进入 child c2 → evB 再挂起（帧路径 [现场帧, 根帧]）。
        out3 = execution.run_distributed(
            flow, saved_context=out2["context"], resume_type="continue")
        # 逐步输出身份 = 本步执行的节点 c2；内层真位置（evB）由帧路径钉。
        assert out3["is_suspend"] is True and out3["id"] == "c2"
        frames = out3["context"][f"${SUSPEND_FRAMES_KEY}"]
        # 帧 path 语义：现场帧（evB 所在内层 flow）在前、根帧在后。flow_id 为
        # None 是 plaita 方言（内联 childFlow 不合成 id；rs 合成 "c2.childFlow"
        # ——已知方言差异，NS-M4 对拍集 README 记录）。
        assert [(f["node_id"]) for f in frames] == ["evB", "c2"]
        assert frames[-1]["flow_id"] == "root" and frames[-1]["context"] is None
        sid2 = next(iter(k for k in bus.subscriptions if k != sid1))
        # step4：嵌套恢复 evB（沿帧路径：决策步 → 返回步回填 $NODE[c2] → 本步收尾）。
        out4 = execution.run_distributed(
            flow, saved_context=out3["context"], resume_type="event",
            resume_data={"subscription_id": sid2, "event_type": "second", "signal": "2"},
        )
        assert out4["id"] == "c2" and out4["result"]["event_type"] == "second"
        # step5：continue 到根 end，输出 $NODE.c2 = evB 恢复态。
        out5 = execution.run_distributed(
            flow, saved_context=out4["context"], resume_type="continue")
        assert out5["result"]["status"] == "completed"
        assert out5["result"]["event_type"] == "second"
