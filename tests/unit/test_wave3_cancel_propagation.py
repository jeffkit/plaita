"""波次③ 引擎层取消传播测试——双 Event 拆分 + 公共 cancel()。

对应 docs/DESIGN-cancellation-and-lease.md §3.2（实现）/ §3.3（默认语义）/
§5 T6（引擎级取消传播）/ T7（并发分支超时互不误伤回归钉）/ §6 波次③验证门。

职责边界钉死：

- ``cancel_event``：节点级协作取消信号——sync 超时置位、**每个节点入口无条件
  clear**（历史语义，红线不变）；code 沙箱等待循环消费。
- ``cancel_requested``：执行级取消意图——``FlowExecution.cancel()`` 置位、
  粘滞、**任何节点入口只读不 clear**；置位时节点入口抛 ``FlowCancelledException``
  拒绝继续，且在重试循环/errorHandler 分发**之前**抛出（不被重试或
  continue 策略吞掉）。
"""

import asyncio
import pickle
import threading
import time
import unittest
from typing import ClassVar, Tuple

from plaita.core import types
from plaita.core.callback import FlowCallback
from plaita.core.context import ExecutionContext
from plaita.core.errors import (
    FlowCancelledException,
    FlowExecutionException,
)
from plaita.core.executor import FlowExecution
from plaita.core.flow import Flow
from plaita.core.runner import NodeRunner
from plaita.io import Property
from plaita.node import End, Node, Start
from plaita.node.concurrent import Parallel, ParallelBranch

# 探针状态：node_id -> 阶段标记。模块级 dict，跨分支/跨线程可见。
_PROBE_STATE = {}


class _CancelWaitNode(Node):
    """长跑节点模拟：阻塞直到 cancel_requested 置位（带超时兜底防挂死）。"""

    node_type: ClassVar[str] = "wave3_cancel_wait"
    node_name: ClassVar[str] = "等待取消"
    probe_key: str = ""
    max_wait: float = 5.0

    def run(self, execution):
        _PROBE_STATE[self.probe_key] = "entered"
        deadline = time.monotonic() + self.max_wait
        while time.monotonic() < deadline:
            if execution.cancel_requested.is_set():
                _PROBE_STATE[self.probe_key] = "saw_cancel"
                return "waited-cancel"
            time.sleep(0.01)
        return "timeout-fallback"


class _CancelTriggerNode(Node):
    """run 中途调用 execution.cancel()——模拟"外部取消恰在本节点运行期到达"。

    连续调用两次，顺带验证 cancel() 幂等。
    """

    node_type: ClassVar[str] = "wave3_cancel_trigger"
    node_name: ClassVar[str] = "触发取消"
    probe_key: str = ""

    def run(self, execution):
        _PROBE_STATE[self.probe_key] = "entered"
        execution.cancel()
        execution.cancel()
        return "cancel-triggered"


class _ProbeNode(Node):
    """记录自己是否被执行（被取消拒绝的节点绝不应跑到这里）。"""

    node_type: ClassVar[str] = "wave3_probe"
    node_name: ClassVar[str] = "探针"
    probe_key: str = ""

    def run(self, execution):
        _PROBE_STATE[self.probe_key] = "ran"
        return "probe-ok"


class _SleepNode(Node):
    node_type: ClassVar[str] = "wave3_sleep"
    node_name: ClassVar[str] = "睡眠"
    seconds: float = 0.0

    def run(self, execution):
        time.sleep(self.seconds)
        return f"slept {self.seconds}"


def _branch_flow(flow_id: str, wait_key: str, after_key: str) -> Flow:
    """分支子流程：Start → 等待取消 → 探针（取消后必须被拒绝） → End。"""
    return Flow(
        flow_id=flow_id,
        version="1",
        runtime="python",
        output_type=Property(data_type=types.STRING),
        nodes=[
            Start(id="start", next="wait"),
            _CancelWaitNode(id="wait", next="after", probe_key=wait_key),
            _ProbeNode(id="after", next="end", probe_key=after_key),
            End(id="end", **{"resultType": "success", "output": "$NODE.after"}),
        ],
    )


def _parallel_parent_flow(mode: str) -> Flow:
    """父流程：Start → Parallel(A, B) → End。"""
    return Flow(
        flow_id="wave3-parent",
        version="1",
        runtime="python",
        output_type=Property(data_type=types.OBJECT),
        nodes=[
            Start(id="start", next="par"),
            Parallel(
                id="par",
                name="par",
                mode=mode,
                join_branches=["A", "B"],
                branches=[
                    {"name": "A", "flow": _branch_flow("w3-a", "a_wait", "a_after")},
                    {"name": "B", "flow": _branch_flow("w3-b", "b_wait", "b_after")},
                ],
                next="end",
            ),
            End(id="end", **{"resultType": "success", "output": "$NODE.par"}),
        ],
    )


class _ParallelResultProbe(FlowCallback):
    """捕获 Parallel 节点的 on_node_end 结果（分支哨兵在此可见）。"""

    def __init__(self):
        self.results = None

    def on_node_end(self, flow, node, result=None, error=None, exception=None, **kwargs):
        if getattr(node, "node_type", "") == "parallel":
            self.results = result


async def _wait_probes(keys: Tuple[str, ...], marker: str = "entered", timeout: float = 5.0) -> bool:
    """轮询等待探针到达指定阶段（不阻塞事件循环）。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if all(_PROBE_STATE.get(k) == marker for k in keys):
            return True
        await asyncio.sleep(0.01)
    return False


class TestT6CancelPropagationAcrossBranches(unittest.IsolatedAsyncioTestCase):
    """T6：父子 flow + 并行分支——A/B 分支长跑中 cancel()，两分支下一节点入口
    拒绝、父流程在下一节点边界终止。"""

    def setUp(self):
        _PROBE_STATE.clear()

    async def _run_parallel_cancel(self, mode):
        flow = _parallel_parent_flow(mode)
        execution = FlowExecution()
        probe = _ParallelResultProbe()
        execution.callback_manager.add_handler(probe)

        task = asyncio.create_task(execution.arun_compatible(flow, False))

        # 两分支都进入长跑节点后再取消——保证取消落在"分支在途"窗口
        self.assertTrue(
            await _wait_probes(("a_wait", "b_wait")),
            "分支未在超时内进入等待节点",
        )

        execution.cancel()
        execution.cancel()  # 幂等：重复调用必须无害

        with self.assertRaises(FlowCancelledException) as cm:
            await task
        self.assertIsInstance(cm.exception, FlowExecutionException)
        self.assertEqual(cm.exception.code, -4)

        # 分支的下一节点入口全部被拒绝（探针绝不能跑）
        self.assertEqual(_PROBE_STATE.get("a_wait"), "saw_cancel")
        self.assertEqual(_PROBE_STATE.get("b_wait"), "saw_cancel")
        self.assertNotIn("a_after", _PROBE_STATE, "A 分支取消后的节点不应执行")
        self.assertNotIn("b_after", _PROBE_STATE, "B 分支取消后的节点不应执行")

        # 节点入口的 cancel_event clear 不得触碰 cancel_requested（粘滞）
        self.assertTrue(execution.cancel_requested.is_set())

        # Parallel 节点结果里两分支均为取消错误哨兵（分支异常经既有
        # _process_future_result/_join 语义留痕）
        results = probe.results
        self.assertIsNotNone(results)
        for branch in ("A", "B"):
            self.assertIn("__parallel_error__", results[branch], f"{branch} 分支应有错误哨兵")
            self.assertIn("Execution cancelled", results[branch]["__parallel_error__"])
            self.assertEqual(results[branch]["__branch__"], branch)

    async def test_t6_coroutine_branches_cancel_propagates(self):
        await self._run_parallel_cancel("coroutine")

    async def test_t6_thread_branches_cancel_propagates(self):
        await self._run_parallel_cancel("thread")


class TestCancelRejectionAtNodeEntry(unittest.TestCase):
    """cancel() 之后节点入口拒绝 + clear/cancel_requested 互不干扰。"""

    def setUp(self):
        _PROBE_STATE.clear()

    def _gate_flow(self, victim_kwargs=None):
        return Flow(
            flow_id="wave3-gate",
            version="1",
            runtime="python",
            output_type=Property(data_type=types.STRING),
            nodes=[
                Start(id="start", next="trigger"),
                _CancelTriggerNode(id="trigger", next="victim", probe_key="trigger"),
                _ProbeNode(id="victim", next="end", probe_key="victim", **(victim_kwargs or {})),
                End(id="end", **{"resultType": "success", "output": "$NODE.victim"}),
            ],
        )

    def test_cancel_rejects_next_node_entry_and_clear_does_not_touch_requested(self):
        execution = FlowExecution()
        with self.assertRaises(FlowCancelledException) as cm:
            execution.run_compatible(self._gate_flow(), False)

        # victim（被拒节点）携带 node/源码行回标信息
        self.assertEqual(cm.exception.node.id, "victim")
        self.assertEqual(cm.exception.code, -4)

        # trigger 跑过（在那里触发的 cancel），victim 被入口拒绝
        self.assertEqual(_PROBE_STATE.get("trigger"), "entered")
        self.assertNotIn("victim", _PROBE_STATE, "取消后的下一节点不应执行")

        # 关键不变量：victim 入口照旧 clear 了 cancel_event（历史语义不变），
        # 而 cancel_requested 不受 clear 影响（粘滞）
        self.assertFalse(
            execution.cancel_event.is_set(),
            "节点入口应照旧 clear cancel_event（超时信号语义不变）",
        )
        self.assertTrue(
            execution.cancel_requested.is_set(),
            "cancel_event 的节点级 clear 不得影响 cancel_requested",
        )

    def test_error_handler_continue_cannot_swallow_cancellation(self):
        """取消在重试循环与 errorHandler 分发之前抛出——continue 策略 + 重试
        都不得把取消吞成"节点无结果继续跑"。"""
        victim_handler = {"strategy": "continue", "retryTimes": 3}
        execution = FlowExecution()
        with self.assertRaises(FlowCancelledException):
            execution.run_compatible(
                self._gate_flow(victim_kwargs={"error_handler": victim_handler}), False,
            )
        self.assertNotIn("victim", _PROBE_STATE)
        self.assertTrue(execution.cancel_requested.is_set())

    def test_cancel_before_any_node_rejects_first_node(self):
        """运行中取消（首个节点运行期内）→ 后续节点全部拒绝，流程以取消异常收尾。"""
        execution = FlowExecution()
        flow = Flow(
            flow_id="wave3-first-node",
            version="1",
            runtime="python",
            output_type=Property(data_type=types.STRING),
            nodes=[
                Start(id="start", next="slow"),
                _CancelWaitNode(id="slow", next="after", probe_key="slow"),
                _ProbeNode(id="after", next="end", probe_key="after"),
                End(id="end", **{"resultType": "success", "output": "$NODE.after"}),
            ],
        )
        box = {}

        def _run():
            try:
                box["result"] = execution.run_compatible(flow, False)
            except BaseException as e:  # noqa: BLE001
                box["error"] = e

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and _PROBE_STATE.get("slow") != "entered":
            time.sleep(0.01)
        self.assertEqual(_PROBE_STATE.get("slow"), "entered")

        execution.cancel()
        t.join(timeout=10)
        self.assertFalse(t.is_alive(), "取消后流程线程应在节点边界退出")

        self.assertIsInstance(box.get("error"), FlowCancelledException)
        self.assertNotIn("after", _PROBE_STATE)
        self.assertTrue(execution.cancel_requested.is_set())


class TestT7ParallelTimeoutNoCrossHarm(unittest.IsolatedAsyncioTestCase):
    """T7 回归钉：并发分支超时互不误伤。

    A 分支节点超时置位的仍是节点级 cancel_event（绝不是 cancel_requested）；
    B 分支下一节点入口照旧 clear cancel_event 后正常执行。
    """

    def setUp(self):
        _PROBE_STATE.clear()

    async def test_branch_timeout_does_not_set_cancel_requested_and_peer_branch_runs(self):
        branch_a = Flow(
            flow_id="w3-t7-a",
            version="1",
            runtime="python",
            output_type=Property(data_type=types.STRING),
            nodes=[
                Start(id="start", next="slow"),
                _SleepNode(id="slow", next="end", seconds=1.2, timeout="200"),
                End(id="end", **{"resultType": "success", "output": "$NODE.slow"}),
            ],
        )
        branch_b = Flow(
            flow_id="w3-t7-b",
            version="1",
            runtime="python",
            output_type=Property(data_type=types.STRING),
            nodes=[
                Start(id="start", next="b1"),
                _SleepNode(id="b1", next="b2", seconds=0.5),
                _ProbeNode(id="b2", next="end", probe_key="t7_b2"),
                End(id="end", **{"resultType": "success", "output": "$NODE.b2"}),
            ],
        )
        flow = Flow(
            flow_id="w3-t7-parent",
            version="1",
            runtime="python",
            output_type=Property(data_type=types.OBJECT),
            nodes=[
                Start(id="start", next="par"),
                Parallel(
                    id="par",
                    name="par",
                    mode="thread",
                    join_branches=["A", "B"],
                    branches=[
                        {"name": "A", "flow": branch_a},
                        {"name": "B", "flow": branch_b},
                    ],
                    next="end",
                ),
                End(id="end", **{"resultType": "success", "output": "$NODE.par"}),
            ],
        )
        execution = FlowExecution()
        probe = _ParallelResultProbe()
        execution.callback_manager.add_handler(probe)

        # 时序：A 在 ~0.2s 超时置位 cancel_event；B 的 b2 在 ~0.5s 入口 clear
        # 掉它并正常执行。流程正常完成。
        result = await execution.arun_compatible(flow, False)

        self.assertIsNotNone(result)
        # 红线一：节点超时只置节点级 cancel_event，绝不置执行级 cancel_requested
        self.assertFalse(
            execution.cancel_requested.is_set(),
            "节点超时不得置位 cancel_requested",
        )
        # 红线二：B 分支的下一节点在 A 超时置位后照常执行（clear 语义未被破坏）
        self.assertEqual(_PROBE_STATE.get("t7_b2"), "ran", "B 分支下一节点应正常执行")
        # A 分支超时以错误哨兵留痕；B 分支正常产出
        results = probe.results
        self.assertIsNotNone(results)
        self.assertIn("__parallel_error__", results["A"])
        self.assertIn("timeout", results["A"]["__parallel_error__"].lower())
        self.assertEqual(results["B"], "probe-ok")


class TestCancelEventMechanics(unittest.TestCase):
    """cancel() 幂等 / 父子共享 / pickle 剥离重建 / clean() 重同步 / 进程模式入口。"""

    def test_cancel_idempotent_and_shared_across_child_chain(self):
        parent = FlowExecution()
        child = parent.get_child_execution()
        # 父子共享同一对 Event 实例（子执行链免费获得传播的机制基础）
        self.assertIs(parent.cancel_event, child.cancel_event)
        self.assertIs(parent.cancel_requested, child.cancel_requested)

        child.cancel()
        child.cancel()
        parent.cancel()
        self.assertTrue(parent.cancel_requested.is_set())
        self.assertTrue(parent.cancel_event.is_set())
        self.assertTrue(child.cancel_requested.is_set())
        self.assertTrue(child.cancel_event.is_set())

    def test_pickle_strips_and_rebuilds_both_events(self):
        ctx = ExecutionContext()
        ctx.cancel_event.set()
        ctx.cancel_requested.set()
        clone = pickle.loads(pickle.dumps(ctx))
        self.assertFalse(clone.cancel_event.is_set(), "跨进程重建的应是全新未触发 Event")
        self.assertFalse(clone.cancel_requested.is_set(), "跨进程重建的应是全新未触发 Event")

    def test_setstate_rebuilds_missing_cancel_requested(self):
        """旧数据/手工构造的 state 缺 cancel_requested 时 __setstate__ 兜底重建。"""
        ctx = ExecutionContext()
        state = {k: v for k, v in ctx.__dict__.items()
                 if k not in ("cancel_event", "cancel_requested")}
        rebuilt = ExecutionContext.__new__(ExecutionContext)
        rebuilt.__setstate__(state)
        self.assertIsNotNone(rebuilt.cancel_event)
        self.assertIsNotNone(rebuilt.cancel_requested)
        self.assertFalse(rebuilt.cancel_requested.is_set())

    def test_clean_resyncs_cancel_requested_same_as_cancel_event(self):
        # 子 context：clean() 后重新同步到 parent 的同一实例
        parent_ctx = ExecutionContext()
        parent_ctx.cancel_requested.set()
        child_ctx = parent_ctx.child()
        self.assertIs(child_ctx.cancel_requested, parent_ctx.cancel_requested)
        child_ctx.clean()
        self.assertIs(child_ctx.cancel_requested, parent_ctx.cancel_requested)

        # 根 context：clean() 换新 Event（与 cancel_event 同型语义）
        root_ctx = ExecutionContext()
        root_ctx.cancel_requested.set()
        root_ctx.clean()
        self.assertFalse(root_ctx.cancel_requested.is_set())

    def test_process_mode_entry_rejects_on_cancel_requested_alone(self):
        """进程模式入口检查（concurrent._build_executor）的 2 行改动：
        cancel_event 已被节点入口 clear、仅剩粘滞 cancel_requested 时，
        仍必须拒绝启动子进程。"""
        execution = FlowExecution()
        node = Parallel(id="p", branches=[], mode="process")

        # 基线：cancel_event 置位（历史路径）仍然拒绝
        execution.cancel_event.set()
        self.assertIsNone(node._build_executor("process", execution))

        # 新增路径：cancel_event 未置位、仅 cancel_requested 置位（即
        # "取消发生在上一个节点边界之后"的现实场景）
        execution.cancel_event.clear()
        execution.cancel_requested.set()
        self.assertIsNone(
            node._build_executor("process", execution),
            "进程模式入口必须同时查 cancel_requested",
        )

    def test_runner_entry_rejects_via_node_runner_directly(self):
        """NodeRunner 层直测：cancel_requested 置位 → run_node 抛
        FlowCancelledException，且 cancel_event 的 clear 逻辑照旧执行。"""
        ctx = ExecutionContext()
        ctx.cancel_event.set()
        ctx.cancel_requested.set()
        runner = NodeRunner(ctx)

        probe = _ProbeNode(id="victim", next=None, probe_key="direct_victim")

        class _Flow:
            pass

        async def _drive():
            return await runner.run_node(_Flow(), probe)

        with self.assertRaises(FlowCancelledException):
            asyncio.run(_drive())
        # 抛出即为拒绝；此处补验 clear 已发生而 requested 未动
        self.assertFalse(ctx.cancel_event.is_set())
        self.assertTrue(ctx.cancel_requested.is_set())


if __name__ == "__main__":
    unittest.main()
