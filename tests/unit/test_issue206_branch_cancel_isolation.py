"""plaita#206：Parallel 分支不得共享父的节点级 cancel_event.

历史缺陷：``ExecutionContext(parent=...)`` 直接复用父的 ``threading.Event``
（同一实例）。Parallel 各分支因此共享节点级信号：A 分支的 sync 节点超时置位
后，B 分支正在跑的 ``code`` 子进程等待循环看到置位即 killpg 整棵进程树——
**兄弟分支被误杀**。

修复后每个子上下文持有自己的 Event，父级 Event 只读可见：
- 分支置位不会传播给父/兄弟（隔离）；
- 父执行超时/``cancel()`` 仍然向下传播（既有语义不丢）；
- 执行级 ``cancel_requested`` 继续沿整条链共享（粘滞取消）。
"""
from __future__ import annotations

import asyncio
import threading
import time
import unittest

import pytest

from plaita.core import types
from plaita.core.context import ExecutionContext
from plaita.core.errors import FlowCancelledException
from plaita.core.executor import FlowExecution
from plaita.core.flow import Flow
from plaita.io import Property
from plaita.node import End, Node, Start
from plaita.node.code import CodeNode
from plaita.node.concurrent import Parallel

BRANCH_B_CODE = """
def run(input):
    import time
    time.sleep(1.0)
    return "code-ok"
"""

LONG_CODE = """
def run(input):
    import time
    time.sleep(8.0)
    return "long"
"""


class _SlowNode(Node):
    node_type = "w35_slow_node"
    node_name = "慢节点"
    seconds: float = 0.0

    def run(self, execution):
        time.sleep(self.seconds)
        return f"slept {self.seconds}"


def _string_flow(flow_id: str, node) -> Flow:
    return Flow(
        flow_id=flow_id, version="1", runtime="python",
        output_type=Property(data_type=types.STRING),
        nodes=[
            Start(id="start", next=node.id),
            node,
            End(id="end", **{"resultType": "success", "output": f"$NODE.{node.id}"}),
        ],
    )


def _timed_out_branch() -> Flow:
    return _string_flow(
        "w35-206-a",
        _SlowNode(id="slow", next="end", seconds=0.6, timeout="200"),
    )


def _code_branch(flow_id: str = "w35-206-b") -> Flow:
    return _string_flow(
        flow_id,
        CodeNode(id="code", next="end", language="python",
                 code=BRANCH_B_CODE, sandbox_backend="subprocess"),
    )


def _long_code_branch(flow_id: str = "w35-206-long") -> Flow:
    return _string_flow(
        flow_id,
        CodeNode(id="code", next="end", language="python",
                 code=LONG_CODE, sandbox_backend="subprocess"),
    )


def _parallel_flow(branch_a: Flow = None, branch_b: Flow = None) -> Flow:
    return Flow(
        flow_id="w35-206-parent", version="1", runtime="python",
        output_type=Property(data_type=types.OBJECT),
        nodes=[
            Start(id="start", next="par"),
            Parallel(
                id="par", name="par", mode="thread", join_branches=["A", "B"],
                branches=[
                    {"name": "A", "flow": branch_a or _timed_out_branch()},
                    {"name": "B", "flow": branch_b or _code_branch()},
                ],
                next="end",
            ),
            End(id="end", **{"resultType": "success", "output": "$NODE.par"}),
        ],
    )


class TestBranchCancelEventIsolation(unittest.TestCase):
    def test_child_has_own_event_but_sees_parent(self):
        parent = ExecutionContext()
        branch_a = ExecutionContext(parent=parent)
        branch_b = ExecutionContext(parent=parent)

        self.assertIsInstance(branch_a.cancel_event, threading.Event)
        self.assertIsNot(branch_a.cancel_event, parent.cancel_event)

        branch_a.cancel_event.set()
        self.assertFalse(parent.cancel_event.is_set(), "分支置位不得传播到父")
        self.assertFalse(branch_b.cancel_event.is_set(), "分支置位不得传播到兄弟")

        parent.cancel_event.set()
        self.assertTrue(branch_a.cancel_event.is_set(), "父级置位仍须向下传播")
        self.assertTrue(branch_b.cancel_event.is_set())

    def test_child_clear_does_not_clear_parent_signal(self):
        parent = ExecutionContext()
        child = ExecutionContext(parent=parent)
        parent.cancel_event.set()
        child.cancel_event.clear()
        self.assertTrue(child.cancel_event.is_set(), "子节点入口的 clear 不得清掉父级信号")
        self.assertTrue(parent.cancel_event.is_set())

    def test_cancel_requested_stays_shared(self):
        parent = ExecutionContext()
        child = ExecutionContext(parent=parent)
        self.assertIs(child.cancel_requested, parent.cancel_requested)


class TestParallelBranchTimeoutDoesNotKillSibling(unittest.IsolatedAsyncioTestCase):
    async def test_sibling_timeout_leaves_code_branch_untouched(self):
        """验收用例：A 的 sync 节点 200ms 超时；B 的 code 节点 sleep 1s 正常完成。"""
        execution = FlowExecution()
        result = await execution.arun_compatible(_parallel_flow(), False)

        self.assertEqual(
            result.get("B"), "code-ok",
            f"B 分支的 code 子进程被兄弟分支的超时误杀: {result.get('B')!r}",
        )
        self.assertIn("__parallel_error__", result.get("A", {}))
        self.assertIn("timeout", result["A"]["__parallel_error__"].lower())
        self.assertFalse(execution.cancel_requested.is_set(),
                         "节点级超时不得升级为执行级取消")

    async def test_execution_cancel_still_reaches_all_branches(self):
        """父执行 cancel()：两个在跑 code 分支都被中止（不再各等 8s）。"""
        flow = _parallel_flow(
            branch_a=_long_code_branch("w35-206-la"),
            branch_b=_long_code_branch("w35-206-lb"),
        )
        execution = FlowExecution()
        task = asyncio.ensure_future(execution.arun_compatible(flow, False))
        await asyncio.sleep(0.3)
        started = time.monotonic()
        execution.cancel()

        with self.assertRaises(FlowCancelledException):
            await task
        self.assertLess(
            time.monotonic() - started, 4.0,
            "cancel() 未传播到分支 ⇒ 在途 code 子进程未被中止",
        )
        self.assertTrue(execution.cancel_requested.is_set())


if __name__ == "__main__":
    unittest.main()
