"""plaita#194：节点自抛 TimeoutError 不得被当成引擎预算超时。

基线缺陷：``_execute_with_retry`` 里 ``except (TimeoutError, asyncio.TimeoutError)``
位于通用异常分支之前，而引擎预算超时与节点自抛超时在 catch 点完全同型
（py3.10+ ``asyncio.TimeoutError`` 即内建 ``TimeoutError``）。结果：

- 节点配 ``retryTimes`` / ``on_error=continue-with``：一次就 abort（静默失效）；
- ``timeout_handler`` 被错误触发（节点超时 ≠ 引擎预算超时）。

修复契约：引擎预算超时用私有类型 ``_NodeBudgetExceeded`` 标记；
节点自抛的 TimeoutError 落普通异常分支（重试 / errorHandler / abort）。
"""
from __future__ import annotations

import asyncio
import time
import unittest
from typing import ClassVar

from plaita.core.context import ExecutionContext
from plaita.core.errors import NodeExecutionError, NodeTimeoutError
from plaita.core.runner import NodeRunner
from plaita.node.basic import Node


class FakeFlow:
    flow_id = "test-194"


def _runner():
    ctx = ExecutionContext()
    ctx.clean()
    return NodeRunner(ctx)


async def _run(node):
    """跑一个节点并返回 (result, branch)；异常原样上抛。"""
    return await _runner().run_node(FakeFlow(), node)


class TestNodeOwnTimeoutIsNotBudgetTimeout(unittest.IsolatedAsyncioTestCase):
    async def test_async_node_timeout_retries_and_uses_error_handler(self):
        """验收用例：节点自抛 asyncio.TimeoutError，retryTimes=2 → 执行 3 次、返回 fallback。"""
        calls = []

        class _TimeoutNode(Node):
            node_type: ClassVar[str] = "t194_async_self"
            node_name: ClassVar[str] = "async-self-timeout"

            async def arun(self, execution=None):
                calls.append(1)
                raise asyncio.TimeoutError("upstream read timed out")

        node = _TimeoutNode(
            id="t1",
            error_handler={
                "strategy": "continue-with",
                "retryTimes": 2,
                "defaultValue": "fallback",
            },
        )
        result, _branch = await _run(node)

        self.assertEqual(result, "fallback")
        self.assertEqual(len(calls), 3, f"retryTimes=2 应执行 3 次，实得 {len(calls)}")

    async def test_sync_node_timeout_retries_and_uses_error_handler(self):
        """同步节点线程里的 TimeoutError（socket.timeout 同族）同样不得被抢走。"""
        calls = []

        class _TimeoutNode(Node):
            node_type: ClassVar[str] = "t194_sync_self"
            node_name: ClassVar[str] = "sync-self-timeout"

            def execute(self, execution=None):
                calls.append(1)
                raise TimeoutError("socket read timed out")

        node = _TimeoutNode(
            id="t1",
            error_handler={
                "strategy": "continue-with",
                "retryTimes": 2,
                "defaultValue": "fallback",
            },
        )
        result, _branch = await _run(node)

        self.assertEqual(result, "fallback")
        self.assertEqual(len(calls), 3)

    async def test_node_timeout_does_not_trigger_timeout_handler(self):
        """timeout_handler 只属于引擎预算：节点自抛超时仍走 errorHandler（abort）。"""
        calls = []

        class _TimeoutNode(Node):
            node_type: ClassVar[str] = "t194_async_self_abort"
            node_name: ClassVar[str] = "async-self-timeout-abort"

            async def arun(self, execution=None):
                calls.append(1)
                raise asyncio.TimeoutError("upstream read timed out")

        node = _TimeoutNode(id="t1", timeout_handler={"strategy": "continue"})
        with self.assertRaises(NodeExecutionError) as ctx:
            await _run(node)
        self.assertIsInstance(ctx.exception.__cause__, TimeoutError)
        self.assertEqual(len(calls), 1, "默认 abort 不重试")

    async def test_node_timeout_abort_is_node_execution_error(self):
        calls = []

        class _TimeoutNode(Node):
            node_type: ClassVar[str] = "t194_async_self_abort2"
            node_name: ClassVar[str] = "async-self-timeout-abort2"

            async def arun(self, execution=None):
                calls.append(1)
                raise asyncio.TimeoutError("boot")

        with self.assertRaises(NodeExecutionError):
            await _run(_TimeoutNode(id="t1"))
        self.assertEqual(len(calls), 1)


class TestEngineBudgetTimeoutStillWorks(unittest.IsolatedAsyncioTestCase):
    async def test_async_budget_timeout_still_raises_node_timeout(self):
        calls = []

        class _SleepNode(Node):
            node_type: ClassVar[str] = "t194_async_sleep"
            node_name: ClassVar[str] = "async-sleep"

            async def arun(self, execution=None):
                calls.append(1)
                await asyncio.sleep(0.2)
                return "ok"

        with self.assertRaises(NodeTimeoutError):
            await _run(_SleepNode(id="t1", timeout="30"))
        self.assertEqual(len(calls), 1)

    async def test_sync_budget_timeout_still_raises_node_timeout(self):
        calls = []

        class _SleepNode(Node):
            node_type: ClassVar[str] = "t194_sync_sleep"
            node_name: ClassVar[str] = "sync-sleep"

            def execute(self, execution=None):
                calls.append(1)
                time.sleep(0.2)
                return "ok"

        with self.assertRaises(NodeTimeoutError):
            await _run(_SleepNode(id="t1", timeout="30"))
        self.assertEqual(len(calls), 1)

    async def test_budget_timeout_handler_continue_applies_once(self):
        calls = []

        class _SleepNode(Node):
            node_type: ClassVar[str] = "t194_async_sleep_continue"
            node_name: ClassVar[str] = "async-sleep-continue"

            async def arun(self, execution=None):
                calls.append(1)
                await asyncio.sleep(0.2)
                return "ok"

        node = _SleepNode(
            id="t1", timeout="30", timeout_handler={"strategy": "continue"}
        )
        result, _branch = await _run(node)

        self.assertIsNone(result, "预算超时 + timeout_handler=continue 应返回 None")
        self.assertEqual(len(calls), 1, "预算超时不得重试（timeout_handler 恰好生效一次）")
