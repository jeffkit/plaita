"""plaita#219：``arun`` 路径必须与 ``execute`` 路径同样执行 ``_validate_output``。

历史实现里 ``NodeRunner._run_async_node`` 直接 ``await node.arun(...)``，
输出校验只在 ``Node.run``（sync 路径）里发生 ⇒ 同一个节点逻辑写成 ``arun``
可静默产出非法输出，下游按错误形状消费。
"""
from __future__ import annotations

import asyncio
import unittest
from typing import ClassVar

from plaita.core.context import ExecutionContext
from plaita.core.errors import NodeExecutionError
from plaita.core.runner import NodeRunner
from plaita.node.basic import Node


class _Flow:
    flow_id = "test219"


def _runner() -> NodeRunner:
    ctx = ExecutionContext()
    ctx.clean()
    return NodeRunner(ctx)


class _Rejecting:
    """共享的 ``_validate_output``：恒抛固定文案，供断言精确红文案。"""

    def _validate_output(self, result):
        raise ValueError("r7 invalid output")


class _BadSync(_Rejecting, Node):
    node_type: ClassVar[str] = "test219_bad_sync"

    def execute(self, execution=None):
        return "x"


class _BadAsync(_Rejecting, Node):
    node_type: ClassVar[str] = "test219_bad_async"

    async def arun(self, execution=None):
        return "x"


class _CleanAsync(Node):
    node_type: ClassVar[str] = "test219_clean_async"

    async def arun(self, execution=None):
        return {"ok": True}


class _RetryingAsync(_Rejecting, Node):
    node_type: ClassVar[str] = "test219_retrying_async"

    async def arun(self, execution=None):
        _RETRY_CALLS.append(1)
        return "x"


_RETRY_CALLS: list = []


class TestAsyncOutputValidation(unittest.IsolatedAsyncioTestCase):
    async def test_execute_path_still_validates(self):
        """对照：sync 路径的既有行为不变。"""
        with self.assertRaises(NodeExecutionError) as ctx:
            await _runner().run_node(_Flow(), _BadSync(id="v"))
        self.assertIn("ValueError: r7 invalid output", str(ctx.exception))

    async def test_arun_path_validates_output(self):
        with self.assertRaises(NodeExecutionError) as ctx:
            await _runner().run_node(_Flow(), _BadAsync(id="v"))
        self.assertIn("ValueError: r7 invalid output", str(ctx.exception))

    async def test_arun_path_with_budget_also_validates(self):
        """有超时预算的 async 分支（wait_for 正常返回）同样校验。"""
        with self.assertRaises(NodeExecutionError):
            await _runner().run_node(_Flow(), _BadAsync(id="v"), max_timeout_ms=5000)

    async def test_node_without_override_is_unchanged(self):
        result, _branch = await _runner().run_node(_Flow(), _CleanAsync(id="ok"))
        self.assertEqual(result, {"ok": True})

    async def test_validation_error_goes_through_retry_policy(self):
        """校验异常按普通节点异常处理：retryTimes=2 ⇒ 执行 3 次后走 fallback。"""
        _RETRY_CALLS.clear()
        node = _RetryingAsync(
            id="r",
            error_handler={
                "strategy": "continue-with",
                "retryTimes": 2,
                "defaultValue": "fallback",
            },
        )
        result, _branch = await _runner().run_node(_Flow(), node)
        self.assertEqual(result, "fallback")
        self.assertEqual(len(_RETRY_CALLS), 3,
                         f"retryTimes=2 应执行 3 次，实得 {len(_RETRY_CALLS)}")

    async def test_node_own_timeout_error_still_attributed_to_node(self):
        """相邻守卫（#194）：节点自己抛的 TimeoutError 不被当成预算超时。"""

        class _TimeoutAsync(Node):
            node_type: ClassVar[str] = "test219_timeout_async"

            async def arun(self, execution=None):
                raise asyncio.TimeoutError("upstream read timed out")

        node = _TimeoutAsync(
            id="t",
            error_handler={"strategy": "continue-with", "defaultValue": "fallback"},
        )
        result, _branch = await _runner().run_node(_Flow(), node)
        self.assertEqual(result, "fallback")


if __name__ == "__main__":
    unittest.main()
