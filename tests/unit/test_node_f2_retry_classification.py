"""node-F2：NodeRunner 重试按错误类型分类 + 指数退避。

规格（审计文档 P2「node-F2」）：

- 按 ``NodeException.code`` 分类：1001（HTTP 请求生成 = 确定性配置错误）
  **不重试**；1002（发送请求）/ 1003（节点处理，含 429 限流）为瞬态错误，
  可重试；
- 非 ``NodeException`` 的 ``ValueError`` 类配置错误同样不重试；
- 瞬态错误的重试间隔按指数退避：``1s × 2^n``，封顶 30s——旧实现零间隔连打，
  429/限流窗口会被打爆；
- 其余异常（``RuntimeError`` / 未知 NodeException 码）维持历史重试行为。

负控（验收时手工执行，勿提交改动）：

1. 把 ``plaita.core.runner._is_retryable_error`` 还原为 ``return True``（旧
   “一律重试”）：``TestDeterministicErrorsAreNotRetried`` 的三条用例必须变红
   （节点被执行 4 次而不是 1 次）；
2. 把 ``_backoff_sleep`` 还原为空操作：``TestTransientErrorsRetryWithBackoff``
   的延迟断言必须变红（等待序列变成空列表）。
"""
from __future__ import annotations

import contextlib
from typing import ClassVar
from unittest.mock import MagicMock, patch

import pytest

from plaita.core.context import ExecutionContext
from plaita.core.errors import (
    ErrorStrategy,
    NodeException,
    NodeExecutionError,
    RecoverableErrorHandler,
)
from plaita.core.runner import (
    NodeRunner,
    _backoff_sleep,
    _retry_backoff_seconds,
)
from plaita.node.basic import Node


def _handler(retry_times: int, strategy: ErrorStrategy = ErrorStrategy.ABORT):
    return RecoverableErrorHandler.model_validate(
        {"strategy": strategy.value, "retryTimes": retry_times}
    )


def _node(side_effect, *, retry_times: int, strategy: ErrorStrategy = ErrorStrategy.ABORT):
    """最小节点替身：只走 sync ``run`` 路径，不导入真实节点。"""
    node = MagicMock()
    node.id = "node-f2"
    node.name = "node-f2"
    node.timeout = None
    node.timeout_handler = None
    node.source_line = None
    node.error_handler = _handler(retry_times, strategy)
    node.run.side_effect = side_effect
    return node


def _runner() -> NodeRunner:
    return NodeRunner(ExecutionContext())


@contextlib.contextmanager
def _capture_backoff_sleeps():
    """替换真实 ``asyncio.sleep``，记录每次重试前的等待秒数。"""
    delays = []

    async def fake_sleep(delay):
        delays.append(delay)

    with patch("plaita.core.runner._backoff_sleep", new=fake_sleep):
        yield delays


class TestDeterministicErrorsAreNotRetried:
    """确定性错误（1001 / ValueError）配 retryTimes=3 时也只执行一次。"""

    @pytest.mark.asyncio
    async def test_node_exception_1001_runs_once_and_aborts(self):
        node = _node(NodeException(1001, "Failed to parse URL: bad config"), retry_times=3)

        with _capture_backoff_sleeps() as delays:
            with pytest.raises(NodeExecutionError) as ctx:
                await _runner()._execute_with_retry(MagicMock(), node, None)

        assert "bad config" in str(ctx.value)
        assert node.run.call_count == 1, (
            f"1001 是确定性配置错误，retryTimes=3 也不得重试；实际执行 "
            f"{node.run.call_count} 次"
        )
        assert delays == [], f"不重试就不该有任何退避等待，实际 {delays}"

    @pytest.mark.asyncio
    async def test_node_exception_1001_continue_returns_none_without_retry(self):
        node = _node(
            NodeException(1001, "Failed to parse headers"),
            retry_times=3,
            strategy=ErrorStrategy.CONTINUE,
        )

        with _capture_backoff_sleeps() as delays:
            result = await _runner()._execute_with_retry(MagicMock(), node, None)

        assert result is None
        assert node.run.call_count == 1
        assert delays == []

    @pytest.mark.asyncio
    async def test_value_error_config_error_does_not_retry(self):
        """非 NodeException 的 ValueError 同属配置错误，不得重试。"""
        node = _node(ValueError("invalid node parameter"), retry_times=3)

        with _capture_backoff_sleeps() as delays:
            with pytest.raises(NodeExecutionError):
                await _runner()._execute_with_retry(MagicMock(), node, None)

        assert node.run.call_count == 1, (
            f"ValueError 是确定性配置错误，retryTimes=3 也不得重试；实际执行 "
            f"{node.run.call_count} 次"
        )
        assert delays == []


class _AsyncValidationFailNode(Node):
    node_type: ClassVar[str] = "f2_async_validation_fail"
    async_node: ClassVar[bool] = True
    calls: ClassVar[int] = 0

    async def arun(self, execution=None):
        type(self).calls += 1
        return "x"

    def _validate_output(self, result):
        raise ValueError("invalid output")


class _SyncValidationFailNode(Node):
    node_type: ClassVar[str] = "f2_sync_validation_fail"
    calls: ClassVar[int] = 0

    def execute(self, execution=None):
        type(self).calls += 1
        return "x"

    def _validate_output(self, result):
        raise ValueError("invalid output")


class TestOutputValidationKeepsRetryContract:
    """#219 契约：校验钩子抛出的 ValueError 仍走 retryTimes，不被 node-F2 误伤。"""

    @pytest.mark.asyncio
    async def test_async_validation_value_error_retries(self):
        _AsyncValidationFailNode.calls = 0
        node = _AsyncValidationFailNode(
            id="v-async",
            errorHandler={
                "strategy": "continue-with",
                "retryTimes": 2,
                "defaultValue": "fallback",
            },
        )

        with _capture_backoff_sleeps() as delays:
            result = await _runner()._execute_with_retry(MagicMock(), node, None)

        assert result == "fallback"
        assert _AsyncValidationFailNode.calls == 3, (
            f"校验异常必须走 retryTimes=2（执行 3 次），实际 "
            f"{_AsyncValidationFailNode.calls} 次"
        )
        assert delays == [1.0, 2.0]

    @pytest.mark.asyncio
    async def test_sync_validation_value_error_retries(self):
        _SyncValidationFailNode.calls = 0
        node = _SyncValidationFailNode(
            id="v-sync",
            errorHandler={
                "strategy": "continue-with",
                "retryTimes": 2,
                "defaultValue": "fallback",
            },
        )

        with _capture_backoff_sleeps() as delays:
            result = await _runner()._execute_with_retry(MagicMock(), node, None)

        assert result == "fallback"
        assert _SyncValidationFailNode.calls == 3, (
            f"sync 校验异常必须走 retryTimes=2（执行 3 次），实际 "
            f"{_SyncValidationFailNode.calls} 次"
        )
        assert delays == [1.0, 2.0]


class TestTransientErrorsRetryWithBackoff:
    """1002/1003 等瞬态错误重试，且每次重试前按 1s×2^n 退避（封顶 30s）。"""

    @pytest.mark.asyncio
    async def test_node_exception_1002_retries_and_succeeds(self):
        node = _node(
            [NodeException(1002, "connection reset"), NodeException(1002, "connection reset"), "ok"],
            retry_times=3,
        )

        with _capture_backoff_sleeps() as delays:
            result = await _runner()._execute_with_retry(MagicMock(), node, None)

        assert result == "ok"
        assert node.run.call_count == 3
        assert delays == [1.0, 2.0], f"退避应为 1s、2s，实际 {delays}"

    @pytest.mark.asyncio
    async def test_node_exception_1003_rate_limit_retries_with_backoff(self):
        """1003 覆盖上游 429 限流：可重试，但必须退避后再打。"""
        node = _node(
            NodeException(1003, "HTTP 429 Too Many Requests"),
            retry_times=2,
            strategy=ErrorStrategy.CONTINUE,
        )

        with _capture_backoff_sleeps() as delays:
            result = await _runner()._execute_with_retry(MagicMock(), node, None)

        assert result is None
        assert node.run.call_count == 3
        assert delays == [1.0, 2.0]

    @pytest.mark.asyncio
    async def test_backoff_is_capped_at_30_seconds(self):
        node = _node(
            NodeException(1002, "upstream unavailable"),
            retry_times=6,
            strategy=ErrorStrategy.CONTINUE,
        )

        with _capture_backoff_sleeps() as delays:
            await _runner()._execute_with_retry(MagicMock(), node, None)

        assert node.run.call_count == 7
        assert delays == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0], (
            f"退避序列应为 1/2/4/8/16/30s，实际 {delays}"
        )

    def test_backoff_formula(self):
        assert [_retry_backoff_seconds(n) for n in range(6)] == [
            1.0, 2.0, 4.0, 8.0, 16.0, 30.0,
        ]
        assert _retry_backoff_seconds(10_000) == 30.0

    @pytest.mark.asyncio
    async def test_backoff_sleep_real_helper_awaits(self):
        """真实 helper 可被 await（其余用例替换它只为避免真实等待）。"""
        await _backoff_sleep(0)

    def test_http_error_code_constants_are_the_pinned_source(self):
        """core 不能 import node，字面量分类必须与 HTTP 节点常量同值。"""
        from plaita.core import runner as runner_mod
        from plaita.node.http import (
            HTTP_DO_REQUEST_ERROR,
            HTTP_GEN_REQUEST_ERROR,
            HTTP_NODE_EXEC_ERROR,
        )

        assert runner_mod._NON_RETRYABLE_NODE_CODES == frozenset({HTTP_GEN_REQUEST_ERROR})
        assert runner_mod._RETRYABLE_NODE_CODES == frozenset(
            {HTTP_DO_REQUEST_ERROR, HTTP_NODE_EXEC_ERROR}
        )

    @pytest.mark.asyncio
    async def test_plain_runtime_error_keeps_historical_retry(self):
        """非配置类异常维持旧行为：可重试。"""
        node = _node([RuntimeError("boom"), "ok"], retry_times=1)

        with _capture_backoff_sleeps() as delays:
            result = await _runner()._execute_with_retry(MagicMock(), node, None)

        assert result == "ok"
        assert node.run.call_count == 2
        assert delays == [1.0]

    @pytest.mark.asyncio
    async def test_unknown_node_exception_code_keeps_historical_retry(self):
        """未知错误码保守地维持历史重试行为（只把 1001 定为不重试）。"""
        node = _node([NodeException(9999, "future code"), "ok"], retry_times=1)

        with _capture_backoff_sleeps() as delays:
            result = await _runner()._execute_with_retry(MagicMock(), node, None)

        assert result == "ok"
        assert node.run.call_count == 2
        assert delays == [1.0]
