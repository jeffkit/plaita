"""A50-03：node-F2 确定性分类在 worker 层丢失（core 说 1 次，worker 仍重投 5 次）。

## 缺陷

node-F2 在 core 把 ``NodeException(code=1001)`` 与非校验 ``ValueError`` 判为
不重试（``core/runner.py::is_retryable_error``）。但 distributed 路径把原始
异常包成 ``NodeExecutionError``（``raise err from e``），worker 的
``_is_retryable_node_failure`` 命中包装层就返回 True，不再看更深的确定性
分类 → 同一错误仍被执行 5 次（首次 + 4 次重投，退避 1/2/4/8s）。

## 修复契约

1. ``is_retryable_error`` 从 core 公开导出，worker 复用同一份分类（单一来源）；
2. ``_is_retryable_node_failure`` 在 ``NodeExecutionError`` 之后继续沿
   ``__cause__`` 检查：确定性 1001 / 非校验 ValueError → 不重试；
3. #219 输出校验 ValueError（实例标记）保持可重试；
4. 1002/1003、未知码、无 cause 的 ``NodeExecutionError`` 维持历史可重试；
5. 更深处的超时/取消仍然优先（不被包装层吞掉）。

## 负控（还原修复后必须精确红）

- 还原 ``_is_retryable_node_failure`` 的「命中 NodeExecutionError → True」：
  ``test_deterministic_1001_executes_once_on_full_chain`` 的
  ``node.run.call_count == 1`` 与 ``_is_retryable_node_failure(...) is False``
  同时变红；``test_worker_terminalizes_...`` 变为 ``NodeExecutionRetryableError``
  且 ``noderetry`` 键被写入。
"""
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.core.context import ExecutionContext
from plaita.core.errors import (
    ErrorStrategy,
    FlowErrorException,
    FlowTimeoutError,
    NodeException,
    NodeExecutionError,
    NodeTimeoutError,
    RecoverableErrorHandler,
    mark_output_validation_retryable,
)
from plaita.core.runner import NodeRunner
from plaita.server.flow_worker import (
    NodeExecutionRetryableError,
    NodeFailureTerminalizedError,
    RedisFlowWorker,
    _is_retryable_node_failure,
)
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

TEST_FLOW = {
    "flow_id": "f1",
    "version": "1",
    "nodes": [
        {"id": "start", "type": "start", "next": "a"},
        {"id": "a", "type": "assignment", "result": {"step": 1}, "next": "end"},
        {"id": "end", "type": "end", "output": "ok"},
    ],
}

CHECKPOINT_CONTEXT = {"$LAST_NODE": "start", "$NODE": {"start": {"status": "success"}}}
NODERETRY_KEY = "plaita:execution:noderetry:exec-1:a"


def _handler(retry_times: int):
    return RecoverableErrorHandler.model_validate(
        {"strategy": ErrorStrategy.ABORT.value, "retryTimes": retry_times}
    )


def _node(side_effect, *, retry_times: int):
    node = MagicMock()
    node.id = "a"
    node.name = "a"
    node.timeout = None
    node.timeout_handler = None
    node.source_line = None
    node.error_handler = _handler(retry_times)
    node.run.side_effect = side_effect
    return node


def _worker() -> tuple[RedisFlowWorker, MemoryExecutionStorage, fakeredis.FakeRedis]:
    fake = fakeredis.FakeRedis(decode_responses=True)
    storage = MemoryExecutionStorage()
    flows = MemoryFlowStorage()
    flows.save_flow(TEST_FLOW)
    worker = RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:r50-f2-worker-parity",
        execution_storage=storage,
        flow_storage=flows,
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
    )
    storage.save_execution_state(
        "exec-1",
        ExecutionState(
            execution_id="exec-1",
            flow_id="f1",
            flow_version="1",
            status="running",
            context=dict(CHECKPOINT_CONTEXT),
        ),
    )
    return worker, storage, fake


def _distributed_chain(original: BaseException) -> FlowErrorException:
    """真实分布式形态：FlowErrorException → NodeExecutionError → 原始异常。"""
    wrapped = NodeExecutionError(f"执行节点a出错了: {original}", node="a")
    wrapped.__cause__ = original
    outer = FlowErrorException(str(wrapped))
    outer.__cause__ = wrapped
    return outer


def _drive_resume(worker, exc) -> BaseException:
    with patch("plaita.server.flow_worker.FlowExecution") as FE:
        inst = MagicMock()
        FE.return_value = inst
        inst.run_distributed.side_effect = exc
        try:
            worker.resume_flow("f1", "exec-1", "continue")
        except BaseException as err:  # noqa: BLE001 — 断言具体类型
            return err
    raise AssertionError("resume_flow 未抛出异常")


class TestFullChainExecutionCount:
    """retry_times=2 的完整链路：core 只执行 1 次，worker 不再把错误重投。"""

    def test_deterministic_1001_executes_once_on_full_chain(self):
        node = _node(NodeException(1001, "Failed to parse URL: bad config"), retry_times=2)
        core_error = None
        try:
            import asyncio

            asyncio.run(
                NodeRunner(ExecutionContext())._execute_with_retry(
                    MagicMock(), node, None
                )
            )
        except NodeExecutionError as exc:
            core_error = exc
        assert core_error is not None

        assert node.run.call_count == 1, "F2：core 对 1001 只执行 1 次（retryTimes=2）"

        chain = _distributed_chain(core_error)
        assert _is_retryable_node_failure(chain) is False, (
            "A50-03：abort 包装层不得把 1001 重新判成可重试"
        )

        worker, storage, fake = _worker()
        outcome = _drive_resume(worker, chain)
        assert isinstance(outcome, NodeFailureTerminalizedError)
        assert storage.load_execution_state("exec-1").status == "error"
        assert fake.get(NODERETRY_KEY) is None, "确定性失败不得消耗可重试预算"

    def test_deterministic_value_error_executes_once_on_full_chain(self):
        node = _node(ValueError("invalid node parameter"), retry_times=2)
        core_error = None
        try:
            import asyncio

            asyncio.run(
                NodeRunner(ExecutionContext())._execute_with_retry(
                    MagicMock(), node, None
                )
            )
        except NodeExecutionError as exc:
            core_error = exc
        assert core_error is not None
        assert node.run.call_count == 1

        chain = _distributed_chain(core_error)
        assert _is_retryable_node_failure(chain) is False

        worker, storage, fake = _worker()
        outcome = _drive_resume(worker, chain)
        assert isinstance(outcome, NodeFailureTerminalizedError)
        assert storage.load_execution_state("exec-1").status == "error"
        assert fake.get(NODERETRY_KEY) is None


class TestWorkerClassificationParity:
    def test_transient_1002_still_retried(self):
        chain = _distributed_chain(NodeException(1002, "connection reset"))
        assert _is_retryable_node_failure(chain) is True

        worker, storage, fake = _worker()
        outcome = _drive_resume(worker, chain)
        assert isinstance(outcome, NodeExecutionRetryableError)
        assert storage.load_execution_state("exec-1").status == "running"
        assert fake.get(NODERETRY_KEY) == "1"

    def test_output_validation_value_error_still_retried(self):
        """#219：输出校验钩子抛出的 ValueError 仍走 retryTimes（不能被 F2 一刀切）。"""
        validation_error = ValueError("output schema mismatch")
        mark_output_validation_retryable(validation_error)
        chain = _distributed_chain(validation_error)

        assert _is_retryable_node_failure(chain) is True

        worker, storage, fake = _worker()
        outcome = _drive_resume(worker, chain)
        assert isinstance(outcome, NodeExecutionRetryableError)
        assert fake.get(NODERETRY_KEY) == "1"

    def test_node_execution_error_without_cause_keeps_historical_retry(self):
        """人工构造/旧形态的 NodeExecutionError（无原始 cause）维持历史可重试。"""
        err = NodeExecutionError("abort without original", node="a")
        outer = FlowErrorException(str(err))
        outer.__cause__ = err
        assert _is_retryable_node_failure(outer) is True

    def test_deeper_timeout_still_wins_inside_wrapper(self):
        """超时被节点函数包进普通异常再 abort 的病态链：确定性优先，不重试。"""
        chain = _distributed_chain(NodeTimeoutError("node a timeout"))
        assert _is_retryable_node_failure(chain) is False

        flow_timeout = _distributed_chain(FlowTimeoutError("flow timeout"))
        assert _is_retryable_node_failure(flow_timeout) is False
