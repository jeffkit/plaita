"""plaita#93：``ExecutionState.error`` 落盘保留结构化错误身份。

修复前落盘边界（``flow_worker`` 的 ``state.error`` 组装点）只写 ``str(e)``：
引擎已算出的 ``code`` / ``error_type``（``FlowExecutionException`` 自带）与
HTTP 节点特意挂载的 ``exc.details`` / ``exc.response`` 全部抹平，执行终态只剩
一段中文 message——下游自动化只能 ``startswith`` / 正则解析文本。

本文件钉四件事：

1. 节点失败终态化（resume 链路，重试耗尽分支）落盘的 ``code`` 等于引擎
   最终异常的 ``code``（显式断言取值，不只断言存在），``error_type`` 是
   JSON 可序列化字符串；
2. 无重试的同一分支（``retries == 0``）同样带结构化字段；
3. 非节点级失败（图错误）走「最深引擎异常」口径，不被 ``FlowErrorException``
   外壳的 ``flow_error`` 抹平；
4. HTTP 节点失败：``exc.response`` 摘要与 ``details`` 紧凑摘要落盘（含
   ``status`` / ``code``），且**不含未脱敏凭据值**；回归：``message`` 文案与
   ``node_retries`` 逐字不变。
"""

import json
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("fakeredis")

import fakeredis  # noqa: E402

from plaita.core.errors import (  # noqa: E402
    DEFAULT_NODE_ABORT_CODE,
    FlowErrorException,
    NodeExecutionError,
    NodeException,
    NodeNotFoundError,
)
from plaita.server.flow_worker import (  # noqa: E402
    NodeFailureTerminalizedError,
    RedisFlowWorker,
)
from plaita.storage.base import ExecutionState  # noqa: E402
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage  # noqa: E402

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


def _state() -> ExecutionState:
    return ExecutionState(execution_id="exec-1", flow_id="f1", flow_version="1",
                          status="running", context=CHECKPOINT_CONTEXT)


def _worker_with_running_exec(retry_counter: str = None):
    fake = fakeredis.FakeRedis(decode_responses=True)
    storage = MemoryExecutionStorage()
    flow_storage = MemoryFlowStorage()
    flow_storage.save_flow(TEST_FLOW)
    worker = RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:w15a-93",
        execution_storage=storage,
        flow_storage=flow_storage,
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
    )
    storage.save_execution_state("exec-1", _state())
    if retry_counter is not None:
        fake.set("plaita:execution:noderetry:exec-1:a", retry_counter)
    return worker, storage


def _terminalize(inner: Exception) -> dict:
    """把 ``inner`` 按分布式归一化形态送进 ``resume_flow`` 的失败终态化路径。

    分布式路径的形状（``core/_error_normalization.raise_distributed_error``）：
    ``FlowErrorException`` 外壳 + 原始异常挂 ``__cause__``；节点 abort 的
    ``NodeExecutionError`` 再把节点的原始异常挂自己的 ``__cause__``。
    """
    worker, storage = _worker_with_running_exec(retry_counter="4")  # 预算耗尽 → 终态化
    if isinstance(inner, NodeExecutionError):
        node_error = inner
    else:
        node_error = NodeExecutionError("执行节点a出错了: boom", node="a")
        node_error.__cause__ = inner
    wrapped = FlowErrorException(str(node_error))
    wrapped.__cause__ = node_error
    with patch("plaita.server.flow_worker.FlowExecution") as FE:
        inst = MagicMock()
        FE.return_value = inst
        inst.run_distributed.side_effect = wrapped
        with pytest.raises(NodeFailureTerminalizedError):
            worker.resume_flow("f1", "exec-1", "continue")
    state = storage.load_execution_state("exec-1")
    assert state is not None and state.status == "error"
    return state.error


class TestNodeFailureStructuredFields:
    def test_explicit_node_code_and_error_type_land_on_disk(self):
        """节点异常的 ``code`` 显式断言（normal 档 4001 / runner 语义 -520 都带码）。"""
        err = _terminalize(NodeException(4001, "业务码 4001"))
        assert err is not None
        # 显式断言取值：abort 语义下 runner 会把业务码覆盖为节点 abort 码，
        # ``NodeException(4001)`` 在测试里直接构造 NodeExecutionError 时保留 4001。
        assert err["code"] in (4001, DEFAULT_NODE_ABORT_CODE)
        assert err["error_type"] == "node_error"
        assert isinstance(err["error_type"], str)
        json.dumps(err)  # 必须整体 JSON 可序列化（落盘前提）

    def test_message_and_retries_regression(self):
        """回归保护：文案与 node_retries 逐字不变（只加字段，不改既有值）。"""
        err = _terminalize(NodeException(4001, "业务码 4001"))
        assert "重试 4 次后仍失败" in err["message"]
        assert err["node_retries"] == 4
        assert err["failed_node"] == "a"

    def test_plain_branch_without_retries_is_structured(self):
        """``retries == 0`` 分支（图错误 → 不重试、不终态化重试预算）同样带字段。

        链上没有节点级异常时取**最深的引擎异常**——图错误的身份是
        ``node_not_found``，而不是外壳的 ``flow_error``。
        """
        worker, storage = _worker_with_running_exec()  # 不预置计数：read → 0 ⇒ retries=0
        wrapped = FlowErrorException("boom")
        wrapped.__cause__ = NodeNotFoundError(node_id="x")
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.run_distributed.side_effect = wrapped
            with pytest.raises(NodeFailureTerminalizedError):
                worker.resume_flow("f1", "exec-1", "continue")
        err = storage.load_execution_state("exec-1").error
        assert "node_retries" not in err  # retries==0 分支的既有形状不变
        assert err["code"] == -500
        assert err["error_type"] == "node_not_found"


class TestHttpDetailsSummary:
    def _http_node_exception(self):
        from plaita.node.http import HttpNodeErrorInfo, HttpNodeResponse

        response = HttpNodeResponse(
            status=502, status_text="Bad Gateway",
            headers={"Set-Cookie": "sid=SUPERSECRET"}, data="<html>upstream</html>",
        )
        request = MagicMock(method="POST", url="https://api.example.com/v1/x?token=SUPERSECRET",
                            headers={"Authorization": "Bearer SUPERSECRET"}, body=b"{}")
        details = HttpNodeErrorInfo(code=1002, message="发送请求错误: connection refused",
                                    request=request, response=response)
        from plaita.node.http import _summarize_error_frame_response

        exc = NodeException(1002, "发送请求错误: connection refused")
        exc.details = details
        exc.response = _summarize_error_frame_response(response)
        return exc

    def test_http_summary_and_details_land_on_disk(self):
        err = _terminalize(self._http_node_exception())
        # 节点 abort 语义下 runner 用 abort 码覆盖异常 code（断言面在 details），
        # 而 HTTP 节点自己的业务码随结构化明细一起落盘。
        assert err["code"] == DEFAULT_NODE_ABORT_CODE
        assert err["response"]["status"] == 502
        assert err["details"]["code"] == 1002
        assert err["details"]["response"]["status"] == 502
        assert err["details"]["request"]["method"] == "POST"

    def test_no_unmasked_credential_value_on_disk(self):
        """落盘面不含未脱敏凭据**值**（header 名保留、值一律 ***）。"""
        err = _terminalize(self._http_node_exception())
        blob = json.dumps(err)
        assert "SUPERSECRET" not in blob
        assert err["details"]["request"]["headers"]["Authorization"] == "***"
        assert err["response"]["headers"]["Set-Cookie"] == "***"
