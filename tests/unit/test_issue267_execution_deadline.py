"""plaita#267：执行级 deadline + 重试/退避内取消 + node.timeout 构造期拒绝。

issue #267（P1）：r47 把 ``retryTimes`` 压到 ≤10 之后，重试乘法族另外两个因子
仍无上界——``requestTimeout`` 逐跳重新起算（600s × 21 跳）且 ``Flow.timeout``
缺省时引擎完全无限额。合法边界配置下单次执行可占用共享 worker 38.6h；取消只在
节点入口检查，38.6h 内无效。

本文件钉住四组验收：

1. ``Flow.timeout`` 缺省 → 执行级总预算（默认 1800s，环境变量可调），超时抛具名
   ``ExecutionDeadlineError``；显式 timeout 仍优先，``0`` 仍是零预算；
2. 重试退避计入预算且可被 cancel 在 ≤1s 内打断；
3. ``node.timeout`` 的负值/超大值在**构造期**具名拒绝（合法边界不回归）；
4. ``requestTimeout`` 是跨重定向跳的总预算（旧语义逐跳重新起算）；
5. worker 侧把 ``ExecutionDeadlineError`` 判为不可重试（收尸，不重投）。

负控（还原修复时手工执行，勿提交）：
- 去掉 strategies 的 ``execution_deadline_ms(timeout_ms)`` → 默认预算用例
  「跑满 1s 后成功」而不是 ``ExecutionDeadlineError``；
- 去掉 ``_raise_if_cancel_requested`` / 可取消退避 → cancel 用例在 1s 内不返回；
- 去掉 ``_send_sync_with_budget``（恢复逐跳 ``timeout=self.request_timeout``）→
  总预算用例在 21 跳 × 0.08s ≈ 1.7s 后仍成功。
"""
from __future__ import annotations

import asyncio
import http.server
import logging
import threading
import time
from typing import ClassVar
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from plaita.core.context import ExecutionContext
from plaita.core.deadline import (
    DEFAULT_EXECUTION_DEADLINE_SECONDS,
    EXECUTION_DEADLINE_ENV,
    default_execution_deadline_ms,
    execution_deadline_ms,
)
from plaita.core.errors import (
    ExecutionDeadlineError,
    FlowCancelledException,
    FlowErrorException,
    FlowErrorType,
    FlowTimeoutError,
    NodeException,
    RecoverableErrorHandler,
)
from plaita.core.flow import Flow
from plaita.core.runner import MAX_NODE_TIMEOUT_MS, NodeRunner
from plaita.node.basic import Node


class _SleepNode(Node):
    node_type: ClassVar[str] = "t267_sleep"
    node_name: ClassVar[str] = "t267-sleep"

    seconds: float = 1.0

    def execute(self, execution=None):
        time.sleep(self.seconds)
        return f"slept {self.seconds}s"


def _flow(nodes, timeout="", flow_id="issue267"):
    return Flow(flow_id=flow_id, version="1.0", runtime="python", timeout=timeout, nodes=nodes)


def _sleep_flow(seconds: float, timeout: str = ""):
    return _flow(
        [
            {"id": "s", "type": "start", "next": "n1"},
            _SleepNode(id="n1", next="e", seconds=seconds),
            {"id": "e", "type": "end", "resultType": "success", "output": "$NODE.n1"},
        ],
        timeout=timeout,
    )


# ---------------------------------------------------------------------------
# 1. 执行级 deadline：默认预算 / env 可调 / 显式优先 / 具名错误
# ---------------------------------------------------------------------------

class TestExecutionDeadlineDefault:
    def test_named_error_is_a_flow_timeout(self):
        err = ExecutionDeadlineError("deadline")
        assert isinstance(err, FlowTimeoutError)
        assert err.code == -1
        assert err.error_type is FlowErrorType.FLOW_ERROR

    def test_default_budget_when_flow_timeout_missing(self, monkeypatch):
        monkeypatch.setenv(EXECUTION_DEADLINE_ENV, "0.2")
        flow = _sleep_flow(1.0)  # 节点 1s，远大于 0.2s 默认预算
        started = time.time()
        with pytest.raises(ExecutionDeadlineError) as ctx:
            flow.run()
        elapsed = time.time() - started
        assert elapsed < 0.9, f"执行未被 0.2s deadline 切断（耗时 {elapsed:.2f}s）"
        assert "deadline" in str(ctx.value).lower()
        assert ctx.value.code == -1

    def test_explicit_timeout_still_wins_over_env(self, monkeypatch):
        monkeypatch.setenv(EXECUTION_DEADLINE_ENV, "10")
        flow = _sleep_flow(1.0, timeout="PT0.1S")
        started = time.time()
        with pytest.raises(ExecutionDeadlineError) as ctx:
            flow.run()
        elapsed = time.time() - started
        assert elapsed < 0.6
        assert "100ms" in str(ctx.value), "显式 flow.timeout 必须优先于 env 默认"

    def test_zero_budget_is_still_zero(self, monkeypatch):
        monkeypatch.setenv(EXECUTION_DEADLINE_ENV, "10")
        assert execution_deadline_ms(0) == 0
        monkeypatch.setenv(EXECUTION_DEADLINE_ENV, "0.25")
        assert default_execution_deadline_ms() == 250

    def test_deadline_is_not_swallowed_by_timeout_handler(self, monkeypatch):
        """执行级 deadline 是终态：节点的 timeoutHandler=continue 不得吞掉它。"""
        from plaita.core.errors import ErrorHandler

        monkeypatch.setenv(EXECUTION_DEADLINE_ENV, "0.15")
        flow = _sleep_flow(1.0)
        flow.nodes[1].timeout_handler = ErrorHandler.model_validate(
            {"strategy": "continue"}
        )
        with pytest.raises(ExecutionDeadlineError):
            flow.run()

    def test_invalid_env_falls_back_with_warning(self, monkeypatch, caplog):
        monkeypatch.setenv(EXECUTION_DEADLINE_ENV, "not-a-number")
        with caplog.at_level(logging.WARNING, logger="plaita.core.deadline"):
            assert default_execution_deadline_ms() == int(
                DEFAULT_EXECUTION_DEADLINE_SECONDS * 1000
            )
        assert any("回退默认执行 deadline" in r.message for r in caplog.records)

    @pytest.mark.parametrize("value", ["0", "-5", "inf", "nan"])
    def test_non_positive_or_non_finite_env_falls_back(self, monkeypatch, value):
        monkeypatch.setenv(EXECUTION_DEADLINE_ENV, value)
        assert default_execution_deadline_ms() == int(
            DEFAULT_EXECUTION_DEADLINE_SECONDS * 1000
        )


# ---------------------------------------------------------------------------
# 2. 重试/退避：计入预算 + cancel ≤1s
# ---------------------------------------------------------------------------

def _boom_node(retry_times: int):
    def _slow_failure(execution=None):
        # 50ms：保证「预算耗尽后的下一次尝试」一定能观察到 0 剩余预算，
        # 而不是在 wait_for(0) 前就完成（那会变成普通可重试错误）。
        time.sleep(0.05)
        raise NodeException(1002, "transient upstream")

    node = MagicMock()
    node.id = "boom"
    node.name = "boom"
    node.timeout = None
    node.timeout_handler = None
    node.source_line = None
    node.error_handler = RecoverableErrorHandler.model_validate(
        {"strategy": "abort", "retryTimes": retry_times}
    )
    node.run.side_effect = _slow_failure
    return node


class TestRetryBudgetAndCancel:
    @pytest.mark.asyncio
    async def test_backoff_is_capped_by_execution_deadline(self):
        node = _boom_node(retry_times=10)
        runner = NodeRunner(ExecutionContext())
        started = time.time()
        with pytest.raises(ExecutionDeadlineError):
            await runner._execute_with_retry(MagicMock(), node, 250)
        elapsed = time.time() - started
        assert elapsed < 0.7, f"退避睡过了执行 deadline（{elapsed:.2f}s）"
        assert node.run.call_count == 2, "首次失败 → 退避到预算耗尽后不再尝试"

    @pytest.mark.asyncio
    async def test_cancel_interrupts_backoff_within_one_second(self):
        node = _boom_node(retry_times=10)
        ctx = ExecutionContext()
        runner = NodeRunner(ctx)
        task = asyncio.ensure_future(
            runner._execute_with_retry(MagicMock(), node, None)
        )
        await asyncio.sleep(0.2)  # 首次尝试已失败，正处在 1s 退避中
        assert node.run.call_count == 1
        cancelled_at = time.time()
        ctx.cancel_requested.set()
        with pytest.raises(FlowCancelledException):
            await asyncio.wait_for(task, timeout=3.0)
        # 轮询粒度 0.5s；非可取消退避会睡满 1s（cancel 后仍需 ~0.8s），
        # 0.7s 的界把「退避内打断」与「等退避结束再检查」区分开。
        assert time.time() - cancelled_at < 0.7, "cancel 必须在 ≤1s 内打断退避"
        assert node.run.call_count == 1, "取消后不得再尝试重试"

    @pytest.mark.asyncio
    async def test_long_backoff_helper_is_cancellable(self):
        """直接钉住 helper：5s 退避在 cancel 后 ≤1s 返回（不睡满）。"""
        from plaita.core.runner import _backoff_sleep_cancellable

        ctx = ExecutionContext()
        node = _boom_node(retry_times=0)
        task = asyncio.ensure_future(
            _backoff_sleep_cancellable(5.0, ctx.cancel_requested, node)
        )
        await asyncio.sleep(0.1)
        cancelled_at = time.time()
        ctx.cancel_requested.set()
        with pytest.raises(FlowCancelledException):
            await asyncio.wait_for(task, timeout=3.0)
        assert time.time() - cancelled_at < 0.7, "5s 退避必须被 cancel 当场打断"

    @pytest.mark.asyncio
    async def test_cancel_between_attempts_prevents_another_run(self):
        node = _boom_node(retry_times=10)
        ctx = ExecutionContext()
        runner = NodeRunner(ctx)

        original_sleep = asyncio.sleep

        # 退避开始瞬间置位取消：覆盖「退避刚结束、下一次尝试入口」的窗口。
        async def fake_backoff(delay):
            ctx.cancel_requested.set()
            await original_sleep(0)

        import plaita.core.runner as runner_mod

        original = runner_mod._backoff_sleep
        runner_mod._backoff_sleep = fake_backoff
        try:
            with pytest.raises(FlowCancelledException):
                await runner._execute_with_retry(MagicMock(), node, None)
        finally:
            runner_mod._backoff_sleep = original
        assert node.run.call_count == 1


# ---------------------------------------------------------------------------
# 3. node.timeout 构造期具名拒绝
# ---------------------------------------------------------------------------

class TestNodeTimeoutConstructionRejection:
    @pytest.mark.parametrize(
        "value",
        ["-5", "-0.5", "999999999999999999", str(10**400), "PT-1S"],
    )
    def test_illegal_node_timeout_rejected_at_construction(self, value):
        from plaita.node.http import HTTP

        with pytest.raises(ValidationError) as exc:
            HTTP(id="h", url="https://example.com", timeout=value)
        assert "timeout" in str(exc.value).lower()

    def test_over_24h_rejected_but_boundary_accepted(self):
        from plaita.node.http import HTTP

        at_cap = HTTP(
            id="h-ok", url="https://example.com", timeout=str(MAX_NODE_TIMEOUT_MS)
        )
        assert at_cap.timeout == str(MAX_NODE_TIMEOUT_MS)
        with pytest.raises(ValidationError) as exc:
            HTTP(
                id="h-bad",
                url="https://example.com",
                timeout=str(MAX_NODE_TIMEOUT_MS + 1),
            )
        assert "超过上限" in str(exc.value)

    def test_flow_model_validate_rejects_illegal_node_timeout(self):
        with pytest.raises(ValidationError) as exc:
            Flow.model_validate(
                {
                    "flow_id": "issue267-timeout",
                    "nodes": [
                        {"id": "s", "type": "start", "next": "h"},
                        {
                            "id": "h",
                            "type": "http",
                            "url": "https://example.com",
                            "timeout": "999999999999999999",
                            "next": "e",
                        },
                        {"id": "e", "type": "end"},
                    ],
                }
            )
        assert "timeout" in str(exc.value).lower()

    def test_legal_configs_still_load(self):
        flow = Flow.model_validate(
            {
                "flow_id": "issue267-ok",
                "timeout": "PT30S",
                "nodes": [
                    {"id": "s", "type": "start", "next": "h"},
                    {
                        "id": "h",
                        "type": "http",
                        "url": "https://example.com",
                        "timeout": "PT5S",
                        "requestTimeout": 600,
                        "maxRedirects": 20,
                        "errorHandler": {"strategy": "abort", "retryTimes": 10},
                        "next": "e",
                    },
                    {"id": "e", "type": "end"},
                ],
            }
        )
        http_node = next(n for n in flow.nodes if type(n).__name__ == "HTTP")
        assert http_node.timeout == "PT5S"
        assert http_node.request_timeout == 600


# ---------------------------------------------------------------------------
# 4. requestTimeout = 跨重定向总预算
# ---------------------------------------------------------------------------

class _SlowRedirectHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    delay = 0.08

    def do_GET(self):
        time.sleep(self.delay)
        self.send_response(302)
        self.send_header("Location", "/next")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, *args):
        pass


@pytest.fixture()
def slow_redirect_server():
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _SlowRedirectHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}/start"
    finally:
        server.shutdown()
        server.server_close()


def _http_executor(url: str, **kwargs):
    from plaita.node.http import HttpExecutor

    return HttpExecutor(
        url=url, method="GET", query=None, body=None, headers=None,
        addressing=None, delegate=None, **kwargs
    )


class TestRequestTimeoutTotalBudget:
    def test_sync_redirects_share_one_budget(self, slow_redirect_server):
        ex = _http_executor(slow_redirect_server, request_timeout=0.25, max_redirects=20)
        started = time.time()
        _rsp, err = ex.handle_request(None)
        elapsed = time.time() - started
        assert err is not None, "总预算耗尽必须返回错误，而不是跑完 21 跳"
        assert elapsed < 0.9, f"逐跳重新起算（{elapsed:.2f}s）——总预算未生效"

    @pytest.mark.asyncio
    async def test_async_redirects_share_one_budget(self, slow_redirect_server):
        pytest.importorskip("aiohttp")
        ex = _http_executor(slow_redirect_server, request_timeout=0.25, max_redirects=20)
        started = time.time()
        _rsp, err = await ex.handle_request_async(None)
        elapsed = time.time() - started
        assert err is not None, "总预算耗尽必须返回错误，而不是跑完 21 跳"
        assert elapsed < 0.9, f"逐跳重新起算（{elapsed:.2f}s）——总预算未生效"

    @pytest.mark.parametrize("value", [0, -1, float("inf")])
    def test_request_timeout_must_be_positive_finite(self, value):
        from plaita.node.http import HTTP

        with pytest.raises(ValidationError) as exc:
            HTTP(id="h", url="https://example.com", requestTimeout=value)
        assert "requestTimeout" in str(exc.value)


class TestIssue267ScaledWorstCase:
    """issue 正文 38.6h 配置的等比例缩小复现（不真跑 21×600s×11 次）。

    原配置：requestTimeout=600 × maxRedirects=20 × retryTimes=10，且
    Flow.timeout 缺省。这里等比缩到 requestTimeout=0.2s、deadline=0.45s：
    旧语义（逐跳重置 + 无 deadline）会跑 ~21×0.08s×11 次 ≈ 18s；修复后必须在
    执行 deadline 附近被切断，且抛具名 ExecutionDeadlineError。
    """

    def test_scaled_worst_case_is_cut_at_deadline(self, slow_redirect_server, monkeypatch):
        monkeypatch.setenv(EXECUTION_DEADLINE_ENV, "0.45")
        flow = Flow.model_validate(
            {
                "flow_id": "issue267-38h-scaled",
                "nodes": [
                    {"id": "s", "type": "start", "next": "h"},
                    {
                        "id": "h",
                        "type": "http",
                        "url": slow_redirect_server,
                        "requestTimeout": 0.2,
                        "maxRedirects": 20,
                        "errorHandler": {"strategy": "abort", "retryTimes": 10},
                        "next": "e",
                    },
                    {"id": "e", "type": "end"},
                ],
            }
        )
        started = time.time()
        with pytest.raises(ExecutionDeadlineError):
            flow.run()
        elapsed = time.time() - started
        assert elapsed < 1.5, (
            f"38.6h 配置的等比复现跑出 {elapsed:.2f}s：deadline/总预算未切断"
        )


# ---------------------------------------------------------------------------
# 5. worker 收尸契约：deadline 不重试，可结构化落盘
# ---------------------------------------------------------------------------

class TestWorkerReapsDeadline:
    def test_worker_does_not_retry_deadline_failures(self):
        from plaita.server.flow_worker import _is_retryable_node_failure

        wrapped = FlowErrorException("execution deadline")
        wrapped.__cause__ = ExecutionDeadlineError("deadline exceeded")
        assert _is_retryable_node_failure(wrapped) is False

    def test_worker_error_fields_keep_deadline_identity(self):
        from plaita.server.flow_worker import _error_fields

        wrapped = FlowErrorException("execution deadline")
        wrapped.__cause__ = ExecutionDeadlineError("deadline exceeded")
        fields = _error_fields(wrapped)
        assert fields["code"] == -1
        assert str(fields["error_type"]) in ("flow_error", "FLOW_ERROR")
