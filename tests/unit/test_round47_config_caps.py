"""A2 r22（P1）：构造期上限——retryTimes / maxRedirects / requestTimeout。

editor 可发布 flow，worker 为多租户共享；`retryTimes=1e9`（以及同类的
`maxRedirects` / `requestTimeout` 超大值）足以让单个节点把共享 worker 长期
钉住。修复要求在**构造期**（Pydantic 校验）具名拒绝，使 console 保存/发布
直接 422、运行期加载也失败，而不是等节点执行时才暴露。

负控：还原三处上限校验（只保留测试）→ 本文件 5 条拒绝用例精确红（构造
成功、无异常），合法值用例保持绿。
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

import plaita.core.errors as errors_mod
import plaita.node.http as http_mod
from plaita.core.flow import Flow

# 负控（还原修复时）这些名字不存在——回退到与实现相同的字面量，让负控
# 表现为「DID NOT RAISE」的断言失败，而不是 import/收集错误。
RecoverableErrorHandler = errors_mod.RecoverableErrorHandler
MAX_RETRY_TIMES = getattr(errors_mod, "MAX_RETRY_TIMES", 10)
HTTP = http_mod.HTTP
MAX_REDIRECTS = getattr(http_mod, "MAX_REDIRECTS", 20)
MAX_REQUEST_TIMEOUT_SECONDS = getattr(
    http_mod, "MAX_REQUEST_TIMEOUT_SECONDS", 600.0
)


def test_retry_times_at_cap_is_accepted():
    assert RecoverableErrorHandler(retryTimes=MAX_RETRY_TIMES).retry_times == MAX_RETRY_TIMES
    # 0/普通值不回归
    assert RecoverableErrorHandler().retry_times == 0
    assert RecoverableErrorHandler(retryTimes=3).retry_times == 3


@pytest.mark.parametrize("value", [MAX_RETRY_TIMES + 1, 10**9])
def test_retry_times_over_cap_is_named_rejected(value):
    with pytest.raises(ValidationError) as exc:
        RecoverableErrorHandler(retryTimes=value)
    message = str(exc.value)
    assert "retryTimes" in message
    assert f"> {MAX_RETRY_TIMES}" in message


def test_http_caps_at_boundary_are_accepted():
    node = HTTP(
        id="http-ok",
        url="https://example.com",
        maxRedirects=MAX_REDIRECTS,
        requestTimeout=MAX_REQUEST_TIMEOUT_SECONDS,
    )
    assert node.max_redirects == MAX_REDIRECTS
    assert node.request_timeout == MAX_REQUEST_TIMEOUT_SECONDS
    # 存量默认值不回归
    default = HTTP(id="http-default", url="https://example.com")
    assert default.max_redirects == 5
    assert default.request_timeout == 30.0


@pytest.mark.parametrize(
    "kwargs, needle",
    [
        ({"maxRedirects": MAX_REDIRECTS + 1}, "maxRedirects"),
        ({"maxRedirects": 10**9}, "maxRedirects"),
        ({"requestTimeout": MAX_REQUEST_TIMEOUT_SECONDS + 1}, "requestTimeout"),
        ({"requestTimeout": 10**9}, "requestTimeout"),
    ],
)
def test_http_caps_over_boundary_are_named_rejected(kwargs, needle):
    with pytest.raises(ValidationError) as exc:
        HTTP(id="http-bad", url="https://example.com", **kwargs)
    message = str(exc.value)
    assert needle in message
    assert "超过上限" in message


def _flow(retry_times: int, **http_kwargs) -> dict:
    return {
        "flow_id": "round47-cap",
        "nodes": [
            {"id": "s", "type": "start", "next": "h"},
            {
                "id": "h",
                "type": "http",
                "url": "https://example.com",
                "errorHandler": {"strategy": "abort", "retryTimes": retry_times},
                "next": "e",
                **http_kwargs,
            },
            {"id": "e", "type": "end"},
        ],
    }


def test_flow_model_validate_rejects_over_cap_values():
    """发布/加载入口（Flow.model_validate）必须失败，而非落到运行期。"""
    with pytest.raises(ValidationError) as exc:
        Flow.model_validate(_flow(10**9))
    assert "retryTimes" in str(exc.value)

    with pytest.raises(ValidationError) as exc:
        Flow.model_validate(_flow(0, maxRedirects=10**9))
    assert "maxRedirects" in str(exc.value)

    with pytest.raises(ValidationError) as exc:
        Flow.model_validate(_flow(0, requestTimeout=10**9))
    assert "requestTimeout" in str(exc.value)


def test_flow_model_validate_accepts_legal_boundary_values():
    flow = Flow.model_validate(
        _flow(MAX_RETRY_TIMES, maxRedirects=MAX_REDIRECTS,
              requestTimeout=MAX_REQUEST_TIMEOUT_SECONDS)
    )
    http = next(n for n in flow.nodes if type(n).__name__ == "HTTP")
    assert http.error_handler.retry_times == MAX_RETRY_TIMES
    assert http.max_redirects == MAX_REDIRECTS
    assert http.request_timeout == MAX_REQUEST_TIMEOUT_SECONDS
