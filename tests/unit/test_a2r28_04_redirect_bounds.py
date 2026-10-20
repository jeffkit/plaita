"""A2R28-04：``maxRedirects`` 负值/非有限必须构造期具名拒绝。

r47 只校验了上限（``> MAX_REDIRECTS``），``maxRedirects=-1`` 可以一路构造
成功，而 sync/async 的重定向循环对负值解释不同：

- sync：``while response.is_redirect and hops < -1`` —— 先发一跳，再报
  ``Too many redirects (> -1)``；
- async：``for _hop in range(max_redirects + 1)`` —— 空循环，**一跳都不发**
  就报同一个错误。

同一份配置两条路径的行为分叉。修复把负值/非有限与上限同批放进 Pydantic
字段校验（console 保存/发布 422、运行期加载同样失败），``0`` 保持合法的
「不跟随重定向」。

负控：还原 ``_validate_max_redirects_bounds`` 的负值分支（只留上限）→ 负值
用例精确红（构造成功，DID NOT RAISE）。
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from plaita.core.flow import Flow
from plaita.node.http import HTTP, MAX_REDIRECTS


def _flow(**http_kwargs) -> dict:
    return {
        "flow_id": "a2r28-04",
        "nodes": [
            {"id": "s", "type": "start", "next": "h"},
            {
                "id": "h",
                "type": "http",
                "url": "https://example.com",
                **http_kwargs,
                "next": "e",
            },
            {"id": "e", "type": "end"},
        ],
    }


@pytest.mark.parametrize("value", [-1, -2, -10**9])
def test_negative_max_redirects_is_named_rejected_at_construction(value):
    with pytest.raises(ValidationError) as exc:
        HTTP(id="http-bad", url="https://example.com", maxRedirects=value)
    message = str(exc.value)
    assert "maxRedirects" in message
    assert "不能为负值" in message


@pytest.mark.parametrize("value", [float("inf"), float("-inf"), float("nan"), 5.5, -1.5])
def test_non_finite_or_fractional_max_redirects_is_named_rejected(value):
    with pytest.raises(ValidationError) as exc:
        HTTP(id="http-bad", url="https://example.com", maxRedirects=value)
    message = str(exc.value)
    assert "maxRedirects" in message
    assert "有限整数" in message


@pytest.mark.parametrize("value", [0, 1, 5, MAX_REDIRECTS])
def test_zero_and_legal_values_still_construct(value):
    node = HTTP(id="http-ok", url="https://example.com", maxRedirects=value)
    assert node.max_redirects == value
    # 默认值不回归
    assert HTTP(id="http-default", url="https://example.com").max_redirects == 5


def test_flow_model_validate_rejects_negative_redirects():
    """console 保存/发布与运行期加载都走 Flow.model_validate，必须硬拒绝。"""
    with pytest.raises(ValidationError) as exc:
        Flow.model_validate(_flow(maxRedirects=-1))
    assert "maxRedirects" in str(exc.value)

    ok = Flow.model_validate(_flow(maxRedirects=0))
    http = next(n for n in ok.nodes if type(n).__name__ == "HTTP")
    assert http.max_redirects == 0
