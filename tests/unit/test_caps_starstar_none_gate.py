"""B2 第六十三轮（duo-a-ledger 第九十八轮对抗发现）：caps 门 ``**`` 绕行收口
+ None 口径统一 + ``±`` 字面量对齐。

① ``@flow`` 的 ``**`` 展开：``{k.arg: k.value …}`` 把展开存成 ``None`` 键，
   具名查找全部落空——``HTTP(**{"max_redirects": -1})`` 绕过 ``_caps_literal``
   门静默落默认 5、``**{"retry_times": 2}`` 合法值被静默丢弃（配置丢失）。
   现在字面量 dict 逐键并入具名视图（与具名写法同一条编译路径：caps 字段
   过门、合法值生效），非字面量 dict 具名拒（「caps 字段不支持动态展开」），
   键冲突与 Python TypeError 语义一致具名拒。
② None 口径：sexpr ``:max-redirects None`` / ``:request-timeout None`` /
   ``:retry None`` 此前当省略静默落默认，而 JSON/console（Pydantic 非可选
   数值字段的 int_type）拒——现 sexpr 具名拒；codeflow
   ``ErrorHandler(retry_times=None)``（含 ``**{"retry_times": None}``）此前
   被 ``.get() or .get()`` 吞成默认 0——现键存在即过门具名拒。Builder 工厂
   ``retry_times=None`` 是 Python 默认参语义（None=省略，与不传不可区分），
   作为边界注记钉在本文件。
③ ``+5`` / ``-0``：合法 Python 数值字面量（UnaryOp ±）此前被过严拒绝
   （「必须是数字字面量: UnaryOp」/「不能为负数: -0」）——现折叠为等价值，
   与 JSON/Builder 对同一数值的接受对齐；负值仍具名拒。

负控（/tmp 备份、禁 stash）：还原 ``_nodes.py`` + ``sexpr.py`` 后，①的
``**{-1}`` 三拒 / ``**{合法值}`` 生效 / 动态 dict 拒与 ②③ 对应用例精确红
（pre-fix 实测：``**{-1}`` 三处 ACCEPT 落默认 5/30/0、``**{3}`` 落 5、
``**{2}`` 落 0、动态 ``**`` 整体静默、``+5``/``-0`` 双拒、sexpr None 落默认）。
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from plaita.core.flow import Flow
from plaita.dsl import builder
from plaita.dsl.codeflow import flow_from_source
from plaita.dsl.codeflow._common import _CodeflowError
from plaita.dsl.sexpr import parse_sexpr

# kw 串填充到 ``HTTP(url=…{kw})`` 的 ``…`` 与收尾括号之间；ErrorHandler
# 自带的 ``)`` 由 kw 串自己负责配平。
_HTTP = 'def f(INPUT):\n    r = HTTP(url="https://example.com"{kw})\n    return r\n'


def _http_node(src: str):
    flow = flow_from_source(src)
    return next(n for n in flow.nodes if type(n).__name__ == "HTTP")


# ---------------------------------------------------------------------------
# ① `**` 展开：负值不再绕门、合法值不再丢弃、动态展开具名拒
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("kw,field", [
    (', **{"max_redirects": -1}', "maxRedirects"),
    (', **{"request_timeout": -1}', "requestTimeout"),
    (', on_error=ErrorHandler("abort", **{"retry_times": -5})', "retryTimes"),
])
def test_starstar_negative_caps_named_rejected(kw, field):
    """`**{-1}` 与具名 `-1` 同门：编译期具名拒，不落默认 5/30/0。"""
    with pytest.raises(_CodeflowError) as exc:
        flow_from_source(_HTTP.format(kw=kw))
    message = str(exc.value)
    assert field in message
    assert "不能为负数" in message


@pytest.mark.parametrize("kw,check", [
    (', **{"max_redirects": 3}', lambda h: h.max_redirects == 3),
    (', **{"maxRedirects": 3}', lambda h: h.max_redirects == 3),
    (', **{"request_timeout": 7}', lambda h: h.request_timeout == 7),
    (', on_error=ErrorHandler("abort", **{"retry_times": 2})',
     lambda h: h.error_handler.retry_times == 2),
])
def test_starstar_legal_values_take_effect(kw, check):
    """`**{合法值}` 必须生效而非静默丢弃（pre-fix 落默认 5/30/0）。"""
    assert check(_http_node(_HTTP.format(kw=kw)))


def test_starstar_non_caps_field_takes_effect():
    """字面量 `**` 与具名写法等价：非 caps 字段同样不再被静默丢弃。"""
    node = _http_node(_HTTP.format(kw=', **{"headers": {"X-A": "b"}}'))
    assert node.headers == {"X-A": "b"}


@pytest.mark.parametrize("kw", [
    ', **extra_var',
    ', **build_extra()',
])
def test_starstar_dynamic_dict_named_rejected(kw):
    """非字面量 dict 静态不可校验 → 「caps 字段不支持动态展开」具名拒。"""
    with pytest.raises(_CodeflowError) as exc:
        flow_from_source(_HTTP.format(kw=kw))
    assert "caps 字段不支持动态展开" in str(exc.value)


def test_starstar_dynamic_dict_via_assignment_named_rejected():
    src = ('def f(INPUT):\n'
           '    extra = {"max_redirects": 3}\n'
           '    r = HTTP(url="https://example.com", **extra)\n'
           '    return r\n')
    with pytest.raises(_CodeflowError) as exc:
        flow_from_source(src)
    assert "caps 字段不支持动态展开" in str(exc.value)


def test_starstar_key_conflict_named_rejected():
    """与 Python 语义一致：具名与 ** 展开同键 → TypeError 的具名等价。"""
    src = ('def f(INPUT):\n'
           '    r = HTTP(url="https://a.com", **{"url": "https://b.com"})\n'
           '    return r\n')
    with pytest.raises(_CodeflowError) as exc:
        flow_from_source(src)
    assert "'url'" in str(exc.value)


def test_starstar_non_string_key_named_rejected():
    with pytest.raises(_CodeflowError) as exc:
        flow_from_source(_HTTP.format(kw=', **{1: 2}'))
    assert "字符串字面量" in str(exc.value)


def test_starstar_duplicate_key_across_dicts_named_rejected():
    src = ('def f(INPUT):\n'
           '    r = HTTP(url="https://example.com",'
           ' **{"max_redirects": 3}, **{"max_redirects": 4})\n'
           '    return r\n')
    with pytest.raises(_CodeflowError) as exc:
        flow_from_source(src)
    assert "'max_redirects' 重复" in str(exc.value)


# ---------------------------------------------------------------------------
# ② None 口径：显式 None 在能区分「省略」的入口一律具名拒
# ---------------------------------------------------------------------------

def _flow_dict(**node_extra) -> dict:
    node = {"id": "h", "type": "http", "url": "https://example.com",
            "next": "e"}
    node.update(node_extra)
    return {
        "flow_id": "caps-none",
        "nodes": [
            {"id": "s", "type": "start", "next": "h"},
            node,
            {"id": "e", "type": "end"},
        ],
    }


@pytest.mark.parametrize("extra,field", [
    ({"maxRedirects": None}, "maxRedirects"),
    ({"requestTimeout": None}, "requestTimeout"),
    ({"errorHandler": {"strategy": "abort", "retryTimes": None}},
     "retryTimes"),
])
def test_json_caps_none_named_rejected(extra, field):
    """JSON 锚点（console 同门）：显式 None 走 Pydantic 非可选数值 int_type。"""
    with pytest.raises(ValidationError) as exc:
        Flow.model_validate(_flow_dict(**extra))
    assert field in str(exc.value)


@pytest.mark.parametrize("extra,field", [
    (' :max-redirects None', "max-redirects"),
    (' :request-timeout nil', "request-timeout"),
    (' :on-error (on-error abort :retry null)', "retry"),
])
def test_sexpr_caps_none_named_rejected(extra, field):
    """nil/None/null 三拼写同闸：显式 None 不再当省略落默认。"""
    src = ('(flow f (start :id s -> h)'
           f' (http :id h :method GET :url "https://example.com"{extra} -> e)'
           ' (end :id e))')
    with pytest.raises(SyntaxError) as exc:
        parse_sexpr(src)
    message = str(exc.value)
    assert f":{field}" in message
    assert "None" in message


def test_sexpr_optional_none_still_omitted():
    """可选字段不受闸：模型本就 Optional，None=省略与其他入口一致。"""
    src = ('(flow f (start :id s -> h)'
           ' (event :id h :type tick :subscription-timeout None -> e)'
           ' (end :id e))')
    flow = parse_sexpr(src)
    event = next(n for n in flow.nodes if type(n).__name__ == "EventNode")
    assert event.subscription_timeout is None


@pytest.mark.parametrize("kw", [
    ', on_error=ErrorHandler("abort", retry_times=None)',
    ', on_error=ErrorHandler("abort", **{"retry_times": None})',
])
def test_codeflow_errorhandler_none_retry_times_named_rejected(kw):
    """`.get() or .get()` 吞 None（落默认 0）→ 键存在即过门具名拒。"""
    with pytest.raises(_CodeflowError) as exc:
        flow_from_source(_HTTP.format(kw=kw))
    message = str(exc.value)
    assert "retryTimes" in message
    assert "None" in message


def test_builder_caps_none_boundary():
    """Builder 边界注记：``**extra`` 透传字段（maxRedirects）None 撞 Pydantic
    拒；``error_handler`` 工厂的 ``retry_times=None`` 是 Python 默认参语义
    （None=省略，与不传不可区分），落默认而非拒绝——显式 None 的可表达性
    止于 JSON/sexpr/@flow 三入口。"""
    fb = builder.FlowBuilder(flow_id="caps-none-builder")
    fb.add(builder.start("s", next="h"))
    fb.add(builder.http("h", "GET", "https://example.com",
                        max_redirects=None, next="e"))
    fb.add(builder.end("e"))
    with pytest.raises(ValidationError) as exc:
        fb.build()
    # 双拼写都收的模型在 build 路径回显 snake 字段名
    assert "max_redirects" in str(exc.value)
    assert "retryTimes" not in builder.error_handler("abort", retry_times=None)


# ---------------------------------------------------------------------------
# ③ ± 字面量折叠：+5/-0 与等价值同口径，负值仍具名拒
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("kw,check", [
    (', max_redirects=+5', lambda h: h.max_redirects == 5),
    (', max_redirects=-0', lambda h: h.max_redirects == 0),
    (', request_timeout=+0.5', lambda h: h.request_timeout == 0.5),
    (', on_error=ErrorHandler("abort", retry_times=+2)',
     lambda h: h.error_handler.retry_times == 2),
])
def test_signed_literals_fold_like_json_builder(kw, check):
    """`+5`/`-0` 是合法 Python 数值字面量：折叠为等价值放行（pre-fix 双拒）。"""
    assert check(_http_node(_HTTP.format(kw=kw)))


@pytest.mark.parametrize("kw,field", [
    (', max_redirects=-5', "maxRedirects"),
    (', request_timeout=-1', "requestTimeout"),
])
def test_negative_literals_still_named_rejected(kw, field):
    with pytest.raises(_CodeflowError) as exc:
        flow_from_source(_HTTP.format(kw=kw))
    message = str(exc.value)
    assert field in message
    assert "不能为负数" in message
