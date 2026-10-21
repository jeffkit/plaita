"""A2R35-01 + A2R28-04：caps 数值字段五入口统一构造期形状校验。

``maxRedirects`` / ``requestTimeout`` / ``retryTimes`` 的布尔/数字字符串此前被
Pydantic 宽松强转（``True``→1/1.0s、``"5"``→5），JSON/Builder/@flow/console 四
入口 ACCEPT、sexpr 拒——契约分叉；``@flow`` 的负字面量（``ast.UnaryOp``）更是
要么编译成 ``$F.sub(0, 1)`` 后以 ``int_parsing`` 的面目失败（非具名边界），
要么在 ErrorHandler 里被**静默丢弃**（``retry_times=-5`` 等价于没写）。

修复以现有 sexpr 严格式为锚统一五入口：bool/str 一律具名拒、负数具名拒、
整数字段非有限/带小数具名拒。**B2R27-01 口径变更**：``retryTimes`` 负数此前
构造期接受、运行期 ``max(0, ...)`` 静默钳 0（无 WARNING），现构造期具名拒绝。

负控（/tmp 备份、禁 stash）：还原三处 Pydantic 形状/下界校验与 codeflow
``_caps_literal`` → 本文件拒绝用例按入口精确红；恢复后全绿。基线矩阵
（五入口 × 三字段 × {true,"5",5,-1,600.5}）见 duo-b2-ledger 第六十一轮。
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

import plaita.core.errors as errors_mod
import plaita.node.http as http_mod
from plaita.core.flow import Flow
from plaita.dsl import builder
from plaita.dsl.codeflow import flow_from_source
from plaita.dsl.codeflow._common import _CodeflowError
from plaita.dsl.sexpr import parse_sexpr
from plaita.node.http import HTTP

# 负控（还原修复时）这些名字不存在——回退到与实现相同的字面量，让负控
# 表现为「DID NOT RAISE」的断言失败，而不是 import/收集错误。
MAX_REDIRECTS = getattr(http_mod, "MAX_REDIRECTS", 20)
MAX_REQUEST_TIMEOUT_SECONDS = getattr(http_mod, "MAX_REQUEST_TIMEOUT_SECONDS", 600.0)
MAX_RETRY_TIMES = getattr(errors_mod, "MAX_RETRY_TIMES", 10)

FIELDS = ("maxRedirects", "requestTimeout", "retryTimes")
REJECT_VALUES = (True, "5", -1, 600.5)
ENTRIES = ("json", "builder", "codeflow", "sexpr")


# ---------------------------------------------------------------------------
# 五入口驱动（与 /tmp/b2r61-probe/matrix.py 同构）
# ---------------------------------------------------------------------------

def _flow_dict(http=None, retry=None) -> dict:
    node = {"id": "h", "type": "http", "url": "https://example.com", "next": "e"}
    if http:
        node.update(http)
    if retry is not None:
        node["errorHandler"] = {"strategy": "abort", "retryTimes": retry}
    return {
        "flow_id": "caps-shape",
        "nodes": [
            {"id": "s", "type": "start", "next": "h"},
            node,
            {"id": "e", "type": "end"},
        ],
    }


def _entry_json(http=None, retry=None) -> Flow:
    return Flow.model_validate(_flow_dict(http, retry))


def _entry_builder(http=None, retry=None) -> Flow:
    fb = builder.FlowBuilder(flow_id="caps-shape")
    fb.add(builder.start("s", next="h"))
    kw = dict(http or {})
    if retry is not None:
        kw["error_handler"] = builder.error_handler("abort", retry_times=retry)
    fb.add(builder.http("h", "GET", "https://example.com", next="e", **kw))
    fb.add(builder.end("e"))
    return fb.build()


_CODEFLOW_TMPL = (
    "def caps_shape(INPUT):\n"
    "    resp = HTTP(url=\"https://example.com\"{http_kw}{eh_kw})\n"
    "    return resp\n"
)

_SEXPR_TMPL = (
    '(flow f (start :id s -> h) '
    '(http :id h :method GET :url "https://example.com"{extra} -> e) '
    '(end :id e))'
)

_SEXPR_KW = {"maxRedirects": "max-redirects", "requestTimeout": "request-timeout"}


def _sexpr_lit(value) -> str:
    if isinstance(value, str):
        return f'"{value}"'
    if value is True:
        return "true"
    return str(value)


def _entry_codeflow(http=None, retry=None) -> Flow:
    http_kw = ""
    if http:
        key, value = next(iter(http.items()))
        snake = {"maxRedirects": "max_redirects",
                 "requestTimeout": "request_timeout"}[key]
        lit = repr(value) if isinstance(value, str) else (
            "True" if value is True else repr(value))
        http_kw = f", {snake}={lit}"
    eh_kw = ""
    if retry is not None:
        eh_kw = f', on_error=ErrorHandler("abort", retry_times={retry!r})'
    # flow_from_source 的 flow_id 是「按函数名选主流程」，函数名 caps_shape
    # 不传 flow_id 让编译器自选唯一定义。
    return flow_from_source(_CODEFLOW_TMPL.format(http_kw=http_kw, eh_kw=eh_kw))


def _entry_sexpr(http=None, retry=None) -> Flow:
    extra = ""
    if http:
        key, value = next(iter(http.items()))
        extra = f" :{_SEXPR_KW[key]} {_sexpr_lit(value)}"
    if retry is not None:
        extra += f" :on-error (on-error abort :retry {_sexpr_lit(retry)})"
    return parse_sexpr(_SEXPR_TMPL.format(extra=extra))


_ENTRIES = {
    "json": _entry_json,
    "builder": _entry_builder,
    "codeflow": _entry_codeflow,
    "sexpr": _entry_sexpr,
}


def _call(entry, field, value):
    if field == "retryTimes":
        return _ENTRIES[entry](retry=value)
    return _ENTRIES[entry](http={field: value})


# ---------------------------------------------------------------------------
# 全表矩阵：五入口 × 三字段 × {true, "5", 5, -1, 600.5}
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("entry", ENTRIES)
@pytest.mark.parametrize("field", FIELDS)
@pytest.mark.parametrize("value", REJECT_VALUES)
def test_matrix_rejects_loose_coercion(entry, field, value):
    """bool/数字字符串/负数/带小数在四个在仓入口全部具名拒绝。

    console 是第五入口：保存/发布走 ``Flow.model_validate``
    （plaita-console/backend/services/validation.py），与本表的 json 行同门，
    422 行为由 console 侧 test_a2r35_01_caps_shape_422.py 覆盖。
    """
    with pytest.raises((ValidationError, _CodeflowError, SyntaxError)) as exc:
        _call(entry, field, value)
    message = str(exc.value)
    # 具名：Pydantic/codeflow 报 camelCase 字段名，sexpr 读侧报 :kebab-case
    snake = {"maxRedirects": "max-redirects",
             "requestTimeout": "request-timeout"}.get(field, field)
    assert field in message or snake in message, message


@pytest.mark.parametrize("entry", ENTRIES)
def test_matrix_accepts_value_five(entry):
    for field in FIELDS:
        flow = _call(entry, field, 5)
        http = next(n for n in flow.nodes if type(n).__name__ == "HTTP")
        if field == "maxRedirects":
            assert http.max_redirects == 5
        elif field == "requestTimeout":
            assert http.request_timeout == 5
        else:
            assert http.error_handler.retry_times == 5


# ---------------------------------------------------------------------------
# 允许域回归（合法形状不因收紧而误伤）
# ---------------------------------------------------------------------------

def test_legal_shapes_still_construct():
    node = HTTP(id="ok", url="https://example.com",
                maxRedirects=0, requestTimeout=0.5)
    assert node.max_redirects == 0  # 0 = 不跟随重定向（r56 契约不回退）
    assert node.request_timeout == 0.5  # 小数秒合法（sexpr _positive_seconds 同）
    assert HTTP(id="ok2", url="https://example.com",
                maxRedirects=5.0).max_redirects == 5  # 整数值 float 归一为 int
    assert HTTP(id="ok3", url="https://example.com",
                requestTimeout=MAX_REQUEST_TIMEOUT_SECONDS)


def test_retry_times_legal_domain_and_bounds():
    eh = errors_mod.RecoverableErrorHandler
    assert eh(retryTimes=0).retry_times == 0
    assert eh(retryTimes=MAX_RETRY_TIMES).retry_times == MAX_RETRY_TIMES


# ---------------------------------------------------------------------------
# 具名文案（字段名在前，用户可读）
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("kwargs, needle", [
    ({"maxRedirects": True}, "不能是布尔值"),
    ({"maxRedirects": "5"}, "不能是字符串"),
    ({"requestTimeout": True}, "不能是布尔值"),
    ({"requestTimeout": "600"}, "不能是字符串"),
])
def test_http_shape_messages_are_named(kwargs, needle):
    with pytest.raises(ValidationError) as exc:
        HTTP(id="bad", url="https://example.com", **kwargs)
    message = str(exc.value)
    assert list(kwargs)[0] in message
    assert needle in message


@pytest.mark.parametrize("kwargs, needle", [
    ({"retryTimes": True}, "不能是布尔值"),
    ({"retryTimes": "5"}, "不能是字符串"),
    ({"retryTimes": -1}, "不能为负值"),
    ({"retryTimes": -5}, "不能为负值"),
])
def test_retry_times_messages_are_named(kwargs, needle):
    with pytest.raises(ValidationError) as exc:
        errors_mod.RecoverableErrorHandler(**kwargs)
    message = str(exc.value)
    assert "retryTimes" in message
    assert needle in message


# ---------------------------------------------------------------------------
# A2R28-04：@flow 负字面量与非 Constant 不再「$F.sub 面目 / 静默丢弃」
# ---------------------------------------------------------------------------

def test_codeflow_negative_literal_is_named_rejected_not_int_parsing():
    with pytest.raises(_CodeflowError) as exc:
        _entry_codeflow(http={"maxRedirects": -1})
    message = str(exc.value)
    assert "maxRedirects" in message
    assert "不能为负数" in message
    # 不再落入 Pydantic int_parsing（input='$F.sub(0, 1)'）的非具名边界
    assert "int_parsing" not in message


@pytest.mark.parametrize("retry_value", [-1, -5])
def test_codeflow_retry_negative_no_longer_silently_dropped(retry_value):
    """此前 UnaryOp 非 Constant 被静默丢弃（IR 无 retryTimes，等价 0）。"""
    with pytest.raises(_CodeflowError) as exc:
        _entry_codeflow(retry=retry_value)
    assert "retryTimes" in str(exc.value)
    assert "不能为负数" in str(exc.value)


def test_codeflow_retry_literal_still_reaches_ir():
    flow = _entry_codeflow(retry=2)
    http = next(n for n in flow.nodes if type(n).__name__ == "HTTP")
    assert http.error_handler.retry_times == 2


def test_codeflow_expression_value_is_named_rejected():
    """变量/表达式此前会被编译成 ``$F.*`` 字符串再撞 int_parsing。"""
    src = (
        "def caps_shape(INPUT):\n"
        "    resp = HTTP(url=\"https://example.com\", max_redirects=INPUT.n)\n"
        "    return resp\n"
    )
    with pytest.raises(_CodeflowError) as exc:
        flow_from_source(src)
    assert "maxRedirects" in str(exc.value)
    assert "数字字面量" in str(exc.value)
