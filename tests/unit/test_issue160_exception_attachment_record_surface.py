"""plaita#160 —— ``exc.details``（``HttpNodeErrorInfo``）的记录/序列化面不得带请求凭据。

``wrap_http_node_err`` 把 ``HttpNodeErrorInfo(message, request=rsp.raw_request, response=…)``
挂到 ``NodeException.details``；``request.url`` 与 response 的 headers/body 都是**逐字**
快照。今天没有运行时代码读 ``.details``（只有 C2 契约测试读字段），所以不是活体泄露——
但「安全不许依赖现在没人读」：任何 ``repr`` / ``traceback`` / JSON shipper / 结构化日志
一接上就是新 sink。

**本单选型（验收的二选一）**：``details`` **显式声明为非日志面**，记录面 fail-closed——
:class:`HttpRequestInfo` / :class:`HttpNodeResponse` / :class:`HttpNodeErrorInfo` 都有
**掩码 ``__repr__``**，另有显式掩码视图 :meth:`HttpNodeErrorInfo.redacted`（JSON 安全）
与 :attr:`HttpRequestInfo.url_masked`。**不**把字段本身掩码，理由：

1. ``details.request.url`` 的原文是 C2 错误帧契约（``test_clean_c2_http_error_frame.py``
   断言 ``http://127.0.0.1:1/api`` 的 path 原样可读）钉住的「实际发出了什么」记录，
   掩码字段会破坏那份契约、改动外溢到别的单；
2. 泄露信道是**序列化**（``repr``/``str``/traceback/JSON），``__repr__`` 是唯一必然
   进日志的那一面；
3. 字段名 ``code`` / ``message`` / ``request`` / ``response`` 逐字不变 ⇒ 无消费者破坏。

**传输路径不变**：``request=rsp.raw_request`` 仍是同一个对象、``url`` 逐字节不变
（:class:`TestTransportPathUnchanged`）；掩码只发生在**记录/序列化**面。

**修复轮（组合轮 ``08b3f7fe`` 定验 FAIL 的那一点）**：上面的「记录面掩码」当时只把
**旧的名字规则**搬进了 ``__repr__``/``redacted()``——header **值**没有过内容清洗，
于是两族 header 整套穿过：响应侧回显 URL 的 ``Location``（真实 302 回环）/
``X-Echo-Url``，请求侧用户自带的 ``X-Api-Key`` / ``X-Token`` / ``X-Auth-Token`` /
``apikey`` / ``X-Goog-Api-Key``。现在``http.py`` 里 header 只有**一份**渲染
（``_render_header_for_record``，事件面与记录面共用）：键 = 7 名清单 ∪
``url_redaction._is_credential_key``（按名字复用共享谓词），值 = 共享
``_scrub_url_text``（与 body 同一个渲染器）。对应测试：
:class:`TestHeaderRendererIsOneSharedImplementation` /
:class:`TestResponseHeaderEchoFamily` /
:class:`TestCredentialHeaderNameFamily` /
:class:`TestAbsoluteClaimEnforced`（把 ``http.py`` 的绝对 claim 逐面钉死）；
:class:`TestDeclaredRawSurfaces` 逐项声明**刻意原样**的残余（C2 传输忠实契约）。
"""
from __future__ import annotations

import copy
import dataclasses
import http.server
import json
import logging
import pickle
import socket
import threading
import traceback

import pytest

import plaita.node.http as http_mod
from plaita.core.errors import NodeException
from plaita.node.http import (
    HTTP,
    HTTP_DO_REQUEST_ERROR,
    HttpNodeErrorInfo,
    HttpNodeResponse,
    HttpRequestInfo,
    _render_header_for_record,
)
from plaita.core import url_redaction
import plaita.server.factory  # noqa: F401 — plaita#160/#161 重切：_mask_url 的注册点在本模块
                          # （factory 模块体 server→core 向下注册）；不 import 则日志/渲染路径 fail-closed。
from tests.unit._url_sink_support import find_needles, leak_needles

SECRET = "Ks9Qm4Zt"
URL = f"http://host/robot/send?access_token={SECRET}"
NEEDLES = leak_needles(SECRET)

#: C2 契约里被钉住的「无凭据 URL」（path 是诊断价值本身，不属于本单掩码范围）。
C2_URL = "http://127.0.0.1:1/api"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _assert_clean(text: str, label: str) -> None:
    hits = find_needles(text, NEEDLES)
    assert hits == [], f"{label} 泄露了请求 URL 里的凭据（needles={hits}）：\n{text}"


class _FakeRawResponse:
    """``requests.Response`` / aiohttp 包装器的最小面（含凭据类响应头）。"""

    def __init__(self):
        self.status_code = 500
        self.reason = "Internal Server Error"
        self.headers = {
            "Content-Type": "application/json",
            "Set-Cookie": f"session={SECRET}",
            "WWW-Authenticate": f'Bearer realm="{SECRET}"',
            "X-Request-Id": "req-123",
        }


class _FakeRsp:
    """``handle_http_node_err`` 收到的 HttpResponse 形状（含请求/响应快照）。"""

    def __init__(self, request, res=None):
        self.raw_request = request
        self.raw_response = _FakeRawResponse()
        self.res = res

    def empty(self):
        return False

    def send_request_fail(self):
        return False


def _request(**kwargs) -> HttpRequestInfo:
    params = {
        "method": "POST",
        "url": URL,
        "headers": {"Content-Type": "application/json", "Authorization": f"Bearer {SECRET}"},
        "body": b'{"text": "hi"}',
    }
    params.update(kwargs)
    return HttpRequestInfo(**params)


def _error_info(rsp=None) -> HttpNodeErrorInfo:
    """经**真实构造点** ``wrap_http_node_err`` 拿 ``exc.details``。"""
    node = HTTP(id="h160", name="h160", url=URL, method="POST")
    request = _request()
    exc = node.wrap_http_node_err(
        HTTP_DO_REQUEST_ERROR,
        f"connection failed for {URL}",
        rsp if rsp is not None else _FakeRsp(request, res=f"upstream echoed {URL}"),
    )
    assert isinstance(exc, NodeException)
    return exc.details


# --------------------------------------------------------------------------- #
# ① 常见序列化路径：repr / str / traceback / json / 掩码视图
# --------------------------------------------------------------------------- #
class TestSerialisationSurfaces:
    def test_repr_of_the_attachment_family_is_masked(self):
        """附件**三个对象各自**的 ``repr``/``str`` 都要掩码。

        ``repr(exc.details)`` 走 :meth:`redacted`（dict 视图），不会碰到
        ``HttpRequestInfo`` / ``HttpNodeResponse`` 的 ``__repr__``；但消费者完全可能
        单独打印快照（``logger.info("req=%r", details.request)``）——那是同一族的
        序列化面，必须单独钉住（否则「响应面 repr 还原原文」这类变异会存活）。
        """
        info = _error_info()
        for label, obj in (
            ("exc.details", info),
            ("exc.details.request", info.request),
            ("exc.details.response", info.response),
        ):
            _assert_clean(repr(obj), f"repr({label})")
            _assert_clean(str(obj), f"str({label})")
        # 诊断价值：端点与 path 形状仍在
        assert "http://host/***/***?***" in repr(info)
        assert "req-123" in repr(info)

    def test_str_of_the_attachment_is_masked(self):
        info = _error_info()
        _assert_clean(str(info), "str(exc.details)")

    def test_repr_of_exception_is_masked(self):
        node = HTTP(id="h160", name="h160", url=URL, method="POST")
        exc = node.wrap_http_node_err(HTTP_DO_REQUEST_ERROR, f"boom {URL}", _FakeRsp(_request()))
        _assert_clean(repr(exc), "repr(exc)")
        _assert_clean(str(exc), "str(exc)")

    def test_traceback_format_exception_is_masked(self):
        node = HTTP(id="h160", name="h160", url=URL, method="POST")
        try:
            raise node.wrap_http_node_err(HTTP_DO_REQUEST_ERROR, f"boom {URL}", _FakeRsp(_request()))
        except NodeException as exc:
            rendered = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        _assert_clean(rendered, "traceback.format_exception(exc)")

    def test_json_dumps_of_the_attachment_is_masked(self):
        info = _error_info()
        # 消费者最常见的两种写法：default=str（`__repr__` 面）与掩码视图
        _assert_clean(json.dumps(info, default=str), "json.dumps(exc.details, default=str)")
        _assert_clean(json.dumps(info.__dict__, default=str), "json.dumps(exc.details.__dict__, default=str)")

    def test_dataclasses_asdict_is_not_applicable(self):
        """附件不是 dataclass ⇒ ``asdict`` 抛 TypeError（没有「悄悄序列化原文」的路径）。"""
        info = _error_info()
        with pytest.raises(TypeError):
            dataclasses.asdict(info)


# --------------------------------------------------------------------------- #
# ② 掩码视图：redacted() / url_masked
# --------------------------------------------------------------------------- #
class TestMaskedView:
    def test_redacted_is_json_safe_and_clean(self):
        info = _error_info()
        view = info.redacted()
        _assert_clean(json.dumps(view), "json.dumps(details.redacted())")
        assert view["code"] == HTTP_DO_REQUEST_ERROR
        assert view["request"]["url"] == "http://host/***/***?***"
        assert view["request"]["headers"]["Authorization"] == "***"
        assert view["response"]["headers"]["Set-Cookie"] == "***"
        assert view["response"]["headers"]["WWW-Authenticate"] == "***"
        assert view["response"]["headers"]["X-Request-Id"] == "req-123"

    def test_redacted_scrubs_a_body_that_echoes_the_url(self):
        info = _error_info()
        view = info.redacted()
        _assert_clean(view["response"]["body"], "redacted response body")
        assert "http://host/***/***?***" in view["response"]["body"]

    def test_redacted_is_idempotent(self):
        info = _error_info()
        once = info.redacted()
        twice = HttpNodeErrorInfo(
            code=once["code"], message=once["message"],
        ).redacted()
        assert twice["message"] == once["message"]

    def test_redacted_masks_an_unscrubbed_message(self):
        """即使调用方塞进未清洗的 message，视图也不带凭据（视图自己清洗）。"""
        info = HttpNodeErrorInfo(code=1, message=f"raw {URL}")
        _assert_clean(json.dumps(info.redacted()), "redacted(unscrubbed message)")

    def test_url_masked_keeps_endpoint(self):
        request = _request(url=f"http://[::1]:999999/robot/send?access_token={SECRET}")
        assert request.url_masked == "http://[::1]:999999/***/***?***"
        _assert_clean(request.url_masked, "url_masked")


# --------------------------------------------------------------------------- #
# ③ 传输路径不变：同一个对象、url 逐字节
# --------------------------------------------------------------------------- #
class TestTransportPathUnchanged:
    def test_details_request_is_the_same_object_as_raw_request(self):
        request = _request()
        rsp = _FakeRsp(request)
        info = _error_info(rsp)
        assert info.request is rsp.raw_request, "记录面换了对象——传输快照被复制/改写了"

    def test_raw_request_url_is_byte_identical_after_masking_the_record(self):
        request = _request()
        before = request.url.encode("utf-8")
        rsp = _FakeRsp(request)
        info = _error_info(rsp)
        repr(info)          # 触发记录面掩码
        info.redacted()
        assert request.url.encode("utf-8") == before
        assert rsp.raw_request.url == URL
        assert SECRET in rsp.raw_request.url

    def test_post_path_sees_the_original_url_end_to_end(self):
        """真实 ``execute``：连接失败后 ``PreparedRequest.url`` 仍是原始串。"""
        pytest.importorskip("requests")
        port = _free_port()
        url = f"http://127.0.0.1:{port}/robot/send?access_token={SECRET}"
        node = HTTP(id="h160", name="h160", url=url, method="GET")
        with pytest.raises(NodeException) as excinfo:
            node.execute({})
        details = excinfo.value.details
        assert details.request.url == url
        _assert_clean(repr(details), "end-to-end repr(details)")
        assert f"127.0.0.1:{port}" in repr(details)  # 端点诊断价值


# --------------------------------------------------------------------------- #
# ④ 声明面：字段仍持原文（「记录 = 实际发出内容」的契约），只有渲染面掩码
# --------------------------------------------------------------------------- #
class TestDeclaredNonLogSurface:
    def test_raw_fields_are_unchanged(self):
        info = _error_info()
        assert info.request.url == URL, "字段被掩码了——本单选型是「非日志面 + 掩码渲染」"
        assert info.request.headers["Authorization"] == f"Bearer {SECRET}"

    def test_c2_contract_url_still_readable(self):
        """C2 契约：无凭据 URL 的 path 在**字段**上仍原样可读（本单不碰字段）。

        记录渲染面则一律抹 path 的每一段（``http://127.0.0.1:1/***``）——这是
        「cannot-leak by construction」的代价：渲染器不猜哪一段是凭据（``#159``
        的同一纪律），所以无凭据 URL 的 path 在 ``repr`` 里也看不到。诊断价值留在
        **端点**（``host:port``）与字段本身。
        """
        request = HttpRequestInfo(method="GET", url=C2_URL)
        node = HTTP(id="h160", name="h160", url=C2_URL, method="GET")
        exc = node.wrap_http_node_err(HTTP_DO_REQUEST_ERROR, "boom", _FakeRsp(request))
        assert exc.details.request.url == C2_URL
        assert "http://127.0.0.1:1/***" in repr(exc.details)

    def test_response_headers_and_body_keep_their_values(self):
        info = _error_info()
        assert info.response.headers["Set-Cookie"] == f"session={SECRET}"
        assert SECRET in info.response.data


# =========================================================================== #
# 修复轮（组合轮 ``08b3f7fe`` 定验 FAIL 的那一点）：header **值**不得原样穿过记录面
# =========================================================================== #
# 根因：``_mask_record_headers`` 只按 **7 名清单**掩**键**、且**不洗值**，而同一轮的
# ``_mask_record_body`` 已经过 ``_scrub_url_text``——判据在同一个记录面里分叉。于是
# 两族 header 整套穿过：
#
# * 响应侧**回显请求 URL** 的头：``Location``（真实 302 回环）、``X-Echo-Url``；
# * 请求侧**用户自带凭据名**的头：``X-Api-Key`` / ``X-Token`` / ``X-Auth-Token`` /
#   ``apikey`` / ``X-Goog-Api-Key``（值 = 密钥本体）。
#
# 修复后的判据（``http.py::_render_header_for_record``，**全模块唯一** header 渲染，
# 事件面 ``_mask_error_frame_headers`` 与记录面 ``_mask_record_headers`` 共用）：
# 键 = 7 名清单 ∪ ``url_redaction._is_credential_key``（按名字 import，不复制文法）；
# 值 = ``url_redaction._scrub_url_text``（与 body 同一个渲染器）。

ECHO_HEADER_NAMES = ("Location", "X-Echo-Url")
CREDENTIAL_HEADER_NAMES = ("X-Api-Key", "X-Token", "X-Auth-Token", "apikey", "X-Goog-Api-Key")


class _RawResponse:
    """可定制 headers 的 ``requests.Response`` / aiohttp 包装器最小面。"""

    def __init__(self, headers, status_code=500, reason="Internal Server Error"):
        self.status_code = status_code
        self.reason = reason
        self.headers = dict(headers)


def _rsp(request, response_headers, data, status_code=500):
    """``handle_http_node_err`` 形状的响应（自定义 headers/status）——:func:`_wrap` 用。"""
    return _SnapshotRsp(
        request, _RawResponse(response_headers, status_code=status_code), data
    )


class _SnapshotRsp:
    """``handle_http_node_err`` 收到的 HttpResponse 形状（自定义 headers/status）。"""

    def __init__(self, request, raw_response, res):
        self.raw_request = request
        self.raw_response = raw_response
        self.res = res

    def empty(self):
        return False

    def send_request_fail(self):
        return False


def _wrap(request, response_headers, data, message, status_code=500):
    """经**真实构造点** ``wrap_http_node_err`` 拿异常（custom headers/status）。"""
    node = HTTP(id="h160fix", name="h160fix", url=URL, method="POST")
    return node.wrap_http_node_err(
        HTTP_DO_REQUEST_ERROR, message,
        _rsp(request, response_headers, data, status_code),
    )


def _all_serialisation_paths(exc) -> list:
    """任务点 2 点名的**全部**序列化路径 → ``[(label, text)]``。

    含 ``exc`` 本体、``exc.details`` 家族（三对象各自 ``repr``/``str``）、traceback、
    两种 ``json.dumps``、掩码视图 ``redacted()``、``url_masked``、``repr([details])``、
    ``logging`` 的 ``%r``/``%s``。**不含** ``exc.response``——那是 r4 定稿的事件载荷，
    由 :class:`TestDeclaredRawSurfaces` 单独钉住（本清单是「``exc.details`` 的序列化面」）。
    """
    details = exc.details
    traceback_text = "".join(
        traceback.format_exception(type(exc), exc, exc.__traceback__)
    )
    r_rec = logging.LogRecord("t", logging.ERROR, __file__, 1, "%r", (details,), None)
    s_rec = logging.LogRecord("t", logging.ERROR, __file__, 1, "%s", (details,), None)
    return [
        ("repr(exc)", repr(exc)),
        ("str(exc)", str(exc)),
        ("traceback.format_exception", traceback_text),
        ("repr(details)", repr(details)),
        ("str(details)", str(details)),
        ("repr(details.request)", repr(details.request)),
        ("str(details.request)", str(details.request)),
        ("repr(details.response)", repr(details.response)),
        ("str(details.response)", str(details.response)),
        ("json.dumps(details.__dict__, default=str)",
         json.dumps(details.__dict__, default=str)),
        ("json.dumps(details, default=str)", json.dumps(details, default=str)),
        ("json.dumps(details.redacted())", json.dumps(details.redacted())),
        ("details.request.url_masked", details.request.url_masked),
        ("repr([details])", repr([details])),
        ("logging %r", r_rec.getMessage()),
        ("logging %s", s_rec.getMessage()),
    ]


def _assert_all_paths_clean(exc, label: str) -> None:
    """逐面断言无 needle；“没有哪条路径脏”这件事本身就是该 case 的证明。"""
    for path_label, text in _all_serialisation_paths(exc):
        hits = find_needles(str(text), NEEDLES)
        assert hits == [], f"{label} / {path_label} 泄露（needles={hits}）：\n{text}"


class TestHeaderRendererIsOneSharedImplementation:
    def test_url_text_renderer_is_the_shared_one(self):
        assert http_mod._scrub_url_text is url_redaction._scrub_url_text

    def test_credential_key_predicate_is_the_shared_one(self):
        """键判据的第二腿**按名字**复用 ``url_redaction``，不是复制一份文法。"""
        assert http_mod._is_credential_key is url_redaction._is_credential_key
        assert http_mod._is_credential_header_name("X-Api-Key") is True
        assert http_mod._is_credential_header_name("X-Echo-Url") is False
        assert http_mod._is_credential_header_name("Cookie2") is True  # 7 名清单腿
        assert http_mod._is_credential_header_name("X-Request-Id") is False

    def test_non_credential_non_url_values_pass_through(self):
        assert _render_header_for_record("X-Request-Id", "req-123") == "req-123"
        assert _render_header_for_record("Content-Type", "application/json") == "application/json"

    def test_one_header_implementation_for_both_surfaces(self):
        """事件面与记录面共用同一渲染器：非 URL 值两处都原样，URL 值两处都掩。"""
        headers = {"Location": URL, "X-Api-Key": SECRET, "X-Request-Id": "req-123"}
        request = HttpRequestInfo(method="POST", url=URL, headers={"Authorization": "x"}, body=None)
        exc = _wrap(request, headers, "ok", "boom")
        assert exc.response["headers"]["Location"] == "http://host/***/***?***"
        assert exc.response["headers"]["X-Api-Key"] == "***"
        assert exc.response["headers"]["X-Request-Id"] == "req-123"
        assert exc.details.redacted()["response"]["headers"] == exc.response["headers"]


class TestResponseHeaderEchoFamily:
    """响应头回显请求 URL：``Location`` / ``X-Echo-Url``（定验点名的第一族）。"""

    @pytest.mark.parametrize("header_name", ECHO_HEADER_NAMES)
    def test_echoing_response_header_is_scrubbed_on_every_path(self, header_name):
        request = HttpRequestInfo(
            method="POST", url=URL,
            headers={"Authorization": f"Bearer {SECRET}"}, body=b"{}",
        )
        response_headers = {
            "Content-Type": "application/json",
            header_name: URL,
            "X-Request-Id": "req-123",
        }
        exc = _wrap(request, response_headers, "no echo in body", f"boom {URL}")
        _assert_all_paths_clean(exc, f"echo-header {header_name}")
        view = exc.details.redacted()
        assert view["response"]["headers"][header_name] == "http://host/***/***?***"
        assert view["response"]["headers"]["X-Request-Id"] == "req-123"
        # 诊断价值：端点仍在渲染面
        assert "http://host" in repr(exc.details.response)
        # 传输忠实：字段仍是逐字快照（刻意原样，见 TestDeclaredRawSurfaces）
        assert exc.details.response.headers[header_name] == URL

    def test_real_302_round_trip_location_echo(self):
        """真实 ``http.server`` 302 回环：``requests.get(allow_redirects=False)``。

        前提断言（``SECRET in last_location``）保证这不是空转：服务端**确实**把带
        令牌的 URL 放进了 ``Location``/``X-Echo-Url``。
        """
        requests = pytest.importorskip("requests")
        captured = {}

        class _Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                port = self.server.server_address[1]
                location = f"http://127.0.0.1:{port}/robot/send?access_token={SECRET}"
                captured["location"] = location
                self.send_response(302)
                self.send_header("Location", location)
                self.send_header("X-Echo-Url", location)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, *args):  # 静音
                pass

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        port = server.server_address[1]
        url = f"http://127.0.0.1:{port}/robot/send?access_token={SECRET}"
        try:
            rsp = requests.get(url, allow_redirects=False, timeout=5)
            assert rsp.status_code == 302
            assert captured["location"] == rsp.headers["Location"]
            assert SECRET in rsp.headers["Location"], "前提失效：302 未回显凭据"
            request = HttpRequestInfo(method="GET", url=url, headers={}, body=None)
            node = HTTP(id="h302", name="h302", url=url, method="GET")
            exc = node.wrap_http_node_err(
                HTTP_DO_REQUEST_ERROR,
                f"unexpected {rsp.status_code} from {url}",
                _SnapshotRsp(request, _RawResponse(dict(rsp.headers), status_code=302), rsp.text),
            )
        finally:
            server.shutdown()
            server.server_close()

        _assert_all_paths_clean(exc, "real-302")
        rendered = repr(exc.details)
        assert f"127.0.0.1:{port}" in rendered           # 端点诊断价值
        assert "/***" in rendered                        # 路径形状保留
        assert SECRET not in json.dumps(exc.response)     # 事件面 header 也已洗
        assert exc.details.response.headers["Location"] == captured["location"]


class TestCredentialHeaderNameFamily:
    """请求侧用户自带凭据名（定验点名的第二族）：键判据加宽到共享谓词。"""

    @pytest.mark.parametrize("header_name", CREDENTIAL_HEADER_NAMES)
    def test_credential_named_header_is_masked_on_every_path(self, header_name):
        request = HttpRequestInfo(
            method="POST", url=URL,
            headers={"Content-Type": "application/json", header_name: SECRET},
            body=b"{}",
        )
        exc = _wrap(request, {"Content-Type": "application/json"}, "ok", "boom")
        _assert_all_paths_clean(exc, f"credential-header {header_name}")
        view = exc.details.redacted()
        assert view["request"]["headers"][header_name] == "***"
        assert view["request"]["headers"]["Content-Type"] == "application/json"
        rendered = repr(exc.details.request)
        assert header_name in rendered and "***" in rendered
        # 字段仍是逐字快照（刻意原样）
        assert exc.details.request.headers[header_name] == SECRET

    def test_all_credential_names_together(self):
        headers = {name: SECRET for name in CREDENTIAL_HEADER_NAMES}
        request = HttpRequestInfo(method="POST", url=URL, headers=headers, body=None)
        exc = _wrap(request, {}, "ok", "boom")
        _assert_all_paths_clean(exc, "credential-headers-all")
        view = exc.details.redacted()
        assert all(view["request"]["headers"][name] == "***" for name in CREDENTIAL_HEADER_NAMES)


# --------------------------------------------------------------------------- #
# ③′ 绝对 claim：维持 ``http.py`` 的原话，并用**负向**测试把它钉死
# --------------------------------------------------------------------------- #
class TestAbsoluteClaimEnforced:
    """``http.py`` ``HttpRequestInfo.__repr__`` 的绝对 claim（定验 falsify 的那句）：

    「本对象（及 ``exc.details`` / ``exc.details.response`` 同族）的任何
    ``repr``/``str``——含 traceback 里的对象打印、``repr([obj])``、
    ``json.dumps(obj, default=str)``、``json.dumps(vars(obj), default=str)``、
    ``logging`` 的 ``%r``/``%s``——都拿不到凭据」。

    **选择 = 维持（uphold）**，理由：定验点名的强制点就是这个 ``__repr__`` 面；
    删/改 claim 只是把「不安全」改写成「不承诺」，而修复本身（header 值过共享
    渲染器）已经让 claim 成真。本类把 claim 逐面机械断言——一旦 header 值不再
    过 ``_scrub_url_text``、或键判据退回 7 名清单，**本类立刻变红**（负向测试）。
    """

    def _worst_case(self):
        """三个泄露家族挤在同一个异常上：URL 回显头 + 凭据名头 + body 回显。"""
        request = HttpRequestInfo(
            method="POST", url=URL,
            headers={
                **{name: SECRET for name in CREDENTIAL_HEADER_NAMES},
                "Authorization": f"Bearer {SECRET}",
                "Content-Type": "application/json",
            },
            body=f'{{"echo": "{URL}"}}'.encode(),
        )
        response_headers = {
            **{name: URL for name in ECHO_HEADER_NAMES},
            "Content-Type": "application/json",
        }
        return _wrap(
            request, response_headers, f"upstream echoed {URL}",
            f"Max retries exceeded with url: /robot/send?access_token={SECRET}",
        )

    def test_claim_holds_on_every_serialisation_path(self):
        try:
            raise self._worst_case()
        except NodeException as exc:
            _assert_all_paths_clean(exc, "absolute-claim")

    def test_claim_holds_for_direct_object_prints(self):
        exc = self._worst_case()
        for label, obj in (
            ("details", exc.details),
            ("details.request", exc.details.request),
            ("details.response", exc.details.response),
        ):
            for render in (repr, str):
                hits = find_needles(render(obj), NEEDLES)
                assert hits == [], f"{render.__name__}({label}) 泄露：{hits}"

    def test_diagnostics_survive_the_claim(self):
        """掩码不许把诊断一起删掉：端点、``Max retries exceeded``、栈都要在。"""
        try:
            raise self._worst_case()
        except NodeException as exc:
            rendered = repr(exc.details)
            assert "Max retries exceeded" in rendered
            assert "http://host" in rendered
            assert "/***" in rendered
            paths = dict(_all_serialisation_paths(exc))
            assert "Traceback (most recent call last)" in paths["traceback.format_exception"]
            assert SECRET not in paths["traceback.format_exception"]


# --------------------------------------------------------------------------- #
# ⑤ 残余清单：逐项声明 (i) 已修 或 (ii) 刻意原样（C2 契约），并钉住 (ii)
# --------------------------------------------------------------------------- #
class TestDeclaredRawSurfaces:
    """**残余清单**逐项声明 + 钉住（任务点 5；定验列出的比正文多）。

    刻意原样的**唯一理由**是 C2-1 契约：``details`` 的字段是「实际发出了什么 /
    实际收到了什么」的**传输忠实**记录（``details.request is rsp.raw_request``、
    ``url`` 与真正发出的字节相同），字段级掩码会破坏该契约，并把改动外溢到
    ``#158``/``#159``/``#88`` 与 C2 自己的断言。安全边界因此是**渲染面**
    （``__repr__``/``str``/``redacted()``），不是属性面。

    本类把每一条**刻意原样**钉住：行为无论变松（引入泄露）还是变紧（破坏传输
    忠实/C2 契约）都会变红——包括 pickle/deepcopy 这类「绕开 ``__repr__`` 的
    结构化复制面」。
    """

    def test_details_request_url_is_raw_by_contract(self):
        info = _error_info()
        assert info.request.url == URL
        assert SECRET in info.request.url

    def test_response_data_is_raw_by_contract(self):
        info = _error_info()
        assert SECRET in info.response.data

    def test_vars_url_headers_body_are_raw_by_contract(self):
        request = _request(headers={"X-Api-Key": SECRET})
        info = _error_info(_FakeRsp(request))
        raw = vars(info.request)
        assert raw["url"] == URL
        assert raw["headers"]["X-Api-Key"] == SECRET
        assert raw["body"] == b'{"text": "hi"}'

    def test_pickle_and_deepcopy_round_trips_keep_raw_fields(self):
        request = _request(headers={"X-Api-Key": SECRET})
        clones = [pickle.loads(pickle.dumps(request)), copy.deepcopy(request)]
        for clone in clones:
            assert clone.url == URL
            assert clone.headers["X-Api-Key"] == SECRET
        info = _error_info(_FakeRsp(request))
        restored = pickle.loads(pickle.dumps(info))
        assert restored.request.url == URL
        assert restored.request.headers["X-Api-Key"] == SECRET

    def test_event_frame_body_is_deliberately_raw_headers_are_fixed(self):
        """``exc.response``（r4 C2 事件载荷）：**body 原样**是刻意声明。

        定验列出的残余里有 ``exc.response['body']``；本类把它钉死（r4 的 C2 契约
        逐字节），同时证明同一帧的 **header 值**已经不是残余（修复轮一并洗掉）。
        """
        request = _request(headers={"X-Api-Key": SECRET})
        exc = _wrap(
            request, {"Location": URL, "X-Request-Id": "req-123"},
            f"upstream echoed {URL}", "boom",
        )
        assert SECRET in exc.response["body"], "C2 契约：事件帧 body 刻意原样"
        assert exc.response["headers"]["Location"] == "http://host/***/***?***"
        assert exc.response["headers"]["X-Request-Id"] == "req-123"
        assert exc.response["status"] == 500
        assert exc.response["statusText"] == "Internal Server Error"
        assert set(exc.response) == {"status", "statusText", "headers", "body"}

    def test_masked_view_is_the_safe_path_for_the_same_fixture(self):
        """同一 fixture：字段/事件帧仍是原文时，``redacted()`` 是**安全的那条路**。"""
        request = _request(headers={"X-Api-Key": SECRET})
        exc = _wrap(
            request, {"Location": URL}, f"upstream echoed {URL}", "boom",
        )
        view = exc.details.redacted()
        assert find_needles(json.dumps(view), NEEDLES) == []
