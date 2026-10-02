"""test_clean_c2_http_error_frame — 清理批次 C2-1 回归。

两个修复点的行为断言（修复前实证：wrap_http_node_err 构造的
HttpNodeErrorInfo 被直接丢弃；async 连接类错误归 1003 而同步同错误归 1002）：

1. 错误帧信息量：HTTP 节点错误抛出的 NodeException 上可读到
   ``response``（status/statusText/headers/body 的事件安全摘要——凭据头脱敏、
   body 截断）与 ``details``（完整 HttpNodeErrorInfo）；NodeException 本体的
   code/message 契约不变（core 层零侵入）。
2. sync/async 一致性：连接类错误（目标端口无监听）在两条路径同归
   1002(DO_REQUEST)，async 错误帧带请求快照（method/url）；URLPolicyError
   分类对齐；4xx/5xx 一律按成功结果返回的设计选择不变。

全程本地（127.0.0.1 / 无监听端口 / mock 响应对象），零外网请求。
"""

from __future__ import annotations

import asyncio
import http.server
import json
import socket
import threading
import unittest

import pytest

pytest.importorskip("aiohttp")
pytest.importorskip("requests")

from plaita.core.errors import NodeException
from plaita.node.http import (
    HTTP_DO_REQUEST_ERROR,
    HTTP_GEN_REQUEST_ERROR,
    HTTP_NODE_EXEC_ERROR,
    HTTP,
    HttpExecutor,
    HttpNodeErrorInfo,
    HttpRequestInfo,
    _summarize_error_frame_response,
)

GEN_REQUEST = HTTP_GEN_REQUEST_ERROR      # 1001
DO_REQUEST = HTTP_DO_REQUEST_ERROR        # 1002
NODE_EXEC = HTTP_NODE_EXEC_ERROR          # 1003


def _free_local_port() -> int:
    """拿一个当前无监听的本地端口（连接必败，零外网）。"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _make_node(url: str, method: str = "GET", **kwargs) -> HTTP:
    return HTTP(id="http_c2", name="http_c2", url=url, method=method, **kwargs)


# ---------------------------------------------------------------------------
# ① 错误帧信息量：response 摘要 / details 挂载
# ---------------------------------------------------------------------------

class _FakeRawResponse:
    """模拟 requests.Response / _AiohttpResponseWrapper 的最小面。"""

    def __init__(self, status_code=503, reason="Service Unavailable"):
        self.status_code = status_code
        self.reason = reason
        self.headers = {
            "Content-Type": "application/json",
            "X-Request-Id": "req-123",
            "Set-Cookie": "session=should-be-masked",
            "WWW-Authenticate": "Bearer realm=masked",
        }


class _FakeRsp:
    """handle_http_node_err 收到的 HttpResponse 形状（含响应）。"""

    def __init__(self, res=None, with_response=True):
        self.raw_request = HttpRequestInfo(method="GET", url="http://127.0.0.1:1/api")
        self.raw_response = _FakeRawResponse() if with_response else None
        self.res = res

    def empty(self):
        return False


class TestErrorFrameCarriesResponseSummary(unittest.TestCase):
    """修复点①：错误帧不再丢 response 细节（exc.response / exc.details）。"""

    def setUp(self):
        self.node = _make_node("http://127.0.0.1:1/api")

    def _wrap_with_big_body(self):
        body = {"error": "upstream down", "detail": "x" * 5000}
        return self.node.wrap_http_node_err(NODE_EXEC, "boom", _FakeRsp(res=body))

    def test_exception_type_and_code_contract_unchanged(self):
        exc = self._wrap_with_big_body()
        self.assertIsInstance(exc, NodeException)
        self.assertEqual(exc.code, NODE_EXEC)
        self.assertEqual(exc.message, "boom")
        # str(e) 保持 core 层原样（事件里的 message 字段不变）
        self.assertEqual(str(exc), f"NodeException: code={NODE_EXEC}, message=boom")

    def test_response_summary_has_status_headers_body(self):
        exc = self._wrap_with_big_body()
        summary = exc.response
        self.assertIsInstance(summary, dict)
        self.assertEqual(summary["status"], 503)
        self.assertEqual(summary["statusText"], "Service Unavailable")
        self.assertEqual(summary["headers"]["Content-Type"], "application/json")
        self.assertIn("upstream down", summary["body"])

    def test_response_summary_masks_credential_headers(self):
        exc = self._wrap_with_big_body()
        headers = exc.response["headers"]
        self.assertEqual(headers["Set-Cookie"], "***")
        self.assertEqual(headers["WWW-Authenticate"], "***")
        # 非凭据头原样
        self.assertEqual(headers["X-Request-Id"], "req-123")

    def test_response_summary_truncates_big_body(self):
        exc = self._wrap_with_big_body()
        body = exc.response["body"]
        self.assertLess(len(body), 5000)
        self.assertTrue(body.endswith("chars]"), f"截断摘要应带长度标记: ...{body[-20:]}")

    def test_response_summary_is_json_safe(self):
        exc = self._wrap_with_big_body()
        # 事件载荷里要能直接序列化
        json.dumps(exc.response)

    def test_details_carries_structured_error_info(self):
        exc = self._wrap_with_big_body()
        self.assertIsInstance(exc.details, HttpNodeErrorInfo)
        self.assertEqual(exc.details.code, NODE_EXEC)
        self.assertEqual(exc.details.response.status, 503)
        # 请求快照可从 details 读到
        self.assertEqual(exc.details.request.url, "http://127.0.0.1:1/api")

    def test_send_failure_frame_has_response_none(self):
        """连接类错误（无响应）：response 摘要为 None，details 仍带请求信息。"""
        rsp = _FakeRsp(with_response=False)
        exc = self.node.wrap_http_node_err(DO_REQUEST, "Connection refused", rsp)
        self.assertIsNone(exc.response)
        self.assertIsInstance(exc.details, HttpNodeErrorInfo)
        self.assertIsNone(exc.details.response)
        self.assertEqual(exc.details.request.method, "GET")

    def test_no_rsp_keeps_plain_exception(self):
        """new_executor 失败（rsp=None → 1001）：无帧可挂，属性不出现。"""
        exc = self.node.wrap_http_node_err(GEN_REQUEST, "bad url", None)
        self.assertIsInstance(exc, NodeException)
        self.assertEqual(exc.code, GEN_REQUEST)
        self.assertFalse(hasattr(exc, "response"))
        self.assertFalse(hasattr(exc, "details"))

    def test_summarize_helper_none_response(self):
        self.assertIsNone(_summarize_error_frame_response(None))

    def test_summarize_helper_serializes_dict_body(self):
        from plaita.node.http import HttpNodeResponse
        resp = HttpNodeResponse(status=200, status_text="OK",
                                headers={"a": "b"}, data={"k": "v"})
        summary = _summarize_error_frame_response(resp)
        self.assertEqual(summary["status"], 200)
        self.assertEqual(summary["body"], '{"k":"v"}')

    def test_summarize_helper_unserializable_body_falls_back_to_str(self):
        from plaita.node.http import HttpNodeResponse

        class _Weird:
            def __str__(self):
                return "weird-object"

        resp = HttpNodeResponse(status=200, status_text="OK", headers={},
                                data=_Weird())
        summary = _summarize_error_frame_response(resp)
        self.assertEqual(summary["body"], "weird-object")


# ---------------------------------------------------------------------------
# ② sync/async 同错误同码 + 设计选择不变
# ---------------------------------------------------------------------------

def _start_local_server(status=503, body=b'{"err": "nope"}'):
    class _Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):  # 静音
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


class TestSyncAsyncErrorCodeParity(unittest.TestCase):
    """修复点②：同一连接类错误 sync/async 同码 1002，async 帧带请求快照。"""

    def setUp(self):
        self.port = _free_local_port()
        self.url = f"http://127.0.0.1:{self.port}/x"

    def _sync_error(self):
        node = _make_node(self.url, request_timeout=2.0)
        with self.assertRaises(NodeException) as ctx:
            node.run({})
        return ctx.exception

    def _async_error(self):
        node = _make_node(self.url, request_timeout=2.0)

        async def go():
            await node.arun({})

        with self.assertRaises(NodeException) as ctx:
            asyncio.run(go())
        return ctx.exception

    def test_sync_connection_error_is_do_request(self):
        exc = self._sync_error()
        self.assertEqual(exc.code, DO_REQUEST)

    def test_async_connection_error_is_do_request(self):
        """修复前 async 归 1003（节点处理错误）——与 sync 1002 分叉。"""
        exc = self._async_error()
        self.assertEqual(exc.code, DO_REQUEST)

    def test_async_error_frame_carries_request_snapshot(self):
        exc = self._async_error()
        self.assertIsInstance(exc.details, HttpNodeErrorInfo)
        req = exc.details.request
        self.assertIsInstance(req, HttpRequestInfo)
        self.assertEqual(req.method, "GET")
        self.assertIn(f":{self.port}", req.url)
        self.assertIsNone(exc.response)  # 连接失败无响应

    def test_sync_and_async_same_error_same_code(self):
        self.assertEqual(self._sync_error().code, self._async_error().code)

    def test_url_policy_error_classifies_do_request_both_paths(self):
        """策略拒绝（URLPolicyError）sync/async 同归 1002，帧带请求快照。"""
        node = _make_node("http://internal.example/x",
                          allowed_hosts=["example.com"])
        ex = HttpExecutor(
            url="http://internal.example/x", method="GET", query=None, body=None,
            headers=None, addressing=None, delegate=None,
            allowed_hosts=["example.com"],
        )
        rsp, err = ex.handle_request({})
        self.assertIsInstance(err, Exception)
        exc = node.handle_http_node_err(err, rsp)
        self.assertIsInstance(exc, NodeException)
        self.assertEqual(exc.code, DO_REQUEST)
        # sync 路径请求快照是 requests.PreparedRequest（async 才是 HttpRequestInfo）
        self.assertIsNotNone(exc.details.request)


class TestHttpErrorStatusDesignUnchanged(unittest.TestCase):
    """4xx/5xx 一律按成功结果返回是设计选择——不因错误帧增强而改变。"""

    def setUp(self):
        self.server = _start_local_server(status=503)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}/x"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()

    def test_sync_503_is_success_result_with_status(self):
        node = _make_node(self.url, request_timeout=5.0)
        # 4xx/5xx 不抛错：结果就是响应 data（output=None 时 _build_response_result
        # 返回 RESPONSE_DATA），状态与响应上下文写在 $NODE.<id>.STATUS
        result = node.run({})
        self.assertEqual(result, {"err": "nope"})

    def test_async_503_is_success_result_with_status(self):
        node = _make_node(self.url, request_timeout=5.0)

        async def go():
            return await node.arun({})

        result = asyncio.run(go())
        self.assertEqual(result, {"err": "nope"})


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
