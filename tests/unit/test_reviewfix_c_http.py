"""test_reviewfix_c_http — 评审修复包 C1 回归。

覆盖两件事：

1. restricted（策略激活）路径首跳禁自动重定向——302 把请求带进
   allowedHosts/deniedHosts/blockPrivateNetworks 禁止的目标时必须被逐跳
   校验拦下（该修复已随 7c631a2 的 BFF 热路径合并入库，这里补的是
   端到端行为断言：被禁目标的一次真实请求都不该发生）；
2. restricted 逐跳手动跟随重定向时，跨源（host 变化）必须剥离
   Authorization/Cookie 等凭据类头（RFC 7231 §9.4 语义），同源保留——
   本次修复的泄漏点，sync/async 两条路径各一份断言。

全程本地 http.server（127.0.0.1 / localhost 双主机名构造跨源），零外网请求。
"""

import http.server
import threading
import unittest

from plaita.node.http import (
    HttpExecutor,
    URLPolicyError,
    _strip_sensitive_headers,
)

CREDENTIAL_HEADERS = {
    "Authorization": "Bearer secret-token",
    "Cookie": "session=abc",
    "Proxy-Authorization": "Basic cHJveHk=",
}


def _start_redirect_server(received):
    """127.0.0.1 上起一个服务：/start 302 → http://127.0.0.1:<port>/steal，
    /samesite 302 → http://localhost:<port>/final（host 变化=跨源 /
    host 不变=同源），其余路径回 200。记录每个请求收到的头。"""

    class _Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            length = int(self.headers.get("Content-Length", 0) or 0)
            if length:
                self.rfile.read(length)
            port = self.server.server_address[1]
            received[self.path] = dict(self.headers)
            if self.path == "/start":
                self.send_response(302)
                self.send_header("Location", f"http://127.0.0.1:{port}/steal")
                self.end_headers()
            elif self.path == "/samesite":
                self.send_response(302)
                self.send_header("Location", f"http://localhost:{port}/final")
                self.end_headers()
            else:
                body = b'{"ok": true}'
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        def log_message(self, *args):  # 静音
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def _executor(url, restricted=True):
    return HttpExecutor(
        url=url, method="GET", query=None, body=None,
        headers=dict(CREDENTIAL_HEADERS, **{"X-Keep": "yes"}),
        addressing=None, delegate=None,
        allowed_hosts=["localhost", "127.0.0.1"] if restricted else None,
    )


class TestRestrictedRedirectBlocksDisallowedHost(unittest.TestCase):
    """修复包 C1 前半：302 → 非白名单 host 被拦（首跳 allow_redirects=False）。"""

    def setUp(self):
        self.received = {}
        self.server = _start_redirect_server(self.received)
        self.port = self.server.server_address[1]

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()

    def test_redirect_to_disallowed_host_never_requested(self):
        # allowed_hosts 只放行 localhost；302 目标是 127.0.0.1（host 不同）
        ex = HttpExecutor(
            url=f"http://localhost:{self.port}/start", method="GET",
            query=None, body=None, headers=None, addressing=None, delegate=None,
            allowed_hosts=["localhost"],
        )
        rsp, err = ex.handle_request({})
        self.assertIsInstance(err, URLPolicyError)
        self.assertIn("127.0.0.1", str(err))
        # 被禁目标一次真实请求都没发生（自动跟随会让它发生）
        self.assertNotIn("/steal", self.received)


class TestCrossOriginRedirectStripsCredentials(unittest.TestCase):
    """修复包 C1 后半：跨源重定向剥离凭据类头，同源保留。"""

    def setUp(self):
        self.received = {}
        self.server = _start_redirect_server(self.received)
        self.port = self.server.server_address[1]

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()

    def test_sync_cross_origin_strips_credential_headers(self):
        rsp, err = _executor(f"http://localhost:{self.port}/start").handle_request({})
        self.assertIsNone(err)
        got = self.received["/steal"]
        self.assertIsNone(got.get("Authorization"))
        self.assertIsNone(got.get("Cookie"))
        self.assertIsNone(got.get("Proxy-Authorization"))
        # 非凭据头不受影响
        self.assertEqual(got.get("X-Keep"), "yes")

    def test_sync_same_origin_keeps_headers(self):
        rsp, err = _executor(f"http://localhost:{self.port}/samesite").handle_request({})
        self.assertIsNone(err)
        got = self.received["/final"]
        self.assertEqual(got.get("Authorization"), "Bearer secret-token")
        self.assertEqual(got.get("Cookie"), "session=abc")
        self.assertEqual(got.get("X-Keep"), "yes")

    def test_async_cross_origin_strips_credential_headers(self):
        rsp, err = self._async_run("/start")
        self.assertIsNone(err)
        got = self.received["/steal"]
        self.assertIsNone(got.get("Authorization"))
        self.assertIsNone(got.get("Cookie"))
        self.assertIsNone(got.get("Proxy-Authorization"))
        self.assertEqual(got.get("X-Keep"), "yes")

    def test_async_same_origin_keeps_headers(self):
        rsp, err = self._async_run("/samesite")
        self.assertIsNone(err)
        got = self.received["/final"]
        self.assertEqual(got.get("Authorization"), "Bearer secret-token")
        self.assertEqual(got.get("Cookie"), "session=abc")

    def _async_run(self, path):
        import asyncio

        async def go():
            return await _executor(
                f"http://localhost:{self.port}{path}").handle_request_async({})

        return asyncio.run(go())


class TestStripSensitiveHeadersHelper(unittest.TestCase):
    def test_cross_origin_strips(self):
        stripped = _strip_sensitive_headers(
            {"Authorization": "x", "Cookie": "y", "Proxy-Authorization": "z",
             "WWW-Authenticate": "w", "X-Ok": "1"},
            "http://a.example.com/x", "http://b.example.com/y")
        self.assertEqual(stripped, {"X-Ok": "1"})

    def test_same_origin_keeps(self):
        headers = {"Authorization": "x", "Cookie": "y", "X-Ok": "1"}
        self.assertEqual(
            _strip_sensitive_headers(headers, "http://a.example.com/x",
                                     "http://a.example.com/y"),
            headers)

    def test_scheme_or_port_change_still_same_host(self):
        """判定维度是 host（与 requests rebuild_auth 一致）：换 scheme/端口不剥离。"""
        headers = {"Authorization": "x"}
        self.assertEqual(
            _strip_sensitive_headers(headers, "http://a.example.com:80/x",
                                     "https://a.example.com:443/y"),
            headers)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
