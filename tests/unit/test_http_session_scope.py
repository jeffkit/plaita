"""test_http_session_scope — flow 作用域 HTTP 会话与策略 DNS 缓存的回归。

覆盖 2026-10 BFF 热路径评审的四个硬性要求：

1. flow 作用域 session 被节点复用（作用域内不再落一次性会话兜底）、
   作用域结束时确定性关闭；
2. cookie 默认阻断（跨 flow/租户泄漏防护）：async DummyCookieJar +
   sync _BlockAllCookies；fork 后共享 sync session 重建；
3. 首跳禁自动重定向（两条路径同一形状）：restricted 时 requests 的自动跟随
   会让逐跳 SSRF 校验失效；默认路径时自动跟随会对每个 3xx 读尽 hop body
   （绕过响应体上限）——统一手动逐跳；
4. 策略 DNS 缓存：命中、失败不缓存、TTL=0 直通、_host_allowed 保持纯函数。
"""

from __future__ import annotations

import asyncio
import http.server
import os
import threading
import unittest
from unittest import TestCase
from unittest.mock import patch

import pytest

pytest.importorskip("aiohttp")
pytest.importorskip("requests")

import aiohttp
import requests

import plaita.node.http as http_mod
from plaita.core.http_session import (
    _BlockAllCookies,
    close_flow_session,
    get_flow_session,
    get_shared_sync_session,
    clear_shared_sync_session,
    open_flow_session,
)
from plaita.node.http import (
    HttpExecutor,
    _cached_resolve_host,
    _host_allowed,
    clear_dns_cache,
)


def _start_server():
    class _Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
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


class TestFlowScopeSession(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.server = _start_server()
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}/x"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()

    async def test_scope_session_reused_and_deterministically_closed(self):
        executor = HttpExecutor(url=self.url, method="GET", query=None, body=None,
                                headers=None, addressing=None, delegate=None)
        await open_flow_session()
        session = get_flow_session()
        self.assertIsNotNone(session)
        try:
            # 作用域内节点不得落一次性会话兜底（即 session 真被复用）
            with patch("plaita.node.http._new_oneshot_session",
                       side_effect=AssertionError("one-shot fallback used inside scope")):
                for _ in range(3):
                    rsp, err = await executor.handle_request_async(None)
                    self.assertIsNone(err)
                    self.assertEqual(rsp.res, {"ok": True})
            self.assertFalse(session.closed)
        finally:
            await close_flow_session()
        self.assertIsNone(get_flow_session())
        self.assertTrue(session.closed)

    async def test_oneshot_fallback_outside_scope(self):
        executor = HttpExecutor(url=self.url, method="GET", query=None, body=None,
                                headers=None, addressing=None, delegate=None)
        self.assertIsNone(get_flow_session())
        rsp, err = await executor.handle_request_async(None)
        self.assertIsNone(err)
        self.assertEqual(rsp.res, {"ok": True})

    async def test_cookie_jar_dummy_by_default(self):
        await open_flow_session()
        try:
            session = get_flow_session()
            self.assertIsInstance(session.cookie_jar, aiohttp.DummyCookieJar)
        finally:
            await close_flow_session()


class TestSyncSharedSession(TestCase):
    def setUp(self):
        clear_shared_sync_session()
        clear_dns_cache()

    def tearDown(self):
        clear_shared_sync_session()

    def test_reused_within_process(self):
        s1 = get_shared_sync_session()
        s2 = get_shared_sync_session()
        self.assertIs(s1, s2)

    def test_rebuilt_after_fork(self):
        import plaita.core.http_session as hs

        s1 = get_shared_sync_session()
        hs._sync_session_pid = -1  # 模拟 fork 后 PID 变化
        try:
            s2 = get_shared_sync_session()
            self.assertIsNot(s1, s2)
        finally:
            hs._sync_session_pid = os.getpid()

    def test_cookie_policy_blocks_storage(self):
        session = get_shared_sync_session()
        self.assertIsInstance(session.cookies._policy, _BlockAllCookies)
        cookie = requests.cookies.create_cookie("sid", "x", domain="example.com", path="/")
        request = type("R", (), {
            "get_full_url": lambda self: "http://example.com/",
            "unverifiable": False,
        })()
        self.assertFalse(session.cookies._policy.set_ok(cookie, request))


class TestRestrictedRedirectFirstHop(TestCase):
    """现状 bug 回归：首跳必须 allow_redirects=False（两条路径都手动逐跳）。"""

    def setUp(self):
        clear_shared_sync_session()
        clear_dns_cache()

    def tearDown(self):
        clear_shared_sync_session()
        clear_dns_cache()

    def test_first_hop_send_disables_auto_redirect(self):
        # 字面 IP：resolver 走 getaddrinfo("127.0.0.1")，无真实网络依赖
        executor = HttpExecutor(url="http://127.0.0.1:1/a", method="GET", query=None,
                                body=None, headers=None, addressing=None, delegate=None,
                                allowed_hosts=["127.0.0.1"])
        session = get_shared_sync_session()
        fake_response = type("R", (), {
            "is_redirect": False, "status_code": 200, "url": "http://127.0.0.1:1/a",
            "text": "{}", "headers": {},
            "json": staticmethod(lambda: (_ for _ in ()).throw(ValueError())),
        })()
        with patch.object(type(session), "send", return_value=fake_response) as mock_send:
            executor.handle_request(None)
        _, kwargs = mock_send.call_args
        self.assertFalse(
            kwargs.get("allow_redirects", True),
            "restricted 首跳若允许自动重定向，逐跳策略校验形同虚设",
        )

    def test_unrestricted_first_hop_also_disables_auto_redirect(self):
        """默认路径同样手动逐跳（requests 自动跟随会对每个 3xx 读尽 hop body）。"""
        executor = HttpExecutor(url="http://127.0.0.1:1/a", method="GET", query=None,
                                body=None, headers=None, addressing=None, delegate=None)
        session = get_shared_sync_session()
        fake_response = type("R", (), {
            "is_redirect": False, "status_code": 200, "url": "http://127.0.0.1:1/a",
            "text": "{}", "headers": {},
            "json": staticmethod(lambda: (_ for _ in ()).throw(ValueError())),
        })()
        with patch.object(type(session), "send", return_value=fake_response) as mock_send:
            executor.handle_request(None)
        _, kwargs = mock_send.call_args
        self.assertFalse(
            kwargs.get("allow_redirects", True),
            "自动跟随会在 send 内部读尽 3xx 的 body，绕过响应体上限",
        )


class TestPolicyDNSCache(TestCase):
    def setUp(self):
        clear_dns_cache()
        self._old_ttl = os.environ.get("PLAITA_HTTP_DNS_TTL")
        os.environ["PLAITA_HTTP_DNS_TTL"] = "30"

    def tearDown(self):
        clear_dns_cache()
        if self._old_ttl is None:
            os.environ.pop("PLAITA_HTTP_DNS_TTL", None)
        else:
            os.environ["PLAITA_HTTP_DNS_TTL"] = self._old_ttl

    def test_resolution_cached_within_ttl(self):
        calls = []

        def fake_resolve(host, port):
            calls.append((host, port))
            return ["1.2.3.4"]

        with patch("plaita.node.http._resolve_host", side_effect=fake_resolve):
            self.assertEqual(_cached_resolve_host("h1", 80), ["1.2.3.4"])
            self.assertEqual(_cached_resolve_host("h1", 80), ["1.2.3.4"])
        self.assertEqual(len(calls), 1)

    def test_resolution_failure_not_cached(self):
        calls = []

        def fake_resolve(host, port):
            calls.append((host, port))
            return None

        with patch("plaita.node.http._resolve_host", side_effect=fake_resolve):
            self.assertIsNone(_cached_resolve_host("h2", 80))
            self.assertIsNone(_cached_resolve_host("h2", 80))
        self.assertEqual(len(calls), 2, "解析失败不得进入缓存（防瞬时抖动变周期全拒）")

    def test_ttl_zero_bypasses_cache(self):
        os.environ["PLAITA_HTTP_DNS_TTL"] = "0"
        calls = []

        def fake_resolve(host, port):
            calls.append(1)
            return ["5.6.7.8"]

        with patch("plaita.node.http._resolve_host", side_effect=fake_resolve):
            _cached_resolve_host("h3", 80)
            _cached_resolve_host("h3", 80)
        self.assertEqual(len(calls), 2)

    def test_host_allowed_stays_pure(self):
        """缺省 resolver 下 _host_allowed 每次现查（单测 monkeypatch 不被缓存污染）。"""
        calls = []

        def fake_getaddrinfo(host, port, proto=None):
            calls.append(host)
            return [(2, 1, 6, "", ("10.0.0.5", port or 80))]

        url = "http://internal.example/"
        with patch("plaita.node.http.socket.getaddrinfo", side_effect=fake_getaddrinfo):
            with self.assertRaises(http_mod.URLPolicyError):  # 10.x 是私网
                _host_allowed(url, None, None, True)
            with self.assertRaises(http_mod.URLPolicyError):
                _host_allowed(url, None, None, True)
        self.assertEqual(len(calls), 2, "纯函数路径不得吃缓存")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
