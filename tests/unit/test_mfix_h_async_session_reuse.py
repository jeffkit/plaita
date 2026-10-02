"""test_mfix_h_async_session_reuse — 修复包 MH2 回归：异步 HTTP 会话复用与隔离。

复核结论（MH 批次）：每请求 ``async with aiohttp.ClientSession()`` 已随
ec1a0de 消灭——flow 驱动层（plaita/core/async_utils）为每次 flow run 开一个
共享 ClientSession（contextvar 传递，作用域结束在同一 loop 上确定性关闭），
节点经 ``get_flow_session`` 复用；脱离 flow 驱动的直接调用退回一次性会话。
每 driver 形态下「per loop」与「per flow run」重合，故天然禁止跨 loop 复用
（aiohttp 硬约束）。本文件把 MH2 的硬性要求逐条钉住：

1. 同一作用域内两次请求共享同一 ClientSession 及其 connector——且连接层
   真复用（两次顺序请求只发生一次 TCP accept）；
2. 不同 event loop（不同 flow run）各自独立：session 对象不同、旧 session
   在其作用域结束时被确定性关闭；
3. 多租户 cookie 隔离：默认 DummyCookieJar——服务器 Set-Cookie 后下一个
   请求不带 Cookie 头；
4. 一次性会话兜底（脱离 flow 驱动的直接调用）：每请求独立、用完即关
   （async with），钉住兜底语义。

全程本地 http.server（127.0.0.1），零外网。
"""

from __future__ import annotations

import asyncio
import http.server
import os
import threading
import unittest
from unittest import TestCase
from unittest.mock import patch

import plaita.node.http as http_mod
from plaita.core.http_session import (
    close_flow_session,
    get_flow_session,
    open_flow_session,
)
from plaita.node.http import HttpExecutor


def _make_server():
    """HTTP/1.1 keep-alive 本地服务：按 accept 计连接、按请求记录 Cookie 头。"""
    state = {"conns": 0, "requests": 0, "cookie_headers": {}}

    class _Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            state["requests"] += 1
            state["cookie_headers"][self.path] = self.headers.get("Cookie")
            body = b'{"ok": true}'
            if self.path == "/setcookie":
                self.send_response(200)
                self.send_header("Set-Cookie", "sid=tenant-a; Path=/")
            else:
                self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):  # 静音
            pass

    class _Server(http.server.ThreadingHTTPServer):
        def get_request(self):
            state["conns"] += 1
            return super().get_request()

    server = _Server(("127.0.0.1", 0), _Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, state


def _executor(url):
    return HttpExecutor(url=url, method="GET", query=None, body=None,
                        headers=None, addressing=None, delegate=None)


class TestAsyncScopeSessionReuse(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._env = os.environ.pop("PLAITA_HTTP_COOKIES", None)
        self.server, self.state = _make_server()
        self.base = f"http://127.0.0.1:{self.server.server_address[1]}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        if self._env is not None:
            os.environ["PLAITA_HTTP_COOKIES"] = self._env
        else:
            os.environ.pop("PLAITA_HTTP_COOKIES", None)
        self.assertIsNone(get_flow_session(), "测试不得遗留打开的 flow 作用域")

    async def test_two_requests_share_one_clientsession_and_connection(self):
        executor = _executor(self.base)
        await open_flow_session()
        try:
            session = get_flow_session()
            self.assertIsNotNone(session)
            # 作用域内不得落一次性会话兜底（session 真被复用）
            with patch.object(
                http_mod, "_new_oneshot_session",
                side_effect=AssertionError("one-shot fallback used inside scope"),
            ):
                rsp1, err1 = await executor.handle_request_async(None)
                rsp2, err2 = await executor.handle_request_async(None)
            self.assertIsNone(err1)
            self.assertIsNone(err2)
            self.assertEqual(rsp1.res, {"ok": True})
            self.assertEqual(rsp2.res, {"ok": True})
            self.assertFalse(session.closed)
            # 连接层复用：同一作用域内两次顺序请求共用一条 TCP 连接
            self.assertEqual(
                self.state["conns"], 1,
                "两次顺序请求发生了多次 TCP accept：共享 connector 未被复用",
            )
        finally:
            await close_flow_session()
        self.assertTrue(session.closed, "作用域结束必须确定性关闭 session")

    async def test_set_cookie_not_resent_with_dummy_jar(self):
        await open_flow_session()
        try:
            await _executor(f"{self.base}/setcookie").handle_request_async(None)
            await _executor(f"{self.base}/after").handle_request_async(None)
        finally:
            await close_flow_session()
        self.assertIsNone(
            self.state["cookie_headers"]["/after"],
            "DummyCookieJar 下下一个请求不得携带 Set-Cookie 种下的 cookie",
        )


class TestAsyncLoopIndependence(TestCase):
    """不同 event loop（= 不同 flow run）各自独立：禁止跨 loop 复用；
    以及对已关闭 session 的迟到调用必须退回一次性会话兜底。"""

    def setUp(self):
        self._env = os.environ.pop("PLAITA_HTTP_COOKIES", None)
        self.server, _ = _make_server()
        self.base = f"http://127.0.0.1:{self.server.server_address[1]}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        if self._env is not None:
            os.environ["PLAITA_HTTP_COOKIES"] = self._env
        else:
            os.environ.pop("PLAITA_HTTP_COOKIES", None)

    def test_distinct_loops_get_distinct_sessions(self):
        async def scoped_run():
            await open_flow_session()
            try:
                return get_flow_session()
            finally:
                await close_flow_session()

        s1 = asyncio.run(scoped_run())
        s2 = asyncio.run(scoped_run())
        self.assertIsNot(s1, s2, "不同 flow run 不得共享 ClientSession（跨 loop 复用）")
        self.assertTrue(s1.closed, "前一个 loop 的 session 必须随作用域确定性关闭")

    def test_closed_session_falls_back_to_oneshot(self):
        """作用域异常提前关闭后迟到的请求：session.closed → 一次性会话兜底。"""
        import aiohttp

        async def make_closed():
            async with aiohttp.ClientSession() as session:
                return session

        stale = asyncio.run(make_closed())
        self.assertTrue(stale.closed)

        async def use_closed():
            with patch.object(http_mod, "get_flow_session",
                              side_effect=lambda: stale):
                return await _executor(self.base).handle_request_async(None)

        rsp, err = asyncio.run(use_closed())
        self.assertIsNone(err)
        self.assertEqual(rsp.res, {"ok": True})


class TestAsyncOneshotFallback(unittest.IsolatedAsyncioTestCase):
    """兜底路径（无 flow 驱动的直接调用）：每请求一次性会话、用完即关。"""

    def setUp(self):
        self._env = os.environ.pop("PLAITA_HTTP_COOKIES", None)
        self.server, _ = _make_server()
        self.base = f"http://127.0.0.1:{self.server.server_address[1]}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        if self._env is not None:
            os.environ["PLAITA_HTTP_COOKIES"] = self._env
        else:
            os.environ.pop("PLAITA_HTTP_COOKIES", None)

    async def test_each_out_of_scope_request_gets_its_own_closed_session(self):
        executor = _executor(self.base)
        self.assertIsNone(get_flow_session())

        made = []
        real_oneshot = http_mod._new_oneshot_session

        def recording_oneshot():
            session = real_oneshot()
            made.append(session)
            return session

        with patch.object(http_mod, "_new_oneshot_session",
                          side_effect=recording_oneshot):
            rsp1, err1 = await executor.handle_request_async(None)
            rsp2, err2 = await executor.handle_request_async(None)

        self.assertIsNone(err1)
        self.assertIsNone(err2)
        self.assertEqual(rsp1.res, {"ok": True})
        self.assertEqual(rsp2.res, {"ok": True})
        self.assertEqual(len(made), 2, "作用域外每请求应有独立一次性会话")
        self.assertIsNot(made[0], made[1])
        for session in made:
            self.assertTrue(session.closed, "一次性会话必须用完即关（async with）")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
