"""plaita#187：嵌套 flow 作用域不得关闭父作用域的共享 aiohttp session.

语义：``open_flow_session()`` 返回「本作用域是否创建了 session」的所有权标记；
``_flow_session_scoped`` / ``_flow_session_scoped_agen`` 只在自己创建时关闭。

历史缺陷：嵌套作用域（InlineFlow / Parallel 分支 / Map 每轮驱动）无条件
``close_flow_session()``——任一分支先结束就关掉父/兄弟分支正在复用的 session，
在途请求以 ``Connector is closed`` 失败，共享复用承诺（2026-10 BFF 热路径）
也随之失效。
"""
from __future__ import annotations

import asyncio
import http.server
import threading
import time
import unittest

import pytest

pytest.importorskip("aiohttp")


from plaita.core.async_utils import _flow_session_scoped, _flow_session_scoped_agen
from plaita.core.http_session import (
    close_flow_session,
    get_flow_session,
    open_flow_session,
)


class _SlowHandler(http.server.BaseHTTPRequestHandler):
    delay = 0.0

    def do_GET(self):  # noqa: N802 — stdlib handler API
        time.sleep(type(self).delay)
        body = b'{"ok": true}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):  # 静音
        pass


def _start_server(delay: float):
    handler = type("_Handler", (_SlowHandler,), {"delay": delay})
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


class TestNestedFlowSessionScope(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.assertIsNone(get_flow_session(), "测试不得遗留打开的 flow 作用域")

    async def test_open_returns_ownership_token(self):
        self.assertTrue(await open_flow_session(), "根本作用域应报告「我创建了 session」")
        try:
            self.assertFalse(
                await open_flow_session(),
                "嵌套作用域复用已有 session，必须报告 False（不拥有）",
            )
        finally:
            await close_flow_session()

    async def test_nested_scope_keeps_parent_session_open(self):
        """验收用例 A：子作用域结束后父 session 仍可复用、contextvar 不丢。"""
        await open_flow_session()
        outer = get_flow_session()
        self.assertIsNotNone(outer)
        try:
            await _flow_session_scoped(asyncio.sleep(0))
            self.assertIs(get_flow_session(), outer, "嵌套作用域不得清空 contextvar")
            self.assertFalse(outer.closed, "嵌套作用域不得关闭父 session")
            async with outer.get(f"http://127.0.0.1:{self._port()}") as rsp:
                self.assertEqual(rsp.status, 200)
        finally:
            await close_flow_session()

    async def test_nested_lazy_scope_keeps_parent_session_open(self):
        """lazy（async generator）驱动同款所有权语义。"""
        await open_flow_session()
        outer = get_flow_session()
        try:
            async def agen():
                yield 1

            seen = [item async for item in _flow_session_scoped_agen(agen())]
            self.assertEqual(seen, [1])
            self.assertIs(get_flow_session(), outer)
            self.assertFalse(outer.closed)
        finally:
            await close_flow_session()

    async def test_concurrent_branch_finishing_first_does_not_break_sibling(self):
        """验收用例 B：一支先结束，另一支在途请求仍成功（历史：Connector is closed）。"""
        server = _start_server(delay=0.6)
        self._server = server
        url = f"http://127.0.0.1:{server.server_address[1]}/slow"
        try:
            async def branch_b():
                session = get_flow_session()
                async with session.get(url) as rsp:
                    return {"status": rsp.status, "body": await rsp.json()}

            async def branch_a():
                await asyncio.sleep(0.15)  # 让 B 的请求先进入在途状态
                await _flow_session_scoped(asyncio.sleep(0))
                return "short-branch-done"

            async def both():
                return await asyncio.gather(
                    _flow_session_scoped(branch_a()),
                    _flow_session_scoped(branch_b()),
                    return_exceptions=True,
                )

            results = await _flow_session_scoped(both())
            self.assertEqual(results[0], "short-branch-done")
            self.assertIsInstance(
                results[1], dict,
                f"兄弟分支提前结束打断了在途请求: {results[1]!r}",
            )
            self.assertEqual(results[1]["status"], 200)
            self.assertEqual(results[1]["body"], {"ok": True})
        finally:
            server.shutdown()
            server.server_close()

    async def test_root_scope_closes_session(self):
        """验收用例 C：根作用域结束确实关闭（不泄漏连接）。"""
        session_box = {}

        async def probe():
            session_box["session"] = get_flow_session()
            return "done"

        self.assertEqual(await _flow_session_scoped(probe()), "done")
        self.assertIsNotNone(session_box["session"])
        self.assertTrue(session_box["session"].closed, "根作用域必须关闭自己创建的 session")
        self.assertIsNone(get_flow_session())

    def _port(self) -> int:
        server = getattr(self, "_server", None)
        if server is None:
            server = self._server = _start_server(delay=0.0)
        return server.server_address[1]

    async def asyncTearDown(self):
        server = getattr(self, "_server", None)
        if server is not None:
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    unittest.main()
