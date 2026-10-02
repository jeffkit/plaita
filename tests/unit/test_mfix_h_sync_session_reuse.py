"""test_mfix_h_sync_session_reuse — 修复包 MH1 回归：同步 HTTP 节点连接复用。

复核结论（MH 批次）：进程级共享 ``requests.Session`` 已随 ec1a0de 落地
（``plaita.core.http_session.get_shared_sync_session``：HTTPAdapter 连接池 +
PID fork 防护 + ``_BlockAllCookies`` 默认阻断 cookie），
``HttpExecutor.handle_request``（plaita/node/http.py）经它复用 urllib3 连接池。
本文件把 MH1 的硬性要求逐条钉住，防止回退成每 executor 新建 Session：

1. 两次节点执行共享同一 Session / HTTPAdapter / 连接池；
2. 连接层复用：干净连接池内两次顺序请求只发生一次 TCP accept
   （HTTP/1.1 keep-alive，服务端按 accept 计数——真实连接复用，非 mock）；
3. 多租户 cookie 隔离（默认阻断）：服务器 Set-Cookie 后
   ① cookie 不入库（jar 为空）、下一次请求不带 Cookie 头（含重定向逐跳，
   requests 的 resolve_redirects 会把 session jar 合并进下一跳——默认阻断下
   jar 恒空所以无害）；
   ② opt-in（PLAITA_HTTP_COOKIES=1）下 jar 可存储，但 executor 的
   ``Request.prepare()`` + ``session.send()`` 模式依旧不把 jar cookie 合并进
   下一个独立请求（requests 2.34.2 实证行为：jar 合并只发生在
   Session.prepare_request 与 resolve_redirects，send 不读 jar）；
4. 线程安全冒烟：多线程并发共享 session（urllib3 连接池线程安全 +
   只读 jar），请求全部成功、jar 保持为空。

全程本地 http.server（127.0.0.1），零外网。
"""

from __future__ import annotations

import concurrent.futures
import http.server
import os
import threading
import unittest
from unittest import TestCase

from plaita.core.http_session import (
    clear_shared_sync_session,
    get_shared_sync_session,
)
from plaita.node.http import HttpExecutor


def _make_server(set_cookie_paths=("/setcookie",)):
    """HTTP/1.1 keep-alive 本地服务：按 accept 计连接、按请求记录 Cookie 头。"""
    state = {"conns": 0, "requests": 0, "cookie_headers": {}}

    class _Handler(http.server.BaseHTTPRequestHandler):
        # 默认 HTTP/1.0 每响应后关连接，量不出连接复用——必须 1.1 + Content-Length
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            state["requests"] += 1
            state["cookie_headers"][self.path] = self.headers.get("Cookie")
            port = self.server.server_address[1]
            body = b'{"ok": true}'
            if self.path in set_cookie_paths:
                self.send_response(200)
                self.send_header("Set-Cookie", "sid=tenant-a; Path=/")
            elif self.path == "/redir":
                self.send_response(302)
                self.send_header("Location", f"http://127.0.0.1:{port}/final")
            else:
                self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):  # 静音
            pass

    class _Server(http.server.ThreadingHTTPServer):
        def get_request(self):  # 每个 TCP 连接恰好调用一次
            state["conns"] += 1
            return super().get_request()

    server = _Server(("127.0.0.1", 0), _Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, state


def _executor(url, restricted=False):
    return HttpExecutor(
        url=url, method="GET", query=None, body=None,
        headers=None, addressing=None, delegate=None,
        allowed_hosts=["127.0.0.1"] if restricted else None,
    )


class TestSharedSessionWiring(TestCase):
    """MH1 主张：连接池跨 executor/跨执行复用，不再每 executor 一个 Session。"""

    def setUp(self):
        clear_shared_sync_session()
        self.server, self.state = _make_server()
        self.base = f"http://127.0.0.1:{self.server.server_address[1]}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        clear_shared_sync_session()

    def test_executors_share_session_and_adapter(self):
        # 两个独立 executor（= 两次节点执行；errorHandler 重试同构）
        ex1, ex2 = _executor(f"{self.base}/a"), _executor(f"{self.base}/b")
        rsp1, err1 = ex1.handle_request({})
        rsp2, err2 = ex2.handle_request({})
        self.assertIsNone(err1)
        self.assertIsNone(err2)
        self.assertEqual(rsp1.res, {"ok": True})
        # 关键回归点：executor 不许各自新建 Session/连接池
        self.assertIsNot(ex1, ex2)
        self.assertIs(ex1.c, ex2.c)
        self.assertIs(ex1.c, get_shared_sync_session())
        adapter = ex1.c.get_adapter(f"{self.base}/a")
        self.assertIs(adapter, ex2.c.get_adapter(f"{self.base}/b"))

    def test_connection_reused_across_requests_in_clean_pool(self):
        # 干净池（setUp 已清 session）：两次顺序请求必须共用一条 TCP 连接
        _executor(f"{self.base}/r1").handle_request({})
        _executor(f"{self.base}/r2").handle_request({})
        self.assertEqual(
            self.state["conns"], 1,
            "两次顺序请求发生了多次 TCP accept：连接池复用失效",
        )
        self.assertEqual(self.state["requests"], 2)


class TestCookieIsolationDefaultBlocked(TestCase):
    """多租户硬要求：默认（PLAITA_HTTP_COOKIES 未开）cookie 不持久化。"""

    def setUp(self):
        clear_shared_sync_session()
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
        clear_shared_sync_session()

    def test_set_cookie_not_stored_and_not_resent(self):
        _executor(f"{self.base}/setcookie").handle_request({})
        _executor(f"{self.base}/after").handle_request({})
        session = get_shared_sync_session()
        self.assertEqual(len(session.cookies), 0, "默认策略下 cookie 不得入库")
        self.assertIsNone(
            self.state["cookie_headers"]["/after"],
            "下一个请求不得携带上一个响应种下的 cookie",
        )

    def test_set_cookie_not_leaked_via_redirect_hop(self):
        # unrestricted 自动跟随：requests 的 resolve_redirects 会把 session jar
        # 合并进下一跳（prepared_request._cookies.update(self.cookies)）——
        # 默认阻断下 jar 恒空，重定向逐跳也必须干净。
        _executor(f"{self.base}/setcookie").handle_request({})
        _executor(f"{self.base}/redir").handle_request({})
        self.assertEqual(len(get_shared_sync_session().cookies), 0)
        self.assertIsNone(self.state["cookie_headers"]["/final"],
                          "重定向跳不得携带此前响应种下的 cookie")


class TestCookieOptInBehaviour(TestCase):
    """PLAITA_HTTP_COOKIES=1：jar 可存储（文档化的 per-process 选择），
    但 executor 的 prepare()+send() 模式不把 jar cookie 发进独立请求。"""

    def setUp(self):
        clear_shared_sync_session()
        self._env = os.environ.pop("PLAITA_HTTP_COOKIES", None)
        os.environ["PLAITA_HTTP_COOKIES"] = "1"
        self.server, self.state = _make_server()
        self.base = f"http://127.0.0.1:{self.server.server_address[1]}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        if self._env is not None:
            os.environ["PLAITA_HTTP_COOKIES"] = self._env
        else:
            os.environ.pop("PLAITA_HTTP_COOKIES", None)
        clear_shared_sync_session()

    def test_optin_jar_stores_but_standalone_requests_stay_clean(self):
        # 钉住 requests 2.34.2 行为：Session.send 把响应 cookie 抽进 jar，
        # 但 executor 用 Request.prepare() 直接构造请求（不经
        # Session.prepare_request），send 不读 jar——独立请求仍不带 Cookie。
        _executor(f"{self.base}/setcookie").handle_request({})
        _executor(f"{self.base}/after").handle_request({})
        session = get_shared_sync_session()
        self.assertEqual(len(session.cookies), 1, "opt-in 下 cookie 应入库")
        self.assertIsNone(
            self.state["cookie_headers"]["/after"],
            "prepare()+send() 模式不得把 jar cookie 合并进下一个独立请求",
        )


class TestConcurrentSharedSession(TestCase):
    """线程安全冒烟：共享 session + 并发线程（urllib3 池线程安全）。"""

    def setUp(self):
        clear_shared_sync_session()
        self.server, self.state = _make_server()
        self.base = f"http://127.0.0.1:{self.server.server_address[1]}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        clear_shared_sync_session()

    def test_parallel_threads_share_session_safely(self):
        threads = 8
        per_thread = 4

        def worker(i):
            errors = []
            for j in range(per_thread):
                rsp, err = _executor(f"{self.base}/t{i}-{j}").handle_request({})
                if err is not None:
                    errors.append(err)
            return errors

        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as pool:
            all_errors = [
                e for errs in pool.map(worker, range(threads)) for e in errs
            ]
        self.assertEqual(all_errors, [])
        self.assertEqual(self.state["requests"], threads * per_thread)
        self.assertEqual(len(get_shared_sync_session().cookies), 0)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
