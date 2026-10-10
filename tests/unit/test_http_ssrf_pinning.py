"""#31 回归：HTTP 节点 SSRF 加固——运营者级开关 / DNS pinning / 响应体上限。

三件事：

1. **运营者级开关**：``PLAITA_HTTP_BLOCK_PRIVATE=1`` 后节点声明只能更严不能更松
   （effective = 节点声明 OR 运营者开关）；
2. **DNS pinning**：校验与建连同源——连接期解析经同一策略判定，并只连校验过的
   IP；攻击者权威 DNS「先答公网过校验、连接时答内网」不再穿透。断言方式：让一个
   **真实不存在**的域名解析到 127.0.0.1，请求能打到本地 server（证明连接用了我们
   解析的 IP，且 Host 头仍是原域名）；再用「预检返回公网、建连返回私网」的序列
   证明 rebinding 在建连期被拦下；
3. **响应体上限**：``response.text`` 换流式读 + 字节上限，超限报
   ``ResponseTooLargeError`` 而非把 GB 级 body 读进内存/状态。

全程本地 http.server，零外网。
"""

from __future__ import annotations

import asyncio
import http.server
import json
import ssl
import threading

import pytest

from plaita.node import http as http_mod
from plaita.node.http import (
    DEFAULT_MAX_RESPONSE_BYTES,
    HttpExecutor,
    ResponseTooLargeError,
    URLPolicyError,
    clear_dns_cache,
    http_max_response_bytes,
)


def _exec(url="http://127.0.0.1:1/x", **kw):
    return HttpExecutor(url=url, method="GET", query=None, body=None,
                        headers=None, addressing=None, delegate=None, **kw)


def _exec_req(url, method="GET", body=None, headers=None, **kw):
    """``_exec`` 的可指定 method/body/headers 版本（重定向语义用例需要）。"""
    return HttpExecutor(url=url, method=method, query=None, body=body, headers=headers,
                        addressing=None, delegate=None, **kw)


def _start_server(body=b'{"ok": true}'):
    seen_hosts = []

    class _Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            seen_hosts.append(self.headers.get("Host"))
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):  # 静音
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, seen_hosts


def _start_chunked_server(body, chunk=4096):
    """分多次 flush 写出 body——逼出「单次 content.read(n) 只返回非 EOF 分片」。"""

    class _Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            for i in range(0, len(body), chunk):
                self.wfile.write(body[i:i + chunk])
                self.wfile.flush()

        def log_message(self, *args):  # 静音
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv(http_mod.HTTP_BLOCK_PRIVATE_ENV, raising=False)
    monkeypatch.delenv(http_mod.HTTP_MAX_RESPONSE_BYTES_ENV, raising=False)
    clear_dns_cache()
    yield
    clear_dns_cache()


# ---------------------------------------------------------------------------
# 1. 运营者级开关
# ---------------------------------------------------------------------------

class TestOperatorBlockPrivate:
    def test_env_off_keeps_historical_default(self):
        ex = _exec(block_private_networks=False)
        assert ex._restrictions_active is False
        ex._check_policy("http://127.0.0.1:1/x")  # 不抛

    @pytest.mark.parametrize("value", ["1", "true", "yes", "YES"])
    def test_env_forces_block_even_when_node_says_false(self, monkeypatch, value):
        monkeypatch.setenv(http_mod.HTTP_BLOCK_PRIVATE_ENV, value)
        ex = _exec(block_private_networks=False)
        assert ex._effective_block_private is True
        assert ex._restrictions_active is True
        with pytest.raises(URLPolicyError):
            ex._check_policy("http://127.0.0.1:1/x")

    def test_env_node_cannot_loosen_but_can_tighten(self, monkeypatch):
        monkeypatch.setenv(http_mod.HTTP_BLOCK_PRIVATE_ENV, "1")
        # 节点显式写 False 也无法关闭运营者强制（字面 IP 走本地 getaddrinfo）
        with pytest.raises(URLPolicyError):
            _exec(block_private_networks=False)._check_policy("http://127.0.0.1:1/x")
        # 节点额外收紧（allowed_hosts）与运营者 block 叠加
        monkeypatch.setattr(http_mod, "_cached_resolve_host",
                            lambda host, port: ["93.184.216.34"])
        with pytest.raises(URLPolicyError):
            _exec(block_private_networks=False,
                  allowed_hosts=["only.example"])._check_policy("http://other.example/")

    def test_http_node_declaration_cannot_loosen_operator_switch(self, monkeypatch):
        monkeypatch.setenv(http_mod.HTTP_BLOCK_PRIVATE_ENV, "1")
        node = http_mod.HTTP(id="h1", url="http://127.0.0.1/",
                             blockPrivateNetworks=False)
        ex = node.new_executor(None)
        assert ex._restrictions_active is True
        with pytest.raises(URLPolicyError):
            ex._check_policy("http://127.0.0.1/")

    def test_operator_switch_blocks_request_end_to_end(self, monkeypatch):
        monkeypatch.setenv(http_mod.HTTP_BLOCK_PRIVATE_ENV, "1")
        server, _ = _start_server()
        port = server.server_address[1]
        monkeypatch.setattr(http_mod, "_cached_resolve_host",
                            lambda host, p: ["127.0.0.1"])
        try:
            rsp, err = _exec(url=f"http://int.invalid:{port}/x").handle_request({})
        finally:
            server.shutdown()
            server.server_close()
        assert isinstance(err, URLPolicyError), err


# ---------------------------------------------------------------------------
# 2. DNS pinning
# ---------------------------------------------------------------------------

def _pin_alias(monkeypatch, ip):
    """把任意主机名的解析钉到 ip（真实 DNS 不参与）。"""
    monkeypatch.setattr(http_mod, "_cached_resolve_host", lambda host, port: [ip])


class TestConnectTimePinning:
    def test_sync_connects_to_policy_resolved_ip(self, monkeypatch):
        """不存在的域名 pin 到 127.0.0.1：请求成功即证明连接用了我们解析的 IP。"""
        server, seen_hosts = _start_server()
        port = server.server_address[1]
        _pin_alias(monkeypatch, "127.0.0.1")
        try:
            ex = _exec(url=f"http://pinned.invalid:{port}/x",
                       allowed_hosts=["pinned.invalid"])
            rsp, err = ex.handle_request({})
        finally:
            server.shutdown()
            server.server_close()
        assert err is None, err
        assert rsp.res == {"ok": True}
        # pinning 只作用于 socket 目标：Host 头仍是原域名（含端口）
        assert seen_hosts == [f"pinned.invalid:{port}"]

    def test_async_connects_to_policy_resolved_ip(self, monkeypatch):
        server, seen_hosts = _start_server()
        port = server.server_address[1]
        _pin_alias(monkeypatch, "127.0.0.1")
        try:
            ex = _exec(url=f"http://pinned.invalid:{port}/x",
                       allowed_hosts=["pinned.invalid"])
            rsp, err = asyncio.run(ex.handle_request_async(None))
        finally:
            server.shutdown()
            server.server_close()
        assert err is None, err
        assert rsp.res == {"ok": True}
        assert seen_hosts == [f"pinned.invalid:{port}"]

    def test_sync_falls_back_across_resolved_ips(self, monkeypatch):
        """多 A 记录：首个地址拒连时回退到下一个（不能只试第一个）。"""
        server, _ = _start_server()
        port = server.server_address[1]
        monkeypatch.setattr(http_mod, "_cached_resolve_host",
                            lambda host, p: ["127.0.0.2", "127.0.0.1"])
        try:
            ex = _exec(url=f"http://multi.invalid:{port}/x",
                       allowed_hosts=["multi.invalid"])
            rsp, err = ex.handle_request({})
        finally:
            server.shutdown()
            server.server_close()
        assert err is None, err
        assert rsp.res == {"ok": True}

    def test_sync_rebinding_blocked_at_connect(self, monkeypatch):
        """预检答公网 IP 过校验、建连答私网 IP：连接期策略必须拦下。"""
        server, _ = _start_server()
        port = server.server_address[1]
        answers = iter(["1.2.3.4", "10.0.0.1"])
        monkeypatch.setattr(http_mod, "_cached_resolve_host",
                            lambda host, p: [next(answers)])
        try:
            ex = _exec(url=f"http://rebind.invalid:{port}/x",
                       block_private_networks=True)
            rsp, err = ex.handle_request({})
        finally:
            server.shutdown()
            server.server_close()
        assert isinstance(err, URLPolicyError), err
        assert "private" in str(err)

    def test_async_rebinding_blocked_at_connect(self, monkeypatch):
        server, _ = _start_server()
        port = server.server_address[1]
        answers = iter(["1.2.3.4", "10.0.0.1"])
        monkeypatch.setattr(http_mod, "_cached_resolve_host",
                            lambda host, p: [next(answers)])
        try:
            ex = _exec(url=f"http://rebind.invalid:{port}/x",
                       block_private_networks=True)
            rsp, err = asyncio.run(ex.handle_request_async(None))
        finally:
            server.shutdown()
            server.server_close()
        assert isinstance(err, URLPolicyError), err
        assert "private" in str(err)

    def test_unresolvable_host_blocked_before_connect(self, monkeypatch):
        """解析失败（返回 None）时 restricted 请求必须拒绝而非放行。"""
        monkeypatch.setattr(http_mod, "_cached_resolve_host", lambda host, p: None)
        ex = _exec(url="http://nope.invalid/x", block_private_networks=True)
        rsp, err = ex.handle_request({})
        assert isinstance(err, URLPolicyError), err


# ---------------------------------------------------------------------------
# 3. 响应体上限
# ---------------------------------------------------------------------------

class TestResponseByteCap:
    def test_env_parsing(self, monkeypatch):
        assert http_max_response_bytes() == DEFAULT_MAX_RESPONSE_BYTES
        monkeypatch.setenv(http_mod.HTTP_MAX_RESPONSE_BYTES_ENV, "4096")
        assert http_max_response_bytes() == 4096
        monkeypatch.setenv(http_mod.HTTP_MAX_RESPONSE_BYTES_ENV, "0")
        assert http_max_response_bytes() == DEFAULT_MAX_RESPONSE_BYTES
        monkeypatch.setenv(http_mod.HTTP_MAX_RESPONSE_BYTES_ENV, "bogus")
        assert http_max_response_bytes() == DEFAULT_MAX_RESPONSE_BYTES

    def test_sync_over_cap_raises(self, monkeypatch):
        server, _ = _start_server(body=b"x" * 128)
        port = server.server_address[1]
        monkeypatch.setenv(http_mod.HTTP_MAX_RESPONSE_BYTES_ENV, "16")
        try:
            rsp, err = _exec(url=f"http://127.0.0.1:{port}/big").handle_request({})
        finally:
            server.shutdown()
            server.server_close()
        assert isinstance(err, ResponseTooLargeError), err

    def test_async_over_cap_raises(self, monkeypatch):
        server, _ = _start_server(body=b"x" * 128)
        port = server.server_address[1]
        monkeypatch.setenv(http_mod.HTTP_MAX_RESPONSE_BYTES_ENV, "16")
        try:
            rsp, err = asyncio.run(
                _exec(url=f"http://127.0.0.1:{port}/big").handle_request_async(None))
        finally:
            server.shutdown()
            server.server_close()
        assert isinstance(err, ResponseTooLargeError), err

    def test_async_over_cap_raises_on_chunked_body(self, monkeypatch):
        server = _start_chunked_server(b"x" * 200000)
        port = server.server_address[1]
        monkeypatch.setenv(http_mod.HTTP_MAX_RESPONSE_BYTES_ENV, "4096")
        try:
            rsp, err = asyncio.run(
                _exec(url=f"http://127.0.0.1:{port}/big").handle_request_async(None))
        finally:
            server.shutdown()
            server.server_close()
        assert isinstance(err, ResponseTooLargeError), err

    def test_under_cap_still_decodes_json(self, monkeypatch):
        server, _ = _start_server(body=b'{"ok": true}')
        port = server.server_address[1]
        monkeypatch.setenv(http_mod.HTTP_MAX_RESPONSE_BYTES_ENV, "1024")
        try:
            rsp, err = _exec(url=f"http://127.0.0.1:{port}/small").handle_request({})
        finally:
            server.shutdown()
            server.server_close()
        assert err is None, err
        assert rsp.res == {"ok": True}

    def test_async_under_cap_still_decodes_json(self, monkeypatch):
        server, _ = _start_server(body=b'{"ok": true}')
        port = server.server_address[1]
        try:
            rsp, err = asyncio.run(
                _exec(url=f"http://127.0.0.1:{port}/small").handle_request_async(None))
        finally:
            server.shutdown()
            server.server_close()
        assert err is None, err
        assert rsp.res == {"ok": True}

    def test_async_large_body_under_cap_read_fully(self):
        """大 body 分片到达时不得被单次 read() 静默截断（非 EOF 分片）。"""
        pad = "x" * 200000
        body = ('{"pad": "' + pad + '"}').encode("utf-8")
        server = _start_chunked_server(body)
        port = server.server_address[1]
        try:
            rsp, err = asyncio.run(
                _exec(url=f"http://127.0.0.1:{port}/chunked").handle_request_async(None))
        finally:
            server.shutdown()
            server.server_close()
        assert err is None, err
        assert rsp.res["pad"] == pad

    def test_sync_large_body_under_cap_read_fully(self):
        pad = "x" * 200000
        body = ('{"pad": "' + pad + '"}').encode("utf-8")
        server = _start_chunked_server(body)
        port = server.server_address[1]
        try:
            rsp, err = _exec(url=f"http://127.0.0.1:{port}/chunked").handle_request({})
        finally:
            server.shutdown()
            server.server_close()
        assert err is None, err
        assert rsp.res["pad"] == pad


# ---------------------------------------------------------------------------
# 4. 重定向 hop body（评审阻断项②：自动跟随会无条件读尽每个 3xx 的 body）
# ---------------------------------------------------------------------------

def _start_holding_redirect_server():
    """302 头后一个字节 body 都不写、连接保持到客户端关：读 hop body 必失败/超时。"""

    class _Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            port = self.server.server_address[1]
            if self.path == "/redir":
                self.send_response(302)
                self.send_header("Location", f"http://127.0.0.1:{port}/final")
                self.send_header("Content-Length", str(8 * 1024 * 1024))
                self.end_headers()
                self.connection.settimeout(3.0)
                try:
                    self.connection.recv(1)
                except OSError:
                    pass
                self.close_connection = True
                return
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


def _start_method_recording_redirect_server(seen):
    """/moved 302 → /target，/temp 307 → /target；/target 记录方法+body。"""

    class _Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def _body(self):
            length = int(self.headers.get("Content-Length", 0) or 0)
            return self.rfile.read(length) if length else b""

        def _redirect(self, status):
            port = self.server.server_address[1]
            self.send_response(status)
            self.send_header("Location", f"http://127.0.0.1:{port}/target")
            self.send_header("Content-Length", "0")
            self.end_headers()

        def _do(self):
            if self.path == "/moved":
                self._body()
                self._redirect(302)
                return
            if self.path == "/temp":
                self._body()
                self._redirect(307)
                return
            seen.append((self.command, self._body()))
            body = b'{"ok": true}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        do_GET = do_POST = _do

        def log_message(self, *args):  # 静音
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def _start_simple_redirect_server():
    class _Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            port = self.server.server_address[1]
            if self.path == "/redir":
                self.send_response(302)
                self.send_header("Location", f"http://127.0.0.1:{port}/final")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
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


class TestRedirectHopBodyNotRead:
    """默认（无策略）与 restricted 两条路径都不得把 3xx 的 body 读进内存。"""

    def test_sync_default_path_does_not_read_hop_body(self):
        server = _start_holding_redirect_server()
        port = server.server_address[1]
        try:
            rsp, err = _exec(url=f"http://127.0.0.1:{port}/redir",
                             request_timeout=2.0).handle_request({})
        finally:
            server.shutdown()
            server.server_close()
        # hop body 一个字节都没发：读它（旧行为）必然超时/连接错误
        assert err is None, err
        assert rsp.res == {"ok": True}

    def test_sync_restricted_path_does_not_read_hop_body(self, monkeypatch):
        server = _start_holding_redirect_server()
        port = server.server_address[1]
        _pin_alias(monkeypatch, "127.0.0.1")
        try:
            rsp, err = _exec(url=f"http://127.0.0.1:{port}/redir",
                             allowed_hosts=["127.0.0.1"],
                             request_timeout=2.0).handle_request({})
        finally:
            server.shutdown()
            server.server_close()
        assert err is None, err
        assert rsp.res == {"ok": True}

    def test_async_default_path_does_not_read_hop_body(self):
        """async 默认路径同样逐跳手动跟随：3xx 的 body 一个字节都不读。"""
        server = _start_holding_redirect_server()
        port = server.server_address[1]
        try:
            rsp, err = asyncio.run(
                _exec(url=f"http://127.0.0.1:{port}/redir",
                      request_timeout=2.0).handle_request_async(None))
        finally:
            server.shutdown()
            server.server_close()
        assert err is None, err
        assert rsp.res == {"ok": True}

    def test_sync_default_path_still_follows_redirects(self):
        server = _start_simple_redirect_server()
        port = server.server_address[1]
        try:
            rsp, err = _exec(url=f"http://127.0.0.1:{port}/redir").handle_request({})
        finally:
            server.shutdown()
            server.server_close()
        assert err is None, err
        assert rsp.res == {"ok": True}

    def test_sync_redirect_semantics_match_requests(self):
        """302 → GET 丢弃 body；307 保留方法与 body（与 requests 一致）。"""
        seen = []
        server = _start_method_recording_redirect_server(seen)
        port = server.server_address[1]
        body = {"a": 1}

        def post(url):
            return HttpExecutor(url=url, method="POST", query=None, body=body,
                                headers=None, addressing=None, delegate=None)

        try:
            rsp, err = post(f"http://127.0.0.1:{port}/moved").handle_request({})
            assert err is None, err
            assert rsp.res == {"ok": True}
            rsp, err = post(f"http://127.0.0.1:{port}/temp").handle_request({})
        finally:
            server.shutdown()
            server.server_close()
        assert err is None, err
        assert rsp.res == {"ok": True}
        assert seen[0] == ("GET", b"")
        assert seen[1][0] == "POST"
        assert json.loads(seen[1][1]) == {"a": 1}

    def test_async_restricted_307_keeps_method_and_body(self, monkeypatch):
        """307/308 保留方法**与 body**：async restricted 与 sync 同规则。

        （评审补丁：async 逐跳跳过的 data 恒为 None，307 会把 body 丢掉，与
        MIGRATION 里「307/308 保留方法与 body」的说法不符。）
        """
        seen = []
        server = _start_method_recording_redirect_server(seen)
        port = server.server_address[1]
        _pin_alias(monkeypatch, "127.0.0.1")
        try:
            ex = HttpExecutor(url=f"http://127.0.0.1:{port}/temp", method="POST", query=None,
                              body={"a": 1}, headers=None, addressing=None, delegate=None,
                              allowed_hosts=["127.0.0.1"])
            rsp, err = asyncio.run(ex.handle_request_async(None))
        finally:
            server.shutdown()
            server.server_close()
        assert err is None, err
        assert rsp.res == {"ok": True}
        assert seen[0][0] == "POST"
        assert json.loads(seen[0][1]) == {"a": 1}


# ---------------------------------------------------------------------------
# 5. 代理环境变量不得旁路连接期 pinning（评审阻断项③）
# ---------------------------------------------------------------------------

def _start_recording_proxy(seen):
    class _Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            seen.append(self.requestline)
            body = b'{"ok": "via-proxy"}'
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


class TestPolicySessionIgnoresProxyEnv:
    def test_proxy_env_cannot_bypass_pinning(self, monkeypatch):
        """HTTP_PROXY 置位时请求仍必须直连 pinning 目标（代理侧解析=绕过校验）。"""
        server, seen_hosts = _start_server()
        port = server.server_address[1]
        proxied = []
        proxy = _start_recording_proxy(proxied)
        monkeypatch.setenv("HTTP_PROXY", f"http://127.0.0.1:{proxy.server_address[1]}")
        _pin_alias(monkeypatch, "127.0.0.1")
        try:
            ex = _exec(url=f"http://pinned.invalid:{port}/x",
                       allowed_hosts=["pinned.invalid"])
            rsp, err = ex.handle_request({})
        finally:
            server.shutdown()
            server.server_close()
            proxy.shutdown()
            proxy.server_close()
        assert err is None, err
        assert rsp.res == {"ok": True}
        assert seen_hosts == [f"pinned.invalid:{port}"]
        assert proxied == [], "restricted 请求被交给代理：连接期 pinning 被旁路"


# ---------------------------------------------------------------------------
# 6. 重定向语义（复审 R1/R2/R3：跨源定义 / 跟随上限 / 方法改写）
# ---------------------------------------------------------------------------

def _start_redirect_recorder(seen):
    """``/redir/<status>`` 按 status 跳到 ``/target``；/target 记录 (方法, body, Authorization)。"""

    class _Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def _body(self):
            length = int(self.headers.get("Content-Length", 0) or 0)
            return self.rfile.read(length) if length else b""

        def _do(self):
            port = self.server.server_address[1]
            if self.path.startswith("/redir/"):
                status = int(self.path.rsplit("/", 1)[1])
                self._body()
                self.send_response(status)
                self.send_header("Location", f"http://127.0.0.1:{port}/target")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            seen.append((self.command, self._body(), self.headers.get("Authorization")))
            body = b'{"ok": true}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        do_GET = do_POST = do_PUT = do_PATCH = do_DELETE = do_OPTIONS = _do

        def log_message(self, *args):  # 静音
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def _start_redirector(location):
    """所有请求都 302 到 ``location``（跨 server 跳用）。"""

    class _Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            self.send_response(302)
            self.send_header("Location", location)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *args):  # 静音
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def _start_header_recorder(seen):
    class _Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            seen.append((self.path, dict(self.headers)))
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


class TestRedirectCrossOrigin:
    """跨源判定对齐 ``requests.Session.should_strip_auth``（复审 R1）。

    旧实现只比 hostname：https→http / 同 host 换端口会把 Authorization 明文
    带到另一个 origin，而自动跟随的 requests 会剥离。
    """

    @pytest.mark.parametrize("from_url,to_url,stripped", [
        ("https://h/a", "http://h/b", True),            # scheme 降级
        ("https://h:443/a", "https://h:8443/b", True),  # 同 scheme 换端口
        ("http://h:8080/a", "http://h/b", True),        # 非默认端口 → 默认端口
        ("http://h/a", "https://h/b", False),           # requests 例外：http:80 → https:443
        ("http://h/a", "http://h/b", False),            # 同源
        ("https://h/a", "https://h/b", False),          # 同源（默认端口写法不同）
        ("http://h1/a", "http://h2/b", True),           # host 变化
    ])
    def test_strip_definition(self, from_url, to_url, stripped):
        headers = {"Authorization": "Bearer SECRET", "Cookie": "c=1", "X-Keep": "1"}
        out = http_mod._strip_sensitive_headers(dict(headers), from_url, to_url)
        assert ("Authorization" in out) is (not stripped)
        assert ("Cookie" in out) is (not stripped)
        assert out["X-Keep"] == "1"

    def test_same_host_port_change_strips_end_to_end(self):
        """两个本地 server、同 host 不同端口：Authorization 不得跟到 :B。"""
        seen = []
        final = _start_header_recorder(seen)
        redir = _start_redirector(f"http://127.0.0.1:{final.server_address[1]}/final")
        try:
            ex = _exec_req(url=f"http://127.0.0.1:{redir.server_address[1]}/start",
                           headers={"Authorization": "Bearer SECRET"})
            rsp, err = ex.handle_request({})
        finally:
            redir.shutdown()
            redir.server_close()
            final.shutdown()
            final.server_close()
        assert err is None, err
        assert rsp.res == {"ok": True}
        assert seen[0][0] == "/final"
        assert "Authorization" not in seen[0][1]

    def test_same_host_port_change_strips_end_to_end_async(self):
        """async 同规则（此前 aiohttp 自动跟随只按 host 判定，凭据会带过去）。"""
        seen = []
        final = _start_header_recorder(seen)
        redir = _start_redirector(f"http://127.0.0.1:{final.server_address[1]}/final")
        try:
            ex = _exec_req(url=f"http://127.0.0.1:{redir.server_address[1]}/start",
                           headers={"Authorization": "Bearer SECRET"})
            rsp, err = asyncio.run(ex.handle_request_async(None))
        finally:
            redir.shutdown()
            redir.server_close()
            final.shutdown()
            final.server_close()
        assert err is None, err
        assert rsp.res == {"ok": True}
        assert seen[0][0] == "/final"
        assert "Authorization" not in seen[0][1]

    def test_same_origin_redirect_keeps_credentials(self):
        """同 host 同端口的重定向保留 Authorization（未过度剥离）。"""
        seen = []
        server = _start_redirect_recorder(seen)
        port = server.server_address[1]
        try:
            ex = _exec_req(url=f"http://127.0.0.1:{port}/redir/302",
                           headers={"Authorization": "Bearer SECRET"})
            rsp, err = ex.handle_request({})
        finally:
            server.shutdown()
            server.server_close()
        assert err is None, err
        assert seen[0][2] == "Bearer SECRET"


class TestRedirectMethodSemantics:
    """方法改写逐条对齐 ``requests.Session.rebuild_method``（复审 R3）。

    旧实现把 301 的 PUT/PATCH/DELETE/OPTIONS 也转成 GET。
    """

    @pytest.mark.parametrize("status,method,expected_method,keeps_body", [
        (301, "POST", "GET", False),      # requests：仅 POST 的 301 转 GET
        (301, "PUT", "PUT", False),       # 其余方法保留，body 仍丢弃
        (301, "DELETE", "DELETE", False),
        (302, "POST", "GET", False),
        (302, "PUT", "GET", False),
        (303, "PUT", "GET", False),
        (307, "PUT", "PUT", True),
        (308, "PATCH", "PATCH", True),
    ])
    def test_sync_redirect_method(self, status, method, expected_method, keeps_body):
        seen = []
        server = _start_redirect_recorder(seen)
        port = server.server_address[1]
        try:
            rsp, err = _exec_req(url=f"http://127.0.0.1:{port}/redir/{status}",
                                 method=method, body={"a": 1}).handle_request({})
        finally:
            server.shutdown()
            server.server_close()
        assert err is None, err
        assert rsp.res == {"ok": True}
        got_method, got_body, _ = seen[0]
        assert got_method == expected_method
        assert (got_body != b"") is keeps_body

    @pytest.mark.parametrize("status,method,expected_method,keeps_body", [
        (301, "POST", "GET", False),
        (301, "PUT", "PUT", False),
        (302, "PUT", "GET", False),
        (307, "PUT", "PUT", True),
    ])
    def test_async_default_path_same_rules(self, status, method, expected_method, keeps_body):
        """async 默认路径同样逐跳手动跟随：不再有 aiohttp 自己的规则。

        此前 aiohttp 自动跟随对 301/302 的非 POST 方法**保留 body**，与同步
        路径（requests）不一致。
        """
        seen = []
        server = _start_redirect_recorder(seen)
        port = server.server_address[1]
        try:
            rsp, err = asyncio.run(
                _exec_req(url=f"http://127.0.0.1:{port}/redir/{status}",
                          method=method, body={"a": 1}).handle_request_async(None))
        finally:
            server.shutdown()
            server.server_close()
        assert err is None, err
        assert rsp.res == {"ok": True}
        got_method, got_body, _ = seen[0]
        assert got_method == expected_method
        assert (got_body != b"") is keeps_body


def _start_chain_server():
    """``/chain/<n>`` 302 到 ``/chain/<n-1>``；``/chain/0`` 返回 200。"""

    class _Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            port = self.server.server_address[1]
            if self.path.startswith("/chain/"):
                n = int(self.path.rsplit("/", 1)[1])
                if n > 0:
                    self.send_response(302)
                    self.send_header("Location", f"http://127.0.0.1:{port}/chain/{n - 1}")
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
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


class TestRedirectCap:
    """上限统一为节点 ``maxRedirects``（复审 R2）。

    此前默认路径走 requests 的 30（async 走 aiohttp 的 10），节点声明的
    maxRedirects 只在策略激活时生效——默认路径静默收紧成 5 且 sync/async 不一致。
    """

    def test_sync_default_path_caps_at_max_redirects(self):
        server = _start_chain_server()
        port = server.server_address[1]
        try:
            rsp, err = _exec(url=f"http://127.0.0.1:{port}/chain/5").handle_request({})
            assert err is None, err
            assert rsp.res == {"ok": True}
            rsp, err = _exec(url=f"http://127.0.0.1:{port}/chain/6").handle_request({})
        finally:
            server.shutdown()
            server.server_close()
        assert "Too many redirects (> 5)" in str(err)

    def test_async_default_path_caps_at_max_redirects(self):
        """自动跟随（async 默认路径）与同步路径同口径、同文案。"""
        server = _start_chain_server()
        port = server.server_address[1]
        try:
            rsp, err = asyncio.run(
                _exec(url=f"http://127.0.0.1:{port}/chain/5").handle_request_async(None))
            assert err is None, err
            assert rsp.res == {"ok": True}
            rsp, err = asyncio.run(
                _exec(url=f"http://127.0.0.1:{port}/chain/6").handle_request_async(None))
        finally:
            server.shutdown()
            server.server_close()
        assert "Too many redirects (> 5)" in str(err)

    def test_max_redirects_is_configurable(self):
        server = _start_chain_server()
        port = server.server_address[1]
        try:
            rsp, err = _exec(url=f"http://127.0.0.1:{port}/chain/8",
                             max_redirects=10).handle_request({})
        finally:
            server.shutdown()
            server.server_close()
        assert err is None, err
        assert rsp.res == {"ok": True}


class TestMidChainPolicyViolationFrame:
    """逐跳策略拒绝时错误帧带「将要尝试的下一跳」（sync/async 一致）。"""

    def _redirect_to_blocked_host(self):
        seen = []
        final = _start_header_recorder(seen)
        redir = _start_redirector(f"http://blocked.invalid:{final.server_address[1]}/final")
        return redir, final, seen

    def test_sync_frame_carries_next_hop(self):
        redir, final, seen = self._redirect_to_blocked_host()
        target = f"http://blocked.invalid:{final.server_address[1]}/final"
        try:
            ex = _exec(url=f"http://127.0.0.1:{redir.server_address[1]}/start",
                       allowed_hosts=["127.0.0.1"])
            rsp, err = ex.handle_request({})
        finally:
            redir.shutdown()
            redir.server_close()
            final.shutdown()
            final.server_close()
        assert isinstance(err, URLPolicyError), err
        assert rsp.raw_request.url == target
        assert seen == []

    def test_async_frame_carries_next_hop(self):
        redir, final, seen = self._redirect_to_blocked_host()
        target = f"http://blocked.invalid:{final.server_address[1]}/final"
        try:
            ex = _exec(url=f"http://127.0.0.1:{redir.server_address[1]}/start",
                       allowed_hosts=["127.0.0.1"])
            rsp, err = asyncio.run(ex.handle_request_async(None))
        finally:
            redir.shutdown()
            redir.server_close()
            final.shutdown()
            final.server_close()
        assert isinstance(err, URLPolicyError), err
        assert rsp.raw_request.url == target
        assert seen == []


# ---------------------------------------------------------------------------
# 7. pinned HTTPS（SNI / Host / 证书校验；复审：此前只有 HTTP 路径有回归）
# ---------------------------------------------------------------------------

def _make_test_ca(tmp_path, domain="pinned.invalid"):
    """自签 CA + 域名证书（只换信任锚，不改动节点侧任何校验逻辑）。"""
    pytest.importorskip("cryptography")
    import datetime

    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    now = datetime.datetime.now(datetime.timezone.utc)
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "plaita-test-ca")])
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(ca_name).issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(ca_key, hashes.SHA256())
    )
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, domain)]))
        .issuer_name(ca_name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName(domain)]), critical=False)
        .sign(ca_key, hashes.SHA256())
    )
    ca_path = tmp_path / "ca.pem"
    ca_path.write_bytes(ca_cert.public_bytes(serialization.Encoding.PEM))
    chain_path = tmp_path / "server.pem"
    chain_path.write_bytes(
        cert.public_bytes(serialization.Encoding.PEM)
        + key.private_bytes(serialization.Encoding.PEM,
                            serialization.PrivateFormat.TraditionalOpenSSL,
                            serialization.NoEncryption())
    )
    return ca_path, chain_path


def _start_tls_server(chain_path, seen_hosts):
    class _Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            seen_hosts.append(self.headers.get("Host"))
            body = b'{"ok": true}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):  # 静音
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.load_cert_chain(str(chain_path))
    server.socket = tls.wrap_socket(server.socket, server_side=True)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def _sync_session_trusting(monkeypatch, ca_path):
    """把 sync 策略 session 的信任锚指向测试 CA（requests 公开 API：session.verify）。

    pinning adapter / 连接目标 / SNI 全部不变——换的只是信任锚。
    """
    real_cls = http_mod._sync_session_cls()

    class _CATrustSession(real_cls):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.verify = str(ca_path)

    monkeypatch.setattr(http_mod, "_sync_session_cls", lambda: _CATrustSession)


def _async_trust(monkeypatch, ca_path):
    """把 aiohttp「verify_ssl=True」用的默认 SSLContext 换成信任测试 CA 的一份。

    替换后的上下文仍 ``check_hostname=True`` / ``verify_mode=CERT_REQUIRED``，
    只换信任锚。
    """
    import aiohttp.connector

    context = ssl.create_default_context()
    context.load_verify_locations(str(ca_path))
    monkeypatch.setattr(aiohttp.connector, "_SSL_CONTEXT_VERIFIED", context)


class TestPinnedHTTPS:
    """策略激活 + https：socket 打到 pin 的 IP，Host/SNI 仍是域名，证书按域名校验。"""

    def test_sync_connects_to_ip_with_domain_host_and_sni(self, tmp_path, monkeypatch):
        seen_hosts = []
        ca_path, chain_path = _make_test_ca(tmp_path)
        server = _start_tls_server(chain_path, seen_hosts)
        port = server.server_address[1]
        _pin_alias(monkeypatch, "127.0.0.1")
        _sync_session_trusting(monkeypatch, ca_path)
        try:
            ex = _exec(url=f"https://pinned.invalid:{port}/x",
                       allowed_hosts=["pinned.invalid"])
            rsp, err = ex.handle_request({})
        finally:
            server.shutdown()
            server.server_close()
        assert err is None, err
        assert rsp.res == {"ok": True}
        # 证书只对 pinned.invalid 有效：能握手成功即证明 SNI/校验用的是域名而非 pin 的 IP
        assert seen_hosts == [f"pinned.invalid:{port}"]

    def test_async_connects_to_ip_with_domain_host_and_sni(self, tmp_path, monkeypatch):
        seen_hosts = []
        ca_path, chain_path = _make_test_ca(tmp_path)
        server = _start_tls_server(chain_path, seen_hosts)
        port = server.server_address[1]
        _pin_alias(monkeypatch, "127.0.0.1")
        _async_trust(monkeypatch, ca_path)
        try:
            ex = _exec(url=f"https://pinned.invalid:{port}/x",
                       allowed_hosts=["pinned.invalid"])
            rsp, err = asyncio.run(ex.handle_request_async(None))
        finally:
            server.shutdown()
            server.server_close()
        assert err is None, err
        assert rsp.res == {"ok": True}
        assert seen_hosts == [f"pinned.invalid:{port}"]

    def test_sync_rejects_cert_for_other_domain(self, tmp_path, monkeypatch):
        """负向对照：域名不符必须在 TLS 阶段失败（证明校验没被关掉）。"""
        seen_hosts = []
        ca_path, chain_path = _make_test_ca(tmp_path)
        server = _start_tls_server(chain_path, seen_hosts)
        port = server.server_address[1]
        _pin_alias(monkeypatch, "127.0.0.1")
        _sync_session_trusting(monkeypatch, ca_path)
        try:
            ex = _exec(url=f"https://other.invalid:{port}/x",
                       allowed_hosts=["other.invalid"])
            rsp, err = ex.handle_request({})
        finally:
            server.shutdown()
            server.server_close()
        assert err is not None
        assert "CERTIFICATE_VERIFY_FAILED" in str(err)
        assert seen_hosts == []
