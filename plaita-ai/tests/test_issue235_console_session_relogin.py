"""plaita#235：ConsoleClient 会话失效后必须自愈（重登一次并重放请求）。

修复前：``_session_token`` 只在为空时登录，收到 401 后既不清 token 也不重登，
进程内所有 console 调用永久 401（MCP server 常驻时只能重启）。

修复后契约：

- 会话模式（用户名/密码、无 admin key）收到 401：清 token → ``login()`` 一次
  → 重放本次请求一次；重放仍 401 按普通错误抛出（不无限重试）；
- API-Key 模式（含同时配了用户名/密码）的 401 不重登——那是 key 配错；
- ``/auth/login`` 自身 401 不递归重试。

负控：把 ``_request`` 的 401 重登分支还原成「直接抛错」⇒ 本文件
``test_expired_session...`` / ``test_repeated_401...`` 红
（login 计数停在失效前的值，第二次调用直接 401）。
"""
from __future__ import annotations

import httpx
import pytest

from plaita_ai.console_client import ConsoleClient, ConsoleClientError, ConsoleConfig


class SessionConsole:
    """Minimal console state: login issues tokens, the server can revoke them."""

    def __init__(self, *, reject_all: bool = False, login_status: int = 200) -> None:
        self.issued: set[str] = set()
        self.login_count = 0
        self.auth_attempts = 0
        self.reject_all = reject_all
        self.login_status = login_status

    def expire_sessions(self) -> None:
        """Simulate console-side session deletion (TTL expiry / logout / disable)."""
        self.issued.clear()

    def handler(self, request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/auth/login":
            self.login_count += 1
            if self.login_status >= 400:
                return httpx.Response(self.login_status, json={"detail": "用户名或密码错误"})
            token = f"tok-{self.login_count}"
            self.issued = {token}
            return httpx.Response(200, json={"token": token})
        self.auth_attempts += 1
        bearer = (request.headers.get("Authorization") or "").removeprefix("Bearer ").strip()
        if self.reject_all or bearer not in self.issued:
            return httpx.Response(401, json={"detail": "会话无效或已过期，请重新登录"})
        return httpx.Response(200, json={"flows": [], "total": 0})


def _session_client(console: SessionConsole) -> ConsoleClient:
    return ConsoleClient(
        ConsoleConfig(base_url="http://fake", username="sup", password="pw"),
        transport=httpx.MockTransport(console.handler),
    )


def test_expired_session_relogins_once_and_retry_succeeds():
    console = SessionConsole()
    client = _session_client(console)

    assert client.list_flows()["total"] == 0
    assert console.login_count == 1

    console.expire_sessions()
    # 第二次调用：stale token → 401 → 清 token + 重登 → 重放成功。
    assert client.list_flows()["total"] == 0
    assert console.login_count == 2
    assert console.auth_attempts == 3  # 首次成功 + stale 401 + 重放成功

    # 新 token 持久化：下一次调用不再重登。
    assert client.list_flows()["total"] == 0
    assert console.login_count == 2


def test_repeated_401_relogins_at_most_once_per_call():
    console = SessionConsole()
    client = _session_client(console)
    client.list_flows()  # login #1

    # 服务端此后对一切会话 401（例如用户被禁用）：重登后仍 401。
    console.reject_all = True
    with pytest.raises(ConsoleClientError) as excinfo:
        client.list_flows()

    assert excinfo.value.status == 401
    assert console.login_count == 2  # 该次调用恰好重登一次，不是循环
    assert console.auth_attempts == 3  # 首次成功 + 原请求 401 + 重放 401，无第三次


def test_api_key_401_never_logs_in():
    """API-Key 模式（即使同时配了用户名/密码）的 401 不触发登录。"""
    console = SessionConsole(reject_all=True)
    client = ConsoleClient(
        ConsoleConfig(
            base_url="http://fake",
            admin_api_key="wrong-key",
            username="sup",
            password="pw",
        ),
        transport=httpx.MockTransport(console.handler),
    )

    with pytest.raises(ConsoleClientError) as excinfo:
        client.list_flows()

    assert excinfo.value.status == 401
    assert console.login_count == 0
    assert console.auth_attempts == 1  # 没有重放


def test_failed_login_is_not_retried():
    """登录端点自身的 401（密码错）不递归触发重登。"""
    console = SessionConsole(login_status=401)
    client = _session_client(console)

    with pytest.raises(ConsoleClientError) as excinfo:
        client.list_flows()

    assert excinfo.value.status == 401
    assert console.login_count == 1
    assert console.auth_attempts == 0
