"""plaita.core.http_session — flow-scoped shared aiohttp session.

2026-10 BFF hot-path review: the HTTP node used to create (and destroy) an
``aiohttp.ClientSession`` per request, so every node paid a fresh TCP+TLS
handshake even when consecutive calls hit the same host.  This module gives
each **flow run** one shared session:

* created lazily by :func:`open_flow_session` at flow-driver entry
  (``plaita.core.async_utils`` wraps every eager coro / lazy generator with
  the scope), passed to nodes through a :class:`contextvars.ContextVar` so
  every subtask spawned by the flow (Parallel / Map fan-out) inherits it;
* closed deterministically by :func:`close_flow_session` on the **same,
  still-running** loop when the flow ends — no atexit and no GC-timed
  closes (a session bound to a dead loop cannot be closed properly);
* cookie persistence is **off by default** (``DummyCookieJar``): sessions
  would otherwise leak Set-Cookie state across flows / tenants sharing the
  process.  Opt in per process with ``PLAITA_HTTP_COOKIES=1``.

Lifecycle model (2026-10 review P0): plaita's sync entry points create one
event loop per ``flow.run()`` (``asyncio.run``), and distributed workers
create one loop per step — so "per loop" and "per flow run" coincide in
every driver shape, but only an explicit scope gives deterministic close.

This module lives in **core** (not ``plaita.node``) because the driver in
``core.async_utils`` must open/close the scope; it lazy-imports aiohttp so
plaita stays installable without the ``http`` extra — every helper becomes
a no-op (or returns ``None``) in that case.
"""

from __future__ import annotations

import os
import threading
from contextvars import ContextVar
from typing import Any, Optional

logger_name = "plaita.core.http_session"

_flow_session_var: ContextVar[Optional[Any]] = ContextVar(
    "plaita_http_flow_session", default=None,
)

_CONNECTOR_LIMIT_ENV = "PLAITA_HTTP_CONNECTOR_LIMIT"
_COOKIES_ENV = "PLAITA_HTTP_COOKIES"


def _connector_limit() -> int:
    """共享 connector 的并发连接上限（可经环境变量调整）。

    注意 aiohttp 的 ``ClientTimeout(total=...)`` 从请求发起开始计时、**包含
    在 connector 信号量上的排队时间**：fan-out 并发数长期高于 limit 时会表现
    为莫名 timeout——调大 limit 或调大节点超时。默认与 aiohttp 默认一致。
    """
    raw = os.environ.get(_CONNECTOR_LIMIT_ENV, "")
    try:
        return max(1, int(raw)) if raw else 100
    except ValueError:
        return 100


def _cookies_enabled() -> bool:
    return os.environ.get(_COOKIES_ENV, "").strip() in ("1", "true", "yes")


async def open_flow_session() -> None:
    """Open the flow-scoped session and publish it on the contextvar.

    No-op when aiohttp is not installed (the HTTP node is unavailable
    anyway).  Safe to call when a session is already open (nested flows via
    child executions reuse the outer scope — closing is refcount-free by
    design: only the driver scope that opened a session closes it).
    """
    if _flow_session_var.get() is not None:
        return
    try:
        import aiohttp
    except Exception:  # noqa: BLE001 - http extra 未安装：整块 no-op（debug 留痕）
        import logging

        logging.getLogger(logger_name).debug(
            "aiohttp unavailable; flow http session scope is a no-op", exc_info=True,
        )
        return
    jar = None if _cookies_enabled() else aiohttp.DummyCookieJar()
    connector = aiohttp.TCPConnector(limit=_connector_limit())
    # trust_env 显式 False（aiohttp 默认值，钉死语义）：不读 HTTP_PROXY 等
    # 环境变量——共享 session 若开始吃代理 env 会是静默行为变化。
    session = aiohttp.ClientSession(connector=connector, cookie_jar=jar, trust_env=False)
    _flow_session_var.set(session)


async def close_flow_session() -> None:
    """Close the flow-scoped session on the current (still-running) loop."""
    session = _flow_session_var.get()
    _flow_session_var.set(None)
    if session is not None and not session.closed:
        try:
            await session.close()
        except Exception:  # noqa: BLE001 - 关闭失败不掩盖流程结果
            import logging

            logging.getLogger(logger_name).debug(
                "flow http session close failed", exc_info=True,
            )


def get_flow_session() -> Optional[Any]:
    """Return the current flow-scoped session (``None`` outside a scope)."""
    return _flow_session_var.get()


def _new_oneshot_session(resolver=None):
    """Fallback one-shot session for node calls outside a flow scope.

    Direct ``HTTP.arun`` invocations (unit tests, embedding without the
    flow driver) have no scope; they get a throwaway session with the same
    cookie/trust_env defaults as the shared one.

    ``resolver`` 可传自定义 :class:`aiohttp.abc.AbstractResolver`——HTTP 节点在
    访问策略激活时用它把**建连解析**交给策略校验并只连校验过的 IP（见
    ``plaita/node/http.py::_PolicyResolver``）；缺省沿用 aiohttp 默认解析器。
    """
    import aiohttp

    jar = None if _cookies_enabled() else aiohttp.DummyCookieJar()
    connector = aiohttp.TCPConnector(limit=_connector_limit(), resolver=resolver)
    return aiohttp.ClientSession(connector=connector, trust_env=False, cookie_jar=jar)


# --- sync (requests) shared session -----------------------------------------
# 冷路径：只有 Parallel 的 sync 分支桥（run_in_executor → node.run → execute）
# 走 requests。仍共享——每个 executor 一个 Session 意味着每次节点执行一次
# TCP+TLS 握手。urllib3 连接池线程安全；cookie 默认阻断（跨 flow 泄漏防护）。

_sync_session: Optional[Any] = None
_sync_session_pid: Optional[int] = None
_sync_session_lock = threading.Lock()


class _BlockAllCookies:
    """CookiePolicy that refuses to store or send any cookie."""

    netscape = True
    rfc2965 = False
    hide_cookie2 = False

    def set_ok(self, cookie, request):  # noqa: ARG002
        return False

    def return_ok(self, cookie, request):  # noqa: ARG002
        return False

    def domain_return_ok(self, cookie, request):  # noqa: ARG002
        return False

    def path_return_ok(self, cookie, request):  # noqa: ARG002
        return False


def _sync_cookies_enabled() -> bool:
    return _cookies_enabled()


_SYNC_SESSION_CLS: Optional[Any] = None


def _sync_session_cls():
    """Lazily build (and cache) the ``requests.Session`` subclass used for sync nodes.

    Only one difference from ``requests.Session``: ``Session.send`` with
    ``allow_redirects=False`` still calls ``resolve_redirects(yield_requests=True)``
    to populate ``Response.next()``, and that generator does ``resp.content`` on the
    3xx before yielding — an **unbounded** read of every redirect-hop body. plaita
    follows redirects hop-by-hop itself (see ``plaita.node.http``) and never uses
    ``Response.next()``, so the prefetch is short-circuited; otherwise a hostile
    3xx bypasses the response byte cap.
    """
    global _SYNC_SESSION_CLS
    if _SYNC_SESSION_CLS is None:
        import requests

        class _NoPrefetchRedirectSession(requests.Session):
            def resolve_redirects(self, resp, req, **kwargs):
                if kwargs.get("yield_requests"):
                    return
                yield from super().resolve_redirects(resp, req, **kwargs)

        _SYNC_SESSION_CLS = _NoPrefetchRedirectSession
    return _SYNC_SESSION_CLS


def get_shared_sync_session():
    """Process-wide ``requests.Session`` with connection pooling.

    Rebuilt after ``fork`` (``Parallel(mode=process)`` children inherit the
    parent's sockets — same hazard as the sync node pool's PID guard).
    """
    global _sync_session, _sync_session_pid
    from requests.adapters import HTTPAdapter

    pid = os.getpid()
    with _sync_session_lock:
        if _sync_session is None or _sync_session_pid != pid:
            session = _sync_session_cls()()
            adapter = HTTPAdapter(pool_connections=16, pool_maxsize=64)
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            if not _sync_cookies_enabled():
                session.cookies.set_policy(_BlockAllCookies())
            _sync_session = session
            _sync_session_pid = pid
        return _sync_session


def clear_shared_sync_session() -> None:
    """Drop the cached sync session (tests / fork-simulation helpers)."""
    global _sync_session, _sync_session_pid
    with _sync_session_lock:
        _sync_session = None
        _sync_session_pid = None
