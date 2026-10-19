"""A2R20-05：Redis 连接/读超时必须由仓内显式声明，不随 redis-py 版本漂移。

负控（修复前会红）：
1. redis-py 5.0.x 默认 ``socket_timeout=None``（∞）——半开端点（TCP accept
   后不响应）上 ``ping()`` 永久阻塞；探针实测 console/worker/nonce >8s。
2. 运行时 ``Redis.from_url`` 未声明 read/connect 超时（安装版本决定预算）。
3. console 启动/健康检查在半开 Redis 上没有上界。

本文件的判据：**声明值到位 + 半开端点在声明值处返回**，而非依赖 redis-py
当时的默认值。
"""
from __future__ import annotations

import ast
import socket
import threading
import time
from pathlib import Path

import pytest
import redis

from plaita.redis_client import (
    CONSOLE_CONNECT_TIMEOUT_SECONDS,
    CONSOLE_SOCKET_TIMEOUT_SECONDS,
    DEFAULT_CONNECT_TIMEOUT_SECONDS,
    DEFAULT_SOCKET_TIMEOUT_SECONDS,
    HEALTH_CONNECT_TIMEOUT_SECONDS,
    HEALTH_SOCKET_TIMEOUT_SECONDS,
    make_async_redis_client,
    make_redis_client,
    redis_timeout_kwargs,
)

REPO = Path(__file__).resolve().parents[2]

#: 运行时路径（不含 async 事件总线：其客户端由事件总线内部按循环重建，
#: 不在 A2R20-05 的 console/worker 建连清单内）。
_RUNTIME_FILES = [
    REPO / "plaita" / "redis_client.py",
    REPO / "plaita" / "server" / "flow_worker.py",
    REPO / "plaita" / "server" / "services" / "__main__.py",
    REPO / "plaita" / "server" / "event_filter.py",
    *sorted((REPO / "plaita-console" / "backend").rglob("*.py")),
    *sorted((REPO / "plaita-console" / "scripts").glob("*.py")),
    *sorted((REPO / "scripts").glob("*.py")),
]


def _scan_files() -> list[Path]:
    files = []
    for path in _RUNTIME_FILES:
        parts = set(path.parts)
        if "tests" in parts or "build" in parts or not path.is_file():
            continue
        files.append(path)
    return files


def _from_url_calls(tree: ast.AST):
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = None
        if isinstance(func, ast.Attribute):
            name = func.attr
        elif isinstance(func, ast.Name):
            name = func.id
        if name == "from_url":
            yield node


def _declares_timeouts(call: ast.Call) -> bool:
    keywords = {kw.arg for kw in call.keywords if kw.arg}
    if {"socket_timeout", "socket_connect_timeout"} <= keywords:
        return True
    # **redis_timeout_kwargs(...) / **timeouts：工厂内部展开的声明值
    for kw in call.keywords:
        if kw.arg is not None:
            continue
        value = kw.value
        if isinstance(value, ast.Call) and getattr(value.func, "id", "") == "redis_timeout_kwargs":
            return True
        if isinstance(value, ast.Name) and value.id in {"timeouts", "timeout_kwargs"}:
            return True
    return False


def test_every_runtime_from_url_declares_timeouts():
    """无裸 from_url：连接与读超时都必须显式（helper 或关键字）。"""
    offenders = []
    for path in _scan_files():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for call in _from_url_calls(tree):
            if not _declares_timeouts(call):
                offenders.append(f"{path.relative_to(REPO)}:{call.lineno}")
    assert not offenders, (
        "以下 Redis.from_url 未声明 socket_timeout/socket_connect_timeout：\n  "
        + "\n  ".join(offenders)
    )


def test_helpers_use_repo_declared_defaults():
    assert DEFAULT_CONNECT_TIMEOUT_SECONDS == 5.0
    assert DEFAULT_SOCKET_TIMEOUT_SECONDS == 30.0
    assert CONSOLE_CONNECT_TIMEOUT_SECONDS == 2.0
    assert CONSOLE_SOCKET_TIMEOUT_SECONDS == 5.0
    assert HEALTH_CONNECT_TIMEOUT_SECONDS == 3.0
    assert HEALTH_SOCKET_TIMEOUT_SECONDS == 3.0
    assert redis_timeout_kwargs() == {
        "socket_connect_timeout": 5.0,
        "socket_timeout": 30.0,
    }


def test_env_overrides_defaults(monkeypatch):
    monkeypatch.setenv("PLAITA_REDIS_CONNECT_TIMEOUT", "7")
    monkeypatch.setenv("PLAITA_REDIS_SOCKET_TIMEOUT", "11")
    assert redis_timeout_kwargs() == {
        "socket_connect_timeout": 7.0,
        "socket_timeout": 11.0,
    }


def test_invalid_env_falls_back_with_warning(monkeypatch, caplog):
    monkeypatch.setenv("PLAITA_REDIS_SOCKET_TIMEOUT", "nope")
    assert redis_timeout_kwargs()["socket_timeout"] == 30.0
    assert "PLAITA_REDIS_SOCKET_TIMEOUT" in caplog.text
    assert "非法" in caplog.text


def test_make_redis_client_carries_timeouts():
    client = make_redis_client(
        "redis://example:6379/0",
        connect_timeout=2.0,
        socket_timeout=5.0,
    )
    kwargs = client.connection_pool.connection_kwargs
    assert kwargs["socket_connect_timeout"] == 2.0
    assert kwargs["socket_timeout"] == 5.0


def test_url_query_wins_over_kwargs():
    """运维仍可逐 URL 覆盖（redis-py 的 from_url 以 URL query 为准）。"""
    client = make_redis_client(
        "redis://example:6379/0?socket_timeout=1.5", socket_timeout=5.0
    )
    assert client.connection_pool.connection_kwargs["socket_timeout"] == 1.5


def test_make_async_redis_client_carries_timeouts():
    client = make_async_redis_client(
        "redis://example:6379/0",
        connect_timeout=2.0,
        socket_timeout=5.0,
    )
    assert client.connection_pool.connection_kwargs["socket_connect_timeout"] == 2.0
    assert client.connection_pool.connection_kwargs["socket_timeout"] == 5.0


class _Blackhole:
    """TCP accept 后永不响应：connect 成功、read 必须靠声明值返回。"""

    def __init__(self) -> None:
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(10)
        self.port = self.sock.getsockname()[1]
        self.accepted: list[socket.socket] = []
        self._lock = threading.Lock()
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self) -> None:
        while True:
            try:
                conn, _ = self.sock.accept()
            except OSError:
                return
            with self._lock:
                self.accepted.append(conn)

    def close(self) -> None:
        with self._lock:
            for conn in self.accepted:
                conn.close()
            self.accepted.clear()
        self.sock.close()


def test_half_open_redis_returns_at_declared_read_timeout():
    """负控：半开端点上 declared read timeout 生效（不再 >8s 阻塞）。

    用 0.3s 覆盖只为让单测快速；生产默认值由上面的常量断言固定为 5s/30s。
    """
    hole = _Blackhole()
    try:
        client = make_redis_client(
            f"redis://127.0.0.1:{hole.port}/0",
            connect_timeout=0.3,
            socket_timeout=0.3,
        )
        started = time.monotonic()
        with pytest.raises((redis.exceptions.TimeoutError, redis.exceptions.ConnectionError)):
            client.ping()
        elapsed = time.monotonic() - started
        assert elapsed < 2.0, f"应在声明的 0.3s 读超时附近返回，实测 {elapsed:.2f}s"
    finally:
        hole.close()
