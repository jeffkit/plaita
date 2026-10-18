"""登录失败限速（#208）：按「客户端 IP + 用户名」计数、锁定与指数退避。

威胁模型：``POST /api/auth/login`` 原先对失败尝试零成本——可在线爆破口令。
本模块提供进程内计数（issue 边界：不引入外部限流服务，集群档另议）：

- 时间窗 ``WINDOW_SECONDS`` 内失败达到 ``MAX_FAILURES`` 次 → 锁定
  ``LOCKOUT_SECONDS``，锁定期内**即使口令正确也拒绝**（否则爆破仍可撞库）；
- 同一键再次触发锁定 → 锁定时长翻倍（``MAX_LOCKOUT_SECONDS`` 封顶）；
- 成功登录清零该键的失败计数与退避等级；
- 键 = (IP, username)，按 IP 与账号两个维度同时收敛；大小写按小写归一，
  避免仅靠改用户名大小写绕过计数。

进程内存储有上限（``MAX_TRACKED``），过期条目在写入时清理；不信任
``X-Forwarded-For``（可伪造），IP 取 ASGI 的 ``request.client.host``。
"""
from __future__ import annotations

import math
import threading
import time
from dataclasses import dataclass, field

#: 时间窗内允许的失败次数（第 MAX_FAILURES 次失败后锁定）
MAX_FAILURES = 5
#: 失败计数时间窗（秒）
WINDOW_SECONDS = 300.0
#: 首次锁定基础时长（秒）
LOCKOUT_SECONDS = 900.0
#: 指数退避上限（秒）
MAX_LOCKOUT_SECONDS = 24 * 60 * 60.0
#: 进程内最多跟踪的 (IP, username) 键数
MAX_TRACKED = 10_000


@dataclass
class _Entry:
    """单个 (IP, username) 键的状态。"""

    failures: list[float] = field(default_factory=list)
    locked_until: float = 0.0
    lock_level: int = 0


def _key(client_ip: str, username: str) -> tuple[str, str]:
    return (client_ip or "unknown", (username or "").strip().lower())


class LoginThrottle:
    """线程安全的进程内登录限速器（单进程语义，见模块 docstring）。"""

    def __init__(
        self,
        max_failures: int = MAX_FAILURES,
        window_seconds: float = WINDOW_SECONDS,
        lockout_seconds: float = LOCKOUT_SECONDS,
        max_lockout_seconds: float = MAX_LOCKOUT_SECONDS,
        max_tracked: int = MAX_TRACKED,
    ) -> None:
        self.max_failures = max_failures
        self.window_seconds = window_seconds
        self.lockout_seconds = lockout_seconds
        self.max_lockout_seconds = max_lockout_seconds
        self.max_tracked = max_tracked
        self._entries: dict[tuple[str, str], _Entry] = {}
        self._lock = threading.Lock()

    # -- 查询 ---------------------------------------------------------------
    def retry_after(self, client_ip: str, username: str, now: float | None = None) -> int:
        """还需等待多少秒才允许再试；0 = 未锁定。"""
        now = time.monotonic() if now is None else now
        with self._lock:
            entry = self._entries.get(_key(client_ip, username))
            if entry is None:
                return 0
            return max(0, math.ceil(entry.locked_until - now))

    # -- 写入 ---------------------------------------------------------------
    def record_failure(self, client_ip: str, username: str, now: float | None = None) -> int:
        """记一次失败；返回本次失败触发锁定时剩余秒数（未触发 = 0）。

        到达阈值的那次失败仍以「用户名或密码错误」返回，从下一次尝试起生效
        （与验收「连续 N 次错误后返回 429」一致，不泄露阈值边界）。
        """
        now = time.monotonic() if now is None else now
        key = _key(client_ip, username)
        with self._lock:
            self._prune(now)
            entry = self._entries.setdefault(key, _Entry())
            if entry.locked_until > now:
                return max(0, math.ceil(entry.locked_until - now))
            entry.failures = [
                ts for ts in entry.failures if now - ts < self.window_seconds
            ]
            entry.failures.append(now)
            if len(entry.failures) < self.max_failures:
                return 0
            entry.lock_level += 1
            duration = min(
                self.lockout_seconds * (2 ** (entry.lock_level - 1)),
                self.max_lockout_seconds,
            )
            entry.failures.clear()
            entry.locked_until = now + duration
            return max(1, math.ceil(duration))

    def reset(self, client_ip: str, username: str) -> None:
        """成功登录后清零该键（失败计数与退避等级）。"""
        with self._lock:
            self._entries.pop(_key(client_ip, username), None)

    def reset_all(self) -> None:
        """清空全部状态（测试与运维重置用）。"""
        with self._lock:
            self._entries.clear()

    # -- 内部 ---------------------------------------------------------------
    def _prune(self, now: float) -> None:
        if len(self._entries) < self.max_tracked:
            return
        stale = [
            key
            for key, entry in self._entries.items()
            if entry.locked_until <= now
            and all(now - ts >= self.window_seconds for ts in entry.failures)
        ]
        for key in stale:
            del self._entries[key]
        # 仍然超限：丢最早的锁定项（其窗口最早结束）
        if len(self._entries) >= self.max_tracked:
            oldest = min(
                self._entries,
                key=lambda k: self._entries[k].locked_until,
            )
            del self._entries[oldest]


#: 进程级默认限速器（auth 端点使用；测试可 reset_all）
_throttle = LoginThrottle()


def retry_after(client_ip: str, username: str, now: float | None = None) -> int:
    return _throttle.retry_after(client_ip, username, now)


def record_failure(client_ip: str, username: str, now: float | None = None) -> int:
    return _throttle.record_failure(client_ip, username, now)


def reset(client_ip: str, username: str) -> None:
    _throttle.reset(client_ip, username)


def reset_all() -> None:
    _throttle.reset_all()
