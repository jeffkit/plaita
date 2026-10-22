"""登录失败限速（#208 / r64 A112-01/02、B112-01/02）。

维度：
- **per-key** ``(client_ip, username)``：同 IP 对同账号失败达 5 次 → 锁定；
- **IP 桶**：单 IP 对多用户名 fan-out 达 ``IP_MAX_FAILURES`` → 锁该 IP
  （阈值高于账号维度，NAT/共享出口少量重试不误锁；锁时长短于账号锁）；
- **账号桶**：同一账号的失败来自 >= ``ACCOUNT_DISTINCT_IPS`` 个不同 IP →
  锁账号（分布式爆破；单 IP 重复失败只走 per-key/IP 桶）。
锁定期内即使口令正确也 429；再次触发锁定时长翻倍（MAX_LOCKOUT 封顶）；
成功登录清零 per-key、IP 计数与账号桶。

客户端 IP：默认取 ASGI peer；仅当 peer 在 ``PLAITA_TRUSTED_PROXIES``
（逗号分隔 IP/CIDR）内才解析 ``X-Forwarded-For``，并从右向左取第一个
untrusted 地址（不裸信 XFF）。

多 worker：传入 Redis 时用 ``RedisLoginThrottle``（Lua 原子）共享计数；
无 Redis 的本地档退回进程内 ``LoginThrottle``（单进程语义，部署文档注明）。

break-glass：``PLAITA_LOGIN_BREAK_GLASS_USERS`` 绕过限速（仍校验口令，
审计 WARNING），用于紧急运维。
"""
from __future__ import annotations

import hashlib
import ipaddress
import logging
import math
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple

logger = logging.getLogger("plaita.console.login_guard")

MAX_FAILURES = 5
WINDOW_SECONDS = 300.0
LOCKOUT_SECONDS = 900.0
MAX_LOCKOUT_SECONDS = 24 * 60 * 60.0
MAX_TRACKED = 10_000

#: 单 IP 对多用户名的 fan-out 阈值（高于单账号阈值，避免 NAT 少量重试误锁）。
IP_MAX_FAILURES = 10
#: 同一账号失败来自多少个不同 IP 才触发账号锁（分布式爆破）。
ACCOUNT_DISTINCT_IPS = 3
#: IP 桶锁定上限（短于账号锁，降低共享出口/NAT 影响）。
IP_LOCKOUT_SECONDS = 300.0

TRUSTED_PROXIES_ENV = "PLAITA_TRUSTED_PROXIES"
BREAK_GLASS_ENV = "PLAITA_LOGIN_BREAK_GLASS_USERS"
REDIS_PREFIX = "plaita:login-guard:"


def _key(client_ip: str, username: str) -> Tuple[str, str]:
    return (client_ip or "unknown", (username or "").strip().lower())


def _username(username: str) -> str:
    return (username or "").strip().lower()


@dataclass
class _Entry:
    failures: List[float] = field(default_factory=list)
    locked_until: float = 0.0
    lock_level: int = 0


def _next_lock(level: int, base: float, cap: float) -> Tuple[int, float]:
    level += 1
    return level, min(base * (2 ** (level - 1)), cap)


class LoginThrottle:
    """线程安全的进程内限速器（per-key + IP 桶 + 账号桶）。"""

    def __init__(
        self,
        max_failures: int = MAX_FAILURES,
        window_seconds: float = WINDOW_SECONDS,
        lockout_seconds: float = LOCKOUT_SECONDS,
        max_lockout_seconds: float = MAX_LOCKOUT_SECONDS,
        max_tracked: int = MAX_TRACKED,
        ip_max_failures: int = IP_MAX_FAILURES,
        account_distinct_ips: int = ACCOUNT_DISTINCT_IPS,
        ip_lockout_seconds: float = IP_LOCKOUT_SECONDS,
    ) -> None:
        self.max_failures = max_failures
        self.window_seconds = window_seconds
        self.lockout_seconds = lockout_seconds
        self.max_lockout_seconds = max_lockout_seconds
        self.max_tracked = max_tracked
        self.ip_max_failures = ip_max_failures
        self.account_distinct_ips = account_distinct_ips
        self.ip_lockout_seconds = ip_lockout_seconds
        self._entries: Dict[Tuple[str, str], _Entry] = {}
        self._ip_entries: Dict[str, _Entry] = {}
        self._account_ips: Dict[str, Dict[str, float]] = {}
        self._account_locks: Dict[str, _Entry] = {}
        self._lock = threading.Lock()

    def retry_after(self, client_ip: str, username: str, now: float | None = None) -> int:
        now = time.monotonic() if now is None else now
        with self._lock:
            waits = [
                self._entries.get(_key(client_ip, username)),
                self._ip_entries.get(client_ip or "unknown"),
                self._account_locks.get(_username(username)),
            ]
            return max(
                (max(0, math.ceil(e.locked_until - now)) for e in waits if e),
                default=0,
            )

    def record_failure(self, client_ip: str, username: str, now: float | None = None) -> int:
        now = time.monotonic() if now is None else now
        ip = client_ip or "unknown"
        user = _username(username)
        with self._lock:
            self._prune(now)
            waits = [0]
            key = (ip, user)
            if key in self._entries or len(self._entries) < self.max_tracked:
                waits.append(
                    self._hit(
                        self._entries.setdefault(key, _Entry()),
                        now, self.max_failures, self.lockout_seconds,
                    )
                )
            if ip in self._ip_entries or len(self._ip_entries) < self.max_tracked:
                waits.append(
                    self._hit(
                        self._ip_entries.setdefault(ip, _Entry()),
                        now, self.ip_max_failures, self.ip_lockout_seconds,
                    )
                )
            self._account_ips.setdefault(user, {})[ip] = now
            account = self._account_locks.setdefault(user, _Entry())
            if len(self._account_ips[user]) >= self.account_distinct_ips:
                if account.locked_until <= now:
                    account.lock_level, duration = _next_lock(
                        account.lock_level,
                        self.lockout_seconds,
                        self.max_lockout_seconds,
                    )
                    account.locked_until = now + duration
                    waits.append(max(1, math.ceil(duration)))
                else:
                    waits.append(max(0, math.ceil(account.locked_until - now)))
            return max(waits)

    def _hit(self, entry: _Entry, now: float, threshold: int, base: float) -> int:
        if entry.locked_until > now:
            return max(0, math.ceil(entry.locked_until - now))
        entry.failures = [
            ts for ts in entry.failures if now - ts < self.window_seconds
        ]
        entry.failures.append(now)
        if len(entry.failures) < threshold:
            return 0
        entry.lock_level, duration = _next_lock(
            entry.lock_level, base, self.max_lockout_seconds
        )
        entry.failures.clear()
        entry.locked_until = now + duration
        return max(1, math.ceil(duration))

    def reset(self, client_ip: str, username: str) -> None:
        ip = client_ip or "unknown"
        user = _username(username)
        with self._lock:
            self._entries.pop((ip, user), None)
            self._ip_entries.pop(ip, None)
            self._account_ips.pop(user, None)
            self._account_locks.pop(user, None)

    def reset_all(self) -> None:
        with self._lock:
            self._entries.clear()
            self._ip_entries.clear()
            self._account_ips.clear()
            self._account_locks.clear()

    def _prune(self, now: float) -> None:
        """只清过期条目；**绝不驱逐活动锁**（A1 L10）。

        表满且全是活动锁时，新键不跟踪（record_failure 的容量判断），
        已有锁保持有效——宁可新键限速降级，也不放飞受害者账号。
        """
        if (
            len(self._entries) < self.max_tracked
            and len(self._ip_entries) < self.max_tracked
            and len(self._account_locks) < self.max_tracked
        ):
            # 未触顶不做清理：保留已过期 key 的 lock_level，锁再次触发才能翻倍。
            return
        for store in (self._entries, self._ip_entries, self._account_locks):
            stale = [
                key
                for key, entry in store.items()
                if entry.locked_until <= now
                and all(now - ts >= self.window_seconds for ts in entry.failures)
            ]
            for key in stale:
                del store[key]
        stale_users = [
            user
            for user, ips in self._account_ips.items()
            if all(now - ts >= self.window_seconds for ts in ips.values())
        ]
        for user in stale_users:
            self._account_ips.pop(user, None)


_HIT_LUA = """
local h = KEYS[1]
local now = tonumber(ARGV[1])
local win = tonumber(ARGV[2])
local maxf = tonumber(ARGV[3])
local base = tonumber(ARGV[4])
local cap = tonumber(ARGV[5])
local ttl = tonumber(ARGV[6])
local until_ = tonumber(redis.call('HGET', h, 'until') or '0')
if until_ > now then return math.ceil(until_ - now) end
local first = tonumber(redis.call('HGET', h, 'first') or '0')
local n = tonumber(redis.call('HGET', h, 'n') or '0')
if first == 0 or now - first > win then n = 0; first = now end
n = n + 1
if n >= maxf then
  local level = tonumber(redis.call('HGET', h, 'level') or '0') + 1
  local d = base * (2 ^ (level - 1))
  if d > cap then d = cap end
  redis.call('HSET', h, 'until', now + d, 'level', level, 'n', 0, 'first', now)
  redis.call('EXPIRE', h, ttl)
  return math.ceil(d)
end
redis.call('HSET', h, 'n', n, 'first', first, 'until', 0)
redis.call('EXPIRE', h, ttl)
return 0
"""

_ACCOUNT_LUA = """
local h = KEYS[1]
local ips = KEYS[2]
local now = tonumber(ARGV[1])
local win = tonumber(ARGV[2])
local distinct = tonumber(ARGV[3])
local base = tonumber(ARGV[4])
local cap = tonumber(ARGV[5])
local ttl = tonumber(ARGV[6])
local ip = ARGV[7]
redis.call('SADD', ips, ip)
redis.call('EXPIRE', ips, ttl)
local count = redis.call('SCARD', ips)
if count < distinct then return 0 end
local until_ = tonumber(redis.call('HGET', h, 'until') or '0')
if until_ > now then return math.ceil(until_ - now) end
local level = tonumber(redis.call('HGET', h, 'level') or '0') + 1
local d = base * (2 ^ (level - 1))
if d > cap then d = cap end
redis.call('HSET', h, 'until', now + d, 'level', level)
redis.call('EXPIRE', h, ttl)
return math.ceil(d)
"""


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:32]


class RedisLoginThrottle:
    """共享 Redis 计数的限速器（多 worker 一致；Lua 原子）。"""

    def __init__(
        self,
        redis,
        *,
        max_failures: int = MAX_FAILURES,
        window_seconds: float = WINDOW_SECONDS,
        lockout_seconds: float = LOCKOUT_SECONDS,
        max_lockout_seconds: float = MAX_LOCKOUT_SECONDS,
        ip_max_failures: int = IP_MAX_FAILURES,
        account_distinct_ips: int = ACCOUNT_DISTINCT_IPS,
        ip_lockout_seconds: float = IP_LOCKOUT_SECONDS,
    ) -> None:
        self.redis = redis
        self.max_failures = max_failures
        self.window_seconds = window_seconds
        self.lockout_seconds = lockout_seconds
        self.max_lockout_seconds = max_lockout_seconds
        self.ip_max_failures = ip_max_failures
        self.account_distinct_ips = account_distinct_ips
        self.ip_lockout_seconds = ip_lockout_seconds
        self._ttl = int(max(self.window_seconds, self.max_lockout_seconds)) + 60

    def _key_key(self, ip: str, user: str) -> str:
        return f"{REDIS_PREFIX}key:{_digest(ip + '|' + user)}"

    def _ip_key(self, ip: str) -> str:
        return f"{REDIS_PREFIX}ip:{_digest(ip)}"

    def _account_key(self, user: str) -> str:
        return f"{REDIS_PREFIX}account:{_digest(user)}"

    def _account_ips_key(self, user: str) -> str:
        return f"{REDIS_PREFIX}account-ips:{_digest(user)}"

    def _until(self, key: str) -> int:
        try:
            raw = self.redis.hget(key, "until")
        except Exception:
            return 0
        try:
            return int(float(raw or 0))
        except (TypeError, ValueError):
            return 0

    def retry_after(self, client_ip: str, username: str, now: float | None = None) -> int:
        now = time.monotonic() if now is None else now
        try:
            waits = [
                self._until(self._key_key(client_ip or "unknown", _username(username))),
                self._until(self._ip_key(client_ip or "unknown")),
                self._until(self._account_key(_username(username))),
            ]
        except Exception as exc:  # noqa: BLE001 — 读失败按未锁定（抖动不锁死）
            logger.warning("login_guard Redis 读失败（按未锁定）: %s", exc)
            return 0
        return max((max(0, math.ceil(v - now)) for v in waits), default=0)

    def record_failure(self, client_ip: str, username: str, now: float | None = None) -> int:
        now = time.monotonic() if now is None else now
        ip = client_ip or "unknown"
        user = _username(username)
        waits = []
        try:
            waits.append(
                int(
                    self.redis.eval(
                        _HIT_LUA, 1, self._key_key(ip, user),
                        now, self.window_seconds, self.max_failures,
                        self.lockout_seconds, self.max_lockout_seconds, self._ttl,
                    )
                )
            )
            waits.append(
                int(
                    self.redis.eval(
                        _HIT_LUA, 1, self._ip_key(ip),
                        now, self.window_seconds, self.ip_max_failures,
                        self.ip_lockout_seconds, self.max_lockout_seconds, self._ttl,
                    )
                )
            )
            waits.append(
                int(
                    self.redis.eval(
                        _ACCOUNT_LUA, 2,
                        self._account_key(user), self._account_ips_key(user),
                        now, self.window_seconds, self.account_distinct_ips,
                        self.lockout_seconds, self.max_lockout_seconds, self._ttl, ip,
                    )
                )
            )
        except Exception as exc:  # noqa: BLE001 — Redis 瞬断计数降级
            logger.warning("login_guard Redis 计数失败（fail-open 计数）: %s", exc)
            return 0
        return max(waits)

    def reset(self, client_ip: str, username: str) -> None:
        ip = client_ip or "unknown"
        user = _username(username)
        try:
            self.redis.delete(
                self._key_key(ip, user),
                self._ip_key(ip),
                self._account_key(user),
                self._account_ips_key(user),
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("login_guard Redis 清零失败: %s", exc)

    def reset_all(self) -> None:
        try:
            keys = list(self.redis.scan_iter(match=f"{REDIS_PREFIX}*", count=500))
            if keys:
                self.redis.delete(*keys)
        except Exception as exc:  # noqa: BLE001
            logger.warning("login_guard Redis reset_all 失败: %s", exc)


_throttle = LoginThrottle()


def _throttle_for(redis) -> Any:
    return RedisLoginThrottle(redis) if redis is not None else _throttle


def retry_after(
    client_ip: str, username: str, now: float | None = None, redis=None
) -> int:
    return _throttle_for(redis).retry_after(client_ip, username, now)


def record_failure(
    client_ip: str, username: str, now: float | None = None, redis=None
) -> int:
    return _throttle_for(redis).record_failure(client_ip, username, now)


def reset(client_ip: str, username: str, redis=None) -> None:
    _throttle_for(redis).reset(client_ip, username)


def reset_all(redis=None) -> None:
    _throttle_for(redis).reset_all()


def break_glass_users() -> set[str]:
    raw = os.environ.get(BREAK_GLASS_ENV, "")
    return {item.strip().lower() for item in raw.split(",") if item.strip()}


def is_break_glass(username: str) -> bool:
    return _username(username) in break_glass_users()


_proxy_cache: Tuple[str, Optional[List[Any]]] = ("", None)


def trusted_proxy_networks() -> List[Any]:
    raw = os.environ.get(TRUSTED_PROXIES_ENV, "")
    global _proxy_cache
    if _proxy_cache[0] == raw:
        return _proxy_cache[1] or []
    networks: List[Any] = []
    for item in raw.split(","):
        text = item.strip()
        if not text:
            continue
        try:
            networks.append(ipaddress.ip_network(text, strict=False))
        except ValueError:
            logger.warning("PLAITA_TRUSTED_PROXIES 非法条目（忽略）: %r", text)
    _proxy_cache = (raw, networks)
    return networks


def _is_trusted(ip: str, networks: Iterable[Any]) -> bool:
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return False
    return any(addr in network for network in networks)


def client_ip_from(peer: str, xff: Optional[str]) -> str:
    """可信代理解析：peer 可信时 XFF 从右向左取首个 untrusted。"""
    peer = (peer or "").strip() or "unknown"
    networks = trusted_proxy_networks()
    if not networks or not _is_trusted(peer, networks):
        return peer
    chain = [part.strip() for part in (xff or "").split(",") if part.strip()]
    for candidate in reversed(chain):
        if not _is_trusted(candidate, networks):
            return candidate
    return chain[0] if chain else peer
