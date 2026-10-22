"""登录失败限速（#208 / r64 硬化 / r65 设计级收口 #293/#294/#295）。

三维桶：
- **per-key**：精确 canonical IP + username，窗口 300s 内 5 次失败 → 锁；
- **IP 桶**：IP（IPv6 按 /64 聚合）对多用户名 fan-out 10 次 → 锁该 IP 5m；
- **账号桶**：同一账号的失败来自 **窗口内 ≥3 个不同的 IP 桶** → 锁账号。
账号桶的 IP 集合按 ``WINDOW_SECONDS`` 滑动清理（进程内 dict / Redis ZSET），
锁过期后不会因为一个旧 IP 再次失败而续锁；IPv6 /64 内的多个地址只算一个。
锁定期内正确/错误口令都 429；再次锁定翻倍（24h 封顶）。

IP 归一：``ipaddress`` canonical（IPv4-mapped→IPv4、压缩 IPv6 展开）；账号/IP
桶再按 /64 聚合（``IPV6_BUCKET_PREFIX``）。``client_ip_from`` 的 XFF 解析同样
归一。

可信代理：只有 peer 命中 ``PLAITA_TRUSTED_PROXIES`` 才解析 XFF，并从右向左
取首个 untrusted；``0.0.0.0/0``/``::/0`` 全 trust 配置具名拒绝（WARNING）。

共享计数：RedisLoginThrottle 的 Lua 内部用 ``redis.call('TIME')`` 取服务端
UTC 时间（retry 读也用 ``redis.time()``），不把进程 monotonic 写进共享键；
多副本/重启后锁语义一致。无 Redis 的本地档退回进程内 LoginThrottle（单
worker 语义，runbook 注明）。

break-glass（#294）：不再是「用户名列表即绕过」。必须同时满足：
目标用户名在 ``PLAITA_LOGIN_BREAK_GLASS_USERS``、
来源 IP 在 ``PLAITA_LOGIN_BREAK_GLASS_ALLOWED_IPS``（默认回环）、
请求头携带一次性 HMAC token（``bg1.<exp>.<sig>``，secret 为
``PLAITA_LOGIN_BREAK_GLASS_SECRET``，短 TTL、单次使用）。
失败照常计数；每次尝试写独立 ``auth.login_break_glass`` 审计（break_glass=true）。
"""
from __future__ import annotations

import hashlib
import hmac
import ipaddress
import logging
import math
import os
import secrets
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

IP_MAX_FAILURES = 10
ACCOUNT_DISTINCT_IPS = 3
IP_LOCKOUT_SECONDS = 300.0
IPV6_BUCKET_PREFIX = 64

#: MAX_TRACKED 满表且全是活动锁时，新键的具名拒绝窗口（秒）。
SATURATION_SECONDS = 60.0

TRUSTED_PROXIES_ENV = "PLAITA_TRUSTED_PROXIES"
BREAK_GLASS_ENV = "PLAITA_LOGIN_BREAK_GLASS_USERS"
BREAK_GLASS_SECRET_ENV = "PLAITA_LOGIN_BREAK_GLASS_SECRET"
BREAK_GLASS_IPS_ENV = "PLAITA_LOGIN_BREAK_GLASS_ALLOWED_IPS"
BREAK_GLASS_TOKEN_TTL_SECONDS = 300
REDIS_PREFIX = "plaita:login-guard:"
_BG_USED_PREFIX = f"{REDIS_PREFIX}break-glass-used:"


def _username(username: str) -> str:
    return (username or "").strip().lower()


def canonical_ip(value: Any) -> str:
    """IP canonical 化：IPv4-mapped → IPv4；IPv6 压缩展开。非法原样返回。"""
    text = str(value or "").strip()
    if not text:
        return "unknown"
    try:
        addr = ipaddress.ip_address(text)
    except ValueError:
        return text
    if isinstance(addr, ipaddress.IPv6Address) and addr.ipv4_mapped is not None:
        return str(addr.ipv4_mapped)
    return str(addr)


def bucket_ip(value: Any, prefix: Optional[int] = None) -> str:
    """桶维度 IP：IPv4 canonical；IPv6 按 /prefix（默认 64）聚合。"""
    text = canonical_ip(value)
    try:
        addr = ipaddress.ip_address(text)
    except ValueError:
        return text
    if isinstance(addr, ipaddress.IPv6Address):
        length = IPV6_BUCKET_PREFIX if prefix is None else int(prefix)
        return str(ipaddress.ip_network(f"{addr}/{length}", strict=False))
    return str(addr)


def _key(client_ip: str, username: str) -> Tuple[str, str]:
    return (canonical_ip(client_ip), _username(username))


@dataclass
class _Entry:
    failures: List[float] = field(default_factory=list)
    locked_until: float = 0.0
    lock_level: int = 0


def _next_lock(level: int, base: float, cap: float) -> Tuple[int, float]:
    level += 1
    return level, min(base * (2 ** (level - 1)), cap)


class LoginThrottle:
    """线程安全的进程内限速器（三维桶；账号 IP 集合按窗口清理）。"""

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
        # 饱和只作用于触发它的新键：key -> until（不全局施加，见 #296）
        self._saturated_keys: Dict[Tuple[str, str], float] = {}
        self._saturation_warned = False
        self._lock = threading.Lock()

    def _account_recent(self, user: str, now: float) -> Dict[str, float]:
        ips = self._account_ips.get(user, {})
        fresh = {
            ip: ts for ip, ts in ips.items()
            if now - ts < self.window_seconds
        }
        if fresh:
            self._account_ips[user] = fresh
        else:
            self._account_ips.pop(user, None)
        return fresh

    def retry_after(self, client_ip: str, username: str, now: float | None = None) -> int:
        now = time.monotonic() if now is None else now
        with self._lock:
            user = _username(username)
            self._account_recent(user, now)
            key = _key(client_ip, username)
            waits = [
                self._entries.get(key),
                self._ip_entries.get(bucket_ip(client_ip)),
                self._account_locks.get(user),
            ]
            saturated_until = self._saturated_keys.get(key, 0.0)
            if saturated_until > now:
                waits.append(_Entry(locked_until=saturated_until))
            return max(
                (max(0, math.ceil(e.locked_until - now)) for e in waits if e),
                default=0,
            )

    def record_failure(self, client_ip: str, username: str, now: float | None = None) -> int:
        now = time.monotonic() if now is None else now
        ip = canonical_ip(client_ip)
        bucket = bucket_ip(client_ip)
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
            else:
                # #296：饱和只绑定「触发它的新键」，既有键 retry_after 不受影响。
                saturated_until = self._saturated_keys.get(key, 0.0)
                if saturated_until > now:
                    waits.append(max(1, math.ceil(saturated_until - now)))
                else:
                    self._saturated_keys[key] = now + SATURATION_SECONDS
                    waits.append(int(SATURATION_SECONDS))
                    if not self._saturation_warned:
                        self._saturation_warned = True
                        logger.warning(
                            "login_guard MAX_TRACKED=%s 已满且无过期条目：新键 %s "
                            "拒绝跟踪 %.0fs（具名饱和，不静默放行；既有键不受影响）",
                            self.max_tracked, key, SATURATION_SECONDS,
                        )
            if bucket in self._ip_entries or len(self._ip_entries) < self.max_tracked:
                waits.append(
                    self._hit(
                        self._ip_entries.setdefault(bucket, _Entry()),
                        now, self.ip_max_failures, self.ip_lockout_seconds,
                    )
                )
            recent = self._account_recent(user, now)
            recent[bucket] = now
            self._account_ips[user] = recent
            account = self._account_locks.setdefault(user, _Entry())
            if len(recent) >= self.account_distinct_ips:
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
        ip = canonical_ip(client_ip)
        user = _username(username)
        with self._lock:
            self._entries.pop((ip, user), None)
            self._saturated_keys.pop((ip, user), None)
            self._ip_entries.pop(bucket_ip(client_ip), None)
            self._account_ips.pop(user, None)
            self._account_locks.pop(user, None)

    def reset_all(self) -> None:
        with self._lock:
            self._entries.clear()
            self._ip_entries.clear()
            self._account_ips.clear()
            self._account_locks.clear()
            self._saturated_keys.clear()

    def _prune(self, now: float) -> None:
        """清过期条目；活动锁永不驱逐（#293/L10）。

        账号桶每个用户按窗口清理（不是只在满表时清）；表满且全是活动锁时
        新键走具名饱和（record_failure），不再静默不跟踪。
        """
        for store in (self._entries, self._ip_entries, self._account_locks):
            if len(store) < self.max_tracked:
                # 未触顶也清理已过期且无失败计数的条目；保留锁等级用于翻倍。
                stale = [
                    key
                    for key, entry in store.items()
                    if entry.locked_until <= now
                    and all(now - ts >= self.window_seconds for ts in entry.failures)
                    and entry.lock_level == 0
                ]
                for key in stale:
                    del store[key]
                continue
            stale = [
                key
                for key, entry in store.items()
                if entry.locked_until <= now
                and all(now - ts >= self.window_seconds for ts in entry.failures)
            ]
            for key in stale:
                del store[key]
        for key in [
            k for k, until in self._saturated_keys.items() if until <= now
        ]:
            self._saturated_keys.pop(key, None)
        for user in list(self._account_ips):
            self._account_recent(user, now)


# ---------------------------------------------------------------------------
# Redis 共享计数（Lua 内用 Redis TIME，不写进程 monotonic —— #295）
# ---------------------------------------------------------------------------

_HIT_LUA = """
local h = KEYS[1]
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
local win = tonumber(ARGV[1])
local maxf = tonumber(ARGV[2])
local base = tonumber(ARGV[3])
local cap = tonumber(ARGV[4])
local ttl = tonumber(ARGV[5])
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
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
local win = tonumber(ARGV[1])
local distinct = tonumber(ARGV[2])
local base = tonumber(ARGV[3])
local cap = tonumber(ARGV[4])
local ttl = tonumber(ARGV[5])
local ip = ARGV[6]
redis.call('ZREMRANGEBYSCORE', ips, '-inf', now - win)
redis.call('ZADD', ips, now, ip)
redis.call('EXPIRE', ips, ttl)
local count = redis.call('ZCARD', ips)
if count < distinct then return 0 end
local until_ = tonumber(redis.call('HGET', h, 'until') or '0')
if until_ > now then return math.ceil(until_ - now) end
local level = tonumber(redis.call('HGET', h, 'level') or '0') + 1
local d = base * (2 ^ (level - 1))
if d > cap then d = cap end
-- NTP 回拨/并发：until 只前进不后退
local new_until = now + d
if new_until < until_ then new_until = until_ end
redis.call('HSET', h, 'until', new_until, 'level', level)
redis.call('EXPIRE', h, ttl)
return math.ceil(new_until - now)
"""


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:32]


class RedisLoginThrottle:
    """共享 Redis 计数的限速器；时间全部来自 Redis 服务端（#295）。"""

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
        return f"{REDIS_PREFIX}key:{_digest(canonical_ip(ip) + '|' + user)}"

    def _ip_key(self, ip: str) -> str:
        return f"{REDIS_PREFIX}ip:{_digest(bucket_ip(ip))}"

    def _account_key(self, user: str) -> str:
        return f"{REDIS_PREFIX}account:{_digest(user)}"

    def _account_ips_key(self, user: str) -> str:
        return f"{REDIS_PREFIX}account-ips:{_digest(user)}"

    def _server_now(self) -> Optional[float]:
        try:
            sec, usec = self.redis.time()
            return float(sec) + float(usec) / 1_000_000
        except Exception as exc:  # noqa: BLE001 — 读时间失败按未锁定
            logger.warning("login_guard Redis TIME 读取失败（按未锁定）: %s", exc)
            return None

    def _until(self, key: str) -> float:
        try:
            raw = self.redis.hget(key, "until")
        except Exception:
            return 0.0
        try:
            return float(raw or 0)
        except (TypeError, ValueError):
            return 0.0

    def retry_after(self, client_ip: str, username: str) -> int:
        now = self._server_now()
        if now is None:
            return 0
        try:
            waits = [
                self._until(self._key_key(client_ip, _username(username))),
                self._until(self._ip_key(client_ip)),
                self._until(self._account_key(_username(username))),
            ]
        except Exception as exc:  # noqa: BLE001
            logger.warning("login_guard Redis 读失败（按未锁定）: %s", exc)
            return 0
        return max((max(0, math.ceil(v - now)) for v in waits), default=0)

    def record_failure(self, client_ip: str, username: str) -> int:
        ip = canonical_ip(client_ip)
        user = _username(username)
        bucket = bucket_ip(client_ip)
        waits = []
        try:
            waits.append(
                int(
                    self.redis.eval(
                        _HIT_LUA, 1, self._key_key(ip, user),
                        self.window_seconds, self.max_failures,
                        self.lockout_seconds, self.max_lockout_seconds, self._ttl,
                    )
                )
            )
            waits.append(
                int(
                    self.redis.eval(
                        _HIT_LUA, 1, self._ip_key(bucket),
                        self.window_seconds, self.ip_max_failures,
                        self.ip_lockout_seconds, self.max_lockout_seconds, self._ttl,
                    )
                )
            )
            waits.append(
                int(
                    self.redis.eval(
                        _ACCOUNT_LUA, 2,
                        self._account_key(user), self._account_ips_key(user),
                        self.window_seconds, self.account_distinct_ips,
                        self.lockout_seconds, self.max_lockout_seconds, self._ttl,
                        bucket,
                    )
                )
            )
        except Exception as exc:  # noqa: BLE001 — 计数降级
            logger.warning("login_guard Redis 计数失败（fail-open 计数）: %s", exc)
            return 0
        return max(waits)

    def reset(self, client_ip: str, username: str) -> None:
        user = _username(username)
        try:
            self.redis.delete(
                self._key_key(client_ip, user),
                self._ip_key(client_ip),
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
    backend = _throttle_for(redis)
    if redis is not None:
        return backend.retry_after(client_ip, username)
    return backend.retry_after(client_ip, username, now)


def record_failure(
    client_ip: str, username: str, now: float | None = None, redis=None
) -> int:
    backend = _throttle_for(redis)
    if redis is not None:
        return backend.record_failure(client_ip, username)
    return backend.record_failure(client_ip, username, now)


def reset(client_ip: str, username: str, redis=None) -> None:
    _throttle_for(redis).reset(client_ip, username)


def reset_all(redis=None) -> None:
    _throttle_for(redis).reset_all()


# ---------------------------------------------------------------------------
# 可信代理（#293/B112-01）
# ---------------------------------------------------------------------------

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
            network = ipaddress.ip_network(text, strict=False)
        except ValueError:
            logger.warning("PLAITA_TRUSTED_PROXIES 非法条目（忽略）: %r", text)
            continue
        if network.prefixlen == 0:
            logger.warning(
                "PLAITA_TRUSTED_PROXIES=%s 覆盖全地址空间（全 trust），已拒绝该条目；"
                "XFF 只在明确代理网段内解析",
                text,
            )
            continue
        networks.append(network)
    _proxy_cache = (raw, networks)
    return networks


def _is_trusted(ip: str, networks: Iterable[Any]) -> bool:
    text = canonical_ip(ip)
    try:
        addr = ipaddress.ip_address(text)
    except ValueError:
        return False
    return any(addr in network for network in networks)


def client_ip_from(peer: str, xff: Optional[str]) -> str:
    """可信代理解析：peer 可信时 XFF 从右向左取首个 untrusted（已归一）。"""
    peer = canonical_ip(peer)
    networks = trusted_proxy_networks()
    if not networks or not _is_trusted(peer, networks):
        return peer
    chain = [
        canonical_ip(part)
        for part in (xff or "").split(",")
        if part.strip()
    ]
    for candidate in reversed(chain):
        if not _is_trusted(candidate, networks):
            return candidate
    return chain[0] if chain else peer


# ---------------------------------------------------------------------------
# break-glass（#294）：用户白名单 + 来源 IP 白名单 + 一次性 HMAC token
# ---------------------------------------------------------------------------

_bg_used: Dict[str, float] = {}
_bg_lock = threading.Lock()


def break_glass_users() -> set[str]:
    raw = os.environ.get(BREAK_GLASS_ENV, "")
    return {item.strip().lower() for item in raw.split(",") if item.strip()}


def break_glass_allowed_networks() -> List[Any]:
    raw = os.environ.get(BREAK_GLASS_IPS_ENV, "127.0.0.1/32,::1/128")
    networks: List[Any] = []
    for item in raw.split(","):
        text = item.strip()
        if not text:
            continue
        try:
            networks.append(ipaddress.ip_network(text, strict=False))
        except ValueError:
            logger.warning(
                "PLAITA_LOGIN_BREAK_GLASS_ALLOWED_IPS 非法条目（忽略）: %r", text
            )
    return networks


def _break_glass_secret() -> Optional[str]:
    secret = os.environ.get(BREAK_GLASS_SECRET_ENV, "").strip()
    return secret if len(secret) >= 32 else None


def break_glass_token(
    username: str,
    *,
    ttl_seconds: int = BREAK_GLASS_TOKEN_TTL_SECONDS,
    secret: Optional[str] = None,
    now: Optional[float] = None,
) -> str:
    """生成一次性 break-glass token（运维 CLI/测试用）。"""
    secret = secret or _break_glass_secret()
    if not secret:
        raise ValueError("PLAITA_LOGIN_BREAK_GLASS_SECRET 未配置（>=32 字符）")
    current = time.time() if now is None else float(now)
    expires = int(current) + max(1, int(ttl_seconds))
    user = _username(username)
    nonce = secrets.token_hex(8)
    sig = hmac.new(
        secret.encode(), f"bg1|{user}|{expires}|{nonce}".encode(),
        hashlib.sha256,
    ).hexdigest()
    return f"bg1.{expires}.{nonce}.{sig}"


def _consume_break_glass_token(
    token: str, expires: int, *, redis=None, now: float
) -> bool:
    remaining = max(1, int(expires - now))
    key = _BG_USED_PREFIX + hashlib.sha256(token.encode()).hexdigest()
    if redis is not None:
        try:
            acquired = redis.set(key, "1", nx=True, ex=remaining)
            return bool(acquired)
        except Exception as exc:  # noqa: BLE001 — 无法保证一次性 ⇒ fail-closed
            logger.warning("break-glass token 一次性校验失败（拒绝）: %s", exc)
            return False
    with _bg_lock:
        stale = [k for k, exp in _bg_used.items() if exp <= now]
        for k in stale:
            _bg_used.pop(k, None)
        if key in _bg_used:
            return False
        _bg_used[key] = expires
        return True


def validate_break_glass(
    username: str,
    token: Optional[str],
    client_ip: str,
    *,
    redis=None,
    now: Optional[float] = None,
) -> Tuple[bool, str, bool]:
    """返回 ``(allowed, reason, requested)``。

    ``requested`` = 用户命中 break-glass 名单或请求带了 token（用于独立审计）。
    token 合法即消费（单次）；失败/过期/来源不符一律 fail-closed。
    """
    user = _username(username)
    requested = bool(token) or user in break_glass_users()
    if user not in break_glass_users():
        return False, "user_not_allowed", requested
    secret = _break_glass_secret()
    if not secret:
        return False, "secret_missing", requested
    ip = canonical_ip(client_ip)
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return False, "source_invalid", requested
    if not any(addr in network for network in break_glass_allowed_networks()):
        return False, "source_not_allowed", requested
    if not token:
        return False, "token_missing", requested
    parts = token.strip().split(".")
    if len(parts) != 4 or parts[0] != "bg1":
        return False, "token_invalid", requested
    try:
        expires = int(parts[1])
    except ValueError:
        return False, "token_invalid", requested
    current = time.time() if now is None else float(now)
    if expires <= current:
        return False, "token_expired", requested
    nonce = parts[2]
    expected = hmac.new(
        secret.encode(), f"bg1|{user}|{expires}|{nonce}".encode(),
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(expected, parts[3]):
        return False, "token_invalid", requested
    if not _consume_break_glass_token(token, expires, redis=redis, now=current):
        return False, "token_used", requested
    return True, "ok", True
