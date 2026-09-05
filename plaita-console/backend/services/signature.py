"""
HMAC 签名对称验签

与 ``plaita/client.py:generate_signature`` 算法对称：
1. key_time = "{sign_time};{sign_expire}"（sign_expire = sign_time + validity）
2. key_string = HMAC-SHA256(secret_key, key_time).hexdigest()
3. signature = HMAC-SHA256(key_string, "{sign_time}\\n").hexdigest()
   - 启用重放保护时 (客户端传 ``nonce``): 签名材料改为 "{sign_time}\\n{nonce}\\n"
4. Authorization = urlencode({secret-id, sign-time, key-time, signature[, nonce]})

验签：解析 Authorization → 校 secret-id → 校时效（从 key-time 取 sign_expire）
→ 用客户端的 key-time 重算 signature → 常量时间比较。
→ 若带 nonce: 查 nonce store, 命中即拒绝 (重放), 未命中则记入。

2026-07 重放保护 (B 方案, 增量兼容) + 多 worker Redis 后端:
- 客户端 ``replay_protected=True`` 时每次请求生成 uuid nonce 并覆盖进签名;
- 服务端检测 Authorization 里有没有 ``nonce`` 字段决定走新/旧验签路径,
  未升级的旧客户端继续走旧路径, 不影响。
- nonce 存储抽象为 ``NonceStore``: ``InMemoryNonceStore`` (单进程/测试) 与
  ``RedisNonceStore`` (生产, 跨 worker 共享, 用 ``SET key NX EX ttl`` 原子去重)。
  用 ``enable_replay_protection(redis_url=...)`` 或 ``configure_nonce_store(...)``
  注入。默认仍是进程内 store——未配置 Redis 时多 worker 部署会在窗口期内漏重放,
  ``enable_replay_protection`` 会显式 warning 提醒。
"""
import hashlib
import hmac
import logging
import threading
from time import time
from typing import Dict, Optional, Protocol, Tuple
from urllib.parse import parse_qs, urlparse

logger = logging.getLogger(__name__)

# 允许的时钟偏移（秒），客户端签发时间不应过分超前于此
_MAX_SKEW = 300

# Redis nonce key 前缀
_REDIS_NONCE_PREFIX = "plaita:nonce:"


def _compute_signature(secret_key: str, sign_time: int, key_time: str,
                       nonce: Optional[str] = None) -> str:
    key_string = hmac.new(
        secret_key.encode(), key_time.encode(), hashlib.sha256
    ).hexdigest()
    if nonce is None:
        string_to_sign = f"{sign_time}\n"
    else:
        string_to_sign = f"{sign_time}\n{nonce}\n"
    return hmac.new(
        key_string.encode(), string_to_sign.encode(), hashlib.sha256
    ).hexdigest()


class NonceStore(Protocol):
    """nonce 存储抽象——记录已用 nonce 防重放。

    ``check_and_record`` 必须是原子的"未见过则记入并返回 True, 见过返回 False",
    否则并发下有 TOCTOU 漏洞。``InMemoryNonceStore`` 用锁保证; ``RedisNonceStore``
    用 ``SET key NX EX ttl`` 单命令原子保证。
    """

    def check_and_record(self, nonce: str, expire_at: float) -> bool: ...

    def reset(self) -> None: ...


class InMemoryNonceStore:
    """进程内 nonce 缓存, 记录已用过的 nonce 防重放。

    TTL = ``sign_expire`` (取自每次验签的 key-time)。惰性清理: 每次 ``check``
    顺手清掉已过期的条目, 避免独立清理线程。

    多进程部署 (gunicorn -w N) 下每个 worker 有独立缓存, 跨 worker 重放仍
    可能在窗口期内绕过——生产部署请用 ``RedisNonceStore``。
    """

    def __init__(self) -> None:
        self._seen: Dict[str, float] = {}  # nonce -> expire_at
        self._lock = threading.Lock()

    def check_and_record(self, nonce: str, expire_at: float) -> bool:
        """返回 True 表示 nonce 未用过 (本次验签可放行), False 表示重放。"""
        now = time()
        with self._lock:
            # 惰性清理: 顺手清掉已过期的
            expired = [k for k, exp in self._seen.items() if exp <= now]
            for k in expired:
                del self._seen[k]
            if nonce in self._seen:
                return False
            self._seen[nonce] = expire_at
            return True

    def reset(self) -> None:
        """测试专用: 清空缓存。"""
        with self._lock:
            self._seen.clear()


class RedisNonceStore:
    """跨 worker 共享的 nonce 存储, 用 Redis ``SET key NX EX ttl`` 原子去重。

    所有 worker 进程连同一 Redis, 真正实现多 worker 重放保护。TTL 由 Redis
    原生过期, 无需惰性清理。``redis`` 客户端在构造期创建一次, 之后复用。

    Args:
        redis_url: Redis 连接字符串 (``redis://host:port/db``)。
        key_prefix: nonce key 前缀, 默认 ``plaita:nonce:``。
    """

    def __init__(self, redis_url: str, key_prefix: str = _REDIS_NONCE_PREFIX) -> None:
        import redis  # 延迟导入: 仅启用 Redis 后端时依赖 redis 包
        self._redis = redis.Redis.from_url(redis_url)
        self._prefix = key_prefix

    def ping(self) -> None:
        """可达性探测：连接/认证失败抛异常，供启动期降级判定。"""
        self._redis.ping()

    def check_and_record(self, nonce: str, expire_at: float) -> bool:
        now = time()
        ttl = int(expire_at - now)
        if ttl < 1:
            # 已过期或刚好到期: 不必记入, 让上层时效校验拒绝即可。
            return True
        key = f"{self._prefix}{nonce}"
        # SET key value NX EX ttl: 仅当 key 不存在时设置, 原子去重。
        acquired = self._redis.set(key, "1", nx=True, ex=ttl)
        return bool(acquired)

    def reset(self) -> None:
        """测试专用: 清掉本前缀下的 nonce。仅用于 fakeredis/测试库, 生产勿调。"""
        for key in self._redis.scan_iter(f"{self._prefix}*"):
            self._redis.delete(key)


# 模块级默认 store。测试可通过 reset_nonce_cache() 重置。
_default_nonce_store: NonceStore = InMemoryNonceStore()

# 外部 store（Redis 等）连接类故障时的进程内兜底 store：降级期间重放判定
# 退化为单进程有效（与本地单机档能力一致），恢复后自动切回原 store。
_fallback_nonce_store = InMemoryNonceStore()


def configure_nonce_store(store: NonceStore) -> None:
    """注入自定义 nonce store (如 ``RedisNonceStore``)。"""
    global _default_nonce_store
    _default_nonce_store = store


def reset_nonce_cache() -> None:
    """测试专用: 清空默认 nonce store。"""
    _default_nonce_store.reset()


def enable_replay_protection(redis_url: Optional[str] = None) -> None:
    """启用服务端重放保护, 配置 nonce store。

    - ``redis_url`` 非空: 配置 ``RedisNonceStore`` (多 worker 共享, 真重放保护)。
      配置前先 ping 一次做可达性校验, 连不上则**降级**为 ``InMemoryNonceStore``
      并 ``logger.warning``——不让 nonce store 的连接故障拖垮启动或造成
      "multi-worker safe" 的假象。
    - ``redis_url`` 为 None: 用 ``InMemoryNonceStore``——**仅在单 worker 部署下
      有效**, 多 worker (gunicorn -w N) 下跨 worker 重放仍可能在窗口期内绕过。
      会显式 ``logger.warning`` 提醒。
    """
    if redis_url:
        try:
            store = RedisNonceStore(redis_url)
            store.ping()
        except Exception as exc:  # noqa: BLE001 — 连接/认证类故障统一降级
            logger.warning(
                "Redis nonce store 不可达（%s: %s），重放保护降级为 IN-MEMORY "
                "nonce store：多 worker 部署在故障期间无法跨 worker 防重放。"
                "恢复 Redis 并重启可启用完整保护。",
                type(exc).__name__, exc,
            )
            configure_nonce_store(InMemoryNonceStore())
        else:
            configure_nonce_store(store)
            logger.info("replay protection enabled with Redis nonce store (multi-worker safe)")
    else:
        configure_nonce_store(InMemoryNonceStore())
        logger.warning(
            "replay protection enabled with IN-MEMORY nonce store: multi-worker "
            "deployments (gunicorn -w N) will NOT be protected against cross-worker "
            "replay within the validity window. Pass redis_url=... to enable a "
            "RedisNonceStore for production."
        )


def _parse_authorization(authorization: str) -> Optional[dict]:
    """解析 Authorization 头, 失败返回 None。"""
    if not authorization:
        return None
    parsed = parse_qs(urlparse(f"?{authorization}").query)
    try:
        return {
            "secret_id": parsed["secret-id"][0],
            "sign_time": int(parsed["sign-time"][0]),
            "key_time": parsed["key-time"][0],
            "signature": parsed["signature"][0],
            "nonce": parsed["nonce"][0] if "nonce" in parsed else None,
        }
    except (KeyError, IndexError, ValueError):
        return None


def _validate_key_time(key_time: str, sign_time: int) -> Optional[Tuple[int, int]]:
    """校 key-time 字段结构, 返回 (kt_sign_time, sign_expire) 或 None。"""
    parts = key_time.split(";")
    if len(parts) != 2:
        return None
    try:
        kt_sign_time = int(parts[0])
        sign_expire = int(parts[1])
    except ValueError:
        return None
    if kt_sign_time != sign_time:
        return None
    return kt_sign_time, sign_expire


def verify_authorization(
    authorization: str,
    secret_id: str,
    secret_key: str,
    now: Optional[float] = None,
    nonce_store: Optional[NonceStore] = None,
) -> bool:
    """校验 Authorization 头。通过返回 True，否则 False。

    Args:
        nonce_store: 可选 nonce 存储覆盖默认 store。默认用模块级 store
            (经 ``configure_nonce_store`` / ``enable_replay_protection`` 配置)。
    """
    fields = _parse_authorization(authorization)
    if fields is None:
        return False

    if not hmac.compare_digest(fields["secret_id"], secret_id):
        return False

    kt = _validate_key_time(fields["key_time"], fields["sign_time"])
    if kt is None:
        return False
    _, sign_expire = kt

    now = time() if now is None else now
    if now > sign_expire:
        return False
    if fields["sign_time"] - now > _MAX_SKEW:
        return False

    expected_sig = _compute_signature(
        secret_key, fields["sign_time"], fields["key_time"], nonce=fields["nonce"],
    )
    if not hmac.compare_digest(fields["signature"], expected_sig):
        return False

    store = nonce_store if nonce_store is not None else _default_nonce_store
    # 签名 + 时效都对, 再做重放检查 (仅当 nonce 存在)
    if fields["nonce"] is not None:
        try:
            ok = store.check_and_record(fields["nonce"], sign_expire)
        except Exception as exc:  # noqa: BLE001 — nonce store 故障不拖垮验签
            # 降级口径（与 main.py 本地单机档一致：可用性优先）：
            # 外部 store（Redis）连接类故障时不让验签 500/fail-closed，而是
            # 退回进程内判定——本进程内重放仍被拦截，跨 worker 防护在故障
            # 窗口内弱化；store 恢复后下一次请求自动走回原路径。
            logger.warning(
                "nonce store 不可用（%s: %s），降级为进程内重放判定"
                "（故障窗口内多 worker 部署的跨 worker 防护弱化）",
                type(exc).__name__, exc,
            )
            ok = _fallback_nonce_store.check_and_record(fields["nonce"], sign_expire)
        if not ok:
            logger.warning("replay detected: nonce=%s already used", fields["nonce"])
            return False

    return True
