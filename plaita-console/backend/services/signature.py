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
→ 若带 nonce: 查 nonce 缓存, 命中即拒绝 (重放), 未命中则记入。

2026-07 新增重放保护 (B 方案, 增量兼容):
- 客户端 ``replay_protected=True`` 时每次请求生成 uuid nonce 并覆盖进签名;
- 服务端检测 Authorization 里有没有 ``nonce`` 字段决定走新/旧验签路径,
  未升级的旧客户端继续走旧路径, 不影响。
- nonce 缓存 process-local (内存字典 + TTL 惰性清理)。多进程部署需要换
  Redis 实现, 见 ``_NonceCache`` 文档。
"""
import hashlib
import hmac
import logging
import threading
from time import time
from typing import Dict, Optional, Tuple
from urllib.parse import parse_qs, urlparse

logger = logging.getLogger(__name__)

# 允许的时钟偏移（秒），客户端签发时间不应过分超前于此
_MAX_SKEW = 300


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


class _NonceCache:
    """进程内 nonce 缓存, 记录已用过的 nonce 防重放。

    TTL = ``sign_expire`` (取自每次验签的 key-time)。惰性清理: 每次 ``check``
    顺手清掉已过期的条目, 避免独立清理线程。

    多进程部署 (gunicorn -w N) 下每个 worker 有独立缓存, 跨 worker 重放仍
    可能在窗口期内绕过——生产部署建议注入 Redis 后端 (TTL 原生支持)。
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


# 模块级单例。测试可通过 reset_nonce_cache() 重置。
_nonce_cache = _NonceCache()


def reset_nonce_cache() -> None:
    """测试专用: 清空进程内 nonce 缓存。"""
    _nonce_cache.reset()


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
) -> bool:
    """校验 Authorization 头。通过返回 True，否则 False。"""
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

    # 签名 + 时效都对, 再做重放检查 (仅当 nonce 存在)
    if fields["nonce"] is not None:
        if not _nonce_cache.check_and_record(fields["nonce"], sign_expire):
            logger.warning("replay detected: nonce=%s already used", fields["nonce"])
            return False

    return True
