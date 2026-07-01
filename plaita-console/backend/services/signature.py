"""
HMAC 签名对称验签

与 ``plaita/client.py:generate_signature`` 算法对称：
1. key_time = "{sign_time};{sign_expire}"（sign_expire = sign_time + validity）
2. key_string = HMAC-SHA256(secret_key, key_time).hexdigest()
3. signature = HMAC-SHA256(key_string, "{sign_time}\\n").hexdigest()
4. Authorization = urlencode({secret-id, sign-time, key-time, signature})

验签：解析 Authorization → 校 secret-id → 校时效（从 key-time 取 sign_expire）
→ 用客户端的 key-time 重算 signature → 常量时间比较。
"""
import hashlib
import hmac
import logging
from time import time
from typing import Optional
from urllib.parse import parse_qs, urlparse

logger = logging.getLogger(__name__)

# 允许的时钟偏移（秒），客户端签发时间不应过分超前于此
_MAX_SKEW = 300


def _compute_signature(secret_key: str, sign_time: int, key_time: str) -> str:
    key_string = hmac.new(
        secret_key.encode(), key_time.encode(), hashlib.sha256
    ).hexdigest()
    string_to_sign = f"{sign_time}\n"
    return hmac.new(
        key_string.encode(), string_to_sign.encode(), hashlib.sha256
    ).hexdigest()


def verify_authorization(
    authorization: str,
    secret_id: str,
    secret_key: str,
    now: Optional[float] = None,
) -> bool:
    """校验 Authorization 头。通过返回 True，否则 False。"""
    if not authorization or not secret_id or not secret_key:
        return False
    parsed = parse_qs(urlparse(f"?{authorization}").query)
    try:
        req_secret_id = parsed["secret-id"][0]
        sign_time = int(parsed["sign-time"][0])
        req_key_time = parsed["key-time"][0]
        req_signature = parsed["signature"][0]
    except (KeyError, IndexError, ValueError):
        return False

    if not hmac.compare_digest(req_secret_id, secret_id):
        return False

    # key-time = "sign_time;sign_expire"
    parts = req_key_time.split(";")
    if len(parts) != 2:
        return False
    try:
        kt_sign_time = int(parts[0])
        sign_expire = int(parts[1])
    except ValueError:
        return False
    if kt_sign_time != sign_time:
        return False

    now = time() if now is None else now
    if now > sign_expire:
        return False
    if sign_time - now > _MAX_SKEW:
        return False

    expected_sig = _compute_signature(secret_key, sign_time, req_key_time)
    return hmac.compare_digest(req_signature, expected_sig)
