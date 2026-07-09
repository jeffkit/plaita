"""
管理面 API 鉴权。

生产默认 fail-closed：未配置 PLAITA_CONSOLE_ADMIN_API_KEY 时拒绝所有管理 API。
本地开发可设 PLAITA_CONSOLE_ALLOW_INSECURE_ADMIN=true 跳过（仅限显式 opt-in）。

契约面 /api/flowVersion/* 使用独立 HMAC，不走本依赖。
"""
from __future__ import annotations

import logging
import secrets
from typing import Optional

from fastapi import Header, HTTPException

try:
    from ..config import get_settings
except ImportError:
    from config import get_settings

logger = logging.getLogger(__name__)


def require_admin_auth(
    x_admin_api_key: Optional[str] = Header(default=None, alias="X-Admin-API-Key"),
    authorization: Optional[str] = Header(default=None),
) -> None:
    """FastAPI Depends：校验管理面 API Key。"""
    settings = get_settings()
    expected = (settings.admin_api_key or "").strip()

    if not expected:
        if settings.allow_insecure_admin:
            logger.warning(
                "管理面鉴权已跳过（PLAITA_CONSOLE_ALLOW_INSECURE_ADMIN=true）；"
                "切勿用于可被网络触达的部署"
            )
            return
        raise HTTPException(
            status_code=503,
            detail=(
                "管理面未配置 PLAITA_CONSOLE_ADMIN_API_KEY。"
                "生产必须设置密钥；本地开发可设 PLAITA_CONSOLE_ALLOW_INSECURE_ADMIN=true。"
            ),
        )

    provided = _extract_key(x_admin_api_key, authorization)
    if not provided or not secrets.compare_digest(provided, expected):
        raise HTTPException(status_code=401, detail="管理面鉴权失败")


def _extract_key(
    x_admin_api_key: Optional[str],
    authorization: Optional[str],
) -> Optional[str]:
    if x_admin_api_key:
        return x_admin_api_key.strip()
    if authorization:
        parts = authorization.strip().split(None, 1)
        if len(parts) == 2 and parts[0].lower() == "bearer":
            return parts[1].strip()
        return authorization.strip()
    return None
