"""管理面 API 鉴权（RBAC）。

授权来源（按序）：
1. ``Authorization: Bearer <session token>`` —— /api/auth/login 签发的会话，
   角色取 users 表当前值（admin / editor / viewer）
2. ``X-Admin-API-Key``（或 Bearer 等值）—— 命中 PLAITA_CONSOLE_ADMIN_API_KEY
   即视为 admin（服务账号/脚本兼容）
3. ``PLAITA_CONSOLE_ALLOW_INSECURE_ADMIN=true`` —— 一律 admin（仅本地开发）

角色规则（require_auth 内集中裁决）：
- GET/HEAD：任意已认证角色（viewer+）
- 其余方法：editor+
- admin 专属前缀：/api/users、/api/audit、/api/credentials、/api/cluster
- 生产环境（PLAITA_CONSOLE_ENV=prod）的 DELETE：admin

本地开发未配置任何鉴权且无用户时：首次启动会引导生成 admin 用户，
登录后走会话；ALLOW_INSECURE_ADMIN 仍可完全跳过（仅限本机）。

契约面 /api/flowVersion/* 使用独立 HMAC，不走本依赖。
"""
from __future__ import annotations

import logging
import secrets
from typing import Any, Dict, Optional

from fastapi import Header, HTTPException, Request

try:
    from .config import get_settings
    from .services import users_svc
except ImportError:
    from config import get_settings  # type: ignore
    from services import users_svc  # type: ignore

logger = logging.getLogger(__name__)

_ROLE_RANK = {"viewer": 0, "editor": 1, "admin": 2}
_ADMIN_PREFIXES = ("/api/users", "/api/audit", "/api/credentials", "/api/cluster")


def require_auth(request: Request) -> Dict[str, Any]:
    """FastAPI Depends：会话/API-Key/ insecure 三源认证 + 角色/方法裁决。

    通过后把 {actor, role, source} 写入 request.state 供审计使用。
    """
    settings = get_settings()
    token = _extract_bearer(request)
    api_key_header = request.headers.get("X-Admin-API-Key")

    actor = role = source = None

    expected_key = (settings.admin_api_key or "").strip()
    if expected_key and api_key_header and secrets.compare_digest(api_key_header.strip(), expected_key):
        actor, role, source = "api-key", "admin", "api-key"
    elif expected_key and not token and _bearer_matches_admin_key(request, expected_key):
        actor, role, source = "api-key", "admin", "api-key"
    elif token:
        resolved = users_svc.resolve_session(request.app.state.store, token)
        if resolved is None:
            raise HTTPException(status_code=401, detail="会话无效或已过期，请重新登录")
        actor, role, source = resolved["username"], resolved["role"], "session"
    elif settings.allow_insecure_admin:
        actor, role, source = "insecure", "admin", "insecure"
        logger.warning(
            "管理面鉴权已跳过（PLAITA_CONSOLE_ALLOW_INSECURE_ADMIN=true）；"
            "切勿用于可被网络触达的部署"
        )
    else:
        raise HTTPException(
            status_code=401,
            detail="未认证：请登录（POST /api/auth/login）或配置 X-Admin-API-Key",
        )

    _authorize(request, role)
    request.state.actor = actor
    request.state.role = role
    request.state.auth_source = source
    return {"actor": actor, "role": role}


def _bearer_matches_admin_key(request: Request, expected: str) -> bool:
    auth = request.headers.get("Authorization") or ""
    parts = auth.strip().split(None, 1)
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return secrets.compare_digest(parts[1].strip(), expected)
    return False


def _extract_bearer(request: Request) -> Optional[str]:
    auth = request.headers.get("Authorization") or ""
    parts = auth.strip().split(None, 1)
    if len(parts) == 2 and parts[0].lower() == "bearer":
        token = parts[1].strip()
        # 与 admin key 等值的 Bearer 不当会话处理（上面已单独比对）
        expected_key = (get_settings().admin_api_key or "").strip()
        if expected_key and secrets.compare_digest(token, expected_key):
            return None
        return token
    return None


def _authorize(request: Request, role: str) -> None:
    if _ROLE_RANK.get(role, -1) < 0:
        raise HTTPException(status_code=403, detail=f"未知角色: {role}")

    path = request.url.path
    method = request.method.upper()

    if any(path.startswith(p) for p in _ADMIN_PREFIXES):
        if role != "admin":
            raise HTTPException(status_code=403, detail="该操作需要 admin 角色")

    if method in ("GET", "HEAD", "OPTIONS"):
        return  # viewer+
    if role == "viewer":
        raise HTTPException(status_code=403, detail="该操作需要 editor 及以上角色")

    if (
        method == "DELETE"
        and get_settings().console_env == "prod"
        and role != "admin"
    ):
        raise HTTPException(status_code=403, detail="生产环境的删除操作需要 admin 角色")


# 兼容旧引用（部分测试/脚本直接 import）
def require_admin_auth(*args, **kwargs) -> None:  # pragma: no cover
    raise HTTPException(
        status_code=500,
        detail="require_admin_auth 已由 RBAC require_auth 取代",
    )

