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
- admin 专属前缀：/api/users、/api/audit、/api/credentials、/api/cluster、/api/tenants
- **平台身份专属（#260）**：/api/users 的全局账号变更（创建/改角色/改密/删除）
  与 /api/services/{id}/stop 等全局生命周期操作要求 ``platform_admin``——
  租户角色 admin ≠ 平台身份；本人改密是自助操作，不受此限
- 生产环境（PLAITA_CONSOLE_ENV=prod）的 DELETE：admin

租户上下文（多租户）：
- 会话用户钉死在其活跃租户（login/switch-tenant 决定），request.state.tenant_id
- 平台管理员 / API-Key 可带 ``X-Tenant-ID`` 头指定租户；不带则 tenant_id=None
  （平台全量视角：读跨租户，写需明确租户——由 auth.tenant_scope(required=True) 把关）
- **普通用户会话无活跃租户 → fail-closed（#254）**：tenant_scope 一律 403，
  绝不把 None 解释为平台全量，写路径也不回退 default；只有可证明的平台级
  身份（platform_admin / API-Key / admin-key）才享有 None=全量的语义。

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
_ADMIN_PREFIXES = (
    "/api/users",
    "/api/audit",
    "/api/credentials",
    "/api/cluster",
    "/api/tenants",
)

#: 平台级 auth_source（require_auth 写入 request.state.auth_source）。
_PLATFORM_AUTH_SOURCES = frozenset({"api-key", "admin-key"})

#: #254：普通会话无活跃租户的具名拒绝。任何「无租户 → 平台全量/default」
#: 的降级都不得再出现（fail-closed，对齐 #239 租户参数不得降级）。
NO_ACTIVE_TENANT_DETAIL = "当前会话无活跃租户，请先切换租户或联系管理员"

#: #260：全局账号 / 全局生命周期操作的具名拒绝。租户角色（哪怕是租户
#: admin）不是平台身份，不能借此触达平台账号或全局服务。
PLATFORM_ADMIN_REQUIRED_DETAIL = "该操作需要平台管理员"


def require_auth(request: Request) -> Dict[str, Any]:
    """FastAPI Depends：会话/API-Key/ insecure 三源认证 + 角色/租户裁决。

    通过后把 {actor, role, source, tenant_id, platform_admin} 写入 request.state
    供审计与租户过滤使用。
    """
    settings = get_settings()
    token = _extract_bearer(request)
    api_key_header = request.headers.get("X-Admin-API-Key")
    header_tenant = (request.headers.get("X-Tenant-ID") or "").strip() or None

    actor = role = source = None
    tenant_id: Optional[str] = None
    platform_admin = False

    expected_key = (settings.admin_api_key or "").strip()
    if expected_key and api_key_header and secrets.compare_digest(api_key_header.strip(), expected_key):
        actor, role, source = "api-key", "admin", "api-key"
        platform_admin = True  # API-Key 为平台级凭证
    elif expected_key and not token and _bearer_matches_admin_key(request, expected_key):
        actor, role, source = "api-key", "admin", "api-key"
        platform_admin = True
    elif token:
        try:
            resolved = users_svc.resolve_session(request.app.state.store, token)
        except users_svc.TenantDisabledError:
            # 活跃租户被停用（#27）：会话本身有效，是租户不可用 → 403
            raise HTTPException(status_code=403, detail="租户已停用，会话不可用")
        if resolved is None:
            raise HTTPException(status_code=401, detail="会话无效或已过期，请重新登录")
        actor = resolved["username"]
        role = resolved["role"]
        source = "session"
        tenant_id = resolved.get("tenant_id")
        platform_admin = bool(resolved.get("platform_admin"))
    elif settings.allow_insecure_admin:
        actor, role, source = "insecure", "admin", "insecure"
        platform_admin = True
        logger.warning(
            "管理面鉴权已跳过（PLAITA_CONSOLE_ALLOW_INSECURE_ADMIN=true）；"
            "切勿用于可被网络触达的部署"
        )
    else:
        raise HTTPException(
            status_code=401,
            detail="未认证：请登录（POST /api/auth/login）或配置 X-Admin-API-Key",
        )

    # X-Tenant-ID：仅平台管理员 / API-Key 可指定；普通租户用户钉死其活跃租户
    if header_tenant:
        if not platform_admin:
            raise HTTPException(
                status_code=403,
                detail="仅平台管理员可指定 X-Tenant-ID",
            )
        tenant_id = header_tenant

    # #254：普通会话无活跃租户 → fail-closed。身份不可证明为平台级时，
    # tenant_id=None 不得进入管理面（否则 tenant_scope 会把它当平台全量）。
    # /api/auth/* 是会话自操作（me/logout/switch-tenant），保留可用性。
    if not platform_admin and tenant_id is None:
        if not request.url.path.startswith("/api/auth/"):
            raise HTTPException(status_code=403, detail=NO_ACTIVE_TENANT_DETAIL)

    # 先落 state 再裁决：_authorize 的自助改密例外需要 actor（#260），
    # 拒绝审计也需要 actor。_authorize 抛错时 state 已可观测，无副作用。
    request.state.actor = actor
    request.state.role = role
    request.state.auth_source = source
    request.state.tenant_id = tenant_id
    request.state.platform_admin = platform_admin
    _authorize(request, role)
    return {
        "actor": actor,
        "role": role,
        "tenant_id": tenant_id,
        "platform_admin": platform_admin,
    }


def has_auth_context(request: Request) -> bool:
    """请求是否经过 require_auth（state.tenant_id 已写入）。

    未挂 require_auth 的测试/内部直调挂载没有该属性；生产管理面全部经
    ``_mount_admin`` 注入依赖。
    """
    state = getattr(request, "state", None)
    return state is not None and hasattr(state, "tenant_id")


def is_platform_request(request: Request) -> bool:
    """当前请求是否为可证明的平台级身份（#254）。

    ``tenant_scope()`` 只有在它返回真时才允许 None=平台全量。
    """
    state = getattr(request, "state", None)
    if state is None:
        return False
    if bool(getattr(state, "platform_admin", False)):
        return True
    return getattr(state, "auth_source", None) in _PLATFORM_AUTH_SOURCES


def tenant_scope(request: Request, *, required: bool = False) -> Optional[str]:
    """取当前请求的租户上下文。

    - 读路径（required=False）：具体租户 → 该租户；可证明的平台级身份且无
      租户上下文 → None（平台全量视角，跨租户查询不过滤）
    - 写路径（required=True）：无租户上下文的**平台级身份**（api-key /
      未切换租户的平台管理员）回退 default 租户——保持单租户时代的
      API-key/E2E 集成兼容。
    - **普通会话无活跃租户（#254）→ 403 具名拒绝**：读路径不回退平台全量，
      写路径不回退 default。被移出租户的 viewer/editor 重登后即落在此分支。
    - state 上无 tenant_id 属性（未挂 require_auth 的测试/内部挂载）时
      同样回退 default，保持单租户时代的直调行为。
    """
    try:
        from .models.flow import DEFAULT_TENANT_ID
    except ImportError:
        from models.flow import DEFAULT_TENANT_ID  # type: ignore

    state = getattr(request, "state", None)
    if state is None or not hasattr(state, "tenant_id"):
        return None if not required else DEFAULT_TENANT_ID
    tenant = state.tenant_id
    if tenant:
        return tenant
    if is_platform_request(request):
        return None if not required else DEFAULT_TENANT_ID
    raise HTTPException(status_code=403, detail=NO_ACTIVE_TENANT_DETAIL)


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
    path = request.url.path

    if role is None:
        # #254：无活跃租户的普通会话没有租户角色。会话自操作仍放行，
        # 其余路径在 require_auth 前置已具名拒绝（这里兜底直调场景）。
        if path.startswith("/api/auth/"):
            return
        raise HTTPException(status_code=403, detail=NO_ACTIVE_TENANT_DETAIL)

    if _ROLE_RANK.get(role, -1) < 0:
        raise HTTPException(status_code=403, detail=f"未知角色: {role}")

    method = request.method.upper()

    # #260：自助改密不是全局账号操作——任意已认证角色都可改自己的密码。
    # 仅当路径用户名与当前 actor 完全一致时豁免 admin 前缀（路由 handler
    # 会再用解码后的路径参数复核，二者不一致即按全局操作裁决）。
    actor = getattr(getattr(request, "state", None), "actor", None)
    self_password = (
        method == "POST"
        and bool(actor)
        and path == f"/api/users/{actor}/password"
    )

    if any(path.startswith(p) for p in _ADMIN_PREFIXES):
        if not self_password and role != "admin":
            raise HTTPException(status_code=403, detail="该操作需要 admin 角色")

    # /api/auth/* 是会话自操作（me/logout/switch-tenant），任何已认证角色可用
    if path.startswith("/api/auth/"):
        return

    # 自助改密同样跳过 viewer 写闸（写的是自己的凭据）
    if self_password:
        return

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


def ensure_platform_admin(
    request: Request,
    identity: Dict[str, Any],
    *,
    action: str,
    resource: str = "",
    target: str = "",
) -> None:
    """全局账号 / 全局生命周期操作的平台身份闸（#260）。

    租户角色 ``admin`` 只代表租户内管理权，不是平台身份。命中拒绝时：
    - 审计记录 ``<action>.denied``（detail.denied=true，含 method/path）
    - WARNING 日志（拒绝尝试必须可观测，不做静默降级）

    自助操作（改自己的密码）不走本函数；路由层先按 ``actor`` 判断。
    """
    if identity.get("platform_admin"):
        return
    logger.warning(
        "拒绝非平台管理员操作 action=%s actor=%s target=%s path=%s",
        action,
        identity.get("actor"),
        target,
        request.url.path,
    )
    try:
        from .services import audit as audit_svc
    except ImportError:
        from services import audit as audit_svc  # type: ignore

    audit_svc.record(
        request,
        action=f"{action}.denied",
        resource=resource or "auth",
        resource_id=str(target or ""),
        detail={
            "denied": True,
            "reason": "platform_admin_required",
            "method": request.method,
            "path": request.url.path,
        },
    )
    raise HTTPException(status_code=403, detail=PLATFORM_ADMIN_REQUIRED_DETAIL)


# 兼容旧引用（部分测试/脚本直接 import）
def require_admin_auth(*args, **kwargs) -> None:  # pragma: no cover
    raise HTTPException(
        status_code=500,
        detail="require_admin_auth 已由 RBAC require_auth 取代",
    )

