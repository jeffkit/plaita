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
- **平台身份专属（#260 / #261）**：/api/users 的全局账号变更（创建/改角色/
  改密/删除）、/api/services/{id}/stop，以及 /api/cluster 与 /api/clusters
  的全局生命周期 / 基础设施配置 / 集群登记面要求 ``platform_admin``——
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

import hashlib
import logging
import secrets
from typing import Any, AsyncGenerator, Dict, Optional

from fastapi import Depends, Header, HTTPException, Request

try:
    from .config import get_settings
    from .services import users_svc
    from .services.tenant_write_guard import (
        bind_tenant_generation,
        reset_tenant_generation,
    )
except ImportError:
    from config import get_settings  # type: ignore
    from services import users_svc  # type: ignore
    from services.tenant_write_guard import (  # type: ignore
        bind_tenant_generation,
        reset_tenant_generation,
    )

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

#: 审计 action 的资源名（按 path 前缀映射，避免把完整路径塞进 action）。
_RESOURCE_PREFIXES = (
    ("/api/flowVersion", "flow_version"),
    ("/api/flows", "flow"),
    ("/api/executions", "execution"),
    ("/api/schedules", "schedule"),
    ("/api/nodes", "node"),
    ("/api/property-types", "property_type"),
    ("/api/events", "event"),
    ("/api/credentials", "credential"),
    ("/api/clusters", "cluster"),
    ("/api/cluster", "cluster"),
    ("/api/services", "service"),
    ("/api/queues", "queue"),
    ("/api/dispatch", "dispatch"),
    ("/api/tenants", "tenant"),
    ("/api/users", "user"),
    ("/api/audit", "audit"),
    ("/api/copilot", "copilot"),
    ("/api/supervisor", "supervisor"),
)


def _audit_resource(path: str) -> str:
    clean = (path or "").split("?", 1)[0]
    for prefix, resource in _RESOURCE_PREFIXES:
        if clean == prefix or clean.startswith(prefix + "/"):
            return resource
    return "auth"


def _audit_denied(request: Request, action: str, resource: str, reason: str,
                  *, actor: Optional[str] = None, tenant_id: Optional[str] = None,
                  detail: Optional[Dict[str, Any]] = None) -> None:
    """拒绝审计（A55-S2）：state 未写入的认证失败也可用（显式 actor）。"""
    try:
        from .services import audit as audit_svc
    except ImportError:
        from services import audit as audit_svc  # type: ignore

    payload: Dict[str, Any] = {
        "denied": True,
        "reason": reason,
        "method": request.method,
        "path": request.url.path,
    }
    if detail:
        payload.update(detail)
    audit_svc.record_denied(
        request,
        action=action,
        resource=resource,
        detail=payload,
        actor=actor,
        tenant_id=tenant_id,
        key_suffix=reason,
    )


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
    tenant_generation = None
    platform_admin = False
    key_fingerprint = ""

    expected_key = (settings.admin_api_key or "").strip()
    if expected_key and api_key_header and secrets.compare_digest(api_key_header.strip(), expected_key):
        actor, role, source = "api-key", "admin", "api-key"
        platform_admin = True  # API-Key 为平台级凭证
        # A55-S3：只存指纹（sha256 前 16 位），不存 key 明文。
        key_fingerprint = hashlib.sha256(
            api_key_header.strip().encode("utf-8")
        ).hexdigest()[:16]
    elif expected_key and not token and _bearer_matches_admin_key(request, expected_key):
        actor, role, source = "api-key", "admin", "api-key"
        platform_admin = True
        key_fingerprint = hashlib.sha256(
            _extract_authorization_bearer(request).encode("utf-8")
        ).hexdigest()[:16]
    elif token:
        try:
            resolved = users_svc.resolve_session(request.app.state.store, token)
        except users_svc.TenantDisabledError:
            # 活跃租户被停用（#27）：会话本身有效，是租户不可用 → 403
            _audit_denied(
                request, "auth.denied", "auth", "tenant_context",
                actor="anonymous", detail={"cause": "tenant_disabled"},
            )
            raise HTTPException(status_code=403, detail="租户已停用，会话不可用")
        if resolved is None:
            _audit_denied(
                request, "auth.denied", "auth", "unauthenticated",
                actor="anonymous", detail={"cause": "invalid_session"},
            )
            raise HTTPException(status_code=401, detail="会话无效或已过期，请重新登录")
        actor = resolved["username"]
        role = resolved["role"]
        source = "session"
        tenant_id = resolved.get("tenant_id")
        # A51-02：会话解析（单条 JOIN）与身份同一快照给出租户世代；请求
        # 生命周期内所有写路径以它为准，不重新捕获。
        tenant_generation = resolved.get("tenant_generation")
        platform_admin = bool(resolved.get("platform_admin"))
    elif settings.allow_insecure_admin:
        actor, role, source = "insecure", "admin", "insecure"
        platform_admin = True
        logger.warning(
            "管理面鉴权已跳过（PLAITA_CONSOLE_ALLOW_INSECURE_ADMIN=true）；"
            "切勿用于可被网络触达的部署"
        )
    else:
        _audit_denied(
            request, "auth.denied", "auth", "unauthenticated",
            actor="anonymous", detail={"cause": "missing_credentials"},
        )
        raise HTTPException(
            status_code=401,
            detail="未认证：请登录（POST /api/auth/login）或配置 X-Admin-API-Key",
        )

    # X-Tenant-ID：仅平台管理员 / API-Key 可指定；普通租户用户钉死其活跃租户
    if header_tenant:
        if not platform_admin:
            _audit_denied(
                request, "auth.denied", "auth", "tenant_context",
                actor=actor, detail={
                    "cause": "header_requires_platform_admin",
                    "target_tenant": header_tenant,
                },
            )
            raise HTTPException(
                status_code=403,
                detail="仅平台管理员可指定 X-Tenant-ID",
            )
        tenant_id = header_tenant
        # A51-02：平台身份代写目标租户时，认证处读一次 Tenant 行捕获世代。
        # 行缺失/无世代 → 拒绝（不把 None 当合法租户交给写路径）。
        # 内嵌/测试挂载没有 app.state.store（无 DB）：退化为不捕获世代，
        # 但必须留 WARNING——这是「无法校验租户存在性」的可观测降级。
        store = getattr(request.app.state, "store", None)
        if store is None:
            logger.warning(
                "平台身份指定 X-Tenant-ID=%s，但应用未配置 app.state.store；"
                "跳过租户世代捕获（内嵌/测试挂载）",
                header_tenant,
            )
        else:
            tenant_generation = users_svc.read_tenant_generation(
                store, header_tenant
            )
            if tenant_generation is None:
                if not users_svc.tenant_table_initialized(store):
                    # 未 bootstrap 的内嵌/测试挂载：租户只存在于 Redis
                    # namespace。无法校验存在性 → 不捕获世代，但留 WARNING。
                    logger.warning(
                        "租户表为空（未 bootstrap），跳过 X-Tenant-ID=%s 的"
                        "世代捕获（内嵌/测试挂载）",
                        header_tenant,
                    )
                else:
                    _audit_denied(
                        request, "auth.denied", "auth", "tenant_context",
                        actor=actor, tenant_id=header_tenant,
                        detail={
                            "cause": "tenant_not_found",
                            "target_tenant": header_tenant,
                        },
                    )
                    raise HTTPException(
                        status_code=403,
                        detail=f"租户不存在或不可用: {header_tenant}",
                    )

    # #254：普通会话无活跃租户 → fail-closed。身份不可证明为平台级时，
    # tenant_id=None 不得进入管理面（否则 tenant_scope 会把它当平台全量）。
    # /api/auth/* 是会话自操作（me/logout/switch-tenant），保留可用性。
    if not platform_admin and tenant_id is None:
        if not request.url.path.startswith("/api/auth/"):
            _audit_denied(
                request, "auth.denied", "auth", "tenant_context",
                actor=actor, detail={"cause": "no_active_tenant"},
            )
            raise HTTPException(status_code=403, detail=NO_ACTIVE_TENANT_DETAIL)

    # 先落 state 再裁决：_authorize 的自助改密例外需要 actor（#260），
    # 拒绝审计也需要 actor。_authorize 抛错时 state 已可观测，无副作用。
    request.state.actor = actor
    request.state.role = role
    request.state.auth_source = source
    request.state.tenant_id = tenant_id
    request.state.key_fingerprint = key_fingerprint
    # A51-02：请求级世代（会话解析同一 JOIN / 平台 X-Tenant-ID 的一次读）。
    # 管理面依赖把它 bind 到 ContextVar；无它（旧挂载/CLI）时各服务回退
    # 「写路径首次读捕获」的存量行为。
    request.state.tenant_generation = tenant_generation
    request.state.platform_admin = platform_admin
    _authorize(request, role)
    return {
        "actor": actor,
        "role": role,
        "tenant_id": tenant_id,
        "platform_admin": platform_admin,
    }


async def tenant_generation_context(
    request: Request,
    identity: Dict[str, Any] = Depends(require_auth),
) -> AsyncGenerator[None, None]:
    """管理面请求级租户世代绑定（A51-02 最小改造清单第 3 条）。

    在 ``require_auth`` 之后、handler 之前把认证快照里的世代放进
    ContextVar；同一请求的所有租户作用域写路径（含在 threadpool 里执行的
    sync handler，anyio 会复制当前 context）都据此在 flush 后复核。无绑定
    （CLI/单测直接调服务/旧挂载）时 ContextVar 为 None，各服务回退存量行为。

    必须是 **async** 依赖：sync 依赖经线程池执行，其 ContextVar 修改不会
    传播回请求上下文。
    """
    token = bind_tenant_generation(
        getattr(request.state, "tenant_id", None),
        getattr(request.state, "tenant_generation", None),
    )
    try:
        yield
    finally:
        reset_tenant_generation(token)


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


def _extract_authorization_bearer(request: Request) -> str:
    """Authorization 头里的 Bearer 值（无则空串）；仅供指纹计算，不落日志。"""
    auth = request.headers.get("Authorization") or ""
    parts = auth.strip().split(None, 1)
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1].strip()
    return ""


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
    resource = _audit_resource(path)

    if role is None:
        # #254：无活跃租户的普通会话没有租户角色。会话自操作仍放行，
        # 其余路径在 require_auth 前置已具名拒绝（这里兜底直调场景）。
        if path.startswith("/api/auth/"):
            return
        _audit_denied(
            request, f"{resource}.denied", resource, "tenant_context",
            detail={"cause": "no_active_tenant"},
        )
        raise HTTPException(status_code=403, detail=NO_ACTIVE_TENANT_DETAIL)

    if _ROLE_RANK.get(role, -1) < 0:
        _audit_denied(
            request, f"{resource}.denied", resource, "unknown_role",
            detail={"role": role},
        )
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
            _audit_denied(
                request, f"{resource}.denied", resource, "role_required",
                detail={"required_role": "admin"},
            )
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
        _audit_denied(
            request, f"{resource}.denied", resource, "role_required",
            detail={"required_role": "editor"},
        )
        raise HTTPException(status_code=403, detail="该操作需要 editor 及以上角色")

    if (
        method == "DELETE"
        and get_settings().console_env == "prod"
        and role != "admin"
    ):
        _audit_denied(
            request, f"{resource}.denied", resource, "role_required",
            detail={"required_role": "admin", "prod_delete": True},
        )
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

    拒绝行走 ``record_denied``（A55-S2）：同一 ``(action, actor, ip, tenant)``
    在窗口内只落一行——平台闸是低权流量的高频目标（DLQ replay、cluster
    生命周期），不能按请求数撑大审计表。

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

    audit_svc.record_denied(
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
        key_suffix="platform_admin_required",
    )
    raise HTTPException(status_code=403, detail=PLATFORM_ADMIN_REQUIRED_DETAIL)


# 兼容旧引用（部分测试/脚本直接 import）
def require_admin_auth(*args, **kwargs) -> None:  # pragma: no cover
    raise HTTPException(
        status_code=500,
        detail="require_admin_auth 已由 RBAC require_auth 取代",
    )

