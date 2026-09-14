"""
对外契约接口 — 兼容 PlaitaClient（多租户：按租户发密钥）

POST /api/flowVersion/semver/detail
- 请求：application/x-www-form-urlencoded，字段 flowId、version
- 鉴权：Authorization 头，HMAC-SHA256 签名（与 plaita/client.py 对称）
  - 租户密钥：Authorization 的 secret_id 命中 tenants.contract_secret_id
    → 用该租户的 contract_secret_key 验签，只能拉取本租户已发布流程
  - 平台密钥（兼容）：命中全局 PLAITA_CONSOLE_SECRET_ID/SECRET_KEY
    → 平台上下文，可拉取任意租户已发布流程（存量集成不破坏）
- 响应：{ code, message, data: { flow: "<Flow JSON string>" } }
  - code 为 0 表示成功；非零表示业务错误（flow 不存在/未发布）
  - 鉴权失败返回 HTTP 401
"""
from typing import Optional

from fastapi import APIRouter, HTTPException, Request

try:
    from ..config import get_settings
    from ..services import flow_store, signature, tenants_svc
except ImportError:
    from config import get_settings  # type: ignore
    from services import flow_store, signature, tenants_svc  # type: ignore

router = APIRouter()


def _envelope(code: int, message: str, flow: Optional[str] = None) -> dict:
    data = {"flow": flow} if flow is not None else {}
    return {"code": code, "message": message, "data": data}


def _resolve_caller(authorization: str) -> tuple[str, Optional[str]]:
    """按 Authorization 头解析（secret_id, tenant_id）。

    返回 (secret_id, tenant_id_or_None)；tenant_id=None 表示平台上下文。
    未命中任何密钥抛 HTTPException(401)。
    """
    settings = get_settings()
    # 1) 租户密钥：secret_id → 租户，用租户密钥验签
    parsed_secret_id = signature.extract_secret_id(authorization)
    if parsed_secret_id:
        try:
            tenant = tenants_svc.find_tenant_by_contract_secret_id(
                flow_store.get_flow_store(), parsed_secret_id
            )
        except LookupError:
            tenant = None
        if tenant is not None:
            if not signature.verify_authorization(
                authorization, tenant["contract_secret_id"], tenant["contract_secret_key"]
            ):
                raise HTTPException(status_code=401, detail="签名校验失败")
            return tenant["contract_secret_id"], tenant["id"]
    # 2) 平台全局密钥（兼容存量集成）
    if settings.secret_id and settings.secret_key and signature.verify_authorization(
        authorization, settings.secret_id, settings.secret_key
    ):
        return settings.secret_id, None
    raise HTTPException(status_code=401, detail="签名校验失败")


@router.post("/flowVersion/semver/detail")
async def flow_version_detail(request: Request):
    settings = get_settings()
    # fail-closed：租户密钥与全局密钥都未配置时禁用
    has_global = bool(settings.secret_id and settings.secret_key)
    has_tenant_secret = _any_tenant_secret()
    if not has_global and not has_tenant_secret:
        raise HTTPException(
            status_code=503,
            detail="契约接口未配置任何密钥（租户契约密钥或 PLAITA_CONSOLE_SECRET_ID/SECRET_KEY），已禁用",
        )

    authorization = request.headers.get("authorization", "")
    _, tenant_id = _resolve_caller(authorization)

    form = await request.form()
    flow_id = form.get("flowId")
    version = form.get("version")

    if not flow_id or not version:
        return _envelope(1, "flowId/version 不能为空")

    store = flow_store.get_flow_store()
    out = store.get_version(flow_id, version, tenant_id=tenant_id)
    if out is None:
        return _envelope(2, f"流程不存在: {flow_id}@{version}")
    if out.status != "published":
        return _envelope(3, f"版本未发布: {flow_id}@{version}")

    return _envelope(0, "success", flow=out.definition)


def _any_tenant_secret() -> bool:
    try:
        from ..models.flow import Tenant
    except ImportError:
        from models.flow import Tenant  # type: ignore
    from sqlalchemy import select

    with flow_store.get_flow_store()._session_local() as session:
        row = session.scalars(
            select(Tenant).where(Tenant.contract_secret_key != "")
        ).first()
        return row is not None
