"""审计/部署查询 API（admin）。"""
from datetime import datetime
from typing import Dict, Optional

from fastapi import APIRouter, Depends, Request

try:
    from ..auth import require_auth, tenant_scope
    from ..services import audit as audit_svc
except ImportError:
    from auth import require_auth, tenant_scope  # type: ignore
    from services import audit as audit_svc  # type: ignore

router = APIRouter()


def _tenant_generation_since(tenant: Optional[str]) -> Optional[datetime]:
    """租户当前世代起点（A54-05）；取不到 ⇒ None（调用方 fail-closed）。"""
    if not tenant:
        return None
    try:
        from ..services import flow_store, tenants_svc
    except ImportError:
        from services import flow_store, tenants_svc  # type: ignore
    try:
        generation = tenants_svc.tenant_generation(
            flow_store.get_flow_store(), tenant
        )
    except Exception:  # noqa: BLE001 — 世代不可证明 ⇒ 租户视角不返回历史
        import logging

        logging.getLogger(__name__).exception(
            "读取租户世代失败（tenant=%s），审计读面按 fail-closed 处理", tenant
        )
        return None
    if not generation:
        return None
    try:
        return datetime.fromisoformat(generation)
    except ValueError:
        return None


@router.get("/audit")
def list_audit(
    action: Optional[str] = None,
    actor: Optional[str] = None,
    limit: int = 200,
    request: Request = None,
    identity: Dict = Depends(require_auth),
):
    """最近审计记录（默认 200 条，可按 action/actor 过滤）。

    A54-05：租户视角只看本租户**当前世代**（同名重建后旧世代审计不可见）；
    平台身份仍可全量审计（合规证据不随租户删除物理清除）。
    """
    tenant = tenant_scope(request) if request is not None else None
    since: Optional[datetime] = None
    if tenant is not None and not bool(identity.get("platform_admin")):
        since = _tenant_generation_since(tenant)
        if since is None:
            return {"logs": []}
    return {
        "logs": audit_svc.list_audit(
            action=action,
            actor=actor,
            limit=min(limit, 1000),
            tenant_id=tenant,
            since=since,
        )
    }


@router.get("/deployments")
def list_deployments(flow_id: Optional[str] = None, request: Request = None,
                     _: Dict = Depends(require_auth)):
    tenant = tenant_scope(request) if request is not None else None
    return {"deployments": audit_svc_deployments(flow_id, tenant_id=tenant)}


def audit_svc_deployments(flow_id: Optional[str], tenant_id: Optional[str] = None):
    try:
        from ..services import deployments as deployments_svc
    except ImportError:
        from services import deployments as deployments_svc  # type: ignore
    return deployments_svc.list_deployments(flow_id=flow_id, tenant_id=tenant_id)
