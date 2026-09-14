"""审计/部署查询 API（admin）。"""
from typing import Dict, Optional

from fastapi import APIRouter, Depends, Request

try:
    from ..auth import require_auth, tenant_scope
    from ..services import audit as audit_svc
except ImportError:
    from auth import require_auth, tenant_scope  # type: ignore
    from services import audit as audit_svc  # type: ignore

router = APIRouter()


@router.get("/audit")
def list_audit(
    action: Optional[str] = None,
    actor: Optional[str] = None,
    limit: int = 200,
    request: Request = None,
    _: Dict = Depends(require_auth),
):
    """最近审计记录（默认 200 条，可按 action/actor 过滤；租户视角只看本租户）。"""
    tenant = tenant_scope(request) if request is not None else None
    return {
        "logs": audit_svc.list_audit(
            action=action, actor=actor, limit=min(limit, 1000), tenant_id=tenant
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
