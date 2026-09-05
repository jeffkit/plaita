"""审计查询 API（admin）。"""
from typing import Optional

from typing import Dict, Optional

from fastapi import APIRouter, Depends

try:
    from ..auth import require_auth
    from ..services import audit as audit_svc
except ImportError:
    from auth import require_auth  # type: ignore
    from services import audit as audit_svc  # type: ignore

router = APIRouter()


@router.get("/audit")
def list_audit(
    action: Optional[str] = None,
    actor: Optional[str] = None,
    limit: int = 200,
    _: Dict = Depends(require_auth),
):
    """最近审计记录（默认 200 条，可按 action/actor 过滤）。"""
    return {"logs": audit_svc.list_audit(action=action, actor=actor, limit=min(limit, 1000))}


@router.get("/deployments")
def list_deployments(flow_id: Optional[str] = None, _: Dict = Depends(require_auth)):
    return {"deployments": audit_svc_deployments(flow_id)}


def audit_svc_deployments(flow_id: Optional[str]):
    try:
        from ..services import deployments as deployments_svc
    except ImportError:
        from services import deployments as deployments_svc  # type: ignore
    return deployments_svc.list_deployments(flow_id=flow_id)
