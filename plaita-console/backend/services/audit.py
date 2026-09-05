"""审计服务：管理面敏感操作留痕。

``record`` 从 request.state（由 auth.require_auth 写入）取操作人；
密钥/机密内容一律不入审计（detail 由调用方保证只含元数据）。
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import select

try:
    from ..models.flow import AuditLog
except ImportError:
    from models.flow import AuditLog  # type: ignore

logger = logging.getLogger(__name__)


def record(request: Any, action: str, resource: str, resource_id: str = "",
           detail: Optional[Dict[str, Any]] = None) -> None:
    """记录一条审计（异步端点里也应调用本同步函数——SQLite 写入极短）。

    失败只告警不抛错：审计不应阻断业务请求。
    """
    try:
        from .flow_store import get_flow_store
    except ImportError:
        from flow_store import get_flow_store  # type: ignore
    try:
        store = get_flow_store()
        actor = getattr(getattr(request, "state", None), "actor", "") or "anonymous"
        ip = request.client.host if getattr(request, "client", None) else ""
        with store._session_local() as session:
            session.add(
                AuditLog(
                    actor=actor,
                    action=action,
                    resource=resource,
                    resource_id=str(resource_id or ""),
                    detail_json=json.dumps(detail or {}, ensure_ascii=False),
                    ip=ip or "",
                )
            )
            session.commit()
    except Exception as e:  # noqa: BLE001
        logger.warning("审计记录失败（%s.%s）: %s", resource, action, e)


def list_audit(action: Optional[str] = None, actor: Optional[str] = None,
               limit: int = 200) -> List[Dict[str, Any]]:
    try:
        from .flow_store import get_flow_store
    except ImportError:
        from flow_store import get_flow_store  # type: ignore
    store = get_flow_store()
    with store._session_local() as session:
        query = select(AuditLog).order_by(AuditLog.ts.desc()).limit(limit)
        if action:
            query = query.where(AuditLog.action == action)
        if actor:
            query = query.where(AuditLog.actor == actor)
        rows = session.scalars(query).all()
        return [
            {
                "ts": r.ts.isoformat(),
                "actor": r.actor,
                "action": r.action,
                "resource": r.resource,
                "resource_id": r.resource_id,
                "detail": json.loads(r.detail_json) if r.detail_json else {},
                "ip": r.ip,
            }
            for r in rows
        ]
