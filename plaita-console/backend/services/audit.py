"""审计服务：管理面敏感操作留痕。

``record`` 从 request.state（由 auth.require_auth 写入）取操作人；
密钥/机密内容一律不入审计（detail 由调用方保证只含元数据）。

归属（A55-S3）：写路径的**生效租户**由 ``tenant_scope(required=True)`` 决定，
与 ``request.state.tenant_id`` 可能分叉（平台/API-key 无 header 时写路径回退
``default``）。所有写 callsite 必须把生效租户显式传给 ``tenant_id``；只有未传
（``None``）才回退 state。

身份列（A55-S3）：``auth_source`` / ``key_fingerprint``（sha256 前 16 位，不存
明文）/ ``request_id`` / ``role_snapshot`` 由认证与请求中间件写入 state。

拒绝审计（A55-S2）：``record_denied`` 对同一 ``(action, actor, ip, tenant)``
在窗口内只落一行并累计 ``suppressed`` 次数——未认证流量不能按请求数撑大
审计表。
"""
from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import select

try:
    from ..models.flow import AuditLog
except ImportError:
    from models.flow import AuditLog  # type: ignore

logger = logging.getLogger(__name__)

#: 拒绝审计的合并窗口（秒）：同一键在窗口内只写一行，其余计数。
DENIED_WINDOW_SECONDS = 60.0
#: 进程内最多跟踪的拒绝键数（防未认证流量把合并表撑大）。
DENIED_MAX_KEYS = 1024

#: 审计读面默认条数与硬上限（A55-S4）。API 的 ``Query(ge=1)`` 是第一道闸；
#: 服务层再钳一次——任何绕过 FastAPI 校验的内部调用也不能用 ``limit=-1``
#: 在 SQLite 上物化全表。
AUDIT_LIMIT_DEFAULT = 200
AUDIT_LIMIT_MAX = 1000

_throttle_lock = threading.Lock()
#: key -> (window_start_monotonic, suppressed_count)
_denied_windows: Dict[Tuple[str, str, str, str, str], Tuple[float, int]] = {}


def _client_ip(request: Any) -> str:
    client = getattr(request, "client", None)
    return getattr(client, "host", "") or ""


def record(request: Any, action: str, resource: str, resource_id: str = "",
           detail: Optional[Dict[str, Any]] = None, *,
           tenant_id: Optional[str] = None,
           actor: Optional[str] = None) -> None:
    """记录一条审计（异步端点里也应调用本同步函数——SQLite 写入极短）。

    ``tenant_id``：显式传入**资源生效租户**；``None`` 才回退 state。空串是
    合法值（平台级全局操作，如 cluster/service/tenant 生命周期）。

    ``actor``：仅认证失败等没有 request.state.actor 的场景使用；其余调用方
    一律用 state（由 require_auth 写入）。

    失败只告警不抛错：审计不应阻断业务请求。
    """
    try:
        from .flow_store import get_flow_store
    except ImportError:
        from flow_store import get_flow_store  # type: ignore
    try:
        store = get_flow_store()
        state = getattr(request, "state", None)
        effective_actor = actor if actor is not None else (
            getattr(state, "actor", "") or "anonymous"
        )
        effective_tenant = tenant_id if tenant_id is not None else (
            getattr(state, "tenant_id", "") or ""
        )
        auth_source = getattr(state, "auth_source", "") or ""
        key_fingerprint = getattr(state, "key_fingerprint", "") or ""
        request_id = getattr(state, "request_id", "") or ""
        role_snapshot = getattr(state, "role", "") or ""
        ip = _client_ip(request)
        with store._session_local() as session:
            session.add(
                AuditLog(
                    tenant_id=effective_tenant,
                    actor=effective_actor,
                    action=action,
                    resource=resource,
                    resource_id=str(resource_id or ""),
                    detail_json=json.dumps(detail or {}, ensure_ascii=False),
                    ip=ip or "",
                    auth_source=auth_source,
                    key_fingerprint=key_fingerprint,
                    request_id=request_id,
                    role_snapshot=role_snapshot,
                )
            )
            session.commit()
    except Exception as e:  # noqa: BLE001
        logger.warning("审计记录失败（%s.%s）: %s", resource, action, e)


def record_denied(request: Any, action: str, resource: str,
                  resource_id: str = "",
                  detail: Optional[Dict[str, Any]] = None, *,
                  tenant_id: Optional[str] = None,
                  actor: Optional[str] = None,
                  window_seconds: float = DENIED_WINDOW_SECONDS,
                  key_suffix: str = "") -> bool:
    """拒绝/认证失败审计（A55-S2），按窗口合并防刷。

    返回 True 表示本次真的落了一行；False 表示被合并（detail 里不写库）。
    合并键不含 resource_id：攻击者变化目标不会按请求数产生新行；
    ``key_suffix`` 用于区分不同拒绝原因（如 401 vs 租户上下文 403）。
    """
    state = getattr(request, "state", None)
    effective_actor = actor if actor is not None else (
        getattr(state, "actor", "") or "anonymous"
    )
    effective_tenant = tenant_id if tenant_id is not None else (
        getattr(state, "tenant_id", "") or ""
    )
    key = (action, effective_actor, _client_ip(request), effective_tenant,
           key_suffix)
    now = time.monotonic()
    suppressed = 0
    with _throttle_lock:
        if len(_denied_windows) >= DENIED_MAX_KEYS:
            _prune_denied_windows(now, window_seconds)
            if key not in _denied_windows and len(_denied_windows) >= DENIED_MAX_KEYS:
                oldest = min(
                    _denied_windows,
                    key=lambda k: _denied_windows[k][0],
                )
                _denied_windows.pop(oldest, None)
        entry = _denied_windows.get(key)
        if entry is not None and now - entry[0] < window_seconds:
            _denied_windows[key] = (entry[0], entry[1] + 1)
            return False
        if entry is not None:
            suppressed = entry[1]
        _denied_windows[key] = (now, 0)

    payload = dict(detail or {})
    if suppressed:
        payload["suppressed"] = suppressed
    record(
        request,
        action=action,
        resource=resource,
        resource_id=resource_id,
        detail=payload,
        tenant_id=effective_tenant,
        actor=effective_actor,
    )
    return True


def _prune_denied_windows(now: float, window_seconds: float) -> None:
    stale = [
        key for key, (started, _) in _denied_windows.items()
        if now - started >= window_seconds
    ]
    for key in stale:
        _denied_windows.pop(key, None)


def reset_denied_throttle() -> None:
    """清空拒绝合并窗口（测试与运维重置用）。"""
    with _throttle_lock:
        _denied_windows.clear()


def list_audit(action: Optional[str] = None, actor: Optional[str] = None,
               limit: int = AUDIT_LIMIT_DEFAULT, tenant_id: Optional[str] = None,
               since: Optional[datetime] = None,
               before_id: Optional[int] = None) -> List[Dict[str, Any]]:
    # A55-S4 纵深：服务层同样钳制。SQLite 把负 LIMIT 当「无上限」，内部
    # 调用（脚本/测试/未来端点）若不经过 FastAPI 的 ge=1 会物化全表。
    try:
        effective_limit = int(limit)
    except (TypeError, ValueError):
        effective_limit = AUDIT_LIMIT_DEFAULT
    limit = max(1, min(effective_limit, AUDIT_LIMIT_MAX))
    try:
        from .flow_store import get_flow_store
    except ImportError:
        from flow_store import get_flow_store  # type: ignore
    store = get_flow_store()
    with store._session_local() as session:
        # A55-S4：按 id desc 稳定排序，before_id 游标翻页（无重无漏）。
        query = select(AuditLog).order_by(AuditLog.id.desc()).limit(limit)
        if tenant_id is not None:
            query = query.where(AuditLog.tenant_id == tenant_id)
        # A54-05：租户视角只读当前世代（审计行本身按合规要求保留）。
        if since is not None:
            query = query.where(AuditLog.ts >= since)
        if action:
            query = query.where(AuditLog.action == action)
        if actor:
            query = query.where(AuditLog.actor == actor)
        if before_id is not None:
            query = query.where(AuditLog.id < before_id)
        rows = session.scalars(query).all()
        return [
            {
                "id": r.id,
                "ts": r.ts.isoformat(),
                "tenant_id": r.tenant_id,
                "actor": r.actor,
                "action": r.action,
                "resource": r.resource,
                "resource_id": r.resource_id,
                "detail": json.loads(r.detail_json) if r.detail_json else {},
                "ip": r.ip,
                "auth_source": getattr(r, "auth_source", "") or "",
                "key_fingerprint": getattr(r, "key_fingerprint", "") or "",
                "request_id": getattr(r, "request_id", "") or "",
                "role_snapshot": getattr(r, "role_snapshot", "") or "",
            }
            for r in rows
        ]
