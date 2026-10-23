"""本地调度**行存储**：``LocalSchedule`` / ``LocalScheduleFire`` 的 SQL 权威读写。

C1 控制面边界（plaita#323）：调度**定义行**的 CRUD 与触发历史读是「SQL 为
权威的定义模型」（registry 域）；调度**到点认领 / 触发 / 扫描循环**是运行面
（gateway 域，见 :mod:`local_scheduler`）。此前两者同居 ``local_scheduler``，
使 :mod:`tenants_svc`（租户生命周期，registry）在停用租户时反向依赖运行面
模块——边界门 ``test_c1_control_plane_layering`` 禁止 registry→gateway，故行
读写下沉本模块；``local_scheduler`` 再导出同名符号，``api/flows``、
``api/schedules`` 与既有测试的调用面不变。

本模块只做行读写：不触发执行、不读 Redis、不计算 cron。集群档调度仍在
Redis（``api/schedules`` 集群分支 + ``plaita.server`` schedule_service），与
本地行互不可见（A48-01 语义不变）。
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import func, select

try:
    from ..models.flow import LocalSchedule, LocalScheduleFire
    from .tenant_write_guard import assert_tenant_generation
except ImportError:
    from models.flow import LocalSchedule, LocalScheduleFire  # type: ignore
    from services.tenant_write_guard import assert_tenant_generation  # type: ignore


# ---- 行转视图（与集群档 _view 对齐） ----

def _view(row: LocalSchedule) -> Dict[str, Any]:
    return {
        "schedule_id": row.schedule_id,
        "tenant_id": getattr(row, "tenant_id", "") or "",
        "name": row.name,
        "flow_id": row.flow_id,
        "version": row.version,
        "cron": row.cron,
        "params": json.loads(row.params_json) if row.params_json else {},
        "enabled": row.enabled,
        "created_by": row.created_by,
        "created_at": row.created_at.isoformat() if row.created_at else "",
        "updated_at": row.updated_at.isoformat() if row.updated_at else "",
        "next_run_at": row.next_run_at or "",
        "last_run_at": row.last_run_at or "",
        "status": "running" if row.enabled else "paused",
    }


# ---- CRUD（api/schedules.py 本地分支经 local_scheduler 再导出调用） ----

def create_schedule(store, schedule: Dict[str, Any]) -> Dict[str, Any]:
    with store._session_local() as session:
        session.add(
            LocalSchedule(
                schedule_id=schedule["schedule_id"],
                tenant_id=schedule.get("tenant_id") or "",
                name=schedule["name"],
                flow_id=schedule["flow_id"],
                version=schedule.get("version"),
                cron=schedule["cron"],
                params_json=json.dumps(schedule.get("params") or {}, ensure_ascii=False),
                enabled=bool(schedule.get("enabled", True)),
                created_by=schedule.get("created_by") or "",
                next_run_at=schedule.get("next_run_at") or "",
            )
        )
        session.flush()
        assert_tenant_generation(session, schedule.get("tenant_id"))
        session.commit()
    return get_schedule(store, schedule["schedule_id"])


def update_schedule(store, schedule_id: str, schedule: Dict[str, Any],
                    tenant_id: Optional[str] = None) -> Dict[str, Any]:
    with store._session_local() as session:
        query = select(LocalSchedule).where(LocalSchedule.schedule_id == schedule_id)
        if tenant_id is not None:
            query = query.where(LocalSchedule.tenant_id == tenant_id)
        row = session.scalars(query).first()
        if row is None:
            raise LookupError(schedule_id)
        row.name = schedule["name"]
        row.flow_id = schedule["flow_id"]
        row.version = schedule.get("version")
        row.cron = schedule["cron"]
        row.params_json = json.dumps(schedule.get("params") or {}, ensure_ascii=False)
        row.enabled = bool(schedule.get("enabled", True))
        row.next_run_at = schedule.get("next_run_at") or ""
        row.updated_at = datetime.utcnow()
        session.flush()
        assert_tenant_generation(session, tenant_id or row.tenant_id)
        session.commit()
    return get_schedule(store, schedule_id, tenant_id=tenant_id)


def get_schedule(store, schedule_id: str,
                 tenant_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    with store._session_local() as session:
        query = select(LocalSchedule).where(LocalSchedule.schedule_id == schedule_id)
        if tenant_id is not None:
            query = query.where(LocalSchedule.tenant_id == tenant_id)
        row = session.scalars(query).first()
        return _view(row) if row else None


def list_schedules(store, tenant_id: Optional[str] = None) -> List[Dict[str, Any]]:
    with store._session_local() as session:
        query = select(LocalSchedule)
        if tenant_id is not None:
            query = query.where(LocalSchedule.tenant_id == tenant_id)
        rows = session.scalars(query).all()
        return [_view(r) for r in rows]


def delete_schedule(store, schedule_id: str,
                    tenant_id: Optional[str] = None) -> bool:
    with store._session_local() as session:
        query = select(LocalSchedule).where(LocalSchedule.schedule_id == schedule_id)
        if tenant_id is not None:
            query = query.where(LocalSchedule.tenant_id == tenant_id)
        row = session.scalars(query).first()
        if row is None:
            return False
        session.delete(row)
        fires = session.scalars(
            select(LocalScheduleFire).where(LocalScheduleFire.schedule_id == schedule_id)
        ).all()
        for f in fires:
            session.delete(f)
        session.flush()
        assert_tenant_generation(session, tenant_id or row.tenant_id)
        session.commit()
    return True


# ---- 触发历史（只读 + 单行追加） ----

def fire_history_total(store, schedule_id: str) -> int:
    """触发历史总条数（A2R24-05：``total`` 必须是匹配总数，不是本页条数）。"""
    with store._session_local() as session:
        return int(session.scalar(
            select(func.count())
            .select_from(LocalScheduleFire)
            .where(LocalScheduleFire.schedule_id == schedule_id)
        ) or 0)


def fire_history(store, schedule_id: str, limit: int = 10, offset: int = 0,
                 flow_id: str = "", version: str = "") -> List[Dict[str, Any]]:
    """最近触发记录（A2R24 契约族余项：与集群档/前端统一字段形状）。

    历史：本地档返回 ``{triggered_at, status, error, execution_id}``，而集群档
    与前端 ``ScheduleFireRecord`` 用 ``{fired_at, flow_id, version, enqueue,
    msg_id}``——本地档对话框的时间列恒空、状态恒显示「入队失败」。这里以集群
    形状为准映射，并保留本地专有的 ``execution_id/status/error``。
    """
    with store._session_local() as session:
        rows = session.scalars(
            select(LocalScheduleFire)
            .where(LocalScheduleFire.schedule_id == schedule_id)
            .order_by(LocalScheduleFire.created_at.desc())
            .offset(max(0, offset))
            .limit(max(1, min(50, limit)))
        ).all()
        return [
            {
                "fired_at": r.created_at.isoformat() if r.created_at else "",
                "trigger_kind": r.trigger_kind,
                "flow_id": flow_id,
                "version": version,
                "enqueue": "ok" if r.status == "fired" else "failed",
                "msg_id": None,
                "execution_id": r.execution_id or None,
                "status": r.status,
                "error": r.error or None,
            }
            for r in rows
        ]


def _record_fire(store, schedule_id: str, execution_id: str, trigger_kind: str,
                 status: str, error: str) -> None:
    with store._session_local() as session:
        session.add(
            LocalScheduleFire(
                schedule_id=schedule_id,
                execution_id=execution_id,
                trigger_kind=trigger_kind,
                status=status,
                error=error or "",
            )
        )
        session.commit()
