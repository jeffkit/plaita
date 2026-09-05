"""本地单机模式：调度管理 + 内置调度循环。

集群档的调度存 Redis HASH、由独立 schedule_service 进程触发；本地档把同样
的调度定义存 SQLite，由 console 进程内的守护线程扫描触发（直接进程内执行，
不入队）。CRUD 与触发历史对外结构与集群档一致，前端无感。

cron 语义与集群档共享同一实现（plaita.server.services.schedule_service 的
``validate_cron`` / ``next_run_after``，croniter，本地时区）。
"""
from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import select

try:
    from plaita.server.services.schedule_service import next_run_after, validate_cron
except ImportError:  # 兼容不同安装布局
    from plaita.server.services.schedule_service import (  # type: ignore
        next_run_after,
        validate_cron,
    )

try:
    from ..models.flow import LocalSchedule, LocalScheduleFire
except ImportError:
    from models.flow import LocalSchedule, LocalScheduleFire  # type: ignore

logger = logging.getLogger(__name__)

_SCAN_INTERVAL_SECONDS = 5.0
_stop_event = threading.Event()
_thread: Optional[threading.Thread] = None
_lock = threading.Lock()


# ---- 行转视图（与集群档 _view 对齐） ----

def _view(row: LocalSchedule) -> Dict[str, Any]:
    return {
        "schedule_id": row.schedule_id,
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


# ---- CRUD（api/schedules.py 本地分支调用） ----

def create_schedule(store, schedule: Dict[str, Any]) -> Dict[str, Any]:
    with store._session_local() as session:
        session.add(
            LocalSchedule(
                schedule_id=schedule["schedule_id"],
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
        session.commit()
    return get_schedule(store, schedule["schedule_id"])


def update_schedule(store, schedule_id: str, schedule: Dict[str, Any]) -> Dict[str, Any]:
    with store._session_local() as session:
        row = session.scalars(
            select(LocalSchedule).where(LocalSchedule.schedule_id == schedule_id)
        ).first()
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
        session.commit()
    return get_schedule(store, schedule_id)


def get_schedule(store, schedule_id: str) -> Optional[Dict[str, Any]]:
    with store._session_local() as session:
        row = session.scalars(
            select(LocalSchedule).where(LocalSchedule.schedule_id == schedule_id)
        ).first()
        return _view(row) if row else None


def list_schedules(store) -> List[Dict[str, Any]]:
    with store._session_local() as session:
        rows = session.scalars(select(LocalSchedule)).all()
        return [_view(r) for r in rows]


def delete_schedule(store, schedule_id: str) -> bool:
    with store._session_local() as session:
        row = session.scalars(
            select(LocalSchedule).where(LocalSchedule.schedule_id == schedule_id)
        ).first()
        if row is None:
            return False
        session.delete(row)
        fires = session.scalars(
            select(LocalScheduleFire).where(LocalScheduleFire.schedule_id == schedule_id)
        ).all()
        for f in fires:
            session.delete(f)
        session.commit()
    return True


# ---- 触发 ----

def fire(store, schedule: Dict[str, Any], trigger_kind: str = "cron") -> Optional[str]:
    """触发一次调度：本地档直接进程内执行（不入队），记录触发历史。"""
    from . import local_executor

    try:
        info = local_executor.start_local_execution(
            store, schedule["flow_id"], schedule.get("version"), schedule.get("params") or {}
        )
        execution_id = info["execution_id"]
    except Exception as e:  # noqa: BLE001 — 触发失败要留痕
        logger.warning("调度 %s 触发失败: %s", schedule.get("schedule_id"), e)
        _record_fire(store, schedule["schedule_id"], "", trigger_kind, "failed", str(e))
        return None

    _record_fire(store, schedule["schedule_id"], execution_id, trigger_kind, "fired", "")
    return execution_id


def trigger_now(store, schedule: Dict[str, Any]) -> Optional[str]:
    return fire(store, schedule, trigger_kind="manual")


def fire_history(store, schedule_id: str, limit: int = 10) -> List[Dict[str, Any]]:
    with store._session_local() as session:
        rows = session.scalars(
            select(LocalScheduleFire)
            .where(LocalScheduleFire.schedule_id == schedule_id)
            .order_by(LocalScheduleFire.created_at.desc())
            .limit(max(1, min(50, limit)))
        ).all()
        return [
            {
                "trigger_kind": r.trigger_kind,
                "execution_id": r.execution_id,
                "status": r.status,
                "error": r.error,
                "triggered_at": r.created_at.isoformat() if r.created_at else "",
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


# ---- 内置调度循环 ----

def start_scheduler_loop(store) -> None:
    """启动后台调度线程（本地模式 lifespan 调用；幂等）。"""
    global _thread
    with _lock:
        if _thread is not None and _thread.is_alive():
            return
        _stop_event.clear()
        _thread = threading.Thread(target=_loop, args=(store,), name="local-scheduler", daemon=True)
        _thread.start()
    logger.info("本地调度循环已启动（扫描间隔 %.0fs）", _SCAN_INTERVAL_SECONDS)


def _loop(store) -> None:
    while not _stop_event.is_set():
        try:
            _scan_once(store)
        except Exception as e:  # noqa: BLE001 — 调度循环永不退出
            logger.warning("本地调度扫描异常: %s", e)
        _stop_event.wait(_SCAN_INTERVAL_SECONDS)


def _scan_once(store) -> int:
    """扫一遍到期调度并触发。返回触发数量。"""
    now_ms = time.time() * 1000
    fired = 0
    for schedule in list_schedules(store):
        if not schedule.get("enabled"):
            continue
        nxt = schedule.get("next_run_at") or ""
        if not nxt:
            continue
        try:
            due = float(nxt)
        except ValueError:
            continue
        if due > now_ms:
            continue
        info = fire(store, schedule, trigger_kind="cron")
        if info is None:
            continue
        fired += 1
        # 推进 next_run_at；若已过期多次，从当前时间重新排班（不补跑）
        with store._session_local() as session:
            row = session.scalars(
                select(LocalSchedule).where(LocalSchedule.schedule_id == schedule["schedule_id"])
            ).first()
            if row is not None:
                row.last_run_at = datetime.now().isoformat()
                row.next_run_at = str(next_run_after(row.cron))
                row.updated_at = datetime.utcnow()
                session.commit()
    return fired
