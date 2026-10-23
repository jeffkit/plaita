"""本地单机模式：调度管理 + 内置调度循环。

集群档的调度存 Redis HASH、由独立 schedule_service 进程触发；本地档把同样
的调度定义存 SQLite，由 console 进程内的守护线程扫描触发（直接进程内执行，
不入队）。CRUD 与触发历史对外结构与集群档一致，前端无感。

C1 控制面边界（plaita#323）：调度**行读写**（CRUD / 触发历史）已下沉
:mod:`schedule_store`（registry 域，SQL 权威定义模型），本模块保留**运行面**
——到点认领（``_claim_due``）、触发（``fire`` / ``trigger_now``）与扫描循环，
并再导出行读写符号保持 ``api/flows``、``api/schedules`` 与既有测试调用面
不变。

该循环**只在本地档启动**（A48-01）：集群档的调度列表/触发都走 Redis，本地
表里的遗留条目既不执行也不可见（保留原样，切回本地档仍可用）。到点触发以
``next_run_at`` 条件更新作为 due 幂等键（A48-02）：先推进再执行，同一 due
不会因崩溃/历史写失败/多副本扫描再起一条执行。

cron 语义与集群档共享同一实现（plaita.server.services.schedule_service 的
``validate_cron`` / ``next_run_after``，croniter，本地时区）。
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy import select, update

try:
    from plaita.server.services.schedule_service import next_run_after, validate_cron
except ImportError:  # 兼容不同安装布局
    from plaita.server.services.schedule_service import (  # type: ignore
        next_run_after,
        validate_cron,
    )

try:
    from ..models.flow import LocalSchedule
    # 行读写再导出（registry 域实现见 schedule_store）；调用面守恒。
    from .schedule_store import (  # noqa: F401
        _record_fire,
        create_schedule,
        delete_schedule,
        fire_history,
        fire_history_total,
        get_schedule,
        list_schedules,
        update_schedule,
    )
except ImportError:
    from models.flow import LocalSchedule  # type: ignore
    from services.schedule_store import (  # type: ignore # noqa: F401
        _record_fire,
        create_schedule,
        delete_schedule,
        fire_history,
        fire_history_total,
        get_schedule,
        list_schedules,
        update_schedule,
    )

logger = logging.getLogger(__name__)

_SCAN_INTERVAL_SECONDS = 5.0
_stop_event = threading.Event()
_thread: Optional[threading.Thread] = None
_lock = threading.Lock()


# ---- 运行面：租户闸 / 到点认领 / 触发 ----

def _tenant_executable(store, tenant_id: Optional[str]) -> bool:
    """运行面严格租户闸（#27 + #255 墓碑语义）。

    与 ``users_svc.tenant_is_active`` 的唯一区别：非 default 租户行缺失视为
    **已删除**（A48-04）。扫描快照可能持有删除前的行；#255 的 404 补清扫尚未
    跑完时，lenient 闸（缺失=active）会让已删除租户再执行一次。default 行缺失
    无法区分「已删除」与「尚未 bootstrap」，保留 fail-open。
    """
    try:
        from . import users_svc
    except ImportError:
        from services import users_svc  # type: ignore
    return users_svc.tenant_is_executable(store, tenant_id)


def _claim_due(store, schedule: Dict[str, Any]) -> bool:
    """原子认领一次到点触发（A48-02 的本地 due 幂等键）。

    以 ``schedule_id + next_run_at(旧值) + enabled`` 为条件推进排班：先推进再
    执行，崩溃/历史写失败最多少跑这一次，不会对同一 due 再起一条执行；多副本
    同时扫描时也只有一个能命中。cron 无法计算下一次时保持原值并返回 False
    （不做补偿）。
    """
    schedule_id = schedule["schedule_id"]
    expected = schedule.get("next_run_at") or ""
    if not expected:
        return False
    with store._session_local() as session:
        row = session.scalars(
            select(LocalSchedule).where(LocalSchedule.schedule_id == schedule_id)
        ).first()
        if row is None or not row.enabled or (row.next_run_at or "") != expected:
            return False
        try:
            next_at = str(next_run_after(row.cron))
        except Exception as e:  # noqa: BLE001 — 坏 cron 不能拖垮整轮扫描
            logger.warning("调度 %s 计算下次触发失败（跳过本轮）: %s", schedule_id, e)
            return False
        result = session.execute(
            update(LocalSchedule)
            .where(
                LocalSchedule.schedule_id == schedule_id,
                LocalSchedule.next_run_at == expected,
                LocalSchedule.enabled.is_(True),
            )
            .values(
                last_run_at=datetime.now().isoformat(),
                next_run_at=next_at,
                updated_at=datetime.utcnow(),
            )
        )
        session.commit()
        return (result.rowcount or 0) == 1


def fire(store, schedule: Dict[str, Any], trigger_kind: str = "cron") -> Optional[str]:
    """触发一次调度：本地档直接进程内执行（不入队），记录触发历史。

    ``cron`` 是调度循环的到点触发：先在 SQLite 上原子推进 ``next_run_at``
    （命中者才启动执行）；``manual`` 不改排班。租户与调度行都在触发前重新
    读取——扫描快照不能越过删除/停用闸（A48-04）。
    """
    from . import local_executor

    schedule_id = schedule.get("schedule_id") or ""

    # 租户状态闸（#27）：停用租户的调度不再触发（与集群档 fire_schedule 同源）。
    # 非 default 租户行缺失 = 已删除（#255），不再按 active 放行。
    if not _tenant_executable(store, schedule.get("tenant_id")):
        logger.info(
            "租户 %s 已停用或已删除，跳过本地调度 %s",
            schedule.get("tenant_id"), schedule_id,
        )
        return None

    # A48-02：到点触发先认领 due。并发扫描/副本中只有一个能把旧
    # next_run_at 改成新值，其余命中 0 行直接放弃（不再创建执行）。
    if trigger_kind == "cron" and not _claim_due(store, schedule):
        return None

    # A48-04：快照可能是删除前的读取——重新确认调度行仍在且仍启用。
    fresh = get_schedule(store, schedule_id) if schedule_id else None
    if fresh is None or not fresh.get("enabled"):
        logger.info("调度 %s 在扫描后已删除或暂停，跳过触发", schedule_id)
        return None

    try:
        info = local_executor.start_local_execution(
            store, fresh["flow_id"], fresh.get("version"), fresh.get("params") or {},
            tenant_id=fresh.get("tenant_id") or "",
        )
        execution_id = info["execution_id"]
    except Exception as e:  # noqa: BLE001 — 触发失败要留痕
        logger.warning("调度 %s 触发失败: %s", schedule_id, e)
        _record_fire(store, schedule_id, "", trigger_kind, "failed", str(e))
        return None

    _record_fire(store, schedule_id, execution_id, trigger_kind, "fired", "")
    return execution_id


def trigger_now(store, schedule: Dict[str, Any]) -> Optional[str]:
    return fire(store, schedule, trigger_kind="manual")


# ---- 内置调度循环 ----

def start_scheduler_loop(store) -> None:
    """启动后台调度线程（仅本地档 lifespan 调用；幂等）。"""
    global _thread
    with _lock:
        if _thread is not None and _thread.is_alive():
            return
        _stop_event.clear()
        _thread = threading.Thread(target=_loop, args=(store,), name="local-scheduler", daemon=True)
        _thread.start()
    logger.info("本地调度循环已启动（扫描间隔 %.0fs）", _SCAN_INTERVAL_SECONDS)


def stop_scheduler_loop(timeout: float = 5.0) -> None:
    """停止后台调度线程（lifespan shutdown / 离开本地档；幂等）。

    A48-01：集群档不能留一个可见性之外、关停也不停的本地调度线程。停止后
    再关 Redis/存储，避免线程在收尾期间继续起执行。
    """
    global _thread
    with _lock:
        thread = _thread
        _stop_event.set()
    if thread is None:
        return
    if thread is not threading.current_thread():
        thread.join(timeout)
    with _lock:
        if _thread is thread and not thread.is_alive():
            _thread = None
    if thread.is_alive():
        logger.warning("本地调度线程在 %.0fs 内未退出（仍在收尾）", timeout)


def warn_local_schedules_inactive(store) -> int:
    """集群档启动/切换时的遗留语义提示（A48-01）。

    本地调度只存在 SQLite；集群档的列表与触发都走 Redis。不启动本地循环后，
    已落盘的行会保留但**不会执行、也不在集群调度列表里显示**——不自动删除
    或迁移（自动改数据会让切回本地档丢排班），只在启动日志里明确提示。
    """
    try:
        rows = list_schedules(store)
    except Exception as e:  # noqa: BLE001 — 提示失败不得阻断启动
        logger.warning("检查本地档遗留调度失败（忽略）: %s", e)
        return 0
    if rows:
        logger.warning(
            "集群档未启动本地调度循环：SQLite 中仍有 %d 条本地单机档调度，"
            "它们不会执行、也不在集群调度列表中显示。需要继续运行请切回本地"
            "单机档，或在集群档按相同 cron 重建后删除本地条目。",
            len(rows),
        )
    return len(rows)


def _loop(store) -> None:
    while not _stop_event.is_set():
        try:
            _scan_once(store)
        except Exception as e:  # noqa: BLE001 — 调度循环永不退出
            logger.warning("本地调度扫描异常: %s", e)
        _stop_event.wait(_SCAN_INTERVAL_SECONDS)


def _scan_once(store) -> int:
    """扫一遍到期调度并触发。返回触发数量。

    ``fire(kind="cron")`` 内部先原子推进 ``next_run_at``（A48-02），这里不再
    有「先执行、后单独提交排班」的崩溃窗口。
    """
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
        if fire(store, schedule, trigger_kind="cron") is not None:
            fired += 1
    return fired
