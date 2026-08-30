"""
调度服务（schedule_service）

时间驱动的常驻扩展服务：周期性扫描 Redis 中的调度定义（cron），
到期的流程以与手动启动完全一致的消息形状写入任务队列 Stream，
由 FlowWorker 消费执行。

数据视图（Redis）：
- ``plaita:schedules``                hash: schedule_id -> 调度定义 JSON
- ``plaita:schedule:fires:{id}``      list: 触发记录（保留最近 50 条）
- ``plaita:schedule:lock:{id}``       触发互斥锁（NX + PX，防多实例双发）

调度定义字段：
``schedule_id / name / flow_id / version / cron / params / enabled /
created_at / updated_at / next_run_at / last_fired_at / last_enqueue_ok``

触发语义（v1，刻意从简）：
- 错过的周期**不补偿**：到期后只触发一次，``next_run_at`` 直接跳到当前时间之后
  的下一个 cron 时点（服务停机一小时，恢复后不会连环补跑）。
- 时区：cron 按调度服务所在机器的本地时间解释。
"""
import json
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from croniter import croniter
from redis import Redis

from ...logger import logger
from .base_service import BaseExtendedService

# 与 console 后端共享的 Redis 视图键（console/api/schedules.py 同款常量）
SCHEDULES_KEY = "plaita:schedules"
SCHEDULE_FIRES_KEY = "plaita:schedule:fires:{schedule_id}"
SCHEDULE_LOCK_KEY = "plaita:schedule:lock:{schedule_id}"
FIRE_HISTORY_MAX = 50


# ---------- cron / 调度工具（console 后端复用） ----------

def validate_cron(cron: str) -> bool:
    """校验 cron 表达式（5 段，minute hour day month weekday）。"""
    if not cron or not isinstance(cron, str):
        return False
    try:
        croniter(cron, datetime.now())
        return True
    except (ValueError, KeyError) as e:
        logger.debug("非法 cron 表达式 %r: %s", cron, e)
        return False


def next_run_after(cron: str, dt: Optional[datetime] = None) -> int:
    """cron 在 dt（默认现在，本地时区）之后的下一次触发，返回 epoch 毫秒。

    注意必须走 get_next(datetime) + naive.timestamp()（按本地时区解释）；
    get_next(float) 会把 naive 基准当 UTC，导致触发时间偏移时区。
    """
    it = croniter(cron, dt or datetime.now())
    nxt = it.get_next(datetime)
    return int(nxt.timestamp() * 1000)


def fire_schedule(
    redis_client: Redis,
    schedule: Dict[str, Any],
    queue_name: str,
    trigger_kind: str = "cron",
) -> Optional[str]:
    """触发一次调度：入队 Stream + 记录触发历史 + 回写状态字段。

    供调度服务循环（trigger_kind="cron"）与 console「立即触发」
    （trigger_kind="manual"）复用，保证消息形状单一来源。

    Returns:
        Stream message id；入队失败返回 None。
    """
    schedule_id = schedule["schedule_id"]
    now = datetime.now()
    now_ms = int(now.timestamp() * 1000)

    message: Dict[str, Any] = {
        "type": "start",
        "flow_id": schedule["flow_id"],
        "params": schedule.get("params") or {},
        "timestamp": now.isoformat(),
        "trigger": {
            "kind": trigger_kind,
            "schedule_id": schedule_id,
            "schedule_name": schedule.get("name", ""),
        },
    }
    version = schedule.get("version")
    if version:
        message["version"] = version

    try:
        msg_id = redis_client.xadd(queue_name, {"payload": json.dumps(message, ensure_ascii=False)})
        msg_id = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
        enqueue_ok = True
    except Exception as e:
        logger.error("调度 %s 入队失败: %s", schedule_id, e, exc_info=True)
        msg_id = None
        enqueue_ok = False

    fire_record = {
        "fired_at": now.isoformat(),
        "trigger_kind": trigger_kind,
        "flow_id": schedule["flow_id"],
        "version": schedule.get("version") or "",
        "enqueue": "ok" if enqueue_ok else "failed",
        "msg_id": msg_id or "",
    }
    try:
        fires_key = SCHEDULE_FIRES_KEY.format(schedule_id=schedule_id)
        pipe = redis_client.pipeline()
        pipe.lpush(fires_key, json.dumps(fire_record, ensure_ascii=False))
        pipe.ltrim(fires_key, 0, FIRE_HISTORY_MAX - 1)
        pipe.execute()
    except Exception:
        logger.warning("调度 %s 触发历史写入失败", schedule_id, exc_info=True)

    # 回写触发状态；错过的周期不补偿——next_run_at 直接跳到现在之后
    try:
        updates = {
            "last_fired_at": now.isoformat(),
            "last_enqueue_ok": "1" if enqueue_ok else "0",
        }
        if trigger_kind == "cron":
            updates["next_run_at"] = str(next_run_after(schedule["cron"], now))
        redis_client.hset(SCHEDULES_KEY, key=schedule_id, value=json.dumps(_merge(schedule, updates), ensure_ascii=False))
    except Exception:
        logger.warning("调度 %s 状态回写失败", schedule_id, exc_info=True)

    logger.info(
        "调度 %s（%s）已触发 %s@%s → %s",
        schedule.get("name", schedule_id), trigger_kind,
        schedule["flow_id"], schedule.get("version") or "latest",
        "已入队" if enqueue_ok else "入队失败",
    )
    return msg_id


def _merge(schedule: Dict[str, Any], updates: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(schedule)
    merged.update(updates)
    return merged


def list_schedules(redis_client: Redis) -> List[Dict[str, Any]]:
    """读取全部调度定义（console 后端与调度服务共用）。"""
    raw = redis_client.hgetall(SCHEDULES_KEY)
    out: List[Dict[str, Any]] = []
    for value in raw.values():
        if isinstance(value, bytes):
            value = value.decode()
        try:
            out.append(json.loads(value))
        except json.JSONDecodeError:
            logger.warning("忽略无法解析的调度定义: %r", value)
    return out


# ---------- 服务 ----------

class ScheduleService(BaseExtendedService):
    """
    调度服务：cron → 任务队列 Stream。

    与 delay_service 等任务驱动型外延服务不同，调度服务是时间驱动的，
    不消费任务，只维护一个扫描循环。
    """

    def __init__(self, event_bus, redis_client: Redis, service_config: Optional[Dict[str, Any]] = None):
        import os
        # 注意：必须在 super().__init__() 之前就绪——基类构造期间会调用
        # _get_registry_metadata() 读取这些属性
        cfg = service_config or {}
        self.queue_name = (
            cfg.get("queue_name")
            or os.environ.get("PLAITA_QUEUE_NAME", os.environ.get("QUEUE_NAME", "plaita:flow:queue"))
        )
        self.check_interval = float(cfg.get("check_interval") or os.environ.get("PLAITA_CHECK_INTERVAL", "1"))
        self._loop_thread: Optional[threading.Thread] = None

        super().__init__(
            event_bus=event_bus,
            service_config=service_config,
            redis_client=redis_client,
        )

    def get_service_type(self) -> str:
        return "schedule_service"

    def _get_registry_metadata(self) -> Dict[str, Any]:
        return {
            "queue_name": self.queue_name,
            "check_interval": self.check_interval,
        }

    def start_service(self) -> bool:
        try:
            # 注册 + 心跳：__main__ 启动器只调 start_service，不调
            # start_with_registry，不补这一步注册会在 TTL 30s 后过期，
            # 拓扑/服务列表再也看不到调度服务
            if self._enable_registry and not self.register_service():
                logger.warning("调度服务注册失败，服务仍将启动")
            self.is_running = True
            self._loop_thread = threading.Thread(
                target=self._run_loop, name="schedule-service-loop", daemon=True
            )
            self._loop_thread.start()
            logger.info(
                "调度服务已启动：队列=%s，扫描间隔=%ss", self.queue_name, self.check_interval
            )
            return True
        except Exception as e:
            logger.error("启动调度服务失败: %s", e, exc_info=True)
            return False

    def stop_service(self) -> bool:
        self.is_running = False
        logger.info("调度服务已停止")
        return True

    async def handle_task(self, task_config: Dict[str, Any]) -> bool:
        # 调度服务是时间驱动，不接任务；满足基类抽象即可
        return False

    def validate_task_config(self, task_config: Dict[str, Any]) -> bool:
        return False

    # ---------- 调度循环 ----------

    def _run_loop(self) -> None:
        while not self.is_shutdown_requested():
            try:
                self._tick()
            except Exception as e:
                logger.error("调度扫描失败: %s", e, exc_info=True)
            # shutdown_event.wait 兼顾休眠与快速响应停止
            if self._shutdown_event.wait(timeout=self.check_interval):
                break

    def _tick(self) -> None:
        now_ms = int(time.time() * 1000)
        for schedule in list_schedules(self._redis_client):
            schedule_id = schedule.get("schedule_id")
            if not schedule_id or not schedule.get("enabled", False):
                continue
            try:
                self._fire_if_due(schedule, now_ms)
            except Exception as e:
                logger.error("调度 %s 处理失败: %s", schedule_id, e, exc_info=True)

    def _fire_if_due(self, schedule: Dict[str, Any], now_ms: int) -> None:
        schedule_id = schedule["schedule_id"]
        cron = schedule.get("cron")
        if not cron or not validate_cron(cron):
            logger.warning("调度 %s 的 cron 非法（%r），跳过", schedule_id, cron)
            return

        next_run_at = schedule.get("next_run_at")
        if not next_run_at:
            # 自愈：缺失/清零的 next_run_at 从当前时间重算
            next_run_at = next_run_after(cron)
            self._redis_client.hset(
                SCHEDULES_KEY,
                key=schedule_id,
                value=json.dumps(_merge(schedule, {"next_run_at": str(next_run_at)}), ensure_ascii=False),
            )
        try:
            next_run_at = int(float(next_run_at))
        except (TypeError, ValueError):
            logger.warning("调度 %s 的 next_run_at 非法（%r），已重算", schedule_id, next_run_at)
            next_run_at = next_run_after(cron)

        if now_ms < next_run_at:
            return

        # 互斥：多实例部署时保证同一时点只触发一次
        lock_key = SCHEDULE_LOCK_KEY.format(schedule_id=schedule_id)
        lock_holder = self._service_info.instance_id if self._service_info else "schedule"
        if not self._redis_client.set(lock_key, lock_holder, nx=True, px=30_000):
            return

        fire_schedule(self._redis_client, schedule, self.queue_name, trigger_kind="cron")
