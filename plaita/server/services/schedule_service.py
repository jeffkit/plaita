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

派发幂等键（``dedup_key``）语义（#23 引入，plaita#118 修正取值基准）：
- **cron**：``sched:{schedule_id}:due:{due_ms}``。``due_ms`` = **本次到期时点**
  ``next_run_at``（epoch 毫秒的十进制整数），由 ``_fire_if_due`` 把它判定用的
  ``next_run_at`` 显式传入 ``due_at``。同一个 due 无论被触发几次（状态回写失败
  + 30s 锁过期重触发、消息 XCLAIM 重派）都得到**同一个键**，worker 侧 ``SET NX``
  只放行一次；不同 due 的键必然不同，不会被误吞。
- **manual**（console「立即触发」）：``sched:{schedule_id}:manual:{ns}-{seq}``，
  每次调用唯一，与 cron 键空间**不相交**——manual 是用户显式要求「立刻再跑一次」，
  不得与 cron 同秒互相吞并；同一条 manual 消息被重派时键随消息走，仍收敛。
- **降级**：cron 且既无 ``due_at`` 也无可用 ``next_run_at`` 时，退回
  ``sched:{schedule_id}:fired:{墙钟秒}`` 并 warning（调度循环会先自愈
  ``next_run_at``，该路径只在定义缺失且调用方未传 due 时可达）。
"""
import itertools
import json
import threading
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from croniter import croniter
from redis import Redis

from plaita.server.task_queue import enqueue_task

from ...logger import logger
from ..tenant_quota import release_tenant_start, try_admit_tenant_start
from ..tenant_status import tenant_is_active
from .base_service import BaseExtendedService
from plaita.core.url_redaction import _scrub_exception_text, _scrub_url_text

# 与 console 后端共享的 Redis 视图键（console/api/schedules.py 同款常量）
SCHEDULES_KEY = "plaita:schedules"
SCHEDULE_FIRES_KEY = "plaita:schedule:fires:{schedule_id}"
SCHEDULE_LOCK_KEY = "plaita:schedule:lock:{schedule_id}"
FIRE_HISTORY_MAX = 50

# manual 键内序号：同一纳秒内的两次调用也必须拿到不同键（进程内单调）。
_MANUAL_SEQ = itertools.count()


# ---------- cron / 调度工具（console 后端复用） ----------

def validate_cron(cron: str) -> bool:
    """校验 cron 表达式（5 段，minute hour day month weekday）。"""
    if not cron or not isinstance(cron, str):
        return False
    try:
        croniter(cron, datetime.now())
        return True
    except (ValueError, KeyError) as e:
        logger.debug("非法 cron 表达式 %r: %s", cron, _scrub_url_text(e))
        return False


def next_run_after(cron: str, dt: Optional[datetime] = None) -> int:
    """cron 在 dt（默认现在，本地时区）之后的下一次触发，返回 epoch 毫秒。

    注意必须走 get_next(datetime) + naive.timestamp()（按本地时区解释）；
    get_next(float) 会把 naive 基准当 UTC，导致触发时间偏移时区。
    """
    it = croniter(cron, dt or datetime.now())
    nxt = it.get_next(datetime)
    return int(nxt.timestamp() * 1000)


def normalized_due_ms(schedule: Dict[str, Any], due_at: Any = None) -> Optional[int]:
    """本次到期时点的规范表示：epoch 毫秒整数，取不到返回 None。

    优先取调用方显式传入的 ``due_at``（cron 侧即 ``_fire_if_due`` 判定用的
    ``next_run_at``），其次取调度定义里的 ``next_run_at``。规范化固定为
    ``int(float(...))``：同一 due 的等价写法（``"1791530071848"`` /
    ``1791530071848`` / ``1791530071848.0``）必须得到同一个键。
    """
    for candidate in (due_at, schedule.get("next_run_at")):
        if candidate is None or candidate == "":
            continue
        try:
            return int(float(candidate))
        except (TypeError, ValueError):
            continue
    return None


def build_dedup_key(
    schedule_id: str,
    trigger_kind: str,
    due_ms: Optional[int],
    wall_second: int,
) -> str:
    """构造派发幂等键（语义见模块 docstring）。

    cron 以**本次到期时点**为基准（同一 due ⇒ 同一键）；manual 走独立且每次
    调用唯一的键空间（不与 cron 同秒互吞）；cron 且无 due 时降级为墙钟秒。
    """
    if trigger_kind == "manual":
        return "sched:{schedule_id}:manual:{ns}-{seq}".format(
            schedule_id=schedule_id, ns=time.time_ns(), seq=next(_MANUAL_SEQ)
        )
    if due_ms is not None:
        return "sched:{schedule_id}:due:{due_ms}".format(
            schedule_id=schedule_id, due_ms=due_ms
        )
    return "sched:{schedule_id}:fired:{wall_second}".format(
        schedule_id=schedule_id, wall_second=wall_second
    )


def fire_schedule(
    redis_client: Redis,
    schedule: Dict[str, Any],
    queue_name: str,
    trigger_kind: str = "cron",
    due_at: Any = None,
) -> Optional[str]:
    """触发一次调度：入队 Stream + 记录触发历史 + 回写状态字段。

    供调度服务循环（trigger_kind="cron"，应传 ``due_at`` = 本次到期时点）与
    console「立即触发」（trigger_kind="manual"）复用，保证消息形状单一来源。

    Args:
        due_at: 本次到期时点（epoch 毫秒或可转 float 的字符串）。cron 侧由
            ``_fire_if_due`` 传入其判定用的 ``next_run_at``；缺省时回落到调度
            定义里的 ``next_run_at``（见 ``normalized_due_ms``）。manual 忽略。

    Returns:
        Stream message id；入队失败返回 None。
    """
    schedule_id = schedule["schedule_id"]
    # 租户状态闸（plaita#27）：停用租户的调度不再入队。状态来自 console 发布
    # 的 Redis 短视图；缺失/无记录 = active（fail-open，见 tenant_status 模块）。
    if not tenant_is_active(redis_client, schedule.get("tenant_id")):
        logger.info("租户 %s 已停用，跳过调度 %s 派发", schedule.get("tenant_id"), schedule_id)
        return None
    # 每租户 start 准入配额（plaita#28）：预铸 execution_id 并在入口占名额，
    # 消息形状向 console start 对齐（worker 闸按成员幂等放行）。超限则本次
    # 触发不入队（返回 None，与「租户停用跳过」同语义）：cron 调度等下一次
    # 到期自然重试，console「立即触发」拿到 None 显示未触发——这就是
    # 「超限排队/拒绝」在调度入口的形态。fail-open：未配额/redis 故障放行。
    tenant_id = schedule.get("tenant_id") or "default"
    execution_id = uuid.uuid4().hex
    if not try_admit_tenant_start(redis_client, tenant_id, execution_id):
        logger.info(
            "租户 %s start 配额已满，跳过调度 %s 本次触发（execution=%s）",
            tenant_id, schedule_id, execution_id,
        )
        return None
    now = datetime.now()
    now_ms = int(now.timestamp() * 1000)

    message: Dict[str, Any] = {
        "type": "start",
        # 调度定义归属租户（console 写入 HASH 值）；缺省视为 default（兼容旧值）
        "tenant_id": schedule.get("tenant_id") or "default",
        # plaita#28：预铸执行 id（入口已占名额；终态释放按同一 id 路由）
        "execution_id": execution_id,
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
    # 确定性幂等键（#23，plaita#118 修正基准）：基准必须是**本次到期时点**
    # 而不是调用墙钟秒。旧键 sched:{id}:{墙钟秒} 有两个失败面：① 同一个 due 被
    # 重触发（enqueue 成功而 next_run_at 回写失败 + 30s 锁过期）时两次落在不同
    # 秒 ⇒ 两个不同键 ⇒ worker 各自 SET NX 成功、各自铸 execution_id，同一次
    # cron 到期整条 flow 双跑；② cron 与 manual 同秒撞成同一个键，manual 被
    # 静默吞掉。现在 cron 以 due 为基准（同一 due ⇒ 同键，不同 due ⇒ 不同键），
    # manual 走独立且唯一的键空间。
    due_ms = normalized_due_ms(schedule, due_at)
    if trigger_kind != "manual" and due_ms is None:
        logger.warning(
            "调度 %s 无可用到期时点（due_at=%r，next_run_at=%r），幂等键降级为墙钟秒",
            schedule_id, due_at, schedule.get("next_run_at"),
        )
    message["dedup_key"] = build_dedup_key(
        schedule_id, trigger_kind, due_ms, now_ms // 1000
    )

    try:
        # 统一走 enqueue_task：消息信封带 schema_version，老格式（无该字段）按 v1 处理
        msg_id = enqueue_task(redis_client, queue_name, message)
        enqueue_ok = True
    except Exception as e:
        logger.error("调度 %s 入队失败: %s", schedule_id, _scrub_exception_text(e))
        msg_id = None
        enqueue_ok = False
        # 入队失败回滚入口名额（plaita#28），防泄漏到 TTL 才自愈
        release_tenant_start(redis_client, tenant_id, execution_id)

    fire_record = {
        "fired_at": now.isoformat(),
        "trigger_kind": trigger_kind,
        "flow_id": schedule["flow_id"],
        "version": schedule.get("version") or "",
        "dedup_key": message["dedup_key"],
        "enqueue": "ok" if enqueue_ok else "failed",
        "msg_id": msg_id or "",
    }
    try:
        fires_key = SCHEDULE_FIRES_KEY.format(schedule_id=schedule_id)
        pipe = redis_client.pipeline()
        pipe.lpush(fires_key, json.dumps(fire_record, ensure_ascii=False))
        pipe.ltrim(fires_key, 0, FIRE_HISTORY_MAX - 1)
        pipe.execute()
    except Exception as exc:
        logger.warning("调度 %s 触发历史写入失败: %s", schedule_id, _scrub_exception_text(exc))

    # 回写触发状态；错过的周期不补偿——next_run_at 直接跳到现在之后
    try:
        updates = {
            "last_fired_at": now.isoformat(),
            "last_enqueue_ok": "1" if enqueue_ok else "0",
        }
        if trigger_kind == "cron":
            updates["next_run_at"] = str(next_run_after(schedule["cron"], now))
        redis_client.hset(SCHEDULES_KEY, key=schedule_id, value=json.dumps(_merge(schedule, updates), ensure_ascii=False))
    except Exception as exc:
        logger.warning("调度 %s 状态回写失败: %s", schedule_id, _scrub_exception_text(exc))

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
            logger.error("启动调度服务失败: %s", _scrub_exception_text(e))
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
                logger.error("调度扫描失败: %s", _scrub_exception_text(e))
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
                logger.error("调度 %s 处理失败: %s", schedule_id, _scrub_exception_text(e))

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

        # due_at 显式传本次判定用的到期时点：幂等键必须锚在 due 上，而不是
        # 触发墙钟（见 fire_schedule / 模块 docstring）。
        fire_schedule(
            self._redis_client, schedule, self.queue_name,
            trigger_kind="cron", due_at=next_run_at,
        )
