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
import os
import threading
import time
import uuid
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from croniter import croniter
from redis import Redis
from redis.exceptions import WatchError

from plaita.server.task_queue import enqueue_task

from ...logger import logger
from ..tenant_context import tenant_namespace
from ..tenant_quota import release_tenant_start, try_admit_tenant_start
from ..tenant_status import tenant_is_active
from .. import start_dedup as _dedup
from .base_service import BaseExtendedService
from plaita.core.url_redaction import _scrub_exception_text, _scrub_url_text
from plaita.storage.base import ExecutionState
from plaita.storage.redis import RedisExecutionStorage

# 与 console 后端共享的 Redis 视图键（console/api/schedules.py 同款常量）
SCHEDULES_KEY = "plaita:schedules"
SCHEDULE_FIRES_KEY = "plaita:schedule:fires:{schedule_id}"
SCHEDULE_LOCK_KEY = "plaita:schedule:lock:{schedule_id}"
FIRE_HISTORY_MAX = 50

#: 派发队列默认名（与 FlowWorker / console 缺省一致）。
DEFAULT_QUEUE_NAME = "plaita:flow:queue"

#: 已告警过的 (console_queue, worker_queue) 组合（避免每次派发刷屏）。
_queue_split_warned: set[tuple[str, str]] = set()


def resolve_dispatch_queue_name(configured: Optional[str] = None) -> str:
    """解析调度派发队列名（A56-02，调用点求值）。

    console 生产侧读 ``PLAITA_CONSOLE_TASK_QUEUE``（api/executions.py 的
    ``_task_queue_name()``），worker 消费侧读 ``PLAITA_QUEUE_NAME``/``QUEUE_NAME``。
    调度服务此前在 ``__init__`` 固化后者（且完全忽略 console env），分裂队列/
    滚动升级时“立即触发”与 cron 会落到不同 Stream。这里改为每次派发时解析，
    console env 优先；显式 ``service_config["queue_name"]`` 仍然最高优先。

    优先级：``service_config["queue_name"]`` > ``PLAITA_CONSOLE_TASK_QUEUE``
    > ``PLAITA_QUEUE_NAME`` > ``QUEUE_NAME`` > 默认值。

    当 console/调度侧与 worker 消费侧的 env **同时存在且不同**时打一次
    WARNING：调度消息会写入 console 队列，而 worker 仍在消费另一个 Stream，
    这是“200 受理但无人消费”的前兆（迁移时两个 env 必须一起改）。
    """
    explicit = str(configured).strip() if configured else ""
    if explicit:
        return explicit
    # B2R31-02：与 manual start/capacity 共用单一 resolver；空串不再静默
    # 回退（identity 配置错误），跨源不同仍打一次 WARNING（A56-02）。
    from plaita.runtime_config import resolve_dispatch_queue_name as _resolve

    return _resolve("schedule")

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


# queued 占位行的调用者标记（A43-04；worker 接管后按 trigger 归因落行
# invoker=schedule 并透传 trigger——A2R44-02，归因不再被接管覆写断链）。
_SCHEDULE_PLACEHOLDER_INVOKER = "schedule"

# 触发拒绝留痕（B44-01）：fire_record.enqueue="rejected" + rejection_reason。
# 拒绝此前在 fire_record 写入前 return——「到期被拒」与「未到期」在调度面
# 不可区分（静默停走）。拒绝不带 execution_id：预铸 id 未落任何行/消息、
# 幂等映射已回滚，留 id 只会链到 404。
FIRE_ENQUEUE_REJECTED = "rejected"
REJECTION_TENANT_DISABLED = "tenant_disabled"
REJECTION_QUOTA_EXHAUSTED = "tenant_quota_exhausted"


def _placeholder_storage(
    redis_client: Redis, tenant_id: str
) -> RedisExecutionStorage:
    """占位行读写用的租户命名空间 storage（与 console GET/列表同一键形状/索引）。"""
    return RedisExecutionStorage(
        client=redis_client, namespace=tenant_namespace(tenant_id)
    )


def _write_queued_placeholder(
    redis_client: Redis,
    tenant_id: str,
    execution_id: str,
    flow_id: str,
    version: Optional[str],
    now_iso: str,
    trigger: Optional[Dict[str, Any]] = None,
) -> bool:
    """同步落 ``queued`` 占位状态行（A43-04 复用 #273 受理语义）：触发返回前该
    id 必须可查。

    经 ``RedisExecutionStorage.save_execution_state`` 写——索引成员同 pipeline
    维护（列表/状态过滤立即命中）；非终态无 TTL。行 context 为空：worker 侧
    ``_existing_start_checkpoint`` 对「非终态且空 checkpoint」按首投处理，
    消费时以 running 行覆盖接管，占位行不会把 start 载体误转成 resume。
    ``trigger``（A2R44-02）：触发归因随占位行落盘——受理窗口内归因即可查，
    不等 worker 接管。失败返回 False，调用方不入队并逐级回滚（绝不带着
    「已受理、行不存在」的窗口入队——那正是本单要关的可见性缺口）。
    """
    state = ExecutionState(
        execution_id=execution_id,
        flow_id=flow_id,
        flow_version=version,
        tenant_id=tenant_id,
        context={},
        status="queued",
        start_time=now_iso,
        last_update_time=now_iso,
        invoker=_SCHEDULE_PLACEHOLDER_INVOKER,
        trigger=trigger,
    )
    try:
        return _placeholder_storage(redis_client, tenant_id).save_execution_state(
            execution_id, state
        )
    except Exception as exc:  # noqa: BLE001 — 落盘异常与 False 同处置（不入队+回滚）
        logger.error(
            "queued 占位状态行写入异常 execution=%s: %s",
            execution_id, _scrub_exception_text(exc),
        )
        return False


def _discard_queued_placeholder(
    redis_client: Redis, tenant_id: str, execution_id: str
) -> None:
    """入队失败时清掉占位行（状态键 + 索引成员）；尽力而为，失败留 ERROR。"""
    try:
        _placeholder_storage(redis_client, tenant_id).delete_execution_state(
            execution_id
        )
    except Exception as exc:  # noqa: BLE001 — 清理失败必须可观测（幽灵 queued 行）
        logger.error(
            "入队失败后占位状态行清理失败 execution=%s（将残留 queued 行，"
            "需人工清理）: %s", execution_id, _scrub_exception_text(exc),
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

    受理语义与 console POST /executions 同源（#273/#274，A43-04 收口调度侧
    可见性双标）：幂等键预认领 → 占租户 start 配额 → 落 queued 占位状态行 →
    入队；任一步失败逐级回滚并具名留痕，绝不带病入队。worker 停机/积压期，
    触发产生的 execution_id 可经 fire_record 定位、经占位行 GET/CANCEL——
    与手动 start 同一受理窗口。

    Args:
        due_at: 本次到期时点（epoch 毫秒或可转 float 的字符串）。cron 侧由
            ``_fire_if_due`` 传入其判定用的 ``next_run_at``；缺省时回落到调度
            定义里的 ``next_run_at``（见 ``normalized_due_ms``）。manual 忽略。

    Returns:
        Stream message id；入队失败 / 预算或落盘回滚 / 同键 dedup 收敛 /
        闸拒绝（停用租户、配额满——留 rejected fire_record，B44-01）返回 None。
    """
    schedule_id = schedule["schedule_id"]
    # 租户状态闸（plaita#27）：停用租户的调度不再入队。状态来自 console 发布
    # 的 Redis 短视图；缺失/无记录 = active（fail-open，见 tenant_status 模块）。
    # 拒绝留痕（B44-01）：拒绝也落 fire_record（rejected + 原因）——否则
    # 「到期被拒」与「未到期」在调度面不可区分（静默停走）。不推进
    # next_run_at/last_fired_at/last_enqueue_ok：恢复后同一 due 立即正常受理，
    # 恢复后的 ok 记录与 rejected 记录并存（历史即调度面健康证据）。
    if not tenant_is_active(redis_client, schedule.get("tenant_id")):
        logger.info("租户 %s 已停用，跳过调度 %s 派发", schedule.get("tenant_id"), schedule_id)
        _write_fire_record(
            redis_client, schedule_id,
            fired_at=datetime.now().isoformat(), trigger_kind=trigger_kind,
            flow_id=schedule["flow_id"], version=schedule.get("version") or "",
            dedup_key="", enqueue=FIRE_ENQUEUE_REJECTED, msg_id="",
            execution_id="",
            rejection_reason=REJECTION_TENANT_DISABLED,
        )
        return None
    tenant_id = schedule.get("tenant_id") or "default"
    now = datetime.now()
    now_ms = int(now.timestamp() * 1000)

    # 确定性幂等键（#23，plaita#118 修正基准）：基准必须是**本次到期时点**
    # 而不是调用墙钟秒（旧键同 due 重触发双跑、cron/manual 同秒互吞，见下）。
    due_ms = normalized_due_ms(schedule, due_at)
    if trigger_kind != "manual" and due_ms is None:
        logger.warning(
            "调度 %s 无可用到期时点（due_at=%r，next_run_at=%r），幂等键降级为墙钟秒",
            schedule_id, due_at, schedule.get("next_run_at"),
        )
    dedup_key = build_dedup_key(schedule_id, trigger_kind, due_ms, now_ms // 1000)

    # 每租户 start 准入配额（plaita#28）：预铸 execution_id 并在入口占名额，
    # 消息形状向 console start 对齐（worker 闸按成员幂等放行）。超限则本次
    # 触发不入队（返回 None，与「租户停用跳过」同语义）：cron 调度等下一次
    # 到期自然重试，console「立即触发」拿到 None 显示未触发——这就是
    # 「超限排队/拒绝」在调度入口的形态。fail-open：未配额/redis 故障放行。
    execution_id = uuid.uuid4().hex
    # r59 ③：schedule/cron 与 console 共用 token 预认领状态机（A43-04 的
    # 占位行/受理可见性保留）。同 dedup_key 重触发收敛既有 id：不入队、
    # 不重复占配额；首次触发才 admit + placeholder + enqueue。
    claim = _dedup.claim(redis_client, tenant_id, dedup_key, execution_id)
    claim_token: Optional[str] = None
    dedup_hit = False
    if claim.error:
        logger.error(
            "调度 %s 预认领失败（fail-closed）: %s", schedule_id, claim.error
        )
        return None
    if claim.claimed:
        claim_token = claim.token
        if not try_admit_tenant_start(redis_client, tenant_id, execution_id):
            _dedup.release(
                redis_client, tenant_id, dedup_key, execution_id,
                token=claim_token, require_row_absent=True,
            )
            logger.info(
                "租户 %s start 配额已满，跳过调度 %s 本次触发（execution=%s）",
                tenant_id, schedule_id, execution_id,
            )
            # 拒绝留痕（B44-01）：rejected + 原因；execution_id 不落——预铸 id
            # 未落任何行/消息且认领已回滚，留 id 只会链到 404。不推进调度状态：
            # 同 due 每 30s 锁窗重试，配额释放后正常受理（ok 与 rejected 并存）。
            _write_fire_record(
                redis_client, schedule_id,
                fired_at=now.isoformat(), trigger_kind=trigger_kind,
                flow_id=schedule["flow_id"], version=schedule.get("version") or "",
                dedup_key=dedup_key, enqueue=FIRE_ENQUEUE_REJECTED, msg_id="",
                execution_id="",
                rejection_reason=REJECTION_QUOTA_EXHAUSTED,
            )
            return None
    else:
        mapped = claim.mapped
        owner = _dedup.read_owner(redis_client, tenant_id, dedup_key)
        if mapped and owner.live and owner.execution_id == mapped:
            execution_id = mapped
            dedup_hit = True
            logger.warning(
                "调度 %s 同键重触发（dedup=%s 已由 execution=%s 受理），本次不入队",
                schedule_id, dedup_key, mapped,
            )
            _write_fire_record(
                redis_client, schedule_id,
                fired_at=now.isoformat(), trigger_kind=trigger_kind,
                flow_id=schedule["flow_id"], version=schedule.get("version") or "",
                dedup_key=dedup_key, enqueue="dedup", msg_id="",
                execution_id=mapped,
            )
            _advance_schedule_after_fire(
                redis_client, schedule_id, schedule, trigger_kind, now,
                enqueue_ok=None,
            )
            return None
        # 孤儿（row 可能残留）：discard 占位行（写墓碑）+ 条件释放映射，然后
        # 用新 id 重试一次本 due——不把孤儿 map/queued 行留 7 天静默吞掉。
        if mapped:
            try:
                if redis_client.exists(
                    _dedup.execution_row_key(tenant_id, mapped)
                ):
                    _discard_queued_placeholder(redis_client, tenant_id, mapped)
            except Exception as exc:  # noqa: BLE001 — 清理失败具名 WARNING
                logger.warning(
                    "调度 %s 孤儿占位行清理失败 execution=%s: %s",
                    schedule_id, mapped, _scrub_url_text(exc),
                )
            _dedup.release(
                redis_client, tenant_id, dedup_key, mapped,
                require_row_absent=False,
            )
        if mapped and _dedup.read_mapped(redis_client, tenant_id, dedup_key) == mapped:
            logger.warning(
                "调度 %s 认领命中孤儿映射（mapped=%r owner=%r）且条件释放失败，"
                "本轮不重复入队（等 TTL/人工处置）",
                schedule_id, mapped, owner.state,
            )
            return None
        logger.warning(
            "调度 %s 已清理孤儿映射（mapped=%r），用新 id 重试本 due",
            schedule_id, mapped,
        )
        return fire_schedule(
            redis_client, schedule, queue_name,
            trigger_kind=trigger_kind, due_at=due_at,
        )

    # trigger（A2R44-02）：触发归因三元组单源构造——随占位行落盘（受理窗口内
    # 归因即可查）+ 随 start 消息透传（worker 接管落行按 schedule_id 派生
    # invoker=schedule，周期/手动由 kind 区分）。
    trigger = {
        "kind": trigger_kind,
        "schedule_id": schedule_id,
        "schedule_name": schedule.get("name", ""),
    }
    if claim_token is not None:
        # A43-04：入队前同步落 queued 占位行（invoker=schedule），失败逐级回滚。
        if not _write_queued_placeholder(
            redis_client, tenant_id, execution_id,
            schedule["flow_id"], schedule.get("version"), now.isoformat(),
            trigger=trigger,
        ):
            logger.error(
                "调度 %s 占位状态行写入失败 execution=%s：本次触发不入队，"
                "配额与幂等认领已回滚（cron 等下一次到期，manual 显示未触发）",
                schedule_id, execution_id,
            )
            release_tenant_start(redis_client, tenant_id, execution_id)
            _discard_queued_placeholder(redis_client, tenant_id, execution_id)
            _dedup.release(
                redis_client, tenant_id, dedup_key, execution_id,
                require_row_absent=False,
            )
            _write_fire_record(
                redis_client, schedule_id,
                fired_at=now.isoformat(), trigger_kind=trigger_kind,
                flow_id=schedule["flow_id"], version=schedule.get("version") or "",
                dedup_key=dedup_key, enqueue="failed", msg_id="",
                execution_id=execution_id,
            )
            _advance_schedule_after_fire(
                redis_client, schedule_id, schedule, trigger_kind, now,
                enqueue_ok=False,
            )
            return None

    message: Dict[str, Any] = {
        "type": "start",
        # 调度定义归属租户（console 写入 HASH 值）；缺省视为 default（兼容旧值）
        "tenant_id": schedule.get("tenant_id") or "default",
        # plaita#28：预铸执行 id（入口已占名额；终态释放按同一 id 路由）
        "execution_id": execution_id,
        "flow_id": schedule["flow_id"],
        "params": schedule.get("params") or {},
        "timestamp": now.isoformat(),
        # 触发归因（A2R44-02）：与占位行同源三元组，worker 接管落行时透传
        # （invoker 按 schedule_id 派生为 schedule，周期/手动由 kind 区分）。
        "trigger": trigger,
        # 受理凭据回填（A43-04）：fire_record 记同一 id，受理窗口可定位
        "dedup_key": dedup_key,
    }
    version = schedule.get("version")
    if version:
        message["version"] = version

    enqueue_ok = True
    if dedup_hit:
        msg_id = execution_id
    else:
        # r59 不变量 6：XADD 前必须有 reservation。慢占位行跨过 claiming TTL
        # 时先 guard 用同一 token 续期，再 reserve；仍失败则放弃入队并清理。
        reserved = _dedup.reserve_enqueue(
            redis_client, tenant_id, dedup_key, execution_id, claim_token
        )
        if not reserved and _dedup.guard(
            redis_client, tenant_id, dedup_key, execution_id, claim_token
        ):
            reserved = _dedup.reserve_enqueue(
                redis_client, tenant_id, dedup_key, execution_id, claim_token
            )
        if not reserved:
            logger.error(
                "调度 %s 入队前 reservation 失败 execution=%s：放弃 XADD，"
                "回滚配额/占位行/幂等映射",
                schedule_id, execution_id,
            )
            release_tenant_start(redis_client, tenant_id, execution_id)
            _discard_queued_placeholder(redis_client, tenant_id, execution_id)
            _dedup.release(
                redis_client, tenant_id, dedup_key, execution_id,
                require_row_absent=False,
            )
            _write_fire_record(
                redis_client, schedule_id,
                fired_at=now.isoformat(), trigger_kind=trigger_kind,
                flow_id=schedule["flow_id"], version=schedule.get("version") or "",
                dedup_key=dedup_key, enqueue="failed", msg_id="",
                execution_id=execution_id,
            )
            _advance_schedule_after_fire(
                redis_client, schedule_id, schedule, trigger_kind, now,
                enqueue_ok=False,
            )
            return None
        try:
            # 统一走 enqueue_task：消息信封带 schema_version，老格式（无该字段）按 v1 处理
            # B2R31-01：新生产消息写当前三维代际。
            from plaita.runtime_config import envelope_for_stream
            from plaita.server.tenant_status import read_tenant_generation

            msg_id = enqueue_task(
                redis_client, queue_name, message,
                tenant_generation=read_tenant_generation(
                    redis_client, message.get("tenant_id")
                ),
                **envelope_for_stream(queue_name, role="schedule"),
            )
        except Exception as e:
            logger.error("调度 %s 入队失败: %s", schedule_id, _scrub_exception_text(e))
            msg_id = None
            enqueue_ok = False
            # 入队失败逐级回滚（plaita#28 + A43-04）：名额、占位行、token 认领。
            release_tenant_start(redis_client, tenant_id, execution_id)
            _discard_queued_placeholder(redis_client, tenant_id, execution_id)
            _dedup.release(
                redis_client, tenant_id, dedup_key, execution_id,
                require_row_absent=False,
            )
        else:
            if claim_token and not _dedup.mark_enqueued(
                redis_client, tenant_id, dedup_key, execution_id, claim_token
            ):
                # 认领在入队窗口内被接管/墓碑化：清收刚入队的载体 + 回滚。
                try:
                    redis_client.xdel(queue_name, msg_id)
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "调度 %s 失去认领后清收载体失败（msg=%s）: %s",
                        schedule_id, msg_id, _scrub_url_text(exc),
                    )
                release_tenant_start(redis_client, tenant_id, execution_id)
                _discard_queued_placeholder(redis_client, tenant_id, execution_id)
                _dedup.release(
                    redis_client, tenant_id, dedup_key, execution_id,
                    require_row_absent=False,
                )
                msg_id = None
                enqueue_ok = False

    _write_fire_record(
        redis_client, schedule_id,
        fired_at=now.isoformat(), trigger_kind=trigger_kind,
        flow_id=schedule["flow_id"], version=schedule.get("version") or "",
        dedup_key=dedup_key,
        enqueue=("ok" if enqueue_ok else "failed"),
        msg_id=msg_id or "",
        execution_id=execution_id,
    )

    # 回写触发状态；错过的周期不补偿——next_run_at 直接跳到现在之后。
    _advance_schedule_after_fire(
        redis_client, schedule_id, schedule, trigger_kind, now,
        enqueue_ok=enqueue_ok,
    )

    logger.info(
        "调度 %s（%s）已触发 %s@%s execution=%s → %s",
        schedule.get("name", schedule_id), trigger_kind,
        schedule["flow_id"], schedule.get("version") or "latest",
        execution_id,
        "已入队" if enqueue_ok else "入队失败",
    )
    return msg_id


def _write_fire_record(
    redis_client: Redis,
    schedule_id: str,
    *,
    fired_at: str,
    trigger_kind: str,
    flow_id: str,
    version: str,
    dedup_key: str,
    enqueue: str,
    msg_id: str,
    execution_id: str,
    rejection_reason: str = "",
) -> None:
    """追加触发历史（保留最近 FIRE_HISTORY_MAX 条）。

    ``execution_id``（A43-04）：受理窗口内定位执行的唯一凭据——worker 停机/
    积压期凭它 GET 占位行 / CANCEL；console history 端点的
    ``ScheduleFireRecord.execution_id`` 字段已就绪，集群档记录补上即透出。
    ``enqueue="dedup"``：同键重触发收敛到既有 execution_id、未入队。
    ``enqueue="rejected"`` + ``rejection_reason``（B44-01）：触发被闸拒绝
    （tenant_disabled / tenant_quota_exhausted）也留痕——「到期被拒」与
    「未到期」在调度面可区分；rejected 记录不带 execution_id（未落任何
    行/消息）。非拒绝路径不写该键（存量记录形状零变化）。
    """
    fire_record = {
        "fired_at": fired_at,
        "trigger_kind": trigger_kind,
        "flow_id": flow_id,
        "version": version,
        "dedup_key": dedup_key,
        "enqueue": enqueue,
        "msg_id": msg_id,
        "execution_id": execution_id,
    }
    if rejection_reason:
        fire_record["rejection_reason"] = rejection_reason
    try:
        fires_key = SCHEDULE_FIRES_KEY.format(schedule_id=schedule_id)
        pipe = redis_client.pipeline()
        pipe.lpush(fires_key, json.dumps(fire_record, ensure_ascii=False))
        pipe.ltrim(fires_key, 0, FIRE_HISTORY_MAX - 1)
        pipe.execute()
    except Exception as exc:
        logger.warning("调度 %s 触发历史写入失败: %s", schedule_id, _scrub_exception_text(exc))


def _advance_schedule_after_fire(
    redis_client: Redis,
    schedule_id: str,
    schedule: Dict[str, Any],
    trigger_kind: str,
    now: datetime,
    *,
    enqueue_ok: Optional[bool] = None,
) -> None:
    """触发后回写状态；错过的周期不补偿——next_run_at 直接跳到现在之后。

    plaita#198：**CAS 合并到当前值**，只碰 last_fired_at/last_enqueue_ok/
    next_run_at——调用方传入的 schedule 是扫描开始时的旧快照，整份 `_merge`
    回写会把期间 console 的停用/编辑翻回去。

    ``enqueue_ok``：本次是否真实入队。``None``（dedup 收敛路径——本次调用
    未产生新入队）**不碰** last_enqueue_ok，保留首次受理那次的真值；
    ``False``（落盘失败未入队）/``True`` 照实写。

    （A43-04 起 dedup 收敛/落盘失败路径同样推进 next_run_at：错过不补偿的
    既有语义对「已受理」与「未入队」一致，避免失败触发在同时点上重放。）
    """
    try:
        def _apply_fire(current: Dict[str, Any]) -> None:
            current["last_fired_at"] = now.isoformat()
            if enqueue_ok is not None:
                current["last_enqueue_ok"] = "1" if enqueue_ok else "0"
            if trigger_kind == "cron":
                # 用**当前** cron 重算：并发编辑后的 cron 不能被旧快照的
                # next_run_at 计算覆盖。
                cron = current.get("cron") or schedule.get("cron")
                if cron:
                    current["next_run_at"] = str(next_run_after(cron, now))

        cas_update_schedule(redis_client, schedule_id, _apply_fire)
    except Exception as exc:
        logger.warning("调度 %s 状态回写失败: %s", schedule_id, _scrub_exception_text(exc))


class ScheduleConflictError(RuntimeError):
    """plaita#198：console PUT 的乐观锁冲突（当前 updated_at 与基准不一致）。"""

    def __init__(self, schedule_id: str, latest_updated_at: Optional[str]):
        super().__init__(
            f"调度 {schedule_id} 已被其他编辑更新"
            f"（latest updated_at={latest_updated_at!r}）"
        )
        self.schedule_id = schedule_id
        self.latest_updated_at = latest_updated_at


def cas_update_schedule(
    redis_client: Redis,
    schedule_id: str,
    mutate: Callable[[Dict[str, Any]], None],
    *,
    expected_updated_at: Optional[str] = None,
    check_updated_at: bool = False,
) -> Optional[Dict[str, Any]]:
    """plaita#198：原子读-改-写一个调度定义（WATCH/MULTI），只写回改动的字段。

    ``mutate(current)`` 在 **WATCH 读到的当前值** 上就地修改；调用方持有的
    旧快照（``fire_schedule`` 的 ``schedule``）不参与写回，因此期间的 console
    停用/编辑不会被翻回去。

    - ``check_updated_at=True`` 时，当前记录的 ``updated_at`` 必须等于
      ``expected_updated_at``，否则抛 :class:`ScheduleConflictError`——console
      PUT 的乐观锁（即使基准是 None/空串也做等值比较，legacy 记录首写后
      后续写者会冲突）。
    - 记录不存在 / JSON 损坏 ⇒ 返回 ``None``（调用方按 404/跳过处理）。
    - ``WatchError``（其他写者抢先）⇒ 重新 WATCH 读-合并-写，不丢更新。
    """
    with redis_client.pipeline() as pipe:
        while True:
            try:
                pipe.watch(SCHEDULES_KEY)
                raw = pipe.hget(SCHEDULES_KEY, schedule_id)
                if raw is None:
                    pipe.unwatch()
                    return None
                if isinstance(raw, bytes):
                    raw = raw.decode()
                try:
                    current = json.loads(raw)
                except json.JSONDecodeError:
                    pipe.unwatch()
                    logger.warning("调度 %s 定义无法解析，放弃 CAS 回写", schedule_id)
                    return None
                if not isinstance(current, dict):
                    pipe.unwatch()
                    return None
                if check_updated_at and current.get("updated_at") != expected_updated_at:
                    latest = current.get("updated_at")
                    pipe.unwatch()
                    raise ScheduleConflictError(schedule_id, latest)
                mutate(current)
                pipe.multi()
                pipe.hset(
                    SCHEDULES_KEY,
                    schedule_id,
                    json.dumps(current, ensure_ascii=False),
                )
                pipe.execute()
                return current
            except WatchError:
                continue
            except ScheduleConflictError:
                raise
            except Exception as exc:  # noqa: BLE001 — 回写失败不阻塞触发主链路
                logger.warning(
                    "调度 %s CAS 回写失败: %s",
                    schedule_id, _scrub_exception_text(exc),
                )
                return None


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
        # 注意：必须在 super().__init__() 之前就绪——基类构造期间会调用
        # _get_registry_metadata() 读取这些属性
        cfg = service_config or {}
        # A56-02：显式配置最高优先；env 在**每次派发**时解析（见
        # resolve_dispatch_queue_name），不在构造期固化。
        self._explicit_queue_name = str(cfg.get("queue_name") or "").strip() or None
        self.queue_name = resolve_dispatch_queue_name(self._explicit_queue_name)
        # B2R31-03：0/负值 = 紧环，拒绝（不是「关闭」）；env 读取走 WARNING+
        # 安全默认，显式 service_config 非法直接具名失败。
        if cfg.get("check_interval") is not None:
            self.check_interval = float(cfg.get("check_interval"))
            if self.check_interval <= 0:
                raise ValueError(
                    "schedule check_interval must be > 0 "
                    f"(got {cfg.get('check_interval')!r})"
                )
        else:
            from plaita.runtime_config import optional_float_env

            self.check_interval = optional_float_env(
                "PLAITA_CHECK_INTERVAL", 1.0, minimum=1e-9
            )
        self._loop_thread: Optional[threading.Thread] = None

        super().__init__(
            event_bus=event_bus,
            service_config=service_config,
            redis_client=redis_client,
        )

    def get_service_type(self) -> str:
        return "schedule_service"

    def _get_registry_metadata(self) -> Dict[str, Any]:
        # 注册/心跳时刷新：env 可能在进程启动后改动（滚动升级按序改 env）
        self.queue_name = resolve_dispatch_queue_name(self._explicit_queue_name)
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
            # 自愈：缺失/清零的 next_run_at 从当前时间重算。
            # plaita#198：同样 CAS 合并，只写 next_run_at；用**当前** cron，
            # 不整份回写扫描开始时的旧快照（否则会翻回并发的停用/编辑）。
            next_run_at = next_run_after(cron)

            def _apply_self_heal(current: Dict[str, Any]) -> None:
                effective_cron = current.get("cron") or cron
                if effective_cron:
                    current["next_run_at"] = str(next_run_after(effective_cron))

            cas_update_schedule(self._redis_client, schedule_id, _apply_self_heal)
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
        queue_name = resolve_dispatch_queue_name(self._explicit_queue_name)
        self.queue_name = queue_name
        fire_schedule(
            self._redis_client, schedule, queue_name,
            trigger_kind="cron", due_at=next_run_at,
        )
