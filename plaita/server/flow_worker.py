from datetime import datetime
import hashlib
import inspect
import json
import logging
import os
import signal
import threading
import uuid
from typing import Dict, Any, Optional, Set, Tuple

import argparse
import importlib
import sys

from cachetools import TTLCache
from redis import Redis
from plaita.core.errors import ResumeType
from plaita.event.core import EventBus
from plaita.core.errors import (
    FlowCancelledException,
    FlowTimeoutError,
    NodeExecutionError,
    NodeTimeoutError,
)
from plaita.core.flow import Flow
from plaita.core.executor import FlowExecution, ExecutionMode
from plaita.storage.base import ExecutionState, ExecutionStorage, FlowStorage
from plaita.storage.fenced import (
    reset_current_fence_token,
    set_current_fence_token,
)
from plaita.storage.redis import ExecutionStateLoadError, TERMINAL_EXECUTION_STATUSES
from plaita.logger import logger
from plaita.server.registry import RegistryMixin, ServiceRegistry, ServiceInfo
from plaita.server.control import ControlMixin, ControlListener
from plaita.server.log_handler import setup_redis_logging
from plaita.server.task_queue import (
    DEFAULT_CLAIM_MIN_IDLE_MS,
    DEFAULT_CONSUMER_GROUP,
    DEFAULT_MAX_DELIVERIES,
    RedisStreamTaskQueue,
    StreamTask,
    enqueue_task,
)
from plaita.server.execution_lease import (
    DEFAULT_LEASE_TTL_SECONDS,
    ExecutionLease,
    ExecutionLeaseError,
    NullExecutionLease,
    RedisExecutionLease,
    new_holder_token,
)
from plaita.server.tenant_context import (
    TenantRoutingExecutionLease,
    current_tenant,
    reset_current_tenant,
    set_current_tenant,
    tenant_namespace,
)


def _env_switch(name: str) -> bool:
    """读 ``=1`` 形式的环境回滚开关（每次调用读取，便于测试注入）。"""
    return os.environ.get(name, "").strip() == "1"


def _cancel_checkpoint_disabled() -> bool:
    """波次①回滚开关：PLAITA_DISABLE_CANCEL_CHECKPOINT=1 时 worker 不查取消标志键。"""
    return _env_switch("PLAITA_DISABLE_CANCEL_CHECKPOINT")


def _cancel_interrupt_disabled() -> bool:
    """波次③回滚开关（步内中断半边）：PLAITA_DISABLE_CANCEL_INTERRUPT=1 时
    不启动取消监听线程——回到「只在步界检查取消标志键」的现状语义。"""
    return _env_switch("PLAITA_DISABLE_CANCEL_INTERRUPT")


def _fencing_disabled() -> bool:
    """波次②回滚开关（fencing 半边）：PLAITA_DISABLE_FENCING=1 时退回 SET NX acquire。"""
    return _env_switch("PLAITA_DISABLE_FENCING")


def _watchdog_disabled() -> bool:
    """波次②回滚开关（看门狗半边）：PLAITA_DISABLE_LEASE_WATCHDOG=1 时不启动续租线程。"""
    return _env_switch("PLAITA_DISABLE_LEASE_WATCHDOG")


def _affinity_disabled() -> bool:
    """机器亲和闸回滚开关：PLAITA_DISABLE_AFFINITY=1 时不做路径检查
    （回到「领到就跑」的现状；单机/同构环境本就不需要本闸）。"""
    return _env_switch("PLAITA_DISABLE_AFFINITY")


def _node_retry_disabled() -> bool:
    """波次二任务①回滚开关：PLAITA_DISABLE_NODE_RETRY=1 时节点失败直接终态化
    error（完全回到波次前行为）。"""
    return _env_switch("PLAITA_DISABLE_NODE_RETRY")


# 允许的 code 沙箱后端。默认 subprocess：无需 docker daemon，适合 keeper/console
# 拉起的 worker（生产流程只在 code 节点里跑确定性胶水代码，非用户任意代码）。
# 多租户对外部署应改 docker 并收紧 allowed_backends。
_CODE_ALLOWED_BACKENDS = ("subprocess", "docker", "unsafe")


def _code_backend_for_worker() -> str:
    """worker 启动时 CodeNode 的沙箱后端（PLAITA_CODE_BACKEND，默认 subprocess）。"""
    raw = os.environ.get("PLAITA_CODE_BACKEND", "").strip()
    return raw or "subprocess"


def _code_node_enabled() -> bool:
    """PLAITA_DISABLE_CODE_NODE=1 时不注册 CodeNode（含 code 节点的流程会被丢弃）。"""
    return not _env_switch("PLAITA_DISABLE_CODE_NODE")


def _register_code_node_for_worker() -> None:
    """为 worker 注册 CodeNode（生产流程如 self-improve-v2 含 code 节点）。

    默认注册表自 0.4.0 起不含 CodeNode（执行任意用户代码须显式 opt-in），worker
    不注册则整单被丢弃（unRecognized node type: code）。后端不可用（如选 docker
    但无 daemon）时**降级到 subprocess 并告警**，不让整机起不来。
    """
    from plaita.node import register_code_node

    backend = _code_backend_for_worker()
    try:
        register_code_node(default_backend=backend, allowed_backends=list(_CODE_ALLOWED_BACKENDS))
        logger.info("CodeNode 已注册（sandbox_backend=%s）", backend)
        return
    except RuntimeError as e:  # 多为 docker daemon 不可用
        logger.warning("CodeNode 注册失败（backend=%s）：%s —— 降级 subprocess 重试", backend, e)
    register_code_node(default_backend="subprocess", allowed_backends=list(_CODE_ALLOWED_BACKENDS))
    logger.info("CodeNode 已注册（sandbox_backend=subprocess，降级）")



# 节点级重试判据沿 __cause__ 链回溯的最大深度。分布式归一化链固定一层
# （FlowErrorException → NodeExecutionError），多留几层防御双重包装。
_NODE_RETRY_CHAIN_MAX_DEPTH = 5


def _is_retryable_node_failure(exc: BaseException) -> bool:
    """判别「可重试的节点执行失败」vs「协议/图错误/超时/取消」。

    分布式路径上 ``run_distributed`` 把一切异常归一化为 ``FlowErrorException``
    且原始异常挂在 ``__cause__``（core/_error_normalization.py:59-69）。判据：

    - 链中出现 ``NodeExecutionError``（runner.py 节点 abort 时 raise，原始
      异常再挂它的 ``__cause__``）→ **可重试**——LLM/HTTP 网络抖动属瞬态，
      磁盘 state 仍停在最后成功步的 checkpoint（strategies.py
      ``_execute_current_node`` 的 ``runner.run_node`` 抛出时 context 未变）；
    - 链中出现超时（``NodeTimeoutError``/``FlowTimeoutError``）→ **不重试**
      （确定性信号，重试=再烧一次全款，对齐 v3 宿主 D4 决策），维持现状
      终态化 error；
    - 链中出现取消（``FlowCancelledException``）→ **不重试**（执行级意图）；
    - 其余（``ResumeError`` 等协议类、图结构错误、无 ``__cause__`` 的引擎
      管线异常）→ **不重试**，维持现状终态化。

    超时/取消优先于 NodeExecutionError 判定：防御节点函数把超时包进普通
    异常再被 abort 包装的病态链。
    """
    cause = getattr(exc, "__cause__", None)
    depth = 0
    while cause is not None and depth < _NODE_RETRY_CHAIN_MAX_DEPTH:
        if isinstance(cause, (NodeTimeoutError, FlowTimeoutError, FlowCancelledException)):
            return False
        if isinstance(cause, NodeExecutionError):
            return True
        cause = cause.__cause__
        depth += 1
    return False


def _chain_has_cancellation(exc: BaseException) -> bool:
    """异常链（含自身）中是否含执行级取消 ``FlowCancelledException``。

    波次③：取消监听在节点执行期置位 ``cancel_requested`` 后，引擎在**下一个
    节点入口**抛 ``FlowCancelledException``；distributed 策略把它归一化为
    ``FlowErrorException`` 并把原始异常挂在 ``__cause__``（_error_normalization）。
    步循环的通用 ``except`` 需要据此把终态写成 ``cancelled`` 而非 ``error``。

    取消优先于一切其他判定：链中一旦出现取消即整链定性为取消（即便更深处
    还有被 abort 包装的节点异常）。
    """
    node: Optional[BaseException] = exc
    depth = 0
    while node is not None and depth <= _NODE_RETRY_CHAIN_MAX_DEPTH + 1:
        if isinstance(node, FlowCancelledException):
            return True
        node = node.__cause__
        depth += 1
    return False


class StatePersistError(RuntimeError):
    """执行状态落盘失败（``save_execution_state`` 返回 False）。

    Redis 后端把一切异常（网络瞬断/序列化失败）吞成 False；终态/挂起/步进
    落盘失败若被静默放行，消息会被 ack——节点副作用已发生而执行状态永远
    停在旧值（僵尸执行）。此处把 False 升级为异常，让 ``RedisFlowWorker.run``
    的兜底路径不 ack 消息、走 at-least-once 重投。

    刻意**不是** ValueError 子类：run() 把 ValueError 当畸形消息 poison ack，
    继承它会让存储瞬态错误仍被丢弃（ReviewFix D1 同款教训）。
    """


class ServiceDispatchError(RuntimeError):
    """挂起服务任务投递失败（有 redis 客户端时不再吞）。

    挂起时先 save(suspended) 再 rpush 服务队列；rpush 失败若被吞，执行已
    suspended、消息被 ack → delay/approval 任务永无人接，永久挂起。抛出后
    消息走重投、下轮 resume 重新执行挂起节点再派发（重复注册的订阅由
    EventFilter 终态 GC / TTL 兜底）。suspended 状态保留，不翻 error。
    """


class FlowHashMismatchError(ValueError):
    """flow 定义自启动后已变更（hash 不匹配），执行无法安全续跑（波次二任务②）。

    resume_flow 在终态化 error **之后**抛出本异常：run() 把它当 ValueError
    poison ack（终态已可观测，重投只会命中 already_terminal 短路）。刻意
    用独立子类而非裸 ValueError——resume 的通用 except 需要区分「指纹
    不匹配（已终态化，原样上抛）」与「引擎内部其他 ValueError（维持现状
    终态化路径）」。
    """


class TaskNotForThisWorker(RuntimeError):
    """任务与本机不亲和（repo/run_dir 指向别的机器的绝对路径）。

    消费入口抛出——由 run() 的专门分支**交接**（见 _handover_non_affine）：
    ack 原消息 + 重入队一份同体新副本（delivery 归 1）。不终态化、不死信：
    任务本身没毛病，只是不该在本机跑。

    为何要「重入队」而非「留 pending」：留 pending 的消息会被对端 worker
    按 claim_min_idle_ms 反复 XCLAIM，而 XCLAIM 每次让 Redis 的
    deliveries +1——两 worker 环境下 60s 一轮，5 轮即触顶 max_deliveries
    → 误死信（2026-10-06 双机实测：非亲和任务被让出 5 次后进 DLQ）。
    重入队新副本把计数清零，交接无损耗。
    """


class NodeExecutionRetryableError(RuntimeError):
    """节点执行可重试失败（波次二任务①）：执行**未**终态化，消息等重投。

    节点异常经 ``_is_retryable_node_failure`` 判为瞬态且重试预算未耗尽时，
    处理函数跳过终态化改抛本异常——磁盘 state 停在最后成功步 checkpoint，
    ``run()`` 对本异常仿照 ``ExecutionLeaseError`` 处理：不 ack、不计
    poison、留 pending，消息被回收（间隔=claim_min_idle_ms，默认 60s）后
    resume 自然重跑失败节点。

    刻意**不是** ValueError 子类（同 StatePersistError 的理由）：ValueError
    会被 run() 当畸形消息 poison ack。

    Attributes:
        execution_id: 所属执行（日志与守卫后备判据用）。
        attempt: 已放行的重试次数（重试计数键 INCR 后的值）。
        delivery_count: 消息当前投递次数（仅可观测；预算判定用重试计数键，
            见 ``_record_node_retry`` 的说明）。
    """

    def __init__(self, message: str, execution_id: Optional[str] = None,
                 attempt: int = 0, delivery_count: Optional[int] = None):
        super().__init__(message)
        self.execution_id = execution_id
        self.attempt = attempt
        self.delivery_count = delivery_count


class FlowWorker:
    """
    流程工作器：把 ``run_distributed`` 与 ExecutionStorage / FlowStorage / EventBus 串起来。

    这是 **suspend/resume 编排器**，不是具备至少一次投递或崩溃安全的工作流引擎。
    可靠性边界见 docs-site ``distributed/flow-worker.md`` 与类常量
    ``PERSIST_EVERY_N_STEPS`` / ``RedisFlowWorker.run`` 的队列语义说明。
    """

    # 连续推进时每隔 N 步落一次中间态。挂起 / 结束 / 出错始终立即持久化。
    # 默认 1 = 每步落盘，崩溃不丢步进进度（Wave 3）。
    PERSIST_EVERY_N_STEPS = 1

    def __init__(
        self,
        execution_storage: ExecutionStorage,
        flow_storage: FlowStorage,
        event_bus: EventBus = None,
        cache_size: int = 100,
        cache_ttl: int = 300,
        callback_handlers: Optional[list] = None,
        execution_lease: Optional[ExecutionLease] = None,
        lease_ttl_seconds: int = DEFAULT_LEASE_TTL_SECONDS,
    ):
        """
        初始化流程工作器

        Args:
            execution_storage: 执行状态存储实例
            flow_storage: 流程定义存储实例
            event_bus: 事件总线实例
            cache_size: 缓存大小，默认100
            cache_ttl: 缓存过期时间(秒)，默认300秒
            callback_handlers: 分布式执行期间贯穿所有步骤的回调列表
            execution_lease: resume 租约（默认 NullExecutionLease）；RedisFlowWorker 注入 Redis 实现
            lease_ttl_seconds: resume 租约 TTL（秒），推进过程中会 renew
        """
        self.execution_storage = execution_storage
        self.flow_storage = flow_storage
        self.event_bus = event_bus
        self.callback_handlers = list(callback_handlers) if callback_handlers else []
        # 初始化流程定义缓存，使用TTL缓存
        self.flow_definition_cache = TTLCache(maxsize=cache_size, ttl=cache_ttl)
        self.execution_lease = execution_lease or NullExecutionLease()
        self.lease_ttl_seconds = lease_ttl_seconds
        # 取消监听（波次③：步内可中断）：基类先建好登记表与停止位，内存 worker
        # 不启动线程（_cancel_requested 无 redis 客户端时恒为 False，登记为空转）。
        self._cancel_watch_lock = threading.Lock()
        self._cancel_watch: Dict[str, Tuple[Any, str]] = {}
        self._cancel_thread: Optional[threading.Thread] = None
        self._cancel_stop = threading.Event()
        self._cancel_poll_seconds: Optional[float] = None

    # ---- 节点级有界重试（波次二任务①）----

    # 重试计数键 TTL：7 天自清理（与取消标志键同款「带 TTL 意图键」模式）。
    NODE_RETRY_COUNTER_TTL_SECONDS = 7 * 86400

    def _node_retry_budget(self) -> int:
        """节点重试预算：沿用队列 max_deliveries 语义（默认 5）。

        RedisFlowWorker 以其队列配置为准；基类（内存 worker / 单测）回退
        DEFAULT_MAX_DELIVERIES。
        """
        configured = getattr(self, "_max_deliveries", None)
        try:
            return max(1, int(configured)) if configured else DEFAULT_MAX_DELIVERIES
        except (TypeError, ValueError):
            return DEFAULT_MAX_DELIVERIES

    def _retry_counter_key(self, execution_id: str) -> str:
        """节点重试计数键：``{ns}:execution:noderetry:{id}``（租户路由，与租约键同规则）。"""
        return f"{tenant_namespace(current_tenant())}:execution:noderetry:{execution_id}"

    def _read_node_retry_counter(self, execution_id: str) -> int:
        """读重试计数；无 redis 客户端 / 键不存在 / 读取异常 → 0（按未重试过）。"""
        redis_client = getattr(self, "redis_client", None)
        if redis_client is None or not hasattr(redis_client, "get"):
            return 0
        try:
            raw = redis_client.get(self._retry_counter_key(execution_id))
        except Exception as e:  # noqa: BLE001 — 瞬断按 0 处理（保守放行死信重入队）
            logger.warning("读取节点重试计数失败（按 0 处理）: %s: %s", execution_id, e)
            return 0
        if raw is None:
            return 0
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 0

    def _record_node_retry(self, execution_id: str) -> int:
        """重试放行前自增计数键（INCR + 滑动 7 天 EX），返回自增后的值。

        预算判定**不**用消息 delivery_count：核实发现队列在 reclaim 路径
        上报的是 XCLAIM **前**的 times_delivered（task_queue.py:402，与该处
        注释 "count + 1" 相悖）、fresh 读取恒报 1，且达限消息在队列层就地
        死信、根本不会进 handler（task_queue.py:404）——按 delivery_count
        判预算永远不会触发，只会造成「死信守卫重入队 → delivery 归 1 →
        再耗尽 → 再重入队」的无限循环。计数键在 worker 侧自增，语义是
        「本执行累计已放行的节点重试次数」，预算耗尽即终态化。

        无 redis 客户端（内存 worker / 单测）→ 返回 1 不设上限：预算由
        调用方/重投机制兜底（直连调用没有 run() 循环，异常直接冒给调用方）。
        """
        redis_client = getattr(self, "redis_client", None)
        if redis_client is None or not hasattr(redis_client, "incr"):
            return 1
        key = self._retry_counter_key(execution_id)
        try:
            pipe = redis_client.pipeline(transaction=True)
            pipe.incr(key)
            pipe.expire(key, self.NODE_RETRY_COUNTER_TTL_SECONDS)
            return int(pipe.execute()[0])
        except Exception as e:  # noqa: BLE001 — 计数失败按 1 处理，不阻断重试放行
            logger.warning("节点重试计数自增失败（按 1 处理）: %s: %s", execution_id, e)
            return 1

    def _reset_node_retry_counter(self, execution_id: str) -> None:
        """清零节点重试计数键（DEL；键不存在为幂等 no-op）。

        两个调用时机（rebase 组合语义，2026-10 二波 vs G1 43828aa）：

        - G1 retry 唤醒放行时：预算耗尽终态化的执行经人工 retry 唤醒后拿
          全新预算——否则唤醒的执行第一次失败就立刻再耗尽，G1 形同虚设；
        - 任一节点成功推进后：计数语义是「当前节点的**连续**失败次数」，
          不同节点的失败不共享预算——节点 A 抖一次花掉的预算不应让之后
          节点 B 的第一次失败就少一次重试机会。
        """
        redis_client = getattr(self, "redis_client", None)
        if redis_client is None or not hasattr(redis_client, "delete"):
            return
        try:
            redis_client.delete(self._retry_counter_key(execution_id))
        except Exception as e:  # noqa: BLE001 — 清零失败无害（下次失败继续累计）
            logger.warning("节点重试计数清零失败（忽略）: %s: %s", execution_id, e)

    def _node_failure_retry_decision(
        self,
        exc: Exception,
        execution_id: str,
        delivery_count: Optional[int],
    ) -> Optional[NodeExecutionRetryableError]:
        """节点失败重试决策：返回 NodeExecutionRetryableError（调用方 raise，
        状态不终态化）或 None（按现状终态化 error）。

        - 回滚开关 / 判据不符 → None（现状）；
        - 重试计数达预算 → None（现状终态化；error 里带重试次数供观测）；
        - 其余 → 计数自增后返回重试异常。
        """
        if _node_retry_disabled():
            return None
        if not isinstance(execution_id, str) or not execution_id:
            return None
        if not _is_retryable_node_failure(exc):
            return None
        attempt = self._record_node_retry(execution_id)
        if attempt >= self._node_retry_budget():
            logger.error(
                "执行 %s 节点重试预算耗尽（%s/%s），终态化 error: %s",
                execution_id, attempt, self._node_retry_budget(), exc,
            )
            return None
        logger.warning(
            "执行 %s 节点执行失败，不终态化等待消息重投后重跑失败节点"
            "（第 %s/%s 次重试，重投间隔=claim_min_idle_ms）: %s",
            execution_id, attempt, self._node_retry_budget(), exc,
        )
        return NodeExecutionRetryableError(
            f"节点执行失败（可重试，第 {attempt}/{self._node_retry_budget()} 次重试，"
            f"execution_id={execution_id}）: {exc}",
            execution_id=execution_id,
            attempt=attempt,
            delivery_count=delivery_count,
        )

    # ---- 取消检查点（波次①，设计稿 §3.1 选型 B：独立取消标志键）----

    # 取消标志键 TTL：7 天自清理（设计稿 §3.1；复用 event_filter 去重键的
    # 同类「带 TTL 意图键」模式）。BFF 侧写入（executions.py._cancel_key）。
    CANCEL_FLAG_TTL_SECONDS = 7 * 86400

    def _cancel_flag_key(self, execution_id: str) -> str:
        """取消标志键：``{ns}:execution:cancel:{id}``（租户路由，与 _exec_key 同规则）。"""
        return f"{tenant_namespace(current_tenant())}:execution:cancel:{execution_id}"

    def _cancel_requested(self, execution_id: str) -> bool:
        """步间取消检查：控制面写的意图标志键是否存在。

        - 无 redis 客户端（内存 worker / 单测派生类）或查询异常 → 容错降级
          视为未取消（设计稿 §3.5 兼容红线）；
        - ``PLAITA_DISABLE_CANCEL_CHECKPOINT=1``（波次①回滚开关）→ 跳过检查。
        """
        if _cancel_checkpoint_disabled():
            return False
        redis_client = getattr(self, "redis_client", None)
        if redis_client is None or not hasattr(redis_client, "exists"):
            return False
        try:
            return bool(redis_client.exists(self._cancel_flag_key(execution_id)))
        except Exception as e:
            logger.warning(
                "取消标志检查失败（视为未取消，下个步界重试）: %s: %s", execution_id, e
            )
            return False

    # ---- 取消监听（波次③：步内可中断）----
    #
    # 现状缺口：取消标志键只在**步界**（start/resume 入口、步循环顶）被检查，
    # 一个节点一旦跑起来就打断不了——agentrun 默认 timeout_secs=1800，取消后
    # 最多白等 30 分钟。本监听线程在**节点执行期间**轮询同一标志键，命中即
    # ``execution.cancel()``（同时置位 cancel_event/cancel_requested）：
    #   - code 沙箱等待循环（code._popen_wait_cancellable）当场 killpg 进程树；
    #   - 引擎在**下一个节点入口**抛 FlowCancelledException 拒绝继续；
    #   - 若节点自身协作（agentrun 等）消费 cancel_event，则连在途进程一起中止。
    # 语义与 §3.3 一致：默认软中断，协作节点即时击杀。
    #
    # 基类持有登记表/停止位与轮询逻辑（start/resume/步循环都在基类）；线程
    # 启动由 RedisFlowWorker.run() 触发——无 redis 客户端的内存 worker 登记
    # 为空转（``_cancel_requested`` 恒 False），永不误中。

    def _register_cancel_watch(self, execution_id: str, execution: Any) -> None:
        """登记活跃执行：取消监听线程据此轮询标志键并中止在途节点。"""
        with self._cancel_watch_lock:
            self._cancel_watch[execution_id] = (execution, current_tenant())

    def _unregister_cancel_watch(self, execution_id: str) -> None:
        with self._cancel_watch_lock:
            self._cancel_watch.pop(execution_id, None)

    def _cancel_poll_interval(self) -> float:
        if self._cancel_poll_seconds is not None:
            return max(0.05, float(self._cancel_poll_seconds))
        return 1.0

    def _start_cancel_watcher(self) -> None:
        """启动取消监听线程（RedisFlowWorker.run() 调用）。

        ``PLAITA_DISABLE_CANCEL_INTERRUPT=1``（波次③回滚开关）不启动——回到
        「只在步界检查取消标志键」的现状语义。无 redis 客户端（内存 worker）
        不启动（无标志键可读）。
        """
        if _cancel_interrupt_disabled():
            logger.info("取消监听已禁用（PLAITA_DISABLE_CANCEL_INTERRUPT=1）")
            return
        if getattr(self, "redis_client", None) is None:
            return
        if self._cancel_thread is not None and self._cancel_thread.is_alive():
            return
        self._cancel_stop.clear()
        self._cancel_thread = threading.Thread(
            target=self._cancel_watch_loop,
            name="plaita-cancel-watcher",
            daemon=True,
        )
        self._cancel_thread.start()

    def _stop_cancel_watcher(self) -> None:
        self._cancel_stop.set()
        watcher = self._cancel_thread
        if (
            watcher is not None
            and watcher is not threading.current_thread()
            and watcher.is_alive()
        ):
            watcher.join(timeout=2.0)
        self._cancel_thread = None

    def _cancel_watch_loop(self) -> None:
        interval = self._cancel_poll_interval()
        logger.info("取消监听已启动（轮询间隔 %.2fs）", interval)
        # 首次立即轮询一次，再按间隔等待——缩短「进入长跑节点」到「被中止」
        # 的最坏延迟（最长 ≈ interval + 一次 Redis 往返）。
        self._cancel_poll_safely()
        while not self._cancel_stop.wait(interval):
            self._cancel_poll_safely()

    def _cancel_poll_safely(self) -> None:
        try:
            self._cancel_poll_once()
        except Exception:  # noqa: BLE001 — 监听线程自身绝不能带崩 worker
            logger.error("取消监听周期异常", exc_info=True)

    def _cancel_poll_once(self) -> None:
        """对全部活跃执行轮询一次取消标志键，命中即中止在途执行。"""
        with self._cancel_watch_lock:
            entries = list(self._cancel_watch.items())
        for execution_id, (execution, tenant_id) in entries:
            token = set_current_tenant(tenant_id)
            try:
                requested = self._cancel_requested(execution_id)
            finally:
                reset_current_tenant(token)
            if not requested:
                continue
            # 命中：撤登记（幂等——重复轮询不再对同一执行重复 cancel），
            # 调 execution.cancel() 置位双 Event，在途协作节点当场收手。
            self._unregister_cancel_watch(execution_id)
            cancel = getattr(execution, "cancel", None)
            if callable(cancel):
                try:
                    cancel()
                except Exception:  # noqa: BLE001 — cancel 失败仍有步界兜底
                    logger.warning(
                        "取消监听调 execution.cancel() 失败: %s",
                        execution_id, exc_info=True,
                    )
            logger.info("取消监听命中标志键，已请求中止在途节点: %s", execution_id)

    # ---- 租约看门狗挂钩（波次②；基类 no-op，RedisFlowWorker 覆写）----

    def _acquire_lease(self, execution_id: str, holder: str) -> Tuple[Optional[str], Optional[int]]:
        """取得执行租约，返回 ``(lease_value, fence_token)``。

        fencing 开启且 lease 实现支持时走世代号 acquire（execution_lease.
        try_acquire_fenced）：租约值变为 ``{holder}:{gen}``，renew/release 与
        fenced storage CAS 均以完整 value 串比较。回滚开关
        ``PLAITA_DISABLE_FENCING=1`` 或 lease 为 NullExecutionLease（内存/
        单测）时退回 SET NX，fence_token=None（设计稿 §4.2 / §6 波次②回滚）。
        lease_value 为 None 表示 acquire 失败（已被他人持有）。
        """
        if not _fencing_disabled():
            fenced = getattr(self.execution_lease, "try_acquire_fenced", None)
            if callable(fenced):
                generation = fenced(execution_id, holder, self.lease_ttl_seconds)
                if generation is not None:
                    return f"{holder}:{generation}", generation
                return None, None
        acquired = self.execution_lease.try_acquire(
            execution_id, holder, self.lease_ttl_seconds
        )
        return (holder if acquired else None), None

    def _register_lease_watch(
        self, execution_id: str, lease_value: str, execution: Any
    ) -> None:
        """登记活跃执行供看门狗续租（基类 no-op）。"""

    def _unregister_lease_watch(self, execution_id: str, lease_value: str) -> None:
        """注销看门狗登记（基类 no-op）。"""

    def _raise_if_lease_lost(self, execution_id: Optional[str]) -> None:
        """看门狗已标记失租则抛 ExecutionLeaseError（基类 no-op）。"""

    def _renew_lease_if_held(self, lease_execution_id: Optional[str], lease_holder: Optional[str]) -> None:
        if not lease_execution_id or not lease_holder:
            return
        # 看门狗已判死（含注入式 renew 失败）：步界立即自爆，不再续期
        self._raise_if_lease_lost(lease_execution_id)
        if not self.execution_lease.renew(lease_execution_id, lease_holder, self.lease_ttl_seconds):
            raise ExecutionLeaseError(
                f"lost lease for execution {lease_execution_id}; aborting resume"
            )

    def _persist_state_or_raise(self, execution_id: str, state: ExecutionState, phase: str) -> None:
        """落盘执行状态并检查返回值：False → 抛 ``StatePersistError``。

        ``save_execution_state`` 的契约是吞掉一切异常返回 False（Redis 后端
        网络瞬断/序列化失败均如此）。start 路径历史上检查过返回值，但
        终态/挂起/取消/步进/错误态保存与 resume 两条路径此前不检查——
        is_end 落盘失败 → break → 正常返回 → 消息被 ack，节点副作用已发生
        而执行状态永远停在 running/suspended（僵尸执行）。统一在此收口：
        失败即抛，消息不 ack，走 at-least-once 重投。

        注意：fenced 世代失配是 storage 层 raise ``ExecutionLeaseError``、
        不经本方法的 False 路径——既有失租链路不受影响。
        """
        saved = self.execution_storage.save_execution_state(execution_id, state)
        if not saved:
            logger.error(
                "保存执行状态失败 (%s): execution_id=%s, status=%s——消息将不 ack 等待重投",
                phase,
                execution_id,
                getattr(state, "status", "?"),
            )
            raise StatePersistError(
                f"保存执行状态失败 ({phase}): execution_id={execution_id}, "
                f"status={getattr(state, 'status', '?')}"
            )
    
    def get_flow_definition(self, flow_id: str, version: Optional[str] = None) -> Flow:
        """
        获取流程定义，支持按ID和版本获取
        
        Args:
            flow_id: 流程ID
            version: 流程版本，如果不指定则获取最新版本
            
        Returns:
            Dict[str, Any]: 流程定义
            
        Raises:
            ValueError: 如果找不到流程定义或版本不匹配
        """
        # 生成缓存键（含租户段：同名流程可共存于不同租户 namespace）
        cache_key = f"{current_tenant()}:{flow_id}:{version or 'latest'}"
        
        # 尝试从缓存获取
        if cache_key in self.flow_definition_cache:
            logger.info("从缓存获取流程定义: %s", cache_key)
            return self.flow_definition_cache[cache_key]
        
        # 缓存未命中，从存储获取
        logger.info("从存储获取流程定义: %s, 版本: %s", flow_id, version or 'latest')
        flow_definition = self.flow_storage.get_flow(flow_id, version)
        
        if not flow_definition:
            # Delegate diagnostic details to the storage layer via an optional
            # diagnose() method — FlowWorker must not peek at storage internals
            # (e.g. Redis keys / namespaces) directly.
            if hasattr(self.flow_storage, "diagnose_missing_flow"):
                try:
                    self.flow_storage.diagnose_missing_flow(flow_id)
                except Exception:
                    logger.warning("flow_storage.diagnose_missing_flow(%s) failed", flow_id, exc_info=True)

            error_msg = f"找不到流程定义: {flow_id}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # 解析流程定义
        try:
            logger.info("成功获取流程定义: %s, 数据: %s", flow_id, flow_definition.get('name', 'unknown'))
            flow = Flow.model_validate(flow_definition)
        except Exception as e:
            error_msg = f"解析流程定义失败: {e}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        self.flow_definition_cache[cache_key] = flow
        
        return flow
    
    # ---- flow 定义指纹（波次二任务②）----

    def _compute_flow_hash(self, flow: Flow) -> str:
        """对实际加载执行的 Flow 计算稳定指纹（sha256 of 规范化 JSON dump）。

        用 ``model_dump(mode="json")`` + ``sort_keys`` 保证跨进程确定性（同一
        pydantic 版本下字段序稳定，sort_keys 再兜底 dict 序）。指纹在 start
        时写入 ExecutionState.flow_hash，resume 时不一致即拒绝续跑。
        """
        payload = json.dumps(
            flow.model_dump(mode="json"),
            sort_keys=True,
            ensure_ascii=False,
            default=str,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    # ---- start 幂等键（波次二任务③）----

    # start 去重映射键 TTL：7 天自清理（与取消标志/重试计数键同款模式）。
    START_DEDUP_TTL_SECONDS = 7 * 86400

    def _start_dedup_key(self, dedup_key: str) -> str:
        """start 去重映射键：``{ns}:start-dedup:{key}``（租户路由，与租约键同规则）。"""
        return f"{tenant_namespace(current_tenant())}:start-dedup:{dedup_key}"

    def _claim_start_dedup(self, dedup_key: Optional[str], execution_id: str) -> Tuple[str, Optional[str]]:
        """原子认领 start 幂等键（SET NX + EX 7 天）。

        返回 ``(action, mapped_execution_id)``：

        - ``("claimed", None)``：认领成功，本消息是这条逻辑 start 的首次执行；
        - ``("hit", mapped)``：键已存在（重投/双开），mapped 为首次执行映射的
          execution_id，调用方**绝不二次 start**；mapped 为 None 表示键刚好
          过期/被清（罕见竞态），调用方按孤儿映射处理；
        - ``("inactive", None)``：未提供 dedup_key、无 redis 客户端或认领
          瞬断——按未启用处理，行为与存量完全一致。

        映射在 ``run_distributed`` **之前**认领（execution_id 在
        ``FlowExecution``/``ExecutionContext`` 构造时即生成，core/context.py:220），
        首节点副作用受保护；映射值指向的执行状态此后由 ``start_flow`` 正常落盘。
        """
        redis_client = getattr(self, "redis_client", None)
        if not dedup_key or redis_client is None or not hasattr(redis_client, "set"):
            return ("inactive", None)
        key = self._start_dedup_key(dedup_key)
        try:
            claimed = redis_client.set(
                key, execution_id, ex=self.START_DEDUP_TTL_SECONDS, nx=True
            )
        except Exception as e:  # noqa: BLE001 — 认领瞬断：不阻塞启动（退化为存量行为）
            logger.warning("start 幂等键认领失败（按未启用处理）: %s: %s", key, e)
            return ("inactive", None)
        if claimed:
            return ("claimed", None)
        try:
            raw = redis_client.get(key)
        except Exception as e:  # noqa: BLE001 — 读取瞬断同上
            logger.warning("start 幂等键读取失败（按孤儿映射处理）: %s: %s", key, e)
            return ("hit", None)
        mapped = raw.decode() if isinstance(raw, bytes) else raw
        return ("hit", mapped or None)

    def _release_start_dedup(self, dedup_key: str, execution_id: str) -> None:
        """删除孤儿映射（映射指向的执行状态从未落盘）。GET==id 才 DEL，
        防误删并发的后来者认领；比较与删除间的微小竞窗最多导致重复启动，
        与存量 at-least-once 语义一致。"""
        redis_client = getattr(self, "redis_client", None)
        if redis_client is None or not hasattr(redis_client, "get"):
            return
        key = self._start_dedup_key(dedup_key)
        try:
            raw = redis_client.get(key)
            current = raw.decode() if isinstance(raw, bytes) else raw
            if current == execution_id:
                redis_client.delete(key)
        except Exception as e:  # noqa: BLE001 — 释放失败无害（键 7 天自动过期）
            logger.warning("start 幂等键孤儿释放失败（忽略）: %s: %s", key, e)

    def _handle_start_dedup_hit(
        self, flow_id: str, mapped_execution_id: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        """处理幂等键命中：绝不二次 start。

        - 映射缺失/指向的执行状态不存在（孤儿：认领后首次执行从未落盘，
          如 crash 在 claim 与 save 之间）→ 返回 None，调用方释放映射后按
          新启动继续（首节点可能重跑——首节点须幂等是既有文档约定）；
        - 已终态 → 返回 already 形状（镜像 resume 终态短路）；
        - 非终态 running（start 消息处理中 worker 崩溃/任务①重试搁浅）→
          **重入队一份 resume 消息**把执行接续起来再 ack start（若只返回
          现状形状，执行将无人推进永久卡 running）；入队失败抛
          ServiceDispatchError 让消息重投再来；
        - 非终态 suspended → 只返回现状形状不重入队：挂起执行自有
          delay/approval 服务的 resume 链路，重入队 continue 会被
          ``_handle_resume`` 的 pending 校验拒绝（ResumeError）。
        """
        if not mapped_execution_id:
            return None
        state = self.execution_storage.load_execution_state(mapped_execution_id)
        if state is None:
            logger.warning(
                "start 幂等键命中但映射的执行无状态（孤儿映射）: %s",
                mapped_execution_id,
            )
            return None
        status = getattr(state, "status", "") or ""
        if status in ("completed", "error", "cancelled"):
            logger.info(
                "start 幂等键命中：执行 %s 已终态 (%s)，不再二次启动",
                mapped_execution_id, status,
            )
            return {
                "execution_id": mapped_execution_id,
                "status": status,
                "already_terminal": True,
                "deduplicated": True,
                "result": getattr(state, "result", None),
                "error": getattr(state, "error", None),
            }
        if status == "running":
            # 崩溃/重试搁浅的 start：重入队 resume 接续执行（lease 保证串行）
            redis_client = getattr(self, "redis_client", None)
            queue_name = getattr(self, "queue_name", None)
            if redis_client is not None and queue_name:
                resume_msg = {
                    "type": "resume",
                    "flow_id": flow_id,
                    "execution_id": mapped_execution_id,
                    "resume_type": "continue",
                    "tenant_id": current_tenant(),
                    "timestamp": datetime.now().isoformat(),
                }
                try:
                    enqueue_task(redis_client, queue_name, resume_msg)
                except Exception as e:  # noqa: BLE001 — 入队失败让 start 消息重投
                    raise ServiceDispatchError(
                        f"start 幂等命中后重入队 resume 失败 "
                        f"(execution_id={mapped_execution_id}): {e}"
                    ) from e
                logger.info(
                    "start 幂等键命中：执行 %s 仍在 running，已重入队 resume 接续",
                    mapped_execution_id,
                )
                return {
                    "execution_id": mapped_execution_id,
                    "status": status,
                    "deduplicated": True,
                    "resume_requeued": True,
                }
            logger.warning(
                "start 幂等键命中：执行 %s 仍在 running，但无队列可重入队 resume"
                "（内存 worker），执行可能停滞", mapped_execution_id,
            )
        # suspended / 无队列的 running：只回报现状（绝不二次 start）
        return {
            "execution_id": mapped_execution_id,
            "status": status,
            "deduplicated": True,
        }

    def _finalize_observers(self) -> None:
        """run 终结（completed/failed）时收尾观测回调。

        distributed 模式下内核不发 on_flow_end（is_end 由宿主循环判定）。
        观测回调实现 finalize()（如 LangfuseCallback：收口根 span + flush）
        则终态调用之，仅有 flush() 的退化为直接 flush。
        """
        for handler in self.callback_handlers:
            finalizer = getattr(handler, "finalize", None)
            flusher = getattr(handler, "flush", None)
            try:
                if callable(finalizer):
                    finalizer()
                elif callable(flusher):
                    flusher()
            except Exception:
                logger.warning("观测回调终态收尾失败: %r", handler, exc_info=True)

    def _bind_observers(self, execution) -> None:
        """把执行实例通知给支持 ``bind_execution`` 的观测回调（plaita.obs）。

        LangfuseCallback 等观测回调以此拿到运行时 ``$EXECUTION_ID`` 作跨进程
        trace id；绑定同时重置其 run 状态，防止上一执行的 trace 串入。绑定
        失败只告警，不影响执行。
        """
        for handler in self.callback_handlers:
            binder = getattr(handler, "bind_execution", None)
            if callable(binder):
                try:
                    binder(execution)
                except Exception:
                    logger.warning("观测回调 bind_execution 失败: %r", handler, exc_info=True)

    def start_flow(self, flow_id: str, params: Dict[str, Any], version: Optional[str] = None,
                   execution_id: Optional[str] = None,
                   dedup_key: Optional[str] = None,
                   delivery_count: Optional[int] = None) -> Dict[str, Any]:
        """
        启动流程执行

        Args:
            flow_id: 流程ID
            params: 流程输入参数
            version: 流程版本，如果不指定则使用最新版本
            execution_id: 预铸 id（BFF start 时铸造、随消息透传）——提交方
                即刻拿 id 返给调用方，无需等 worker 消费；缺省就地铸造
            dedup_key: 可选 start 幂等键（消息重投/双开收敛；未提供则行为
                与存量完全一致，见 ``_claim_start_dedup``）
            delivery_count: 消息投递次数（可观测；见 _process_execution_result）

        Returns:
            Dict[str, Any]: 流程执行结果
        """
        # 获取流程定义
        flow = self.get_flow_definition(flow_id, version)
        
        
        
        logger.info("开始执行流程: %s, 版本: %s", flow_id, version or '最新版本')
        
        try:
            # 创建流程执行器并执行流程
            # 复用同一个 FlowExecution 贯穿所有分布式步骤, 保留用户回调
            execution = FlowExecution(
                event_bus=self.event_bus,
                callback_handlers=self.callback_handlers,
            )
            execution.mode = ExecutionMode.DISTRIBUTED
            self._bind_observers(execution)

            # P0 可见性修复（keeper 迁移设计稿 §5.5 清单①，43828aa）：
            # execution_id 优先吃 BFF 预铸（随消息透传，提交方即刻可轮询），
            # 否则就地铸造；行 id 与 result.execution_id 天然一致。异常遗留
            # 的 running 行是 zombie，交 reaper/心跳年龄判定处置。
            execution_id = execution_id or uuid.uuid4().hex

            # start 幂等键（波次二任务③，与 G1 预铸 id 组合）：在先行落行
            # 之前认领，映射值=预铸或就地铸造的 execution_id——重投/双开
            # 在**任何新行落盘之前**即被拦截，首节点副作用由此受重投保护。
            dedup_action, mapped_execution_id = self._claim_start_dedup(
                dedup_key, execution_id
            )
            if dedup_action == "hit":
                hit_result = self._handle_start_dedup_hit(flow_id, mapped_execution_id)
                if hit_result is not None:
                    return hit_result
                # 孤儿映射（首次执行从未落盘）：释放后重新认领，让本次启动
                # 继续受保护；并发后来者抢先认领则放弃设防（存量语义）
                if dedup_key:
                    self._release_start_dedup(dedup_key, mapped_execution_id)
                    dedup_action, mapped_execution_id = self._claim_start_dedup(
                        dedup_key, execution_id
                    )
                    if dedup_action == "hit":
                        logger.warning(
                            "start 幂等键孤儿释放后被并发认领，本次启动不设防: %s",
                            dedup_key,
                        )

            # 先落 running 行再执行（P0 可见性）：首节点期间 /api/executions
            # 查得到此行、cancel 有锚。flow_hash：对实际加载执行的 Flow 计算
            # 指纹（波次二任务②）随行落盘——resume 时与当前定义比对，防运行
            # 中改定义后续跑走错分支。
            state = ExecutionState(
                execution_id=execution_id,
                flow_id=flow_id,
                flow_version=version,
                flow_hash=self._compute_flow_hash(flow),
                tenant_id=current_tenant(),
                context={},
                status="running",
                start_time=datetime.now().isoformat(),
                invoker="worker"
            )
            self._persist_state_or_raise(execution_id, state, "start")

            # start 入口取消检查点（与 resume 入口对称）：BFF cancel 可发生在
            # 「已入队、未消费」窗口，更关键的是 start 消息 at-least-once 重投
            # 时（在途失败→未 ack→重投）同 id 重跑整条 flow——不查标志则
            # 已取消意图永远等不到步界（E2E drill 3 实证：超时 flow 空转
            # N×900s）。命中即终态化，不再执行。
            if self._cancel_requested(execution_id):
                state.status = "cancelled"
                state.end_time = datetime.now().isoformat()
                self._persist_state_or_raise(execution_id, state, "cancelled_at_start_entry")
                self._finalize_observers()
                logger.info("执行 %s 在 start 入口命中取消标志，终态化", execution_id)
                return {
                    "execution_id": execution_id,
                    "status": "cancelled",
                    "cancelled_at_start": True,
                }

            # 执行流程，获取初始结果
            # 租约（A′，2026-10-06）：start 路径此前**不持租约**——resume 有、
            # start 无。三后果实证（plaita#41 多机验证）：
            # ①多 worker 抢到同一 start 消息时无闸可拦，双方同时跑同 id（双跑）；
            # ②死信守卫按「租约是否被持有」判活，start 执行恒"租约空"→ 长步
            #   误判持有者已死 → 误死信 + 重入队，长任务被反复打断；
            # ③抢占者白烧 5 次 delivery 配额。
            # 对齐 resume：acquire → 看门狗续租 → finally 释放。acquire 失败
            # 即抛 ExecutionLeaseError（run() 走不 ack/重投语义），抢占者礼貌退出。
            holder = new_holder_token(prefix="start")
            lease_value, fence_token = self._acquire_lease(execution_id, holder)
            if lease_value is None:
                raise ExecutionLeaseError(
                    f"execution {execution_id} is leased by another worker; "
                    "refuse concurrent start"
                )
            fence_token_reset = None
            if fence_token is not None:
                fence_token_reset = set_current_fence_token(fence_token)

            # 取消监听（波次③）：登记整个推进窗口——取消标志键命中即在途节点被
            # execution.cancel() 中止（code 沙箱当场 killpg；协作节点如 agentrun
            # 消费 cancel_event 收手）。租约看门狗同窗口。
            self._register_cancel_watch(execution_id, execution)
            self._register_lease_watch(execution_id, lease_value, execution)
            try:
                result = execution.run_distributed(flow, params=params, execution_id=execution_id)

                # 处理执行结果
                final_result = self._process_execution_result(
                    flow, result, state, execution, delivery_count=delivery_count,
                    lease_execution_id=execution_id,
                )

                return final_result
            finally:
                self._unregister_cancel_watch(execution_id)
                self._unregister_lease_watch(execution_id, lease_value)
                if fence_token_reset is not None:
                    reset_current_fence_token(fence_token_reset)
                # release 对整个租约 value 串 compare（fencing 档 = {holder}:{gen}）
                self.execution_lease.release(execution_id, lease_value)

        except (NodeExecutionRetryableError, ServiceDispatchError, ExecutionLeaseError):
            # 节点级可重试失败（波次二任务①）/ 幂等命中后重入队 resume 失败
            # （波次二任务③）/ **租约被他人持有**（A′，2026-10-06）：原样上抛给
            # run() 按不 ack 语义处理——ExecutionLeaseError 尤其关键：run() 的
            # `except ExecutionLeaseError` 分支据此 note_lease_conflict 并把消息
            # 留在 pending 等租约过期后 reclaim；若被下方通用 except 包成
            # RuntimeError，run() 会误 ack 消息 → 抢占者把别人的活执行 ack 掉，
            # 且真实持有者崩溃后无人 reclaim（执行永久失联）。绝不能包。
            raise

        except Exception as e:
            logger.error("执行流程出错: %s", e, exc_info=True)
            # 波次③步内取消：start 路径（无租约）首个节点在途命中取消监听 →
            # 引擎自 run_distributed 归一化抛出（挂 __cause__）或协作节点自杀
            # 抛普通异常。两条判据任一命中即终态化 cancelled，绝不包成
            # RuntimeError 让执行停在 running（取消是控制面意图）。
            if (
                _chain_has_cancellation(e) or self._cancel_requested(execution_id)
            ) and "state" in locals() and state is not None:
                state.status = "cancelled"
                state.end_time = datetime.now().isoformat()
                self._persist_state_or_raise(execution_id, state, "cancelled_in_start_node")
                self._finalize_observers()
                logger.info(
                    "执行 %s 在 start 首节点执行期响应取消，终态化 cancelled",
                    execution_id,
                )
                return {"execution_id": execution_id, "status": "cancelled"}
            self._finalize_observers()
            raise RuntimeError(f"执行流程出错: {e}")

    def resume_flow(self, flow_id: str, execution_id: str, resume_type: str,
                    data: Optional[Dict[str, Any]] = None,
                    delivery_count: Optional[int] = None) -> Dict[str, Any]:
        """
        恢复流程执行。

        取得 ``execution_id`` 租约后才推进；另一 worker 已持有租约时抛
        ``ExecutionLeaseError``（RedisFlowWorker 对此**不** XACK，待租约过期后回收）。

        Args:
            flow_id: 流程ID
            execution_id: 执行ID
            resume_type: 恢复类型
            data: 恢复数据
            delivery_count: 消息投递次数（可观测；见 _process_execution_result）
        """
        # 加载执行状态。None 仅表示键不存在——Redis 后端的读取瞬断/反序列化
        # 失败现以上抛 ExecutionStateLoadError，由 run() 的重投递路径处理，
        # 不再被吞成 None 触发 poison ack（ReviewFix D1）。
        state = self.execution_storage.load_execution_state(execution_id)
        if not state:
            error_msg = f"找不到执行状态: {execution_id}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # 验证流程ID是否匹配
        stored_flow_id = state.flow_id
        if stored_flow_id and stored_flow_id != flow_id:
            error_msg = f"流程ID不匹配: 期望 {flow_id}, 实际 {stored_flow_id}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # 终态短路（2026-09 分布式评审 P1-1）：at-least-once 下重复投递的
        # resume 任务会命中已完成的执行。历史实现走完正常 resume 再在异常
        # 处理里把终态改写成 error——监控按 status 查询会得出错误结论。
        # 幂等语义：已终态的执行直接原样返回，不再推进。
        # cancelled 同为终态（console cancel 端点先写 cancelled 再投
        # resume_type=cancel 消息；若此处不放行，挂起执行会被 on_cancel
        # 续跑到 end 翻成 completed、非挂起执行被 ResumeError 翻成 error
        # ——「已取消」跳回「已完成/失败」。E2E cancel 回归用例钉住此语义）。
        # G1 例外：error + resume_type=retry 放行——error 不再是绝对终态，
        # 从断点（上一成功节点的后继）步进重跑失败节点（keeper 迁移设计稿
        # §G1，验收=失败节点恰重跑一次、已完成不重放）。
        state_status = getattr(state, "status", "") or ""
        retry_wakeup = state_status == "error" and \
            ResumeType.coerce(resume_type) is ResumeType.RETRY
        if state_status in ("completed", "error", "cancelled") and not retry_wakeup:
            logger.info(
                "执行 %s 已是终态 (%s)，跳过重复 resume", execution_id, state_status,
            )
            return {
                "execution_id": execution_id,
                "status": state_status,
                "already_terminal": True,
                "result": getattr(state, "result", None),
                "error": getattr(state, "error", None),
            }
        
        # 获取流程版本
        version = state.flow_version

        # 获取流程定义
        flow = self.get_flow_definition(flow_id, version)

        # flow 定义指纹（波次二任务②）：worker 的定义 TTLCache 有 300s 窗口、
        # console engine_sync 可直接覆盖 Redis 定义——挂起执行 resume 用
        # latest 版本定义时，若定义自启动后被改过，``_get_next_from_last``
        # 找不到节点/走错分支且无任何告警。指纹在 lease 内判定（与活
        # worker 的推进写串行化，防「他人持租约推进中却被误终态化」），
        # 不一致 → 终态化 error + raise ValueError（poison ack）——意图是
        # 可观测的终态而不是重投风暴。老状态 flow_hash 为 None → 跳过校验
        # （零回归）。
        current_flow_hash = self._compute_flow_hash(flow)

        # 解析流程定义
        logger.info("恢复流程执行: %s, 执行ID: %s, 恢复类型: %s", flow_id, execution_id, resume_type)

        holder = new_holder_token(prefix="resume")
        lease_value, fence_token = self._acquire_lease(execution_id, holder)
        if lease_value is None:
            raise ExecutionLeaseError(
                f"execution {execution_id} is leased by another worker; refuse concurrent resume"
            )

        fence_token_reset = None
        if fence_token is not None:
            # fencing 写侧守门（设计稿 §4.2）：本 resume 的所有状态落盘带
            # 世代号，FencedExecutionStorage 据此 CAS；finally 复位。
            fence_token_reset = set_current_fence_token(fence_token)

        try:
            # XCLAIM 恢复路径取消检查点（设计稿 §3.1）：crash 后 cancel 消息
            # 或任一 resume 消息被重投，若取消标志在（控制面对运行中执行写的
            # 意图键），直接终态化而非继续推进。上一 checkpoint 已落盘，
            # cancelled 的 context 即取消点前一步的完整快照（§3.3）。
            if self._cancel_requested(execution_id):
                state.status = "cancelled"
                state.end_time = datetime.now().isoformat()
                self._persist_state_or_raise(execution_id, state, "cancelled_at_resume_entry")
                logger.info("执行 %s 在 resume 入口命中取消标志，终态化", execution_id)
                return {
                    "execution_id": execution_id,
                    "status": "cancelled",
                    "cancelled_at_resume": True,
                }

            # flow 定义指纹校验（波次二任务②）在 G1 retry 唤醒**之前**：
            # 定义被改时执行保持 error（hash 不匹配信息落盘），修复定义后
            # 仍可再 retry——G1 的「error 态可反复唤醒」语义不被破坏。
            # 取消是用户意图，已在上方优先放行。写盘走 _persist_state_or_raise
            # （带本 resume 的 fence 世代），随后 raise FlowHashMismatchError
            # → run() 按 ValueError poison ack（终态已可观测，重投无意义）。
            stored_flow_hash = getattr(state, "flow_hash", None)
            if stored_flow_hash and stored_flow_hash != current_flow_hash:
                mismatch_msg = (
                    "flow 定义自启动后已变更，hash 不匹配，执行无法安全续跑 "
                    f"(execution_id={execution_id}, flow_id={flow_id}, "
                    f"stored_hash={stored_flow_hash[:12]}..., "
                    f"current_hash={current_flow_hash[:12]}...)"
                )
                logger.error(mismatch_msg)
                state.status = "error"
                state.error = {
                    "message": mismatch_msg,
                    "stored_flow_hash": stored_flow_hash,
                    "current_flow_hash": current_flow_hash,
                }
                state.end_time = datetime.now().isoformat()
                self._persist_state_or_raise(execution_id, state, "flow_hash_mismatch")
                raise FlowHashMismatchError(mismatch_msg)

            # G1 retry 唤醒：error → running 翻转并先落盘（观察者/监控立即
            # 可见；若本 worker 接着硬死，行是 running 而非 error——再一轮
            # retry 仍可放行，zombie 判定也不误报「终态」）。checkpoint 原样，
            # saved_context 由下方 run_distributed 携带。
            # 与任务①组合语义：唤醒即清零节点重试计数键——预算耗尽终态化的
            # 执行经人工 retry 唤醒后拿全新预算，否则唤醒的执行第一次失败就
            # 立刻再耗尽，G1 形同虚设。翻转落盘成功后才清零（落盘失败抛
            # StatePersistError 走重投，唤醒未生效不清预算）。
            if retry_wakeup:
                state.status = "running"
                state.error = None
                state.end_time = None
                self._persist_state_or_raise(execution_id, state, "retry_wakeup")
                self._reset_node_retry_counter(execution_id)
                logger.info("执行 %s error 态经 retry 放行（重试计数已清零），从断点步进", execution_id)

            # 复用同一个 FlowExecution 贯穿恢复后的所有分布式步骤
            execution = FlowExecution(
                event_bus=self.event_bus,
                callback_handlers=self.callback_handlers,
            )
            execution.mode = ExecutionMode.DISTRIBUTED
            self._bind_observers(execution)
            # 登记看门狗（波次②）：持租约期间每 TTL/3 续租，防长步 > TTL
            # 被 XCLAIM 抢占双跑
            self._register_lease_watch(execution_id, lease_value, execution)
            # 登记取消监听（波次③）：命中标志键即中止在途节点（与租约登记
            # 同窗口；finally 一并撤销）
            self._register_cancel_watch(execution_id, execution)

            # 直接使用 run_distributed 恢复执行
            result = execution.run_distributed(
                flow,
                saved_context=state.context,
                resume_type=resume_type,
                resume_data=data,
            )

            # 处理执行结果
            final_result = self._process_execution_result(
                flow,
                result,
                state,
                execution,
                lease_execution_id=execution_id,
                lease_holder=lease_value,
                delivery_count=delivery_count,
            )

            return final_result

        except ExecutionLeaseError:
            raise
        except (StatePersistError, ServiceDispatchError, NodeExecutionRetryableError):
            # 落盘/派发失败：执行状态保持原样（is_end 前失败 → 仍 running，
            # 挂起派发失败 → 仍 suspended），消息不 ack 走 at-least-once 重投，
            # 下轮从 checkpoint / 挂起节点重入。绝不落 error 终态——重投消息
            # 会命中上方 already_terminal 短路被 ack，执行永久卡死。
            # NodeExecutionRetryableError（波次二任务①）同理：节点失败未
            # 终态化，重投后从 checkpoint 重跑失败节点；finally 释放租约，
            # 重投消息可重新取得租约。
            raise
        except FlowHashMismatchError:
            # flow 定义指纹不匹配（波次二任务②）：执行已在 raise 前终态化
            # error（可观测终态），原样上抛给 run() 按 ValueError poison
            # ack——重投只会命中 already_terminal 短路，终态化已完成，无意义。
            # 不落进下面的通用 except 二次终态化/包 RuntimeError。
            raise
        except Exception as e:
            logger.error("恢复流程执行出错: %s", e, exc_info=True)

            # 波次③步内取消：被恢复的节点在途命中取消监听 → 引擎抛
            # FlowCancelledException（归一化后挂 __cause__）或协作节点自杀抛
            # 普通异常。两条判据任一命中即终态化 cancelled，绝不写 error。
            if _chain_has_cancellation(e) or self._cancel_requested(execution_id):
                state.status = "cancelled"
                state.end_time = datetime.now().isoformat()
                self._persist_state_or_raise(execution_id, state, "cancelled_in_node")
                self._finalize_observers()
                logger.info(
                    "执行 %s 在恢复节点执行期响应取消，终态化 cancelled", execution_id
                )
                return {
                    "execution_id": execution_id,
                    "status": "cancelled",
                }

            # 节点级有界重试（波次二任务①）：同 _process_execution_result，
            # 瞬态节点失败不终态化、消息等重投；否则现状终态化 error。
            retry_exc = self._node_failure_retry_decision(
                e, execution_id, delivery_count
            )
            if retry_exc is not None:
                raise retry_exc from e

            # 更新执行状态为错误
            state.status = "error"
            retries = max(0, self._read_node_retry_counter(execution_id) - 1)
            if retries > 0:
                state.error = {
                    "message": f"节点执行失败（重试 {retries} 次后仍失败）: {e}",
                    "node_retries": retries,
                }
            else:
                state.error = {"message": str(e)}
            state.end_time = datetime.now().isoformat()

            self._persist_state_or_raise(execution_id, state, "resume_error_handler")
            self._finalize_observers()

            raise RuntimeError(f"恢复流程执行出错: {e}")
        finally:
            self._unregister_lease_watch(execution_id, lease_value)
            self._unregister_cancel_watch(execution_id)
            if fence_token_reset is not None:
                reset_current_fence_token(fence_token_reset)
            # release 对整个租约 value 串 compare（fencing 档 = {holder}:{gen}）
            self.execution_lease.release(execution_id, lease_value)

    def _process_execution_result(
        self,
        flow: Flow,
        result: Dict[str, Any],
        state: ExecutionState,
        execution: Optional[FlowExecution] = None,
        lease_execution_id: Optional[str] = None,
        lease_holder: Optional[str] = None,
        delivery_count: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        处理流程执行结果

        Args:
            flow: 流程对象
            result: 执行结果
            state: 执行状态
            execution: 贯穿本执行全过程的 FlowExecution (复用以保留回调);
                为 None 时按需创建, 仅供向后兼容的简单调用场景使用
            lease_execution_id / lease_holder: resume 租约续期参数
            delivery_count: 消息投递次数（可观测/日志用；节点重试预算判定
                用重试计数键，见 ``_record_node_retry``）

        Returns:
            Dict[str, Any]: 最终的执行结果
        """

        # 初始化状态
        # 凭据可见性（升级演练 P2-1 + MIGRATION 轮换提醒）：expose_env 白名单
        # 命中的变量会随 checkpoint 持久化到 Redis——每个执行只告警一次。
        context_for_warn = result.get("context") or {}
        env_snapshot = context_for_warn.get("$ENV") if isinstance(context_for_warn, dict) else None
        if env_snapshot:
            logger.warning(
                "执行 %s 的 checkpoint 包含 $ENV 快照（%d 个变量，可能含密钥）。"
                "这些值会在 Redis 中存活至 TTL/删除——请在升级/退役窗口轮换凭据。",
                state.execution_id if state is not None else "?", len(env_snapshot),
            )
        # id 以「被加载记录的 execution_id」为准（升级演练 P2-2）：老 checkpoint
        # 的 context 可能缺 $EXECUTION_ID，信任 result 会把终态写到
        # plaita:execution: 空键，原记录永久停留 suspended。
        execution_id = result.get("execution_id") or (
            state.execution_id if state is not None else None
        )
        context = result.get("context")

        if execution is None:
            execution = FlowExecution(
                event_bus=self.event_bus,
                callback_handlers=self.callback_handlers,
            )
            execution.mode = ExecutionMode.DISTRIBUTED
            self._bind_observers(execution)

        steps_since_persist = 0

        while True:
            is_end = result.get("is_end", False)
            is_suspend = result.get("is_suspend", False)

            if is_end:
                # 落盘前失租检查（波次② §4.1）：看门狗已判死则不写状态，
                # 消息不 ack 走重投——防止与接管者双写终态
                self._raise_if_lease_lost(lease_execution_id)
                state.status = "completed"
                state.context = context
                state.end_time = datetime.now().isoformat()
                self._persist_state_or_raise(execution_id, state, "completed")
                self._finalize_observers()
                break
            elif is_suspend:
                self._raise_if_lease_lost(lease_execution_id)
                state.status = "suspended"
                state.context = context
                self._persist_state_or_raise(execution_id, state, "suspended")
                self._dispatch_service_task(result, context, execution_id)
                break
            else:
                # 取消检查点（波次① §3.1 选型 B）：控制面对运行中执行只写
                # 取消标志键不再直写 cancelled；worker 在步界消费意图——
                # 上一节点结果已随 PERSIST_EVERY_N_STEPS=1 落盘，此处
                # state.context 即取消点前一步的完整 checkpoint（§3.3）。
                # 默认语义：在途节点跑完（软中断），code 沙箱类由引擎层
                # cancel_event 即时击杀（§3.3，波次③）。
                if self._cancel_requested(execution_id):
                    self._raise_if_lease_lost(lease_execution_id)
                    state.status = "cancelled"
                    state.context = context
                    state.end_time = datetime.now().isoformat()
                    self._persist_state_or_raise(execution_id, state, "cancelled")
                    self._finalize_observers()
                    logger.info("执行 %s 已在步界响应取消（标志键命中），终态化", execution_id)
                    break
                state.status = "running"
                try:
                    self._renew_lease_if_held(lease_execution_id, lease_holder)
                    result = execution.run_distributed(
                        flow,
                        saved_context=context,
                        resume_type="continue",
                    )

                    context = result.get("context", context)
                    steps_since_persist += 1
                    # 重试计数按「当前节点的连续失败」计（组合语义）：任一
                    # 节点成功推进即清零——不同节点的失败不共享预算。
                    self._reset_node_retry_counter(execution_id)

                    is_end = result.get("is_end", False)
                    is_suspend = result.get("is_suspend", False)

                    if (
                        not is_end
                        and not is_suspend
                        and steps_since_persist >= self.PERSIST_EVERY_N_STEPS
                    ):
                        self._raise_if_lease_lost(lease_execution_id)
                        state.context = context
                        state.last_update_time = datetime.now().isoformat()
                        self._persist_state_or_raise(execution_id, state, "step_persist")
                        steps_since_persist = 0
                        logger.info("流程步骤执行完成，继续下一步: %s", execution_id)

                except ExecutionLeaseError:
                    raise
                except StatePersistError:
                    # 步进落盘失败必须原样上抛：若落进下面的通用 except，
                    # 会把可重投的瞬态失败改写成 error 终态（消息被 ack 后
                    # 重投命中 already_terminal 短路），执行永久卡死。
                    raise
                except Exception as e:
                    logger.error("流程执行出错: %s", e, exc_info=True)
                    # 波次③步内取消：两条判据任一命中即终态化 cancelled（= 取消
                    # 点前 checkpoint），绝不写 error：
                    #   ① 异常链含 FlowCancelledException（引擎在下一节点入口
                    #      抛出的执行级取消）；
                    #   ② 取消标志键命中——协作节点（如 agentrun）收到
                    #      cancel_event 后自杀抛出普通异常，异常类型无取消
                    #      特征，但控制面意图键仍在，以键为准最稳。
                    # 放行前做失租检查：与步界取消分支同规则（看门狗已判死
                    # 则不写状态，让接管者来写）。
                    if _chain_has_cancellation(e) or self._cancel_requested(execution_id):
                        self._raise_if_lease_lost(lease_execution_id)
                        state.status = "cancelled"
                        state.context = context
                        state.end_time = datetime.now().isoformat()
                        self._persist_state_or_raise(execution_id, state, "cancelled_in_node")
                        self._finalize_observers()
                        logger.info(
                            "执行 %s 在节点执行期响应取消（取消监听命中），终态化 cancelled",
                            execution_id,
                        )
                        break
                    # 节点级有界重试（波次二任务①）：瞬态节点失败不终态化，
                    # 抛重试异常让消息走 at-least-once 重投，从 checkpoint
                    # 重跑失败节点；预算耗尽/判据不符/回滚开关 → 现状终态化。
                    retry_exc = self._node_failure_retry_decision(
                        e, execution_id, delivery_count
                    )
                    if retry_exc is not None:
                        raise retry_exc from e
                    state.status = "error"
                    state.context = context
                    retries = max(
                        0, self._read_node_retry_counter(execution_id) - 1
                    )
                    if retries > 0:
                        state.error = {
                            "message": f"节点执行失败（重试 {retries} 次后仍失败）: {e}",
                            "node_retries": retries,
                        }
                    else:
                        state.error = {"message": str(e)}
                    state.end_time = datetime.now().isoformat()
                    self._persist_state_or_raise(execution_id, state, "error_state")
                    break

        return result

    def _dispatch_service_task(
        self, result: Dict[str, Any], context: Dict[str, Any], execution_id: str
    ) -> None:
        """挂起时把扩展节点的 service_config 投递给对应外延服务。

        历史上挂起只落执行状态，service_config 无人消费——
        delay/approval 等节点的任务永远不会被服务接走，执行会永久挂起。
        service_config 通常埋在 ``context.$NODE.{最后节点}.service_config``，
        顶层偶有直接携带；任务按 subtype 投递到 ``plaita:{subtype}:queue``，
        由对应外延服务消费（DelayService 等）。
        无 redis 客户端（单测/纯内存派生类）时跳过投递。
        """
        service_config = result.get("service_config")
        if not isinstance(service_config, dict) or not service_config:
            nodes = (context or {}).get("$NODE") or {}
            last_node = (context or {}).get("$LAST_NODE")
            node_result = nodes.get(last_node) if last_node else None
            if isinstance(node_result, dict):
                service_config = node_result.get("service_config")
        if not isinstance(service_config, dict) or not service_config:
            return
        subtype = str(service_config.get("type") or result.get("node_subtype") or "").strip()
        if not subtype:
            return
        redis_client = getattr(self, "redis_client", None)
        if redis_client is None or not hasattr(redis_client, "rpush"):
            logger.warning(
                "挂起任务投递跳过（无 redis 客户端）: %s (execution_id=%s)",
                subtype, execution_id,
            )
            return
        queue_key = f"plaita:{subtype}:queue"
        try:
            task = dict(service_config)
            task.setdefault("execution_id", execution_id)
            # 挂起服务消费后要 resume 原执行——透传租户，服务侧据此路由
            task.setdefault("tenant_id", current_tenant())
            redis_client.rpush(queue_key, json.dumps(task, ensure_ascii=False))
            logger.info(
                "挂起任务已投递: %s → %s (execution_id=%s)", subtype, queue_key, execution_id
            )
        except Exception as e:
            # 有 redis 客户端时投递失败必须上抛（任务②）：此刻执行已落盘为
            # suspended，吞掉异常会让消息被 ack——delay/approval 任务永无人
            # 接，执行永久挂起。抛出后消息不 ack 走重投，下轮 resume 重新
            # 执行挂起节点再派发；suspended 状态经 resume_flow 的定向放行
            # 保留（不翻 error）。代价：重投会重复注册订阅，EventFilter 的
            # 终态 GC 只回收终态，孤儿订阅留到 TTL 过期——可接受。
            # ServiceDispatchError 非 ValueError 子类，不会被 run() 毒丸 ack。
            logger.error(
                "挂起任务投递失败: %s → %s: %s", subtype, queue_key, e, exc_info=True
            )
            raise ServiceDispatchError(
                f"挂起任务投递失败: {subtype} → {queue_key} "
                f"(execution_id={execution_id}): {e}"
            ) from e

class RedisFlowWorker(RegistryMixin, ControlMixin, FlowWorker):
    """
    基于 Redis Stream 队列的流程工作器（服务注册 / 心跳 / 远程控制）。

    任务队列语义为 **at-least-once**：consumer group + ``XACK``；处理成功前
    崩溃则 pending 可被其他 consumer 在 ``claim_min_idle_ms`` 后回收重投。
    resume 通过 Redis execution lease 保证同一 ``execution_id`` 最多一个 worker 推进。
    控制面硬依赖 Redis。
    """

    SERVICE_TYPE = "flow_worker"
    
    def __init__(
        self,
        redis_url: str,
        queue_name: str,
        execution_storage: ExecutionStorage,
        flow_storage: FlowStorage,
        event_bus: EventBus = None,
        cache_size: int = 100,
        cache_ttl: int = 300,
        enable_registry: bool = True,
        registry_ttl: int = ServiceRegistry.DEFAULT_TTL,
        heartbeat_interval: int = ServiceRegistry.DEFAULT_HEARTBEAT_INTERVAL,
        enable_redis_logging: bool = True,
        redis_client: Redis = None,
        callback_handlers=None,
        consumer_group: str = DEFAULT_CONSUMER_GROUP,
        consumer_name: Optional[str] = None,
        claim_min_idle_ms: int = DEFAULT_CLAIM_MIN_IDLE_MS,
        lease_ttl_seconds: int = DEFAULT_LEASE_TTL_SECONDS,
        execution_lease: Optional[ExecutionLease] = None,
        max_deliveries: int = DEFAULT_MAX_DELIVERIES,
        dlq_key: Optional[str] = None,
        read_block_ms: int = 1_000,
        watchdog_interval_seconds: Optional[float] = None,
        cancel_poll_seconds: Optional[float] = None,
    ):
        redis_client = redis_client or Redis.from_url(redis_url)
        super().__init__(
            execution_storage,
            flow_storage,
            event_bus,
            cache_size,
            cache_ttl,
            callback_handlers=callback_handlers,
            execution_lease=execution_lease or TenantRoutingExecutionLease(redis_client),
            lease_ttl_seconds=lease_ttl_seconds,
        )
        self.redis_url = redis_url
        self.redis_client = redis_client
        self.queue_name = queue_name
        self._running = False
        # 消费阻塞窗口上限（毫秒）——见 run() 主循环内的分片说明
        self.read_block_ms = max(100, int(read_block_ms))
        self._active_task_count = 0
        self._log_handler = None
        self._consumer_group = consumer_group
        self._claim_min_idle_ms = claim_min_idle_ms
        self._consumer_name = consumer_name
        self._max_deliveries = max_deliveries
        self._dlq_key = dlq_key
        self._task_queue: Optional[RedisStreamTaskQueue] = None

        # 租约看门狗（波次② §4.1）：登记活跃 (execution_id → 租约值串/
        # FlowExecution/租户)，每 watchdog_interval_seconds（默认 TTL/3）
        # renew 一次，保证存活 worker 的租约永不空窗——XCLAIM 只会捡到
        # 真死 worker 的消息，抢占双跑被根除，fencing 是其下第二道保险。
        self._watchdog_interval_seconds = watchdog_interval_seconds
        self._watchdog_thread: Optional[threading.Thread] = None
        self._watchdog_stop = threading.Event()
        self._lease_watch_lock = threading.Lock()
        # execution_id -> (lease_value, execution, tenant_id)
        self._lease_watch: Dict[str, tuple] = {}
        self._lease_lost: Set[str] = set()

        # 取消监听（波次③ §3.3 步内中断）：登记活跃 (execution_id →
        # FlowExecution/租户)，每 cancel_poll_seconds（默认 1s）轮询取消标志键，
        # 命中即 execution.cancel() 中止在途节点。登记表/停止位已在基类建好，
        # 此处只接轮询间隔旋钮（单测可缩短）。
        if cancel_poll_seconds is not None:
            self._cancel_poll_seconds = cancel_poll_seconds
        
        # 服务注册
        self._enable_registry = enable_registry
        if self._enable_registry:
            self.init_registry(
                redis_client=self.redis_client,
                service_type=self.SERVICE_TYPE,
                metadata={
                    "queue_name": queue_name,
                    "redis_url": redis_url,
                    "cache_size": cache_size,
                    "cache_ttl": cache_ttl
                },
                ttl=registry_ttl,
                heartbeat_interval=heartbeat_interval
            )
            
            # 初始化控制监听
            self.init_control(
                redis_client=self.redis_client,
                instance_id=self._service_info.instance_id if self._service_info else "unknown"
            )
        
        # Redis 日志处理器
        self._enable_redis_logging = enable_redis_logging
        if self._enable_redis_logging and self._service_info:
            self._log_handler = setup_redis_logging(
                redis_client=self.redis_client,
                service_type=self.SERVICE_TYPE,
                instance_id=self._service_info.instance_id,
                logger_name="plaita",
                level=logging.INFO
            )

    def _resolve_consumer_name(self) -> str:
        if self._consumer_name:
            return self._consumer_name
        if self._enable_registry and getattr(self, "_service_info", None):
            return self._service_info.instance_id
        return f"worker-{os.getpid()}"

    def _get_task_queue(self) -> RedisStreamTaskQueue:
        if self._task_queue is None:
            kwargs: Dict[str, Any] = dict(
                group_name=self._consumer_group,
                consumer_name=self._resolve_consumer_name(),
                claim_min_idle_ms=self._claim_min_idle_ms,
                max_deliveries=self._max_deliveries,
                dlq_key=self._dlq_key,
            )
            # 死信守卫接线（Track B 契约）：队列类已支持 ``dead_letter_guard``
            # 参数才传入（并行合入期向后兼容——旧类不收该参数，TypeError 会
            # 炸掉 _get_task_queue 的全部调用方）。Track B 合入前守卫不生效，
            # 合入后由集成联测覆盖。
            if (
                "dead_letter_guard"
                in inspect.signature(RedisStreamTaskQueue.__init__).parameters
            ):
                kwargs["dead_letter_guard"] = self._dead_letter_guard
            self._task_queue = RedisStreamTaskQueue(self.redis_client, self.queue_name, **kwargs)
        return self._task_queue

    def _dead_letter_guard(self, task: StreamTask) -> bool:
        """死信守卫（Track B 契约）：True=允许死信；False/抛异常=跳过。

        判据是**执行状态**而非仅租约（合并评审修正）：delivery_count 被
        XCLAIM 抢占虚增后不可逆——若只看「租约在」，持有者崩溃后租约过期，
        守卫放行 → 消息立即死信，非终态执行的唯一恢复路径被切断；若只返回
        False 拒绝，超限消息在队列回收循环里只会被反复 XCLAIM/跳过、**永远
        不会再被派发**，同样救不回执行。因此：

        - 执行状态缺失或已终态（completed/error/cancelled）→ 放行死信
          （终态消息本就会被 already_terminal 短路 ack，死信记录更可查）；
        - 状态加载失败（Redis 瞬断/损坏）→ 保守跳过死信；
        - 非终态 + 租约在 → 有活 worker 正持有（长步骤处理中）→ 跳过死信；
        - 非终态 + 租约空（持有者已死）→ **重新入队一份同体消息**（delivery
          归 1，恢复路径重生）后放行原消息死信。并发守卫双入队无害：两条
          新消息被 resume 租约串行化，第二条命中 already_terminal 短路。

        租户路由：状态存储经 ContextVar 路由、租约键手工拼装，两者都用
        消息体 ``tenant_id``（守卫在 run() 主循环内 ``_dispatch_task`` 之外
        被调用，ContextVar 已复位）。键格式与 ``TenantRoutingExecutionLease``
        同规则：``{ns}:execution:lease:{id}``，ns = ``plaita``（default/空
        租户）或 ``plaita:{tenant_id}``。
        """
        body = getattr(task, "body", None)
        if not isinstance(body, dict):
            return True
        execution_id = body.get("execution_id")
        if not execution_id:
            # start 任务入队时还没有 execution_id，无执行可保护。
            return True

        token = set_current_tenant(body.get("tenant_id"))
        try:
            try:
                state = self.execution_storage.load_execution_state(execution_id)
            except Exception as exc:  # noqa: BLE001 — 瞬断保守跳过，不误杀活执行
                logger.warning(
                    "死信守卫读执行状态失败（保守跳过死信）: %s: %s", execution_id, exc
                )
                return False
            retry_count = self._read_node_retry_counter(execution_id)
        finally:
            reset_current_tenant(token)

        if state is None:
            return True
        if getattr(state, "status", "") in TERMINAL_EXECUTION_STATUSES:
            return True

        namespace = tenant_namespace(body.get("tenant_id"))
        lease_key = f"{namespace}:execution:lease:{execution_id}"
        try:
            lease_held = bool(self.redis_client.exists(lease_key))
        except Exception as exc:  # noqa: BLE001 — 瞬断保守跳过，不误杀活执行
            logger.warning(
                "死信守卫查询租约失败（保守跳过死信）: %s: %s", lease_key, exc
            )
            return False
        if lease_held:
            logger.warning(
                "死信跳过：执行 %s 租约仍在（活 worker 处理中），消息留 pending: %s",
                execution_id,
                getattr(task, "message_id", "?"),
            )
            return False

        # 节点重试预算耗尽后备判据（波次二任务①）：达限消息在队列层被就地
        # 死信、不经过处理函数；此刻执行非终态、租约已空且重试计数已达预算
        # ——说明这是节点反复失败的搁浅（含「预算耗尽判定后、终态化写盘前
        # worker 崩溃」窗口），终态化 error 后放行死信。**不**重入队：重入队
        # delivery 归 1 → 再耗尽 → 再重入队 = 无限循环。终态化写盘失败 →
        # 保守跳过死信（消息留 pending，下轮再处置）。
        if retry_count >= self._node_retry_budget():
            logger.error(
                "执行 %s 非终态但节点重试计数已达预算（%s/%s），终态化 error "
                "后放行死信，不再重入队: %s",
                execution_id, retry_count, self._node_retry_budget(),
                getattr(task, "message_id", "?"),
            )
            state.status = "error"
            state.error = {
                "message": (
                    f"节点执行失败：重试预算耗尽（{retry_count} 次），"
                    "执行无法继续推进"
                ),
                "node_retries": retry_count,
            }
            state.end_time = datetime.now().isoformat()
            try:
                self._persist_state_or_raise(
                    execution_id, state, "dlq_guard_node_retry_exhausted"
                )
            except Exception as exc:  # noqa: BLE001 — 写盘失败不丢消息
                logger.warning(
                    "死信守卫终态化失败（保守跳过死信）: %s: %s", execution_id, exc
                )
                return False
            return True

        try:
            enqueue_task(self.redis_client, self.queue_name, dict(body))
            logger.warning(
                "执行 %s 非终态且租约已空（持有者已死），原消息 delivery 已超限："
                "已重新入队恢复路径，放行原消息死信: %s",
                execution_id,
                getattr(task, "message_id", "?"),
            )
        except Exception as exc:  # noqa: BLE001 — 重入队失败则不能丢原消息
            logger.warning(
                "死信守卫重新入队失败（保守跳过死信）: %s: %s", execution_id, exc
            )
            return False
        return True

    # ---- 租约看门狗（波次② §4.1）----

    def _register_lease_watch(
        self, execution_id: str, lease_value: str, execution: Any
    ) -> None:
        """登记活跃执行：看门狗据此续租；失租鸭子调 execution.cancel()。"""
        with self._lease_watch_lock:
            # 同一执行的新租约（重投 resume 重入）清除陈旧失租标记
            self._lease_lost.discard(execution_id)
            self._lease_watch[execution_id] = (
                lease_value,
                execution,
                current_tenant(),
            )

    def _unregister_lease_watch(self, execution_id: str, lease_value: str) -> None:
        with self._lease_watch_lock:
            entry = self._lease_watch.get(execution_id)
            # 只撤自己的登记（防误撤后来者的新世代租约）
            if entry is not None and entry[0] == lease_value:
                self._lease_watch.pop(execution_id, None)
            self._lease_lost.discard(execution_id)

    def _raise_if_lease_lost(self, execution_id: Optional[str]) -> None:
        if not execution_id:
            return
        with self._lease_watch_lock:
            lost = execution_id in self._lease_lost
        if lost:
            raise ExecutionLeaseError(
                f"execution {execution_id} lease lost (watchdog flagged); aborting"
            )

    def _start_lease_watchdog(self) -> None:
        """启动看门狗线程（run() 调用）；PLAITA_DISABLE_LEASE_WATCHDOG=1 不启动。"""
        if _watchdog_disabled():
            logger.info("租约看门狗已禁用（PLAITA_DISABLE_LEASE_WATCHDOG=1）")
            return
        if self._watchdog_thread is not None and self._watchdog_thread.is_alive():
            return
        self._watchdog_stop.clear()
        self._watchdog_thread = threading.Thread(
            target=self._lease_watchdog_loop,
            name="plaita-lease-watchdog",
            daemon=True,
        )
        self._watchdog_thread.start()

    def _stop_lease_watchdog(self) -> None:
        self._watchdog_stop.set()
        watchdog = self._watchdog_thread
        if (
            watchdog is not None
            and watchdog is not threading.current_thread()
            and watchdog.is_alive()
        ):
            watchdog.join(timeout=2.0)
        self._watchdog_thread = None

    def _watchdog_interval(self) -> float:
        if self._watchdog_interval_seconds is not None:
            return max(0.01, float(self._watchdog_interval_seconds))
        # 默认 TTL/3（设计稿 §4.1：120s TTL → 40s renew）
        return max(1.0, self.lease_ttl_seconds / 3.0)

    def _lease_watchdog_loop(self) -> None:
        interval = self._watchdog_interval()
        logger.info(
            "租约看门狗已启动（renew 间隔 %.2fs, ttl=%ss）", interval, self.lease_ttl_seconds
        )
        while not self._watchdog_stop.wait(interval):
            try:
                self._watchdog_renew_once()
            except Exception:  # noqa: BLE001 — 看门狗自身绝不能带崩 worker
                logger.error("租约看门狗周期异常", exc_info=True)

    def _watchdog_renew_once(self) -> None:
        """对全部活跃执行续租一轮；renew 失败（Lua compare 不符 = 已被他人
        持有/过期）→ 标记 lease_lost + 鸭子调 execution.cancel() 中止当前步
        （引擎 cancel() 为波次③实现，缺席时跳过——失租兜底是步界
        _renew_lease_if_held / persist 前失租检查抛 ExecutionLeaseError 且
        不写状态，消息不 ack）。Redis 瞬断（renew 抛异常）不判死，下周期重试。
        """
        with self._lease_watch_lock:
            entries = list(self._lease_watch.items())
        for execution_id, (lease_value, execution, tenant_id) in entries:
            token = set_current_tenant(tenant_id)
            try:
                renewed = self.execution_lease.renew(
                    execution_id, lease_value, self.lease_ttl_seconds
                )
            except Exception as exc:  # noqa: BLE001 — 瞬断不判死
                logger.warning(
                    "看门狗 renew 异常（下周期重试）: %s: %s", execution_id, exc
                )
                continue
            finally:
                reset_current_tenant(token)
            if renewed:
                continue
            with self._lease_watch_lock:
                self._lease_lost.add(execution_id)
                self._lease_watch.pop(execution_id, None)
            cancel = getattr(execution, "cancel", None)
            if callable(cancel):
                try:
                    cancel()
                except Exception:  # noqa: BLE001 — cancel 失败不影响失租兜底
                    logger.warning(
                        "看门狗调 execution.cancel() 失败: %s", execution_id, exc_info=True
                    )
            else:
                logger.debug("执行对象无 cancel()（引擎侧波次③前），仅步界兜底")
            logger.error(
                "执行 %s 租约续期失败（已被他人持有/过期），已标记 lease_lost "
                "并请求中止当前步；消息将在步界以 ExecutionLeaseError 退出不 ack",
                execution_id,
            )

    def _detect_affinity_mismatch(self, message_data: Dict[str, Any]) -> Optional[str]:
        """任务的 repo/run_dir 是否指向本机不存在的路径（机器亲和判定）。

        返回 None = 亲和（可跑）；返回字符串 = 不亲和的原因（供日志）。
        判据：消息 params（或顶层）里的 repo / run_dir 是绝对路径且本机
        **不存在** → 不亲和。只查 start 类消息（resume 任务路径已由首次
        start 验过，且 resume 可能不带完整 params）。

        宽松原则：拿不到路径、相对路径、路径存在 → 一律判亲和（不拦）。
        宁可多跑一次失败，不可误拦本机该跑的任务。
        """
        if str(message_data.get("type") or "start") not in ("start", ""):
            return None
        params = message_data.get("params")
        if not isinstance(params, dict):
            return None
        for field in ("repo", "run_dir"):
            raw = params.get(field)
            if not isinstance(raw, str) or not raw.strip():
                continue
            path = raw.strip()
            if not path.startswith("/"):
                continue  # 相对路径/标识符：无法判定，放过
            # run_dir 首次运行时尚未建（<repo>/.flowcast/runs/<run_id>），
            # 故判它的**仓根**（run_dir 上溯到 .flowcast 的父目录）是否存在。
            # 找不到 .flowcast 段则退回查 run_dir 自身（保守）。
            probe = path
            if field == "run_dir":
                head = path
                while head and head != "/":
                    if os.path.basename(head) == ".flowcast":
                        probe = os.path.dirname(head)
                        break
                    head = os.path.dirname(head)
            if not os.path.exists(probe):
                return f"{field}={path} 在本机不存在（本机无此仓路径）"
        return None

    def _handover_non_affine(self, task: Any, queue: Any, exc: Exception) -> None:
        """把不亲和的 task 交接给其他 worker：ack 原条 + 重入队同体新副本。

        重入队而非留 pending 的原因见 TaskNotForThisWorker docstring（留
        pending 会被对端反复 XCLAIM 虚增 delivery → 误死信）。

        **防 ping-pong**：新副本有极小概率又被本机抢到（竞争）。用一个短
        TTL 的「本机刚让过此消息」标记抑制——命中则改成留 pending（让对端
        按正常 reclaim 拿），避免两机无限交接。标记按 body 指纹（消息体
        无稳定 id 可用时应退化为整体序列化）。
        """
        body = getattr(task, "body", None)
        if not isinstance(body, dict):
            # 拿不到消息体：退化为留 pending（对端 reclaim）
            logger.info("任务 %s 不亲和但无体可交接，留 pending: %s",
                        getattr(task, "message_id", "?"), exc)
            return
        try:
            import hashlib as _hl
            key = "plaita:affinity:handover:" + _hl.sha1(
                json.dumps(body, sort_keys=True, ensure_ascii=False).encode()
            ).hexdigest()
            # 若本机刚让过这条（标记在）→ 说明可能 ping-pong，改为留 pending
            if self.redis_client.exists(key):
                logger.info("任务 %s 不亲和（本机刚让过），留 pending 交对端 reclaim: %s",
                            getattr(task, "message_id", "?"), exc)
                return
            self.redis_client.set(key, "1", ex=30)
            # 先 ack 原条（移出 PEL、防对端再抢），再重入队新副本
            queue.ack(getattr(task, "message_id"))
            enqueue_task(self.redis_client, self.queue_name, dict(body))
            logger.info("任务 %s 与本机不亲和，已交接（ack+重入队）给其他 worker: %s",
                        getattr(task, "message_id", "?"), exc)
        except Exception as e:  # noqa: BLE001 — 交接失败退化为留 pending（绝不丢消息）
            logger.warning("不亲和任务交接失败（退化为留 pending）: %s: %s", e, exc)

    def _dispatch_task(self, message_data: Dict[str, Any], delivery_count: Optional[int] = None) -> None:
        # 机器亲和性闸（路线二首版，2026-10-06 多机验证）：任务参数里的 repo/
        # run_dir 是**派发方所在机器**的绝对路径。本机不具备该路径 = 跑不了，
        # 应让给有它的 worker（或等它出现）。不拦的话本机抢到就跑 → 秒失败
        # → 烧 delivery 配额 → 死信（plaita#41 双机实证）。抛
        # TaskNotForThisWorker（下方 except 分支不 ack、留 pending，由队列
        # 回收交给别的 consumer）。
        # 回滚/放行：PLAITA_DISABLE_AFFINITY=1，或消息无 repo 参数（不自带
        # 路径的任务不受影响）。
        if not _affinity_disabled():
            missing = self._detect_affinity_mismatch(message_data)
            if missing is not None:
                raise TaskNotForThisWorker(missing)

        # 租户上下文：消息携带 tenant_id（缺省 = default，兼容旧生产方）；
        # 存储路由包装器/日志 handler/租约据此选租户 namespace。
        token = set_current_tenant(message_data.get("tenant_id"))
        try:
            message_type = message_data.get("type")
            if message_type == "start":
                self.start_flow(
                    message_data.get("flow_id"),
                    message_data.get("params"),
                    message_data.get("version"),
                    execution_id=message_data.get("execution_id"),
                    dedup_key=message_data.get("dedup_key"),
                    delivery_count=delivery_count,
                )
            elif message_type == "resume":
                self.resume_flow(
                    message_data.get("flow_id"),
                    message_data.get("execution_id"),
                    message_data.get("resume_type"),
                    message_data.get("data"),
                    delivery_count=delivery_count,
                )
            else:
                raise ValueError(f"unknown task type: {message_type!r}")
        finally:
            reset_current_tenant(token)

    def run(self):
        """
        从 Redis Stream consumer group 拉取 ``start`` / ``resume`` 任务。

        成功处理后 ``XACK``；崩溃或未 ack 的消息留在 pending，超时后可被
        其他 consumer ``XCLAIM`` 回收（at-least-once，业务侧应幂等）。
        """
        self._running = True
        queue = self._get_task_queue()
        queue.ensure_group()

        # 租约看门狗（波次②）：持租约执行每 TTL/3 续租，防长步被 XCLAIM 抢占
        self._start_lease_watchdog()
        # 取消监听（波次③）：轮询取消标志键，命中即中止在途节点
        self._start_cancel_watcher()

        # 注册服务
        if self._enable_registry:
            self.register_service()
            # 启动控制监听
            self.start_control_listener()
        
        logger.info(
            "流程工作器已启动，监听 stream: %s (group=%s, consumer=%s)",
            self.queue_name,
            self._consumer_group,
            queue.consumer_name,
        )
        
        try:
            while self._running:
                # 分片阻塞读取（2026-09 分布式评审 P2-2）：XREADGROUP 的
                # BLOCK 无法被信号中断出循环，整块 10s 会让 SIGTERM 后的
                # worker 继续抢任务最长 10s。切成 ≤1s 的窗口，停机延迟
                # 上限 ≈1s，空轮询的 Redis 往返开销可忽略。
                task = queue.read(block_ms=min(self.read_block_ms, 1_000))
                if not task:
                    continue

                self._active_task_count += 1
                if self._enable_registry:
                    self.update_registry_info(active_tasks=self._active_task_count)

                acked = False
                try:
                    self._dispatch_task(task.body, delivery_count=task.delivery_count)
                    queue.ack(task.message_id)
                    acked = True
                except TaskNotForThisWorker as exc:
                    # 任务不亲和本机（repo/run_dir 指向别的机器的路径）：**交接**
                    # 给其他 worker——ack 原消息 + 重入队新副本（delivery 归 1）。
                    # 不能只「留 pending」：对端会按 claim_min_idle_ms 反复 XCLAIM，
                    # 每次让 deliveries +1，两 worker 下 5 轮即触顶 → 误死信
                    # （2026-10-06 双机实测）。交接无损耗，且新副本仍会回到
                    # 共享队列由亲和的 worker 领走。
                    queue.note_lease_conflict()  # 复用「让给别人」计数口径
                    self._handover_non_affine(task, queue, exc)
                except ExecutionLeaseError as exc:
                    # 另一 worker 持有 resume 租约：不 ack，待租约过期后 reclaim
                    queue.note_lease_conflict()
                    logger.warning(
                        "任务 %s 未取得 execution lease，留在 pending: %s",
                        task.message_id,
                        exc,
                    )
                except NodeExecutionRetryableError as exc:
                    # 节点级可重试失败（波次二任务①）：执行状态停在最后成功
                    # 步 checkpoint，不 ack、不计 poison、留 pending 等回收——
                    # 重投间隔=claim_min_idle_ms（默认 60s），下轮 resume 从
                    # checkpoint 重跑失败节点。预算（重试计数键）已在处理函数
                    # 内判定，耗尽时已在处理函数内终态化 error，不会走到这里
                    # 无限重投。
                    queue.note_failed()
                    logger.warning(
                        "任务 %s 节点执行失败待重投 (delivery=%s): %s",
                        task.message_id,
                        task.delivery_count,
                        exc,
                    )
                except ValueError as exc:
                    # 畸形消息：ack 掉避免 poison pill 无限重投
                    logger.error("丢弃无效任务 %s: %s", task.message_id, exc)
                    queue.ack(task.message_id)
                    queue.note_poison()
                    acked = True
                except Exception as exc:
                    # 兜底重投递路径（at-least-once）：不 ack，等超时回收。
                    # 刻意放在 except ValueError 之后——存储层抛出的
                    # ExecutionStateLoadError（读取瞬断/状态损坏，ReviewFix D1）
                    # 由此处理：消息留在 pending 重投，超过 max_deliveries 才
                    # 进 DLQ；绝不能像 ValueError 一样被当畸形消息 poison ack，
                    # 否则 Redis 抖动一次 = 挂起执行永久失去恢复机会。
                    queue.note_failed()
                    if task.delivery_count >= queue.max_deliveries:
                        queue.dead_letter(
                            task,
                            reason=f"processing_failed:{type(exc).__name__}:{exc}"[:500],
                        )
                        acked = True
                    else:
                        logger.error(
                            "任务处理失败 %s (delivery=%s/%s)，未 ack（将重投）: %s",
                            task.message_id,
                            task.delivery_count,
                            queue.max_deliveries,
                            exc,
                            exc_info=True,
                        )
                finally:
                    self._active_task_count -= 1
                    if self._enable_registry:
                        self.update_registry_info(active_tasks=self._active_task_count)
                    if not acked:
                        logger.debug("任务 %s 留在 pending 等待回收", task.message_id)
        finally:
            self.stop()
    
    def stop(self):
        """停止流程工作器"""
        logger.info("正在停止流程工作器...")
        self._running = False

        # 停止租约看门狗（波次②）
        self._stop_lease_watchdog()

        # 停止取消监听（波次③）
        self._stop_cancel_watcher()

        # 停止控制监听
        if self._enable_registry:
            self.stop_control_listener()
        
        # 注销服务
        if self._enable_registry:
            self.unregister_service()
        
        # 关闭日志处理器
        if self._log_handler:
            self._log_handler.close()
        
        logger.info("流程工作器已停止")
    
    def _on_stop_command(self, graceful: bool):
        """响应停止命令"""
        logger.info("收到远程停止命令，优雅停止: %s", graceful)
        self.stop()
    
    def _on_status_command(self) -> Dict[str, Any]:
        """响应状态查询命令"""
        status = {
            "status": "running" if self._running else "stopped",
            "active_tasks": self._active_task_count,
            "queue_name": self.queue_name,
            "consumer_group": self._consumer_group,
            "consumer_name": self._resolve_consumer_name(),
        }
        try:
            status["queue"] = self._get_task_queue().stats()
        except Exception as exc:
            status["queue_error"] = str(exc)
        return status

# 新增命令行入口


def _request_graceful_stop(worker: "RedisFlowWorker", signum: int) -> None:
    """信号处理器入口：只请求优雅停机，绝不立即退出进程（ReviewFix D3）。

    旧实现在 handler 里 ``sys.exit(0)``——SystemExit 会在 ``_dispatch_task``
    的任意字节码边界抛出（可能正卡在 LLM 调用/存储写入之间），在途任务当场
    腰斩、消息未 ack，退化为 crash 式重投；与控制通道停止命令的 drain 语义
    （``_on_stop_command`` 只置位、等当前任务做完）不一致。现在 handler 只调
    ``worker.stop()`` 置位，由 ``run()`` 主循环在任务边界自然退出——read 已切
    成 ≤1s 分片，空转停机延迟上限 ≈1s；强制退出兜底交给 systemd/K8s 的
    SIGKILL 超时。
    """
    logger.info("收到信号 %s，请求优雅停机（当前任务完成后退出）...", signum)
    worker.stop()


from plaita.server.factory import create_storage_component, create_event_bus  # noqa: F401


def main():
    """命令行入口程序"""
    # CLI 默认给控制台日志：库内 logger 无 handler，原样跑运维看到的是
    # 零输出（2026-09 分布式评审 P2-6）。--quiet / PLAITA_LOG_LEVEL 可调。
    logging.basicConfig(
        level=os.environ.get("PLAITA_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(description="Plaita流程工作器")
    
    # Redis参数（支持环境变量 PLAITA_REDIS_URL / REDIS_URL）
    parser.add_argument("--redis-url",
                      default=os.environ.get("PLAITA_REDIS_URL", os.environ.get("REDIS_URL", "redis://localhost:6379/0")),
                      help="Redis连接URL")
    parser.add_argument("--queue-name",
                      default=os.environ.get("PLAITA_QUEUE_NAME", os.environ.get("QUEUE_NAME", "plaita:flow:queue")),
                      help="Redis Stream 键名（任务队列，需 Redis 5+）")
    parser.add_argument("--consumer-group",
                      default=os.environ.get("PLAITA_CONSUMER_GROUP", DEFAULT_CONSUMER_GROUP),
                      help="Stream consumer group 名称")
    parser.add_argument("--consumer-name",
                      default=os.environ.get("PLAITA_CONSUMER_NAME"),
                      help="本 worker 的 consumer 名称（默认 instance_id 或 worker-pid）")
    parser.add_argument("--claim-min-idle-ms", type=int,
                      default=int(os.environ.get("PLAITA_CLAIM_MIN_IDLE_MS", str(DEFAULT_CLAIM_MIN_IDLE_MS))),
                      help="pending 消息最短空闲毫秒数后才可被其他 consumer 回收")
    parser.add_argument("--lease-ttl-seconds", type=int,
                      default=int(os.environ.get("PLAITA_LEASE_TTL_SECONDS", str(DEFAULT_LEASE_TTL_SECONDS))),
                      help="resume execution lease TTL（秒），推进中会 renew")
    parser.add_argument("--max-deliveries", type=int,
                      default=int(os.environ.get("PLAITA_MAX_DELIVERIES", str(DEFAULT_MAX_DELIVERIES))),
                      help="任务最大投递次数，超过后写入 DLQ 并 ack")
    parser.add_argument("--dlq-key",
                      default=os.environ.get("PLAITA_DLQ_KEY"),
                      help="死信 Stream 键（默认 <queue-name>:dlq）")
    
    # 数据库参数
    parser.add_argument("--database-url", default="sqlite:///flow.db",
                      help="数据库连接URL")
    
    # 存储组件类型（execution/flow 仅 memory|redis；db 与同步 ABC 不兼容，见 factory）
    parser.add_argument("--execution-storage-type", choices=["memory", "redis"], default="redis",
                      help="执行状态存储类型（memory|redis；db 已下架）")
    parser.add_argument("--flow-storage-type", choices=["memory", "redis"], default="redis",
                      help="流程定义存储类型（memory|redis；db 已下架）")
    
    # 事件总线参数（默认启用：分布式挂起/恢复依赖订阅写入；
    # 历史 --use-event-bus 为 opt-in，导致默认部署订阅落内存、EventFilter 读 Redis）
    parser.add_argument("--event-bus-type", choices=["memory", "redis"], default="redis",
                      help="事件总线类型（生产用 redis；db/sqlalchemy 已标 experimental，见 factory）")
    parser.add_argument("--use-event-bus", action="store_true",
                      help=argparse.SUPPRESS)  # 已默认启用，保留兼容旧脚本
    parser.add_argument("--no-event-bus", action="store_true",
                      help="禁用事件总线（仅无挂起节点的纯同步场景）")
    
    # 缓存参数
    parser.add_argument("--cache-size", type=int, default=100,
                      help="缓存大小")
    parser.add_argument("--cache-ttl", type=int, default=300,
                      help="缓存TTL(秒)")
    
    # 服务注册参数
    parser.add_argument("--enable-registry", action="store_true", default=True,
                      help="启用服务注册")
    parser.add_argument("--no-registry", action="store_true",
                      help="禁用服务注册")
    parser.add_argument("--registry-ttl", type=int, default=30,
                      help="服务注册TTL(秒)")
    parser.add_argument("--read-block-ms", type=int, default=1_000,
                        help="XREADGROUP 阻塞窗口上限（毫秒）。默认 1000：让 SIGTERM "
                             "后的停机延迟 ≤1s；调大可略降 Redis 往返次数")
    parser.add_argument("--quiet", action="store_true",
                        help="关闭 INFO 级控制台日志（等价 PLAITA_LOG_LEVEL=WARNING）")
    parser.add_argument("--heartbeat-interval", type=int, default=10,
                      help="心跳间隔(秒)")
    parser.add_argument("--langfuse", action="store_true", default=None,
                        help="启用 Langfuse 观测（需 pip install plaita[langfuse]；"
                             "凭据走 LANGFUSE_PUBLIC_KEY/SECRET_KEY/HOST 环境变量）。"
                             "也可用环境变量 PLAITA_WORKER_LANGFUSE=1 开启")

    args = parser.parse_args()
    if args.quiet:
        logging.getLogger().setLevel(logging.WARNING)

    callback_handlers = []
    if args.langfuse or os.environ.get("PLAITA_WORKER_LANGFUSE") == "1":
        try:
            from plaita.obs import LangfuseCallback

            callback_handlers.append(LangfuseCallback())
            logger.info("Langfuse 观测已启用（trace id = 运行时 $EXECUTION_ID）")
        except ImportError as e:
            logger.warning("Langfuse 观测未启用（缺依赖）: %s", e)
        except Exception as e:  # noqa: BLE001 — SDK 初始化失败（如缺凭据）只降级不退出
            logger.warning("Langfuse 观测未启用: %s", e)

    # 外部业务节点模块加载（与 console 的 PLAITA_CONSOLE_NODE_MODULES 约定对齐）：
    # PLAITA_NODE_PATH 冒号分隔追加 sys.path；PLAITA_NODE_MODULES 逗号分隔，
    # 逐个 import 并调用其 register_all()（或 register）。业务仓（如 mediaflow
    # 的 plaita_flows.nodes）由此在 console 拉起的 worker 内生效。
    import importlib

    for extra in [p for p in os.environ.get("PLAITA_NODE_PATH", "").split(os.pathsep) if p]:
        if extra not in sys.path:
            sys.path.insert(0, extra)
    for mod_path in [m.strip() for m in os.environ.get("PLAITA_NODE_MODULES", "").split(",") if m.strip()]:
        try:
            mod = importlib.import_module(mod_path)
            register = getattr(mod, "register_all") or getattr(mod, "register")
            register()
            logger.info("已加载外部节点模块: %s", mod_path)
        except Exception as e:
            logger.error("外部节点模块加载失败 %s: %s", mod_path, e, exc_info=True)
            raise SystemExit(f"外部节点模块加载失败: {mod_path}")

    # CodeNode 按需注册：0.4.0 起移出默认注册表（其执行任意用户代码，需显式 opt-in）。
    # 生产流程（如 self-improve-v2）含 code 节点，worker 必须注册否则整单被丢弃
    # （「unRecognized node type: code」）。后端由 PLAITA_CODE_BACKEND 选择，默认
    # subprocess（无需 docker daemon）；声明值非法/不可用时降级并告警，不让整机退出。
    if _code_node_enabled():
        _register_code_node_for_worker()
    
    # 处理注册开关
    enable_registry = args.enable_registry and not args.no_registry
    
    try:
        # 创建执行状态存储
        storage_kwargs = {
            "redis_url": args.redis_url,
            "database_url": args.database_url
        }
        
        execution_storage = create_storage_component(
            args.execution_storage_type,
            "execution",
            tenant_routing=True,
            **storage_kwargs
        )
        logger.info("已创建执行状态存储: %s类型（多租户路由）", args.execution_storage_type)

        # 创建流程定义存储
        flow_storage = create_storage_component(
            args.flow_storage_type,
            "flow",
            tenant_routing=True,
            **storage_kwargs
        )
        logger.info("已创建流程定义存储: %s类型（多租户路由）", args.flow_storage_type)
        
        # 创建事件总线（默认启用；--no-event-bus 显式关闭）
        event_bus = None
        if not args.no_event_bus:
            event_bus = create_event_bus(
                args.event_bus_type,
                **storage_kwargs
            )
            logger.info("已创建事件总线: %s类型", args.event_bus_type)
        else:
            logger.warning("已禁用事件总线（--no-event-bus）；含挂起节点的流程将无法恢复")
        
        # 创建Redis流程工作器
        worker = RedisFlowWorker(
            redis_url=args.redis_url,
            queue_name=args.queue_name,
            execution_storage=execution_storage,
            flow_storage=flow_storage,
            event_bus=event_bus,
            callback_handlers=callback_handlers or None,
            cache_size=args.cache_size,
            cache_ttl=args.cache_ttl,
            enable_registry=enable_registry,
            registry_ttl=args.registry_ttl,
            heartbeat_interval=args.heartbeat_interval,
            consumer_group=args.consumer_group,
            consumer_name=args.consumer_name or None,
            claim_min_idle_ms=args.claim_min_idle_ms,
            lease_ttl_seconds=args.lease_ttl_seconds,
            max_deliveries=args.max_deliveries,
            dlq_key=args.dlq_key or None,
            read_block_ms=args.read_block_ms,
        )
        
        # 注册信号处理器以支持优雅关闭。只置位、由 run() 主循环在任务边界
        # 自然退出；不再 sys.exit 腰斩在途任务（ReviewFix D3，语义见
        # _request_graceful_stop）。
        def signal_handler(signum, frame):
            _request_graceful_stop(worker, signum)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # 启动工作器。历史上这里有 --debug-mode 分支硬编码 flow_id="event_flow_demo"
        # 直接读 Redis + 手动 lrem 队列消息, 是开发期临时脚本——已删除。需要类似
        # 调试请用 Redis CLI 或独立 dev 脚本, 不要留在生产 CLI 入口里。
        logger.info("流程工作器启动成功，监听队列: %s", args.queue_name)
        worker.run()

    except Exception as e:
        logger.error("流程工作器启动失败: %s", e)
        sys.exit(1)

if __name__ == "__main__":
    main()


