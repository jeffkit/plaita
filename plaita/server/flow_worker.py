from datetime import datetime
import hashlib
import inspect
import json
import logging
import os
import signal
import threading
import time
import uuid
from typing import Dict, Any, Mapping, NamedTuple, Optional, Set, Tuple

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
    ResumeError,
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
from plaita.writefile_jail import apply_writefile_jail
from plaita.server.registry import RegistryMixin, ServiceRegistry, ServiceInfo
from plaita.server.control import ControlMixin, ControlListener
from plaita.server.alerts import WebhookAlerter
from plaita.server.metrics import (
    Metric,
    MetricsHttpServer,
    collect_queue_metrics,
    collect_worker_metrics,
    render_prometheus,
)
from plaita.server.log_handler import setup_redis_logging
from plaita.server.node_timings import NodeTimingCallback
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
    holder_instance_id,
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


def _holder_liveness_disabled() -> bool:
    """#50 回滚开关：PLAITA_DISABLE_HOLDER_LIVENESS=1 时租约冲突分支不做
    持有者存活核算——无条件回到「ack 释放」（2026-10-06 语义）。"""
    return _env_switch("PLAITA_DISABLE_HOLDER_LIVENESS")


def _affinity_disabled() -> bool:
    """机器亲和闸回滚开关：PLAITA_DISABLE_AFFINITY=1 时不做路径检查
    （回到「领到就跑」的现状；单机/同构环境本就不需要本闸）。"""
    return _env_switch("PLAITA_DISABLE_AFFINITY")


def _metrics_port_from_env() -> int:
    """``/metrics`` 抓取端端口（#26）；未配置 / 非法 = 0 = 不启动。"""
    raw = (os.environ.get("PLAITA_METRICS_PORT") or "").strip()
    if not raw:
        return 0
    try:
        return max(0, int(raw))
    except ValueError:
        logger.warning("PLAITA_METRICS_PORT=%r 非法，已禁用 /metrics 端点", raw)
        return 0


def _metrics_host_from_env() -> str:
    return (os.environ.get("PLAITA_METRICS_HOST") or "").strip() or "0.0.0.0"


def _alert_webhook_from_env() -> Optional[str]:
    return (os.environ.get("PLAITA_ALERT_WEBHOOK") or "").strip() or None


def _paused_sweeper(max_age_secs: int = 6 * 3600):
    """准备「暂停沙箱清扫」：**全部 import 在主线程完成**，返回无 import 的闭包。

    ⚠️ 为什么死磕 import 位置：线程内 import 与主线程的懒加载 import 并发时会撞
    import 锁——2026-10-08 实测本机 worker 启动即整体卡死（栈停在 e2b/httpx 导入链，
    进程 0% CPU、零日志、连不上 Redis），根因就是清扫线程里那句
    ``from e2b_code_interpreter import ...``。预导入之后线程内只做网络调用。

    为什么需要这条清扫：失败/取消改为一律 pause 保现场后（sandbox_agent.
    _preserve_scene），没人续跑的暂停实例会累积占实例配额（AGS ~20；暂停不计
    计算力费，但配额满会让新建失败）。

    返回 ``None`` 表示无需/不可清扫（未装 plaita-nodes、无注册表、无 ags driver）。
    e2b 预导入是尽力而为的加速项，失败不算「不可清扫」——缺装时照常交出闭包，
    由驱动内部再导入时自报错并吞掉；任何异常只告警，绝不拦启动。
    """
    try:
        from plaita_nodes import sandbox as _sb
        if not _sb.load_sandboxes():
            return None
        import plaita_nodes.sandbox_ags  # noqa: F401 — import 即注册 ags driver
        drv = _sb.get_driver("ags")
        if drv is None or not hasattr(drv, "sweep_paused"):
            return None
    except Exception as exc:  # noqa: BLE001 — 清扫失败不影响启动
        logger.warning("沙箱清扫准备失败（忽略）：%s", exc)
        return None

    try:
        import e2b_code_interpreter  # noqa: F401 — 预导入（sweep 内部再取时已缓存）
    except Exception as exc:  # noqa: BLE001 — 缺装只退化为驱动内部导入
        logger.warning("e2b 预导入失败（忽略，清扫仍会尝试）：%s", exc)

    def _run() -> list:
        try:
            killed = drv.sweep_paused(max_age_secs=max_age_secs)
            if killed:
                logger.info("启动清扫：回收超龄暂停沙箱 %d 个：%s", len(killed), killed)
            return killed
        except Exception as exc:  # noqa: BLE001
            logger.warning("启动清扫沙箱失败（忽略）：%s", exc)
            return []

    return _run


def sweep_paused_sandboxes(max_age_secs: int = 6 * 3600) -> list:
    """同步执行一次暂停清扫（测试/手工调用用）。

    worker 启动路径请用 ``_paused_sweeper()`` + 后台线程（import 前置到主线程）。
    """
    run = _paused_sweeper(max_age_secs)
    return run() if run is not None else []


def _deny_repos() -> set:
    """本机拒跑仓名单（2026-10-07）：PLAITA_WORKER_DENY_REPOS=<仓名,仓名…>。

    用途：把 Rust 重仓（cargo 冷构建）挡在小容量 worker 之外——2 核远端
    跑 cargo 会饱和/超时（实测 load 3.59/2 核），重仓留给大容量 worker。
    命中 → 复用既有「不亲和 → 交接」路径（对端接力；无人接则留 pending
    等 reclaim）。空 = 不拒（默认零行为变化）。"""
    raw = os.environ.get("PLAITA_WORKER_DENY_REPOS", "")
    return {x.strip() for x in raw.split(",") if x.strip()}


def _node_retry_disabled() -> bool:
    """波次二任务①回滚开关：PLAITA_DISABLE_NODE_RETRY=1 时节点失败直接终态化
    error（完全回到波次前行为）。"""
    return _env_switch("PLAITA_DISABLE_NODE_RETRY")


def _code_backend_for_worker() -> str:
    """worker 启动时 CodeNode 的沙箱后端（PLAITA_CODE_BACKEND，默认 subprocess）。"""
    raw = os.environ.get("PLAITA_CODE_BACKEND", "").strip()
    return raw or "subprocess"


def _code_allowed_backends_for_worker(backend: str) -> list:
    """worker 生效的沙箱后端白名单（plaita#22）。

    ``PLAITA_SANDBOX_ALLOWED_BACKENDS`` 未配置时默认 ``(docker,)`` 并入生效后端，
    即流程 JSON 不得逐节点降级到 ``"unsafe"``（进程内 raw exec）——未接线前该白名单
    机制只存在于注释里，任意租户写一行 ``sandbox_backend: "unsafe"`` 即宿主 RCE。
    """
    from plaita.node import resolve_sandbox_allowed_backends

    return list(resolve_sandbox_allowed_backends(backend, "flow-worker"))


def _code_allowed_languages_for_worker() -> list:
    """worker 生效的语言白名单（plaita#29）。

    默认只放行 ``python``：``language: "js"`` 的历史实现（PyExecJS）绕开整个
    ``sandbox_backend`` 档位体系（无隔离/无超时/无取消），放行 js 须经
    ``PLAITA_SANDBOX_ALLOWED_LANGUAGES`` 显式配置。
    """
    from plaita.node import resolve_sandbox_allowed_languages

    return list(resolve_sandbox_allowed_languages("flow-worker"))


def _code_node_enabled() -> bool:
    """PLAITA_DISABLE_CODE_NODE=1 时不注册 CodeNode（含 code 节点的流程会被丢弃）。"""
    return not _env_switch("PLAITA_DISABLE_CODE_NODE")


def _register_code_node_for_worker() -> None:
    """为 worker 注册 CodeNode（生产流程如 self-improve-v2 含 code 节点）。

    默认注册表自 0.4.0 起不含 CodeNode（执行任意用户代码须显式 opt-in），worker
    不注册则整单被丢弃（unRecognized node type: code）。后端不可用（如选 docker
    但无 daemon）时**降级到 subprocess 并告警**，不让整机起不来。

    白名单由 ``_code_allowed_backends_for_worker`` 解析（plaita#22）：未显式配置
    ``PLAITA_SANDBOX_ALLOWED_BACKENDS`` 时默认只放行 ``docker`` ∪ 生效后端，流程
    JSON 逐节点覆盖成 ``"unsafe"`` 在解析期被拒。语言白名单由
    ``_code_allowed_languages_for_worker`` 解析（plaita#29）：默认只放行
    ``python``，``language: "js"`` 在解析期被拒。
    """
    from plaita.node import register_code_node

    backend = _code_backend_for_worker()
    try:
        register_code_node(default_backend=backend,
                           allowed_backends=_code_allowed_backends_for_worker(backend),
                           allowed_languages=_code_allowed_languages_for_worker())
        logger.info("CodeNode 已注册（sandbox_backend=%s）", backend)
        return
    except RuntimeError as e:  # 多为 docker daemon 不可用
        logger.warning("CodeNode 注册失败（backend=%s）：%s —— 降级 subprocess 重试", backend, e)
    register_code_node(default_backend="subprocess",
                       allowed_backends=_code_allowed_backends_for_worker("subprocess"),
                       allowed_languages=_code_allowed_languages_for_worker())
    logger.info("CodeNode 已注册（sandbox_backend=subprocess，降级）")



# 节点级重试判据沿 __cause__ 链回溯的最大深度。分布式归一化链固定一层
# （FlowErrorException → NodeExecutionError），多留几层防御双重包装。
_NODE_RETRY_CHAIN_MAX_DEPTH = 5

# 队列残留回收（#43）兜底间隔：worker 启动时扫一次，之后每 N 秒一次
# （best-effort，见 RedisStreamTaskQueue.sweep_acked_residue）。
DEFAULT_RESIDUE_SWEEP_INTERVAL_SECONDS = 300.0


# flow 定义指纹的**算法标记**：只有算法口径本身变化（指纹输入的取法、字段规范化）
# 才 bump。resume 时据它分级判定——算法不同但哈希相同说明定义没变（升级导致标签
# 变化），可直接续跑；算法不同且哈希也不同才需要人工显式裁决。
#
# v2（#36）：指纹输入从「`Flow.model_validate` 之后的 `model_dump`」换成**原始存储
# 定义 JSON**。旧口径把引擎的序列化字段集当成定义的一部分——引擎任一次给 Node/
# Flow 增删序列化字段（哪怕带默认值）、pydantic 升版都会让同一份存储定义算出不同
# 指纹，混部舰队（滚动升级窗口 / console 用 PLAITA_PYTHON 指向未同步的 venv）下
# 在途 run 被批量终态化。原始 JSON 与模型演进无关，天然稳定。
FLOW_HASH_ALGO = "flow-raw-json-sortkeys-v2"

# 旧口径的算法标记（`model_dump(mode="json")`）。仅用于识别**存量状态**：这类状态的
# 指纹与 v2 不可互比（同一定义必然算出不同哈希），resume 时按一次性重基线处理，
# 不当作「定义已变更」（见 classify_flow_hash_change 的 rebaseline）。
LEGACY_FLOW_HASH_ALGO = "flow-dump-json-sortkeys-v1"

# 与当前口径**不可互比**、但已确认由旧口径产生的标记集合。None = 早于算法标记字段
# 的构建（0.6.1 及更早）写下的状态——那时 flow_hash 只有 dump 一种口径，故 None 与
# LEGACY_FLOW_HASH_ALGO 同义。
INCOMPARABLE_FLOW_HASH_ALGOS = (None, LEGACY_FLOW_HASH_ALGO)

# resume 显式裁决键：允许在指纹变化时继续续跑（升级导致算法口径变化后的补救入口）。
# 必须由调用方在 resume data 里显式传入，且会打 WARNING + 把新指纹写回状态。
ALLOW_FLOW_HASH_CHANGE_KEY = "allow_flow_hash_change"

# 优雅停机时等待在途任务的上限（秒）；超时则放弃当前步并退出——消息不 ack，
# 由其他 worker 经 XCLAIM 从**步界检查点**续跑（该步会重放，业务节点须幂等）。
DEFAULT_WORKER_DRAIN_TIMEOUT = 30.0


class _CachedDefinition(NamedTuple):
    """一条定义缓存登记：解析后的 ``Flow`` + **原始存储定义**（指纹基准，见 #36）。"""

    flow: Flow
    raw: Mapping[str, Any]


def _drain_timeout_from_env() -> float:
    raw = (os.environ.get("PLAITA_WORKER_DRAIN_TIMEOUT") or "").strip()
    if not raw:
        return DEFAULT_WORKER_DRAIN_TIMEOUT
    try:
        value = float(raw)
    except ValueError:
        logger.warning(
            "PLAITA_WORKER_DRAIN_TIMEOUT=%r 非法，回退默认 %ss", raw, DEFAULT_WORKER_DRAIN_TIMEOUT
        )
        return DEFAULT_WORKER_DRAIN_TIMEOUT
    return max(0.0, value)


def classify_flow_hash_change(
    *,
    stored_hash: Optional[str],
    current_hash: str,
    stored_algo: Optional[str],
    current_algo: str,
    allow_change: bool,
) -> str:
    """resume 时 flow 指纹变化的判定表（纯函数，便于测试与复用）。

    返回：
    - ``no_guard``  ：老状态没有指纹，跳过校验（存量语义）
    - ``match``     ：指纹相同、算法标签也相同
    - ``refresh``   ：指纹相同但算法标签升级——定义确实没变，续跑并刷新标签
    - ``rebaseline``：指纹由**旧口径**算得，与当前口径不可互比（#36）——跨口径比大小
      得不出「定义变没变」的结论，故按一次性重基线放行（WARNING + 指标留痕），
      绝不据此终态化在途执行
    - ``accepted``  ：指纹不同 + 算法标签不同 + 调用方显式放行（升级后的补救入口）
    - ``mismatch``  ：其余指纹不同——按定义变更处理，拒绝续跑并给可执行的提示
    """
    if not stored_hash:
        return "no_guard"
    if stored_hash == current_hash:
        return "match" if stored_algo == current_algo else "refresh"
    if stored_algo in INCOMPARABLE_FLOW_HASH_ALGOS:
        return "rebaseline"
    algo_changed = bool(stored_algo) and stored_algo != current_algo
    if algo_changed and allow_change:
        return "accepted"
    return "mismatch"


def _major_minor(version: str) -> str:
    """取 major.minor（'0.6.1' → '0.6'）；非法值原样返回，免得比较时抛错。"""
    parts = str(version).split(".")
    return ".".join(parts[:2]) if len(parts) >= 2 else str(version)


def _engine_version() -> str:
    """当前引擎版本（只用于观测：记录「谁创建了这个执行」）。"""
    try:
        from plaita import __version__

        return __version__
    except Exception:  # noqa: BLE001 — 版本信息拿不到不能影响执行
        logger.debug("读取引擎版本失败，按 unknown 记录", exc_info=True)
        return "unknown"


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


def _chain_has_resume_protocol_error(exc: BaseException) -> bool:
    """异常链（含自身）中是否含 ``ResumeError``（恢复协议/挂起守卫类错误）。

    #33 配套：``run_distributed`` 把策略层 ResumeError 归一化为
    ``FlowErrorException`` 且原异常挂 ``__cause__``，worker 通用 except 因此
    需要「豁免」判据——挂起节点上的 continue/retry 守卫 ResumeError 描述的是
    **调用方协议错误**（消息类型与执行状态不匹配），不是执行自身失败；
    执行应保持原状（suspended）让真正的决议路径（event/cancel/timeout）进来，
    而不是被终态化成不可逆的 error。
    """
    node: Optional[BaseException] = exc
    depth = 0
    while node is not None and depth <= _NODE_RETRY_CHAIN_MAX_DEPTH + 1:
        if isinstance(node, ResumeError):
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


class NodeFailureTerminalizedError(ValueError):
    """节点失败已**终态化 error**（非瞬态/预算耗尽）——消息必须 poison ack。

    为什么要有这个独立异常（plaita#73，2026-10-10 实证）：

    终态化路径原本抛裸 ``RuntimeError``。``RuntimeError`` **不是**
    ``ValueError``，于是一路落到 ``run()`` 的兜底 ``except Exception``
    ——那条分支**不 ack**、留 pending 等超时回收 ⇒ 消息被反复重投 ⇒
    ``resume_flow`` 见 ``status=error`` 只放行 ``resume_type=retry`` ⇒
    再次撞同一确定性失败 ⇒ 再次终态化 + 再次 ``RuntimeError``……
    **状态早已 error，消息却永远在转**（实测某日同一执行重投 598 次、
    沙箱实例持续占位 5.66 实例小时）。

    更糟的是这条路径**不碰重试计数键**：``INCR`` 位于
    ``_node_failure_retry_decision`` 内、在 ``_is_retryable_node_failure``
    判据**之后**，确定性失败（``exited 1`` / ``AgsError: sync_in`` /
    超时）直接 ``return None`` ⇒ 计数键恒为 1（实测全库 74 个键**全为 1**）
    ⇒ 「预算耗尽」分支从未触发，重投**无界**。

    语义与既有的 ``FlowHashMismatchError`` 同族：**raise 前状态已终态化**，
    run() 按 ``ValueError`` poison ack（终态已可观测，重投只会命中
    ``already_terminal`` 短路，纯属浪费）。刻意用独立子类而非裸
    ``ValueError``，以便 resume 的通用 except 区分「已终态化」与
    「引擎内部其他 ValueError（维持现状终态化路径）」。
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


class ResumeProtocolError(RuntimeError):
    """resume 协议错误（#33）：执行状态与 resume_type 不匹配，执行保持原状。

    挂起守卫类 ``ResumeError``（continue/retry 试图绕过 pending 挂起节点）
    经 ``_chain_has_resume_protocol_error`` 判出后，resume_flow 不终态化、
    把执行留在原状态（suspended/running），以本异常上抛。run() 主循环按
    「消费失败」处理：消息走 ack（执行状态原样可查、事件订阅仍在，真正的
    决议路径 event/cancel/timeout 随时可入），重投只会重复命中同一守卫，
    无意义。刻意**不是** ValueError 子类（防 poison ack 语义误伤）。
    """

    def __init__(self, message: str, execution_id: Optional[str] = None):
        super().__init__(message)
        self.execution_id = execution_id


class NodeExecutionRetryableError(RuntimeError):
    """节点执行可重试失败（波次二任务①）：执行**未**终态化，消息等重投。

    节点异常经 ``_is_retryable_node_failure`` 判为瞬态且重试预算未耗尽时，
    处理函数跳过终态化改抛本异常——磁盘 state 停在最后成功步 checkpoint，
    ``run()`` 据此**显式重投**一份同体新副本（``_requeue_retryable_failure``：
    先入队、后 ack 原条），新副本 resume 时从 checkpoint 重跑失败节点。

    2026-10-09 变更（plaita#52 真因）：此前是「不 ack、留 pending 等
    claim_min_idle_ms 回收」。该语义在**消息已被竞争者 ack 释放**时不成立
    （ExecutionLeaseError 分支为避免烧 delivery 会显式 ack），实测导致执行
    既不重试也不终态、卡在 running 58 分钟。现改为显式重投；仅当重投本身
    失败（Redis 抖动等）才退化为留 pending 交 reclaim。

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
        # 节点耗时采集：**按执行**一个采集器（并发处理多个执行时不串场），只监听
        # on_node_start/on_node_end；落盘时由 _persist_state_or_raise 写入
        # ExecutionState.node_timings。
        self._node_timings: Dict[str, NodeTimingCallback] = {}
        # 沙箱生命周期回调：**按执行**缓存（同一执行的多个 step 必须复用同一个
        # 实例，否则 agent 节点在早先 step 产生的 workspace 快照到 flow 结束那一步
        # 已经丢了）；终态落盘时按持久化上下文释放并回收（_release_sandboxes）。
        self._sandbox_callbacks: Dict[str, Any] = {}
        # 优雅停机（无损升级）：draining = 不再领新任务、等 in-flight 收尾；
        # 超时由 _force_stop_after_drain 兜底退出（消息留 pending 待接管）。
        self._draining = threading.Event()
        self._drain_timer: Optional[threading.Timer] = None
        self._drain_started_at: Optional[float] = None
        # 注意：_draining 在 _drain_event 里惰性兜底，__new__ 构造的骨架 worker 也安全
        # 初始化流程定义缓存，使用TTL缓存（值 = _CachedDefinition：解析结果与原始定义
        # 同条目，见 _load_flow_definition）
        self.flow_definition_cache = TTLCache(maxsize=cache_size, ttl=cache_ttl)
        # resume 指纹兼容门的裁决计数（#36 观测面）：键 = (裁决, 成因)。导出为
        # plaita_resume_guard_total{decision,category}——混部舰队下「引擎换了」与
        # 「定义真被改」在指标上可区分，不必逐条翻 error message。
        self._resume_guard_counts: Dict[Tuple[str, str], int] = {}
        self._resume_guard_lock = threading.Lock()
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

    # G1 唤醒上限（plaita#73 补丁）：同一执行最多被 `resume_type=retry` 唤醒
    # 这么多次，防「唤醒→重置预算→再耗尽→再唤醒」无限循环。默认 2（人工救
    # 一次 + 自动救一次足够；再多说明该失败是确定性的，救不回来）。
    G1_MAX_WAKEUPS = 2
    G1_WAKEUP_COUNTER_TTL_SECONDS = 7 * 86400

    # 确定性失败累计上限（plaita#73 遗留层，2026-10-10）：`exited 1` /
    # `AgsError` / 超时 / 协议错等**不可重试**的失败也要有界。此前它们完全不
    # 进计数器（全库 noderetry 键 74/74 恒为 1 即此故），唯一收敛机制是消息层
    # 重投、而消息层无次数概念 ⇒ 同一执行被重投数百次、沙箱持续占位。
    # 达上限即判「不可救」，与可达上限分开记账（noderetry 是可重试预算，语义不同）。
    DETERMINISTIC_FAILURE_MAX = 12
    DETERMINISTIC_FAILURE_TTL_SECONDS = 7 * 86400

    def _deterministic_failure_key(self, execution_id: str) -> str:
        """确定性失败计数键：``{ns}:execution:nofail:{id}``（租户路由同上）。"""
        return f"{tenant_namespace(current_tenant())}:execution:nofail:{execution_id}"

    def _read_deterministic_failure_count(self, execution_id: str) -> int:
        """读确定性失败计数；无 redis / 键缺失 / 异常 → 0（放行，保守）。"""
        redis_client = getattr(self, "redis_client", None)
        if redis_client is None or not hasattr(redis_client, "get"):
            return 0
        try:
            raw = redis_client.get(self._deterministic_failure_key(execution_id))
            return int(raw) if raw is not None else 0
        except Exception as e:  # noqa: BLE001 — 读失败按 0（不误判不可救）
            logger.warning("确定性失败计数读取失败（按 0）: %s: %s", execution_id, e)
            return 0

    def _record_deterministic_failure(self, execution_id: str) -> int:
        """自增确定性失败计数（INCR + 7d TTL），返回自增后的值。"""
        redis_client = getattr(self, "redis_client", None)
        if redis_client is None or not hasattr(redis_client, "incr"):
            return 1
        try:
            key = self._deterministic_failure_key(execution_id)
            pipe = redis_client.pipeline(transaction=True)
            pipe.incr(key)
            pipe.expire(key, self.DETERMINISTIC_FAILURE_TTL_SECONDS)
            return int(pipe.execute()[0])
        except Exception as e:  # noqa: BLE001 — 计数失败按 1（不阻断现状语义）
            logger.warning("确定性失败计数自增失败（按 1）: %s: %s", execution_id, e)
            return 1

    def _deterministic_failure_exhausted(self, execution_id: str) -> bool:
        """该执行是否已判「确定性失败不可救」（达上限）。"""
        return (self._read_deterministic_failure_count(execution_id)
                >= self.DETERMINISTIC_FAILURE_MAX)

    def _g1_wakeup_key(self, execution_id: str) -> str:
        """G1 唤醒计数键：``{ns}:execution:g1wakeups:{id}``（租户路由同上）。"""
        return f"{tenant_namespace(current_tenant())}:execution:g1wakeups:{execution_id}"

    def _read_g1_wakeup_count(self, execution_id: str) -> int:
        """读 G1 唤醒计数；无 redis / 键不存在 / 异常 → 0（放行，保守）。"""
        redis_client = getattr(self, "redis_client", None)
        if redis_client is None or not hasattr(redis_client, "get"):
            return 0
        try:
            raw = redis_client.get(self._g1_wakeup_key(execution_id))
            return int(raw) if raw is not None else 0
        except Exception as e:  # noqa: BLE001 — 读失败按 0（不阻断人工救援）
            logger.warning("G1 唤醒计数读取失败（按 0 放行）: %s: %s", execution_id, e)
            return 0

    def _g1_wakeup_budget_exhausted(self, execution_id: str) -> bool:
        """唤醒是否已达上限——达限即拒绝（执行保持 error 终态）。"""
        return self._read_g1_wakeup_count(execution_id) >= self.G1_MAX_WAKEUPS

    def _record_g1_wakeup(self, execution_id: str) -> None:
        """记一次 G1 唤醒（INCR + 7d TTL）。"""
        redis_client = getattr(self, "redis_client", None)
        if redis_client is None or not hasattr(redis_client, "incr"):
            return
        try:
            key = self._g1_wakeup_key(execution_id)
            pipe = redis_client.pipeline(transaction=True)
            pipe.incr(key)
            pipe.expire(key, self.G1_WAKEUP_COUNTER_TTL_SECONDS)
            pipe.execute()
        except Exception as e:  # noqa: BLE001 — 计数失败不阻断唤醒（人工救援优先）
            logger.warning("G1 唤醒计数失败（忽略）: %s: %s", execution_id, e)

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
            # 确定性失败（`exited 1` / `AgsError: sync_in` / 超时 / 协议错）：
            # 不重试，但仍要**记账**（plaita#73 遗留层，2026-10-10 实证）。
            #
            # 此前直接 return None ⇒ 这些失败**完全不进任何计数器**（实测全库
            # `noderetry` 键 74/74 恒为 1），于是它们唯一的收敛机制只剩「消息层
            # 重投」，而消息层没有次数概念 ⇒ 同一执行可被重投数百次、沙箱实例
            # 持续占位（实测 5 小时烧 5.66 实例小时、产出 0）。
            #
            # 这里用**独立**计数键（`nofail`）而不是 noderetry：noderetry 的语义
            # 是「可重试预算」（会被 G1 唤醒/PROGRESS 清零），塞进确定性失败会污染
            # 该语义。确定性失败**只增不减**，达上限即判该执行不可救、触发终态化 +
            # 后续重投一律短路（见 `_deterministic_failure_exhausted`）。
            n = self._record_deterministic_failure(execution_id)
            if n >= self.DETERMINISTIC_FAILURE_MAX:
                logger.error(
                    "执行 %s 确定性失败累计达上限（%s/%s）——判定不可救，终态化 error"
                    "（后续重投将短路，不再空转）: %s",
                    execution_id, n, self.DETERMINISTIC_FAILURE_MAX, exc,
                )
            else:
                logger.warning(
                    "执行 %s 确定性失败（不重试）第 %s/%s 次: %s",
                    execution_id, n, self.DETERMINISTIC_FAILURE_MAX, exc,
                )
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
        self._collect_node_timings(execution_id, state)
        # 沙箱回收挂在同一收口（覆盖 start/步进/挂起/终态所有路径）：
        # 非终态是 no-op，终态按上下文快照释放实例
        self._release_sandboxes(execution_id, state)
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

    def _handlers_for(self, execution_id: str) -> list:
        """本次执行要挂的回调列表：用户/观测回调 + 节点耗时采集器
        （+ 沙箱生命周期回收，条件装配）。

        采集器**按执行**新建并登记，避免同一 worker 并发处理多个执行时耗时串场；
        登记表在终态落盘后回收（见 ``_collect_node_timings``）。

        沙箱回收（2026-10-07）：存在沙箱注册表时挂 `SandboxLifecycleCallback`
        —**flow 结束/挂起/异常三条路径都精确释放实例**，不再干等 AGS 侧 timeout
        兜底（AGS 无闲置自动处置，"总寿命"比实际需要长得多）。plaita-nodes 为
        可选依赖，缺装/无注册表时静默跳过（非沙箱部署零行为变化）。
        """
        timing = NodeTimingCallback()
        self._node_timings[execution_id] = timing
        handlers = [*self.callback_handlers, timing]
        sandbox_cb = self._sandbox_lifecycle_for(execution_id)
        if sandbox_cb is not None:
            handlers.append(sandbox_cb)
        return handlers

    def _sandbox_lifecycle_for(self, execution_id: str):
        """按执行取沙箱生命周期回调（缓存复用）。

        ⚠️ 分布式步进下**每步都会新建 handlers 列表**——若每步都造一个新回调，
        它只在当步收集快照，到 flow 结束那一步早已空空如也，终态释放静默失效
        （2026-10-08 实测：plaita#22 跑完 24 分钟后沙箱仍 running、日志零条
        ``sandbox lifecycle``）。因此同一执行必须复用同一实例。
        """
        cb = self._sandbox_callbacks.get(execution_id)
        if cb is None:
            cb = self._sandbox_lifecycle_handler()
            if cb is not None:
                self._sandbox_callbacks[execution_id] = cb
        return cb

    def _release_sandboxes(self, execution_id: str, state: ExecutionState) -> None:
        """终态落盘时释放本执行用过的沙箱实例（跨 step/跨进程都成立）。

        快照来源=**持久化上下文**的 ``$NODE.*.workspace``（
        ``collect_workspace_snapshots``）——进程内累积在分布式路径不可靠（见
        ``_sandbox_lifecycle_for``）。成功（completed）→ kill 不留现场；失败/取消
        → pause 保现场（可恢复）。best-effort：任何异常只告警，绝不拖累落盘。
        """
        if getattr(state, "status", "") not in TERMINAL_EXECUTION_STATUSES:
            return
        cb = self._sandbox_lifecycle_for(execution_id)
        self._sandbox_callbacks.pop(execution_id, None)
        if cb is None:
            return
        try:
            from plaita_nodes.sandbox import collect_workspace_snapshots
        except Exception:  # noqa: BLE001 — 可选依赖
            return
        try:
            snaps = collect_workspace_snapshots(getattr(state, "context", None) or {})
        except Exception as exc:  # noqa: BLE001
            logger.warning("沙箱快照收集失败（忽略）: %s", exc)
            return
        if not snaps:
            return
        keep = getattr(state, "status", "") != "completed"   # 失败/取消→保现场
        try:
            results = cb.drain(snaps, phase="terminal", keep_data=keep)
            if results:
                logger.info("终态沙箱释放 %s: %s", execution_id, results)
        except Exception as exc:  # noqa: BLE001 — 回收失败由 AGS TTL 兜底
            logger.warning("终态沙箱释放失败（忽略）: %s", exc)

    @staticmethod
    def _sandbox_lifecycle_handler():
        """惰性装配沙箱生命周期回调；未装 plaita-nodes 或无沙箱注册表 → None。"""
        if os.environ.get("PLAITA_SANDBOX_LIFECYCLE", "1") == "0":
            return None
        try:
            from plaita_nodes.lifecycle import SandboxLifecycleCallback
        except Exception:  # noqa: BLE001 — 可选依赖/导入差异都跳过
            return None
        try:
            from plaita_nodes import sandbox as _sb
            specs = _sb.load_sandboxes()
        except Exception:  # noqa: BLE001 — 无注册表=非沙箱部署
            return None
        if not specs:
            return None
        try:
            return SandboxLifecycleCallback(sandboxes=specs)
        except Exception:  # noqa: BLE001 — 装配失败绝不拖累执行
            return None

    def _collect_node_timings(self, execution_id: str, state: ExecutionState) -> None:
        """把本次执行的节点耗时并进状态（在**唯一的落盘收口**上做，覆盖 start/步进/
        挂起/终态所有路径）。

        - 与状态里已有的 ``node_timings`` **合并**而不是覆盖：resume 可能发生在
          另一个进程，旧节点的时间不能被新进程的采集器抹掉；
        - 终态落盘后回收采集器，避免长跑 worker 泄漏。
        """
        timing = self._node_timings.get(execution_id)
        if timing is None:
            return
        merged = dict(state.node_timings or {})
        merged.update(timing.snapshot())
        state.node_timings = merged
        if getattr(state, "status", "") in TERMINAL_EXECUTION_STATUSES:
            self._node_timings.pop(execution_id, None)
    
    @staticmethod
    def _definition_cache_key(flow_id: str, version: Optional[str]) -> str:
        """定义缓存键（含租户段：同名流程可共存于不同租户 namespace）。"""
        return f"{current_tenant()}:{flow_id}:{version or 'latest'}"

    def _load_flow_definition(
        self, flow_id: str, version: Optional[str] = None
    ) -> Tuple[Flow, Dict[str, Any]]:
        """取「解析后 ``Flow`` + ``Flow.model_validate`` 之前的原始存储定义」。

        两者同键成对缓存：指纹必须算在**原始定义**上（#36），若原始定义与解析
        结果分属两个缓存，任一侧被 TTL 挤掉都会让指纹口径静默降级。

        Returns:
            ``(Flow, 原始存储定义 dict)``——命中的永远是成对的两个，不会出现
            「Flow 在缓存里但原始定义被挤掉」导致的指纹口径静默降级。

        Raises:
            ValueError: 如果找不到流程定义或版本不匹配
        """
        cache_key = self._definition_cache_key(flow_id, version)

        # 尝试从缓存获取
        cached = self.flow_definition_cache.get(cache_key)
        if cached is not None:
            logger.info("从缓存获取流程定义: %s", cache_key)
            return cached.flow, cached.raw

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

        self.flow_definition_cache[cache_key] = _CachedDefinition(flow, flow_definition)

        return flow, flow_definition

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
        return self._load_flow_definition(flow_id, version)[0]

    def get_flow_definition_raw(
        self, flow_id: str, version: Optional[str] = None
    ) -> Dict[str, Any]:
        """取存储层的原始流程定义（``Flow.model_validate`` 之前的 dict）。

        指纹口径的基准（#36）：与引擎的模型演进无关，同一份存储定义在任何
        plaita/pydantic 版本下算出同一指纹。
        """
        return self._load_flow_definition(flow_id, version)[1]

    # ---- flow 定义指纹（波次二任务②，#36 改口径）----

    def _compute_flow_hash(self, definition: "Flow | Mapping[str, Any]") -> str:
        """flow 定义指纹（sha256 of 规范化 JSON）。

        - **原始存储定义**（``Mapping``，``Flow.model_validate`` 之前）：正式口径
          （``FLOW_HASH_ALGO``）。引擎给 Flow/Node 增删序列化字段、pydantic 升版
          都不改存储 JSON，故不漂移；定义内容真的被改（改图、改参数、改版本号、
          显式写出/删除 legacy 键）仍然改指纹——反过来说，「同一份存储定义在任何
          引擎版本下同哈希」正是 resume 守卫要的语义。
        - 已解析的 ``Flow``：退回旧口径（``model_dump(mode="json")``），只用于
          拿不到原始定义的注入路径（子类/单测覆写了 ``get_flow_definition``）；
          两种口径不可互比，调用方须按 ``_definition_fingerprint`` 给出的算法
          标记落盘。
        """
        payload_source = (
            definition.model_dump(mode="json")
            if isinstance(definition, Flow)
            else definition
        )
        payload = json.dumps(
            payload_source,
            sort_keys=True,
            ensure_ascii=False,
            default=str,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def _definition_fingerprint(
        self, flow_id: str, version: Optional[str], flow: Flow
    ) -> Tuple[str, str]:
        """``(指纹, 算法标记)``：按原始存储定义算（正式口径），拿不到则退回旧口径。

        原始定义不可得的唯一场景是``get_flow_definition``被覆写（子类/单测注入
        ``Flow``）——此时退回 dump 口径并**按旧标记落盘**，让 resume 侧的判定表
        知道「这个哈希与正式口径不可比」，而不是静默按新标记冒充。
        """
        try:
            raw = self.get_flow_definition_raw(flow_id, version)
        except ValueError:
            raw = None
        if raw is None:
            logger.warning(
                "定义 %s@%s 的原始存储定义不可得（调用方注入了 Flow），指纹退回"
                "解析后 dump 口径（%s）——与 %s 不可互比",
                flow_id,
                version or "latest",
                LEGACY_FLOW_HASH_ALGO,
                FLOW_HASH_ALGO,
            )
            return self._compute_flow_hash(flow), LEGACY_FLOW_HASH_ALGO
        return self._compute_flow_hash(raw), FLOW_HASH_ALGO

    def _record_resume_guard(self, decision: str, category: str) -> None:
        """记一次 resume 指纹兼容门裁决（导出于 ``/metrics``，见 metrics_text）。

        ``category`` 把「定义真被改」（同口径失配 → flow_definition_changed）与
        「状态由别的口径/构建写下」（口径标记不同 → engine_version_drift，疑似
        混部）/「指纹由旧口径算得」（legacy_algo）分开——混部舰队下整批终态化在
        指标上一眼可辨，不必逐条读 message。
        """
        counts = getattr(self, "_resume_guard_counts", None)
        lock = getattr(self, "_resume_guard_lock", None)
        if counts is None or lock is None:  # __new__ 构造的骨架 worker（测试）无此字段
            return
        key = (str(decision), str(category))
        with lock:
            counts[key] = counts.get(key, 0) + 1

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
          ``_handle_resume`` 的 pending 校验拒绝（ResumeError）；#33 起
          resume_flow 对 suspended+continue 直接幂等短路，重入队更是无谓。
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
            # 崩溃/重试搁浅的 start：重入队 resume 接续执行。若原 start 持有者
            # 仍在推进，重入队的 resume 会被执行租约拦下（ExecutionLeaseError，
            # run() 对该分支 ack 释放）；持有者已死则租约过期后由重入队的
            # resume 从 checkpoint 接管——两条路都不产生并发推进（#23）。
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

        全程持有 ``execution_id`` 执行租约（先行落 running 行之前取得，处理
        结束 finally 释放）：队列按 ``claim_min_idle_ms``（默认 60s）把长任务
        的 pending 消息 XCLAIM 重派给空闲 worker——无租约的 start 在多 worker
        池下跑超过 60s 必被重派双跑（#23）。另一 worker 已持租约（消息重派/
        并发投递）时抛 ``ExecutionLeaseError``，本消息不 ack 留待处理。

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
            # P0 可见性修复（keeper 迁移设计稿 §5.5 清单①，43828aa）：
            # execution_id 优先吃 BFF 预铸（随消息透传，提交方即刻可轮询），
            # 否则就地铸造；行 id 与 result.execution_id 天然一致。异常遗留
            # 的 running 行是 zombie，交 reaper/心跳年龄判定处置。
            # 位置前移到执行器构造之前：节点耗时采集器按执行登记，需要 id 已定。
            execution_id = execution_id or uuid.uuid4().hex

            # 创建流程执行器并执行流程
            # 复用同一个 FlowExecution 贯穿所有分布式步骤, 保留用户回调
            execution = FlowExecution(
                event_bus=self.event_bus,
                callback_handlers=self._handlers_for(execution_id),
            )
            execution.mode = ExecutionMode.DISTRIBUTED
            self._bind_observers(execution)

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
            # 查得到此行、cancel 有锚。flow_hash：对**原始存储定义**计算指纹
            # （波次二任务②/#36 口径）随行落盘——resume 时与当前定义比对，防
            # 运行中改定义后续跑走错分支；原始 JSON 与模型演进无关，故引擎/
            # pydantic 版本漂移本身不改指纹。engine_version：写下本行的引擎
            # 版本，resume 用它区分「解读代码换了」与「定义真被改」。
            flow_hash, flow_hash_algo = self._definition_fingerprint(flow_id, version, flow)
            state = ExecutionState(
                execution_id=execution_id,
                flow_id=flow_id,
                flow_version=version,
                flow_hash=flow_hash,
                flow_hash_algo=flow_hash_algo,
                engine_version=_engine_version(),
                tenant_id=current_tenant(),
                context={},
                status="running",
                start_time=datetime.now().isoformat(),
                invoker="worker"
            )

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

            # 租约（#23 扩展 A′）：**在落 running 行之前 acquire**，持有整个
            # start 处理窗口。A′（2026-10-06）只把租约对齐到 run_distributed
            # 之前，仍在先行落行**之后**——「落行 → acquire」间可被别的 worker
            # 抢先 XCLAIM 同一 start 消息（>claim_min_idle_ms 即可），抢占者
            # 先 acquire 成租约，原持有者 acquire 失败礼貌退出，但其 running
            # 行已被抢占者覆写。移动到落行前同时消除 A′ 的三个后果：
            # ①多 worker 抢到同一 start 消息时无闸可拦，双方同时跑同 id（双跑）；
            # ②死信守卫按「租约是否被持有」判活，start 执行恒"租约空"→ 长步
            #   误判持有者已死 → 误死信 + 重入队，长任务被反复打断；
            # ③抢占者白烧 5 次 delivery 配额。
            # 对齐 resume：acquire → 看门狗续租 → finally 释放。acquire 失败
            # 即抛 ExecutionLeaseError（run() 走不 ack/重投语义），抢占者礼貌退出。
            # holder 嵌入注册表 instance id（#50）：其他 worker 在租约冲突时
            # 可反解持有者并核其在册与否，区分「活持有者」与「死后 TTL 尾巴」。
            holder = new_holder_token(
                prefix="start", instance_id=getattr(self, "instance_id", None)
            )
            lease_value, fence_token = self._acquire_lease(execution_id, holder)
            if lease_value is None:
                raise ExecutionLeaseError(
                    f"execution {execution_id} is leased by another worker; "
                    "refuse concurrent start"
                )
            fence_token_reset = None
            if fence_token is not None:
                fence_token_reset = set_current_fence_token(fence_token)

            # 落 running 行（持租约窗口内）：与本执行此后所有写同 fence 世代，
            # 不会被后续接管者的 fenced CAS 拒绝序列排斥。
            self._persist_state_or_raise(execution_id, state, "start")

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
            # （波次二任务③）/ **租约被他人持有**（A′ + #23，租约覆盖整个
            # start 窗口含先行落行）：原样上抛给 run() 按不 ack 语义处理——
            # ExecutionLeaseError 尤其关键：run() 的
            # `except ExecutionLeaseError` 分支据此 note_lease_conflict 并把消息
            # 留在 pending 等租约过期后 reclaim；若被下方通用 except 包成
            # RuntimeError，run() 会误 ack 消息 → 抢占者把别人的活执行 ack 掉，
            # 且真实持有者崩溃后无人 reclaim（执行永久失联）。绝不能包。
            raise

        except Exception as e:
            logger.error("执行流程出错: %s", e, exc_info=True)
            # 波次③步内取消：start 路径首个节点在途命中取消监听 →
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
        if retry_wakeup and self._g1_wakeup_budget_exhausted(execution_id):
            # G1 唤醒**有界**（plaita#73 补丁，2026-10-10 实证）：
            # 每次唤醒都会清零节点重试预算（下方 retry_wakeup 分支），于是
            # 「唤醒 → 重置预算 → 跑 5 次 → 又终态化 → 再唤醒」可以无限循环。
            # 实测某执行 07:20/07:21/07:23 连续被唤醒、每轮烧 5 次 impl；
            # 调用方是 inflight-watch 的自动 resume（flow 侧 `resume_type=retry`，
            # 每 15 分钟一轮，账本「同 exec 只 resume 一次」挡不住跨轮重复）。
            # 达上限即拒绝唤醒、保持 error 终态（幂等返回，不抛异常——
            # 抛异常会让调用方以为失败并重试）。
            logger.error(
                "执行 %s G1 唤醒次数达上限（%s/%s），拒绝再次唤醒——"
                "error 终态保留，避免「唤醒重置预算」无限循环",
                execution_id, self._read_g1_wakeup_count(execution_id),
                self.G1_MAX_WAKEUPS,
            )
            return {
                "execution_id": execution_id,
                "status": state_status,
                "already_terminal": True,
                "g1_wakeups_exhausted": True,
            }
        if self._deterministic_failure_exhausted(execution_id) and \
                ResumeType.coerce(resume_type) is ResumeType.RETRY:
            # 确定性失败已达上限（plaita#73 遗留层）：该执行的失败是**确定性**的
            # （`exited 1` / `sync_in` / 超时…），重跑只会再烧一次全额成本。
            # 与 G1 上限同理返回幂等结果、不抛异常（抛异常会让调用方当失败重试），
            # 让消息层重投在此短路、不再空转沙箱。
            logger.error(
                "执行 %s 确定性失败已判不可救（%s/%s），拒绝唤醒重跑——"
                "error 终态保留",
                execution_id, self._read_deterministic_failure_count(execution_id),
                self.DETERMINISTIC_FAILURE_MAX,
            )
            return {
                "execution_id": execution_id,
                "status": state_status,
                "already_terminal": True,
                "deterministic_failure_exhausted": True,
            }
        if state_status in ("completed", "error", "cancelled") and not retry_wakeup:
            logger.info(
                "执行 %s 已是终态 (%s)，跳过重复 resume", execution_id, state_status,
            )
            return {
                "execution_id": execution_id,                "status": state_status,
                "already_terminal": True,
                "result": getattr(state, "result", None),
                "error": getattr(state, "error", None),
            }

        # 挂起幂等短路（#33）：suspended 执行收到 resume_type=continue ——策略层
        # 的 pending 守卫必抛 ResumeError（continue 不允许绕过挂起节点），历史上
        # 这条路会被下方通用 except 终态化成 error：一次重复投递把本可等
        # 事件/审批/延迟恢复的执行永久打封（连 resume_type=event 都被终态短路
        # 拒绝，retry 也救不回）。continue 对挂起执行不携带任何推进语义——
        # checkpoint 未变、事件订阅仍在，正确动作是幂等跳过（ack 消息，状态
        # 原样保留），等真正的决议路径（event/cancel/timeout）来唤醒。
        # resume_type=retry 对挂起执行同样过不了策略层守卫，一并短路——
        # retry 的对象是 error 态断点，不是挂起节点。
        if state_status == "suspended" and ResumeType.coerce(resume_type) in (
            ResumeType.CONTINUE, ResumeType.RETRY,
        ):
            logger.info(
                "执行 %s 处于 suspended（挂起节点待事件决议），resume_type=%s "
                "不携带推进语义，幂等跳过", execution_id, resume_type,
            )
            return {
                "execution_id": execution_id,
                "status": state_status,
                "already_suspended": True,
            }

        # 获取流程版本
        version = state.flow_version

        # 获取流程定义
        flow = self.get_flow_definition(flow_id, version)

        # flow 定义指纹（波次二任务②/#36）：worker 的定义 TTLCache 有 300s
        # 窗口、console engine_sync 可直接覆盖 Redis 定义——挂起执行 resume 用
        # latest 版本定义时，若定义自启动后被改过，``_get_next_from_last``
        # 找不到节点/走错分支且无任何告警。指纹在 lease 内判定（与活 worker
        # 的推进写串行化，防「他人持租约推进中却被误终态化」），不一致 →
        # 终态化 error + raise ValueError（poison ack）——意图是可观测的终态
        # 而不是重投风暴。老状态 flow_hash 为 None → 跳过校验（零回归）。
        #
        # 指纹算在**原始存储定义**上（#36）：引擎/节点包/pydantic 版本变化不改
        # 同一份定义的指纹，故失配只剩「定义真被改」一种解释——混部舰队不再被
        # 解读代码的升级批量终态化（旧口径算的是解析后 model_dump，见
        # LEGACY_FLOW_HASH_ALGO）。
        current_flow_hash, current_algo = self._definition_fingerprint(
            flow_id, version, flow
        )

        # 解析流程定义
        logger.info("恢复流程执行: %s, 执行ID: %s, 恢复类型: %s", flow_id, execution_id, resume_type)

        # holder 嵌入注册表 instance id（#50），语义同 start 路径。
        holder = new_holder_token(
            prefix="resume", instance_id=getattr(self, "instance_id", None)
        )
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

            # flow 定义指纹校验（波次二任务②/#36）在 G1 retry 唤醒**之前**：
            # 定义被改时执行保持 error（hash 不匹配信息落盘），修复定义后
            # 仍可再 retry——G1 的「error 态可反复唤醒」语义不被破坏。
            # 取消是用户意图，已在上方优先放行。写盘走 _persist_state_or_raise
            # （带本 resume 的 fence 世代），随后 raise FlowHashMismatchError
            # → run() 按 ValueError poison ack（终态已可观测，重投无意义）。
            stored_flow_hash = getattr(state, "flow_hash", None)
            stored_algo = getattr(state, "flow_hash_algo", None)
            stored_engine = getattr(state, "engine_version", None)
            current_engine = _engine_version()
            # 成因区分（#36）：**口径标记**是否变化才是成因的判据。v2 指纹算在
            # 原始存储定义上，与引擎版本无关——同口径下哈希不同只能是定义真被
            # 改；口径标记不同才说明该状态由别的构建/口径写下（混部舰队、滚升
            # 窗口、console 用 PLAITA_PYTHON 指向未同步 venv）。engine_version
            # 的漂移只作佐证记进 error（stored/current_engine_version），不当作
            # 成因——它永不随 resume 覆写，按它分类会把此后每一次失配都永久
            # 归到「混部」，把值班视线从真正的定义变更上引开。
            algo_changed = bool(stored_algo) and stored_algo != current_algo
            allow_hash_change = bool(
                isinstance(data, dict) and data.get(ALLOW_FLOW_HASH_CHANGE_KEY)
            )
            hash_decision = classify_flow_hash_change(
                stored_hash=stored_flow_hash,
                current_hash=current_flow_hash,
                stored_algo=stored_algo,
                current_algo=current_algo,
                allow_change=allow_hash_change,
            )
            if hash_decision in ("mismatch", "accepted", "rebaseline"):
                if hash_decision == "rebaseline":
                    # 存量状态（旧口径指纹）：跨口径比大小得不出「定义变没变」的
                    # 结论，据此终态化就是 #36 报的批量误杀。一次性重基线到当前
                    # 口径 + 留痕（WARNING + 指标），执行继续推进。
                    logger.warning(
                        "执行 %s 的 flow 指纹由旧口径算得（%s → %s，引擎 %s → %s），"
                        "跨口径不可比，按一次性重基线放行: stored=%s... current=%s...",
                        execution_id,
                        stored_algo or "unknown",
                        current_algo,
                        stored_engine or "unknown",
                        current_engine,
                        stored_flow_hash[:12],
                        current_flow_hash[:12],
                    )
                    state.flow_hash = current_flow_hash
                    state.flow_hash_algo = current_algo
                    self._persist_state_or_raise(execution_id, state, "flow_hash_rebaseline")
                    self._record_resume_guard("rebaseline", "legacy_algo")
                elif hash_decision == "accepted":
                    # 升级后的人工裁决入口：显式承认「换算法/换定义」并续跑，
                    # 必须留痕（WARNING + 状态里刷新指纹），不静默放行。
                    logger.warning(
                        "执行 %s 的 flow 指纹变化被显式放行续跑（%s）：stored=%s(%s) current=%s(%s)",
                        execution_id,
                        ALLOW_FLOW_HASH_CHANGE_KEY,
                        stored_flow_hash[:12],
                        stored_algo or "unknown",
                        current_flow_hash[:12],
                        current_algo,
                    )
                    state.flow_hash = current_flow_hash
                    state.flow_hash_algo = current_algo
                    self._persist_state_or_raise(execution_id, state, "flow_hash_change_accepted")
                    self._record_resume_guard(
                        "accepted",
                        "engine_version_drift" if algo_changed else "flow_definition_changed",
                    )
                else:
                    # 口径标记不同 = 该状态由别的构建/口径写下（混部/滚升窗口），
                    # 其指纹与当前口径不可比，只能靠显式放行裁决；口径相同而哈希
                    # 不同 = 存储定义确实变了，开关对这类失配**无效**（判定表见
                    # classify_flow_hash_change）。两者都终态化——重投 N 次结果
                    # 相同，只会制造 DLQ 噪音。
                    mismatch_category = (
                        "engine_version_drift" if algo_changed else "flow_definition_changed"
                    )
                    mismatch_msg = (
                        "flow 定义自启动后已变更，hash 不匹配，执行无法安全续跑 "
                        f"(execution_id={execution_id}, flow_id={flow_id}, "
                        f"stored_hash={stored_flow_hash[:12]}..., "
                        f"current_hash={current_flow_hash[:12]}..., "
                        f"stored_algo={stored_algo or 'unknown'}, current_algo={current_algo}, "
                        f"stored_engine={stored_engine or 'unknown'}, "
                        f"current_engine={current_engine})"
                    )
                    logger.error(mismatch_msg)
                    state.status = "error"
                    state.error = {
                        "message": mismatch_msg,
                        # 机器可读成因（#36）：flow_definition_changed /
                        # engine_version_drift（= 口径标记不同，疑似混部）
                        "category": mismatch_category,
                        "stored_flow_hash": stored_flow_hash,
                        "current_flow_hash": current_flow_hash,
                        "stored_flow_hash_algo": stored_algo,
                        "current_flow_hash_algo": current_algo,
                        "stored_engine_version": stored_engine,
                        "current_engine_version": current_engine,
                        # 升级疑似（口径标记变了）：给运维明确下一步，而不是只丢
                        # 一句「hash 不匹配」。提示必须**可执行**——同口径失配下
                        # allow_flow_hash_change 不放行（classify 的 accepted 只
                        # 认口径变化），提示若指向它就等于让值班照做后撞回同一条
                        # 错误。
                        "upgrade_suspected": algo_changed,
                        "hint": (
                            f"该状态由不同指纹口径的构建写下（{stored_algo} → "
                            f"{current_algo}），疑似混部/滚升未收尾；确认流程定义未变"
                            f"且可接受后，在 resume data 里带 {ALLOW_FLOW_HASH_CHANGE_KEY}"
                            ": true 显式放行"
                        ) if algo_changed else (
                            "同口径下指纹不同 = 流程定义自启动后已被改动：必须改回定义或"
                            f"新建执行；{ALLOW_FLOW_HASH_CHANGE_KEY} 只对口径标记不同的"
                            "失配生效，同口径下无效"
                        ),
                    }
                    state.end_time = datetime.now().isoformat()
                    self._persist_state_or_raise(execution_id, state, "flow_hash_mismatch")
                    self._record_resume_guard("mismatch", mismatch_category)
                    raise FlowHashMismatchError(mismatch_msg)
            elif hash_decision == "refresh":
                # 哈希相同 → 定义确实没变，只是算法标记变了（引擎升级）：续跑并刷新标记
                logger.info(
                    "执行 %s 的 flow 指纹一致但算法标记升级（%s → %s），续跑并刷新标记",
                    execution_id,
                    stored_algo,
                    current_algo,
                )
                state.flow_hash_algo = current_algo
                self._record_resume_guard("refresh", "fingerprint_algo_upgraded")

            # 引擎版本跨 minor：只告警不拦截（硬门由 flow_hash 承担，这里给可观测性）
            if stored_engine and _major_minor(stored_engine) != _major_minor(current_engine):
                logger.warning(
                    "执行 %s 由引擎 %s 创建，当前 %s：跨 minor 续跑，"
                    "若流程行为与预期不符请复核定义与节点实现",
                    execution_id,
                    stored_engine,
                    current_engine,
                )

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
                # 记一次唤醒（plaita#73 补丁）：唤醒会重置节点预算，必须计数
                # 才能让「唤醒→重置→耗尽→再唤醒」收敛。达上限的拒绝在上方
                # 入口处（`_g1_wakeup_budget_exhausted`）。
                self._record_g1_wakeup(execution_id)
                logger.info("执行 %s error 态经 retry 放行（重试计数已清零），从断点步进", execution_id)

            # 复用同一个 FlowExecution 贯穿恢复后的所有分布式步骤
            execution = FlowExecution(
                event_bus=self.event_bus,
                callback_handlers=self._handlers_for(execution_id),
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

            # 挂起守卫豁免（#33）：ResumeError 是恢复协议错误（continue/retry
            # 试图绕过 pending 挂起节点等），不是执行自身失败。终态化 error 会
            # 把本可被 event/cancel/timeout 唤醒的挂起执行永久打封（终态短路
            # 从此拒绝一切 resume 类型，死局）。豁免为「保持原状 + 上抛」：
            # suspended 执行保持 suspended，消息被 ack（重投也只会再次命中
            # 同一守卫，重投无意义）；running 执行保持 checkpoint 现状。
            if _chain_has_resume_protocol_error(e):
                logger.warning(
                    "执行 %s (status=%s) 收到与挂起状态不匹配的 resume 请求，"
                    "协议错误不终态化，执行保持原状: %s",
                    execution_id, getattr(state, "status", "?"), e,
                )
                self._finalize_observers()
                raise ResumeProtocolError(
                    f"resume 协议错误（执行保持原状 status={getattr(state, 'status', '?')}）: {e}",
                    execution_id=execution_id,
                ) from e

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

            # plaita#73：终态化**已完成**，必须 poison ack 终止重投循环。
            # 原先抛裸 RuntimeError → 落到 run() 兜底 except → 不 ack → 重投
            # → status=error 只放行 retry → 再撞同一确定性失败 → 无限循环
            # （且全程不碰重试计数键，预算永不耗尽）。
            raise NodeFailureTerminalizedError(
                f"恢复流程执行出错（已终态化 error，消息 poison ack）: {e}"
            )
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
                callback_handlers=self._handlers_for(execution_id),
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
        concurrency: int = 1,
        watchdog_interval_seconds: Optional[float] = None,
        cancel_poll_seconds: Optional[float] = None,
        residue_sweep_interval_seconds: Optional[float] = None,
        metrics_port: Optional[int] = None,
        metrics_host: Optional[str] = None,
        alert_webhook: Optional[str] = None,
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
        # 并发消费线程数（2026-10-07）：>1 时 run() 起 N 个消费线程共享本实例，
        # 单机可同时处理 N 个任务（原为严格串行——一次 read 一条、处理完再读）。
        # 每线程各自持有一个独立 queue 对象（独立 Redis 连接），避免连接竞争；
        # 消息级 at-least-once 与执行级租约语义不变（并发下同一 execution 仍由
        # 租约串行化）。默认 1 = 零行为变化。
        self.concurrency = max(1, int(concurrency))
        self._active_task_count = 0
        self._active_count_lock = threading.Lock()
        self._log_handler = None
        self._consumer_group = consumer_group
        self._claim_min_idle_ms = claim_min_idle_ms
        self._consumer_name = consumer_name
        self._max_deliveries = max_deliveries
        self._dlq_key = dlq_key
        self._task_queue: Optional[RedisStreamTaskQueue] = None

        # #26 观测导出：``/metrics`` 抓取端（0 = 不启动）与死信告警 webhook。
        # 队列计数器（enqueued/acked/dead_lettered…）是**进程内**状态，只有
        # worker 自己持有——console 无法代它导出，故端点必须跑在本进程里。
        self._metrics_port = (
            _metrics_port_from_env() if metrics_port is None else max(0, int(metrics_port))
        )
        self._metrics_host = metrics_host or _metrics_host_from_env()
        self._metrics_server: Optional[MetricsHttpServer] = None
        self._alert_webhook = (
            alert_webhook if alert_webhook is not None else _alert_webhook_from_env()
        )
        self._alerter: Optional[WebhookAlerter] = None

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

        # 队列残留回收（#43）：启动时扫一次 + 每 residue_sweep_interval_seconds
        # 兜底一次，清掉「已 ack 未 XDEL」的 Stream 残留条目（它们只增不减地
        # 计入 XLEN，让运维把队列读数读成假阳性）。非阻塞锁保证并发消费下
        # 同一时刻只有一个扫描在跑（幂等，重复扫描只是浪费一次往返）。
        self._residue_sweep_interval_seconds = (
            DEFAULT_RESIDUE_SWEEP_INTERVAL_SECONDS
            if residue_sweep_interval_seconds is None
            else float(residue_sweep_interval_seconds)
        )
        self._last_residue_sweep: Optional[float] = None
        self._residue_sweep_lock = threading.Lock()
        
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
                    "cache_ttl": cache_ttl,
                    # 引擎版本（#36）：console 可用 PLAITA_PYTHON 给 worker 指定
                    # 业务 venv 解释器，混部舰队里 worker 跑的未必是 console 那套
                    # plaita——此前注册元数据只有队列/缓存项，新旧 worker 在服务页
                    # 长得一模一样，滚升窗口的旧实例完全不可观测。ServiceInfo 被
                    # 心跳线程反复序列化，故注册与每次心跳都带版本。
                    "plaita_version": _engine_version(),
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
            # #26 死信告警接线：同样按签名探测向后兼容（并行合入期的旧队列类
            # 不收该参数）。
            if (
                "on_dead_letter"
                in inspect.signature(RedisStreamTaskQueue.__init__).parameters
            ):
                kwargs["on_dead_letter"] = self._on_dead_letter
            self._task_queue = RedisStreamTaskQueue(self.redis_client, self.queue_name, **kwargs)
        return self._task_queue

    def _on_dead_letter(self, event: Dict[str, Any]) -> None:
        """死信事件出口：未配置 webhook 时空操作（只保留原始 error 日志）。"""
        if self._alerter is not None:
            self._alerter.send(event)

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
        # 按仓拒跑名单（2026-10-07）：与路径亲和独立——路径在本机存在（大仓有
        # 软链、双机都能解析）时仍可按仓名拒收，把重仓挡在小容量机器外。
        deny = _deny_repos()
        if deny:
            raw_repo = params.get("repo")
            if isinstance(raw_repo, str) and raw_repo.strip():
                name = os.path.basename(raw_repo.strip().rstrip("/"))
                if name in deny:
                    return (f"repo={name} 在拒跑名单（PLAITA_WORKER_DENY_REPOS）"
                            "——重仓留给大容量 worker")
        return None

    def _lease_conflict_ack_safe(self, body: Any) -> bool:
        """ExecutionLeaseError 时 ack 是否安全（#50 加固）。

        「租约冲突 ack 释放」（2026-10-06 修）的前提是**有人在跑**：租约被
        持有 = 持有者正常推进，ack 只释放重复载体。但租约键在持有者进程
        死后仍可存活 ≤TTL（看门狗最后一次续期的尾巴，120s）——维护窗重启
        /崩溃后的这段窗口里，另一 worker 按 claim_min_idle（60s）回收重投
        消息，命中该分支把**非终态执行唯一的重投载体** ack 掉：执行停在
        running，恢复退化成 zombie reap（plaita#50 双机两例实证）。

        判据：从租约值反解持有者 instance id（``new_holder_token`` 嵌入）
        → 查服务注册表在册与否。注册表心跳 10s / TTL 30s，持有者死后
        ≤30s 消失，早于 60s 的首次回收——在册 = 活持有者，ack 安全；
        不在册 = TTL 尾巴，ack 会吞掉重试载体。

        拿不到任何信号（旧格式租约 / 未启用注册表 / Redis 瞬断）一律按
        「存活未知」返回 True 保持 ack——宁可回到 zombie reap 兜底，不
        回归 2026-10-06 修掉的假死信风暴（对端每 60s XCLAIM 烧 delivery）。
        回滚开关 PLAITA_DISABLE_HOLDER_LIVENESS=1 整体旁路（回到无条件 ack）。
        """
        if _holder_liveness_disabled():
            return True
        if not (isinstance(body, dict) and body.get("execution_id")):
            return True
        if not getattr(self, "_enable_registry", False):
            return True
        token = set_current_tenant(body.get("tenant_id"))
        try:
            try:
                lease_value = self.execution_lease.get_holder(body["execution_id"])
            except Exception as exc:  # noqa: BLE001 — 瞬断按存活未知处理
                logger.debug("租约持有者存活核算读租约失败（按存活未知）: %s", exc)
                return True
        finally:
            reset_current_tenant(token)
        instance = holder_instance_id(lease_value)
        if not instance:
            return True
        registry_key = (
            f"{ServiceRegistry.REGISTRY_PREFIX}:{self.SERVICE_TYPE}:{instance}"
        )
        try:
            registered = bool(self.redis_client.exists(registry_key))
        except Exception as exc:  # noqa: BLE001 — 瞬断按存活未知处理
            logger.debug("租约持有者存活核算查注册表失败（按存活未知）: %s", exc)
            return True
        if registered:
            return True
        logger.warning(
            "执行 %s 租约持有者 %s 已不在服务注册表（持有者已死、租约处 ≤%ss "
            "TTL 尾巴）——不 ack，留 pending 等租约过期后 reclaim 续跑 (#50)",
            body.get("execution_id"),
            instance,
            getattr(self, "lease_ttl_seconds", "?"),
        )
        return False

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

    def _requeue_retryable_failure(self, task: Any, queue: Any) -> bool:
        """节点可重试失败：**显式重投**一份同体新副本（ack 原条）。

        为什么不能只依赖「不 ack 留 pending」的回收语义（2026-10-09 实证，
        执行 030ef793981743f6a9c217187022bb38 / plaita#28 卡 58 分钟）：

        - 同一执行的消息可能已被**竞争者的 ExecutionLeaseError 分支 ack 掉**
          （该分支为「避免烧 delivery」显式 ack，见 run() 内注释）；
        - 此时消息不在 PEL 里，reclaim/XCLAIM 无从回收 → 依赖它的重投
          **永不发生**；而处理函数又刻意不终态化 → 执行既不重试也不落终态，
          一直挂在 running，只能等 keeper 的 zombie 线（默认 2h）或人工 resume。

        新副本 delivery 归 1（`enqueue_task` 语义），节点重试预算由独立计数键
        约束，因此不会无限重投。顺序为**先入队、后 ack**：中间崩溃只会产生
        重复投递（租约机制天然去重），绝不会丢消息。

        返回 True=已重投并 ack；False=退化为留 pending（由 reclaim 回收）。
        """
        body = getattr(task, "body", None)
        mid = getattr(task, "message_id", "?")
        if not isinstance(body, dict):
            logger.info("任务 %s 可重试失败但无体可重投，留 pending 交 reclaim", mid)
            return False
        try:
            queue.enqueue(dict(body))
        except Exception as e:  # noqa: BLE001 — 重投失败退化为 reclaim（绝不丢消息）
            logger.warning("任务 %s 显式重投失败（退化为留 pending 回收）: %s", mid, e)
            return False
        try:
            queue.ack(mid)
        except Exception as e:  # noqa: BLE001 — ack 失败=重复投递，不致命
            logger.warning("任务 %s 重投后 ack 失败（将产生重复投递，租约去重）: %s", mid, e)
            return True
        return True

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

        # 队列残留回收（#43）：启动时先扫一次，之后由消费循环按间隔兜底
        self._sweep_residue_if_due(queue)

        # 租约看门狗（波次②）：持租约执行每 TTL/3 续租，防长步被 XCLAIM 抢占
        self._start_lease_watchdog()
        # 取消监听（波次③）：轮询取消标志键，命中即中止在途节点
        self._start_cancel_watcher()

        # 注册服务
        if self._enable_registry:
            self.register_service()
            # 启动控制监听
            self.start_control_listener()

        # #26 观测导出：/metrics 抓取端 + 死信告警 webhook（未配置 = 无操作）
        self._start_metrics_server()
        self._start_alerting()

        logger.info(
            "流程工作器已启动，监听 stream: %s (group=%s, consumer=%s, concurrency=%d)",
            self.queue_name,
            self._consumer_group,
            queue.consumer_name,
            self.concurrency,
        )

        # 并发消费（2026-10-07）：concurrency=1 时原地串行（零行为变化）；
        # >1 时起 N 个消费线程共享本实例。线程各自跑 _consume_loop，靠
        # XREADGROUP 的天然互斥（Redis 保证一条消息只投给一个 consumer 的一次读）
        # 分到不同任务；共享 queue 对象线程安全（redis-py 连接池）。
        threads: list = []
        try:
            if self.concurrency <= 1:
                self._consume_loop(queue)
            else:
                for i in range(self.concurrency):
                    t = threading.Thread(
                        target=self._consume_loop,
                        args=(queue,),
                        name=f"flow-consume-{i}",
                        daemon=True,
                    )
                    t.start()
                    threads.append(t)
                logger.info("已启动 %d 个并发消费线程", len(threads))
                # 主线程等任一消费线程退出（正常只在 stop() 后发生）
                for t in threads:
                    while t.is_alive():
                        t.join(timeout=1.0)
                        if not self._running:
                            break
        finally:
            self.stop()

    def _sweep_residue_if_due(self, queue: RedisStreamTaskQueue) -> None:
        """按间隔兜底回收「已 ack 未 XDEL」的队列残留条目（#43）。

        与消费循环同线程执行，只在到期时（`_last_residue_sweep` 为空 = 启动
        后第一次）扫一轮；并发消费下多线程共享实例，非阻塞锁保证不并发扫。
        队列类已实现 `sweep_acked_residue` 才调用（并行合入期向后兼容——注入/
        子类化的旧队列没有该方法，AttributeError 会炸掉消费线程）。
        """
        interval = self._residue_sweep_interval_seconds
        if interval <= 0:
            return
        sweep = getattr(queue, "sweep_acked_residue", None)
        if not callable(sweep):
            return
        now = time.monotonic()
        last = self._last_residue_sweep
        if last is not None and now - last < interval:
            return
        if not self._residue_sweep_lock.acquire(blocking=False):
            return
        try:
            self._last_residue_sweep = time.monotonic()
            sweep()
        finally:
            self._residue_sweep_lock.release()

    def _consume_loop(self, queue: RedisStreamTaskQueue) -> None:
        """单条消费循环（原 run() 主体）。可被 1 或 N 个线程并发执行。

        ``draining``（无损升级）后不再领新任务：循环条件的检查发生在**任务边界**，
        所以在途任务会跑完，随后各消费线程自然退出；残留 sweep 也一并停——
        它属于「领任务」的配套动作，draining 期间不该再触发。
        """
        while self._running and not self._drain_event.is_set():
            self._sweep_residue_if_due(queue)
            # 分片阻塞读取（2026-09 分布式评审 P2-2）：XREADGROUP 的
            # BLOCK 无法被信号中断出循环，整块 10s 会让 SIGTERM 后的
            # worker 继续抢任务最长 10s。切成 ≤1s 的窗口，停机延迟
            # 上限 ≈1s，空轮询的 Redis 往返开销可忽略。
            task = queue.read(block_ms=min(self.read_block_ms, 1_000))
            if not task:
                continue

            self._bump_active(+1)
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
                # 另一 worker 持有该执行的租约（A′ 起点后的正常竞争态）：
                # 持有者**活着**时 ack 释放本消息（2026-10-06 修）。
                #
                # 原行为「不 ack 留 pending」的实测问题：持有者跑长节点
                # （agentrun 分钟级）期间，对端每 claim_min_idle_ms(60s)
                # XCLAIM 一次，**每次让 Redis deliveries +1** → 烧到超限
                # 触发死信（实测 delivery 达 22/31），死信守卫虽正确拦下
                # （租约仍在→跳过）并最终重入队，但产生大量假死信污染 DLQ、
                # 浪费队列往返。
                #
                # 为何 ack 对活持有者是安全的：任务**没有丢**——它在持有者
                # 手里正常推进。但「租约在」≠「持有者在」：持有者进程死后
                # 租约键还有 ≤TTL 的尾巴（#50 双机实证：维护窗 SIGTERM 杀
                # 持有者 → 节点失败重试的重投消息在 60s 被对端回收 → 命中
                # 本分支被 ack → 非终态执行唯一重投载体被吞，执行停 running
                # 退化成 2h zombie reap）。故 ack 前先经
                # ``_lease_conflict_ack_safe`` 核实持有者仍在注册表：
                # 在册（活持有者）→ ack；不在册（TTL 尾巴）→ 不 ack 留
                # pending，等租约过期后下一轮 reclaim 正常取得租约从
                # checkpoint 续跑（代价 ≤TTL+60s，远小于 2h reap）。
                # 亲和闸路径（TaskNotForThisWorker）保留"交接重入队"是对的：
                # 那里**没人**能跑该任务，必须留给对端。
                if self._lease_conflict_ack_safe(task.body):
                    queue.note_lease_conflict()
                    queue.ack(task.message_id)
                    acked = True
                    logger.info(
                        "任务 %s 执行被他人持租约（持有者在册，正常竞争），已 ack 释放避免烧 delivery: %s",
                        task.message_id,
                        exc,
                    )
                else:
                    queue.note_lease_conflict()
                    logger.warning(
                        "任务 %s 租约持有者已死，消息留 pending 等租约过期后重投，防重试载体被吞 (#50): %s",
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
                # 2026-10-09 修复（plaita#52 真因）：**显式重投**，不再把
                # 重投的成败押在「消息仍在 PEL」上——同执行可能已被竞争者的
                # ExecutionLeaseError 分支 ack 释放（"避免烧 delivery"），
                # 此时 reclaim 无条目可回收，重投永不发生而执行又不终态化，
                # 结果卡在 running 直到 zombie 线/人工 resume（实测 58 分钟）。
                if self._requeue_retryable_failure(task, queue):
                    acked = True
                    logger.warning(
                        "任务 %s 节点执行失败：已显式重投同体新副本（原 delivery=%s）: %s",
                        task.message_id,
                        task.delivery_count,
                        exc,
                    )
                else:
                    logger.warning(
                        "任务 %s 节点执行失败待重投 (delivery=%s): %s",
                        task.message_id,
                        task.delivery_count,
                        exc,
                    )
            except ResumeProtocolError as exc:
                # resume 协议错误（#33）：执行状态与 resume_type 不匹配（如
                # continue 打在挂起执行上）。执行**未**终态化、保持原状——
                # 错误已可观测（状态行/日志），消息重投只会重复命中同一守卫
                # 且烧 delivery，ack 掉。非终态执行不需要重投载体：决议路径
                # （event/cancel/timeout resume）各有独立消息。
                queue.ack(task.message_id)
                queue.note_poison()
                acked = True
                logger.warning(
                    "任务 %s resume 协议错误（执行保持原状不终态化）: %s",
                    task.message_id,
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
                self._bump_active(-1)
                if not acked:
                    logger.debug("任务 %s 留在 pending 等待回收", task.message_id)

    def _bump_active(self, delta: int) -> None:
        """原子调整在跑任务数并刷新注册表 active_tasks（并发下多线程安全）。"""
        with self._active_count_lock:
            self._active_task_count = max(0, self._active_task_count + delta)
            count = self._active_task_count
        if self._enable_registry:
            self.update_registry_info(active_tasks=count)
    
    @property
    def _drain_event(self) -> threading.Event:
        """draining 标志（惰性创建）。

        某些路径会以 ``__new__`` 构造「骨架 worker」（例如只测消费循环调度的单测），
        不走 ``__init__``。把状态做成惰性属性后，这些路径同样安全——否则会在
        循环条件里直接 AttributeError。
        """
        event = self.__dict__.get("_draining")
        if event is None:
            event = threading.Event()
            self.__dict__["_draining"] = event
        return event

    @property
    def draining(self) -> bool:
        return self._drain_event.is_set()

    def request_drain(self, reason: str = "manual") -> None:
        """进入 draining：不再领新任务、注册表标 draining，并启动**有界**等待。

        有界是关键：单个节点步可能跑几十分钟（如 HITL gate），等它做完等于把
        停机窗口拉到不可接受。超时后 ``_force_stop_after_drain`` 放弃当前步并
        退出——消息不 ack，留在 pending 由其他 worker 经 XCLAIM 从**步界检查点**
        续跑。代价是那一步会重放，业务节点须幂等（at-least-once 契约）。
        """
        with self._active_count_lock:
            if self._drain_event.is_set():
                return
            self._drain_event.set()
            self._drain_started_at = time.time()
        if self._enable_registry:
            # 注册表可见：编排/K8s 就绪探针据此把这台从流量里摘掉
            self.update_registry_info(status="draining")
        timeout = _drain_timeout_from_env()
        logger.info(
            "worker 进入 draining（%s）：不再领新任务，等待 %d 个在途任务收尾（上限 %.0fs）",
            reason,
            self._active_task_count,
            timeout,
        )
        if timeout <= 0:
            self._force_stop_after_drain(timeout)
            return
        timer = threading.Timer(timeout, self._force_stop_after_drain, args=(timeout,))
        timer.daemon = True
        self._drain_timer = timer
        timer.start()

    def _force_stop_after_drain(self, timeout: float = 0.0) -> None:
        """drain 超时兜底：放弃当前步并让 run() 收尾退出（消息不 ack）。"""
        if not self._drain_event.is_set():
            return
        active = self._active_task_count
        if active <= 0:
            return
        logger.warning(
            "drain 超时（%.0fs）仍有 %d 个任务在跑：放弃当前步并退出；"
            "这些消息不 ack，将由其他 worker XCLAIM 后从步界检查点续跑（该步会重放）",
            timeout,
            active,
        )
        self._running = False

    def wait_for_idle(self, timeout: Optional[float] = None) -> bool:
        """等所有在途任务结束；超时返回 False（调用方决定是否强退）。"""
        deadline = None if timeout is None else time.time() + max(0.0, timeout)
        while self._active_task_count > 0:
            if deadline is not None and time.time() >= deadline:
                return False
            time.sleep(0.05)
        return True

    def stop(self):
        """停止流程工作器。

        顺序对无损升级很重要：先 draining（摘流量：注册表标 draining + 停止
        领新任务）→ 等 in-flight 收尾（有界）→ 再注销服务。反过来的话，注销
        之后仍可能领到任务，编排会看到「已下线但仍消费」的矛盾状态。
        """
        if self._drain_timer is not None:
            self._drain_timer.cancel()
            self._drain_timer = None
        if self._running and not self._drain_event.is_set():
            # 直接 stop（非 drain 路径）也保证「先摘流量再退出」
            with self._active_count_lock:
                self._drain_event.set()
                self._drain_started_at = self._drain_started_at or time.time()
            if self._enable_registry:
                self.update_registry_info(status="draining")
        logger.info("正在停止流程工作器...")
        self._running = False

        # 停止租约看门狗（波次②）
        self._stop_lease_watchdog()

        # 停止取消监听（波次③）
        self._stop_cancel_watcher()

        # 停止 #26 观测导出（抓取端线程 + 告警 webhook 后台线程）
        self._stop_metrics_server()
        self._stop_alerting()

        # 停止控制监听
        if self._enable_registry:
            self.stop_control_listener()
        
        # 注销服务（先标 stopping：编排侧能看到「正在下线」而非突然消失）
        if self._enable_registry:
            self.update_registry_info(status="stopping")
            self.unregister_service()
        
        # 关闭日志处理器
        if self._log_handler:
            self._log_handler.close()
        
        logger.info("流程工作器已停止")
    
    def _on_stop_command(self, graceful: bool):
        """响应停止命令。

        优雅路径走 ``request_drain``：不再领新任务、等 in-flight 收尾，
        超时由 drain 定时器兜底退出；非优雅路径直接 ``stop()``（尽快退出，
        消息留 pending 待接管）。
        """
        logger.info("收到远程停止命令，优雅停止: %s", graceful)
        if graceful:
            self.request_drain("remote stop command")
        else:
            self.stop()
    
    def _on_status_command(self) -> Dict[str, Any]:
        """响应状态查询命令"""
        status = {
            "status": (
                "draining" if self._drain_event.is_set()
                else ("running" if self._running else "stopped")
            ),
            "draining": self._drain_event.is_set(),
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

    # ---------------------------------------------------------- #26 观测导出

    def metrics_text(self) -> str:
        """Prometheus 文本快照（``/metrics`` 每次抓取时调用）。

        队列 stats 读失败（Redis 瞬断）不整单 500：退化为只报 worker 自身
        指标——抓不到队列读数本身也是信号，不该让整张看板空白。
        """
        try:
            stats = self._get_task_queue().stats()
        except Exception as exc:
            logger.warning("读取队列 stats 失败（/metrics 仅返回 worker 指标）: %s", exc)
            stats = {"stream_key": self.queue_name}
        metrics = collect_queue_metrics(stats)
        service_info = getattr(self, "_service_info", None)
        heartbeat = None
        if service_info is not None and service_info.last_heartbeat:
            try:
                heartbeat = datetime.fromisoformat(service_info.last_heartbeat).timestamp()
            except ValueError:
                heartbeat = None
        metrics.extend(collect_worker_metrics(
            active_tasks=self._active_task_count,
            draining=self._drain_event.is_set(),
            running=self._running,
            heartbeat_timestamp=heartbeat,
            instance=self._resolve_consumer_name(),
            registered=service_info is not None,
        ))
        if self._alerter is not None:
            alert_stats = self._alerter.stats()
            for key, help_text in (
                ("sent", "已成功投递的告警数"),
                ("failed", "投递失败的告警数"),
                ("dropped", "告警队列满而丢弃的事件数"),
                ("pending", "告警队列中待投递的事件数"),
            ):
                metrics.append(Metric(
                    f"alert_webhook_{key}_total" if key != "pending" else "alert_webhook_pending",
                    alert_stats[key],
                    {},
                    "counter" if key != "pending" else "gauge",
                    help_text,
                ))
        # resume 指纹兼容门裁决数（#36）：与执行状态里的 error.category 同源，供
        # 混部舰队告警（「一批 mismatch」vs「一批 rebaseline」含义完全不同）。
        # 快照必须在同一把锁下取：`_record_resume_guard` 从消费线程写、本方法从
        # /metrics 的 handler 线程读，直接迭代会命中「dictionary changed size
        # during iteration」→ 抓取端返回空 500。
        guard_counts = getattr(self, "_resume_guard_counts", None)
        guard_lock = getattr(self, "_resume_guard_lock", None)
        # 骨架 worker（__new__ 构造，测试）无这两个字段
        guard_snapshot: Tuple[Tuple[Tuple[str, str], int], ...] = ()
        if guard_counts is not None and guard_lock is not None:
            with guard_lock:
                guard_snapshot = tuple(sorted(guard_counts.items()))
        for (decision, category), value in guard_snapshot:
            metrics.append(Metric(
                "resume_guard_total",
                value,
                {"decision": decision, "category": category},
                "counter",
                "resume 的 flow 指纹兼容门裁决数（按裁决/成因分档）",
            ))
        return render_prometheus(metrics)

    def _start_metrics_server(self) -> None:
        """按配置起 ``/metrics`` 抓取端（未配置端口 = 无操作）。"""
        if self._metrics_port <= 0 or self._metrics_server is not None:
            return
        try:
            self._metrics_server = MetricsHttpServer(
                self.metrics_text, host=self._metrics_host, port=self._metrics_port
            )
            self._metrics_server.start()
        except Exception as exc:  # noqa: BLE001 - 端口占用等不得拖垮 worker 启动
            self._metrics_server = None
            logger.error("启动 /metrics 抓取端失败（端口 %s）: %s", self._metrics_port, exc)

    def _stop_metrics_server(self) -> None:
        if self._metrics_server is not None:
            server, self._metrics_server = self._metrics_server, None
            server.stop()

    # ------------------------------------------------------------------ 告警

    def _start_alerting(self) -> None:
        """配置了 webhook 才建 alerter（未配置 = 死信只走日志，存量行为）。"""
        if self._alert_webhook and self._alerter is None:
            self._alerter = WebhookAlerter(self._alert_webhook)
            logger.info("死信告警 webhook 已启用: %s", self._alert_webhook)

    def _stop_alerting(self) -> None:
        if self._alerter is not None:
            alerter, self._alerter = self._alerter, None
            alerter.close()

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
    worker.request_drain(f"signal {signum}")


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
    parser.add_argument("--concurrency", type=int,
                        default=int(os.environ.get("PLAITA_WORKER_CONCURRENCY", "1")),
                        help="并发消费线程数（默认 1=串行）。>1 时单机可同时处理多个任务；"
                             "轻任务（TS/Python/Node）可放大，Rust 重构建任务建议保持 1。"
                             "也可用环境变量 PLAITA_WORKER_CONCURRENCY")
    parser.add_argument("--quiet", action="store_true",
                        help="关闭 INFO 级控制台日志（等价 PLAITA_LOG_LEVEL=WARNING）")
    parser.add_argument("--heartbeat-interval", type=int, default=10,
                      help="心跳间隔(秒)")
    parser.add_argument("--metrics-port", type=int,
                      default=_metrics_port_from_env(),
                      help="Prometheus /metrics 抓取端端口（默认跟随 PLAITA_METRICS_PORT；"
                           "0 = 不启动）。暴露 stream/pending/dlq/计数器/心跳")
    parser.add_argument("--metrics-host", default=_metrics_host_from_env(),
                      help="/metrics 抓取端监听地址（默认 0.0.0.0 或 PLAITA_METRICS_HOST）")
    parser.add_argument("--alert-webhook", default=_alert_webhook_from_env(),
                      help="死信告警 webhook URL（JSON POST）；默认跟随 PLAITA_ALERT_WEBHOOK")
    parser.add_argument("--langfuse", action="store_true", default=None,
                        help="启用 Langfuse 观测（需 pip install plaita[langfuse]；"
                             "凭据走 LANGFUSE_PUBLIC_KEY/SECRET_KEY/HOST 环境变量）。"
                             "也可用环境变量 PLAITA_WORKER_LANGFUSE=1 开启")

    args = parser.parse_args()
    if args.quiet:
        logging.getLogger().setLevel(logging.WARNING)

    # writefile 写入 jail（plaita#39）：writefile 节点默认任意路径可写，而部署入口
    # 此前从不设置 PLAITA_NODES_WORKSPACE_ROOT——机制在节点侧，门在运营侧缺省开着。
    # worker 启动即注入（未显式配置则 fail-closed 推导默认根）。
    apply_writefile_jail("flow-worker")

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
    # 后端白名单由 PLAITA_SANDBOX_ALLOWED_BACKENDS 配置（见 _register_code_node_for_worker）。
    if _code_node_enabled():
        _register_code_node_for_worker()

    # 沙箱暂停实例清扫（启动一次性、后台线程）：**import 已在主线程完成**
    # （`_paused_sweeper`）——线程内 import 会与主线程懒加载撞 import 锁并整体卡死
    # （2026-10-08 实测）。线程内只做网络调用。
    _sweeper = _paused_sweeper()
    if _sweeper is not None:
        threading.Thread(target=_sweeper, name="sandbox-sweep", daemon=True).start()

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
            concurrency=args.concurrency,
            metrics_port=args.metrics_port,
            metrics_host=args.metrics_host,
            alert_webhook=args.alert_webhook or None,
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


