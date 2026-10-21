"""
执行实例管理 API
提供执行列表、详情、启动、停止等接口
"""
import asyncio
import heapq
import json
import logging
import os
import secrets
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field

try:
    from ..request_models import EmptyRequestModel, StrictRequestModel
except ImportError:  # 顶层 ``api`` 包（测试/脚本模式）
    from request_models import EmptyRequestModel, StrictRequestModel  # type: ignore
from redis import Redis
from sse_starlette.sse import EventSourceResponse
from starlette.routing import Route

# 手工 resume 的订阅绑定私有载体键（与引擎同一常量，见 plaita#221）。
from plaita.core.strategies import RESUME_BINDING_KEY
from plaita.core.url_redaction import _scrub_url_text
# A2R20-03：执行列表索引键/分数与 storage 写路径同源，避免键名/score 漂移。
from plaita.storage.redis import (
    execution_index_key,
    execution_index_ready_key,
    execution_start_time_score,
)
from plaita.server.tenant_quota import tenant_running_key
from plaita.server import start_dedup as _dedup

try:  # 包布局（pip 安装 / `python -m plaita_console`）
    from ..services import obs_link
except ImportError:  # 平铺布局（cwd=backend）运行时
    from services import obs_link  # type: ignore

try:
    from .redis_scan import scan_keys
except ImportError:  # 平铺布局（cwd=backend）运行时
    from api.redis_scan import scan_keys  # type: ignore

try:
    from .sse_redis import SSE_REDIS_RETRY_SECONDS, get_sse_async_redis
except ImportError:  # 平铺布局（cwd=backend）运行时
    from api.sse_redis import (  # type: ignore
        SSE_REDIS_RETRY_SECONDS,
        get_sse_async_redis,
    )

try:  # A2R24-01：统一错误 detail 形状 + 异常文本脱敏
    from ..error_contract import error_detail, scrubbed_detail
except ImportError:
    from error_contract import error_detail, scrubbed_detail  # type: ignore

logger = logging.getLogger(__name__)

router = APIRouter()


# ============ 数据模型 ============

class ExecutionInfo(BaseModel):
    """执行实例信息"""
    execution_id: str = Field(..., description="执行 ID")
    flow_id: str = Field(..., description="流程 ID")
    # A2R24-06：集群档 Lua 投影与构造器一直传 flow_name，模型却没声明——
    # Pydantic 默认 extra="ignore" 把它静默丢掉。声明后列表/详情都可见；
    # 本地档执行行暂无该列（None = 未提供，不是空串）。
    flow_name: Optional[str] = Field(
        None, description="流程名称（集群档取自执行状态；null=该视图未提供）"
    )
    flow_version: Optional[str] = Field(None, description="流程版本")
    status: str = Field(..., description="状态")
    tenant_id: str = Field("", description="租户 ID（本地模式）")
    start_time: Optional[str] = Field(None, description="开始时间")
    end_time: Optional[str] = Field(None, description="结束时间")
    last_update_time: Optional[str] = Field(None, description="最后更新时间")
    context: Optional[Dict[str, Any]] = Field(None, description="执行上下文")
    error: Optional[Dict[str, Any]] = Field(None, description="错误信息")
    invoker: Optional[str] = Field(None, description="调用者")
    # 触发归因（A2R44-02）：调度触发的执行带 {kind, schedule_id, schedule_name}；
    # None = 手动启动/老执行（缺省兼容）。列表经服务端投影 JSON 列透出，
    # 详情读全量状态键。
    trigger: Optional[Dict[str, Any]] = Field(
        None, description="触发归因（调度触发带 {kind, schedule_id, schedule_name}；null=手动/老执行）"
    )
    # 本地单机模式专有：节点级 trace 与最终输出（集群模式为 None）
    nodes: Optional[List[Dict[str, Any]]] = Field(None, description="节点级执行 trace（本地模式）")
    output: Optional[Any] = Field(None, description="流程输出（本地模式）")
    # 节点级耗时：集群模式由 worker 的 NodeTimingCallback 写入执行状态，
    # 本地模式由回调采集的 trace 时间戳折算；老状态/未采集时为 None
    node_timings: Optional[Dict[str, Any]] = Field(None, description="节点级耗时")
    # token/cost 两级汇总（plaita#37 P2）。**null = 未采集，不是 0**：
    # - usage：run 级汇总（形状见 plaita.storage.usage.usage_view，含 source/
    #   estimated/display 串），本地档读 local_executions.usage_json，集群档读
    #   worker 落盘的 ExecutionState.usage；
    # - node_usage：节点级只读投影（真源 context.$NODE[*].usage）；
    #   集群档**列表**路径不读 context（投影不携带），故列表行为 null，
    #   详情页/本地档给出逐个节点。某节点不在表里 = 该节点未报 usage（≠0）。
    usage: Optional[Dict[str, Any]] = Field(
        None, description="run 级 token/成本汇总（null=未采集，非 0）"
    )
    node_usage: Optional[Dict[str, Any]] = Field(
        None, description="节点级 token/成本只读投影（null=未采集/本视图未读取）"
    )
    # B108-02：start 载体命中已终态被具名放弃的审计记录（worker 写平台根键；
    # null = 没有发生过 dedup discard，不是「未采集」）
    dedup_discarded: Optional[Dict[str, Any]] = Field(
        None, description="start 载体命中已终态被放弃的审计记录（null=未发生）"
    )
    # Langfuse 观测深链（启用观测且配了 LANGFUSE_PROJECT_ID 时非空）
    langfuse_trace_url: Optional[str] = Field(None, description="Langfuse trace 页面 URL")


class ExecutionListResponse(BaseModel):
    """执行列表响应"""
    executions: List[ExecutionInfo]
    total: int
    page: int
    size: int
    # A2R20-03：索引/扫描触界时列表只是部分结果（已记 WARNING）。
    # 无过滤时 total 是索引基数；有过滤且 truncated 时 total 是**已找到的
    # 匹配数下界**，调用方不得把它当成全集。
    truncated: bool = False


class StartFlowRequest(StrictRequestModel):
    """启动流程请求"""
    flow_id: str = Field(..., description="流程 ID")
    version: Optional[str] = Field(None, description="流程版本")
    params: Dict[str, Any] = Field(default_factory=dict, description="输入参数")
    dedup_key: Optional[str] = Field(
        None,
        description=(
            "可选 start 幂等键：worker 对同键 start 消息收敛（重投/双开"
            "不再二次新建执行）。键按租户隔离，7 天过期；不传则行为不变"
        ),
    )


class ResumeFlowRequest(StrictRequestModel):
    """恢复流程请求"""
    resume_type: str = Field(
        ..., description="恢复类型: continue, retry(error 态断点重跑), cancel, timeout, event")
    data: Optional[Dict[str, Any]] = Field(None, description="恢复数据")
    # plaita#157：手动 resume 的调度入参通道。此前该请求体**没有** params
    # 字段，人工/值守用 continue 抢在 pending start 被 reclaim 之前时，调度
    # 时下发的 params 永远到不了 `$INPUT`（worker 侧 #122 已修好 start→resume
    # 转换路径的丢参，这条是**另一条路由上"从来传不了"**的同类面）。
    # 语义与 #122 一致：**只有 resume_type=continue** 打在盘上 checkpoint 为空
    # （假值 saved_context）时才被引擎用来重新 `setup_flow` 并绑定 `$INPUT`；
    # 空 checkpoint 下的 retry/cancel/timeout/event 在该分支**之前**就被策略层
    # 守卫以 ResumeGuardError 拒绝（执行保持 running、消息被 ack 吞掉、
    # `$INPUT` 永不绑定——预存语义）；checkpoint 非空（正常续跑）时 params
    # 惰性，绑定值一律以盘上 checkpoint 为准。
    params: Optional[Dict[str, Any]] = Field(
        None,
        description=(
            "可选调度入参（#157）：仅在 `resume_type=continue` 且盘上 "
            "checkpoint 为空时被引擎绑定为 `$INPUT`（手动 continue 抢在 "
            "pending start 被 reclaim 之前的窗口）；空 checkpoint 下 "
            "retry/cancel/timeout/event 被策略层守卫以 `ResumeGuardError` "
            "拒绝（执行保持 running）；不传/空/`null` 不落 `params` 键"
            "（与 #122 的 resume 载体形状一致）；checkpoint 非空时 "
            "`params` 惰性，绑定值一律以盘上 checkpoint 为准"
        ),
    )


# ============ 工具函数 ============

# 任务队列 Stream 名默认值（与 FlowWorker 的 PLAITA_QUEUE_NAME 默认值一致）。
# 实际取值在使用点经 PLAITA_CONSOLE_TASK_QUEUE 覆盖，便于新旧并行验证时把
# console 派发到独立队列——不得在模块导入时求值，否则无法被测试/运行时覆盖。
DEFAULT_TASK_QUEUE_NAME = "plaita:flow:queue"
# 兼容别名（外部脚本可能按此名导入）：**只代表默认值，不参与派发**。
# 任何生产派发必须用 _task_queue_name() 在使用点解析 env（A56-02）。
TASK_QUEUE_NAME = DEFAULT_TASK_QUEUE_NAME


def _task_queue_name() -> str:
    """解析派发队列名：env ``PLAITA_CONSOLE_TASK_QUEUE`` > 默认值。

    B2R31-02：与 capacity/schedule 共用 ``resolve_queue_name``；空串是
    identity 配置错误（不再写空 Stream 键，A2R31-03）。
    """
    from plaita.runtime_config import resolve_queue_name

    return resolve_queue_name("console")

try:
    from plaita.server.task_queue import enqueue_task
    from plaita.server.tenant_context import tenant_namespace
    from plaita.server.tenant_quota import (
        release_tenant_start,
        tenant_max_running,
        try_admit_tenant_start,
    )
    from plaita.storage.fenced import FencedExecutionStorage
    from plaita.storage.base import ExecutionState
    from plaita.storage.redis import (
        TERMINAL_EXECUTION_STATUSES,
        RedisExecutionStorage,
        execution_state_ttl_seconds,
    )
    from plaita.storage.usage import (
        node_results_from_context,
        node_usage_breakdown,
        usage_view,
    )
except ImportError:  # 平铺布局（cwd=backend）运行时
    import sys as _sys
    from pathlib import Path as _Path
    _plaita_root = str(_Path(__file__).resolve().parents[3])
    if _plaita_root not in _sys.path:
        _sys.path.insert(0, _plaita_root)
    from plaita.server.task_queue import enqueue_task
    from plaita.server.tenant_context import tenant_namespace
    from plaita.server.tenant_quota import (
        release_tenant_start,
        tenant_max_running,
        try_admit_tenant_start,
    )
    from plaita.storage.fenced import FencedExecutionStorage
    from plaita.storage.base import ExecutionState
    from plaita.storage.redis import (
        TERMINAL_EXECUTION_STATUSES,
        RedisExecutionStorage,
        execution_state_ttl_seconds,
    )
    from plaita.storage.usage import (
        node_results_from_context,
        node_usage_breakdown,
        usage_view,
    )

try:
    from ..services import flow_store
    from ..auth import is_platform_request, require_auth, tenant_scope
except ImportError:  # 平铺布局（cwd=backend）运行时
    from services import flow_store  # type: ignore
    from auth import is_platform_request, require_auth, tenant_scope  # type: ignore

# plaita#166：执行表示的读面脱敏（与日志出口**同一个**清洗器/凭据键判据）。
from plaita.core.url_redaction import redact_credential_values


def get_redis(request: Request) -> Redis:
    """获取 Redis 客户端（本地单机模式下拒绝并给出明确提示）"""
    redis = request.app.state.redis
    if redis is None:
        raise HTTPException(
            status_code=503,
            detail=(
                "当前为本地单机模式（未连接 Redis），该功能不可用。"
                "启动 Redis 并重启 console 可恢复完整集群能力。"
            ),
        )
    return redis


def get_redis_or_none(request: Request) -> Optional[Redis]:
    """本地模式返回 None 而不报错——executions 端点据此走本地执行分支。"""
    return request.app.state.redis


def _audit(request: Request, action: str, resource_id: str,
           detail: Dict | None = None, *, tenant_id: Optional[str] = None) -> None:
    try:
        from ..services import audit as audit_svc
    except ImportError:
        try:
            from services import audit as audit_svc  # type: ignore
        except ImportError:
            return
    audit_svc.record(request, action=action, resource="execution",
                     resource_id=resource_id, detail=detail, tenant_id=tenant_id)


def get_local_executor(request: Request):
    """本地单机模式分支：返回 local_executor 模块；集群模式返回 None。"""
    if getattr(request.app.state, "local_mode", False):
        try:
            from ..services import local_executor
        except ImportError:
            from services import local_executor  # type: ignore
        return local_executor
    return None


def _enqueue(message: Dict[str, Any], redis: Redis) -> str:
    """把 start/resume 消息写入任务队列 Stream。

    FlowWorker 以 Redis Stream 消费组（XREADGROUP）消费；
    历史上这里误用 rpush（list 类型），与 Stream 同 key 类型冲突，
    消息永远到不了 worker。统一走 XADD。

    B2R31-01：新生产消息写当前三维代际（增量信封字段；旧消费者忽略）。
    """
    queue = _task_queue_name()
    from plaita.runtime_config import envelope_for_stream
    from plaita.server.tenant_status import read_tenant_generation

    return enqueue_task(
        redis, queue, message,
        tenant_generation=read_tenant_generation(redis, message.get("tenant_id")),
        **envelope_for_stream(queue, role="console"),
    )


# ---- plaita#166：执行表示的读面脱敏 ----
#
# 两个面在 #157 的验收里被确认为**按引用留存明文**，且都不是 #157 引入的：
#   * 队列载体（start/resume 消息的 ``params``）——载体本义，flow 要用它；
#   * execution row 的 ``context.$INPUT``（盘上 checkpoint）——resume 要用它。
# 两者都是**功能数据**，本单**刻意不改**（改了就丢语义；#157/#122 的通道红线）。
#
# 但「留存」不等于「谁都能读」：控制台的执行表示（详情 / 列表 / SSE
# initial_state）是**同租户任何人**都能拿到的面，而盘上的 ``context`` 是整份
# checkpoint——`$INPUT` 里的 ``api_key`` / 带口令的 clone URL 因此对非提交者
# 完全可读。掩码族（#158/#159/#160/#161/#165）只覆盖**日志出口**，这个面一直在
# 它们的雷达之外，故在此单独立起。
#
# 落点纪律（与掩码族同源，见 issue #166 的选项 B）：脱敏施加在**构造表示时**
# ——``json.dumps`` / pydantic 序列化**之前**，而不是事后过滤已序列化的字符串。
# 下游任何一次 re-serialize（SSE 的 ``json.dumps``、``ExecutionInfo`` 的
# ``model_dump``）看到的都已经是抹过的值；漏掉哪一个出口都只是「多一处明文」，
# 而不是「掩码被绕过」。
#
# 不改的：队列载体（``_enqueue`` 的 message）、worker 侧 ``$INPUT`` 绑定、
# 盘上 checkpoint、`$INPUT` 的键名与功能值（非凭据项一字不动）。
#
# plaita#222：同一个 ``ExecutionInfo`` 里的 ``output``（流程最终输出）与
# ``nodes[*].output``（本地档节点 trace）也是凭据明文的主要落点——
# ``assignment(output="$INPUT")`` / ``end(output="$INPUT")`` 会整份回显入参，
# http 节点响应体也整段进 ``nodes[*].output``。遍历面因此扩到这四个字段；
# ``redact_credential_values`` 已支持 list 递归，无需新判据。
def _redact_execution_view(info: Dict[str, Any]) -> Dict[str, Any]:
    """执行表示的读面副本：``context`` / ``input`` / ``output`` / ``nodes``
    里的凭据值换占位（#166；遍历面扩至 ``output``/``nodes`` 见 #222）。

    返回**新 dict**（每个被遍历的字段也是新对象；``nodes`` 是 list-of-dict，
    ``redact_credential_values`` 已递归 list）：调用方拿到的盘上对象本体一个
    字节都不改——功能副本必须逐字节保持不变。

    ``output`` 是流程最终输出、``nodes[*].output`` 是本地档节点级 trace：
    ``assignment(output="$INPUT")`` / ``end(output="$INPUT")`` 会把整份入参
    （含凭据）回显到这两处，故与 ``context``/``input`` 同属读面（#222）。
    """
    out = dict(info)
    for field in ("context", "input", "output", "nodes"):
        value = out.get(field)
        if value is not None:
            out[field] = redact_credential_values(value)
    return out


# ---- 集群档租户键助手 ----

def _exec_key(tenant: Optional[str], execution_id: str) -> str:
    """租户上下文的执行状态键：``{ns}:execution:{id}``。"""
    return f"{tenant_namespace(tenant)}:execution:{execution_id}"


# 取消标志键 TTL：7 天自清理（设计稿 §3.1，与 worker 侧 CANCEL_FLAG_TTL_SECONDS 同值）
CANCEL_FLAG_TTL_SECONDS = 7 * 86400


def _cancel_key(tenant: Optional[str], execution_id: str) -> str:
    """取消意图标志键：``{ns}:execution:cancel:{id}``（租户路由复用 _exec_key 规则）。

    意图与状态分离（设计稿 §3.1 选型 B）：控制面对运行中执行只写本键 +
    投 cancel 消息，不再直写 status=cancelled——推进中的 worker 每步会把
    内存 state 覆写回 running，直写等于覆写战争起点；worker 在步界消费
    本键后终态化。带 TTL 自清理。
    """
    return f"{tenant_namespace(tenant)}:execution:cancel:{execution_id}"


# ---- plaita#273 / #274：队列档 start 的「已受理未接管」窗口 ----
#
# POST /executions 返回 queued+id 之后、worker 消费落 running 行之前，调用方
# 拿着这个 id 必须能 GET（200/queued）也能 CANCEL；同 dedup_key 的幂等重试
# 必须拿到同一个 id。三件事配合：
#
# 1. 占位状态行（#273）：入队**之前**同步落 ``queued`` 行（经 storage 写路径
#    ——索引成员同 pipeline 维护，列表/状态过滤立即可见；非终态无 TTL）。
#    行 context 为空，worker 侧 ``_existing_start_checkpoint`` 对「非终态且
#    空 checkpoint」返回 None（按首投处理），消费时在租约内以 running 行
#    覆盖接管——占位行不会把 start 载体误转成 resume。
# 2. 生产侧幂等认领（#274）：铸造 id 后、入队前，以 **worker 同源键**
#    ``{ns}:start-dedup:{key}``（task_queue.start_dedup_map_key 单源，SET NX
#    + 7d）认领；命中即返回既有 execution_id（同键必同 id），不再入队、不再
#    占配额。worker 消费侧对「映射值==载体 id」按自命中继续（#274 配套改动，
#    flow_worker._claim_start_dedup），start 照常推进。
# 3. queued 行的直接取消（#273）：先写取消标志（覆盖「console 置终态与
#    worker 消费」竞窗——标志在位时 worker 的 start 入口/步界检查仍会
#    终态化），再投 cancel 消息，最后以 CAS（仅当行仍是 queued）置终态
#    cancelled 并释放租户 start 配额——worker 全灭时排队执行也能取消、
#    配额立即回收，不再等条目 TTL 自愈。

# queued 占位行的调用者标记（worker 接管后写的 running 行 invoker=worker）
_QUEUED_PLACEHOLDER_INVOKER = "console"

# 条件终态化（CAS）：仅当状态键当前行仍是 queued 时，把调用方构造好的
# cancelled 行（ARGV[1]，Python json.dumps——刻意不用 Lua cjson 回写整行：
# cjson 把空表编码成 ``[]``，会把占位行的 ``context:{}`` 篡改成数组，GET 侧
# ExecutionInfo 直接 500）原子换入并补终态 TTL。
# 返回 1=已置终态；0=行已被接管/不存在（worker 竞态赢，标志+消息兜底）；
# -1=行损坏（不动，交标志+消息路径）。KEYS[1]=状态键；KEYS[2]=租户配额
# ZSET（plaita#280 顺带 P2：CAS 置 cancelled 与 release_tenant_start 必须
# 同一脚本原子完成，否则 CAS 后崩溃会留下 cancelled 行 + 永久配额占用）。
# ARGV: payload, ttl 秒, execution_id。
_CANCEL_QUEUED_ROW_LUA = """
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local ok, obj = pcall(cjson.decode, raw)
if not ok or type(obj) ~= 'table' then return -1 end
if tostring(obj['status'] or '') ~= 'queued' then return 0 end
local ttl = tonumber(ARGV[2]) or 0
if ttl > 0 then
  redis.call('set', KEYS[1], ARGV[1], 'EX', ttl)
else
  redis.call('set', KEYS[1], ARGV[1])
end
redis.call('ZREM', KEYS[2], ARGV[3])
return 1
"""


def _start_dedup_map_key(tenant: Optional[str], dedup_key: str) -> str:
    """生产侧 start 幂等映射键——与 worker 消费侧**同一把键**（单源
    ``task_queue.start_dedup_map_key``，键形状/TTL 漂移即「同键必同 id」失效）。"""
    return _dedup.map_key(tenant, dedup_key)


def _start_dedup_owner_key(tenant: Optional[str], dedup_key: str) -> str:
    """认领者标记键——独立前缀（#285），值与旧格式同形 ``state:id``。

    token 存独立键 ``{ns}:start-dedup-token:{key}``，不挤进 owner 值，保证
    迁移窗口内旧格式 owner 可读；所有转换由 ``plaita.server.start_dedup``
    的 Lua CAS 校验 token。
    """
    return _dedup.owner_key(tenant, dedup_key)


def _start_dedup_token_key(tenant: Optional[str], dedup_key: str) -> str:
    return _dedup.token_key(tenant, dedup_key)


def _start_dedup_legacy_owner_key(tenant: Optional[str], dedup_key: str) -> str:
    return _dedup.legacy_owner_key(tenant, dedup_key)


# 认领者状态（owner 值前缀；token 在独立键）。claiming/enqueueing 短 TTL 让
# 崩溃认领可被接管；enqueued 与映射同 TTL。
_START_DEDUP_OWNER_CLAIMING = "claiming:"
_START_DEDUP_OWNER_ENQUEUEING = "enqueueing:"
_START_DEDUP_OWNER_ENQUEUED = "enqueued:"
_START_DEDUP_OWNER_CLAIM_TTL_SECONDS = _dedup.CLAIM_TTL_SECONDS


def _read_start_dedup_map(
    redis: Redis, tenant: Optional[str], dedup_key: str
) -> Optional[str]:
    return _dedup.read_mapped(redis, tenant, dedup_key)


def _read_start_dedup_owner(
    redis: Redis, tenant: Optional[str], dedup_key: str
) -> Optional[str]:
    """owner 值（``state:id``，双读新前缀/旧 ``<map>:owner``）；无则 None。"""
    view = _dedup.read_owner(redis, tenant, dedup_key)
    if not view.live or not view.execution_id:
        return None
    return f"{view.state}:{view.execution_id}"


def _read_start_dedup_token(
    redis: Redis, tenant: Optional[str], dedup_key: str
) -> Optional[str]:
    try:
        raw = redis.get(_start_dedup_token_key(tenant, dedup_key))
    except Exception:
        return None
    text = raw.decode() if isinstance(raw, bytes) else raw
    if not text or ":" not in text:
        return None
    return str(text).split(":", 1)[1] or None


def _start_claim_guard(
    redis: Redis, tenant: Optional[str], dedup_key: str, execution_id: str,
    token: Optional[str] = None, *,
    state: str = _START_DEDUP_OWNER_CLAIMING,
    ttl: Optional[int] = None,
    allow_missing_owner: bool = True,
) -> bool:
    """A 的写前所有权复核（token CAS）：map/owner/token/墓碑全部匹配才放行。

    ``token`` 缺省时从 token 键反查（兼容旧调用形状）；拿不到 token 一律
    fail-closed。复核通过顺带续期。
    """
    token = token or _read_start_dedup_token(redis, tenant, dedup_key)
    if not token:
        return False
    state_name = state[:-1] if state.endswith(":") else state
    return _dedup.guard(
        redis, tenant, dedup_key, execution_id, token,
        state=state_name,
        ttl=ttl if ttl is not None else _START_DEDUP_OWNER_CLAIM_TTL_SECONDS,
    )


def _start_dedup_mapping_is(
    redis: Redis, tenant: Optional[str], dedup_key: str, execution_id: str
) -> bool:
    """映射仍指向 execution_id 且无墓碑（只读复核，B 恢复补入队用）。"""
    if _read_start_dedup_map(redis, tenant, dedup_key) != execution_id:
        return False
    return not _execution_tombstoned(redis, execution_id)


def _set_start_dedup_owner(
    redis: Redis, tenant: Optional[str], dedup_key: str,
    execution_id: str, *, state: str, ttl: int,
    allow_from_state: Optional[str] = None,
    token: Optional[str] = None,
) -> bool:
    """owner 终态 CAS（token 校验）：只允许 enqueueing/claiming → enqueued。"""
    token = token or _read_start_dedup_token(redis, tenant, dedup_key)
    if not token:
        return False
    if not state.startswith(_START_DEDUP_OWNER_ENQUEUED):
        return False
    ok = _dedup.mark_enqueued(
        redis, tenant, dedup_key, execution_id, token, ttl=ttl
    )
    if not ok:
        logger.warning(
            "start 幂等认领 owner CAS 拒绝: key=%s execution=%s state=%s",
            _start_dedup_map_key(tenant, dedup_key), execution_id, state,
        )
    return ok


def _clear_start_dedup_owner(
    redis: Redis, tenant: Optional[str], dedup_key: str
) -> None:
    try:
        redis.delete(
            _start_dedup_owner_key(tenant, dedup_key),
            _start_dedup_token_key(tenant, dedup_key),
        )
    except Exception:  # noqa: BLE001
        pass


def _read_dedup_discard(
    redis: Redis, execution_id: str
) -> Optional[Dict[str, Any]]:
    """B108-02：worker 写下的「载体命中已终态被放弃」审计记录。"""
    try:
        raw = redis.get(f"plaita:execution:dedup-discard:{execution_id}")
    except Exception:
        return None
    if raw is None:
        return None
    text = raw.decode(errors="replace") if isinstance(raw, bytes) else raw
    try:
        data = json.loads(text)
    except (TypeError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def _execution_tombstoned(redis: Redis, execution_id: str) -> bool:
    """mapped 执行是否已有删除墓碑（force DELETE 后同 key 必须换新 id）。"""
    try:
        return bool(redis.exists(_dedup.tombstone_key(execution_id)))
    except Exception:  # noqa: BLE001 — 读不到按无墓碑（保守走 owner/行判定）
        return False


def _claim_start_dedup_at_post(
    redis: Redis, tenant: Optional[str], dedup_key: str, execution_id: str
):
    """POST 入队前的 token 化 start 幂等认领（Lua 状态机）。

    返回 ``StartClaim``（``claimed/mapped`` 二元组解包兼容）；``.token`` 供
    后续 guard/reserve/mark/release 使用。旧格式 claiming owner 会迁移成带
    token 的新键；``foo:owner`` 反向碰撞先迁 old owner 再认领。
    """
    return _dedup.claim(redis, tenant, dedup_key, execution_id)


def _release_start_dedup_if_owner(
    redis: Redis, tenant: Optional[str], dedup_key: str, execution_id: str,
    *, token: Optional[str] = None, require_row_absent: bool = False,
) -> bool:
    """条件释放（Lua 原子复核 token/owner/row/墓碑；#286）。

    token 给出 = 认领者失败清理；无 token = 孤儿回收（row 存在/token 在位/
    enqueued 一律拒绝）。返回是否真的释放。
    """
    return _dedup.release(
        redis, tenant, dedup_key, execution_id,
        token=token, require_row_absent=require_row_absent,
    )


def _safe_xdel_carrier(redis: Redis, message_id: Optional[str]) -> None:
    """清收刚入队但已失去认领的 start 载体（#r59 ⑤）。"""
    if not message_id:
        return
    try:
        redis.xdel(_task_queue_name(), message_id)
    except Exception as exc:  # noqa: BLE001 — 清收失败留 WARNING（worker 墓碑兜底）
        logger.warning(
            "失去认领后清理 start 载体失败（message=%s）: %s",
            message_id, _scrub_url_text(exc),
        )


def _start_in_progress_error(mapped: Optional[str], dedup_key: str) -> HTTPException:
    return HTTPException(
        status_code=409,
        detail=error_detail(
            "execution_start_in_progress",
            "同 dedup_key 的 start 正在受理，请稍后用同一 dedup_key 重试",
            error="execution_start_in_progress",
        ),
        headers={"Retry-After": "1"},
    )


async def _start_claim_lost_response(
    redis: Redis, tenant: Optional[str], dedup_key: str, execution_id: str,
    request: Any, http_request: Request, *, stage: str,
) -> Dict[str, Any]:
    """A 失去认领后的响应：收敛到当前合法 id，或 409（带 Retry-After）。"""
    mapped = _read_start_dedup_map(redis, tenant, dedup_key)
    if (
        mapped
        and mapped != execution_id
        and not _execution_tombstoned(redis, mapped)
    ):
        row = _load_full_state(redis, _exec_key(tenant, mapped))
        if row is not None:
            response = await _dedup_hit_row_response(
                redis, tenant, request, http_request, mapped, row
            )
            if response is not None:
                return response
    logger.warning(
        "start 认领已失效（%s），拒绝返回自己 id: dedup_key=%s execution=%s "
        "map=%r tombstoned=%s",
        stage, dedup_key, execution_id, mapped,
        _execution_tombstoned(redis, execution_id),
    )
    _audit(
        http_request, "execution.start", execution_id,
        {"flow_id": request.flow_id, "mode": "queue", "dedup_key": dedup_key,
         "claim_lost": True, "claim_lost_stage": stage, "rejected": True},
        tenant_id=tenant,
    )
    raise HTTPException(
        status_code=409,
        detail=error_detail(
            "execution_start_superseded",
            "同 dedup_key 的 start 认领已失效（执行被删除或被并发请求接管），"
            "请用同一 dedup_key 重试",
            error="execution_start_superseded",
        ),
        headers={"Retry-After": "1"},
    )


async def _dedup_hit_row_response(
    redis: Redis, tenant: Optional[str], request: Any, http_request: Request,
    mapped: str, row: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """mapped 行已存在时的幂等命中响应（#274/#280/#285/#286/#287）。

    返回 None = 映射/认领在判定期间变化，调用方应重试认领。
    queued 行：enqueued → 200；claiming/enqueueing → 异步等待 A 完成
    （不阻塞事件循环），超时 409 in-progress；无 token/owner → 原子建立
    recovery reservation、补入队、mark_enqueued（同 id，不换主）。
    """
    from plaita.server.task_queue import START_DEDUP_TTL_SECONDS

    status = str(row.get("status") or "queued")
    owner = _dedup.read_owner(redis, tenant, request.dedup_key)
    requeued = False
    if status == "queued":
        if (
            owner.state == _dedup.STATE_ENQUEUED
            and owner.execution_id == mapped
        ):
            pass  # 载体已入队：同 id 返回
        elif (
            owner.state in (_dedup.STATE_CLAIMING, _dedup.STATE_ENQUEUEING)
            and owner.execution_id == mapped
        ):
            for _ in range(10):
                await asyncio.sleep(0.05)
                owner = _dedup.read_owner(redis, tenant, request.dedup_key)
                if (
                    owner.state == _dedup.STATE_ENQUEUED
                    and owner.execution_id == mapped
                ):
                    break
                if (
                    _read_start_dedup_map(redis, tenant, request.dedup_key) != mapped
                    or _execution_tombstoned(redis, mapped)
                ):
                    return None
                if _load_full_state(redis, _exec_key(tenant, mapped)) is None:
                    return None
            else:
                _audit(
                    http_request, "execution.start", mapped,
                    {"flow_id": request.flow_id, "mode": "queue",
                     "dedup_hit": True, "dedup_inflight": True,
                     "dedup_key": request.dedup_key},
                    tenant_id=tenant,
                )
                raise _start_in_progress_error(mapped, request.dedup_key)
        else:
            # 行已落、无 token/owner（崩溃在「落行→入队」之间）：只有映射仍
            # 属于该 id 才能建立 recovery reservation；绝不复活已换主的 id。
            if not _start_dedup_mapping_is(
                redis, tenant, request.dedup_key, mapped
            ):
                return None
            recovery_token = _dedup.adopt_for_recovery(
                redis, tenant, request.dedup_key, mapped
            )
            if recovery_token is None:
                return None
            recovery_message = {
                "type": "start",
                "flow_id": request.flow_id,
                "version": request.version,
                "params": request.params,
                "execution_id": mapped,
                "tenant_id": tenant,
                "timestamp": datetime.now().isoformat(),
                "dedup_key": request.dedup_key,
            }
            try:
                recovery_msg_id = _enqueue(recovery_message, redis)
            except Exception as exc:  # noqa: BLE001 — reservation 到期后可重试
                logger.error(
                    "queued 占位行补入队失败（reservation 保留，重试可再补）: "
                    "execution=%s: %s", mapped, _scrub_url_text(exc),
                )
            else:
                if not _dedup.mark_enqueued(
                    redis, tenant, request.dedup_key, mapped, recovery_token,
                    ttl=START_DEDUP_TTL_SECONDS,
                ):
                    _safe_xdel_carrier(redis, recovery_msg_id)
                    return None
                requeued = True
                logger.warning(
                    "queued 占位行无 owner（崩溃窗口），已用同 id 补入队: "
                    "execution=%s dedup_key=%s", mapped, request.dedup_key,
                )
    _audit(http_request, "execution.start", mapped,
           {"flow_id": request.flow_id, "mode": "queue",
            "dedup_hit": True, "dedup_key": request.dedup_key,
            "dedup_requeued": requeued},
           tenant_id=tenant)
    return {
        "status": status,
        "flow_id": row.get("flow_id") or request.flow_id,
        "execution_id": mapped,
        "message": "同 dedup_key 的 start 已受理（幂等重试收敛到既有执行）",
    }


def _placeholder_storage(redis: Redis, tenant: Optional[str]) -> "RedisExecutionStorage":
    """占位行读写用的租户命名空间 storage（与 GET/列表同一键形状/索引）。"""
    return RedisExecutionStorage(client=redis, namespace=tenant_namespace(tenant))


def _write_queued_placeholder(
    redis: Redis, tenant: Optional[str], execution_id: str,
    flow_id: str, version: Optional[str], now_iso: str,
) -> bool:
    """同步落 ``queued`` 占位状态行（#273）：POST 返回前该 id 必须可 GET。

    经 ``RedisExecutionStorage.save_execution_state`` 写——索引成员同 pipeline
    维护（列表/状态过滤立即命中）；非终态不设 TTL。失败返回 False，调用方
    以 5xx 终止 POST 并回滚配额（绝不带着「响应已发、行不存在」的窗口入队）。
    """
    state = ExecutionState(
        execution_id=execution_id,
        flow_id=flow_id,
        flow_version=version,
        tenant_id=tenant,
        context={},
        status="queued",
        start_time=now_iso,
        last_update_time=now_iso,
        invoker=_QUEUED_PLACEHOLDER_INVOKER,
    )
    try:
        return _placeholder_storage(redis, tenant).save_execution_state(
            execution_id, state
        )
    except Exception as exc:  # noqa: BLE001 — 落盘异常与 False 同处置（5xx + 回滚）
        logger.error("queued 占位状态行写入异常 execution=%s: %s", execution_id, exc)
        return False


def _discard_queued_placeholder(
    redis: Redis, tenant: Optional[str], execution_id: str
) -> None:
    """入队失败时清掉占位行（状态键 + 索引成员）；尽力而为，失败留 ERROR。"""
    try:
        _placeholder_storage(redis, tenant).delete_execution_state(execution_id)
    except Exception as exc:  # noqa: BLE001 — 清理失败必须可观测（幽灵 queued 行）
        logger.error(
            "入队失败后占位状态行清理失败 execution=%s（将残留 queued 行，"
            "需人工清理）: %s", execution_id, exc,
        )


def _is_mechanism_key(key: str) -> bool:
    """排除与执行状态同前缀的机制键（租约/世代计数/取消标志/死信）。

    fence 世代键（plaita:execution:fence:{id}，波次②）存裸整数计数器——
    混进列表会被 Lua cjson 投影当 summary 索引而 500（2026-10-02 e2e 实证：
    worker 开始持租约后审批/触发类轮询全 500）。

    同类第三次（2026-10-06）：`plaita:execution:noderetry:{id}`（节点级重试
    计数，波次二任务①）也是**裸整数**，同样混进 `plaita:execution:*` 投影 →
    `summary.get` 对 int 调用即 500（远端 console 实测：列表 API 全崩，
    AttributeError: 'int' object has no attribute 'get'）。同前缀机制键必须
    逐一排除——新增此类键时同步登记本条。

    同类第四次（2026-10-07）：`plaita:execution:index`（执行列表索引 ZSET，
    见 plaita/storage/redis.py）与 `{...}:execution:index:ready`（回填标记，
    值为裸字符串 "1"）。ZSET 键 GET 抛 WRONGTYPE 尚可被兜底吞掉，但 ready
    标记是字符串 "1" → json.loads 得 int → `summary.get` 即 500，必须排除。

    同类第七次（2026-10-10，plaita#123）：`noderetry` 键**新增了节点维度
    子键** `{ns}:execution:noderetry:{id}:{node_id}`（修复「计数按执行维度
    却被成功推进清零 ⇒ 预算永不耗尽 ⇒ 无限重投」的根因）。值同样是裸整数，
    但前缀判定 `":execution:noderetry:" in key` 是**子串**匹配，子键天然
    命中，无需再加分支——此处留痕说明该前缀的覆盖面已包含子键形态，改动
    键名/前缀时须同步核对（`test_console_mechanism_keys.py` 的自动防线扫
    worker 源码里的 `:execution:<name>:{` 形态，键名不变则防线照常覆盖）。

    同类第八次（2026-10-10，同批）：`{ns}:execution:noderefetch:{id}`
    （plaita#123 兜底总重试上限，**裸整数**）——这是**新前缀**，不是
    `noderetry` 的子串，必须单独登记（自动防线扫
    `:execution:noderefetch:{` 已覆盖）。
    """
    return (
        ":execution:lease:" in key
        or ":execution:fence:" in key
        or ":execution:cancel:" in key
        # 子串匹配：`noderetry:{id}`（旧执行维度）与 `noderetry:{id}:{node}`
        # （#123 起的节点维度子键）一并排除。
        or ":execution:noderetry:" in key
        # 兜底总重试计数（#123 评审建议，同类第八次）：独立前缀——防键空间
        # 漂移导致无界重投的兜底计数器，值与 noderetry 同为裸整数。
        or ":execution:noderefetch:" in key
        # 同类第五次（2026-10-10，值守侧引入）：`:execution:g1wakeups:{id}`
        # （G1 唤醒计数，plaita#73 补丁）同样是**裸整数**——漏登记即复现本函数
        # docstring 记录的「列表 API 全崩」（实测 `AttributeError: 'int' object
        # has no attribute 'get'`，`GET /api/executions` 500）。
        # **新增此类机制键必须同步登记本条**（本函数 docstring 已列四次先例）。
        or ":execution:g1wakeups:" in key
        # 同类第六次（2026-10-10，同批）：`:execution:nofail:{id}`
        # （确定性失败计数，plaita#73 遗留层）同样是**裸整数**。
        # 教训：新增机制键**必须**同步登记；`test_console_mechanism_keys.py`
        # 的自动防线会扫 worker 源码，漏登记即 CI 红。
        or ":execution:nofail:" in key
        # 同类第九次（2026-10-11，无限交接风暴修复）：`:execution:affinityhandover:{id}`
        # （不亲和交接计数，取代旧的 `plaita:affinity:handover:<指纹>` 布尔标记）
        # 同样是**裸整数**。旧前缀 `affinity:handover` 不在 `execution:` 段下、天然
        # 不参与列表投影；新键改到执行维度后**落进了投影面**，故必须排除，否则
        # `json.loads` 得 int → `summary.get` 即 500（与 noderetry/g1wakeups 同病）。
        or ":execution:affinityhandover:" in key
        # 同类第十次（2026-10-17，收口批②）：`:execution:events:{id}`
        # （**#25 执行事件 SSE 频道**，`plaita/server/execution_timeline.py` 的
        # `CHANNEL_PREFIX = "plaita:execution:events:"`，`flow_worker.py` 为唯一发布者）
        # —— 它落在 `plaita:execution:*` 投影面内，未排除即混进列表投影。
        # `test_console_mechanism_keys.py` 的自动防线扫 worker/声明侧源码时
        # 报「未登记: ['events']」；console 侧自己的用例
        # （`test_clean_c4_executions_api.py`）也把它列为需排除项。
        or ":execution:events:" in key
        # 同类第十一次（2026-10-19，A2R13）：`:execution:fencegen:{id}`
        # （fence 世代计数器，与 fence 键分离）也是**裸整数**。它不是
        # `:execution:fence:` 的子串（"fencegen:" 中没有 "fence:"），必须
        # 单独登记，否则 list 投影把它当 summary dict → `summary.get` 500。
        or ":execution:fencegen:" in key
        or ":execution:index" in key
        or key.endswith(":dlq")
    )


def _tenant_from_key(key: str) -> str:
    """从 ``plaita[:tenant]:execution:{id}`` 解析租户（default 键无租户段）。"""
    parts = key.split(":")
    return parts[1] if len(parts) > 3 else "default"


def _execution_id_from_key(key: str) -> str:
    """从 ``{ns}:execution:{id}`` 取 execution_id（ns 可含租户段）。"""
    return key.split(":execution:", 1)[1]


def _events_key(execution_id: str) -> str:
    """执行事件 SSE 频道键（与 ``execution_timeline.CHANNEL_PREFIX`` 同源）。"""
    return f"plaita:execution:events:{execution_id}"


def _delete_cluster_execution_state(
    redis: Redis, tenant: Optional[str], execution_id: str
) -> bool:
    """经 storage API 删除执行状态，并吊销 fence/lease（A43-01）。

    生产代码里 console 的删除此前是裸键删除，只删状态/取消标志/事件频道，
    在途 fenced worker 的下一次落盘仍命中 fence 世代 CAS，把状态与 ZSET
    索引写回（删除后复活）。这里收敛到 ``delete_execution_state``：状态键、
    索引成员、fence 世代键、lease 租约键在同一 pipeline 内清掉；此后 worker
    的 save/delete 因世代失配以 ``ExecutionLeaseError`` 具名拒绝。

    ``FencedExecutionStorage`` 在无世代号的调用方（控制面）直接透传内层；
    若调用方意外持有世代号，删除同样受 CAS 保护（A43-07）。
    """
    storage = FencedExecutionStorage(
        RedisExecutionStorage(client=redis, namespace=tenant_namespace(tenant))
    )
    return storage.delete_execution_state(execution_id)


def _signal_cancel_for_force_delete(
    redis: Redis, tenant: Optional[str], execution_id: str, info: Dict[str, Any]
) -> None:
    """显式 force 删除前的「先取消」步骤：写取消意图 + 投 cancel 消息。

    与 ``POST /executions/{id}/cancel`` 同一形态（运行中→标志键 + 消息；
    挂起→消息即可，状态马上被删）。删除本身会清掉标志键，故这里的作用是
    让在途/待处理消息走取消语义，而不是留下一个「已删除但消息继续推进」
    的窗口。
    """
    message = {
        "type": "resume",
        "flow_id": info.get("flow_id"),
        "execution_id": execution_id,
        "resume_type": "cancel",
        "tenant_id": tenant or "default",
        "data": None,
        "timestamp": datetime.now().isoformat(),
    }
    _enqueue(message, redis)
    if (info.get("status") or "") != "suspended":
        redis.set(
            _cancel_key(tenant, execution_id),
            datetime.now().isoformat(),
            ex=CANCEL_FLAG_TTL_SECONDS,
        )


# ---- 唤醒预算闸（plaita#73）：把 worker 的「幂等拒绝唤醒」变成用户可见的 409 ----
#
# worker 对 `resume_type=retry` 有**有界唤醒**语义：G1 唤醒达上限（默认 2）或
# 确定性失败连续达上限（默认 12）时，`resume_flow` 幂等返回
# `already_terminal=True` + `g1_wakeups_exhausted` / `deterministic_failure_exhausted`
# ——**不抛异常、不改状态**，只在 worker 日志里留一行。BFF 此前照常返回
# 「已受理」，操作台的「从断点重试」于是变成静默哑弹（点了没反应、也没报错）。
# 这里按 worker 同源的阈值（直接取 FlowWorker 的类常量，避免两份口径漂移）
# 在入队前先判一次：达限即 409 并把「人工解封动作」写进 detail。
def _retry_budget_block(
    redis: Redis, tenant: Optional[str], execution_id: str
) -> Optional[Dict[str, Any]]:
    """返回阻塞重试的 detail（达限时）或 None（可重试）。

    注意只读两个计数器键、不写：重置预算的动作留给运维（删键即恢复，见
    docs-site/docs/distributed/ops-runbook.md 的机制键表）。
    """
    try:
        from plaita.server.flow_worker import FlowWorker
    except ImportError:  # pragma: no cover — console 独立部署缺 worker 依赖
        return None
    namespace = tenant_namespace(tenant)

    def _counter(suffix: str) -> int:
        raw = redis.get(f"{namespace}:execution:{suffix}:{execution_id}")
        try:
            return int(raw) if raw is not None else 0
        except (TypeError, ValueError):
            return 0

    g1 = _counter("g1wakeups")
    if g1 >= FlowWorker.G1_MAX_WAKEUPS:
        key = f"{namespace}:execution:g1wakeups:{execution_id}"
        return {
            "message": (
                f"该执行的 retry 唤醒次数已达上限（{g1}/{FlowWorker.G1_MAX_WAKEUPS}），"
                "worker 会幂等拒绝唤醒（error 终态保留），重试不会再触发重跑。"
                f"确认失败原因已修复后，删计数键 {key} 再重试。"
            ),
            "reason": "g1_wakeups_exhausted",
            "counter_key": key,
            "count": g1,
            "limit": FlowWorker.G1_MAX_WAKEUPS,
        }
    nofail = _counter("nofail")
    if nofail >= FlowWorker.DETERMINISTIC_FAILURE_MAX:
        key = f"{namespace}:execution:nofail:{execution_id}"
        return {
            "message": (
                "该执行的确定性失败已连续达上限"
                f"（{nofail}/{FlowWorker.DETERMINISTIC_FAILURE_MAX}），worker 判定"
                "「不可救」并幂等拒绝唤醒（error 终态保留），重试不会再触发重跑。"
                f"确认失败原因已修复后，删计数键 {key} 再重试。"
            ),
            "reason": "deterministic_failure_exhausted",
            "counter_key": key,
            "count": nofail,
            "limit": FlowWorker.DETERMINISTIC_FAILURE_MAX,
        }
    return None


# ---- C4-3：列表路径服务端投影 ----
# 历史：SCAN 出全部状态键后逐键 GET 完整 JSON（含全部 context）并 json.loads，
# 内存排序分页——执行量大时网络/内存/延迟全线劣化。
# 选型说明：未采用「save 时维护 ZSET/Hash 索引」，因为 FencedExecutionStorage
# 持 fence 世代时以单段 Lua 直接 SET 状态键，不经过 RedisExecutionStorage
# .save_execution_state（已核实，fenced.py:85-122）——worker resume 路径的
# 所有进度/终态写都不触达 save 侧索引逻辑，索引必然滞后（列表会长期显示
# 过期 status）。改为**读时投影**：SCAN 出键后按块 EVAL，Redis 服务端
# cjson.decode 状态 JSON、只回传列表字段，完整 context 永不离开 Redis、
# 永不进 console 内存；数据永远取自状态键本体，零滞后、零回填。
# EVAL 不可用（受限代理/无 Lua）时逐键回退旧全量路径，行为不劣化。

_EXECUTION_PROJECTION_LUA = """
local out = {}
local n = 0
local function s(v)
  if type(v) == 'string' then return v end
  if type(v) == 'number' then return tostring(v) end
  return ''
end
for i = 1, #KEYS do
  local ok, data = pcall(redis.call, 'GET', KEYS[i])
  if ok and data and type(data) == 'string' then
    local okd, obj = pcall(cjson.decode, data)
    if okd and type(obj) == 'table' then
      n = n + 1
      -- plaita#37 P2：usage 只回传**序列化后的字符串**（列表行要能区分
      -- 「未采集(null)」与「采集到 0」；完整 context 依旧不出 Redis）。
      -- usage 缺失/不可编码 ⇒ 'null'（读取侧折算成 None，不是 0）。
      local ujson = 'null'
      local uok, uenc = pcall(cjson.encode, obj['usage'])
      if uok and type(uenc) == 'string' then ujson = uenc end
      -- A2R44-02：触发归因同 usage 款式——序列化字符串回传，缺失 ⇒ 'null'
      -- （读取侧折算成 None，手动启动/老执行按缺省兼容）。
      local tjson = 'null'
      local tok, tenc = pcall(cjson.encode, obj['trigger'])
      if tok and type(tenc) == 'string' then tjson = tenc end
      out[n] = {KEYS[i],
        s(obj['status']),
        s(obj['flow_id']),
        s(obj['flow_name']),
        s(obj['flow_version']),
        s(obj['start_time']),
        s(obj['last_update_time']),
        s(obj['end_time']),
        s(obj['invoker']),
        ujson,
        tjson}
    end
  end
end
return out
"""

# 单次 EVAL 处理的键数：约束 Redis 单线程被脚本阻塞的时长上限。
EXECUTION_PROJECTION_CHUNK = 50


def _load_full_state(redis: Redis, key: str) -> Optional[Dict[str, Any]]:
    """逐键全量读取（回退路径）；非字符串键/坏 JSON 返回 None。"""
    try:
        raw = redis.get(key)
    except Exception:
        return None
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def _project_summaries(
    redis: Redis, keys: List[str]
) -> Dict[str, Dict[str, Any]]:
    """对状态键做服务端投影，返回 {key: 列表字段摘要}。

    cjson 解析失败的键（如机制键的字符串值）回退全量 GET 兜底。
    """
    summaries: Dict[str, Dict[str, Any]] = {}
    for start in range(0, len(keys), EXECUTION_PROJECTION_CHUNK):
        chunk = keys[start : start + EXECUTION_PROJECTION_CHUNK]
        if not chunk:
            continue
        rows = None
        try:
            rows = redis.eval(_EXECUTION_PROJECTION_LUA, len(chunk), *chunk)
        except Exception:
            rows = None
        if rows is None:
            # EVAL 不可用 → 旧全量路径兜底（本块）
            for key in chunk:
                info = _load_full_state(redis, key)
                if info is not None:
                    summaries[key] = info
            continue
        projected = set()
        for row in rows:
            key = row[0]
            projected.add(key)
            summaries[key] = {
                "status": row[1] or None,
                "flow_id": row[2] or None,
                "flow_name": row[3] or None,
                "flow_version": row[4] or None,
                "start_time": row[5] or None,
                "last_update_time": row[6] or None,
                "end_time": row[7] or None,
                "invoker": row[8] or None,
                # plaita#37 P2：usage 由 Lua 侧 cjson 序列化回传（'null' = 未采集）。
                # 老脚本/老 Redis 返回的行可能只有 9 列 ⇒ 用长度守卫取。
                "usage": _parse_projected_usage(row[9] if len(row) > 9 else None),
                # A2R44-02：触发归因同款 JSON 列（'null' = 手动/老执行）。
                "trigger": _parse_projected_trigger(row[10] if len(row) > 10 else None),
            }
        # cjson 解析失败被跳过的键：全量兜底
        for key in chunk:
            if key not in projected:
                info = _load_full_state(redis, key)
                if info is not None:
                    summaries[key] = info
    return summaries


def _parse_projected_usage(raw: Any) -> Optional[Dict[str, Any]]:
    """Lua 投影回传的 usage 串 → 原始 usage dict（/ ``None`` = 未采集）。

    ``'null'``/空/坏 JSON 一律 ``None``：**绝不当成 0**（列表行必须能区分
    「这个 run 没采到」与「这个 run 采到 0」）。
    """
    if not isinstance(raw, str) or not raw:
        return None
    try:
        value = json.loads(raw)
    except (TypeError, ValueError):
        return None
    return value if isinstance(value, dict) and value else None


def _parse_projected_trigger(raw: Any) -> Optional[Dict[str, Any]]:
    """Lua 投影回传的 trigger 串 → 触发归因 dict（/ ``None`` = 手动/老执行）。

    ``'null'``/空/坏 JSON/非 dict 一律 ``None``（A2R44-02：缺省兼容，读取方
    不得把缺字段当调度触发）。
    """
    if not isinstance(raw, str) or not raw:
        return None
    try:
        value = json.loads(raw)
    except (TypeError, ValueError):
        return None
    return value if isinstance(value, dict) and value else None


def _execution_info_from_summary(
    key_str: str, summary: Dict[str, Any]
) -> ExecutionInfo:
    """由投影摘要构建列表行（context/error 不进列表响应，详情页读全量）。

    ``node_usage`` 恒为 ``None``：集群档列表路径**刻意不读 context**（全量
    context 不得进 console 内存，见 ``_EXECUTION_PROJECTION_LUA`` 选型说明），
    所以这里没有节点级数据——是「本视图不提供」，不是「节点没报 usage」。
    节点级明细请读详情页（``GET /executions/{id}``）。
    """
    return ExecutionInfo(
        execution_id=_execution_id_from_key(key_str),
        flow_id=summary.get("flow_id") or "",
        flow_name=summary.get("flow_name"),
        flow_version=summary.get("flow_version"),
        status=summary.get("status") or "unknown",
        tenant_id=_tenant_from_key(key_str),
        start_time=summary.get("start_time"),
        end_time=summary.get("end_time"),
        last_update_time=summary.get("last_update_time"),
        invoker=summary.get("invoker"),
        trigger=summary.get("trigger"),
        context=None,
        error=None,
        usage=project_usage(summary.get("usage")),
        node_usage=None,
    )


# ---- A2R20-03：执行列表按索引真分页 ----
# 历史：列表对匹配的全部执行键做无界 SCAN + 逐键服务端投影 + 内存排序，
# 最后才 `executions[start:end]`——20k 执行 35k 命令 / 2.4s，成本与 page/size
# 无关；`size` 也没有上限（size=201 会投影并返回 201 行）。
#
# ``{ns}:execution:index`` ZSET（score=start_time、member=execution_id）由
# storage 与 fenced 两条写路径同步维护。列表改为：
# - 索引就绪（``{ns}:execution:index:ready``）时按 (start_time,id) 真分页：
#   ZREVRANGE 只取当页 id，再只投影当页（成本 O(page)）；
# - 存量库（就绪标记缺失）退化为**有界** SCAN 一次：扫完则顺带回填索引并置
#   标记；触界返回 truncated + WARNING（scan_keys 已记），不静默短页；
# - 带 status/flow_id 过滤时按索引顺序分块投影、条件收窄，候选/墙钟触界即停。
EXECUTION_LIST_MAX_SIZE = 200
EXECUTION_SCAN_COUNT = 1000
EXECUTION_SCAN_MAX_ROUNDS = 1500
EXECUTION_SCAN_TIME_BUDGET_S = 1.0
EXECUTION_FILTER_MAX_CANDIDATES = 10_000
EXECUTION_FILTER_TIME_BUDGET_S = 1.0
EXECUTION_INDEX_PAGE_CHUNK = 200

# ---- A2R28-02：存量索引回填「批量 + deadline + 断点续跑」----
# 历史：回填依赖单次列表的有界 SCAN（max_keys=100k）；触界即返回
# truncated 且**不置 ready**。执行数一旦超过 100k，完整 SCAN 永远不可能在
# 单请求内完成 ⇒ ready 永不置位 ⇒ 每次列表都重复扫描并投影前 100k 行
# （120k 下 3.8s / 102k 命令，total 永久只到 100000）。
#
# 现在回填自己驱动 SCAN，并按 r39 lifespan 回填的 batch+deadline 语义收口：
# - 每批 ``EXECUTION_BACKFILL_BATCH_SIZE`` 个键一个 pipeline（ZADD 批量写）；
# - 单请求墙钟 ``EXECUTION_BACKFILL_DEADLINE_S`` / 键数
#   ``EXECUTION_BACKFILL_MAX_KEYS`` 任一触界即停，**SCAN 游标持久化**到
#   ``{ns}:execution:index:backfill:cursor``，下一请求从断点续跑（ZADD 幂等，
#   重复批次不会写脏）；
# - 只有 SCAN 游标回到 0 才置 ready 并清游标——中途退出绝不谎报「索引就绪」。
# 触界/失败都留 WARNING（含已扫描键数与游标），降级必须可观测。
EXECUTION_BACKFILL_BATCH_SIZE = 1000
# 单请求墙钟：120k 键的 Lua 投影约 3.2s + ZADD 约 0.9s（探针机实测），
# 5s 过于贴边；10s 留 2x 余量，同时仍是硬上限（超界保留游标续跑）。
EXECUTION_BACKFILL_DEADLINE_S = 10.0
EXECUTION_BACKFILL_MAX_KEYS = 1_000_000
EXECUTION_BACKFILL_CURSOR_TTL_S = 86_400
# 平台视角发现「Redis 里存在但租户表没有」的命名空间：有界 SCAN 结果缓存，
# 避免单租户平台视角每个请求都走一遍 keyspace。
_EXECUTION_NS_CACHE_TTL_S = 30.0
_execution_ns_cache: Dict[str, Any] = {}


def _discover_execution_namespaces(redis: Redis) -> List[str]:
    """有界 SCAN 发现 ``plaita:<tenant>:execution:*`` 的租户命名空间。

    仅在租户表未给出任何租户命名空间时使用（未 bootstrap / 内嵌挂载 /
    Redis-only 租户）。结果缓存 30s；触界结果不缓存（下次重试完整发现）。
    """
    cached_at = _execution_ns_cache.get("at")
    cached_redis = _execution_ns_cache.get("redis")
    if (
        cached_at is not None
        and cached_redis is redis
        and time.monotonic() - cached_at < _EXECUTION_NS_CACHE_TTL_S
    ):
        return list(_execution_ns_cache.get("namespaces") or [])

    result = scan_keys(
        redis,
        "plaita:*:execution:*",
        count=EXECUTION_SCAN_COUNT,
        max_keys=1000,
        max_rounds=EXECUTION_SCAN_MAX_ROUNDS,
        time_budget_s=EXECUTION_SCAN_TIME_BUDGET_S,
    )
    found = set()
    for key in result.keys:
        if _is_mechanism_key(key):
            continue
        namespace = key.split(":execution:", 1)[0]
        if namespace and namespace != "plaita":
            found.add(namespace)
    namespaces = sorted(found)
    if not result.truncated:
        # 持引用（而不是 id）：避免测试里同 id 复用导致跨实例误命中缓存。
        _execution_ns_cache["redis"] = redis
        _execution_ns_cache["at"] = time.monotonic()
        _execution_ns_cache["namespaces"] = list(namespaces)
    return namespaces


def _execution_namespaces(request: Request, tenant: Optional[str], redis: Redis) -> List[str]:
    """列表要覆盖的命名空间：租户视角单 ns；平台视角 default + 各租户。"""
    from plaita.server.tenant_context import tenant_namespace

    if tenant:
        return [tenant_namespace(tenant)]
    namespaces = ["plaita"]
    store = getattr(getattr(request, "app", None), "state", None)
    store = getattr(store, "store", None)
    if store is not None:
        try:
            try:
                from ..services import tenants_svc
            except ImportError:
                from services import tenants_svc  # type: ignore
            for item in tenants_svc.list_tenants(store):
                namespace = tenant_namespace(item.get("id"))
                if namespace not in namespaces:
                    namespaces.append(namespace)
        except Exception as exc:  # noqa: BLE001 - 枚举失败不阻断列表
            logger.warning("枚举租户命名空间失败（平台执行列表退回 default）: %s", exc)
    if len(namespaces) == 1:
        # 租户表为空/不可用：Redis-only 租户仍须出现在平台视角（有界发现）。
        try:
            for namespace in _discover_execution_namespaces(redis):
                if namespace not in namespaces:
                    namespaces.append(namespace)
        except Exception as exc:  # noqa: BLE001 - 发现失败只影响跨租户覆盖
            logger.warning("发现租户执行命名空间失败（平台执行列表可能不完整）: %s", exc)
    return namespaces


def _execution_backfill_cursor_key(namespace: str) -> str:
    """回填断点（SCAN 游标）键（A2R28-02）。"""
    return f"{namespace}:execution:index:backfill:cursor"


def _load_backfill_cursor(redis: Redis, namespace: str) -> int:
    """读取持久化 SCAN 游标；缺失/坏值按 0（从头开始，幂等安全）。"""
    key = _execution_backfill_cursor_key(namespace)
    try:
        raw = redis.get(key)
    except Exception as exc:  # noqa: BLE001 - 读游标失败只影响续跑效率
        logger.warning("执行索引回填游标读取失败（本次从头开始）: %s", exc)
        return 0
    if raw is None:
        return 0
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        logger.warning("执行索引回填游标非法（%r），本次从头开始", raw)
        return 0


def _save_backfill_cursor(redis: Redis, namespace: str, cursor: int) -> bool:
    """持久化 SCAN 游标；失败返回 False（下次从头重复扫描，结果仍正确）。"""
    try:
        redis.set(
            _execution_backfill_cursor_key(namespace),
            str(cursor),
            ex=EXECUTION_BACKFILL_CURSOR_TTL_S,
        )
        return True
    except Exception as exc:  # noqa: BLE001 - 游标写失败只影响续跑效率
        logger.warning(
            "执行索引回填游标写入失败（namespace=%s cursor=%s，下次从头重扫）: %s",
            namespace, cursor, exc,
        )
        return False


def _write_backfill_batch(
    redis: Redis, namespace: str, summaries: Dict[str, Dict[str, Any]]
) -> bool:
    """一批 ZADD（score=start_time）到索引；失败返回 False（游标不前移）。"""
    if not summaries:
        return True
    index_key = execution_index_key(namespace)
    try:
        pipe = redis.pipeline(transaction=False)
        for key, summary in summaries.items():
            pipe.zadd(
                index_key,
                {_execution_id_from_key(key): execution_start_time_score(summary)},
            )
        pipe.execute()
        return True
    except Exception as exc:  # noqa: BLE001 - 本批失败：保留游标，下次重试
        logger.warning(
            "执行索引回填批次写入失败（namespace=%s，%d 个成员，保留游标）: %s",
            namespace, len(summaries), exc,
        )
        return False


def _backfill_execution_index(
    redis: Redis,
    namespace: str,
    *,
    status: Optional[str],
    flow_id: Optional[str],
    limit: int,
    deadline: Optional[float] = None,
) -> Tuple[List[ExecutionInfo], int, bool, bool]:
    """分批回填执行索引（A2R28-02），返回
    ``(本次采到的行, 索引基数, 是否完成, 是否全扫完成)``。

    从持久化游标续跑；每批 ``EXECUTION_BACKFILL_BATCH_SIZE`` 个键一次 ZADD。
    墙钟/键数触界即停并保留游标（下次续跑），SCAN 走到 0 才置 ready。
    未完成时 ``索引基数`` 是已索引成员数（total 的下界，响应同时带
    ``truncated=true``）。``全扫完成`` 表示本请求从游标 0 起跑并扫完全部键，
    此时返回的行就是全量 top-N（调用方无需再读一遍索引）。
    """
    index_key = execution_index_key(namespace)
    ready_key = execution_index_ready_key(namespace)
    pattern = f"{namespace}:execution:*"
    deadline_at = (
        deadline
        if deadline is not None
        else time.monotonic() + EXECUTION_BACKFILL_DEADLINE_S
    )
    cursor = _load_backfill_cursor(redis, namespace)
    start_cursor = cursor
    scanned = 0
    # 当页窗口的 top-N（score=start_time）：边扫边维护，内存 O(offset+size)，
    # 而不是把全部摘要留在 console（120k 行）再排序。仅在「从 0 起跑且扫完」
    # 时这 N 行就是全量 top-N，可直接作为列表结果（避免回填完成瞬间再投影一遍）。
    collected: List[Tuple[float, str, Dict[str, Any]]] = []
    matched = 0
    complete = False
    stop_reason: Optional[str] = None
    batch_size = max(1, EXECUTION_BACKFILL_BATCH_SIZE)
    window = max(1, limit)

    try:
        while True:
            if scanned >= EXECUTION_BACKFILL_MAX_KEYS:
                stop_reason = "max_keys"
                break
            if time.monotonic() >= deadline_at:
                stop_reason = "deadline"
                break

            cursor, batch = redis.scan(
                cursor=cursor, match=pattern, count=batch_size
            )
            keys = [
                key if isinstance(key, str) else key.decode()
                for key in batch
                if not _is_mechanism_key(
                    key if isinstance(key, str) else key.decode()
                )
            ]
            scanned += len(keys)
            summaries = _project_summaries(redis, keys) if keys else {}
            if summaries and not _write_backfill_batch(redis, namespace, summaries):
                # 本批没写进索引：游标不前移，下一请求重试同一批。
                stop_reason = "write_failed"
                break
            for key, summary in summaries.items():
                if status and (summary.get("status") or "") != status:
                    continue
                if flow_id and (summary.get("flow_id") or "") != flow_id:
                    continue
                matched += 1
                score = execution_start_time_score(summary)
                item = (score, key, summary)
                if len(collected) < window:
                    heapq.heappush(collected, item)
                elif (score, key) > (collected[0][0], collected[0][1]):
                    heapq.heapreplace(collected, item)

            if cursor == 0:
                complete = True
                break
            _save_backfill_cursor(redis, namespace, cursor)
    except Exception as exc:  # noqa: BLE001 - 回填失败不影响本次列表响应
        stop_reason = "scan_failed"
        logger.warning(
            "执行索引回填扫描失败（namespace=%s cursor=%s 已扫描=%d）: %s",
            namespace, cursor, scanned, exc,
        )

    try:
        indexed = int(redis.zcard(index_key) or 0)
    except Exception as exc:  # noqa: BLE001 - 基数读失败按本批已写数计
        logger.warning("执行索引基数读取失败（按已扫描数计）: %s", exc)
        indexed = scanned

    ordered = sorted(
        collected, key=lambda item: (item[0], item[1]), reverse=True
    )
    rows = [
        _execution_info_from_summary(key, summary)
        for _score, key, summary in ordered
    ]
    # 过滤时 total 是匹配数（完整扫描时精确，中断时是下界）；无过滤时用
    # ZCARD（索引成员数，含本请求已写入的成员）。
    reported_total = matched if (status or flow_id) else indexed
    # 从游标 0 起跑且扫完：这批 top-N 就是全量 top-N，可被调用方直接使用；
    # 续跑后完成只覆盖部分键，调用方须改用索引页（否则顺序/内容不完整）。
    full_scan = complete and start_cursor == 0

    if complete:
        try:
            pipe = redis.pipeline(transaction=False)
            pipe.set(ready_key, "1")
            pipe.delete(_execution_backfill_cursor_key(namespace))
            pipe.execute()
            logger.info(
                "执行索引回填完成（namespace=%s）：%d 个成员，本次扫描 %d 键"
                "（此后列表走 ZSET 真分页）",
                namespace, indexed, scanned,
            )
        except Exception as exc:  # noqa: BLE001 - 标记写失败：下次重跑同一批
            logger.warning(
                "执行索引回填完成但就绪标记写入失败（下次请求重试）: %s", exc
            )
        return rows, reported_total, True, full_scan

    logger.warning(
        "执行索引回填未完成（namespace=%s reason=%s）：已扫描 %d 键、索引 %d "
        "成员、本次返回 %d 行；游标已保留，下次列表续跑",
        namespace, stop_reason or "unknown", scanned, indexed, len(rows),
    )
    return rows, reported_total, False, False


def _index_page_rows(
    redis: Redis, namespace: str, offset: int, limit: int
) -> Tuple[List[ExecutionInfo], int]:
    """按索引取一页（ZREVRANGE 下推 offset/limit），只投影当页。

    悬空成员（状态键已被 TTL/外部删除）就地 ZREM，并继续向后取直到本页
    填满——不返回空洞/短页。
    """
    index_key = execution_index_key(namespace)
    total = int(redis.zcard(index_key) or 0)
    rows: List[ExecutionInfo] = []
    start = offset
    pruned = 0
    stalled = 0
    while len(rows) < limit:
        chunk = max(1, limit - len(rows))
        members = redis.zrevrange(index_key, start, start + chunk - 1)
        if not members:
            break
        ids = [m if isinstance(m, str) else m.decode() for m in members]
        keys = [f"{namespace}:execution:{eid}" for eid in ids]
        summaries = _project_summaries(redis, keys)
        missing = [eid for eid, key in zip(ids, keys) if key not in summaries]
        if missing:
            pruned += len(missing)
            pruned_ok = True
            try:
                redis.zrem(index_key, *missing)
            except Exception as exc:  # noqa: BLE001 - 剪枝失败不阻断本页
                pruned_ok = False
                logger.warning("悬空执行索引成员剪枝失败: %s", exc)
        else:
            pruned_ok = True
        for key in keys:
            summary = summaries.get(key)
            if summary is not None:
                rows.append(_execution_info_from_summary(key, summary))
        if missing:
            # 已剪枝：窗口左移 len(missing)，下一轮从剩余有效成员之后继续，
            # 避免把本窗口里已返回的成员再取一次（重复行）。剪枝成功则索引
            # 严格变小、循环必然收敛；只有 ZREM 连续失败时才放弃本页。
            start += len(ids) - len(missing)
            if not pruned_ok:
                stalled += 1
                if stalled >= 3:
                    break
            continue
        start += len(ids)
        if len(ids) < chunk:
            break
    return rows[:limit], max(0, total - pruned)


def _ready_namespaces_total(
    redis: Redis, namespaces: List[str]
) -> Optional[int]:
    """全部命名空间索引就绪时返回 ZCARD 总和；任一未就绪/读失败返回 None。

    A2R28-03 页界预检：``ZCARD`` 是 O(1)，先算总页界再决定是否投影。
    有任一命名空间未就绪（存量回填中）时放弃预检——total 尚不可知，走
    原有回填/分页路径（响应带 truncated）。
    """
    total = 0
    for namespace in namespaces:
        try:
            if not redis.exists(execution_index_ready_key(namespace)):
                return None
            total += int(redis.zcard(execution_index_key(namespace)) or 0)
        except Exception as exc:  # noqa: BLE001 - 读失败只跳过预检
            logger.warning(
                "执行索引基数读取失败（namespace=%s，跳过页界预检）: %s",
                namespace, exc,
            )
            return None
    return total


def _filtered_index_rows(
    redis: Redis,
    namespace: str,
    offset: int,
    limit: int,
    status: Optional[str],
    flow_id: Optional[str],
) -> Tuple[List[ExecutionInfo], int, bool]:
    """带过滤时按索引顺序分块投影；候选/墙钟触界返回 truncated。

    返回 (最多 offset+limit 行, 已找到的匹配数, truncated)。截断时匹配数
    是下界（响应里 truncated=true 已显式标注）。
    """
    need = offset + limit
    index_key = execution_index_key(namespace)
    total = int(redis.zcard(index_key) or 0)
    rows: List[ExecutionInfo] = []
    matched = 0
    scanned = 0
    start = 0
    truncated = False
    deadline = time.monotonic() + EXECUTION_FILTER_TIME_BUDGET_S
    while start < total:
        if (
            scanned >= EXECUTION_FILTER_MAX_CANDIDATES
            or time.monotonic() >= deadline
        ):
            truncated = True
            break
        chunk = min(
            EXECUTION_INDEX_PAGE_CHUNK,
            EXECUTION_FILTER_MAX_CANDIDATES - scanned,
        )
        members = redis.zrevrange(index_key, start, start + chunk - 1)
        if not members:
            break
        ids = [m if isinstance(m, str) else m.decode() for m in members]
        keys = [f"{namespace}:execution:{eid}" for eid in ids]
        summaries = _project_summaries(redis, keys)
        for key in keys:
            summary = summaries.get(key)
            if summary is None:
                continue
            if status and (summary.get("status") or "") != status:
                continue
            if flow_id and (summary.get("flow_id") or "") != flow_id:
                continue
            matched += 1
            if len(rows) < need:
                rows.append(_execution_info_from_summary(key, summary))
        start += len(ids)
        scanned += len(ids)
        if len(ids) < chunk:
            break
    if truncated:
        logger.warning(
            "执行列表过滤扫描触界（namespace=%s）：已扫描 %d/%d 个索引成员，"
            "本次 total=%d 是已找到匹配数的下界",
            namespace,
            scanned,
            total,
            matched,
        )
    return rows, matched, truncated


def _namespace_rows(
    redis: Redis,
    namespace: str,
    offset: int,
    limit: int,
    status: Optional[str],
    flow_id: Optional[str],
) -> Tuple[List[ExecutionInfo], int, bool]:
    """单命名空间取行：索引就绪走真分页，否则有界扫描兜底。"""
    index_ready = False
    try:
        index_ready = bool(redis.exists(execution_index_ready_key(namespace)))
    except Exception as exc:  # noqa: BLE001 - 就绪标记读失败按未就绪处理
        logger.warning("执行索引就绪标记读取失败（按未就绪处理）: %s", exc)

    if not index_ready:
        # A2R28-02：分批 + deadline 回填（可断点续跑）；完成前返回已采到的行
        # 并标 truncated=true，完成瞬间直接走索引当页（只投影本页）。
        rows, indexed, complete, full_scan = _backfill_execution_index(
            redis,
            namespace,
            status=status,
            flow_id=flow_id,
            limit=offset + limit,
            deadline=time.monotonic() + EXECUTION_BACKFILL_DEADLINE_S,
        )
        if not complete:
            return rows, indexed, True
        if full_scan:
            return rows, indexed, False
        if status or flow_id:
            return _filtered_index_rows(
                redis, namespace, offset, limit, status, flow_id
            )
        page_rows, total = _index_page_rows(redis, namespace, offset, limit)
        return page_rows, total, False

    if status or flow_id:
        return _filtered_index_rows(
            redis, namespace, offset, limit, status, flow_id
        )
    rows, total = _index_page_rows(redis, namespace, offset, limit)
    return rows, total, False


def _heal_terminal_ttl(redis: Redis, key: str, info: Dict[str, Any]) -> None:
    """C4-3 读时补偿：fenced 写路径落盘的终态键没有 TTL（其 CAS Lua 只 SET），
    控制面首次读到时补 EXPIRE，使终态键最终自清理。失败不影响本次读取。"""
    try:
        if info.get("status") in ("completed", "error", "cancelled"):
            if redis.ttl(key) == -1:
                ttl_seconds = execution_state_ttl_seconds()
                if ttl_seconds > 0:
                    redis.expire(key, ttl_seconds)
    except Exception:
        pass


def _find_execution(
    request: Request, redis: Redis, execution_id: str
) -> tuple[Optional[str], Optional[Dict[str, Any]]]:
    """按租户上下文定位执行状态。

    租户视角只查本租户 namespace；平台视角（tenant=None）先查 default
    （历史前缀），再 SCAN 各租户前缀。返回 (tenant_id, data)。
    """
    tenant = tenant_scope(request)
    if tenant:
        key = _exec_key(tenant, execution_id)
        raw = redis.get(key)
        if not raw:
            return tenant, None
        data = json.loads(raw)
        _heal_terminal_ttl(redis, key, data)
        return tenant, data

    key = _exec_key(None, execution_id)
    raw = redis.get(key)
    if raw:
        data = json.loads(raw)
        _heal_terminal_ttl(redis, key, data)
        return "default", data
    for key in redis.scan_iter(match=f"plaita:*:execution:{execution_id}"):
        key_str = key if isinstance(key, str) else key.decode()
        if _is_mechanism_key(key_str):
            continue
        raw = redis.get(key_str)
        if raw:
            data = json.loads(raw)
            _heal_terminal_ttl(redis, key_str, data)
            return _tenant_from_key(key_str), data
    return None, None


def suspended_subscription_id(context: Any) -> Optional[str]:
    """从执行 checkpoint 取「当前挂起节点」的订阅 id（plaita#221）。

    console 的 ``POST /executions/{id}/resume``（审批通过）是**手工** resume：
    用户载荷（如 ``{"approved": true}``）里没有 event_filter 代投时携带的
    ``subscription_id``。准入校验要求 resume 绑定挂起期登记的那条订阅，故这里
    从 ``context.$NODE[$LAST_NODE]`` 取绑定值，以私有载体键放进 resume 数据
    （引擎分发前剥掉，不进 ``event_data`` / 流程输出）。

    返回 ``None`` 表示 checkpoint 里没有可用的绑定信息（非挂起 / 历史形状）——
    调用方保持原行为，由引擎侧准入校验裁决。
    """
    if not isinstance(context, dict):
        return None
    last_node = context.get("$LAST_NODE")
    node_results = context.get("$NODE")
    if not last_node or not isinstance(node_results, dict):
        return None
    state = node_results.get(last_node)
    if not isinstance(state, dict):
        return None
    subscription_id = state.get("subscription_id")
    return str(subscription_id) if subscription_id else None


# ---- plaita#37 P2：token/成本的两级展示投影（列表/详情共用） ----

def project_usage(usage: Any) -> Optional[Dict[str, Any]]:
    """run 级 usage → 展示视图；``None``/坏形状 ⇒ ``None``（未采集，不是 0）。"""
    return usage_view(usage)


def project_node_usage(context: Any) -> Optional[Dict[str, Any]]:
    """节点级 usage 只读投影（真源 ``context.$NODE[*].usage``）。

    没有任何节点报 usage ⇒ ``None``（未采集）；某节点报 0 ⇒ 该节点在表里且
    ``total_tokens == 0``（采集到 0）——两级都可分。
    """
    try:
        breakdown = node_usage_breakdown(node_results_from_context(context))
    except Exception:  # noqa: BLE001 — 展示投影失败绝不把详情接口带崩
        logger.warning("node_usage 投影失败（按未采集返回）", exc_info=True)
        return None
    out = {node_id: view for node_id, view in
           ((nid, usage_view(entry)) for nid, entry in breakdown.items()) if view}
    return out or None


def _with_usage(info: Dict[str, Any]) -> Dict[str, Any]:
    """给执行行补上 usage/node_usage（就地返回同一 dict，调用方自持副本）。

    集群档列表路径**不**走这里（列表投影刻意不读 context）：见
    ``_execution_info_from_summary``——那里的 ``node_usage`` 恒为 null，
    因为该视图根本没有节点数据（不是「节点未报 usage」）。
    """
    info["usage"] = project_usage(info.get("usage"))
    info["node_usage"] = project_node_usage(info.get("context"))
    return info


# ============ API 端点 ============
@router.get("/executions", response_model=ExecutionListResponse)
async def list_executions(
    request: Request,
    page: int = Query(default=1, ge=1, le=10_000),
    size: int = Query(default=20, ge=1, le=EXECUTION_LIST_MAX_SIZE),
    status: Optional[str] = None,
    flow_id: Optional[str] = None,
    redis: Optional[Redis] = Depends(get_redis_or_none),
):
    """
    获取执行实例列表
    
    - **page**: 页码（从 1 开始）
    - **size**: 每页数量（上限 200；超限 422）
    - **status**: 按状态筛选
    - **flow_id**: 按流程 ID 筛选

    A2R20-03：索引就绪时按 start_time ZSET 真分页，只投影当页；存量库首次
    访问用有界 SCAN 兜底并回填索引。触界返回 ``truncated=true``。
    """
    truncated = False
    local = get_local_executor(request)
    if local is not None:
        executions = [
            # 两批各自独立加了包装（#166：凭据脱敏 ‖ #37 P2：usage 汇总）⇒ 组合：
            # **先用原始 context 算 usage，最后做脱敏**——`_with_usage` 需要未脱敏的
            # context 才能算出 node_usage；`_redact_execution_view` 返回新 dict 且
            # 不动盘上对象，放最后可保证脱敏后不再有任何字段被追加。
            ExecutionInfo(**_redact_execution_view(_with_usage(info)))
            for info in local.list_local_executions(
                status=status, flow_id=flow_id, tenant_id=tenant_scope(request)
            )
        ]
        total = len(executions)
    else:
        tenant = tenant_scope(request)
        # 平台视角覆盖 default（历史前缀）+ 各租户命名空间；每个命名空间只取
        # 全局窗口（offset+size）内的行，合并后排序切片——page=2 不再全扫。
        offset = (page - 1) * size
        need = offset + size
        namespaces = _execution_namespaces(request, tenant, redis)
        # A2R28-03：页界预检（无过滤时）。索引就绪的命名空间用 ZCARD 直接
        # 算总数，offset 越界立即空返回——不再把 offset+size 行历史投影出来
        # 再切片（120k 下 page=601 曾 4.24s / 122,404 命令，并冻结并发
        # /health 4.15s）。ZCARD 是 O(1)，越界页只花几条命令。
        if not status and not flow_id:
            known_total = _ready_namespaces_total(redis, namespaces)
            if known_total is not None and offset >= known_total:
                return ExecutionListResponse(
                    executions=[],
                    total=known_total,
                    page=page,
                    size=size,
                    truncated=False,
                )
        executions = []
        total = 0
        for namespace in namespaces:
            rows, ns_total, ns_truncated = _namespace_rows(
                redis, namespace, 0, need, status, flow_id
            )
            executions.extend(rows)
            total += ns_total
            truncated = truncated or ns_truncated
    
    # 按开始时间排序（最新的在前）
    executions.sort(
        key=lambda x: x.start_time or "",
        reverse=True
    )
    
    # 分页
    start = (page - 1) * size
    end = start + size
    paginated = executions[start:end]
    
    return ExecutionListResponse(
        executions=paginated,
        total=total,
        page=page,
        size=size,
        truncated=truncated,
    )


@router.get("/executions/{execution_id}", response_model=ExecutionInfo)
async def get_execution(
    execution_id: str,
    request: Request,
    redis: Optional[Redis] = Depends(get_redis_or_none),
):
    """
    获取执行详情
    
    - **execution_id**: 执行 ID
    """
    if (local := get_local_executor(request)) is not None:
        info = local.get_local_execution(execution_id, tenant_id=tenant_scope(request))
        if info is None:
            raise HTTPException(status_code=404, detail=f"执行不存在: {execution_id}")
        # #166：读面在**序列化之前**脱敏（``$INPUT`` 是功能数据，保留在盘上与
        # 载体里；只有表示面抹值）
        info = _redact_execution_view(dict(info))
        info["langfuse_trace_url"] = obs_link.langfuse_trace_url(execution_id)
        return ExecutionInfo(**_with_usage(info))

    _tenant, data = _find_execution(request, redis, execution_id)
    if not data:
        raise HTTPException(status_code=404, detail=f"执行不存在: {execution_id}")

    try:
        data = _redact_execution_view(dict(data))
        data["langfuse_trace_url"] = obs_link.langfuse_trace_url(execution_id)
        data["dedup_discarded"] = _read_dedup_discard(redis, execution_id)
        # plaita#37 P2：集群档详情 —— run 级 usage 来自 worker 落盘的
        # ExecutionState.usage；节点级从 context.$NODE 只读投影（列表路径不做，
        # 见 _execution_info_from_summary）。
        return ExecutionInfo(**_with_usage(data))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=scrubbed_detail(
                logger, "execution_data_parse_failed", "执行数据解析失败", e
            ),
        )


@router.post("/executions")
async def start_execution(
    request: StartFlowRequest,
    http_request: Request,
    redis: Optional[Redis] = Depends(get_redis_or_none),
):
    """
    启动新的流程执行
    
    - **flow_id**: 流程 ID
    - **version**: 流程版本
    - **params**: 输入参数
    """
    # 本地单机模式：console 进程内直接执行
    if (local := get_local_executor(http_request)) is not None:
        tenant = tenant_scope(http_request, required=True)
        try:
            info = local.start_local_execution(
                flow_store.get_flow_store(), request.flow_id, request.version,
                request.params, tenant_id=tenant,
            )
        except LookupError as e:
            raise HTTPException(status_code=404, detail=error_detail("flow_not_found", str(e)))
        except ValueError as e:
            raise HTTPException(
                status_code=400,
                detail=error_detail("invalid_execution_request", str(e)),
            )
        _audit(http_request, "execution.start", info["execution_id"],
               {"flow_id": request.flow_id, "version": request.version,
                "mode": "local"}, tenant_id=tenant)
        return {
            "status": "running",
            "flow_id": request.flow_id,
            "execution_id": info["execution_id"],
            "message": "本地模式：流程已在 console 进程内启动",
        }

    # 构建任务消息。execution_id 提交时铸造并随消息透传（worker
    # start_flow 认账）——调用方即刻可凭 id 轮询/取消，无需等 worker
    # 消费后从执行列表里猜（P0 可见性修复的 API 侧一半）。
    execution_id = uuid.uuid4().hex
    tenant = tenant_scope(http_request, required=True)

    # start 幂等键生产侧认领（plaita#274/#280/#285/r59）：token 化 Lua 状态机。
    # 命中后区分「行/载体已可读」与「在途认领」：前者幂等返回，后者异步等行；
    # A 失去认领（过期/墓碑/被接管）时不得再写行/入队/覆盖 owner，更不能
    # 返回自己的 ghost id。新认领才有 token；后续每步转换都校验它。
    claim_token: Optional[str] = None
    if request.dedup_key:
        for _attempt in range(3):
            claim = _claim_start_dedup_at_post(
                redis, tenant, request.dedup_key, execution_id
            )
            if claim.error:
                if claim.error == "legacy_owner_collision":
                    raise HTTPException(
                        status_code=409,
                        detail=error_detail(
                            "execution_start_legacy_collision",
                            "旧 owner 键与另一 dedup_key 的映射键冲突且无法安全迁移，"
                            "请先按 runbook 清理旧 console 的 owner 键后重试",
                            error="execution_start_legacy_collision",
                        ),
                        headers={"Retry-After": "1"},
                    )
                raise HTTPException(
                    status_code=503,
                    detail=error_detail(
                        "execution_start_claim_failed",
                        "start 幂等认领暂时不可用，请稍后重试",
                    ),
                    headers={"Retry-After": "1"},
                )
            if claim.claimed:
                claim_token = claim.token
                break
            mapped = claim.mapped
            if not mapped:
                raise HTTPException(
                    status_code=503,
                    detail=error_detail(
                        "execution_start_claim_failed",
                        "start 幂等认领暂时不可用，请稍后重试",
                    ),
                    headers={"Retry-After": "1"},
                )
            row = _load_full_state(redis, _exec_key(tenant, mapped))
            if row is not None:
                response = await _dedup_hit_row_response(
                    redis, tenant, request, http_request, mapped, row
                )
                if response is not None:
                    return response
                continue
            owner = _dedup.read_owner(redis, tenant, request.dedup_key)
            if (
                owner.state in (_dedup.STATE_CLAIMING, _dedup.STATE_ENQUEUEING)
                and owner.execution_id == mapped
                and not _execution_tombstoned(redis, mapped)
            ):
                # 并发同 key：认领已建立但占位行尚未落盘。异步等行；等不到
                # 且认领仍有效 ⇒ 409 + Retry-After（不返回无载体的 ghost id）。
                for _ in range(10):
                    await asyncio.sleep(0.05)
                    row = _load_full_state(redis, _exec_key(tenant, mapped))
                    if row is not None:
                        break
                if row is not None:
                    response = await _dedup_hit_row_response(
                        redis, tenant, request, http_request, mapped, row
                    )
                    if response is not None:
                        return response
                    continue
                if (
                    _read_start_dedup_map(redis, tenant, request.dedup_key) == mapped
                    and not _execution_tombstoned(redis, mapped)
                ):
                    _audit(
                        http_request, "execution.start", mapped,
                        {"flow_id": request.flow_id, "mode": "queue",
                         "dedup_hit": True, "dedup_inflight": True,
                         "dedup_key": request.dedup_key},
                        tenant_id=tenant,
                    )
                    raise _start_in_progress_error(mapped, request.dedup_key)
            # 孤儿映射（行不存在；force DELETE 墓碑或 owner 过期/被接管）：
            # Lua 条件释放（原子复核 token/row/墓碑），随后重试认领。
            _release_start_dedup_if_owner(
                redis, tenant, request.dedup_key, mapped,
                require_row_absent=True,
            )
        else:
            _audit(
                http_request, "execution.start", execution_id,
                {"flow_id": request.flow_id, "mode": "queue",
                 "dedup_key": request.dedup_key, "dedup_contended": True},
                tenant_id=tenant,
            )
            raise _start_in_progress_error(execution_id, request.dedup_key)
        if claim_token is None:
            raise HTTPException(
                status_code=503,
                detail=error_detail(
                    "execution_start_claim_failed",
                    "start 幂等认领暂时不可用，请稍后重试",
                ),
                headers={"Retry-After": "1"},
            )

    message = {
        "type": "start",
        "flow_id": request.flow_id,
        "version": request.version,
        "params": request.params,
        "execution_id": execution_id,
        "tenant_id": tenant,
        "timestamp": datetime.now().isoformat()
    }
    # start 幂等键 additive 透传（波次二任务③）：仅显式提供时进入消息体，
    # 旧客户端消息形状零变化。与 G1 预铸 execution_id 并存：同一条消息
    # 既带预铸 id（提交方即刻可轮询）又可选带幂等键（重投收敛）。
    if request.dedup_key:
        message["dedup_key"] = request.dedup_key
    # 每租户 start 准入配额（plaita#28）：超限 **429**，不入队。名额在入口
    # 即占用（ZSET 成员=execution_id），worker 终态化时释放（崩溃由条目
    # TTL 自愈）。必须在「入口」拒绝——单 stream FIFO 下，消息一旦入队就
    # 排死了顺序，事后在 worker 侧再拦已经拦不住积压的形成。未配额租户 /
    # redis 故障 fail-open（零行为变化）；回滚开关见 tenant_quota 模块。
    if not try_admit_tenant_start(redis, tenant, execution_id):
        max_running = tenant_max_running(redis, tenant)
        logger.warning(
            "租户 %s start 配额已满（%r），拒绝 start: flow=%s execution=%s",
            tenant or "default", max_running, request.flow_id, execution_id,
        )
        # 认领失败路径不能让同 key 重试撞到一个永远没有行的在途映射。
        if request.dedup_key:
            _release_start_dedup_if_owner(
                redis, tenant, request.dedup_key, execution_id,
                token=claim_token,
            )
        raise HTTPException(
            status_code=429,
            detail=error_detail(
                "tenant_quota_exceeded",
                (
                    f"租户 {tenant or 'default'} 并发 start 数已达上限"
                    f"（{max_running}），请等待在跑执行终态后重试"
                ),
                error="tenant_quota_exceeded",
                tenant_id=tenant or "default",
                max_running=max_running,
            ),
        )
    # 入队**之前**同步落 queued 占位状态行（plaita#273）：POST 返回的 id
    # 立刻可 GET/列表可见，worker 消费时以 running 行覆盖接管（占位行空
    # context，worker 按「非终态+空 checkpoint=首投」处理，不误转 resume）。
    # 落盘失败即 5xx 并回滚配额，不入队（不带病返回 200）。
    _now_iso = datetime.now().isoformat()
    # plaita#285：写占位行前复核所有权（map/owner 仍属我、无墓碑）。慢存储
    # 导致 claiming TTL 过期但无人接管时原地续期；被接管/删除即放弃。
    if request.dedup_key and not _start_claim_guard(
        redis, tenant, request.dedup_key, execution_id, claim_token
    ):
        release_tenant_start(redis, tenant, execution_id)
        _release_start_dedup_if_owner(
            redis, tenant, request.dedup_key, execution_id,
            token=claim_token,
        )
        return await _start_claim_lost_response(
            redis, tenant, request.dedup_key, execution_id, request,
            http_request, stage="before_placeholder",
        )
    if not _write_queued_placeholder(
        redis, tenant, execution_id, request.flow_id, request.version, _now_iso
    ):
        release_tenant_start(redis, tenant, execution_id)
        if request.dedup_key:
            # 失败路径写墓碑 + 释放映射：等待中的同 key 请求重查后换新 id，
            # 不会把本 id 当 ghost 200 长期持有（#285）。
            _discard_queued_placeholder(redis, tenant, execution_id)
            _release_start_dedup_if_owner(
                redis, tenant, request.dedup_key, execution_id,
                token=claim_token,
            )
        raise HTTPException(
            status_code=503,
            detail=error_detail(
                "execution_state_persist_failed",
                "执行状态落盘失败，请稍后重试",
            ),
            headers={"Retry-After": "1"},
        )
    # 入队前原子 reservation（claimed/row → enqueueing）：token/墓碑/映射在
    # 同一 Lua 内复核；失配则清掉自己的行并放弃入队（#286/r59）。
    if request.dedup_key and not _dedup.reserve_enqueue(
        redis, tenant, request.dedup_key, execution_id, claim_token
    ):
        release_tenant_start(redis, tenant, execution_id)
        _discard_queued_placeholder(redis, tenant, execution_id)
        _release_start_dedup_if_owner(
            redis, tenant, request.dedup_key, execution_id,
            token=claim_token,
        )
        return await _start_claim_lost_response(
            redis, tenant, request.dedup_key, execution_id, request,
            http_request, stage="before_enqueue",
        )
    # 写入任务队列 Stream（FlowWorker 消费组消费）
    enqueued_msg_id: Optional[str] = None
    try:
        enqueued_msg_id = _enqueue(message, redis)
    except Exception:
        # 入队失败回滚名额，防泄漏（否则该 execution 的名额要等 TTL 才自愈）；
        # 占位行同步清掉，不留无人消费的幽灵 queued 行（清理失败留 ERROR）。
        release_tenant_start(redis, tenant, execution_id)
        _discard_queued_placeholder(redis, tenant, execution_id)
        if request.dedup_key:
            _release_start_dedup_if_owner(
                redis, tenant, request.dedup_key, execution_id,
                token=claim_token,
            )
        raise
    if request.dedup_key:
        from plaita.server.task_queue import START_DEDUP_TTL_SECONDS

        # owner CAS：载体已入队但若映射已被换走/墓碑化，绝不能再把自己写成
        # enqueued（覆盖 B 的 owner）；清掉自己的行并收敛到当前映射或 409。
        if not _set_start_dedup_owner(
            redis, tenant, request.dedup_key, execution_id,
            state=_START_DEDUP_OWNER_ENQUEUED,
            ttl=START_DEDUP_TTL_SECONDS,
            allow_from_state=_START_DEDUP_OWNER_CLAIMING,
            token=claim_token,
        ):
            # #289：mark 失败时 carrier 可能已被 worker 消费并执行——此时
            # discard/墓碑/XDEL 撤不回运行，重试会换新 id 双执行。先原地恢复
            # （guard→reserve→mark）；map 仍属本 id 且无墓碑时只 park，保留
            # 载体/占位行/映射，同 key 重试收敛原 id；只有映射换主/墓碑才清收。
            if _dedup.recover_after_mark_failure(
                redis, tenant, request.dedup_key, execution_id, claim_token
            ):
                row = _load_full_state(redis, _exec_key(tenant, execution_id))
                status = str((row or {}).get("status") or "queued")
                _audit(http_request, "execution.start", execution_id,
                       {"flow_id": request.flow_id, "version": request.version,
                        "mode": "queue", "dedup_key": request.dedup_key,
                        "mark_recovered": True, "status": status},
                       tenant_id=tenant)
                return {
                    "status": status,
                    "flow_id": (row or {}).get("flow_id") or request.flow_id,
                    "execution_id": execution_id,
                    "message": "流程启动请求已受理（mark 恢复后原 id 收敛）",
                }
            mapped = _read_start_dedup_map(redis, tenant, request.dedup_key)
            tombstoned = _execution_tombstoned(redis, execution_id)
            if mapped == execution_id and not tombstoned:
                _dedup.mark_parked(
                    redis, tenant, request.dedup_key, execution_id
                )
                logger.warning(
                    "start mark 失败但载体可能已被消费：park 原 id（不 discard/"
                    "不 XDEL，同 key 重试收敛）: dedup_key=%s execution=%s",
                    request.dedup_key, execution_id,
                )
                _audit(http_request, "execution.start", execution_id,
                       {"flow_id": request.flow_id, "version": request.version,
                        "mode": "queue", "dedup_key": request.dedup_key,
                        "mark_parked": True},
                       tenant_id=tenant)
                raise _start_in_progress_error(execution_id, request.dedup_key)
            # 映射已换主/墓碑：A 载体不会被 worker 执行（dedup hit/墓碑闸），
            # 可以安全清收占位行与迟到载体。
            release_tenant_start(redis, tenant, execution_id)
            _discard_queued_placeholder(redis, tenant, execution_id)
            _safe_xdel_carrier(redis, enqueued_msg_id)
            return await _start_claim_lost_response(
                redis, tenant, request.dedup_key, execution_id, request,
                http_request, stage="after_enqueue",
            )


    _audit(http_request, "execution.start", execution_id,
           {"flow_id": request.flow_id, "version": request.version,
            "mode": "queue"}, tenant_id=tenant)
    return {
        "status": "queued",
        "flow_id": request.flow_id,
        "execution_id": execution_id,
        "message": "流程启动请求已加入队列"
    }


@router.post("/executions/{execution_id}/cancel")
async def cancel_execution(
    execution_id: str,
    request: Request,
    redis: Optional[Redis] = Depends(get_redis_or_none),
    body: Optional[EmptyRequestModel] = None,
):
    """
    取消/终止执行（发送取消命令到队列）
    
    - **execution_id**: 执行 ID
    """
    # 本地单机模式：标记状态（尽力而为，不中断线程）
    if (local := get_local_executor(request)) is not None:
        tenant = tenant_scope(request)
        if not local.cancel_local_execution(execution_id, tenant_id=tenant):
            raise HTTPException(status_code=404, detail=f"执行不存在: {execution_id}")
        _audit(request, "execution.cancel", execution_id, tenant_id=tenant)
        return {
            "success": True,
            "status": "cancelled",
            "execution_id": execution_id,
            "message": "执行已取消（本地模式）",
        }

    tenant, info = _find_execution(request, redis, execution_id)
    if not info:
        raise HTTPException(status_code=404, detail=f"执行不存在: {execution_id}")

    try:
        flow_id = info.get("flow_id")
        status = info.get("status")
    except Exception:
        raise HTTPException(status_code=500, detail="数据解析失败")

    # 终态幂等：已终态的执行不再取消（与 worker 侧 resume_flow 的终态短路
    # 同一语义——否则对 completed 执行的 cancel 会把状态改写成 cancelled）
    if status in ("completed", "error", "cancelled"):
        if status == "cancelled":
            # plaita#280 顺带 P2：旧版 CAS+release 非原子的遗留行，再 CANCEL
            # 命中终态短路时补一次幂等释放（ZREM 不存在即 no-op）。
            release_tenant_start(redis, tenant, execution_id)
        return {
            "success": True,
            "status": status,
            "execution_id": execution_id,
            "message": f"执行已是终态（{status}），忽略取消",
        }

    # queued（已受理、worker 未接管）直接终态化（plaita#273）：worker 停机/
    # 积压期的排队执行必须可取消，且配额立即回收（不等条目 TTL 自愈）。
    # 顺序：先写取消标志——覆盖「console 置终态与 worker 消费落 running」的
    # 竞窗（标志在位时 worker 的 start 入口检查仍会终态化 cancelled）；再投
    # cancel 消息（保留：PEL/重投语义）；最后 CAS 仅当行仍是 queued 时置
    # 终态——CAS 输了说明 worker 恰好接管（running），走下方标志+消息路径。
    if status == "queued":
        redis.set(
            _cancel_key(tenant, execution_id),
            datetime.now().isoformat(),
            ex=CANCEL_FLAG_TTL_SECONDS,
        )
        _enqueue(
            {
                "type": "resume",
                "flow_id": flow_id,
                "execution_id": execution_id,
                "resume_type": "cancel",
                "tenant_id": tenant or "default",
                "data": None,
                "timestamp": datetime.now().isoformat(),
            },
            redis,
        )
        _now = datetime.now().isoformat()
        cancelled_row = dict(info)
        cancelled_row["status"] = "cancelled"
        cancelled_row["end_time"] = _now
        cancelled_row["last_update_time"] = _now
        try:
            cas = redis.eval(
                _CANCEL_QUEUED_ROW_LUA,
                2,
                _exec_key(tenant, execution_id),
                tenant_running_key(tenant),
                json.dumps(cancelled_row),
                str(execution_state_ttl_seconds()),
                execution_id,
            )
        except Exception as exc:  # noqa: BLE001 — CAS 失败不回滚取消意图（标志已落）
            cas = -1
            logger.warning(
                "queued 执行条件终态化失败（取消意图已落，worker 消费时终态化）: "
                "execution=%s: %s", execution_id, exc,
            )
        if cas == 1:
            release_tenant_start(redis, tenant, execution_id)
        elif cas == -1:
            logger.warning(
                "queued 执行状态行损坏，跳过条件终态化（标志+消息路径兜底）: "
                "execution=%s", execution_id,
            )
        _audit(request, "execution.cancel", execution_id, tenant_id=tenant)
        return {
            "success": True,
            "status": "cancelled",
            "execution_id": execution_id,
            "message": (
                "执行已取消（排队中，配额已释放）" if cas == 1
                # 竞态：worker 恰好接管——标志+消息路径生效，配额由其终态释放
                else "执行已取消"
            ),
        }

    # 取消消息入队（保留）：worker 全灭后的死人开关——消息留在 pending，
    # worker 恢复后经 XCLAIM 重投、resume 入口取消检查点终态化。
    message = {
        "type": "resume",
        "flow_id": flow_id,
        "execution_id": execution_id,
        "resume_type": "cancel",
        "tenant_id": tenant or "default",
        "data": None,
        "timestamp": datetime.now().isoformat()
    }
    _enqueue(message, redis)

    if status == "suspended":
        # 挂起执行保持现状（已验证路径，设计稿 §3.5 兼容红线）：直接写
        # cancelled + 入队 cancel 消息；worker resume_flow 终态短路原样
        # 返回，EventNode 的 on_cancel 不会续跑到 end。终态键带 TTL
        # （C4-3，与 storage 层 save 侧同规则）自清理。
        info["status"] = "cancelled"
        info["end_time"] = datetime.now().isoformat()
        _exec_payload = json.dumps(info)
        ttl_seconds = execution_state_ttl_seconds()
        if ttl_seconds > 0:
            redis.set(_exec_key(tenant, execution_id), _exec_payload, ex=ttl_seconds)
        else:
            redis.set(_exec_key(tenant, execution_id), _exec_payload)
    else:
        # 运行中执行（波次① §3.1）：只表达取消意图——写标志键（7 天 TTL
        # 自清理），不再直接写 status=cancelled。推进中的 worker 每步会把
        # 内存 state 覆写回 running，直写必被覆写；worker 在步界检查点
        # 消费标志键后终态化（cancelled 的 context = 取消点前一步 checkpoint）。
        redis.set(
            _cancel_key(tenant, execution_id),
            datetime.now().isoformat(),
            ex=CANCEL_FLAG_TTL_SECONDS,
        )

    _audit(request, "execution.cancel", execution_id, tenant_id=tenant)
    return {
        "success": True,
        "status": "cancelled",
        "execution_id": execution_id,
        "message": "执行已取消",
    }


@router.delete("/executions/{execution_id}")
async def delete_execution(
    execution_id: str,
    request: Request,
    force: bool = Query(
        False,
        description=(
            "对 running/suspended 执行显式强制删除：先写取消意图并投 cancel "
            "消息，再删除状态并吊销 fence/lease。默认对非终态执行返回 409，"
            "要求先取消到终态"
        ),
    ),
    redis: Optional[Redis] = Depends(get_redis_or_none),
    body: Optional[EmptyRequestModel] = None,
):
    """
    删除执行记录（集群档：状态 + 索引 + 取消标志 + 事件频道 + fence/lease）

    - **execution_id**: 执行 ID
    - **force**: 非终态执行需显式强制（默认 409，指引先取消）
    """
    # 本地单机模式：先停执行（取消标志/线程汇合），再持久删除记录与日志。
    # 线程汇合最长 LOCAL_DELETE_JOIN_TIMEOUT_SECONDS 秒——放线程池执行，
    # 不阻塞 console 事件循环（与 logs SSE 非阻塞修复同一红线）。
    if (local := get_local_executor(request)) is not None:
        tenant = tenant_scope(request)
        try:
            deleted = await asyncio.to_thread(
                local.delete_local_execution,
                execution_id,
                tenant_id=tenant,
                force=force,
            )
        except local.ExecutionActiveError as exc:
            raise HTTPException(
                status_code=409, detail=error_detail("execution_active", str(exc))
            ) from exc
        if not deleted:
            raise HTTPException(status_code=404, detail=f"执行不存在: {execution_id}")
        _audit(request, "execution.delete", execution_id,
               {"mode": "local", "force": force}, tenant_id=tenant)
        return {
            "success": True,
            "execution_id": execution_id,
            "message": "执行记录已删除",
        }

    _tenant, _data = _find_execution(request, redis, execution_id)
    if not _data:
        raise HTTPException(status_code=404, detail=f"执行不存在: {execution_id}")
    _status = str(_data.get("status") or "")

    # 终态守卫（A43-01）：非终态删除默认拒绝——否则用户在「删除后不可恢复」
    # 的确认文案下把一条仍在推进的执行删掉，调用方无法区分「已停止」与
    # 「稍后复活」。显式 force 才允许，并且先走取消意图再吊销。
    if _status not in TERMINAL_EXECUTION_STATUSES and not force:
        raise HTTPException(
            status_code=409,
            detail=(
                f"执行仍在 {_status or '非终态'} 状态，先取消（POST "
                f"/api/executions/{execution_id}/cancel）待终态后删除；"
                "如确需立即删除请显式传 force=true"
            ),
        )

    if _status not in TERMINAL_EXECUTION_STATUSES:
        _signal_cancel_for_force_delete(redis, _tenant, execution_id, _data)

    # 删除即吊销：状态 + 索引成员 + fence 世代键 + lease 租约键（同 pipeline）。
    # 在途 worker 后续 save 因世代失配抛 ExecutionLeaseError，不会把状态写回。
    purged = _delete_cluster_execution_state(redis, _tenant, execution_id)

    # 取消标志与事件频道同事务兜底清理（storage 不拥有这两个键）
    pipe = redis.pipeline()
    pipe.delete(_cancel_key(_tenant, execution_id))
    pipe.delete(_events_key(execution_id))
    pipe.execute()

    if not purged:
        logger.error(
            "删除执行 %s 的状态失败（storage 返回 False），fence/lease 状态"
            "未知；返回 503 供客户端重试",
            execution_id,
        )
        raise HTTPException(
            status_code=503, detail="执行状态删除失败，请稍后重试"
        )

    _audit(request, "execution.delete", execution_id,
           {"force": force, "prior_status": _status}, tenant_id=_tenant)
    return {
        "success": True,
        "execution_id": execution_id,
        "message": "执行记录已删除"
    }


@router.post("/executions/{execution_id}/resume")
async def resume_execution(
    execution_id: str,
    request: ResumeFlowRequest,
    http_request: Request,
    redis: Optional[Redis] = Depends(get_redis_or_none),
):
    """
    恢复暂停的执行
    
    - **execution_id**: 执行 ID
    - **resume_type**: 恢复类型
    - **data**: 恢复数据
    - **params**: 可选调度入参（#157）。**只在 `resume_type=continue` 且盘上
      checkpoint 为空时**被引擎绑定为 `$INPUT`（手动 continue 抢在 pending
      start 被 reclaim 之前的窗口）；空 checkpoint 下 retry/cancel/timeout/
      event 被策略层守卫以 `ResumeGuardError` 拒绝（执行保持 running）；
      不传/空/`null` 不落 `params` 键；checkpoint 非空时 `params` 惰性，
      绑定值一律以盘上 checkpoint 为准。
    """
    # 本地单机模式：从 SQLite checkpoint 继续（分布式策略）
    if (local := get_local_executor(http_request)) is not None:
        try:
            ok = local.resume_local_execution(
                flow_store.get_flow_store(), execution_id, request.resume_type,
                request.data, tenant_id=tenant_scope(http_request),
            )
        except ValueError as e:
            raise HTTPException(
                status_code=400,
                detail=error_detail("invalid_resume_request", str(e)),
            )
        if not ok:
            raise HTTPException(status_code=404, detail=f"执行不存在: {execution_id}")
        _audit(http_request, "execution.resume", execution_id,
               {"resume_type": request.resume_type, "mode": "local"},
               tenant_id=tenant_scope(http_request))
        return {
            "status": "resuming",
            "execution_id": execution_id,
            "resume_type": request.resume_type,
            "message": "恢复请求已受理（本地模式）",
        }

    # 验证执行存在（按租户上下文定位）
    resume_tenant, data = _find_execution(http_request, redis, execution_id)
    if not data:
        raise HTTPException(status_code=404, detail=f"执行不存在: {execution_id}")

    try:
        flow_id = data.get("flow_id")
    except Exception:
        raise HTTPException(status_code=500, detail="数据解析失败")

    # 唤醒预算闸（plaita#73）：error 态执行 + retry 才可能被 worker 幂等拒绝
    # （挂起/运行中执行另有幂等短路语义，与此闸无关）。达限即 409，不静默入队。
    if request.resume_type == "retry" and (data.get("status") or "") == "error":
        block = _retry_budget_block(redis, resume_tenant, execution_id)
        if block is not None:
            raise HTTPException(status_code=409, detail=block)

    # 发送恢复消息
    # #221：手工 event/timeout resume 也要绑定挂起订阅——用户载荷里没有
    # subscription_id（那是 event_filter 代投时的字段），从 checkpoint 取当前挂起
    # 节点的订阅 id，以私有载体键附加（引擎准入校验后剥掉，不进流程输出）。
    resume_data = request.data
    if request.resume_type in ("event", "timeout"):
        binding = suspended_subscription_id(data.get("context"))
        if binding:
            # data=None 的手工决议同样携带绑定（载荷为空 dict），与本地档
            # local_executor._with_resume_binding 的形状保持一致。
            resume_data = dict(resume_data) if isinstance(resume_data, dict) else {}
            resume_data[RESUME_BINDING_KEY] = binding
    message = {
        "type": "resume",
        "flow_id": flow_id,
        "execution_id": execution_id,
        "resume_type": request.resume_type,
        "tenant_id": resume_tenant or "default",
        "data": resume_data,
        "timestamp": datetime.now().isoformat()
    }
    # plaita#157：把请求体里的调度入参透传进 resume 载体。空/未传**不落键**
    # ——与 #122（start 幂等命中转出的 resume 载体）逐字同形：worker 侧
    # `_dispatch_task` 读的是 `message_data.get("params")`，缺键即 None，
    # 存量消息与既有调用方零变化。
    if isinstance(request.params, dict) and request.params:
        message["params"] = dict(request.params)

    _enqueue(message, redis)
    _audit(http_request, "execution.resume", execution_id,
           {"resume_type": request.resume_type, "mode": "queue"},
           tenant_id=resume_tenant)

    return {
        "status": "resuming",
        "execution_id": execution_id,
        "resume_type": request.resume_type,
        "message": "恢复请求已加入队列"
    }


# ---- C4-4：SSE 实时推送（一次性票据 + redis.asyncio pubsub） ----
#
# 两个问题：
# 1. EventSource 无法携带 Authorization/X-Admin-API-Key 头——鉴权部署下
#    SSE 连接必然 401（require_auth 挂在 router 级，main.py `_mount_admin`）。
# 2. 历史实现在 async gen 内调同步 pubsub.get_message(timeout=1.0)——每次
#    等待都阻塞整个事件循环，两个并发 SSE 就能让其他 API 卡顿秒级。
#
# 票据方案：鉴权客户端先 POST /executions/{id}/stream/ticket 换取 60s 单次
# 票据，EventSource 以 ?ticket= 直连——stream 端点**并行接受**票据与标准鉴权。
#
# 实现说明（关键约束）：main.py 以 include_router(dependencies=[require_auth])
# 挂载本 router——FastAPI 只对 APIRoute 附加 include 级依赖，raw Starlette
# Route 不附加（fastapi/routing.py include 路径对 routing.Route 原样重建）。
# 因此 stream 路由用 raw Route 注册，鉴权在端点内自行裁决：有 ticket 参数则
# 严格校验票据（无效即 401），否则回退 require_auth（带头请求照常工作）。
# tests/console 的回归用与 main.py 相同的挂载方式验证票据直连可用——若未来
# FastAPI 改变该语义（raw Route 也附加依赖），该测试会失败报警。
SSE_TICKET_TTL_SECONDS = 60
SSE_TICKET_KEY_PREFIX = "plaita:sse:ticket:"


class StreamTicketResponse(BaseModel):
    """A2R24-09：执行 SSE 的连接票据（stream 本体刻意不进 schema）。"""

    ticket: str = Field(..., description="一次性 SSE 票据（60s、单次使用）")
    expires_in: int = Field(..., description="票据有效期（秒）")


def _sse_ticket_key(ticket: str) -> str:
    return f"{SSE_TICKET_KEY_PREFIX}{ticket}"


@router.post(
    "/executions/{execution_id}/stream/ticket", response_model=StreamTicketResponse
)
async def create_stream_ticket(
    execution_id: str,
    request: Request,
    redis: Redis = Depends(get_redis),
    body: Optional[EmptyRequestModel] = None,
):
    """
    签发 SSE 一次性连接票据（60 秒、单次使用、绑定 execution 与租户上下文）。

    EventSource 无法携带鉴权头，前端先经本端点（带头 fetch）换票据，
    再以 ``?ticket=`` 建立 SSE 连接。
    """
    _tenant, data = _find_execution(request, redis, execution_id)
    if not data:
        raise HTTPException(status_code=404, detail=f"执行不存在: {execution_id}")

    ticket = secrets.token_urlsafe(32)
    payload = json.dumps(
        {
            "execution_id": execution_id,
            "tenant_id": tenant_scope(request),
            # #254：票据必须延续签发方的身份性质——平台身份的 None 才是
            # 全量视角；否则 EventSource 重放时无租户上下文会被 fail-closed。
            "platform_admin": is_platform_request(request),
            "issued_at": datetime.now().isoformat(),
        }
    )
    redis.set(_sse_ticket_key(ticket), payload, ex=SSE_TICKET_TTL_SECONDS)
    _audit(request, "execution.stream_ticket", execution_id, tenant_id=_tenant)
    return {"ticket": ticket, "expires_in": SSE_TICKET_TTL_SECONDS}


def _consume_stream_ticket(
    request: Request, redis: Redis, ticket: str, execution_id: str
) -> None:
    """校验并消费一次性 SSE 票据：无效/过期/已用/execution 不符 → 401。

    GETDEL 原子取走（单次语义）；校验通过后把签发方的租户上下文写回
    request.state——EventSource 请求本身不带任何头，后续 tenant_scope
    据此与签发方视角对齐。
    """
    try:
        raw = redis.getdel(_sse_ticket_key(ticket))
    except Exception as exc:
        logger.warning("SSE 票据校验失败（redis 异常）: %s", exc)
        raise HTTPException(status_code=401, detail="SSE 票据校验失败")
    if not raw:
        raise HTTPException(status_code=401, detail="SSE 票据无效、已使用或已过期")
    try:
        grant = json.loads(raw)
    except Exception:
        raise HTTPException(status_code=401, detail="SSE 票据数据损坏")
    if not isinstance(grant, dict) or grant.get("execution_id") != execution_id:
        raise HTTPException(status_code=401, detail="SSE 票据与目标执行不匹配")
    request.state.actor = "sse-ticket"
    request.state.role = "viewer"
    request.state.auth_source = "sse-ticket"
    # #254：票据如实承载签发方的平台身份；普通用户票据仍钉死具体租户。
    request.state.platform_admin = bool(grant.get("platform_admin", False))
    request.state.tenant_id = grant.get("tenant_id")


def _resolve_stream_auth(
    request: Request, redis: Optional[Redis], execution_id: str
) -> None:
    """stream 路由的双轨鉴权：?ticket= 严格校验，否则标准 require_auth。"""
    ticket = request.query_params.get("ticket")
    if ticket:
        if redis is None:
            # 本地单机模式无从校验票据（票据端点在本地档也不可用）
            raise HTTPException(status_code=401, detail="SSE 票据在本地单机模式不可用")
        _consume_stream_ticket(request, redis, ticket, execution_id)
        return
    require_auth(request)


def _execution_stream_async_redis(request: Request):
    """惰性创建/复用 app 级 ``redis.asyncio`` 客户端（仅 SSE pubsub 使用）。

    与 lifespan 的同步客户端并存：SSE 消费必须非阻塞，同步 pubsub 的
    get_message(timeout=1.0) 会阻塞整个事件循环。实现在 ``sse_redis.py``，
    与 logs SSE 共用同一客户端；测试可直接向
    ``app.state.execution_sse_aioredis`` 注入 fake 客户端。
    """
    return get_sse_async_redis(request)


async def _execution_event_stream(request: Request, data: Dict[str, Any], channel: str):
    """集群档 SSE 事件流（redis.asyncio pubsub，非阻塞消费）。

    模块级工厂便于直接单测（starlette 1.7 TestClient 会等响应整体完成，
    无法承载无限 SSE 流）；异常不终结流，finally 必关 pubsub。
    """
    pubsub = None
    try:
        try:
            pubsub = _execution_stream_async_redis(request).pubsub()
            await pubsub.subscribe(channel)
        except Exception as exc:
            logger.warning("SSE 订阅失败 %s: %s", channel, exc)
            yield {
                "event": "error",
                "data": json.dumps({"detail": "事件订阅失败，请回落轮询"}),
            }
            return

        # 发送初始状态
        if data:
            yield {
                "event": "initial_state",
                "data": json.dumps(data, ensure_ascii=False, default=str),
            }

        # 持续监听事件
        while True:
            try:
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=1.0
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Redis 抖动不终结 SSE：短暂退避后继续消费
                logger.warning("SSE 消费异常（保持连接）%s: %s", channel, exc)
                await asyncio.sleep(SSE_REDIS_RETRY_SECONDS)
                continue
            if message and message.get("type") == "message":
                payload = message.get("data")
                if isinstance(payload, bytes):
                    payload = payload.decode("utf-8", "replace")
                if payload is not None:
                    yield {"event": "update", "data": payload}

            # 检查客户端是否断开
            if await request.is_disconnected():
                break
    finally:
        if pubsub is not None:
            try:
                await pubsub.unsubscribe(channel)
            except Exception:
                pass
            try:
                await pubsub.aclose()
            except Exception:
                pass


async def stream_execution_endpoint(request: Request):
    """
    SSE 端点：实时推送执行状态变化（raw Route，见上方 C4-4 说明）

    - 集群档：redis.asyncio pubsub 订阅（非阻塞）
    - 本地档：对 SQLite 执行记录做 1s 轮询，变化才推
    """
    execution_id = request.path_params["execution_id"]
    redis = request.app.state.redis
    _resolve_stream_auth(request, redis, execution_id)

    if (local := get_local_executor(request)) is not None:
        info = local.get_local_execution(execution_id, tenant_id=tenant_scope(request))
        if info is None:
            raise HTTPException(status_code=404, detail=f"执行不存在: {execution_id}")

        async def local_event_generator():
            last = None
            # #166：SSE 的 initial_state / update 都是整行执行表示——与详情路由
            # 同一条脱敏，且都在 ``json.dumps`` 之前。
            yield {
                "event": "initial_state",
                "data": json.dumps(_redact_execution_view(dict(info)), ensure_ascii=False),
            }
            while True:
                await asyncio.sleep(1.0)
                current = local.get_local_execution(execution_id, tenant_id=tenant_scope(request))
                if current is None:
                    break
                payload = json.dumps(
                    _redact_execution_view(dict(current)), ensure_ascii=False, default=str
                )
                if payload != last:
                    last = payload
                    yield {"event": "update", "data": payload}
                if current.get("status") in ("completed", "failed", "cancelled"):
                    # S17-04：终态必须显式发 finished（即使与上一条 update 相同），
                    # 保证「最后一条事件一定是 finished」；前端据此正常收尾，
                    # 不再把服务器正常关闭当断连永久轮询。
                    yield {"event": "finished", "data": payload}
                    break
                if await request.is_disconnected():
                    break

        return EventSourceResponse(local_event_generator())

    # 验证执行存在（按租户上下文定位）
    _tenant, data = _find_execution(request, redis, execution_id)
    if not data:
        raise HTTPException(status_code=404, detail=f"执行不存在: {execution_id}")

    channel = f"plaita:execution:events:{execution_id}"
    # #166：cluster 档的 initial_state 是整行执行表示（含 context.$INPUT），
    # 与详情路由共用同一条脱敏；update 事件是 worker 发布的事件载荷（另一个面，
    # 不在本单口径内）。
    return EventSourceResponse(
        _execution_event_stream(request, _redact_execution_view(dict(data)), channel)
    )


# raw Starlette Route：绕过 include 级 require_auth（仅此路由），鉴权在端点内
# 双轨裁决（ticket / require_auth）。不进 OpenAPI schema（FastAPI 只文档化
# APIRoute；SSE 端点本也无文档价值）。
router.add_route(
    "/executions/{execution_id}/stream",
    stream_execution_endpoint,
    methods=["GET"],
    include_in_schema=False,
)


# ============ 执行事件时间线（plaita#25） ============

#: 时间线事件类型白名单（本地档投影只发这两类；集群档由 worker 落盘，
#: 还含 enqueued/run_start/run_end）。
_TIMELINE_NODE_START = "node_start"
_TIMELINE_NODE_END = "node_end"


def _timeline_envelope(execution_id: Optional[str], status: Optional[str],
                       source: str, timeline: Optional[Dict[str, Any]],
                       *, collected: bool, start_time: Any = None,
                       end_time: Any = None) -> Dict[str, Any]:
    """时间线响应信封（集群档与本地档共用一个形状）。

    ``collected=False`` = **未采集**（老状态/非分布式宿主）——与「采集到了但
    事件为空」严格区分，也与 ``queue_wait_ms=None``（排队时长未知）自洽；
    读取方（前端/值守脚本）不得把 None 折算成 0。
    """
    tl = timeline if isinstance(timeline, dict) else {}
    events = tl.get("events")
    if not isinstance(events, list):
        events = []
    return {
        "execution_id": execution_id,
        "status": status,
        "source": source,
        "collected": collected,
        "events": events,
        "dropped": int(tl.get("dropped") or 0),
        "max_events": tl.get("max_events"),
        "enqueued_at": tl.get("enqueued_at"),
        "queue_wait_ms": tl.get("queue_wait_ms"),
        "start_time": start_time,
        "end_time": end_time,
    }


def project_state_timeline(data: Dict[str, Any]) -> Dict[str, Any]:
    """集群档：直接读 worker 落盘的 ``ExecutionState.timeline``。

    老状态（本字段上线前写入）没有 ``timeline`` 键 ⇒ ``collected=False``：
    答案是「没有采集到」，不是「没错过任何事件」。
    """
    tl = data.get("timeline")
    return _timeline_envelope(
        data.get("execution_id"),
        data.get("status"),
        "state",
        tl,
        collected=isinstance(tl, dict),
        start_time=data.get("start_time"),
        end_time=data.get("end_time"),
    )


def project_local_timeline(info: Dict[str, Any]) -> Dict[str, Any]:
    """本地档：由 ``nodes_json`` 投影出节点级事件（本地执行在 console 进程内跑，
    节点 trace 本身就带 ``started_ms``/``ended_ms``，是既有真源的**只读视图**，
    不新增列、不新增写入方）。

    本地档没有 cluster 的入队等待窗口（``start_local_execution`` 建档即开跑），
    故 ``queue_wait_ms`` 恒为 None（**不假造 0**）；``enqueued_at`` 取建档时间
    ``start_time`` 仅作参照。
    """
    nodes = info.get("nodes")
    events: List[Dict[str, Any]] = []
    if isinstance(nodes, list):
        seq = 0
        for node in nodes:
            if not isinstance(node, dict):
                continue
            node_id = node.get("id")
            started_ms = node.get("started_ms")
            if not isinstance(started_ms, int):
                continue
            seq += 1
            events.append({
                "seq": seq,
                "type": _TIMELINE_NODE_START,
                "node_id": node_id,
                "at": node.get("started_at"),
                "at_ms": started_ms,
            })
            ended_ms = node.get("ended_ms")
            if not isinstance(ended_ms, int):
                continue        # 在跑节点：只发 start（与 keeper 活性判据同形态）
            seq += 1
            event: Dict[str, Any] = {
                "seq": seq,
                "type": _TIMELINE_NODE_END,
                "node_id": node_id,
                "at": node.get("ended_at"),
                "at_ms": ended_ms,
                "ok": node.get("status") != "error",
                "error": node.get("error"),
            }
            if isinstance(node.get("duration_ms"), int):
                event["duration_ms"] = node["duration_ms"]
            events.append(event)
    return _timeline_envelope(
        info.get("execution_id"),
        info.get("status"),
        "local_nodes_projection",
        {"events": events},
        collected=bool(events),
        start_time=info.get("start_time"),
        end_time=info.get("end_time"),
    )


@router.get("/executions/{execution_id}/timeline")
async def get_execution_timeline(
    execution_id: str,
    request: Request,
    redis: Optional[Redis] = Depends(get_redis_or_none),
):
    """执行事件时间线（plaita#25）：**排队多久 / 每个节点何时开始跑了多久 /
    哪个节点失败**——修掉「除 Langfuse（失败节点恰缺失）外没有第二时间线」的盲区。

    - 集群档：读 worker 落盘的 ``ExecutionState.timeline``（有序事件日志，
      同一份事件已实时发布到 SSE 频道）；
    - 本地档：读 ``local_executions.nodes_json`` 的只读投影；
    - 老状态/未采集 ⇒ ``collected=false``、``queue_wait_ms=null``，**不是 0**。
    """
    if (local := get_local_executor(request)) is not None:
        info = local.get_local_execution(execution_id, tenant_id=tenant_scope(request))
        if info is None:
            raise HTTPException(status_code=404, detail=f"执行不存在: {execution_id}")
        return project_local_timeline(info)

    _tenant, data = _find_execution(request, redis, execution_id)
    if not data:
        raise HTTPException(status_code=404, detail=f"执行不存在: {execution_id}")
    return project_state_timeline(data)
