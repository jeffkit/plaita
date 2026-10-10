"""
执行实例管理 API
提供执行列表、详情、启动、停止等接口
"""
import asyncio
import json
import logging
import os
import secrets
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from redis import Redis
from sse_starlette.sse import EventSourceResponse
from starlette.routing import Route

try:
    from services import obs_link
except ImportError:  # 平铺布局（cwd=backend）运行时
    import obs_link  # type: ignore

logger = logging.getLogger(__name__)

router = APIRouter()


# ============ 数据模型 ============

class ExecutionInfo(BaseModel):
    """执行实例信息"""
    execution_id: str = Field(..., description="执行 ID")
    flow_id: str = Field(..., description="流程 ID")
    flow_version: Optional[str] = Field(None, description="流程版本")
    status: str = Field(..., description="状态")
    tenant_id: str = Field("", description="租户 ID（本地模式）")
    start_time: Optional[str] = Field(None, description="开始时间")
    end_time: Optional[str] = Field(None, description="结束时间")
    last_update_time: Optional[str] = Field(None, description="最后更新时间")
    context: Optional[Dict[str, Any]] = Field(None, description="执行上下文")
    error: Optional[Dict[str, Any]] = Field(None, description="错误信息")
    invoker: Optional[str] = Field(None, description="调用者")
    # 本地单机模式专有：节点级 trace 与最终输出（集群模式为 None）
    nodes: Optional[List[Dict[str, Any]]] = Field(None, description="节点级执行 trace（本地模式）")
    output: Optional[Any] = Field(None, description="流程输出（本地模式）")
    # 节点级耗时：集群模式由 worker 的 NodeTimingCallback 写入执行状态，
    # 本地模式由回调采集的 trace 时间戳折算；老状态/未采集时为 None
    node_timings: Optional[Dict[str, Any]] = Field(None, description="节点级耗时")
    # Langfuse 观测深链（启用观测且配了 LANGFUSE_PROJECT_ID 时非空）
    langfuse_trace_url: Optional[str] = Field(None, description="Langfuse trace 页面 URL")


class ExecutionListResponse(BaseModel):
    """执行列表响应"""
    executions: List[ExecutionInfo]
    total: int
    page: int
    size: int


class StartFlowRequest(BaseModel):
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


class ResumeFlowRequest(BaseModel):
    """恢复流程请求"""
    resume_type: str = Field(
        ..., description="恢复类型: continue, retry(error 态断点重跑), cancel, timeout, event")
    data: Optional[Dict[str, Any]] = Field(None, description="恢复数据")


# ============ 工具函数 ============

# 任务队列 Stream 名默认值（与 FlowWorker 的 PLAITA_QUEUE_NAME 默认值一致）。
# 实际取值在使用点经 PLAITA_CONSOLE_TASK_QUEUE 覆盖，便于新旧并行验证时把
# console 派发到独立队列——不得在模块导入时求值，否则无法被测试/运行时覆盖。
DEFAULT_TASK_QUEUE_NAME = "plaita:flow:queue"
# 兼容别名：schedules 等模块按此名导入（缺省队列名；实际派发请用 _task_queue_name）
TASK_QUEUE_NAME = DEFAULT_TASK_QUEUE_NAME


def _task_queue_name() -> str:
    """解析派发队列名：env ``PLAITA_CONSOLE_TASK_QUEUE`` > 默认值。"""
    return os.getenv("PLAITA_CONSOLE_TASK_QUEUE", DEFAULT_TASK_QUEUE_NAME)

try:
    from plaita.server.task_queue import enqueue_task
    from plaita.server.tenant_context import tenant_namespace
    from plaita.storage.redis import execution_state_ttl_seconds
except ImportError:  # 平铺布局（cwd=backend）运行时
    import sys as _sys
    from pathlib import Path as _Path
    _plaita_root = str(_Path(__file__).resolve().parents[3])
    if _plaita_root not in _sys.path:
        _sys.path.insert(0, _plaita_root)
    from plaita.server.task_queue import enqueue_task
    from plaita.server.tenant_context import tenant_namespace
    from plaita.storage.redis import execution_state_ttl_seconds

try:
    from ..services import flow_store
    from ..auth import require_auth, tenant_scope
except ImportError:  # 平铺布局（cwd=backend）运行时
    from services import flow_store  # type: ignore
    from auth import require_auth, tenant_scope  # type: ignore


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


def _audit(request: Request, action: str, resource_id: str, detail: Dict | None = None) -> None:
    try:
        from ..services import audit as audit_svc
    except ImportError:
        try:
            from services import audit as audit_svc  # type: ignore
        except ImportError:
            return
    audit_svc.record(request, action=action, resource="execution", resource_id=resource_id, detail=detail)


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
    """
    return enqueue_task(redis, _task_queue_name(), message)


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

    同类第九次（2026-10-10，plaita#47）：`{ns}:execution:dlq_requeue:{id}`
    （死信守卫恢复副本重入队冷却标记，值为时间戳字符串）——同样会被
    `plaita:execution:*` 前缀扫到，漏登记即 `json.loads("1759…")` 得 int →
    `summary.get` 500（自动防线扫 `:execution:dlq_requeue:{` 已覆盖）。
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
        # 同类第九次（plaita#47）：恢复副本重入队冷却标记（值=时间戳字符串）。
        or ":execution:dlq_requeue:" in key
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
      out[n] = {KEYS[i],
        s(obj['status']),
        s(obj['flow_id']),
        s(obj['flow_name']),
        s(obj['flow_version']),
        s(obj['start_time']),
        s(obj['last_update_time']),
        s(obj['end_time']),
        s(obj['invoker'])}
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
            }
        # cjson 解析失败被跳过的键：全量兜底
        for key in chunk:
            if key not in projected:
                info = _load_full_state(redis, key)
                if info is not None:
                    summaries[key] = info
    return summaries


def _execution_info_from_summary(
    key_str: str, summary: Dict[str, Any]
) -> ExecutionInfo:
    """由投影摘要构建列表行（context/error 不进列表响应，详情页读全量）。"""
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
        context=None,
        error=None,
    )


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


# ============ API 端点 ============

@router.get("/executions", response_model=ExecutionListResponse)
async def list_executions(
    request: Request,
    page: int = 1,
    size: int = 20,
    status: Optional[str] = None,
    flow_id: Optional[str] = None,
    redis: Optional[Redis] = Depends(get_redis_or_none),
):
    """
    获取执行实例列表
    
    - **page**: 页码（从 1 开始）
    - **size**: 每页数量
    - **status**: 按状态筛选
    - **flow_id**: 按流程 ID 筛选
    """
    if (local := get_local_executor(request)) is not None:
        executions = [
            ExecutionInfo(**info)
            for info in local.list_local_executions(
                status=status, flow_id=flow_id, tenant_id=tenant_scope(request)
            )
        ]
    else:
        tenant = tenant_scope(request)
        # 租户视角只扫本租户 namespace；平台视角扫 default（历史前缀）+ 各租户
        if tenant:
            patterns = [f"{tenant_namespace(tenant)}:execution:*"]
        else:
            patterns = ["plaita:execution:*", "plaita:*:execution:*"]

        # C4-3：SCAN 出键后服务端投影（EVAL+cjson）取列表字段，
        # 完整 context 不再逐键 GET/loads 进 console 内存。
        summaries: Dict[str, Dict[str, Any]] = {}
        seen: set = set()
        for pattern in patterns:
            keys: List[str] = []
            for key in redis.scan_iter(match=pattern):
                key_str = key if isinstance(key, str) else key.decode()
                # 排除租约/取消标志/队列等同前缀机制键
                if _is_mechanism_key(key_str) or key_str in seen:
                    continue
                seen.add(key_str)
                keys.append(key_str)
            summaries.update(_project_summaries(redis, keys))

        executions = []
        for key_str, summary in summaries.items():
            # 筛选
            if status and (summary.get("status") or "") != status:
                continue
            if flow_id and (summary.get("flow_id") or "") != flow_id:
                continue
            executions.append(_execution_info_from_summary(key_str, summary))
    
    # 按开始时间排序（最新的在前）
    executions.sort(
        key=lambda x: x.start_time or "",
        reverse=True
    )
    
    # 分页
    total = len(executions)
    start = (page - 1) * size
    end = start + size
    paginated = executions[start:end]
    
    return ExecutionListResponse(
        executions=paginated,
        total=total,
        page=page,
        size=size
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
        info = dict(info)
        info["langfuse_trace_url"] = obs_link.langfuse_trace_url(execution_id)
        return ExecutionInfo(**info)

    _tenant, data = _find_execution(request, redis, execution_id)
    if not data:
        raise HTTPException(status_code=404, detail=f"执行不存在: {execution_id}")

    try:
        data = dict(data)
        data["langfuse_trace_url"] = obs_link.langfuse_trace_url(execution_id)
        return ExecutionInfo(**data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"数据解析失败: {e}")


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
            raise HTTPException(status_code=404, detail=str(e))
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        _audit(http_request, "execution.start", info["execution_id"],
               {"flow_id": request.flow_id, "version": request.version, "mode": "local"})
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
    message = {
        "type": "start",
        "flow_id": request.flow_id,
        "version": request.version,
        "params": request.params,
        "execution_id": execution_id,
        "tenant_id": tenant_scope(http_request, required=True),
        "timestamp": datetime.now().isoformat()
    }
    # start 幂等键 additive 透传（波次二任务③）：仅显式提供时进入消息体，
    # 旧客户端消息形状零变化。与 G1 预铸 execution_id 并存：同一条消息
    # 既带预铸 id（提交方即刻可轮询）又可选带幂等键（重投收敛）。
    if request.dedup_key:
        message["dedup_key"] = request.dedup_key
    # 写入任务队列 Stream（FlowWorker 消费组消费）
    _enqueue(message, redis)

    _audit(http_request, "execution.start", execution_id,
           {"flow_id": request.flow_id, "version": request.version, "mode": "queue"})
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
):
    """
    取消/终止执行（发送取消命令到队列）
    
    - **execution_id**: 执行 ID
    """
    # 本地单机模式：标记状态（尽力而为，不中断线程）
    if (local := get_local_executor(request)) is not None:
        if not local.cancel_local_execution(execution_id, tenant_id=tenant_scope(request)):
            raise HTTPException(status_code=404, detail=f"执行不存在: {execution_id}")
        _audit(request, "execution.cancel", execution_id)
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
        return {
            "success": True,
            "status": status,
            "execution_id": execution_id,
            "message": f"执行已是终态（{status}），忽略取消",
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

    _audit(request, "execution.cancel", execution_id)
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
    redis: Optional[Redis] = Depends(get_redis_or_none),
):
    """
    删除执行记录（从 Redis 中永久删除）
    
    - **execution_id**: 执行 ID
    """
    # 本地单机模式：从 SQLite 删除
    if (local := get_local_executor(request)) is not None:
        if not local.delete_local_execution(execution_id, tenant_id=tenant_scope(request)):
            raise HTTPException(status_code=404, detail=f"执行不存在: {execution_id}")
        _audit(request, "execution.delete", execution_id)
        return {
            "success": True,
            "execution_id": execution_id,
            "message": "执行记录已删除",
        }

    _tenant, _data = _find_execution(request, redis, execution_id)
    if not _data:
        raise HTTPException(status_code=404, detail=f"执行不存在: {execution_id}")
    key = _exec_key(_tenant, execution_id)

    # 删除记录（连同取消标志键，避免残留意图键指向已删除的执行）
    redis.delete(key)
    redis.delete(_cancel_key(_tenant, execution_id))

    # 同时删除相关的事件通道（如果存在）
    event_key = f"plaita:execution:events:{execution_id}"
    redis.delete(event_key)
    
    _audit(request, "execution.delete", execution_id)
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
    """
    # 本地单机模式：从 SQLite checkpoint 继续（分布式策略）
    if (local := get_local_executor(http_request)) is not None:
        try:
            ok = local.resume_local_execution(
                flow_store.get_flow_store(), execution_id, request.resume_type,
                request.data, tenant_id=tenant_scope(http_request),
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        if not ok:
            raise HTTPException(status_code=404, detail=f"执行不存在: {execution_id}")
        _audit(http_request, "execution.resume", execution_id,
               {"resume_type": request.resume_type, "mode": "local"})
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
    message = {
        "type": "resume",
        "flow_id": flow_id,
        "execution_id": execution_id,
        "resume_type": request.resume_type,
        "tenant_id": resume_tenant or "default",
        "data": request.data,
        "timestamp": datetime.now().isoformat()
    }

    _enqueue(message, redis)

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
# Redis 抖动时 SSE 循环的重试间隔（异常不终结流）。
SSE_REDIS_RETRY_SECONDS = 0.5


def _sse_ticket_key(ticket: str) -> str:
    return f"{SSE_TICKET_KEY_PREFIX}{ticket}"


@router.post("/executions/{execution_id}/stream/ticket")
async def create_stream_ticket(
    execution_id: str,
    request: Request,
    redis: Redis = Depends(get_redis),
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
            "issued_at": datetime.now().isoformat(),
        }
    )
    redis.set(_sse_ticket_key(ticket), payload, ex=SSE_TICKET_TTL_SECONDS)
    _audit(request, "execution.stream_ticket", execution_id)
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
    request.state.platform_admin = False
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
    get_message(timeout=1.0) 会阻塞整个事件循环。测试可直接向
    ``app.state.execution_sse_aioredis`` 注入 fake 客户端。
    """
    client = getattr(request.app.state, "execution_sse_aioredis", None)
    if client is None:
        try:
            from ..config import get_settings
        except ImportError:  # 平铺布局（cwd=backend）运行时
            from config import get_settings  # type: ignore
        import redis.asyncio as aioredis

        client = aioredis.from_url(get_settings().redis_url, decode_responses=True)
        request.app.state.execution_sse_aioredis = client
    return client


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
            yield {"event": "initial_state", "data": json.dumps(info, ensure_ascii=False)}
            while True:
                await asyncio.sleep(1.0)
                current = local.get_local_execution(execution_id, tenant_id=tenant_scope(request))
                if current is None:
                    break
                payload = json.dumps(current, ensure_ascii=False, default=str)
                if payload != last:
                    last = payload
                    yield {"event": "update", "data": payload}
                if current.get("status") in ("completed", "failed", "cancelled"):
                    break
                if await request.is_disconnected():
                    break

        return EventSourceResponse(local_event_generator())

    # 验证执行存在（按租户上下文定位）
    _tenant, data = _find_execution(request, redis, execution_id)
    if not data:
        raise HTTPException(status_code=404, detail=f"执行不存在: {execution_id}")

    channel = f"plaita:execution:events:{execution_id}"
    return EventSourceResponse(_execution_event_stream(request, data, channel))


# raw Starlette Route：绕过 include 级 require_auth（仅此路由），鉴权在端点内
# 双轨裁决（ticket / require_auth）。不进 OpenAPI schema（FastAPI 只文档化
# APIRoute；SSE 端点本也无文档价值）。
router.add_route(
    "/executions/{execution_id}/stream",
    stream_execution_endpoint,
    methods=["GET"],
    include_in_schema=False,
)

