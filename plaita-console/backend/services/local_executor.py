"""本地单机模式执行控制面与进程内注册表（C1 阶段 3 后的残面）。

本地单机档（``app.state.local_mode = True``）的**执行驱动**自 C1 阶段 2/3
（plaita#323/#335）起统一走 :mod:`engine_backend`（与集群同一 ``FlowWorker``
状态机，ADR-001「console local = engine backend」）；本模块不再有第二套
executor 驱动（``start/resume_local_execution`` / ``_run_flow`` 已删），保留：

- **进程内注册表**：执行线程表 / 取消 Event 表 / 强删墓碑——engine backend
  的驱动线程与 API 控制面共用同一份（``cancel_local_execution`` 置位同一
  Event、``delete_local_execution`` join 同一线程、墓碑抑制同一窗口）；
- **控制面/读面**：``cancel_local_execution`` / ``delete_local_execution`` /
  ``get_local_execution`` / ``list_local_executions``；
- **共享装配**：``_LocalTraceCallback``（节点 trace 回写）、langfuse 回调
  构造、``_with_resume_binding``（#221 挂起订阅绑定，engine resume 同源）。

语义注记（存量行为，如实说明）：

- 无跨进程：resume 必须走同一个 console 进程；进程重启后挂起的执行
  checkpoint 仍在（SQLite），显式 resume 可继续，但进程内事件订阅已丢失
- cancel 为步界协作取消（波次①）：置位取消 Event + 落 cancelled 终态，
  执行线程在步界收口；在途节点不中断（软中断默认语义，设计稿 §3.3）
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from plaita.core.callback import FlowCallback
from plaita.core.executor import FlowExecution
from plaita.core.strategies import RESUME_BINDING_KEY

try:
    from . import flow_store as fs
except ImportError:  # 平铺布局（cwd=backend）运行时
    import flow_store as fs  # type: ignore

if TYPE_CHECKING:
    from .flow_store import FlowStore

logger = logging.getLogger(__name__)

# 已启动线程表：execution_id -> Thread（进程内生命周期，仅防重复启动/恢复）
_threads: Dict[str, threading.Thread] = {}
_lock = threading.Lock()

# 取消事件表（设计稿 §3.1 本地档，波次①）：execution_id -> threading.Event，
# 与 _threads 同锁管理。cancel_local_execution 置位；执行线程在步界消费并
# 终态化，每步状态落库改用条件更新——执行线程不再无条件覆写终态。
_cancel_events: Dict[str, threading.Event] = {}

# 强删墓碑（A43-01 本地档）：force 删除时执行线程可能仍在途（软中断只在
# 步界生效）。删除是持久语义，故墓碑登记后该执行的日志写入（engine backend
# ``BackendLogHandler``）一律丢弃，避免「删了记录、日志还在长」；线程退出时
# 在驱动线程 finally 清除墓碑，避免无限增长。
_deleted_executions: set[str] = set()

# force 删除等待执行线程收口的期限（秒）。超时不阻塞请求（删除仍完成），
# 只记录具名 WARNING——在途节点按本地档软中断语义跑到步界。
LOCAL_DELETE_JOIN_TIMEOUT_SECONDS = 5.0


class ExecutionActiveError(RuntimeError):
    """本地执行仍在运行/挂起：删除需先取消，或显式 ``force=True``。"""


def _register_cancel_event(execution_id: str) -> None:
    with _lock:
        _cancel_events[execution_id] = threading.Event()


def _pop_cancel_event(execution_id: str) -> None:
    with _lock:
        _cancel_events.pop(execution_id, None)


def _cancel_event_set(execution_id: str) -> bool:
    with _lock:
        event = _cancel_events.get(execution_id)
    return bool(event is not None and event.is_set())


def _live_thread(execution_id: str) -> Optional[threading.Thread]:
    """返回该执行当前存活的线程（None = 无在途线程）。"""
    with _lock:
        thread = _threads.get(execution_id)
    if thread is not None and thread.is_alive():
        return thread
    return None


def _mark_deleted(execution_id: str) -> None:
    with _lock:
        _deleted_executions.add(execution_id)


def _is_deleted(execution_id: str) -> bool:
    with _lock:
        return execution_id in _deleted_executions


def _finalize_cancelled(execution_id: str, context: Optional[Dict[str, Any]]) -> None:
    """本地档取消收口：落 cancelled 终态（context = 取消点前 checkpoint）。

    幂等：cancel_local_execution 可能已写过 cancelled，此处再写仅补
    context 快照，不产生覆写战争（条件更新闸门保证线程侧不再翻回 running）。
    """
    fs.finish_local_execution(
        execution_id,
        status="cancelled",
        context_json=json.dumps(_safe(context), ensure_ascii=False),
        usage_json=_usage_json_for(context),
    )
    logger.info("本地执行 %s 已取消（步界收口）", execution_id)


def _usage_json_for(context: Any) -> str:
    """终态 usage 汇总（plaita#37 P2）：JSON 串；未采集 ⇒ ``'null'``（不是 0）。

    - 与集群档 worker 的 ``_collect_usage`` 同一口径（复用
      ``plaita.storage.usage.aggregate_node_usage``，真源 = ``$NODE[*].usage``），
      不新增第二真相源；
    - **best-effort**：聚合抛错只告警并落 ``'null'``——观测口径绝不影响本地
      执行的终态与前端可用性；
    - 采集到 0 落 ``{"total_tokens": 0, ...}``，与「未采集」在读取侧严格可分。
    """
    try:
        from plaita.storage.usage import aggregate_node_usage, node_results_from_context

        usage = aggregate_node_usage(node_results_from_context(context))
    except Exception:  # noqa: BLE001 — 观测口径失败绝不拖累执行/落库
        logger.warning("本地执行 usage 聚合失败（按未采集落库）", exc_info=True)
        return "null"
    if usage is None:
        return "null"
    try:
        return json.dumps(usage, ensure_ascii=False)
    except (TypeError, ValueError):  # pragma: no cover — usage 是纯标量字典
        logger.warning("本地执行 usage 序列化失败（按未采集落库）", exc_info=True)
        return "null"


# ---- 租户感知的凭据文件解析 ----
# 引擎 plaita.credentials 原生按租户上下文路由凭据文件（default = 基础文件，
# 其余租户 = 旁文件 .plaita-credentials.{tenant}.json）。本地档执行线程把
# 执行租户 set 进引擎租户上下文（_run_flow 内），与集群档 worker 同一路由
# 机制，无需再对运行时打补丁。


class _LocalTraceCallback(FlowCallback):
    """把节点开始/结束写回本地执行记录（每节点一次 SQLite 更新，示例规模可接受）。

    同时负责把 console 侧的执行 ID 同步进流程状态的 ``$EXECUTION_ID``：
    plaita 运行时在 fresh start 时会 ``clean()`` 重新生成 ``$EXECUTION_ID``，
    而 ``on_flow_start`` 在 clean/setup 之后、首个节点执行之前触发，是唯一
    能在不侵入运行时的前提下覆写它的时机（resume 路径从 checkpoint 恢复，
    天然保留首次注入的 ID，无需再设）。
    """

    def __init__(self, execution_id: str, initial_nodes: Optional[List[Dict[str, Any]]] = None):
        self._execution_id = execution_id
        self._nodes: List[Dict[str, Any]] = initial_nodes or []
        self._execution: Optional[FlowExecution] = None

    def bind_execution(self, execution: FlowExecution) -> None:
        self._execution = execution

    def on_flow_start(self, flow, **kwargs) -> None:
        # fresh start 时在此把 $EXECUTION_ID 覆写为 console 的 execution_id，
        # 使流程内 ${$EXECUTION_ID} 与执行页显示/查询的 ID 一致。
        if self._execution is not None:
            self._execution.set_state(
                f"{self._execution.express_prefix}EXECUTION_ID", self._execution_id
            )

    def _flush(self) -> None:
        fs.update_local_execution(
            self._execution_id, nodes_json=json.dumps(self._nodes, ensure_ascii=False)
        )

    def on_node_start(self, flow, node, **kwargs) -> None:
        self._nodes.append(
            {
                "id": node.id,
                "type": getattr(node, "node_type", type(node).__name__),
                "name": getattr(node, "name", "") or node.id,
                "input": _safe(getattr(node, "input", None)),
                "output": None,
                "status": "running",
                # 节点级耗时：起止时间戳（与 worker 侧 NodeTimingCallback 同构，
                # 由 flow_store._timings_from_nodes 折算成 node_timings）
                "started_at": _now(),
                "started_ms": int(time.time() * 1000),
            }
        )
        self._flush()

    def on_node_end(self, flow, node, result=None, error=None, exception=None, **kwargs) -> None:
        ended_ms = int(time.time() * 1000)
        for entry in reversed(self._nodes):
            if entry["id"] == node.id and entry["status"] == "running":
                entry["output"] = _safe(result)
                entry["ended_at"] = _now()
                entry["ended_ms"] = ended_ms
                started_ms = entry.get("started_ms")
                if isinstance(started_ms, int):
                    entry["duration_ms"] = max(0, ended_ms - started_ms)
                if error or exception:
                    entry["status"] = "error"
                    entry["error"] = str(error or exception)
                else:
                    entry["status"] = "success"
                break
        self._flush()


def _safe(value: Any) -> Any:
    """回调里的值可能不可 JSON 序列化，兜底转字符串。"""
    try:
        json.dumps(value)
        return value
    except (TypeError, ValueError):
        return repr(value)


def _now() -> str:
    return datetime.utcnow().isoformat()


# C1 阶段 3（plaita#335）：``_load_definition`` / ``start_local_execution`` /
# ``resume_local_execution`` / ``_run_flow`` / ``_ThreadLogHandler``（第二套
# 执行器驱动）已删——executions API 手动 start/resume 自本阶段起经
# :mod:`engine_backend`（与集群同一 ``FlowWorker`` 状态机 + ``resume_flow``
# 驱动链，ADR-001「console local = engine backend」）。本模块保留：进程内
# 注册表（线程/取消 Event/强删墓碑，engine backend 共用）、控制面（cancel/
# delete）与读面（get/list）、以及二者共用的 trace 回调与观测装配。


def _with_resume_binding(context: Any, resume_type: str,
                         data: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """手工 event/timeout resume 附上挂起订阅绑定（plaita#221）。

    本地档审批载荷（如 ``{"approved": True}``）里没有 event_filter 代投时携带的
    ``subscription_id``；引擎准入校验要求绑定挂起期登记的那条订阅，故从
    checkpoint 的 ``$NODE[$LAST_NODE]`` 取订阅 id，以私有载体键附加——引擎在
    分发到节点前剥掉，不污染 ``event_data`` / 流程输出。

    取不到绑定信息时原样返回（历史形状 / 非挂起：交给引擎侧准入裁决）。
    """
    if resume_type not in ("event", "timeout"):
        return data
    if not isinstance(context, dict):
        return data
    last_node = context.get("$LAST_NODE")
    node_results = context.get("$NODE")
    state = node_results.get(last_node) if isinstance(node_results, dict) else None
    binding = state.get("subscription_id") if isinstance(state, dict) else None
    if not binding:
        return data
    enriched = dict(data) if isinstance(data, dict) else {}
    enriched[RESUME_BINDING_KEY] = str(binding)
    return enriched


def _spawn(execution_id: str, target, *args, **kwargs) -> None:
    """启动执行线程。同一 execution_id 已有存活线程时拒绝（防重复启动/恢复）。"""
    with _lock:
        existing = _threads.get(execution_id)
        if existing is not None and existing.is_alive():
            raise RuntimeError(f"执行 {execution_id} 已有运行中的线程")
        # 新执行线程配对一个新的取消事件（旧线程已退出才会走到这里）；
        # 同 id 重新启动时清掉上一轮的强删墓碑（新执行生命周期）
        _cancel_events[execution_id] = threading.Event()
        _deleted_executions.discard(execution_id)
        thread = threading.Thread(
            target=target, args=args, kwargs=kwargs,
            name=f"local-exec-{execution_id}", daemon=True,
        )
        _threads[execution_id] = thread
    thread.start()


_lf_warned = False


def _build_langfuse_callback():
    """按配置构造 LangfuseCallback（可选观测，任何失败都降级为不观测）。

    - ``PLAITA_CONSOLE_LANGFUSE=false`` 强制关；
    - ``true`` 强制开；``auto``（默认）= 配了 ``LANGFUSE_PUBLIC_KEY`` 才开；
    - 缺 ``plaita[langfuse]`` 依赖或 SDK 初始化失败（如缺凭据）只告警一次。
    """
    global _lf_warned
    from plaita.runtime_config import resolve_langfuse_mode

    mode = resolve_langfuse_mode()
    public_key = os.getenv("LANGFUSE_PUBLIC_KEY", "")
    if mode == "false" or (mode == "auto" and not public_key):
        return None
    try:
        from plaita.obs import LangfuseCallback

        return LangfuseCallback()
    except ImportError as e:
        if not _lf_warned:
            _lf_warned = True
            logger.warning("Langfuse 观测未启用（缺 plaita[langfuse] 依赖）: %s", e)
        return None
    except Exception as e:  # noqa: BLE001 — SDK 初始化失败（如缺凭据）不阻塞执行
        if not _lf_warned:
            _lf_warned = True
            logger.warning("Langfuse 观测未启用: %s", e)
        return None


def _latest_published(
    store: FlowStore, flow_id: str, tenant_id: Optional[str] = None
) -> Optional[str]:
    published = [
        v
        for v in store.list_versions(flow_id, tenant_id=tenant_id)
        if v.status == "published"
    ]
    return published[-1].version if published else None


def get_local_execution(
    execution_id: str, tenant_id: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    return fs.get_local_execution(execution_id, tenant_id=tenant_id)


def list_local_executions(
    status: Optional[str] = None,
    flow_id: Optional[str] = None,
    tenant_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    rows = fs.list_local_executions(tenant_id=tenant_id)
    out = []
    for row in rows:
        if status and row.get("status") != status:
            continue
        if flow_id and row.get("flow_id") != flow_id:
            continue
        out.append(row)
    out.sort(key=lambda x: x.get("start_time") or "", reverse=True)
    return out


def cancel_local_execution(
    execution_id: str, tenant_id: Optional[str] = None
) -> bool:
    """取消本地执行（波次① §3.1 本地档）。

    - 置位取消事件：执行线程在步界消费并终态化（在途节点跑完，软中断）；
    - 同步落 cancelled 终态：控制面立即反馈；线程侧每步状态改用条件更新，
      不会把 cancelled 翻回 running/completed——覆写战争消失。
    """
    row = fs.get_local_execution(execution_id, tenant_id=tenant_id)
    if row is None:
        return False
    with _lock:
        event = _cancel_events.get(execution_id)
    if event is not None:
        event.set()
    if row["status"] in ("running", "suspended"):
        fs.finish_local_execution(execution_id, status="cancelled")
    return True


def delete_local_execution(
    execution_id: str, tenant_id: Optional[str] = None, *, force: bool = False
) -> bool:
    """持久删除本地执行（A43-01 本地档）。

    非终态（running/suspended）默认拒绝（``ExecutionActiveError``）——此前
    直接删行，后台线程继续跑到结束、日志写入（engine backend handler）继续进 local_logs，
    记录「删了又能在日志页查到」。

    ``force=True`` 时先取消再删：置位取消 Event → 有界等待线程在步界收口
    →（超时则登记强删墓碑，后续日志写入丢弃并打 WARNING）→ 删除该执行的
    local_logs → 删除 local_executions 行。日志属于执行数据，随执行一并
    删除，删除后不可查。
    """
    row = fs.get_local_execution(execution_id, tenant_id=tenant_id)
    if row is None:
        return False

    status = row.get("status") or ""
    active = status in ("running", "suspended")
    thread = _live_thread(execution_id)

    if active and not force:
        raise ExecutionActiveError(
            f"执行仍在 {status} 状态，先取消（POST "
            f"/api/executions/{execution_id}/cancel）待终态后删除；"
            "如确需立即删除请显式传 force=true"
        )

    if thread is not None:
        # 执行线程仍在途：先立墓碑（并发日志立即停止写入），再按取消语义
        # 收口。终态行但线程尚在 finally 的窗口同样走这段。
        _mark_deleted(execution_id)
        with _lock:
            event = _cancel_events.get(execution_id)
        if event is not None:
            event.set()
        if thread is not threading.current_thread():
            thread.join(timeout=LOCAL_DELETE_JOIN_TIMEOUT_SECONDS)
            if thread.is_alive():
                logger.warning(
                    "本地执行 %s 强删时线程未在 %.1fs 内收口：记录与日志仍删除，"
                    "在途节点跑到步界后退出（其后续日志写入已丢弃）",
                    execution_id,
                    LOCAL_DELETE_JOIN_TIMEOUT_SECONDS,
                )

    fs.delete_local_logs(execution_id, tenant_id=tenant_id)
    return fs.delete_local_execution(execution_id, tenant_id=tenant_id)


def _to_info(row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    return row
