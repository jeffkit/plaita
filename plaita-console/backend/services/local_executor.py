"""本地单机模式执行器。

Redis 不可达时（``app.state.local_mode = True``），流程在 console 进程内以
**分布式策略** 执行：EventBus 用进程内 InMemoryEventBus，checkpoint 存
SQLite（local_executions.context_json）。

因此本地档支持挂起-恢复（审批/事件节点挂起后，经 /resume 或进程内事件
恢复）。限制（如实说明）：

- 无跨进程：resume 必须走同一个 console 进程；进程重启后挂起的执行
  checkpoint 仍在（SQLite），显式 resume 可继续，但进程内事件订阅已丢失
- cancel 为步界协作取消（波次①）：置位取消 Event + 落 cancelled 终态，
  执行线程在步界收口；在途节点不中断（软中断默认语义，设计稿 §3.3）
"""
from __future__ import annotations

import contextvars
import json
import logging
import os
import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from plaita.core.callback import FlowCallback
from plaita.core.executor import FlowExecution
from plaita.core.flow import Flow
from plaita.core.strategies import ExecutionMode
from plaita.usage import UsageCollector

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


def _finalize_cancelled(execution_id: str, context: Optional[Dict[str, Any]]) -> None:
    """本地档取消收口：落 cancelled 终态（context = 取消点前 checkpoint）。

    幂等：cancel_local_execution 可能已写过 cancelled，此处再写仅补
    context 快照，不产生覆写战争（条件更新闸门保证线程侧不再翻回 running）。
    """
    fs.finish_local_execution(
        execution_id,
        status="cancelled",
        context_json=json.dumps(_safe(context), ensure_ascii=False),
    )
    logger.info("本地执行 %s 已取消（步界收口）", execution_id)

# ---- 租户感知的凭据文件解析 ----
# plaita 运行时每次 get_credential 都从 PLAITA_CREDENTIALS_FILE 环境变量解析
# 路径（进程全局）；本地档多租户并发执行时不能改 env（跨租户串文件）。
# 这里用 ContextVar 按执行线程注入租户专属凭据文件：只在包装函数内生效，
# 未设置时与原行为完全一致。
_tenant_credentials_file: contextvars.ContextVar = contextvars.ContextVar(
    "plaita_console_tenant_credentials_file", default=None
)


def _patch_runtime_credentials() -> None:
    import plaita.credentials as _pc

    if getattr(_pc, "_console_tenant_patched", False):
        return
    _orig = _pc.credentials_file

    def _tenant_aware_credentials_file():
        override = _tenant_credentials_file.get()
        if override:
            return Path(override)
        return _orig()

    _pc.credentials_file = _tenant_aware_credentials_file
    _pc._console_tenant_patched = True  # type: ignore[attr-defined]


_patch_runtime_credentials()


class _LocalTraceCallback(FlowCallback):
    """把节点开始/结束写回本地执行记录（每节点一次 SQLite 更新，示例规模可接受）。

    同时负责把 console 侧的执行 ID 同步进流程状态的 ``$EXECUTION_ID``：
    plaita 运行时在 fresh start 时会 ``clean()`` 重新生成 ``$EXECUTION_ID``，
    而 ``on_flow_start`` 在 clean/setup 之后、首个节点执行之前触发，是唯一
    能在不侵入运行时的前提下覆写它的时机（resume 路径从 checkpoint 恢复，
    天然保留首次注入的 ID，无需再设）。
    """

    def __init__(
        self,
        execution_id: str,
        initial_nodes: Optional[List[Dict[str, Any]]] = None,
        initial_usage: Optional[Dict[str, Any]] = None,
    ):
        self._execution_id = execution_id
        self._nodes: List[Dict[str, Any]] = initial_nodes or []
        self._execution: Optional[FlowExecution] = None
        # token 用量归集（issue #37）：与 nodes_json 同一批回写。resume 时用
        # 已落盘用量打底，挂起前步骤的用量不丢。
        self._usage = UsageCollector()
        self._usage.seed(initial_usage)

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
            self._execution_id,
            nodes_json=json.dumps(self._nodes, ensure_ascii=False),
            usage_json=json.dumps(self._usage.summary(), ensure_ascii=False),
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
            }
        )
        self._flush()

    def on_node_end(self, flow, node, result=None, error=None, exception=None, **kwargs) -> None:
        for entry in reversed(self._nodes):
            if entry["id"] == node.id and entry["status"] == "running":
                entry["output"] = _safe(result)
                if error or exception:
                    entry["status"] = "error"
                    entry["error"] = str(error or exception)
                else:
                    entry["status"] = "success"
                break
        self._usage.on_node_end(flow, node, result, error, exception)
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


def _load_definition(
    store: FlowStore,
    flow_id: str,
    version: Optional[str],
    tenant_id: Optional[str] = None,
) -> Dict[str, Any]:
    version = version or _latest_published(store, flow_id, tenant_id)
    record = store.get_version(flow_id, version, tenant_id=tenant_id)
    if record is None:
        raise LookupError(f"流程版本不存在: {flow_id}@{version}")
    definition = json.loads(record.definition)
    definition["flow_id"] = flow_id
    definition["version"] = version
    return definition


def start_local_execution(
    store: FlowStore,
    flow_id: str,
    version: Optional[str],
    params: Optional[Dict[str, Any]],
    invoker: str = "local",
    tenant_id: str = "",
) -> Dict[str, Any]:
    """以本地模式启动流程：同步建档 + 后台线程以分布式策略执行。"""
    version = version or _latest_published(store, flow_id, tenant_id)
    if version is None:
        raise ValueError(f"流程 {flow_id} 没有已发布版本，请先在编排页发布")
    record = store.get_version(flow_id, version, tenant_id=tenant_id or None)
    if record is None:
        raise LookupError(f"流程版本不存在: {flow_id}@{version}")
    if record.status != "published":
        raise ValueError(f"仅已发布版本可启动: {flow_id}@{version} 是 {record.status}")

    execution_id = uuid.uuid4().hex[:16]
    definition = json.loads(record.definition)
    definition["flow_id"] = flow_id
    definition["version"] = version

    fs.insert_local_execution(
        execution_id=execution_id,
        flow_id=flow_id,
        flow_version=version,
        status="running",
        input_json=json.dumps(params or {}, ensure_ascii=False),
        invoker=invoker,
        tenant_id=tenant_id,
    )

    _spawn(execution_id, _run_flow,
           store, execution_id, flow_id, version, definition, params or {}, None,
           tenant_id=tenant_id)
    return _to_info(fs.get_local_execution(execution_id))


def resume_local_execution(
    store: FlowStore,
    execution_id: str,
    resume_type: str,
    data: Optional[Dict[str, Any]],
    tenant_id: Optional[str] = None,
) -> bool:
    """恢复挂起的执行：从 SQLite checkpoint 继续（分布式策略）。"""
    row = fs.get_local_execution(execution_id, tenant_id=tenant_id)
    if row is None:
        return False
    if row["status"] != "suspended":
        raise ValueError(f"仅挂起状态可恢复: 当前 {row['status']}")

    definition = _load_definition(store, row["flow_id"], row["flow_version"],
                                  tenant_id=row.get("tenant_id") or None)
    context = row.get("context")
    if context is None:
        raise ValueError("挂起 checkpoint 缺失，无法恢复")

    # TOCTOU 防护：不用上面的快照做最终判定，而是原子条件更新
    # （UPDATE ... WHERE status='suspended'），并发 resume 只有一个能抢到，
    # 抢不到的按当前真实状态报错。
    if not fs.update_local_execution_status_if(execution_id, "suspended", "running"):
        current = fs.get_local_execution(execution_id)
        raise ValueError(
            f"仅挂起状态可恢复: 当前 {current['status'] if current else '已删除'}"
        )

    try:
        _spawn(execution_id, _run_flow,
               store, execution_id, row["flow_id"], row["flow_version"], definition,
               {}, {"context": context, "resume_type": resume_type, "data": data},
               initial_nodes=row.get("nodes") or [],
               initial_usage=row.get("usage"),
               tenant_id=row.get("tenant_id") or "")
    except RuntimeError:
        # 同执行 ID 的线程仍存活：回滚状态并拒绝，避免双线程写同一执行记录。
        fs.update_local_execution(execution_id, status="suspended")
        raise ValueError(f"执行 {execution_id} 尚有线程在运行，不能重复恢复")
    return True


def _spawn(execution_id: str, target, *args, **kwargs) -> None:
    """启动执行线程。同一 execution_id 已有存活线程时拒绝（防重复启动/恢复）。"""
    with _lock:
        existing = _threads.get(execution_id)
        if existing is not None and existing.is_alive():
            raise RuntimeError(f"执行 {execution_id} 已有运行中的线程")
        # 新执行线程配对一个新的取消事件（旧线程已退出才会走到这里）
        _cancel_events[execution_id] = threading.Event()
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
    mode = os.getenv("PLAITA_CONSOLE_LANGFUSE", "auto").strip().lower()
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


def _run_flow(
    store: FlowStore,
    execution_id: str,
    flow_id: str,
    version: str,
    definition: Dict[str, Any],
    params: Dict[str, Any],
    resume: Optional[Dict[str, Any]],
    initial_nodes: Optional[List[Dict[str, Any]]] = None,
    initial_usage: Optional[Dict[str, Any]] = None,
    tenant_id: str = "",
) -> None:
    """分布式策略驱动循环（与集群档 FlowWorker._process_execution_result 对齐）。"""
    handler = _ThreadLogHandler(execution_id, threading.get_ident(), tenant_id)
    root = logging.getLogger()
    root.addHandler(handler)
    # 本线程内节点解析凭据时按租户路由凭据文件（见 _patch_runtime_credentials）
    try:
        from . import credentials_svc as _creds
    except ImportError:
        import credentials_svc as _creds  # type: ignore
    tenant_token = _tenant_credentials_file.set(
        str(_creds.credentials_file(tenant_id or _creds.DEFAULT_TENANT_ID))
    )
    langfuse_callback = None
    try:
        logger.info(
            "本地执行 %s %s（flow=%s@%s）",
            execution_id, "恢复" if resume else "开始", flow_id, version,
        )
        flow = Flow.model_validate(definition)
        # _LocalTraceCallback 必须在前：它把 $EXECUTION_ID 覆写为 console 的
        # execution_id，随后的 LangfuseCallback 读到的 trace id 与控制台记录一致。
        trace_callback = _LocalTraceCallback(
            execution_id, initial_nodes=initial_nodes, initial_usage=initial_usage
        )
        handlers: List[FlowCallback] = [trace_callback]
        langfuse_callback = _build_langfuse_callback()
        if langfuse_callback is not None:
            handlers.append(langfuse_callback)
        execution = FlowExecution(callback_handlers=handlers)
        execution.mode = ExecutionMode.DISTRIBUTED
        # 让回调能拿到 execution：fresh start 的 on_flow_start（clean 之后触发）
        # 会把 $EXECUTION_ID 覆写为 console 的 execution_id，保持两边一致。
        trace_callback.bind_execution(execution)
        if langfuse_callback is not None:
            langfuse_callback.bind_execution(execution)

        if resume is None:
            result = execution.run_distributed(flow, params=params)
        else:
            result = execution.run_distributed(
                flow,
                saved_context=resume["context"],
                resume_type=resume["resume_type"],
                resume_data=resume.get("data"),
            )
        context = result.get("context")

        while True:
            context = result.get("context", context)
            # 取消检查点（波次① §3.1 本地档）：cancel_local_execution 置位
            # 的事件在此消费，步界终态化（软中断默认语义：在途节点跑完）。
            if _cancel_event_set(execution_id):
                _finalize_cancelled(execution_id, context)
                if langfuse_callback is not None:
                    langfuse_callback.finalize()
                break
            if result.get("is_end"):
                logger.info("本地执行 %s 完成", execution_id)
                # 条件推进 running→completed：已被置 cancelled 时条件不命中，
                # 不覆写既有终态（覆写战争消失，T8）。字段先落、状态后翻，
                # 中间态（running+output）无观察者依赖。
                fs.update_local_execution(
                    execution_id,
                    output_json=json.dumps(_safe(result.get("result")), ensure_ascii=False),
                    context_json=json.dumps(_safe(context), ensure_ascii=False),
                )
                if fs.update_local_execution_status_if(execution_id, "running", "completed"):
                    fs.update_local_execution(execution_id, end_time=datetime.utcnow())
                    if langfuse_callback is not None:
                        langfuse_callback.finalize()  # distributed 不发 on_flow_end，终态收口 root
                else:
                    logger.info("本地执行 %s 已非 running，保留现有终态", execution_id)
                break
            if result.get("is_suspend"):
                logger.info("本地执行 %s 挂起，等待恢复", execution_id)
                fs.update_local_execution(
                    execution_id,
                    context_json=json.dumps(_safe(context), ensure_ascii=False),
                )
                # 条件推进 running→suspended：取消已落 cancelled 时不覆写
                if not fs.update_local_execution_status_if(execution_id, "running", "suspended"):
                    logger.info("本地执行 %s 已被取消，保持 cancelled 终态", execution_id)
                break

            # 单步推进：resume_type="continue"。checkpoint 先落、状态用
            # 条件更新充当取消闸门（原语已在 resume_local_execution 使用）：
            # cancel_local_execution 已写 cancelled 时条件不命中 → 立即收口，
            # 不再翻回 running。
            fs.update_local_execution(
                execution_id,
                context_json=json.dumps(_safe(context), ensure_ascii=False),
            )
            if not fs.update_local_execution_status_if(execution_id, "running", "running"):
                _finalize_cancelled(execution_id, context)
                if langfuse_callback is not None:
                    langfuse_callback.finalize()
                break
            result = execution.run_distributed(
                flow, saved_context=context, resume_type="continue"
            )
    except Exception as e:  # noqa: BLE001 — 执行失败要落库而不是带崩线程
        logger.warning("本地执行 %s 失败: %s", execution_id, e)
        fs.finish_local_execution(
            execution_id,
            status="failed",
            error_json=json.dumps(
                {"message": str(e), "type": type(e).__name__}, ensure_ascii=False
            ),
        )
        if langfuse_callback is not None:
            langfuse_callback.finalize()
    finally:
        try:
            root.removeHandler(handler)
        except Exception:  # noqa: BLE001
            pass
        _tenant_credentials_file.reset(tenant_token)
        with _lock:
            _threads.pop(execution_id, None)
        _pop_cancel_event(execution_id)


class _ThreadLogHandler(logging.Handler):
    """只捕获本执行线程的 logging 记录，写入 local_logs（日志页本地档数据源）。"""

    def __init__(self, execution_id: str, thread_id: int, tenant_id: str = ""):
        super().__init__(level=logging.INFO)
        self._execution_id = execution_id
        self._thread_id = thread_id
        self._tenant_id = tenant_id

    def emit(self, record: logging.LogRecord) -> None:
        if record.thread != self._thread_id:
            return
        try:
            fs.insert_local_log(
                execution_id=self._execution_id,
                level=record.levelname,
                logger_name=record.name,
                message=record.getMessage(),
                tenant_id=self._tenant_id,
            )
        except Exception:  # noqa: BLE001 — 日志失败不影响执行
            pass


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
    execution_id: str, tenant_id: Optional[str] = None
) -> bool:
    return fs.delete_local_execution(execution_id, tenant_id=tenant_id)


def _to_info(row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    return row
