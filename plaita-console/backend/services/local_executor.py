"""本地单机模式执行器。

Redis 不可达时（``app.state.local_mode = True``），流程在 console 进程内
以普通线程执行：SQLite 落执行记录，回调采集节点级 trace，executions API
读本模块写入的记录（与 Redis 模式的 ExecutionInfo 结构对齐）。

限制（设计取舍）：
- 无 distributed/eventbus：挂起型节点（event/approval）在本模式下不会真正
  挂起，直接把 pending 结果当作普通输出继续往下走。
- cancel 为尽力而为：标记状态后不中断正在运行的线程。
"""
from __future__ import annotations

import json
import logging
import threading
import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from plaita.core.callback import FlowCallback
from plaita.core.executor import FlowExecution
from plaita.core.flow import Flow

try:
    from . import flow_store as fs
except ImportError:  # 平铺布局（cwd=backend）运行时
    import flow_store as fs  # type: ignore

if TYPE_CHECKING:
    from .flow_store import FlowStore

logger = logging.getLogger(__name__)

# 已启动线程表：execution_id -> Thread（进程内生命周期，仅防重复启动）
_threads: Dict[str, threading.Thread] = {}
_lock = threading.Lock()


class _LocalTraceCallback(FlowCallback):
    """把节点开始/结束写回本地执行记录（每节点一次 SQLite 更新，示例规模可接受）。"""

    def __init__(self, execution_id: str):
        self._execution_id = execution_id
        self._nodes: List[Dict[str, Any]] = []

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


def start_local_execution(
    store: FlowStore,
    flow_id: str,
    version: Optional[str],
    params: Optional[Dict[str, Any]],
    invoker: str = "local",
) -> Dict[str, Any]:
    """以本地模式启动流程：同步建档 + 后台线程执行。返回 ExecutionInfo dict。"""
    version = version or _latest_published(store, flow_id)
    if version is None:
        raise ValueError(f"流程 {flow_id} 没有已发布版本，请先在编排页发布")
    record = store.get_version(flow_id, version)
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
    )

    thread = threading.Thread(
        target=_run_flow,
        args=(execution_id, definition, params or {}),
        name=f"local-exec-{execution_id}",
        daemon=True,
    )
    with _lock:
        _threads[execution_id] = thread
    thread.start()
    return _to_info(fs.get_local_execution(execution_id))


class _ThreadLogHandler(logging.Handler):
    """只捕获本执行线程的 logging 记录，写入 local_logs（日志页本地档数据源）。"""

    def __init__(self, execution_id: str, thread_id: int):
        super().__init__(level=logging.INFO)
        self._execution_id = execution_id
        self._thread_id = thread_id

    def emit(self, record: logging.LogRecord) -> None:
        if record.thread != self._thread_id:
            return
        try:
            fs.insert_local_log(
                execution_id=self._execution_id,
                level=record.levelname,
                logger_name=record.name,
                message=record.getMessage(),
            )
        except Exception:  # noqa: BLE001 — 日志失败不影响执行
            pass


def _run_flow(execution_id: str, definition: dict, params: dict) -> None:
    handler = _ThreadLogHandler(execution_id, threading.get_ident())
    root = logging.getLogger()
    root.addHandler(handler)
    try:
        logger.info("本地执行 %s 开始（flow=%s）", execution_id, definition.get("flow_id"))
        flow = Flow.model_validate(definition)
        callback = _LocalTraceCallback(execution_id)
        result = FlowExecution(callback_handlers=[callback]).run_compatible(flow, False, **params)
        logger.info("本地执行 %s 完成", execution_id)
        fs.finish_local_execution(
            execution_id,
            status="completed",
            output_json=json.dumps(_safe(result), ensure_ascii=False),
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
    finally:
        try:
            root.removeHandler(handler)
        except Exception:  # noqa: BLE001
            pass
        with _lock:
            _threads.pop(execution_id, None)


def _latest_published(store: FlowStore, flow_id: str) -> Optional[str]:
    published = [v for v in store.list_versions(flow_id) if v.status == "published"]
    return published[-1].version if published else None


def get_local_execution(execution_id: str) -> Optional[Dict[str, Any]]:
    return fs.get_local_execution(execution_id)


def list_local_executions(
    status: Optional[str] = None, flow_id: Optional[str] = None
) -> List[Dict[str, Any]]:
    rows = fs.list_local_executions()
    out = []
    for row in rows:
        if status and row.get("status") != status:
            continue
        if flow_id and row.get("flow_id") != flow_id:
            continue
        out.append(row)
    out.sort(key=lambda x: x.get("start_time") or "", reverse=True)
    return out


def cancel_local_execution(execution_id: str) -> bool:
    row = fs.get_local_execution(execution_id)
    if row is None:
        return False
    if row["status"] == "running":
        fs.finish_local_execution(execution_id, status="cancelled")
    return True


def delete_local_execution(execution_id: str) -> bool:
    return fs.delete_local_execution(execution_id)


def _to_info(row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    return row
