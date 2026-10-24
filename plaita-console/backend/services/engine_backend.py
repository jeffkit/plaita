"""C1 阶段 2：local 降级 engine backend（plaita#323 / ADR-001 §1「console local = engine backend」）。

console 本地单机档的执行运行面自本模块起改挂 **engine backend**：复用集群
同一个 ``FlowWorker`` 状态机（plaita.server.flow_worker，含步界驱动循环
``_process_execution_result``、取消检查点、租约面、usage/node_timings/
timeline 采集），只换 storage——local 不是第四种执行语义，是 Python 引擎的
进程内降级后端形态（ADR-001 裁定）。

双后端形态（同一套行为用例跑两遍 = conformance 等价性证明基建，见
``tests/console/test_engine_backend_conformance.py``）：

- **memory**：``MemoryExecutionStorage`` + ``DictFlowStorage``——纯进程内
  参照形态（worker 域内测试/单机语义，无 console SQLite 依赖）；
- **engine backend（local 降级形态）**：``LocalExecutionStorage`` 适配
  ``local_executions`` 行 + ``ConsoleFlowStorage`` 适配 console FlowStore
  （SQL 权威定义模型只读）——API/日志读模型与旧本地环逐列一致。

进程内注册表（执行线程 / 取消 Event / 强删墓碑）与 ``local_executor``
**共用同一份模块级表**：API 面（cancel/delete/logs 的本地分支）对 engine
起跑的执行零感知——``cancel_local_execution`` 置位同一 Event、
``delete_local_execution`` join 同一线程、日志抑制（``BackendLogHandler``）
同一窗口。

阶段边界（#323 C1 分阶段）：阶段 2 切 ``local_scheduler.fire``；**阶段 3
（#335）** 起 executions API 的手动 start/resume 同经本模块
（``start_execution`` / ``resume_execution``），``local_executor`` 的第二套
执行器驱动（手搓 run_distributed 循环）随之删除——engine backend 是本地档
唯一执行语义（ADR-001）。本地墓碑是进程内
强删窗口（线程收口即清），与集群持久墓碑的口径差异在
``LocalExecutionStorage.load_execution_tombstone`` 如实声明。
"""
from __future__ import annotations

import json
import logging
import threading
import uuid
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from plaita.event.memory import InMemoryEventBus
from plaita.server.flow_worker import FlowWorker, _cancel_checkpoint_disabled
from plaita.storage.base import ExecutionState, ExecutionStorage, FlowStorage
from plaita.storage.memory import MemoryExecutionStorage

try:
    from . import flow_store as fs
    from . import local_executor as le
    from .credentials_svc import DEFAULT_TENANT_ID
except ImportError:  # 平铺布局（cwd=backend）运行时
    import flow_store as fs  # type: ignore
    import local_executor as le  # type: ignore
    from credentials_svc import DEFAULT_TENANT_ID  # type: ignore

logger = logging.getLogger(__name__)

# 行状态与 ExecutionState 状态的翻译：本地行模型用 ``failed``，引擎状态行
# 用 ``error``（集群语义）。两边词汇在适配层一次性收口。
_ROW_TERMINAL_STATUSES = ("completed", "failed", "cancelled")


def _row_status_for(state_status: str) -> str:
    """ExecutionState.status → local_executions.status（error↔failed）。"""
    return "failed" if state_status == "error" else state_status


def _state_status_for(row_status: str) -> str:
    """local_executions.status → ExecutionState.status（failed↔error）。"""
    return "error" if row_status == "failed" else row_status


def _json_or_null(value: Any) -> str:
    if value is None:
        return "null"
    try:
        return json.dumps(value, ensure_ascii=False)
    except (TypeError, ValueError):
        return "null"


# ---------------------------------------------------------------------------
# storage 适配：local 降级形态（local_executions 行）
# ---------------------------------------------------------------------------


class LocalExecutionStorage(ExecutionStorage):
    """``local_executions`` 表上的 ExecutionStorage 适配（local 降级形态）。

    执行状态权威仍是 console SQLite 行模型（#323 边界「不动 SQL 为权威的
    定义模型」）。映射为**必要字段子集**：context/status/start/end/error/
    usage/tenant——flow_hash/generation/pending/next_step/timeline 等 worker
    协议字段本地行无列不落（本地档 resume 走既有 API 旧环，指纹守卫属集群
    重投语义；分叉已随 conformance 套件声明）。

    CAS 语义与旧本地环同源（同一 ``update_local_execution_status_if`` 原子
    原语），两条规则消掉覆写战争（T8 契约）：

    - **非终态写不复活终态行**：控制面已终态化（cancelled）后，worker 的
      running/suspended 步界写一律跳过（返回 True——这是刻意的镜像跳过，
      不是落盘失败，绝不能喂 ``StatePersistError`` 把执行带崩）；
    - **首个终态获胜**：行已是**不同**终态时迟到终态写跳过（软中断语义下
      终节点先于取消被消费完成时，控制面意图仍获胜）；同终态补写只丰富
      字段（取消点 context 快照补写 = ``_finalize_cancelled`` 同语义）。
    """

    def save_execution_state(self, execution_id: str, state: ExecutionState) -> bool:
        row = fs.get_local_execution(execution_id)
        row_status = (row or {}).get("status")
        status = _row_status_for(state.status)
        # 空上下文写 "null"（行 context 读回 None），与旧环
        # ``_finalize_cancelled(context=None)`` 的行形状一致。
        context_json = (
            json.dumps(le._safe(state.context), ensure_ascii=False)
            if state.context
            else "null"
        )
        if status in _ROW_TERMINAL_STATUSES:
            if row_status in _ROW_TERMINAL_STATUSES and row_status != status:
                logger.info(
                    "本地执行 %s 已是终态 %s，跳过迟到的 %s 写（首个终态获胜）",
                    execution_id, row_status, status,
                )
                return True
            fs.finish_local_execution(
                execution_id,
                status=status,
                context_json=context_json,
                error_json=_json_or_null(state.error) if state.error else None,
                # state.usage None = 未采集 ⇒ 显式落 'null'（不是 0），与旧环
                # _usage_json_for 的「未采集」口径一致。
                usage_json=_json_or_null(state.usage) if state.usage else "null",
            )
            return True
        if row_status != "running":
            logger.info(
                "本地执行 %s 已终态（%s），跳过非终态 %s 写（不复活）",
                execution_id, row_status, status,
            )
            return True
        # 步界推进：先落 checkpoint 再条件翻 suspended——取消竞态下翻转不命中
        # 即保持 cancelled，下一拍 worker 的取消分支补终态写。
        fields: Dict[str, str] = {"usage_json": _json_or_null(state.usage)}
        if state.context:
            fields["context_json"] = context_json
        fs.update_local_execution(execution_id, **fields)
        if status == "suspended":
            fs.update_local_execution_status_if(execution_id, "running", "suspended")
        return True

    def load_execution_state(self, execution_id: str) -> Optional[ExecutionState]:
        row = fs.get_local_execution(execution_id)
        if row is None:
            return None
        return ExecutionState(
            execution_id=row["execution_id"],
            flow_id=row["flow_id"],
            flow_version=row["flow_version"] or None,
            context=row["context"] or {},
            status=_state_status_for(row["status"]),
            start_time=row["start_time"],
            end_time=row["end_time"],
            error=row["error"],
            usage=row["usage"],
            invoker=row["invoker"] or None,
            tenant_id=row["tenant_id"] or None,
        )

    def delete_execution_state(self, execution_id: str) -> bool:
        return fs.delete_local_execution(execution_id)

    def list_executions(
        self,
        query: Optional[Any] = None,
        order_by: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[ExecutionState]:
        states = []
        for row in fs.list_local_executions():
            state = self.load_execution_state(row["execution_id"])
            if state is not None:
                states.append(state)
        return states[offset:offset + limit]

    def load_execution_tombstone(self, execution_id: str) -> Optional[Dict[str, Any]]:
        """本地墓碑 = 强删窗口（``local_executor._deleted_executions``，进程内）。

        与集群持久墓碑的口径差异如实声明：本地档删除语义的墓碑是**进程内
        窗口**（执行线程收口即清，旧环同一语义——A43-01 的「删后日志抑制」
        与 preminted start 复核共用该窗口）；跨进程/跨重启的持久墓碑属队列
        重投语义，本地档无重投故不需要。
        """
        if le._is_deleted(execution_id):
            return {
                "tenant": None,
                "reason": "local_force_delete",
                "deleted_at": None,
                "scope": "in_process_window",
            }
        return None


class ConsoleFlowStorage(FlowStorage):
    """console FlowStore 上的 FlowStorage 适配：定义**只读**。

    SQL 权威定义模型的写面不经过 worker（#323 边界）；租户路由随执行线程
    的引擎租户上下文（``current_tenant``），与 Redis 后端的 namespace 路由
    同构。version 缺省解析最新已发布版本（旧环 ``_latest_published`` 同源）。
    """

    def __init__(self, store):
        self._store = store

    def get_flow(self, flow_id: str, version: Optional[str] = None) -> Optional[Dict[str, Any]]:
        try:
            from plaita.tenant_context import current_tenant

            tenant = current_tenant() or None
        except Exception:  # noqa: BLE001 — 租户上下文不可用按平台视角读
            tenant = None
        version = version or le._latest_published(self._store, flow_id, tenant)
        if version is None:
            return None
        record = self._store.get_version(flow_id, version, tenant_id=tenant)
        if record is None:
            return None
        definition = json.loads(record.definition)
        definition["flow_id"] = flow_id
        definition["version"] = version
        return definition

    def save_flow(self, flow: Dict[str, Any]) -> bool:
        logger.warning(
            "engine backend 不经 flow_storage.save_flow 写定义（console FlowStore "
            "为权威写面）；本次调用被忽略"
        )
        return False


# ---------------------------------------------------------------------------
# storage 适配：memory 参照形态（conformance memory 腿）
# ---------------------------------------------------------------------------


class DictFlowStorage(FlowStorage):
    """进程内定义表（memory 参照形态）：``{flow_id: {version: definition}}``。

    ``put_flow`` 是测试/宿主装配入口；``save_flow`` 与 ConsoleFlowStorage
    同口径（定义写面不经过 worker）。
    """

    def __init__(self) -> None:
        self._flows: Dict[str, Dict[str, Dict[str, Any]]] = {}

    def put_flow(self, flow_id: str, version: str, definition: Dict[str, Any]) -> None:
        self._flows.setdefault(flow_id, {})[version] = definition

    def get_flow(self, flow_id: str, version: Optional[str] = None) -> Optional[Dict[str, Any]]:
        versions = self._flows.get(flow_id)
        if not versions:
            return None
        if version is None:
            version = "latest" if "latest" in versions else max(versions)
        return versions.get(version)

    def save_flow(self, flow: Dict[str, Any]) -> bool:
        return False


# ---------------------------------------------------------------------------
# 镜像收口：终态字段向后端读模型的落点
# ---------------------------------------------------------------------------


class LocalMirror:
    """local 降级形态的镜像：``local_executions`` 行 + ``local_logs`` 表。

    ``record_output`` 只在行已 completed 时补写 output_json（worker 终态落盘
    由 storage 适配完成；output 不在 ExecutionState 里，由驱动线程从
    start_flow 返回值补）——字段先落、状态已在，与旧环同序。
    """

    def read_state(self, execution_id: str) -> Optional[Dict[str, Any]]:
        return fs.get_local_execution(execution_id)

    def record_output(self, execution_id: str, result: Dict[str, Any]) -> None:
        row = fs.get_local_execution(execution_id)
        if row is not None and row["status"] == "completed":
            fs.update_local_execution(
                execution_id,
                output_json=json.dumps(le._safe(result.get("result")), ensure_ascii=False),
            )

    def record_failed(self, execution_id: str, exc: BaseException) -> None:
        """失败终态镜像。usage 聚合与旧环同口径（#37 P2：终态才聚合、能采
        多少算多少、未采集落 'null'）——worker 的 start 失败路径**刻意留
        running 行**（集群 at-least-once 契约，不终态化），咽喉点的
        ``_collect_usage`` 不经此路。聚合源两段兜底：行上最后持久化的
        checkpoint context（步界已落盘的失败）→ trace 回调已落库的节点
        ``output.usage``（流内异常先于步界 checkpoint 时行上无 context，
        已完成节点的 usage 采集面等价）。"""
        row = fs.get_local_execution(execution_id)
        usage_json = le._usage_json_for((row or {}).get("context"))
        if usage_json == "null":
            nodes = (row or {}).get("nodes") or []
            node_results = {
                n["id"]: {"usage": n["output"].get("usage")}
                for n in nodes
                if isinstance(n, dict) and n.get("id")
                and isinstance(n.get("output"), dict)
                and isinstance(n["output"].get("usage"), dict)
            }
            if node_results:
                try:
                    from plaita.storage.usage import aggregate_node_usage

                    usage = aggregate_node_usage(node_results)
                    if usage is not None:
                        usage_json = json.dumps(usage, ensure_ascii=False)
                except Exception:  # noqa: BLE001 — 观测口径失败绝不拖累终态
                    logger.warning(
                        "失败终态 usage 聚合失败（按未采集落库）", exc_info=True
                    )
        fs.finish_local_execution(
            execution_id,
            status="failed",
            error_json=json.dumps(
                {"message": str(exc), "type": type(exc).__name__}, ensure_ascii=False
            ),
            usage_json=usage_json,
        )


class MemoryMirror:
    """memory 参照形态的镜像：读模型 = ``MemoryExecutionStorage`` 本体。"""

    def __init__(self, storage: MemoryExecutionStorage) -> None:
        self._storage = storage
        self._outputs: Dict[str, Any] = {}
        self._failures: Dict[str, Dict[str, str]] = {}

    def read_state(self, execution_id: str) -> Optional[Dict[str, Any]]:
        state = self._storage.load_execution_state(execution_id)
        if state is None:
            return None
        return {
            "status": _row_status_for(state.status),
            "context": state.context,
            "output": self._outputs.get(execution_id),
            "error": state.error or self._failures.get(execution_id),
            "end_time": state.end_time,
            "usage": state.usage,
        }

    def record_output(self, execution_id: str, result: Dict[str, Any]) -> None:
        state = self._storage.load_execution_state(execution_id)
        if state is not None and _row_status_for(state.status) == "completed":
            self._outputs[execution_id] = result.get("result")

    def record_failed(self, execution_id: str, exc: BaseException) -> None:
        self._failures[execution_id] = {
            "message": str(exc),
            "type": type(exc).__name__,
        }


# ---------------------------------------------------------------------------
# 日志 sink 与 handler（线程过滤 + 强删抑制，与旧 _ThreadLogHandler 同语义）
# ---------------------------------------------------------------------------


class MemoryLogSink:
    """内存日志汇（memory 腿）：按执行收集记录，``records`` 供断言。"""

    def __init__(self) -> None:
        self.records: List[Dict[str, str]] = []

    def __call__(
        self, *, execution_id: str, level: str, logger_name: str,
        message: str, tenant_id: str = "",
    ) -> None:
        self.records.append(
            {
                "execution_id": execution_id,
                "level": level,
                "logger_name": logger_name,
                "message": message,
            }
        )

    def messages_for(self, execution_id: str) -> List[str]:
        return [r["message"] for r in self.records if r["execution_id"] == execution_id]

    def forget(self, execution_id: str) -> None:
        """随执行删除清掉其日志（``fs.delete_local_logs`` 的 memory 腿同语义）。"""
        self.records = [r for r in self.records if r["execution_id"] != execution_id]


def _fs_log_sink(
    *, execution_id: str, level: str, logger_name: str,
    message: str, tenant_id: str = "",
) -> None:
    fs.insert_local_log(
        execution_id=execution_id,
        level=level,
        logger_name=logger_name,
        message=message,
        tenant_id=tenant_id,
    )


class BackendLogHandler(logging.Handler):
    """只捕获本执行线程的 logging 记录写入后端 sink。

    ``local_executor._ThreadLogHandler`` 的 sink 可插版：local 腿 sink 落
    ``local_logs`` 表，memory 腿落 ``MemoryLogSink``。线程过滤与强删墓碑
    抑制语义逐字对齐旧 handler。
    """

    def __init__(
        self, execution_id: str, thread_id: int,
        sink: Callable[..., None], tenant_id: str = "",
    ):
        super().__init__(level=logging.INFO)
        self._execution_id = execution_id
        self._thread_id = thread_id
        self._sink = sink
        self._tenant_id = tenant_id

    def emit(self, record: logging.LogRecord) -> None:
        if record.thread != self._thread_id:
            return
        # 强删窗口内的日志一律丢弃（删除是持久语义，不得删后续长）。
        if le._is_deleted(self._execution_id):
            return
        try:
            self._sink(
                execution_id=self._execution_id,
                level=record.levelname,
                logger_name=record.name,
                message=record.getMessage(),
                tenant_id=self._tenant_id,
            )
        except Exception:  # noqa: BLE001 — 日志失败不影响执行
            pass


# ---------------------------------------------------------------------------
# worker 派生与门面
# ---------------------------------------------------------------------------


class EngineBackendWorker(FlowWorker):
    """engine backend 的 FlowWorker 派生：取消面改读进程内 Event 表。

    集群档取消 = Redis 标志键（控制面写键、worker 步界消费）；本地降级形态
    的同一语义由 ``local_executor._cancel_events`` 的 threading.Event 承担
    （控制面置位 + 即时落 cancelled 终态，worker 步界消费——T8 契约不变，
    软中断默认语义：在途节点跑完，步界收口）。其余（租约 NullExecutionLease、
    事件发布无 Redis no-op、步界驱动循环、usage/timeline 采集）全部沿用基类。
    回滚开关 ``PLAITA_DISABLE_CANCEL_CHECKPOINT`` 与集群档同源生效。
    """

    def _cancel_requested(self, execution_id: str) -> bool:
        if _cancel_checkpoint_disabled():
            return False
        return le._cancel_event_set(execution_id)


class EngineBackend:
    """engine backend 门面：FlowWorker 状态机 + 可插 storage/mirror/日志 sink。

    线程 / 取消 Event / 强删墓碑注册表与 local_executor 共用模块级表（见
    模块 docstring）。``launch`` 经 ``local_executor._spawn`` 起跑驱动线程，
    因此重复启动/恢复防护、取消事件配对、墓碑窗口复位与旧环逐项一致。
    """

    def __init__(
        self,
        name: str,
        execution_storage: ExecutionStorage,
        flow_storage: FlowStorage,
        mirror: Any,
        log_sink: Callable[..., None],
    ):
        self.name = name
        self.execution_storage = execution_storage
        self.flow_storage = flow_storage
        self.mirror = mirror
        self.log_sink = log_sink

    # ---- 装配 ----

    def build_worker(
        self, execution_id: str, tenant_id: str = "",
        extra_handlers: Optional[List[Any]] = None,
        initial_nodes: Optional[List[Any]] = None,
    ) -> EngineBackendWorker:
        trace = le._LocalTraceCallback(execution_id, initial_nodes=initial_nodes)
        handlers: List[Any] = [trace]
        langfuse = le._build_langfuse_callback()
        if langfuse is not None:
            handlers.append(langfuse)
        handlers.extend(extra_handlers or [])
        return EngineBackendWorker(
            execution_storage=self.execution_storage,
            flow_storage=self.flow_storage,
            event_bus=InMemoryEventBus(),
            callback_handlers=handlers,
        )

    # ---- 起跑 ----

    def start(
        self,
        flow_id: str,
        version: str,
        params: Optional[Dict[str, Any]],
        execution_id: Optional[str] = None,
        tenant_id: str = "",
    ) -> str:
        """起跑一个执行（预铸/就地铸造 id），返回 execution_id。

        无建档校验（那是宿主入口 start_execution 的职责）；进程内语义共享
        注册表使重复起跑防护照常生效。
        """
        execution_id = execution_id or uuid.uuid4().hex[:16]
        self.launch(execution_id, flow_id, version, params, tenant_id)
        return execution_id

    def launch(
        self,
        execution_id: str,
        flow_id: str,
        version: str,
        params: Optional[Dict[str, Any]],
        tenant_id: str = "",
    ) -> None:
        le._spawn(
            execution_id, self._drive,
            execution_id, flow_id, version, params or {}, tenant_id or "",
        )

    def launch_resume(
        self,
        execution_id: str,
        flow_id: str,
        resume_type: str,
        data: Optional[Dict[str, Any]],
        initial_nodes: Optional[List[Any]] = None,
        tenant_id: str = "",
    ) -> None:
        """恢复挂起执行：同一 ``_spawn`` 防护（同 id 线程存活拒绝）起跑
        resume 驱动线程（C1 阶段 3）。"""
        le._spawn(
            execution_id, self._drive,
            execution_id, flow_id, "", {}, tenant_id or "",
            {"resume_type": resume_type, "data": data,
             "initial_nodes": initial_nodes},
        )

    def _drive(
        self, execution_id: str, flow_id: str, version: str,
        params: Dict[str, Any], tenant_id: str,
        resume: Optional[Dict[str, Any]] = None,
    ) -> None:
        """驱动线程：旧环 _run_flow 的脚手架（日志/租户/回调/收尾）+
        FlowWorker 状态机驱动（start_flow 内含整个步界循环；resume 走
        ``resume_flow``——lease/终态幂等/#33 守卫沿用基类，C1 阶段 3）。"""
        handler = BackendLogHandler(
            execution_id, threading.get_ident(), self.log_sink, tenant_id
        )
        root = logging.getLogger()
        root.addHandler(handler)
        engine_tenant_token = None
        try:
            from plaita.tenant_context import reset_current_tenant, set_current_tenant

            engine_tenant_token = set_current_tenant(tenant_id or DEFAULT_TENANT_ID)
        except Exception:  # noqa: BLE001 — 引擎侧上下文不可用不阻塞本地执行
            pass
        try:
            if resume is None:
                logger.info(
                    "engine backend 执行 %s 开始（flow=%s@%s）",
                    execution_id, flow_id, version,
                )
            else:
                logger.info(
                    "engine backend 执行 %s 恢复（flow=%s, resume_type=%s）",
                    execution_id, flow_id, resume["resume_type"],
                )
            worker = self.build_worker(
                execution_id, tenant_id,
                initial_nodes=(resume or {}).get("initial_nodes"),
            )
            if resume is None:
                result = worker.start_flow(
                    flow_id, params=params, version=version, execution_id=execution_id
                )
            else:
                result = worker.resume_flow(
                    flow_id, execution_id, resume["resume_type"],
                    data=resume.get("data"),
                )
            self.mirror.record_output(execution_id, result or {})
            logger.info(
                "engine backend 执行 %s 收口（status=%s）",
                execution_id, (result or {}).get("status"),
            )
        except Exception as e:  # noqa: BLE001 — 执行失败要落库而不是带崩线程
            logger.warning("engine backend 执行 %s 失败: %s", execution_id, e)
            self.mirror.record_failed(execution_id, e)
        finally:
            try:
                root.removeHandler(handler)
            except Exception:  # noqa: BLE001
                pass
            if engine_tenant_token is not None:
                try:
                    from plaita.tenant_context import reset_current_tenant

                    reset_current_tenant(engine_tenant_token)
                except Exception:  # noqa: BLE001
                    pass
            with le._lock:
                le._threads.pop(execution_id, None)
                le._deleted_executions.discard(execution_id)
            le._pop_cancel_event(execution_id)

    # ---- 控制面 / 读面（conformance 与宿主共用）----

    def read_state(self, execution_id: str) -> Optional[Dict[str, Any]]:
        return self.mirror.read_state(execution_id)

    def tombstone(self, execution_id: str) -> Optional[Dict[str, Any]]:
        """读执行删除墓碑（存储面；local 腿 = 强删窗口，memory 腿 = 内存墓碑）。"""
        return self.execution_storage.load_execution_tombstone(execution_id)

    def read_status(self, execution_id: str) -> Optional[str]:
        state = self.mirror.read_state(execution_id)
        return None if state is None else state.get("status")

    def cancel(self, execution_id: str, tenant_id: Optional[str] = None) -> bool:
        """控制面取消：置位取消 Event + 即时落 cancelled 终态。

        local 腿直接复用 ``local_executor.cancel_local_execution``（API 面
        本体）；memory 腿按同语义对 storage 落终态。
        """
        if self.name == "local":
            return le.cancel_local_execution(execution_id, tenant_id=tenant_id)
        state = self.mirror.read_state(execution_id)
        if state is None:
            return False
        with le._lock:
            event = le._cancel_events.get(execution_id)
        if event is not None:
            event.set()
        raw = self.execution_storage.load_execution_state(execution_id)
        if raw is not None and raw.status in ("running", "suspended"):
            raw.status = "cancelled"
            raw.end_time = raw.end_time or datetime.now().isoformat()
            self.execution_storage.save_execution_state(execution_id, raw)
        return True

    def force_delete(self, execution_id: str, tenant_id: Optional[str] = None) -> bool:
        """强删：立墓碑（日志抑制 + preminted 拒绝窗口）→ 置位取消 → 有界
        join → 删状态。local 腿复用 ``local_executor.delete_local_execution``
        （API 面本体，含行/日志清理）；memory 腿按同语义删 storage。"""
        if self.name == "local":
            return le.delete_local_execution(execution_id, tenant_id=tenant_id, force=True)
        state = self.mirror.read_state(execution_id)
        if state is None:
            return False
        le._mark_deleted(execution_id)
        with le._lock:
            event = le._cancel_events.get(execution_id)
        if event is not None:
            event.set()
        thread = le._live_thread(execution_id)
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=le.LOCAL_DELETE_JOIN_TIMEOUT_SECONDS)
        self.execution_storage.delete_execution_state(execution_id)
        if hasattr(self.log_sink, "forget"):
            self.log_sink.forget(execution_id)
        return True

    def recent_logs(self, execution_id: str) -> List[str]:
        """按执行读日志消息（local 腿走 ``local_logs`` 表，memory 腿走内存汇）。"""
        if self.name == "local":
            return [
                entry["message"]
                for entry in fs.list_local_logs(execution_id=execution_id, limit=200)
            ]
        return self.log_sink.messages_for(execution_id)


# ---------------------------------------------------------------------------
# 双后端构造器与宿主入口
# ---------------------------------------------------------------------------


def local_backend(store) -> EngineBackend:
    """local 降级形态：storage/mirror/log 全部落在 console SQLite 读模型。"""
    return EngineBackend(
        name="local",
        execution_storage=LocalExecutionStorage(),
        flow_storage=ConsoleFlowStorage(store),
        mirror=LocalMirror(),
        log_sink=_fs_log_sink,
    )


def memory_backend() -> EngineBackend:
    """memory 参照形态：纯进程内（conformance memory 腿）。"""
    storage = MemoryExecutionStorage()
    sink = MemoryLogSink()
    return EngineBackend(
        name="memory",
        execution_storage=storage,
        flow_storage=DictFlowStorage(),
        mirror=MemoryMirror(storage),
        log_sink=sink,
    )


def start_execution(
    store,
    flow_id: str,
    version: Optional[str],
    params: Optional[Dict[str, Any]],
    invoker: str = "local",
    tenant_id: str = "",
) -> Dict[str, Any]:
    """本地档经 engine backend 起跑——``local_scheduler.fire`` 与 executions
    API 手动 start 的运行面入口（C1 阶段 2/3）。

    行为契约与旧环 ``local_executor.start_local_execution``（C1 阶段 3 已删）
    逐项对齐：版本解析/
    发布校验同规则（同型异常，``fire`` 的失败留痕语义不变）、行建档同列
    （input/invoker/tenant/start_time）、返回同形（``get_local_execution``
    行 dict）。差异仅在执行驱动：FlowWorker 状态机替代手搓 run_distributed
    循环（等价性由 conformance 双后端套件证明）。
    """
    version = version or le._latest_published(store, flow_id, tenant_id or None)
    if version is None:
        raise ValueError(f"流程 {flow_id} 没有已发布版本，请先在编排页发布")
    record = store.get_version(flow_id, version, tenant_id=tenant_id or None)
    if record is None:
        raise LookupError(f"流程版本不存在: {flow_id}@{version}")
    if record.status != "published":
        raise ValueError(f"仅已发布版本可启动: {flow_id}@{version} 是 {record.status}")

    execution_id = uuid.uuid4().hex[:16]
    fs.insert_local_execution(
        execution_id=execution_id,
        flow_id=flow_id,
        flow_version=version,
        status="running",
        input_json=json.dumps(params or {}, ensure_ascii=False),
        invoker=invoker,
        tenant_id=tenant_id,
    )
    local_backend(store).launch(execution_id, flow_id, version, params, tenant_id)
    return fs.get_local_execution(execution_id)


def resume_execution(
    store,
    execution_id: str,
    resume_type: str,
    data: Optional[Dict[str, Any]],
    tenant_id: Optional[str] = None,
) -> bool:
    """本地档经 engine backend 恢复挂起执行——executions API 手动 resume 的
    运行面入口（C1 阶段 3，#335）。

    行为契约与旧环 ``local_executor.resume_local_execution``（本阶段已删）
    逐项对齐：行缺失 ``False``；非挂起 / ``continue|retry``（#33，挂起执行
    只认 event/cancel/timeout 决议）/ checkpoint 缺失均 ``ValueError`` 同
    文案；挂起→running 原子 CAS 充当并发 resume 互斥（抢不到按当前真实
    状态报错）；``_spawn`` 同 id 线程存活拒绝时回滚状态再拒。差异仅在执行
    驱动：``worker.resume_flow``（lease/终态幂等/挂起幂等短路/flow_hash
    守卫沿用基类，本地行无 flow_hash 列跳过）替代手搓 run_distributed 循环；
    ``_with_resume_binding``（#221 挂起订阅绑定）与行上 nodes 续栽（恢复
    不清节点 trace）同源保留。
    """
    row = fs.get_local_execution(execution_id, tenant_id=tenant_id)
    if row is None:
        return False
    if row["status"] != "suspended":
        raise ValueError(f"仅挂起状态可恢复: 当前 {row['status']}")
    # #33 对齐 worker 契约：挂起执行只认 event/cancel/timeout 决议——
    # continue/retry 不携带推进语义（策略层 pending 守卫必拒），本地档
    # 同样拒绝而不是让执行线程把失败落成 failed 终态。
    if resume_type in ("continue", "retry"):
        raise ValueError(
            f"挂起执行不接受 resume_type={resume_type}（会被挂起节点守卫拒绝），"
            "请用 event/cancel/timeout 决议挂起节点"
        )
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
        local_backend(store).launch_resume(
            execution_id, row["flow_id"], resume_type,
            le._with_resume_binding(context, resume_type, data),
            initial_nodes=row.get("nodes") or [],
            tenant_id=row.get("tenant_id") or "",
        )
    except RuntimeError:
        # 同执行 ID 的线程仍存活：回滚状态并拒绝，避免双线程写同一执行记录。
        fs.update_local_execution(execution_id, status="suspended")
        raise ValueError(f"执行 {execution_id} 尚有线程在运行，不能重复恢复")
    return True
