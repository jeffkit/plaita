"""
流程定义存储服务

基于 SQLAlchemy 的 flow / flow_version 读写（node_descriptor 的 CRUD 留给节点管理服务，
本层只提供表初始化支持）。同步引擎 + 同步 Session：sqlite 本地访问延迟极低，
API 层（M3）可通过 ``run_in_executor`` 调用，避免引入 aiosqlite 额外依赖。
"""
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import create_engine, select, update
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

try:
    from ..models.flow import (
    Base,
    CopilotThread,
    FlowRecord,
    FlowVersion,
    LocalExecution,
    LocalLog,
    LocalSchedule,
    LocalScheduleFire,
    NodeDescriptor,
)
except ImportError:
    from models.flow import (  # type: ignore
    Base,
    CopilotThread,
    FlowRecord,
    FlowVersion,
    LocalExecution,
    LocalLog,
    LocalSchedule,
    LocalScheduleFire,
    NodeDescriptor,
)

logger = logging.getLogger(__name__)


# ============ Pydantic I/O 模型 ============

class FlowSummary(BaseModel):
    """流程摘要（列表项）"""

    flow_id: str
    author: str = ""
    desc: str = ""
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class FlowVersionOut(BaseModel):
    """流程版本详情"""

    flow_id: str
    version: str
    status: str
    definition: str = ""
    layout: str = ""
    created_at: Optional[datetime] = None
    published_at: Optional[datetime] = None
    created_by: str = ""


class SaveFlowDefinitionResult(BaseModel):
    """保存结果"""

    flow_id: str
    version: str
    status: str


class NodeDescriptorOut(BaseModel):
    """节点描述（内置 + 自定义）"""

    # 字段名用 node_schema_json 避开 pydantic v2 "schema_json shadows BaseModel"
    # 启动警告；alias 保持对外键名不变（构造/序列化仍认 schema_json）。
    model_config = ConfigDict(populate_by_name=True)

    node_type: str
    node_name: str = ""
    category: str = ""
    node_schema_json: str = Field("{}", alias="schema_json")
    is_builtin: bool = False


# ============ 服务 ============

_engine: Optional[Engine] = None
_SessionLocal: Optional[sessionmaker] = None


class FlowStore:
    """flow / flow_version CRUD 服务"""

    def __init__(self, session_local: sessionmaker):
        self._session_local = session_local

    # ---- flow ----

    def ensure_flow(self, flow_id: str, author: str = "", desc: str = "") -> FlowRecord:
        """确保 flow 记录存在，不存在则创建。返回 ORM 记录。"""
        with self._session_local() as session:  # type: Session
            record = session.scalars(
                select(FlowRecord).where(FlowRecord.flow_id == flow_id)
            ).first()
            if record is None:
                record = FlowRecord(flow_id=flow_id, author=author, desc=desc)
                session.add(record)
                session.commit()
                session.refresh(record)
            return record

    def list_flows(self) -> List[FlowSummary]:
        with self._session_local() as session:
            rows = session.scalars(select(FlowRecord).order_by(FlowRecord.updated_at.desc())).all()
            return [
                FlowSummary(
                    flow_id=r.flow_id,
                    author=r.author,
                    desc=r.desc,
                    created_at=r.created_at,
                    updated_at=r.updated_at,
                )
                for r in rows
            ]

    def get_flow_record(self, flow_id: str) -> Optional[FlowRecord]:
        with self._session_local() as session:
            return session.scalars(
                select(FlowRecord).where(FlowRecord.flow_id == flow_id)
            ).first()

    def create_flow(self, flow_id: str, author: str = "", desc: str = "") -> FlowRecord:
        """新建 flow 记录，已存在则抛 ValueError。"""
        with self._session_local() as session:
            existing = session.scalars(
                select(FlowRecord).where(FlowRecord.flow_id == flow_id)
            ).first()
            if existing is not None:
                raise ValueError(f"流程已存在: {flow_id}")
            record = FlowRecord(flow_id=flow_id, author=author, desc=desc)
            session.add(record)
            try:
                session.commit()
            except IntegrityError as e:
                session.rollback()
                raise ValueError(f"流程已存在: {flow_id}") from e
            session.refresh(record)
            return record

    def delete_flow(self, flow_id: str) -> bool:
        """删除 flow 及其全部版本（级联）。不存在抛 LookupError。"""
        with self._session_local() as session:
            record = session.scalars(
                select(FlowRecord).where(FlowRecord.flow_id == flow_id)
            ).first()
            if record is None:
                raise LookupError(f"流程不存在: {flow_id}")
            session.delete(record)
            session.commit()
            return True

    # ---- version ----

    def save_flow_definition(
        self,
        flow_id: str,
        version: str,
        definition: str,
        layout: str = "",
        status: str = "draft",
        created_by: str = "",
    ) -> SaveFlowDefinitionResult:
        """保存（草稿）版本。已存在的 published 版本不可覆盖。"""
        self.ensure_flow(flow_id)
        with self._session_local() as session:
            existing = session.scalars(
                select(FlowVersion).where(
                    FlowVersion.flow_id == flow_id, FlowVersion.version == version
                )
            ).first()
            if existing is not None:
                if existing.status == "published":
                    raise ValueError(
                        f"版本 {flow_id}@{version} 已发布，不可覆盖"
                    )
                existing.definition = definition
                existing.layout = layout
                existing.status = status
                session.commit()
                return SaveFlowDefinitionResult(
                    flow_id=flow_id, version=version, status=existing.status
                )
            row = FlowVersion(
                flow_id=flow_id,
                version=version,
                status=status,
                definition=definition,
                layout=layout,
                created_by=created_by,
            )
            session.add(row)
            try:
                session.commit()
            except IntegrityError as e:
                session.rollback()
                raise ValueError(f"版本 {flow_id}@{version} 已存在") from e
            return SaveFlowDefinitionResult(flow_id=flow_id, version=version, status=status)

    def get_version(self, flow_id: str, version: str) -> Optional[FlowVersionOut]:
        with self._session_local() as session:
            row = session.scalars(
                select(FlowVersion).where(
                    FlowVersion.flow_id == flow_id, FlowVersion.version == version
                )
            ).first()
            if row is None:
                return None
            return FlowVersionOut(
                flow_id=row.flow_id,
                version=row.version,
                status=row.status,
                definition=row.definition,
                layout=row.layout,
                created_at=row.created_at,
                published_at=row.published_at,
                created_by=row.created_by,
            )

    def list_versions(self, flow_id: str) -> List[FlowVersionOut]:
        with self._session_local() as session:
            rows = session.scalars(
                select(FlowVersion)
                .where(FlowVersion.flow_id == flow_id)
                .order_by(FlowVersion.created_at.asc())
            ).all()
            return [
                FlowVersionOut(
                    flow_id=r.flow_id,
                    version=r.version,
                    status=r.status,
                    definition=r.definition,
                    layout=r.layout,
                    created_at=r.created_at,
                    published_at=r.published_at,
                    created_by=r.created_by,
                )
                for r in rows
            ]

    def publish_version(self, flow_id: str, version: str) -> FlowVersionOut:
        """发布版本：draft → published。已发布则幂等返回。不存在则 LookupError。"""
        with self._session_local() as session:
            row = session.scalars(
                select(FlowVersion).where(
                    FlowVersion.flow_id == flow_id, FlowVersion.version == version
                )
            ).first()
            if row is None:
                raise LookupError(f"版本不存在: {flow_id}@{version}")
            if row.status != "published":
                row.status = "published"
                row.published_at = datetime.utcnow()
                session.commit()
                session.refresh(row)
            return FlowVersionOut(
                flow_id=row.flow_id,
                version=row.version,
                status=row.status,
                definition=row.definition,
                layout=row.layout,
                created_at=row.created_at,
                published_at=row.published_at,
                created_by=row.created_by,
            )

    def delete_version(self, flow_id: str, version: str) -> bool:
        with self._session_local() as session:
            row = session.scalars(
                select(FlowVersion).where(
                    FlowVersion.flow_id == flow_id, FlowVersion.version == version
                )
            ).first()
            if row is None:
                raise LookupError(f"版本不存在: {flow_id}@{version}")
            session.delete(row)
            session.commit()
            return True

    # ---- node descriptors ----

    def list_node_descriptors(self) -> List[NodeDescriptorOut]:
        with self._session_local() as session:
            rows = session.scalars(
                select(NodeDescriptor).order_by(NodeDescriptor.node_type.asc())
            ).all()
            return [self._descriptor_to_out(r) for r in rows]

    def get_node_descriptor(self, node_type: str) -> Optional[NodeDescriptorOut]:
        with self._session_local() as session:
            row = session.scalars(
                select(NodeDescriptor).where(NodeDescriptor.node_type == node_type)
            ).first()
            return self._descriptor_to_out(row) if row else None

    def upsert_node_descriptor(
        self,
        node_type: str,
        node_name: str = "",
        category: str = "",
        schema_json: str = "{}",
        is_builtin: bool = False,
    ) -> NodeDescriptorOut:
        """插入或更新节点描述。"""
        with self._session_local() as session:
            row = session.scalars(
                select(NodeDescriptor).where(NodeDescriptor.node_type == node_type)
            ).first()
            if row is None:
                row = NodeDescriptor(
                    node_type=node_type,
                    node_name=node_name,
                    category=category,
                    schema_json=schema_json,
                    is_builtin=is_builtin,
                )
                session.add(row)
            else:
                row.node_name = node_name
                row.category = category
                row.schema_json = schema_json
                row.is_builtin = is_builtin
            try:
                session.commit()
            except IntegrityError as e:
                session.rollback()
                raise ValueError(f"节点描述 {node_type} 已存在") from e
            session.refresh(row)
            return self._descriptor_to_out(row)


    # ---- copilot threads ----

    def upsert_copilot_thread(
        self,
        thread_id: str,
        flow_id: str,
        version: str = "",
        title: str = "",
        bump_message: bool = False,
    ) -> None:
        """记录/更新 Copilot 会话与流程的关联（不存在则创建）。"""
        from datetime import datetime

        with self._session_local() as session:  # type: Session
            record = session.scalars(
                select(CopilotThread).where(CopilotThread.thread_id == thread_id)
            ).first()
            if record is None:
                record = CopilotThread(
                    thread_id=thread_id, flow_id=flow_id, version=version, title=title
                )
                session.add(record)
            else:
                record.flow_id = flow_id or record.flow_id
                if version:
                    record.version = version
                if title:
                    record.title = title
            if bump_message:
                record.message_count = (record.message_count or 0) + 1
            record.updated_at = datetime.utcnow()
            session.commit()

    def list_copilot_threads(self, flow_id: str) -> List[Dict]:
        """列出某流程的 Copilot 会话（最近更新优先）。"""
        with self._session_local() as session:  # type: Session
            records = session.scalars(
                select(CopilotThread)
                .where(CopilotThread.flow_id == flow_id)
                .order_by(CopilotThread.updated_at.desc())
            ).all()
            return [
                {
                    "thread_id": r.thread_id,
                    "flow_id": r.flow_id,
                    "version": r.version,
                    "title": r.title,
                    "message_count": r.message_count,
                    "updated_at": r.updated_at.isoformat() if r.updated_at else None,
                }
                for r in records
            ]

    def delete_node_descriptor(self, node_type: str) -> bool:
        with self._session_local() as session:
            row = session.scalars(
                select(NodeDescriptor).where(NodeDescriptor.node_type == node_type)
            ).first()
            if row is None:
                raise LookupError(f"节点描述不存在: {node_type}")
            session.delete(row)
            session.commit()
            return True

    @staticmethod
    def _descriptor_to_out(row: NodeDescriptor) -> NodeDescriptorOut:
        return NodeDescriptorOut(
            node_type=row.node_type,
            node_name=row.node_name,
            category=row.category,
            schema_json=row.schema_json,
            is_builtin=row.is_builtin,
        )


# ============ 本地单机模式执行记录 ============

def insert_local_execution(
    execution_id: str,
    flow_id: str,
    flow_version: str,
    status: str = "running",
    input_json: str = "{}",
    invoker: str = "local",
) -> None:
    """新建本地执行记录。"""
    store = get_flow_store()
    with store._session_local() as session:
        session.add(
            LocalExecution(
                execution_id=execution_id,
                flow_id=flow_id,
                flow_version=flow_version,
                status=status,
                input_json=input_json,
                invoker=invoker,
            )
        )
        session.commit()


def update_local_execution(execution_id: str, **fields: str) -> None:
    """部分更新（nodes_json 等）。"""
    store = get_flow_store()
    with store._session_local() as session:
        row = session.scalars(
            select(LocalExecution).where(LocalExecution.execution_id == execution_id)
        ).first()
        if row is None:
            return
        for key, value in fields.items():
            setattr(row, key, value)
        session.commit()


def update_local_execution_status_if(
    execution_id: str, expected_status: str, new_status: str
) -> bool:
    """条件状态推进：``UPDATE ... WHERE status=<expected>``，返回是否命中。

    原子条件更新，用于消除"读状态→判断→写新状态"的 TOCTOU 窗口（如
    resume：两个并发请求都读到 suspended、都拉起执行线程）。命中行数为
    0 时（记录不存在或状态已非 expected）返回 False。
    """
    store = get_flow_store()
    with store._session_local() as session:
        result = session.execute(
            update(LocalExecution)
            .where(
                LocalExecution.execution_id == execution_id,
                LocalExecution.status == expected_status,
            )
            .values(status=new_status)
        )
        session.commit()
        return bool(result.rowcount)


def finish_local_execution(
    execution_id: str,
    status: str,
    output_json: Optional[str] = None,
    error_json: Optional[str] = None,
    context_json: Optional[str] = None,
) -> None:
    """终结一条本地执行记录。"""
    update_local_execution(
        execution_id,
        status=status,
        end_time=datetime.utcnow(),
        **({"output_json": output_json} if output_json is not None else {}),
        **({"error_json": error_json} if error_json is not None else {}),
        **({"context_json": context_json} if context_json is not None else {}),
    )


def _local_row_to_dict(row: LocalExecution) -> dict:
    return {
        "execution_id": row.execution_id,
        "flow_id": row.flow_id,
        "flow_version": row.flow_version,
        "status": row.status,
        "start_time": row.start_time.isoformat() if row.start_time else None,
        "end_time": row.end_time.isoformat() if row.end_time else None,
        "last_update_time": row.last_update_time.isoformat() if row.last_update_time else None,
        "context": _loads_or_none(getattr(row, "context_json", None)),
        "error": _loads_or_none(row.error_json),
        "invoker": row.invoker,
        "nodes": _loads_or_none(row.nodes_json) or [],
        "input": _loads_or_none(row.input_json) or {},
        "output": _loads_or_none(row.output_json),
    }


def _loads_or_none(text: Optional[str]) -> Any:
    if not text:
        return None
    try:
        return json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return None


def get_local_execution(execution_id: str) -> Optional[dict]:
    """取本地执行详情（ExecutionInfo 兼容结构 + nodes/input/output）。"""
    store = get_flow_store()
    with store._session_local() as session:
        row = session.scalars(
            select(LocalExecution).where(LocalExecution.execution_id == execution_id)
        ).first()
        return _local_row_to_dict(row) if row else None


def list_local_executions() -> List[dict]:
    store = get_flow_store()
    with store._session_local() as session:
        rows = session.scalars(select(LocalExecution)).all()
        return [_local_row_to_dict(r) for r in rows]


def delete_local_execution(execution_id: str) -> bool:
    store = get_flow_store()
    with store._session_local() as session:
        row = session.scalars(
            select(LocalExecution).where(LocalExecution.execution_id == execution_id)
        ).first()
        if row is None:
            return False
        session.delete(row)
        session.commit()
        return True


# ============ 初始化辅助 ============

def init_engine(db_url: str) -> Engine:
    """创建/替换全局引擎并建表。返回引擎实例。"""
    global _engine, _SessionLocal
    _engine = create_engine(db_url, future=True)
    _SessionLocal = sessionmaker(bind=_engine, expire_on_commit=False)
    create_all()
    logger.info("FlowStore 引擎已初始化: %s", db_url)
    return _engine


def create_all() -> None:
    """在当前引擎上创建所有表。"""
    if _engine is None:
        raise RuntimeError("引擎未初始化，请先调用 init_engine()")
    Base.metadata.create_all(_engine)
    _migrate_sqlite_columns()


def _migrate_sqlite_columns() -> None:
    """轻量迁移：旧 SQLite 库补新增列（仅 ADD COLUMN，保守策略）。"""
    if _engine is None:
        return
    from sqlalchemy import text as _text

    wanted = {
        "local_executions": {"context_json": "TEXT NOT NULL DEFAULT 'null'"},
    }
    with _engine.begin() as conn:
        for table, columns in wanted.items():
            rows = conn.execute(_text(f"PRAGMA table_info({table})")).fetchall()
            existing = {r[1] for r in rows}
            if not existing:
                continue  # 新库由 create_all 直接建全
            for col, ddl in columns.items():
                if col not in existing:
                    conn.execute(_text(f"ALTER TABLE {table} ADD COLUMN {col} {ddl}"))


def get_init_engine() -> Engine:
    """获取已初始化的引擎（未初始化则抛错）。"""
    if _engine is None:
        raise RuntimeError("引擎未初始化，请先调用 init_engine()")
    return _engine


def get_flow_store() -> FlowStore:
    """获取全局 FlowStore 实例。"""
    if _SessionLocal is None:
        raise RuntimeError("FlowStore 未初始化，请先调用 init_engine()")
    return FlowStore(_SessionLocal)


def parse_layout(layout: str) -> dict:
    """解析 layout JSON 字符串为 dict（容错）。"""
    if not layout:
        return {}
    try:
        return json.loads(layout)
    except (json.JSONDecodeError, TypeError):
        return {}


# ============ 本地档调度 / 触发历史 / 日志 ============

def insert_local_log(execution_id: str, level: str, logger_name: str, message: str) -> None:
    """写入一条本地执行日志（由 _ThreadLogHandler 高频调用，单行提交可接受）。"""
    store = get_flow_store()
    with store._session_local() as session:
        session.add(
            LocalLog(
                execution_id=execution_id,
                level=level,
                logger=logger_name,
                message=message,
            )
        )
        session.commit()


def list_local_logs(level: str = None, execution_id: str = None, limit: int = 200) -> list:
    store = get_flow_store()
    with store._session_local() as session:
        query = select(LocalLog).order_by(LocalLog.ts.desc()).limit(max(1, min(1000, limit)))
        if level:
            query = query.where(LocalLog.level == level)
        if execution_id:
            query = query.where(LocalLog.execution_id == execution_id)
        return [
            {
                "timestamp": r.ts.isoformat() if r.ts else "",
                "level": r.level,
                "service_type": "local-console",
                "instance_id": r.execution_id or "",
                "message": r.message,
                "logger": r.logger,
            }
            for r in session.scalars(query).all()
        ]


def local_log_stats(limit: int = 1000) -> dict:
    rows = list_local_logs(limit=limit)
    stats: dict = {}
    total = len(rows)
    for r in rows:
        service = r["service_type"]
        level = r["level"]
        stats.setdefault(service, {"levels": {}, "total": 0})
        stats[service]["levels"][level] = stats[service]["levels"].get(level, 0) + 1
        stats[service]["total"] += 1
    return {"stats": stats, "total": total}
