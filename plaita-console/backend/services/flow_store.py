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
from sqlalchemy import create_engine, select, text, update
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

try:
    from ..models.flow import (
    Base,
    CopilotThread,
    DEFAULT_TENANT_ID,
    FlowRecord,
    FlowVersion,
    LocalExecution,
    LocalLog,
    LocalSchedule,
    LocalScheduleFire,
    NodeDescriptor,
    PropertyType,
    Tenant,
    TenantMember,
    User,
)
except ImportError:
    from models.flow import (  # type: ignore
    Base,
    CopilotThread,
    DEFAULT_TENANT_ID,
    FlowRecord,
    FlowVersion,
    LocalExecution,
    LocalLog,
    LocalSchedule,
    LocalScheduleFire,
    NodeDescriptor,
    PropertyType,
    Tenant,
    TenantMember,
    User,
)

logger = logging.getLogger(__name__)


# ============ Pydantic I/O 模型 ============

class FlowSummary(BaseModel):
    """流程摘要（列表项）"""

    flow_id: str
    tenant_id: str = ""
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
    # 代码位置（2026-09 节点管理重设计）：仅内置节点有——Python 模块路径与类名，
    # 供控制台展示「该节点在哪实现」；控制台注册的自定义节点是纯编排元数据，
    # 没有可执行代码，留空。
    source_module: str = ""
    source_class: str = ""


class PropertyTypeOut(BaseModel):
    """自定义属性类型（命名别名）"""

    name: str
    base_type: str = "string"
    enum_options: List[Any] = Field(default_factory=list)
    default_value: Optional[Any] = None
    desc: str = ""


# ============ 服务 ============

_engine: Optional[Engine] = None
_SessionLocal: Optional[sessionmaker] = None


class FlowStore:
    """flow / flow_version CRUD 服务"""

    def __init__(self, session_local: sessionmaker):
        self._session_local = session_local

    # ---- flow ----

    def ensure_flow(
        self, flow_id: str, author: str = "", desc: str = "", tenant_id: str = ""
    ) -> FlowRecord:
        """确保 flow 记录存在，不存在则创建。返回 ORM 记录。"""
        with self._session_local() as session:  # type: Session
            record = session.scalars(
                select(FlowRecord).where(
                    FlowRecord.tenant_id == tenant_id, FlowRecord.flow_id == flow_id
                )
            ).first()
            if record is None:
                record = FlowRecord(
                    tenant_id=tenant_id, flow_id=flow_id, author=author, desc=desc
                )
                session.add(record)
                session.commit()
                session.refresh(record)
            return record

    @staticmethod
    def _tenant_filter(model, tenant_id: Optional[str]):
        """租户过滤条件：None=平台全量（跨租户读），str=限定租户。"""
        if tenant_id is None:
            return True
        return model.tenant_id == tenant_id

    def list_flows(self, tenant_id: Optional[str] = None) -> List[FlowSummary]:
        with self._session_local() as session:
            rows = session.scalars(
                select(FlowRecord)
                .where(self._tenant_filter(FlowRecord, tenant_id))
                .order_by(FlowRecord.updated_at.desc())
            ).all()
            return [
                FlowSummary(
                    flow_id=r.flow_id,
                    tenant_id=r.tenant_id,
                    author=r.author,
                    desc=r.desc,
                    created_at=r.created_at,
                    updated_at=r.updated_at,
                )
                for r in rows
            ]

    def get_flow_record(
        self, flow_id: str, tenant_id: Optional[str] = None
    ) -> Optional[FlowRecord]:
        with self._session_local() as session:
            return session.scalars(
                select(FlowRecord).where(
                    self._tenant_filter(FlowRecord, tenant_id),
                    FlowRecord.flow_id == flow_id,
                )
            ).first()

    def create_flow(
        self, flow_id: str, author: str = "", desc: str = "", tenant_id: str = ""
    ) -> FlowRecord:
        """新建 flow 记录（租户内唯一），已存在则抛 ValueError。"""
        with self._session_local() as session:
            existing = session.scalars(
                select(FlowRecord).where(
                    FlowRecord.tenant_id == tenant_id, FlowRecord.flow_id == flow_id
                )
            ).first()
            if existing is not None:
                raise ValueError(f"流程已存在: {flow_id}")
            record = FlowRecord(
                tenant_id=tenant_id, flow_id=flow_id, author=author, desc=desc
            )
            session.add(record)
            try:
                session.commit()
            except IntegrityError as e:
                session.rollback()
                raise ValueError(f"流程已存在: {flow_id}") from e
            session.refresh(record)
            return record

    def delete_flow(self, flow_id: str, tenant_id: Optional[str] = None) -> bool:
        """删除 flow 及其全部版本（级联）。不存在抛 LookupError。"""
        with self._session_local() as session:
            record = session.scalars(
                select(FlowRecord).where(
                    self._tenant_filter(FlowRecord, tenant_id),
                    FlowRecord.flow_id == flow_id,
                )
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
        tenant_id: str = "",
    ) -> SaveFlowDefinitionResult:
        """保存（草稿）版本。已存在的 published 版本不可覆盖。"""
        self.ensure_flow(flow_id, tenant_id=tenant_id)
        with self._session_local() as session:
            existing = session.scalars(
                select(FlowVersion).where(
                    FlowVersion.tenant_id == tenant_id,
                    FlowVersion.flow_id == flow_id,
                    FlowVersion.version == version,
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
                tenant_id=tenant_id,
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

    def get_version(
        self, flow_id: str, version: str, tenant_id: Optional[str] = None
    ) -> Optional[FlowVersionOut]:
        with self._session_local() as session:
            row = session.scalars(
                select(FlowVersion).where(
                    self._tenant_filter(FlowVersion, tenant_id),
                    FlowVersion.flow_id == flow_id,
                    FlowVersion.version == version,
                )
            ).first()
            if row is None:
                return None
            return self._version_to_out(row)

    def list_versions(
        self, flow_id: str, tenant_id: Optional[str] = None
    ) -> List[FlowVersionOut]:
        with self._session_local() as session:
            rows = session.scalars(
                select(FlowVersion)
                .where(
                    self._tenant_filter(FlowVersion, tenant_id),
                    FlowVersion.flow_id == flow_id,
                )
                .order_by(FlowVersion.created_at.asc())
            ).all()
            return [self._version_to_out(r) for r in rows]

    def publish_version(
        self, flow_id: str, version: str, tenant_id: Optional[str] = None
    ) -> FlowVersionOut:
        """发布版本：draft → published。已发布则幂等返回。不存在则 LookupError。"""
        with self._session_local() as session:
            row = session.scalars(
                select(FlowVersion).where(
                    self._tenant_filter(FlowVersion, tenant_id),
                    FlowVersion.flow_id == flow_id,
                    FlowVersion.version == version,
                )
            ).first()
            if row is None:
                raise LookupError(f"版本不存在: {flow_id}@{version}")
            if row.status != "published":
                row.status = "published"
                row.published_at = datetime.utcnow()
                session.commit()
                session.refresh(row)
            return self._version_to_out(row)

    def delete_version(
        self, flow_id: str, version: str, tenant_id: Optional[str] = None
    ) -> bool:
        with self._session_local() as session:
            row = session.scalars(
                select(FlowVersion).where(
                    self._tenant_filter(FlowVersion, tenant_id),
                    FlowVersion.flow_id == flow_id,
                    FlowVersion.version == version,
                )
            ).first()
            if row is None:
                raise LookupError(f"版本不存在: {flow_id}@{version}")
            session.delete(row)
            session.commit()
            return True

    @staticmethod
    def _version_to_out(row: FlowVersion) -> FlowVersionOut:
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

    # ---- node descriptors ----

    def list_node_descriptors(self, tenant_id: Optional[str] = None) -> List[NodeDescriptorOut]:
        with self._session_local() as session:
            rows = session.scalars(
                select(NodeDescriptor)
                .where(self._tenant_filter(NodeDescriptor, tenant_id))
                .order_by(NodeDescriptor.node_type.asc())
            ).all()
            return [self._descriptor_to_out(r) for r in rows]

    def get_node_descriptor(
        self, node_type: str, tenant_id: Optional[str] = None
    ) -> Optional[NodeDescriptorOut]:
        with self._session_local() as session:
            row = session.scalars(
                select(NodeDescriptor).where(
                    self._tenant_filter(NodeDescriptor, tenant_id),
                    NodeDescriptor.node_type == node_type,
                )
            ).first()
            return self._descriptor_to_out(row) if row else None

    def upsert_node_descriptor(
        self,
        node_type: str,
        node_name: str = "",
        category: str = "",
        schema_json: str = "{}",
        is_builtin: bool = False,
        tenant_id: str = "",
    ) -> NodeDescriptorOut:
        """插入或更新租户内节点描述。"""
        with self._session_local() as session:
            row = session.scalars(
                select(NodeDescriptor).where(
                    NodeDescriptor.tenant_id == tenant_id,
                    NodeDescriptor.node_type == node_type,
                )
            ).first()
            if row is None:
                row = NodeDescriptor(
                    tenant_id=tenant_id,
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
        tenant_id: str = "",
    ) -> None:
        """记录/更新 Copilot 会话与流程的关联（不存在则创建）。"""
        from datetime import datetime

        with self._session_local() as session:  # type: Session
            record = session.scalars(
                select(CopilotThread).where(CopilotThread.thread_id == thread_id)
            ).first()
            if record is None:
                record = CopilotThread(
                    tenant_id=tenant_id,
                    thread_id=thread_id,
                    flow_id=flow_id,
                    version=version,
                    title=title,
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

    def list_copilot_threads(
        self, flow_id: str, tenant_id: Optional[str] = None
    ) -> List[Dict]:
        """列出某流程的 Copilot 会话（最近更新优先）。"""
        with self._session_local() as session:  # type: Session
            records = session.scalars(
                select(CopilotThread)
                .where(
                    self._tenant_filter(CopilotThread, tenant_id),
                    CopilotThread.flow_id == flow_id,
                )
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

    def delete_node_descriptor(
        self, node_type: str, tenant_id: Optional[str] = None
    ) -> bool:
        with self._session_local() as session:
            row = session.scalars(
                select(NodeDescriptor).where(
                    self._tenant_filter(NodeDescriptor, tenant_id),
                    NodeDescriptor.node_type == node_type,
                )
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

    # ---- property types（自定义属性类型，2026-09 节点管理重设计）----

    def list_property_types(self, tenant_id: Optional[str] = None) -> List[PropertyTypeOut]:
        with self._session_local() as session:
            rows = session.scalars(
                select(PropertyType)
                .where(self._tenant_filter(PropertyType, tenant_id))
                .order_by(PropertyType.name.asc())
            ).all()
            return [self._property_type_to_out(r) for r in rows]

    def upsert_property_type(
        self,
        name: str,
        base_type: str,
        enum_json: str = "[]",
        default_json: str = "null",
        desc: str = "",
        tenant_id: str = "",
    ) -> PropertyTypeOut:
        """插入或更新租户内自定义属性类型。"""
        with self._session_local() as session:
            row = session.scalars(
                select(PropertyType).where(
                    PropertyType.tenant_id == tenant_id, PropertyType.name == name
                )
            ).first()
            if row is None:
                row = PropertyType(
                    tenant_id=tenant_id,
                    name=name,
                    base_type=base_type,
                    enum_json=enum_json,
                    default_json=default_json,
                    desc=desc,
                )
                session.add(row)
            else:
                row.base_type = base_type
                row.enum_json = enum_json
                row.default_json = default_json
                row.desc = desc
            session.commit()
            session.refresh(row)
            return self._property_type_to_out(row)

    def delete_property_type(self, name: str, tenant_id: Optional[str] = None) -> bool:
        with self._session_local() as session:
            row = session.scalars(
                select(PropertyType).where(
                    self._tenant_filter(PropertyType, tenant_id),
                    PropertyType.name == name,
                )
            ).first()
            if row is None:
                raise LookupError(f"属性类型不存在: {name}")
            session.delete(row)
            session.commit()
            return True

    @staticmethod
    def _property_type_to_out(row: PropertyType) -> PropertyTypeOut:
        import json as _json

        def _load(text: str, fallback):
            try:
                return _json.loads(text)
            except (json.JSONDecodeError, TypeError):
                return fallback

        return PropertyTypeOut(
            name=row.name,
            base_type=row.base_type,
            enum_options=_load(row.enum_json, []),
            default_value=_load(row.default_json, None),
            desc=row.desc,
        )


# ============ 本地单机模式执行记录 ============

def insert_local_execution(
    execution_id: str,
    flow_id: str,
    flow_version: str,
    status: str = "running",
    input_json: str = "{}",
    invoker: str = "local",
    tenant_id: str = "",
) -> None:
    """新建本地执行记录。"""
    store = get_flow_store()
    with store._session_local() as session:
        session.add(
            LocalExecution(
                tenant_id=tenant_id,
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
        "tenant_id": getattr(row, "tenant_id", "") or "",
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


def get_local_execution(
    execution_id: str, tenant_id: Optional[str] = None
) -> Optional[dict]:
    """取本地执行详情（ExecutionInfo 兼容结构 + nodes/input/output）。"""
    store = get_flow_store()
    with store._session_local() as session:
        row = session.scalars(
            select(LocalExecution).where(
                FlowStore._tenant_filter(LocalExecution, tenant_id),
                LocalExecution.execution_id == execution_id,
            )
        ).first()
        return _local_row_to_dict(row) if row else None


def list_local_executions(tenant_id: Optional[str] = None) -> List[dict]:
    store = get_flow_store()
    with store._session_local() as session:
        rows = session.scalars(
            select(LocalExecution).where(
                FlowStore._tenant_filter(LocalExecution, tenant_id)
            )
        ).all()
        return [_local_row_to_dict(r) for r in rows]


def delete_local_execution(
    execution_id: str, tenant_id: Optional[str] = None
) -> bool:
    store = get_flow_store()
    with store._session_local() as session:
        row = session.scalars(
            select(LocalExecution).where(
                FlowStore._tenant_filter(LocalExecution, tenant_id),
                LocalExecution.execution_id == execution_id,
            )
        ).first()
        if row is None:
            return False
        session.delete(row)
        session.commit()
        return True


# ============ 初始化辅助 ============

# 需要补 tenant_id 列的表（唯一约束无需变更，ADD COLUMN 即可）
_TENANT_ADDCOLUMN_TABLES = (
    "audit_logs",
    "copilot_threads",
    "deployments",
    "local_executions",
    "local_logs",
    "local_schedules",
)
# 唯一约束必须改为「租户内唯一」的表：SQLite 改约束要重建表
# （备份 → 删 → create_all 重建新 schema → 回填数据 → 删备份）
_TENANT_REBUILD_TABLES = (
    "credentials",
    "flow_versions",
    "flows",
    "node_descriptors",
    "property_types",
)


def init_engine(db_url: str) -> Engine:
    """创建/替换全局引擎并建表。返回引擎实例。"""
    global _engine, _SessionLocal
    _engine = create_engine(db_url, future=True)
    _SessionLocal = sessionmaker(bind=_engine, expire_on_commit=False)
    create_all()
    logger.info("FlowStore 引擎已初始化: %s", db_url)
    return _engine


def create_all() -> None:
    """在当前引擎上创建所有表（含多租户 schema 迁移）。"""
    if _engine is None:
        raise RuntimeError("引擎未初始化，请先调用 init_engine()")
    _migrate_tenant_schema()
    Base.metadata.create_all(_engine)
    _migrate_sqlite_columns()


def _sqlite_columns(conn, table: str) -> list[str]:
    from sqlalchemy import text as _text

    return [r[1] for r in conn.execute(_text(f"PRAGMA table_info({table})")).fetchall()]


def _migrate_tenant_schema() -> None:
    """多租户 schema 迁移（仅 SQLite；幂等，以 tenant_id 列存在与否为判据）。

    - 5 张全局唯一约束表：旧形态（无 tenant_id 列）→ 备份/删除，交由
      create_all 按新 schema（租户内唯一）重建后回填；
    - 其余表 + users/session_tokens：直接 ADD COLUMN。
    """
    if _engine is None or _engine.url.get_backend_name() != "sqlite":
        return
    from sqlalchemy import text as _text

    rebuild: list[str] = []
    with _engine.begin() as conn:
        for table in _TENANT_REBUILD_TABLES:
            cols = _sqlite_columns(conn, table)
            if not cols or "tenant_id" in cols:
                continue  # 新库由 create_all 直接建全；或已迁移
            conn.execute(_text(f"CREATE TABLE {table}_mig_old AS SELECT * FROM {table}"))
            conn.execute(_text(f"DROP TABLE {table}"))
            rebuild.append(table)
        for table in _TENANT_ADDCOLUMN_TABLES:
            cols = _sqlite_columns(conn, table)
            if not cols or "tenant_id" in cols:
                continue
            conn.execute(_text(f"ALTER TABLE {table} ADD COLUMN tenant_id TEXT NOT NULL DEFAULT ''"))
        for table, col, ddl in (
            ("users", "platform_admin", "BOOLEAN NOT NULL DEFAULT 0"),
            ("session_tokens", "active_tenant", "TEXT NULL"),
        ):
            cols = _sqlite_columns(conn, table)
            if cols and col not in cols:
                conn.execute(_text(f"ALTER TABLE {table} ADD COLUMN {col} {ddl}"))
        if rebuild:
            # 重建出的表由 create_all 补齐索引/约束
            Base.metadata.create_all(_engine)
            for table in rebuild:
                old = f"{table}_mig_old"
                cols = _sqlite_columns(conn, old)
                col_list = ", ".join(cols)
                conn.execute(
                    _text(
                        f"INSERT INTO {table} ({col_list}, tenant_id) "
                        f"SELECT {col_list}, '{DEFAULT_TENANT_ID}' FROM {old}"
                    )
                )
                conn.execute(_text(f"DROP TABLE {old}"))
    if rebuild:
        logger.info("多租户迁移：重建表 %s，存量数据归入租户 '%s'", ",".join(rebuild), DEFAULT_TENANT_ID)


def ensure_tenant_bootstrap() -> None:
    """多租户启动引导（幂等，多租户首次启动时把存量数据收编进 default 租户）。

    - default 租户不存在则创建；
    - 各租户域表中 tenant_id='' 的存量行归入 default；
    - 无任何 membership 的非平台管理员补 membership(default, users.role)；
    - 首次引导（tenants 表为空）时，既有全局 admin 提升为 platform_admin。
    """
    if _engine is None:
        raise RuntimeError("引擎未初始化，请先调用 init_engine()")
    with _SessionLocal() as session:  # type: Session
        first_boot = session.query(Tenant).count() == 0
        if first_boot:
            session.add(Tenant(id=DEFAULT_TENANT_ID, name="默认租户", status="active"))
        elif session.query(Tenant).filter(Tenant.id == DEFAULT_TENANT_ID).count() == 0:
            session.add(Tenant(id=DEFAULT_TENANT_ID, name="默认租户", status="active"))

        with _engine.begin() as conn:
            for table in _TENANT_ADDCOLUMN_TABLES + _TENANT_REBUILD_TABLES:
                cols = _sqlite_columns(conn, table)
                if cols and "tenant_id" in cols:
                    conn.execute(
                        text(
                            f"UPDATE {table} SET tenant_id = '{DEFAULT_TENANT_ID}' "
                            f"WHERE tenant_id = ''"
                        )
                    )

        # 成员回填（先于平台管理员提升：legacy admin 也要拿到 default 成员资格）
        users = session.query(User).all()
        existing = {
            m.username
            for m in session.query(TenantMember).filter(TenantMember.tenant_id == DEFAULT_TENANT_ID)
        }
        for user in users:
            if user.platform_admin or user.username in existing:
                continue
            session.add(
                TenantMember(
                    username=user.username, tenant_id=DEFAULT_TENANT_ID, role=user.role
                )
            )
        if first_boot:
            # legacy 全局 admin 提升为平台管理员（首次多租户启动一次性执行）
            session.query(User).filter(User.role == "admin").update(
                {User.platform_admin: True}
            )
        session.commit()


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

def insert_local_log(
    execution_id: str,
    level: str,
    logger_name: str,
    message: str,
    tenant_id: str = "",
) -> None:
    """写入一条本地执行日志（由 _ThreadLogHandler 高频调用，单行提交可接受）。"""
    store = get_flow_store()
    with store._session_local() as session:
        session.add(
            LocalLog(
                tenant_id=tenant_id,
                execution_id=execution_id,
                level=level,
                logger=logger_name,
                message=message,
            )
        )
        session.commit()


def list_local_logs(
    level: str = None,
    execution_id: str = None,
    limit: int = 200,
    tenant_id: Optional[str] = None,
) -> list:
    store = get_flow_store()
    with store._session_local() as session:
        query = (
            select(LocalLog)
            .where(FlowStore._tenant_filter(LocalLog, tenant_id))
            .order_by(LocalLog.ts.desc())
            .limit(max(1, min(1000, limit)))
        )
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


def local_log_stats(limit: int = 1000, tenant_id: Optional[str] = None) -> dict:
    rows = list_local_logs(limit=limit, tenant_id=tenant_id)
    stats: dict = {}
    total = len(rows)
    for r in rows:
        service = r["service_type"]
        level = r["level"]
        stats.setdefault(service, {"levels": {}, "total": 0})
        stats[service]["levels"][level] = stats[service]["levels"].get(level, 0) + 1
        stats[service]["total"] += 1
    return {"stats": stats, "total": total}
