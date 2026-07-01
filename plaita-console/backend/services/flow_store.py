"""
流程定义存储服务

基于 SQLAlchemy 的 flow / flow_version 读写（node_descriptor 的 CRUD 留给节点管理服务，
本层只提供表初始化支持）。同步引擎 + 同步 Session：sqlite 本地访问延迟极低，
API 层（M3）可通过 ``run_in_executor`` 调用，避免引入 aiosqlite 额外依赖。
"""
import json
import logging
from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field
from sqlalchemy import create_engine, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

try:
    from ..models.flow import Base, FlowRecord, FlowVersion, NodeDescriptor
except ImportError:
    from models.flow import Base, FlowRecord, FlowVersion, NodeDescriptor

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

    node_type: str
    node_name: str = ""
    category: str = ""
    schema_json: str = "{}"
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
