"""
流程定义存储服务

基于 SQLAlchemy 的 flow / flow_version 读写（node_descriptor 的 CRUD 留给节点管理服务，
本层只提供表初始化支持）。同步引擎 + 同步 Session：sqlite 本地访问延迟极低，
API 层（M3）可通过 ``run_in_executor`` 调用，避免引入 aiosqlite 额外依赖。
"""
import json
import logging
import os
import re
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import (
    Column, DateTime, create_engine, delete, select, text, tuple_, update,
)
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.exc import DBAPIError, IntegrityError, InterfaceError, OperationalError
from sqlalchemy.orm import Session, sessionmaker

from plaita.core.url_redaction import _redact_env_value

try:
    from ..models.flow import (
    Base,
    CopilotThread,
    Credential,
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
    from .tenant_write_guard import assert_tenant_generation
except ImportError:
    from models.flow import (  # type: ignore
    Base,
    CopilotThread,
    Credential,
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
    from services.tenant_write_guard import assert_tenant_generation  # type: ignore

logger = logging.getLogger(__name__)


# ---- C5-1 乐观并发：flow_versions.updated_at ----
# models/flow.py 的 FlowVersion 未声明 updated_at（历史遗留：草稿更新时间无处
# 可查，保存是 last-write-wins）。本服务层把列追加到映射表的 Table 元数据上：
# 新库由 create_all 直接建出，旧库由 _migrate_sqlite_columns 补列（ADD COLUMN，
# 幂等）。列刻意不走 ORM 映射（mapper 已在类定义时构建，后补列不进
# column_attrs），读写一律经下方 _touch_updated_at/_updated_at_map 原生 SQL。
# 旧库缺列时原生 SQL 报错 → 按无并发信息处理（退化为原行为，不阻塞保存）。
if "updated_at" not in FlowVersion.__table__.c:
    FlowVersion.__table__.append_column(Column("updated_at", DateTime, nullable=True))


def _dt_to_iso(value: Any) -> str:
    """updated_at 原生 SQL 取回值 → 标准 ISO 串（SQLite 裸查返回 str，需归一）。"""
    if isinstance(value, datetime):
        return value.isoformat()
    try:
        return datetime.fromisoformat(str(value)).isoformat()
    except ValueError:
        return str(value)


def _parse_updated_at(value: Any) -> Optional[datetime]:
    """客户端回传的乐观锁基准串 → datetime（CAS 谓词用）；解析失败返回 None。

    基准以 datetime 绑定进条件 UPDATE，由驱动按列类型归一（SQLite 存储格式
    与 PG timestamp 均正确比对），避免「ISO 串 vs 库内格式」的表面差异误判。
    """
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def _is_unique_violation(exc: IntegrityError) -> bool:
    """判断 IntegrityError 是否为**唯一约束冲突**（而非主键冲突等其它完整性错误）。

    背景：迁移到 PostgreSQL 后若自增序列未重置（``setval`` 漏做），首次 INSERT
    会撞**主键**唯一约束。旧代码把任何 IntegrityError 都翻译成「已存在」，
    把「序列没推进」这种基础设施故障误报为「数据已存在」——误导排查。
    此处按驱动错误标识区分：

    - psycopg/psycopg2（PG）：SQLSTATE 23505 = unique_violation；
    - sqlite3：异常文本含 ``UNIQUE constraint failed``。

    无法判定的情形保守返回 False（保留原始错误信息，宁可信息多不可误导）。
    """
    orig = getattr(exc, "orig", None)
    # psycopg / psycopg2：有 sqlstate 属性
    sqlstate = getattr(orig, "sqlstate", None) or getattr(orig, "pgcode", None)
    if sqlstate is not None:
        if str(sqlstate) != "23505":
            return False
        # PG 把**主键冲突**与**唯一约束冲突**都报成 23505，必须再按约束名区分：
        #   - 主键约束名由 PG 自动命名为 ``<table>_pkey``（或自定义名含 pkey 语义）；
        #   - 业务唯一约束在本模型里显式命名（如 ``uq_tenant_flow`` / ``uq_tenant_flow_version``）。
        # 主键冲突 = 基础设施故障（典型：迁移后序列未 setval），不是「数据已存在」。
        constraint = None
        diag = getattr(orig, "diag", None)
        if diag is not None:
            constraint = getattr(diag, "constraint_name", None)
        if constraint:
            return not str(constraint).endswith("_pkey")
        return True
    # sqlite3 及兜底：看异常文本
    text_ = str(orig or exc).lower()
    return "unique constraint failed" in text_


class VersionConflictError(ValueError):
    """草稿乐观锁冲突：base_updated_at 与服务端不一致（C5-1）。

    继承 ValueError 以兼容既有「ValueError → HTTP 409」映射；
    ``latest_updated_at`` 供 API 层组装结构化错误体（前端据此提示加载最新）。
    """

    def __init__(self, message: str, latest_updated_at: Optional[str] = None):
        super().__init__(message)
        self.latest_updated_at = latest_updated_at


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
    # C5-1：草稿最近一次保存时间，前端保存时作为乐观锁基准（base_updated_at）回传
    updated_at: Optional[datetime] = None


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
        # flow_versions.updated_at 列能力探测缓存（None=未探测）
        self._updated_at_ok: Optional[bool] = None

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
                # A51-02：flush 取得写串行化点后、同一事务内复核请求世代。
                session.flush()
                assert_tenant_generation(session, tenant_id)
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
                session.flush()
                assert_tenant_generation(session, tenant_id)
                session.commit()
            except IntegrityError as e:
                session.rollback()
                # 唯一约束冲突才是「已存在」；其它 IntegrityError（如序列未重置
                # 导致的主键冲突）保留原始信息，避免误导排查。
                if _is_unique_violation(e):
                    raise ValueError(f"流程已存在: {flow_id}") from e
                raise ValueError(
                    f"创建流程 {flow_id} 失败（数据库完整性错误）: {e.orig or e}"
                ) from e
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
            session.flush()
            assert_tenant_generation(session, tenant_id)
            session.commit()
            return True

    # ---- version ----

    @staticmethod
    def _next_semver_of(versions: List[str]) -> str:
        """版本号列表中的下一个 patch 号（无合法 semver 时从 0.0.1 起步）。"""
        best: tuple = (0, 0, 0)
        for v in versions:
            m = re.match(r"^(\d+)\.(\d+)\.(\d+)$", v or "")
            if not m:
                continue
            t = (int(m.group(1)), int(m.group(2)), int(m.group(3)))
            if t > best:
                best = t
        return f"{best[0]}.{best[1]}.{best[2] + 1}"

    def save_flow_definition(
        self,
        flow_id: str,
        version: str,
        definition: str,
        layout: str = "",
        status: str = "draft",
        created_by: str = "",
        tenant_id: str = "",
        base_updated_at: Optional[str] = None,
        force: bool = False,
        allocate_version: bool = False,
    ) -> SaveFlowDefinitionResult:
        """保存（草稿）版本。已存在的 published 版本不可覆盖。

        C5-1 乐观并发控制：
        - ``base_updated_at``：调用方所基于版本的 updated_at（ISO 串，来自
          读取/上次保存的响应）。更新既有草稿前与服务端比对，不一致抛
          VersionConflictError（409 语义），消除 last-write-wins 静默覆盖；
          ``force=True`` 绕过检查（用户显式选择强制覆盖）。
        - ``allocate_version``：「另存新版本」时忽略调用方 version，按库内
          现有最大 semver 原子分配 patch+1（同一 session 内读+插，
          IntegrityError 重试兜底），消除前端本地列表算 next 的撞号；
          实际版本号经返回值 ``version`` 通告。
        """
        self.ensure_flow(flow_id, tenant_id=tenant_id)
        attempts = 3 if allocate_version else 1
        last_exc: Optional[IntegrityError] = None
        for attempt in range(attempts):
            try:
                return self._save_flow_once(
                    flow_id=flow_id,
                    version=version,
                    definition=definition,
                    layout=layout,
                    status=status,
                    created_by=created_by,
                    tenant_id=tenant_id,
                    base_updated_at=base_updated_at,
                    force=force,
                    allocate_version=allocate_version,
                )
            except IntegrityError as e:
                # allocate_version 下并发插入撞了唯一约束：重试重新分配版本号
                last_exc = e
                continue
        raise ValueError(f"版本 {flow_id}@{version} 已存在") from last_exc

    def _save_flow_once(
        self,
        flow_id: str,
        version: str,
        definition: str,
        layout: str,
        status: str,
        created_by: str,
        tenant_id: str,
        base_updated_at: Optional[str],
        force: bool,
        allocate_version: bool,
    ) -> SaveFlowDefinitionResult:
        with self._session_local() as session:
            if allocate_version:
                existing_versions = session.scalars(
                    select(FlowVersion.version).where(
                        FlowVersion.tenant_id == tenant_id,
                        FlowVersion.flow_id == flow_id,
                    )
                ).all()
                version = self._next_semver_of(list(existing_versions))
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
                # ---- #101 修复：乐观锁必须是原子 CAS ----
                # 旧实现「独立 SELECT 读 updated_at 比对 → 按主键 ORM UPDATE」
                # 两步之间无行锁无条件更新，两个持同一 base 的并发保存会双双
                # 通过检查、后写者静默覆盖先写者。现合并为单条条件 UPDATE：
                # WHERE 带 ``updated_at = :base`` 谓词，rowcount==0 即基准已
                # 被他人推进（或行已消失）→ 拒绝（409 语义），面朝安全侧。
                cas_ok = self._cas_update_draft(
                    session,
                    flow_id=flow_id,
                    version=version,
                    tenant_id=tenant_id,
                    definition=definition,
                    layout=layout,
                    status=status,
                    base_updated_at=base_updated_at,
                    force=force,
                )
                if not cas_ok:
                    stored = self._stored_updated_at(
                        session, flow_id, version, tenant_id
                    )
                    raise VersionConflictError(
                        f"画布所基于的版本已被他人更新（{flow_id}@{version}），"
                        "请加载最新或选择强制覆盖",
                        latest_updated_at=stored,
                    )
                # A51-02：UPDATE 已在事务内执行；commit 前复核请求世代。
                assert_tenant_generation(session, tenant_id)
                session.commit()
                return SaveFlowDefinitionResult(
                    flow_id=flow_id, version=version, status=status
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
            session.flush()
            self._touch_updated_at(session, flow_id, version, tenant_id)
            assert_tenant_generation(session, tenant_id)
            session.commit()
            return SaveFlowDefinitionResult(flow_id=flow_id, version=version, status=status)

    def _cas_update_draft(
        self,
        session: Session,
        *,
        flow_id: str,
        version: str,
        tenant_id: str,
        definition: str,
        layout: str,
        status: str,
        base_updated_at: Optional[str],
        force: bool,
    ) -> bool:
        """单条条件 UPDATE 原子完成「基准比对 + 草稿写入 + 基准前进」（#101）。

        返回 True=写入成功；False=基准已过期/无法判定（调用方应抛
        VersionConflictError，拒绝面朝安全侧）。能力降级：

        - ``flow_versions.updated_at`` 列不存在（旧库）：无法 CAS，退化为
          无条件更新（保留历史行为），但 warning 让「无并发保护」可见；
        - ``base_updated_at`` 为 None（存量调用方，未带基准）或 ``force=True``：
          按设计跳过比对，无条件更新（原行为不变）。
        """
        params: Dict[str, Any] = {
            "definition": definition,
            "layout": layout,
            "status": status,
            "ts": datetime.utcnow(),
            "tid": tenant_id,
            "fid": flow_id,
            "ver": version,
        }
        sql = (
            "UPDATE flow_versions SET definition = :definition, layout = :layout, "
            "status = :status, updated_at = :ts "
            "WHERE tenant_id = :tid AND flow_id = :fid AND version = :ver"
        )
        if not self._updated_at_supported(session):
            logger.warning(
                "flow_versions.updated_at 列不可用，草稿保存退化为无并发保护"
                "的覆盖写（flow=%s@%s）——乐观锁 #101 失效，请升级库结构",
                flow_id,
                version,
            )
            sql = (
                "UPDATE flow_versions SET definition = :definition, "
                "layout = :layout, status = :status "
                "WHERE tenant_id = :tid AND flow_id = :fid AND version = :ver"
            )
        elif base_updated_at is not None and not force:
            base_dt = _parse_updated_at(base_updated_at)
            if base_dt is None:
                # 基准串解析不了就无法构造可信的 CAS 谓词——按过期处理，
                # 宁可误 409 也不静默覆盖（安全侧）
                return False
            sql += " AND updated_at = :base_updated_at"
            params["base_updated_at"] = base_dt
        result = session.execute(text(sql), params)
        return bool(result.rowcount)

    # ---- updated_at 原生 SQL（C5-1）----
    # 列不在 ORM 映射内（见模块头注释）；这里统一读写并做能力降级：
    # 旧库缺列时按「无并发信息」处理，保存行为与历史版本完全一致。

    def _updated_at_supported(self, session: Session) -> bool:
        """探测 flow_versions.updated_at 列是否存在（独立连接，结果缓存）。"""
        if self._updated_at_ok is None:
            try:
                with session.get_bind().connect() as conn:
                    conn.execute(text("SELECT updated_at FROM flow_versions LIMIT 1"))
                self._updated_at_ok = True
            except Exception:  # noqa: BLE001 — 旧库缺列
                self._updated_at_ok = False
        return self._updated_at_ok

    def _touch_updated_at(
        self, session: Session, flow_id: str, version: str, tenant_id: str
    ) -> None:
        """同事务内刷新 updated_at（保存即前进乐观锁基准）。

        #101 后仅用于新建版本的 INSERT 分支；既有行的更新已并入
        ``_cas_update_draft`` 的单条条件 UPDATE（比对与写必须同一原子操作）。
        """
        if not self._updated_at_supported(session):
            return
        session.execute(
            text(
                "UPDATE flow_versions SET updated_at = :ts "
                "WHERE tenant_id = :tid AND flow_id = :fid AND version = :ver"
            ),
            {"ts": datetime.utcnow(), "tid": tenant_id, "fid": flow_id, "ver": version},
        )

    def _stored_updated_at(
        self, session: Session, flow_id: str, version: str, tenant_id: Optional[str]
    ) -> Optional[str]:
        """读取某版本当前 updated_at（ISO 串）；列缺失/为空返回 None。"""
        if not self._updated_at_supported(session):
            return None
        sql = "SELECT updated_at FROM flow_versions WHERE flow_id = :fid AND version = :ver"
        params: Dict[str, Any] = {"fid": flow_id, "ver": version}
        if tenant_id is not None:
            sql += " AND tenant_id = :tid"
            params["tid"] = tenant_id
        try:
            row = session.execute(text(sql), params).first()
        except Exception:  # noqa: BLE001 — 旧库缺列
            return None
        if not row or not row[0]:
            return None
        return _dt_to_iso(row[0])

    def _stored_updated_at_map(
        self, session: Session, flow_id: str, tenant_id: Optional[str]
    ) -> Dict[str, str]:
        """一次取整个 flow 各版本的 updated_at（列表查询用）。"""
        if not self._updated_at_supported(session):
            return {}
        sql = "SELECT version, updated_at FROM flow_versions WHERE flow_id = :fid"
        params: Dict[str, Any] = {"fid": flow_id}
        if tenant_id is not None:
            sql += " AND tenant_id = :tid"
            params["tid"] = tenant_id
        try:
            rows = session.execute(text(sql), params).all()
        except Exception:  # noqa: BLE001 — 旧库缺列
            return {}
        out: Dict[str, str] = {}
        for r in rows:
            if r[1]:
                out[r[0]] = _dt_to_iso(r[1])
        return out

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
            updated_map = self._stored_updated_at_map(session, flow_id, tenant_id)
            return self._version_to_out(row, updated_at=updated_map.get(row.version))

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
            updated_map = self._stored_updated_at_map(session, flow_id, tenant_id)
            return [
                self._version_to_out(r, updated_at=updated_map.get(r.version))
                for r in rows
            ]

    def iter_published_versions(
        self, tenant_id: Optional[str] = None, page_size: int = 1000
    ) -> Iterator[Tuple[str, str, str, str]]:
        """分页流式返回全部 published 版本：(tenant_id, flow_id, version, definition)。

        A2R20-06：启动回填原先对每个 flow 调一次 ``list_versions``（2 次 SQL/
        flow），pipeline 只解决了 Redis 往返，SQL 仍随 flow 数线性增长。这里
        用键集分页（每页 1 次 SQL、按 ``(tenant_id, flow_id, version)`` 排序），
        SQL 往返降到 O(页数)；definition 按页取回，内存不随总版本数增长。
        """
        page_size = max(1, int(page_size))
        cursor: Optional[Tuple[str, str, str]] = None
        while True:
            with self._session_local() as session:
                stmt = select(
                    FlowVersion.tenant_id,
                    FlowVersion.flow_id,
                    FlowVersion.version,
                    FlowVersion.definition,
                ).where(FlowVersion.status == "published")
                stmt = stmt.where(self._tenant_filter(FlowVersion, tenant_id))
                if cursor is not None:
                    stmt = stmt.where(
                        tuple_(
                            FlowVersion.tenant_id,
                            FlowVersion.flow_id,
                            FlowVersion.version,
                        ) > tuple_(*cursor)
                    )
                rows = session.execute(
                    stmt.order_by(
                        FlowVersion.tenant_id,
                        FlowVersion.flow_id,
                        FlowVersion.version,
                    ).limit(page_size)
                ).all()
            if not rows:
                return
            for row in rows:
                yield (row[0] or "", row[1], row[2], row[3] or "")
            if len(rows) < page_size:
                return
            last = rows[-1]
            cursor = (last[0] or "", last[1], last[2])

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
                session.flush()
                assert_tenant_generation(session, tenant_id)
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
            session.flush()
            assert_tenant_generation(session, tenant_id)
            session.commit()
            return True

    @staticmethod
    def _version_to_out(row: FlowVersion, updated_at: Optional[str] = None) -> FlowVersionOut:
        return FlowVersionOut(
            flow_id=row.flow_id,
            version=row.version,
            status=row.status,
            definition=row.definition,
            layout=row.layout,
            created_at=row.created_at,
            published_at=row.published_at,
            created_by=row.created_by,
            # ISO 串 → datetime（解析失败按 None，保持兼容）
            updated_at=datetime.fromisoformat(updated_at) if updated_at else None,
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
                session.flush()
                assert_tenant_generation(session, tenant_id)
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
            session.flush()
            assert_tenant_generation(session, tenant_id)
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
            session.flush()
            assert_tenant_generation(session, tenant_id)
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
            session.flush()
            assert_tenant_generation(session, tenant_id)
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
        # A51-02：本地执行行同属租户作用域（迟到 insert 会被重建租户继承）。
        session.flush()
        assert_tenant_generation(session, tenant_id)
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
    usage_json: Optional[str] = None,
) -> None:
    """终结一条本地执行记录。

    ``usage_json``（plaita#37）：run 级 usage 汇总的 JSON 串；**不传 = 不动该
    列**（老调用方零变化），未采集由调用方显式传 ``"null"``。
    """
    update_local_execution(
        execution_id,
        status=status,
        end_time=datetime.utcnow(),
        **({"output_json": output_json} if output_json is not None else {}),
        **({"error_json": error_json} if error_json is not None else {}),
        **({"context_json": context_json} if context_json is not None else {}),
        **({"usage_json": usage_json} if usage_json is not None else {}),
    )


def _local_row_to_dict(row: LocalExecution) -> dict:
    nodes = _loads_or_none(row.nodes_json) or []
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
        "nodes": nodes,
        "input": _loads_or_none(row.input_json) or {},
        "output": _loads_or_none(row.output_json),
        # 与 worker 侧 NodeTimingCallback 同构的节点耗时视图，前端只认这一份
        "node_timings": _timings_from_nodes(nodes) or None,
        # plaita#37 P2：run 级 usage 原样读出（页面级展示视图由 API 层折算，
        # 本层只做 JSON 解码）。老行/未采集 ⇒ None（**不是 0**）。
        "usage": _loads_or_none(getattr(row, "usage_json", None)),
    }


def _timings_from_nodes(nodes: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """把本地模式 trace 的起止时间戳折算成 ``node_timings``（node_id → 耗时）。

    同一节点循环/重试多次时：``duration_ms`` 取最后一次，``attempts`` 计数，
    ``total_duration_ms`` 累计——与集群模式采集器同一语义，前端无需分支处理。
    """
    out: Dict[str, Dict[str, Any]] = {}
    for entry in nodes:
        if not isinstance(entry, dict):
            continue
        node_id = entry.get("id")
        duration = entry.get("duration_ms")
        if not node_id or not isinstance(duration, int):
            continue
        prev = out.get(node_id) or {}
        out[node_id] = {
            "started_at": entry.get("started_at"),
            "ended_at": entry.get("ended_at"),
            "started_ms": entry.get("started_ms"),
            "ended_ms": entry.get("ended_ms"),
            "duration_ms": duration,
            "total_duration_ms": int(prev.get("total_duration_ms", 0)) + duration,
            "attempts": int(prev.get("attempts", 0)) + 1,
            "failed": entry.get("status") == "error",
        }
    return out


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


def delete_local_logs(
    execution_id: str, tenant_id: Optional[str] = None
) -> int:
    """删除一条本地执行的全部日志（随执行删除，返回删除行数）。

    日志是执行数据的一部分：此前删执行只删 ``local_executions`` 行，
    ``local_logs`` 仍可按 ``execution_id`` 查到（「删除后不可恢复」不成立）。
    """
    store = get_flow_store()
    with store._session_local() as session:
        result = session.execute(
            delete(LocalLog).where(
                FlowStore._tenant_filter(LocalLog, tenant_id),
                LocalLog.execution_id == execution_id,
            )
        )
        session.commit()
        return int(result.rowcount or 0)


# ============ 初始化辅助 ============

# A2R20-07：非 SQLite 后端的启动连接/池预算。此前 ``create_engine(db_url)`` 无
# connect timeout——PG 半开（accept 但不完成握手）时 alembic 迁移永久阻塞，
# readiness 永不达成。取值与仓内既有 5s 口径对齐；环境变量可覆盖。
DEFAULT_DB_CONNECT_TIMEOUT_SECONDS = 5.0
DEFAULT_DB_POOL_TIMEOUT_SECONDS = 5.0
#: PG 迁移/DDL 等锁上界（仅锁等待，不限制语句总时长）。
DEFAULT_DB_LOCK_TIMEOUT_MS = 5000
_DB_CONNECT_TIMEOUT_ENV = "PLAITA_CONSOLE_DB_CONNECT_TIMEOUT"
_DB_POOL_TIMEOUT_ENV = "PLAITA_CONSOLE_DB_POOL_TIMEOUT"


def _env_positive_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError):
        value = None
    if value is None or value <= 0:
        logger.warning(
            "%s=%s 非法（需正数秒），使用默认 %.1fs",
            name, _redact_env_value(raw), default,
        )
        return default
    return value


def _db_display_url(db_url: str) -> str:
    """启动日志用的脱敏目标（口令 ``***``）。"""
    try:
        return make_url(db_url).render_as_string(hide_password=True)
    except Exception:  # noqa: BLE001 — 渲染失败不得炸启动路径
        return "<unparsable db_url>"


def _db_engine_kwargs(db_url: str) -> dict:
    """按后端注入连接/池超时；SQLite 路径零变化。"""
    url = make_url(db_url)
    backend = url.get_backend_name()
    kwargs: dict = {"future": True}
    if backend == "sqlite":
        return kwargs

    connect_timeout = _env_positive_float(
        _DB_CONNECT_TIMEOUT_ENV, DEFAULT_DB_CONNECT_TIMEOUT_SECONDS
    )
    pool_timeout = _env_positive_float(
        _DB_POOL_TIMEOUT_ENV, DEFAULT_DB_POOL_TIMEOUT_SECONDS
    )
    kwargs["pool_pre_ping"] = True
    kwargs["pool_timeout"] = pool_timeout

    connect_args: dict = {}
    if backend in ("postgresql", "mysql", "mariadb"):
        # psycopg/psycopg2/pymysql 的 connect_timeout 都是整数秒（最小 1）。
        connect_args["connect_timeout"] = max(1, int(connect_timeout))
    if backend == "postgresql" and "options" not in url.query:
        connect_args["options"] = f"-c lock_timeout={DEFAULT_DB_LOCK_TIMEOUT_MS}"
    if connect_args:
        kwargs["connect_args"] = connect_args
    return kwargs


def _is_db_connection_error(exc: BaseException) -> bool:
    """连接/握手/超时类错误（沿 cause/context 链看，alembic 会包一层）。"""
    seen: set[int] = set()
    current: Optional[BaseException] = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, (OperationalError, InterfaceError)):
            return True
        if isinstance(current, DBAPIError) and getattr(
            current, "connection_invalidated", False
        ):
            return True
        current = current.__cause__ or current.__context__
    return False


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
    """创建/替换全局引擎并对齐 schema。返回引擎实例。

    schema 对齐统一交给 alembic 引导（services/schema_migrations）：
    空库 create_all + 认领基线；存量库（无 alembic_version）认领基线不重放；
    已有版本记录则 upgrade head。这样「升级到哪个 schema」有版本可查、可回滚，
    不再依赖散落的一次性补列脚本。

    A2R20-07：非 SQLite 后端显式声明 connect/pool 超时与 ``pool_pre_ping``；
    连接类失败**不回退** ``create_all``（半开 DB 上回退只是再阻塞一次），
    具名抛出让启动 fail-fast。
    """
    global _engine, _SessionLocal
    engine_kwargs = _db_engine_kwargs(db_url)
    display_url = _db_display_url(db_url)
    _engine = create_engine(db_url, **engine_kwargs)
    _SessionLocal = sessionmaker(bind=_engine, expire_on_commit=False)
    logger.info(
        "FlowStore 引擎参数: %s（%s）",
        display_url,
        engine_kwargs.get("connect_args") or "sqlite（无池超时）",
    )
    try:
        from . import schema_migrations
    except ImportError:  # 平铺布局（cwd=backend）运行时
        import schema_migrations  # type: ignore
    try:
        _engine.schema_summary = schema_migrations.run_migrations(_engine, db_url)
    except Exception as exc:  # noqa: BLE001 — 见下：仅在 alembic 缺失时降级
        if _is_db_connection_error(exc):
            # 连接/握手/超时：回退 create_all 只会用同一不可达目标再阻塞一次。
            _engine = None
            _SessionLocal = None
            logger.error(
                "FlowStore 引擎初始化失败：数据库连接失败/超时（%s）: %s",
                display_url, exc,
            )
            raise
        # alembic 是 console 的依赖；缺失（如最小安装/测试桩）时退回旧的
        # create_all 路径，保证「装不全也能启动」，但明确告警提示 schema 无版本记录。
        logger.warning("alembic 迁移不可用（%s），回退 create_all（schema 将无版本记录）", exc)
        create_all()
        _engine.schema_summary = {"action": "create_all-fallback", "error": str(exc)}
    logger.info(
        "FlowStore 引擎已初始化: %s（schema=%s）", display_url, _engine.schema_summary
    )
    return _engine


def create_all() -> None:
    """在当前引擎上创建所有表（含多租户 schema 迁移）。"""
    if _engine is None:
        raise RuntimeError("引擎未初始化，请先调用 init_engine()")
    _migrate_tenant_schema()
    Base.metadata.create_all(_engine)
    _migrate_sqlite_columns()


def _sqlite_columns(conn, table: str) -> list[str]:
    """读取 SQLite 表的列名（PRAGMA table_info）。

    仅 SQLite：PRAGMA 为 SQLite 专有语法，在 PostgreSQL 等后端会抛
    ``ProgrammingError: syntax error at or near "PRAGMA"``（console 启动即崩）。
    非 sqlite 后端（或引擎未初始化）直接返回 ``[]``——全部调用点均以
    ``if not cols: continue`` / ``if cols and ...`` 形式处理空结果，语义等价于
    「本后端无此 SQLite 迁移内容，跳过」。一处守卫覆盖所有调用点。
    """
    if _engine is None or _engine.url.get_backend_name() != "sqlite":
        return []
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


def _remove_default_credentials_export() -> None:
    """删除 default 租户的导出文件（若存在）；延迟导入避免服务循环依赖。"""
    try:
        from . import credentials_svc
    except ImportError:
        from services import credentials_svc  # type: ignore
    path = credentials_svc.credentials_file(DEFAULT_TENANT_ID)
    try:
        path.unlink()
    except FileNotFoundError:
        return
    except OSError:
        logger.exception("default 租户重建：凭据导出文件清理失败: %s", path)
        raise


def ensure_tenant_bootstrap() -> None:
    """多租户启动引导（幂等，多租户首次启动时把存量数据收编进 default 租户）。

    - default 租户不存在则创建（重建前清除前代同租户凭据，见 A66-02）；
    - 各租户域表中 tenant_id='' 的存量行归入 default；
    - 无任何 membership 的非平台管理员补 membership(default, users.role)；
    - 首次引导（tenants 表为空）时，既有全局 admin 提升为 platform_admin。

    仅 SQLite：整段是 SQLite 存量库的收编/迁移逻辑，依赖 ``_sqlite_columns``
    （PRAGMA 专有）；PostgreSQL 等后端是全新库、无存量需迁移，直接 return。
    """
    if _engine is None:
        raise RuntimeError("引擎未初始化，请先调用 init_engine()")
    if _engine.url.get_backend_name() != "sqlite":
        return
    # A66-02：default 行不在 ⇒ 已标记 'default' 的凭据行与导出文件属于前代
    # （旧版本删租户未清凭据）。必须在下方 `tenant_id='' → default` 迁移
    # UPDATE 之前删除（否则会把前代行一起「归户」），也必须独立于随后
    # `_engine.begin()` 的迁移事务（同一 SQLite 库上两个写连接会互相锁死）。
    with _engine.connect() as conn:
        default_missing = conn.execute(
            select(Tenant.id).where(Tenant.id == DEFAULT_TENANT_ID)
        ).first() is None
    if default_missing:
        with _engine.begin() as conn:
            purged = conn.execute(
                delete(Credential).where(Credential.tenant_id == DEFAULT_TENANT_ID)
            ).rowcount or 0
        if purged:
            logger.warning(
                "default 租户重建：清除 %d 条前代孤儿凭据行（不允许继承）",
                purged,
            )
        _remove_default_credentials_export()

    with _SessionLocal() as session:  # type: Session
        first_boot = session.query(Tenant).count() == 0
        if default_missing:
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
    """轻量迁移：旧 SQLite 库补新增列（仅 ADD COLUMN，保守策略）。

    仅 SQLite：函数体用 ``PRAGMA table_info``（SQLite 专有），在 PostgreSQL
    等后端会语法报错导致启动即崩——非 sqlite 直接 return（与
    ``_migrate_tenant_schema`` 同款守卫）。
    """
    if _engine is None or _engine.url.get_backend_name() != "sqlite":
        return
    from sqlalchemy import text as _text

    wanted = {
        "local_executions": {
            "context_json": "TEXT NOT NULL DEFAULT 'null'",
            # plaita#37 P2：usage 汇总列。默认 'null' = 未采集——老行补列即得
            # None（不是 0），零迁移。
            "usage_json": "TEXT NOT NULL DEFAULT 'null'",
        },
        # C5-1 乐观并发：旧库补 updated_at 列（新库由 create_all 按追加列后的
        # 映射表直接建出；此处 PRAGMA 判缺再补，幂等）
        "flow_versions": {"updated_at": "DATETIME"},
        # A55-S3：审计身份列（alembic 0002 为主；alembic 不可用时的
        # create_all 回退路径由这里补列，两路都幂等）。
        "audit_logs": {
            "auth_source": "TEXT NOT NULL DEFAULT ''",
            "key_fingerprint": "TEXT NOT NULL DEFAULT ''",
            "request_id": "TEXT NOT NULL DEFAULT ''",
            "role_snapshot": "TEXT NOT NULL DEFAULT ''",
        },
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
