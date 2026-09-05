"""
流程编排持久化模型

flow / flow_version / node_descriptor 三表的 SQLAlchemy 声明式模型。

设计说明：
- ``definition`` 存 ``Flow.model_dump_json()`` 产出的 JSON 字符串（运行时所需）。
- ``layout`` 单独存画布坐标 JSON 字符串 ``{nodeId: {x, y}}``，避免污染 Flow 定义。
- 独立维护 console 自己的 declarative Base，不复用 ``plaita.storage`` 的 Base，
  以免改动 SDK 公共存储抽象（见 plan.md 决策日志）。
"""
from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class FlowRecord(Base):
    """流程记录（flow_id 维度的元信息）"""

    __tablename__ = "flows"

    id = Column(Integer, primary_key=True, autoincrement=True)
    flow_id = Column(String(128), nullable=False, unique=True, index=True)
    author = Column(String(128), nullable=False, default="")
    desc = Column(Text, nullable=False, default="")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(
        DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )


class FlowVersion(Base):
    """流程版本（flow_id + version 唯一；草稿/发布状态机）"""

    __tablename__ = "flow_versions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    flow_id = Column(
        String(128),
        ForeignKey("flows.flow_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    version = Column(String(64), nullable=False)
    status = Column(String(16), nullable=False, default="draft")  # draft | published
    definition = Column(Text, nullable=False, default="")
    layout = Column(Text, nullable=False, default="")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    published_at = Column(DateTime, nullable=True)
    created_by = Column(String(128), nullable=False, default="")

    __table_args__ = (
        UniqueConstraint("flow_id", "version", name="uq_flow_version"),
    )


class NodeDescriptor(Base):
    """节点描述（内置 + 自定义，node_type 唯一）"""

    __tablename__ = "node_descriptors"

    id = Column(Integer, primary_key=True, autoincrement=True)
    node_type = Column(String(128), nullable=False, unique=True, index=True)
    node_name = Column(String(128), nullable=False, default="")
    category = Column(String(64), nullable=False, default="")
    schema_json = Column(Text, nullable=False, default="{}")
    is_builtin = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(
        DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )


class CopilotThread(Base):
    """Copilot 会话（AG-UI thread 与编排流程的关联，支持历史回看/审计）

    flow_id 不加外键：thread 可指向已被删除的 flow，会话历史独立留存。
    """

    __tablename__ = "copilot_threads"

    id = Column(Integer, primary_key=True, autoincrement=True)
    thread_id = Column(String(128), nullable=False, unique=True, index=True)
    flow_id = Column(String(128), nullable=False, index=True)
    version = Column(String(64), nullable=False, default="")
    title = Column(String(256), nullable=False, default="")
    message_count = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(
        DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )


class LocalExecution(Base):
    """本地单机模式执行记录（无 Redis 退化模式）。

    本地模式下流程在 console 进程内执行，节点级 trace 由回调写入
    ``nodes_json``；集群模式（有 Redis）执行数据仍在 Redis，本表不使用。
    """

    __tablename__ = "local_executions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    execution_id = Column(String(64), nullable=False, unique=True, index=True)
    flow_id = Column(String(128), nullable=False, index=True)
    flow_version = Column(String(64), nullable=False, default="")
    status = Column(String(16), nullable=False, default="running")  # running/completed/failed/cancelled
    input_json = Column(Text, nullable=False, default="{}")
    output_json = Column(Text, nullable=False, default="null")
    error_json = Column(Text, nullable=False, default="null")
    nodes_json = Column(Text, nullable=False, default="[]")
    invoker = Column(String(64), nullable=False, default="local")
    start_time = Column(DateTime, nullable=False, default=datetime.utcnow)
    end_time = Column(DateTime, nullable=True)
    last_update_time = Column(
        DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )


class Credential(Base):
    """凭据（加密存储）：外部服务的机密信息，流程节点按名引用。

    ``data_json`` 为 Fernet 加密后的 JSON 字符串；明文只在保存/使用瞬间存在。
    保存时同步导出到 PLAITA_CREDENTIALS_FILE（默认 .plaita-credentials.json）
    供引擎节点运行时解密读取。
    """

    __tablename__ = "credentials"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(128), nullable=False, unique=True, index=True)
    type = Column(String(64), nullable=False, default="generic")
    data_json = Column(Text, nullable=False)
    desc = Column(Text, nullable=False, default="")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(
        DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )


class User(Base):
    """编排台用户。role ∈ admin/editor/viewer。

    密码存 PBKDF2-SHA256（格式 salt$hash，stdlib 实现，无额外依赖）。
    """

    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(64), nullable=False, unique=True, index=True)
    password_hash = Column(String(256), nullable=False)
    role = Column(String(16), nullable=False, default="viewer")
    disabled = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class SessionToken(Base):
    """登录会话。只存 token 的 SHA-256，明文 token 仅在签发时返回给前端。"""

    __tablename__ = "session_tokens"

    token_hash = Column(String(64), primary_key=True)
    username = Column(String(64), nullable=False, index=True)
    role = Column(String(16), nullable=False)
    expires_at = Column(DateTime, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class AuditLog(Base):
    """审计日志：管理面敏感操作留痕（操作人不记密钥/机密内容）。"""

    __tablename__ = "audit_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ts = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)
    actor = Column(String(64), nullable=False, default="")
    action = Column(String(64), nullable=False, index=True)
    resource = Column(String(64), nullable=False, default="")
    resource_id = Column(String(128), nullable=False, default="")
    detail_json = Column(Text, nullable=False, default="{}")
    ip = Column(String(64), nullable=False, default="")


class Deployment(Base):
    """部署记录：每次 publish 留痕（环境、操作人、定义指纹），支撑环境晋升审计。"""

    __tablename__ = "deployments"

    id = Column(Integer, primary_key=True, autoincrement=True)
    flow_id = Column(String(128), nullable=False, index=True)
    version = Column(String(64), nullable=False)
    environment = Column(String(32), nullable=False, default="dev")
    actor = Column(String(64), nullable=False, default="")
    definition_hash = Column(String(64), nullable=False, default="")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)
