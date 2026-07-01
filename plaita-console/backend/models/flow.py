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
