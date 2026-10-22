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
    ForeignKeyConstraint,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import declarative_base

try:  # 包布局 plaita_console.models.flow
    from ..secure_fields import EncryptedText
except ImportError:  # 平铺布局（cwd=backend，models 为顶层包）
    from secure_fields import EncryptedText  # type: ignore

Base = declarative_base()

# 多租户：存量数据/默认租户的固定 slug（首次多租户启动时创建）
DEFAULT_TENANT_ID = "default"


class Tenant(Base):
    """租户。id 为 slug（创建后不可改），contract_secret_* 为对外 HMAC 契约凭证。

    A2R25-08/09：``contract_secret_key`` 经 ``secure_fields.EncryptedText``
    透明加密（``enc:v1:`` 密文；主密钥与凭据加密同源、按用途派生）。ORM
    读取仍是明文（验签/一次性返回不受影响），裸 SQL/备份只能看到密文；
    存量明文由 ``init_engine`` 回填。SQLite 文件权限仍是 0600 的第二道闸。
    """

    __tablename__ = "tenants"

    id = Column(String(64), primary_key=True)
    name = Column(String(128), nullable=False, default="")
    status = Column(String(16), nullable=False, default="active")  # active | disabled
    contract_secret_id = Column(String(64), nullable=False, default="", unique=True, index=True)
    # A2R25-08/09：落盘即密文（enc:v1:）；读取侧由 TypeDecorator 透明解密。
    contract_secret_key = Column(EncryptedText(purpose="tenant-contract-secret"),
                                  nullable=False, default="")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class TenantMember(Base):
    """租户成员（用户↔租户多对多，租户内角色）。授权角色以本表为准。"""

    __tablename__ = "tenant_members"

    username = Column(
        String(64),
        ForeignKey("users.username", ondelete="CASCADE"),
        nullable=False,
        primary_key=True,
    )
    tenant_id = Column(
        String(64),
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
        primary_key=True,
    )
    role = Column(String(16), nullable=False, default="viewer")  # admin | editor | viewer
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class FlowRecord(Base):
    """流程记录（租户内 flow_id 维度的元信息）"""

    __tablename__ = "flows"

    id = Column(Integer, primary_key=True, autoincrement=True)
    tenant_id = Column(String(64), nullable=False, default="", index=True)
    flow_id = Column(String(128), nullable=False, index=True)
    author = Column(String(128), nullable=False, default="")
    desc = Column(Text, nullable=False, default="")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(
        DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    __table_args__ = (
        UniqueConstraint("tenant_id", "flow_id", name="uq_tenant_flow"),
    )


class FlowVersion(Base):
    """流程版本（租户内 flow_id + version 唯一；草稿/发布状态机）

    外键为**复合外键** ``(tenant_id, flow_id) -> flows(tenant_id, flow_id)``：

    - ``flow_id`` 按设计是「租户内唯一」（见 ``flows.uq_tenant_flow``），
      不同租户可同名；单列外键会跨租户串数据，且 PostgreSQL 因
      ``flows.flow_id`` 无单列唯一约束而**拒绝建表**
      （``InvalidForeignKey: no unique constraint matching given keys``）。
    - 复合外键命中 ``flows`` 已有的 ``uq_tenant_flow`` 唯一约束，既满足 PG
      要求，语义上也把版本锚定到「同租户的同名 flow」。
    """

    __tablename__ = "flow_versions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    tenant_id = Column(String(64), nullable=False, default="", index=True)
    flow_id = Column(String(128), nullable=False, index=True)
    version = Column(String(64), nullable=False)
    status = Column(String(16), nullable=False, default="draft")  # draft | published
    definition = Column(Text, nullable=False, default="")
    layout = Column(Text, nullable=False, default="")
    # plaita#300：目标 runtime pool（go / python）。console 按 runtime 把
    # start/resume 消息路由到对应队列，publish/start 按池内 worker 能力清单
    # 做节点支持门。存量版本经迁移补 "python"（历史队列语义即 Python worker）。
    runtime = Column(String(16), nullable=False, server_default="python", default="python")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    published_at = Column(DateTime, nullable=True)
    created_by = Column(String(128), nullable=False, default="")

    __table_args__ = (
        UniqueConstraint("tenant_id", "flow_id", "version", name="uq_tenant_flow_version"),
        ForeignKeyConstraint(
            ["tenant_id", "flow_id"],
            ["flows.tenant_id", "flows.flow_id"],
            ondelete="CASCADE",
            name="fk_flow_versions_flow",
        ),
    )


class FlowPublishOutbox(Base):
    """flow 发布 outbox（#299：跨 Redis 部署的 durable 发布投递链）。

    发布事务内与 ``flow_versions`` 同事务写入一条 pending（同事务 = publish
    回滚时 outbox 不残留）；由投递 worker 至少一次送达目标 Redis 的
    ``{ns}:flow:{flow_id}:{version}``。幂等键是四元组唯一约束
    ``(tenant_id, flow_id, version, definition_hash)``——同版本重复发布
    （同内容）不产生第二行；同版本换了内容（hash 不同）是新投递。

    状态机：pending（待投/退避中）→ delivered（已送达且读回校验通过）
    / failed（连续失败到上限，DLQ 档；由投递循环按慢速退避自动重排）；
    对账把「delivered 但目标缺 key/hash 不符」打回 pending 重投。
    deleting（#303 删除协议 tombstone）：删除守卫在清目标 key 前标记，
    投递/对账一律跳过；删除事务 purge 行，失败回滚恢复原状。

    注意**故意不建**指向 flow_versions 的外键：版本删除时由删除路径显式
    清理对应 outbox 行（purge），否则对账会把已删除的版本「复活」进目标
    Redis。tenant_id 同 flow_versions 口径：default/存量行为空串。
    """

    __tablename__ = "flow_publish_outbox"

    id = Column(Integer, primary_key=True, autoincrement=True)
    tenant_id = Column(String(64), nullable=False, default="", index=True)
    flow_id = Column(String(128), nullable=False)
    version = Column(String(64), nullable=False)
    definition_hash = Column(String(64), nullable=False)
    definition = Column(Text, nullable=False, default="")
    status = Column(String(16), nullable=False, default="pending", index=True)
    attempts = Column(Integer, nullable=False, default=0)
    # #302：源表定义漂移被对账命中的累计次数（/health.engine_outbox.
    # drift_total 聚合；>0 = 该行经历过 DB 级定义修改并被按源表现值重投）。
    drift_count = Column(Integer, nullable=False, default=0)
    last_error = Column(Text, nullable=False, default="")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    delivered_at = Column(DateTime, nullable=True)
    # 退避调度：下次允许投递的时点（None=立即可投）。issue 建议字段之外
    # 的实现列——重试退避与 failed 慢速重排都需要它，避免全表逐行 sleep。
    next_attempt_at = Column(DateTime, nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "tenant_id", "flow_id", "version", "definition_hash",
            name="uq_outbox_delivery_key",
        ),
    )


class NodeDescriptor(Base):
    """节点描述（内置 + 租户自定义；租户内 node_type 唯一）"""

    __tablename__ = "node_descriptors"

    id = Column(Integer, primary_key=True, autoincrement=True)
    tenant_id = Column(String(64), nullable=False, default="", index=True)
    node_type = Column(String(128), nullable=False, index=True)
    node_name = Column(String(128), nullable=False, default="")
    category = Column(String(64), nullable=False, default="")
    schema_json = Column(Text, nullable=False, default="{}")
    is_builtin = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(
        DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    __table_args__ = (
        UniqueConstraint("tenant_id", "node_type", name="uq_tenant_node_type"),
    )


class PropertyType(Base):
    """自定义属性类型（2026-09 节点管理重设计）

    命名别名：base_type 必须是运行时内置类型；enum/default 是录入约束。
    关键语义——别名只在 console 生成节点 schema_json 时**展开**为基础类型 +
    约束，运行时（Property.match）永不接触自定义类型名（未知 data_type 一律
    匹配失败），所以这里不需要任何运行时注册机制。
    租户内 name 唯一。
    """

    __tablename__ = "property_types"

    id = Column(Integer, primary_key=True, autoincrement=True)
    tenant_id = Column(String(64), nullable=False, default="", index=True)
    name = Column(String(64), nullable=False, index=True)
    base_type = Column(String(32), nullable=False, default="string")
    # enum 选项与默认值存 JSON（list / 标量），读取方负责解析
    enum_json = Column(Text, nullable=False, default="[]")
    default_json = Column(Text, nullable=False, default="null")
    desc = Column(String(256), nullable=False, default="")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(
        DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    __table_args__ = (
        UniqueConstraint("tenant_id", "name", name="uq_tenant_property_type"),
    )


class CopilotThread(Base):
    """Copilot 会话（AG-UI thread 与编排流程的关联，支持历史回看/审计）

    flow_id 不加外键：thread 可指向已被删除的 flow，会话历史独立留存。
    """

    __tablename__ = "copilot_threads"

    id = Column(Integer, primary_key=True, autoincrement=True)
    tenant_id = Column(String(64), nullable=False, default="", index=True)
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

    A2R25-08/09：``input_json``（执行 params）与 ``context_json``
    （checkpoint/``$INPUT``）经 ``secure_fields.EncryptedText`` 落盘即密文；
    ``output_json/nodes_json`` 的读面脱敏仍是 #166/#222 的独立契约。
    """

    __tablename__ = "local_executions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    tenant_id = Column(String(64), nullable=False, default="", index=True)
    execution_id = Column(String(64), nullable=False, unique=True, index=True)
    flow_id = Column(String(128), nullable=False, index=True)
    flow_version = Column(String(64), nullable=False, default="")
    status = Column(String(16), nullable=False, default="running")  # running/completed/failed/cancelled
    input_json = Column(EncryptedText(purpose="local-execution-input"),
                        nullable=False, default="{}")
    output_json = Column(Text, nullable=False, default="null")
    error_json = Column(Text, nullable=False, default="null")
    nodes_json = Column(Text, nullable=False, default="[]")
    # run 级 token/cost 汇总（plaita#37 P2）：JSON 对象或字符串 'null'。
    # 默认 'null' = **未采集**——老行零迁移（SQLite 补列时 ADD COLUMN 的默认
    # 值即 'null'，读取侧 `_loads_or_none` 折算成 None），**绝不折算成 0**。
    # 形状与引擎侧 ExecutionState.usage 同源（plaita.storage.usage 聚合）；
    # 展示的两级汇总（run/节点）由读取侧经 usage_view / node_usage_breakdown
    # 只读折算，本列不是第二真相源（真源仍是 context 的 $NODE）。
    usage_json = Column(Text, nullable=False, default="null")
    # 分布式模式 checkpoint：挂起/每步推进后的上下文快照（resume 用）
    context_json = Column(EncryptedText(purpose="local-execution-context"),
                          nullable=False, default="null")
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
    tenant_id = Column(String(64), nullable=False, default="", index=True)
    name = Column(String(128), nullable=False, index=True)
    type = Column(String(64), nullable=False, default="generic")
    data_json = Column(Text, nullable=False)
    desc = Column(Text, nullable=False, default="")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(
        DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    __table_args__ = (
        UniqueConstraint("tenant_id", "name", name="uq_tenant_credential"),
    )


class User(Base):
    """编排台用户。role 为遗留全局角色（无租户上下文时回退用）；
    授权角色以 tenant_members 为准。platform_admin 为平台级管理员。"""

    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(64), nullable=False, unique=True, index=True)
    password_hash = Column(String(256), nullable=False)
    role = Column(String(16), nullable=False, default="viewer")
    platform_admin = Column(Boolean, nullable=False, default=False)
    disabled = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class SessionToken(Base):
    """登录会话。只存 token 的 SHA-256，明文 token 仅在签发时返回给前端。
    active_tenant 为会话当前所在租户（多租户成员切换）。"""

    __tablename__ = "session_tokens"

    token_hash = Column(String(64), primary_key=True)
    username = Column(String(64), nullable=False, index=True)
    role = Column(String(16), nullable=False)
    active_tenant = Column(String(64), nullable=True, default=None)
    expires_at = Column(DateTime, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class AuditLog(Base):
    """审计日志：管理面敏感操作留痕（操作人不记密钥/机密内容）。

    A55-S3：认证来源 / key 指纹（sha256 前 16 位，不存明文）/ 请求 ID /
    角色快照——让「哪把 key、哪次请求、当时的角色」可追溯。
    """

    __tablename__ = "audit_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ts = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)
    tenant_id = Column(String(64), nullable=False, default="", index=True)
    actor = Column(String(64), nullable=False, default="")
    action = Column(String(64), nullable=False, index=True)
    resource = Column(String(64), nullable=False, default="")
    resource_id = Column(String(128), nullable=False, default="")
    detail_json = Column(Text, nullable=False, default="{}")
    ip = Column(String(64), nullable=False, default="")
    # server_default：旧调用/脚本以原生 SQL 只写历史列时不触发 NOT NULL。
    auth_source = Column(String(16), nullable=False, default="", server_default="")
    key_fingerprint = Column(String(32), nullable=False, default="", server_default="")
    request_id = Column(String(64), nullable=False, default="", server_default="")
    role_snapshot = Column(String(16), nullable=False, default="", server_default="")


class Deployment(Base):
    """部署记录：每次 publish 留痕（环境、操作人、定义指纹），支撑环境晋升审计。"""

    __tablename__ = "deployments"

    id = Column(Integer, primary_key=True, autoincrement=True)
    tenant_id = Column(String(64), nullable=False, default="", index=True)
    flow_id = Column(String(128), nullable=False, index=True)
    version = Column(String(64), nullable=False)
    environment = Column(String(32), nullable=False, default="dev")
    actor = Column(String(64), nullable=False, default="")
    definition_hash = Column(String(64), nullable=False, default="")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)


class LocalSchedule(Base):
    """本地单机模式的调度定义（集群档存 Redis HASH，由 schedule_service 触发；
    本地档由 console 内置调度循环触发）。"""

    __tablename__ = "local_schedules"

    schedule_id = Column(String(64), primary_key=True)
    tenant_id = Column(String(64), nullable=False, default="", index=True)
    name = Column(String(128), nullable=False, default="")
    flow_id = Column(String(128), nullable=False, index=True)
    version = Column(String(64), nullable=True)
    cron = Column(String(64), nullable=False, default="")
    params_json = Column(Text, nullable=False, default="{}")
    enabled = Column(Boolean, nullable=False, default=True)
    created_by = Column(String(64), nullable=False, default="")
    next_run_at = Column(String(64), nullable=False, default="")  # ms 时间戳字符串，与集群档一致
    last_run_at = Column(String(64), nullable=False, default="")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(
        DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )


class LocalScheduleFire(Base):
    """本地调度触发历史（对应集群档 plaita:schedule:fires:{id}）。"""

    __tablename__ = "local_schedule_fires"

    id = Column(Integer, primary_key=True, autoincrement=True)
    schedule_id = Column(String(64), nullable=False, index=True)
    execution_id = Column(String(64), nullable=False, default="")
    trigger_kind = Column(String(16), nullable=False, default="cron")  # cron/manual
    status = Column(String(16), nullable=False, default="fired")  # fired/failed
    error = Column(Text, nullable=False, default="")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)


class LocalLog(Base):
    """本地单机模式执行日志（线程级捕获引擎 logging 输出）。"""

    __tablename__ = "local_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ts = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)
    tenant_id = Column(String(64), nullable=False, default="", index=True)
    execution_id = Column(String(64), nullable=True, index=True)
    level = Column(String(16), nullable=False, default="INFO")
    logger = Column(String(128), nullable=False, default="")
    message = Column(Text, nullable=False, default="")
