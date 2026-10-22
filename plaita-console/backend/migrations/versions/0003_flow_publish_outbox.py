"""#299：flow_publish_outbox（跨 Redis 部署的 durable 发布投递链）。

存量库由 0002 ``upgrade head`` 执行本迁移建表；新库由 create_all 直接建出
同 schema 后 stamp 基线（因此 upgrade 必须**幂等**：表已存在即跳过，
与 0002 的补列口径一致）。

唯一约束 ``uq_outbox_delivery_key`` = 幂等键
``(tenant_id, flow_id, version, definition_hash)``：同版本同内容重复发布
不再入队；同版本换了内容（hash 不同）视为新投递。索引服务投递扫描
（status）与租户视角查询（tenant_id）。

Revision ID: 0003_flow_publish_outbox
Revises: 0002_audit_identity
Create Date: 2026-10-22
"""
from alembic import op
import sqlalchemy as sa

revision = "0003_flow_publish_outbox"
down_revision = "0002_audit_identity"
branch_labels = None
depends_on = None

_TABLE = "flow_publish_outbox"


def _table_exists() -> bool:
    inspector = sa.inspect(op.get_bind())
    return _TABLE in inspector.get_table_names()


def upgrade() -> None:
    if _table_exists():
        return
    op.create_table(
        _TABLE,
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("tenant_id", sa.String(length=64), nullable=False, server_default=""),
        sa.Column("flow_id", sa.String(length=128), nullable=False),
        sa.Column("version", sa.String(length=64), nullable=False),
        sa.Column("definition_hash", sa.String(length=64), nullable=False),
        sa.Column("definition", sa.Text(), nullable=False, server_default=""),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="pending"),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text(), nullable=False, server_default=""),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("delivered_at", sa.DateTime(), nullable=True),
        sa.Column("next_attempt_at", sa.DateTime(), nullable=True),
        sa.UniqueConstraint(
            "tenant_id", "flow_id", "version", "definition_hash",
            name="uq_outbox_delivery_key",
        ),
    )
    op.create_index("ix_flow_publish_outbox_tenant_id", _TABLE, ["tenant_id"])
    op.create_index("ix_flow_publish_outbox_status", _TABLE, ["status"])


def downgrade() -> None:
    if not _table_exists():
        return
    op.drop_index("ix_flow_publish_outbox_status", table_name=_TABLE)
    op.drop_index("ix_flow_publish_outbox_tenant_id", table_name=_TABLE)
    op.drop_table(_TABLE)
