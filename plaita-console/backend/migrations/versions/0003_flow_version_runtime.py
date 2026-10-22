"""plaita#300：flow_versions.runtime 列（目标 runtime pool，go/python）。

存量库由 0002 认领后 ``upgrade head`` 执行本迁移；新库由 create_all 直接建出
列后再 stamp 基线。SQLite 与 PostgreSQL 都走 ``ADD COLUMN``，不需要后端分支。
默认 ``python``：console 的历史队列语义就是 Python worker 池，存量版本行为
不变（旧 flow 无 runtime 字段的默认策略，#300 验收⑥）。

Revision ID: 0003_flow_version_runtime
Revises: 0002_audit_identity
Create Date: 2026-10-22
"""
from alembic import op
import sqlalchemy as sa

revision = "0003_flow_version_runtime"
down_revision = "0002_audit_identity"
branch_labels = None
depends_on = None


def _existing_columns() -> set:
    """幂等：空库由 create_all 建全新 schema 后 stamp 基线，列已经存在。"""
    inspector = sa.inspect(op.get_bind())
    return {column["name"] for column in inspector.get_columns("flow_versions")}


def upgrade() -> None:
    if "runtime" in _existing_columns():
        return
    op.add_column(
        "flow_versions",
        sa.Column("runtime", sa.String(length=16), nullable=False,
                  server_default="python"),
    )


def downgrade() -> None:
    if "runtime" not in _existing_columns():
        return
    with op.batch_alter_table("flow_versions") as batch_op:
        batch_op.drop_column("runtime")
