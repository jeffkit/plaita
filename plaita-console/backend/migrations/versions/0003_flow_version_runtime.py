"""plaita#300：flow_versions.runtime 列（目标 runtime pool，go/python）。

存量库由 0002 认领后 ``upgrade head`` 执行本迁移；新库由 create_all 直接建出
列后再 stamp 基线。SQLite 与 PostgreSQL 都走 ``ADD COLUMN``，不需要后端分支。
默认 ``python``：console 的历史队列语义就是 Python worker 池，存量版本行为
不变（旧 flow 无 runtime 字段的默认策略，#300 验收⑥）。

Revision ID: 0003_flow_version_runtime
Revises: 0003_flow_publish_outbox
Create Date: 2026-10-22

注：本迁移原以 0002_audit_identity 为父，与 #299 的 0003_flow_publish_outbox
并发落地造成迁移图双 head（upgrade head 抛 MultipleHeads，二次启动降级
create_all-fallback，CI 三连红）。改挂 0003_flow_publish_outbox 线性化；
纯 ADD COLUMN 且幂等，重挂对已 stamp 任意版本的库都安全。
"""
from alembic import op
import sqlalchemy as sa

revision = "0003_flow_version_runtime"
down_revision = "0003_flow_publish_outbox"
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
