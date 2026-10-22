"""#302：flow_publish_outbox 补 drift_count 列（源表定义漂移对账计数）。

存量库由 0003 ``upgrade head`` 执行本迁移补列；新库由 create_all 直接建出
同名列后再 stamp 基线（因此 upgrade 必须**幂等**：列已存在即跳过，与 0002
的补列口径一致）。SQLite 与 PostgreSQL 都走 ``ADD COLUMN``，不需要后端分支。

``drift_count``：该行源表定义（``flow_versions.definition``）被 DB 级修改、
被对账以「源表现值 ≠ 行内指纹」命中的累计次数；/health.engine_outbox.
``drift_total`` 聚合全表。

Revision ID: 0004_outbox_drift_count
Revises: 0003_flow_publish_outbox
Create Date: 2026-10-22
"""
from alembic import op
import sqlalchemy as sa

revision = "0004_outbox_drift_count"
down_revision = "0003_flow_publish_outbox"
branch_labels = None
depends_on = None

_TABLE = "flow_publish_outbox"
_COLUMN = "drift_count"


def _existing_columns() -> set:
    inspector = sa.inspect(op.get_bind())
    if _TABLE not in inspector.get_table_names():
        return set()
    return {column["name"] for column in inspector.get_columns(_TABLE)}


def upgrade() -> None:
    existing = _existing_columns()
    if not existing or _COLUMN in existing:
        # 表不存在（异常态：链上 0003 必先建表）或列已存在：幂等跳过。
        return
    op.add_column(
        _TABLE,
        sa.Column(_COLUMN, sa.Integer(), nullable=False, server_default="0"),
    )


def downgrade() -> None:
    if _COLUMN not in _existing_columns():
        return
    with op.batch_alter_table(_TABLE) as batch_op:
        batch_op.drop_column(_COLUMN)
