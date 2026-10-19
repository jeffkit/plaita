"""A55-S3：审计身份列（auth_source / key_fingerprint / request_id / role_snapshot）。

存量库由 0001 认领基线后 ``upgrade head`` 执行本迁移；新库由 create_all 直接
建出同名列后再 stamp 基线。SQLite 与 PostgreSQL 都走 ``ADD COLUMN``，因此
不需要后端分支。

Revision ID: 0002_audit_identity
Revises: 0001_baseline
Create Date: 2026-10-19
"""
from alembic import op
import sqlalchemy as sa

revision = "0002_audit_identity"
down_revision = "0001_baseline"
branch_labels = None
depends_on = None


_COLUMNS = (
    ("auth_source", sa.String(length=16)),
    ("key_fingerprint", sa.String(length=32)),
    ("request_id", sa.String(length=64)),
    ("role_snapshot", sa.String(length=16)),
)


def _existing_columns() -> set:
    """幂等：空库由 create_all 建全新 schema 后 stamp 基线，列已经存在。"""
    inspector = sa.inspect(op.get_bind())
    return {column["name"] for column in inspector.get_columns("audit_logs")}


def upgrade() -> None:
    existing = _existing_columns()
    for name, column_type in _COLUMNS:
        if name in existing:
            continue
        op.add_column(
            "audit_logs",
            sa.Column(name, column_type, nullable=False, server_default=""),
        )


def downgrade() -> None:
    existing = _existing_columns()
    with op.batch_alter_table("audit_logs") as batch_op:
        for name, _ in reversed(_COLUMNS):
            if name in existing:
                batch_op.drop_column(name)
