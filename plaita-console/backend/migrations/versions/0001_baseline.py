"""baseline：当前 console flow store 的完整 schema。

存量库（由 create_all 建出、无 alembic_version 表）用 ``stamp 0001`` 认领，
不重放本迁移；新库直接 upgrade 到 0001。之后所有变更都必须是**独立的新 revision**，
并遵循 expand → backfill → 切读 → contract（见 docs：运维 runbook「无损升级」）。

Revision ID: 0001_baseline
Revises:
Create Date: 2026-10-07
"""
from alembic import op
import sqlalchemy as sa

revision = "0001_baseline"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 基线不重复 create_all 的全部 DDL：新库由 create_all 建表后 stamp 本版本，
    # 存量库同样 stamp。这里只保证「alembic 记录与真实 schema 对齐」。
    # 之所以不在 baseline 里重放 DDL：本仓历史上 create_all 与若干 SQLite
    # 补列脚本共同定义了现状，重放会与已存在的表冲突，且没有收益。
    pass


def downgrade() -> None:
    pass
