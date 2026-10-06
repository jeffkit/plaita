"""flow_versions 外键必须是**复合外键** (tenant_id, flow_id) → flows。

背景（阻塞 console 迁移到 PostgreSQL 的 P0）：
``FlowRecord``（表 ``flows``）对 ``flow_id`` 只有**复合唯一**约束
``uq_tenant_flow`` = (tenant_id, flow_id)，**没有单列 flow_id 唯一**。
多租户设计下 ``flow_id`` 是「租户内唯一」，不同租户可以同名。

但 ``FlowVersion.flow_id`` 原先内联 ``ForeignKey("flows.flow_id")`` 指向
**单列**。后果有二：

1. **PostgreSQL 建表直接失败**：PG 要求外键目标列上有唯一约束，
   而 ``flows.flow_id`` 无单列唯一 →
   ``psycopg.errors.InvalidForeignKey: there is no unique constraint
   matching given keys for referenced table "flows"``。
   （SQLite 默认不强制外键，长期掩盖此缺陷。）
2. **跨租户串数据**：单列外键只锚定 flow_id，tenantB 的版本可以被指到
   tenantA 的同名 flow 上——语义错误。

修法：把外键改为复合外键 (tenant_id, flow_id) → flows(tenant_id, flow_id)，
命中已有的 ``uq_tenant_flow`` 唯一约束，则 PG 满足要求且语义正确。

本文件覆盖**不连 PG** 也能验证的模型定义断言（CI 默认跑），以及一个
连真实 PG 的端到端用例（无 ``PG_TEST_DSN`` 环境变量时跳过）。
"""
import os
import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.schema import ForeignKeyConstraint, UniqueConstraint

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from models.flow import Base, FlowRecord, FlowVersion  # noqa: E402


# --------------------------------------------------------------------------
# 模型定义断言（不需要连库）
# --------------------------------------------------------------------------

def _flow_version_fk() -> ForeignKeyConstraint:
    """取 flow_versions 上唯一的外键约束（应为复合两列）。"""
    constraints = [c for c in FlowVersion.__table__.constraints
                   if isinstance(c, ForeignKeyConstraint)]
    assert len(constraints) == 1, f"期望恰好 1 个 ForeignKeyConstraint，实际 {len(constraints)}"
    return constraints[0]


def test_flow_versions_fk_targets_two_columns():
    """flow_versions 的外键必须是复合两列 (tenant_id, flow_id)。"""
    fk = _flow_version_fk()
    pairs = [(e.parent.name, e.column.name) for e in fk.elements]
    assert len(pairs) == 2, f"外键应为 2 列，实际: {pairs}"
    assert ("tenant_id", "tenant_id") in pairs, f"缺少 tenant_id 映射: {pairs}"
    assert ("flow_id", "flow_id") in pairs, f"缺少 flow_id 映射: {pairs}"
    assert sorted(col.name for col in fk.columns) == ["flow_id", "tenant_id"]


def test_flow_versions_fk_references_flows_table():
    """外键目标表必须是 flows（复合外键命中 uq_tenant_flow）。"""
    fk = _flow_version_fk()
    targets = {e.column.table.name for e in fk.elements}
    assert targets == {"flows"}, f"外键目标表应为 flows，实际: {targets}"


def test_flow_versions_fk_is_table_level_constraint_named():
    """复合外键应以具名 Table 级 ForeignKeyConstraint 形式声明。"""
    fk = _flow_version_fk()
    assert fk.name == "fk_flow_versions_flow"
    assert fk.ondelete == "CASCADE"


def test_no_inline_single_column_fk_remains_in_flow_versions():
    """不得再有指向单列 flows.flow_id 的内联 ForeignKey（避免重复/回退）。

    单列外键的判据：出现只锚定 flow_id 一个元素的 ForeignKeyConstraint。
    """
    fk = _flow_version_fk()
    cols = [e.parent.name for e in fk.elements]
    assert cols != ["flow_id"], (
        "flow_versions.flow_id 仍是单列外键——PG 会因 flows.flow_id 无唯一约束而建表失败"
    )


def test_flows_has_composite_unique_covering_fk_target():
    """FK 目标 (tenant_id, flow_id) 上必须有唯一约束（PG 硬性要求）。"""
    uniques = [
        tuple(c.name for c in uc.columns)
        for uc in FlowRecord.__table__.constraints
        if isinstance(uc, UniqueConstraint)
    ]
    assert ("tenant_id", "flow_id") in uniques, f"flows 缺少 (tenant_id, flow_id) 唯一: {uniques}"
    # 且必须没有单列 flow_id 唯一（否则说明设计被改成全局唯一）
    assert ("flow_id",) not in uniques


def test_flow_versions_has_tenant_id_column():
    """复合外键需要 flow_versions.tenant_id 列存在且 NOT NULL。"""
    col = FlowVersion.__table__.c.tenant_id
    assert col is not None
    assert not col.nullable


# --------------------------------------------------------------------------
# SQLite 回归：create_all 仍成功（默认就跑，不依赖外部库）
# --------------------------------------------------------------------------

def test_create_all_on_sqlite_still_works(tmp_path):
    """回归：修复后 SQLite 上 create_all 必须照常成功，且两表都在。"""
    engine = create_engine(f"sqlite:///{tmp_path / 'reg.db'}", future=True)
    Base.metadata.create_all(engine)
    insp = __import__("sqlalchemy").inspect(engine)
    names = set(insp.get_table_names())
    assert {"flows", "flow_versions"} <= names


def test_composite_fk_enforced_on_sqlite_with_pragma(tmp_path):
    """开启 SQLite 外键强制后，复合外键应真实生效（不存在父行则插入失败）。

    这条同时证明修复没有把 SQLite 路径弄坏，且外键在 SQLite 上确实可强制。
    """
    from sqlalchemy import text
    from sqlalchemy.exc import IntegrityError

    engine = create_engine(f"sqlite:///{tmp_path / 'enforce.db'}", future=True)
    Base.metadata.create_all(engine)

    with engine.begin() as conn:
        conn.execute(text("PRAGMA foreign_keys=ON"))
        # 没有 flows 行 → 复合外键应拒绝
        with pytest.raises(IntegrityError):
            conn.execute(
                text(
                    "INSERT INTO flow_versions (tenant_id, flow_id, version, status,"
                    " definition, layout, created_at, created_by)"
                    " VALUES ('t1','nope','v1','draft','','',datetime('now'),'')"
                )
            )


# --------------------------------------------------------------------------
# 真实 PG 端到端（无 PG_TEST_DSN 时跳过）
# --------------------------------------------------------------------------

PG_DSN = os.environ.get("PG_TEST_DSN")
pg_required = pytest.mark.skipif(
    not PG_DSN, reason="需要 PG_TEST_DSN 环境变量（真实 PostgreSQL）"
)


@pg_required
def test_create_all_on_postgres_and_fk_is_two_columns():
    """真实 PG：create_all 成功，且 flow_versions 的外键指向 flows 的 2 列。"""
    from sqlalchemy import text

    engine = create_engine(PG_DSN, future=True)
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))
    Base.metadata.create_all(engine)

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT tc.constraint_name, kcu.column_name, ccu.table_name AS ref_table,
                       ccu.column_name AS ref_column
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage ccu
                  ON tc.constraint_name = ccu.constraint_name
                 AND tc.table_schema = ccu.table_schema
                WHERE tc.table_name = 'flow_versions'
                  AND tc.constraint_type = 'FOREIGN KEY'
                """
            )
        ).fetchall()

    pairs = {(r[1], r[3]) for r in rows}
    assert ("tenant_id", "tenant_id") in pairs, f"PG 外键缺少 tenant_id: {pairs}"
    assert ("flow_id", "flow_id") in pairs, f"PG 外键缺少 flow_id: {pairs}"
    assert {r[2] for r in rows} == {"flows"}


@pg_required
def test_cross_tenant_same_flow_id_coexists_on_postgres():
    """真实 PG：两个租户同名 flow_id + 各自版本不冲突，且版本归属各自租户。"""
    from sqlalchemy import text

    engine = create_engine(PG_DSN, future=True)
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))
    Base.metadata.create_all(engine)

    with engine.begin() as conn:
        for tenant in ("tenantA", "tenantB"):
            conn.execute(
                text(
                    "INSERT INTO flows (tenant_id, flow_id, author, \"desc\", created_at, updated_at)"
                    " VALUES (:t, 'dup', '', '', now(), now())"
                ),
                {"t": tenant},
            )
            conn.execute(
                text(
                    "INSERT INTO flow_versions (tenant_id, flow_id, version, status,"
                    " definition, layout, created_at, created_by)"
                    " VALUES (:t, 'dup', 'v1', 'draft', '', '', now(), '')"
                ),
                {"t": tenant},
            )

    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT tenant_id, count(*) FROM flow_versions GROUP BY tenant_id ORDER BY tenant_id")
        ).fetchall()
    assert rows == [("tenantA", 1), ("tenantB", 1)], f"版本归属错误: {rows}"

    # 跨租户的孤儿版本必须被拒（复合外键语义）
    with engine.begin() as conn:
        with pytest.raises(Exception):
            conn.execute(
                text(
                    "INSERT INTO flow_versions (tenant_id, flow_id, version, status,"
                    " definition, layout, created_at, created_by)"
                    " VALUES ('tenantC', 'dup', 'v1', 'draft', '', '', now(), '')"
                )
            )
