"""Console flow store 的 schema 迁移引导（alembic）。

重点覆盖**存量库接管**：历史上 schema 由 create_all + 一次性补列脚本演进，
库里没有 alembic_version。引导必须能认领基线（不重放 DDL、不动业务数据），
否则升级路径上要么冲突、要么「以为迁移了其实没有」。
"""

import sqlite3
import sys
from pathlib import Path

import pytest

pytest.importorskip("alembic")
pytest.importorskip("sqlalchemy")

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services import flow_store as fs  # noqa: E402
from services import schema_migrations as sm  # noqa: E402


def _tables(db: Path) -> set:
    con = sqlite3.connect(db)
    try:
        return {r[0] for r in con.execute("select name from sqlite_master where type='table'")}
    finally:
        con.close()


def _make_legacy_db(db: Path) -> None:
    """造一个「历史库」：有业务表与数据，但没有 alembic_version。"""
    con = sqlite3.connect(db)
    try:
        # 贴近真实旧库形态：现行 schema 的 NOT NULL 列必须齐全——多租户迁移的
        # 重建 SQL 会按这些列 SELECT/INSERT，缺列会让对齐阶段失败（那是夹具失真，
        # 不是产品缺陷）。
        con.execute(
            "create table flows (id integer primary key autoincrement,"
            " flow_id text not null unique, author text not null, desc text not null,"
            " created_at datetime not null, updated_at datetime not null)"
        )
        con.execute(
            "create table flow_versions (id integer primary key autoincrement,"
            " flow_id text not null, version text not null, definition text not null,"
            " layout text not null, status text not null, created_at datetime not null,"
            " created_by text not null)"
        )
        con.execute(
            "insert into flows (flow_id, author, desc, created_at, updated_at)"
            " values ('legacy-flow', 'kong', '', '2026-01-01 00:00:00', '2026-01-01 00:00:00')"
        )
        con.execute(
            "insert into flow_versions (flow_id, version, definition, layout, status,"
            " created_at, created_by) values ('legacy-flow', '1.0.0', '{}', '{}', 'draft',"
            " '2026-01-01 00:00:00', 'kong')"
        )
        con.commit()
    finally:
        con.close()


class TestMigrationBootstrap:
    def test_empty_db_is_created_and_stamped(self, tmp_path):
        db = tmp_path / "fresh.db"
        engine = fs.init_engine(f"sqlite:///{db}")

        assert engine.schema_summary["action"] == "create_all+stamp"
        assert sm.current_revision(engine) == sm.BASELINE_REVISION
        # 业务表与版本表都应在
        tables = _tables(db)
        assert {"flows", "flow_versions", "alembic_version"} <= tables

    def test_legacy_db_is_adopted_without_replaying_ddl(self, tmp_path):
        db = tmp_path / "legacy.db"
        _make_legacy_db(db)

        engine = fs.init_engine(f"sqlite:///{db}")

        assert engine.schema_summary["action"] == "stamp-baseline"
        assert sm.current_revision(engine) == sm.BASELINE_REVISION
        # 认领基线**不得碰业务数据**
        con = sqlite3.connect(db)
        try:
            assert con.execute("select count(*) from flows").fetchone()[0] == 1
            assert con.execute("select count(*) from flow_versions").fetchone()[0] == 1
        finally:
            con.close()

    def test_second_boot_upgrades_cleanly(self, tmp_path):
        db = tmp_path / "twice.db"
        first = fs.init_engine(f"sqlite:///{db}")
        assert first.schema_summary["action"] == "create_all+stamp"

        second = fs.init_engine(f"sqlite:///{db}")
        assert second.schema_summary["action"] == "upgrade"
        assert sm.current_revision(second) == sm.BASELINE_REVISION

    def test_bootstrap_is_idempotent_many_times(self, tmp_path):
        db = tmp_path / "many.db"
        for _ in range(3):
            engine = fs.init_engine(f"sqlite:///{db}")
            assert sm.current_revision(engine) == sm.BASELINE_REVISION

    def test_migration_targets_the_engine_it_was_given(self, tmp_path):
        """回归：env.py 曾经只读 console 配置，导致迁移写到「配置指向的库」。

        症状很隐蔽——stamp 表面上成功，真实库却永远没有 alembic_version。
        """
        db = tmp_path / "explicit.db"
        engine = fs.init_engine(f"sqlite:///{db}")
        assert "alembic_version" in _tables(db)
        assert engine.schema_summary.get("action") != "create_all-fallback"

    def test_current_revision_is_none_without_version_table(self, tmp_path):
        from sqlalchemy import create_engine

        bare = create_engine(f"sqlite:///{tmp_path / 'bare.db'}", future=True)
        assert sm.current_revision(bare) is None
