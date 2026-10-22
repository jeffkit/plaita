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


def _head_revision() -> str:
    """当前迁移 head（避免每加一条 revision 都改测试里的硬编码）。"""
    from alembic.config import Config
    from alembic.script import ScriptDirectory

    cfg = Config()
    cfg.set_main_option("script_location", str(sm._MIGRATIONS_DIR))
    return ScriptDirectory.from_config(cfg).get_current_head()


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
        assert sm.current_revision(second) == _head_revision()

    def test_bootstrap_is_idempotent_many_times(self, tmp_path):
        db = tmp_path / "many.db"
        for index in range(3):
            engine = fs.init_engine(f"sqlite:///{db}")
            # 首次认领基线；之后每次启动 upgrade 到 head 并保持不变。
            expected = sm.BASELINE_REVISION if index == 0 else _head_revision()
            assert sm.current_revision(engine) == expected

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


class TestFlowPublishOutboxMigration:
    """#299：flow_publish_outbox 三条引导路径都必须长出该表。

    - 空库：create_all（模型在 Base.metadata）+ stamp 基线；
    - 存量库（无 alembic_version）：create_all 补齐缺失表 + stamp 基线
      （**不会** upgrade——0003 的 upgrade 分支对这些库不执行）；
    - 已登记库：upgrade head 执行 0003（表已存在时幂等跳过）。
    """

    def test_fresh_db_has_outbox_table(self, tmp_path):
        db = tmp_path / "fresh-outbox.db"
        engine = fs.init_engine(f"sqlite:///{db}")
        assert "flow_publish_outbox" in _tables(db)
        # 空库走 create_all+stamp：schema 已是全量，版本停在基线（既有语义）
        assert sm.current_revision(engine) == sm.BASELINE_REVISION

    def test_legacy_db_gets_outbox_table_via_create_all(self, tmp_path):
        db = tmp_path / "legacy-outbox.db"
        _make_legacy_db(db)
        engine = fs.init_engine(f"sqlite:///{db}")
        assert engine.schema_summary["action"] == "stamp-baseline"
        assert "flow_publish_outbox" in _tables(db)

    def test_stamped_db_upgrades_outbox_table(self, tmp_path):
        """0002 → 0003 真实升级路径：删表 + 版本号回拨，再启动应补回。"""
        db = tmp_path / "upgrade-outbox.db"
        fs.init_engine(f"sqlite:///{db}")
        con = sqlite3.connect(db)
        try:
            con.execute("drop table flow_publish_outbox")
            con.execute(
                "update alembic_version set version_num='0002_audit_identity'"
            )
            con.commit()
        finally:
            con.close()

        engine = fs.init_engine(f"sqlite:///{db}")
        assert engine.schema_summary["action"] == "upgrade"
        assert "flow_publish_outbox" in _tables(db)
        assert sm.current_revision(engine) == _head_revision()
        # 幂等：表已存在时再次 upgrade 不炸（create_all+stamp 路径的库二启）
        again = fs.init_engine(f"sqlite:///{db}")
        assert sm.current_revision(again) == _head_revision()


class TestOutboxDriftCountMigration:
    """#302：flow_publish_outbox.drift_count 三条引导路径都必须长出该列。

    - 空库：create_all（模型含新列）+ stamp 基线；
    - 已登记 0003 的存量库：upgrade head 执行 0004 补列（幂等）。
    """

    def test_fresh_db_has_drift_count_column(self, tmp_path):
        db = tmp_path / "fresh-drift.db"
        fs.init_engine(f"sqlite:///{db}")
        con = sqlite3.connect(db)
        try:
            cols = {r[1] for r in con.execute("pragma table_info(flow_publish_outbox)")}
        finally:
            con.close()
        assert "drift_count" in cols

    def test_stamped_0003_db_upgrades_drift_count(self, tmp_path):
        """0003 → 0004 真实升级路径：去列 + 版本号回拨，再启动应补回。"""
        db = tmp_path / "upgrade-drift.db"
        fs.init_engine(f"sqlite:///{db}")
        con = sqlite3.connect(db)
        try:
            con.execute("alter table flow_publish_outbox drop column drift_count")
            con.execute(
                "update alembic_version set version_num='0003_flow_publish_outbox'"
            )
            con.commit()
        finally:
            con.close()

        engine = fs.init_engine(f"sqlite:///{db}")
        assert engine.schema_summary["action"] == "upgrade"
        con = sqlite3.connect(db)
        try:
            cols = {r[1] for r in con.execute("pragma table_info(flow_publish_outbox)")}
        finally:
            con.close()
        assert "drift_count" in cols
        assert sm.current_revision(engine) == _head_revision()
        # 幂等：列已存在时再次 upgrade 不炸
        again = fs.init_engine(f"sqlite:///{db}")
        assert sm.current_revision(again) == _head_revision()
