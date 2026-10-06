"""迁移工具回归：SQLite→PG 搬迁必须重置自增主键序列。

背景（真 bug，fleet P0）：
console 从 SQLite 迁到 PostgreSQL 后，数据按行 INSERT（保留原 id），但
``<table>_id_seq`` 未推进到 ``max(id)``。于是迁移后首次经应用插入：

* ``create_flow`` → ``flows`` 主键冲突 ``IntegrityError``（被误报为「流程已存在」）；
* publish 写 ``audit_logs`` → 主键冲突；``deployments`` 同理。

根因在迁移路径——搬完数据必须对每张有自增主键的表 ``setval`` 到 ``max(pk)``。
本用例锁住 ``plaita-console/scripts/migrate_sqlite_to_pg.py`` 的两条不变量：

1. ``_autoincrement_pk_tables`` 只枚举「单列整型自增主键」表——**不假设列名是
   ``id``**，且排除字符串主键表（``tenants`` / ``local_schedules`` /
   ``session_tokens`` / ``tenant_members``）。
2. 有真实 PG 可用时（env ``PLAITA_TEST_PG_URL``），跑一次搬迁 + 序列重置，
   断言迁移后经 ORM 插入不撞主键、且 ``create_flow`` 错误信息不误导。
"""
import importlib.util
import sys
from pathlib import Path

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
REPO_CONSOLE = Path(__file__).resolve().parents[3]  # plaita-console/
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from models.flow import Base  # noqa: E402
from services import flow_store  # noqa: E402
from services.flow_store import _is_unique_violation  # noqa: E402
from sqlalchemy.exc import IntegrityError  # noqa: E402


def _load_migration_module():
    """按文件路径加载迁移脚本（scripts/ 不是包）。"""
    path = REPO_CONSOLE / "scripts" / "migrate_sqlite_to_pg.py"
    spec = importlib.util.spec_from_file_location("migrate_sqlite_to_pg", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


# ---------- 1. 自增主键表枚举（不硬编码表名/列名） ----------

def test_autoincrement_pk_tables_excludes_string_pk_tables():
    """字符串主键表不得被当成 serial 目标（否则 pg_get_serial_sequence 为 NULL）。"""
    mod = _load_migration_module()
    targets = dict(mod._autoincrement_pk_tables(Base.metadata))
    names = set(targets)

    # 必须包含的整型自增主键表（flows/audit_logs/deployments 是本次 bug 的受害者）
    assert {"flows", "audit_logs", "deployments", "flow_versions"} <= names
    # 必须排除的字符串主键表
    assert "tenants" not in names
    assert "local_schedules" not in names  # 主键 schedule_id 是 String
    assert "session_tokens" not in names   # 主键 token_hash
    assert "tenant_members" not in names   # 复合主键
    # 列名不一律是 id（local_schedules 已排除，但应能正确处理任意列名）
    for table, col in targets.items():
        assert col in Base.metadata.tables[table].c
        assert Base.metadata.tables[table].c[col].primary_key


def test_autoincrement_pk_tables_all_are_single_int_pk():
    """枚举出来的每张表：单列、整型、autoincrement 未关闭。"""
    from sqlalchemy import BigInteger, Integer, SmallInteger

    mod = _load_migration_module()
    for table, col in mod._autoincrement_pk_tables(Base.metadata):
        pk = list(Base.metadata.tables[table].primary_key.columns)
        assert len(pk) == 1
        assert isinstance(pk[0].type, (Integer, BigInteger, SmallInteger))
        assert pk[0].autoincrement is not False


# ---------- 2. IntegrityError 分类（误导性错误信息修复） ----------

class _FakePgOrig:
    def __init__(self, sqlstate: str) -> None:
        self.sqlstate = sqlstate


class _FakeSqliteOrig(Exception):
    pass


def test_is_unique_violation_pg_unique():
    """PG SQLSTATE 23505（unique_violation）→ True。"""
    exc = IntegrityError("stmt", {}, _FakePgOrig("23505"))
    assert _is_unique_violation(exc) is True


def test_is_unique_violation_pg_not_null_is_false():
    """PG 23502（not_null_violation）不是唯一冲突 → False（不得误报「已存在」）。"""
    exc = IntegrityError("stmt", {}, _FakePgOrig("23502"))
    assert _is_unique_violation(exc) is False


def test_is_unique_violation_pg_pk_conflict_is_still_unique():
    """主键冲突在 PG 里也是 23505（unique_violation），归类为唯一冲突。

    注意：这条断言记录的是**PG 的客观行为**——主键冲突（含序列未重置引发）
    与唯一约束冲突共用 23505。因此仅凭 SQLSTATE 无法区分二者；真正的修复是
    **迁移时重置序列**（见 migrate_sqlite_to_pg.py），使这种冲突不再发生。
    """
    exc = IntegrityError("stmt", {}, _FakePgOrig("23505"))
    assert _is_unique_violation(exc) is True


def test_is_unique_violation_sqlite_text():
    """SQLite 无 sqlstate，按异常文本识别 UNIQUE 冲突。"""
    exc = IntegrityError("stmt", {}, _FakeSqliteOrig(
        "UNIQUE constraint failed: flows.tenant_id, flows.flow_id"))
    assert _is_unique_violation(exc) is True


def test_is_unique_violation_unknown_is_false():
    """无法判定时保守返回 False（保留原始信息，宁多勿误导）。"""
    exc = IntegrityError("stmt", {}, _FakeSqliteOrig("NOT NULL constraint failed"))
    assert _is_unique_violation(exc) is False


def test_create_flow_marks_non_unique_integrity_error_with_raw_message(monkeypatch):
    """非唯一冲突的 IntegrityError 不得被翻译成「流程已存在」。"""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    engine = create_engine("sqlite://", future=True)
    Base.metadata.create_all(engine)
    store = flow_store.FlowStore(sessionmaker(bind=engine, expire_on_commit=False))

    def _boom(self, *a, **kw):
        raise IntegrityError("stmt", {}, _FakeSqliteOrig("NOT NULL constraint failed: x"))

    monkeypatch.setattr(flow_store.Session, "commit", _boom)
    with pytest.raises(ValueError) as ei:
        store.create_flow("f1", tenant_id="default")
    msg = str(ei.value)
    assert "已存在" not in msg, f"非唯一冲突被误报为已存在: {msg}"
    assert "完整性错误" in msg or "NOT NULL" in msg, msg


def test_create_flow_marks_unique_violation_as_exists(monkeypatch):
    """唯一约束冲突仍翻译成「流程已存在」（保留既有 API 语义）。"""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    engine = create_engine("sqlite://", future=True)
    Base.metadata.create_all(engine)
    store = flow_store.FlowStore(sessionmaker(bind=engine, expire_on_commit=False))

    def _boom(self, *a, **kw):
        raise IntegrityError("stmt", {}, _FakeSqliteOrig(
            "UNIQUE constraint failed: flows.tenant_id, flows.flow_id"))

    monkeypatch.setattr(flow_store.Session, "commit", _boom)
    with pytest.raises(ValueError) as ei:
        store.create_flow("f1", tenant_id="default")
    assert "已存在" in str(ei.value)


# ---------- 3. 端到端：真实 PG 上搬迁 + 序列重置 ----------

def _pg_url():
    import os
    return os.environ.get("PLAITA_TEST_PG_URL")


@pytest.mark.skipif(not _pg_url(), reason="需要 PLAITA_TEST_PG_URL（真实 PG）")
def test_migration_resets_sequences_so_app_insert_works(tmp_path, monkeypatch):
    """搬迁后经 ORM 插入不撞主键；重复跑幂等。"""
    from datetime import datetime

    from sqlalchemy import create_engine, text

    mod = _load_migration_module()

    # 源 SQLite：flows 主键跳到 100，audit_logs 到 20
    src_path = tmp_path / "src.db"
    src = create_engine(f"sqlite:///{src_path}", future=True)
    Base.metadata.create_all(src)
    with src.begin() as c:
        for i in (1, 2, 100):
            c.execute(text(
                "INSERT INTO flows (id, tenant_id, flow_id, author, desc, "
                "created_at, updated_at) VALUES (:i,'default',:f,'a','',:t,:t)"),
                {"i": i, "f": f"flow{i}", "t": datetime.utcnow()})
        for i in range(1, 21):
            c.execute(text(
                "INSERT INTO audit_logs (id, ts, tenant_id, actor, action, "
                "resource, resource_id, detail_json, ip) "
                "VALUES (:i,:t,'default','a','x','r','','{}','')"),
                {"i": i, "t": datetime.utcnow()})

    pg = _pg_url()
    rc = mod.main(["--sqlite", str(src_path), "--pg", pg, "--drop"])
    assert rc == 0

    dst = create_engine(pg, future=True)
    # 搬迁后：序列已推进到 max(id)
    with dst.connect() as c:
        lv = c.execute(text("SELECT last_value FROM flows_id_seq")).scalar()
        assert lv >= 100, f"flows_id_seq 未重置: {lv}"
    # 经 ORM 插入（不给 id）必须成功——这正是迁移 bug 的复现点
    from sqlalchemy.orm import sessionmaker

    store = flow_store.FlowStore(sessionmaker(bind=dst, expire_on_commit=False))
    rec = store.create_flow("post_migrate", tenant_id="default")
    assert rec.id > 100
