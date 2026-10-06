"""PG 可移植性回归（第二轮）：消灭 `ensure_tenant_bootstrap` 路径上残留的 PRAGMA。

背景（P0：console 迁移 PostgreSQL 后启动仍崩）：
前一轮只给 ``_migrate_sqlite_columns()`` 加了 backend 守卫，但
``main.py:78`` 启动还会调 ``ensure_tenant_bootstrap()``——其内部经
``_sqlite_columns()`` **无条件执行** ``PRAGMA table_info(...)``（SQLite 专有），
在 PostgreSQL 上抛 ``ProgrammingError: syntax error at or near "PRAGMA"``。

修法（源头一处顶五处）：
1. ``_sqlite_columns()`` 加守卫：非 sqlite（或 ``_engine`` 为 None）返回 ``[]``；
   所有调用点（``_migrate_tenant_schema`` 的 :1004/:1011/:1019/:1027 与
   ``ensure_tenant_bootstrap`` 的 :1059）均以 ``if not cols`` / ``if cols``
   处理空结果，语义等价于「跳过」。
2. ``ensure_tenant_bootstrap()`` 直接早退（整段为 SQLite 存量迁移，PG 新库无需迁移）。

测试手法：伪引擎（``url.get_backend_name()`` 返回目标后端名，``begin()``
一旦被进入即抛 AssertionError）——无需装 PG 驱动、无需真实连接即可精确断言
「非 sqlite 路径完全不触碰 DB」。
"""
import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services import flow_store  # noqa: E402


class _FakeUrl:
    def __init__(self, backend: str) -> None:
        self._backend = backend

    def get_backend_name(self) -> str:
        return self._backend


class _SpyConn:
    """伪连接：任何 execute 都算「执行了 PRAGMA」——守卫正确时不应被调用。"""

    def __init__(self) -> None:
        self.execute_calls = 0

    def execute(self, *args, **kwargs):  # pragma: no cover - 守卫正确时不被调用
        self.execute_calls += 1
        raise AssertionError(
            "_sqlite_columns 在非 sqlite 后端执行了 SQL（PRAGMA 守卫缺失）"
        )


class _SpyEngine:
    """伪引擎：url 报告指定 backend；begin()/connect() 记录是否被进入。"""

    def __init__(self, backend: str) -> None:
        self.url = _FakeUrl(backend)
        self.begin_calls = 0

    def begin(self):  # pragma: no cover - 守卫正确时永不被调用
        self.begin_calls += 1
        raise AssertionError(
            f"backend={self.url.get_backend_name()} 上进入了 engine.begin()"
            "（守卫缺失，会执行 PRAGMA）"
        )


@pytest.fixture()
def restore_engine():
    """保存/还原全局 _engine，避免污染其它用例。"""
    saved = flow_store._engine
    saved_factory = flow_store._SessionLocal
    try:
        yield
    finally:
        flow_store._engine = saved
        flow_store._SessionLocal = saved_factory


# ---- 1. _sqlite_columns 源头守卫 ----

@pytest.mark.parametrize("backend", ["postgresql", "mysql", "mariadb", "mssql"])
def test_sqlite_columns_returns_empty_on_non_sqlite(monkeypatch, restore_engine, backend):
    """非 sqlite 后端：_sqlite_columns 必须返回 []，且不执行任何 SQL。"""
    spy = _SpyEngine(backend)
    monkeypatch.setattr(flow_store, "_engine", spy)
    conn = _SpyConn()

    assert flow_store._sqlite_columns(conn, "audit_logs") == []
    assert conn.execute_calls == 0, "非 sqlite 后端不得执行 PRAGMA"


def test_sqlite_columns_returns_empty_when_engine_none(monkeypatch, restore_engine):
    """_engine 为 None：_sqlite_columns 不得抛错，返回 []（不触碰 DB）。"""
    monkeypatch.setattr(flow_store, "_engine", None)
    conn = _SpyConn()

    assert flow_store._sqlite_columns(conn, "audit_logs") == []
    assert conn.execute_calls == 0


def test_sqlite_columns_still_reads_on_sqlite(monkeypatch, restore_engine, tmp_path):
    """回归：sqlite 上仍正常读出列名（守卫没把 sqlite 误伤）。"""
    engine = create_engine(f"sqlite:///{tmp_path / 'cols.db'}", future=True)
    monkeypatch.setattr(flow_store, "_engine", engine)
    with engine.begin() as conn:
        from sqlalchemy import text

        conn.execute(text("CREATE TABLE audit_logs (id TEXT PRIMARY KEY, actor TEXT)"))
        cols = flow_store._sqlite_columns(conn, "audit_logs")
    assert set(cols) == {"id", "actor"}


# ---- 2. ensure_tenant_bootstrap 早退 ----

@pytest.mark.parametrize("backend", ["postgresql", "mysql", "mariadb", "mssql"])
def test_ensure_tenant_bootstrap_noop_on_non_sqlite(monkeypatch, restore_engine, backend):
    """非 sqlite：ensure_tenant_bootstrap 直接 return，不碰 DB（不建 session）。"""
    spy = _SpyEngine(backend)
    monkeypatch.setattr(flow_store, "_engine", spy)

    def _boom(*args, **kwargs):  # pragma: no cover - 守卫正确时不被调用
        raise AssertionError(
            "非 sqlite 后端不应创建 Session（ensure_tenant_bootstrap 守卫缺失）"
        )

    monkeypatch.setattr(flow_store, "_SessionLocal", _boom)

    flow_store.ensure_tenant_bootstrap()
    assert spy.begin_calls == 0


def test_ensure_tenant_bootstrap_requires_engine(monkeypatch, restore_engine):
    """_engine 为 None 仍抛 RuntimeError（保持原契约，不因守卫改动而静默）。"""
    monkeypatch.setattr(flow_store, "_engine", None)
    with pytest.raises(RuntimeError):
        flow_store.ensure_tenant_bootstrap()


# ---- 3. sqlite 回归：正常路径行为不变 ----

def test_create_all_and_bootstrap_sqlite_path_unchanged(monkeypatch, restore_engine, tmp_path):
    """sqlite 路径端到端不变：create_all + ensure_tenant_bootstrap 正常且幂等。"""
    engine = create_engine(f"sqlite:///{tmp_path / 'boot.db'}", future=True)
    monkeypatch.setattr(flow_store, "_engine", engine)
    monkeypatch.setattr(
        flow_store, "_SessionLocal", flow_store.sessionmaker(bind=engine, expire_on_commit=False)
    )

    flow_store.create_all()
    flow_store.ensure_tenant_bootstrap()
    flow_store.ensure_tenant_bootstrap()  # 幂等

    with engine.begin() as conn:
        from sqlalchemy import text

        n = conn.execute(
            text("SELECT COUNT(*) FROM tenants WHERE id = :tid"),
            {"tid": flow_store.DEFAULT_TENANT_ID},
        ).scalar()
    assert n == 1, "sqlite 首启仍应创建 default 租户"


def test_ensure_tenant_bootstrap_backfills_tenant_on_sqlite(monkeypatch, restore_engine, tmp_path):
    """sqlite 回归：存量 tenant_id='' 的行仍被归入 default 租户。"""
    engine = create_engine(f"sqlite:///{tmp_path / 'boot2.db'}", future=True)
    monkeypatch.setattr(flow_store, "_engine", engine)
    monkeypatch.setattr(
        flow_store, "_SessionLocal", flow_store.sessionmaker(bind=engine, expire_on_commit=False)
    )
    flow_store.create_all()

    table = "audit_logs"
    with engine.begin() as conn:
        from sqlalchemy import text

        conn.execute(
            text(
                f"INSERT INTO {table} "
                "(id, ts, tenant_id, actor, action, resource, resource_id, detail_json, ip) "
                "VALUES (1, '2026-01-01 00:00:00', '', 'u', 'a', 'r', '1', 'null', '')"
            )
        )

    flow_store.ensure_tenant_bootstrap()

    with engine.begin() as conn:
        from sqlalchemy import text

        tid = conn.execute(text(f"SELECT tenant_id FROM {table} WHERE id=1")).scalar()
    assert tid == flow_store.DEFAULT_TENANT_ID
