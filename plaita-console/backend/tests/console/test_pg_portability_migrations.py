"""PG 可移植性回归：SQLite 专有迁移不得在非 SQLite 后端执行。

背景（阻塞 console 迁移到 PostgreSQL 的 P0）：
``flow_store.create_all()`` 在应用启动时无条件调用
``_migrate_sqlite_columns()``，而该函数体直接执行 ``PRAGMA table_info(...)``
（SQLite 专有），**没有 backend 守卫**——在 PostgreSQL 引擎上会抛
``ProgrammingError: syntax error at or near "PRAGMA"``，即启动即崩。

对照：同文件 ``_migrate_tenant_schema()`` 已有正确守卫
``if _engine is None or _engine.url.get_backend_name() != "sqlite": return``。
本用例断言给 ``_migrate_sqlite_columns()`` 补上同样形式的守卫，且 sqlite
路径行为不变（回归）。

测试手法：把 ``flow_store._engine`` 换成一个「伪引擎」，其
``url.get_backend_name()`` 返回目标后端名，``begin()`` 记录是否被进入。
守卫缺失时 ``begin()`` 会被进入（即尝试执行 PRAGMA），守卫存在时不会。
这样无需安装 PG 驱动、也无需真实连接即可复现并验证。
"""
import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services import flow_store  # noqa: E402


class _FakeDbApi:
    """仅用于让 URL.get_backend_name() 返回期望后端名。"""

    def __init__(self, name: str) -> None:
        self.name = name


class _FakeUrl:
    def __init__(self, backend: str) -> None:
        self._backend = backend

    def get_backend_name(self) -> str:
        return self._backend


class _SpyEngine:
    """伪引擎：url 报告指定 backend；begin() 记录/抛错（若被调用）。"""

    def __init__(self, backend: str) -> None:
        self.url = _FakeUrl(backend)
        self.begin_calls = 0

    def begin(self):  # pragma: no cover - 守卫正确时永不被调用
        self.begin_calls += 1
        raise AssertionError(
            f"_migrate_sqlite_columns 在 backend={self.url.get_backend_name()} 上"
            "进入了 engine.begin()（守卫缺失，会执行 PRAGMA）"
        )


@pytest.fixture()
def restore_engine():
    """保存/还原全局 _engine，避免污染其它用例。"""
    saved = flow_store._engine
    try:
        yield
    finally:
        flow_store._engine = saved


def test_migrate_sqlite_columns_is_noop_on_postgres(monkeypatch, restore_engine):
    """P0：postgres 引擎上 _migrate_sqlite_columns 必须直接 return，不碰 engine。"""
    spy = _SpyEngine("postgresql")
    monkeypatch.setattr(flow_store, "_engine", spy)

    # 守卫存在 => 直接 return；守卫缺失 => spy.begin() 抛 AssertionError。
    flow_store._migrate_sqlite_columns()

    assert spy.begin_calls == 0, "非 sqlite 后端不得执行任何 SQL（PRAGMA）"


@pytest.mark.parametrize("backend", ["postgresql", "mysql", "mariadb", "mssql"])
def test_migrate_sqlite_columns_noop_across_non_sqlite_backends(
    monkeypatch, restore_engine, backend
):
    """任何非 sqlite 后端都应被守卫拦下。"""
    spy = _SpyEngine(backend)
    monkeypatch.setattr(flow_store, "_engine", spy)

    flow_store._migrate_sqlite_columns()

    assert spy.begin_calls == 0


def test_migrate_sqlite_columns_still_runs_on_sqlite(monkeypatch, restore_engine, tmp_path):
    """回归：sqlite 后端仍照常执行迁移（补列），不得被守卫误伤。

    用真实 sqlite 文件库：先建表（无 updated_at 的旧形态难构造，直接建全表），
    断言函数在 sqlite 上进入 engine.begin()（begin_calls 语义由真实引擎承担），
    且幂等不抛错。
    """
    db_file = tmp_path / "legacy.db"
    engine = create_engine(f"sqlite:///{db_file}", future=True)
    monkeypatch.setattr(flow_store, "_engine", engine)

    # 真实 sqlite：函数应正常执行（不抛异常），幂等。
    flow_store._migrate_sqlite_columns()
    flow_store._migrate_sqlite_columns()


def test_migrate_sqlite_columns_adds_missing_column_on_sqlite(monkeypatch, restore_engine, tmp_path):
    """回归：sqlite 上确实补出缺失列（证明守卫没把 sqlite 一起关掉）。"""
    from sqlalchemy import text

    db_file = tmp_path / "legacy2.db"
    engine = create_engine(f"sqlite:///{db_file}", future=True)
    with engine.begin() as conn:
        # 模拟旧库：local_executions 存在但缺 context_json 列
        conn.execute(text("CREATE TABLE local_executions (id TEXT PRIMARY KEY)"))
    monkeypatch.setattr(flow_store, "_engine", engine)

    flow_store._migrate_sqlite_columns()

    with engine.begin() as conn:
        cols = {r[1] for r in conn.execute(text("PRAGMA table_info(local_executions)")).fetchall()}
    assert "context_json" in cols, "sqlite 迁移路径应仍补出 context_json"
