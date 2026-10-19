"""A2R20-06/07：启动路径有界（回填 pipelining/deadline + PG connect timeout）。

负控（修复前会红）：
- 回填 3 次 Redis 往返/版本，2,000 版本 6,000 次串行往返；
- 无 deadline/版本上限，Redis 半开时启动永不完成；
- ``create_engine`` 无 connect timeout，PG 黑洞端点 ``init_engine`` >8s 阻塞，
  且连接类失败会回退 ``create_all`` 再阻塞一次。

判据：往返数随**批次数**增长而非版本数；触界/连接失败都留可观测状态；
PG 黑洞在声明超时（本测试 1s）内具名失败，SQLite 零变化。
"""
from __future__ import annotations

import socket
import threading
import time
from types import SimpleNamespace

import pytest
import redis
from fakeredis import FakeRedis
from sqlalchemy.exc import OperationalError

from services import engine_sync, flow_store
from plaita.tenant_context import (
    DEFAULT_TENANT_ID,
    TENANT_SOURCE_EXPLICIT,
    set_current_tenant,
)


@pytest.fixture(autouse=True)
def _explicit_tenant_context():
    set_current_tenant(DEFAULT_TENANT_ID, source=TENANT_SOURCE_EXPLICIT)


class CountingRedis:
    """透明代理：记录 pipeline 次数（一次 pipeline = 一次 Redis 往返）。"""

    def __init__(self, inner: FakeRedis) -> None:
        self.inner = inner
        self.pipeline_calls = 0

    def pipeline(self, *args, **kwargs):
        self.pipeline_calls += 1
        return self.inner.pipeline(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(self.inner, name)


class ListedStore:
    """鸭子类型 store：直接产出已发布版本（不经过 SQL）。"""

    def __init__(self, count: int, versions_per_flow: int = 1) -> None:
        self.count = count
        self.versions_per_flow = versions_per_flow

    def iter_published_versions(self, tenant_id=None):
        for i in range(self.count):
            flow_id = f"f{i // self.versions_per_flow}"
            version = f"1.0.{i % self.versions_per_flow}"
            yield ("default", flow_id, version, '{"nodes": []}')


class _SlowPipeline:
    def __init__(self, delay: float) -> None:
        self.delay = delay

    def set(self, *args, **kwargs):
        return None

    def sadd(self, *args, **kwargs):
        return None

    def execute(self):
        time.sleep(self.delay)
        return []


class SlowRedis:
    """每次 pipeline 执行固定耗时，用来触发墙钟预算。"""

    def __init__(self, delay: float) -> None:
        self.delay = delay
        self.pipeline_calls = 0

    def pipeline(self, *args, **kwargs):
        self.pipeline_calls += 1
        return _SlowPipeline(self.delay)


class _FailingPipeline:
    def set(self, *args, **kwargs):
        return None

    def sadd(self, *args, **kwargs):
        return None

    def execute(self):
        raise redis.exceptions.ConnectionError("redis half-open")


class FailingRedis:
    def __init__(self) -> None:
        self.pipeline_calls = 0

    def pipeline(self, *args, **kwargs):
        self.pipeline_calls += 1
        return _FailingPipeline()


# ---------------------------------------------------------------- A2R20-06


def test_round_trips_scale_with_batches_not_versions():
    """负控：同一版本量下往返数 = 批次数，而非 3×版本数。"""
    inner = FakeRedis(decode_responses=True)
    proxy = CountingRedis(inner)
    stats = engine_sync.sync_all_published_to_engine(
        proxy, ListedStore(2000), batch_size=200, deadline_seconds=30
    )
    assert stats["written"] == 2000
    assert stats["truncated"] is False
    assert proxy.pipeline_calls == 10, (
        f"2,000 版本应 10 个 pipeline（每批 200），实际 {proxy.pipeline_calls}"
    )
    payload = inner.get("plaita:flow:f1999:1.0.0")
    assert payload is not None and '"flow_id": "f1999"' in payload
    assert inner.sismember("plaita:flow_list", "f1999")
    assert inner.sismember("plaita:flow_versions:f1999", "1.0.0")


def test_version_count_negative_control():
    """版本数 ×10 时 pipeline 次数只 ×10（线性于批次），不出现逐版本放大。"""
    small = CountingRedis(FakeRedis(decode_responses=True))
    large = CountingRedis(FakeRedis(decode_responses=True))
    engine_sync.sync_all_published_to_engine(
        small, ListedStore(100), batch_size=100, deadline_seconds=30
    )
    engine_sync.sync_all_published_to_engine(
        large, ListedStore(1000), batch_size=100, deadline_seconds=30
    )
    assert small.pipeline_calls == 1
    assert large.pipeline_calls == 10


def test_deadline_truncates_and_warns(caplog):
    slow = SlowRedis(delay=0.05)
    stats = engine_sync.sync_all_published_to_engine(
        slow, ListedStore(500), batch_size=10, deadline_seconds=0.12
    )
    assert stats["truncated"] is True
    assert stats["truncated_reason"] == "deadline"
    assert stats["written"] < 500
    assert stats["elapsed_ms"] < 2000
    assert "回填未完成（deadline）" in caplog.text


def test_max_versions_truncates(caplog):
    proxy = CountingRedis(FakeRedis(decode_responses=True))
    stats = engine_sync.sync_all_published_to_engine(
        proxy, ListedStore(100), batch_size=10, deadline_seconds=30, max_versions=25
    )
    assert stats["truncated"] is True
    assert stats["truncated_reason"] == "max_versions"
    assert stats["written"] == 25
    assert "回填未完成（max_versions）" in caplog.text


def test_connection_failure_stops_and_is_observable(caplog):
    failing = FailingRedis()
    stats = engine_sync.sync_all_published_to_engine(
        failing, ListedStore(100), batch_size=10, deadline_seconds=30
    )
    assert stats["truncated"] is True
    assert stats["truncated_reason"] == "redis_unavailable"
    assert stats["errors"] >= 10
    assert failing.pipeline_calls == 1, "连接级失败后不得继续逐批撞超时"
    assert "连接/超时" in caplog.text


def test_health_exposes_engine_sync_status():
    import main as main_module

    app = SimpleNamespace(state=SimpleNamespace(
        local_mode=False,
        engine_sync={"status": "degraded", "truncated": True, "written": 42},
    ))
    body = main_module.build_health_body(app)
    assert body["engine_sync"]["status"] == "degraded"
    assert body["engine_sync"]["written"] == 42


def test_connect_redis_declares_read_timeout(monkeypatch):
    import main as main_module

    captured = {}

    class _Stub:
        def ping(self):
            return True

    def fake_from_url(url, **kwargs):
        captured.update(kwargs)
        return _Stub()

    monkeypatch.setattr(main_module.Redis, "from_url", fake_from_url)
    main_module.connect_redis("redis://example:6379/0")
    assert captured["socket_connect_timeout"] == 2.0
    assert captured["socket_timeout"] == 5.0


def test_flow_store_iter_published_versions_pages(tmp_path):
    flow_store.init_engine(f"sqlite:///{tmp_path / 'paging.db'}")
    store = flow_store.get_flow_store()
    for i in range(3):
        fid = f"f{i}"
        store.create_flow(fid, tenant_id="default")
        store.save_flow_definition(
            fid, "1.0.0", '{"nodes": []}', status="published", tenant_id="default"
        )
        store.save_flow_definition(
            fid, "1.0.1", '{"nodes": []}', status="draft", tenant_id="default"
        )
    rows = list(store.iter_published_versions(page_size=2))
    assert len(rows) == 3
    assert {row[1] for row in rows} == {"f0", "f1", "f2"}
    assert all(row[0] == "default" and row[2] == "1.0.0" for row in rows)


# ---------------------------------------------------------------- A2R20-07


def test_sqlite_engine_kwargs_unchanged():
    assert flow_store._db_engine_kwargs("sqlite:///x.db") == {"future": True}


def test_pg_engine_kwargs_declare_connect_pool_and_lock(monkeypatch):
    monkeypatch.delenv("PLAITA_CONSOLE_DB_CONNECT_TIMEOUT", raising=False)
    monkeypatch.delenv("PLAITA_CONSOLE_DB_POOL_TIMEOUT", raising=False)
    kwargs = flow_store._db_engine_kwargs(
        "postgresql+psycopg://u:p@127.0.0.1:5432/x"
    )
    assert kwargs["connect_args"]["connect_timeout"] == 5
    assert "lock_timeout=5000" in kwargs["connect_args"]["options"]
    assert kwargs["pool_timeout"] == 5.0
    assert kwargs["pool_pre_ping"] is True


def test_pg_connect_timeout_env_override(monkeypatch):
    monkeypatch.setenv("PLAITA_CONSOLE_DB_CONNECT_TIMEOUT", "1")
    kwargs = flow_store._db_engine_kwargs(
        "postgresql+psycopg://u:p@127.0.0.1:5432/x"
    )
    assert kwargs["connect_args"]["connect_timeout"] == 1


def test_pg_blackhole_fails_fast_and_masks_url(monkeypatch, caplog):
    """负控：PG 黑洞端点不再 >8s 阻塞；启动日志不得回显口令。"""
    pytest.importorskip("psycopg")
    monkeypatch.setenv("PLAITA_CONSOLE_DB_CONNECT_TIMEOUT", "1")

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(10)
    port = srv.getsockname()[1]
    accepted: list[socket.socket] = []

    def accept_loop():
        while True:
            try:
                conn, _ = srv.accept()
            except OSError:
                return
            accepted.append(conn)

    threading.Thread(target=accept_loop, daemon=True).start()

    box: dict = {}

    def target():
        started = time.monotonic()
        try:
            flow_store.init_engine(
                f"postgresql+psycopg://u:secret@{'127.0.0.1'}:{port}/x"
            )
        except Exception as exc:  # noqa: BLE001 — 断言异常类型与耗时
            box["exc"] = exc
        box["elapsed"] = time.monotonic() - started

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(8)
    alive = thread.is_alive()
    for conn in accepted:
        conn.close()
    srv.close()

    assert not alive, "黑洞端点仍阻塞 >8s：connect timeout 未生效"
    assert box["elapsed"] < 8
    assert isinstance(box["exc"], OperationalError)
    assert flow_store._is_db_connection_error(box["exc"])
    messages = "\n".join(
        record.message for record in caplog.records if "flow_store" in record.name
    )
    assert "数据库连接失败/超时" in messages
    assert "secret" not in messages
    assert flow_store._engine is None
