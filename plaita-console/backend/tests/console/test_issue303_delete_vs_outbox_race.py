"""#303：删除协议「先清目标后删行」与投递/对账线程并发——对偶破口收口。

issue 机制（确定性重放，非概率撞窗）：DELETE 与投递/对账线程无同步点——
delivered 行被对账打回 pending → DELETE 第一步清目标 key → 投递 poll 取
pending 行重投（key 复活）→ 删除事务 purge 行 → 终态 key 在/行无/对账
永不覆盖/零日志。「回滚自愈」只覆盖 DELETE 失败方向，防不住对偶方向。
对称窗：投递线程已在 ``_iter_due_rows`` 读出行、DELETE 全程完成后投递
写出 key（毫秒级窗口，同构后果）。

修法口径（**口径一：删除完成后目标侧不复活**；二选一拍板记录于
``engine_outbox`` 模块文档）：删除与投递/对账经进程内互斥锁串行化 +
行级 ``deleting`` tombstone 双保险——

- 删除守卫（``deleting_guard``）全程持锁：标记行 deleting（独立事务提交）
  → 清目标三件 → 删除事务（purge 行）；
- 投递/对账逐行动作前在锁内**重读行现态**（``_reload_row``）：deleting /
  已删行跳过——跨事务陈旧读防线；
- 删除事务失败 → 快照恢复行原状（#301「先清后删」自愈方向保留）；
- 进程死在「已标记、未删行」之间 → 启动恢复 ``recover_deleting_rows``
  打回 pending，交还常规投递/对账自愈。

负控（修复前全红）：A1b 确定性重放（API 层 seam 注入：purge 后、删行
前跑对账+投递轮）、对称陈旧读窗（投递先读行→DELETE 全程→投递续写）、
真实线程并发（在途投递持锁时删除必须等待）。
"""
from __future__ import annotations

import json
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

import pytest
from fakeredis import FakeRedis
from fastapi import FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from models.flow import FlowPublishOutbox  # noqa: E402
from services import engine_outbox  # noqa: E402
from services.flow_store import FlowStore  # noqa: E402
from sqlalchemy import create_engine, select  # noqa: E402
from sqlalchemy.orm import sessionmaker  # noqa: E402

TARGET_URL = "redis://target-env:6379/0"


def _demo_definition(flow_id: str = "demo") -> str:
    return json.dumps(
        {
            "flow_id": flow_id,
            "desc": "outbox-race",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "end"},
                {"type": "end", "id": "end", "output": "$INPUT.name",
                 "resultType": "success"},
            ],
        }
    )


@pytest.fixture()
def env_enabled(monkeypatch):
    monkeypatch.setenv(engine_outbox.TARGET_URL_ENV, TARGET_URL)
    monkeypatch.setenv(engine_outbox.POLL_ENV, "0.05")
    monkeypatch.setenv(engine_outbox.RECONCILE_ENV, "0.2")
    yield


@pytest.fixture()
def store(tmp_path) -> FlowStore:
    engine = create_engine(f"sqlite:///{tmp_path / 'outbox-race.db'}", future=True)
    from models.flow import Base

    Base.metadata.create_all(engine)
    return FlowStore(sessionmaker(bind=engine, expire_on_commit=False))


@pytest.fixture()
def sessions(store: FlowStore):
    return store._session_local


@pytest.fixture()
def target_redis() -> FakeRedis:
    return FakeRedis(decode_responses=True)


@pytest.fixture()
def console_redis() -> FakeRedis:
    return FakeRedis(decode_responses=True)


def _rows(sessions, **filters):
    with sessions() as session:
        stmt = select(FlowPublishOutbox).order_by(FlowPublishOutbox.id.asc())
        for col, val in filters.items():
            stmt = stmt.where(getattr(FlowPublishOutbox, col) == val)
        return session.scalars(stmt).all()


def _publish_version(store: FlowStore, flow_id: str = "demo",
                     version: str = "1.0.0", tenant_id: str = ""):
    store.ensure_flow(flow_id, tenant_id=tenant_id)
    store.save_flow_definition(flow_id, version, _demo_definition(flow_id),
                               "{}", tenant_id=tenant_id)
    return store.publish_version(flow_id, version, tenant_id=tenant_id or None)


def _api_client(tmp_path, console_redis: FakeRedis):
    from api import flows as flows_api

    flow_store_mod = sys.modules["services.flow_store"]
    flow_store_mod.init_engine(f"sqlite:///{tmp_path / 'api-race.db'}")
    app = FastAPI()
    app.state.redis = console_redis
    app.state.local_mode = False
    app.state.store = flow_store_mod.get_flow_store()
    app.include_router(flows_api.router, prefix="/api")
    return TestClient(app)


# ---- A1b 确定性重放（API 层）：purge 后、删行前跑对账+投递轮 ----


def test_a1b_delete_vs_reconcile_delivery_no_resurrection(
        tmp_path, env_enabled, console_redis: FakeRedis,
        target_redis: FakeRedis, monkeypatch):
    """A1b 重放：DELETE 清 key 后、删行 commit 前投递/对账轮插入——不得复活。

    修复前：对账把 delivered 行打回 pending（key 已清）→ 投递轮重投写回
    key → 删除事务 purge 行 → key 在/行无/零覆盖（幽灵）。修复后：行在
    清 key 前已标记 deleting，两轮扫描全跳过；删除终态目标侧无 key。
    """
    client = _api_client(tmp_path, console_redis)
    client.app.state.engine_outbox_target = target_redis
    assert client.post("/api/flows", json={"flow_id": "race"}).status_code == 200
    assert client.put("/api/flows/race/versions/1.0.0", json={
        "definition": _demo_definition("race"), "layout": "{}",
    }).status_code == 200
    assert client.post("/api/flows/race/publish",
                       json={"version": "1.0.0"}).status_code == 200
    sessions = sys.modules["services.flow_store"].session_factory()
    assert engine_outbox.run_delivery_cycle(target_redis, sessions)["delivered"] == 1
    key = "plaita:flow:race:1.0.0"
    assert target_redis.exists(key)

    # seam 注入：purge_target_keys 返回后（= DELETE 第一步清 key 完成）、
    # 删行事务前，投递/对账线程各跑一轮（确定性重放 issue 的交错）。
    orig_purge = engine_outbox.purge_target_keys
    seen: dict = {}

    def spy(flow_id, **kw):
        result = orig_purge(flow_id, **kw)
        seen["statuses_mid_delete"] = [r.status for r in _rows(sessions)]
        seen["reconcile"] = engine_outbox.run_reconcile_cycle(target_redis, sessions)
        seen["delivery"] = engine_outbox.run_delivery_cycle(target_redis, sessions)
        seen["key_after_cycles"] = target_redis.exists(key)
        return result

    monkeypatch.setattr(engine_outbox, "purge_target_keys", spy)

    # #304：published 版本删除需显式 force（本测无在途执行，引用判据放行）
    resp = client.delete("/api/flows/race?force=true")
    assert resp.status_code == 200
    # 删除窗口内行已置 deleting（tombstone）：对账/投递扫描零命中
    assert set(seen["statuses_mid_delete"]) == {"deleting"}
    assert seen["reconcile"]["checked"] == 0
    assert seen["delivery"]["scanned"] == 0
    assert seen["key_after_cycles"] == 0
    # 终态：key 不在（不复活）、行已 purge（对账无覆盖对象即无幽灵）
    assert _rows(sessions) == []
    assert not target_redis.exists(key)
    # 事后对账也扫不到任何行（无 delivered 行可打回）
    assert engine_outbox.run_reconcile_cycle(target_redis, sessions)["checked"] == 0


def test_symmetric_stale_read_delivery_skips_after_delete(
        store, sessions, env_enabled, target_redis: FakeRedis, monkeypatch):
    """对称窗：投递线程已读出行 → DELETE 全程完成 → 投递续写——不得落 key。"""
    _publish_version(store)  # 行保持 pending（投递轮未跑）
    now = datetime.utcnow()
    stale = engine_outbox._iter_due_rows(sessions(), 200, now)
    assert len(stale) == 1

    # DELETE 全程（守卫：标记 deleting → 清 key → 删行事务）
    with engine_outbox.deleting_guard(sessions, tenant_id="", flow_id="demo"):
        engine_outbox.purge_target_keys("demo", client=target_redis)
        assert store.delete_version("demo", "1.0.0") is True
    assert _rows(sessions) == []

    # 投递线程带着陈旧读到的行继续跑——重读现态后必须跳过
    monkeypatch.setattr(
        engine_outbox, "_iter_due_rows",
        lambda session, limit, now_: stale,
    )
    dstat = engine_outbox.run_delivery_cycle(target_redis, sessions)
    assert dstat["scanned"] == 1 and dstat["delivered"] == 0
    assert dstat.get("skipped", 0) == 1
    assert not target_redis.exists("plaita:flow:demo:1.0.0")


def test_delete_waits_for_inflight_delivery_then_purges(
        store, sessions, env_enabled, target_redis: FakeRedis, monkeypatch):
    """真实线程并发：在途投递持锁期间，删除守卫必须等待，随后清掉其在途 key。"""
    _publish_version(store)
    in_flight = threading.Event()
    release = threading.Event()
    orig_deliver = engine_outbox._deliver_one

    def slow_deliver(target, row, now):
        in_flight.set()
        assert release.wait(timeout=10), "delivery not released in time"
        return orig_deliver(target, row, now)

    monkeypatch.setattr(engine_outbox, "_deliver_one", slow_deliver)

    delivery_done = threading.Event()

    def run_delivery():
        engine_outbox.run_delivery_cycle(target_redis, sessions)
        delivery_done.set()

    t = threading.Thread(target=run_delivery, daemon=True)
    t.start()
    assert in_flight.wait(timeout=5)

    delete_done = threading.Event()

    def do_delete():
        with engine_outbox.deleting_guard(sessions, tenant_id="", flow_id="demo"):
            engine_outbox.purge_target_keys("demo", client=target_redis)
            assert store.delete_version("demo", "1.0.0") is True
        delete_done.set()

    t2 = threading.Thread(target=do_delete, daemon=True)
    t2.start()
    time.sleep(0.2)
    # 在途投递未放锁前，删除不得先行（互斥串行化的直接断言）
    assert not delete_done.is_set()
    release.set()
    assert delivery_done.wait(timeout=10)
    assert delete_done.wait(timeout=10)
    t.join(timeout=10)
    t2.join(timeout=10)
    # 在途投递写出的 key 被随后持锁的删除清掉——终态无幽灵
    assert _rows(sessions) == []
    assert not target_redis.exists("plaita:flow:demo:1.0.0")


# ---- tombstone 语义与守卫协议 ----


def test_cycles_skip_deleting_rows(store, sessions, env_enabled,
                                   target_redis: FakeRedis):
    """deleting 行：不进投递扫描（pending/failed 才扫）、不进对账（只扫 delivered）。"""
    _publish_version(store)
    engine_outbox.run_delivery_cycle(target_redis, sessions)
    assert engine_outbox.mark_deleting(
        sessions, tenant_id="", flow_id="demo") == [(_rows(sessions)[0].id,
                                                     "delivered")]
    assert _rows(sessions)[0].status == "deleting"
    assert engine_outbox.run_delivery_cycle(target_redis, sessions)["scanned"] == 0
    assert engine_outbox.run_reconcile_cycle(target_redis, sessions)["checked"] == 0


def test_guard_rollback_restores_exact_status(store, sessions, env_enabled,
                                              target_redis: FakeRedis):
    """删除事务失败：守卫按快照恢复行原状（delivered/pending/resolved 各归位）。"""
    _publish_version(store)                      # 1.0.0
    _publish_version(store, version="1.0.1")     # 1.0.1 → 先置 resolved（不参与投递）
    pend = _rows(sessions, version="1.0.1")[0]
    engine_outbox.resolve_row(sessions, pend.id, tenant_id="", actor="t")
    engine_outbox.run_delivery_cycle(target_redis, sessions)  # 仅 1.0.0 delivered
    resolved_id = pend.id

    with pytest.raises(RuntimeError, match="delete txn fails"):
        with engine_outbox.deleting_guard(sessions, tenant_id="", flow_id="demo"):
            engine_outbox.purge_target_keys("demo", client=target_redis)
            raise RuntimeError("delete txn fails")

    statuses = {r.version: r.status for r in _rows(sessions)}
    assert statuses == {"1.0.0": "delivered", "1.0.1": "resolved"}


def test_rollback_self_heal_still_redelivers(store, sessions, env_enabled,
                                             target_redis: FakeRedis):
    """#301 自愈方向保留：回滚后 delivered 行还在，对账打回 → 重投收敛。"""
    _publish_version(store)
    engine_outbox.run_delivery_cycle(target_redis, sessions)
    assert target_redis.exists("plaita:flow:demo:1.0.0")

    with pytest.raises(RuntimeError):
        with engine_outbox.deleting_guard(sessions, tenant_id="", flow_id="demo"):
            engine_outbox.purge_target_keys("demo", client=target_redis)  # key 已清
            raise RuntimeError("delete txn fails")

    # 行恢复 delivered（key 缺失）→ 对账打回 pending → 投递重投 → key 回归
    rec = engine_outbox.run_reconcile_cycle(target_redis, sessions)
    assert rec["requeued"] == 1
    assert engine_outbox.run_delivery_cycle(target_redis, sessions)["delivered"] == 1
    assert target_redis.exists("plaita:flow:demo:1.0.0")


def test_startup_recovery_requeues_crashed_deleting_rows(
        store, sessions, env_enabled, target_redis: FakeRedis):
    """进程死在「已标记、未删行」之间：启动恢复把 deleting 行打回 pending 自愈。"""
    _publish_version(store)
    engine_outbox.mark_deleting(sessions, tenant_id="", flow_id="demo")
    assert _rows(sessions)[0].status == "deleting"

    assert engine_outbox.recover_deleting_rows(sessions) == 1
    row = _rows(sessions)[0]
    assert row.status == "pending"
    # 常规机器接管：源表仍在 → 投递收敛
    assert engine_outbox.run_delivery_cycle(target_redis, sessions)["delivered"] == 1
    assert target_redis.exists("plaita:flow:demo:1.0.0")


def test_manual_exit_rejects_deleting_row(store, sessions, env_enabled):
    """删除协议进行中的行不可人工出口（resolve/discard 均拒）。"""
    _publish_version(store)
    row = _rows(sessions)[0]
    engine_outbox.mark_deleting(sessions, tenant_id="", flow_id="demo")
    with pytest.raises(ValueError, match="deleting"):
        engine_outbox.resolve_row(sessions, row.id, tenant_id="", actor="t")
    with pytest.raises(ValueError, match="deleting"):
        engine_outbox.discard_row(sessions, row.id, tenant_id="", actor="t")


def test_stats_snapshot_counts_deleting(store, sessions, env_enabled):
    """/health backlog 视图暴露 deleting 行数（删除窗口可观测）。"""
    _publish_version(store)
    assert engine_outbox.stats_snapshot(sessions)["deleting"] == 0
    engine_outbox.mark_deleting(sessions, tenant_id="", flow_id="demo")
    assert engine_outbox.stats_snapshot(sessions)["deleting"] == 1


def test_guard_passthrough_when_outbox_disabled(store, sessions, monkeypatch):
    """同 Redis 部署（outbox 关闭）：守卫透传，不触碰任何状态。"""
    monkeypatch.delenv(engine_outbox.TARGET_URL_ENV, raising=False)
    assert not engine_outbox.is_enabled()
    with engine_outbox.deleting_guard(sessions, tenant_id="", flow_id="demo"):
        pass  # 纯透传：无锁语义依赖、无行标记
