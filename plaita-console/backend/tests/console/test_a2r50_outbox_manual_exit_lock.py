"""A2R50-02：outbox 人工出口（resolve/discard）入 ``OUTBOX_DELETE_LOCK``。

#303 的锁只进了 ``deleting_guard`` 与投递/对账循环——``resolve_row``/
``discard_row`` 两个人工出口的 deleting 检查仍是**锁外 check-then-act**：
行读在 ``mark_deleting`` 提交之前、状态写在之后 → tombstone 被覆盖
（resolve）/行被抢删（discard）。删除事务随后失败时 ``restore_deleting``
只恢复「仍为 deleting」的行 → 被出口动过的行不恢复：published 版本的
投递行永久消失，投递扫不到、对账扫不到、重复 publish 幂等早退不重建
→ 跨 Redis 部署幽灵不可自愈。

修法：人工出口与删除协议/投递/对账**同锁**（行动粒度串行，等待方最多
让出一个行动位、不饿死），锁内 ``_reload_row`` 重读现态——竞窗内落下
的 tombstone / 删除在此显形（deleting → ``ValueError`` 409、已删 →
``None`` 404，陈旧读不放行）。

负控（修复前全红）：确定性重放（monkeypatch ``_get_scoped_row`` 在返回
前插入 ``mark_deleting``——= 真实竞窗中「读后、写前」被删除协议插队），
A2 r56 归档剧本同构；幽灵危害链全程；真实线程等锁对照面；API 层具名
409。守恒格钉住顺序语义零漂移。
"""
from __future__ import annotations

import json
import sys
import threading
import time
from pathlib import Path

import pytest
from fakeredis import FakeRedis
from fastapi import FastAPI
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from models.flow import FlowPublishOutbox  # noqa: E402
from services import engine_outbox  # noqa: E402
from services.flow_store import FlowStore  # noqa: E402

TARGET_URL = "redis://target-env:6379/0"


def _demo_definition(flow_id: str = "demo") -> str:
    return json.dumps(
        {
            "flow_id": flow_id,
            "desc": "a2r50-manual-exit-lock",
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
    yield


@pytest.fixture()
def store(tmp_path) -> FlowStore:
    engine = create_engine(f"sqlite:///{tmp_path / 'a2r50-lock.db'}",
                           future=True)
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


def _publish_version(store: FlowStore, flow_id: str = "demo",
                     version: str = "1.0.0", tenant_id: str = ""):
    store.ensure_flow(flow_id, tenant_id=tenant_id)
    store.save_flow_definition(flow_id, version, _demo_definition(flow_id),
                               "{}", tenant_id=tenant_id)
    return store.publish_version(flow_id, version, tenant_id=tenant_id or None)


def _row_to_failed(sessions, flow_id: str = "demo") -> int:
    """行置 failed（DLQ 档）——人工出口的合法前置态。返回 row_id。"""
    with sessions() as s:
        row = s.scalars(select(FlowPublishOutbox).where(
            FlowPublishOutbox.flow_id == flow_id)).first()
        row.status = engine_outbox.STATUS_FAILED
        s.commit()
        return row.id


def _row_status(sessions, row_id: int):
    with sessions() as s:
        row = s.get(FlowPublishOutbox, row_id)
        return None if row is None else row.status


def _install_race(monkeypatch, sessions, row_id: int, flow_id: str = "demo",
                  tenant_id: str = ""):
    """确定性重放注入：``_get_scoped_row`` 返回前删除协议插队提交
    ``mark_deleting``（独立事务），行对象保持陈旧读——与真实线程竞窗
    「读后、写前」同构（A2 r56 归档剧本手法）。"""
    orig_get = engine_outbox._get_scoped_row

    def racing_get(session, rid, tid):
        row_obj = orig_get(session, rid, tid)
        if row_obj is not None and row_obj.id == row_id:
            engine_outbox.mark_deleting(sessions, tenant_id=tenant_id,
                                        flow_id=flow_id)
        return row_obj

    monkeypatch.setattr(engine_outbox, "_get_scoped_row", racing_get)


# ---- 竞窗具名拒（修复前：resolve 盖掉 tombstone / discard 抢删行） ----


@pytest.mark.parametrize("action", ["resolve", "discard"])
def test_manual_exit_race_window_rejected_by_tombstone(
        store, sessions, env_enabled, monkeypatch, action):
    """竞窗内落下的 tombstone 必须被锁内重读看见——出口按既有 409 语义拒。"""
    _publish_version(store)
    row_id = _row_to_failed(sessions)
    _install_race(monkeypatch, sessions, row_id)

    fn = engine_outbox.resolve_row if action == "resolve" \
        else engine_outbox.discard_row
    with pytest.raises(ValueError, match="deleting"):
        fn(sessions, row_id, tenant_id="", actor="a2r50")
    # tombstone 原样：出口既没盖掉它、也没抢删行
    assert _row_status(sessions, row_id) == engine_outbox.STATUS_DELETING


def test_manual_exit_race_no_ghost_after_failed_delete(
        store, sessions, env_enabled, monkeypatch):
    """完整危害链：竞窗出口被拒 → 删除事务失败 restore → 行可恢复、无幽灵。

    修复前：discard 竞窗删行 → restore_deleting 只恢复仍为 deleting 的行
    （恢复 0 行）→ published 版本永久失去投递/对账载体。
    """
    _publish_version(store)
    row_id = _row_to_failed(sessions)
    _install_race(monkeypatch, sessions, row_id)

    with pytest.raises(ValueError, match="deleting"):
        engine_outbox.discard_row(sessions, row_id, tenant_id="", actor="a2r50")

    # 删除协议继续：清目标 → 删除事务失败 → 守卫按快照恢复
    snapshot = [(row_id, engine_outbox.STATUS_FAILED)]
    assert engine_outbox.restore_deleting(
        sessions, snapshot, tenant_id="", flow_id="demo") == 1
    assert _row_status(sessions, row_id) == engine_outbox.STATUS_FAILED
    # 常规机器接管：failed 行仍在投递扫描内（自愈可达，无幽灵）
    stat = engine_outbox.run_delivery_cycle(target_redis, sessions)
    assert stat["scanned"] == 1
    # 源表 published、行在——投递/对账机器完整
    ver = store.get_version("demo", "1.0.0", tenant_id="")
    assert ver is not None and ver.status == "published"


# ---- 对照面：投递线程持锁时人工出口等待、不饿死（#303 语义不变） ----


def test_manual_exit_waits_for_inflight_row_action(
        store, sessions, env_enabled, target_redis: FakeRedis, monkeypatch):
    """在途投递持锁期间，人工出口必须等待（行动粒度串行），放锁后完成。

    在途投递构造成**失败**投递（返回具名原因、行保持 failed）——若在途
    投递成功把行转 delivered，出口等锁后按既有语义拒 delivered，与等待
    断言混在一起分不开。
    """
    _publish_version(store)
    row_id = _row_to_failed(sessions)

    in_flight = threading.Event()
    release = threading.Event()

    def slow_deliver(target, row, now):
        in_flight.set()
        assert release.wait(timeout=10), "delivery not released in time"
        return "redis_unavailable: (模拟在途投递失败)"

    monkeypatch.setattr(engine_outbox, "_deliver_one", slow_deliver)
    delivery_done = threading.Event()

    def run_delivery():
        engine_outbox.run_delivery_cycle(target_redis, sessions)
        delivery_done.set()

    t = threading.Thread(target=run_delivery, daemon=True)
    t.start()
    assert in_flight.wait(timeout=5)

    resolve_done = threading.Event()
    resolve_result: dict = {}
    resolve_error: list = []

    def do_resolve():
        try:
            resolve_result["brief"] = engine_outbox.resolve_row(
                sessions, row_id, tenant_id="", actor="a2r50")
        except Exception as e:  # noqa: BLE001 — 线程内异常转存供断言
            resolve_error.append(repr(e))
        resolve_done.set()

    t2 = threading.Thread(target=do_resolve, daemon=True)
    t2.start()
    time.sleep(0.2)
    # 出口不得在投递持锁期间落状态（修复前：无锁、此刻已完成=红）
    assert not resolve_done.is_set()
    release.set()
    assert delivery_done.wait(timeout=10)
    assert resolve_done.wait(timeout=10)
    t.join(timeout=10)
    t2.join(timeout=10)
    assert not resolve_error, f"出口在等锁后出错: {resolve_error}"
    # 等到锁后按现态（failed）落 resolved——等待有限（单行动作），不饿死
    assert resolve_result["brief"]["status"] == "resolved"
    assert _row_status(sessions, row_id) == engine_outbox.STATUS_RESOLVED


# ---- API 层：竞窗落进人工出口 → 具名 409（非静默成功） ----


def _api_client(tmp_path, console_redis: FakeRedis) -> TestClient:
    from api import flows as flows_api

    flow_store_mod = sys.modules["services.flow_store"]
    flow_store_mod.init_engine(f"sqlite:///{tmp_path / 'api-a2r50.db'}")
    app = FastAPI()
    app.state.redis = console_redis
    app.state.local_mode = False
    app.state.store = flow_store_mod.get_flow_store()
    app.include_router(flows_api.router, prefix="/api")
    return TestClient(app)


def test_api_manual_exit_race_named_409(tmp_path, env_enabled,
                                        console_redis: FakeRedis,
                                        monkeypatch):
    """竞窗内 resolve 走 API → 409 ``outbox_row_not_eligible``，行保持 deleting。"""
    from models.flow import DEFAULT_TENANT_ID

    client = _api_client(tmp_path, console_redis)
    flow_store_mod = sys.modules["services.flow_store"]
    sessions = flow_store_mod.session_factory()
    with sessions() as session:
        session.add(FlowPublishOutbox(
            tenant_id=DEFAULT_TENANT_ID, flow_id="demo", version="1.0.0",
            definition_hash="deadbeef", definition="{}",
            status=engine_outbox.STATUS_FAILED))
        session.commit()
    row = sessions().scalars(select(FlowPublishOutbox)).first()
    _install_race(monkeypatch, sessions, row.id,
                  flow_id="demo", tenant_id=DEFAULT_TENANT_ID)

    resp = client.post(f"/api/flows/publish-outbox/{row.id}/resolve")
    assert resp.status_code == 409
    assert resp.json()["detail"]["code"] == "outbox_row_not_eligible"
    assert _row_status(sessions, row.id) == engine_outbox.STATUS_DELETING


# ---- 守恒锚：锁+重读不改变顺序语义（修复前后均绿） ----


def test_manual_exit_sequential_semantics_conserved(
        store, sessions, env_enabled, monkeypatch):
    """顺序面零漂移：resolve 置终态留痕且幂等；discard 删行；404/租户面不变。"""
    _publish_version(store)
    rid = _row_to_failed(sessions)

    brief = engine_outbox.resolve_row(sessions, rid, tenant_id="", actor="t")
    assert brief["status"] == "resolved"
    with sessions() as s:
        row = s.get(FlowPublishOutbox, rid)
        assert row.status == "resolved" and "manually resolved" in row.last_error
    # 幂等：重复 resolve 仍返回 brief、不重复追加留痕
    brief2 = engine_outbox.resolve_row(sessions, rid, tenant_id="", actor="t")
    assert brief2["status"] == "resolved"
    with sessions() as s:
        assert s.get(FlowPublishOutbox, rid).last_error.count(
            "manually resolved") == 1

    # 不存在的行 / 他租户行 → None（404 语义）
    assert engine_outbox.resolve_row(sessions, 999999, tenant_id="") is None
    assert engine_outbox.discard_row(sessions, 999999, tenant_id="") is None
    assert engine_outbox.resolve_row(sessions, rid, tenant_id="other") is None

    # discard：删行（pending 行亦可 discard，仅 delivered/deleting 拒）
    _publish_version(store, version="1.0.1")
    with sessions() as s:
        r2 = s.scalars(select(FlowPublishOutbox).where(
            FlowPublishOutbox.version == "1.0.1")).first()
        rid2 = r2.id
    assert engine_outbox.discard_row(sessions, rid2, tenant_id="",
                                     actor="t")["status"] == "pending"
    assert _row_status(sessions, rid2) is None
