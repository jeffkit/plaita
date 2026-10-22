"""#299：flow publish outbox（跨 Redis 部署的 durable 发布投递链）。

负控（修复前全红，证据 docs/proofs/data/r71-299-negative-control-{pre,post}.txt）：
- 配了目标 Redis 的发布只写 console 自己的 Redis，目标环境永远缺 key；
- 无 outbox 行、无投递、无对账、无 DLQ。

判据（全部先红后绿）：
- publish 事务内与 flow_versions 同事务写 pending 行（回滚不残留）；
- 幂等键 (tenant, flow, version, hash)：同版本重复发布不重复入队/投递；
- 至少一次投递：写后读回校验 hash 才记 delivered；
- 失败退避重试 → 超限转 failed（DLQ 档）→ 慢速重排自动补投；
- 对账：delivered 行目标缺 key/hash 不符 → 打回 pending 重投；
- 版本/流程删除清 outbox（对账不复活已删版本）；
- 指纹口径与引擎 worker（flow-definition-json-sortkeys-v2）逐位一致。
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta
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
            "desc": "outbox",
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
    """启用 outbox（配目标 Redis）；压短退避便于测试。"""
    monkeypatch.setenv(engine_outbox.TARGET_URL_ENV, TARGET_URL)
    monkeypatch.setenv(engine_outbox.POLL_ENV, "0.05")
    monkeypatch.setenv(engine_outbox.RECONCILE_ENV, "0.2")
    yield


@pytest.fixture()
def store(tmp_path) -> FlowStore:
    engine = create_engine(f"sqlite:///{tmp_path / 'outbox.db'}", future=True)
    from models.flow import Base

    Base.metadata.create_all(engine)
    return FlowStore(sessionmaker(bind=engine, expire_on_commit=False))


@pytest.fixture()
def sessions(store: FlowStore):
    return store._session_local


@pytest.fixture()
def console_redis() -> FakeRedis:
    return FakeRedis(decode_responses=True)


@pytest.fixture()
def target_redis() -> FakeRedis:
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


# ---- 指纹口径 ----


def test_hash_algo_parity_with_engine_worker():
    """outbox 指纹与引擎 worker 的 compute_flow_definition_hash 逐位一致。"""
    from plaita.server.flow_worker import compute_flow_definition_hash

    defn = json.dumps({"b": 1, "a": {"y": [1, 2, {"中文": "值"}]}, "c": None})
    payload, outbox_hash = engine_outbox.engine_payload("demo", "1.0.0", defn)
    assert outbox_hash == compute_flow_definition_hash(json.loads(payload))
    # 注入键必须参与指纹（worker 校验的是含 flow_id/version 的存储原文）
    _, h1 = engine_outbox.engine_payload("demo", "1.0.0", defn)
    _, h2 = engine_outbox.engine_payload("demo", "1.0.1", defn)
    assert h1 != h2


def test_engine_payload_matches_engine_sync_artifact(target_redis: FakeRedis):
    """engine_payload 的 hash 与 engine_sync 实际落盘原文的 hash 一致。"""
    from services.engine_sync import sync_flow_to_engine

    defn = _demo_definition()
    _, expected = engine_outbox.engine_payload("demo", "1.0.0", defn)
    assert sync_flow_to_engine(target_redis, "demo", "1.0.0", defn) is True
    raw = target_redis.get("plaita:flow:demo:1.0.0")
    assert engine_outbox._stored_key_hash(raw) == expected


# ---- 发布入队 ----


def test_publish_disabled_no_outbox(store: FlowStore, monkeypatch):
    """未配目标 Redis（同 Redis 部署，默认）：publish 不写 outbox 行。"""
    monkeypatch.delenv(engine_outbox.TARGET_URL_ENV, raising=False)
    assert not engine_outbox.is_enabled()
    _publish_version(store)
    assert _rows(sessions := store._session_local) == []


def test_publish_enqueues_pending_row(store: FlowStore, env_enabled):
    """启用后：publish 事务内写 pending 行，指纹按投递后工件算。"""
    out = _publish_version(store)
    assert out.status == "published"
    rows = _rows(store._session_local)
    assert len(rows) == 1
    row = rows[0]
    assert (row.tenant_id, row.flow_id, row.version) == ("", "demo", "1.0.0")
    assert row.status == "pending"
    assert row.attempts == 0
    _, expected_hash = engine_outbox.engine_payload("demo", "1.0.0",
                                                    _demo_definition())
    assert row.definition_hash == expected_hash
    assert row.definition == _demo_definition()


def test_republish_idempotent_no_duplicate(store: FlowStore, env_enabled):
    """同版本重复发布（已发布幂等返回）：不产生第二行、不重复投递。"""
    _publish_version(store)
    out = store.publish_version("demo", "1.0.0")  # 二次发布走幂等分支
    assert out.status == "published"
    assert len(_rows(store._session_local)) == 1


def test_publish_rollback_leaves_no_outbox(store: FlowStore, env_enabled,
                                           monkeypatch):
    """负控：publish 事务回滚 ⇒ outbox 不残留（同事务性的直接证据）。"""
    from services import flow_store as flow_store_mod

    # 先建好 draft（ensure/save 也过租户世代闸），再打断 publish 的提交链路
    # ——flush（含 outbox 入队）之后、commit 之前抛出。
    store.ensure_flow("demo")
    store.save_flow_definition("demo", "1.0.0", _demo_definition(), "{}")

    def _boom(session, tenant_id):
        raise RuntimeError("tenant generation check failed")

    monkeypatch.setattr(flow_store_mod, "assert_tenant_generation", _boom)
    with pytest.raises(RuntimeError):
        store.publish_version("demo", "1.0.0")
    # 版本未发布 + outbox 零残留
    assert store.get_version("demo", "1.0.0").status == "draft"
    assert _rows(store._session_local) == []


# ---- 投递 ----


def test_delivery_cross_redis(store: FlowStore, sessions, env_enabled,
                              console_redis: FakeRedis, target_redis: FakeRedis):
    """跨 Redis：投递只写目标实例；同内容重复投递幂等（覆盖写同值）。"""
    _publish_version(store)
    stats = engine_outbox.run_delivery_cycle(target_redis, sessions)
    assert stats["delivered"] == 1 and stats["errors"] == 0
    key = "plaita:flow:demo:1.0.0"
    assert target_redis.exists(key)
    assert engine_outbox._stored_key_hash(
        target_redis.get(key)) == _rows(sessions)[0].definition_hash
    assert target_redis.sismember("plaita:flow_list", "demo")
    assert target_redis.sismember("plaita:flow_versions:demo", "1.0.0")
    # 行转 delivered（ack）
    row = _rows(sessions)[0]
    assert row.status == "delivered" and row.delivered_at is not None
    # console 自己的 Redis 不被 outbox 触碰（跨实例隔离）
    assert not console_redis.exists(key)
    # 已 delivered 的行不会被再次投递
    again = engine_outbox.run_delivery_cycle(target_redis, sessions)
    assert again["scanned"] == 0


def test_delivery_tenant_namespace(store: FlowStore, sessions, env_enabled,
                                   target_redis: FakeRedis):
    """租户命名空间：t1 → plaita:t1:flow:...；default → 历史前缀 plaita。"""
    _publish_version(store, flow_id="t1flow", tenant_id="t1")
    assert engine_outbox.run_delivery_cycle(target_redis, sessions)["delivered"] == 1
    assert target_redis.exists("plaita:t1:flow:t1flow:1.0.0")


def test_delivery_failure_retries_then_dlq(store: FlowStore, sessions,
                                           env_enabled, monkeypatch):
    """失败退避重试；连续失败到上限转 failed（DLQ 档），恢复后自动补投。"""
    monkeypatch.setenv(engine_outbox.MAX_ATTEMPTS_ENV, "2")

    class Unreachable:
        def __getattr__(self, name):
            raise ConnectionError("connection refused")

    _publish_version(store)
    stats = engine_outbox.run_delivery_cycle(Unreachable(), sessions,
                                             attempts_cap=2)
    assert stats["retried"] == 1 and stats["to_dlq"] == 0
    row = _rows(sessions)[0]
    assert row.status == "pending" and row.attempts == 1
    assert row.next_attempt_at is not None  # 退避中，未到期不再投
    early = engine_outbox.run_delivery_cycle(
        Unreachable(), sessions, now=row.next_attempt_at - timedelta(seconds=1))
    assert early["scanned"] == 0

    # 第二次失败 → 转 failed（DLQ），退避拉长
    stats2 = engine_outbox.run_delivery_cycle(Unreachable(), sessions,
                                              attempts_cap=2,
                                              now=row.next_attempt_at)
    assert stats2["to_dlq"] == 1
    row = _rows(sessions)[0]
    assert row.status == "failed" and "connection refused" in row.last_error
    snapshot = engine_outbox.stats_snapshot(sessions)
    assert snapshot["failed"] == 1 and snapshot["last_error"]

    # 目标恢复（真 FakeRedis）+ 到期 → 慢速重排自动补投（恢复后自动补齐）
    healthy = FakeRedis(decode_responses=True)
    stats3 = engine_outbox.run_delivery_cycle(
        healthy, sessions, now=row.next_attempt_at + timedelta(seconds=1))
    assert stats3["delivered"] == 1
    assert healthy.exists("plaita:flow:demo:1.0.0")
    assert _rows(sessions)[0].status == "delivered"


def test_delivery_hash_corruption_goes_dlq(store: FlowStore, sessions,
                                           env_enabled, target_redis: FakeRedis):
    """库内定义与指纹不一致：拒绝投递（不写坏数据），转 DLQ 档。"""
    _publish_version(store)
    with sessions() as session:
        row = session.scalars(select(FlowPublishOutbox)).first()
        row.definition = json.dumps({"tampered": True})
        session.commit()
    stats = engine_outbox.run_delivery_cycle(target_redis, sessions,
                                             attempts_cap=1)
    assert stats["to_dlq"] == 1
    assert not target_redis.exists("plaita:flow:demo:1.0.0")
    assert "mismatch" in _rows(sessions)[0].last_error


# ---- 对账 ----


def test_reconcile_missing_key_requeues(store: FlowStore, sessions, env_enabled,
                                        target_redis: FakeRedis):
    """delivered 行目标 key 被删 → 对账打回 pending → 重投补齐。"""
    _publish_version(store)
    engine_outbox.run_delivery_cycle(target_redis, sessions)
    target_redis.delete("plaita:flow:demo:1.0.0")
    rec = engine_outbox.run_reconcile_cycle(target_redis, sessions)
    assert rec["requeued"] == 1
    row = _rows(sessions)[0]
    assert row.status == "pending" and "缺失" in row.last_error
    assert engine_outbox.run_delivery_cycle(target_redis, sessions)["delivered"] == 1
    assert target_redis.exists("plaita:flow:demo:1.0.0")


def test_reconcile_hash_mismatch_requeues(store: FlowStore, sessions,
                                          env_enabled, target_redis: FakeRedis):
    """delivered 行被外部改写（hash 不符）→ 对账重投正确内容。"""
    _publish_version(store)
    engine_outbox.run_delivery_cycle(target_redis, sessions)
    target_redis.set("plaita:flow:demo:1.0.0", json.dumps({"corrupted": True}))
    assert engine_outbox.run_reconcile_cycle(target_redis, sessions)["requeued"] == 1
    assert engine_outbox.run_delivery_cycle(target_redis, sessions)["delivered"] == 1
    assert engine_outbox._stored_key_hash(
        target_redis.get("plaita:flow:demo:1.0.0")
    ) == _rows(sessions)[0].definition_hash


def test_reconcile_healthy_rows_untouched(store: FlowStore, sessions,
                                          env_enabled, target_redis: FakeRedis):
    """对账守恒：key 在且 hash 一致的 delivered 行不动、不产生写放大。"""
    _publish_version(store)
    engine_outbox.run_delivery_cycle(target_redis, sessions)
    rec = engine_outbox.run_reconcile_cycle(target_redis, sessions)
    assert rec == {"checked": 1, "requeued": 0, "errors": 0,
                   "drifted": 0, "source_missing": 0}
    assert _rows(sessions)[0].status == "delivered"


def test_version_delete_purges_outbox_no_resurrect(store: FlowStore, sessions,
                                                   env_enabled,
                                                   target_redis: FakeRedis):
    """版本删除清 outbox：对账不会把已删版本复活进目标 Redis。"""
    _publish_version(store)
    engine_outbox.run_delivery_cycle(target_redis, sessions)
    assert store.delete_version("demo", "1.0.0") is True
    assert _rows(sessions) == []
    assert engine_outbox.run_reconcile_cycle(target_redis, sessions)["checked"] == 0


def test_flow_delete_purges_outbox(store: FlowStore, sessions, env_enabled):
    """整流程删除清全部 outbox 行。"""
    _publish_version(store)
    _publish_version(store, version="1.0.1")
    assert len(_rows(sessions)) == 2
    assert store.delete_flow("demo") is True
    assert _rows(sessions) == []


# ---- API 层（发布端点 → outbox → 投递） ----


def _api_client(tmp_path, console_redis: FakeRedis):
    from api import flows as flows_api

    flow_store_mod = sys.modules["services.flow_store"]
    flow_store_mod.init_engine(f"sqlite:///{tmp_path / 'api.db'}")
    app = FastAPI()
    app.state.redis = console_redis
    app.state.local_mode = False
    app.state.store = flow_store_mod.get_flow_store()
    app.include_router(flows_api.router, prefix="/api")
    return TestClient(app)


def test_publish_api_enqueues_and_delivers(tmp_path, env_enabled,
                                           console_redis: FakeRedis,
                                           target_redis: FakeRedis):
    """API 发布 → outbox 行落库；console Redis 走原同步、目标 Redis 靠投递。"""
    client = _api_client(tmp_path, console_redis)
    assert client.post("/api/flows", json={"flow_id": "demo"}).status_code == 200
    assert client.put("/api/flows/demo/versions/1.0.0", json={
        "definition": _demo_definition(), "layout": "{}",
    }).status_code == 200
    assert client.post("/api/flows/demo/publish",
                       json={"version": "1.0.0"}).status_code == 200
    # console 自己的 Redis：原有进程内同步行为不变
    assert console_redis.exists("plaita:flow:demo:1.0.0")
    # outbox 行 + 投递到目标
    sessions = sys.modules["services.flow_store"].session_factory()
    rows = _rows(sessions)
    assert len(rows) == 1 and rows[0].status == "pending"
    assert engine_outbox.run_delivery_cycle(target_redis, sessions)["delivered"] == 1
    assert target_redis.exists("plaita:flow:demo:1.0.0")


# ---- 观测 ----


def test_stats_snapshot_and_health_body(store: FlowStore, sessions, env_enabled,
                                        target_redis: FakeRedis):
    """/health 视图：backlog（pending/failed 计数）、最近送达、最近错误。"""
    from types import SimpleNamespace

    from main import build_health_body

    assert engine_outbox.stats_snapshot(sessions)["pending"] == 0
    _publish_version(store)
    engine_outbox.run_delivery_cycle(target_redis, sessions)
    snap = engine_outbox.stats_snapshot(sessions)
    assert snap["pending"] == 0 and snap["failed"] == 0
    assert snap["last_delivered_at"] is not None

    app_state = SimpleNamespace(
        local_mode=False,
        engine_sync={"status": "ready"},
        engine_outbox={**snap, "status": "ready"},
    )
    body = build_health_body(SimpleNamespace(state=app_state))
    assert body["engine_outbox"]["status"] == "ready"
    assert body["engine_outbox"]["last_delivered_at"] == snap["last_delivered_at"]


def test_enqueue_unparsable_definition_skips(store: FlowStore, sessions,
                                             env_enabled):
    """坏定义（非 JSON）：跳过入队 + 不阻断发布（ERROR 可观测）。"""
    store.ensure_flow("broken")
    store.save_flow_definition("broken", "1.0.0", "not-json{{{", "{}")
    out = store.publish_version("broken", "1.0.0")
    assert out.status == "published"
    assert _rows(sessions) == []



# ==== r73：#301 删除投递语义 / #302 源表漂移对账 / c117-06 人工出口 / b117-04 inf-nan ====


def _target_state(target: FakeRedis, flow_id: str, version: str,
                  tenant: str = "") -> dict:
    """目标侧三件的状态快照：版本 key / flow_list 成员 / flow_versions 注册集。"""
    ns = "plaita" if not tenant else f"plaita:{tenant}"
    return {
        "key": bool(target.exists(f"{ns}:flow:{flow_id}:{version}")),
        "in_flow_list": bool(target.sismember(f"{ns}:flow_list", flow_id)),
        "versions_set": bool(target.exists(f"{ns}:flow_versions:{flow_id}")),
        "version_member": bool(
            target.sismember(f"{ns}:flow_versions:{flow_id}", version)
        ),
    }


TARGET_CLEAN = {"key": False, "in_flow_list": False,
                "versions_set": False, "version_member": False}


# ---- #301：删除链路清目标 Redis 三件 ----


def test_delete_flow_purges_target_three_things(store, sessions, env_enabled,
                                                target_redis: FakeRedis):
    """#301：删流程后目标 Redis 的 key/flow_list 成员/flow_versions 注册集三件全清。"""
    _publish_version(store)
    assert engine_outbox.run_delivery_cycle(target_redis, sessions)["delivered"] == 1
    assert _target_state(target_redis, "demo", "1.0.0") == {
        "key": True, "in_flow_list": True,
        "versions_set": True, "version_member": True}
    assert store.delete_flow("demo") is True
    engine_outbox.purge_target_keys("demo", client=target_redis)
    assert _target_state(target_redis, "demo", "1.0.0") == TARGET_CLEAN
    assert _rows(sessions) == []  # DB 行照旧 purge（对账不复活）


def test_delete_version_purges_target_only_that_version(store, sessions,
                                                        env_enabled,
                                                        target_redis: FakeRedis):
    """#301：单版本删除只清该版本 key 与注册成员；兄弟版本与 flow_list 保留。"""
    _publish_version(store, version="1.0.0")
    _publish_version(store, version="1.0.1")
    assert engine_outbox.run_delivery_cycle(target_redis, sessions)["delivered"] == 2
    assert store.delete_version("demo", "1.0.0") is True
    engine_outbox.purge_target_keys("demo", version="1.0.0", client=target_redis)
    assert not target_redis.exists("plaita:flow:demo:1.0.0")
    assert not target_redis.sismember("plaita:flow_versions:demo", "1.0.0")
    # 兄弟版本不受影响
    assert target_redis.exists("plaita:flow:demo:1.0.1")
    assert target_redis.sismember("plaita:flow_list", "demo")


def test_delete_api_purges_target_keys(tmp_path, env_enabled,
                                       console_redis: FakeRedis,
                                       target_redis: FakeRedis):
    """#301 API 层：DELETE /flows/{id} 复用 lifespan 缓存目标连接清三件。"""
    client = _api_client(tmp_path, console_redis)
    client.app.state.engine_outbox_target = target_redis  # lifespan 缓存形态
    assert client.post("/api/flows", json={"flow_id": "ghost"}).status_code == 200
    assert client.put("/api/flows/ghost/versions/1.0.0", json={
        "definition": _demo_definition("ghost"), "layout": "{}",
    }).status_code == 200
    assert client.post("/api/flows/ghost/publish",
                       json={"version": "1.0.0"}).status_code == 200
    sessions = sys.modules["services.flow_store"].session_factory()
    assert engine_outbox.run_delivery_cycle(target_redis, sessions)["delivered"] == 1
    assert _target_state(target_redis, "ghost", "1.0.0")["key"] is True
    assert client.delete("/api/flows/ghost").status_code == 200
    assert _target_state(target_redis, "ghost", "1.0.0") == TARGET_CLEAN


def test_purge_target_disabled_noop(monkeypatch, target_redis: FakeRedis):
    """同 Redis 部署（未配目标）：purge_target_keys no-op（None），不建连不触碰。"""
    monkeypatch.delenv(engine_outbox.TARGET_URL_ENV, raising=False)
    target_redis.set("plaita:flow:demo:1.0.0", "{}")
    assert engine_outbox.purge_target_keys("demo", client=target_redis) is None
    assert target_redis.exists("plaita:flow:demo:1.0.0")  # 未触碰


def test_purge_target_unreachable_named_error_not_raises(env_enabled,
                                                         monkeypatch):
    """#301：目标不可达 → 具名 ERROR + 返回 None，不抛、不阻断删除语义。"""

    def _boom(url=None):
        raise ConnectionError("connection refused")

    monkeypatch.setattr(engine_outbox, "connect_target", _boom)
    assert engine_outbox.purge_target_keys("demo") is None


# ---- #302：对账以源表现值重算 hash，漂移具名 + 重投 ----


def _db_edit_source_definition(sessions, flow_id: str, definition: str):
    """模拟绕开 API 的 DB 级定义修改（运维修数/迁移脚本）。"""
    from models.flow import FlowVersion

    with sessions() as session:
        fv = session.scalars(
            select(FlowVersion).where(FlowVersion.flow_id == flow_id)
        ).first()
        fv.definition = definition
        session.commit()


def test_reconcile_source_definition_drift_redelivers(store, sessions,
                                                      env_enabled,
                                                      target_redis: FakeRedis):
    """#302：源表 definition 被改 → 对账具名漂移、按源表现值重投收敛。"""
    import json as _json

    _publish_version(store)
    assert engine_outbox.run_delivery_cycle(target_redis, sessions)["delivered"] == 1
    key = "plaita:flow:demo:1.0.0"
    _db_edit_source_definition(
        sessions, "demo",
        _json.dumps({**_json.loads(_demo_definition()), "desc": "OPS-EDITED"}))

    rec = engine_outbox.run_reconcile_cycle(target_redis, sessions)
    assert rec["checked"] == 1 and rec["drifted"] == 1 and rec["requeued"] == 0
    row = _rows(sessions)[0]
    assert row.status == "pending" and row.attempts == 0
    assert row.drift_count == 1
    assert "源表定义与行内副本不一致" in row.last_error
    # 行内副本已更新为源表现值（指纹同步重算）
    _, expect_hash = engine_outbox.engine_payload("demo", "1.0.0", row.definition)
    assert row.definition_hash == expect_hash
    assert _json.loads(row.definition)["desc"] == "OPS-EDITED"

    # 重投收敛：目标拿到的是「源表现值」，不再是旧定义
    assert engine_outbox.run_delivery_cycle(target_redis, sessions)["delivered"] == 1
    assert _json.loads(target_redis.get(key))["desc"] == "OPS-EDITED"
    assert _rows(sessions)[0].status == "delivered"
    # /health：drift 累计具名可见
    assert engine_outbox.stats_snapshot(sessions)["drift_total"] == 1


def test_reconcile_source_unparsable_drift_visible_in_dlq(store, sessions,
                                                          env_enabled,
                                                          target_redis: FakeRedis):
    """#302：源表被改成坏 JSON → 漂移具名入队；投递期自检拒投进 DLQ 可见。"""
    import json as _json

    _publish_version(store)
    assert engine_outbox.run_delivery_cycle(target_redis, sessions)["delivered"] == 1
    _db_edit_source_definition(sessions, "demo", "not-json{{{")

    rec = engine_outbox.run_reconcile_cycle(target_redis, sessions)
    assert rec["drifted"] == 1
    row = _rows(sessions)[0]
    assert row.status == "pending" and row.drift_count == 1

    # 投递期自检拒投（不可解析）→ 到上限转 failed，/health 可见
    stats = engine_outbox.run_delivery_cycle(target_redis, sessions,
                                             attempts_cap=1)
    assert stats["delivered"] == 0 and stats["to_dlq"] == 1
    row = _rows(sessions)[0]
    assert row.status == "failed" and "unparsable" in row.last_error
    # 目标保持旧定义，不被坏数据污染
    assert _json.loads(target_redis.get("plaita:flow:demo:1.0.0"))["desc"] == "outbox"


def test_reconcile_source_missing_purges_row_and_target(store, sessions,
                                                        env_enabled,
                                                        target_redis: FakeRedis):
    """#302：源表行已删而 outbox 行仍在（删除竞态残留）→ 清目标三件 + 删行。"""
    from models.flow import FlowVersion

    _publish_version(store)
    assert engine_outbox.run_delivery_cycle(target_redis, sessions)["delivered"] == 1
    with sessions() as session:  # 直删源表行，模拟漏 purge 的残留
        fv = session.scalars(
            select(FlowVersion).where(FlowVersion.flow_id == "demo")).first()
        session.delete(fv)
        session.commit()

    rec = engine_outbox.run_reconcile_cycle(target_redis, sessions)
    assert rec["source_missing"] == 1 and rec["checked"] == 1
    assert _rows(sessions) == []  # 行已收尾删除
    assert _target_state(target_redis, "demo", "1.0.0") == TARGET_CLEAN
    assert engine_outbox.run_reconcile_cycle(target_redis, sessions)["checked"] == 0


def test_reconcile_source_missing_target_unreachable_keeps_row(store, sessions,
                                                               env_enabled):
    """#302：源表缺失但目标清理失败 → 保留行下轮重试（不丢唯一记录）。"""

    class Unreachable:
        def __getattr__(self, name):
            raise ConnectionError("connection refused")

    _publish_version(store)
    with sessions() as session:  # 源表行直删 + 行直置 delivered 进对账视野
        from models.flow import FlowVersion

        fv = session.scalars(
            select(FlowVersion).where(FlowVersion.flow_id == "demo")).first()
        session.delete(fv)
        row = _rows(sessions)[0]
        row.status = "delivered"
        session.add(row)
        session.commit()

    rec = engine_outbox.run_reconcile_cycle(Unreachable(), sessions)
    assert rec["source_missing"] == 0 and rec["errors"] == 1
    assert len(_rows(sessions)) == 1  # 行保留（唯一记录不丢）
    assert "保留行重试" in _rows(sessions)[0].last_error


def test_reconcile_healthy_row_no_drift_signal(store, sessions, env_enabled,
                                               target_redis: FakeRedis):
    """#302 守恒：源表与目标都健康 → 无漂移信号、drift_total 恒 0。"""
    _publish_version(store)
    assert engine_outbox.run_delivery_cycle(target_redis, sessions)["delivered"] == 1
    rec = engine_outbox.run_reconcile_cycle(target_redis, sessions)
    assert rec["drifted"] == 0 and rec["source_missing"] == 0
    assert _rows(sessions)[0].drift_count == 0
    assert engine_outbox.stats_snapshot(sessions)["drift_total"] == 0


# ---- c117-06：DLQ 人工出口（resolve / discard API） ----


def _api_sessions():
    return sys.modules["services.flow_store"].session_factory()


def _api_publish(client, flow_id: str = "demo"):
    assert client.post("/api/flows", json={"flow_id": flow_id}).status_code == 200
    assert client.put(f"/api/flows/{flow_id}/versions/1.0.0", json={
        "definition": _demo_definition(flow_id), "layout": "{}",
    }).status_code == 200
    assert client.post(f"/api/flows/{flow_id}/publish",
                       json={"version": "1.0.0"}).status_code == 200


def _api_row_to_failed(client, flow_id: str = "demo"):
    """经 API 建行并投到不可达目标直至 failed（DLQ 档）；返回 (row, sessions)。"""

    class Unreachable:
        def __getattr__(self, name):
            raise ConnectionError("connection refused")

    _api_publish(client, flow_id)
    sessions = _api_sessions()
    now = None
    for _ in range(3):  # 推进时钟穿过退避，直至 failed
        engine_outbox.run_delivery_cycle(Unreachable(), sessions,
                                         attempts_cap=2, now=now)
        row = _rows(sessions, flow_id=flow_id)[0]
        if row.status == "failed":
            return row, sessions
        now = row.next_attempt_at + timedelta(seconds=1)
    raise AssertionError("行未按预期转 failed")


def test_dlq_resolve_api_exits_queue_with_trace(tmp_path, env_enabled,
                                                console_redis: FakeRedis):
    """c117-06：failed 行 resolve → resolved 终态；不再进投递扫描；留痕。"""
    client = _api_client(tmp_path, console_redis)
    row, sessions = _api_row_to_failed(client)
    resp = client.post(f"/api/flows/publish-outbox/{row.id}/resolve")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "resolved" and body["flow_id"] == "demo"
    after = _rows(sessions)[0]
    assert after.status == "resolved"
    assert "manually resolved" in after.last_error
    # 不再被投递扫描 / 不再自动重排
    assert engine_outbox.run_delivery_cycle(
        FakeRedis(decode_responses=True), sessions)["scanned"] == 0
    snap = engine_outbox.stats_snapshot(sessions)
    assert snap["resolved"] == 1 and snap["failed"] == 0
    # 幂等：重复 resolve 仍 200
    assert client.post(f"/api/flows/publish-outbox/{row.id}/resolve").status_code == 200


def test_dlq_discard_api_deletes_row(tmp_path, env_enabled,
                                     console_redis: FakeRedis):
    """c117-06：failed 行 discard → 行删除；对账/投递均不再触及。"""
    client = _api_client(tmp_path, console_redis)
    row, sessions = _api_row_to_failed(client)
    resp = client.post(f"/api/flows/publish-outbox/{row.id}/discard")
    assert resp.status_code == 200
    assert resp.json()["success"] is True
    assert _rows(sessions) == []
    assert engine_outbox.stats_snapshot(sessions)["resolved"] == 0


def test_manual_exit_rejects_delivered_row(tmp_path, env_enabled,
                                           console_redis: FakeRedis,
                                           target_redis: FakeRedis):
    """c117-06：delivered 行 resolve/discard 都 409（会失去对账覆盖/无意义）。"""
    client = _api_client(tmp_path, console_redis)
    _api_publish(client)
    sessions = _api_sessions()
    assert engine_outbox.run_delivery_cycle(target_redis, sessions)["delivered"] == 1
    row = _rows(sessions)[0]
    assert client.post(
        f"/api/flows/publish-outbox/{row.id}/resolve").status_code == 409
    assert client.post(
        f"/api/flows/publish-outbox/{row.id}/discard").status_code == 409
    assert _rows(sessions)[0].status == "delivered"


def test_manual_exit_scoped_404_and_missing(tmp_path, env_enabled,
                                            console_redis: FakeRedis):
    """c117-06：他租户行 / 不存在的行 → 404（不越权、不泄露存在性）。"""
    client = _api_client(tmp_path, console_redis)
    sessions = _api_sessions()
    with sessions() as session:
        session.add(FlowPublishOutbox(
            tenant_id="t1", flow_id="t1flow", version="1.0.0",
            definition_hash="deadbeef", definition="{}", status="failed"))
        session.commit()
    row = _rows(sessions, flow_id="t1flow")[0]
    # default 租户作用域的调用方碰他租户行 → 404
    assert client.post(
        f"/api/flows/publish-outbox/{row.id}/resolve").status_code == 404
    assert client.post(
        f"/api/flows/publish-outbox/999999/discard").status_code == 404


# ---- b117-04：_env_float 对 inf/nan 具名拒（对齐 DNS_TTL 先例） ----


def test_env_float_rejects_inf_and_nan(monkeypatch, caplog):
    """b117-04：poll/reconcile 的 inf/nan → 具名 WARNING + 回退默认（不再静默）。"""
    import logging

    cases = (
        (engine_outbox.POLL_ENV, engine_outbox.poll_seconds,
         engine_outbox.DEFAULT_POLL_SECONDS),
        (engine_outbox.RECONCILE_ENV, engine_outbox.reconcile_seconds,
         engine_outbox.DEFAULT_RECONCILE_SECONDS),
    )
    for env_name, fn, default in cases:
        for raw in ("inf", "nan", "-inf"):
            monkeypatch.setenv(env_name, raw)
            with caplog.at_level(logging.WARNING):
                assert fn() == default, (env_name, raw)
            assert any("非法" in r.message for r in caplog.records), (env_name, raw)
            monkeypatch.delenv(env_name)
            caplog.clear()
