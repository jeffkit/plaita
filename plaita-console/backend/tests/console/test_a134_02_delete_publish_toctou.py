"""A134-02 负控：delete-vs-publish TOCTOU——删除守卫「先查后删」、DELETE 无 status 谓词（B2 r88）。

缺陷（A1 台账第一百三十四轮 §2，P2）：

``_enforce_version_delete_guard`` 的 ``version_published`` 409 判据读的是
**独立事务快照**（delete_version 端点 ``get_version`` / delete_flow 端点
``list_versions``）；守卫内两次 Redis 扫描 + ``_outbox_purge_target`` 把
「判据读 → 删行」窗口拉宽到百 ms 级。store 侧 ``delete_version``
（``session.delete(row)`` 按主键）与 ``delete_flow``（批量 DELETE）**均无
status 谓词**——窗内并发 ``publish_version`` 提交翻转（draft→published +
outbox 行 + 引擎同步）后，删除事务无条件删行 + ``_outbox_purge`` 清掉刚写
的 outbox 行：刚发布版本被**静默删除**（无 force 语义参与）；跨 Redis 部署
outbox 被 purge ⇒ 定义永不到达目标引擎 ⇒ 在途/新 start 撞
FlowDefinitionMissingError 族。A2R54-02 的「谓词 + rowcount→409」药方只套
了 publish/save 面，delete 面未套。

修法（本仓 fix）：DELETE 钉在守卫观察到的 ``(version, status)`` 快照上——
``delete_version(expected_status=)`` 单行条件 DELETE、``delete_flow(
expected_versions=)`` 逐行条件 DELETE + 幸存者复核；rowcount 失配 →
``VersionConflictError`` → API 409 ``version_stale``（复用 A2R54-02 具名面）。
force 只越 published **状态守卫**、不越快照漂移：force+观察 draft+窗内
publish 同样 409，重试 force 即过（此时守卫看到 published、快照一致）。
``_outbox_delete_guard`` 异常路径按快照恢复 deleting 行（#303 守恒）。

判绿口径=断言「正确行为」成立；先红后绿证据：
docs/proofs/r88-a134-02-delete-publish-toctou.md。
"""
import json
import sys
from pathlib import Path

import pytest
from fakeredis import FakeRedis
from fastapi import Depends, FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import flows as flows_api  # noqa: E402
from auth import require_auth  # noqa: E402
from models.flow import FlowPublishOutbox, FlowRecord, FlowVersion  # noqa: E402
from services import engine_outbox, flow_store  # noqa: E402
from services.engine_sync import sync_flow_to_engine  # noqa: E402
from sqlalchemy import select  # noqa: E402

ADMIN_KEY = "a134-02-admin"
TENANT = "default"
FLOW = "toctou-flow"
TARGET_URL = "redis://target-env:6379/0"


def _definition(flow_id: str = FLOW) -> str:
    return json.dumps({
        "flow_id": flow_id,
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "end"},
            {"type": "end", "id": "end", "output": "$INPUT.name",
             "resultType": "success"},
        ],
    })


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


@pytest.fixture()
def redis(tmp_path, monkeypatch) -> FakeRedis:
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    flow_store.init_engine(f"sqlite:///{tmp_path / 'a134-02.db'}")
    return FakeRedis(decode_responses=True)


@pytest.fixture()
def outbox_enabled(monkeypatch):
    """跨 Redis 部署形态：publish 写 outbox 行、删除走 #303 删除协议。"""
    monkeypatch.setenv(engine_outbox.TARGET_URL_ENV, TARGET_URL)
    monkeypatch.setenv(engine_outbox.POLL_ENV, "0.05")
    monkeypatch.setenv(engine_outbox.RECONCILE_ENV, "0.2")
    yield


def _client(redis, target_redis=None) -> TestClient:
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.state.store = flow_store.get_flow_store()
    if target_redis is not None:
        app.state.engine_outbox_target = target_redis
    app.include_router(
        flows_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    return TestClient(app)


def _headers() -> dict:
    return {"X-Admin-API-Key": ADMIN_KEY}


def _seed(client: TestClient, version: str = "1.0.0", flow_id: str = FLOW) -> None:
    client.post("/api/flows", json={"flow_id": flow_id}, headers=_headers())
    r = client.put(f"/api/flows/{flow_id}/versions/{version}",
                   json={"definition": _definition(flow_id)}, headers=_headers())
    assert r.status_code == 200, r.text


def _publish_api(client: TestClient, version: str, flow_id: str = FLOW) -> None:
    r = client.post(f"/api/flows/{flow_id}/publish", json={"version": version},
                    headers=_headers())
    assert r.status_code == 200, r.text


def _eng_key(version: str, flow_id: str = FLOW) -> str:
    return f"plaita:flow:{flow_id}:{version}"


def _detail(r) -> dict:
    return (r.json() or {}).get("detail", {}) or {}


def _row(version: str, flow_id: str = FLOW):
    with flow_store.get_flow_store()._session_local() as s:
        return s.scalars(select(FlowVersion).where(
            FlowVersion.tenant_id == TENANT, FlowVersion.flow_id == flow_id,
            FlowVersion.version == version)).first()


def _record(flow_id: str = FLOW):
    with flow_store.get_flow_store()._session_local() as s:
        return s.scalars(select(FlowRecord).where(
            FlowRecord.tenant_id == TENANT, FlowRecord.flow_id == flow_id)).first()


def _outbox_rows(flow_id: str = FLOW):
    with flow_store.session_factory()() as s:
        return [(r.version, r.status) for r in s.scalars(
            select(FlowPublishOutbox).where(
                FlowPublishOutbox.flow_id == flow_id
            ).order_by(FlowPublishOutbox.id.asc())).all()]


# ---- 窗口注入 seam ----
#
# 「判据已过、DELETE 未执行」的窗口：``_outbox_purge_target`` 是删除端点里
# 守卫通过后、``_store().delete_*`` 之前的最后一步（守卫 Redis 两扫之后），
# 在其返回后注入并发 publish = A1 剧本「判据过 → publish 插窗 → DELETE」的
# 确定性复刻。hook 只触发一次（重试路径不再插窗）。


def _arm_window(monkeypatch, hook) -> dict:
    orig = flows_api._outbox_purge_target
    fired = {"n": 0}

    def seam(flow_id, **kw):
        orig(flow_id, **kw)
        if fired["n"] == 0:
            fired["n"] += 1
            hook()

    monkeypatch.setattr(flows_api, "_outbox_purge_target", seam)
    return fired


def _window_publish(redis, version: str, flow_id: str = FLOW) -> None:
    """并发 publish 的全部副作用：store 翻转（+outbox 行）+ 引擎同步。"""
    store = flow_store.get_flow_store()
    out = store.publish_version(flow_id, version, tenant_id=TENANT)
    assert out.status == "published"
    sync_flow_to_engine(redis, flow_id, version, out.definition, tenant_id=TENANT)


def _window_create_and_publish(redis, version: str, flow_id: str = FLOW) -> None:
    """import 直发链形态：窗内新建版本并立即发布。"""
    flow_store.get_flow_store().save_flow_definition(
        flow_id=flow_id, version=version, definition=_definition(flow_id),
        tenant_id=TENANT, created_by="window",
    )
    _window_publish(redis, version, flow_id)


# ============ 负控：delete_version 面 ============


def test_delete_version_in_window_publish_409_row_and_engine_key_kept(
        redis, monkeypatch):
    """守卫读 draft → 窗内 publish 翻转 → DELETE：具名 409、行保持 published、引擎键在。

    修前：200，刚发布行被静默删 + 引擎键被 remove_flow_version_from_engine 清掉。
    """
    client = _client(redis)
    _seed(client)
    fired = _arm_window(monkeypatch, lambda: _window_publish(redis, "1.0.0"))

    r = client.delete(f"/api/flows/{FLOW}/versions/1.0.0", headers=_headers())
    assert fired["n"] == 1
    assert r.status_code == 409, r.text
    assert _detail(r)["conflict"] == "version_stale"
    assert _detail(r)["code"] == "version_stale"
    row = _row("1.0.0")
    assert row is not None and row.status == "published"
    assert row.published_at is not None
    assert redis.exists(_eng_key("1.0.0")) == 1


def test_delete_version_in_window_publish_outbox_row_not_purged(
        redis, outbox_enabled, monkeypatch):
    """跨 Redis 形态：窗内 publish 写出的 outbox 行不得被删除事务 purge。

    修前：删除事务 ``_outbox_purge`` 按 scope 清掉 pending 行 → 定义永不到达
    目标引擎（#304 FlowDefinitionMissingError 族）。修后：409，行保持 pending。
    """
    target = FakeRedis(decode_responses=True)
    client = _client(redis, target_redis=target)
    _seed(client)
    assert _outbox_rows() == []
    _arm_window(monkeypatch, lambda: _window_publish(redis, "1.0.0"))

    r = client.delete(f"/api/flows/{FLOW}/versions/1.0.0", headers=_headers())
    assert r.status_code == 409, r.text
    assert _outbox_rows() == [("1.0.0", engine_outbox.STATUS_PENDING)]
    assert _row("1.0.0").status == "published"
    # 行还在、对账/投递照常：投递轮把定义送达目标（修前该行已被 purge，永不送达）
    sessions = flow_store.session_factory()
    assert engine_outbox.run_delivery_cycle(target, sessions)["delivered"] == 1
    assert target.exists(_eng_key("1.0.0")) == 1


def test_delete_version_force_in_window_publish_pinned_409_then_retry_ok(
        redis, monkeypatch):
    """force 路径同窗核：force 只越 published 状态守卫，不越快照漂移。

    守卫观察 draft（force 无事可越）→ 窗内 publish → DELETE 钉在 draft 失配
    → 409（修前：force 掩护下静默删掉 published 行）。重试 force：守卫看到
    published、显式放行、快照一致 → 200（force 语义守恒）。
    """
    client = _client(redis)
    _seed(client)
    fired = _arm_window(monkeypatch, lambda: _window_publish(redis, "1.0.0"))

    r = client.delete(f"/api/flows/{FLOW}/versions/1.0.0?force=true",
                      headers=_headers())
    assert fired["n"] == 1
    assert r.status_code == 409, r.text
    assert _detail(r)["conflict"] == "version_stale"
    assert _row("1.0.0").status == "published"
    assert redis.exists(_eng_key("1.0.0")) == 1

    r2 = client.delete(f"/api/flows/{FLOW}/versions/1.0.0?force=true",
                       headers=_headers())
    assert fired["n"] == 1  # 重试不再插窗
    assert r2.status_code == 200, r2.text
    assert _row("1.0.0") is None
    assert redis.exists(_eng_key("1.0.0")) == 0


# ============ 负控：delete_flow 面 ============


def test_delete_flow_in_window_publish_409_nothing_deleted(redis, monkeypatch):
    """delete_flow 批量 DELETE 同款：窗内 publish 翻转 → 409，flow 记录与版本行全在。"""
    client = _client(redis)
    _seed(client)
    fired = _arm_window(monkeypatch, lambda: _window_publish(redis, "1.0.0"))

    r = client.delete(f"/api/flows/{FLOW}", headers=_headers())
    assert fired["n"] == 1
    assert r.status_code == 409, r.text
    assert _detail(r)["conflict"] == "version_stale"
    assert _record() is not None
    assert _row("1.0.0").status == "published"
    assert redis.exists(_eng_key("1.0.0")) == 1


def test_delete_flow_in_window_new_version_published_409_survivor(
        redis, monkeypatch):
    """窗内 import 直发链新建 + 发布 1.0.1（守卫从未见过）→ 幸存者复核 409。

    修前：批量 DELETE 把守卫没扫过的 published 1.0.1 一并静默删掉。修后：
    事务整体回滚——1.0.0 draft 与 1.0.1 published 都在、flow 记录在。
    """
    client = _client(redis)
    _seed(client)
    _arm_window(monkeypatch, lambda: _window_create_and_publish(redis, "1.0.1"))

    r = client.delete(f"/api/flows/{FLOW}", headers=_headers())
    assert r.status_code == 409, r.text
    assert _detail(r)["conflict"] == "version_stale"
    assert _record() is not None
    assert _row("1.0.0").status == "draft"
    assert _row("1.0.1").status == "published"
    assert redis.exists(_eng_key("1.0.1")) == 1


def test_delete_flow_force_in_window_outbox_deleting_rows_restored(
        redis, outbox_enabled, monkeypatch):
    """#303 删除协议 × A134-02：409 回滚时 deleting 行按快照恢复、窗内新行不动。

    1.0.0 published+delivered、2.0.0 draft；force 删 flow：守卫放行 →
    deleting_guard 把 1.0.0 行标 deleting → 窗内 publish 2.0.0（新 pending 行）
    → 逐行 DELETE：1.0.0@published 命中、2.0.0@draft 失配 → 冲突回滚。
    终态：两版本行都在、1.0.0 outbox 行恢复 delivered、2.0.0 pending 未被
    purge；「先清后删」已清的目标键由对账 + 投递自愈（#301 方向守恒）。
    """
    target = FakeRedis(decode_responses=True)
    client = _client(redis, target_redis=target)
    _seed(client, "1.0.0")
    _publish_api(client, "1.0.0")
    sessions = flow_store.session_factory()
    assert engine_outbox.run_delivery_cycle(target, sessions)["delivered"] == 1
    assert target.exists(_eng_key("1.0.0")) == 1
    r = client.put(f"/api/flows/{FLOW}/versions/2.0.0",
                   json={"definition": _definition()}, headers=_headers())
    assert r.status_code == 200, r.text
    assert _outbox_rows() == [("1.0.0", engine_outbox.STATUS_DELIVERED)]

    seen: dict = {}

    def hook():
        seen["mid_window"] = _outbox_rows()  # 1.0.0 已标 deleting
        _window_publish(redis, "2.0.0")

    _arm_window(monkeypatch, hook)
    r = client.delete(f"/api/flows/{FLOW}?force=true", headers=_headers())
    assert r.status_code == 409, r.text
    assert seen["mid_window"] == [("1.0.0", engine_outbox.STATUS_DELETING)]
    assert _record() is not None
    assert _row("1.0.0").status == "published"
    assert _row("2.0.0").status == "published"
    assert _outbox_rows() == [
        ("1.0.0", engine_outbox.STATUS_DELIVERED),
        ("2.0.0", engine_outbox.STATUS_PENDING),
    ]
    # 目标键已被「先清后删」清掉：delivered 行由对账打回重投，pending 行投递
    assert target.exists(_eng_key("1.0.0")) == 0
    engine_outbox.run_reconcile_cycle(target, sessions)
    stats = engine_outbox.run_delivery_cycle(target, sessions)
    assert stats["delivered"] == 2, stats
    assert target.exists(_eng_key("1.0.0")) == 1
    assert target.exists(_eng_key("2.0.0")) == 1


# ============ 守恒：无竞窗与既有语义 ============


def test_no_race_draft_delete_still_200(redis, monkeypatch):
    client = _client(redis)
    _seed(client)
    fired = _arm_window(monkeypatch, lambda: None)
    r = client.delete(f"/api/flows/{FLOW}/versions/1.0.0", headers=_headers())
    assert fired["n"] == 1
    assert r.status_code == 200, r.text
    assert _row("1.0.0") is None


def test_no_race_force_published_delete_still_200(redis):
    client = _client(redis)
    _seed(client)
    _publish_api(client, "1.0.0")
    r = client.delete(f"/api/flows/{FLOW}/versions/1.0.0?force=true",
                      headers=_headers())
    assert r.status_code == 200, r.text
    assert _row("1.0.0") is None
    assert redis.exists(_eng_key("1.0.0")) == 0


def test_no_race_delete_flow_mixed_statuses_force_200(redis):
    """逐行钉扎对混合状态（published + draft）force 删除零误伤。"""
    client = _client(redis)
    _seed(client, "1.0.0")
    _publish_api(client, "1.0.0")
    r = client.put(f"/api/flows/{FLOW}/versions/1.1.0",
                   json={"definition": _definition()}, headers=_headers())
    assert r.status_code == 200, r.text
    r = client.delete(f"/api/flows/{FLOW}?force=true", headers=_headers())
    assert r.status_code == 200, r.text
    assert _record() is None
    assert _row("1.0.0") is None and _row("1.1.0") is None
    assert redis.exists(_eng_key("1.0.0")) == 0


def test_published_without_force_still_409_version_published(redis):
    """#304 状态守卫口径不变：published 无 force → 409 version_published（非 version_stale）。"""
    client = _client(redis)
    _seed(client)
    _publish_api(client, "1.0.0")
    r = client.delete(f"/api/flows/{FLOW}/versions/1.0.0", headers=_headers())
    assert r.status_code == 409, r.text
    assert _detail(r)["code"] == "version_published"
    assert _row("1.0.0").status == "published"


def test_delete_missing_version_404_untouched(redis):
    client = _client(redis)
    _seed(client)
    r = client.delete(f"/api/flows/{FLOW}/versions/9.9.9", headers=_headers())
    assert r.status_code == 404, r.text
    assert _detail(r)["code"] == "version_not_found"
    assert _row("1.0.0").status == "draft"


def test_delete_missing_flow_404(redis):
    client = _client(redis)
    r = client.delete("/api/flows/no-such-flow", headers=_headers())
    assert r.status_code == 404, r.text
    assert _detail(r)["code"] == "flow_not_found"


# ============ store 层：谓词面直证 ============


def test_store_delete_version_expected_status_mismatch_conflict(redis):
    store = flow_store.get_flow_store()
    store.create_flow(FLOW, tenant_id=TENANT)
    store.save_flow_definition(flow_id=FLOW, version="1.0.0",
                               definition=_definition(), tenant_id=TENANT)
    store.publish_version(FLOW, "1.0.0", tenant_id=TENANT)
    with pytest.raises(flow_store.VersionConflictError) as ei:
        store.delete_version(FLOW, "1.0.0", tenant_id=TENANT,
                             expected_status="draft")
    assert "published" in str(ei.value)
    assert _row("1.0.0").status == "published"
    # 快照一致即删
    assert store.delete_version(FLOW, "1.0.0", tenant_id=TENANT,
                                expected_status="published") is True
    assert _row("1.0.0") is None


def test_store_delete_version_missing_still_lookup_error(redis):
    store = flow_store.get_flow_store()
    store.create_flow(FLOW, tenant_id=TENANT)
    with pytest.raises(LookupError):
        store.delete_version(FLOW, "0.0.1", tenant_id=TENANT,
                             expected_status="draft")


def test_store_delete_flow_pinned_rows_and_survivor_check(redis):
    store = flow_store.get_flow_store()
    store.create_flow(FLOW, tenant_id=TENANT)
    for v in ("1.0.0", "1.1.0"):
        store.save_flow_definition(flow_id=FLOW, version=v,
                                   definition=_definition(), tenant_id=TENANT)
    store.publish_version(FLOW, "1.0.0", tenant_id=TENANT)
    # 状态失配：1.0.0 观察为 draft
    with pytest.raises(flow_store.VersionConflictError):
        store.delete_flow(FLOW, tenant_id=TENANT,
                          expected_versions={"1.0.0": "draft", "1.1.0": "draft"})
    assert _record() is not None and _row("1.0.0") is not None
    # 幸存者：守卫没见过 1.1.0
    with pytest.raises(flow_store.VersionConflictError):
        store.delete_flow(FLOW, tenant_id=TENANT,
                          expected_versions={"1.0.0": "published"})
    assert _record() is not None
    assert _row("1.0.0") is not None and _row("1.1.0") is not None
    # 快照完整一致即删
    assert store.delete_flow(
        FLOW, tenant_id=TENANT,
        expected_versions={"1.0.0": "published", "1.1.0": "draft"},
    ) is True
    assert _record() is None
    assert _row("1.0.0") is None and _row("1.1.0") is None


def test_store_legacy_unconditional_delete_kept(redis):
    """不传快照（存量直调方）保持无条件删除语义。"""
    store = flow_store.get_flow_store()
    store.create_flow(FLOW, tenant_id=TENANT)
    store.save_flow_definition(flow_id=FLOW, version="1.0.0",
                               definition=_definition(), tenant_id=TENANT)
    store.publish_version(FLOW, "1.0.0", tenant_id=TENANT)
    assert store.delete_version(FLOW, "1.0.0", tenant_id=TENANT) is True
    store.save_flow_definition(flow_id=FLOW, version="2.0.0",
                               definition=_definition(), tenant_id=TENANT)
    store.publish_version(FLOW, "2.0.0", tenant_id=TENANT)
    assert store.delete_flow(FLOW, tenant_id=TENANT) is True
    assert _record() is None
