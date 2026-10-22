"""A2R54-01/02 负控：flows 版本发布面 TOCTOU + CAS 状态盲写（B3 r86）。

缺陷（A2 台账 r54 提报 / r56 核销双红）：

1. **A2R54-01 #300 门快照 TOCTOU**：publish_flow 的门判据取 get_version 快照
   → publish_version 新会话重 SELECT 无复核——竞窗内并发 CAS 草稿保存落进，
   未过门定义被发布 + 引擎同步 + outbox 三件。import 直发链同构。
2. **A2R54-02 CAS 状态盲写**：_save_flow_once 既有行分支「SELECT(draft) 检查 →
   CAS UPDATE」两语句间夹进 publish 提交：CAS 谓词不含 status 且
   publish_version 不前进 updated_at ⇒ 已发布行被打回 draft + 定义覆盖 +
   published_at 残留。

修法（本仓 fix）：publish_version 改单条条件 UPDATE 翻转（谓词
``status='draft'`` + 翻转前 updated_at 快照钉扎，发布同时**前进 updated_at**）
+ 新增 ``gate_fn``（发布事务内对 fresh 行 re-gate，API publish/import 两处
注入）；_cas_update_draft 谓词补 ``AND status='draft'``（legacy 降级档同样
受保护）。

判绿口径=断言「正确行为」成立。先红后绿全量证据：
docs/proofs/r86-flows-publish-gate-toctou.md（探针 r56 原版换根复跑 2 红 →
修后 8/8 绿 + 原探针红格2/佐证格翻绿）。
"""
import json
import sys
from pathlib import Path

import pytest
from fastapi import Depends, FastAPI
from fakeredis import FakeRedis
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import flows as flows_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import flow_store  # noqa: E402
from services.capability_gate import (  # noqa: E402
    UnsupportedNodeTypesError,
    gate_flow_nodes,
)

ADMIN_KEY = "cluster-admin-key"
TENANT = "default"

GO_SUPPORTED = ["assignment", "code", "end", "http", "parallel", "start", "switch"]


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


@pytest.fixture()
def redis(tmp_path, monkeypatch):
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    flow_store.init_engine(f"sqlite:///{tmp_path}/a2r54.db")
    r = FakeRedis(decode_responses=True)
    _register_worker(r, "go-1", engine="plaita-go", supported=GO_SUPPORTED)
    _register_worker(r, "py-1", engine="python")
    return r


def _register_worker(redis, instance_id, *, engine, supported=None):
    from plaita.server.capability import build_capability_manifest, python_worker_manifest

    metadata = {}
    if engine == "plaita-go":
        metadata["queue_name"] = "plaita:flow:queue:go"
        metadata["capability_manifest"] = build_capability_manifest(
            supported or [], engine=engine, unsupported_node_types=(
                ["calculate", "case", "filter", "find", "map", "mock", "reference",
                 "reduce", "while"]
            )
        )
    else:
        metadata["queue_name"] = "plaita:flow:queue"
        metadata["capability_manifest"] = python_worker_manifest()
    redis.setex(
        f"plaita:registry:flow_worker:{instance_id}", 30,
        json.dumps({
            "instance_id": instance_id,
            "service_type": "flow_worker",
            "host": "localhost",
            "status": "running",
            "metadata": metadata,
            "active_tasks": 0,
        }),
    )


def _client(redis) -> TestClient:
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.state.store = flow_store.get_flow_store()
    app.include_router(
        flows_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    return TestClient(app)


def _headers():
    return {"X-Admin-API-Key": ADMIN_KEY}


def _plain_def(flow_id="racy"):
    return json.dumps({
        "flow_id": flow_id,
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "end"},
            {"type": "end", "id": "end", "output": "$INPUT.name", "resultType": "success"},
        ],
    })


def _map_def(flow_id="racy"):
    """go 池不支持的 map 节点——re-gate 必拒的「未过门定义」形态。"""
    return json.dumps({
        "flow_id": flow_id,
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "m"},
            {"type": "map", "id": "m", "over": "$INPUT.items", "next": "end",
             "child_flow": {
                 "flow_id": "child",
                 "nodes": [
                     {"type": "start", "id": "s", "next": "e"},
                     {"type": "end", "id": "e", "output": "$INPUT",
                      "resultType": "success"},
                 ],
             }},
            {"type": "end", "id": "end", "output": "$INPUT", "resultType": "success"},
        ],
    })


def _seed(runtime="go", definition=None, created_by="t"):
    store = flow_store.get_flow_store()
    store.create_flow("racy", tenant_id=TENANT)
    store.save_flow_definition(
        flow_id="racy", version="1.0.0",
        definition=definition or _plain_def(), created_by=created_by,
        tenant_id=TENANT, runtime=runtime,
    )
    return store


def _regate(fresh):
    """真实能力门（同 API gate_fn 语义）：fresh 行对象 → 过门或抛。"""
    gate_flow_nodes(
        redis_ctx["r"], getattr(fresh, "runtime", None) or "python",
        fresh.definition, flow_id=fresh.flow_id, version=fresh.version,
    )


redis_ctx: dict = {}


def _raw_row(store):
    from sqlalchemy import select

    from models.flow import FlowVersion

    with store._session_local() as s:
        return s.scalars(select(FlowVersion).where(
            FlowVersion.tenant_id == TENANT, FlowVersion.flow_id == "racy",
            FlowVersion.version == "1.0.0")).first()


class TestRegateFreshRow:
    """A2R54-01：发布事务内 re-gate——门校验对象=落盘对象。"""

    def test_race_before_publish_ungated_fresh_row_rejected(self, redis):
        """门快照读 D1 → 竞窗 CAS 落 D2（go 池不支持 map）→ re-gate 拒，版本保持 draft。"""
        redis_ctx["r"] = redis
        store = _seed()
        snap = store.get_version("racy", "1.0.0", tenant_id=TENANT)
        store.save_flow_definition(
            flow_id="racy", version="1.0.0", definition=_map_def(),
            tenant_id=TENANT, base_updated_at=snap.updated_at.isoformat(),
            created_by="racer",
        )
        with pytest.raises(UnsupportedNodeTypesError):
            store.publish_version("racy", "1.0.0", tenant_id=TENANT, gate_fn=_regate)
        row = _raw_row(store)
        assert row.status == "draft"
        assert json.loads(row.definition)["nodes"][1]["type"] == "map"
        assert row.published_at is None

    def test_import_direct_publish_chain_same_rejection(self, redis):
        """import 直发链同构：promotion 草稿 + 同一门 → 未过门 fresh 行同样拒。"""
        redis_ctx["r"] = redis
        store = _seed(created_by="promotion")
        snap = store.get_version("racy", "1.0.0", tenant_id=TENANT)
        store.save_flow_definition(
            flow_id="racy", version="1.0.0", definition=_map_def(),
            tenant_id=TENANT, base_updated_at=snap.updated_at.isoformat(),
            created_by="racer",
        )
        with pytest.raises(UnsupportedNodeTypesError):
            store.publish_version("racy", "1.0.0", tenant_id=TENANT, gate_fn=_regate)
        row = _raw_row(store)
        assert row.status == "draft" and row.published_at is None

    def test_no_race_gated_definition_publishes(self, redis):
        """无竞窗：re-gate 过门即发布，正常面不误伤。"""
        redis_ctx["r"] = redis
        store = _seed()
        out = store.publish_version("racy", "1.0.0", tenant_id=TENANT, gate_fn=_regate)
        assert out.status == "published" and json.loads(out.definition)["nodes"][1]["type"] == "end"


class TestPublishPinning:
    """A2R54-01 钉扎：翻转前 updated_at 快照——门校验期间落进的并发保存 → 409。"""

    def test_race_during_gate_conflicts(self, redis):
        redis_ctx["r"] = redis
        store = _seed()
        snap = store.get_version("racy", "1.0.0", tenant_id=TENANT)
        race_base = snap.updated_at.isoformat()

        def gate_with_race(fresh):
            store.save_flow_definition(
                flow_id="racy", version="1.0.0", definition=_map_def(),
                tenant_id=TENANT, base_updated_at=race_base,
                created_by="racer2",
            )
            _regate(fresh)

        with pytest.raises(flow_store.VersionConflictError) as ei:
            store.publish_version("racy", "1.0.0", tenant_id=TENANT,
                                  gate_fn=gate_with_race)
        assert ei.value.latest_updated_at is not None
        row = _raw_row(store)
        assert row.status == "draft" and row.published_at is None
        assert json.loads(row.definition)["nodes"][1]["type"] == "map"


class TestPublishedRowProtection:
    """A2R54-02：publish 前进 updated_at + CAS status 谓词——已发布行不被打回。"""

    def test_publish_advances_updated_at_stale_save_conflicts(self, redis):
        """publish 后旧基准保存：公开路径 SELECT 检查先拦（已发布），行不被覆盖。"""
        redis_ctx["r"] = redis
        store = _seed(runtime="python")
        base = store.get_version("racy", "1.0.0", tenant_id=TENANT).updated_at
        store.publish_version("racy", "1.0.0", tenant_id=TENANT)
        with pytest.raises(ValueError, match="已发布"):
            store.save_flow_definition(
                flow_id="racy", version="1.0.0", definition=_plain_def("other"),
                tenant_id=TENANT, base_updated_at=base.isoformat(),
                created_by="stale-saver",
            )
        row = _raw_row(store)
        assert row.status == "published"
        assert json.loads(row.definition)["nodes"][1]["type"] == "end"
        assert row.published_at is not None

    def test_sandwich_cas_update_blocked_by_status_predicate(self, redis):
        """faithful 语句序复刻：会话内 SELECT(draft) → （他会话）publish → 同会话 CAS UPDATE。"""
        redis_ctx["r"] = redis
        store = _seed(runtime="python")
        from sqlalchemy import select, text

        from models.flow import FlowVersion

        base = store.get_version("racy", "1.0.0", tenant_id=TENANT).updated_at
        with store._session_local() as s1:
            existing = s1.scalars(select(FlowVersion).where(
                FlowVersion.tenant_id == TENANT, FlowVersion.flow_id == "racy",
                FlowVersion.version == "1.0.0")).first()
            assert existing.status == "draft"
            store.publish_version("racy", "1.0.0", tenant_id=TENANT)  # 并发发布提交
            advanced = s1.execute(text(
                "SELECT updated_at FROM flow_versions "
                "WHERE tenant_id=:t AND flow_id=:f AND version=:v"),
                {"t": TENANT, "f": "racy", "v": "1.0.0"}).first()[0]
            assert advanced is not None and advanced != base  # 修①：发布前进基准
            ok = store._cas_update_draft(
                s1, flow_id="racy", version="1.0.0", tenant_id=TENANT,
                definition=_map_def(), layout="", status="draft",
                base_updated_at=base.isoformat(), force=False, runtime=None,
            )
            s1.commit()
        assert ok is False  # CAS 失配（updated_at 前进 + status=published 双保险）
        row = _raw_row(store)
        assert row.status == "published" and row.published_at is not None

    def test_legacy_degrade_status_predicate_still_blocks(self, redis):
        """updated_at 列缺失（legacy 降级档）：status 谓词独立挡住打回。"""
        redis_ctx["r"] = redis
        store = _seed(runtime="python")
        store._updated_at_ok = False
        from sqlalchemy import select

        from models.flow import FlowVersion

        with store._session_local() as s1:
            existing = s1.scalars(select(FlowVersion).where(
                FlowVersion.tenant_id == TENANT, FlowVersion.flow_id == "racy",
                FlowVersion.version == "1.0.0")).first()
            assert existing.status == "draft"
            store.publish_version("racy", "1.0.0", tenant_id=TENANT)
            ok = store._cas_update_draft(
                s1, flow_id="racy", version="1.0.0", tenant_id=TENANT,
                definition=_map_def(), layout="", status="draft",
                base_updated_at=None, force=False, runtime=None,
            )
            s1.commit()
        assert ok is False
        row = _raw_row(store)
        assert row.status == "published"


class TestPublishApi:
    """API 层：竞窗 409/422 结构化 + 草稿保持。"""

    def test_race_landing_between_gates_returns_422_draft_kept(self, redis, monkeypatch):
        """A2R54-01 API 形态：pre-gate 放行 D1 → 竞窗落未过门 D2 → 事务内 re-gate 422。

        竞窗位置=pre-gate 放行之后、publish_version 重 SELECT 之前——
        publish 内部 fresh 行已是未过门定义，gate_fn 对 fresh 行过门必拒。
        """
        store = _seed()
        snap = store.get_version("racy", "1.0.0", tenant_id=TENANT)
        real_gate = flows_api.gate_flow_nodes
        calls = {"n": 0}

        def gate_with_race(r, runtime, definition, **kw):
            out = real_gate(r, runtime, definition, **kw)
            calls["n"] += 1
            if calls["n"] == 1:  # pre-gate（快照 plain 定义）放行后：竞窗 CAS 落未过门定义
                store.save_flow_definition(
                    flow_id="racy", version="1.0.0", definition=_map_def(),
                    tenant_id=TENANT, base_updated_at=snap.updated_at.isoformat(),
                    created_by="racer",
                )
            return out

        monkeypatch.setattr(flows_api, "gate_flow_nodes", gate_with_race)
        resp = _client(redis).post(
            "/api/flows/racy/publish", json={"version": "1.0.0"},
            headers=_headers(),
        )
        assert resp.status_code == 422, resp.text
        assert resp.json()["detail"]["error"] == "unsupported_node_types"
        row = _raw_row(store)
        assert row.status == "draft" and row.published_at is None

    def test_race_during_in_publish_gate_returns_409(self, redis, monkeypatch):
        """钉扎 API 形态：re-gate 期间并发保存 → 条件翻转失配 → 409 version_stale。"""
        store = _seed()
        snap = store.get_version("racy", "1.0.0", tenant_id=TENANT)
        real_gate = flows_api.gate_flow_nodes
        calls = {"n": 0}

        def gate_with_race(r, runtime, definition, **kw):
            calls["n"] += 1
            if calls["n"] == 2:  # 事务内 re-gate 期间并发保存落进
                store.save_flow_definition(
                    flow_id="racy", version="1.0.0", definition=_map_def(),
                    tenant_id=TENANT, base_updated_at=snap.updated_at.isoformat(),
                    created_by="racer2",
                )
            return real_gate(r, runtime, definition, **kw)

        monkeypatch.setattr(flows_api, "gate_flow_nodes", gate_with_race)
        resp = _client(redis).post(
            "/api/flows/racy/publish", json={"version": "1.0.0"},
            headers=_headers(),
        )
        assert resp.status_code == 409, resp.text
        assert resp.json()["detail"]["conflict"] == "version_stale"
        assert resp.json()["detail"]["latest_updated_at"]
        row = _raw_row(store)
        assert row.status == "draft" and row.published_at is None

    def test_happy_publish_unaffected(self, redis):
        """守恒：无竞窗时 API 发布照常 200 + published（修复不误伤）。"""
        _seed()
        resp = _client(redis).post(
            "/api/flows/racy/publish", json={"version": "1.0.0"},
            headers=_headers(),
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["status"] == "published"
