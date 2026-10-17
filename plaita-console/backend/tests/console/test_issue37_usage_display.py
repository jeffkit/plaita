"""plaita#37 P2 —— console 展示半边：usage_json 零迁移 + run/节点两级汇总。

红先证（本轮基线树实跑，见 /tmp/w20-a-out/RED-PROOF.md）：

    $NODE.a.usage 真源: {"prompt_tokens": 7, "completion_tokens": 5, "total_tokens": 12}
    detail 行键        : [... 无 usage / node_usage ...]
    detail.usage      : null
    detail.node_usage : null
    RED: ExecutionInfo 无 usage/node_usage 字段；local_executions 无 usage_json 列

即：引擎侧已经采到了 usage（#37 P1），**console 一个字段都没有**——未开
Langfuse 的部署里 token 消耗在执行页仍然完全不可见。

本文件守住 P2 竖切：

- ``local_executions.usage_json``（默认 ``'null'``）**零迁移**：老库 ADD COLUMN
  即得 ``'null'`` ⇒ 读出 ``None``（未采集），**绝不折算成 0**；
- 列表/详情两级汇总：run 级 ``usage`` + 节点级 ``node_usage``（只读投影，
  真源仍是 context 的 ``$NODE``）；
- ``source=estimated``/``mixed`` 必须显式标注（``estimated=true``），
  **不得与观测值混同**；
- 集群档：详情读 worker 落盘的 ``ExecutionState.usage`` 与 context，列表走
  Redis 投影（不读 context）。
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import ClassVar

import pytest
from fastapi import Depends, FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import executions as executions_api  # noqa: E402
from auth import require_auth  # noqa: E402
from models.flow import LocalExecution  # noqa: E402
from services import flow_store  # noqa: E402
from services import local_executor  # noqa: E402

ADMIN_KEY = "w37p2-admin-key"
BOOM_TYPE = "w37p2_test_boom"


# ---------- 引擎侧展示口径（纯函数） ----------


def _view(usage):
    from plaita.storage.usage import usage_view

    return usage_view(usage)


class TestUsageViewShape:
    def test_uncollected_is_none_not_zero(self):
        assert _view(None) is None
        assert _view({}) is None
        assert _view("nonsense") is None

    def test_collected_zero_is_a_zero_object(self):
        view = _view({"total_tokens": 0, "prompt_tokens": 0,
                      "completion_tokens": 0, "llm_calls": 1})
        assert view is not None
        assert view["total_tokens"] == 0
        assert view["total_tokens_display"] == "0"     # 「0」而不是「未采集」

    def test_uncollected_display_string_is_explicit(self):
        view = _view({"models": ["m1"]})                # 没有计数值
        assert view["total_tokens_display"] == "未采集"
        assert view["count"] is None
        assert view["count_display"] == "未采集"

    def test_estimated_source_is_flagged_not_conflated(self):
        observed = _view({"total_tokens": 10, "source": "observed"})
        estimated = _view({"total_tokens": 10, "source": "estimated"})
        mixed = _view({"total_tokens": 10, "source": "mixed"})
        assert observed["estimated"] is False
        assert estimated["estimated"] is True
        assert estimated["source"] == "estimated"
        assert mixed["estimated"] is True
        assert mixed["source"] == "mixed"

    def test_unknown_source_defaults_to_observed(self):
        view = _view({"total_tokens": 10, "source": "guess"})
        assert view["source"] == "observed"

    def test_patch_count_fields_are_uncollected_by_default(self):
        view = _view({"total_tokens": 40, "count": 10, "tokens_per_count": 4.0})
        assert view["count"] == 10
        assert view["tokens_per_count"] == 4.0
        assert view["count_display"] == "10"
        assert view["tokens_per_count_display"] == "4"

    def test_node_breakdown_skips_unreported_nodes(self):
        from plaita.storage.usage import node_usage_breakdown

        breakdown = node_usage_breakdown({
            "a": {"model": "m1", "usage": {"total_tokens": 10}},
            "b": {"usage": None},
            "c": {"value": 1},
            "zero": {"usage": {"total_tokens": 0}},
        })
        assert set(breakdown) == {"a", "zero"}
        assert breakdown["zero"]["total_tokens"] == 0     # 采集到 0 ≠ 未采集
        assert breakdown["a"]["source"] == "observed"
        assert node_usage_breakdown({}) == {}
        assert node_usage_breakdown(None) == {}


# ---------- 零迁移：新列 + 老行 ----------


class TestUsageColumnZeroMigration:
    def test_sqlite_migration_adds_usage_json_with_null_default(self, tmp_path, monkeypatch):
        from sqlalchemy import create_engine, text

        db_file = tmp_path / "legacy.db"
        engine = create_engine(f"sqlite:///{db_file}", future=True)
        with engine.begin() as conn:
            # 模拟老库：local_executions 存在但缺 usage_json（也没有 context_json）
            conn.execute(text("CREATE TABLE local_executions (id INTEGER PRIMARY KEY)"))
            conn.execute(text("INSERT INTO local_executions (id) VALUES (1)"))
        monkeypatch.setattr(flow_store, "_engine", engine)

        flow_store._migrate_sqlite_columns()
        flow_store._migrate_sqlite_columns()      # 幂等

        with engine.begin() as conn:
            cols = {r[1] for r in conn.execute(
                text("PRAGMA table_info(local_executions)")).fetchall()}
            raw = conn.execute(text("SELECT usage_json FROM local_executions")).fetchone()[0]
        assert "usage_json" in cols, "零迁移前提：老库必须补出 usage_json 列"
        assert raw == "null", f"老行必须是未采集（'null'），得到 {raw!r}"

    def test_orm_column_defaults_to_null_string(self):
        assert "usage_json" in LocalExecution.__table__.c
        assert LocalExecution.__table__.c.usage_json.default.arg == "null"

    def test_local_row_without_usage_reads_uncollected(self, tmp_path):
        flow_store.init_engine(f"sqlite:///{tmp_path / 'console.db'}")
        flow_store.insert_local_execution(
            execution_id="old-row", flow_id="f", flow_version="1", tenant_id="default"
        )
        row = local_executor.get_local_execution("old-row", tenant_id="default")
        assert row["usage"] is None, "未采集 ⇒ None（不是 0）"


# ---------- 本地档：列表/详情两级汇总 ----------


def _flow_def(version: str = "1.0.0", nodes=None) -> str:
    return json.dumps({
        "flow_id": "usage-flow", "name": "usage-flow", "version": version,
        "nodes": nodes or [
            {"id": "start", "type": "start", "next": "a"},
            {"id": "a", "type": "assignment", "next": "end",
             "output": {"model": "m1", "usage": {"prompt_tokens": 7,
                                                 "completion_tokens": 5,
                                                 "total_tokens": 12}}},
            {"id": "end", "type": "end", "output": "ok"},
        ],
    })


@pytest.fixture()
def local_client(tmp_path):
    flow_store.init_engine(f"sqlite:///{tmp_path / 'console.db'}")
    app = FastAPI()
    app.state.redis = None
    app.state.local_mode = True
    app.include_router(executions_api.router, prefix="/api")
    return TestClient(app)


def _publish(definition: str, flow_id: str = "usage-flow") -> None:
    store = flow_store.get_flow_store()
    store.ensure_flow(flow_id, tenant_id="default")
    store.save_flow_definition(flow_id, "1.0.0", definition, status="published",
                              tenant_id="default")


def _wait_terminal(client, execution_id: str, timeout: float = 20.0) -> dict:
    deadline = time.time() + timeout
    body = {}
    while time.time() < deadline:
        body = client.get(f"/api/executions/{execution_id}").json()
        if body.get("status") in ("completed", "failed", "cancelled"):
            return body
        time.sleep(0.05)
    raise AssertionError(f"执行未在 {timeout}s 内终结: {body}")


class TestLocalApiUsage:
    def test_detail_and_list_expose_run_and_node_level(self, local_client):
        _publish(_flow_def())
        eid = local_client.post("/api/executions", json={"flow_id": "usage-flow"}).json()[
            "execution_id"
        ]
        detail = _wait_terminal(local_client, eid)

        usage = detail["usage"]
        assert usage is not None, "详情必须给出 run 级 usage（修复前零字段）"
        assert usage["total_tokens"] == 12
        assert usage["llm_calls"] == 1
        assert usage["models"] == ["m1"]
        assert usage["source"] == "observed" and usage["estimated"] is False
        assert usage["total_tokens_display"] == "12"

        node_usage = detail["node_usage"]
        assert node_usage is not None, "详情必须给出节点级 usage"
        assert set(node_usage) == {"a"}, "只列真报了 usage 的节点"
        assert node_usage["a"]["total_tokens"] == 12
        assert "start" not in node_usage, "没报 usage 的节点不得补 0"

        rows = {r["execution_id"]: r
                for r in local_client.get("/api/executions").json()["executions"]}
        assert rows[eid]["usage"]["total_tokens"] == 12, "列表行同样要能看 token 消耗"

    def test_negative_control_uncollected_run_shows_null_not_zero(self, local_client):
        """负控：没有任何节点报 usage 的 run ⇒ null（未采集），不得显示 0。"""
        _publish(_flow_def(nodes=[
            {"id": "start", "type": "start", "next": "a"},
            {"id": "a", "type": "assignment", "next": "end", "output": {"v": 1}},
            {"id": "end", "type": "end", "output": "ok"},
        ]))
        eid = local_client.post("/api/executions", json={"flow_id": "usage-flow"}).json()[
            "execution_id"
        ]
        detail = _wait_terminal(local_client, eid)
        assert detail["usage"] is None, "未采集必须保持 null，绝不折算成 0"
        assert detail["node_usage"] is None

    def test_collected_zero_is_distinguishable_from_uncollected(self, local_client):
        _publish(_flow_def(nodes=[
            {"id": "start", "type": "start", "next": "a"},
            {"id": "a", "type": "assignment", "next": "end",
             "output": {"model": "m1", "usage": {"total_tokens": 0}}},
            {"id": "end", "type": "end", "output": "ok"},
        ]))
        eid = local_client.post("/api/executions", json={"flow_id": "usage-flow"}).json()[
            "execution_id"
        ]
        detail = _wait_terminal(local_client, eid)
        assert detail["usage"] is not None and detail["usage"]["total_tokens"] == 0
        assert detail["usage"]["total_tokens_display"] == "0"
        assert detail["node_usage"]["a"]["total_tokens"] == 0

    def test_estimated_usage_is_flagged_in_api(self, local_client):
        _publish(_flow_def(nodes=[
            {"id": "start", "type": "start", "next": "a"},
            {"id": "a", "type": "assignment", "next": "end",
             "output": {"model": "m1", "usage": {"total_tokens": 12,
                                                 "source": "estimated"}}},
            {"id": "end", "type": "end", "output": "ok"},
        ]))
        eid = local_client.post("/api/executions", json={"flow_id": "usage-flow"}).json()[
            "execution_id"
        ]
        detail = _wait_terminal(local_client, eid)
        assert detail["usage"]["source"] == "estimated"
        assert detail["usage"]["estimated"] is True, (
            "估算值必须在展示层显式标注，不得与观测值混同"
        )

    def test_failed_local_run_still_records_usage(self, local_client):
        """失败终态同样聚合（能采到多少算多少）：usage 不因失败而消失。"""
        from plaita.node import Node, get_default_registry

        class BoomNode(Node):
            node_type: ClassVar[str] = BOOM_TYPE

            def execute(self, execution):  # noqa: ARG002
                raise RuntimeError("w37p2 boom")

        reg = get_default_registry()
        reg.register(BoomNode)
        try:
            _publish(_flow_def(nodes=[
                {"id": "start", "type": "start", "next": "a"},
                {"id": "a", "type": "assignment", "next": "boom",
                 "output": {"model": "m1",
                            "usage": {"total_tokens": 12}}},
                {"id": "boom", "type": BOOM_TYPE, "next": "end"},
                {"id": "end", "type": "end", "output": "ok"},
            ]))
            eid = local_client.post(
                "/api/executions", json={"flow_id": "usage-flow"}
            ).json()["execution_id"]
            detail = _wait_terminal(local_client, eid)
            assert detail["status"] == "failed"
            assert detail["usage"] is not None
            assert detail["usage"]["total_tokens"] == 12
        finally:
            reg.unregister(BOOM_TYPE)

    def test_uncollected_rows_in_list_are_null_not_zero(self, local_client):
        flow_store.insert_local_execution(
            execution_id="legacy-1", flow_id="f", flow_version="1", tenant_id="default"
        )
        rows = {r["execution_id"]: r
                for r in local_client.get("/api/executions").json()["executions"]}
        assert rows["legacy-1"]["usage"] is None
        assert rows["legacy-1"]["usage"] != {"total_tokens": 0}


# ---------- 集群档：状态投影 ----------


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


@pytest.fixture()
def cluster_client(monkeypatch):
    pytest.importorskip("fakeredis")
    import fakeredis

    redis = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.include_router(executions_api.router, prefix="/api",
                       dependencies=[Depends(require_auth)])
    return TestClient(app), redis


def _headers():
    return {"X-Admin-API-Key": ADMIN_KEY}


def _seed_state(redis, execution_id, *, usage="missing", context=None):
    data = {
        "execution_id": execution_id,
        "flow_id": "f1",
        "status": "completed",
        "start_time": "2026-09-01T00:00:00",
        "end_time": "2026-09-01T00:00:05",
        "context": context or {},
    }
    if usage != "missing":
        data["usage"] = usage
    redis.set(f"plaita:execution:{execution_id}", json.dumps(data))


_NODE_CONTEXT = {"$NODE": {
    "a": {"model": "m1", "usage": {"total_tokens": 10}},
    "zero": {"usage": {"total_tokens": 0, "source": "estimated"}},
    "b": {"value": 1},
}}


class TestClusterUsage:
    def test_detail_reads_state_usage_and_context_nodes(self, cluster_client):
        client, redis = cluster_client
        _seed_state(redis, "c1", usage={"total_tokens": 10, "llm_calls": 1,
                                        "models": ["m1"], "source": "observed"},
                    context=_NODE_CONTEXT)
        body = client.get("/api/executions/c1", headers=_headers()).json()
        assert body["usage"]["total_tokens"] == 10
        assert set(body["node_usage"]) == {"a", "zero"}
        assert body["node_usage"]["zero"]["total_tokens"] == 0
        assert body["node_usage"]["zero"]["estimated"] is True

    def test_old_state_without_usage_is_uncollected(self, cluster_client):
        client, redis = cluster_client
        _seed_state(redis, "c2", usage="missing", context={})
        body = client.get("/api/executions/c2", headers=_headers()).json()
        assert body["usage"] is None
        assert body["node_usage"] is None

    def test_list_projection_carries_usage(self, cluster_client):
        client, redis = cluster_client
        _seed_state(redis, "c3", usage={"total_tokens": 42, "llm_calls": 2,
                                        "models": ["m1"], "source": "observed"})
        _seed_state(redis, "c4", usage="missing")
        rows = {r["execution_id"]: r
                for r in client.get("/api/executions", headers=_headers()).json()["executions"]}
        assert rows["c3"]["usage"]["total_tokens"] == 42
        assert rows["c3"]["usage"]["total_tokens_display"] == "42"
        assert rows["c4"]["usage"] is None, "老状态 ⇒ 未采集（绝不当成 0）"
        # 列表路径刻意不读 context ⇒ 节点级不可用（详情页才有）
        assert rows["c3"]["node_usage"] is None

    def test_projection_parses_bad_usage_string_as_uncollected(self):
        assert executions_api._parse_projected_usage("null") is None
        assert executions_api._parse_projected_usage("") is None
        assert executions_api._parse_projected_usage("{bad json") is None
        assert executions_api._parse_projected_usage('[1,2]') is None
        assert executions_api._parse_projected_usage('{"total_tokens": 0}') == {
            "total_tokens": 0
        }
