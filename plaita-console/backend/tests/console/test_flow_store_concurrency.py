"""
C5-1 乐观并发控制单测：草稿保存 base_updated_at 比对 / 强制覆盖 / allocate_version 原子分配。

对应修复：services/flow_store.py（updated_at 列 + VersionConflictError + allocate_version）
与 api/flows.py（SaveVersionRequest 透传 + 结构化 409 + VersionView.updated_at）。
"""
import json
import sys
from pathlib import Path

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from sqlalchemy import create_engine  # noqa: E402
from sqlalchemy.orm import sessionmaker  # noqa: E402

from models.flow import Base  # noqa: E402
from services import flow_store  # noqa: E402
from services.flow_store import FlowStore, VersionConflictError  # noqa: E402


def _echo_definition(flow_id: str = "echo", tag: str = "v1") -> str:
    return json.dumps(
        {
            "flow_id": flow_id,
            "desc": tag,
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "end"},
                {"type": "end", "id": "end", "output": "$INPUT.name", "resultType": "success"},
            ],
        }
    )


@pytest.fixture()
def store(tmp_path) -> FlowStore:
    db_file = tmp_path / "test_conc.db"
    engine = create_engine(f"sqlite:///{db_file}", future=True)
    Base.metadata.create_all(engine)
    session_local = sessionmaker(bind=engine, expire_on_commit=False)
    return FlowStore(session_local)


# ---- 服务层：乐观锁 ----

def test_updated_at_exposed_and_bumps_on_save(store: FlowStore):
    res = store.save_flow_definition("echo", "0.0.1", _echo_definition(tag="a"))
    first = store.get_version("echo", "0.0.1")
    assert first is not None and first.updated_at is not None
    base = first.updated_at.isoformat()

    res2 = store.save_flow_definition("echo", "0.0.1", _echo_definition(tag="b"))
    second = store.get_version("echo", "0.0.1")
    assert second is not None and second.updated_at is not None
    assert second.updated_at.isoformat() != base  # 保存后基准必须前进
    assert json.loads(second.definition)["desc"] == "b"


def test_stale_base_conflicts(store: FlowStore):
    # 「标签页 A」写入后，「标签页 B」仍持旧基准保存 → 冲突
    store.save_flow_definition("echo", "0.0.1", _echo_definition(tag="a"))
    stale_base = store.get_version("echo", "0.0.1").updated_at.isoformat()
    store.save_flow_definition("echo", "0.0.1", _echo_definition(tag="b"))  # A 的更新

    with pytest.raises(VersionConflictError) as ei:
        store.save_flow_definition(
            "echo", "0.0.1", _echo_definition(tag="c"), base_updated_at=stale_base
        )
    latest = store.get_version("echo", "0.0.1").updated_at.isoformat()
    assert ei.value.latest_updated_at == latest  # 错误携带最新 updated_at
    # 内容未被 B 覆盖
    assert json.loads(store.get_version("echo", "0.0.1").definition)["desc"] == "b"


def test_matching_base_saves(store: FlowStore):
    store.save_flow_definition("echo", "0.0.1", _echo_definition(tag="a"))
    base = store.get_version("echo", "0.0.1").updated_at.isoformat()
    store.save_flow_definition(
        "echo", "0.0.1", _echo_definition(tag="b"), base_updated_at=base
    )
    assert json.loads(store.get_version("echo", "0.0.1").definition)["desc"] == "b"


def test_force_overrides_conflict(store: FlowStore):
    store.save_flow_definition("echo", "0.0.1", _echo_definition(tag="a"))
    stale_base = store.get_version("echo", "0.0.1").updated_at.isoformat()
    store.save_flow_definition("echo", "0.0.1", _echo_definition(tag="b"))

    store.save_flow_definition(
        "echo", "0.0.1", _echo_definition(tag="c"),
        base_updated_at=stale_base, force=True,
    )
    assert json.loads(store.get_version("echo", "0.0.1").definition)["desc"] == "c"


def test_no_base_keeps_legacy_behavior(store: FlowStore):
    # 未带 base_updated_at 的调用方（存量 API 消费者）行为不变
    store.save_flow_definition("echo", "0.0.1", _echo_definition(tag="a"))
    store.save_flow_definition("echo", "0.0.1", _echo_definition(tag="b"))
    assert json.loads(store.get_version("echo", "0.0.1").definition)["desc"] == "b"


def test_published_still_blocked_before_conflict_check(store: FlowStore):
    store.save_flow_definition("echo", "1.0.0", _echo_definition())
    store.publish_version("echo", "1.0.0")
    with pytest.raises(ValueError, match="已发布"):
        store.save_flow_definition(
            "echo", "1.0.0", _echo_definition(tag="x"), base_updated_at="2000-01-01T00:00:00"
        )


def test_version_conflict_error_is_value_error(store: FlowStore):
    """VersionConflictError 兼容既有 ValueError → 409 映射。"""
    store.save_flow_definition("echo", "0.0.1", _echo_definition(tag="a"))
    store.save_flow_definition("echo", "0.0.1", _echo_definition(tag="b"))
    stale = "2000-01-01T00:00:00"
    with pytest.raises(ValueError):
        store.save_flow_definition("echo", "0.0.1", _echo_definition(), base_updated_at=stale)


# ---- 服务层：allocate_version（另存新版本不撞号）----

def test_allocate_version_assigns_next_patch(store: FlowStore):
    store.save_flow_definition("echo", "0.0.1", _echo_definition())
    store.save_flow_definition("echo", "0.0.2", _echo_definition())
    res = store.save_flow_definition(
        "echo", "0.0.2", _echo_definition(tag="fresh"), allocate_version=True
    )
    assert res.version == "0.0.3"  # 忽略调用方的 0.0.2，按库内最大分配
    assert json.loads(store.get_version("echo", "0.0.3").definition)["desc"] == "fresh"


def test_allocate_version_avoids_stale_collision(store: FlowStore):
    """双「标签页」：A 已创建 0.0.2，B 本地列表陈旧仍算 0.0.2 → 分配 0.0.3，不覆盖。"""
    store.save_flow_definition("echo", "0.0.1", _echo_definition())
    # A 另存 0.0.2（B 不知道）
    a = store.save_flow_definition(
        "echo", "0.0.2", _echo_definition(tag="from-A"), allocate_version=True
    )
    assert a.version == "0.0.2"
    # B 用陈旧列表也「另存 0.0.2」
    b = store.save_flow_definition(
        "echo", "0.0.2", _echo_definition(tag="from-B"), allocate_version=True
    )
    assert b.version == "0.0.3"
    # A 的内容不被 B 覆盖
    assert json.loads(store.get_version("echo", "0.0.2").definition)["desc"] == "from-A"
    assert json.loads(store.get_version("echo", "0.0.3").definition)["desc"] == "from-B"


def test_allocate_version_on_empty_flow(store: FlowStore):
    res = store.save_flow_definition("echo", "0.0.1", _echo_definition(), allocate_version=True)
    assert res.version == "0.0.1"


def test_next_semver_ignores_non_semver(store: FlowStore):
    assert FlowStore._next_semver_of([]) == "0.0.1"
    assert FlowStore._next_semver_of(["0.0.9", "0.1.0", "junk", ""]) == "0.1.1"
    assert FlowStore._next_semver_of(["1.2.3"]) == "1.2.4"


# ---- API 层：透传 + 结构化 409 ----

@pytest.fixture()
def api_client(tmp_path) -> TestClient:
    import api.flows as flows_api

    flow_store.init_engine(f"sqlite:///{tmp_path / 'api_conc.db'}")
    app = FastAPI()
    app.state.redis = None      # 本地单机模式：发布/删除跳过引擎同步
    app.state.local_mode = True
    app.state.store = flow_store.get_flow_store()
    app.include_router(flows_api.router, prefix="/api")
    return TestClient(app)


def _setup_ready_flow(client: TestClient):
    assert client.post("/api/flows", json={"flow_id": "echo"}).status_code == 200
    r = client.put(
        "/api/flows/echo/versions/0.0.1",
        json={"definition": _echo_definition(tag="a"), "layout": "{}"},
    )
    assert r.status_code == 200, r.text


def test_api_save_without_base_sets_updated_at(api_client: TestClient):
    _setup_ready_flow(api_client)
    r = api_client.put(
        "/api/flows/echo/versions/0.0.1",
        json={"definition": _echo_definition(tag="b"), "layout": "{}"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["updated_at"]  # VersionView 暴露 updated_at

    r2 = api_client.get("/api/flows/echo/versions/0.0.1")
    assert r2.json()["updated_at"] == body["updated_at"]


def test_api_stale_base_returns_structured_409(api_client: TestClient):
    _setup_ready_flow(api_client)
    stale = api_client.get("/api/flows/echo/versions/0.0.1").json()["updated_at"]
    # 他人更新
    api_client.put(
        "/api/flows/echo/versions/0.0.1",
        json={"definition": _echo_definition(tag="other"), "layout": "{}"},
    )
    r = api_client.put(
        "/api/flows/echo/versions/0.0.1",
        json={
            "definition": _echo_definition(tag="stale"),
            "layout": "{}",
            "base_updated_at": stale,
        },
    )
    assert r.status_code == 409, r.text
    detail = r.json()["detail"]
    assert detail["conflict"] == "version_stale"
    assert detail["latest_updated_at"]
    assert detail["message"]


def test_api_force_overrides(api_client: TestClient):
    _setup_ready_flow(api_client)
    stale = api_client.get("/api/flows/echo/versions/0.0.1").json()["updated_at"]
    api_client.put(
        "/api/flows/echo/versions/0.0.1",
        json={"definition": _echo_definition(tag="other"), "layout": "{}"},
    )
    r = api_client.put(
        "/api/flows/echo/versions/0.0.1",
        json={
            "definition": _echo_definition(tag="forced"),
            "layout": "{}",
            "base_updated_at": stale,
            "force": True,
        },
    )
    assert r.status_code == 200, r.text
    assert json.loads(r.json()["definition"])["desc"] == "forced"


def test_api_allocate_version_returns_actual(api_client: TestClient):
    _setup_ready_flow(api_client)
    r = api_client.put(
        "/api/flows/echo/versions/0.0.1",
        json={"definition": _echo_definition(tag="new"), "layout": "{}", "allocate_version": True},
    )
    assert r.status_code == 200, r.text
    assert r.json()["version"] == "0.0.2"
