"""#300 触发面池路由收口（第七十五轮）——console 调度 API 面。

r71/r73 台账留痕的两个 #300 已知缺口之一（触发面）：

- **调度行冗余 runtime**：cron 派发进程读不到 console store，create /
  update（version 变化）/ enable 按 start 同源的版本行解析把 runtime 盖进
  调度行；读面（``_view``）如实投影。
- **manual 触发与手动 POST /executions 同语义**：先过受理闸（能力门，
  不支持即 422 结构化拒绝、零入队零占位行），再按版本行 runtime 选池队列
  （此前恒落默认 python 队列——go-runtime flow 的调度触发串池）。

负控先红（基线 1dd62a3）：盖章用例红在「行/视图无 runtime 键」；触发用例
红在「422 变 200 + 落默认队列」。
"""
from __future__ import annotations

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
from api import schedules as schedules_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import flow_store  # noqa: E402
from plaita.server.services.schedule_service import (  # noqa: E402
    SCHEDULES_KEY,
)

ADMIN_KEY = "cluster-admin-key"
DEFAULT_QUEUE = "plaita:flow:queue"
GO_QUEUE = "plaita:flow:queue:go"

# 与 test_issue300_runtime_capability_gate.py 同源的真值子集（B3 r77 对齐）
GO_SUPPORTED = ["assignment", "code", "end", "http", "parallel", "start", "switch"]
GO_UNSUPPORTED = ["calculate", "case", "filter", "find", "map", "mock", "reference",
                  "reduce", "while"]


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
    monkeypatch.setenv("PLAITA_CONSOLE_GO_TASK_QUEUE", GO_QUEUE)
    flow_store.init_engine(f"sqlite:///{tmp_path}/issue300-schedule-pool.db")
    return FakeRedis(decode_responses=True)


def _client(redis) -> TestClient:
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.state.store = flow_store.get_flow_store()
    auth = [Depends(require_auth)]
    app.include_router(flows_api.router, prefix="/api", dependencies=auth)
    app.include_router(schedules_api.router, prefix="/api", dependencies=auth)
    return TestClient(app)


def _headers():
    return {"X-Admin-API-Key": ADMIN_KEY}


def _def(node_types, flow_id="f1"):
    """start → *middles → end 的最小定义；门只消费 type 集合。``map`` 按
    定义闸要求补最小 child_flow（内嵌 start/end；code 已被默认 registry
    除名，不可作发布面样本）。"""
    middles = []
    for i, t in enumerate(node_types):
        if t in ("start", "end"):
            continue
        node = {"type": t, "id": f"n{i}", "next": "end"}
        if t == "map":
            node["over"] = "$INPUT.items"
            node["child_flow"] = {
                "flow_id": "child",
                "nodes": [
                    {"type": "start", "id": "s", "next": "e"},
                    {"type": "end", "id": "e", "output": "$INPUT",
                     "resultType": "success"},
                ],
            }
        middles.append(node)
    return json.dumps({
        "flow_id": flow_id,
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start",
             "next": middles[0]["id"] if middles else "end"},
            *middles,
            {"type": "end", "id": "end", "output": "$INPUT",
             "resultType": "success"},
        ],
    })


def _publish_go_flow(client, flow_id, node_types=("start", "end"), version="1.0.0"):
    """发布 go-runtime flow。``with_manifest=False`` 时先发布后注册清单
    （fail-open 路径：清单缺席时能力门放行 + WARNING）。"""
    store = flow_store.get_flow_store()
    store.ensure_flow(flow_id, tenant_id="default")
    resp = client.put(
        f"/api/flows/{flow_id}/versions/{version}",
        json={"definition": _def(list(node_types), flow_id), "created_by": "t",
              "runtime": "go"},
        headers=_headers(),
    )
    assert resp.status_code == 200, resp.text
    resp = client.post(f"/api/flows/{flow_id}/publish",
                       json={"version": version}, headers=_headers())
    assert resp.status_code == 200, resp.text


def _publish_python_flow(client, flow_id, version="1.0.0"):
    store = flow_store.get_flow_store()
    store.ensure_flow(flow_id, tenant_id="default")
    resp = client.put(
        f"/api/flows/{flow_id}/versions/{version}",
        json={"definition": _def(["start", "end"], flow_id), "created_by": "t"},
        headers=_headers(),
    )
    assert resp.status_code == 200, resp.text
    resp = client.post(f"/api/flows/{flow_id}/publish",
                       json={"version": version}, headers=_headers())
    assert resp.status_code == 200, resp.text


def _register_go_worker(redis, *, supported=None, with_manifest=True):
    from plaita.server.capability import build_capability_manifest

    metadata = {"queue_name": GO_QUEUE}
    if with_manifest:
        metadata["capability_manifest"] = build_capability_manifest(
            supported if supported is not None else GO_SUPPORTED,
            engine="plaita-go",
            unsupported_node_types=GO_UNSUPPORTED,
        )
    redis.setex(
        "plaita:registry:flow_worker:go-1", 30,
        json.dumps({
            "instance_id": "go-1",
            "service_type": "flow_worker",
            "host": "localhost",
            "status": "running",
            "metadata": metadata,
            "active_tasks": 0,
        }),
    )


def _create_schedule(client, redis, **overrides):
    body = {
        "name": "n",
        "flow_id": "f1",
        "version": "1.0.0",
        "cron": "* * * * *",
        "params": {},
        "enabled": True,
        "created_by": "t",
    }
    body.update(overrides)
    resp = client.post("/api/schedules", json=body, headers=_headers())
    assert resp.status_code == 200, resp.text
    view = resp.json()
    row = json.loads(redis.hget(SCHEDULES_KEY, view["schedule_id"]))
    return view, row


def _start_messages(redis, queue):
    out = []
    for entry in redis.xrange(queue):
        payload = json.loads(entry[1]["payload"])
        if payload.get("type") == "start":
            out.append(payload)
    return out


class TestScheduleRowRuntimeStamp:
    def test_create_stamps_runtime_from_version_row(self, redis):
        """先红：基线调度行/视图无 runtime——go flow 的调度触发因此恒落默认池。"""
        client = _client(redis)
        _publish_go_flow(client, "f1")
        view, row = _create_schedule(client, redis)
        assert view["runtime"] == "go"
        assert row["runtime"] == "go"

    def test_create_version_none_uses_latest_published(self, redis):
        client = _client(redis)
        _publish_python_flow(client, "f1", version="1.0.0")
        _publish_go_flow(client, "f1", version="2.0.0")
        view, row = _create_schedule(client, redis, version=None)
        assert view["runtime"] == "go"
        assert row["runtime"] == "go"

    def test_update_version_restamps(self, redis):
        client = _client(redis)
        _publish_go_flow(client, "f1", version="2.0.0")
        _publish_python_flow(client, "f1", version="1.0.0")
        view, row = _create_schedule(client, redis, version="2.0.0")
        assert row["runtime"] == "go"
        resp = client.put(
            f"/api/schedules/{view['schedule_id']}",
            json={"version": "1.0.0"}, headers=_headers(),
        )
        assert resp.status_code == 200, resp.text
        row = json.loads(redis.hget(SCHEDULES_KEY, view["schedule_id"]))
        assert row["version"] == "1.0.0"
        assert row["runtime"] == "python"

    def test_enable_restamps_stale_row(self, redis):
        """启用即重盖章：老行（无 runtime 键）按版本行现算补齐。"""
        client = _client(redis)
        _publish_go_flow(client, "f1")
        view, _row = _create_schedule(client, redis)
        redis.hdel(SCHEDULES_KEY, view["schedule_id"])
        stale = {
            "schedule_id": view["schedule_id"], "tenant_id": "default",
            "name": "n", "flow_id": "f1", "version": "1.0.0",
            "cron": "* * * * *", "params": {}, "enabled": False,
            "created_at": "t0", "updated_at": "t0", "next_run_at": "",
        }
        redis.hset(SCHEDULES_KEY, view["schedule_id"], json.dumps(stale))
        resp = client.post(
            f"/api/schedules/{view['schedule_id']}/enable", headers=_headers()
        )
        assert resp.status_code == 200, resp.text
        row = json.loads(redis.hget(SCHEDULES_KEY, view["schedule_id"]))
        assert row["runtime"] == "go"

    def test_unresolvable_version_defaults_python(self, redis):
        """version=None 且尚无已发布版本：合法态，落默认池（等于历史行为）。"""
        client = _client(redis)
        store = flow_store.get_flow_store()
        store.create_flow("f1", tenant_id="default")
        view, row = _create_schedule(client, redis, version=None)
        assert row["runtime"] == "python"


class TestManualTriggerGateAndRouting:
    def test_unsupported_flow_rejected_before_enqueue(self, redis):
        """先红：基线无门——422 变 200 且消息落默认队列。发布在清单注册前
        （fail-open），触发时清单已判明不支持——与 start 同语义在受理前拒。"""
        client = _client(redis)
        _publish_go_flow(client, "f1", node_types=("start", "map", "end"))
        _register_go_worker(redis, supported=GO_SUPPORTED)  # map ∉ go 清单
        view, _row = _create_schedule(client, redis)
        resp = client.post(
            f"/api/schedules/{view['schedule_id']}/trigger", headers=_headers()
        )
        assert resp.status_code == 422, resp.text
        detail = resp.json()["detail"]
        assert detail["error"] == "unsupported_node_types"
        assert detail["runtime"] == "go"
        assert detail["unsupported"] == ["map"]
        assert redis.xlen(GO_QUEUE) == 0
        assert redis.xlen(DEFAULT_QUEUE) == 0

    def test_go_flow_trigger_lands_in_go_queue(self, redis):
        """先红：基线恒传 _task_queue_name()——go flow 触发落默认池（串池）。"""
        client = _client(redis)
        _publish_go_flow(client, "f1")
        _register_go_worker(redis)
        view, _row = _create_schedule(client, redis)
        resp = client.post(
            f"/api/schedules/{view['schedule_id']}/trigger", headers=_headers()
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["success"] is True
        assert len(_start_messages(redis, GO_QUEUE)) == 1
        assert redis.xlen(DEFAULT_QUEUE) == 0

    def test_python_flow_trigger_stays_in_default_queue(self, redis):
        # 正控：python flow 触发照旧落默认池
        client = _client(redis)
        _publish_python_flow(client, "f1")
        _register_go_worker(redis)
        view, _row = _create_schedule(client, redis)
        resp = client.post(
            f"/api/schedules/{view['schedule_id']}/trigger", headers=_headers()
        )
        assert resp.status_code == 200, resp.text
        assert len(_start_messages(redis, DEFAULT_QUEUE)) == 1
        assert redis.xlen(GO_QUEUE) == 0

    def test_trigger_without_manifests_fails_open(self, redis):
        # 正控：池内无清单（旧 build 部署）→ 门放行，行为与上线前一致
        client = _client(redis)
        _publish_go_flow(client, "f1", node_types=("start", "map", "end"))
        _register_go_worker(redis, with_manifest=False)
        view, _row = _create_schedule(client, redis)
        resp = client.post(
            f"/api/schedules/{view['schedule_id']}/trigger", headers=_headers()
        )
        assert resp.status_code == 200, resp.text
        assert len(_start_messages(redis, GO_QUEUE)) == 1
