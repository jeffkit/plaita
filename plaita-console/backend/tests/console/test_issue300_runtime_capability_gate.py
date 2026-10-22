"""#300 runtime capability manifest + publish/start 能力门 + runtime pool 路由。

负控矩阵（先红后绿）：

1. **全支持 flow 正常发布启动**：start/end flow 到 go 池 → publish 200、
   start 200，消息进 go 队列（不串池）；python 池走默认队列。
2. **含不支持节点的 flow 发布被结构化拒**：map flow 到 go 池 → publish 422
   ``{error: unsupported_node_types, runtime, unsupported, supported}``，
   状态仍 draft、引擎存储不落键；已发布旧 flow start 同门 → 422 且**不写
   queued 占位行、不占配额、不入队**。
3. **manifest 缺失 worker 的兜底路径**：池内 worker 注册条目无清单（旧
   build）→ publish/start 放行（fail-open）+ WARNING 可观测（降级必须留痕）。

配套面：flow_versions.runtime 列（go/python，默认 python）、按 runtime 选
队列、services 读面透出 capability_manifest。
"""
import json
import logging
import sys
from pathlib import Path

import pytest
from fastapi import Depends, FastAPI
from fakeredis import FakeRedis
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import executions as executions_api  # noqa: E402
from api import flows as flows_api  # noqa: E402
from api import services as services_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import flow_store  # noqa: E402
from services.capability_gate import POOL_MANIFEST_MISSING_WARNING  # noqa: E402

ADMIN_KEY = "cluster-admin-key"

DEFAULT_QUEUE = "plaita:flow:queue"
GO_QUEUE = "plaita:flow:queue:go"

# 测试夹具：plaita-go IR accept-list 的代表性子集（真值由 B3 r77 的
# ir.SupportedNodeTypes() 导出对齐；能力门只消费清单本身，不硬编码它）。
GO_SUPPORTED = ["assignment", "code", "end", "http", "parallel", "start", "switch"]
# Go 侧明确知道且拒绝的 Python 家族（#300 示例形态）。
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
    flow_store.init_engine(f"sqlite:///{tmp_path}/issue300.db")
    r = FakeRedis(decode_responses=True)
    _register_worker(r, "go-1", engine="plaita-go", supported=GO_SUPPORTED)
    _register_worker(r, "py-1", engine="python")
    return r


def _client(redis) -> TestClient:
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.state.store = flow_store.get_flow_store()
    app.include_router(
        flows_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    app.include_router(
        executions_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    app.include_router(
        services_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    return TestClient(app)


def _headers():
    return {"X-Admin-API-Key": ADMIN_KEY}


def _register_worker(redis, instance_id, *, engine, supported=None,
                     with_manifest=True):
    from plaita.server.capability import build_capability_manifest, python_worker_manifest

    metadata = {"queue_name": GO_QUEUE if engine == "plaita-go" else DEFAULT_QUEUE}
    if with_manifest:
        manifest = (
            build_capability_manifest(
                supported or [], engine=engine,
                unsupported_node_types=GO_UNSUPPORTED if engine == "plaita-go" else (),
            )
            if engine != "python"
            else python_worker_manifest()
        )
        metadata["capability_manifest"] = manifest
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


def _map_def(flow_id="mappy", version="1.0.0"):
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


def _plain_def(flow_id="plain", version="1.0.0"):
    return json.dumps({
        "flow_id": flow_id,
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "end"},
            {"type": "end", "id": "end", "output": "$INPUT.name", "resultType": "success"},
        ],
    })


def _save(client, flow_id, version, definition, runtime=None):
    body = {"definition": definition, "created_by": "t"}
    if runtime is not None:
        body["runtime"] = runtime
    return client.put(f"/api/flows/{flow_id}/versions/{version}", json=body,
                      headers=_headers())


def _publish(client, flow_id, version):
    return client.post(f"/api/flows/{flow_id}/publish",
                       json={"version": version}, headers=_headers())


def _start(client, flow_id, version=None):
    body = {"flow_id": flow_id}
    if version is not None:
        body["version"] = version
    return client.post("/api/executions", json=body, headers=_headers())


class TestPublishGate:
    def test_unsupported_publish_422_structured_not_published(self, redis):
        client = _client(redis)
        flow_store.get_flow_store().create_flow("mappy", tenant_id="default")
        resp = _save(client, "mappy", "1.0.0", _map_def(), runtime="go")
        assert resp.status_code == 200, resp.text

        resp = _publish(client, "mappy", "1.0.0")
        assert resp.status_code == 422, resp.text
        detail = resp.json()["detail"]
        assert detail["error"] == "unsupported_node_types"
        assert detail["runtime"] == "go"
        assert detail["unsupported"] == ["map"]
        assert "start" in detail["supported"] and "http" in detail["supported"]
        # 未发布：状态机不动（draft），引擎存储无键（不入队/不同步）
        row = flow_store.get_flow_store().get_version("mappy", "1.0.0")
        assert row.status == "draft"
        assert redis.get("plaita:flow:mappy:1.0.0") is None

    def test_supported_publish_ok(self, redis):
        client = _client(redis)
        flow_store.get_flow_store().create_flow("plain", tenant_id="default")
        _save(client, "plain", "1.0.0", _plain_def(), runtime="go")
        resp = _publish(client, "plain", "1.0.0")
        assert resp.status_code == 200, resp.text
        assert resp.json()["status"] == "published"

    def test_python_pool_takes_map_flow(self, redis):
        client = _client(redis)
        flow_store.get_flow_store().create_flow("mappy", tenant_id="default")
        saved = _save(client, "mappy", "1.0.0", _map_def(), runtime="python")
        assert saved.status_code == 200, saved.text
        resp = _publish(client, "mappy", "1.0.0")
        assert resp.status_code == 200, resp.text

    def test_local_mode_without_redis_skips_gate(self, tmp_path, monkeypatch):
        # 本地单机档：无 registry 可查、执行在 console 进程内（Python 引擎
        # 即权威），gate 无从谈起也不该拦。
        monkeypatch.setattr("config.get_settings", lambda: _settings())
        monkeypatch.setattr("auth.get_settings", lambda: _settings())
        flow_store.init_engine(f"sqlite:///{tmp_path}/local.db")
        app = FastAPI()
        app.state.redis = None
        app.state.local_mode = False
        app.include_router(flows_api.router, prefix="/api",
                           dependencies=[Depends(require_auth)])
        client = TestClient(app)
        flow_store.get_flow_store().create_flow("mappy", tenant_id="default")
        _save(client, "mappy", "1.0.0", _map_def(), runtime="go")
        resp = _publish(client, "mappy", "1.0.0")
        assert resp.status_code == 200, resp.text

    def test_import_publish_true_same_gate(self, redis):
        client = _client(redis)
        store = flow_store.get_flow_store()
        store.create_flow("mappy", tenant_id="default")
        store.save_flow_definition("mappy", "1.0.0", _map_def("mappy"),
                                   "{}", runtime="go", tenant_id="default")
        from services import deployments as deployments_svc
        package = deployments_svc.build_promotion_package(store, "mappy", "1.0.0")
        resp = client.post("/api/flows/import-version",
                           json={"package": package, "new_version": "2.0.0",
                                 "publish": True},
                           headers=_headers())
        assert resp.status_code == 422, resp.text
        assert resp.json()["detail"]["error"] == "unsupported_node_types"
        row = store.get_version("mappy", "2.0.0")
        assert row is not None and row.status == "draft"


class TestStartGate:
    def _published_go_map_flow(self):
        # 绕过 API 直落一个「已发布但 runtime 不兼容」的旧 flow（gate 上线前
        # 的存量形态），start 必须在入队前拦下。
        store = flow_store.get_flow_store()
        store.create_flow("legacy", tenant_id="default")
        store.save_flow_definition("legacy", "1.0.0", _map_def("legacy"),
                                   "{}", runtime="go", tenant_id="default")
        store.publish_version("legacy", "1.0.0")

    def test_start_incompatible_flow_422_no_enqueue_no_row(self, redis):
        client = _client(redis)
        self._published_go_map_flow()
        resp = _start(client, "legacy", "1.0.0")
        assert resp.status_code == 422, resp.text
        detail = resp.json()["detail"]
        assert detail["error"] == "unsupported_node_types"
        assert detail["runtime"] == "go"
        assert detail["unsupported"] == ["map"]
        # 零残留：无 queued 占位行、无队列消息、无配额占位
        assert redis.xlen(GO_QUEUE) == 0
        assert redis.xlen(DEFAULT_QUEUE) == 0
        assert not redis.keys("plaita:*execution:legacy*")
        assert redis.zcard("plaita:tenant_running:default") == 0

    def test_start_supported_flow_routes_to_go_queue(self, redis):
        client = _client(redis)
        store = flow_store.get_flow_store()
        store.create_flow("plain", tenant_id="default")
        store.save_flow_definition("plain", "1.0.0", _plain_def("plain"),
                                   "{}", runtime="go", tenant_id="default")
        store.publish_version("plain", "1.0.0")
        resp = _start(client, "plain", "1.0.0")
        assert resp.status_code == 200, resp.text
        assert redis.xlen(GO_QUEUE) == 1
        assert redis.xlen(DEFAULT_QUEUE) == 0
        msg = json.loads(redis.xrange(GO_QUEUE)[-1][1]["payload"])
        assert msg["flow_id"] == "plain"

    def test_start_python_flow_routes_to_default_queue(self, redis):
        client = _client(redis)
        store = flow_store.get_flow_store()
        store.create_flow("pysim", tenant_id="default")
        store.save_flow_definition("pysim", "1.0.0", _map_def("pysim"),
                                   "{}", runtime="python", tenant_id="default")
        store.publish_version("pysim", "1.0.0")
        resp = _start(client, "pysim", "1.0.0")
        assert resp.status_code == 200, resp.text
        assert redis.xlen(DEFAULT_QUEUE) == 1
        assert redis.xlen(GO_QUEUE) == 0

    def test_start_without_version_gates_latest_published(self, redis):
        client = _client(redis)
        store = flow_store.get_flow_store()
        store.create_flow("legacy", tenant_id="default")
        store.save_flow_definition("legacy", "1.0.0", _map_def("legacy"),
                                   "{}", runtime="go", tenant_id="default")
        store.publish_version("legacy", "1.0.0")
        resp = _start(client, "legacy")  # version=None → worker 取 latest
        assert resp.status_code == 422, resp.text
        assert redis.xlen(GO_QUEUE) == 0

    def test_start_unknown_flow_keeps_legacy_behavior(self, redis):
        # 版本不在 console 库（直接写引擎存储的集成形态）：无定义可判 → 不拦
        # （保持既有「入队后 worker 侧报 not found」语义，本单不扩 scope）。
        client = _client(redis)
        resp = _start(client, "ghost", "9.9.9")
        assert resp.status_code == 200, resp.text


class TestManifestMissingFallback:
    def test_publish_start_fail_open_with_warning(self, redis, caplog):
        # 旧 build worker：注册条目存在但无 capability_manifest → 池内无清单
        # 可判 → 放行（保持存量行为），但 WARNING 必须可观测。
        redis.delete("plaita:registry:flow_worker:go-1")
        _register_worker(redis, "go-old", engine="plaita-go", with_manifest=False)
        client = _client(redis)
        flow_store.get_flow_store().create_flow("mappy", tenant_id="default")
        _save(client, "mappy", "1.0.0", _map_def(), runtime="go")
        with caplog.at_level(logging.WARNING, logger="services.capability_gate"):
            resp = _publish(client, "mappy", "1.0.0")
        assert resp.status_code == 200, resp.text
        assert any(POOL_MANIFEST_MISSING_WARNING in r.message for r in caplog.records)
        resp = _start(client, "mappy", "1.0.0")
        assert resp.status_code == 200, resp.text
        assert redis.xlen(GO_QUEUE) == 1


class TestRuntimeFieldAndReadFace:
    def test_save_version_persists_runtime(self, redis):
        client = _client(redis)
        flow_store.get_flow_store().create_flow("plain", tenant_id="default")
        resp = _save(client, "plain", "1.0.0", _plain_def(), runtime="go")
        assert resp.status_code == 200, resp.text
        assert resp.json()["runtime"] == "go"
        row = flow_store.get_flow_store().get_version("plain", "1.0.0")
        assert row.runtime == "go"

    def test_runtime_defaults_to_python(self, redis):
        client = _client(redis)
        flow_store.get_flow_store().create_flow("plain", tenant_id="default")
        resp = _save(client, "plain", "1.0.0", _plain_def())
        assert resp.status_code == 200, resp.text
        assert resp.json()["runtime"] == "python"

    def test_invalid_runtime_422(self, redis):
        client = _client(redis)
        flow_store.get_flow_store().create_flow("plain", tenant_id="default")
        resp = _save(client, "plain", "1.0.0", _plain_def(), runtime="perl")
        assert resp.status_code == 422, resp.text

    def test_services_read_face_exposes_manifest(self, redis):
        client = _client(redis)
        resp = client.get("/api/services", headers=_headers())
        assert resp.status_code == 200, resp.text
        rows = {s["instance_id"]: s for s in resp.json()["services"]}
        go = rows["go-1"]
        manifest = go["capability_manifest"]
        assert manifest["engine"] == "plaita-go"
        assert manifest["supported_node_types"] == sorted(GO_SUPPORTED)
        assert "map" in manifest["unsupported_node_types"]
        assert manifest["ir_contract_version"]
        # metadata 原样透出不受影响（旧读者兼容）
        assert go["metadata"]["capability_manifest"]["engine"] == "plaita-go"
        # 无清单的条目投影为 null（不是缺失键）
        _register_worker(redis, "go-old", engine="plaita-go", with_manifest=False)
        resp = client.get("/api/services", headers=_headers())
        rows = {s["instance_id"]: s for s in resp.json()["services"]}
        assert rows["go-old"]["capability_manifest"] is None


class TestCrossTenantIsolation:
    """跨租户隔离（#300）：版本行解析、能力门判定池、路由队列都按请求租户
    的版本行走——同名 flow 在两个租户可有不同 runtime，互不串门/串池。"""

    def _publish_legacy(self, flow_id, definition, runtime, tenant_id):
        store = flow_store.get_flow_store()
        store.create_flow(flow_id, tenant_id=tenant_id)
        store.save_flow_definition(flow_id, "1.0.0", definition, "{}",
                                   runtime=runtime, tenant_id=tenant_id)
        store.publish_version(flow_id, "1.0.0", tenant_id=tenant_id)

    def test_same_flow_id_gates_and_routes_per_tenant_row(self, redis):
        # 同名 flow：tenant-b = go+map（go 池拒绝），default = python+map
        # （python 池接受）。start 各按本租户版本行判门/选池。
        self._publish_legacy("mappy", _map_def("mappy"), "go", "tenant-b")
        self._publish_legacy("mappy", _map_def("mappy"), "python", "default")
        client = _client(redis)
        tenant_b = {**_headers(), "X-Tenant-ID": "tenant-b"}
        resp = client.post("/api/executions",
                           json={"flow_id": "mappy", "version": "1.0.0"},
                           headers=tenant_b)
        assert resp.status_code == 422, resp.text
        assert resp.json()["detail"]["error"] == "unsupported_node_types"
        assert resp.json()["detail"]["runtime"] == "go"
        assert redis.xlen(GO_QUEUE) == 0 and redis.xlen(DEFAULT_QUEUE) == 0
        # default 租户的同名 flow 对 python 池判门通过，只进默认队列
        resp = _start(client, "mappy", "1.0.0")
        assert resp.status_code == 200, resp.text
        assert redis.xlen(DEFAULT_QUEUE) == 1 and redis.xlen(GO_QUEUE) == 0

    def test_publish_does_not_borrow_other_tenant_row(self, redis):
        # go+map 草稿只在 tenant-b；default 租户 publish 同 id 同版本 → 404
        #（流程不存在），不借他租户的行过门/发布。
        self._publish_legacy("mappy", _map_def("mappy"), "go", "tenant-b")
        client = _client(redis)
        resp = _publish(client, "mappy", "1.0.0")  # default 租户上下文
        assert resp.status_code == 404, resp.text

    def test_tenant_b_publish_gates_against_go_pool(self, redis):
        # tenant-b 自己的 go+map 草稿发布 → 对 go 池判门 422，draft 不动。
        store = flow_store.get_flow_store()
        store.create_flow("mappy", tenant_id="tenant-b")
        store.save_flow_definition("mappy", "1.0.0", _map_def("mappy"), "{}",
                                   runtime="go", tenant_id="tenant-b")
        client = _client(redis)
        tenant_b = {**_headers(), "X-Tenant-ID": "tenant-b"}
        resp = client.post("/api/flows/mappy/publish",
                           json={"version": "1.0.0"}, headers=tenant_b)
        assert resp.status_code == 422, resp.text
        detail = resp.json()["detail"]
        assert detail["error"] == "unsupported_node_types"
        assert detail["runtime"] == "go"
        assert store.get_version("mappy", "1.0.0", tenant_id="tenant-b").status == "draft"
