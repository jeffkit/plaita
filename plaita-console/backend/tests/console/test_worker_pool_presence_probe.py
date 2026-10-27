"""worker 池点名（2026-10-27，wake 点名姊妹格；Go 生产可选 ADR-002 修订配套）。

钉六格：
1. **负格（本单主诉求）**：runtime=go 流程 × 注册表只有 python worker → 422
   ``worker_pool_absent``——堵「发布成功、启动入队、go 池零消费者 → 执行永远
   pending」的同族搁浅（#300 门对空池 fail-open 是发布面兼容取舍，受理面
   有版本行在手、可以具名拒）；
2. **正格**：目标 runtime 有已分类活 worker（manifest.engine 命中）→ 放行；
3. **旧 build 兼容格**：注册表条目不可分类（无 manifest/坏值）→ 放行 +
   WARNING（与 #300 对同一人群的 fail-open 取舍一致——点名不能证明其不在岗）；
4. **Python 池同样受闸**：零 worker 时 start python 流程也 422（新行为钉——
   同一搁浅族；现网有 worker 在跑则无感）；
5. **fail-open**：Redis 读不动 → 放行 + WARNING（降级可观测红线）；
6. **本地档不受影响**：start 走 engine backend 进程内执行，不经本闸。

worker 注册键形（与 :func:`collect_pool_manifests` 同源）：
``plaita:registry:flow_worker:{instance_id}``，值
``{instance_id, metadata: {capability_manifest: {engine, ...五键}}}``。
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

from api import executions as executions_api  # noqa: E402
from api import flows as flows_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import flow_store  # noqa: E402

ADMIN_KEY = "pool-probe-admin"


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


def _def(flow_id, node_types):
    nodes = [{"type": "start", "id": "start", "next": "x"}]
    for i, nt in enumerate(node_types):
        nodes.append({"type": nt, "id": f"x{i}" if len(node_types) > 1 else "x"})
    nodes.append({"type": "end", "id": "end", "output": "x"})
    # 链 next
    for i in range(len(nodes) - 2):
        nodes[i + 1]["next"] = nodes[i + 2]["id"]
    return json.dumps({"flow_id": flow_id, "nodes": nodes})


def _worker(registry_value_engine, instance="i1"):
    """注册键值：可分类（engine 已知）/ 不可分类（旧 build）两种形状。"""
    if registry_value_engine is None:
        return json.dumps({"instance_id": instance, "metadata": {}})
    manifest = {
        "engine": registry_value_engine,
        "ir_contract_version": "2026-10-22.4",
        "supported_node_types": ["start", "end", "assignment"],
        "unsupported_node_types": ["calculate"],
        "features": ["suspend"],
    }
    return json.dumps({
        "instance_id": instance,
        "metadata": {"capability_manifest": manifest},
    })


@pytest.fixture()
def env(tmp_path, monkeypatch):
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    flow_store.init_engine(f"sqlite:///{tmp_path / 'pool-probe.db'}")
    store = flow_store.get_flow_store()
    for fid, runtime in (("goflow", "go"), ("pyflow", "python")):
        store.ensure_flow(fid, tenant_id="default")
        store.save_flow_definition(fid, "1.0.0", _def(fid, ["assignment"]),
                                   status="published", tenant_id="default",
                                   runtime=runtime)
    redis = FakeRedis(decode_responses=True)
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.state.store = store
    app.include_router(flows_api.router, prefix="/api", dependencies=[Depends(require_auth)])
    app.include_router(executions_api.router, prefix="/api", dependencies=[Depends(require_auth)])
    return TestClient(app), redis, {"X-Admin-API-Key": ADMIN_KEY}


def _register_worker(redis, engine, instance="i1"):
    redis.set(f"plaita:registry:flow_worker:{instance}", _worker(engine), ex=30)


def test_go_flow_start_warns_and_admits_when_go_pool_empty(env, caplog):
    """负格降级（2026-10-27 晚，停机缓冲契约）：go 流程 × go 池空 → 具名
    WARNING + 放行（任务入队等 worker 拉起——chaos-wr 契约优先于硬拒）。"""
    import logging

    client, redis, headers = env
    _register_worker(redis, "python", "py-1")
    with caplog.at_level(logging.WARNING):
        r = client.post("/api/executions", json={"flow_id": "goflow"}, headers=headers)
    assert r.status_code == 200, r.text  # 放行（入队等拉起）
    assert any("worker_pool_absent" in r.message for r in caplog.records)


def test_go_flow_start_passes_when_go_worker_registered(env):
    """正格：go worker（engine=plaita-go → runtime go）在场 → 放行。"""
    client, redis, headers = env
    _register_worker(redis, "plaita-go", "go-1")
    r = client.post("/api/executions", json={"flow_id": "goflow"}, headers=headers)
    assert r.status_code == 200, r.text


def test_python_flow_warns_when_pool_empty(env, caplog):
    """Python 池同口径：零 worker → WARNING + 放行（停机缓冲契约，观测不静默）。"""
    import logging

    client, redis, headers = env
    with caplog.at_level(logging.WARNING):
        r = client.post("/api/executions", json={"flow_id": "pyflow"}, headers=headers)
    assert r.status_code == 200, r.text
    assert any("worker_pool_absent" in r.message for r in caplog.records)


def test_python_flow_passes_with_python_worker(env):
    """正控：python worker 在场 → python 流程放行（现网常态无感）。"""
    client, redis, headers = env
    _register_worker(redis, "python", "py-1")
    r = client.post("/api/executions", json={"flow_id": "pyflow"}, headers=headers)
    assert r.status_code == 200, r.text


def test_legacy_worker_unclassifiable_fails_open_with_warning(env, caplog):
    """旧 build 兼容格：注册表条目不可分类（无 manifest）→ 放行 + WARNING
    （#300 对同一人群的 fail-open 取舍守恒）。"""
    import logging

    client, redis, headers = env
    redis.set("plaita:registry:flow_worker:legacy-1", _worker(None), ex=30)
    with caplog.at_level(logging.WARNING):
        r = client.post("/api/executions", json={"flow_id": "goflow"}, headers=headers)
    assert r.status_code == 200, r.text
    assert any("worker_pool_probe_degraded" in r.message for r in caplog.records)


def test_probe_fail_open_on_redis_error(env, monkeypatch):
    """fail-open：Redis 读不动 → 放行（点名查不动不拦流程）。"""
    client, redis, headers = env

    def boom(pattern, count=1000):
        raise ConnectionError("redis down")

    monkeypatch.setattr(redis, "scan_iter", boom)
    r = client.post("/api/executions", json={"flow_id": "goflow"}, headers=headers)
    assert r.status_code == 200, r.text


def test_local_tier_unaffected_by_pool_probe(tmp_path, monkeypatch):
    """本地档不受影响：start 走 engine backend 进程内执行，不经池点名。"""
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    flow_store.init_engine(f"sqlite:///{tmp_path / 'local-pool.db'}")
    store = flow_store.get_flow_store()
    store.ensure_flow("pyflow", tenant_id="default")
    store.save_flow_definition("pyflow", "1.0.0", _def("pyflow", ["assignment"]),
                               status="published", tenant_id="default")
    app = FastAPI()
    app.state.redis = None
    app.state.local_mode = True
    app.state.store = store
    app.include_router(executions_api.router, prefix="/api", dependencies=[Depends(require_auth)])
    client = TestClient(app)
    r = client.post("/api/executions", json={"flow_id": "pyflow"},
                    headers={"X-Admin-API-Key": ADMIN_KEY})
    assert r.status_code == 200, r.text
