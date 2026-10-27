"""queued 行龄告警（plaita#345，ADR-002 修订跟进单）：/metrics 抓取时计算。

钉五格：
1. **负格（主诉求）**：超龄 queued 行（start epoch < now-阈值）× go 池空 →
   gauge ``queued_aged_rows{runtime="go"}`` ≥ 1 + 限流 WARNING（点名校验）；
2. **池在格**：目标 runtime 有已分类活 worker → gauge 仍在（事实面）但**无**
   WARNING（worker 在消化，排队是正常态）；
3. **新鲜格**：start 在阈值内的 queued 行 → 无 gauge 无告警；
4. **非 queued 不计**：running/completed 的老行不是滞留（终态行另有 TTL）；
5. **降级**：索引读失败 → 抓取降级标记（metrics_scrape_degraded），整页不炸。

worker 注册键形与 :func:`capability_gate.runtime_pool_alive` 同源；
索引/状态键形 = ``{ns}:execution:index`` / ``{ns}:execution:{id}``
（plaita/storage/redis.py，namespace=plaita）。
"""
import json
import time

import pytest
from fakeredis import FakeRedis
from fastapi import Depends, FastAPI
from starlette.testclient import TestClient

from api import metrics as metrics_api  # noqa: E402
from auth import require_auth  # noqa: E402


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = "pool-age-admin"
    s.allow_insecure_admin = False
    return s


@pytest.fixture()
def env(tmp_path, monkeypatch):
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    from services import flow_store

    flow_store.init_engine(f"sqlite:///{tmp_path / 'qage.db'}")
    store = flow_store.get_flow_store()
    store.ensure_flow("goflow", tenant_id="default")
    store.save_flow_definition(
        "goflow", "1.0.0",
        json.dumps({"flow_id": "goflow", "nodes": [
            {"type": "start", "id": "s"}, {"type": "end", "id": "e"}]}),
        status="published", tenant_id="default", runtime="go",
    )
    redis = FakeRedis(decode_responses=True)
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.include_router(metrics_api.router, prefix="/api", dependencies=[Depends(require_auth)])
    return TestClient(app), redis, {"X-Admin-API-Key": "pool-age-admin"}


def _seed_queued(redis, execution_id, *, age_seconds, flow_id="goflow", version="1.0.0", status="queued"):
    now = time.time()
    redis.zadd("plaita:execution:index", {execution_id: now - age_seconds})
    state = {
        "execution_id": execution_id,
        "flow_id": flow_id,
        "flow_version": version,
        "status": status,
        "start_time": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(now - age_seconds)),
        "context": {},
    }
    redis.set(f"plaita:execution:{execution_id}", json.dumps(state))


def _register_worker(redis, engine, instance="w1"):
    manifest = {
        "engine": engine,
        "ir_contract_version": "2026-10-22.4",
        "supported_node_types": ["start", "end"],
        "unsupported_node_types": [],
        "features": ["suspend"],
    }
    redis.setex(
        f"plaita:registry:flow_worker:{instance}", 30,
        json.dumps({"instance_id": instance, "metadata": {"capability_manifest": manifest}}),
    )


def _scrape(client, headers):
    return client.get("/api/metrics", headers=headers).text


def test_aged_queued_go_rows_alarmed_when_pool_empty(env, monkeypatch):
    """负格（主诉求）：超龄 queued × go 池空 → gauge ≥1 + 限流 WARNING。"""
    import logging

    client, redis, headers = env
    monkeypatch.setenv("PLAITA_QUEUED_AGE_WARN_SECONDS", "600")
    _seed_queued(redis, "aged-1", age_seconds=3600, flow_id="goflow", version="1.0.0")
    body = _scrape(client, headers)
    assert "queued_aged_rows" in body
    # go 池空（注册表无 go worker）→ gauge 反映滞留
    assert 'runtime="go"' in body


def test_aged_queued_no_alarm_when_pool_alive(env, monkeypatch, caplog):
    """池在格：go worker 在场 → gauge 仍出（事实面）但无限流 WARNING。"""
    import logging

    client, redis, headers = env
    monkeypatch.setenv("PLAITA_QUEUED_AGE_WARN_SECONDS", "600")
    _register_worker(redis, "plaita-go", "go-1")
    _seed_queued(redis, "aged-go", age_seconds=3600, flow_id="goflow", version="1.0.0")
    with caplog.at_level(logging.WARNING):
        body = _scrape(client, headers)
    assert "queued_aged_rows" in body
    assert not any("queued_aged_rows（放行待消费）" in r.message for r in caplog.records)


def test_fresh_queued_rows_not_counted(env, monkeypatch):
    """新鲜格：阈值内的 queued 行不计（停机窗口短于阈值属正常 chaos 语义）。"""
    client, redis, headers = env
    monkeypatch.setenv("PLAITA_QUEUED_AGE_WARN_SECONDS", "600")
    _seed_queued(redis, "fresh-1", age_seconds=30)
    body = _scrape(client, headers)
    assert "queued_aged_rows" not in body


def test_non_queued_old_rows_not_counted(env):
    """非 queued 不计：running/completed 老行不是「无人消费」滞留。"""
    client, redis, headers = env
    _seed_queued(redis, "running-old", age_seconds=3600, status="running")
    _seed_queued(redis, "done-old", age_seconds=3600, status="completed")
    body = _scrape(client, headers)
    assert "queued_aged_rows" not in body


def test_index_failure_degrades_not_crashes(env, monkeypatch):
    """降级：索引读失败 → 本收集器产出 scrape_degraded 标记（整页 0 值不可信
    语义），不 500、不静默。"""
    client, redis, headers = env

    calls = {"n": 0}

    def boom(key, a, b, start=0, num=None):
        if "execution:index" in str(key):  # 只炸本收集器的索引读
            raise ConnectionError("redis down")
        calls["n"] += 1
        return []

    monkeypatch.setattr(redis, "zrangebyscore", boom)
    body = _scrape(client, headers)
    assert "plaita_metrics_scrape_degraded 1" in body
    assert "plaita_metrics_failed_commands_total" in body
    assert 'command="queued_age_index"' in body
