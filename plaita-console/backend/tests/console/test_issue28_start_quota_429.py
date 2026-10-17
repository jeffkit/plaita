"""plaita#28 守卫（console 侧）：start 入口每租户准入配额，超限 429。

缺陷（issue #28）：console POST /api/executions 无条件入队——一个租户可以
制造任意大的入队积压；单 stream FIFO + worker 串行消费下，其他租户的任务
排在积压之后无限期等待。入口拒绝（429）是唯一能拦住「积压形成」的位置。

形状：
- 超限 start 返回 429 且**不入队**（stream 长度不增长）；
- 未配额租户 fail-open（存量行为零变化）；
- 名额被占用后，执行终态释放，新 start 可再入。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from fastapi import Depends, FastAPI
from fakeredis import FakeRedis
from starlette.testclient import TestClient

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import executions as executions_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import flow_store  # noqa: E402

from plaita.server.tenant_quota import (  # noqa: E402
    publish_tenant_quota,
    release_tenant_start,
    tenant_running_count,
)

ADMIN_KEY = "cluster-admin-key"


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


@pytest.fixture()
def env(tmp_path, monkeypatch):
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    flow_store.init_engine(f"sqlite:///{tmp_path}/issue28-quota.db")
    return FakeRedis(decode_responses=True)


def _client(redis) -> TestClient:
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.state.store = flow_store.get_flow_store()
    app.include_router(
        executions_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    return TestClient(app)


def _headers():
    return {"X-Admin-API-Key": ADMIN_KEY}


def _stream_len(redis) -> int:
    return redis.xlen("plaita:flow:queue")


class TestStartQuota429:
    def test_over_quota_start_rejected_429_and_not_enqueued(self, env):
        client = _client(env)
        publish_tenant_quota(env, "default", 2)

        assert client.post("/api/executions", json={"flow_id": "f1"}, headers=_headers()).status_code == 200
        assert client.post("/api/executions", json={"flow_id": "f1"}, headers=_headers()).status_code == 200
        assert _stream_len(env) == 2
        assert tenant_running_count(env, "default") == 2

        resp = client.post("/api/executions", json={"flow_id": "f1"}, headers=_headers())
        assert resp.status_code == 429, resp.text
        # 拒绝的 start 不得入队（积压不形成——#28 的入口语义）
        assert _stream_len(env) == 2
        detail = resp.json()["detail"]
        assert detail["error"] == "tenant_quota_exceeded"

    def test_fail_open_without_quota(self, env):
        """未配额租户：行为与存量完全一致（全部入队）。"""
        client = _client(env)
        for _ in range(3):
            resp = client.post("/api/executions", json={"flow_id": "f1"}, headers=_headers())
            assert resp.status_code == 200, resp.text
        assert _stream_len(env) == 3

    def test_release_frees_admission(self, env):
        """终态释放名额后，新 start 可再入（配额是并发数，不是总数）。"""
        client = _client(env)
        publish_tenant_quota(env, "default", 1)
        assert client.post("/api/executions", json={"flow_id": "f1"}, headers=_headers()).status_code == 200
        assert client.post("/api/executions", json={"flow_id": "f1"}, headers=_headers()).status_code == 429
        release_tenant_start(env, "default", env.xrange("plaita:flow:queue") and _last_execution_id(env))
        resp = client.post("/api/executions", json={"flow_id": "f1"}, headers=_headers())
        assert resp.status_code == 200, resp.text
        assert _stream_len(env) == 2


def _last_execution_id(redis) -> str:
    entries = redis.xrange("plaita:flow:queue")
    payload = json.loads(entries[-1][1]["payload"])
    return payload["execution_id"]
