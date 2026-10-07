"""P0 可见性（keeper 迁移设计稿 §5.5 清单①）API 侧一半：start 即返 execution_id。

- POST /api/executions 提交时铸造 execution_id 并随队列消息透传；
- 调用方即刻可凭 id 轮询/取消，无需等 worker 消费。
"""
import json
import sys
from datetime import datetime
from pathlib import Path

import pytest
from fastapi import Depends, FastAPI
from fakeredis import FakeRedis
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import executions as executions_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import flow_store  # noqa: E402

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
    flow_store.init_engine(f"sqlite:///{tmp_path}/start-exec-id.db")
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


class TestStartReturnsExecutionId:
    def test_start_returns_id_and_message_carries_it(self, env):
        client = _client(env)
        resp = client.post("/api/executions", json={
            "flow_id": "f1", "params": {"x": 1},
        }, headers=_headers())
        assert resp.status_code == 200, resp.text
        body = resp.json()
        eid = body["execution_id"]
        assert eid and len(eid) == 32

        # 队列消息携带同一 id（worker start_flow 认账 → 行 id 一致）
        msg = json.loads(env.xrange("plaita:flow:queue")[-1][1]["payload"])
        assert msg["type"] == "start"
        assert msg["execution_id"] == eid
        assert msg["params"] == {"x": 1}
        # timestamp 带时区偏移：worker 的排队时长（queue_wait_ms）按它差分，
        # BFF 与 worker 不同时区时 naive 本地时间会算成整小时级假等待
        assert datetime.fromisoformat(msg["timestamp"]).tzinfo is not None

    def test_start_ids_are_unique(self, env):
        client = _client(env)
        ids = set()
        for _ in range(3):
            resp = client.post("/api/executions", json={"flow_id": "f1", "params": {}},
                               headers=_headers())
            ids.add(resp.json()["execution_id"])
        assert len(ids) == 3
