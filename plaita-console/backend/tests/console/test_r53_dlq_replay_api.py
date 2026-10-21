"""A53-S2（duo-a 第五十三轮）：控制台回灌端点的幂等/授权/审计契约。

``POST /api/queues/dlq/{dlq_id}/replay`` 是回灌的唯一控制台入口：

- 平台管理员专属（租户角色不足以回灌，B53-01）；
- 第二次回灌返回 ``already_replayed``（具名拒绝），不重复入队；
- 响应回显源信封的 ``schema_version``；DLQ 原条目保留；
- 每次尝试写控制台审计行。
"""
from __future__ import annotations

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

from api import auth_users  # noqa: E402
from api import queues as queues_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import audit as audit_svc  # noqa: E402
from services import flow_store, tenants_svc, users_svc  # noqa: E402

ADMIN_KEY = "r53-replay-admin-key"
TENANT = "acme"
DLQ_KEY = "plaita:flow:queue:v2:dlq"
TARGET = "plaita:flow:queue:v2"


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


@pytest.fixture()
def env(tmp_path, monkeypatch):
    db_file = tmp_path / "r53-replay.db"
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{db_file}")
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    flow_store.init_engine(f"sqlite:///{db_file}")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()
    tenants_svc.create_tenant(store, TENANT, "Acme")
    users_svc.create_user(
        store,
        "viewer_acme",
        "viewer-password-1",
        "viewer",
        memberships=[{"tenant_id": TENANT, "role": "viewer"}],
    )
    redis = FakeRedis(decode_responses=True)
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.state.store = store
    app.include_router(auth_users.router, prefix="/api")
    app.include_router(
        queues_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    return TestClient(app), redis, store


def _admin() -> dict:
    return {"X-Admin-API-Key": ADMIN_KEY}


def _viewer(client: TestClient) -> dict:
    r = client.post(
        "/api/auth/login",
        json={"username": "viewer_acme", "password": "viewer-password-1"},
    )
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['token']}"}


def _seed(redis, *, schema_version=1, execution_id="exec-api-1"):
    from plaita.runtime_config import envelope_for_stream

    envelope = {
        "reason": "max_deliveries=5",
        "source_stream": TARGET,
        "source_id": "1699-0",
        "delivery_count": 5,
        "dead_lettered_at": 1700000000.0,
        "schema_version": schema_version,
        # r58：源/目标 family 一致的正控（跨 family 拒绝见新单测）。
        "config_family": envelope_for_stream(TARGET, role="console")["config_family"],
        "payload": {
            "type": "start",
            "flow_id": "f1",
            "version": "1",
            "tenant_id": TENANT,
            "execution_id": execution_id,
            "params": {"x": 1},
        },
    }
    return redis.xadd(DLQ_KEY, {"payload": json.dumps(envelope)})


def _replay(client, dlq_id, *, headers=None, body=None):
    return client.post(
        f"/api/queues/dlq/{dlq_id}/replay",
        json=body or {"dlq_key": DLQ_KEY},
        headers=headers or _admin(),
    )


class TestReplayEndpoint:
    def test_replay_once_and_second_is_named_rejection(self, env):
        client, redis, _store = env
        dlq_id = _seed(redis)
        first = _replay(client, dlq_id)
        assert first.status_code == 200, first.text
        payload = first.json()
        assert payload["replayed"] is True and payload["reason"] == "ok"
        assert payload["execution_id"] == "exec-api-1"
        assert payload["schema_version"] == 1
        assert redis.xlen(TARGET) == 1

        second = _replay(client, dlq_id)
        assert second.status_code == 200, second.text
        assert second.json()["replayed"] is False
        assert second.json()["reason"] == "already_replayed"
        assert second.json()["target_message_id"] == payload["target_message_id"]
        assert redis.xlen(TARGET) == 1, "二次 replay 不得重复入队"
        # DLQ 原条目保留。
        assert redis.xlen(DLQ_KEY) == 1

    def test_console_audit_rows_recorded(self, env):
        client, redis, _store = env
        dlq_id = _seed(redis)
        _replay(client, dlq_id)
        _replay(client, dlq_id)
        rows = audit_svc.list_audit(action="dlq.replay", limit=10)
        assert len(rows) == 2
        results = sorted(row["detail"].get("reason") for row in rows)
        assert results == ["already_replayed", "ok"]

    def test_schema_too_new_is_rejected_before_enqueue(self, env):
        """v2 载体在本 build（写版本 1）上必须入队前具名拒绝。"""
        client, redis, _store = env
        dlq_id = _seed(redis, schema_version=2)
        r = _replay(client, dlq_id, body={"dlq_key": DLQ_KEY})
        assert r.status_code == 200
        assert r.json()["replayed"] is False
        assert r.json()["reason"] == "schema_too_new"
        assert r.json()["schema_version"] == 2
        assert redis.xlen(TARGET) == 0

    def test_schema_version_inherited_from_source_envelope(self, env):
        client, redis, _store = env
        dlq_id = _seed(redis, schema_version=1)
        r = _replay(client, dlq_id, body={"dlq_key": DLQ_KEY})
        assert r.status_code == 200
        assert r.json()["schema_version"] == 1
        _, fields = redis.xrange(TARGET)[0]
        assert fields["schema_version"] == "1"

    def test_dlq_read_surface_echoes_schema_version(self, env):
        """baseline 控制台回显（A53-03 派工要点）：/queues/dlq 带 schema_version。"""
        client, redis, _store = env
        _seed(redis, schema_version=2)
        body = client.get("/api/queues/dlq", headers=_admin()).json()
        entry = next(
            e
            for stream in body["streams"]
            if stream["name"] == DLQ_KEY
            for e in stream["entries"]
        )
        assert entry["schema_version"] == 2

    def test_viewer_is_rejected(self, env):
        client, redis, _store = env
        dlq_id = _seed(redis)
        r = _replay(client, dlq_id, headers=_viewer(client))
        assert r.status_code == 403
        assert redis.xlen(TARGET) == 0

    def test_unknown_entry_is_404(self, env):
        client, _redis, _store = env
        r = _replay(client, "999-0")
        assert r.status_code == 404

    def test_unregistered_target_is_422(self, env):
        client, redis, _store = env
        dlq_id = _seed(redis)
        r = _replay(
            client,
            dlq_id,
            body={"dlq_key": DLQ_KEY, "target_stream": "attacker:stream"},
        )
        assert r.status_code == 422
        assert redis.xlen(TARGET) == 0

    def test_dlq_key_can_be_resolved_by_entry_id(self, env):
        client, redis, _store = env
        dlq_id = _seed(redis)
        r = _replay(client, dlq_id, body={"target_stream": TARGET})
        assert r.status_code == 200
        assert r.json()["replayed"] is True
