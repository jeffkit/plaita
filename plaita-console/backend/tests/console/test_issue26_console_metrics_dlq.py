"""#26 console 侧：DLQ 可见性（队列页/死信端点）+ 集群 /metrics 端点。

此前 ``KNOWN_QUEUES`` 不含 DLQ 键，队列页永远看不到死信堆积；全仓也没有任何
Prometheus 端点。本组覆盖 console 的两处补齐。
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

from api import metrics as metrics_api  # noqa: E402
from api import queues as queues_api  # noqa: E402
from auth import require_auth  # noqa: E402

ADMIN_KEY = "issue26-admin-key"


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


@pytest.fixture()
def redis():
    return FakeRedis(decode_responses=True)


@pytest.fixture()
def client(redis, monkeypatch):
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.include_router(queues_api.router, prefix="/api", dependencies=[Depends(require_auth)])
    app.include_router(metrics_api.router, prefix="/api", dependencies=[Depends(require_auth)])
    return TestClient(app)


def _headers():
    return {"X-Admin-API-Key": ADMIN_KEY}


def _seed_dead_letter(redis, dlq_key="plaita:flow:queue:v2:dlq"):
    envelope = {
        "reason": "max_deliveries=5",
        "source_stream": "plaita:flow:queue:v2",
        "source_id": "1699-0",
        "delivery_count": 5,
        "dead_lettered_at": 1699999999.5,
        "payload": {"type": "start", "flow_id": "f1"},
    }
    return redis.xadd(dlq_key, {"payload": json.dumps(envelope)})


class TestKnownQueuesIncludeDlq:
    def test_dlq_keys_registered(self):
        assert "plaita:flow:queue:dlq" in queues_api.KNOWN_QUEUES
        assert "plaita:flow:queue:v2:dlq" in queues_api.KNOWN_QUEUES
        assert "plaita:flow:queue:v2:dlq" in queues_api.KNOWN_DLQ_KEYS

    def test_list_queues_shows_empty_dlq_row(self, client):
        body = client.get("/api/queues", headers=_headers()).json()
        names = {q["name"] for q in body["queues"]}
        assert "plaita:flow:queue:v2:dlq" in names
        row = next(q for q in body["queues"] if q["name"] == "plaita:flow:queue:v2:dlq")
        assert row["queue_type"] == "stream"

    def test_list_queues_reports_dlq_backlog(self, client, redis):
        _seed_dead_letter(redis, "plaita:flow:queue:dlq")
        body = client.get("/api/queues", headers=_headers()).json()
        row = next(q for q in body["queues"] if q["name"] == "plaita:flow:queue:dlq")
        assert row["length"] == 1


class TestDlqEndpoint:
    def test_empty_dlq_still_listed(self, client):
        body = client.get("/api/queues/dlq", headers=_headers()).json()
        names = {s["name"] for s in body["streams"]}
        assert "plaita:flow:queue:v2:dlq" in names
        assert all(s["length"] == 0 for s in body["streams"])

    def test_entries_expose_dead_letter_envelope(self, client, redis):
        msg_id = _seed_dead_letter(redis)
        body = client.get("/api/queues/dlq", headers=_headers()).json()
        stream = next(s for s in body["streams"] if s["name"] == "plaita:flow:queue:v2:dlq")
        assert stream["length"] == 1
        entry = stream["entries"][0]
        assert entry["dlq_id"] == msg_id
        assert entry["reason"] == "max_deliveries=5"
        assert entry["source_stream"] == "plaita:flow:queue:v2"
        assert entry["payload"]["flow_id"] == "f1"

    def test_count_bounds_returned_entries(self, client, redis):
        for _ in range(5):
            _seed_dead_letter(redis)
        body = client.get("/api/queues/dlq?count=2", headers=_headers()).json()
        stream = next(s for s in body["streams"] if s["name"] == "plaita:flow:queue:v2:dlq")
        assert stream["length"] == 5
        assert len(stream["entries"]) == 2


class TestClusterMetrics:
    def test_prometheus_text_with_queues_and_workers(self, client, redis):
        redis.xadd("plaita:flow:queue:v2", {"payload": "{}"})
        redis.set("plaita:registry:flow_worker:w1", "{}")
        redis.set("plaita:registry:flow_worker:w2", "{}")
        resp = client.get("/api/metrics", headers=_headers())
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/plain")
        body = resp.text
        assert "plaita_workers_registered_total 2" in body
        assert 'plaita_worker_registered{instance="w1"} 1' in body
        assert "plaita_queue_stream_length{" in body
        assert 'stream="plaita:flow:queue:v2"' in body
        assert "plaita_queue_dlq_length" in body

    def test_workers_all_dead_reads_zero(self, client):
        body = client.get("/api/metrics", headers=_headers()).text
        assert "plaita_workers_registered_total 0" in body

    def test_requires_admin_key(self, client):
        assert client.get("/api/metrics").status_code == 401
