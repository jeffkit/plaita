"""A49-02：一条畸形死信让 console ``/api/queues/dlq`` 整体 500。

## 缺陷

``_dead_letter_invalid`` 对畸形 payload 写入的是**截断原始串**，而
``DlqEntry.payload`` 定型 ``Dict``。``_parse_dlq_entry`` 在租户过滤之前构造
模型 → Pydantic ``ValidationError`` → 端点未捕获 → 整个 DLQ 读面（全部流、
平台与全部租户视角）一起 500。毒丸在被 XTRIM/人工删除前持续遮蔽读面。

## 修复契约

1. ``payload`` 非 dict（原始串）时包成 ``{"raw": ...}``，形状不变、证据不丢；
2. 整个信封不可解析时同样按 raw 形态保底，不再抛；
3. 数值字段容错（非法 → None），单条毒丸不得影响其他条目；
4. DLQ 信封的 ``schema_version`` / ``raw_fields`` / ``truncated`` 透出到 API。

## 负控（还原修复后必须精确红）

- 还原 ``_payload_shape``（直接 ``payload=envelope.get("payload") or {}``）：
  ``test_malformed_dead_letter_does_not_500`` 由 200 变 500；
- 还原非 JSON 兜底：``test_non_json_dead_letter_payload_is_raw`` 丢失 raw 证据。
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

from api import queues as queues_api  # noqa: E402
from auth import require_auth  # noqa: E402

ADMIN_KEY = "r49-admin-key"
DLQ_KEY = "plaita:flow:queue:v2:dlq"


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
    app.include_router(
        queues_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    return TestClient(app)


def _headers():
    return {"X-Admin-API-Key": ADMIN_KEY}


def _seed_normal(redis, tenant="acme"):
    envelope = {
        "reason": "max_deliveries=5",
        "source_stream": "plaita:flow:queue:v2",
        "source_id": "1699-0",
        "delivery_count": 5,
        "dead_lettered_at": 1699999999.5,
        "schema_version": 1,
        "payload": {"type": "start", "flow_id": "f1", "tenant_id": tenant},
    }
    return redis.xadd(DLQ_KEY, {"payload": json.dumps(envelope)})


def _seed_malformed(redis):
    """真实 ``_dead_letter_invalid`` 形态：合法 JSON 信封 + 字符串 payload。"""
    envelope = {
        "reason": "invalid_payload: Expecting value: line 1 column 1 (char 0)",
        "source_stream": "plaita:flow:queue:v2",
        "source_id": "1700-0",
        "delivery_count": 1,
        "dead_lettered_at": 1700000000.0,
        "schema_version": 3,
        "payload": "{xxxxxxxx truncated raw payload",
        "raw_fields": {
            "payload": "{xxxxxxxx truncated raw payload",
            "schema_version": "3",
        },
        "truncated": True,
    }
    return redis.xadd(DLQ_KEY, {"payload": json.dumps(envelope)})


def _dlq_stream(client):
    body = client.get("/api/queues/dlq", headers=_headers()).json()
    return next(s for s in body["streams"] if s["name"] == DLQ_KEY)


class TestMalformedDeadLetterDoesNotBreakReadSurface:
    def test_malformed_dead_letter_does_not_500(self, client, redis):
        """正常条目 + 畸形条目 → 200，畸形条目以 raw 形态可见。"""
        normal_id = _seed_normal(redis)
        malformed_id = _seed_malformed(redis)

        resp = client.get("/api/queues/dlq", headers=_headers())
        assert resp.status_code == 200, f"一条毒丸不得让 DLQ 读面 500: {resp.text}"

        stream = next(s for s in resp.json()["streams"] if s["name"] == DLQ_KEY)
        assert stream["length"] == 2
        by_id = {entry["dlq_id"]: entry for entry in stream["entries"]}
        assert set(by_id) == {normal_id, malformed_id}, "毒丸不得遮蔽其他条目"

        malformed = by_id[malformed_id]
        assert malformed["payload"] == {"raw": "{xxxxxxxx truncated raw payload"}
        assert malformed["schema_version"] == 3
        assert malformed["truncated"] is True
        assert malformed["raw_fields"]["schema_version"] == "3"
        assert "invalid_payload" in malformed["reason"]

        normal = by_id[normal_id]
        assert normal["payload"]["flow_id"] == "f1"
        assert normal["schema_version"] == 1
        assert normal["truncated"] is None

    def test_non_json_dead_letter_payload_is_raw(self, client, redis):
        """整个信封不是 JSON：payload 以 ``{"raw": ...}`` 保底，端点仍 200。"""
        redis.xadd(DLQ_KEY, {"payload": "not-json {broken"})

        assert client.get("/api/queues/dlq", headers=_headers()).status_code == 200
        stream = _dlq_stream(client)
        assert stream["entries"][0]["payload"] == {"raw": "not-json {broken"}

    def test_weird_scalar_fields_do_not_raise(self, client, redis):
        """数值字段被脏数据污染时按 None 容错，不因单条条目 500。"""
        envelope = {
            "reason": "manual",
            "payload": {"type": "start"},
            "delivery_count": "five",
            "dead_lettered_at": "yesterday",
            "schema_version": "vNext",
        }
        redis.xadd(DLQ_KEY, {"payload": json.dumps(envelope)})

        resp = client.get("/api/queues/dlq", headers=_headers())
        assert resp.status_code == 200
        entry = _dlq_stream(client)["entries"][0]
        assert entry["delivery_count"] is None
        assert entry["dead_lettered_at"] is None
        assert entry["schema_version"] is None


class TestParseEntryUnit:
    def test_payload_shape_tolerates_every_non_dict(self):
        assert queues_api._payload_shape({"a": 1}) == {"a": 1}
        assert queues_api._payload_shape("raw text") == {"raw": "raw text"}
        assert queues_api._payload_shape(123) == {"raw": "123"}
        assert queues_api._payload_shape(None) == {}
        assert queues_api._payload_shape("") == {}

    def test_parse_never_raises_on_binary_garbage(self):
        entry = queues_api._parse_dlq_entry(b"1-0", {b"payload": b"\xff\xfe"})
        assert entry.dlq_id == "1-0"
        assert "raw" in entry.payload
