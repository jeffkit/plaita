"""A53-11 查漏（duo-a 第五十三轮）：DLQ/队列读面容错的残余格。

B1 r34 已修 ``_parse_dlq_entry`` 的 payload/数值字段容错（A49-02）。本轮补：

- ``msg_id``/字段键/``last_id`` 的严格 ``decode()`` 在非法 UTF-8 上会抛；
- ``get_queue`` 的 Stream/List 分支对「合法 JSON 但非 object」直接 ``**``
  展开会 TypeError → 500；
- console Redis 客户端 ``decode_responses=True`` 但无 ``encoding_errors``，
  非法 UTF-8 在 ``xrevrange`` 客户端层就 500（修复在 ``connect_redis``）。

负控：还原 ``_text``/非 dict 展开/``encoding_errors`` → 对应用例精确红。
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

from api import queues as queues_api  # noqa: E402
from auth import require_auth  # noqa: E402

ADMIN_KEY = "r53-view-key"
DLQ_KEY = "plaita:flow:queue:v2:dlq"
QUEUE = "plaita:flow:queue"


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


@pytest.fixture()
def client(monkeypatch):
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    redis = FakeRedis(decode_responses=False)
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.include_router(
        queues_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    return TestClient(app), redis


def _headers():
    return {"X-Admin-API-Key": ADMIN_KEY}


class TestBinaryTolerance:
    def test_parse_entry_tolerates_invalid_utf8_key_and_id(self):
        entry = queues_api._parse_dlq_entry(
            b"1-0\xff",
            {b"payl\xffoad": b"\xff\xfe"},
        )
        assert entry.dlq_id == "1-0\ufffd"
        assert entry.payload == {}  # 没有 payload 字段 → 空（不抛）

    def test_parse_entry_tolerates_invalid_utf8_value(self):
        entry = queues_api._parse_dlq_entry(
            b"1-0", {b"payload": b'{"reason":"\xff","payload":{"type":"start"}}'}
        )
        assert entry.dlq_id == "1-0"
        assert entry.payload["type"] == "start"

    def test_dlq_endpoint_with_binary_payload_does_not_500(self, client):
        http, redis = client
        redis.xadd(DLQ_KEY, {"payload": b"\xff\xfe not-json"})
        resp = http.get("/api/queues/dlq", headers=_headers())
        assert resp.status_code == 200, resp.text
        stream = next(s for s in resp.json()["streams"] if s["name"] == DLQ_KEY)
        assert stream["entries"][0]["payload"] == {"raw": "\ufffd\ufffd not-json"}


class TestQueueDetailNonObjectJson:
    def test_stream_entry_json_array_does_not_500(self, client):
        http, redis = client
        redis.xadd(QUEUE, {"payload": json.dumps([1, 2, 3])})
        resp = http.get(f"/api/queues/{QUEUE}", headers=_headers())
        assert resp.status_code == 200, resp.text
        tasks = resp.json()["tasks"]
        assert tasks[0]["data"]["raw"] == [1, 2, 3]
        assert tasks[0]["data"]["_msg_id"]

    def test_stream_entry_json_scalar_does_not_500(self, client):
        http, redis = client
        redis.xadd(QUEUE, {"payload": json.dumps(42)})
        resp = http.get(f"/api/queues/{QUEUE}", headers=_headers())
        assert resp.status_code == 200, resp.text
        assert resp.json()["tasks"][0]["data"]["raw"] == 42

    def test_list_entry_json_array_does_not_500(self, client):
        http, redis = client
        redis.rpush("plaita:delay:queue", json.dumps(["x"]))
        resp = http.get("/api/queues/plaita:delay:queue", headers=_headers())
        assert resp.status_code == 200, resp.text
        assert resp.json()["tasks"][0]["data"]["raw"] == ["x"]


class TestRedisClientEncoding:
    def test_connect_redis_uses_replacement_errors(self, monkeypatch):
        import main as main_module

        captured = {}

        class _Stub:
            def ping(self):
                return True

        def fake_from_url(url, **kwargs):
            captured["url"] = url
            captured.update(kwargs)
            return _Stub()

        monkeypatch.setattr(main_module.Redis, "from_url", fake_from_url)
        main_module.connect_redis("redis://example:6379/0")
        assert captured["decode_responses"] is True
        assert captured["encoding_errors"] == "replace"
