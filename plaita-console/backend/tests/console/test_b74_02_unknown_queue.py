"""B74-02 / B75-01：console 派发面的 unknown ≠ default 三态。

## 规格

- ``candidate_queue_name`` 缺失/空白/非字符串 ⇒ unknown（不是默认队列）；
- unknown 只拒绝**定向**，不拒绝**广播**（legacy default worker 仍可消费）；
- 默认 ``PLAITA_DISPATCH_UNKNOWN_QUEUE=skip``；``assume-default`` 仅在请求
  默认队列时把 unknown 纳入，并标记 ``queue_source=assumed-default`` + WARNING；
- plan / dispatch / 台账 / 指标共用同一判据。

## 负控（pre-fix 精确红）

1. ``/dispatch/plan`` 把 unknown 列为 allowed/target（本组断言 target None、
   ``queue: unknown``、``queue_source=declared``）；
2. ``dispatch_one`` 把消息定向到 unknown worker（断言广播、消息无 target 字段）；
3. ``assume-default`` 无 ``queue_source`` / 台账无字段（pre-fix 无此概念）；
4. blank/non-string 同样被当 default（参数化断言 target None）；
5. plan 与 dispatch 同判据（pre-fix 两者都定向 unknown）。
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi import Depends, FastAPI
from fakeredis import FakeRedis
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
PLAITA_ROOT = str(BACKEND_DIR.parents[1])
for _p in (str(BACKEND_DIR), PLAITA_ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from api import capacity as capacity_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import dispatcher as dispatcher_svc  # noqa: E402

from plaita.server.capacity import (  # noqa: E402
    CAPACITY_METADATA_KEY,
    DEFAULT_DISPATCH_QUEUE,
    QUEUE_SOURCE_ASSUMED_DEFAULT,
    QUEUE_SOURCE_DECLARED,
    REASON_QUEUE_MISMATCH,
    REASON_QUEUE_UNKNOWN,
    target_mismatch,
)
from plaita.server.dispatch_ledger import DEFAULT_LEDGER_KEY  # noqa: E402
from plaita.server.registry import ServiceInfo, ServiceRegistry  # noqa: E402

ADMIN_KEY = "b74-02-admin-key"
DEFAULT_QUEUE = DEFAULT_DISPATCH_QUEUE
V2_QUEUE = "plaita:flow:queue:v2"


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


def _capacity_block(**over):
    block = {
        "schema_version": 1,
        "free_disk_gib": 120.0,
        "min_free_disk_gib": 20.0,
        "disk_blocked": False,
        "active_tasks": 0,
        "concurrency": 1,
        "sampled_at": datetime.now(timezone.utc).isoformat(),
    }
    block.update(over)
    return block


def _register_worker(redis, instance_id, *, queue_name=None, capacity=True):
    metadata = {}
    if queue_name is not None:
        metadata["queue_name"] = queue_name
    if capacity:
        metadata[CAPACITY_METADATA_KEY] = json.dumps(_capacity_block())
    info = ServiceInfo(
        instance_id=instance_id,
        service_type="flow_worker",
        host=f"{instance_id}.host",
        metadata=metadata,
    )
    ServiceRegistry(redis).register(info)
    return info


@pytest.fixture()
def redis():
    return FakeRedis(decode_responses=True)


@pytest.fixture()
def prod_shadow_off(monkeypatch):
    monkeypatch.setenv("PLAITA_CONSOLE_DISPATCH_SHADOW", "0")
    monkeypatch.delenv("PLAITA_DISPATCH_UNKNOWN_QUEUE", raising=False)


@pytest.fixture()
def client(redis, monkeypatch):
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.include_router(
        capacity_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    return TestClient(app)


def _headers():
    return {"X-Admin-API-Key": ADMIN_KEY}


def _message(redis, queue, index=0):
    return json.loads(redis.xrange(queue)[index][1]["payload"])


def _ledger_payload(redis):
    raw = redis.xrange(DEFAULT_LEDGER_KEY)[-1][1]["payload"]
    return json.loads(raw)


class TestPlanUnknownQueue:
    """负控组 1/4：plan 不把 unknown 列为 allowed/target。"""

    def test_unknown_worker_is_not_allowed_or_targeted(self, redis):
        _register_worker(redis, "w-unknown", queue_name=None)
        plan = dispatcher_svc.build_dispatch_plan(redis, ["i-1"])
        assert plan["target"] is None
        assert plan["allowed"] == []
        assert plan["queue"] == DEFAULT_QUEUE
        assert plan["queue_source"] == QUEUE_SOURCE_DECLARED
        assert plan["rejected"][0]["instance_id"] == "w-unknown"
        assert plan["rejected"][0]["reason"].startswith(REASON_QUEUE_UNKNOWN)

    @pytest.mark.parametrize("raw", ["", "   ", 12, None, [], {}])
    def test_blank_and_non_string_metadata_are_unknown(self, redis, raw):
        _register_worker(redis, "w-odd", queue_name=raw)
        plan = dispatcher_svc.build_dispatch_plan(redis, ["i-1"])
        assert plan["target"] is None
        assert plan["allowed"] == []
        assert plan["rejected"][0]["reason"].startswith(REASON_QUEUE_UNKNOWN)

    def test_http_plan_reports_queue_source(self, client, redis):
        _register_worker(redis, "w-unknown", queue_name=None)
        resp = client.post(
            "/api/dispatch/plan", headers=_headers(), json={"issue_ids": ["i-1"]},
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["target"] is None
        assert body["queue_source"] == QUEUE_SOURCE_DECLARED
        assert body["rejected"][0]["reason"].startswith(REASON_QUEUE_UNKNOWN)


class TestDispatchUnknownQueue:
    """负控组 2：unknown 只拒绝定向，不拒绝广播；legacy 仍可消费。"""

    def test_dispatch_broadcasts_without_target_and_ledger_records_source(
        self, redis, prod_shadow_off, caplog,
    ):
        _register_worker(redis, "w-unknown", queue_name=None)
        with caplog.at_level(logging.WARNING, logger="plaita.server.capacity"):
            res = dispatcher_svc.dispatch_one(
                redis, "issue-unknown", queue_name=DEFAULT_QUEUE, dry_run=False,
            )
        assert res["target"] is None
        assert res["decision"] == "broadcast"
        assert res["queue"] == DEFAULT_QUEUE
        assert res["queue_source"] == QUEUE_SOURCE_DECLARED
        assert res["enqueued"] is True
        assert res["rejected"][0]["reason"].startswith(REASON_QUEUE_UNKNOWN)
        assert "w-unknown" in caplog.text

        message = _message(redis, DEFAULT_QUEUE)
        assert "target_instance_id" not in message
        # legacy default 消费者不会被目标闸拦（无目标字段 = 放行）。
        assert target_mismatch(message, "w-legacy") is None
        assert redis.xlen(DEFAULT_QUEUE) == 1

        row = _ledger_payload(redis)
        assert row["queue_source"] == QUEUE_SOURCE_DECLARED
        assert row["decision"] == "broadcast"
        assert row["queue"] == DEFAULT_QUEUE


class TestAssumeDefault:
    """负控组 3：兼容开关必须同时落 WARNING / 响应 / 台账。"""

    def test_assume_default_targets_unknown_and_marks_every_surface(
        self, redis, prod_shadow_off, monkeypatch, caplog,
    ):
        monkeypatch.setenv("PLAITA_DISPATCH_UNKNOWN_QUEUE", "assume-default")
        _register_worker(redis, "w-unknown", queue_name=None)

        plan = dispatcher_svc.build_dispatch_plan(redis, ["i-1"])
        assert plan["target"] == "w-unknown"
        assert plan["queue_source"] == QUEUE_SOURCE_ASSUMED_DEFAULT

        with caplog.at_level(logging.WARNING, logger="plaita.server.capacity"):
            res = dispatcher_svc.dispatch_one(
                redis, "issue-assumed", queue_name=DEFAULT_QUEUE, dry_run=False,
            )
        assert res["target"] == "w-unknown"
        assert res["queue_source"] == QUEUE_SOURCE_ASSUMED_DEFAULT
        assert _message(redis, DEFAULT_QUEUE)["target_instance_id"] == "w-unknown"

        row = _ledger_payload(redis)
        assert row["queue_source"] == QUEUE_SOURCE_ASSUMED_DEFAULT
        assert row["target_instance_id"] == "w-unknown"
        assert "assumed-default" in caplog.text
        assert "w-unknown" in caplog.text

    def test_assume_default_does_not_apply_to_non_default_queue(
        self, redis, prod_shadow_off, monkeypatch,
    ):
        monkeypatch.setenv("PLAITA_DISPATCH_UNKNOWN_QUEUE", "assume-default")
        _register_worker(redis, "w-unknown", queue_name=None)
        plan = dispatcher_svc.build_dispatch_plan(redis, ["i-1"], queue_name=V2_QUEUE)
        assert plan["target"] is None
        assert plan["rejected"][0]["reason"].startswith(REASON_QUEUE_UNKNOWN)


class TestKnownAndUnknownTogether:
    """负控组 5：plan 与 dispatch 同判据；known 不匹配仍是 mismatch。"""

    def test_known_worker_is_targeted_and_unknown_rejected(
        self, redis, prod_shadow_off,
    ):
        _register_worker(redis, "w-known", queue_name=DEFAULT_QUEUE)
        _register_worker(redis, "w-unknown", queue_name=None)

        plan = dispatcher_svc.build_dispatch_plan(redis, ["i-1"])
        res = dispatcher_svc.dispatch_one(
            redis, "i-1", queue_name=DEFAULT_QUEUE, dry_run=False,
        )
        assert plan["target"] == res["target"] == "w-known"
        assert plan["allowed"] == res["allowed"] == ["w-known"]
        assert plan["queue_source"] == res["queue_source"] == QUEUE_SOURCE_DECLARED
        assert {
            r["instance_id"]: r["reason"] for r in plan["rejected"]
        }["w-unknown"].startswith(REASON_QUEUE_UNKNOWN)
        assert {
            r["instance_id"]: r["reason"] for r in res["rejected"]
        }["w-unknown"].startswith(REASON_QUEUE_UNKNOWN)
        assert _message(redis, DEFAULT_QUEUE)["target_instance_id"] == "w-known"

    def test_known_cross_queue_is_still_mismatch_in_both_paths(self, redis, prod_shadow_off):
        _register_worker(redis, "w-v2", queue_name=V2_QUEUE)
        plan = dispatcher_svc.build_dispatch_plan(redis, ["i-1"], queue_name=DEFAULT_QUEUE)
        res = dispatcher_svc.dispatch_one(
            redis, "i-1", queue_name=DEFAULT_QUEUE, dry_run=True,
        )
        assert plan["target"] is None and res["target"] is None
        assert plan["rejected"][0]["reason"].startswith(REASON_QUEUE_MISMATCH)
        assert res["rejected"][0]["reason"].startswith(REASON_QUEUE_MISMATCH)
        assert REASON_QUEUE_UNKNOWN not in plan["rejected"][0]["reason"]
