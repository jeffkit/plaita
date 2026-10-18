#!/usr/bin/env python3
"""plaita#188 repro / before-after probe (fakeredis, no real Redis).

    PYTHONPATH=/tmp/w35 /Users/jeff.jie/.venvs/plaita/bin/python \
        docs/proofs/data/188-repro.py

Scenario: register a callback path, then inject a transient Redis ``publish``
failure on the first callback delivery.

Before the fix the claim is ``GET+DEL``: the registration record is already
gone when publishing fails, so the provider's retry is answered with
"回调路径未注册" forever.  After the fix the claim is two-phase (processing
marker + record kept) and the retry succeeds exactly once.
"""
from __future__ import annotations

import asyncio
import json

import fakeredis

from plaita.event.memory import InMemoryEventBus
from plaita.server.services.http_callback_service import HttpCallbackService

PATH = "/callback/w35/cb1/e1"


def _task(path=PATH):
    return {
        "type": "http_callback",
        "node_id": "cb1",
        "execution_id": "e1",
        "flow_id": "f1",
        "event_type": "http_callback",
        "callback_config": {"path": path, "method": "POST"},
        "response_config": {},
    }


def run() -> dict:
    redis = fakeredis.FakeRedis(decode_responses=True)
    svc = HttpCallbackService(InMemoryEventBus(), {}, redis_client=redis)
    assert asyncio.run(svc.handle_task(_task()))
    record_key = svc._record_key(PATH)

    real_publish = redis.publish
    broken = {"count": 0}

    def flaky_publish(*args, **kwargs):
        broken["count"] += 1
        raise ConnectionError("redis publish down")

    redis.publish = flaky_publish
    first = asyncio.run(svc.handle_callback_request(PATH, {"ok": True}))
    record_after_failure = int(redis.exists(record_key))

    redis.publish = real_publish
    second = asyncio.run(svc.handle_callback_request(PATH, {"ok": True}))
    record_after_retry = int(redis.exists(record_key))

    # 并发两实例（同一 Redis）：仍必须恰好一个认领成功
    redis2 = fakeredis.FakeRedis(decode_responses=True)
    svc2 = HttpCallbackService(InMemoryEventBus(), {}, redis_client=redis2)
    svc3 = HttpCallbackService(InMemoryEventBus(), {}, redis_client=redis2)
    assert asyncio.run(svc2.handle_task(_task("/callback/w35/cb2/e2")))

    async def race():
        return await asyncio.gather(
            svc2.handle_callback_request("/callback/w35/cb2/e2", {"n": 1}),
            svc3.handle_callback_request("/callback/w35/cb2/e2", {"n": 1}),
        )

    results = asyncio.run(race())
    return {
        "plaita_file": __import__("plaita").__file__,
        "first_delivery": first,
        "record_exists_after_publish_failure": record_after_failure,
        "retry_delivery": second,
        "record_exists_after_retry": record_after_retry,
        "publish_failures_injected": broken["count"],
        "concurrent_successes": sum(1 for r in results if r.get("status") == "success"),
        "concurrent_results": results,
    }


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False, indent=2))
