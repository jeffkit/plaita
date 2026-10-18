"""plaita#188：http_callback 认领必须是两阶段（占位 → 发布成功才删除）.

历史缺陷：认领走 Lua ``GET+DEL`` 一体。resume 事件发布瞬断时注册记录已经
删除且没有回滚，外部回调提供方的下一次重试永远得到「回调路径未注册」——
挂起执行失醒（与 #35 同型：认领/删除先于可恢复的投递）。

修复后：
1. 认领 = ``GET`` + ``SET claim NX PX``（记录本体不动）；
2. 发布成功 → DEL 记录 + DEL 占位（并发仍恰一实例触发）；
3. 发布失败 → 只 DEL 占位，记录保持可认领，重试能成功且只触发一次。
"""
from __future__ import annotations

import asyncio
import unittest

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis

from plaita.event.memory import InMemoryEventBus
from plaita.server.services import http_callback_service as hcs
from plaita.server.services.http_callback_service import HttpCallbackService

PATH = "/callback/w35-188/cb1/e1"


def _task(path: str = PATH):
    return {
        "type": "http_callback",
        "node_id": "cb1",
        "execution_id": "e1",
        "flow_id": "f1",
        "event_type": "http_callback",
        "callback_config": {"path": path, "method": "POST"},
        "response_config": {},
    }


def _run(coro):
    return asyncio.run(coro)


class HttpCallbackClaimRedisTest(unittest.TestCase):
    def setUp(self):
        self.redis = fakeredis.FakeRedis(decode_responses=True)
        self.svc = HttpCallbackService(InMemoryEventBus(), {}, redis_client=self.redis)

    def test_publish_failure_keeps_record_then_retry_fires_once(self):
        assert _run(self.svc.handle_task(_task()))
        record_key = self.svc._record_key(PATH)
        claim_key = self.svc._claim_key(PATH)

        real_publish = self.redis.publish
        broken = {"n": 0}

        def flaky_publish(*args, **kwargs):
            broken["n"] += 1
            raise ConnectionError("redis publish down")

        self.redis.publish = flaky_publish
        first = _run(self.svc.handle_callback_request(PATH, {"ok": True}))
        self.assertEqual(first.get("status"), "error", "发布失败必须返回可重试的错误")
        self.assertEqual(
            int(self.redis.exists(record_key)), 1,
            "发布失败后注册记录被删掉 ⇒ 外部重试永远『未注册』",
        )
        self.assertEqual(
            int(self.redis.exists(claim_key)), 0,
            "发布失败必须释放处理中占位，否则 TTL 内无法重试",
        )

        self.redis.publish = real_publish
        published = []
        real_trigger = self.svc.trigger_event

        async def counting_trigger(event_type, event_data):
            published.append(event_type)
            await real_trigger(event_type, event_data)

        self.svc.trigger_event = counting_trigger
        second = _run(self.svc.handle_callback_request(PATH, {"ok": True}))

        self.assertEqual(second, {"status": "success"}, "重试必须能成功触发")
        self.assertEqual(published, ["http_callback"], "事件必须恰好触发一次")
        self.assertEqual(int(self.redis.exists(record_key)), 0, "成功后记录应删除")
        self.assertEqual(int(self.redis.exists(claim_key)), 0, "成功后占位应删除")
        self.assertEqual(broken["n"], 1, "只注入一次发布失败")

    def test_two_instances_still_trigger_exactly_once(self):
        """并发语义不回退：同一 Redis 上两实例恰一实例认领成功。"""
        other = HttpCallbackService(InMemoryEventBus(), {}, redis_client=self.redis)
        assert _run(self.svc.handle_task(_task("/callback/w35-188/cb2/e2")))

        async def race():
            return await asyncio.gather(
                self.svc.handle_callback_request("/callback/w35-188/cb2/e2", {"n": 1}),
                other.handle_callback_request("/callback/w35-188/cb2/e2", {"n": 1}),
            )

        results = _run(race())
        successes = [r for r in results if r.get("status") == "success"]
        misses = [r for r in results if r.get("message") == hcs.CALLBACK_FAILURE_MESSAGE]
        self.assertEqual(len(successes), 1, f"应恰一实例认领成功: {results}")
        self.assertEqual(len(misses), 1, f"败方必须拿到统一失败响应（Q5）: {results}")

    def test_claim_does_not_delete_before_publish(self):
        """认领本身不得删除记录（两阶段的硬约束）。"""
        assert _run(self.svc.handle_task(_task()))
        info = self.svc._claim_callback_record(PATH)
        self.assertIsInstance(info, dict)
        self.assertEqual(
            int(self.redis.exists(self.svc._record_key(PATH))), 1,
            "认领即删除 = #188 的缺陷本体",
        )
        self.svc._release_callback_claim(PATH)


class HttpCallbackClaimMemoryTest(unittest.TestCase):
    """无 redis 的进程内存回退：同一两阶段语义（不再 pop 即删）。"""

    def setUp(self):
        self.svc = HttpCallbackService(InMemoryEventBus(), {})

    def test_publish_failure_keeps_record_then_retry_fires_once(self):
        assert _run(self.svc.handle_task(_task()))

        real_trigger = self.svc.trigger_event
        calls = {"n": 0}

        async def flaky_trigger(event_type, event_data):
            calls["n"] += 1
            if calls["n"] == 1:
                raise ConnectionError("redis publish down")
            await real_trigger(event_type, event_data)

        self.svc.trigger_event = flaky_trigger
        first = _run(self.svc.handle_callback_request(PATH, {"ok": True}))
        self.assertEqual(first.get("status"), "error")
        self.assertIn(PATH, self.svc.get_registered_callbacks(), "发布失败后记录必须仍在")

        second = _run(self.svc.handle_callback_request(PATH, {"ok": True}))
        self.assertEqual(second, {"status": "success"})
        self.assertNotIn(PATH, self.svc.get_registered_callbacks())
        self.assertEqual(calls["n"], 2)


if __name__ == "__main__":
    unittest.main()
