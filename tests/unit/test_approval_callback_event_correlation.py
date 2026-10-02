"""Track C 任务1核实+回归：Approval / HttpCallback 的 resume 事件必须带 correlation_id。

历史缺陷：base_service.trigger_event 构造 Event 不带 correlation_id
（plaita/server/services/base_service.py:287-307），而 EventFilter.handle_event
开头即丢弃无 correlation_id 的事件（plaita/server/event_filter.py:92-94）——
审批完成（approval_service.py:136）与 HTTP 回调（http_callback_service.py:107）
触发的事件永远到不了挂起执行的 resume，恢复链路是断的。

delay_service 已修同一问题（plaita/server/services/delay_service.py:263-294：
override trigger_event 带 correlation_id=execution_id；有 redis 客户端时经同步
客户端直发 plaita:events:{type} 频道，无 redis 回退 event_bus.publish）。
本文件锁定另外两个服务同款行为，并锁定 base_service.trigger_event 原签名
兼容（delay_service 的 override 依赖它）。
"""
from __future__ import annotations

import asyncio
import json
import threading
import time
import unittest
from unittest.mock import AsyncMock, patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis

from plaita.event.core import Event
from plaita.event.memory import InMemoryEventBus
from plaita.server.services.approval_service import ApprovalService
from plaita.server.services.base_service import BaseExtendedService
from plaita.server.services.http_callback_service import HttpCallbackService


def _approval_task(
    approval_id="a1", execution_id="e1", node_id="approval_1", strategy="any"
):
    return {
        "approval_id": approval_id,
        "node_id": node_id,
        "execution_id": execution_id,
        "flow_id": "f1",
        "event_type": "approval_decision",
        "approval_config": {"title": "请审批"},
        "approver_config": {"approvers": ["u1", "u2"], "strategy": strategy},
    }


def _callback_task(path="/cb/1", execution_id="e1", node_id="cb_1"):
    return {
        "node_id": node_id,
        "execution_id": execution_id,
        "flow_id": "f1",
        "event_type": "http_callback_triggered",
        "callback_config": {"path": path},
    }


class MinimalService(BaseExtendedService):
    """用于锁定 base_service.trigger_event 签名兼容的最小实现。"""

    def get_service_type(self) -> str:
        return "minimal"

    def start_service(self) -> bool:
        return True

    def stop_service(self) -> bool:
        return True

    async def handle_task(self, task_config):
        return True


class TestCorrelationIdViaBus(unittest.TestCase):
    """无 redis（回退 event_bus 路径）：发布的事件必须带 correlation_id。"""

    def test_approval_decision_event_carries_correlation_id(self):
        """审批完成触发的事件 correlation_id == execution_id（修复前为 None，红）。"""
        bus = InMemoryEventBus()
        svc = ApprovalService(bus)
        loop = asyncio.new_event_loop()
        try:
            with patch.object(bus, "publish", new_callable=AsyncMock) as pub:
                loop.run_until_complete(svc.handle_task(_approval_task()))
                loop.run_until_complete(
                    svc.submit_approval_decision("a1", "u1", "approve")
                )
                self.assertEqual(pub.await_count, 1, "审批完成未触发事件")
                event = pub.await_args.args[0]
                self.assertIsInstance(event, Event)
                self.assertEqual(event.correlation_id, "e1")
                self.assertEqual(event.data.get("execution_id"), "e1")
        finally:
            loop.close()

    def test_http_callback_event_carries_correlation_id(self):
        """HTTP 回调触发的事件 correlation_id == execution_id（修复前为 None，红）。"""
        bus = InMemoryEventBus()
        svc = HttpCallbackService(bus)
        loop = asyncio.new_event_loop()
        try:
            with patch.object(bus, "publish", new_callable=AsyncMock) as pub:
                loop.run_until_complete(svc.handle_task(_callback_task()))
                loop.run_until_complete(
                    svc.handle_callback_request("/cb/1", {"ok": 1})
                )
                self.assertEqual(pub.await_count, 1, "回调触发未发布事件")
                event = pub.await_args.args[0]
                self.assertIsInstance(event, Event)
                self.assertEqual(event.correlation_id, "e1")
                self.assertEqual(event.data.get("execution_id"), "e1")
        finally:
            loop.close()

    def test_base_trigger_event_signature_stays(self):
        """base_service.trigger_event 原签名保持 (event_type, event_data)。"""
        bus = InMemoryEventBus()
        svc = MinimalService(bus)
        with patch.object(bus, "publish", new_callable=AsyncMock) as pub:
            loop = asyncio.new_event_loop()
            try:
                loop.run_until_complete(
                    BaseExtendedService.trigger_event(svc, "some_event", {"k": "v"})
                )
            finally:
                loop.close()
            self.assertEqual(pub.await_count, 1)


class TestCorrelationIdViaRedisChannel(unittest.TestCase):
    """有 redis（生产路径）：事件直发 plaita:events:{type} 频道且带 correlation_id。

    与 test_clean_c4_delay_service.TestEndToEndEvent 同款手法：订阅真实频道
    收全量 Event dump，断言顶层 correlation_id。
    """

    def setUp(self):
        self.redis = fakeredis.FakeRedis(decode_responses=True)
        self.bus = InMemoryEventBus()

    def _drain_channel(self, channel, received, timeout=5.0):
        pubsub = self.redis.pubsub()
        pubsub.subscribe(channel)

        def drain():
            deadline = time.time() + timeout
            while time.time() < deadline and not received:
                msg = pubsub.get_message(timeout=0.2)
                if msg and msg.get("type") == "message":
                    received.append(json.loads(msg["data"]))
            pubsub.close()

        t = threading.Thread(target=drain, daemon=True)
        t.start()
        return t

    def test_approval_decision_publishes_with_correlation_id(self):
        received = []
        t = self._drain_channel("plaita:events:approval_decision", received)
        svc = ApprovalService(self.bus, {}, redis_client=self.redis)
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(svc.handle_task(_approval_task()))
            loop.run_until_complete(
                svc.submit_approval_decision("a1", "u1", "approve")
            )
        finally:
            loop.close()
        t.join(timeout=6)
        self.assertTrue(received, "审批完成事件未发布到 plaita:events 频道")
        self.assertEqual(received[0]["correlation_id"], "e1")
        self.assertEqual(received[0]["data"]["execution_id"], "e1")

    def test_http_callback_publishes_with_correlation_id(self):
        received = []
        t = self._drain_channel("plaita:events:http_callback_triggered", received)
        svc = HttpCallbackService(self.bus, {}, redis_client=self.redis)
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(svc.handle_task(_callback_task()))
            loop.run_until_complete(
                svc.handle_callback_request("/cb/1", {"ok": 1})
            )
        finally:
            loop.close()
        t.join(timeout=6)
        self.assertTrue(received, "回调事件未发布到 plaita:events 频道")
        self.assertEqual(received[0]["correlation_id"], "e1")
        self.assertEqual(received[0]["data"]["execution_id"], "e1")


if __name__ == "__main__":
    unittest.main()
