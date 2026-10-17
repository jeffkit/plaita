"""plaita#195：redis_queue / kafka 的事件必须带 correlation_id，否则 EventFilter 丢弃。

历史缺陷：两个服务的所有事件都走基类 ``trigger_event``，构造
``Event(event_type=..., data=event_data)`` 时不带 ``correlation_id``；而
``EventFilter.handle_event`` 第一件事就是无 ``correlation_id`` 直接 return
（正确防线，不改）。于是队列消息真正到达后，等待 ``redis_message`` /
``kafka_message`` 的 EVENT 节点永不 resume。

契约：两个服务 override ``trigger_event``，统一走
``BaseExtendedService.publish_resume_event``（correlation_id = data.execution_id；
有 redis_client 时直发 ``plaita:events:{type}`` 并尽力落盘，无则回退
``event_bus.publish``）。普通/错误事件同一入口，因此都覆盖。
"""
from __future__ import annotations

import asyncio
import json
import unittest

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis

from plaita.event.core import Event, EventSubscription
from plaita.event.memory import InMemoryEventSubscriptionStorage
from plaita.server.event_filter import EventFilter
from plaita.server.services.kafka_queue_service import KafkaQueueService
from plaita.server.services.redis_queue_service import RedisQueueService
from plaita.server.task_queue import PAYLOAD_FIELD
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage

QUEUE = "plaita:flow:queue"
NORMAL = {"execution_id": "e1", "flow_id": "f1", "trigger_type": "redis_message", "message": "hi"}
ERROR = {
    "execution_id": "e1",
    "flow_id": "f1",
    "trigger_type": "redis_error",
    "success": False,
    "error_message": "boom",
}


class RecordingBus:
    """最小 event bus：记录 publish 的 Event。"""

    def __init__(self):
        self.events = []

    async def publish(self, event):
        self.events.append(event)

    def publish_sync(self, event_type, **data):
        self.events.append(Event(event_type=event_type, **data))


def run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _payloads(redis_client, queue=QUEUE):
    tasks = []
    for _mid, fields in redis_client.xrange(queue, min="-", max="+"):
        raw = fields.get(PAYLOAD_FIELD) or fields.get(PAYLOAD_FIELD.encode())
        if isinstance(raw, bytes):
            raw = raw.decode()
        tasks.append(json.loads(raw))
    return tasks


def _event_filter(redis_client, bus):
    execution_storage = MemoryExecutionStorage()
    execution_storage.save_execution_state(
        "e1",
        ExecutionState(
            execution_id="e1", flow_id="f1", status="suspended", context={},
        ),
    )
    subscription_storage = InMemoryEventSubscriptionStorage()
    run(subscription_storage.store_subscription(
        EventSubscription(
            event_type="redis_message", correlation_id="e1", flow_id="f1",
        )
    ))
    return EventFilter(
        execution_storage=execution_storage,
        subscription_storage=subscription_storage,
        redis_client=redis_client,
        event_bus=bus,
        queue_name=QUEUE,
        enable_subscription_timeout_checker=False,
    )


class TestServiceEventCorrelation(unittest.TestCase):
    """无 redis（回退 event_bus）：两服务普通/错误事件的 correlation_id。"""

    def test_redis_queue_normal_and_error_events_carry_correlation_id(self):
        bus = RecordingBus()
        svc = RedisQueueService(event_bus=bus)
        run(svc.trigger_event("redis_message", dict(NORMAL)))
        run(svc.trigger_event("redis_message", dict(ERROR)))
        self.assertEqual(len(bus.events), 2)
        for event, data in zip(bus.events, (NORMAL, ERROR)):
            self.assertEqual(event.correlation_id, "e1")
            self.assertEqual(event.data.get("execution_id"), "e1")
            self.assertEqual(event.event_type, "redis_message")
            self.assertEqual(event.data, data)

    def test_kafka_queue_normal_and_error_events_carry_correlation_id(self):
        bus = RecordingBus()
        svc = KafkaQueueService(event_bus=bus)
        run(svc.trigger_event("kafka_message", dict(NORMAL)))
        run(svc.trigger_event("kafka_message", dict(ERROR)))
        self.assertEqual(len(bus.events), 2)
        for event, data in zip(bus.events, (NORMAL, ERROR)):
            self.assertEqual(event.correlation_id, "e1")
            self.assertEqual(event.data.get("execution_id"), "e1")
            self.assertEqual(event.event_type, "kafka_message")
            self.assertEqual(event.data, data)


class TestServiceDirectChannel(unittest.TestCase):
    """有 redis_client：直发 plaita:events:{type} 且带 correlation_id / 落盘。"""

    def _assert_channel(self, svc, event_type):
        redis = svc._redis_client
        pubsub = redis.pubsub()
        pubsub.subscribe(f"plaita:events:{event_type}")
        run(svc.trigger_event(event_type, dict(NORMAL)))
        received = []
        for _ in range(100):
            msg = pubsub.get_message(timeout=0.02)
            if msg and msg.get("type") == "message":
                received.append(json.loads(msg["data"]))
                break
        pubsub.close()
        self.assertTrue(received, "事件未直发到 plaita:events 频道")
        self.assertEqual(received[0]["correlation_id"], "e1")
        self.assertEqual(received[0]["data"]["execution_id"], "e1")
        # 直发前 best-effort 落盘（EventReconciler 补偿面）
        self.assertTrue(
            redis.keys("plaita:event:events:*"),
            "resume 事件未尽力落盘",
        )

    def test_redis_queue_direct_channel(self):
        redis = fakeredis.FakeRedis(decode_responses=True)
        svc = RedisQueueService(event_bus=RecordingBus(), redis_client=redis)
        self._assert_channel(svc, "redis_message")

    def test_kafka_queue_direct_channel(self):
        redis = fakeredis.FakeRedis(decode_responses=True)
        svc = KafkaQueueService(event_bus=RecordingBus(), redis_client=redis)
        self._assert_channel(svc, "kafka_message")


class TestEventFilterResume(unittest.TestCase):
    """端到端：服务事件 → EventFilter → plaita:flow:queue 的 resume 任务。"""

    def test_queue_message_resumes_suspended_event_node(self):
        redis = fakeredis.FakeRedis(decode_responses=True)
        bus = RecordingBus()
        event_filter = _event_filter(redis, bus)
        svc = RedisQueueService(event_bus=bus)

        run(svc.trigger_event("redis_message", dict(NORMAL)))
        self.assertEqual(len(bus.events), 1)
        event = bus.events[0]
        self.assertEqual(event.correlation_id, "e1")

        run(event_filter.handle_event(event))
        tasks = _payloads(redis)
        self.assertEqual(len(tasks), 1, "事件未触发 resume 入队")
        self.assertEqual(tasks[0]["type"], "resume")
        self.assertEqual(tasks[0]["resume_type"], "event")
        self.assertEqual(tasks[0]["execution_id"], "e1")
        self.assertEqual(tasks[0]["flow_id"], "f1")

    def test_event_without_correlation_id_is_still_dropped(self):
        """EventFilter 的无 correlation_id 丢弃守卫保持不变（修复在服务发布侧）。"""
        redis = fakeredis.FakeRedis(decode_responses=True)
        bus = RecordingBus()
        event_filter = _event_filter(redis, bus)

        # 直接构造一个缺 correlation_id 的事件（模拟修复前的服务事件）
        event = Event(event_type="redis_message", data=dict(NORMAL))
        self.assertIsNone(event.correlation_id)
        run(event_filter.handle_event(event))
        self.assertEqual(_payloads(redis), [], "无 correlation_id 的事件不应入队")


if __name__ == "__main__":
    unittest.main()
