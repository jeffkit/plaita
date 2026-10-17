#!/usr/bin/env python3
"""plaita#195 repro / before-after probe.

Run from the repository root (or with ``PYTHONPATH`` pointing at the tree under
test)::

    PYTHONPATH=. .venv/bin/python docs/proofs/data/195-repro.py

Checks:
1. RedisQueueService / KafkaQueueService normal + error events: the published
   Event's top-level ``correlation_id`` vs ``data.execution_id``;
2. EventFilter end-to-end: feed the service event to a filter with a suspended
   execution + matching subscription; count the resume tasks in
   ``plaita:flow:queue``;
3. guard control: a raw Event without correlation_id is still dropped.

No real Redis/Kafka broker is needed (fakeredis; fake event bus).
"""
from __future__ import annotations

import asyncio
import json
import sys

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
ERROR = {"execution_id": "e1", "flow_id": "f1", "trigger_type": "redis_error",
         "success": False, "error_message": "boom"}


class RecordingBus:
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


def _payloads(redis_client):
    tasks = []
    for _mid, fields in redis_client.xrange(QUEUE, min="-", max="+"):
        raw = fields.get(PAYLOAD_FIELD) or fields.get(PAYLOAD_FIELD.encode())
        if isinstance(raw, bytes):
            raw = raw.decode()
        tasks.append(json.loads(raw))
    return tasks


def _filter(redis_client, bus):
    execution_storage = MemoryExecutionStorage()
    execution_storage.save_execution_state(
        "e1",
        ExecutionState(execution_id="e1", flow_id="f1", status="suspended", context={}),
    )
    subscription_storage = InMemoryEventSubscriptionStorage()
    run(subscription_storage.store_subscription(
        EventSubscription(event_type="redis_message", correlation_id="e1", flow_id="f1")
    ))
    return EventFilter(
        execution_storage=execution_storage,
        subscription_storage=subscription_storage,
        redis_client=redis_client,
        event_bus=bus,
        queue_name=QUEUE,
        enable_subscription_timeout_checker=False,
    )


def _service_case(cls, event_type):
    bus = RecordingBus()
    svc = cls(event_bus=bus)
    run(svc.trigger_event(event_type, dict(NORMAL)))
    normal = bus.events[-1]
    run(svc.trigger_event(event_type, dict(ERROR)))
    error = bus.events[-1]
    return {
        "normal_correlation_id": normal.correlation_id,
        "error_correlation_id": error.correlation_id,
        "normal_execution_id": normal.data.get("execution_id"),
        "error_execution_id": error.data.get("execution_id"),
    }


def _filter_case():
    redis = fakeredis.FakeRedis(decode_responses=True)
    bus = RecordingBus()
    event_filter = _filter(redis, bus)
    svc = RedisQueueService(event_bus=bus)
    run(svc.trigger_event("redis_message", dict(NORMAL)))
    event = bus.events[-1]
    run(event_filter.handle_event(event))
    tasks = _payloads(redis)

    # guard control: raw event without correlation_id
    redis2 = fakeredis.FakeRedis(decode_responses=True)
    bus2 = RecordingBus()
    filter2 = _filter(redis2, bus2)
    raw = Event(event_type="redis_message", data=dict(NORMAL))
    run(filter2.handle_event(raw))
    control_tasks = _payloads(redis2)

    return {
        "event_correlation_id": event.correlation_id,
        "queue_tasks": tasks,
        "control_correlation_id": raw.correlation_id,
        "control_queue_tasks": control_tasks,
    }


def run_all() -> dict:
    return {
        "plaita_file": __import__("plaita").__file__,
        "python": sys.version.split()[0],
        "redis_queue": _service_case(RedisQueueService, "redis_message"),
        "kafka_queue": _service_case(KafkaQueueService, "kafka_message"),
        "filter": _filter_case(),
    }


if __name__ == "__main__":
    print(json.dumps(run_all(), ensure_ascii=False, indent=2))
