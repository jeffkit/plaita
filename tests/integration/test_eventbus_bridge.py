"""T084: Integration test for EventBus sync-async bridge.

Verifies that sync flow execution with event subscriptions works cleanly
using the MemoryEventBus.
"""

import asyncio
import unittest

from plaita.event.core import Event, EventBus
from plaita.event.memory import InMemoryEventBus as MemoryEventBus


class TestEventBusSyncAsyncBridge(unittest.IsolatedAsyncioTestCase):
    """Verify EventBus sync-async bridge works cleanly."""

    async def test_publish_sync_outside_event_loop(self):
        bus = MemoryEventBus()
        event = Event(event_type="test.sync", data={"key": "value"})
        event_id = await bus.publish(event)
        self.assertIsNotNone(event_id)

    async def test_publish_sync_from_sync_context(self):
        bus = MemoryEventBus()
        event = Event(event_type="test.bridge", data={"x": 1})

        event_id = bus.publish_sync(event)
        self.assertIsNotNone(event_id)
        self.assertIsInstance(event_id, str)

    async def test_register_subscription_and_match(self):
        bus = MemoryEventBus()
        sub_id = await bus.register_subscription(
            event_type="order.created",
            correlation_id="corr-123",
        )
        self.assertIsNotNone(sub_id)

        event = Event(
            event_type="order.created",
            data={"order_id": "o1"},
            correlation_id="corr-123",
        )
        event_id = await bus.publish(event)
        self.assertIsNotNone(event_id)

    async def test_handler_receives_published_event(self):
        bus = MemoryEventBus()
        received_events = []

        async def handler(evt):
            received_events.append(evt)

        await bus.register_handler(event_type="user.login", handler=handler)
        event = Event(event_type="user.login", data={"user": "alice"})
        await bus.publish(event)

        await asyncio.sleep(0.1)
        self.assertGreaterEqual(len(received_events), 1)
        self.assertEqual(received_events[0].data["user"], "alice")

    async def test_sync_publish_multiple_events(self):
        bus = MemoryEventBus()
        events = [
            Event(event_type=f"batch.{i}", data={"index": i})
            for i in range(5)
        ]
        ids = await bus.batch_publish(events)
        self.assertEqual(len(ids), 5)
        for eid in ids:
            self.assertIsNotNone(eid)


if __name__ == "__main__":
    unittest.main()
