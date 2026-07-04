"""Extended tests for plaita/event/core.py — covers uncovered abstract pass bodies
and default batch implementations.

Uncovered lines targeted:
- 108, 113, 122, 127: EventStorage abstract pass bodies
- 131-135: EventStorage.batch_store_events default implementation
- 145, 150, 159, 164, 175: EventSubscriptionStorage abstract pass bodies
- 179-183: EventSubscriptionStorage.batch_mark_processed default implementation
- 193, 198, 203, 209, 214: EventProcessingTracker abstract pass bodies
- 265, 285, 290, 297, 320, 325: EventBus abstract pass bodies
- 270-274: EventBus.batch_publish default implementation
"""
from __future__ import annotations

import asyncio
import unittest
from typing import Any, Callable, Dict, List, Optional, Union


from plaita.event.core import (
    Event,
    EventBus,
    EventProcessingTracker,
    EventStorage,
    EventSubscription,
    EventSubscriptionStorage,
    RetryPolicy,
)


# ---------------------------------------------------------------------------
# Minimal stub helpers
# These stubs do NOT override the default batch methods, so the core.py
# default implementations are exercised.
# ---------------------------------------------------------------------------

class _StubEventStorage(EventStorage):
    """Minimal EventStorage that delegates all abstract methods but does NOT
    override batch_store_events (so the default impl in core.py runs)."""

    def __init__(self):
        self.stored: Dict[str, Event] = {}

    async def store_event(self, event: Event) -> str:
        result = await super().store_event(event)  # line 108 (pass)
        self.stored[event.event_id] = event
        return event.event_id

    async def get_event(self, event_id: str) -> Optional[Event]:
        await super().get_event(event_id)  # line 113 (pass)
        return self.stored.get(event_id)

    async def list_events(self, **kw) -> List[Event]:
        await super().list_events(**kw)  # line 122 (pass)
        return list(self.stored.values())

    async def delete_event(self, event_id: str) -> bool:
        await super().delete_event(event_id)  # line 127 (pass)
        return self.stored.pop(event_id, None) is not None


class _StubEventSubscriptionStorage(EventSubscriptionStorage):
    """Minimal storage that hits the abstract pass bodies and does NOT
    override batch_mark_processed."""

    def __init__(self):
        self.subs: Dict[str, EventSubscription] = {}
        self.processed: Dict[str, bool] = {}

    async def store_subscription(self, sub: EventSubscription) -> str:
        await super().store_subscription(sub)  # line 145 (pass)
        self.subs[sub.subscription_id] = sub
        return sub.subscription_id

    async def get_subscription(self, sub_id: str) -> Optional[EventSubscription]:
        await super().get_subscription(sub_id)  # line 150 (pass)
        return self.subs.get(sub_id)

    async def list_subscriptions(self, **kw) -> List[EventSubscription]:
        await super().list_subscriptions(**kw)  # line 159 (pass)
        return list(self.subs.values())

    async def delete_subscription(self, sub_id: str) -> bool:
        await super().delete_subscription(sub_id)  # line 164 (pass)
        return self.subs.pop(sub_id, None) is not None

    async def mark_event_processed(self, sub_id: str, event_id: str) -> bool:
        await super().mark_event_processed(sub_id, event_id)  # line 175 (pass)
        key = f"{sub_id}:{event_id}"
        self.processed[key] = True
        return True


class _StubEventProcessingTracker(EventProcessingTracker):
    """Minimal tracker that hits all abstract pass bodies."""

    async def mark_event_processed(self, event_id: str, handler_id: str) -> bool:
        await super().mark_event_processed(event_id, handler_id)  # line 193 (pass)
        return True

    async def is_event_processed(self, event_id: str, handler_id: str) -> bool:
        await super().is_event_processed(event_id, handler_id)  # line 198 (pass)
        return False

    async def cleanup_old_records(self, max_age_seconds: int = 86400) -> int:
        await super().cleanup_old_records(max_age_seconds)  # line 203 (pass)
        return 0

    async def record_processing_attempt(self, event_id, handler_id, status, error=None):
        await super().record_processing_attempt(event_id, handler_id, status, error)  # line 209 (pass)

    async def get_processing_history(self, event_id: str) -> List[Dict[str, Any]]:
        await super().get_processing_history(event_id)  # line 214 (pass)
        return []


class _StubEventBus(EventBus):
    """Minimal EventBus that hits abstract pass bodies but does NOT override
    batch_publish (so default impl in core.py runs)."""

    def __init__(self):
        self.published: List[Event] = []

    async def publish(self, event, prevent_duplicate_consumption=True, **kwargs) -> str:
        await super().publish(event, prevent_duplicate_consumption, **kwargs)  # line 265 (pass)
        if isinstance(event, Event):
            self.published.append(event)
            return event.event_id
        return "stub-id"

    async def register_subscription(self, event_type, **kwargs) -> str:
        await super().register_subscription(event_type, **kwargs)  # line 285 (pass)
        return "sub-id"

    async def unregister_subscription(self, sub_id: str) -> bool:
        await super().unregister_subscription(sub_id)  # line 290 (pass)
        return True

    async def wait_for_event(self, event_type, timeout=None, condition=None) -> Event:
        await super().wait_for_event(event_type, timeout, condition)  # line 297 (pass)
        return Event(event_type=event_type, data={})

    async def register_handler(self, event_type=None, handler=None,
                                filter_condition=None, retry_policy=None) -> str:
        await super().register_handler(event_type, handler,  # line 320 (pass)
                                       filter_condition, retry_policy)
        return "handler-id"

    async def get_event(self, event_id: str) -> Event:
        await super().get_event(event_id)  # line 325 (pass)
        return Event(event_type="test", data={})


# ---------------------------------------------------------------------------
# EventStorage abstract pass bodies + batch_store_events default
# ---------------------------------------------------------------------------

class TestEventStorageAbstractBodies(unittest.IsolatedAsyncioTestCase):
    async def test_store_event_super_pass(self):
        """Line 108: super().store_event() is a pass (returns None)."""
        storage = _StubEventStorage()
        result = await storage.store_event(Event(event_type="t", data={}))
        self.assertIsNotNone(result)  # our impl returns event_id

    async def test_get_event_super_pass(self):
        """Line 113: super().get_event() is a pass (returns None)."""
        storage = _StubEventStorage()
        result = await storage.get_event("no-such-id")
        self.assertIsNone(result)

    async def test_list_events_super_pass(self):
        """Line 122: super().list_events() is a pass."""
        storage = _StubEventStorage()
        result = await storage.list_events()
        self.assertIsInstance(result, list)

    async def test_delete_event_super_pass(self):
        """Line 127: super().delete_event() is a pass."""
        storage = _StubEventStorage()
        result = await storage.delete_event("nonexistent")
        self.assertFalse(result)

    async def test_batch_store_events_default_impl(self):
        """Lines 131-135: default batch_store_events calls store_event per item."""
        storage = _StubEventStorage()
        events = [
            Event(event_type="t", data={"n": 1}),
            Event(event_type="t", data={"n": 2}),
            Event(event_type="t", data={"n": 3}),
        ]
        ids = await storage.batch_store_events(events)
        self.assertEqual(len(ids), 3)
        self.assertEqual(len(storage.stored), 3)

    async def test_batch_store_events_empty(self):
        """Lines 131-135: empty list returns empty."""
        storage = _StubEventStorage()
        ids = await storage.batch_store_events([])
        self.assertEqual(ids, [])


# ---------------------------------------------------------------------------
# EventSubscriptionStorage abstract pass bodies + batch_mark_processed
# ---------------------------------------------------------------------------

class TestEventSubscriptionStorageAbstractBodies(unittest.IsolatedAsyncioTestCase):
    async def test_store_subscription_super_pass(self):
        """Line 145: super().store_subscription() is a pass."""
        storage = _StubEventSubscriptionStorage()
        sub = EventSubscription(event_type="t")
        result = await storage.store_subscription(sub)
        self.assertEqual(result, sub.subscription_id)

    async def test_get_subscription_super_pass(self):
        """Line 150: super().get_subscription() is a pass."""
        storage = _StubEventSubscriptionStorage()
        result = await storage.get_subscription("no-id")
        self.assertIsNone(result)

    async def test_list_subscriptions_super_pass(self):
        """Line 159: super().list_subscriptions() is a pass."""
        storage = _StubEventSubscriptionStorage()
        result = await storage.list_subscriptions()
        self.assertIsInstance(result, list)

    async def test_delete_subscription_super_pass(self):
        """Line 164: super().delete_subscription() is a pass."""
        storage = _StubEventSubscriptionStorage()
        result = await storage.delete_subscription("no-id")
        self.assertFalse(result)

    async def test_mark_event_processed_super_pass(self):
        """Line 175: super().mark_event_processed() is a pass."""
        storage = _StubEventSubscriptionStorage()
        result = await storage.mark_event_processed("sub-1", "evt-1")
        self.assertTrue(result)

    async def test_batch_mark_processed_all_success(self):
        """Lines 179-183: default batch_mark_processed, all True → True."""
        storage = _StubEventSubscriptionStorage()
        result = await storage.batch_mark_processed("sub-1", ["e1", "e2", "e3"])
        self.assertTrue(result)

    async def test_batch_mark_processed_empty(self):
        """Lines 179-183: empty list → all([]) = True."""
        storage = _StubEventSubscriptionStorage()
        result = await storage.batch_mark_processed("sub-1", [])
        self.assertTrue(result)

    async def test_batch_mark_processed_one_fails(self):
        """Lines 179-183: one mark_event_processed returns False → False."""
        call_count = [0]

        class FailingStorage(_StubEventSubscriptionStorage):
            async def mark_event_processed(self, sub_id, event_id):
                call_count[0] += 1
                if call_count[0] == 2:
                    return False  # second call fails
                return True

        storage = FailingStorage()
        result = await storage.batch_mark_processed("sub-1", ["e1", "e2", "e3"])
        self.assertFalse(result)


# ---------------------------------------------------------------------------
# EventProcessingTracker abstract pass bodies
# ---------------------------------------------------------------------------

class TestEventProcessingTrackerAbstractBodies(unittest.IsolatedAsyncioTestCase):
    async def test_mark_event_processed_super_pass(self):
        """Line 193: super().mark_event_processed() is a pass."""
        tracker = _StubEventProcessingTracker()
        result = await tracker.mark_event_processed("evt-1", "handler-1")
        self.assertTrue(result)

    async def test_is_event_processed_super_pass(self):
        """Line 198: super().is_event_processed() is a pass."""
        tracker = _StubEventProcessingTracker()
        result = await tracker.is_event_processed("evt-1", "handler-1")
        self.assertFalse(result)

    async def test_cleanup_old_records_super_pass(self):
        """Line 203: super().cleanup_old_records() is a pass."""
        tracker = _StubEventProcessingTracker()
        result = await tracker.cleanup_old_records(3600)
        self.assertEqual(result, 0)

    async def test_record_processing_attempt_super_pass(self):
        """Line 209: super().record_processing_attempt() is a pass."""
        tracker = _StubEventProcessingTracker()
        await tracker.record_processing_attempt("evt-1", "h-1", "success")

    async def test_get_processing_history_super_pass(self):
        """Line 214: super().get_processing_history() is a pass."""
        tracker = _StubEventProcessingTracker()
        result = await tracker.get_processing_history("evt-1")
        self.assertEqual(result, [])


# ---------------------------------------------------------------------------
# EventBus abstract pass bodies + batch_publish default
# ---------------------------------------------------------------------------

class TestEventBusAbstractBodies(unittest.IsolatedAsyncioTestCase):
    async def test_publish_super_pass(self):
        """Line 265: super().publish() is a pass."""
        bus = _StubEventBus()
        evt = Event(event_type="test", data={"x": 1})
        result = await bus.publish(evt)
        self.assertIsNotNone(result)

    async def test_batch_publish_default_impl(self):
        """Lines 270-274: default batch_publish calls publish per event."""
        bus = _StubEventBus()
        events = [
            Event(event_type="t", data={"n": i}) for i in range(3)
        ]
        ids = await bus.batch_publish(events, prevent_duplicate_consumption=False)
        self.assertEqual(len(ids), 3)
        self.assertEqual(len(bus.published), 3)

    async def test_batch_publish_empty(self):
        """Lines 270-274: empty events list returns []."""
        bus = _StubEventBus()
        ids = await bus.batch_publish([])
        self.assertEqual(ids, [])

    async def test_register_subscription_super_pass(self):
        """Line 285: super().register_subscription() is a pass."""
        bus = _StubEventBus()
        result = await bus.register_subscription("test.event")
        self.assertEqual(result, "sub-id")

    async def test_unregister_subscription_super_pass(self):
        """Line 290: super().unregister_subscription() is a pass."""
        bus = _StubEventBus()
        result = await bus.unregister_subscription("sub-1")
        self.assertTrue(result)

    async def test_wait_for_event_super_pass(self):
        """Line 297: super().wait_for_event() is a pass."""
        bus = _StubEventBus()
        event = await bus.wait_for_event("test.event")
        self.assertEqual(event.event_type, "test.event")

    async def test_register_handler_super_pass(self):
        """Line 320: super().register_handler() is a pass."""
        bus = _StubEventBus()
        handler_id = await bus.register_handler(event_type="test.event")
        self.assertEqual(handler_id, "handler-id")

    async def test_get_event_super_pass(self):
        """Line 325: super().get_event() is a pass."""
        bus = _StubEventBus()
        event = await bus.get_event("evt-1")
        self.assertEqual(event.event_type, "test")


if __name__ == "__main__":
    unittest.main()
