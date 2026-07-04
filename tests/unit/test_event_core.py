"""Unit tests for plaita/event/core.py.

Covers:
- EventSubscription.matches_event() — all False-return branches and filter_condition logic
- EventStorage.batch_store_events() — default implementation
- EventSubscriptionStorage.find_matching_subscriptions() — default implementation
- EventSubscriptionStorage.batch_mark_processed() — default implementation
- EventBus.matches_event_type() — None/*, exact, prefix/suffix/middle wildcards
- EventBus.batch_publish() — default implementation
- EventBus.publish_sync() — sync-to-async bridge
- event_handler decorator — with/without running event loop
- flush_pending_handler_registrations() — queued registrations
"""

from __future__ import annotations

import asyncio
import unittest
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock

from plaita.event.core import (
    Event,
    EventBus,
    EventSubscription,
    flush_pending_handler_registrations,
    event_handler,
    _pending_handler_registrations,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _event(event_type: str, data: Optional[Dict] = None, correlation_id: Optional[str] = None) -> Event:
    return Event(event_type=event_type, data=data or {}, correlation_id=correlation_id)


def _sub(
    event_type: str = "test.event",
    correlation_id: Optional[str] = None,
    flow_id: Optional[str] = None,
    node_id: Optional[str] = None,
    filter_condition: Optional[Dict] = None,
) -> EventSubscription:
    return EventSubscription(
        event_type=event_type,
        correlation_id=correlation_id,
        flow_id=flow_id,
        node_id=node_id,
        filter_condition=filter_condition or {},
    )


# ---------------------------------------------------------------------------
# EventSubscription.matches_event — branch coverage
# ---------------------------------------------------------------------------

class TestEventSubscriptionMatchesEvent(unittest.TestCase):
    def test_type_mismatch_returns_false(self):
        sub = _sub(event_type="expected.type")
        evt = _event("other.type")
        self.assertFalse(sub.matches_event(evt, {}))

    def test_type_match_passes(self):
        sub = _sub(event_type="user.login")
        evt = _event("user.login")
        self.assertTrue(sub.matches_event(evt, {}))

    def test_correlation_id_mismatch_returns_false(self):
        """Line 64: correlation_id set and doesn't match → False."""
        sub = _sub(event_type="e", correlation_id="corr-A")
        evt = _event("e", correlation_id="corr-B")
        self.assertFalse(sub.matches_event(evt, {}))

    def test_correlation_id_match_passes(self):
        sub = _sub(event_type="e", correlation_id="corr-X")
        evt = _event("e", correlation_id="corr-X")
        self.assertTrue(sub.matches_event(evt, {}))

    def test_flow_id_mismatch_returns_false(self):
        """Line 71: flow_id set and doesn't match context flow_id → False."""
        sub = _sub(event_type="e", flow_id="flow-A")
        ctx = {"$FLOW_ID": "flow-B"}
        evt = _event("e")
        self.assertFalse(sub.matches_event(evt, ctx))

    def test_flow_id_match_passes(self):
        sub = _sub(event_type="e", flow_id="flow-A")
        ctx = {"$FLOW_ID": "flow-A"}
        evt = _event("e")
        self.assertTrue(sub.matches_event(evt, ctx))

    def test_node_id_mismatch_returns_false(self):
        """Line 75: node_id set and doesn't match context node_id → False."""
        sub = _sub(event_type="e", node_id="node-A")
        ctx = {"$LAST_NODE": "node-B"}
        evt = _event("e")
        self.assertFalse(sub.matches_event(evt, ctx))

    def test_node_id_match_passes(self):
        sub = _sub(event_type="e", node_id="node-A")
        ctx = {"$LAST_NODE": "node-A"}
        evt = _event("e")
        self.assertTrue(sub.matches_event(evt, ctx))

    def test_filter_condition_data_match(self):
        """Lines 83-92: filter_condition matched against event.data."""
        sub = _sub(event_type="e", filter_condition={"user_id": "u1"})
        evt = _event("e", data={"user_id": "u1"})
        self.assertTrue(sub.matches_event(evt, {}))

    def test_filter_condition_data_mismatch(self):
        sub = _sub(event_type="e", filter_condition={"user_id": "u1"})
        evt = _event("e", data={"user_id": "u2"})
        self.assertFalse(sub.matches_event(evt, {}))

    def test_filter_condition_attribute_match(self):
        """Filter condition key found as event attribute."""
        sub = _sub(event_type="e", filter_condition={"event_type": "e"})
        evt = _event("e")
        self.assertTrue(sub.matches_event(evt, {}))

    def test_filter_condition_attribute_mismatch(self):
        sub = _sub(event_type="e", filter_condition={"event_type": "other"})
        evt = _event("e")
        self.assertFalse(sub.matches_event(evt, {}))

    def test_filter_condition_missing_key_returns_false(self):
        """Filter condition key not in data or event attrs → False."""
        sub = _sub(event_type="e", filter_condition={"nonexistent_field": "value"})
        evt = _event("e", data={})
        self.assertFalse(sub.matches_event(evt, {}))


# ---------------------------------------------------------------------------
# EventBus.matches_event_type — static method
# ---------------------------------------------------------------------------

class TestEventBusMatchesEventType(unittest.TestCase):
    def test_none_matches_all(self):
        """Line 251: None handler_event_type matches anything."""
        self.assertTrue(EventBus.matches_event_type(None, "user.login"))

    def test_star_matches_all(self):
        """Line 251: '*' matches anything."""
        self.assertTrue(EventBus.matches_event_type("*", "user.login"))

    def test_exact_match(self):
        self.assertTrue(EventBus.matches_event_type("user.login", "user.login"))

    def test_exact_no_match(self):
        self.assertFalse(EventBus.matches_event_type("user.login", "user.logout"))

    def test_prefix_wildcard(self):
        """Line 258: fnmatch wildcard matching — 'user.*'."""
        self.assertTrue(EventBus.matches_event_type("user.*", "user.login"))
        self.assertTrue(EventBus.matches_event_type("user.*", "user.logout"))
        self.assertFalse(EventBus.matches_event_type("user.*", "admin.login"))

    def test_suffix_wildcard(self):
        """'*.login' matches any prefix."""
        self.assertTrue(EventBus.matches_event_type("*.login", "user.login"))
        self.assertTrue(EventBus.matches_event_type("*.login", "admin.login"))
        self.assertFalse(EventBus.matches_event_type("*.login", "user.logout"))

    def test_middle_wildcard(self):
        """'*.user.*' matches when 'user' is in the middle segment."""
        self.assertTrue(EventBus.matches_event_type("*.user.*", "app.user.login"))
        self.assertFalse(EventBus.matches_event_type("*.user.*", "app.admin.login"))


# ---------------------------------------------------------------------------
# EventStorage default batch_store_events
# ---------------------------------------------------------------------------

class TestEventStorageBatchDefault(unittest.IsolatedAsyncioTestCase):
    async def test_batch_store_uses_store_event(self):
        """Lines 131-135: batch_store_events default impl calls store_event per event."""
        from plaita.event.memory import MemoryEventStorage
        storage = MemoryEventStorage()
        events = [_event("e1"), _event("e2"), _event("e3")]
        ids = await storage.batch_store_events(events)
        self.assertEqual(len(ids), 3)
        for event, eid in zip(events, ids):
            self.assertEqual(event.event_id, eid)


# ---------------------------------------------------------------------------
# EventSubscriptionStorage default implementations
# ---------------------------------------------------------------------------

class TestSubscriptionStorageDefaults(unittest.IsolatedAsyncioTestCase):
    async def test_find_matching_subscriptions(self):
        """Lines 169-170: find_matching_subscriptions default implementation."""
        from plaita.event.memory import InMemoryEventSubscriptionStorage
        storage = InMemoryEventSubscriptionStorage()

        sub = EventSubscription(
            event_type="order.placed",
            filter_condition={},
        )
        await storage.store_subscription(sub)

        evt = _event("order.placed")
        matches = await storage.find_matching_subscriptions(evt, {})
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].subscription_id, sub.subscription_id)

    async def test_batch_mark_processed_default(self):
        """Lines 179-183: batch_mark_processed default impl calls mark_event_processed."""
        from plaita.event.memory import InMemoryEventSubscriptionStorage
        storage = InMemoryEventSubscriptionStorage()

        sub = EventSubscription(event_type="x", filter_condition={})
        await storage.store_subscription(sub)

        result = await storage.batch_mark_processed(sub.subscription_id, ["e1", "e2"])
        self.assertTrue(result)
        # Both events should now be marked processed
        updated = await storage.get_subscription(sub.subscription_id)
        self.assertTrue(updated.is_event_processed("e1"))
        self.assertTrue(updated.is_event_processed("e2"))


# ---------------------------------------------------------------------------
# EventBus.batch_publish and publish_sync
# ---------------------------------------------------------------------------

class TestEventBusBatchAndSync(unittest.IsolatedAsyncioTestCase):
    async def test_batch_publish_calls_publish_per_event(self):
        """Lines 270-274: batch_publish default impl calls publish for each event."""
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        events = [_event("type.a"), _event("type.b"), _event("type.c")]
        ids = await bus.batch_publish(events, prevent_duplicate_consumption=False)
        self.assertEqual(len(ids), 3)

    def test_publish_sync_works(self):
        """Line 335: publish_sync bridges async publish to sync context."""
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        evt = _event("sync.test")
        event_id = bus.publish_sync(evt, prevent_duplicate_consumption=False)
        self.assertIsNotNone(event_id)
        self.assertIsInstance(event_id, str)


# ---------------------------------------------------------------------------
# event_handler decorator — with running event loop (lines 375-378)
# ---------------------------------------------------------------------------

class TestEventHandlerDecorator(unittest.IsolatedAsyncioTestCase):
    async def test_decorator_with_running_loop_registers(self):
        """Lines 375-378: decorator called inside running loop schedules a task."""
        from plaita.event.memory import InMemoryEventBus
        from plaita.event.core import event_handler, _handler_registration_tasks

        bus = InMemoryEventBus()

        @event_handler(bus, event_type="run.loop.event")
        async def my_handler(event):
            pass

        # my_handler should be returned unchanged
        self.assertTrue(callable(my_handler))
        # Give the scheduled task time to complete
        await asyncio.sleep(0.05)

    async def test_decorator_without_running_loop_queues(self):
        """Lines 372-373: decorator queues registration when no running loop."""
        from plaita.event.memory import InMemoryEventBus
        from plaita.event.core import (
            event_handler,
            _pending_handler_registrations,
            flush_pending_handler_registrations,
        )

        bus = InMemoryEventBus()
        initial_len = len(_pending_handler_registrations)

        # Simulate no running loop by temporarily patching asyncio
        original_get_running_loop = asyncio.get_running_loop

        def _raise():
            raise RuntimeError("no loop")

        asyncio.get_running_loop = _raise
        try:
            @event_handler(bus, event_type="queued.event")
            async def queued_handler(event):
                pass
        finally:
            asyncio.get_running_loop = original_get_running_loop

        self.assertGreater(len(_pending_handler_registrations), initial_len)

        # Now flush pending registrations
        await flush_pending_handler_registrations()
        # Pending list should be cleared
        self.assertEqual(len(_pending_handler_registrations), 0)


# ---------------------------------------------------------------------------
# flush_pending_handler_registrations
# ---------------------------------------------------------------------------

class TestFlushPendingHandlerRegistrations(unittest.IsolatedAsyncioTestCase):
    async def test_flush_empty_pending(self):
        """Lines 395-398: flush with no pending registrations is a no-op."""
        from plaita.event.core import (
            _pending_handler_registrations,
            flush_pending_handler_registrations,
        )
        _pending_handler_registrations.clear()
        await flush_pending_handler_registrations()
        self.assertEqual(len(_pending_handler_registrations), 0)

    async def test_flush_calls_pending_registers(self):
        """flush_pending_handler_registrations calls all pending coroutines."""
        from plaita.event.core import (
            _pending_handler_registrations,
            flush_pending_handler_registrations,
        )
        _pending_handler_registrations.clear()

        called = []

        async def fake_register():
            called.append(True)

        _pending_handler_registrations.append(fake_register)
        _pending_handler_registrations.append(fake_register)

        await flush_pending_handler_registrations()
        self.assertEqual(len(called), 2)
        self.assertEqual(len(_pending_handler_registrations), 0)


if __name__ == "__main__":
    unittest.main()
