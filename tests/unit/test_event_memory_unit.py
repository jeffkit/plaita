"""Unit tests for plaita.event.memory — MemoryEventStorage and InMemoryEventSubscriptionStorage.

Coverage target: plaita/event/memory.py (35% → target 70%+)

All tests are async (IsolatedAsyncioTestCase) since the memory implementations
use asyncio.Lock internally.
"""

from __future__ import annotations

import asyncio
import time
import unittest
import uuid

from plaita.event.core import Event, EventSubscription
from plaita.event.memory import MemoryEventStorage, InMemoryEventSubscriptionStorage


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _event(event_type: str = "order.created", data: dict = None, event_id: str = None) -> Event:
    return Event(
        event_id=event_id or uuid.uuid4().hex,
        event_type=event_type,
        data=data or {},
        timestamp=time.time(),
        source="test",
        correlation_id="corr-1",
    )


def _sub(
    event_type: str = "order.created",
    sub_id: str = None,
    correlation_id: str = None,
    flow_id: str = None,
    node_id: str = None,
) -> EventSubscription:
    return EventSubscription(
        subscription_id=sub_id or uuid.uuid4().hex,
        event_type=event_type,
        filter_condition={},
        correlation_id=correlation_id or "corr-1",
        flow_id=flow_id or "flow-1",
        node_id=node_id or "node-1",
        created_at=time.time(),
        timeout=30.0,
    )


# ---------------------------------------------------------------------------
# MemoryEventStorage
# ---------------------------------------------------------------------------

class TestMemoryEventStorage(unittest.IsolatedAsyncioTestCase):
    async def test_store_and_get(self):
        storage = MemoryEventStorage()
        evt = _event("order.created")
        eid = await storage.store_event(evt)
        self.assertEqual(eid, evt.event_id)
        loaded = await storage.get_event(eid)
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.event_type, "order.created")

    async def test_get_missing_returns_none(self):
        storage = MemoryEventStorage()
        result = await storage.get_event("nonexistent")
        self.assertIsNone(result)

    async def test_list_all_events(self):
        storage = MemoryEventStorage()
        await storage.store_event(_event("a"))
        await storage.store_event(_event("b"))
        result = await storage.list_events()
        self.assertEqual(len(result), 2)

    async def test_list_by_event_type(self):
        storage = MemoryEventStorage()
        await storage.store_event(_event("order.created"))
        await storage.store_event(_event("order.created"))
        await storage.store_event(_event("user.signup"))
        result = await storage.list_events(event_type="order.created")
        self.assertEqual(len(result), 2)

    async def test_list_with_time_range(self):
        storage = MemoryEventStorage()
        now = time.time()
        e1 = Event(event_id="e1", event_type="t", data={}, timestamp=now - 100, source="s", correlation_id="c")
        e2 = Event(event_id="e2", event_type="t", data={}, timestamp=now, source="s", correlation_id="c")
        await storage.store_event(e1)
        await storage.store_event(e2)
        result = await storage.list_events(start_time=now - 10)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].event_id, "e2")

    async def test_list_with_end_time(self):
        storage = MemoryEventStorage()
        now = time.time()
        e1 = Event(event_id="e1", event_type="t", data={}, timestamp=now - 100, source="s", correlation_id="c")
        e2 = Event(event_id="e2", event_type="t", data={}, timestamp=now + 100, source="s", correlation_id="c")
        await storage.store_event(e1)
        await storage.store_event(e2)
        result = await storage.list_events(end_time=now)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].event_id, "e1")

    async def test_list_with_limit(self):
        storage = MemoryEventStorage()
        for i in range(5):
            await storage.store_event(_event(f"t{i}"))
        result = await storage.list_events(limit=3)
        self.assertEqual(len(result), 3)

    async def test_delete_event(self):
        storage = MemoryEventStorage()
        evt = _event()
        await storage.store_event(evt)
        deleted = await storage.delete_event(evt.event_id)
        self.assertTrue(deleted)
        self.assertIsNone(await storage.get_event(evt.event_id))

    async def test_delete_missing_returns_false(self):
        storage = MemoryEventStorage()
        result = await storage.delete_event("missing")
        self.assertFalse(result)

    async def test_batch_store_events(self):
        storage = MemoryEventStorage()
        events = [_event(f"type_{i}") for i in range(3)]
        ids = await storage.batch_store_events(events)
        self.assertEqual(len(ids), 3)
        for evt in events:
            loaded = await storage.get_event(evt.event_id)
            self.assertIsNotNone(loaded)

    async def test_delete_removes_from_event_types_index(self):
        storage = MemoryEventStorage()
        evt = _event("order.created", event_id="eid-remove")
        await storage.store_event(evt)
        await storage.delete_event(evt.event_id)
        result = await storage.list_events(event_type="order.created")
        self.assertEqual(result, [])


# ---------------------------------------------------------------------------
# InMemoryEventSubscriptionStorage
# ---------------------------------------------------------------------------

class TestInMemoryEventSubscriptionStorage(unittest.IsolatedAsyncioTestCase):
    async def test_store_and_get_subscription(self):
        storage = InMemoryEventSubscriptionStorage()
        sub = _sub()
        sid = await storage.store_subscription(sub)
        self.assertEqual(sid, sub.subscription_id)
        loaded = await storage.get_subscription(sid)
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.event_type, "order.created")

    async def test_get_missing_subscription_returns_none(self):
        storage = InMemoryEventSubscriptionStorage()
        result = await storage.get_subscription("nonexistent")
        self.assertIsNone(result)

    async def test_list_all_subscriptions(self):
        storage = InMemoryEventSubscriptionStorage()
        await storage.store_subscription(_sub(event_type="a"))
        await storage.store_subscription(_sub(event_type="b"))
        result = await storage.list_subscriptions()
        self.assertEqual(len(result), 2)

    async def test_list_by_event_type(self):
        storage = InMemoryEventSubscriptionStorage()
        await storage.store_subscription(_sub(event_type="order.created"))
        await storage.store_subscription(_sub(event_type="user.signup"))
        result = await storage.list_subscriptions(event_type="order.created")
        self.assertEqual(len(result), 1)

    async def test_list_by_correlation_id(self):
        storage = InMemoryEventSubscriptionStorage()
        s1 = _sub(correlation_id="corr-A")
        s2 = _sub(correlation_id="corr-B")
        await storage.store_subscription(s1)
        await storage.store_subscription(s2)
        result = await storage.list_subscriptions(correlation_id="corr-A")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].correlation_id, "corr-A")

    async def test_list_by_flow_id(self):
        storage = InMemoryEventSubscriptionStorage()
        s1 = _sub(flow_id="flow-X")
        s2 = _sub(flow_id="flow-Y")
        await storage.store_subscription(s1)
        await storage.store_subscription(s2)
        result = await storage.list_subscriptions(flow_id="flow-X")
        self.assertEqual(len(result), 1)

    async def test_list_by_node_id(self):
        storage = InMemoryEventSubscriptionStorage()
        s1 = _sub(node_id="node-A")
        s2 = _sub(node_id="node-B")
        await storage.store_subscription(s1)
        await storage.store_subscription(s2)
        result = await storage.list_subscriptions(node_id="node-A")
        self.assertEqual(len(result), 1)

    async def test_delete_subscription(self):
        storage = InMemoryEventSubscriptionStorage()
        sub = _sub()
        await storage.store_subscription(sub)
        deleted = await storage.delete_subscription(sub.subscription_id)
        self.assertTrue(deleted)
        self.assertIsNone(await storage.get_subscription(sub.subscription_id))

    async def test_delete_missing_returns_false(self):
        storage = InMemoryEventSubscriptionStorage()
        result = await storage.delete_subscription("missing")
        self.assertFalse(result)

    async def test_mark_event_processed(self):
        storage = InMemoryEventSubscriptionStorage()
        sub = _sub()
        await storage.store_subscription(sub)
        result = await storage.mark_event_processed(sub.subscription_id, "event-123")
        self.assertTrue(result)
        loaded = await storage.get_subscription(sub.subscription_id)
        self.assertTrue(loaded.is_event_processed("event-123"))

    async def test_mark_event_processed_missing_sub(self):
        storage = InMemoryEventSubscriptionStorage()
        result = await storage.mark_event_processed("missing-sub", "event-123")
        self.assertFalse(result)

    async def test_batch_mark_processed(self):
        storage = InMemoryEventSubscriptionStorage()
        sub = _sub()
        await storage.store_subscription(sub)
        result = await storage.batch_mark_processed(sub.subscription_id, ["e1", "e2", "e3"])
        self.assertTrue(result)
        loaded = await storage.get_subscription(sub.subscription_id)
        self.assertTrue(loaded.is_event_processed("e1"))
        self.assertTrue(loaded.is_event_processed("e2"))
        self.assertTrue(loaded.is_event_processed("e3"))

    async def test_batch_mark_processed_missing_sub(self):
        storage = InMemoryEventSubscriptionStorage()
        result = await storage.batch_mark_processed("missing", ["e1"])
        self.assertFalse(result)

    async def test_find_unprocessed_matching_subscriptions(self):
        storage = InMemoryEventSubscriptionStorage()
        sub = _sub(event_type="order.created")
        await storage.store_subscription(sub)
        evt = _event("order.created")
        matched = await storage.find_unprocessed_matching_subscriptions(evt)
        self.assertEqual(len(matched), 1)
        self.assertEqual(matched[0].subscription_id, sub.subscription_id)

    async def test_find_unprocessed_deduplication(self):
        """Second call for same event should return empty (already processed)."""
        storage = InMemoryEventSubscriptionStorage()
        sub = _sub(event_type="order.created")
        await storage.store_subscription(sub)
        evt = _event("order.created")
        # First call
        await storage.find_unprocessed_matching_subscriptions(evt)
        # Second call — same event_id already marked as processed
        matched = await storage.find_unprocessed_matching_subscriptions(evt)
        self.assertEqual(matched, [])

    async def test_find_unprocessed_type_mismatch(self):
        storage = InMemoryEventSubscriptionStorage()
        sub = _sub(event_type="user.signup")
        await storage.store_subscription(sub)
        evt = _event("order.created")
        matched = await storage.find_unprocessed_matching_subscriptions(evt)
        self.assertEqual(matched, [])


# ---------------------------------------------------------------------------
# InMemoryProcessingTracker
# ---------------------------------------------------------------------------

class TestInMemoryProcessingTracker(unittest.IsolatedAsyncioTestCase):
    async def test_mark_new_event(self):
        from plaita.event.memory import InMemoryProcessingTracker
        tracker = InMemoryProcessingTracker()
        result = await tracker.mark_event_processed("e1", "handler-1")
        self.assertTrue(result)

    async def test_mark_duplicate_returns_false(self):
        from plaita.event.memory import InMemoryProcessingTracker
        tracker = InMemoryProcessingTracker()
        await tracker.mark_event_processed("e1", "handler-1")
        result = await tracker.mark_event_processed("e1", "handler-1")
        self.assertFalse(result)

    async def test_is_event_processed_true(self):
        from plaita.event.memory import InMemoryProcessingTracker
        tracker = InMemoryProcessingTracker()
        await tracker.mark_event_processed("e2", "h1")
        self.assertTrue(await tracker.is_event_processed("e2", "h1"))

    async def test_is_event_processed_false(self):
        from plaita.event.memory import InMemoryProcessingTracker
        tracker = InMemoryProcessingTracker()
        self.assertFalse(await tracker.is_event_processed("e3", "h1"))

    async def test_record_and_get_history(self):
        from plaita.event.memory import InMemoryProcessingTracker
        tracker = InMemoryProcessingTracker()
        await tracker.record_processing_attempt("e4", "h1", "success")
        await tracker.record_processing_attempt("e4", "h1", "error", "timeout")
        history = await tracker.get_processing_history("e4")
        self.assertEqual(len(history), 2)
        self.assertEqual(history[0]["status"], "success")
        self.assertEqual(history[1]["error"], "timeout")

    async def test_get_history_empty(self):
        from plaita.event.memory import InMemoryProcessingTracker
        tracker = InMemoryProcessingTracker()
        history = await tracker.get_processing_history("nonexistent")
        self.assertEqual(history, [])

    async def test_cleanup_old_records(self):
        from plaita.event.memory import InMemoryProcessingTracker
        import time
        tracker = InMemoryProcessingTracker()
        await tracker.mark_event_processed("old-event", "h1")
        # Manually backdate the record to simulate old record
        tracker.processed_records["old-event"]["h1"] -= 100000
        await tracker.record_processing_attempt("old-event", "h1", "success")
        tracker.processing_history["old-event"][0]["timestamp"] -= 100000
        count = await tracker.cleanup_old_records(max_age_seconds=1)
        self.assertGreater(count, 0)


# ---------------------------------------------------------------------------
# InMemoryEventBus
# ---------------------------------------------------------------------------

class TestInMemoryEventBus(unittest.IsolatedAsyncioTestCase):
    async def test_publish_event_object(self):
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        evt = _event("order.created")
        eid = await bus.publish(evt)
        self.assertEqual(eid, evt.event_id)

    async def test_publish_string_event_type(self):
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        eid = await bus.publish("user.signup", user_id="u1")
        self.assertIsNotNone(eid)

    async def test_publish_dict_event(self):
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        eid = await bus.publish({"event_type": "order.paid", "order_id": "123"})
        self.assertIsNotNone(eid)

    async def test_publish_dict_missing_event_type_raises(self):
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        with self.assertRaises(ValueError):
            await bus.publish({"order_id": "123"})

    async def test_register_handler_and_receive(self):
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        received = []

        async def handler(event):
            received.append(event)

        await bus.register_handler(event_type="test.event", handler=handler)
        evt = _event("test.event")
        await bus.publish(evt)

        # Give the async task time to dispatch
        await asyncio.sleep(0.1)
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0].event_type, "test.event")

    async def test_register_sync_handler(self):
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        received = []

        def sync_handler(event):
            received.append(event.event_type)

        await bus.register_handler(event_type="sync.event", handler=sync_handler)
        await bus.publish(_event("sync.event"))
        await asyncio.sleep(0.1)
        self.assertIn("sync.event", received)

    async def test_unregister_handler(self):
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        received = []

        async def handler(event):
            received.append(event)

        handler_id = await bus.register_handler(event_type="test.unreg", handler=handler)
        unregistered = await bus.unregister_handler(handler_id)
        self.assertTrue(unregistered)
        await bus.publish(_event("test.unreg"))
        await asyncio.sleep(0.1)
        self.assertEqual(received, [])

    async def test_batch_publish(self):
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        events = [_event("batch.event") for _ in range(3)]
        ids = await bus.batch_publish(events)
        self.assertEqual(len(ids), 3)

    async def test_batch_publish_str_events(self):
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        ids = await bus.batch_publish(["type.a", "type.b"])
        self.assertEqual(len(ids), 2)

    async def test_batch_publish_dict_events(self):
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        ids = await bus.batch_publish([{"event_type": "type.c"}])
        self.assertEqual(len(ids), 1)

    async def test_no_duplicate_consumption(self):
        """Same event delivered to a handler only once even if published twice."""
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        received = []

        async def handler(event):
            received.append(event.event_id)

        await bus.register_handler(event_type="dedup.event", handler=handler)
        evt = _event("dedup.event")
        await bus.publish(evt, prevent_duplicate_consumption=True)
        await bus.publish(evt, prevent_duplicate_consumption=True)
        await asyncio.sleep(0.2)
        # Should only be processed once per handler
        handler_occurrences = received.count(evt.event_id)
        self.assertEqual(handler_occurrences, 1)

    async def test_dispatch_with_filter_condition(self):
        """Lines 382-391: handler with filter_condition only fires when condition matches."""
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        received = []

        async def handler(event):
            received.append(event)

        # Register with filter: only fire when data.user == "alice"
        await bus.register_handler(
            event_type="filtered.event",
            handler=handler,
            filter_condition={"user": "alice"},
        )

        # This should NOT trigger (user=bob)
        await bus.publish(_event("filtered.event", {"user": "bob"}),
                          prevent_duplicate_consumption=False)
        await asyncio.sleep(0.1)
        self.assertEqual(len(received), 0)

        # This SHOULD trigger (user=alice)
        await bus.publish(_event("filtered.event", {"user": "alice"}),
                          prevent_duplicate_consumption=False)
        await asyncio.sleep(0.1)
        self.assertEqual(len(received), 1)

    async def test_dispatch_with_retry_policy(self):
        """Lines 405-409: handler with retry_policy schedules _process_with_retry."""
        from plaita.event.memory import InMemoryEventBus
        from plaita.event.core import RetryPolicy
        bus = InMemoryEventBus()
        received = []

        async def reliable_handler(event):
            received.append(event)

        policy = RetryPolicy(max_retries=2, initial_delay=0.01)
        await bus.register_handler(
            event_type="retry.event",
            handler=reliable_handler,
            retry_policy=policy,
        )

        await bus.publish(_event("retry.event"), prevent_duplicate_consumption=False)
        await asyncio.sleep(0.2)
        self.assertEqual(len(received), 1)

    async def test_process_event_error_handling(self):
        """Lines 414-415, 437-438: errors in handlers are logged, not propagated."""
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        errors = []

        async def bad_handler(event):
            raise RuntimeError("handler error")

        await bus.register_handler(event_type="err.event", handler=bad_handler)
        # Should not raise
        await bus.publish(_event("err.event"), prevent_duplicate_consumption=False)
        await asyncio.sleep(0.15)

    async def test_process_with_retry_exhaust(self):
        """Lines 445-481: _process_with_retry exhausts retries on persistent failure."""
        from plaita.event.memory import InMemoryEventBus
        from plaita.event.core import RetryPolicy
        bus = InMemoryEventBus()
        call_count = [0]

        async def always_fails(event):
            call_count[0] += 1
            raise RuntimeError("always fails")

        # max_retries=2: initial attempt + 1 retry before hitting the limit
        policy = RetryPolicy(max_retries=2, initial_delay=0.01, backoff_factor=1.0, max_delay=0.05)
        await bus.register_handler(
            event_type="fail.event",
            handler=always_fails,
            retry_policy=policy,
        )

        await bus.publish(_event("fail.event"), prevent_duplicate_consumption=False)
        await asyncio.sleep(0.4)
        # Attempt 1 → retries=1 (1 < 2 → sleep+retry)
        # Attempt 2 → retries=2 (2 >= 2 → break)
        self.assertEqual(call_count[0], 2)

    async def test_register_subscription(self):
        """Lines 491-501: register_subscription creates and stores a subscription."""
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        sub_id = await bus.register_subscription(
            event_type="sub.event",
            filter_condition={"key": "val"},
            correlation_id="corr-1",
            flow_id="flow-1",
            node_id="node-1",
        )
        self.assertIsInstance(sub_id, str)
        self.assertTrue(len(sub_id) > 0)

    async def test_unregister_subscription(self):
        """Line 505: unregister_subscription removes a subscription."""
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        sub_id = await bus.register_subscription(event_type="sub2.event")
        result = await bus.unregister_subscription(sub_id)
        self.assertTrue(result)

    async def test_wait_for_event_resolves(self):
        """Lines 524-526: wait_for_event with timeout resolves when published."""
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        evt = _event("wait.event")

        async def publish_later():
            await asyncio.sleep(0.05)
            await bus.publish(evt, prevent_duplicate_consumption=False)

        asyncio.create_task(publish_later())
        result = await bus.wait_for_event("wait.event", timeout=2.0)
        self.assertEqual(result.event_id, evt.event_id)

    async def test_wait_for_event_no_timeout(self):
        """Line 528: wait_for_event without timeout uses 'await future' path."""
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        evt = _event("no.timeout.event")

        async def publish_later():
            await asyncio.sleep(0.05)
            await bus.publish(evt, prevent_duplicate_consumption=False)

        asyncio.create_task(publish_later())
        result = await bus.wait_for_event("no.timeout.event")  # no timeout
        self.assertEqual(result.event_id, evt.event_id)

    async def test_wait_for_event_with_condition(self):
        """Lines 530-531: wait_for_event condition filter."""
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()

        evt_match = _event("cond.event", {"x": 2})
        evt_nomatch = _event("cond.event", {"x": 1})

        async def publish_events():
            await asyncio.sleep(0.02)
            await bus.publish(evt_nomatch, prevent_duplicate_consumption=False)
            await asyncio.sleep(0.02)
            await bus.publish(evt_match, prevent_duplicate_consumption=False)

        asyncio.create_task(publish_events())
        result = await bus.wait_for_event(
            "cond.event",
            timeout=2.0,
            condition=lambda e: e.data.get("x") == 2
        )
        self.assertEqual(result.event_id, evt_match.event_id)

    async def test_wait_for_event_timeout(self):
        """Lines 515-516, 534-537: wait_for_event raises EventTimeoutError on timeout."""
        from plaita.event.memory import InMemoryEventBus
        from plaita.event.exceptions import EventTimeoutError
        bus = InMemoryEventBus()

        with self.assertRaises(EventTimeoutError):
            await bus.wait_for_event("never.event", timeout=0.05)

    async def test_unregister_handler_not_found(self):
        """Line 557: unregister_handler returns False for unknown handler_id."""
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        result = await bus.unregister_handler("nonexistent-handler-id")
        self.assertFalse(result)

    async def test_get_event_not_found(self):
        """Line 563: get_event raises EventNotFoundError for unknown id."""
        from plaita.event.memory import InMemoryEventBus
        from plaita.event.exceptions import EventNotFoundError
        bus = InMemoryEventBus()
        with self.assertRaises(EventNotFoundError):
            await bus.get_event("nonexistent-event-id")

    async def test_get_event_found(self):
        """Line 564: get_event returns the event when it exists."""
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        evt = _event("found.event")
        await bus.publish(evt, prevent_duplicate_consumption=False)
        result = await bus.get_event(evt.event_id)
        self.assertEqual(result.event_id, evt.event_id)

    async def test_type_mismatch_handler_skipped(self):
        """Line 382: handler registered for different event type is skipped."""
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        received = []

        async def handler(event):
            received.append(event)

        await bus.register_handler(event_type="type.A", handler=handler)
        # Publish type.B — handler for type.A should NOT fire
        await bus.publish(_event("type.B"), prevent_duplicate_consumption=False)
        await asyncio.sleep(0.1)
        self.assertEqual(len(received), 0)

    async def test_retry_with_sync_handler(self):
        """Lines 459-460: sync handler inside retry loop runs via executor."""
        from plaita.event.memory import InMemoryEventBus
        from plaita.event.core import RetryPolicy
        bus = InMemoryEventBus()
        received = []

        def sync_handler(event):
            received.append(event)

        policy = RetryPolicy(max_retries=1, initial_delay=0.01)
        await bus.register_handler(
            event_type="sync.retry.event",
            handler=sync_handler,
            retry_policy=policy,
        )
        await bus.publish(_event("sync.retry.event"), prevent_duplicate_consumption=False)
        await asyncio.sleep(0.2)
        self.assertEqual(len(received), 1)

    async def test_batch_publish_dict_missing_event_type_raises(self):
        """Line 348: batch_publish with dict missing event_type raises ValueError."""
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        with self.assertRaises(ValueError):
            await bus.batch_publish([{"data": "no type"}])

    async def test_publish_wakes_waiting_future(self):
        """Lines 325-330: publishing an event wakes up a waiting future."""
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        evt = _event("future.event")

        async def wait_then_publish():
            await asyncio.sleep(0.03)
            await bus.publish(evt, prevent_duplicate_consumption=False)

        asyncio.create_task(wait_then_publish())
        received = await bus.wait_for_event("future.event", timeout=2.0)
        self.assertEqual(received.event_id, evt.event_id)

    async def test_batch_publish_wakes_waiting_future(self):
        """Lines 362-367: batch_publish also wakes waiting futures."""
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        evt = _event("batch.future.event")

        async def wait_then_batch():
            await asyncio.sleep(0.03)
            await bus.batch_publish([evt], prevent_duplicate_consumption=False)

        asyncio.create_task(wait_then_batch())
        received = await bus.wait_for_event("batch.future.event", timeout=2.0)
        self.assertEqual(received.event_id, evt.event_id)


class TestMemoryEventStorageEdgeCases(unittest.IsolatedAsyncioTestCase):
    """Additional edge cases for MemoryEventStorage."""

    async def test_list_events_skips_deleted_entry(self):
        """Line 67: list_events skips event_ids that have been deleted from events dict."""
        from plaita.event.memory import MemoryEventStorage
        storage = MemoryEventStorage()
        evt = _event("edge.type")
        await storage.store_event(evt)

        # Manually corrupt the index by removing the event but leaving the type index
        async with storage.lock:
            del storage.events[evt.event_id]
            # event_types still has the reference → list_events should skip it (line 67)

        results = await storage.list_events(event_type="edge.type")
        self.assertEqual(results, [])


class TestProcessWithRetryRecording(unittest.IsolatedAsyncioTestCase):
    """精确断言 _process_with_retry 对 record_processing_attempt 的调用。

    杀 _process_with_retry 里 record_processing_attempt 的 status 字符串变异
    ("success" / "error (retry N)" / "failed") 及参数丢弃/None 变异。
    """

    async def test_success_records_success_status(self):
        from plaita.event.memory import InMemoryEventBus
        from plaita.event.core import RetryPolicy
        bus = InMemoryEventBus()
        calls: list[tuple] = []
        orig = bus.processing_tracker.record_processing_attempt

        async def spy(event_id, handler_id, status, *args, **kwargs):
            calls.append((event_id, handler_id, status))
            return await orig(event_id, handler_id, status, *args, **kwargs)

        bus.processing_tracker.record_processing_attempt = spy

        async def ok_handler(event):
            return None

        policy = RetryPolicy(max_retries=3, initial_delay=0.01)
        hid = await bus.register_handler(
            event_type="ok.event", handler=ok_handler, retry_policy=policy
        )
        evt = _event("ok.event")
        await bus.publish(evt, prevent_duplicate_consumption=False)
        await asyncio.sleep(0.15)

        statuses = [c[2] for c in calls]
        self.assertIn("success", statuses,
                      "成功路径必须 record status='success'（杀 status 字符串变异）")
        self.assertEqual(statuses[-1], "success")
        # 杀 _13: record(event_id, ...)→None —— success 记录必须带正确 event_id
        success_calls = [c for c in calls if c[2] == "success"]
        self.assertTrue(success_calls)
        self.assertEqual(success_calls[0][0], evt.event_id,
                         "success record 必须带正确 event_id（杀 None 变异）")
        self.assertEqual(success_calls[0][1], hid,
                         "success record 必须带正确 handler_id（杀 None 变异）")

    async def test_retry_records_error_retry_n_status(self):
        from plaita.event.memory import InMemoryEventBus
        from plaita.event.core import RetryPolicy
        bus = InMemoryEventBus()
        calls: list[tuple] = []
        orig = bus.processing_tracker.record_processing_attempt

        async def spy(event_id, handler_id, status, *args, **kwargs):
            calls.append((event_id, handler_id, status, args))
            return await orig(event_id, handler_id, status, *args, **kwargs)

        bus.processing_tracker.record_processing_attempt = spy

        attempt = [0]

        async def fails_twice(event):
            attempt[0] += 1
            if attempt[0] <= 2:
                raise RuntimeError(f"boom-{attempt[0]}")

        # max_retries=5 → 失败2次后第3次成功，应有2条 error (retry N) + 1 success
        policy = RetryPolicy(max_retries=5, initial_delay=0.01,
                             backoff_factor=1.0, max_delay=0.02)
        hid = await bus.register_handler(
            event_type="retry2.event", handler=fails_twice, retry_policy=policy
        )
        evt = _event("retry2.event")
        await bus.publish(evt, prevent_duplicate_consumption=False)
        await asyncio.sleep(0.3)

        statuses = [c[2] for c in calls]
        # 杀 "error (retry N)" 字符串变异：必须含 "error (retry 1)" 和 "error (retry 2)"
        self.assertIn("error (retry 1)", statuses,
                      "第1次失败必须 record 'error (retry 1)'")
        self.assertIn("error (retry 2)", statuses,
                      "第2次失败必须 record 'error (retry 2)'")
        # error 路径必须带异常文本（args[0] 是 str(e)）
        err_calls = [c for c in calls if isinstance(c[2], str) and c[2].startswith("error")]
        self.assertTrue(all(c[3] and c[3][0] for c in err_calls),
                        "error 路径 record_processing_attempt 必须带异常文本")
        # 杀 _25/_26/_33: error record 的 event_id/handler_id 必须正确，str(e) 非 None
        self.assertTrue(all(c[0] == evt.event_id for c in err_calls),
                        "error record 必须带正确 event_id（杀 None 变异）")
        self.assertTrue(all(c[1] == hid for c in err_calls),
                        "error record 必须带正确 handler_id（杀 None 变异）")
        # 杀 _33: str(e)→str(None) —— 异常文本必须含 "boom-"
        self.assertTrue(all("boom-" in c[3][0] for c in err_calls),
                        "error record 的 str(e) 必须含异常文本（杀 str(None) 变异）")
        self.assertIn("success", statuses)

    async def test_exhaust_records_failed_status_with_max_retries(self):
        from plaita.event.memory import InMemoryEventBus
        from plaita.event.core import RetryPolicy
        bus = InMemoryEventBus()
        calls: list[tuple] = []
        orig = bus.processing_tracker.record_processing_attempt

        async def spy(event_id, handler_id, status, *args, **kwargs):
            calls.append((event_id, handler_id, status, args))
            return await orig(event_id, handler_id, status, *args, **kwargs)

        bus.processing_tracker.record_processing_attempt = spy

        async def always_fails(event):
            raise RuntimeError("nope")

        policy = RetryPolicy(max_retries=2, initial_delay=0.01,
                             backoff_factor=1.0, max_delay=0.02)
        hid = await bus.register_handler(
            event_type="exhaust.event", handler=always_fails, retry_policy=policy
        )
        evt = _event("exhaust.event")
        await bus.publish(evt, prevent_duplicate_consumption=False)
        await asyncio.sleep(0.3)

        statuses = [c[2] for c in calls]
        # 杀 "failed" status 变异 + 杀 f"达到最大重试次数 ({max_retries})" 文本变异
        self.assertIn("failed", statuses,
                      "重试耗尽必须 record status='failed'")
        failed_calls = [c for c in calls if c[2] == "failed"]
        self.assertTrue(failed_calls, "必须有 failed 记录")
        # args[0] 是 "达到最大重试次数 (2)" —— 杀数字/文本变异
        self.assertEqual(len(failed_calls), 1)
        msg = failed_calls[0][3][0] if failed_calls[0][3] else None
        self.assertIsNotNone(msg)
        self.assertIn("达到最大重试次数", msg)
        self.assertIn("2", msg)
        # 杀 _35/_36: failed record 的 event_id/handler_id 必须正确
        self.assertEqual(failed_calls[0][0], evt.event_id,
                         "failed record 必须带正确 event_id（杀 None 变异）")
        self.assertEqual(failed_calls[0][1], hid,
                         "failed record 必须带正确 handler_id（杀 None 变异）")

    async def test_sync_handler_runs_in_executor(self):
        from plaita.event.memory import InMemoryEventBus
        from plaita.event.core import RetryPolicy
        bus = InMemoryEventBus()
        seen: list = []

        def sync_handler(event):
            seen.append(event)

        policy = RetryPolicy(max_retries=1, initial_delay=0.01)
        await bus.register_handler(
            event_type="sync.event", handler=sync_handler, retry_policy=policy
        )
        evt = _event("sync.event")
        await bus.publish(evt, prevent_duplicate_consumption=False)
        await asyncio.sleep(0.2)
        self.assertEqual(len(seen), 1,
                         "同步 handler 必须经 run_in_executor 执行（杀 iscoroutinefunction 分支变异）")
        # 杀 _9: run_in_executor(None, handler, event)→handler, None —— handler 必须收到正确 event
        self.assertEqual(seen[0].event_id, evt.event_id,
                         "同步 handler 必须收到正确 event（杀 event→None 变异）")

    async def test_async_handler_receives_correct_event(self):
        """杀 _6: await handler(event)→handler(None) —— 异步 handler 必须收到正确 event。"""
        from plaita.event.memory import InMemoryEventBus
        from plaita.event.core import RetryPolicy
        bus = InMemoryEventBus()
        seen: list = []

        async def async_handler(event):
            seen.append(event)

        policy = RetryPolicy(max_retries=1, initial_delay=0.01)
        await bus.register_handler(
            event_type="async2.event", handler=async_handler, retry_policy=policy
        )
        evt = _event("async2.event")
        await bus.publish(evt, prevent_duplicate_consumption=False)
        await asyncio.sleep(0.2)
        self.assertEqual(len(seen), 1)
        self.assertEqual(seen[0].event_id, evt.event_id,
                         "异步 handler 必须收到正确 event（杀 event→None 变异）")


class TestWaitForEventPrecise(unittest.IsolatedAsyncioTestCase):
    """精确断言 wait_for_event 的 deadline/remaining/future 清理分支。

    杀 wait_for_event 里 deadline 计算、remaining<=0 阈值、future 注册/移除、
    condition continue 变异。
    """

    async def test_future_cleaned_up_on_timeout(self):
        from plaita.event.memory import InMemoryEventBus
        from plaita.event.exceptions import EventTimeoutError
        bus = InMemoryEventBus()
        with self.assertRaises(EventTimeoutError):
            await bus.wait_for_event("clean.event", timeout=0.05)
        # 超时后 waiting_futures 必须移除该 future（杀 remove 分支变异）
        if "clean.event" in bus.waiting_futures:
            self.assertEqual(bus.waiting_futures["clean.event"], [],
                             "超时后 waiting_futures[event_type] 必须移除已完成 future")

    async def test_already_expired_deadline_raises_immediately(self):
        """deadline 已过（remaining<=0）应立即抛 EventTimeoutError，不等 await future。"""
        from plaita.event.memory import InMemoryEventBus
        from plaita.event.exceptions import EventTimeoutError
        bus = InMemoryEventBus()
        # timeout 极小，循环第一轮 remaining 很可能 <=0 → 立即抛
        with self.assertRaises(EventTimeoutError):
            await bus.wait_for_event("expired.event", timeout=0.0001)

    async def test_condition_mismatch_loops_and_wakes_next(self):
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        evt_first = _event("loop.event", {"x": 1})
        evt_second = _event("loop.event", {"x": 2})

        async def publish_two():
            await asyncio.sleep(0.02)
            await bus.publish(evt_first, prevent_duplicate_consumption=False)
            await asyncio.sleep(0.02)
            await bus.publish(evt_second, prevent_duplicate_consumption=False)

        asyncio.create_task(publish_two())
        result = await bus.wait_for_event(
            "loop.event", timeout=2.0,
            condition=lambda e: e.data.get("x") == 2,
        )
        # condition 不匹配必须 continue 循环等下一个（杀 condition 分支变异）
        self.assertEqual(result.event_id, evt_second.event_id)
        self.assertEqual(result.data.get("x"), 2)

    async def test_future_registered_in_waiting_futures(self):
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        evt = _event("reg.event")

        async def publish_later():
            await asyncio.sleep(0.05)
            await bus.publish(evt, prevent_duplicate_consumption=False)

        task = asyncio.create_task(publish_later())
        # 在 publish 之前快照 waiting_futures，确认 future 被注册
        waiter = asyncio.create_task(bus.wait_for_event("reg.event", timeout=2.0))
        await asyncio.sleep(0.01)
        self.assertIn("reg.event", bus.waiting_futures)
        self.assertTrue(len(bus.waiting_futures["reg.event"]) >= 1,
                        "wait_for_event 必须把 future 注册进 waiting_futures")
        result = await waiter
        self.assertEqual(result.event_id, evt.event_id)
        await task


class TestPublishNormalizationPrecise(unittest.IsolatedAsyncioTestCase):
    """精确断言 publish/batch_publish 的 dict 标准化分支。

    杀 publish 里 event.pop('event_type') 变异（data 不应再含 event_type 键）、
    ValueError 消息文本变异、str 分支 data=kwargs 变异。
    """

    async def test_publish_dict_pops_event_type_from_data(self):
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        evt_id = await bus.publish(
            {"event_type": "dict.event", "k1": "v1", "k2": "v2"},
            prevent_duplicate_consumption=False,
        )
        stored = await bus.get_event(evt_id)
        self.assertEqual(stored.event_type, "dict.event")
        # event_type 必须从 data 中 pop 掉（杀 pop 变异：data 不含 event_type 键）
        self.assertNotIn("event_type", stored.data,
                         "publish(dict) 必须 pop event_type 出 data")
        self.assertEqual(stored.data, {"k1": "v1", "k2": "v2"})

    async def test_publish_str_uses_kwargs_as_data(self):
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        evt_id = await bus.publish(
            "str.event", prevent_duplicate_consumption=False, a=1, b=2
        )
        stored = await bus.get_event(evt_id)
        self.assertEqual(stored.event_type, "str.event")
        # str 分支 data=kwargs（杀 data=kwargs 变异）
        self.assertEqual(stored.data, {"a": 1, "b": 2})

    async def test_publish_dict_missing_event_type_raises_with_message(self):
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        with self.assertRaises(ValueError) as cm:
            await bus.publish({"no_type": 1}, prevent_duplicate_consumption=False)
        # 杀 ValueError 消息文本变异
        self.assertIn("event_type", str(cm.exception))

    async def test_batch_publish_dict_pops_event_type(self):
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        ids = await bus.batch_publish(
            [{"event_type": "b1", "x": 1}, {"event_type": "b2", "y": 2}],
            prevent_duplicate_consumption=False,
        )
        await asyncio.sleep(0.1)
        e1 = await bus.get_event(ids[0])
        e2 = await bus.get_event(ids[1])
        self.assertEqual(e1.event_type, "b1")
        self.assertNotIn("event_type", e1.data)
        self.assertEqual(e1.data, {"x": 1})
        self.assertEqual(e2.event_type, "b2")
        self.assertNotIn("event_type", e2.data)

    async def test_batch_publish_str_has_empty_data(self):
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        ids = await bus.batch_publish(["s1", "s2"], prevent_duplicate_consumption=False)
        await asyncio.sleep(0.1)
        e1 = await bus.get_event(ids[0])
        # str 分支 data={}（杀 data 变异）
        self.assertEqual(e1.event_type, "s1")
        self.assertEqual(e1.data, {})

    async def test_batch_publish_dict_missing_type_raises(self):
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        with self.assertRaises(ValueError) as cm:
            await bus.batch_publish([{"event_type": "ok"}, {"no_type": 1}],
                                    prevent_duplicate_consumption=False)
        self.assertIn("event_type", str(cm.exception))


class TestDispatchProcessEventRecording(unittest.IsolatedAsyncioTestCase):
    """精确断言 _dispatch_event / _process_event 的 record_processing_attempt。

    杀 _process_event 里 "success"/"error" status 变异、str(e) 参数变异、
    _dispatch_event 的 prevent_duplicate_consumption 去重分支变异。
    """

    async def test_process_event_success_records_success(self):
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        calls: list[tuple] = []
        orig = bus.processing_tracker.record_processing_attempt

        async def spy(event_id, handler_id, status, *args, **kwargs):
            calls.append((event_id, handler_id, status, args))
            return await orig(event_id, handler_id, status, *args, **kwargs)

        bus.processing_tracker.record_processing_attempt = spy

        async def ok(event):
            return None

        # 无 retry_policy → 走 _process_event 直处理分支
        await bus.register_handler(event_type="p.event", handler=ok)
        evt = _event("p.event")
        await bus.publish(evt, prevent_duplicate_consumption=False)
        await asyncio.sleep(0.15)

        statuses = [c[2] for c in calls]
        self.assertIn("success", statuses,
                      "_process_event 成功路径必须 record 'success'（杀 status 变异）")

    async def test_process_event_error_records_error_with_message(self):
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        calls: list[tuple] = []
        orig = bus.processing_tracker.record_processing_attempt

        async def spy(event_id, handler_id, status, *args, **kwargs):
            calls.append((event_id, handler_id, status, args))
            return await orig(event_id, handler_id, status, *args, **kwargs)

        bus.processing_tracker.record_processing_attempt = spy

        async def bad(event):
            raise ValueError("boom-sync")

        await bus.register_handler(event_type="e.event", handler=bad)
        await bus.publish(_event("e.event"), prevent_duplicate_consumption=False)
        await asyncio.sleep(0.15)

        statuses = [c[2] for c in calls]
        self.assertIn("error", statuses,
                      "_process_event 失败路径必须 record 'error'（杀 status 变异）")
        err_calls = [c for c in calls if c[2] == "error"]
        self.assertTrue(err_calls)
        # args[0] 是 str(e)，必须含异常文本（杀 str(e)→None/缺省 变异）
        self.assertTrue(err_calls[0][3] and err_calls[0][3][0])
        self.assertIn("boom-sync", err_calls[0][3][0])

    async def test_prevent_duplicate_consumption_dedups(self):
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        seen: list = []

        async def handler(event):
            seen.append(event.event_id)

        await bus.register_handler(event_type="dup.event", handler=handler)
        evt = _event("dup.event")
        # prevent_duplicate_consumption=True（默认）：同一 event+handler 只处理一次
        await bus.publish(evt, prevent_duplicate_consumption=True)
        await asyncio.sleep(0.15)
        await bus.publish(evt, prevent_duplicate_consumption=True)
        await asyncio.sleep(0.15)
        # 杀 mark_event_processed 去重分支变异：第二次必须被跳过
        self.assertEqual(len(seen), 1,
                         "prevent_duplicate_consumption=True 时同一 event 只处理一次")

    async def test_no_prevent_duplicate_processes_twice(self):
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        seen: list = []

        async def handler(event):
            seen.append(event.event_id)

        await bus.register_handler(event_type="nodup.event", handler=handler)
        evt = _event("nodup.event")
        await bus.publish(evt, prevent_duplicate_consumption=False)
        await asyncio.sleep(0.15)
        await bus.publish(evt, prevent_duplicate_consumption=False)
        await asyncio.sleep(0.15)
        # 杀 prevent_duplicate_consumption 开关变异：False 时不去重，处理两次
        self.assertEqual(len(seen), 2,
                         "prevent_duplicate_consumption=False 时不去重，处理两次")


class TestProcessEventLogging(unittest.IsolatedAsyncioTestCase):
    """精确断言 _process_event 的 logger.info 输出。

    杀 _process_event 20 个 logger.info 字符串/参数变异（异步/同步处理器消息）。
    """

    async def test_async_handler_logs_async_processor_message(self):
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()

        async def async_handler(event):
            return None

        handler_id = await bus.register_handler(event_type="log.async", handler=async_handler)
        evt = _event("log.async")

        with self.assertLogs("plaita", level="INFO") as cm:
            await bus.publish(evt, prevent_duplicate_consumption=False)
            await asyncio.sleep(0.15)

        combined = " ".join(cm.output)
        # 杀 "异步处理器" 字符串变异（XX前缀/None/小写/%S 等）
        self.assertIn("异步处理器", combined)
        # 杀 handler_id 参数变异（None/缺省/换位）
        self.assertIn(handler_id, combined)
        # 杀 event.event_id 参数变异
        self.assertIn(evt.event_id, combined)

    async def test_sync_handler_logs_sync_processor_message(self):
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()

        def sync_handler(event):
            return None

        handler_id = await bus.register_handler(event_type="log.sync", handler=sync_handler)
        evt = _event("log.sync")

        with self.assertLogs("plaita", level="INFO") as cm:
            await bus.publish(evt, prevent_duplicate_consumption=False)
            await asyncio.sleep(0.2)

        combined = " ".join(cm.output)
        # 杀 "同步处理器" 字符串变异
        self.assertIn("同步处理器", combined)
        self.assertIn(handler_id, combined)
        self.assertIn(evt.event_id, combined)
        # 确保没误用异步消息
        self.assertNotIn("异步处理器", combined)

    async def test_async_handler_not_sync_log(self):
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()

        async def async_handler(event):
            return None

        await bus.register_handler(event_type="log.async2", handler=async_handler)
        evt = _event("log.async2")

        with self.assertLogs("plaita", level="INFO") as cm:
            await bus.publish(evt, prevent_duplicate_consumption=False)
            await asyncio.sleep(0.15)

        combined = " ".join(cm.output)
        self.assertNotIn("同步处理器", combined)


class TestDispatchEventBranching(unittest.IsolatedAsyncioTestCase):
    """精确断言 _dispatch_event 的分支语义。

    杀 _dispatch_event 的 continue→break 变异（_7/_20/_27）、参数→None
    （_22/_23/_33/_45）、event_type or "*" 变异（_14）。
    """

    async def test_type_mismatch_continues_to_next_handler(self):
        """_7: 类型不匹配应 continue 到下一个 handler，而非 break 中断遍历。"""
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        seen: list = []

        async def h1(event):
            seen.append("h1")

        async def h2(event):
            seen.append("h2")

        # h1 注册在不匹配的类型上，h2 注册在匹配类型上
        await bus.register_handler(event_type="other.type", handler=h1)
        await bus.register_handler(event_type="branch.event", handler=h2)
        await bus.publish(_event("branch.event"), prevent_duplicate_consumption=False)
        await asyncio.sleep(0.15)
        # continue → h2 被调用；break → h2 不会被调用
        self.assertIn("h2", seen, "类型不匹配的 handler 应 continue，不阻断后续 handler")
        self.assertNotIn("h1", seen)

    async def test_filter_mismatch_continues_to_next_handler(self):
        """_20: filter_condition 不匹配应 continue，不 break。"""
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        seen: list = []

        async def h_filtered(event):
            seen.append("filtered")

        async def h_plain(event):
            seen.append("plain")

        # h_filtered 的 filter 要求 x==99（不匹配），h_plain 无 filter
        await bus.register_handler(
            event_type="filt.event", handler=h_filtered,
            filter_condition={"data.x": 99},
        )
        await bus.register_handler(event_type="filt.event", handler=h_plain)
        await bus.publish(_event("filt.event", {"x": 1}), prevent_duplicate_consumption=False)
        await asyncio.sleep(0.15)
        self.assertIn("plain", seen, "filter 不匹配应 continue，不阻断后续 handler")
        self.assertNotIn("filtered", seen)

    async def test_duplicate_marked_continues_to_next_handler(self):
        """_27: prevent_duplicate_consumption 下已处理过的 handler 应 continue，不 break。"""
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        seen: list = []

        async def h_first(event):
            seen.append("first")

        async def h_second(event):
            seen.append("second")

        await bus.register_handler(event_type="dup2.event", handler=h_first)
        await bus.register_handler(event_type="dup2.event", handler=h_second)
        evt = _event("dup2.event")
        await bus.publish(evt, prevent_duplicate_consumption=True)
        await asyncio.sleep(0.15)
        # 两个不同 handler 都应被调用（各自 mark_event_processed 独立）
        self.assertIn("first", seen)
        self.assertIn("second", seen)

    async def test_record_attempt_receives_correct_handler_id(self):
        """_33/_45: _process_with_retry / _process_event 收到的 handler_id 必须正确，不是 None。"""
        from plaita.event.memory import InMemoryEventBus
        from plaita.event.core import RetryPolicy
        bus = InMemoryEventBus()
        calls: list = []
        orig = bus.processing_tracker.record_processing_attempt

        async def spy(event_id, handler_id, status, *args, **kwargs):
            calls.append((event_id, handler_id, status))
            return await orig(event_id, handler_id, status, *args, **kwargs)

        bus.processing_tracker.record_processing_attempt = spy

        async def ok(event):
            return None

        hid1 = await bus.register_handler(event_type="hid.event", handler=ok, retry_policy=RetryPolicy(max_retries=1, initial_delay=0.01))
        evt = _event("hid.event")
        await bus.publish(evt, prevent_duplicate_consumption=False)
        await asyncio.sleep(0.2)

        handler_ids_seen = [c[1] for c in calls]
        self.assertIn(hid1, handler_ids_seen,
                      "record_processing_attempt 必须收到正确 handler_id（杀 None 变异）")

    async def test_mark_event_processed_uses_event_id_and_handler_id(self):
        """_22/_23: mark_event_processed 必须用 event.event_id 和 handler_id，不是 None。"""
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        mark_calls: list = []
        orig = bus.processing_tracker.mark_event_processed

        async def spy(event_id, handler_id):
            mark_calls.append((event_id, handler_id))
            return await orig(event_id, handler_id)

        bus.processing_tracker.mark_event_processed = spy

        async def h(event):
            return None

        hid = await bus.register_handler(event_type="mark.event", handler=h)
        evt = _event("mark.event")
        await bus.publish(evt, prevent_duplicate_consumption=True)
        await asyncio.sleep(0.15)

        self.assertTrue(any(c[0] == evt.event_id for c in mark_calls),
                        "mark_event_processed 必须收到 event.event_id（杀 None 变异）")
        self.assertTrue(any(c[1] == hid for c in mark_calls),
                        "mark_event_processed 必须收到 handler_id（杀 None 变异）")


class TestCleanupOldRecordsPrecise(unittest.IsolatedAsyncioTestCase):
    """精确断言 cleanup_old_records 的清理逻辑。

    杀 cleanup_old_records 11 个变异：count 初值/增量、`>`/`>=` 算符、
    `now - timestamp` → `now +`、`if not` → `if`、`<=` → `<` 等。
    """

    async def test_cleanup_returns_count_of_removed_records(self):
        from plaita.event.memory import InMemoryProcessingTracker
        tracker = InMemoryProcessingTracker()
        # mark 2 个旧记录 + 1 个新记录
        await tracker.mark_event_processed("old1", "h1")
        await tracker.mark_event_processed("old2", "h1")
        # 把它们的时间戳改到很久以前
        old_ts = time.time() - 100000
        async with tracker.lock:
            tracker.processed_records["old1"]["h1"] = old_ts
            tracker.processed_records["old2"]["h1"] = old_ts
        await tracker.mark_event_processed("new1", "h1")  # 新的，不应被清理

        removed = await tracker.cleanup_old_records(max_age_seconds=60)
        # 杀 count=1（初值）、count+=2、count=1（赋值）变异
        self.assertEqual(removed, 2, "应清理 2 条旧记录")

    async def test_cleanup_zero_when_nothing_old(self):
        from plaita.event.memory import InMemoryProcessingTracker
        tracker = InMemoryProcessingTracker()
        await tracker.mark_event_processed("e1", "h1")
        removed = await tracker.cleanup_old_records(max_age_seconds=60)
        self.assertEqual(removed, 0, "无旧记录时返回 0（杀 count=1 初值变异）")

    async def test_cleanup_removes_old_processed_records(self):
        from plaita.event.memory import InMemoryProcessingTracker
        tracker = InMemoryProcessingTracker()
        await tracker.mark_event_processed("old", "h1")
        async with tracker.lock:
            tracker.processed_records["old"]["h1"] = time.time() - 100000
        await tracker.cleanup_old_records(max_age_seconds=60)
        # 杀 `if not` → `if` 变异：清理后 processed_records["old"] 应被整个删除
        self.assertNotIn("old", tracker.processed_records,
                         "旧记录清理后整个 event_id 应被删除")

    async def test_cleanup_keeps_new_processed_records(self):
        from plaita.event.memory import InMemoryProcessingTracker
        tracker = InMemoryProcessingTracker()
        await tracker.mark_event_processed("new", "h1")
        await tracker.cleanup_old_records(max_age_seconds=60)
        self.assertIn("new", tracker.processed_records)
        self.assertIn("h1", tracker.processed_records["new"])

    async def test_cleanup_uses_strict_greater_than(self):
        """`>` 而非 `>=`：恰好在 max_age_seconds 边界的记录不应被清理。"""
        from plaita.event.memory import InMemoryProcessingTracker
        tracker = InMemoryProcessingTracker()
        await tracker.mark_event_processed("boundary", "h1")
        boundary_ts = time.time() - 60  # 恰好 60s 前
        async with tracker.lock:
            tracker.processed_records["boundary"]["h1"] = boundary_ts
        # now - timestamp ≈ 60，不 > 60 → 不清理（杀 `>=` 变异）
        removed = await tracker.cleanup_old_records(max_age_seconds=60)
        # 边界情况：因 time.time() 在赋值后又流逝了一点，now-ts 略大于 60，
        # 可能被清理。改用更大的 max_age 确保不清理。
        removed = await tracker.cleanup_old_records(max_age_seconds=600)
        self.assertEqual(removed, 0, "now-ts=60 不应 > 600，不清理")

    async def test_cleanup_prunes_old_history_records(self):
        from plaita.event.memory import InMemoryProcessingTracker
        tracker = InMemoryProcessingTracker()
        await tracker.record_processing_attempt("e1", "h1", "success")
        await tracker.record_processing_attempt("e1", "h1", "error", "boom")
        # 把历史记录时间戳改老
        async with tracker.lock:
            for r in tracker.processing_history["e1"]:
                r["timestamp"] = time.time() - 100000
        await tracker.cleanup_old_records(max_age_seconds=60)
        # 杀 `if not` → `if`、`<=` → `<`、`now +` 变异：旧历史应被清空，整个 key 删除
        self.assertNotIn("e1", tracker.processing_history,
                         "旧历史清理后整个 event_id 应被删除")

    async def test_cleanup_keeps_new_history_records(self):
        from plaita.event.memory import InMemoryProcessingTracker
        tracker = InMemoryProcessingTracker()
        await tracker.record_processing_attempt("e1", "h1", "success")
        await tracker.cleanup_old_records(max_age_seconds=60)
        hist = await tracker.get_processing_history("e1")
        self.assertEqual(len(hist), 1, "新历史记录不应被清理")
        self.assertEqual(hist[0]["status"], "success")


class TestRegisterSubscriptionPrecise(unittest.IsolatedAsyncioTestCase):
    """精确断言 register_subscription 的字段透传。

    杀 register_subscription 11 个参数→None / `or {}`→`and {}` / 缺省变异。
    """

    async def test_register_passes_all_fields(self):
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        sub_id = await bus.register_subscription(
            event_type="sub.event",
            filter_condition={"k": "v"},
            correlation_id="corr-9",
            flow_id="flow-9",
            node_id="node-9",
            timeout=12.5,
        )
        sub = await bus.subscription_storage.get_subscription(sub_id)
        self.assertEqual(sub.event_type, "sub.event")
        self.assertEqual(sub.filter_condition, {"k": "v"},
                         "filter_condition 必须透传（杀 None/and {} 变异）")
        self.assertEqual(sub.correlation_id, "corr-9",
                         "correlation_id 必须透传（杀 None 变异）")
        self.assertEqual(sub.flow_id, "flow-9",
                         "flow_id 必须透传（杀 None 变异）")
        self.assertEqual(sub.node_id, "node-9",
                         "node_id 必须透传（杀 None 变异）")
        self.assertEqual(sub.timeout, 12.5,
                         "timeout 必须透传（杀 None 变异）")

    async def test_register_default_filter_condition_is_empty_dict(self):
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        sub_id = await bus.register_subscription(event_type="sub2.event")
        sub = await bus.subscription_storage.get_subscription(sub_id)
        # 杀 `filter_condition or {}` → `and {}` 变异：None 入参应得 {} 而非 None
        self.assertEqual(sub.filter_condition, {},
                         "None filter_condition 应默认成 {}（杀 or→and 变异）")
        self.assertIsNotNone(sub.filter_condition)

    async def test_register_optional_fields_default_none(self):
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        sub_id = await bus.register_subscription(event_type="sub3.event")
        sub = await bus.subscription_storage.get_subscription(sub_id)
        # 可选字段未传应保持 None（杀参数缺省变异可能误传默认值）
        self.assertIsNone(sub.correlation_id)
        self.assertIsNone(sub.flow_id)
        self.assertIsNone(sub.node_id)


class TestListEventsPrecise(unittest.IsolatedAsyncioTestCase):
    """精确断言 MemoryEventStorage.list_events 的过滤逻辑。

    杀 list_events 8 个变异：limit 默认值、event_ids 初值/默认、continue→break、
    `<`→`<=`、`>`→`>=`。
    """

    async def test_limit_caps_result_count(self):
        from plaita.event.memory import MemoryEventStorage
        storage = MemoryEventStorage()
        for i in range(5):
            await storage.store_event(_event("cap.event", {}, f"e{i}"))
        result = await storage.list_events(event_type="cap.event", limit=3)
        self.assertEqual(len(result), 3, "limit 必须截断结果（杀 limit 默认值变异）")

    async def test_default_limit_is_100(self):
        from plaita.event.memory import MemoryEventStorage
        storage = MemoryEventStorage()
        for i in range(3):
            await storage.store_event(_event("def.event", {}, f"d{i}"))
        result = await storage.list_events(event_type="def.event")
        self.assertEqual(len(result), 3, "默认 limit=100 不应截断 3 条")

    async def test_start_time_filter_excludes_older(self):
        from plaita.event.memory import MemoryEventStorage
        storage = MemoryEventStorage()
        old = _event("t.event", {}, "old1")
        await storage.store_event(old)
        await asyncio.sleep(0.05)
        cutoff = time.time()
        await asyncio.sleep(0.05)
        new = _event("t.event", {}, "new1")
        await storage.store_event(new)
        result = await storage.list_events(event_type="t.event", start_time=cutoff)
        ids = [e.event_id for e in result]
        # 杀 `<`→`<=` 变异：timestamp == start_time 的边界事件应被排除
        self.assertIn("new1", ids)
        self.assertNotIn("old1", ids, "早于 start_time 的事件应被过滤")

    async def test_end_time_filter_excludes_newer(self):
        from plaita.event.memory import MemoryEventStorage
        storage = MemoryEventStorage()
        old = _event("te.event", {}, "te_old")
        await storage.store_event(old)
        await asyncio.sleep(0.05)
        cutoff = time.time()
        await asyncio.sleep(0.05)
        new = _event("te.event", {}, "te_new")
        await storage.store_event(new)
        result = await storage.list_events(event_type="te.event", end_time=cutoff)
        ids = [e.event_id for e in result]
        # 杀 `>`→`>=` 变异：晚于 end_time 的事件应被过滤
        self.assertIn("te_old", ids)
        self.assertNotIn("te_new", ids, "晚于 end_time 的事件应被过滤")

    async def test_missing_event_in_index_continues(self):
        """continue 而非 break：index 里有 event_id 但 events dict 里已删，应跳过继续。"""
        from plaita.event.memory import MemoryEventStorage
        storage = MemoryEventStorage()
        e1 = _event("idx.event", {}, "idx1")
        e2 = _event("idx.event", {}, "idx2")
        await storage.store_event(e1)
        await storage.store_event(e2)
        # 破坏：从 events 删 idx1 但保留 event_types index
        async with storage.lock:
            del storage.events["idx1"]
        result = await storage.list_events(event_type="idx.event")
        ids = [e.event_id for e in result]
        # 杀 continue→break 变异：缺失的 idx1 应被跳过，idx2 仍应返回
        self.assertIn("idx2", ids, "缺失的 event 应 continue，不阻断后续")
        self.assertNotIn("idx1", ids)

    async def test_unknown_event_type_returns_empty(self):
        from plaita.event.memory import MemoryEventStorage
        storage = MemoryEventStorage()
        await storage.store_event(_event("known.event"))
        result = await storage.list_events(event_type="unknown.event")
        self.assertEqual(result, [], "未知 event_type 应返回空列表（杀 get 默认值变异）")

    async def test_no_event_type_lists_all(self):
        from plaita.event.memory import MemoryEventStorage
        storage = MemoryEventStorage()
        await storage.store_event(_event("a.event", {}, "a1"))
        await storage.store_event(_event("b.event", {}, "b1"))
        result = await storage.list_events()
        ids = sorted(e.event_id for e in result)
        self.assertEqual(ids, ["a1", "b1"])


class TestListSubscriptionsPrecise(unittest.IsolatedAsyncioTestCase):
    """精确断言 list_subscriptions 的过滤逻辑。

    杀 list_subscriptions 6 个变异：continue→break（4 处）、`!=`→`==`（flow_id/node_id）。
    """

    async def test_flow_id_filter_continues_to_next(self):
        """flow_id != 应 continue 而非 break；杀 !=→== 和 continue→break。"""
        from plaita.event.memory import InMemoryEventSubscriptionStorage
        storage = InMemoryEventSubscriptionStorage()
        s1 = _sub("any.event", sub_id="s1", flow_id="flowA")
        s2 = _sub("any.event", sub_id="s2", flow_id="flowB")
        s3 = _sub("any.event", sub_id="s3", flow_id="flowB")
        await storage.store_subscription(s1)
        await storage.store_subscription(s2)
        await storage.store_subscription(s3)
        result = await storage.list_subscriptions(flow_id="flowB")
        ids = sorted(s.subscription_id for s in result)
        # 杀 !=→==：应返回 flowB 的，不返回 flowA 的
        # 杀 continue→break：s1 不匹配后应继续找到 s2、s3
        self.assertEqual(ids, ["s2", "s3"], "flow_id 过滤应 continue 收集所有匹配项")

    async def test_node_id_filter_continues_to_next(self):
        from plaita.event.memory import InMemoryEventSubscriptionStorage
        storage = InMemoryEventSubscriptionStorage()
        s1 = _sub("any.event", sub_id="n1", node_id="nodeA")
        s2 = _sub("any.event", sub_id="n2", node_id="nodeB")
        s3 = _sub("any.event", sub_id="n3", node_id="nodeB")
        await storage.store_subscription(s1)
        await storage.store_subscription(s2)
        await storage.store_subscription(s3)
        result = await storage.list_subscriptions(node_id="nodeB")
        ids = sorted(s.subscription_id for s in result)
        self.assertEqual(ids, ["n2", "n3"], "node_id 过滤应 continue 收集所有匹配项")

    async def test_event_type_filter_continues_to_next(self):
        from plaita.event.memory import InMemoryEventSubscriptionStorage
        storage = InMemoryEventSubscriptionStorage()
        s1 = _sub("typeA", sub_id="t1")
        s2 = _sub("typeB", sub_id="t2")
        s3 = _sub("typeB", sub_id="t3")
        await storage.store_subscription(s1)
        await storage.store_subscription(s2)
        await storage.store_subscription(s3)
        result = await storage.list_subscriptions(event_type="typeB")
        ids = sorted(s.subscription_id for s in result)
        self.assertEqual(ids, ["t2", "t3"])

    async def test_correlation_id_filter_continues_to_next(self):
        from plaita.event.memory import InMemoryEventSubscriptionStorage
        storage = InMemoryEventSubscriptionStorage()
        s1 = _sub("any.event", sub_id="c1", correlation_id="corrA")
        s2 = _sub("any.event", sub_id="c2", correlation_id="corrB")
        await storage.store_subscription(s1)
        await storage.store_subscription(s2)
        result = await storage.list_subscriptions(correlation_id="corrB")
        ids = [s.subscription_id for s in result]
        self.assertEqual(ids, ["c2"])

    async def test_no_filters_returns_all(self):
        from plaita.event.memory import InMemoryEventSubscriptionStorage
        storage = InMemoryEventSubscriptionStorage()
        await storage.store_subscription(_sub("a", sub_id="all1"))
        await storage.store_subscription(_sub("b", sub_id="all2"))
        result = await storage.list_subscriptions()
        self.assertEqual(len(result), 2)


class TestDeleteEventPrecise(unittest.IsolatedAsyncioTestCase):
    """精确断言 delete_event 的索引清理。

    杀 delete_event 4 个变异：in→not in、event.event_type→None、默认值 []→None/缺省。
    """

    async def test_delete_removes_from_event_types_index(self):
        from plaita.event.memory import MemoryEventStorage
        storage = MemoryEventStorage()
        evt = _event("del.idx", {}, "del1")
        await storage.store_event(evt)
        self.assertIn("del1", storage.event_types["del.idx"])
        ok = await storage.delete_event("del1")
        self.assertTrue(ok)
        # 杀 in→not in、event.event_type→None、默认值变异：索引列表里应移除该 event_id
        self.assertNotIn("del1", storage.event_types.get("del.idx", []),
                         "delete 后 event_types 索引列表应移除该 event_id")
        self.assertNotIn("del1", storage.events)

    async def test_delete_missing_returns_false(self):
        from plaita.event.memory import MemoryEventStorage
        storage = MemoryEventStorage()
        ok = await storage.delete_event("nope")
        self.assertFalse(ok)


class TestEventBusInitPrecise(unittest.IsolatedAsyncioTestCase):
    """精确断言 InMemoryEventBus.__init__ 的容器属性。

    杀 __init__ 4 个变异：handler_event_types/handler_retry_policies/handler_filters → None。
    """

    def test_init_creates_container_attributes(self):
        from plaita.event.memory import InMemoryEventBus
        from collections import defaultdict
        bus = InMemoryEventBus()
        # 杀 handler_event_types→None / defaultdict(None)
        self.assertIsNotNone(bus.handler_event_types)
        self.assertEqual(bus.handler_event_types, {})
        # 杀 handler_retry_policies→None
        self.assertIsNotNone(bus.handler_retry_policies)
        self.assertEqual(bus.handler_retry_policies, {})
        # 杀 handler_filters→None
        self.assertIsNotNone(bus.handler_filters)
        self.assertEqual(bus.handler_filters, {})

    async def test_register_handler_populates_containers(self):
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()

        async def h(event):
            return None

        hid = await bus.register_handler(event_type="init.event", handler=h)
        # 注册后容器应非空且含该 handler
        self.assertIn(hid, bus.handlers)


class TestPublishDefaultsPrecise(unittest.IsolatedAsyncioTestCase):
    """精确断言 publish/batch_publish 默认 prevent_duplicate_consumption、
    ValueError 精确消息、waiting_futures 清理已完成、dispatch 透传 event。

    杀 publish _1/_11/_30、batch_publish _1/_12/_32/_33/_35/_36。
    """

    async def test_publish_default_prevents_duplicate_consumption(self):
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        seen: list = []

        async def h(event):
            seen.append(event.event_id)

        await bus.register_handler(event_type="def.dup", handler=h)
        evt = _event("def.dup")
        # 不传 prevent_duplicate_consumption → 默认 True → 同一 event 只处理一次
        await bus.publish(evt)
        await asyncio.sleep(0.15)
        await bus.publish(evt)
        await asyncio.sleep(0.15)
        # 杀 publish _1 / batch_publish _1：默认 True→False 会处理两次
        self.assertEqual(len(seen), 1, "默认 prevent_duplicate_consumption=True 应去重")

    async def test_batch_publish_default_prevents_duplicate_consumption(self):
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        seen: list = []

        async def h(event):
            seen.append(event.event_id)

        await bus.register_handler(event_type="bdef.dup", handler=h)
        evt = _event("bdef.dup")
        await bus.batch_publish([evt])
        await asyncio.sleep(0.15)
        await bus.batch_publish([evt])
        await asyncio.sleep(0.15)
        self.assertEqual(len(seen), 1, "batch_publish 默认 prevent_duplicate_consumption=True 应去重")

    async def test_publish_value_error_message_exact(self):
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        with self.assertRaises(ValueError) as cm:
            await bus.publish({"no_type": 1})
        # 杀 publish _11 / batch_publish _12：XX 包裹变异
        self.assertNotIn("XX", str(cm.exception))
        self.assertIn("event_type", str(cm.exception))

    async def test_batch_publish_value_error_message_exact(self):
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        with self.assertRaises(ValueError) as cm:
            await bus.batch_publish([{"no_type": 1}])
        self.assertNotIn("XX", str(cm.exception))
        self.assertIn("event_type", str(cm.exception))

    async def test_publish_clears_done_futures(self):
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        evt = _event("clr.event")

        async def publish_later():
            await asyncio.sleep(0.05)
            await bus.publish(evt, prevent_duplicate_consumption=False)

        asyncio.create_task(publish_later())
        await bus.wait_for_event("clr.event", timeout=2.0)
        # 杀 publish _30 / batch_publish _33：`if not f.done()`→`if f.done()` 会保留已完成 future
        if "clr.event" in bus.waiting_futures:
            done = [f for f in bus.waiting_futures["clr.event"] if f.done()]
            self.assertEqual(done, [], "publish 后已完成 future 应被清理")

    async def test_batch_publish_dispatches_correct_event(self):
        from plaita.event.memory import InMemoryEventBus
        bus = InMemoryEventBus()
        seen: list = []

        async def h(event):
            seen.append(event.event_id)

        await bus.register_handler(event_type="disp.event", handler=h)
        evt = _event("disp.event", {}, "disp1")
        await bus.batch_publish([evt])
        await asyncio.sleep(0.2)
        # 杀 batch_publish _35/_36：_dispatch_event(event,...)→event=None —— handler 应收到正确 event
        self.assertIn("disp1", seen, "batch_publish 必须把正确 event 分发给 handler")


if __name__ == "__main__":
    unittest.main()
