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
        await bus.register_handler(
            event_type="ok.event", handler=ok_handler, retry_policy=policy
        )
        evt = _event("ok.event")
        await bus.publish(evt, prevent_duplicate_consumption=False)
        await asyncio.sleep(0.15)

        statuses = [c[2] for c in calls]
        self.assertIn("success", statuses,
                      "成功路径必须 record status='success'（杀 status 字符串变异）")
        self.assertEqual(statuses[-1], "success")

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
        await bus.register_handler(
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
        await bus.register_handler(
            event_type="exhaust.event", handler=always_fails, retry_policy=policy
        )
        await bus.publish(_event("exhaust.event"), prevent_duplicate_consumption=False)
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


if __name__ == "__main__":
    unittest.main()
