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


if __name__ == "__main__":
    unittest.main()
