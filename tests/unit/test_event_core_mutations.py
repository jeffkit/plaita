"""
Mutation-killing tests for plaita/event/core.py.

Targets:
  - Event model field defaults (event_id, timestamp)
  - EventSubscription.mark_event_processed / is_event_processed
  - EventSubscription.matches_event (type check, correlation_id, flow_id,
    node_id, filter_condition, context key names)
  - EventStorage.batch_store_events (loop, append, return)
  - EventSubscriptionStorage.find_matching_subscriptions (list call args,
    filter list comprehension)
  - EventSubscriptionStorage.batch_mark_processed (loop, append, all())
  - EventBus.matches_event_type (None/"*" guard, exact match, fnmatch)
  - EventBus.publish_sync (delegation, kwargs pass-through)
  - event_handler decorator (loop detection, task tracking, pending list)
  - flush_pending_handler_registrations (clear, iterate, await)
"""
import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from plaita.event.core import (
    Event,
    EventBus,
    EventSubscription,
    EventSubscriptionStorage,
    flush_pending_handler_registrations,
    event_handler,
    _pending_handler_registrations,
    _handler_registration_tasks,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_event(event_type="order.created", data=None, correlation_id=None):
    return Event(
        event_type=event_type,
        data=data or {"amount": 100},
        correlation_id=correlation_id,
    )


def _make_sub(event_type="order.created", correlation_id=None, flow_id=None,
              node_id=None, filter_condition=None):
    return EventSubscription(
        event_type=event_type,
        correlation_id=correlation_id,
        flow_id=flow_id,
        node_id=node_id,
        filter_condition=filter_condition,
    )


# ---------------------------------------------------------------------------
# Event model
# ---------------------------------------------------------------------------

class TestEventModel(unittest.TestCase):
    def test_event_id_auto_generated(self):
        e = _make_event()
        self.assertIsInstance(e.event_id, str)
        self.assertGreater(len(e.event_id), 0)

    def test_event_ids_unique(self):
        e1 = _make_event()
        e2 = _make_event()
        self.assertNotEqual(e1.event_id, e2.event_id)

    def test_timestamp_positive(self):
        e = _make_event()
        self.assertGreater(e.timestamp, 0)

    def test_event_type_stored(self):
        e = _make_event("user.login")
        self.assertEqual(e.event_type, "user.login")

    def test_data_stored(self):
        e = _make_event(data={"key": "value"})
        self.assertEqual(e.data["key"], "value")

    def test_source_default_empty(self):
        e = _make_event()
        self.assertEqual(e.source, "")

    def test_correlation_id_default_none(self):
        e = _make_event()
        self.assertIsNone(e.correlation_id)


# ---------------------------------------------------------------------------
# EventSubscription.mark_event_processed / is_event_processed
# ---------------------------------------------------------------------------

class TestEventSubscriptionProcessed(unittest.TestCase):
    def test_mark_adds_to_processed(self):
        """Kill processed_events.add(event_id) mutations."""
        sub = _make_sub()
        sub.mark_event_processed("evt-001")
        self.assertIn("evt-001", sub.processed_events)

    def test_is_processed_true_after_mark(self):
        """Kill return event_id in ... mutation."""
        sub = _make_sub()
        sub.mark_event_processed("evt-002")
        self.assertTrue(sub.is_event_processed("evt-002"))

    def test_is_processed_false_before_mark(self):
        sub = _make_sub()
        self.assertFalse(sub.is_event_processed("evt-not-marked"))

    def test_mark_multiple_events(self):
        sub = _make_sub()
        sub.mark_event_processed("e1")
        sub.mark_event_processed("e2")
        self.assertTrue(sub.is_event_processed("e1"))
        self.assertTrue(sub.is_event_processed("e2"))


# ---------------------------------------------------------------------------
# EventSubscription.matches_event
# ---------------------------------------------------------------------------

class TestMatchesEvent(unittest.TestCase):
    def _ctx(self, prefix="$", flow_id=None, last_node=None):
        ctx = {"EXPRESS_PREFIX": prefix}
        if flow_id:
            ctx[f"{prefix}FLOW_ID"] = flow_id
        if last_node:
            ctx[f"{prefix}LAST_NODE"] = last_node
        return ctx

    def test_matching_event_type_returns_true(self):
        sub = _make_sub("order.created")
        evt = _make_event("order.created")
        self.assertTrue(sub.matches_event(evt, self._ctx()))

    def test_mismatched_event_type_returns_false(self):
        """Kill != → == mutation on event_type check."""
        sub = _make_sub("order.created")
        evt = _make_event("order.updated")
        self.assertFalse(sub.matches_event(evt, self._ctx()))

    def test_empty_event_type_matches_all(self):
        """Kill 'if self.event_type and ...' mutation."""
        sub = _make_sub("")
        evt = _make_event("anything")
        self.assertTrue(sub.matches_event(evt, self._ctx()))

    def test_correlation_id_mismatch_returns_false(self):
        """Kill correlation_id check mutation."""
        sub = _make_sub(correlation_id="corr-001")
        evt = _make_event(correlation_id="corr-002")
        self.assertFalse(sub.matches_event(evt, self._ctx()))

    def test_correlation_id_match_returns_true(self):
        sub = _make_sub(correlation_id="corr-same")
        evt = _make_event(correlation_id="corr-same")
        self.assertTrue(sub.matches_event(evt, self._ctx()))

    def test_express_prefix_used_for_context_keys(self):
        """Kill 'EXPRESS_PREFIX' → other key mutation."""
        sub = _make_sub("ev")
        evt = _make_event("ev")
        # Prefix explicitly set in context
        ctx = {"EXPRESS_PREFIX": "$", "$FLOW_ID": "fid", "$LAST_NODE": "n1"}
        self.assertTrue(sub.matches_event(evt, ctx))

    def test_flow_id_mismatch_returns_false(self):
        """Kill flow_id != condition mutation."""
        sub = _make_sub("ev", flow_id="flow-A")
        evt = _make_event("ev")
        ctx = self._ctx(flow_id="flow-B")
        self.assertFalse(sub.matches_event(evt, ctx))

    def test_flow_id_match_returns_true(self):
        sub = _make_sub("ev", flow_id="flow-A")
        evt = _make_event("ev")
        ctx = self._ctx(flow_id="flow-A")
        self.assertTrue(sub.matches_event(evt, ctx))

    def test_flow_id_not_in_context_no_mismatch(self):
        """Kill 'if self.flow_id and flow_id and ...' mutation — missing flow_id in ctx."""
        sub = _make_sub("ev", flow_id="flow-X")
        evt = _make_event("ev")
        ctx = self._ctx()  # no flow_id in context
        self.assertTrue(sub.matches_event(evt, ctx))

    def test_node_id_mismatch_returns_false(self):
        """Kill node_id != condition mutation."""
        sub = _make_sub("ev", node_id="node-A")
        evt = _make_event("ev")
        ctx = self._ctx(last_node="node-B")
        self.assertFalse(sub.matches_event(evt, ctx))

    def test_node_id_match_returns_true(self):
        sub = _make_sub("ev", node_id="node-A")
        evt = _make_event("ev")
        ctx = self._ctx(last_node="node-A")
        self.assertTrue(sub.matches_event(evt, ctx))

    def test_filter_condition_match_in_data(self):
        """Kill event.data[key] != expected_value mutation."""
        sub = _make_sub("ev", filter_condition={"status": "active"})
        evt = _make_event("ev", data={"status": "active"})
        self.assertTrue(sub.matches_event(evt, self._ctx()))

    def test_filter_condition_mismatch_in_data_returns_false(self):
        sub = _make_sub("ev", filter_condition={"status": "active"})
        evt = _make_event("ev", data={"status": "inactive"})
        self.assertFalse(sub.matches_event(evt, self._ctx()))

    def test_filter_condition_key_not_in_data_not_event_returns_false(self):
        """Kill else: return False mutation."""
        sub = _make_sub("ev", filter_condition={"nonexistent_key": "val"})
        evt = _make_event("ev", data={"other": "data"})
        self.assertFalse(sub.matches_event(evt, self._ctx()))

    def test_filter_condition_key_in_event_attr(self):
        """Kill getattr(event, key) != expected mutation."""
        sub = _make_sub("ev", filter_condition={"source": "my_service"})
        evt = Event(event_type="ev", data={}, source="my_service")
        self.assertTrue(sub.matches_event(evt, self._ctx()))

    def test_filter_condition_event_attr_mismatch_returns_false(self):
        sub = _make_sub("ev", filter_condition={"source": "other_service"})
        evt = Event(event_type="ev", data={}, source="my_service")
        self.assertFalse(sub.matches_event(evt, self._ctx()))

    def test_context_copy_does_not_modify_original(self):
        """Kill context.copy() removal mutation."""
        sub = _make_sub("ev", filter_condition={"x": "1"})
        evt = _make_event("ev", data={"x": "1"})
        ctx = self._ctx()
        ctx_keys_before = set(ctx.keys())
        sub.matches_event(evt, ctx)
        ctx_keys_after = set(ctx.keys())
        self.assertEqual(ctx_keys_before, ctx_keys_after)

    def test_default_prefix_dollar(self):
        """Kill context.get('EXPRESS_PREFIX', 'XX') mutation."""
        sub = _make_sub("ev", flow_id="fid")
        evt = _make_event("ev")
        # No EXPRESS_PREFIX in context — defaults to '$'
        ctx = {"$FLOW_ID": "fid"}
        self.assertTrue(sub.matches_event(evt, ctx))


# ---------------------------------------------------------------------------
# EventStorage.batch_store_events (concrete method)
# ---------------------------------------------------------------------------

class TestBatchStoreEvents(unittest.IsolatedAsyncioTestCase):
    async def test_batch_stores_all_events(self):
        """Kill loop/append mutations."""
        from plaita.event.core import EventStorage

        class ConcreteStorage(EventStorage):
            async def store_event(self, event):
                return f"id_{event.event_type}"
            async def get_event(self, event_id): ...
            async def list_events(self, **kw): ...
            async def delete_event(self, event_id): ...

        storage = ConcreteStorage()
        events = [_make_event("ev1"), _make_event("ev2"), _make_event("ev3")]
        result = await storage.batch_store_events(events)
        self.assertEqual(len(result), 3)
        self.assertIn("id_ev1", result)
        self.assertIn("id_ev2", result)
        self.assertIn("id_ev3", result)

    async def test_batch_stores_returns_list(self):
        from plaita.event.core import EventStorage

        class ConcreteStorage(EventStorage):
            async def store_event(self, event):
                return "eid"
            async def get_event(self, event_id): ...
            async def list_events(self, **kw): ...
            async def delete_event(self, event_id): ...

        storage = ConcreteStorage()
        result = await storage.batch_store_events([_make_event()])
        self.assertIsInstance(result, list)


# ---------------------------------------------------------------------------
# EventSubscriptionStorage.find_matching_subscriptions
# ---------------------------------------------------------------------------

class TestFindMatchingSubscriptions(unittest.IsolatedAsyncioTestCase):
    async def test_find_matching_uses_correlation_and_event_type(self):
        """Kill list_subscriptions(correlation_id=..., event_type=...) mutations."""
        from plaita.event.core import EventSubscriptionStorage

        class ConcreteStorage(EventSubscriptionStorage):
            def __init__(self):
                self.list_subs_call_kwargs = None

            async def list_subscriptions(self, **kwargs):
                self.list_subs_call_kwargs = kwargs
                return []
            async def store_subscription(self, s): ...
            async def get_subscription(self, sid): ...
            async def delete_subscription(self, sid): ...
            async def mark_event_processed(self, sid, eid): ...

        storage = ConcreteStorage()
        evt = _make_event("order.placed", correlation_id="corr-1")
        await storage.find_matching_subscriptions(evt, {})
        self.assertEqual(storage.list_subs_call_kwargs["correlation_id"], "corr-1")
        self.assertEqual(storage.list_subs_call_kwargs["event_type"], "order.placed")

    async def test_find_matching_filters_matches_event(self):
        """Kill sub for sub in ... if sub.matches_event() mutation."""
        from plaita.event.core import EventSubscriptionStorage

        matching_sub = _make_sub("ev", filter_condition={"status": "ok"})
        non_matching_sub = _make_sub("ev", filter_condition={"status": "fail"})

        class ConcreteStorage(EventSubscriptionStorage):
            async def list_subscriptions(self, **kw):
                return [matching_sub, non_matching_sub]
            async def store_subscription(self, s): ...
            async def get_subscription(self, sid): ...
            async def delete_subscription(self, sid): ...
            async def mark_event_processed(self, sid, eid): ...

        storage = ConcreteStorage()
        evt = _make_event("ev", data={"status": "ok"})
        result = await storage.find_matching_subscriptions(evt, {})
        self.assertEqual(len(result), 1)
        self.assertIs(result[0], matching_sub)

    async def test_batch_mark_processed_all_true(self):
        """Kill all(results) mutation."""
        from plaita.event.core import EventSubscriptionStorage

        class ConcreteStorage(EventSubscriptionStorage):
            async def mark_event_processed(self, sid, eid):
                return True
            async def list_subscriptions(self, **kw): return []
            async def store_subscription(self, s): ...
            async def get_subscription(self, sid): ...
            async def delete_subscription(self, sid): ...

        storage = ConcreteStorage()
        result = await storage.batch_mark_processed("sub-1", ["e1", "e2", "e3"])
        self.assertTrue(result)

    async def test_batch_mark_processed_any_false_returns_false(self):
        """Kill all() → any() mutation."""
        from plaita.event.core import EventSubscriptionStorage

        call_num = [0]

        class ConcreteStorage(EventSubscriptionStorage):
            async def mark_event_processed(self, sid, eid):
                call_num[0] += 1
                return call_num[0] != 2  # second call returns False
            async def list_subscriptions(self, **kw): return []
            async def store_subscription(self, s): ...
            async def get_subscription(self, sid): ...
            async def delete_subscription(self, sid): ...

        storage = ConcreteStorage()
        result = await storage.batch_mark_processed("sub-1", ["e1", "e2", "e3"])
        self.assertFalse(result)


# ---------------------------------------------------------------------------
# EventBus.matches_event_type
# ---------------------------------------------------------------------------

class TestMatchesEventType(unittest.TestCase):
    def test_none_matches_all(self):
        """Kill not handler_event_type → handler_event_type mutation."""
        self.assertTrue(EventBus.matches_event_type(None, "any.event"))

    def test_star_matches_all(self):
        """Kill handler_event_type == '*' → != '*' mutation."""
        self.assertTrue(EventBus.matches_event_type("*", "any.event"))

    def test_empty_string_matches_all(self):
        self.assertTrue(EventBus.matches_event_type("", "any.event"))

    def test_exact_match(self):
        """Kill == → != on exact match."""
        self.assertTrue(EventBus.matches_event_type("user.login", "user.login"))

    def test_exact_no_match(self):
        self.assertFalse(EventBus.matches_event_type("user.login", "user.logout"))

    def test_wildcard_prefix(self):
        """Kill fnmatch call mutation."""
        self.assertTrue(EventBus.matches_event_type("user.*", "user.login"))
        self.assertTrue(EventBus.matches_event_type("user.*", "user.logout"))
        self.assertFalse(EventBus.matches_event_type("user.*", "admin.login"))

    def test_wildcard_suffix(self):
        self.assertTrue(EventBus.matches_event_type("*.login", "user.login"))
        self.assertTrue(EventBus.matches_event_type("*.login", "admin.login"))
        self.assertFalse(EventBus.matches_event_type("*.login", "user.logout"))

    def test_wildcard_middle(self):
        self.assertTrue(EventBus.matches_event_type("*.user.*", "app.user.login"))
        self.assertFalse(EventBus.matches_event_type("*.user.*", "app.admin.login"))

    def test_returns_bool(self):
        result = EventBus.matches_event_type("ev", "ev")
        self.assertIsInstance(result, bool)


# ---------------------------------------------------------------------------
# EventBus.publish_sync
# ---------------------------------------------------------------------------

class TestPublishSync(unittest.TestCase):
    def test_publish_sync_delegates_to_publish(self):
        """Kill _run_async_from_sync() removal mutation."""
        class ConcreteEventBus(EventBus):
            async def publish(self, event, prevent_duplicate_consumption=True, **kw):
                return "evt-id-123"
            async def batch_publish(self, *a, **kw): ...
            async def register_subscription(self, *a, **kw): ...
            async def unregister_subscription(self, *a, **kw): ...
            async def wait_for_event(self, *a, **kw): ...
            async def register_handler(self, *a, **kw): ...
            async def get_event(self, *a, **kw): ...

        bus = ConcreteEventBus()
        evt = _make_event()
        result = bus.publish_sync(evt)
        self.assertEqual(result, "evt-id-123")

    def test_publish_sync_passes_prevent_duplicate_flag(self):
        """Kill prevent_duplicate_consumption passthrough mutation."""
        received_flag = []

        class ConcreteEventBus(EventBus):
            async def publish(self, event, prevent_duplicate_consumption=True, **kw):
                received_flag.append(prevent_duplicate_consumption)
                return "id"
            async def batch_publish(self, *a, **kw): ...
            async def register_subscription(self, *a, **kw): ...
            async def unregister_subscription(self, *a, **kw): ...
            async def wait_for_event(self, *a, **kw): ...
            async def register_handler(self, *a, **kw): ...
            async def get_event(self, *a, **kw): ...

        bus = ConcreteEventBus()
        bus.publish_sync(_make_event(), prevent_duplicate_consumption=False)
        self.assertEqual(received_flag, [False])


# ---------------------------------------------------------------------------
# Round 5: batch_mark args, publish_sync event passthrough, batch_publish loop
# ---------------------------------------------------------------------------

class TestBatchMarkProcessedArgsR5(unittest.IsolatedAsyncioTestCase):
    async def test_batch_mark_passes_subscription_and_event_ids(self):
        """Kill subscription_id / event_id argument swap mutations."""
        from plaita.event.core import EventSubscriptionStorage

        calls: list[tuple[str, str]] = []

        class ConcreteStorage(EventSubscriptionStorage):
            async def mark_event_processed(self, sid, eid):
                calls.append((sid, eid))
                return True
            async def list_subscriptions(self, **kw): return []
            async def store_subscription(self, s): ...
            async def get_subscription(self, sid): ...
            async def delete_subscription(self, sid): ...

        storage = ConcreteStorage()
        await storage.batch_mark_processed("sub-unique", ["ev-a", "ev-b"])
        self.assertEqual(calls, [("sub-unique", "ev-a"), ("sub-unique", "ev-b")])


class TestPublishSyncEventPassthroughR5(unittest.TestCase):
    def test_publish_sync_passes_same_event_object(self):
        """Kill publish_sync default event parameter mutation."""
        received: list = []

        class ConcreteEventBus(EventBus):
            async def publish(self, event, prevent_duplicate_consumption=True, **kw):
                received.append(event)
                return "id"
            async def batch_publish(self, *a, **kw): ...
            async def register_subscription(self, *a, **kw): ...
            async def unregister_subscription(self, *a, **kw): ...
            async def wait_for_event(self, *a, **kw): ...
            async def register_handler(self, *a, **kw): ...
            async def get_event(self, *a, **kw): ...

        bus = ConcreteEventBus()
        evt = _make_event("passthrough.test")
        bus.publish_sync(evt)
        self.assertEqual(len(received), 1)
        self.assertIs(received[0], evt)

    def test_publish_sync_default_prevent_duplicate_true(self):
        """Kill prevent_duplicate_consumption=True default mutation."""
        received_flag: list = []

        class ConcreteEventBus(EventBus):
            async def publish(self, event, prevent_duplicate_consumption=True, **kw):
                received_flag.append(prevent_duplicate_consumption)
                return "id"
            async def batch_publish(self, *a, **kw): ...
            async def register_subscription(self, *a, **kw): ...
            async def unregister_subscription(self, *a, **kw): ...
            async def wait_for_event(self, *a, **kw): ...
            async def register_handler(self, *a, **kw): ...
            async def get_event(self, *a, **kw): ...

        bus = ConcreteEventBus()
        bus.publish_sync(_make_event())
        self.assertEqual(received_flag, [True])


class TestBatchPublishR5(unittest.IsolatedAsyncioTestCase):
    async def test_batch_publish_calls_publish_per_event(self):
        """Kill batch_publish loop / append mutations."""
        publish_calls: list = []

        class ConcreteEventBus(EventBus):
            async def publish(self, event, prevent_duplicate_consumption=True, **kw):
                publish_calls.append((event.event_type, prevent_duplicate_consumption))
                return f"id_{event.event_type}"
            async def register_subscription(self, *a, **kw): ...
            async def unregister_subscription(self, *a, **kw): ...
            async def wait_for_event(self, *a, **kw): ...
            async def register_handler(self, *a, **kw): ...
            async def get_event(self, *a, **kw): ...

        bus = ConcreteEventBus()
        events = [_make_event("a"), _make_event("b")]
        ids = await bus.batch_publish(events, prevent_duplicate_consumption=False)
        self.assertEqual(ids, ["id_a", "id_b"])
        self.assertEqual(publish_calls, [("a", False), ("b", False)])


class TestExpressPrefixCustomR5(unittest.TestCase):
    def test_custom_prefix_for_flow_id_lookup(self):
        """Kill EXPRESS_PREFIX key / default '$' mutations with non-default prefix."""
        sub = _make_sub("ev", flow_id="flow-99")
        evt = _make_event("ev")
        ctx = {"EXPRESS_PREFIX": "@", "@FLOW_ID": "flow-99"}
        self.assertTrue(sub.matches_event(evt, ctx))


# ---------------------------------------------------------------------------
# flush_pending_handler_registrations
# ---------------------------------------------------------------------------

class TestFlushPendingHandlerRegistrations(unittest.IsolatedAsyncioTestCase):
    async def test_flush_calls_all_pending(self):
        """Kill pending iteration/await mutations."""
        import plaita.event.core as core_mod
        called = []

        async def register_fn():
            called.append(True)

        original = list(core_mod._pending_handler_registrations)
        core_mod._pending_handler_registrations.clear()
        core_mod._pending_handler_registrations.append(register_fn)
        core_mod._pending_handler_registrations.append(register_fn)

        await flush_pending_handler_registrations()

        self.assertEqual(len(called), 2)
        # Restore
        core_mod._pending_handler_registrations.extend(original)

    async def test_flush_clears_pending_list(self):
        """Kill _pending_handler_registrations.clear() mutation."""
        import plaita.event.core as core_mod

        async def register_fn():
            pass

        original = list(core_mod._pending_handler_registrations)
        core_mod._pending_handler_registrations.clear()
        core_mod._pending_handler_registrations.append(register_fn)

        await flush_pending_handler_registrations()

        self.assertEqual(len(core_mod._pending_handler_registrations), 0)
        core_mod._pending_handler_registrations.extend(original)

    async def test_flush_awaits_each_register_fn(self):
        """Kill await register() → call without await mutation."""
        import plaita.event.core as core_mod

        completed = []

        async def register_fn():
            await asyncio.sleep(0)
            completed.append("done")

        original = list(core_mod._pending_handler_registrations)
        core_mod._pending_handler_registrations.clear()
        core_mod._pending_handler_registrations.append(register_fn)

        await flush_pending_handler_registrations()

        self.assertEqual(completed, ["done"])
        core_mod._pending_handler_registrations.extend(original)


if __name__ == "__main__":
    unittest.main()
