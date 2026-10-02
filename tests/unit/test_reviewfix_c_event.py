"""test_reviewfix_c_event — 评审修复包 C2 回归。

EventNode.event_filter（挂起侧原样存为订阅 filter_condition，见
core/strategies.py:_subscribe_event）宣称支持点路径（如
``{"data.status": "ok"}``），但消费链路 EventSubscription.matches_event
历史上只匹配事件 data 顶层键——点路径永不命中，订阅永不触发，流程
僵尸挂起。本文件锁定修复后的点路径遍历语义，并对照
EventNode.can_handle_event（同一语义的既有实现）保持一致。
"""

import unittest

from plaita.event.core import Event, EventSubscription, EventSubscriptionStorage
from plaita.node.event_node import EventNode


def _matches(filter_condition, data):
    sub = EventSubscription(event_type="t", filter_condition=filter_condition)
    return sub.matches_event(Event(event_type="t", data=data), {})


class TestDotPathFilter(unittest.TestCase):
    """`{"data.status": "ok"}` 点路径过滤：命中 / 不命中 / 非法中间层。"""

    def test_dot_path_hit(self):
        self.assertTrue(_matches({"data.status": "ok"}, {"data": {"status": "ok"}}))

    def test_dot_path_value_mismatch(self):
        self.assertFalse(_matches({"data.status": "ok"}, {"data": {"status": "fail"}}))

    def test_dot_path_missing_segment(self):
        self.assertFalse(_matches({"data.status": "ok"}, {"other": 1}))

    def test_non_dict_middle_layer(self):
        # 中间层是标量而非 dict：遍历失败 → 不匹配（与 can_handle_event 一致）
        self.assertFalse(_matches({"a.b": 1}, {"a": "scalar"}))

    def test_deep_path_hit(self):
        self.assertTrue(_matches({"a.b.c": 3}, {"a": {"b": {"c": 3}}}))

    def test_top_level_key_still_matches(self):
        self.assertTrue(_matches({"status": "ok"}, {"status": "ok"}))

    def test_event_attribute_fallback_still_matches(self):
        """顶层键缺省时回退事件属性（历史行为，correlation_id 等依赖它）。"""
        sub = EventSubscription(event_type="t", filter_condition={"correlation_id": "c1"})
        evt = Event(event_type="t", data={}, correlation_id="c1")
        self.assertTrue(sub.matches_event(evt, {}))

    def test_unknown_field_still_rejects(self):
        self.assertFalse(_matches({"nope": 1}, {"other": 2}))


class TestSemanticsMatchCanHandleEvent(unittest.TestCase):
    """matches_event 的点路径判定必须与 EventNode.can_handle_event 同判定。"""

    def _node(self, event_filter):
        return EventNode(id="ev", event_type="t", event_filter=event_filter)

    def test_same_verdicts_on_payloads(self):
        cases = [
            ({"data.status": "ok"}, {"data": {"status": "ok"}}, True),
            ({"data.status": "ok"}, {"data": {"status": "fail"}}, False),
            ({"data.status": "ok"}, {"data": {}}, False),
            ({"a.b": 1}, {"a": "scalar"}, False),
            ({"status": "ok"}, {"status": "ok"}, True),
            ({}, {"anything": 1}, True),  # 空过滤全匹配
        ]
        for event_filter, payload, want in cases:
            node = self._node(event_filter)
            sub = EventSubscription(event_type="t", filter_condition=dict(event_filter))
            evt = Event(event_type="t", data=dict(payload))
            got_sub = sub.matches_event(evt, {})
            got_node = node.can_handle_event("t", payload)
            self.assertEqual(
                got_sub, want,
                f"filter={event_filter} payload={payload}: matches_event={got_sub}")
            self.assertEqual(
                got_node, want,
                f"filter={event_filter} payload={payload}: can_handle_event={got_node}")
            self.assertEqual(got_sub, got_node, "两条实现必须同判定")


class TestFindMatchingSubscriptionsEndToEnd(unittest.TestCase):
    """经存储基类 find_matching_subscriptions 的完整消费链路。"""

    def test_dot_path_subscription_is_found(self):
        sub = EventSubscription(event_type="t", filter_condition={"data.status": "ok"})

        class _Store(EventSubscriptionStorage):
            async def store_subscription(self, s):
                return s.subscription_id

            async def get_subscription(self, sid):
                return sub if sid == sub.subscription_id else None

            async def list_subscriptions(self, event_type=None, correlation_id=None,
                                         flow_id=None, node_id=None):
                return [sub]

            async def delete_subscription(self, sid):
                return True

            async def mark_event_processed(self, sid, eid):
                return True

        store = _Store()
        evt = Event(event_type="t", data={"data": {"status": "ok"}})
        found = self._run(store.find_matching_subscriptions(evt, {}))
        self.assertEqual([s.subscription_id for s in found], [sub.subscription_id])

    def test_non_matching_dot_path_event_not_found(self):
        sub = EventSubscription(event_type="t", filter_condition={"data.status": "ok"})

        class _Store(EventSubscriptionStorage):
            async def store_subscription(self, s):
                return s.subscription_id

            async def get_subscription(self, sid):
                return sub if sid == sub.subscription_id else None

            async def list_subscriptions(self, event_type=None, correlation_id=None,
                                         flow_id=None, node_id=None):
                return [sub]

            async def delete_subscription(self, sid):
                return True

            async def mark_event_processed(self, sid, eid):
                return True

        store = _Store()
        evt = Event(event_type="t", data={"data": {"status": "fail"}})
        found = self._run(store.find_matching_subscriptions(evt, {}))
        self.assertEqual(found, [])

    @staticmethod
    def _run(coro):
        import asyncio
        return asyncio.run(coro)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
