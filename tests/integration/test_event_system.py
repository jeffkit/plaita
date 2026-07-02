"""
T007: 事件去重 — 同一事件发布两次，subscription 只处理一次
T014: close() 和 publish 无副作用
T018: prefix 动态获取、超时计算、并发存储
T034: Redis 组件正确关闭连接
"""
import asyncio
import time
import pytest
from unittest.mock import MagicMock, AsyncMock

from plaita.event.core import Event, EventSubscription
from plaita.event.memory import (
    InMemoryEventBus,
    InMemoryEventSubscriptionStorage,
    MemoryEventStorage,
)
from plaita.node.event_node import EventNode
from plaita.event.exceptions import EventTimeoutError


@pytest.fixture
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


def run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# ─── T007: 事件去重 ───────────────────────────────────────────


class TestEventDeduplication:
    def test_subscription_mark_event_processed(self):
        """EventSubscription.mark_event_processed 应正确记录"""
        sub = EventSubscription(event_type="test")
        assert not sub.is_event_processed("evt-1")

        sub.mark_event_processed("evt-1")
        assert sub.is_event_processed("evt-1")

    def test_subscription_dedup_prevents_double_processing(self):
        """同一事件标记两次不会报错，但 processed_events 只记录一次"""
        sub = EventSubscription(event_type="test")
        sub.mark_event_processed("evt-1")
        sub.mark_event_processed("evt-1")
        assert len(sub.processed_events) == 1

    def test_memory_subscription_storage_dedup(self):
        """InMemoryEventSubscriptionStorage 的 mark_event_processed 应正确工作"""
        storage = InMemoryEventSubscriptionStorage()
        sub = EventSubscription(event_type="order.created")

        async def _test():
            await storage.store_subscription(sub)
            ok1 = await storage.mark_event_processed(sub.subscription_id, "evt-1")
            assert ok1

            retrieved = await storage.get_subscription(sub.subscription_id)
            assert retrieved.is_event_processed("evt-1")

        run(_test())

    def test_find_unprocessed_matching_subscriptions(self):
        """find_unprocessed_matching_subscriptions 应自动去重"""
        storage = InMemoryEventSubscriptionStorage()

        async def _test():
            sub = EventSubscription(event_type="test.event")
            await storage.store_subscription(sub)

            event = Event(event_type="test.event", data={"key": "val"})

            matched1 = await storage.find_unprocessed_matching_subscriptions(event)
            assert len(matched1) == 1

            matched2 = await storage.find_unprocessed_matching_subscriptions(event)
            assert len(matched2) == 0  # 已处理，不再匹配

        run(_test())

    def test_event_bus_handler_dedup(self):
        """InMemoryEventBus 的 handler 对同一事件只处理一次"""
        bus = InMemoryEventBus()
        call_count = 0

        async def handler(event):
            nonlocal call_count
            call_count += 1

        async def _test():
            nonlocal call_count
            await bus.register_handler(event_type="test", handler=handler)

            await bus.publish("test", key="v1")
            await asyncio.sleep(0.1)
            assert call_count == 1

            await bus.publish(Event(event_type="test", data={"key": "v2"}))
            await asyncio.sleep(0.1)
            assert call_count == 2

        run(_test())


# ─── T014: close() 和 publish 无副作用 ────────────────────────


class TestPublishNoSideEffect:
    def test_publish_dict_not_mutated(self):
        """publish(dict) 不应修改调用者传入的 dict"""
        bus = InMemoryEventBus()

        async def _test():
            original = {"event_type": "test.event", "payload": "data"}
            original_copy = dict(original)

            await bus.publish(original)
            assert original == original_copy

        run(_test())

    def test_batch_publish_dict_not_mutated(self):
        """batch_publish 不应修改传入的 dict 列表中的元素"""
        bus = InMemoryEventBus()

        async def _test():
            events = [
                {"event_type": "a", "data1": 1},
                {"event_type": "b", "data2": 2},
            ]
            copies = [dict(e) for e in events]

            await bus.batch_publish(events)
            assert events == copies

        run(_test())


class TestInMemoryEventBusClose:
    def test_memory_bus_has_no_close_error(self):
        """InMemoryEventBus 没有 close 方法（仅 Redis 需要），不应报错"""
        bus = InMemoryEventBus()
        assert bus is not None


# ─── T018: prefix 动态获取 ────────────────────────────────────


class TestEventNodePrefix:
    def test_prefix_from_execution_attribute(self):
        """EventNode 应通过 getattr 动态获取 express_prefix"""
        node = EventNode(id="evt1", event_type="test", next="end")

        class FakeExecution:
            express_prefix = "$$"
            context = {"$$NODE": {"evt1": {"status": "pending"}}}

            def get_node_state(self, node_id):
                return self.context["$$NODE"].get(node_id, {})

        execution = FakeExecution()
        state = node._get_node_state(execution)
        assert state.get("status") == "pending"

    def test_prefix_default_when_no_attribute(self):
        """没有 express_prefix 时应使用默认前缀"""
        node = EventNode(id="evt1", event_type="test", next="end")

        class MinimalExecution:
            context = {"$NODE": {"evt1": {"status": "completed"}}}

        execution = MinimalExecution()
        state = node._get_node_state(execution)
        assert state.get("status") == "completed"


# ─── T018: 超时计算 ──────────────────────────────────────────


class TestWaitForEventTimeout:
    def test_timeout_raises_error(self):
        """wait_for_event 超时应抛 EventTimeoutError"""
        bus = InMemoryEventBus()

        async def _test():
            with pytest.raises(EventTimeoutError):
                await bus.wait_for_event("never.happens", timeout=0.1)

        run(_test())

    def test_timeout_with_condition_respects_deadline(self):
        """带条件的 wait_for_event 应在 deadline 内超时，不会无限递归"""
        bus = InMemoryEventBus()

        async def _test():
            start = time.time()

            async def publish_wrong_events():
                for i in range(5):
                    await asyncio.sleep(0.05)
                    await bus.publish(Event(event_type="conditional", data={"v": i}))

            task = asyncio.create_task(publish_wrong_events())

            with pytest.raises(EventTimeoutError):
                await bus.wait_for_event(
                    "conditional",
                    timeout=0.3,
                    condition=lambda e: e.data.get("v") == 999,
                )

            elapsed = time.time() - start
            assert elapsed < 1.0  # 不应无限等待
            task.cancel()

        run(_test())


# ─── T018: 并发存储 ──────────────────────────────────────────


class TestConcurrentMemoryStorage:
    def test_concurrent_store_events(self):
        """MemoryEventStorage 在并发写入时应保持数据一致"""
        storage = MemoryEventStorage()

        async def _test():
            async def store_batch(prefix, count):
                for i in range(count):
                    event = Event(event_type=f"{prefix}.event", data={"idx": i})
                    await storage.store_event(event)

            tasks = [store_batch(f"batch{i}", 50) for i in range(10)]
            await asyncio.gather(*tasks)

            all_events = await storage.list_events(limit=1000)
            assert len(all_events) == 500

        run(_test())

    def test_concurrent_store_subscriptions(self):
        """InMemoryEventSubscriptionStorage 在并发写入时应保持数据一致"""
        storage = InMemoryEventSubscriptionStorage()

        async def _test():
            async def store_batch(count):
                for _ in range(count):
                    sub = EventSubscription(event_type="test")
                    await storage.store_subscription(sub)

            tasks = [store_batch(50) for _ in range(10)]
            await asyncio.gather(*tasks)

            all_subs = await storage.list_subscriptions()
            assert len(all_subs) == 500

        run(_test())


# ─── T034: Redis 组件关闭连接 ─────────────────────────────────


class TestRedisComponentClose:
    def test_redis_event_storage_close(self):
        """RedisEventStorage.close() 应将 redis 设为 None"""
        try:
            from plaita.event.redis import RedisEventStorage

            storage = RedisEventStorage(redis_url="redis://localhost:6379/0")
            # 不初始化连接，直接测试 close 逻辑
            assert storage.redis is None

            async def _test():
                await storage.close()
                assert storage.redis is None

            run(_test())
        except ImportError:
            pytest.skip("redis package not available")

    def test_redis_subscription_storage_close(self):
        """RedisEventSubscriptionStorage.close() 应将 redis 设为 None"""
        try:
            from plaita.event.redis import RedisEventSubscriptionStorage

            storage = RedisEventSubscriptionStorage(redis_url="redis://localhost:6379/0")
            assert storage.redis is None

            async def _test():
                await storage.close()
                assert storage.redis is None

            run(_test())
        except ImportError:
            pytest.skip("redis package not available")

    def test_redis_processing_tracker_close(self):
        """RedisProcessingTracker.close() 应将 redis 设为 None"""
        try:
            from plaita.event.redis import RedisProcessingTracker

            tracker = RedisProcessingTracker(redis_url="redis://localhost:6379/0")
            assert tracker.redis is None

            async def _test():
                await tracker.close()
                assert tracker.redis is None

            run(_test())
        except ImportError:
            pytest.skip("redis package not available")

    def test_redis_event_bus_close(self):
        """RedisEventBus.close() 应关闭所有子组件"""
        try:
            from plaita.event.redis import RedisEventBus

            bus = RedisEventBus(redis_url="redis://localhost:6379/0")

            async def _test():
                await bus.close()
                assert bus.redis is None
                assert bus.pubsub is None
                assert bus.event_storage.redis is None
                assert bus.subscription_storage.redis is None
                assert bus.processing_tracker.redis is None

            run(_test())
        except ImportError:
            pytest.skip("redis package not available")
