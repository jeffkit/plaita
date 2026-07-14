"""
T021: 测试 SCAN 和 pipeline 行为正确（通过 fakeredis 验证）
T023: 测试同一事件+订阅组合只生成一个 resume 任务
"""
import json
import asyncio
import pytest

import fakeredis

from plaita.event.core import Event, EventSubscription
from plaita.event.memory import InMemoryEventSubscriptionStorage
from plaita.storage.memory import MemoryExecutionStorage
from plaita.storage.base import ExecutionState
from plaita.server.event_filter import EventFilter


def run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# ─── T021: SCAN & Pipeline 行为 ──────────────────────────────


class TestRedisScanAndPipeline:
    """通过 fakeredis 模拟 Redis 异步操作验证 SCAN/pipeline 的正确性"""

    def test_redis_event_storage_scan(self):
        """RedisEventStorage.list_events 使用 SCAN 而非 KEYS"""
        try:
            from plaita.event.redis import RedisEventStorage

            storage = RedisEventStorage(redis_url="redis://localhost:6379/15")

            async def _test():
                server = fakeredis.FakeServer()
                storage.redis = fakeredis.aioredis.FakeRedis(
                    server=server, decode_responses=True
                )

                for i in range(5):
                    event = Event(event_type="scan.test", data={"idx": i})
                    await storage.store_event(event)

                events = await storage.list_events(event_type="scan.test", limit=10)
                assert len(events) == 5

                all_events = await storage.list_events(limit=100)
                assert len(all_events) == 5

                await storage.close()

            run(_test())
        except ImportError:
            pytest.skip("redis or fakeredis not available")

    def test_redis_subscription_storage_pipeline(self):
        """RedisEventSubscriptionStorage.store_subscription 使用 pipeline"""
        try:
            from plaita.event.redis import RedisEventSubscriptionStorage

            storage = RedisEventSubscriptionStorage(redis_url="redis://localhost:6379/15")

            async def _test():
                server = fakeredis.FakeServer()
                storage.redis = fakeredis.aioredis.FakeRedis(server=server)

                sub = EventSubscription(
                    event_type="pipeline.test",
                    correlation_id="corr-1",
                    flow_id="flow-1",
                    node_id="node-1",
                )
                sub_id = await storage.store_subscription(sub)
                assert sub_id == sub.subscription_id

                retrieved = await storage.get_subscription(sub_id)
                assert retrieved is not None
                assert retrieved.event_type == "pipeline.test"
                assert retrieved.correlation_id == "corr-1"

                by_type = await storage.list_subscriptions(event_type="pipeline.test")
                assert len(by_type) == 1

                await storage.close()

            run(_test())
        except ImportError:
            pytest.skip("redis or fakeredis not available")

    def test_redis_subscription_storage_scan_no_filter(self):
        """list_subscriptions 无过滤条件时使用 SCAN"""
        try:
            from plaita.event.redis import RedisEventSubscriptionStorage

            storage = RedisEventSubscriptionStorage(redis_url="redis://localhost:6379/15")

            async def _test():
                server = fakeredis.FakeServer()
                storage.redis = fakeredis.aioredis.FakeRedis(server=server)

                for i in range(3):
                    sub = EventSubscription(event_type=f"type_{i}")
                    await storage.store_subscription(sub)

                all_subs = await storage.list_subscriptions()
                assert len(all_subs) == 3

                await storage.close()

            run(_test())
        except ImportError:
            pytest.skip("redis or fakeredis not available")

    def test_redis_event_ttl_set(self):
        """store_event 应设置 TTL"""
        try:
            from plaita.event.redis import RedisEventStorage

            storage = RedisEventStorage(
                redis_url="redis://localhost:6379/15", ttl=3600
            )

            async def _test():
                server = fakeredis.FakeServer()
                storage.redis = fakeredis.aioredis.FakeRedis(server=server)

                event = Event(event_type="ttl.test", data={"v": 1})
                await storage.store_event(event)

                key = f"{storage.key_prefix}events:{event.event_id}"
                ttl = await storage.redis.ttl(key)
                assert ttl > 0
                assert ttl <= 3600

                await storage.close()

            run(_test())
        except ImportError:
            pytest.skip("redis or fakeredis not available")


# ─── T023: EventFilter 幂等检查 ──────────────────────────────


class TestEventFilterIdempotency:
    def _make_filter(self):
        """创建一个使用内存组件 + fakeredis 的 EventFilter"""
        execution_storage = MemoryExecutionStorage()
        subscription_storage = InMemoryEventSubscriptionStorage()

        redis_client = fakeredis.FakeRedis(decode_responses=True)

        from unittest.mock import AsyncMock
        mock_event_bus = AsyncMock()

        event_filter = EventFilter(
            execution_storage=execution_storage,
            subscription_storage=subscription_storage,
            redis_client=redis_client,
            event_bus=mock_event_bus,
            queue_name="test:queue",
        )
        return event_filter, execution_storage, subscription_storage, redis_client

    @staticmethod
    def _stream_payloads(redis_client, stream_key: str):
        """Read all task bodies from a Stream (post List→Stream migration)."""
        entries = redis_client.xrange(stream_key, min="-", max="+")
        tasks = []
        for _mid, fields in entries:
            raw = fields.get("payload") or fields.get(b"payload")
            if isinstance(raw, bytes):
                raw = raw.decode()
            tasks.append(json.loads(raw))
        return tasks

    def test_same_event_subscription_only_one_resume_task(self):
        """同一事件+订阅组合应只生成一个 resume 任务（SETNX 幂等）"""
        ef, exec_storage, sub_storage, redis_client = self._make_filter()

        state = ExecutionState(
            execution_id="exec-1",
            flow_id="flow-1",
            status="suspended",
            context={},
        )
        exec_storage.save_execution_state("exec-1", state)

        sub = EventSubscription(
            event_type="approval",
            correlation_id="exec-1",
            flow_id="flow-1",
        )

        async def _test():
            await sub_storage.store_subscription(sub)

            event = Event(
                event_type="approval",
                data={"approved": True},
                correlation_id="exec-1",
            )

            await ef.handle_event(event)
            await ef.handle_event(event)

            tasks = self._stream_payloads(redis_client, "test:queue")
            assert len(tasks) == 1
            task = tasks[0]
            assert task["type"] == "resume"
            assert task["execution_id"] == "exec-1"
            assert task["data"]["event_id"] == event.event_id
            assert task["data"]["subscription_id"] == sub.subscription_id

        run(_test())

    def test_different_events_generate_separate_tasks(self):
        """不同事件应各自生成 resume 任务"""
        ef, exec_storage, sub_storage, redis_client = self._make_filter()

        state = ExecutionState(
            execution_id="exec-1",
            flow_id="flow-1",
            status="suspended",
            context={},
        )
        exec_storage.save_execution_state("exec-1", state)

        sub = EventSubscription(
            event_type="approval",
            correlation_id="exec-1",
            flow_id="flow-1",
        )

        async def _test():
            await sub_storage.store_subscription(sub)

            event1 = Event(
                event_type="approval",
                data={"approved": True},
                correlation_id="exec-1",
            )
            event2 = Event(
                event_type="approval",
                data={"approved": False},
                correlation_id="exec-1",
            )

            await ef.handle_event(event1)
            await ef.handle_event(event2)

            tasks = self._stream_payloads(redis_client, "test:queue")
            assert len(tasks) == 2

        run(_test())
