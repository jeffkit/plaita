"""test_clean_c3_events — 清理批次包 C3 事件层回归（plaita 仓）。

C3-1：RedisProcessingTracker.cleanup_old_records 历史上按 ``history:{handler_id}``
形状删历史键，而写入（record_processing_attempt）与读取（get_processing_history）
都在 ``history:{event_id}`` ——清理删的是不存在的键（等于不清理），且当某条旧记录
的 handler_id 恰与某个新鲜事件的 event_id 同名时会把该新鲜事件的 history 误删。
修复后：cleanup 从 processed 键（``events:{event_id}``）取 event_id，按写入形状
删一次 ``history:{event_id}``。

C3-2：三后端重试总投递次数对齐。memory/redis 的现行语义：总尝试次数 =
max_retries（首次 + 重试 max_retries-1 次；max_retries=0 也保底投递 1 次）。
sqlalchemy 历史上是 max_retries+1 次——以 memory/redis 为准改 sqlalchemy。

C3-3：RedisEventSubscriptionStorage.mark_event_processed 历史上 get→set 整份
订阅 JSON，非原子——两个进程并发 mark 同一 (subscription, event) 各自覆盖、
双双返回 True，同一事件可重复 resume。修复后改 SET NX 去重键（参照
server/event_filter.py 的既有 SET NX 模式）：跨进程/并发重复 mark 仅首次生效。

Redis 相关测试用 fakeredis（共享 FakeServer）模拟，全程无需真实实例。
"""

import asyncio
import time
import unittest
from unittest.mock import patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis
import fakeredis.aioredis

import plaita.event.redis as redis_mod
from plaita.event.core import Event, EventSubscription, RetryPolicy
from plaita.event.memory import InMemoryEventBus
from plaita.event.redis import (
    RedisEventBus, RedisEventSubscriptionStorage, RedisProcessingTracker,
)
from plaita.event.sqlalchemy import SqlalchemyEventBus

_SHARED_SERVER = fakeredis.FakeServer()


async def _fake_from_url(url):
    """initialize() 的 from_url 打桩：共享 FakeServer 的 fakeredis 客户端。"""
    return fakeredis.aioredis.FakeRedis(server=_SHARED_SERVER)


def _patch_from_url():
    return patch.object(redis_mod.aioredis, "from_url", side_effect=_fake_from_url)


# ────────────────────────────── C3-1 ──────────────────────────────

class TestCleanupHistoryKeyShape(unittest.IsolatedAsyncioTestCase):
    """cleanup 与写入/读取的 history 键形状对齐（history:{event_id}）。"""

    async def asyncSetUp(self):
        # 共享 FakeServer 跨用例留存键，先清场保证隔离
        client = fakeredis.aioredis.FakeRedis(server=_SHARED_SERVER)
        await client.flushall()
        await client.aclose()

    def _tracker(self) -> RedisProcessingTracker:
        return RedisProcessingTracker("redis://localhost:6379/9")

    async def _age_record(self, tracker: RedisProcessingTracker, event_id: str):
        """把某事件 processed 记录的 last_updated 拨老，落入清理窗口。"""
        key = f"plaita:processed:events:{event_id}"
        await tracker.redis.hset(key, "last_updated", str(time.time() - 10_000))

    async def test_cleanup_deletes_history_in_written_shape(self):
        """旧事件的 history（按写入形状 history:{event_id}）被清理。"""
        tracker = self._tracker()
        with _patch_from_url():
            await tracker.mark_event_processed("evt-old", "handler-1")
            await tracker.record_processing_attempt("evt-old", "handler-1", "success")
            self.assertEqual(
                len(await tracker.get_processing_history("evt-old")), 1)
            await self._age_record(tracker, "evt-old")

            removed = await tracker.cleanup_old_records(max_age_seconds=3600)
            self.assertEqual(removed, 1)
            # 写入形状的 history 键被清理（历史实现删 history:{handler_id}，
            # 形状不符 → 实际什么都没清）
            self.assertFalse(
                await tracker.redis.exists("plaita:processed:history:evt-old"))
            self.assertFalse(
                await tracker.redis.exists("plaita:processed:events:evt-old"))

    async def test_cleanup_spares_fresh_event_history(self):
        """未过期事件的 history 不被误删。"""
        tracker = self._tracker()
        with _patch_from_url():
            await tracker.mark_event_processed("evt-old", "handler-1")
            await tracker.record_processing_attempt("evt-old", "handler-1", "success")
            await tracker.mark_event_processed("evt-new", "handler-1")
            await tracker.record_processing_attempt("evt-new", "handler-1", "success")
            await self._age_record(tracker, "evt-old")

            await tracker.cleanup_old_records(max_age_seconds=3600)

            self.assertTrue(
                await tracker.redis.exists("plaita:processed:history:evt-new"))
            self.assertTrue(
                await tracker.redis.exists("plaita:processed:events:evt-new"))
            self.assertEqual(
                len(await tracker.get_processing_history("evt-new")), 1)

    async def test_cleanup_does_not_delete_fresh_history_matching_handler_id(self):
        """误删场景回归：旧记录的 handler_id 恰与新鲜事件的 event_id 同名，
        历史实现按 handler 形状删会把该新鲜事件的 history 一并误删。"""
        tracker = self._tracker()
        with _patch_from_url():
            # 旧记录的 handler_id == 新鲜事件的 event_id "evt-new"
            await tracker.mark_event_processed("evt-old", "evt-new")
            await tracker.record_processing_attempt("evt-old", "evt-new", "success")
            # 新鲜事件自己的记录与 history
            await tracker.mark_event_processed("evt-new", "handler-2")
            await tracker.record_processing_attempt("evt-new", "handler-2", "success")
            await self._age_record(tracker, "evt-old")

            await tracker.cleanup_old_records(max_age_seconds=3600)

            # 新鲜事件的 history 必须仍在（历史实现在此被误删）
            self.assertTrue(
                await tracker.redis.exists("plaita:processed:history:evt-new"))
            self.assertEqual(
                len(await tracker.get_processing_history("evt-new")), 1)


# ────────────────────────────── C3-2 ──────────────────────────────

class _StubTracker:
    """sqlalchemy 总线重试路径的 tracker 桩（免真实 DB）。"""

    def __init__(self):
        self.marks = []

    async def mark_event_processed(self, event_id, handler_id):
        self.marks.append((event_id, handler_id))
        return True

    async def record_processing_attempt(self, event_id, handler_id, status, error=None):
        pass

    async def is_event_processed(self, event_id, handler_id):
        return False


class TestRetryTotalAttemptsParity(unittest.IsolatedAsyncioTestCase):
    """三后端同一 max_retries 下总投递次数一致（= memory/redis 现行语义）。"""

    @staticmethod
    def _policy(max_retries: int) -> RetryPolicy:
        return RetryPolicy(max_retries=max_retries, initial_delay=0.001,
                           backoff_factor=1.0, max_delay=0.005)

    async def _count_memory(self, max_retries: int) -> int:
        bus = InMemoryEventBus()
        calls = []

        async def failing_handler(event):
            calls.append(1)
            raise RuntimeError("boom")

        await bus.register_handler("t.x", failing_handler,
                                   retry_policy=self._policy(max_retries))
        await bus._process_with_retry(
            failing_handler, Event(event_type="t.x", data={}), "h1",
            self._policy(max_retries))
        return len(calls)

    async def _count_redis(self, max_retries: int) -> int:
        bus = RedisEventBus("redis://localhost:6379/9")
        with _patch_from_url():
            calls = []

            async def failing_handler(event):
                calls.append(1)
                raise RuntimeError("boom")

            await bus._process_with_retry(
                failing_handler, Event(event_type="t.x", data={}), "h1",
                self._policy(max_retries))
        return len(calls)

    async def _count_sqlalchemy(self, max_retries: int) -> int:
        bus = SqlalchemyEventBus(engine=None)
        bus.processing_tracker = _StubTracker()
        calls = []

        async def failing_handler(event):
            calls.append(1)
            raise RuntimeError("boom")

        await bus._execute_with_retry(
            failing_handler, Event(event_type="t.x", data={}), "h1",
            self._policy(max_retries))
        return len(calls)

    async def test_three_backends_same_total_attempts(self):
        for max_retries in (0, 1, 3):
            with self.subTest(max_retries=max_retries):
                expected = max(max_retries, 1)  # 首次 + 重试 max_retries-1 次
                memory = await self._count_memory(max_retries)
                redis = await self._count_redis(max_retries)
                sqlalchemy = await self._count_sqlalchemy(max_retries)
                self.assertEqual(memory, expected,
                                 f"memory: max_retries={max_retries}")
                self.assertEqual(redis, expected,
                                 f"redis: max_retries={max_retries}")
                self.assertEqual(
                    sqlalchemy, expected,
                    f"sqlalchemy 应与 memory/redis 对齐: max_retries={max_retries}")

    async def test_success_stops_retrying(self):
        """成功即停：三后端都只投递一次。"""
        bus = InMemoryEventBus()
        calls = []

        async def ok_handler(event):
            calls.append(1)

        await bus.register_handler("t.x", ok_handler,
                                   retry_policy=self._policy(3))
        await bus._process_with_retry(
            ok_handler, Event(event_type="t.x", data={}), "h1", self._policy(3))
        self.assertEqual(len(calls), 1)

        sa_bus = SqlalchemyEventBus(engine=None)
        sa_bus.processing_tracker = _StubTracker()
        sa_calls = []

        async def sa_ok_handler(event):
            sa_calls.append(1)

        await sa_bus._execute_with_retry(
            sa_ok_handler, Event(event_type="t.x", data={}), "h1",
            self._policy(3))
        self.assertEqual(len(sa_calls), 1)


# ────────────────────────────── C3-3 ──────────────────────────────

class TestSubscriptionMarkDedupAtomic(unittest.IsolatedAsyncioTestCase):
    """订阅 mark_event_processed：SET NX 原子去重，跨进程仅首次生效。"""

    KEY_PREFIX = "plaita:subscription:"

    def _storage(self) -> RedisEventSubscriptionStorage:
        return RedisEventSubscriptionStorage(
            "redis://localhost:6379/9", key_prefix=self.KEY_PREFIX)

    async def _make_subscription(self, storage) -> str:
        subscription = EventSubscription(event_type="e.order", correlation_id="c-1")
        return await storage.store_subscription(subscription)

    async def test_duplicate_mark_returns_false_and_marks_once(self):
        storage = self._storage()
        with _patch_from_url():
            sid = await self._make_subscription(storage)
            self.assertTrue(await storage.mark_event_processed(sid, "evt-1"))
            # 重复 mark（同进程）：不再生效
            self.assertFalse(await storage.mark_event_processed(sid, "evt-1"))

    async def test_cross_process_duplicate_mark_only_first_wins(self):
        """两个 storage 实例（模拟两个进程，共享 FakeServer）：
        历史实现双双返回 True（get→set 互相覆盖）；修复后仅首次生效。"""
        first = self._storage()
        with _patch_from_url():
            sid = await self._make_subscription(first)
            second = self._storage()  # 新实例 = 新"进程"（initialize 重建 client）

            self.assertTrue(await first.mark_event_processed(sid, "evt-1"))
            self.assertFalse(await second.mark_event_processed(sid, "evt-1"))

    async def test_concurrent_marks_exactly_one_wins(self):
        """并发 mark 同一 (subscription, event)：恰好一个 True。"""
        first = self._storage()
        with _patch_from_url():
            sid = await self._make_subscription(first)
            second = self._storage()

            results = await asyncio.gather(
                first.mark_event_processed(sid, "evt-1"),
                second.mark_event_processed(sid, "evt-1"),
            )
            self.assertEqual(sorted(r is True for r in results), [False, True])

    async def test_mark_updates_subscription_data_for_read_side(self):
        """mark 成功后 get_subscription 的 processed_events 含该事件
        （find_unprocessed_matching_subscriptions 等读侧依赖）。"""
        storage = self._storage()
        with _patch_from_url():
            sid = await self._make_subscription(storage)
            await storage.mark_event_processed(sid, "evt-1")
            subscription = await storage.get_subscription(sid)
            self.assertIn("evt-1", subscription.processed_events)

    async def test_mark_missing_subscription_returns_false(self):
        storage = self._storage()
        with _patch_from_url():
            self.assertFalse(await storage.mark_event_processed("no-such", "evt-1"))

    async def test_batch_mark_uses_same_atomic_dedup(self):
        """batch 路径与单发 mark 同一去重原语；订阅缺失仍返回 False。"""
        storage = self._storage()
        with _patch_from_url():
            self.assertFalse(
                await storage.batch_mark_processed("no-such", ["evt-1"]))
            sid = await self._make_subscription(storage)
            self.assertTrue(
                await storage.batch_mark_processed(sid, ["evt-1", "evt-2"]))
            # 重复 batch：订阅仍在 → True（batch 契约是"批次已处理"），
            # 但每个事件的去重键只生效一次
            await storage.batch_mark_processed(sid, ["evt-1"])
            subscription = await storage.get_subscription(sid)
            self.assertEqual(subscription.processed_events, {"evt-1", "evt-2"})


if __name__ == "__main__":
    unittest.main()
