"""Track C 任务3验收：事件→resume 链的兜底回扫（EventReconciler）。

背景：RedisEventBus.publish = 存事件（zset 索引 TTL 7 天）+ Pub/Sub 通知；
EventFilter 只消费 Pub/Sub 推送（event_filter.py register_handler → redis.py
_listen_for_*）。Pub/Sub 不持久：EventFilter 重启/重连窗口内的通知丢了即
永失——事件存储里明明有事件，却无人回扫，挂起执行僵尸化（修复前该行为
不存在，本文件以「新能力测试」验收）。

修复：EventReconciler 周期 + 启动回填扫描事件存储时间窗，逐条调用
EventFilter.handle_event——其 SET NX 去重键（plaita:event_filter:dedup）
保证推送链路已处理的事件不重复 resume（幂等）；游标记 Redis 避免重复扫；
PLAITA_DISABLE_EVENT_RECONCILE=1 回滚。
"""
from __future__ import annotations

import asyncio
import json
import time
import unittest
from unittest.mock import AsyncMock

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis

from plaita.event.core import Event, EventSubscription
from plaita.event.memory import InMemoryEventSubscriptionStorage
from plaita.event.redis import RedisEventStorage
from plaita.server.event_filter import EventFilter
from plaita.server.event_reconcile import EventReconciler
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage


def run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _stream_payloads(redis_client, stream_key):
    entries = redis_client.xrange(stream_key, min="-", max="+")
    tasks = []
    for _mid, fields in entries:
        raw = fields.get("payload") or fields.get(b"payload")
        if isinstance(raw, bytes):
            raw = raw.decode()
        tasks.append(json.loads(raw))
    return tasks


class ReconcileTestBase(unittest.TestCase):
    """共享一个 FakeServer：事件存储（async）与 EventFilter/游标（sync）同数据面。"""

    QUEUE = "test:reconcile:queue"

    def setUp(self):
        self.server = fakeredis.FakeServer()
        self.redis_client = fakeredis.FakeRedis(
            server=self.server, decode_responses=True
        )
        self.event_storage = RedisEventStorage(
            redis_url="redis://localhost:6379/15", ttl=3600
        )
        self.event_storage.redis = fakeredis.aioredis.FakeRedis(
            server=self.server, decode_responses=True
        )
        self.execution_storage = MemoryExecutionStorage()
        self.subscription_storage = InMemoryEventSubscriptionStorage()
        self.event_filter = EventFilter(
            execution_storage=self.execution_storage,
            subscription_storage=self.subscription_storage,
            redis_client=self.redis_client,
            event_bus=AsyncMock(),
            queue_name=self.QUEUE,
        )

    async def _setup_pending_async(self, execution_id="exec-r1",
                                   event_type="approval", flow_id="flow-r1"):
        """挂起执行 + 匹配订阅（协程版，可在已运行 loop 内调用）。"""
        self.execution_storage.save_execution_state(
            execution_id,
            ExecutionState(
                execution_id=execution_id, flow_id=flow_id, status="suspended",
                context={},
            ),
        )
        await self.subscription_storage.store_subscription(
            EventSubscription(
                event_type=event_type, correlation_id=execution_id,
                flow_id=flow_id,
            )
        )

    def _setup_pending(self, execution_id="exec-r1", event_type="approval",
                       flow_id="flow-r1"):
        """挂起执行 + 匹配订阅：事件一旦被处理即入队 resume。"""
        run(self._setup_pending_async(execution_id, event_type, flow_id))

    async def _store_event_directly_async(self, event_type="approval",
                                          execution_id="exec-r1", timestamp=None):
        """事件只进存储、不走 publish 的 Pub/Sub 面（协程版）。"""
        kwargs = {
            "event_type": event_type,
            "data": {"tenant_id": "default", "approved": True},
            "correlation_id": execution_id,
        }
        if timestamp is not None:
            kwargs["timestamp"] = timestamp
        event = Event(**kwargs)
        await self.event_storage.store_event(event)
        return event

    def _store_event_directly(self, event_type="approval", execution_id="exec-r1",
                              timestamp=None):
        """事件只进存储、不走 publish 的 Pub/Sub 面——模拟推送通知丢失。"""
        return run(
            self._store_event_directly_async(event_type, execution_id, timestamp)
        )

    def _make_reconciler(self, **kwargs):
        return EventReconciler(
            self.event_storage,
            self.event_filter,
            self.redis_client,
            **kwargs,
        )


class TestReconcileResumesMissedEvents(ReconcileTestBase):
    def test_stored_event_without_pubsub_gets_resume(self):
        """核心新能力：只进存储的事件（Pub/Sub 通知已丢）被回扫补偿成 resume。"""
        self._setup_pending()
        self._store_event_directly()

        # 修复前：无任何回扫，该挂起执行永远等不到 resume（行为不存在）
        reconciler = self._make_reconciler()
        replayed = run(reconciler.scan_once())

        self.assertEqual(replayed, 1, "存储中的事件未被回扫补偿")
        tasks = _stream_payloads(self.redis_client, self.QUEUE)
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0]["type"], "resume")
        self.assertEqual(tasks[0]["execution_id"], "exec-r1")
        self.assertEqual(tasks[0]["resume_type"], "event")

    def test_scan_twice_is_idempotent(self):
        """重扫不重复 resume（handle_event 的 SET NX 去重键兜底）。"""
        self._setup_pending()
        self._store_event_directly()

        reconciler = self._make_reconciler()
        run(reconciler.scan_once())
        run(reconciler.scan_once())

        tasks = _stream_payloads(self.redis_client, self.QUEUE)
        self.assertEqual(len(tasks), 1, f"回扫重复入队 resume: {len(tasks)} 条")

    def test_push_processed_event_not_duplicated_by_reconcile(self):
        """推送链路已处理的事件，回扫不再补刀（去重键跨链路生效）。"""
        self._setup_pending()
        event = self._store_event_directly()

        # 模拟推送链路已处理过该事件
        run(self.event_filter.handle_event(event))

        reconciler = self._make_reconciler()
        run(reconciler.scan_once())

        tasks = _stream_payloads(self.redis_client, self.QUEUE)
        self.assertEqual(len(tasks), 1, "推送已处理的事件被回扫重复 resume")

    def test_events_outside_backfill_window_skipped(self):
        """回填窗口（默认 1h，可配）之外的老事件不扫。"""
        self._setup_pending()
        self._store_event_directly(timestamp=time.time() - 7200)

        reconciler = self._make_reconciler(backfill_seconds=3600)
        replayed = run(reconciler.scan_once())

        self.assertEqual(replayed, 0)
        self.assertEqual(_stream_payloads(self.redis_client, self.QUEUE), [])

    def test_stale_cursor_capped_to_backfill_window(self):
        """游标过老（停机超过回填窗口）封顶到回填窗口，不无限追扫。"""
        self._setup_pending()
        self._store_event_directly(timestamp=time.time() - 7200)
        self.redis_client.set(
            EventReconciler.CURSOR_KEY, str(time.time() - 86400)
        )

        reconciler = self._make_reconciler(backfill_seconds=3600)
        replayed = run(reconciler.scan_once())

        self.assertEqual(replayed, 0, "游标封顶失效，扫到了回填窗口外的事件")

    def test_cursor_persisted_with_ttl(self):
        """游标落 Redis 且带 TTL（7 天自清理），跨重启避免重复扫。"""
        self._setup_pending()
        self._store_event_directly()

        reconciler = self._make_reconciler()
        run(reconciler.scan_once())

        raw = self.redis_client.get(EventReconciler.CURSOR_KEY)
        self.assertIsNotNone(raw, "游标未落 Redis")
        ttl = self.redis_client.ttl(EventReconciler.CURSOR_KEY)
        self.assertGreater(ttl, 0, "游标键未设置 TTL")

    def test_future_cursor_blocks_rescan(self):
        """游标指向未来（窗口为空）→ 不扫任何事件（游标生效的证据）。"""
        self._setup_pending()
        self._store_event_directly()
        self.redis_client.set(
            EventReconciler.CURSOR_KEY, str(time.time() + 600)
        )

        reconciler = self._make_reconciler()
        replayed = run(reconciler.scan_once())

        self.assertEqual(replayed, 0)
        self.assertEqual(_stream_payloads(self.redis_client, self.QUEUE), [])

    def test_cursor_readable_on_bytes_client(self):
        """生产 Redis.from_url 不带 decode_responses：游标读回 bytes 也要能解析。"""
        self._setup_pending()
        self._store_event_directly()
        bytes_client = fakeredis.FakeRedis(server=self.server)  # bytes 模式
        bytes_client.set(EventReconciler.CURSOR_KEY, str(time.time() + 600))

        reconciler = EventReconciler(
            self.event_storage, self.event_filter, bytes_client
        )
        replayed = run(reconciler.scan_once())

        self.assertEqual(replayed, 0, "bytes 游标解析失败导致回填窗口重扫")

    def test_storage_error_swallowed(self):
        """存储扫描异常吞掉打 warning，绝不外溢影响推送主链路。"""

        class _BrokenStorage:
            async def list_events(self, **kwargs):
                raise RuntimeError("redis down")

        reconciler = EventReconciler(
            _BrokenStorage(), self.event_filter, self.redis_client
        )
        replayed = run(reconciler.scan_once())
        self.assertEqual(replayed, 0)


class TestReconcileCursorSemantics(ReconcileTestBase):
    """游标推进语义（plaita#85）：单轮只消费 batch_size 条时，游标绝不越过
    未消费的事件——修复前游标无条件推到窗口上界，窗口内超过 batch_size 的
    事件（以及本轮回放失败的事件）被永久跳过。"""

    def _seed_window(self, n, base_ts=None):
        """n 个挂起执行 + n 条只落存储的事件（时间戳递增、间隔 1s）。"""
        base_ts = base_ts if base_ts is not None else time.time() - 100
        for i in range(n):
            exec_id = f"exec-c{i}"
            self._setup_pending(execution_id=exec_id)
            self._store_event_directly(
                execution_id=exec_id, timestamp=base_ts + i
            )
        return base_ts

    def test_full_batch_keeps_remaining_events_in_window(self):
        """窗口内事件数 > batch_size：分多轮全部补到，不因游标越界而漏扫。

        修复前：首轮扫 2 条后游标直推窗口上界，余下 3 条永失（只回放 2 条）。
        注：scan_once 返回值含边界重扫的去重命中，故以队列里的 resume 任务
        （每执行恰好一条）为准断言。
        """
        self._seed_window(5)
        reconciler = self._make_reconciler(batch_size=2)

        for _ in range(6):
            run(reconciler.scan_once())

        tasks = _stream_payloads(self.redis_client, self.QUEUE)
        self.assertEqual(
            len(tasks), 5, f"应恰好 5 条 resume（去重后），实际 {len(tasks)} 条"
        )
        execs = {t["execution_id"] for t in tasks}
        self.assertEqual(execs, {f"exec-c{i}" for i in range(5)},
                         "窗口内事件未全部补偿成 resume（游标越界漏扫）")

    def test_cursor_advances_to_batch_tail_not_window_end(self):
        """打满 batch_size 的批次：游标推到本批最晚时间戳，而非窗口上界。"""
        base_ts = self._seed_window(5)
        reconciler = self._make_reconciler(batch_size=2)
        run(reconciler.scan_once())

        raw = self.redis_client.get(EventReconciler.CURSOR_KEY)
        self.assertIsNotNone(raw)
        cursor = float(raw)
        self.assertAlmostEqual(cursor, base_ts + 1, delta=0.5,
                               msg="游标未推到本批（前 2 条）最晚时间戳")
        self.assertLess(cursor, base_ts + 2,
                        "游标越过本批落在未消费事件之后（越界推进）")

    def test_failed_replay_retried_next_round(self):
        """本轮回放失败的事件：游标停在失败位点，下轮重试成功。

        修复前：单事件失败被跳过后游标照推窗口上界，失败者永无重试。
        """
        base_ts = self._seed_window(3)
        target_ts = base_ts + 1  # 中间那条（失败位点早于同批后继成功者）

        stored = {}

        async def _collect():
            for ev in await self.event_storage.list_events(limit=100):
                stored[ev.event_id] = ev.timestamp

        run(_collect())
        target_id = next(
            (eid for eid, ts in stored.items() if abs(ts - target_ts) < 0.5),
            None,
        )
        self.assertIsNotNone(target_id)

        original = self.event_filter.handle_event

        async def _flaky(event):
            if event.event_id == target_id:
                raise RuntimeError("transient replay failure")
            return await original(event)

        self.event_filter.handle_event = _flaky
        reconciler = self._make_reconciler(batch_size=10)
        first = run(reconciler.scan_once())
        self.assertEqual(first, 2, "失败未隔离，拖累同批其余事件")

        # 第二轮：游标停在失败位点，失败者被重试并成功
        self.event_filter.handle_event = original
        second = run(reconciler.scan_once())
        self.assertGreaterEqual(second, 1, "失败事件未被下轮重试")
        execs = {t["execution_id"] for t in _stream_payloads(
            self.redis_client, self.QUEUE)}
        self.assertEqual(execs, {"exec-c0", "exec-c1", "exec-c2"})

    def test_all_failed_cursor_stalls(self):
        """整批全部回放失败：游标停在失败位点不越过（fail-stop），下轮整窗重试。"""
        base_ts = self._seed_window(2)

        async def _always_fail(event):
            raise RuntimeError("downstream down")

        self.event_filter.handle_event = _always_fail
        reconciler = self._make_reconciler(batch_size=10)
        self.assertEqual(run(reconciler.scan_once()), 0)

        raw = self.redis_client.get(EventReconciler.CURSOR_KEY)
        self.assertLessEqual(float(raw), base_ts + 1,
                             "整批失败后游标仍越过失败窗口推进")

        # 下游恢复：游标停在失败位点 ⇒ 两条都在窗口内被重试成功
        self.event_filter.handle_event = (
            lambda event: self.event_filter.__class__.handle_event(
                self.event_filter, event)
        )
        run(reconciler.scan_once())
        execs = {t["execution_id"] for t in _stream_payloads(
            self.redis_client, self.QUEUE)}
        self.assertEqual(execs, {"exec-c0", "exec-c1"},
                         "整批失败后事件未被下轮重试（fail-stop 失效）")


class TestReconcileLifecycle(ReconcileTestBase):
    def test_filter_start_mounts_and_stop_unmounts(self):
        """回扫器随 EventFilter.start/stop 同生命周期。"""

        async def _test():
            ef = self.event_filter
            # 挂载依赖可解析的事件存储（显式注入）
            ef.event_storage = self.event_storage
            task = asyncio.create_task(ef.start())
            await asyncio.sleep(0.05)
            self.assertIsNotNone(ef._reconciler, "EventFilter.start 未挂载事件回扫")
            self.assertTrue(ef._reconciler._running)

            await ef.stop()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            self.assertIsNone(ef._reconciler, "EventFilter.stop 未卸载事件回扫")

        run(_test())

    def test_filter_resolves_storage_from_event_bus(self):
        """未显式注入时回退探测 event_bus.event_storage（RedisEventBus 形态）。"""

        async def _test():
            ef = self.event_filter
            ef.event_bus = AsyncMock()
            ef.event_bus.event_storage = self.event_storage
            task = asyncio.create_task(ef.start())
            await asyncio.sleep(0.05)
            self.assertIsNotNone(ef._reconciler, "未从事件总线探测到事件存储")

            await ef.stop()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        run(_test())

    def test_disable_env_rolls_back(self):
        """回滚开关：PLAITA_DISABLE_EVENT_RECONCILE=1 时不挂载（回现状）。"""

        async def _test():
            import os

            ef = self.event_filter
            ef.event_storage = self.event_storage
            old = os.environ.get("PLAITA_DISABLE_EVENT_RECONCILE")
            os.environ["PLAITA_DISABLE_EVENT_RECONCILE"] = "1"
            try:
                task = asyncio.create_task(ef.start())
                await asyncio.sleep(0.05)
                self.assertIsNone(ef._reconciler, "回滚开关未生效")
                await ef.stop()
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            finally:
                if old is None:
                    os.environ.pop("PLAITA_DISABLE_EVENT_RECONCILE", None)
                else:
                    os.environ["PLAITA_DISABLE_EVENT_RECONCILE"] = old

        run(_test())

    def test_no_event_storage_no_mount(self):
        """内存总线且无事件存储可解析 → 不挂载（日志说明，不报错）。"""

        async def _test():
            ef = self.event_filter
            ef.event_bus = AsyncMock(spec=["register_handler", "unregister_handler"])
            task = asyncio.create_task(ef.start())
            await asyncio.sleep(0.05)
            self.assertIsNone(ef._reconciler)

            await ef.stop()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        run(_test())

    def test_periodic_loop_scans_repeatedly(self):
        """周期循环真实运转：短间隔下两轮各补偿一批事件。"""

        async def _test():
            await self._setup_pending_async(execution_id="exec-p1")
            await self._setup_pending_async(execution_id="exec-p2")
            # 第一批事件先落存储，启动回填扫掉；随后第二批触发下一周期
            await self._store_event_directly_async(execution_id="exec-p1")
            reconciler = self._make_reconciler(interval_seconds=0.05)
            await reconciler.start()
            await asyncio.sleep(0.1)
            await self._store_event_directly_async(execution_id="exec-p2")
            await asyncio.sleep(0.2)
            await reconciler.stop()

            tasks = _stream_payloads(self.redis_client, self.QUEUE)
            execs = {t["execution_id"] for t in tasks}
            self.assertEqual(
                execs, {"exec-p1", "exec-p2"},
                f"周期回扫未覆盖两批事件: {execs}",
            )

        run(_test())


if __name__ == "__main__":
    unittest.main()
