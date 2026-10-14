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
import logging
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


class _LogCapture(logging.Handler):
    """挂在指定 logger 上的内存 handler（观察「失败只剩日志」这一形态）。"""

    def __init__(self):
        super().__init__()
        self.lines = []

    def emit(self, record):
        self.lines.append(record.getMessage())


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

    def test_periodic_loop_survives_failed_round(self):
        """#153：失败轮上抛不得终止回扫循环——`_run_loop` 记日志后继续下一轮。

        对象：`scan_once` 被调用 ≥2 次（失败轮之后循环仍在转），且恢复轮把
        该事件真正补偿进 stream（1 条 resume）。
        """

        async def _test():
            await self._setup_pending_async()
            await self._store_event_directly_async()
            reconciler = self._make_reconciler(interval_seconds=0.05)

            calls = {"n": 0}
            real_scan = reconciler.scan_once

            async def _flaky_scan():
                calls["n"] += 1
                if calls["n"] == 1:
                    raise ConnectionError("simulated transient Redis outage")
                return await real_scan()

            reconciler.scan_once = _flaky_scan
            await reconciler.start()
            await asyncio.sleep(0.3)
            await reconciler.stop()

            self.assertGreaterEqual(
                calls["n"], 2, "失败轮后回扫循环没有继续（单轮异常终止了循环）"
            )
            self.assertEqual(
                len(_stream_payloads(self.redis_client, self.QUEUE)), 1,
                "失败后的恢复轮没有补偿该事件",
            )

        run(_test())


if __name__ == "__main__":
    unittest.main()


class TestEnqueueFailureReleasesDedupKey(ReconcileTestBase):
    """plaita#87：入队失败必须归还去重键，否则该事件的 resume 永久丢失。

    规则：``handle_event`` / ``_on_subscription_timeout`` 中「占去重键 → 入队」
    两步，入队抛错时**删除刚占的键**（成功路径保持键，多实例 NX 去重不变）。

    #153 起失败可见性升级（措辞同步钉死）：``handle_event`` 最外层 ``except``
    记 ERROR 后**继续上抛**（``test_handle_event_failure_propagates_to_caller``）；
    超时回调包装 ``event/timeout.py`` 同样记 ERROR 后**聚合上抛**
    （``TimeoutCallbackError``，单个回调失败不中断本轮）。可重放性依旧来自
    **释放去重键**。
    """

    def test_failed_enqueue_releases_dedup_key_and_replay_enqueues(self):
        self._setup_pending()
        event = self._store_event_directly()

        real_xadd = self.redis_client.xadd

        def broken_xadd(*args, **kwargs):
            raise ConnectionError("simulated transient Redis outage")

        self.redis_client.xadd = broken_xadd
        try:
            with pytest.raises(ConnectionError):
                # #153：失败上抛（最外层 except 记 ERROR 后 raise），不再被吞
                run(self.event_filter.handle_event(event))
        finally:
            self.redis_client.xadd = real_xadd

        # 失败后：去重键不得残留（否则重放被 NX 挡掉），stream 为空
        assert self.redis_client.keys("plaita:event_filter:dedup:*") == []
        assert _stream_payloads(self.redis_client, self.QUEUE) == []

        # Redis 恢复后重放同一事件 → 真正入队一条 resume
        run(self.event_filter.handle_event(event))
        tasks = _stream_payloads(self.redis_client, self.QUEUE)
        assert len(tasks) == 1
        assert tasks[0]["type"] == "resume"
        assert tasks[0]["execution_id"] == "exec-r1"
        # 反向断言：成功入队仍保留去重键（多实例幂等语义不变）
        assert len(self.redis_client.keys("plaita:event_filter:dedup:*")) == 1

    def test_failed_timeout_enqueue_releases_dedup_key(self):
        """超时链路同款：#87 的第二条入队路径。"""
        self._setup_pending()
        subs = run(self.subscription_storage.list_subscriptions())
        subscription = subs[0]

        real_xadd = self.redis_client.xadd

        def broken_xadd(*args, **kwargs):
            raise ConnectionError("simulated transient Redis outage")

        self.redis_client.xadd = broken_xadd
        try:
            # 失败**上抛到直接调用方**（生产里 timeout.py 的回调包装记 ERROR 后
            # 继续上抛给 _check_loop），关键是键已归还，下一轮 checker 能真正入队。
            with pytest.raises(ConnectionError):
                run(self.event_filter._on_subscription_timeout(subscription))
        finally:
            self.redis_client.xadd = real_xadd

        assert self.redis_client.keys("plaita:event_filter:timeout:*") == []
        assert _stream_payloads(self.redis_client, self.QUEUE) == []

        run(self.event_filter._on_subscription_timeout(subscription))
        tasks = _stream_payloads(self.redis_client, self.QUEUE)
        assert len(tasks) == 1
        assert tasks[0]["type"] == "resume"
        assert tasks[0]["resume_type"] == "timeout"
        assert len(self.redis_client.keys("plaita:event_filter:timeout:*")) == 1

    def test_handle_event_failure_propagates_to_caller(self):
        """#153：``handle_event`` 的入队失败**必须**被调用方观察到（上抛）。

        #87 r4 曾钉住"被最外层 except 吞掉"的旧语义；#153 判定那是错误语义：
        回扫器正是靠这层上抛才不会把失败计成 replayed、才不推进游标。这里同时
        钉住"响亮"（ERROR 日志仍在）与"键已释放"（重放前提不变）。
        """
        self._setup_pending()
        event = self._store_event_directly()
        real_xadd = self.redis_client.xadd

        def broken_xadd(*args, **kwargs):
            raise ConnectionError("simulated transient Redis outage")

        lg = logging.getLogger("plaita.server.event_filter")
        prev_level = lg.level
        cap = _LogCapture()
        lg.addHandler(cap)
        self.redis_client.xadd = broken_xadd
        try:
            lg.setLevel(logging.DEBUG)          # 保证 ERROR 记录一定被创建
            # 对象前置：失败真实发生（下面断言键已释放 + stream 空），
            # 且异常**逃出** handle_event ⇒ 调用方可观察。
            with pytest.raises(ConnectionError):
                run(self.event_filter.handle_event(event))
        finally:
            self.redis_client.xadd = real_xadd
            lg.removeHandler(cap)
            lg.setLevel(prev_level)

        assert self.redis_client.keys("plaita:event_filter:dedup:*") == []
        assert _stream_payloads(self.redis_client, self.QUEUE) == []
        # 响亮 + 上抛并存：失败不是纯静默
        self.assertTrue(cap.lines, "失败既不上抛也没有日志（纯静默）")
        self.assertTrue(any("处理事件出错" in line for line in cap.lines), cap.lines)


class TestScanOnceFailureKeepsCursorAndCountsRejected(ReconcileTestBase):
    """plaita#153：回扫轮的失败不得计成 replayed，也不得推进游标。

    规则（独立谓词）：``scan_once`` 回放任一事件失败 ⇒ **该失败事件不算
    replayed**（真实不变式；首事件即失败时 ``last_replayed == 0``，失败前已
    成功回放的事件仍计入 ``last_replayed``）、``last_rejected >= 1``、游标
    **逐字节不变**、异常上抛；Redis 恢复后下一轮把失败事件真正重放
    （``stream`` 相应 +1）。
    """

    @staticmethod
    def _break_xadd(redis_client):
        real = redis_client.xadd

        def broken(*args, **kwargs):
            raise ConnectionError("simulated transient Redis outage")

        redis_client.xadd = broken
        return real

    def test_failed_round_does_not_advance_cursor_and_next_round_replays(self):
        self._setup_pending()
        cursor0 = time.time()
        self.redis_client.set(EventReconciler.CURSOR_KEY, str(cursor0))
        self._store_event_directly(timestamp=time.time())   # 落在 (cursor0, now]
        reconciler = self._make_reconciler(backfill_seconds=3600)

        real = self._break_xadd(self.redis_client)
        try:
            with pytest.raises(ConnectionError):
                run(reconciler.scan_once())
        finally:
            self.redis_client.xadd = real

        # 对象：失败真实发生（键已释放、stream 空）+ 计数 0/1
        self.assertEqual(self.redis_client.keys("plaita:event_filter:dedup:*"), [])
        self.assertEqual(_stream_payloads(self.redis_client, self.QUEUE), [])
        self.assertEqual(reconciler.last_replayed, 0)
        self.assertEqual(reconciler.last_rejected, 1)
        # 核心断言：游标逐字节不变（旧行为在这里推进到 window_end）
        self.assertEqual(
            self.redis_client.get(EventReconciler.CURSOR_KEY), str(cursor0),
            "失败轮推进了游标 ⇒ 失败事件被永久跳过",
        )

        # Redis 恢复 → 下一轮真正重放（失败事件仍在窗口内）
        replayed = run(reconciler.scan_once())
        self.assertEqual(replayed, 1)
        tasks = _stream_payloads(self.redis_client, self.QUEUE)
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0]["type"], "resume")
        self.assertEqual(tasks[0]["execution_id"], "exec-r1")
        self.assertEqual(reconciler.last_replayed, 1)
        self.assertEqual(reconciler.last_rejected, 0)

    def test_multi_event_failing_round_keeps_successful_replay_counted(self):
        """#153/R2：多事件轮**首个成功、其后失败** ⇒ 计数是 (1, 1)，不是 (0, 1)。

        r4 的交付措辞把「失败事件不计成 replayed」误写成「``last_replayed == 0``
        by construction」——那只在**首个事件即失败**时成立（verifier 实测多事件
        轮 ``last_replayed=1, last_rejected=1``）。本用例钉死真实不变式：失败
        事件不计 replayed、失败前已成功的回放仍计入、游标逐字节不变、已成功的
        早先事件 stream 里确有 1 条。
        """
        self._setup_pending(execution_id="exec-r1")
        self._setup_pending(execution_id="exec-r2")
        cursor0 = time.time()
        self.redis_client.set(EventReconciler.CURSOR_KEY, str(cursor0))
        self._store_event_directly(execution_id="exec-r1", timestamp=time.time())
        time.sleep(0.02)
        self._store_event_directly(execution_id="exec-r2", timestamp=time.time())
        reconciler = self._make_reconciler(backfill_seconds=3600)

        # 只让**第二个**事件的入队失败（首个事件真实入队成功）
        real_xadd = self.redis_client.xadd
        calls = {"n": 0}

        def flaky_xadd(*args, **kwargs):
            calls["n"] += 1
            if calls["n"] >= 2:
                raise ConnectionError("simulated transient Redis outage")
            return real_xadd(*args, **kwargs)

        self.redis_client.xadd = flaky_xadd
        try:
            with pytest.raises(ConnectionError):
                run(reconciler.scan_once())
        finally:
            self.redis_client.xadd = real_xadd

        # 前置对象：两个事件都真的被尝试回放（否则下面的计数断言空转）
        self.assertEqual(calls["n"], 2, "第二个事件没有被尝试回放")
        # 真实不变式：失败事件不计 replayed，失败前的成功回放照算 ⇒ (1, 1)
        self.assertEqual(
            (reconciler.last_replayed, reconciler.last_rejected), (1, 1),
            "多事件失败轮计数错误（把 last_replayed 误当恒 0？）",
        )
        # 游标逐字节不变
        self.assertEqual(
            self.redis_client.get(EventReconciler.CURSOR_KEY), str(cursor0),
            "多事件失败轮推进了游标",
        )
        # 早先成功的事件确已入队（1 条），失败事件未入队
        tasks = _stream_payloads(self.redis_client, self.QUEUE)
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0]["execution_id"], "exec-r1")

        # 恢复轮：游标未动 ⇒ 窗口仍覆盖两个事件；exec-r1 被去重键挡住（不重复
        # 入队），exec-r2 真正重放 ⇒ 本轮 handle_event 计 2 条（新入队 1 条）。
        self.assertEqual(run(reconciler.scan_once()), 2)
        self.assertEqual(
            (reconciler.last_replayed, reconciler.last_rejected), (2, 0)
        )
        tasks = _stream_payloads(self.redis_client, self.QUEUE)
        self.assertEqual(len(tasks), 2, "恢复轮没有把失败事件真正重放（或多入了）")
        execs = {t["execution_id"] for t in tasks}
        self.assertEqual(execs, {"exec-r1", "exec-r2"})

    def test_rejected_counter_is_per_round(self):
        """计数按轮重置：成功轮 (1,0) → 失败轮 (0,1) → 恢复轮 (1,0)。"""
        self._setup_pending()
        self._store_event_directly()
        reconciler = self._make_reconciler()

        # 成功轮（对象：真的入队一条）
        self.assertEqual(run(reconciler.scan_once()), 1)
        self.assertEqual(
            (reconciler.last_replayed, reconciler.last_rejected), (1, 0)
        )
        self.assertEqual(len(_stream_payloads(self.redis_client, self.QUEUE)), 1)

        # 第二个事件 + xadd 瞬断：本轮窗口内只有这个失败事件 ⇒ rejected=1 / replayed=0
        time.sleep(0.02)
        self._store_event_directly()
        real = self._break_xadd(self.redis_client)
        try:
            with pytest.raises(ConnectionError):
                run(reconciler.scan_once())
        finally:
            self.redis_client.xadd = real

        self.assertEqual(
            (reconciler.last_replayed, reconciler.last_rejected), (0, 1),
            "失败轮计数错误（失败事件被计成 replayed，或 rejected 未计数）",
        )
        self.assertEqual(len(_stream_payloads(self.redis_client, self.QUEUE)), 1)

        # 恢复轮：失败事件补入队，计数回到 (1,0)
        self.assertEqual(run(reconciler.scan_once()), 1)
        self.assertEqual(
            (reconciler.last_replayed, reconciler.last_rejected), (1, 0)
        )
        self.assertEqual(len(_stream_payloads(self.redis_client, self.QUEUE)), 2)
