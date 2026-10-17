"""plaita#85 事件回扫 no-loss —— **route C 最小实现**的回归面（子集）。

本文件只覆盖已经落在 current main 上的 #85 最小修复所对应的关注点：

* S2 复合游标严格推进（batch_size=1 也能越过边界事件）；
* S3 回放/入队失败必须对调用方可观察（不再被 ``handle_event`` 吞掉）；
* S4 ``batch_size <= 0`` 守卫（不取事件、不推进游标）；
* S6 memory 后端 ``list_events`` 全局时间有序；
* S7 索引成员载荷缺失必须 ERROR 响亮（不是 debug 静默）；
* S8 游标单调（拒绝回退）；
* AD5 游标不得越过窗口上界（时钟超前的伪造事件也不行）；
* AH3 游标写失败必须 ERROR 响亮。

**这不是** `fix/79-85` 分支上那套完整加固套件（隔离清单 / 页契约 / 自愈 /
看门狗 / 排空确认，`event_reconcile.py` 2083 行）。后者尚未落地；其变异定义
（``m13``/``m17``/``m23``/``m25``/… 共 53 条）已连同原因归档在
``scripts/mutation_replay/mutations/parked/``。类名沿用分支命名是为了让
**已重锚**的变异定义（``--one``）能原样重放；不要据此推断完整套件已存在。
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Optional

import pytest

from plaita.event.core import Event, EventSubscription
from plaita.event.memory import (
    InMemoryEventSubscriptionStorage,
    MemoryEventStorage,
)
from plaita.server.event_reconcile import EventReconciler
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage

BASE_TS = 1_700_000_000.0


def run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _event(event_id: str, timestamp: float, event_type: str = "approval",
           correlation_id: Optional[str] = None) -> Event:
    return Event(
        event_type=event_type,
        data={"tenant_id": "default"},
        source="test",
        event_id=event_id,
        timestamp=timestamp,
        correlation_id=correlation_id,
    )


class RecordingFilter:
    """最小 ``handle_event`` 替身：记录每个被回放的事件 id。"""

    def __init__(self):
        self.seen = []

    async def handle_event(self, event):
        self.seen.append(event.event_id)


class FakeRedis:
    """同步游标客户端（get/set 足够；set 失败可被子类覆写）。"""

    def __init__(self):
        self.kv = {}

    def get(self, key):
        return self.kv.get(key)

    def set(self, key, value, ex=None):
        self.kv[key] = value
        return True

    def delete(self, key):
        return 1 if self.kv.pop(key, None) is not None else 0


class _BrokenSetRedis(FakeRedis):
    def set(self, key, value, ex=None):
        raise ConnectionError("simulated cursor write outage")


class _LogCapture(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records = []

    def emit(self, record):
        self.records.append(record)


def _reconciler(store, filt=None, redis=None, **kwargs):
    return EventReconciler(
        store,
        filt if filt is not None else RecordingFilter(),
        redis if redis is not None else FakeRedis(),
        **kwargs,
    )


class TestS2CompositeCursor:
    def test_batch_size_one_progresses_over_distinct_timestamps(self):
        """``batch_size=1``：边界事件（游标那条）不得被当成待处理，否则活锁。"""

        async def _t():
            base = time.time() - 100
            store = MemoryEventStorage()
            for i in range(3):
                await store.store_event(_event(f"e{i}", base + i))
            filt = RecordingFilter()
            r = _reconciler(
                store, filt, backfill_seconds=3600, batch_size=1,
            )
            for _ in range(5):
                await r.scan_once()
            assert sorted(set(filt.seen)) == ["e0", "e1", "e2"], (
                f"batch_size=1 无法越过边界事件: {filt.seen}"
            )

        run(_t())


class TestS3ReplayFailureVisible:
    def test_enqueue_failure_is_visible_and_retried(self):
        """入队失败必须逃出 ``handle_event``（回扫器据此不推进游标）。"""
        fakeredis = pytest.importorskip("fakeredis")
        import fakeredis.aioredis

        from plaita.event.redis import RedisEventStorage
        from plaita.server.event_filter import EventFilter

        server = fakeredis.FakeServer()
        redis_client = fakeredis.FakeRedis(server=server, decode_responses=True)
        storage = RedisEventStorage(redis_url="redis://localhost:6379/15", ttl=3600)
        storage.redis = fakeredis.aioredis.FakeRedis(
            server=server, decode_responses=True
        )
        execution_storage = MemoryExecutionStorage()
        subscription_storage = InMemoryEventSubscriptionStorage()

        async def _setup():
            execution_storage.save_execution_state(
                "exec-s3",
                ExecutionState(
                    execution_id="exec-s3", flow_id="flow-s3", status="suspended",
                    context={},
                ),
            )
            await subscription_storage.store_subscription(
                EventSubscription(
                    event_type="approval", correlation_id="exec-s3",
                    flow_id="flow-s3",
                )
            )
            await storage.store_event(
                _event("ev-s3", BASE_TS + 1, correlation_id="exec-s3")
            )

        run(_setup())
        event_filter = EventFilter(
            execution_storage=execution_storage,
            subscription_storage=subscription_storage,
            redis_client=redis_client,
            event_bus=None,
            queue_name="test:reconcile:s3",
        )

        real_xadd = redis_client.xadd

        def broken_xadd(*args, **kwargs):
            raise ConnectionError("simulated transient Redis outage")

        redis_client.xadd = broken_xadd
        try:
            with pytest.raises(ConnectionError):
                run(event_filter.handle_event(
                    _event("ev-s3", BASE_TS + 1, correlation_id="exec-s3")
                ))
        finally:
            redis_client.xadd = real_xadd

        # 去重键已归还，Redis 恢复后能真正重放（可重放性）
        assert redis_client.keys("plaita:event_filter:dedup:*") == []
        run(event_filter.handle_event(
            _event("ev-s3", BASE_TS + 1, correlation_id="exec-s3")
        ))
        entries = redis_client.xrange("test:reconcile:s3", min="-", max="+")
        assert len(entries) == 1, "失败后重放没有真正入队 resume"


class TestS4BatchSizeGuard:
    def test_batch_size_zero_no_crash_no_scan(self):
        """``batch_size=0``：不取事件、不推进游标（旧实现会把游标推到窗口上界）。"""

        async def _t():
            store = MemoryEventStorage()
            await store.store_event(_event("e0", BASE_TS + 1))
            redis = FakeRedis()
            filt = RecordingFilter()
            r = _reconciler(
                store, filt, redis, backfill_seconds=3600, batch_size=0,
            )
            replayed = await r.scan_once()
            assert replayed == 0
            assert filt.seen == []
            assert EventReconciler.CURSOR_KEY not in redis.kv, (
                "batch_size=0 的轮次推进了游标（整窗被跳过）"
            )

        run(_t())


class TestS6MemoryStorageOrdering:
    def test_memory_storage_list_events_is_time_ordered(self):
        """memory 后端与 Redis 后端同序：按 ``(timestamp, event_id)`` 全局升序。"""

        async def _t():
            store = MemoryEventStorage()
            for i in (2, 0, 1):
                await store.store_event(_event(f"e{i}", BASE_TS + i))
            got = await store.list_events()
            assert [e.event_id for e in got] == ["e0", "e1", "e2"], (
                f"memory list_events 未全局时间有序: {[e.event_id for e in got]}"
            )

        run(_t())

    def test_reconciler_recovers_all_with_out_of_order_insertion(self):
        """乱序写入 + ``batch_size=2``：回扫必须把 5 条全部补偿。"""

        async def _t():
            base = time.time() - 100
            store = MemoryEventStorage()
            for i in (3, 1, 4, 0, 2):
                await store.store_event(_event(f"e{i}", base + i))
            filt = RecordingFilter()
            r = _reconciler(store, filt, backfill_seconds=3600, batch_size=2)
            for _ in range(5):
                await r.scan_once()
            assert sorted(set(filt.seen)) == [f"e{i}" for i in range(5)], (
                f"乱序写入下回扫漏事件: {sorted(set(filt.seen))}"
            )

        run(_t())


class TestS7LoudMissingPayload:
    def test_missing_payload_logs_error_with_event_id(self):
        """索引里有 member、载荷缺失：必须 ERROR 且带 event_id（不是 debug 静默）。"""
        fakeredis = pytest.importorskip("fakeredis")
        import fakeredis.aioredis

        from plaita.event.redis import RedisEventStorage

        async def _t():
            server = fakeredis.FakeServer()
            storage = RedisEventStorage(
                redis_url="redis://localhost:6379/15", ttl=3600
            )
            storage.redis = fakeredis.aioredis.FakeRedis(
                server=server, decode_responses=False
            )
            await storage.redis.zadd(
                f"{storage.key_prefix}types:ghost", {"ghost-event-id": BASE_TS}
            )
            logger = logging.getLogger("plaita.event.redis")
            capture = _LogCapture()
            previous = logger.level
            logger.addHandler(capture)
            logger.setLevel(logging.DEBUG)
            try:
                got = await storage.list_events(limit=10)
            finally:
                logger.removeHandler(capture)
                logger.setLevel(previous)
            assert got == []
            errors = [
                record for record in capture.records
                if record.levelno == logging.ERROR
                and "ghost-event-id" in record.getMessage()
            ]
            assert errors, (
                "载荷缺失没有 ERROR 响亮日志（带 event_id）: "
                f"{[(r.levelname, r.getMessage()) for r in capture.records]}"
            )

        run(_t())


class TestS8CursorNoCas:
    def test_save_cursor_refuses_rollback(self):
        """游标单调：陈旧写者不得把游标推回去（会整窗重扫）。"""
        redis = FakeRedis()
        r = _reconciler(MemoryEventStorage(), redis=redis, backfill_seconds=3600)
        redis.kv[EventReconciler.CURSOR_KEY] = repr(BASE_TS + 100)
        r._save_cursor(BASE_TS)
        assert redis.kv[EventReconciler.CURSOR_KEY] == repr(BASE_TS + 100), (
            "回退游标未被拒绝"
        )


class TestAD5WindowEndClamp:
    def test_forged_skipped_cannot_push_cursor_beyond_window(self):
        """事件时间戳越过窗口上界时，游标只能夹到 ``window_end``。

        伪造的存储返回一条「未来」事件（``timestamp > window_end``）；若游标
        被推到那个未来位点，窗口起点就跳到未来，之后**所有**正常事件都不再被
        任何窗口覆盖（永久漏扫）。
        """

        class _ForgeOnceStorage:
            def __init__(self):
                self.calls = 0

            async def list_events(self, start_time=None, end_time=None, limit=100):
                self.calls += 1
                if self.calls == 1:
                    return [_event("future", 2000.0)]
                return [_event("normal", 1000.2)]

        async def _t():
            storage = _ForgeOnceStorage()
            filt = RecordingFilter()
            r = _reconciler(storage, filt, backfill_seconds=3600, batch_size=1)
            from unittest.mock import patch

            with patch(
                "plaita.server.event_reconcile.time.time",
                side_effect=[1000.0, 1000.5],
            ):
                await r.scan_once()
                await r.scan_once()
            assert "normal" in filt.seen, (
                "游标被未来事件推到窗口上界之外，正常事件永久漏扫"
            )

        run(_t())


class TestAH3CursorWriteLoud:
    def test_cursor_write_failure_is_loud_with_ids(self):
        """游标写失败必须 ERROR 响亮，并说明后果（下一轮重扫、去重键幂等）。"""

        redis = _BrokenSetRedis()
        r = _reconciler(MemoryEventStorage(), redis=redis, backfill_seconds=3600)
        logger = logging.getLogger("plaita.server.event_reconcile")
        capture = _LogCapture()
        previous = logger.level
        logger.addHandler(capture)
        logger.setLevel(logging.DEBUG)
        try:
            r._save_cursor(BASE_TS)
        finally:
            logger.removeHandler(capture)
            logger.setLevel(previous)
        errors = [
            record.getMessage() for record in capture.records
            if record.levelno == logging.ERROR
        ]
        assert errors, f"游标写失败没有 ERROR: {[r.getMessage() for r in capture.records]}"
        message = errors[0]
        assert "重扫" in message and "去重" in message, (
            f"游标写失败的 ERROR 没有说明后果: {message!r}"
        )
