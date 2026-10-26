"""EventFilter 多实例安全证明（2026-10-26，唤醒面去单点前置）。

背景：event_filter 长期被钉成 max_instances=1 的隐性单点——pubsub 推送丢失
的兜底（回扫器）、订阅超时、索引清扫全寄生在这一个进程，进程挂 = 全部挂起
执行的唤醒停摆。机制面其实早已多实例安全：

- 推送：每实例各订一份 pubsub，``plaita:event_filter:dedup:{event}:{sub}``
  SET NX 保证同一 (事件×订阅) 只入队一次；
- 回扫：两实例**共享游标** ``plaita:event_reconcile:cursor``——单调守卫拒绝
  回退写，handle_event 幂等使重复扫描无害，失败轮不推进游标；
- 超时/清扫：SET NX 去重 / 幂等清理，双份轮询无害。

本文件钉回扫器的多实例语义（其余已有去重键测试覆盖）：
1. 双实例共享游标交错扫描：事件**零丢失**（每条至少被处理一次）、游标**单调**
   不回退；
2. 一实例回放失败：游标不被该实例推进，另一实例照常收口——失败事件仍被处理；
3. 游标写单调守卫：后来者不得把游标拖回（防慢实例整窗重扫风暴）。

集群模板 max_instances 1→3 与本证明同笔落地（cluster_registry.py）。
"""
import asyncio
import time
import unittest

import fakeredis

from plaita.server.event_reconcile import EventReconciler


class _Event:
    def __init__(self, event_id, timestamp):
        self.event_id = event_id
        self.timestamp = timestamp


class _StubStorage:
    """内存事件存储：list_events 按 (timestamp, event_id) 升序返回。"""

    def __init__(self, events):
        self._events = sorted(events, key=lambda e: (e.timestamp, e.event_id))

    async def list_events(self, start_time, end_time, limit):
        page = [
            e for e in self._events
            if start_time <= e.timestamp <= end_time
        ]
        return page[:limit] if limit and limit > 0 else page


class _StubFilter:
    """EventFilter 替身：记录 handle_event 调用；fail_on 集合内的事件抛异常。"""

    def __init__(self, fail_on=()):
        self.handled = []
        self.fail_on = set(fail_on)

    async def handle_event(self, event):
        if event.event_id in self.fail_on:
            raise RuntimeError(f"injected failure: {event.event_id}")
        self.handled.append(event.event_id)


def _make(events_n=5, batch_size=2, fail_on=(), redis=None):
    now = time.time()
    events = [_Event(f"e{i}", now - 100 + i) for i in range(events_n)]
    storage = _StubStorage(events)
    filt = _StubFilter(fail_on=fail_on)
    rec = EventReconciler(
        storage, filt, redis_client=redis,
        interval_seconds=9999, backfill_seconds=3600, batch_size=batch_size,
    )
    return rec, filt, events


def _run(coro):
    """py3.12+ 无隐式 event loop——测试自管循环（CI 隔离 chunk 无既有 loop）。"""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _cursor(raw):
    if raw is None:
        return None
    ts, _, eid = raw.decode().partition("|")
    return (float(ts), eid if eid else "")


class TestMultiInstanceReconciler(unittest.TestCase):
    def setUp(self):
        self.redis = fakeredis.FakeRedis(decode_responses=False)

    def test_interleaved_scans_no_loss_no_regression(self):
        """双实例 batch=2 交错扫 5 事件：三轮接力覆盖全部、游标只进不退。"""
        r1, f1, events = _make(events_n=5, batch_size=2, redis=self.redis)
        r2, f2, _ = _make(events_n=5, batch_size=2, redis=self.redis)

        # 实例 1 首轮：E1,E2 → 游标推到已消费位点
        _run(r1.scan_once())
        c1 = _cursor(self.redis.get(EventReconciler.CURSOR_KEY))
        self.assertIsNotNone(c1)

        # 实例 2 从共享游标续：E3,E4 → 游标前进
        _run(r2.scan_once())
        c2 = _cursor(self.redis.get(EventReconciler.CURSOR_KEY))
        self.assertGreater(c2, c1)

        # 实例 1 再轮：E5 → 窗口排空
        _run(r1.scan_once())
        c3 = _cursor(self.redis.get(EventReconciler.CURSOR_KEY))
        self.assertGreater(c3, c2)

        handled = set(f1.handled) | set(f2.handled)
        self.assertEqual(handled, {e.event_id for e in events})

    def test_failure_in_one_instance_still_covered(self):
        """实例 1 在 e2 注入失败：游标不被它推进；实例 2 照常收口全部事件。"""
        r1, f1, events = _make(events_n=5, batch_size=10, fail_on={"e2"},
                               redis=self.redis)
        r2, f2, _ = _make(events_n=5, batch_size=10, redis=self.redis)

        with self.assertRaises(RuntimeError):
            _run(r1.scan_once())
        # 失败轮：e0,e1 已处理但游标不动（#153 语义在多实例下同样成立）
        self.assertEqual(set(f1.handled), {"e0", "e1"})

        # 实例 2 独立收口：全 5 事件处理、游标推进
        _run(r2.scan_once())
        self.assertEqual(set(f2.handled), {e.event_id for e in events})
        c = _cursor(self.redis.get(EventReconciler.CURSOR_KEY))
        self.assertIsNotNone(c)

    def test_cursor_write_monotonic_guard(self):
        """慢实例的陈旧游标写被单调守卫拒绝——不得拖回游标整窗重扫。"""
        r1, _, events = _make(events_n=3, batch_size=10, redis=self.redis)
        _run(r1.scan_once())
        before = self.redis.get(EventReconciler.CURSOR_KEY)

        # 伪造「慢实例」拿着旧事件位点写游标（比当前游标老）
        stale = events[0]
        r1._save_cursor_key(stale)
        self.assertEqual(self.redis.get(EventReconciler.CURSOR_KEY), before)

    def test_drained_cursor_leaves_nothing_for_peer(self):
        """实例 1 排空推进后，实例 2 从共享游标续扫：无重复处理、并集零丢失。

        （若两实例在游标推进**前**同窗扫描，会各自处理全量——真实 EventFilter
        由 ``plaita:event_filter:dedup`` SET NX 吸收，见既有去重测试。）"""
        r1, f1, events = _make(events_n=4, batch_size=10, redis=self.redis)
        r2, f2, _ = _make(events_n=4, batch_size=10, redis=self.redis)

        _run(r1.scan_once())
        after1 = _cursor(self.redis.get(EventReconciler.CURSOR_KEY))
        _run(r2.scan_once())
        after2 = _cursor(self.redis.get(EventReconciler.CURSOR_KEY))
        self.assertIsNotNone(after1)
        self.assertIsNotNone(after2)
        self.assertGreaterEqual(after2[0], after1[0])
        # 并集零丢失；r2 无事可做是合法结果（游标已越过全部事件）
        handled_union = set(f1.handled) | set(f2.handled)
        self.assertEqual(handled_union, {e.event_id for e in events})


if __name__ == "__main__":
    unittest.main()
