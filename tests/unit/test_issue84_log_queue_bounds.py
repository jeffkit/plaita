"""plaita#84 —— RedisStreamHandler 客户端队列**有界**、丢弃**可观测**、告警**不自噬**。

规则（一句可判定谓词，**独立于实现**）：
* 入队后 `len(queue) <= max_queue`（默认 `DEFAULT_MAX_QUEUE=10000`）；
* 全部刷新失败时，`dropped == max(0, emitted - max_queue)`，且每次丢弃都有
  **审计告警**（`DROP_WARN_EVERY` 条聚合一次）；
* 刷新失败回灌同样受上限约束：`len(queue) <= max_queue`；
* 审计告警走 `plaita.server.log_handler.audit`（`propagate=False`）⇒ **不得**被
  本 handler 收进队列（自噬 delta == 0）；
* 恢复（≤ 上限的正常流量 + 成功刷新）⇒ `queue == 0 且 dropped == 0`（一条不丢）。
"""

from __future__ import annotations

import logging
import unittest

from plaita.server.log_handler import RedisStreamHandler, _audit_logger


class _FailingRedis:
    def pipeline(self):
        raise RuntimeError("redis down")


class _WorkingRedis:
    def __init__(self):
        self.commands = 0

    def pipeline(self):
        return _Pipe(self)


class _Pipe:
    def __init__(self, client):
        self.client = client

    def xadd(self, *a, **kw):
        self.client.commands += 1
        return self

    def publish(self, *a, **kw):
        self.client.commands += 1
        return self

    def execute(self):
        return []


def _handler(redis, max_queue=RedisStreamHandler.DEFAULT_MAX_QUEUE):
    h = RedisStreamHandler(redis, "flow_worker", "i1", max_queue=max_queue)
    h._stop_event.set()                      # 停后台线程，避免与断言竞争
    if h._flush_thread:
        h._flush_thread.join(timeout=3)
    return h


def _record(msg="hello"):
    return logging.LogRecord("plaita", logging.INFO, __file__, 1, msg, None, None)


class _AuditCapture(logging.Handler):
    def __init__(self):
        super().__init__()
        self.lines = []

    def emit(self, record):
        self.lines.append(record.getMessage())


class TestIssue84QueueBounds(unittest.TestCase):
    def test_queue_is_bounded_and_drops_counted(self):
        h = _handler(_FailingRedis(), max_queue=100)
        cap = _AuditCapture()
        _audit_logger.addHandler(cap)
        try:
            for _ in range(1000):
                h.emit(_record())
        finally:
            _audit_logger.removeHandler(cap)
        self.assertLessEqual(len(h._queue), h.max_queue)
        self.assertEqual(h.dropped_count, 900)          # 独立谓词：1000 - 100
        self.assertTrue(cap.lines, "丢弃没有任何审计告警（静默丢弃）")
        self.assertTrue(any("丢弃" in line for line in cap.lines), cap.lines[:2])

    def test_failure_reinjection_is_bounded(self):
        h = _handler(_FailingRedis(), max_queue=100)
        for _ in range(100):
            h.emit(_record())
        h._flush()                                       # 失败 ⇒ 回灌同一批
        self.assertLessEqual(len(h._queue), h.max_queue)
        self.assertEqual(h.dropped_count, 0)             # 100 条回灌不超上限 ⇒ 不丢

    def test_recovery_is_lossless(self):
        client = _WorkingRedis()
        h = _handler(client, max_queue=1000)
        for _ in range(500):
            h.emit(_record())
        h._flush()
        self.assertEqual(len(h._queue), 0, "成功刷新后队列未清空")
        self.assertEqual(h.dropped_count, 0, "正常流量被丢弃（不该丢的丢了）")
        self.assertGreater(client.commands, 0)

    def test_failure_warning_is_not_self_captured(self):
        lg = logging.getLogger("plaita")
        h = _handler(_FailingRedis(), max_queue=100)
        lg.addHandler(h)
        try:
            lg.info("one record")
            mid = len(h._queue)
            h._flush()                                   # 失败 ⇒ 产出告警
            after = len(h._queue)
        finally:
            lg.removeHandler(h)
        self.assertEqual(after, mid, "刷新失败告警被自己的 handler 收进队列（自噬）")

    def test_matrix_independent_predicate(self):
        """{全失败, 恢复} × {低, 高} × {默认上限, 自定义上限} 的笛卡尔积。

        期望值由**独立谓词**算出（`max(0, emitted - max_queue)` / 恢复为 0），
        不取自被测实现。
        """
        cells = 0
        for stall in ("all-fail", "recover"):
            for traffic in ("low", "high"):
                for cfg in ("default", "custom"):
                    max_q = RedisStreamHandler.DEFAULT_MAX_QUEUE if cfg == "default" else 128
                    emitted = 64 if traffic == "low" else max_q * 3
                    # 独立谓词：入队阶段的丢弃只取决于"发出量 vs 上限"（与是否恢复
                    # 无关——恢复只决定随后能否排空）
                    expected_dropped = max(0, emitted - max_q)
                    with self.subTest(stall=stall, traffic=traffic, cfg=cfg):
                        client = _FailingRedis()
                        h = _handler(client, max_queue=max_q)
                        for _ in range(emitted):
                            h.emit(_record())
                        if stall == "recover":
                            h.redis_client = _WorkingRedis()
                            h._flush()
                            self.assertEqual(len(h._queue), 0,
                                             "恢复后队列未排空（不该留）")
                        cells += 1
                        self.assertLessEqual(len(h._queue), h.max_queue)
                        self.assertEqual(h.dropped_count, expected_dropped)
        self.assertEqual(cells, 8)
