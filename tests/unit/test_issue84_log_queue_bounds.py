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
        """子句②：flush 失败时**并发追加**导致合并越界 ⇒ 必须截断回上限并计数。

        （r2 的版本用"恰好不超上限"的场景，关掉 overflow 也不会红 ⇒ 断言无效。）
        """
        max_q = 100

        class _ConcurrentFailingRedis:
            """execute 前**并发**追加 max_q 条（模拟 flush 期间的高频日志），然后失败。"""

            def __init__(self):
                self.handler = None

            def pipeline(self):
                for _ in range(max_q):
                    self.handler.emit(_record())
                raise RuntimeError("redis down")

        client = _ConcurrentFailingRedis()
        h = _handler(client, max_queue=max_q)
        client.handler = h
        for _ in range(max_q):
            h.emit(_record())
        pre_dropped = h.dropped_count
        cap = _AuditCapture()
        _audit_logger.addHandler(cap)
        try:
            h._flush()                                  # 越界回灌
        finally:
            _audit_logger.removeHandler(cap)
        self.assertLessEqual(
            len(h._queue), max_q,
            f"回灌越界未被截断：queue={len(h._queue)} > max_queue={max_q}",
        )
        # 独立谓词：entries(max_q) + 并发追加(max_q) - 上限(max_q) = max_q 条被丢
        self.assertEqual(h.dropped_count - pre_dropped, max_q)
        self.assertTrue(
            any(("回灌" in line and "丢弃最旧" in line and "累计丢弃" in line)
                for line in cap.lines),
            f"回灌截断没有可定位的审计告警: {cap.lines[:2]}",
        )

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
        """子句④：审计告警不得出现在父 logger（`plaita`）的 handler 上。

        r2 的版本用 `lg.info(...)` 入队，但 `plaita` 的有效级别是 WARNING（root 默认）
        ⇒ **INFO 记录根本没产生**、队列为空、`_flush()` 首行 `if not self._queue: return`
        直接返回 ⇒ 断言恒真（这就是该变异存活至今的根因）。这里改为：直接 `h.emit(...)`
        保证队列非空，并在父 logger 上挂内存 handler 观察**审计记录**。
        """
        lg = logging.getLogger("plaita")
        cap = _AuditCapture()
        cap.seen_names = []
        _orig_emit = cap.emit

        def _emit(record):
            cap.seen_names.append(record.name)
            _orig_emit(record)

        cap.emit = _emit
        h = _handler(_FailingRedis(), max_queue=100)
        lg.addHandler(cap)
        try:
            for _ in range(3):
                h.emit(_record())                        # 队列非空 ⇒ flush 真的会失败
            queue_before = len(h._queue)
            self.assertEqual(queue_before, 3)
            h._flush()
        finally:
            lg.removeHandler(cap)
        audit_records = [n for n in cap.seen_names if n == _audit_logger.name]
        self.assertEqual(len(audit_records), 0,
                         "审计告警冒泡到父 logger ⇒ 会被本 handler 收进队列（自噬）")
        # 日志框架层面的前置条件（保证上面的 0 不是"根本没发"或"没挂 handler"）
        self.assertIs(_audit_logger.propagate, False,
                      "审计 logger 必须 propagate=False")
        self.assertGreater(len(_audit_logger.handlers), 0,
                           "审计 logger 必须有自有 handler（否则告警不可观测）")

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
