"""plaita#84 —— RedisStreamHandler 客户端队列**有界**、丢弃**可观测**、告警**不自噬**。

规则（一句可判定谓词，**独立于实现**）：
* 入队后 `len(queue) <= max_queue`（默认 `DEFAULT_MAX_QUEUE=10000`）；
* 全部刷新失败时，`dropped == max(0, emitted - max_queue)`，且每次丢弃都有
  **审计告警**（`DROP_WARN_EVERY` 条聚合一次）；
* 队列满时丢的是**最旧**一条（保留最新 `max_queue` 条）——方向由可区分消息
  同时钉住 `queue[0]` / `queue[-1]`（丢最新会立刻变红，docs 与实现同向）；
* 审计告警走 `plaita.server.log_handler.audit`（`propagate=False`）⇒ **不得**被
  本 handler 收进队列（自噬 delta == 0）；且该 logger 的级别**自持**
  （`WARNING`）⇒ root 抬到 ERROR 也照发，不被 root 级别静默；
* 刷新失败回灌同样受上限约束：`len(queue) <= max_queue`，且只回灌**未成功提交**
  的部分（本批起）——已 `execute` 成功的批次**不得重复入队**；
* 积压恢复**分批** `execute()`：`chunk_size` 条一批，单次命令数 ≤ `2 × chunk_size`，
  不再一次性构造 2N 条命令（`DEFAULT_CHUNK_SIZE=500`）；
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


class _RecordingRedis:
    """按批记录每次 execute() 提交的命令数（#84 分批提交断言用）。"""

    def __init__(self):
        self.batches = []

    def pipeline(self):
        return _RecordingPipe(self)


class _RecordingPipe:
    def __init__(self, client):
        self.client = client
        self.n = 0

    def xadd(self, *a, **kw):
        self.n += 1
        return self

    def publish(self, *a, **kw):
        self.n += 1
        return self

    def execute(self):
        self.client.batches.append(self.n)
        return []


class _FailSecondBatchRedis:
    """第 1 批 execute 成功、第 2 批抛（记录每批 xadd 的 message）。

    用于「只回灌未成功提交的本批起部分」：已提交批次的条目不得重复入队。
    ``commands`` 只累计**成功提交**的命令数（xadd + publish）。
    """

    def __init__(self):
        self.committed = []      # 成功 execute 的批次里的 message（按序）
        self.commands = 0        # 成功提交的命令数
        self.executes = 0

    def pipeline(self):
        return _FailSecondBatchPipe(self)


class _FailSecondBatchPipe:
    def __init__(self, client):
        self.client = client
        self.entries = []
        self.n = 0

    def xadd(self, *a, **kw):
        self.entries.append(a[1]["message"])
        self.n += 1
        return self

    def publish(self, *a, **kw):
        self.n += 1
        return self

    def execute(self):
        self.client.executes += 1
        if self.client.executes >= 2:
            raise RuntimeError("redis down mid-flush")
        self.client.commands += self.n
        self.client.committed.extend(self.entries)
        return []


def _handler(redis, max_queue=RedisStreamHandler.DEFAULT_MAX_QUEUE,
             chunk_size=RedisStreamHandler.DEFAULT_CHUNK_SIZE,
             publish_realtime=True):
    h = RedisStreamHandler(redis, "flow_worker", "i1", max_queue=max_queue,
                           chunk_size=chunk_size, publish_realtime=publish_realtime)
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

    def test_drop_direction_is_oldest_kept(self):
        """方向钉死：队列满时丢**最旧**一条，保留最新 `max_queue` 条。

        可区分消息（m0..m4）+ 同时断言 `_queue[0]`（最旧存活）与 `_queue[-1]`
        （最新一条）：把 `self._queue.popleft()` 变异成 `self._queue.pop()`（丢最新）
        后本用例立刻红——r2 交付里这个方向没有任何断言钉住，docs 说「丢最旧」
        而测试两个方向都放行。
        """
        h = _handler(_FailingRedis(), max_queue=3)
        for i in range(5):
            h.emit(_record(f"m{i}"))
        # 前置对象：3 条在队、2 条被丢、计数可见（非空转）
        self.assertEqual(len(h._queue), 3)
        self.assertEqual(h.dropped_count, 2)
        # 独立谓词：按序 m0..m4、上限 3 ⇒ 保留 {m2,m3,m4}，丢弃 {m0,m1}
        self.assertEqual([e["message"] for e in h._queue], ["m2", "m3", "m4"])
        self.assertEqual(h._queue[0]["message"], "m2", "最旧存活的一条不是 m2（丢了最新的？）")
        self.assertEqual(h._queue[-1]["message"], "m4", "最新一条不在队尾")

    def test_audit_warning_survives_root_level_error(self):
        """告警不得被 root logger 级别扣为人质（#84 复审新缺陷）。

        `_audit_logger` 级别留 NOTSET 会继承 root：`PLAITA_LOG_LEVEL=ERROR`
        （已文档化的开关）或嵌入方抬 root 时，`warning(...)` 在**记录创建前**
        就被判 disabled ⇒ 丢弃完全静默而 `dropped_count` 照涨。
        """
        root = logging.getLogger()
        chain = [logging.getLogger(n)
                 for n in ("plaita", "plaita.server", "plaita.server.log_handler")]
        saved_root = root.level
        saved_chain = [(lg, lg.level) for lg in chain]
        cap = _AuditCapture()
        _audit_logger.addHandler(cap)
        try:
            for lg in chain:                    # 祖先级别归零 ⇒ 生效级别只看 root
                lg.setLevel(logging.NOTSET)
            root.setLevel(logging.ERROR)        # 模拟 PLAITA_LOG_LEVEL=ERROR
            h = _handler(_FailingRedis(), max_queue=3)
            for i in range(18):                 # 4..18 共 15 次丢弃
                h.emit(_record(f"m{i}"))
            # 框架级前置：告警在本 logger 上确实 enabled（不是"根本没发"）
            self.assertTrue(_audit_logger.isEnabledFor(logging.WARNING),
                            "审计 logger 级别仍受 root 牵制")
        finally:
            root.setLevel(saved_root)
            for lg, lvl in saved_chain:
                lg.setLevel(lvl)
            _audit_logger.removeHandler(cap)
        # 对象：15 次丢弃属实，队列顶在上限
        self.assertEqual(len(h._queue), 3)
        self.assertEqual(h.dropped_count, 15)
        self.assertTrue(cap.lines, "root=ERROR 时丢弃审计告警被 root 级别挡掉（静默丢弃）")
        self.assertTrue(any("丢弃最旧" in line for line in cap.lines), cap.lines[:2])

    def test_reinjection_only_keeps_uncommitted_batches(self):
        """子句②b：刷新**中途**失败时只回灌未成功提交的部分（本批起）。

        verifier 手测过该路径（1200 条 / chunk=500、第 2 批 execute 抛 ⇒ 回灌
        ``entries[500:1200]``、已提交的 ``entries[0:500]`` 不重复入队）却无交付
        用例。独立谓词：7 条 / chunk=3、第 2 批抛 ⇒ 队列恰为 ``entries[3:7]``，
        已提交的 ``entries[0:3]`` 一条都不在队列里，且无丢弃。
        """
        client = _FailSecondBatchRedis()
        h = _handler(client, max_queue=10000, chunk_size=3)
        for i in range(7):
            h.emit(_record(f"m{i}"))

        h._flush()

        # 前置对象：第 1 批真提交、第 2 批真失败（3 条 × (xadd+publish) = 6 命令）
        self.assertEqual(client.executes, 2)
        self.assertEqual(client.committed, ["m0", "m1", "m2"])
        self.assertEqual(client.commands, 6)
        # 核心断言：只有未提交部分（entries[3:7]）回灌，顺序逐条一致
        queued = [e["message"] for e in h._queue]
        self.assertEqual(queued, ["m3", "m4", "m5", "m6"])
        self.assertEqual(
            set(queued) & set(client.committed), set(),
            f"已成功提交的批次被重复回灌: {queued}",
        )
        self.assertEqual(h.dropped_count, 0)

    def test_pipeline_chunked(self):
        """子句⑤：积压恢复必须**分批** execute，单次命令数有上限。

        独立谓词：backlog=10000、`chunk_size=500`、（每条 1×xadd + 1×publish）
        ⇒ 恰好 10000/500 = 20 次 execute，每次 ≤ 2×500 = 1000 条命令；而不是
        1 次 execute 打回 20000 条（issue 实测的 2N 恢复洪峰）。默认配置另测：
        默认 chunk 必须把 10000 条积压切成多批且单批 < 2×10000。
        """
        chunk = 500
        r = _RecordingRedis()
        h = _handler(r, max_queue=10000, chunk_size=chunk)
        for i in range(10000):
            h.emit(_record(f"m{i}"))
        h._flush()
        # 前置对象：每条恰好进一批（20 批 × 1000 命令 = 20000 条命令）
        self.assertEqual(len(r.batches), 20, f"execute() 批次数 {r.batches}")
        self.assertEqual(sum(r.batches), 20000)
        self.assertLessEqual(max(r.batches), 2 * chunk)
        self.assertEqual(h.dropped_count, 0)

        # 默认 chunk：行为级钉住「默认不是一次性提交」（常量被改成 ≥ backlog 会红）
        r2 = _RecordingRedis()
        h2 = _handler(r2, max_queue=10000)
        for i in range(10000):
            h2.emit(_record(f"m{i}"))
        h2._flush()
        self.assertGreater(len(r2.batches), 1, "默认配置仍是一次性 2N 条命令洪峰")
        self.assertLess(max(r2.batches), 20000)
