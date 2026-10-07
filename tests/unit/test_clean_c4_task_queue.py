"""C4-1 回归：任务 Stream ack 后 XDEL 裁剪 + DLQ 入队后 XTRIM 保留最近 N 条。

背景：enqueue_task/dead_letter 只 XADD 不设上限，ack 后条目也永驻 Stream——
长跑 worker 的任务流与 DLQ 线性膨胀（C4-1 [P2]）。
"""
from __future__ import annotations

import json
import unittest
from unittest.mock import patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis

from plaita.server.task_queue import (
    DEFAULT_DLQ_MAX_LEN,
    RedisStreamTaskQueue,
    _dlq_max_len_from_env,
)


class TestAckDeletesStreamEntry(unittest.TestCase):
    def setUp(self):
        self.redis = fakeredis.FakeRedis(decode_responses=True)
        self.stream = "plaita:flow:queue:c4-1"
        self.q = RedisStreamTaskQueue(self.redis, self.stream, group_name="g", consumer_name="c1")

    def test_ack_shrinks_stream_length(self):
        """ack 成功后 stream 长度收缩（XDEL 已确认条目）。"""
        self.q.enqueue({"type": "start", "flow_id": "f1"})
        task = self.q.read(block_ms=100)
        self.assertIsNotNone(task)
        self.assertEqual(self.redis.xlen(self.stream), 1)
        self.q.ack(task.message_id)
        self.assertEqual(self.redis.xlen(self.stream), 0)
        # 消费组 PEL 同步干净，重复消费读不到任何东西
        self.assertIsNone(self.q.read(block_ms=10))
        self.assertEqual(self.q.stats()["pending"], 0)

    def test_unacked_pending_entry_survives(self):
        """pending 未 ack 条目不受影响：仍可被回收重投。"""
        self.q.claim_min_idle_ms = 1
        self.q.enqueue({"type": "resume", "flow_id": "f", "execution_id": "e", "resume_type": "event"})
        task = self.q.read(block_ms=100)
        self.assertIsNotNone(task)
        # 不 ack，另一 consumer 应能回收该条目（条目本体仍在）
        import time

        time.sleep(0.01)
        q2 = RedisStreamTaskQueue(
            self.redis, self.stream, group_name="g", consumer_name="c2", claim_min_idle_ms=1
        )
        reclaimed = q2.read(block_ms=100)
        self.assertIsNotNone(reclaimed)
        self.assertEqual(reclaimed.body["execution_id"], "e")
        q2.ack(reclaimed.message_id)
        self.assertEqual(self.redis.xlen(self.stream), 0)

    def test_unread_entries_unaffected_by_acking_another(self):
        """ack 一条不影响其他未读条目。"""
        self.q.enqueue({"type": "start", "flow_id": "f1"})
        self.q.enqueue({"type": "start", "flow_id": "f2"})
        task1 = self.q.read(block_ms=100)
        self.q.ack(task1.message_id)
        self.assertEqual(self.redis.xlen(self.stream), 1)
        task2 = self.q.read(block_ms=100)
        self.assertEqual(task2.body["flow_id"], "f2")
        self.q.ack(task2.message_id)
        self.assertEqual(self.redis.xlen(self.stream), 0)


class TestDeadLetterTrim(unittest.TestCase):
    def setUp(self):
        self.redis = fakeredis.FakeRedis(decode_responses=True)
        self.stream = "plaita:flow:queue:c4-1-dlq"

    def _dead_letter_all(self, q: RedisStreamTaskQueue, flow_ids, reason_prefix: str):
        """批量入队后按序读+死信每条（返回 None，断言 xlen 由调用方做）。

        必须先批量入队再逐条读+死信：源 Stream 若在两次入队之间被 ack 的
        XDEL 清空，fakeredis(2.36) 的 XADD 自增 id 基准取自 ``_ids[-1]`` 而
        不是 Redis 的 last-generated-id（model/_stream.py add()），同毫秒内
        的下一条 XADD 会**复用已投递过的 id**，XREADGROUP ">" 再也读不到它
        → read() 返回 None。真 Redis 的 last_id 不因 XDEL 回退，无此问题。
        """
        for flow_id in flow_ids:
            q.enqueue({"type": "start", "flow_id": flow_id})
        for i, flow_id in enumerate(flow_ids):
            task = q.read(block_ms=100)
            self.assertIsNotNone(task)
            self.assertEqual(task.body["flow_id"], flow_id)
            q.dead_letter(task, reason=f"{reason_prefix}-{i}")

    def test_dlq_trimmed_to_max_len_keeps_newest(self):
        q = RedisStreamTaskQueue(
            self.redis, self.stream, group_name="g", consumer_name="c1", dlq_max_len=3
        )
        self._dead_letter_all(q, [f"poison-{i}" for i in range(5)], reason_prefix="test")
        self.assertEqual(self.redis.xlen(q.dlq_key), 3)
        # 保留的是最近 3 条（poison-2/3/4）
        entries = self.redis.xrange(q.dlq_key)
        payloads = [json.loads(fields["payload"])["payload"]["flow_id"] for _id, fields in entries]
        self.assertEqual(payloads, ["poison-2", "poison-3", "poison-4"])

    def test_dlq_under_limit_not_trimmed(self):
        q = RedisStreamTaskQueue(
            self.redis, self.stream, group_name="g", consumer_name="c1", dlq_max_len=10
        )
        self._dead_letter_all(q, [f"p{i}" for i in range(3)], reason_prefix="t")
        self.assertEqual(self.redis.xlen(q.dlq_key), 3)

    def test_dlq_max_len_from_env(self):
        with patch.dict("os.environ", {"PLAITA_DLQ_MAX_LEN": "7"}):
            self.assertEqual(_dlq_max_len_from_env(), 7)
            q = RedisStreamTaskQueue(self.redis, self.stream, group_name="g")
            self.assertEqual(q.dlq_max_len, 7)
        with patch.dict("os.environ", {"PLAITA_DLQ_MAX_LEN": "not-a-number"}):
            self.assertEqual(_dlq_max_len_from_env(), DEFAULT_DLQ_MAX_LEN)
        with patch.dict("os.environ", {"PLAITA_DLQ_MAX_LEN": "0"}):
            # 0/负数视为非法，钳到 1
            self.assertEqual(_dlq_max_len_from_env(), 1)

    def test_constructor_param_overrides_env(self):
        with patch.dict("os.environ", {"PLAITA_DLQ_MAX_LEN": "7"}):
            q = RedisStreamTaskQueue(
                self.redis, self.stream, group_name="g", dlq_max_len=2
            )
            self.assertEqual(q.dlq_max_len, 2)


if __name__ == "__main__":
    unittest.main()
