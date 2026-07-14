"""Redis Stream task queue (at-least-once) for FlowWorker."""
from __future__ import annotations

import json
import time
import unittest
from unittest.mock import patch

import fakeredis

from plaita.server.task_queue import (
    DEFAULT_CONSUMER_GROUP,
    RedisStreamTaskQueue,
    enqueue_task,
)


class TestRedisStreamTaskQueue(unittest.TestCase):
    def setUp(self):
        self.redis = fakeredis.FakeRedis(decode_responses=True)
        self.stream = "plaita:flow:queue:test"
        self.group = "test-group"

    def test_enqueue_read_ack_roundtrip(self):
        q_prod = RedisStreamTaskQueue(self.redis, self.stream, group_name=self.group, consumer_name="c1")
        q_cons = RedisStreamTaskQueue(self.redis, self.stream, group_name=self.group, consumer_name="c1")
        task = {"type": "start", "flow_id": "f1", "params": {}, "version": "1"}
        msg_id = q_prod.enqueue(task)
        self.assertTrue(msg_id)

        read = q_cons.read(block_ms=100)
        self.assertIsNotNone(read)
        self.assertEqual(read.body, task)
        q_cons.ack(read.message_id)

        # 已 ack 的消息不应再被同一 consumer 读到
        again = q_cons.read(block_ms=50)
        self.assertIsNone(again)

    def test_unacked_message_reclaimed_by_other_consumer(self):
        q1 = RedisStreamTaskQueue(
            self.redis,
            self.stream,
            group_name=self.group,
            consumer_name="c1",
            claim_min_idle_ms=1,
        )
        q2 = RedisStreamTaskQueue(
            self.redis,
            self.stream,
            group_name=self.group,
            consumer_name="c2",
            claim_min_idle_ms=1,
        )
        q1.enqueue({"type": "resume", "flow_id": "f", "execution_id": "e", "resume_type": "event"})
        first = q1.read(block_ms=100)
        self.assertIsNotNone(first)
        # 模拟 c1 崩溃：不 ack
        time.sleep(0.01)
        reclaimed = q2.read(block_ms=100)
        self.assertIsNotNone(reclaimed)
        self.assertEqual(reclaimed.body["type"], "resume")
        q2.ack(reclaimed.message_id)

    def test_enqueue_task_helper(self):
        mid = enqueue_task(self.redis, self.stream, {"type": "start", "flow_id": "x"})
        self.assertIn("-", mid)
        length = self.redis.xlen(self.stream)
        self.assertEqual(length, 1)

    def test_dead_letter_moves_and_acks(self):
        q = RedisStreamTaskQueue(
            self.redis,
            self.stream,
            group_name=self.group,
            consumer_name="c1",
            max_deliveries=2,
        )
        q.enqueue({"type": "start", "flow_id": "poison"})
        task = q.read(block_ms=100)
        self.assertIsNotNone(task)
        dlq_id = q.dead_letter(task, reason="test")
        self.assertTrue(dlq_id)
        self.assertEqual(q.stats()["dead_lettered"], 1)
        self.assertGreaterEqual(self.redis.xlen(q.dlq_key), 1)
        # original should be acked — no reclaim
        time.sleep(0.01)
        q2 = RedisStreamTaskQueue(
            self.redis,
            self.stream,
            group_name=self.group,
            consumer_name="c2",
            claim_min_idle_ms=1,
            max_deliveries=2,
        )
        self.assertIsNone(q2.read(block_ms=50))

    def test_stats_includes_keys(self):
        q = RedisStreamTaskQueue(self.redis, self.stream, group_name=self.group)
        q.enqueue({"type": "start", "flow_id": "f"})
        stats = q.stats()
        self.assertEqual(stats["stream_key"], self.stream)
        self.assertIn("dlq_key", stats)
        self.assertEqual(stats["enqueued"], 1)


class TestRedisFlowWorkerDispatch(unittest.TestCase):
    def test_dispatch_start_and_resume(self):
        from plaita.server.flow_worker import RedisFlowWorker
        from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

        worker = RedisFlowWorker(
            redis_url="redis://localhost:6379/0",
            queue_name="q",
            execution_storage=MemoryExecutionStorage(),
            flow_storage=MemoryFlowStorage(),
            redis_client=fakeredis.FakeRedis(decode_responses=True),
            enable_registry=False,
        )
        with patch.object(worker, "start_flow") as start:
            worker._dispatch_task({"type": "start", "flow_id": "f", "params": {}, "version": "1"})
            start.assert_called_once_with("f", {}, "1")
        with patch.object(worker, "resume_flow") as resume:
            worker._dispatch_task(
                {
                    "type": "resume",
                    "flow_id": "f",
                    "execution_id": "e",
                    "resume_type": "event",
                    "data": {"k": 1},
                }
            )
            resume.assert_called_once_with("f", "e", "event", {"k": 1})

    def test_dispatch_unknown_type_raises(self):
        from plaita.server.flow_worker import RedisFlowWorker
        from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

        worker = RedisFlowWorker(
            redis_url="redis://localhost:6379/0",
            queue_name="q",
            execution_storage=MemoryExecutionStorage(),
            flow_storage=MemoryFlowStorage(),
            redis_client=fakeredis.FakeRedis(decode_responses=True),
            enable_registry=False,
        )
        with self.assertRaises(ValueError):
            worker._dispatch_task({"type": "nope"})


if __name__ == "__main__":
    unittest.main()
