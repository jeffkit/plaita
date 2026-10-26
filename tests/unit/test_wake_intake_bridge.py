"""唤醒递送线补桥（2026-10-26）：http_callback / redis_queue / kafka 的任务 LIST
接入 wake 消费骨架。

背景：worker 挂起时把监听/注册任务 RPUSH 到 ``plaita:{subtype}:queue``，但
全仓此前只有 delay/approval 有消费者——三家服务「在岗」（注册键活）也接不走
任务条目，挂起执行照旧睡死（点名闸只防「整岗缺席」，防不了「岗在≠线通」）。

钉四件事：
1. 三家的 ``_wake_queue_key`` 与 worker 投递键逐字对齐；
2. 真实服务 + fakeredis：条目 → ``handle_task`` → 确认出队（http_callback 走
   真实任务体——回调记录落盘；redis_queue/kafka 换桩避免真监听阻塞）；
3. ``ResumePublishError`` → 条目保留（#35 语义在三家同样成立）；
4. ``RedisQueueService.stop_service`` 修后不再依赖 ``asyncio.get_event_loop()``
   （无线程循环的停机路径曾必抛）——假异步客户端过一遍关停。
"""
import asyncio
import json
import threading
import unittest

import fakeredis

from plaita.event.memory import InMemoryEventBus
from plaita.server.services.base_service import ResumePublishError
from plaita.server.services.http_callback_service import HttpCallbackService
from plaita.server.services.kafka_queue_service import KafkaQueueService
from plaita.server.services.redis_queue_service import RedisQueueService


class _InlinePool:
    """测试驱动：同步线程池假件（submit 即内联执行，免去真线程时序）。"""

    def submit(self, fn, *args, **kwargs):
        fn(*args, **kwargs)


def _make_http_callback_service(redis):
    svc = HttpCallbackService(
        event_bus=InMemoryEventBus(), service_config={}, redis_client=redis
    )
    svc.thread_pool = _InlinePool()
    return svc


def _make_redis_queue_service(redis):
    svc = RedisQueueService(event_bus=InMemoryEventBus(), redis_client=redis)
    svc.thread_pool = _InlinePool()
    return svc


def _make_kafka_service(redis):
    svc = KafkaQueueService(
        event_bus=InMemoryEventBus(), service_config={}, redis_client=redis
    )
    svc.thread_pool = _InlinePool()
    return svc


class TestQueueKeyAlignment(unittest.TestCase):
    """钉 1：递送键与 worker ``_dispatch_service_task`` 的投递键逐字一致。"""

    def test_queue_keys_match_worker_dispatch(self):
        self.assertEqual(
            HttpCallbackService._wake_queue_key(None), "plaita:http_callback:queue"
        )
        self.assertEqual(
            RedisQueueService._wake_queue_key(None), "plaita:redis_queue:queue"
        )
        self.assertEqual(
            KafkaQueueService._wake_queue_key(None), "plaita:kafka_queue:queue"
        )


class TestHttpCallbackBridge(unittest.TestCase):
    """钉 2a：真实任务体——LIST 条目 → 注册回调记录 → 出队。"""

    def setUp(self):
        self.redis = fakeredis.FakeRedis(decode_responses=True)
        self.svc = _make_http_callback_service(self.redis)

    def test_entry_registers_callback_record_and_dequeues(self):
        task = {
            "node_id": "cb1",
            "event_type": "http_callback",
            "execution_id": "exec-1",
            "callback_config": {"path": "/cb/exec-1"},
        }
        self.redis.rpush("plaita:http_callback:queue", json.dumps(task))
        self.assertEqual(self.svc._consume_wake_batch(), 1)
        # 真实 handle_task 已把回调注册记录落 Redis（外部 POST 到达时的查档源）
        record = self.redis.get("plaita:http_callback:registered:/cb/exec-1")
        self.assertIsNotNone(record)
        self.assertIn("exec-1", record)
        self.assertEqual(self.redis.llen("plaita:http_callback:queue"), 0)

    def test_bad_entry_is_dropped_without_record(self):
        self.redis.rpush("plaita:http_callback:queue", "{not json")
        self.redis.rpush(
            "plaita:http_callback:queue",
            json.dumps({"node_id": "cb2", "event_type": "http_callback"}),
        )
        self.assertEqual(self.svc._consume_wake_batch(), 0)
        self.assertEqual(self.redis.llen("plaita:http_callback:queue"), 0)

    def test_resume_publish_error_keeps_entry(self):
        original = self.svc.handle_task

        async def fail_publish(task_config):
            raise ResumePublishError("pub down")

        self.svc.handle_task = fail_publish
        raw = json.dumps(
            {
                "node_id": "cb3",
                "event_type": "http_callback",
                "callback_config": {"path": "/cb/exec-3"},
            }
        )
        self.redis.rpush("plaita:http_callback:queue", raw)
        self.svc._consume_wake_batch()
        # #35 语义：触发未确认 → 条目保留待重试
        self.assertEqual(self.redis.llen("plaita:http_callback:queue"), 1)
        self.assertNotIn(raw, self.svc._inflight)
        self.svc.handle_task = original


class TestRedisQueueBridge(unittest.TestCase):
    """钉 2b/3：redis_queue 递送消费（监听任务体换桩，钉递送面语义）。"""

    def setUp(self):
        self.redis = fakeredis.FakeRedis(decode_responses=True)
        self.svc = _make_redis_queue_service(self.redis)
        self.handled = []

        async def fake_listen(task_config):
            self.handled.append(task_config.get("node_id"))
            return True

        self.svc.handle_task = fake_listen

    def _task(self, node_id):
        return {
            "node_id": node_id,
            "event_type": "redis_message",
            "execution_id": f"exec-{node_id}",
            "redis_config": {"host": "localhost", "port": 6379},
            "queue_config": {"name": "user-q", "type": "list"},
            "listen_config": {"timeout_seconds": 5},
        }

    def test_entry_reaches_handle_task_and_dequeues(self):
        self.redis.rpush(
            "plaita:redis_queue:queue", json.dumps(self._task("rq1"))
        )
        self.assertEqual(self.svc._consume_wake_batch(), 1)
        self.assertEqual(self.handled, ["rq1"])
        self.assertEqual(self.redis.llen("plaita:redis_queue:queue"), 0)

    def test_resume_publish_error_keeps_entry(self):
        async def fail_publish(task_config):
            raise ResumePublishError("pub down")

        self.svc.handle_task = fail_publish
        raw = json.dumps(self._task("rq2"))
        self.redis.rpush("plaita:redis_queue:queue", raw)
        self.svc._consume_wake_batch()
        self.assertEqual(self.redis.llen("plaita:redis_queue:queue"), 1)

    def test_bad_entry_is_dropped(self):
        self.redis.rpush("plaita:redis_queue:queue", "not-json-at-all")
        self.assertEqual(self.svc._consume_wake_batch(), 0)
        self.assertEqual(self.redis.llen("plaita:redis_queue:queue"), 0)


class TestKafkaBridge(unittest.TestCase):
    """钉 2c：kafka 同桥（handle_task 换桩——真监听需要外部 broker）。"""

    def setUp(self):
        self.redis = fakeredis.FakeRedis(decode_responses=True)
        self.svc = _make_kafka_service(self.redis)
        self.handled = []

        async def fake_listen(task_config):
            self.handled.append(task_config.get("node_id"))
            return True

        self.svc.handle_task = fake_listen

    def test_entry_reaches_handle_task_and_dequeues(self):
        task = {
            "node_id": "kq1",
            "event_type": "kafka_message",
            "execution_id": "exec-kq1",
            "kafka_config": {"bootstrap_servers": "localhost:9092"},
            "topic_config": {"topic": "t"},
            "consumer_config": {"group_id": "g1"},
        }
        self.redis.rpush("plaita:kafka_queue:queue", json.dumps(task))
        self.assertEqual(self.svc._consume_wake_batch(), 1)
        self.assertEqual(self.handled, ["kq1"])
        self.assertEqual(self.redis.llen("plaita:kafka_queue:queue"), 0)


class _FakeAsyncClient:
    """redis_queue 停机路径的假监听客户端（close 为协程）。"""

    def __init__(self):
        self.closed = False

    async def close(self):
        self.closed = True


class TestRedisQueueStopService(unittest.TestCase):
    """钉 4：stop_service 不再依赖 asyncio.get_event_loop()（无线程循环的
    停机路径 py3.10+ 必抛 DeprecationWarning/RuntimeError），活跃客户端
    仍被逐个关闭。"""

    def test_stop_closes_active_clients_without_running_loop(self):
        svc = _make_redis_queue_service(fakeredis.FakeRedis(decode_responses=True))
        svc.is_running = True
        client = _FakeAsyncClient()
        svc._active_clients["exec-1"] = client
        # 主线程无线程 event loop——旧实现在这里必抛
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass  # 预期：没有 running loop
        self.assertTrue(svc.stop_service())
        self.assertTrue(client.closed)
        self.assertEqual(svc._active_clients, {})


if __name__ == "__main__":
    unittest.main()
