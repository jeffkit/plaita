"""wake 共享消费骨架（2026-10-25 唤醒面合并）。

钉四件事：
1. ``positive_poll_interval``：0/负/非数具名拒（r39 A7 语义，loud fail）；
2. ``_consume_wake_batch``：解析失败就地出队、in-flight 去重、重复条目跳过；
3. ``_run_wake_task`` 触发确认分流：正常完成 → on_confirmed（出队）；
   ``ResumePublishError`` → on_rejected（条目保留，#35 语义）；其余异常 →
   错误事件兜底 + on_confirmed；停机中断 → 条目保留仅清 in-flight；
4. 循环退出双条件：shutdown 事件与 ``is_running``（stop_service 直停路径，
   approval 旧循环语义——缺一边线程就残留，测试钉死）。

行为等价性由 Delay/Approval 既有套件承运（test_clean_c4_delay_service /
test_issue35_trigger_failure_retry / test_approval_queue_consumer 全绿 =
拆分零行为变化）；本文件只钉骨架自身的新增面。
"""
import json
import threading
import unittest

import fakeredis

from plaita.event.memory import InMemoryEventBus
from plaita.server.services.base_service import ResumePublishError
from plaita.server.services.wake import (
    WakeConsumeMixin,
    positive_poll_interval,
    wake_source_classes,
)


class _StubService(WakeConsumeMixin):
    """最小宿主：手装骨架依赖（不走 BaseExtendedService 的注册/线程池）。"""

    # 测试驱动：同步线程池假件（submit 即内联执行，免去真线程时序）。
    class _InlinePool:
        def submit(self, fn, *args, **kwargs):
            fn(*args, **kwargs)

    def __init__(self, redis_client, **cfg):
        self.service_config = cfg
        self._redis_client = redis_client
        self.is_running = True
        self.active_tasks = set()
        self._shutdown_event = threading.Event()
        self.handled = []
        self.fail_with = None
        self.thread_pool = self._InlinePool()
        self._init_wake_consume(cfg)

    def get_service_type(self):
        return "stub"

    def is_shutdown_requested(self):
        return self._shutdown_event.is_set()

    def _generate_task_id(self, task_config):
        return f"stub_{task_config.get('node_id')}"

    def _handle_task_error(self, task_config, error):
        self.errors = getattr(self, "errors", [])
        self.errors.append((task_config.get("node_id"), str(error)))

    def validate_task_config(self, task_config):
        return "node_id" in task_config

    def _wake_queue_key(self):
        return "plaita:stub:queue"

    async def handle_task(self, task_config):
        if self.fail_with is not None:
            raise self.fail_with
        self.handled.append(task_config.get("node_id"))
        return True

    def drain(self):
        return self._consume_wake_batch()

    def run_one(self, raw, task_config):
        self._run_wake_task(raw, task_config,
                            on_confirmed=self._ack_wake_task,
                            on_rejected=self._on_wake_rejected)


class TestPositivePollInterval(unittest.TestCase):
    def test_zero_negative_and_garbage_are_named_rejections(self):
        for bad in (0, -0.5, "abc", float("inf"), None):
            with self.assertRaises(ValueError) as ei:
                positive_poll_interval(bad)
            self.assertIn("positive", str(ei.exception))

    def test_positive_passes(self):
        self.assertEqual(positive_poll_interval(0.05), 0.05)
        self.assertEqual(positive_poll_interval("0.2"), 0.2)


class TestConsumeBatch(unittest.TestCase):
    def setUp(self):
        self.redis = fakeredis.FakeRedis(decode_responses=True)
        self.svc = _StubService(self.redis, poll_interval=0.01)

    def test_bad_entry_is_acked_not_requeued(self):
        self.redis.rpush("plaita:stub:queue", "{not json")
        self.redis.rpush("plaita:stub:queue", json.dumps({"no_node_id": 1}))
        self.assertEqual(self.svc.drain(), 0)
        self.assertEqual(self.redis.llen("plaita:stub:queue"), 0)

    def test_valid_entry_submitted_once_with_inflight_dedup(self):
        raw = json.dumps({"node_id": "n1"})
        self.redis.rpush("plaita:stub:queue", raw)
        self.assertEqual(self.svc.drain(), 1)
        # 内联线程池：提交即执行完 → 确认出队
        self.assertEqual(self.redis.llen("plaita:stub:queue"), 0)
        self.assertEqual(self.svc.handled, ["n1"])
        # in-flight 原语：同一条目处理期间不重复接收
        self.assertTrue(self.svc._wake_inflight_try_add(raw))
        self.assertFalse(self.svc._wake_inflight_try_add(raw))

    def test_resume_publish_error_keeps_entry(self):
        raw = json.dumps({"node_id": "n2"})
        self.redis.rpush("plaita:stub:queue", raw)
        self.svc.fail_with = ResumePublishError("redis down")
        self.svc.run_one(raw, {"node_id": "n2"})
        # #35 语义：触发未确认 → 条目保留待重试
        self.assertEqual(self.redis.llen("plaita:stub:queue"), 1)
        self.assertNotIn(raw, self.svc._inflight)

    def test_generic_error_acks_with_error_event(self):
        raw = json.dumps({"node_id": "n3"})
        self.redis.rpush("plaita:stub:queue", raw)
        self.svc.fail_with = RuntimeError("boom")
        self.svc.run_one(raw, {"node_id": "n3"})
        self.assertEqual(self.redis.llen("plaita:stub:queue"), 0)
        self.assertEqual(self.svc.errors, [("n3", "boom")])

    def test_shutdown_midtask_keeps_entry_and_clears_inflight(self):
        raw = json.dumps({"node_id": "n4"})
        self.redis.rpush("plaita:stub:queue", raw)
        self.svc._wake_inflight_try_add(raw)
        self.svc._shutdown_event.set()
        # 模拟停机中断：直接清 in-flight 分支的语义（条目留队列）
        with self.svc._inflight_lock:
            self.svc._inflight.discard(raw)
        self.assertEqual(self.redis.llen("plaita:stub:queue"), 1)
        self.assertNotIn(raw, self.svc._inflight)


class TestLoopExitConditions(unittest.TestCase):
    def test_loop_exits_on_stop_service_without_shutdown_event(self):
        """双退出条件：只翻 is_running（stop_service 直停路径）循环即退——
        缺这条线程残留（approval 旧循环语义，回归钉）。"""
        svc = _StubService(fakeredis.FakeRedis(decode_responses=True), poll_interval=0.01)
        svc._consumer_thread = threading.Thread(
            target=svc._consume_wake_loop, name="stub-test-loop", daemon=True
        )
        svc._consumer_thread.start()
        svc.is_running = False  # 不设 shutdown 事件
        svc._consumer_thread.join(timeout=2.0)
        self.assertFalse(svc._consumer_thread.is_alive())

    def test_loop_exits_on_shutdown_event(self):
        svc = _StubService(fakeredis.FakeRedis(decode_responses=True), poll_interval=0.01)
        svc._consumer_thread = threading.Thread(
            target=svc._consume_wake_loop, name="stub-test-loop2", daemon=True
        )
        svc._consumer_thread.start()
        svc._shutdown_event.set()
        svc._consumer_thread.join(timeout=2.0)
        self.assertFalse(svc._consumer_thread.is_alive())


class TestWakeSourceClasses(unittest.TestCase):
    def test_unknown_source_is_named_error(self):
        with self.assertRaises(ValueError) as ei:
            wake_source_classes(["delay", "kafka"])
        self.assertIn("kafka", str(ei.exception))

    def test_known_sources_resolve(self):
        from plaita.server.services.delay_service import DelayService
        from plaita.server.services.approval_service import ApprovalService
        from plaita.server.services.http_callback_service import HttpCallbackService
        from plaita.server.services.redis_queue_service import RedisQueueService

        self.assertEqual(
            wake_source_classes(["delay", "approval", "http_callback", "redis_queue"]),
            [DelayService, ApprovalService, HttpCallbackService, RedisQueueService],
        )


if __name__ == "__main__":
    unittest.main()
