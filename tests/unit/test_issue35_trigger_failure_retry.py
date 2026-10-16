"""#35 守卫：delay 到期触发失败不得 ZREM 出排程（最后一跳 at-least-once）。

历史缺陷（基线 48abf2e）：``publish_resume_event`` 捕获一切发布异常只
logger.error（base_service），``DelayService.handle_task`` 的兜底 except 再
吞一层返回 False/True，``_run_scheduled_task`` 的 finally 无差别
``_finalize_scheduled`` ZREM——到期提交恰逢 Redis 抖动：publish 失败被吞、
条目已出排程 ZSET，挂起执行的唯一唤醒凭据消失，执行永久 suspended
（at-most-once，且无计数器/死信能系统性发现）。

修复语义（本文件钉住）：
- 发布失败必须上抛（``ResumePublishError``）；触发未确认 ⇒ 条目留在 ZSET，
  登记重试计数/退避（hash ``{queue}:attempts``），下轮到期轮询重试；
- 超过上限转显式死信 list（``{queue}:dead``，rpush 先于 zrem——宁可
  「死信+重复」不可「出排程+无死信」）；
- 防重：退避窗口内不重提交；条目在 ZSET 中始终至多一份；成功后清计数、
  条目出队不再触发（inflight 进程内去重由既有用例钉住）。

模拟手法：FakeRedis 包装器只对 ``publish`` 注入失败（issue 验收原文：
"mock publish 抛异常"）；排程原语（zadd/zrem/hset...）保持可用，否则连
被测的排程状态都无法维护。
"""
from __future__ import annotations

import json
import time
import unittest
from unittest.mock import AsyncMock, patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis

from plaita.event.memory import InMemoryEventBus
from plaita.server.services.delay_service import DelayService


def _task(delay_ms=1, node_id="n1", execution_id="e1"):
    return {
        "type": "delay",
        "delay_ms": delay_ms,
        "node_id": node_id,
        "execution_id": execution_id,
        "flow_id": "f1",
        "event_type": "delay_trigger",
        "tenant_id": "default",
    }


def _wait_until(cond, timeout=5.0, interval=0.02):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return True
        time.sleep(interval)
    return cond()


class _FlakyPublishRedis:
    """``publish`` 可注入失败的 FakeRedis 包装（其余全委托）。

    ``fail_next``：剩余失败次数；``publish_calls`` / ``published_ok``：
    触发尝试计数 / 真正送达计数（断言「失败未被吞成成功」用）。
    """

    def __init__(self, inner):
        self._inner = inner
        self.fail_next = 0
        self.publish_calls = 0
        self.published_ok = 0

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def publish(self, *args, **kwargs):
        self.publish_calls += 1
        if self.fail_next > 0:
            self.fail_next -= 1
            raise ConnectionError("simulated redis blip (#35)")
        self.published_ok += 1
        return self._inner.publish(*args, **kwargs)


class Issue35TestBase(unittest.TestCase):
    def setUp(self):
        self.redis = fakeredis.FakeRedis(decode_responses=True)
        self.bus = InMemoryEventBus()
        self.queue_key = "plaita:delay:queue:issue35"
        self.scheduled_key = f"{self.queue_key}:scheduled"
        self.attempts_key = f"{self.queue_key}:attempts"
        self.dead_key = f"{self.queue_key}:dead"
        self.services = []

    def tearDown(self):
        for svc in self.services:
            svc.shutdown(timeout=1)

    def make_service(self, redis_client=None, extra_config=None):
        config = {
            "max_workers": 2,
            "poll_interval": 0.05,
            "delay_queue": self.queue_key,
        }
        config.update(extra_config or {})
        svc = DelayService(self.bus, config, redis_client=redis_client or self.redis)
        self.services.append(svc)
        return svc

    def attempts_of(self, svc, member):
        raw = self.redis.hget(svc.attempts_key, member)
        return json.loads(raw) if raw else None


class TestTriggerFailureKeepsScheduled(Issue35TestBase):
    """核心红证：到期触发抛错 ⇒ 条目被 ZREM ⇒ 重试不可能（修复前）。"""

    def test_publish_failure_keeps_member_in_zset(self):
        """publish 恒失败：条目必须留在 ZSET（不 ZREM），登记重试态。"""
        flaky = _FlakyPublishRedis(self.redis)
        flaky.fail_next = 10**9  # 恒失败（多轮重试都在失败窗口内）
        svc = self.make_service(redis_client=flaky)
        member = json.dumps(_task())
        self.redis.rpush(self.queue_key, member)
        self.assertTrue(svc.start_service())

        # 到期触发已尝试且确实失败（事件未送达）
        self.assertTrue(_wait_until(lambda: flaky.publish_calls >= 1), "到期任务未触发 publish")
        self.assertEqual(flaky.published_ok, 0, "失败被吞成发布成功")

        # 等到终态：修复前条目会被误 ZREM（zcard→0 提前返回）；修复后条目
        # 留在 ZSET（等满窗口）。两种世界都能确定性地走到下面的断言。
        _wait_until(lambda: self.redis.zcard(self.scheduled_key) == 0, timeout=0.5)
        # 触发未确认 ⇒ 条目仍在排程 ZSET，且至多一份（不重复入队）
        self.assertEqual(
            self.redis.zcard(self.scheduled_key), 1,
            "触发失败后条目被 ZREM 出排程（#35：重试不可能）",
        )
        self.assertEqual(self.redis.llen(self.queue_key), 0)

        # 跨多个轮询周期保持：仍只有一份、未被消费掉
        time.sleep(0.2)
        self.assertEqual(self.redis.zcard(self.scheduled_key), 1, "重试期间条目数量漂移")

        # 重试计数已登记（可观测：不再是「无计数器」状态）
        rec = self.attempts_of(svc, member)
        self.assertIsNotNone(rec, "触发失败未登记重试计数")
        self.assertGreaterEqual(int(rec["attempts"]), 1)

    def test_retry_after_publish_recovery_succeeds(self):
        """issue 验收：publish 首次抛异常后恢复，下轮重试成功并出队。"""
        flaky = _FlakyPublishRedis(self.redis)
        flaky.fail_next = 1  # 第一次触发失败，随后恢复
        svc = self.make_service(
            redis_client=flaky,
            extra_config={"retry_backoff_base": 0.05, "retry_backoff_max": 0.2},
        )
        member = json.dumps(_task(node_id="n2", execution_id="e2"))
        self.redis.rpush(self.queue_key, member)
        self.assertTrue(svc.start_service())

        # 失败后恢复 ⇒ 重试成功：出队 + 事件送达 + 计数清理
        self.assertTrue(
            _wait_until(lambda: self.redis.zcard(self.scheduled_key) == 0),
            "publish 恢复后未重试成功（条目滞留或未出队）",
        )
        self.assertGreaterEqual(flaky.publish_calls, 2, "失败后未发生重试")
        self.assertGreaterEqual(flaky.published_ok, 1, "重试未真正送达事件")
        self.assertIsNone(self.attempts_of(svc, member), "成功后未清理重试计数")
        self.assertEqual(self.redis.llen(self.dead_key), 0, "可重试失败被误转死信")


class TestBackoffAndNoDuplication(Issue35TestBase):
    """防重：退避窗口内不重提交；条目至多一份；同一到期一轮只处理一次。"""

    def test_backoff_window_blocks_resubmission(self):
        """失败后退避窗口内，到期轮询不得重提交（防打爆/防同周期双触发）。"""
        flaky = _FlakyPublishRedis(self.redis)
        flaky.fail_next = 10**9
        svc = self.make_service(
            redis_client=flaky,
            extra_config={"retry_backoff_base": 5.0},  # 窗口远长于测试时长
        )
        member = json.dumps(_task(node_id="n3", execution_id="e3"))
        # 直接置入 ZSET 并同步执行一次失败触发（不经消费线程，确定性）
        self.redis.zadd(self.scheduled_key, {member: int(time.time() * 1000) - 1})
        svc._run_scheduled_task(member, _task(node_id="n3", execution_id="e3"))
        self.assertEqual(self.redis.zcard(self.scheduled_key), 1, "失败触发不得出队")
        calls_after_failure = flaky.publish_calls

        # 退避窗口内：_submit_due_tasks 不得重提交（无第二次 publish 尝试）
        svc._submit_due_tasks()
        time.sleep(0.3)
        self.assertEqual(
            flaky.publish_calls, calls_after_failure,
            "退避窗口内被重提交（会造成同周期重复触发/打爆故障 Redis）",
        )
        self.assertEqual(svc.get_active_task_count(), 0)

        # 退避到期（人为拨快 next）后：允许重试，成功则出队
        rec = self.attempts_of(svc, member)
        self.assertGreaterEqual(int(rec["attempts"]), 1)
        self.redis.hset(
            svc.attempts_key,
            member,
            json.dumps({"attempts": int(rec["attempts"]), "next": int(time.time() * 1000) - 1}),
        )
        with patch.object(DelayService, "publish_resume_event", new_callable=AsyncMock):
            svc._submit_due_tasks()
            self.assertTrue(
                _wait_until(lambda: self.redis.zcard(self.scheduled_key) == 0),
                "退避到期后未重试出队",
            )

    def test_over_limit_goes_to_dead_letter(self):
        """重试超限：转显式死信 list 并出排程（不再无限重试）。"""
        flaky = _FlakyPublishRedis(self.redis)
        flaky.fail_next = 10**9
        svc = self.make_service(
            redis_client=flaky, extra_config={"max_trigger_attempts": 2}
        )
        member = json.dumps(_task(node_id="n4", execution_id="e4"))
        cfg = _task(node_id="n4", execution_id="e4")
        self.redis.zadd(self.scheduled_key, {member: int(time.time() * 1000) - 1})

        svc._run_scheduled_task(member, cfg)  # 第 1 次失败：仍在排程
        self.assertEqual(self.redis.zcard(self.scheduled_key), 1)
        self.assertEqual(self.redis.llen(self.dead_key), 0)

        svc._run_scheduled_task(member, cfg)  # 第 2 次失败：超限 → 死信
        self.assertEqual(self.redis.zcard(self.scheduled_key), 0, "超限后未出排程")
        self.assertEqual(self.redis.llen(self.dead_key), 1, "超限后未转死信")
        self.assertEqual(self.redis.lindex(self.dead_key, 0), member, "死信必须保留任务原文")
        self.assertIsNone(self.attempts_of(svc, member), "转死信后未清理重试计数")

        # 死信后不再触发：条目已不在 ZSET，到期轮询无从捡起
        calls_before = flaky.publish_calls
        svc._submit_due_tasks()
        time.sleep(0.2)
        self.assertEqual(flaky.publish_calls, calls_before, "死信条目仍被重试")

    def test_success_clears_stale_attempts_record(self):
        """成功路径清理遗留重试态（防 hash 泄漏与误退避）。"""
        svc = self.make_service()
        member = json.dumps(_task(node_id="n5", execution_id="e5"))
        self.redis.zadd(self.scheduled_key, {member: int(time.time() * 1000) - 1})
        self.redis.hset(
            svc.attempts_key,
            member,
            json.dumps({"attempts": 3, "next": int(time.time() * 1000) - 1}),
        )
        svc._run_scheduled_task(member, _task(node_id="n5", execution_id="e5"))
        self.assertEqual(self.redis.zcard(self.scheduled_key), 0, "成功后未出队")
        self.assertIsNone(self.attempts_of(svc, member), "成功后未清理重试计数")


if __name__ == "__main__":
    unittest.main()
