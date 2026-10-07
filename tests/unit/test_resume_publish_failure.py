"""#35：resume 事件发布失败必须上抛，调用方不得把它当成功。

历史缺陷：``base_service.publish_resume_event`` 捕获一切异常只
``logger.error``（「发布失败只告警，不打断任务主流程」）——调用方无法区分
「已唤醒」与「没唤醒」：

- ``DelayService._run_scheduled_task`` 成功/失败同路径 ZREM，到期瞬间
  Redis 抖动 = publish 失败 + 任务出排程，挂起执行永久失醒（delay 侧的
  重试补偿见 test_clean_c4_delay_service.TestTriggerFailureRetry）；
- ``ApprovalService._submit_decision_redis`` 自吞异常后照旧删记录 → 审批人
  连重试的机会都没有；
- ``HttpCallbackService.handle_callback_request`` 原子认领（Lua GET+DEL）
  已删注册记录，自吞异常后回调永久丢失。

修复：``publish_resume_event`` 抛 ``ResumeEventPublishError``；approval 的
Redis 路径靠「记录未回写/未删除」天然可重试（内存路径回滚本次追加），
callback 触发失败回写被认领删掉的注册记录。
"""
from __future__ import annotations

import asyncio
import unittest
from unittest.mock import AsyncMock, patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis

from plaita.event.memory import InMemoryEventBus
from plaita.server.services.approval_service import ApprovalService
from plaita.server.services.base_service import ResumeEventPublishError
from plaita.server.services.http_callback_service import HttpCallbackService


def _approval_task(approval_id="a1", execution_id="e1", strategy="any"):
    return {
        "approval_id": approval_id,
        "node_id": "approval_1",
        "execution_id": execution_id,
        "flow_id": "f1",
        "event_type": "approval_decision",
        "approval_config": {"title": "请审批"},
        "approver_config": {"approvers": ["u1", "u2"], "strategy": strategy},
    }


def _callback_task(path="/cb/1", execution_id="e1"):
    return {
        "node_id": "cb_1",
        "execution_id": execution_id,
        "flow_id": "f1",
        "event_type": "http_callback_triggered",
        "callback_config": {"path": path},
    }


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class PublishResumeEventRaisesTest(unittest.TestCase):
    """publish 失败上抛（不再是 logger.error 吞掉）。"""

    def test_redis_channel_publish_failure_raises(self):
        redis_client = fakeredis.FakeRedis(decode_responses=True)
        svc = ApprovalService(AsyncMock(), {}, redis_client=redis_client)
        with patch.object(
            redis_client, "publish", side_effect=ConnectionError("redis 瞬断")
        ):
            with self.assertRaises(ResumeEventPublishError) as ctx:
                _run(svc.publish_resume_event(
                    "approval", {"execution_id": "e1", "tenant_id": "default"},
                ))
        self.assertIn("approval", str(ctx.exception))

    def test_bus_publish_failure_raises(self):
        bus = InMemoryEventBus()
        svc = ApprovalService(bus, {}, redis_client=None)
        with patch.object(
            bus, "publish", new_callable=AsyncMock,
            side_effect=RuntimeError("bus down"),
        ):
            with self.assertRaises(ResumeEventPublishError):
                _run(svc.publish_resume_event("approval", {"execution_id": "e1"}))


class ApprovalTriggerFailureTest(unittest.TestCase):
    """审批定案事件发布失败：记录保持可重试，不静默删。"""

    def setUp(self):
        self.redis = fakeredis.FakeRedis(decode_responses=True)
        self.bus = InMemoryEventBus()

    def test_redis_decision_stays_retryable_after_publish_failure(self):
        svc = ApprovalService(self.bus, {}, redis_client=self.redis)
        _run(svc.handle_task(_approval_task()))

        with patch.object(
            self.redis, "publish", side_effect=ConnectionError("redis 瞬断")
        ):
            failed = _run(svc.submit_approval_decision("a1", "u1", "approve"))
        self.assertEqual(failed["status"], "error", "发布失败却报成功")
        # 记录未被删除、决策未落盘 → 审批人可重试（历史实现记录已删，执行永久失联）
        self.assertEqual(
            svc.get_pending_approvals()["a1"]["current_approvals"], 0,
            "发布失败却已把决策记入记录",
        )

        retry = _run(svc.submit_approval_decision("a1", "u1", "approve"))
        self.assertEqual(retry["status"], "success")
        self.assertEqual(retry["final_decision"], "approve")

    def test_memory_decision_rolled_back_after_publish_failure(self):
        bus = InMemoryEventBus()
        svc = ApprovalService(bus, {})
        _run(svc.handle_task(_approval_task()))

        with patch.object(
            bus, "publish", new_callable=AsyncMock,
            side_effect=RuntimeError("bus down"),
        ):
            failed = _run(svc.submit_approval_decision("a1", "u1", "approve"))
        self.assertEqual(failed["status"], "error")

        retry = _run(svc.submit_approval_decision("a1", "u1", "approve"))
        self.assertEqual(retry["status"], "success", "回滚缺失：重试被判「已经审批过」")


class CallbackTriggerFailureTest(unittest.TestCase):
    """回调触发失败：原子认领删掉的注册记录回写，外部重试可再触发。"""

    def setUp(self):
        self.redis = fakeredis.FakeRedis(decode_responses=True)
        self.bus = InMemoryEventBus()

    def test_registration_restored_after_publish_failure(self):
        svc = HttpCallbackService(self.bus, {}, redis_client=self.redis)
        _run(svc.handle_task(_callback_task()))

        with patch.object(
            self.redis, "publish", side_effect=ConnectionError("redis 瞬断")
        ):
            resp = _run(svc.handle_callback_request("/cb/1", {"ok": 1}))
        self.assertEqual(resp["status"], "error", "触发失败却报成功")
        self.assertIn("/cb/1", svc.get_registered_callbacks(), "注册记录未回写")

        # 恢复：重试能再次认领并触发
        retry = _run(svc.handle_callback_request("/cb/1", {"ok": 1}))
        self.assertEqual(retry, {"status": "success"})
        self.assertNotIn("/cb/1", svc.get_registered_callbacks())

    def test_normally_claimed_once_unchanged(self):
        """正常路径零变化：认领即删，重复到达得「未注册」。"""
        svc = HttpCallbackService(self.bus, {}, redis_client=self.redis)
        _run(svc.handle_task(_callback_task()))
        self.assertEqual(
            _run(svc.handle_callback_request("/cb/1", {"ok": 1})),
            {"status": "success"},
        )
        again = _run(svc.handle_callback_request("/cb/1", {"ok": 1}))
        self.assertEqual(again["message"], "回调路径未注册")


if __name__ == "__main__":
    unittest.main()
