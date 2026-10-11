"""#56 回归：``plaita:approval:queue`` 的派发必须有人消费。

基线缺陷：worker 在 ``is_suspend`` 分支把审批任务 RPUSH 进
``plaita:approval:queue``（flow_worker._dispatch_service_task），而
``ApprovalService.start_service`` 只置 ``is_running = True``——队列没有读取
方，审批记录（``plaita:approval:pending:{id}``）永不创建，审批节点永久
suspended（``subscription_timeout`` 缺省 None，连超时兜底都没有）。

修复契约：
- ``start_service`` 起常驻消费线程，按 DelayService 的轮询骨架消费（不用
  BLPOP：出队即内存持有、崩溃即丢，见 #11）；处理走完才 LREM（at-least-once），
  重放不覆盖已落记录的审批。
- ``stop_service`` 干净停掉消费线程（按线程名可枚举检查，不残留）。
- ApprovalNode 缺省 ``subscription_timeout`` 非 None：无人审批最终落 timeout
  终态，而不是永久 suspended。
"""
from __future__ import annotations

import asyncio
import json
import threading
import time
import unittest

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis

from plaita.core.context import ExecutionContext
from plaita.core.executor import _subscribe_event
from plaita.event.memory import InMemoryEventBus
from plaita.node.event_node import EventNodeStatus
from plaita.server.flow_worker import FlowWorker
from plaita.server.nodes.approval_node import (
    APPROVAL_DEFAULT_SUBSCRIPTION_TIMEOUT_SECONDS,
    ApprovalNode,
)
from plaita.server.services.approval_service import (
    APPROVAL_DEFAULT_QUEUE_KEY,
    ApprovalService,
)
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage


QUEUE_KEY = APPROVAL_DEFAULT_QUEUE_KEY
CONSUMER_THREAD_NAME = "approval-service-consumer"


def _approval_task(approval_id="a1", execution_id="e1"):
    return {
        "type": "approval",
        "approval_id": approval_id,
        "node_id": "approval_1",
        "execution_id": execution_id,
        "flow_id": "f1",
        "event_type": "approval_decision",
        "approval_config": {"title": "请审批", "content": "内容"},
        "approver_config": {"approvers": ["u1", "u2"], "strategy": "all"},
        "form_config": {"fields": []},
    }


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _wait_until(cond, timeout=5.0, interval=0.02):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return True
        time.sleep(interval)
    return cond()


def _consumer_threads():
    return [t for t in threading.enumerate() if t.name == CONSUMER_THREAD_NAME]


class ApprovalQueueConsumerTest(unittest.TestCase):
    def setUp(self):
        self.redis = fakeredis.FakeRedis(decode_responses=True)
        self.bus = InMemoryEventBus()
        self.services = []

    def tearDown(self):
        for svc in self.services:
            svc.stop_service()

    def make_service(self, config=None, redis_client="default"):
        svc = ApprovalService(
            self.bus,
            config or {"poll_interval": 0.05},
            redis_client=self.redis if redis_client == "default" else redis_client,
        )
        self.services.append(svc)
        return svc

    def test_worker_dispatch_is_consumed_and_record_created(self):
        """F1 段：真实 worker 派发 → 消费线程读走 → 审批记录落盘、队列清空。"""
        worker = FlowWorker(
            execution_storage=MemoryExecutionStorage(),
            flow_storage=MemoryFlowStorage(),
            event_bus=self.bus,
        )
        worker.redis_client = self.redis
        worker._dispatch_service_task(
            {"service_config": _approval_task()}, {"$LAST_NODE": "approval_1"}, "e1"
        )
        self.assertEqual(self.redis.llen(QUEUE_KEY), 1, "派发侧未投递到审批队列")

        svc = self.make_service()
        self.assertTrue(svc.start_service())

        self.assertTrue(
            _wait_until(lambda: self.redis.llen(QUEUE_KEY) == 0),
            f"队列未被消费，仍有 {self.redis.llen(QUEUE_KEY)} 条",
        )
        raw = self.redis.get("plaita:approval:pending:a1")
        self.assertIsNotNone(raw, "审批记录未创建——控制面看不到待办审批")
        record = json.loads(raw)
        self.assertEqual(record["status"], "pending")
        self.assertEqual(record["required_approvers"], ["u1", "u2"])
        self.assertGreater(self.redis.ttl("plaita:approval:pending:a1"), 0)

    def test_raw_rpush_task_consumed(self):
        """生产者契约：直接 RPUSH 的审批任务同样被消费（队列键契约）。"""
        self.redis.rpush(QUEUE_KEY, json.dumps(_approval_task("a2"), ensure_ascii=False))
        svc = self.make_service()
        svc.start_service()

        self.assertTrue(_wait_until(lambda: self.redis.llen(QUEUE_KEY) == 0))
        self.assertIn("a2", svc.get_pending_approvals())

    def test_multiple_tasks_all_consumed(self):
        for i in range(5):
            self.redis.rpush(QUEUE_KEY, json.dumps(_approval_task(f"a{i}")))
        svc = self.make_service()
        svc.start_service()

        self.assertTrue(_wait_until(lambda: self.redis.llen(QUEUE_KEY) == 0))
        self.assertTrue(
            _wait_until(lambda: len(svc.get_pending_approvals()) == 5),
            f"只创建了 {len(svc.get_pending_approvals())} 条审批记录",
        )

    def test_malformed_entry_discarded_without_blocking_queue(self):
        """坏条目就地丢弃并告警，不卡住后续任务（也不无限重读）。"""
        self.redis.rpush(QUEUE_KEY, "not-json")
        self.redis.rpush(QUEUE_KEY, json.dumps({"type": "approval", "node_id": "approval_1"}))
        self.redis.rpush(QUEUE_KEY, json.dumps(_approval_task("a3")))
        svc = self.make_service()
        svc.start_service()

        self.assertTrue(_wait_until(lambda: self.redis.llen(QUEUE_KEY) == 0))
        self.assertIn("a3", svc.get_pending_approvals())

    def test_replay_does_not_clobber_decided_record(self):
        """at-least-once 重放：处理完 LREM 前崩溃/重投，不得清掉已落的审批决策。"""
        task = _approval_task("a4")
        self.redis.rpush(QUEUE_KEY, json.dumps(task))
        svc = self.make_service()
        svc.start_service()
        self.assertTrue(_wait_until(lambda: self.redis.llen(QUEUE_KEY) == 0))

        result = _run(svc.submit_approval_decision("a4", "u1", "approve"))
        self.assertEqual(result["status"], "success")

        # 同一条任务被再次投递（崩溃重放 / worker 重投）
        self.redis.rpush(QUEUE_KEY, json.dumps(task))
        self.assertTrue(_wait_until(lambda: self.redis.llen(QUEUE_KEY) == 0))

        details = svc.get_approval_details("a4")
        self.assertEqual(details["status"], "pending")
        self.assertEqual(len(details["approvals"]), 1, "重放覆盖了已落记录的审批决策")

    def test_stop_service_leaves_no_consumer_thread(self):
        """stop_service 干净停掉消费线程（进程内按线程名枚举，不残留）。"""
        svc = self.make_service()
        svc.start_service()
        self.assertEqual(len(_consumer_threads()), 1)

        self.assertTrue(svc.stop_service())
        self.assertEqual(_consumer_threads(), [], "停机后仍有消费线程残留")
        self.assertFalse(svc.is_running)

    def test_no_redis_client_starts_without_consumer_thread(self):
        """无 redis 客户端（单测/内存模式）：不起消费线程，启动行为不变。"""
        svc = self.make_service(redis_client=None)
        self.assertTrue(svc.start_service())
        self.assertTrue(svc.is_running)
        self.assertEqual(_consumer_threads(), [])

    def test_stopped_service_stops_consuming(self):
        """停机后不再消费：新入队的任务留给下一实例（不静默出队）。"""
        svc = self.make_service()
        svc.start_service()
        svc.stop_service()
        self.redis.rpush(QUEUE_KEY, json.dumps(_approval_task("a5")))
        time.sleep(0.3)
        self.assertEqual(self.redis.llen(QUEUE_KEY), 1)
        self.assertNotIn("a5", svc.get_pending_approvals())


class _RecordingAsyncBus:
    """记录 register_subscription 收到的参数集。"""

    def __init__(self):
        self.calls = []

    async def register_subscription(self, **params):
        self.calls.append(params)
        return "sub-56"


class _FakeFlow:
    flow_id = "flow-56"


def _subscribe(node, bus):
    context = ExecutionContext(event_bus=bus)
    node_state = {"event_type": node.event_type, "status": EventNodeStatus.PENDING.value}
    return _run(_subscribe_event(node, _FakeFlow(), node_state, context))


class TestApprovalNodeTimeoutFallback:
    """无人审批的可观测兜底：审批节点缺省订阅超时非 None（#56 后半）。"""

    def test_default_subscription_timeout_is_not_none(self):
        node = ApprovalNode(
            id="appr", approval_title="t", approval_content="c", approvers=["u1"]
        )
        assert node.subscription_timeout == APPROVAL_DEFAULT_SUBSCRIPTION_TIMEOUT_SECONDS

    def test_explicit_none_restores_unlimited_wait(self):
        node = ApprovalNode(
            id="appr",
            approval_title="t",
            approval_content="c",
            approvers=["u1"],
            subscription_timeout=None,
        )
        assert node.subscription_timeout is None

    def test_camelcase_alias_still_accepted(self):
        node = ApprovalNode(
            id="appr",
            approval_title="t",
            approval_content="c",
            approvers=["u1"],
            subscriptionTimeout=60,
        )
        assert node.subscription_timeout == 60

    def test_default_node_subscribes_with_timeout(self):
        """缺省审批节点把 timeout 透传给订阅——event_filter 才能落 timeout 终态。"""
        bus = _RecordingAsyncBus()
        node = ApprovalNode(
            id="appr", approval_title="t", approval_content="c", approvers=["u1"]
        )
        assert _subscribe(node, bus) is True
        assert bus.calls[0]["timeout"] == APPROVAL_DEFAULT_SUBSCRIPTION_TIMEOUT_SECONDS


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
