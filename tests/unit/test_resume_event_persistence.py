"""Track P2 任务2：服务直发频道的 resume 事件尽力落盘，reconciler 补偿得到。

历史缺陷：第一波 publish_resume_event（base_service.py）为绕开 aioredis
跨 loop 问题，用同步 redis 直发 ``plaita:events:{type}`` 频道——不经
RedisEventBus.publish → 事件不落存储 → EventReconciler（扫事件存储的
``plaita:event:types:{type}`` zset 索引做兜底）补偿不到它们。Pub/Sub
丢通知窗口内，审批/回调/delay 的 resume 照样丢。

修复：publish_resume_event 直发前按 RedisEventStorage.store_event 的键格式
（``plaita:event:events:{id}`` SET + ``plaita:event:types:{type}`` ZADD，
TTL 7 天）同步尽力落盘；失败只 warning（fail-open，不阻塞 resume 主链路）。
"""
from __future__ import annotations

import asyncio
import json
import time
import unittest
import unittest.mock
from unittest.mock import AsyncMock

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis

from plaita.event.core import Event, EventSubscription
from plaita.event.memory import InMemoryEventBus, InMemoryEventSubscriptionStorage
from plaita.event.redis import RedisEventStorage
from plaita.server.event_filter import EventFilter
from plaita.server.event_reconcile import EventReconciler
from plaita.server.services.approval_service import ApprovalService
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage

QUEUE = "test:resume-persist:queue"


def run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _stream_payloads(redis_client, stream_key):
    entries = redis_client.xrange(stream_key, min="-", max="+")
    tasks = []
    for _mid, fields in entries:
        raw = fields.get("payload") or fields.get(b"payload")
        if isinstance(raw, bytes):
            raw = raw.decode()
        tasks.append(json.loads(raw))
    return tasks


class ResumeEventPersistenceTest(unittest.TestCase):
    """共享一个 FakeServer：服务的同步客户端与事件存储（async）同数据面。"""

    def setUp(self):
        self.server = fakeredis.FakeServer()
        self.redis_client = fakeredis.FakeRedis(
            server=self.server, decode_responses=True
        )
        self.event_storage = RedisEventStorage(
            redis_url="redis://localhost:6379/15", ttl=3600
        )
        self.event_storage.redis = fakeredis.aioredis.FakeRedis(
            server=self.server, decode_responses=True
        )
        self.event_filter = EventFilter(
            execution_storage=MemoryExecutionStorage(),
            subscription_storage=InMemoryEventSubscriptionStorage(),
            redis_client=self.redis_client,
            event_bus=AsyncMock(),
            queue_name=QUEUE,
        )
        self.event_filter.execution_storage.save_execution_state(
            "exec-r1",
            ExecutionState(
                execution_id="exec-r1", flow_id="flow-r1", status="suspended",
                context={},
            ),
        )
        run(self.event_filter.subscription_storage.store_subscription(
            EventSubscription(
                event_type="approval", correlation_id="exec-r1", flow_id="flow-r1",
            )
        ))
        self.service = ApprovalService(
            event_bus=AsyncMock(),
            service_config={},
            redis_client=self.redis_client,
        )

    def test_direct_published_resume_event_is_reconciled(self):
        """核心场景：直发频道的 resume 事件落盘后，reconciler 扫描补偿成 resume。"""
        run(self.service.publish_resume_event(
            "approval",
            {"execution_id": "exec-r1", "tenant_id": "default", "approved": True},
        ))

        reconciler = EventReconciler(
            self.event_storage, self.event_filter, self.redis_client
        )
        replayed = run(reconciler.scan_once())

        self.assertEqual(replayed, 1, "直发的 resume 事件未落存储，回扫补偿不到")
        tasks = _stream_payloads(self.redis_client, QUEUE)
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0]["type"], "resume")
        self.assertEqual(tasks[0]["execution_id"], "exec-r1")

    def test_persisted_event_matches_redis_event_storage_shape(self):
        """落盘键格式与 RedisEventStorage.store_event 一致（reconciler 扫描契约）。"""
        run(self.service.publish_resume_event(
            "approval",
            {"execution_id": "exec-r1", "tenant_id": "default", "approved": True},
        ))

        event_keys = self.redis_client.keys("plaita:event:events:*")
        self.assertEqual(len(event_keys), 1, "事件数据键未落盘")
        event_key = event_keys[0]
        # 两键都带 TTL（7 天自清理；测试里显式给存储配了 ttl=3600，断言 >0 即可）
        self.assertGreater(self.redis_client.ttl(event_key), 0, "事件数据键未设 TTL")

        members = self.redis_client.zrange("plaita:event:types:approval", 0, -1)
        self.assertEqual(len(members), 1, "类型索引 zset 未落盘")
        self.assertEqual(
            self.redis_client.zscore("plaita:event:types:approval", members[0]),
            Event.model_validate_json(
                self.redis_client.get(event_key)
            ).timestamp,
            "zset score 必须是事件时间戳（reconciler 按时间窗扫描）",
        )
        self.assertGreater(
            self.redis_client.ttl("plaita:event:types:approval"), 0,
            "类型索引键未设 TTL",
        )

    def test_persist_failure_fail_open(self):
        """落盘失败只 warning：直发主链路照常、异常不外溢。"""

        class _BrokenPipelineClient:
            """除 pipeline 外全部委托（pipeline 一炸即模拟存储故障）。"""

            def __init__(self, inner):
                self._inner = inner

            def __getattr__(self, name):
                return getattr(self._inner, name)

            def pipeline(self):
                raise RuntimeError("redis down")

        broken = _BrokenPipelineClient(self.redis_client)
        service = ApprovalService(
            event_bus=AsyncMock(), service_config={}, redis_client=broken
        )

        pubsub = self.redis_client.pubsub()
        pubsub.subscribe("plaita:events:approval")
        try:
            run(service.publish_resume_event(
                "approval",
                {"execution_id": "exec-r1", "tenant_id": "default"},
            ))
            # 直发主链路不受落盘故障影响
            deadline = time.time() + 5
            msg = None
            while time.time() < deadline:
                msg = pubsub.get_message(timeout=0.2)
                if msg and msg.get("type") == "message":
                    break
            self.assertIsNotNone(msg, "落盘失败时直发主链路被误伤")
        finally:
            pubsub.close()

    def test_memory_bus_fallback_stays_in_memory(self):
        """无 redis_client（进程内 InMemoryEventBus）：走总线发布、不落 Redis。"""
        bus = InMemoryEventBus()
        service = ApprovalService(
            event_bus=bus, service_config={}, redis_client=None
        )

        with unittest.mock.patch.object(
            bus, "publish", new_callable=AsyncMock
        ) as publish:
            run(service.publish_resume_event(
                "approval", {"execution_id": "exec-r1"},
            ))
            publish.assert_awaited_once()

        self.assertEqual(self.redis_client.keys("plaita:event:*"), [])

    def test_publish_failure_propagates(self):
        """直发失败上抛（不再吞）：调用方据此保留排程重试。

        这是「挂起执行最后一跳」的唤醒凭据——吞掉会让 DelayService 以为
        触发成功而 ZREM 出排程，挂起执行永久失醒。
        """
        with unittest.mock.patch.object(
            self.redis_client, "publish", side_effect=RuntimeError("redis down")
        ):
            with self.assertRaises(RuntimeError):
                run(self.service.publish_resume_event(
                    "approval", {"execution_id": "exec-r1"},
                ))

    def test_memory_bus_publish_failure_propagates(self):
        """无 redis 回退路径同样上抛（event_bus.publish 失败不吞）。"""
        bus = InMemoryEventBus()
        service = ApprovalService(bus, {}, redis_client=None)
        with unittest.mock.patch.object(
            bus, "publish", new_callable=AsyncMock, side_effect=RuntimeError("bus down")
        ):
            with self.assertRaises(RuntimeError):
                run(service.publish_resume_event(
                    "approval", {"execution_id": "exec-r1"},
                ))


if __name__ == "__main__":
    unittest.main()
