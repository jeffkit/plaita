"""Track P2 任务1：ServiceManager → 服务的 redis_client 接线。

历史缺陷：ServiceManager 只收 event_bus，start_all_services / restart_service
以 ``service_class(self.event_bus, config)`` 两参构造服务——approval /
http_callback / delay 经它拉起时收不到 redis_client：

- 审批/回调记录回退进程内 dict（Track C 的 Redis 跨实例共享不生效）；
- resume 事件走 event_bus.publish（aioredis 客户端绑定引擎 loop，跨 loop
  使用会静默失败）而非同步 redis 直发频道。

修复：构造器接受可选 redis_client，构造服务时以 ``inspect.signature`` 探测
服务类是否收 ``redis_client`` 参数（flow_worker._get_task_queue 的
dead_letter_guard 签名探测先例）——``RedisQueueService(event_bus,
retry_config)`` 等旧签名服务与自定义三参服务不受影响。
"""
from __future__ import annotations

import unittest

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis

from plaita.event.memory import InMemoryEventBus
from plaita.server.services.approval_service import ApprovalService
from plaita.server.services.base_service import BaseExtendedService
from plaita.server.services.delay_service import DelayService
from plaita.server.services.http_callback_service import HttpCallbackService
from plaita.server.services.service_manager import ServiceManager


class _LegacyTwoParamService(BaseExtendedService):
    """旧签名自定义服务：(event_bus, service_config)——不收 redis_client。"""

    def __init__(self, event_bus, service_config=None):
        super().__init__(event_bus, service_config)
        self.constructed_with = (event_bus, service_config)

    def get_service_type(self) -> str:
        return "legacy"

    def start_service(self) -> bool:
        self.is_running = True
        return True

    def stop_service(self) -> bool:
        self.is_running = False
        return True

    async def handle_task(self, task_config):
        return True


class ServiceManagerRedisWiringTest(unittest.TestCase):
    def setUp(self):
        self.redis_client = fakeredis.FakeRedis(decode_responses=True)
        self.bus = InMemoryEventBus()
        self.managers = []

    def tearDown(self):
        for manager in self.managers:
            try:
                manager.stop_all_services(timeout=2)
            except Exception:
                pass

    def _make_manager(self, **kwargs):
        manager = ServiceManager(self.bus, **kwargs)
        self.managers.append(manager)
        return manager

    def test_started_services_receive_redis_client(self):
        """经 ServiceManager 启动的三服务实例拿到 redis_client（修复前为 None）。"""
        manager = self._make_manager(redis_client=self.redis_client)
        manager.service_classes = {
            "approval": ApprovalService,
            "http_callback": HttpCallbackService,
            "delay": DelayService,
        }

        self.assertTrue(manager.start_all_services({}))

        for service_type in ("approval", "http_callback", "delay"):
            service = manager.get_service(service_type)
            self.assertIsNotNone(service, f"{service_type} 未启动")
            self.assertIs(
                service._redis_client,
                self.redis_client,
                f"{service_type} 未接到 ServiceManager 持有的 redis_client",
            )

    def test_restart_service_preserves_redis_client(self):
        """restart_service 重建实例同样接线（修复前 restart 走两参构造）。"""
        manager = self._make_manager(redis_client=self.redis_client)
        manager.service_classes = {"approval": ApprovalService}

        self.assertTrue(manager.start_all_services({}))
        self.assertTrue(manager.restart_service("approval", {"max_workers": 2}))

        service = manager.get_service("approval")
        self.assertIs(service._redis_client, self.redis_client)

    def test_legacy_two_param_service_still_constructed(self):
        """旧 (event_bus, service_config) 服务不受影响：不传 redis_client 参数。"""
        manager = self._make_manager(redis_client=self.redis_client)
        manager.service_classes = {"legacy": _LegacyTwoParamService}

        self.assertTrue(manager.start_all_services({}))
        service = manager.get_service("legacy")
        self.assertIsNotNone(service, "旧签名服务启动失败（签名探测回归）")
        self.assertIsNone(service._redis_client)

    def test_no_redis_client_still_works(self):
        """不传 redis_client（历史用法）行为不变：服务照常启动、客户端为 None。"""
        manager = self._make_manager()
        manager.service_classes = {"approval": ApprovalService}

        self.assertTrue(manager.start_all_services({}))
        service = manager.get_service("approval")
        self.assertIsNotNone(service)
        self.assertIsNone(service._redis_client)


if __name__ == "__main__":
    unittest.main()
