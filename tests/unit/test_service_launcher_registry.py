"""启动器注册面（2026-10-26）：``python -m plaita.server.services``（单跑 / wake
值班室同路）必须经 ``start_with_registry`` 落注册键。

点名闸（3171cad0）按 ``plaita:registry:{service_type}:*`` 活键判「唤醒岗在
不在」；启动器旧路只调 ``start_service``——``init_registry`` 只构造不写键，
注册键**从不落盘**：服务在跑、消费在转，集群档带唤醒节点的流程却一律 422
``queue_service_absent``（main 预存红：E2E「启动审批/延迟流程」三红同源，
生产集群同雷）。

钉两件事：
1. 四家唤醒 source 的 ``start_with_registry`` 各落自己的
   ``plaita:registry:{service_type}:*`` 活键（TTL 内 = 活着）；
2. ``shutdown`` 注销清键——停机后点名不再误判在岗。
"""
import unittest

import fakeredis

from plaita.event.memory import InMemoryEventBus
from plaita.server.services.approval_service import ApprovalService
from plaita.server.services.delay_service import DelayService
from plaita.server.services.http_callback_service import HttpCallbackService
from plaita.server.services.redis_queue_service import RedisQueueService


def _has_live_key(redis, service_type):
    return bool(list(redis.scan_iter(f"plaita:registry:{service_type}:*", count=100)))


class TestLauncherRegistersServices(unittest.TestCase):
    def _roundtrip(self, svc, service_type):
        self.assertTrue(svc.start_with_registry())
        self.assertTrue(
            _has_live_key(svc._redis_client, service_type),
            f"{service_type} 注册键未落盘——点名闸会误判岗缺",
        )
        svc.shutdown()
        self.assertFalse(
            _has_live_key(svc._redis_client, service_type),
            f"{service_type} 停机后注册键未注销——点名闸会误判在岗",
        )

    def test_delay_registers_and_unregisters(self):
        redis = fakeredis.FakeRedis(decode_responses=True)
        self._roundtrip(DelayService(InMemoryEventBus(), redis_client=redis), "delay")

    def test_approval_registers_and_unregisters(self):
        redis = fakeredis.FakeRedis(decode_responses=True)
        self._roundtrip(
            ApprovalService(InMemoryEventBus(), redis_client=redis), "approval"
        )

    def test_http_callback_registers_and_unregisters(self):
        redis = fakeredis.FakeRedis(decode_responses=True)
        self._roundtrip(
            HttpCallbackService(InMemoryEventBus(), redis_client=redis),
            "http_callback",
        )

    def test_redis_queue_registers_and_unregisters(self):
        redis = fakeredis.FakeRedis(decode_responses=True)
        self._roundtrip(
            RedisQueueService(redis_client=redis), "redis_queue"
        )


if __name__ == "__main__":
    unittest.main()
