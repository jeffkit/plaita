"""A′（2026-10-06）：start 路径持租约——防多 worker 双跑同一 execution。

背景（plaita#41 多机验证实证）：`_acquire_lease` 此前唯一调用点在 resume_flow，
`start_flow` 不持租约。三后果：
①多 worker 抢到同一 start 消息时无闸可拦 → 双跑；
②死信守卫按「租约是否被持有」判活，start 执行恒"租约空" → 长步误死信；
③抢占者白烧 delivery 配额。

本测试锁定 A′ 的核心契约：**他人持租约时 start_flow 拒绝启动**。
"""
from __future__ import annotations

import unittest
from unittest.mock import patch

import pytest

pytest.importorskip("fakeredis")
from plaita.server.execution_lease import ExecutionLeaseError, RedisExecutionLease
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage


class TestStartFlowLease(unittest.TestCase):
    def setUp(self):
        import fakeredis
        self.redis = fakeredis.FakeRedis(decode_responses=True)
        self.lease = RedisExecutionLease(self.redis)
        self.execution_storage = MemoryExecutionStorage()
        self.flow_storage = MemoryFlowStorage()
        self.flow_storage.save_flow({
            "flow_id": "f1", "version": "1.0.0",
            "nodes": [
                {"id": "start", "type": "start", "next": "end"},
                {"id": "end", "type": "end", "output": "ok"},
            ],
        })

    def _worker(self):
        from plaita.server.flow_worker import FlowWorker
        return FlowWorker(
            self.execution_storage, self.flow_storage,
            execution_lease=self.lease, lease_ttl_seconds=60,
        )

    def test_start_refuses_when_lease_held_by_other(self):
        """他人持租约 → start_flow 抛 ExecutionLeaseError（不双跑）。"""
        self.assertTrue(self.lease.try_acquire("exec-1", "other-holder", 60))
        worker = self._worker()
        with self.assertRaises(ExecutionLeaseError):
            worker.start_flow("f1", params={}, version="1.0.0",
                              execution_id="exec-1")
        self.lease.release("exec-1", "other-holder")

    def test_start_acquires_and_releases_lease(self):
        """start 成功路径：租约被取得、执行完释放（他人随后可 claim）。"""
        worker = self._worker()
        result = worker.start_flow("f1", params={}, version="1.0.0",
                                   execution_id="exec-2")
        assert result.get("execution_id") == "exec-2"
        # 执行终态后租约必须已释放——否则同 id 永远无法被 resume/重入
        self.assertTrue(
            self.lease.try_acquire("exec-2", "probe", 60),
            "start 结束时未释放租约 → 后续 resume/重投永久被拒",
        )
        self.lease.release("exec-2", "probe")

    def test_start_releases_lease_on_error(self):
        """执行抛错也要释放租约（finally 语义），不得泄漏。"""
        from plaita.core.flow import Flow
        worker = self._worker()
        with patch.object(worker, "get_flow_definition") as gf:
            gf.return_value = Flow.model_validate({
                "flow_id": "f1", "version": "1.0.0",
                "nodes": [
                    {"id": "start", "type": "start", "next": "end"},
                    {"id": "end", "type": "end", "output": "ok"},
                ],
            })
            with patch.object(worker, "_bind_observers", side_effect=RuntimeError("boom")):
                with self.assertRaises(Exception):
                    worker.start_flow("f1", params={}, version="1.0.0",
                                      execution_id="exec-3")
        # 无论成败，租约都不该被本 worker 持有
        self.assertTrue(
            self.lease.try_acquire("exec-3", "probe", 60),
            "异常路径未释放租约 → 租约泄漏，同 id 永久卡死",
        )
