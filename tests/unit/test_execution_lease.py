"""Execution resume lease: at most one concurrent resume per execution_id."""
from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

import fakeredis

from plaita.server.execution_lease import (
    ExecutionLeaseError,
    NullExecutionLease,
    RedisExecutionLease,
)
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage


class TestRedisExecutionLease(unittest.TestCase):
    def setUp(self):
        self.redis = fakeredis.FakeRedis(decode_responses=True)
        self.lease = RedisExecutionLease(self.redis)

    def test_acquire_exclusive(self):
        self.assertTrue(self.lease.try_acquire("e1", "h1", 30))
        self.assertFalse(self.lease.try_acquire("e1", "h2", 30))
        self.assertTrue(self.lease.release("e1", "h1"))
        self.assertTrue(self.lease.try_acquire("e1", "h2", 30))

    def test_release_only_by_owner(self):
        self.lease.try_acquire("e1", "h1", 30)
        self.assertFalse(self.lease.release("e1", "h2"))
        self.assertFalse(self.lease.try_acquire("e1", "h3", 30))
        self.assertTrue(self.lease.release("e1", "h1"))

    def test_renew_only_by_owner(self):
        self.lease.try_acquire("e1", "h1", 30)
        self.assertTrue(self.lease.renew("e1", "h1", 60))
        self.assertFalse(self.lease.renew("e1", "h2", 60))

    def test_null_lease_always_succeeds(self):
        null = NullExecutionLease()
        self.assertTrue(null.try_acquire("e", "h", 1))
        self.assertTrue(null.release("e", "h"))
        self.assertTrue(null.renew("e", "h", 1))


class TestResumeFlowLease(unittest.TestCase):
    def setUp(self):
        self.execution_storage = MemoryExecutionStorage()
        self.flow_storage = MemoryFlowStorage()
        self.flow_storage.save_flow(
            {
                "flow_id": "f1",
                "version": "1",
                "nodes": [
                    {"id": "start", "type": "start", "next": "end"},
                    {"id": "end", "type": "end", "output": "ok"},
                ],
            }
        )
        self.execution_storage.save_execution_state(
            "exec-1",
            ExecutionState(
                execution_id="exec-1",
                flow_id="f1",
                flow_version="1",
                context={"$LAST_NODE": "start", "$NODE": {}},
                status="suspended",
            ),
        )

    def test_second_resume_raises_lease_error(self):
        from plaita.server.flow_worker import FlowWorker

        redis = fakeredis.FakeRedis(decode_responses=True)
        lease = RedisExecutionLease(redis)
        worker_a = FlowWorker(
            self.execution_storage,
            self.flow_storage,
            execution_lease=lease,
            lease_ttl_seconds=60,
        )
        worker_b = FlowWorker(
            self.execution_storage,
            self.flow_storage,
            execution_lease=lease,
            lease_ttl_seconds=60,
        )

        # A holds lease while B tries resume
        self.assertTrue(lease.try_acquire("exec-1", "holder-a", 60))
        with self.assertRaises(ExecutionLeaseError):
            worker_b.resume_flow("f1", "exec-1", "continue")
        lease.release("exec-1", "holder-a")

        # After release, resume can proceed (mock run_distributed to avoid full engine)
        with patch.object(worker_a, "get_flow_definition") as gf:
            from plaita.core.flow import Flow

            flow = Flow.model_validate(
                {
                    "flow_id": "f1",
                    "version": "1",
                    "nodes": [
                        {"id": "start", "type": "start", "next": "end"},
                        {"id": "end", "type": "end", "output": "ok"},
                    ],
                }
            )
            gf.return_value = flow
            with patch("plaita.server.flow_worker.FlowExecution") as FE:
                inst = MagicMock()
                FE.return_value = inst
                inst.run_distributed.return_value = {
                    "execution_id": "exec-1",
                    "is_end": True,
                    "is_suspend": False,
                    "context": {},
                    "result": "ok",
                }
                result = worker_a.resume_flow("f1", "exec-1", "continue")
                self.assertTrue(result.get("is_end"))
                # lease released in finally
                self.assertTrue(lease.try_acquire("exec-1", "after", 60))


if __name__ == "__main__":
    unittest.main()
