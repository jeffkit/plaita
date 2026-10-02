"""C4-3 回归（storage 层）：终态执行状态键 TTL + list_executions 走 scan_iter。

- 仅终态（completed/error/cancelled）SET 带 TTL（默认 30 天，env 可调）；
  非终态（running/suspended）必须无 TTL（可恢复执行的活动状态不过期）；
- keys()（O(N) 阻塞）→ scan_iter（游标式，语义等价）。
"""
from __future__ import annotations

import unittest

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis

from plaita.storage.base import ExecutionState
from plaita.storage.redis import (
    DEFAULT_EXECUTION_STATE_TTL_DAYS,
    RedisExecutionStorage,
    execution_state_ttl_seconds,
)


def _state(exec_id="e1", status="running", **extra):
    return ExecutionState(
        execution_id=exec_id,
        flow_id="f1",
        context={"k": "v"},
        status=status,
        **extra,
    )


class TestTerminalStateTTL(unittest.TestCase):
    def setUp(self):
        self.redis = fakeredis.FakeRedis(decode_responses=True)
        self.storage = RedisExecutionStorage(client=self.redis, namespace="plaita")
        self.key = "plaita:execution:e1"

    def test_terminal_states_get_ttl(self):
        for status in ("completed", "error", "cancelled"):
            self.redis.delete(self.key)
            self.storage.save_execution_state("e1", _state(status=status))
            ttl = self.redis.ttl(self.key)
            self.assertGreater(
                ttl, 0, f"终态 {status} 应带 TTL"
            )
            self.assertLessEqual(ttl, DEFAULT_EXECUTION_STATE_TTL_DAYS * 86400)

    def test_non_terminal_states_no_ttl(self):
        for status in ("running", "suspended"):
            self.redis.delete(self.key)
            self.storage.save_execution_state("e1", _state(status=status))
            self.assertEqual(
                self.redis.ttl(self.key), -1, f"非终态 {status} 不应有过期时间"
            )

    def test_ttl_days_env_override(self):
        with pytest.MonkeyPatch.context() as mp:
            mp.setenv("PLAITA_EXECUTION_STATE_TTL_DAYS", "7")
            self.assertEqual(execution_state_ttl_seconds(), 7 * 86400)
            self.redis.delete(self.key)
            self.storage.save_execution_state("e1", _state(status="completed"))
            ttl = self.redis.ttl(self.key)
            self.assertLessEqual(ttl, 7 * 86400)
            self.assertGreater(ttl, 0)

    def test_ttl_env_disable(self):
        with pytest.MonkeyPatch.context() as mp:
            mp.setenv("PLAITA_EXECUTION_STATE_TTL_DAYS", "0")
            self.assertEqual(execution_state_ttl_seconds(), 0)
            self.redis.delete(self.key)
            self.storage.save_execution_state("e1", _state(status="completed"))
            self.assertEqual(self.redis.ttl(self.key), -1)

    def test_ttl_env_invalid_falls_back_to_default(self):
        with pytest.MonkeyPatch.context() as mp:
            mp.setenv("PLAITA_EXECUTION_STATE_TTL_DAYS", "abc")
            self.assertEqual(execution_state_ttl_seconds(), DEFAULT_EXECUTION_STATE_TTL_DAYS * 86400)


class TestListExecutionsScan(unittest.TestCase):
    def setUp(self):
        self.redis = fakeredis.FakeRedis(decode_responses=True)
        self.storage = RedisExecutionStorage(client=self.redis, namespace="plaita")

    def test_scan_iter_used_not_keys(self):
        """list_executions 不再调用 O(N) 阻塞的 keys()。"""

        def _keys_boom(pattern):
            raise AssertionError("keys() 不应再被 list_executions 调用")

        self.redis.keys = _keys_boom
        for i in range(3):
            self.storage.save_execution_state(f"e{i}", _state(exec_id=f"e{i}"))
        rows = self.storage.list_executions()
        self.assertEqual(len(rows), 3)
        # 契约保持：返回完整 ExecutionState（含 context）
        self.assertTrue(all(r.context == {"k": "v"} for r in rows))

    def test_list_pagination_and_order(self):
        for i in range(5):
            self.storage.save_execution_state(
                f"e{i}",
                _state(exec_id=f"e{i}", start_time=f"2026-09-0{i+1}T00:00:00"),
            )
        rows = self.storage.list_executions(order_by="-start_time", limit=2)
        self.assertEqual([r.execution_id for r in rows], ["e4", "e3"])


if __name__ == "__main__":
    unittest.main()
