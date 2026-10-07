"""#37 token 用量归集回归：引擎不再只有 Langfuse 链路才有用量可见性。

覆盖三层：
1. ``plaita.usage.UsageCollector``——llm / agentrun 输出 → per-node + run 汇总；
2. ``FlowWorker``——终态/挂起把汇总写进 ``ExecutionState.usage``，resume 以
   已落盘用量打底续算（挂起前的用量不丢）；
3. 存储——memory / redis / sqlalchemy 三后端 round-trip 保留 usage，且
   sqlalchemy 存量库由版本化 DDL 迁移补列。
"""
from __future__ import annotations

import unittest

import pytest

from plaita.event.memory import InMemoryEventBus
from plaita.server.flow_worker import FlowWorker
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage
from plaita.usage import UsageCollector


class _Node:
    def __init__(self, node_id: str):
        self.id = node_id


def _llm_result(input_tokens: int, output_tokens: int):
    """llm / agentrun 节点输出契约形状。"""
    return {
        "model": "GLM-5.2",
        "text": "hi",
        "usage": {"prompt_tokens": input_tokens, "completion_tokens": output_tokens},
    }


# ── 1. UsageCollector ────────────────────────────────────────────────

class TestUsageCollector(unittest.TestCase):
    def test_empty_summary_is_none(self):
        assert UsageCollector().summary() is None

    def test_llm_output_maps_and_aggregates(self):
        collector = UsageCollector()
        collector.on_node_end(None, _Node("llm"), _llm_result(10, 5))
        assert collector.summary() == {
            "total": {"input": 10, "output": 5, "total": 15},
            "nodes": {"llm": {"input": 10, "output": 5, "total": 15}},
        }

    def test_repeated_node_execution_accumulates(self):
        collector = UsageCollector()
        collector.on_node_end(None, _Node("loop"), _llm_result(10, 5))
        collector.on_node_end(None, _Node("loop"), _llm_result(2, 3))
        assert collector.summary()["nodes"]["loop"] == {
            "input": 12, "output": 8, "total": 20,
        }

    def test_observations_fallback_when_node_has_no_own_usage(self):
        collector = UsageCollector()
        collector.on_node_end(None, _Node("agent"), {
            "observations": [
                {"type": "generation", "usage": {"input_tokens": 3, "output_tokens": 1}},
                "not-a-dict",
                {"type": "span", "name": "tool:Bash"},
                {"type": "generation", "usage": {"input_tokens": 4, "output_tokens": 2}},
            ],
        })
        assert collector.summary()["nodes"]["agent"] == {
            "input": 7, "output": 3, "total": 10,
        }

    def test_own_usage_wins_and_observations_not_double_counted(self):
        collector = UsageCollector()
        result = _llm_result(10, 5)
        result["observations"] = [
            {"type": "generation", "usage": {"input_tokens": 3, "output_tokens": 1}},
        ]
        collector.on_node_end(None, _Node("agent"), result)
        assert collector.summary()["total"] == {"input": 10, "output": 5, "total": 15}

    def test_error_node_contributes_nothing(self):
        collector = UsageCollector()
        collector.on_node_end(None, _Node("llm"), _llm_result(10, 5),
                              error={"message": "boom"})
        assert collector.summary() is None

    def test_non_dict_result_and_garbage_usage_ignored(self):
        collector = UsageCollector()
        collector.on_node_end(None, _Node("a"), "plain text")
        collector.on_node_end(None, _Node("b"), {"usage": {"prompt_tokens": "nope"}})
        collector.on_node_end(None, _Node("c"), {"usage": None})
        assert collector.summary() is None

    def test_seed_restores_persisted_usage_and_continues(self):
        collector = UsageCollector()
        collector.seed({
            "total": {"input": 10, "output": 5, "total": 15},
            "nodes": {"llm": {"input": 10, "output": 5, "total": 15}},
        })
        collector.on_node_end(None, _Node("llm2"), _llm_result(1, 2))
        assert collector.summary() == {
            "total": {"input": 11, "output": 7, "total": 18},
            "nodes": {
                "llm": {"input": 10, "output": 5, "total": 15},
                "llm2": {"input": 1, "output": 2, "total": 3},
            },
        }

    def test_seed_drops_garbage_entries(self):
        collector = UsageCollector()
        collector.seed({"nodes": {"x": "not-a-dict", "y": {"prompt_tokens": 4}}})
        assert collector.summary() == {
            "total": {"input": 4}, "nodes": {"y": {"input": 4}},
        }

    def test_seed_with_non_dict_resets(self):
        collector = UsageCollector()
        collector.on_node_end(None, _Node("llm"), _llm_result(10, 5))
        collector.seed(None)
        assert collector.summary() is None

    def test_seed_with_dict_but_no_nodes_map(self):
        collector = UsageCollector()
        collector.on_node_end(None, _Node("llm"), _llm_result(10, 5))
        collector.seed({"total": {"input": 1}})
        assert collector.summary() is None

    def test_flow_start_resets_previous_run(self):
        collector = UsageCollector()
        collector.on_node_end(None, _Node("llm"), _llm_result(10, 5))
        collector.on_flow_start(None)
        assert collector.summary() is None


# ── 2. FlowWorker 落盘 ───────────────────────────────────────────────

def _usage_flow(flow_id: str = "usage-flow"):
    return {
        "flow_id": flow_id,
        "name": "用量流程",
        "version": "1.0.0",
        "nodes": [
            {"id": "start", "type": "start", "next": "llm"},
            {
                "id": "llm",
                "type": "assignment",
                "output": {"model": "GLM-5.2",
                           "usage": {"prompt_tokens": 10, "completion_tokens": 5}},
                "next": "end",
            },
            {"id": "end", "type": "end", "output": "done"},
        ],
    }


def _event_usage_flow(flow_id: str = "usage-event-flow"):
    """llm → 事件挂起 → llm2：跨挂起续算用量。"""
    return {
        "flow_id": flow_id,
        "name": "挂起用量流程",
        "version": "1.0.0",
        "nodes": [
            {"id": "start", "type": "start", "next": "llm"},
            {
                "id": "llm",
                "type": "assignment",
                "output": {"model": "GLM-5.2",
                           "usage": {"prompt_tokens": 10, "completion_tokens": 5}},
                "next": "wait_event",
            },
            {"id": "wait_event", "type": "event",
             "event_type": "test.usage.event", "next": "llm2"},
            {
                "id": "llm2",
                "type": "assignment",
                "output": {"model": "GLM-5.2",
                           "usage": {"input_tokens": 1, "output_tokens": 2}},
                "next": "end",
            },
            {"id": "end", "type": "end", "output": "done"},
        ],
    }


def _worker(flow_json):
    execution_storage = MemoryExecutionStorage()
    flow_storage = MemoryFlowStorage()
    flow_storage.save_flow(flow_json)
    worker = FlowWorker(
        execution_storage=execution_storage,
        flow_storage=flow_storage,
        event_bus=InMemoryEventBus(),
    )
    return worker, execution_storage


class TestWorkerPersistsUsage(unittest.TestCase):
    def test_completed_run_carries_usage(self):
        worker, storage = _worker(_usage_flow())
        result = worker.start_flow(flow_id="usage-flow", params={}, version="1.0.0")
        assert result.get("is_end") is True
        state = storage.load_execution_state(result["execution_id"])
        assert state.status == "completed"
        assert state.usage == {
            "total": {"input": 10, "output": 5, "total": 15},
            "nodes": {"llm": {"input": 10, "output": 5, "total": 15}},
        }

    def test_run_without_usage_keeps_field_none(self):
        flow = {
            "flow_id": "no-usage",
            "name": "无用量",
            "version": "1.0.0",
            "nodes": [
                {"id": "start", "type": "start", "next": "end"},
                {"id": "end", "type": "end", "output": "done"},
            ],
        }
        worker, storage = _worker(flow)
        result = worker.start_flow(flow_id="no-usage", params={}, version="1.0.0")
        state = storage.load_execution_state(result["execution_id"])
        assert state.usage is None

    def test_suspend_persists_usage_and_resume_accumulates(self):
        worker, storage = _worker(_event_usage_flow())
        first = worker.start_flow(
            flow_id="usage-event-flow", params={}, version="1.0.0"
        )
        assert first.get("is_suspend") is True
        execution_id = first["execution_id"]
        suspended = storage.load_execution_state(execution_id)
        assert suspended.status == "suspended"
        assert suspended.usage == {
            "total": {"input": 10, "output": 5, "total": 15},
            "nodes": {"llm": {"input": 10, "output": 5, "total": 15}},
        }

        final = worker.resume_flow(
            flow_id="usage-event-flow",
            execution_id=execution_id,
            resume_type="event",
            data={"payload": 1},
        )
        assert final.get("is_end") is True
        done = storage.load_execution_state(execution_id)
        assert done.status == "completed"
        # 挂起前的 llm 用量由 seed 保留，llm2 增量并入 run 汇总
        assert done.usage == {
            "total": {"input": 11, "output": 7, "total": 18},
            "nodes": {
                "llm": {"input": 10, "output": 5, "total": 15},
                "llm2": {"input": 1, "output": 2, "total": 3},
            },
        }


# ── 3. 存储 round-trip ───────────────────────────────────────────────

_USAGE = {
    "total": {"input": 10, "output": 5, "total": 15},
    "nodes": {"llm": {"input": 10, "output": 5, "total": 15}},
}


def _state_with_usage(execution_id: str) -> ExecutionState:
    return ExecutionState(
        execution_id=execution_id,
        flow_id="f1",
        context={},
        status="completed",
        usage=_USAGE,
    )


class TestMemoryRoundTrip(unittest.TestCase):
    def test_usage_survives_save_load(self):
        storage = MemoryExecutionStorage()
        storage.save_execution_state("e1", _state_with_usage("e1"))
        assert storage.load_execution_state("e1").usage == _USAGE


class TestRedisRoundTrip(unittest.TestCase):
    def setUp(self):
        pytest.importorskip("fakeredis")
        pytest.importorskip("redis")
        import fakeredis

        from plaita.storage.redis import RedisExecutionStorage

        self.storage = RedisExecutionStorage(
            client=fakeredis.FakeRedis(decode_responses=True), namespace="plaita"
        )

    def test_usage_survives_save_load_and_list(self):
        self.storage.save_execution_state("e1", _state_with_usage("e1"))
        assert self.storage.load_execution_state("e1").usage == _USAGE
        rows = self.storage.list_executions()
        assert [r.usage for r in rows] == [_USAGE]


OLD_EXECUTION_STATES_DDL = """
CREATE TABLE execution_states (
    execution_id VARCHAR(50) PRIMARY KEY,
    flow_id VARCHAR(100),
    flow_name VARCHAR(100),
    flow_version VARCHAR(50),
    flow_hash VARCHAR(64),
    tenant_id VARCHAR(100),
    context JSON NOT NULL,
    status VARCHAR(50) NOT NULL,
    start_time VARCHAR(50),
    last_update_time VARCHAR(50),
    end_time VARCHAR(50),
    error JSON,
    invoker VARCHAR(100),
    created_at DATETIME,
    updated_at DATETIME
)
"""


class TestSqlalchemyUsageColumn(unittest.TestCase):
    """需要 sqlalchemy + aiosqlite；缺依赖自动跳过。"""

    def setUp(self):
        pytest.importorskip("sqlalchemy")
        pytest.importorskip("aiosqlite")
        from sqlalchemy import text
        from sqlalchemy.ext.asyncio import create_async_engine

        from plaita.core.async_utils import run_async_from_sync

        self._run = run_async_from_sync
        self.engine = create_async_engine("sqlite+aiosqlite://")

        async def _seed():
            async with self.engine.begin() as conn:
                await conn.execute(text(OLD_EXECUTION_STATES_DDL))
                await conn.execute(text(
                    "CREATE TABLE schema_migrations ("
                    "version INTEGER PRIMARY KEY, "
                    "name VARCHAR(200) NOT NULL, "
                    "applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                ))
                await conn.execute(text(
                    "INSERT INTO schema_migrations (version, name) VALUES (1, 'v1')"
                ))

        run_async_from_sync(_seed())

    def tearDown(self):
        self._run(self.engine.dispose())

    def _storage(self):
        from plaita.storage.sqlalchemy import SqlalchemyExecutionStorage

        return SqlalchemyExecutionStorage(engine=self.engine)

    def test_migration_v2_adds_usage_column(self):
        from sqlalchemy import text

        storage = self._storage()

        async def _check():
            async with self.engine.begin() as conn:
                res = await conn.execute(text("PRAGMA table_info(execution_states)"))
                cols = {row[1] for row in res}
                versions = [
                    row[0]
                    for row in await conn.execute(
                        text("SELECT version FROM schema_migrations")
                    )
                ]
            assert "usage" in cols
            assert versions == [1, 2]
            assert await storage.save_execution_state("e1", _state_with_usage("e1"))

        self._run(_check())

    def test_usage_survives_save_load_and_list(self):
        storage = self._storage()

        async def _check():
            await storage.save_execution_state("e1", _state_with_usage("e1"))
            loaded = await storage.load_execution_state("e1")
            assert loaded.usage == _USAGE
            rows = await storage.list_executions(order_by="execution_id")
            assert [r.usage for r in rows] == [_USAGE]

        self._run(_check())


if __name__ == "__main__":
    unittest.main()
