"""ExecutionStorage / FlowStorage 同步契约：公开路径不得交付 async 假实现。"""
from __future__ import annotations

import inspect
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage


def _sample_state(execution_id: str = "exec-1") -> ExecutionState:
    now = datetime.now(timezone.utc).isoformat()
    return ExecutionState(
        execution_id=execution_id,
        flow_id="flow-1",
        flow_name="demo",
        flow_version="1",
        context={"$NODE": {}},
        status="running",
        start_time=now,
        last_update_time=now,
    )


class TestMemoryExecutionStorageIsSync(unittest.TestCase):
    """memory 后端必须满足同步 ABC（FlowWorker 不 await）。"""

    def test_save_load_roundtrip_returns_bool_not_coroutine(self):
        storage = MemoryExecutionStorage()
        state = _sample_state()
        save_result = storage.save_execution_state(state.execution_id, state)
        self.assertIsInstance(save_result, bool)
        self.assertTrue(save_result)
        self.assertFalse(inspect.isawaitable(save_result))

        loaded = storage.load_execution_state(state.execution_id)
        self.assertIsNotNone(loaded)
        self.assertFalse(inspect.isawaitable(loaded))
        self.assertEqual(loaded.execution_id, state.execution_id)
        self.assertEqual(loaded.context, state.context)

    def test_abc_methods_are_not_coroutines(self):
        storage = MemoryExecutionStorage()
        for name in (
            "save_execution_state",
            "load_execution_state",
            "delete_execution_state",
            "list_executions",
        ):
            self.assertFalse(
                inspect.iscoroutinefunction(getattr(storage, name)),
                f"{name} must be sync for FlowWorker/EventFilter",
            )


class TestMemoryFlowStorageIsSync(unittest.TestCase):
    def test_save_get_roundtrip(self):
        storage = MemoryFlowStorage()
        flow = {"flow_id": "f1", "version": "1", "nodes": []}
        result = storage.save_flow(flow)
        self.assertIsInstance(result, bool)
        self.assertFalse(inspect.isawaitable(result))
        loaded = storage.get_flow("f1", "1")
        self.assertFalse(inspect.isawaitable(loaded))
        self.assertEqual(loaded["flow_id"], "f1")


class TestFactoryRejectsBrokenDbExecutionStorage(unittest.TestCase):
    def test_db_execution_raises_with_actionable_message(self):
        from plaita.server.factory import create_storage_component

        with self.assertRaises(ValueError) as ctx:
            create_storage_component(
                "db", "execution", database_url="sqlite+aiosqlite:///:memory:"
            )
        msg = str(ctx.exception)
        self.assertIn("不兼容", msg)
        self.assertIn("redis", msg)

    def test_db_flow_raises(self):
        from plaita.server.factory import create_storage_component

        with self.assertRaises(ValueError) as ctx:
            create_storage_component(
                "db", "flow", database_url="sqlite+aiosqlite:///:memory:"
            )
        self.assertIn("flow", str(ctx.exception))

    def test_db_subscription_requires_experimental_flag(self):
        from plaita.server.factory import create_storage_component
        import os

        os.environ.pop("PLAITA_ALLOW_EXPERIMENTAL_DB", None)
        with self.assertRaises(ValueError) as ctx:
            create_storage_component(
                "db", "subscription", database_url="sqlite+aiosqlite:///:memory:"
            )
        self.assertIn("experimental", str(ctx.exception))

    def test_db_subscription_allowed_with_flag(self):
        """subscription db 仅在 PLAITA_ALLOW_EXPERIMENTAL_DB=1 时可用。"""
        from plaita.server.factory import create_storage_component

        fake_engine = object()
        with patch.dict("os.environ", {"PLAITA_ALLOW_EXPERIMENTAL_DB": "1"}):
            with patch(
                "plaita.server.factory._create_async_engine", return_value=fake_engine
            ):
                with patch(
                    "plaita.event.sqlalchemy.SqlalchemyEventSubscriptionStorage"
                ) as Stor:
                    Stor.return_value = object()
                    create_storage_component(
                        "db", "subscription", database_url="sqlite+aiosqlite:///:memory:"
                    )
                    Stor.assert_called_once()


class TestSqlalchemyImplementationIsAsync(unittest.TestCase):
    """钉死根因：sqlalchemy storage 方法是 coroutine——所以必须挡在 factory。"""

    def test_execution_methods_are_coroutines(self):
        try:
            from plaita.storage.sqlalchemy import SqlalchemyExecutionStorage
        except ImportError:
            self.skipTest("sqlalchemy extra not installed")

        for name in (
            "save_execution_state",
            "load_execution_state",
            "delete_execution_state",
            "list_executions",
        ):
            self.assertTrue(
                inspect.iscoroutinefunction(getattr(SqlalchemyExecutionStorage, name)),
                f"{name} is expected async (broken vs sync ABC)",
            )


class TestHasSqlalchemyFlag(unittest.TestCase):
    def test_has_sqlalchemy_exported_independently_of_redis(self):
        import plaita.event as event_pkg

        self.assertTrue(hasattr(event_pkg, "HAS_SQLALCHEMY"))
        self.assertIn("HAS_SQLALCHEMY", event_pkg.__all__)


if __name__ == "__main__":
    unittest.main()
