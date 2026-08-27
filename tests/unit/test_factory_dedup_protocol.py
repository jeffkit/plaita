"""factory db 路径 + handler 成功后再去重。"""
from __future__ import annotations

import unittest

import pytest

pytest.importorskip('sqlalchemy')
from unittest.mock import MagicMock, patch

from plaita.core.executor import FlowExecution
from plaita.core.node_context import NodeExecutionContext
from plaita.event.core import Event
from plaita.event.memory import InMemoryEventBus


class TestFactoryDbEngine(unittest.TestCase):
    def test_create_event_bus_db_requires_experimental_flag(self):
        from plaita.server.factory import create_event_bus

        with patch.dict("os.environ", {}, clear=False):
            import os

            os.environ.pop("PLAITA_ALLOW_EXPERIMENTAL_DB", None)
            with self.assertRaises(ValueError) as ctx:
                create_event_bus("db", database_url="sqlite+aiosqlite:///:memory:")
            self.assertIn("experimental", str(ctx.exception))

    def test_create_event_bus_db_passes_engine_when_allowed(self):
        from plaita.server.factory import create_event_bus

        fake_engine = object()
        with patch.dict("os.environ", {"PLAITA_ALLOW_EXPERIMENTAL_DB": "1"}):
            with patch("plaita.server.factory._create_async_engine", return_value=fake_engine) as mk_eng:
                with patch("plaita.event.sqlalchemy.SqlalchemyEventBus") as Bus:
                    Bus.return_value = MagicMock()
                    create_event_bus("db", database_url="sqlite+aiosqlite:///:memory:")
                    mk_eng.assert_called_once_with("sqlite+aiosqlite:///:memory:")
                    Bus.assert_called_once()
                    kwargs = Bus.call_args.kwargs
                    self.assertIs(kwargs.get("engine"), fake_engine)
                    self.assertNotIn("database_url", kwargs)

    def test_create_subscription_storage_db_passes_engine_when_allowed(self):
        from plaita.server.factory import create_storage_component

        fake_engine = object()
        with patch.dict("os.environ", {"PLAITA_ALLOW_EXPERIMENTAL_DB": "1"}):
            with patch("plaita.server.factory._create_async_engine", return_value=fake_engine):
                with patch("plaita.event.sqlalchemy.SqlalchemyEventSubscriptionStorage") as Stor:
                    Stor.return_value = MagicMock()
                    create_storage_component(
                        "db", "subscription", database_url="sqlite+aiosqlite:///:memory:"
                    )
                    Stor.assert_called_once()
                    self.assertIs(Stor.call_args.kwargs.get("engine"), fake_engine)


class TestDedupAfterSuccess(unittest.IsolatedAsyncioTestCase):
    async def test_failed_handler_can_retry(self):
        """失败后不得永久丢事件：再次 publish 应能再投递。"""
        import asyncio

        bus = InMemoryEventBus()
        calls = []

        async def flaky(event):
            calls.append(event.event_id)
            if len(calls) == 1:
                raise RuntimeError("boom")

        await bus.register_handler("retry.me", flaky)
        evt = Event(event_type="retry.me", data={})
        await bus.publish(evt, prevent_duplicate_consumption=True)
        await asyncio.sleep(0.15)
        self.assertEqual(len(calls), 1)
        # 失败后未 mark → 再次 publish 应再调用
        await bus.publish(evt, prevent_duplicate_consumption=True)
        await asyncio.sleep(0.15)
        self.assertEqual(len(calls), 2)
        # 第二次成功后已 mark → 第三次跳过
        await bus.publish(evt, prevent_duplicate_consumption=True)
        await asyncio.sleep(0.15)
        self.assertEqual(len(calls), 2)

    async def test_success_still_dedups(self):
        import asyncio

        bus = InMemoryEventBus()
        calls = []

        async def ok(event):
            calls.append(1)

        await bus.register_handler("ok.event", ok)
        evt = Event(event_type="ok.event", data={})
        await bus.publish(evt, prevent_duplicate_consumption=True)
        await asyncio.sleep(0.15)
        await bus.publish(evt, prevent_duplicate_consumption=True)
        await asyncio.sleep(0.15)
        self.assertEqual(len(calls), 1)


class TestNodeExecutionContext(unittest.TestCase):
    def test_flow_execution_satisfies_protocol(self):
        fe = FlowExecution()
        self.assertIsInstance(fe, NodeExecutionContext)


if __name__ == "__main__":
    unittest.main()
