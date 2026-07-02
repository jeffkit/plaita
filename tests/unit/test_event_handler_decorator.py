"""@event_handler 装饰器在导入期/loop 内的注册行为。

历史问题 (2026-07 整改前):
- 装饰器直接 ``asyncio.create_task(register())`` fire-and-forget, 模块导入期
  没有 running loop 会抛 RuntimeError;
- 即使有 running loop, task 引用未保留, 任意时刻可被 GC 回收, 注册静默丢失。

整改后:
- 无 running loop → register coroutine 入模块级待办列表, 由
  ``flush_pending_handler_registrations`` 显式驱动;
- 有 running loop → ``loop.create_task`` + 模块级集合持引用, done_callback 清理。
"""

import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock

from plaita.event import core
from plaita.event.core import event_handler, flush_pending_handler_registrations


def _make_bus():
    """EventBus 是 ABC, 直接用 MagicMock + register_handler AsyncMock 绕过。"""
    bus = MagicMock()
    bus.register_handler = AsyncMock()
    return bus


class TestEventHandlerDecorator(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        core._pending_handler_registrations.clear()
        core._handler_registration_tasks.clear()

    async def test_register_when_loop_running_creates_task_held_in_set(self):
        # 本测试运行在 IsolatedAsyncioTestCase 的 loop 里 → 走 create_task 分支
        bus = _make_bus()

        @event_handler(bus, event_type="user.created")
        async def my_handler(event):
            return None

        # 让 create_task 跑完
        for _ in range(20):
            await asyncio.sleep(0)
        bus.register_handler.assert_awaited_once_with(
            event_type="user.created",
            handler=my_handler,
            filter_condition=None,
            retry_policy=None,
        )
        # 完成后 done_callback 从集合移除
        self.assertEqual(len(core._handler_registration_tasks), 0)

    async def test_flush_drives_pending_registrations(self):
        # 直接模拟"导入期没 loop"留下的待办: 手动塞 register, flush 驱动。
        bus = _make_bus()
        # 直接复用装饰器内部 register 闭包不好构造, 这里手写一个等价物。
        async def fake_register():
            await bus.register_handler(
                event_type="manual", handler=lambda e: None,
                filter_condition=None, retry_policy=None,
            )

        core._pending_handler_registrations.append(fake_register)
        await flush_pending_handler_registrations()
        bus.register_handler.assert_awaited_once()
        self.assertEqual(len(core._pending_handler_registrations), 0)

    async def test_task_reference_held_until_completion(self):
        # register_handler 故意拖延 → task 完成前必须仍在集合里。
        bus = _make_bus()
        started = asyncio.Event()

        async def slow_register(*args, **kwargs):
            started.set()
            await asyncio.sleep(0.05)

        bus.register_handler = AsyncMock(side_effect=slow_register)

        @event_handler(bus, event_type="slow")
        async def my_handler(event):
            return None

        await started.wait()
        # 完成前, 引用仍在集合里
        self.assertEqual(len(core._handler_registration_tasks), 1)
        await asyncio.sleep(0.1)
        # 完成后被清掉
        self.assertEqual(len(core._handler_registration_tasks), 0)


if __name__ == "__main__":
    unittest.main()
