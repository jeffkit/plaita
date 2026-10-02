"""ReviewFix D2 回归：终态订阅 GC 必须覆盖 cancelled。

基线缺陷：EventFilter 终态 GC 只认 completed/error，漏掉 cancelled——
console 取消执行后残留订阅在 TTL（约 7 天）内每个匹配事件都会入队一条
注定被 worker 终态短路丢弃的 resume，持续制造无效任务。

修复：GC 改用模块常量 TERMINAL_EXECUTION_STATUSES =
("completed", "error", "cancelled")，与 flow_worker.resume_flow 的终态
短路集合对齐。
"""
import asyncio
from unittest.mock import AsyncMock

import fakeredis

from plaita.event.core import Event, EventSubscription
from plaita.event.memory import InMemoryEventSubscriptionStorage
from plaita.server.event_filter import TERMINAL_EXECUTION_STATUSES, EventFilter
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _make_filter(queue_name="test:reviewfix-d2"):
    execution_storage = MemoryExecutionStorage()
    subscription_storage = InMemoryEventSubscriptionStorage()
    redis_client = fakeredis.FakeRedis(decode_responses=True)
    event_filter = EventFilter(
        execution_storage=execution_storage,
        subscription_storage=subscription_storage,
        redis_client=redis_client,
        event_bus=AsyncMock(),
        queue_name=queue_name,
    )
    return event_filter, execution_storage, subscription_storage, redis_client


def _scenario(status, execution_id, flow_id, event_type):
    """构建「指定状态执行 + 残留订阅 + 一次匹配事件」场景并驱动 handle_event。"""
    event_filter, execution_storage, subscription_storage, redis_client = _make_filter()

    async def scenario():
        state = ExecutionState(
            execution_id=execution_id, flow_id=flow_id, status=status, context={}
        )
        execution_storage.save_execution_state(execution_id, state)
        sub = EventSubscription(
            event_type=event_type, correlation_id=execution_id, flow_id=flow_id
        )
        await subscription_storage.store_subscription(sub)

        await event_filter.handle_event(
            Event(event_type=event_type, data={"go": True}, correlation_id=execution_id)
        )

        remaining = await subscription_storage.list_subscriptions(event_type=event_type)
        return remaining, redis_client.xlen(event_filter.queue_name)

    return _run(scenario())


def test_cancelled_execution_subscription_is_reaped():
    """核心回归：cancelled 执行的残留订阅被 GC，且不入队无效 resume。"""
    remaining, queued = _scenario("cancelled", "exec-cx", "flow-cx", "cx.event")
    assert remaining == [], "cancelled 执行的订阅未被回收（GC 漏 cancelled）"
    assert queued == 0, "cancelled 执行仍入队了注定短路的 resume"


def test_error_and_completed_still_reaped():
    """既有 GC 行为不回归：error / completed 仍被回收。"""
    for status in ("error", "completed"):
        remaining, queued = _scenario(status, f"exec-{status}", f"flow-{status}",
                                      f"{status}.event")
        assert remaining == [], f"{status} 执行的订阅未被回收"
        assert queued == 0


def test_suspended_execution_still_enqueues():
    """控制组：非终态（suspended，事件挂起的常态）仍正常入队且订阅保留。"""
    remaining, queued = _scenario("suspended", "exec-sp", "flow-sp", "sp.event")
    assert len(remaining) == 1
    assert queued == 1


def test_terminal_status_constant_pins_worker_semantics():
    """钉死终态常量：必须与 flow_worker.resume_flow 的终态短路集合一致。
    任一侧单独增删终态时，此测试强制重新对齐两侧语义。"""
    assert TERMINAL_EXECUTION_STATUSES == ("completed", "error", "cancelled")
