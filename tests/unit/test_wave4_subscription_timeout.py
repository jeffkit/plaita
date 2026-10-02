"""波次④（EventNode 订阅自动超时）— 设计稿 T9。

覆盖四件事：
1. EventNode.subscription_timeout 模型层字段校验（None=现状、正值通过、0/负数拒绝）。
2. _subscribe_event 仅在配置了 subscription_timeout 时向 register_subscription
   透传 timeout 形参（存量节点调用参数字节级等价）。
3. event_filter 宿主的 SubscriptionTimeoutChecker：超时触发 resume_type=timeout
   入队、去重键挡二次（多实例幂等）、存量无 timeout 字段订阅零变化、
   终态执行残留订阅回收。
4. 消费侧（设计稿称已就绪，钉住）：resume_type=timeout → on_timeout 落 timeout
   状态 + 挂起订阅注销。
"""
import asyncio
import json
import time
from unittest.mock import AsyncMock, MagicMock

import pytest

import fakeredis

from pydantic import ValidationError

from plaita.core.context import ExecutionContext
from plaita.core.executor import FlowExecution, ExecutionMode, _subscribe_event
from plaita.core.flow import Flow
from plaita.event.core import EventSubscription
from plaita.event.memory import InMemoryEventSubscriptionStorage
from plaita.event.timeout import SubscriptionTimeoutChecker
from plaita.node import End, Node, Start
from plaita.node.event_node import EventNode, EventNodeStatus
from plaita.server.event_filter import EventFilter
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage


TIMEOUT_DEDUP_PREFIX = "plaita:event_filter:timeout:"


def run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# ─── 1. 模型层字段校验 ────────────────────────────────────────


class TestEventNodeSubscriptionTimeoutField:
    def test_default_is_none_unlimited_wait(self):
        """缺省 None = 无限等待 = 历史现状。"""
        node = EventNode(id="evt", event_type="approval")
        assert node.subscription_timeout is None

    def test_positive_seconds_accepted(self):
        node = EventNode(id="evt", event_type="approval", subscription_timeout=30)
        assert node.subscription_timeout == 30

    def test_camelcase_alias_accepted(self):
        """codeflow 编译器/console 表单发 camelCase（与 event_type 同理由）。"""
        node = EventNode(id="evt", event_type="approval", subscriptionTimeout=1.5)
        assert node.subscription_timeout == 1.5

    @pytest.mark.parametrize("bad", [0, -1, -0.5])
    def test_zero_and_negative_rejected(self, bad):
        """0/负数无意义（0 会让订阅一注册即超时），构造期 ValidationError。"""
        with pytest.raises(ValidationError):
            EventNode(id="evt", event_type="approval", subscription_timeout=bad)


# ─── 2. _subscribe_event 透传 ────────────────────────────────


class _RecordingAsyncBus:
    """记录 register_subscription 收到的参数集。"""

    def __init__(self):
        self.calls = []
        self.subscription_id = "sub-rec-1"

    async def register_subscription(self, **params):
        self.calls.append(params)
        return self.subscription_id


class _LegacySyncBus:
    """不接受 timeout 形参的简化 bus（历史上大量测试替身长这样）。"""

    def __init__(self):
        self.calls = []

    def register_subscription(self, event_type, filter_condition=None,
                              correlation_id=None, flow_id=None, node_id=None):
        self.calls.append({"event_type": event_type})
        return "sub-legacy-1"


class TestSubscribeEventPassesTimeout:
    def _subscribe(self, node, bus):
        context = ExecutionContext(event_bus=bus)
        node_state = {"event_type": node.event_type, "status": EventNodeStatus.PENDING.value}
        ok = run(_subscribe_event(node, _FakeFlow(), node_state, context))
        return ok

    def test_configured_node_passes_timeout(self):
        bus = _RecordingAsyncBus()
        node = EventNode(id="evt", event_type="approval", subscription_timeout=5)
        assert self._subscribe(node, bus) is True
        assert bus.calls[0]["timeout"] == 5

    def test_legacy_node_keeps_historical_param_set(self):
        """存量节点（未配置）不带 timeout 键——调用参数与历史完全一致。"""
        bus = _RecordingAsyncBus()
        node = EventNode(id="evt", event_type="approval")
        assert self._subscribe(node, bus) is True
        assert "timeout" not in bus.calls[0]

    def test_bus_without_timeout_kwarg_still_works_for_legacy_node(self):
        """未配置超时的节点对不接受 timeout 形参的简化 bus 保持兼容。"""
        bus = _LegacySyncBus()
        node = EventNode(id="evt", event_type="approval")
        assert self._subscribe(node, bus) is True
        assert bus.calls[0]["event_type"] == "approval"


class _FakeFlow:
    flow_id = "flow-wave4"


# ─── 3. event_filter 宿主的 SubscriptionTimeoutChecker（T9 核心） ──


class TestFilterHostedTimeoutChecker:
    def _make_storages(self):
        execution_storage = MemoryExecutionStorage()
        subscription_storage = InMemoryEventSubscriptionStorage()
        redis_client = fakeredis.FakeRedis(decode_responses=True)
        return execution_storage, subscription_storage, redis_client

    def _make_filter(self, execution_storage, subscription_storage, redis_client,
                     queue_name="test:wave4-queue", enable=True, interval=10.0):
        return EventFilter(
            execution_storage=execution_storage,
            subscription_storage=subscription_storage,
            redis_client=redis_client,
            event_bus=AsyncMock(),
            queue_name=queue_name,
            enable_subscription_timeout_checker=enable,
            timeout_check_interval=interval,
        )

    def _save_suspended(self, execution_storage, execution_id="exec-w4-1", flow_id="flow-w4"):
        state = ExecutionState(
            execution_id=execution_id,
            flow_id=flow_id,
            status="suspended",
            context={},
        )
        execution_storage.save_execution_state(execution_id, state)
        return state

    def _timed_out_subscription(self, event_type="approval", timeout=1.0,
                                age=10.0, execution_id="exec-w4-1", flow_id="flow-w4"):
        """created_at 回拨 age 秒，使 timeout=1s 的订阅立即处于已超时状态。"""
        return EventSubscription(
            event_type=event_type,
            correlation_id=execution_id,
            flow_id=flow_id,
            timeout=timeout,
            created_at=time.time() - age,
        )

    @staticmethod
    def _stream_payloads(redis_client, stream_key):
        entries = redis_client.xrange(stream_key, min="-", max="+")
        tasks = []
        for _mid, fields in entries:
            raw = fields.get("payload") or fields.get(b"payload")
            if isinstance(raw, bytes):
                raw = raw.decode()
            tasks.append(json.loads(raw))
        return tasks

    def test_timeout_enqueues_resume_task(self):
        """超时订阅触发 checker → resume_type=timeout 任务入队 + 去重键落位。"""
        execution_storage, subscription_storage, redis_client = self._make_storages()
        self._save_suspended(execution_storage)
        sub = self._timed_out_subscription()
        run(subscription_storage.store_subscription(sub))

        ef = self._make_filter(execution_storage, subscription_storage, redis_client)
        # 走 checker 真实检查路径（list_subscriptions + 超时判定 + 回调）
        run(ef._timeout_checker._check_timeouts())

        tasks = self._stream_payloads(redis_client, "test:wave4-queue")
        assert len(tasks) == 1
        task = tasks[0]
        assert task["type"] == "resume"
        assert task["resume_type"] == "timeout"
        assert task["execution_id"] == "exec-w4-1"
        assert task["flow_id"] == "flow-w4"
        assert task["tenant_id"] == "default"
        assert task["data"]["subscription_id"] == sub.subscription_id
        assert task["data"]["event_type"] == "approval"
        # 去重键已落位（挡住后续重发）
        assert redis_client.exists(TIMEOUT_DEDUP_PREFIX + sub.subscription_id) == 1

    def test_dedup_blocks_second_trigger(self):
        """同一 checker 第二轮检查（订阅尚未来得及注销）不重复入队。"""
        execution_storage, subscription_storage, redis_client = self._make_storages()
        self._save_suspended(execution_storage)
        sub = self._timed_out_subscription()
        run(subscription_storage.store_subscription(sub))

        ef = self._make_filter(execution_storage, subscription_storage, redis_client)
        run(ef._timeout_checker._check_timeouts())
        run(ef._timeout_checker._check_timeouts())
        run(ef._timeout_checker._check_timeouts())

        assert len(self._stream_payloads(redis_client, "test:wave4-queue")) == 1

    def test_multi_instance_idempotent(self):
        """多实例 event_filter（各自 checker、共享存储/Redis）只入队一条。"""
        execution_storage, subscription_storage, redis_client = self._make_storages()
        self._save_suspended(execution_storage)
        sub = self._timed_out_subscription()
        run(subscription_storage.store_subscription(sub))

        ef1 = self._make_filter(execution_storage, subscription_storage, redis_client,
                                queue_name="test:wave4-mi")
        ef2 = self._make_filter(execution_storage, subscription_storage, redis_client,
                                queue_name="test:wave4-mi")
        assert ef1._timeout_checker is not ef2._timeout_checker

        run(ef1._timeout_checker._check_timeouts())
        run(ef2._timeout_checker._check_timeouts())

        assert len(self._stream_payloads(redis_client, "test:wave4-mi")) == 1

    def test_legacy_subscription_untouched_while_checker_runs(self):
        """存量订阅（无 timeout 字段）即使 checker 开着也零变化：不入队、不注销、无去重键。"""
        execution_storage, subscription_storage, redis_client = self._make_storages()
        self._save_suspended(execution_storage)
        # timeout=None（默认）+ created_at 早已过期
        legacy = EventSubscription(
            event_type="approval",
            correlation_id="exec-w4-1",
            flow_id="flow-w4",
            created_at=time.time() - 86400,
        )
        run(subscription_storage.store_subscription(legacy))

        ef = self._make_filter(execution_storage, subscription_storage, redis_client)
        for _ in range(3):
            run(ef._timeout_checker._check_timeouts())

        assert len(self._stream_payloads(redis_client, "test:wave4-queue")) == 0
        remaining = run(subscription_storage.list_subscriptions())
        assert len(remaining) == 1
        assert redis_client.keys(TIMEOUT_DEDUP_PREFIX + "*") == []

    def test_terminal_execution_subscription_reaped_on_timeout(self):
        """终态执行的残留订阅超时触发时就地回收，不产生注定失败的 resume。"""
        execution_storage, subscription_storage, redis_client = self._make_storages()
        state = ExecutionState(
            execution_id="exec-w4-done", flow_id="flow-w4",
            status="cancelled", context={},
        )
        execution_storage.save_execution_state(state.execution_id, state)
        sub = self._timed_out_subscription(execution_id="exec-w4-done")
        run(subscription_storage.store_subscription(sub))

        ef = self._make_filter(execution_storage, subscription_storage, redis_client)
        run(ef._timeout_checker._check_timeouts())

        assert len(self._stream_payloads(redis_client, "test:wave4-queue")) == 0
        remaining = run(subscription_storage.list_subscriptions())
        assert len(remaining) == 0

    def test_missing_execution_state_skips_and_keeps_subscription(self):
        """找不到执行状态：跳过且不注销订阅（与 handle_event 的 not-found 语义一致）。"""
        execution_storage, subscription_storage, redis_client = self._make_storages()
        sub = self._timed_out_subscription(execution_id="exec-ghost")
        run(subscription_storage.store_subscription(sub))

        ef = self._make_filter(execution_storage, subscription_storage, redis_client)
        run(ef._timeout_checker._check_timeouts())

        assert len(self._stream_payloads(redis_client, "test:wave4-queue")) == 0
        remaining = run(subscription_storage.list_subscriptions())
        assert len(remaining) == 1

    def test_checker_disabled_rolls_back_to_status_quo(self):
        """回滚开关：enable_subscription_timeout_checker=False 时不建 checker。"""
        execution_storage, subscription_storage, redis_client = self._make_storages()
        ef = self._make_filter(execution_storage, subscription_storage, redis_client,
                               enable=False)
        assert ef._timeout_checker is None

    def test_checker_lifecycle_follows_filter(self):
        """checker 启停跟随 EventFilter.start()/stop()。"""

        async def _test():
            execution_storage, subscription_storage, redis_client = self._make_storages()
            ef = self._make_filter(execution_storage, subscription_storage, redis_client,
                                   interval=3600.0)
            filter_task = asyncio.create_task(ef.start())
            await asyncio.sleep(0.05)
            assert ef._timeout_checker._running is True

            await ef.stop()
            filter_task.cancel()
            await asyncio.gather(filter_task, return_exceptions=True)
            assert ef._timeout_checker._running is False

        run(_test())


# ─── 4. 消费侧（设计稿称已就绪，此处钉住） ─────────────────────


class SimpleNode(Node):
    node_type = "simple"

    def run(self, execution):
        return {"value": self.id}


def _event_flow():
    return Flow(
        flow_id="test_wave4_timeout_flow",
        nodes=[
            Start(id="start", next="task1"),
            SimpleNode(id="task1", next="wait_event"),
            EventNode(id="wait_event", event_type="approval", next="task2"),
            SimpleNode(id="task2", next="end"),
            End(id="end"),
        ],
    )


class TestTimeoutResumeConsumer:
    """resume_type=timeout 全链路：on_timeout 落状态 + 订阅注销（T9 后半）。"""

    def test_timeout_resume_lands_state_and_unregisters_subscription(self):
        flow = _event_flow()
        mock_bus = MagicMock()
        mock_bus.register_subscription = MagicMock(return_value="sub-wave4-resume")
        mock_bus.unregister_subscription = MagicMock(return_value=True)

        r1 = FlowExecution.run(flow, mode=ExecutionMode.DISTRIBUTED, event_bus=mock_bus)
        r2 = FlowExecution.run(
            flow, mode=ExecutionMode.DISTRIBUTED, context=r1["context"], event_bus=mock_bus,
        )
        assert r2["id"] == "wait_event"
        assert r2.get("is_suspend") is True
        # 挂起侧：subscription_timeout 未配置 → 订阅无限等待（现状）
        pending_state = r2["context"]["$NODE"]["wait_event"]
        assert pending_state["status"] == "pending"

        r3 = FlowExecution.run(
            flow, mode=ExecutionMode.DISTRIBUTED, context=r2["context"], event_bus=mock_bus,
            resume_type="timeout",
        )
        assert r3["id"] == "wait_event"
        # on_timeout 落状态
        node_state = r3["context"]["$NODE"]["wait_event"]
        assert node_state["status"] == EventNodeStatus.TIMEOUT.value
        # resume 后订阅注销
        mock_bus.unregister_subscription.assert_called_once_with("sub-wave4-resume")

    def test_timeout_resume_advances_past_event_node(self):
        """timeout resume 后节点不再挂起，下一步 run 正常推进到 task2
        （distributed 模式单步推进，与既有 E2E checkpoint 语义一致）。"""
        flow = _event_flow()
        mock_bus = MagicMock()
        mock_bus.register_subscription = MagicMock(return_value="sub-wave4-adv")

        r1 = FlowExecution.run(flow, mode=ExecutionMode.DISTRIBUTED, event_bus=mock_bus)
        r2 = FlowExecution.run(
            flow, mode=ExecutionMode.DISTRIBUTED, context=r1["context"], event_bus=mock_bus,
        )
        r3 = FlowExecution.run(
            flow, mode=ExecutionMode.DISTRIBUTED, context=r2["context"], event_bus=mock_bus,
            resume_type="timeout",
        )
        assert r3.get("is_suspend") is not True

        r4 = FlowExecution.run(
            flow, mode=ExecutionMode.DISTRIBUTED, context=r3["context"], event_bus=mock_bus,
        )
        assert r4["id"] == "task2"
