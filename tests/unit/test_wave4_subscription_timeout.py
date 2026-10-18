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

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis

from pydantic import ValidationError

from plaita.core.context import ExecutionContext
from plaita.core.executor import FlowExecution, ExecutionMode, _subscribe_event
from plaita.core.flow import Flow
from plaita.event.core import EventSubscription
from plaita.event.memory import InMemoryEventSubscriptionStorage
from plaita.event.timeout import SubscriptionTimeoutChecker, TimeoutCallbackError
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

    def test_timeout_callback_failure_propagates(self):
        """#153 前提②：超时回调的失败不再被包装吞成日志——聚合上抛给 _check_loop。

        （``_check_loop`` 在调用点记 ERROR 后进入下一轮；失败对上层可观察，
        订阅超时唤醒链路不再"静默丢"。原始异常保留在 ``failures`` 里。）
        """
        execution_storage, subscription_storage, redis_client = self._make_storages()
        self._save_suspended(execution_storage)
        sub = self._timed_out_subscription()
        run(subscription_storage.store_subscription(sub))

        ef = self._make_filter(execution_storage, subscription_storage, redis_client)
        calls = []

        async def _broken_callback(subscription):
            calls.append(subscription.subscription_id)
            raise ConnectionError("simulated callback outage")

        # 真实回调先跑（入队 1 条），随后坏回调的失败被聚合上抛到 _check_timeouts 之外
        ef._timeout_checker.register_timeout_callback(_broken_callback)
        with pytest.raises(TimeoutCallbackError) as excinfo:
            run(ef._timeout_checker._check_timeouts())

        assert calls == [sub.subscription_id], "坏回调根本没跑（断言空转）"
        # 原始异常与定位信息都还在（不是"只剩一行日志"）
        assert excinfo.value.subscription_ids == [sub.subscription_id]
        assert isinstance(excinfo.value.failures[0][1], ConnectionError)
        assert len(self._stream_payloads(redis_client, "test:wave4-queue")) == 1

    def test_callback_failure_does_not_starve_second_callback(self):
        """#153/R1 回归：同轮两个回调，第一个抛 ⇒ 第二个仍必须跑到并**入队**。

        r4 的 `raise` 在第一个坏回调处中断整轮（verifier 实测 ``calls=["bad"]``）；
        修复后是「逐条记账 + 整轮末尾聚合上抛」⇒ ``calls=["bad","good"]``，
        且第二个回调真正把 timeout resume 入了队。
        """
        execution_storage, subscription_storage, redis_client = self._make_storages()
        self._save_suspended(execution_storage)
        sub = self._timed_out_subscription()
        run(subscription_storage.store_subscription(sub))

        ef = self._make_filter(execution_storage, subscription_storage, redis_client)
        calls = []

        async def _bad(subscription):
            calls.append("bad")
            raise ConnectionError("simulated callback outage")

        async def _good(subscription):
            calls.append("good")
            await ef._on_subscription_timeout(subscription)   # 第二个回调真入队

        # 顺序钉死：坏回调在前，good（真入队）在后
        ef._timeout_checker._on_timeout_callbacks.clear()
        ef._timeout_checker.register_timeout_callback(_bad)
        ef._timeout_checker.register_timeout_callback(_good)

        raised = None
        try:
            run(ef._timeout_checker._check_timeouts())
        except Exception as e:  # noqa: BLE001 — 本轮失败由末尾聚合上抛承载
            raised = e

        # 核心断言：第二个回调必须跑到（r4 在这里只剩 ["bad"]）
        assert calls == ["bad", "good"], f"第二个回调被第一个的失败饿死: {calls}"
        assert isinstance(raised, TimeoutCallbackError), (
            f"失败没有以聚合异常抵达调用方: {raised!r}"
        )
        tasks = self._stream_payloads(redis_client, "test:wave4-queue")
        assert len(tasks) == 1, f"第二个回调没有完成入队: {tasks}"
        assert tasks[0]["resume_type"] == "timeout"
        assert tasks[0]["execution_id"] == "exec-w4-1"

    def test_failing_callback_for_one_subscription_does_not_starve_other(self):
        """#153/R1 回归（跨订阅）：subA 的回调持续失败不得饿死 subB。

        verifier 实测 r4：subA 失败即整轮中断 ⇒ subB 从未被处理；修复后同轮
        subA/subB 都被扫到，subB 的 timeout resume 正常入队（stream 2 条）。
        """
        execution_storage, subscription_storage, redis_client = self._make_storages()
        self._save_suspended(execution_storage, execution_id="exec-w4-A")
        self._save_suspended(execution_storage, execution_id="exec-w4-B")
        sub_a = self._timed_out_subscription(execution_id="exec-w4-A")
        sub_b = self._timed_out_subscription(execution_id="exec-w4-B")
        run(subscription_storage.store_subscription(sub_a))
        run(subscription_storage.store_subscription(sub_b))
        # 前置：存储顺序确定（subA 在前），否则本用例钉不住"谁先失败"
        assert [s.subscription_id for s in run(subscription_storage.list_subscriptions())] == [
            sub_a.subscription_id, sub_b.subscription_id,
        ]

        ef = self._make_filter(execution_storage, subscription_storage, redis_client)
        seen = []

        async def _flaky(subscription):
            if subscription.subscription_id == sub_a.subscription_id:
                raise ConnectionError("simulated callback outage for subA")

        async def _recorder(subscription):
            seen.append(subscription.subscription_id)

        # 顺序：[真实入队回调, _flaky（只对 subA 抛）, 记录器]
        ef._timeout_checker.register_timeout_callback(_flaky)
        ef._timeout_checker.register_timeout_callback(_recorder)

        raised = None
        try:
            run(ef._timeout_checker._check_timeouts())
        except Exception as e:  # noqa: BLE001 — 本轮失败由末尾聚合上抛承载
            raised = e

        # 核心断言：subA 失败不得让 subB 从未被扫到（r4 在这里 seen == []）
        assert seen == [sub_a.subscription_id, sub_b.subscription_id], (
            f"subB 被 subA 的失败饿死: {seen}"
        )
        assert isinstance(raised, TimeoutCallbackError), (
            f"失败没有以聚合异常抵达调用方: {raised!r}"
        )
        assert raised.subscription_ids == [sub_a.subscription_id]
        tasks = self._stream_payloads(redis_client, "test:wave4-queue")
        assert {t["execution_id"] for t in tasks} == {"exec-w4-A", "exec-w4-B"}, (
            f"跨订阅入队不完整: {tasks}"
        )

    def test_check_loop_survives_failing_callback_round(self):
        """#153/R1：坏回调的聚合上抛不得终止 checker 循环——下一轮继续检查。"""

        async def _test():
            execution_storage, subscription_storage, redis_client = self._make_storages()
            self._save_suspended(execution_storage)
            sub = self._timed_out_subscription()
            await subscription_storage.store_subscription(sub)

            ef = self._make_filter(execution_storage, subscription_storage, redis_client,
                                   interval=0.01)
            rounds = {"n": 0}
            real_check = ef._timeout_checker._check_timeouts

            async def _counting_check():
                rounds["n"] += 1
                await real_check()

            async def _bad(subscription):
                raise ConnectionError("simulated callback outage")

            ef._timeout_checker._check_timeouts = _counting_check
            ef._timeout_checker._on_timeout_callbacks.clear()
            ef._timeout_checker.register_timeout_callback(_bad)

            await ef._timeout_checker.start()
            await asyncio.sleep(0.1)
            await ef._timeout_checker.stop()

            assert rounds["n"] >= 2, (
                f"失败轮终止了 checker 循环（只跑了 {rounds['n']} 轮）"
            )

        run(_test())

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
            # #221：timeout 载荷必须绑定挂起订阅（生产由 event_filter 填入）。
            resume_data={"subscription_id": pending_state["subscription_id"]},
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
            # #221：绑定挂起期登记的订阅（生产 timeout 载荷带 subscription_id）。
            resume_data={
                "subscription_id": r2["context"]["$NODE"]["wait_event"]["subscription_id"],
            },
        )
        assert r3.get("is_suspend") is not True

        r4 = FlowExecution.run(
            flow, mode=ExecutionMode.DISTRIBUTED, context=r3["context"], event_bus=mock_bus,
        )
        assert r4["id"] == "task2"


# ─── 5. 公开入口契约：TimeoutCallbackError（#153/R1） ─────────


class TestTimeoutCallbackErrorPublicEntry:
    """#153/R1：聚合异常必须从 `plaita.event` 公开可达，且 `__all__` 不得挂空名。

    这一组是**契约钉子**（不是实现钉子）：调用方按 flow-worker.md 写
    ``from plaita.event import TimeoutCallbackError`` 必须成立；文档承诺的公开形状
    （``failures`` / ``subscription_ids`` / ``__cause__``）必须逐项对得上。
    """

    def test_public_import_path_is_the_same_class(self):
        """(a) 公开路径可导入，且与实现处是同一个类（不是影子副本）。"""
        from plaita.event import TimeoutCallbackError as Public

        assert Public is TimeoutCallbackError, "公开入口与 plaita.event.timeout 不是同一个类"

    def test_every_name_in_dunder_all_exists_and_star_import_works(self):
        """(b) `__all__` 全量清扫：每个名字都真的在模块上，star-import 真跑通。

        本仓出过「`__all__` 挂了不存在的名字 ⇒ `from ... import *` 直接
        AttributeError」的事故；下面两步（hasattr 清扫 + star-import 实跑）是这个
        bug 类的通用钉子——`__all__` 里任何空名都会立刻变红。
        """
        import plaita.event as event_pkg

        missing = [n for n in event_pkg.__all__ if not hasattr(event_pkg, n)]
        assert missing == [], f"plaita.event.__all__ 挂了不存在的名字: {missing}"

        namespace: dict = {}
        exec("from plaita.event import *", namespace)  # noqa: S102 — 被测行为本身
        not_exported = [n for n in event_pkg.__all__ if n not in namespace]
        assert not_exported == [], f"star-import 未导出: {not_exported}"
        assert "TimeoutCallbackError" in namespace

    def test_aggregate_public_shape_matches_documented_contract(self):
        """(c) 用**真实一轮 checker** 复核公开形状（不靠构造自证）。

        文档承诺：``.failures`` 是 ``(subscription_id, 原始异常)`` 序列、
        ``.subscription_ids`` 与之逐项对应、``__cause__`` 是**第一个**原始异常。
        同一订阅挂两个回调 ⇒ 一轮里两条失败，形状与顺序一起被钉住。
        """
        sub = EventSubscription(
            subscription_id="sub-public-shape",
            event_type="approval",
            correlation_id="exec-public-shape",
            timeout=1.0,
            created_at=time.time() - 60.0,
        )

        class _OneSubStorage:
            async def list_subscriptions(self):
                return [sub]

        first = ConnectionError("first outage")
        second = ValueError("second outage")
        seen = []

        async def _bad(_sub):
            seen.append(len(seen))
            raise (first if len(seen) == 1 else second)

        checker = SubscriptionTimeoutChecker(_OneSubStorage(), check_interval=0.01)
        checker.register_timeout_callback(_bad)
        checker.register_timeout_callback(_bad)   # 同一轮两个回调 ⇒ 两条失败

        raised = None
        try:
            run(checker._check_timeouts())
        except Exception as e:  # noqa: BLE001 — 本轮失败由末尾聚合上抛承载
            raised = e

        assert isinstance(raised, TimeoutCallbackError), f"未聚合上抛: {raised!r}"
        assert seen == [0, 1], "同一轮两个回调没有都跑到（断言空转）"

        failures = list(raised.failures)
        assert [type(exc) for _sid, exc in failures] == [ConnectionError, ValueError]
        assert [sid for sid, _exc in failures] == [sub.subscription_id] * 2
        assert list(raised.subscription_ids) == [sub.subscription_id] * 2, (
            ".subscription_ids 必须与 .failures 的订阅 id 逐项对应"
        )
        assert raised.__cause__ is failures[0][1], (
            "__cause__ 必须是第一个原始异常（不是聚合异常自身、也不是 None）"
        )
        assert raised.__cause__ is first
