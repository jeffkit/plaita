"""plaita#221：resume 必须绑定挂起期登记的那条订阅。

同一挂起点的 **event** 与 **timeout** 走两把不同的去重键
（``plaita/server/event_filter.py:222`` / ``:333``），因此同一订阅可以同时入队
两条 resume。先到的一条解决 A 节点、流程推进并在下游 B 重新挂起之后，后到的
陈旧 resume 历史上能通过准入（只看 ``is_suspending`` + ``status=pending``），
把 **B** 标成 completed/timeout 并写入 A 的事件载荷——无报错、无告警。

本文件钉住修复后的准入判据：

* 陈旧 event / timeout resume 落在下游 pending 节点 ⇒ ``ResumeGuardError``
  （worker 侧 ``_chain_has_resume_guard_error`` 会保持执行原状、只 ack 消息），
  且 ``$NODE.<下游>`` 一个字节都不被写；
* 同一订阅的 event + timeout 两条 resume ⇒ 只有第一条生效；
* ``cancel`` 是执行级意图（console 取消消息 ``data=None``），不按订阅绑定，
  不得被这道校验误杀。
"""
from __future__ import annotations

import pytest

from plaita.core.errors import ResumeGuardError
from plaita.core.executor import FlowExecution
from plaita.core.flow import Flow
from plaita.node import End, Start
from plaita.node.event_node import EventNode


class _Bus:
    """最小 event bus：每次注册发一个可追踪的订阅 id。"""

    def __init__(self):
        self.registered = []

    def register_subscription(self, **kwargs):
        sid = f"sub-{len(self.registered) + 1}"
        self.registered.append((sid, kwargs))
        return sid

    def unregister_subscription(self, sid):
        return True


def _flow() -> Flow:
    return Flow(
        flow_id="issue221",
        nodes=[
            Start(id="start", next="waitA"),
            EventNode(id="waitA", event_type="a.done", next="waitB"),
            EventNode(id="waitB", event_type="b.done", next="end"),
            End(id="end"),
        ],
    )


def _node_state(step, node_id):
    return (step.get("context") or {}).get("$NODE", {}).get(node_id, {})


def _resume(execution, flow, step, resume_type, data):
    return execution.run_distributed(
        flow,
        saved_context=step["context"],
        resume_type=resume_type,
        resume_data=data,
    )


def _guard_in_chain(exc: BaseException):
    """``run_distributed`` 把策略异常归一化成 ``FlowErrorException``（原异常挂
    ``__cause__``）；worker 的豁免判据正是沿链找 ``ResumeGuardError``。"""
    node = exc
    while node is not None:
        if isinstance(node, ResumeGuardError):
            return node
        node = node.__cause__
    return None


def _suspend_at_wait_a():
    """step1 挂起 waitA（sub-1）；step2 用 sub-1 的 event resume 解决它；
    step3 推进到 waitB 并挂起（sub-2）。返回 (flow, execution, step3, sub_a, sub_b)。"""
    flow = _flow()
    execution = FlowExecution(event_bus=_Bus())

    step1 = execution.run_distributed(flow)
    assert step1["is_suspend"] is True
    assert step1["id"] == "waitA"
    sub_a = _node_state(step1, "waitA")["subscription_id"]
    assert sub_a and _node_state(step1, "waitA")["status"] == "pending"

    event_a = {
        "event_id": "evt-a",
        "event_type": "a.done",
        "event_data": {"who": "A"},
        "subscription_id": sub_a,
    }
    step2 = _resume(execution, flow, step1, "event", event_a)
    assert step2["id"] == "waitA" and step2["is_suspend"] is False
    assert _node_state(step2, "waitA")["status"] == "completed"

    step3 = execution.run_distributed(flow, saved_context=step2["context"])
    assert step3["is_suspend"] is True and step3["id"] == "waitB"
    sub_b = _node_state(step3, "waitB")["subscription_id"]
    assert sub_b and sub_b != sub_a
    return flow, execution, step3, sub_a, sub_b


def test_stale_event_resume_at_downstream_node_is_rejected():
    """回归 1：陈旧 event resume 落在下游 pending 节点 ⇒ 执行仍挂起、B 不被写。"""
    flow, execution, step3, sub_a, _sub_b = _suspend_at_wait_a()
    baseline = _node_state(step3, "waitB")

    with pytest.raises(Exception) as ei:
        _resume(
            execution,
            flow,
            step3,
            "event",
            {"event_id": "evt-a", "event_type": "a.done",
             "event_data": {"who": "A"}, "subscription_id": sub_a},
        )

    guard = _guard_in_chain(ei.value)
    assert guard is not None, f"陈旧 resume 未被守卫拒绝：{ei.value!r}"
    assert "subscription_id" in str(guard)

    # 状态不变：waitB 仍 pending、没有事件载荷（执行保持挂起）。
    after = _node_state(step3, "waitB")
    assert after == baseline
    assert after["status"] == "pending"
    assert "event_data" not in after


def test_event_and_timeout_same_subscription_only_one_applies():
    """回归 2：同一订阅的 event + timeout 两条 resume ⇒ 只有第一条生效。

    第二条（timeout，仍带 sub-1）到达时执行已在下游 waitB 挂起（sub-2）；
    绑定校验必须把它当陈旧消息拒绝，waitB 保持 pending。
    """
    flow, execution, step3, sub_a, _sub_b = _suspend_at_wait_a()
    baseline = _node_state(step3, "waitB")

    with pytest.raises(Exception) as ei:
        _resume(
            execution,
            flow,
            step3,
            "timeout",
            {"subscription_id": sub_a, "event_type": "a.done", "node_id": "waitA"},
        )
    assert _guard_in_chain(ei.value) is not None
    assert _node_state(step3, "waitB") == baseline


def test_resume_without_subscription_id_is_rejected_when_state_has_one():
    """resume 必须**携带** subscription_id（不是「不匹配」才拒）。"""
    flow, execution, step3, _sub_a, sub_b = _suspend_at_wait_a()

    with pytest.raises(Exception) as ei:
        _resume(execution, flow, step3, "event",
                {"event_type": "b.done", "event_data": {"who": "B"}})
    guard = _guard_in_chain(ei.value)
    assert guard is not None
    assert "缺少 subscription_id" in str(guard)
    assert _node_state(step3, "waitB")["status"] == "pending"

    # 正控：同一个节点、同一个订阅的有效 resume 仍然能推进（不是一刀切拒收）。
    sub_b = sub_b
    step4 = _resume(execution, flow, step3, "event",
                    {"event_type": "b.done", "event_data": {"who": "B"},
                     "subscription_id": sub_b})
    assert step4["id"] == "waitB" and step4["is_suspend"] is False
    assert _node_state(step4, "waitB")["status"] == "completed"


def test_resume_with_wrong_event_type_is_rejected():
    """绑定的第二道：订阅 id 对、event_type 对不上 ⇒ 仍视为陈旧。"""
    flow, execution, step3, _sub_a, sub_b = _suspend_at_wait_a()

    with pytest.raises(Exception) as ei:
        _resume(execution, flow, step3, "event",
                {"event_type": "a.done", "event_data": {"who": "A"},
                 "subscription_id": sub_b})
    guard = _guard_in_chain(ei.value)
    assert guard is not None
    assert "event_type" in str(guard)
    assert _node_state(step3, "waitB")["status"] == "pending"


def test_cancel_without_subscription_id_still_applies():
    """cancel 是执行级意图（console 消息 ``data=None``），不得被绑定校验误杀。"""
    flow, execution, step3, _sub_a, _sub_b = _suspend_at_wait_a()

    step4 = _resume(execution, flow, step3, "cancel", None)
    assert step4["id"] == "waitB"
    assert _node_state(step4, "waitB")["status"] == "cancelled"


def test_real_filter_enqueues_event_and_timeout_then_stale_timeout_rejected():
    """生产触发链组合：真实 EventFilter 对同一订阅**双双入队**，后到的 timeout 被拒。

    这条用例把 issue 里的两个 probe 接起来：``handle_event``（event 去重键）与
    ``_on_subscription_timeout``（timeout 去重键）互不遮挡 ⇒ 队列里确有两条
    resume；先消费 event、流程推进到下游 B 后，第二条 timeout 必须被绑定校验
    拒绝，B 保持 pending。前 5 条用例只验证消费侧判据，这里补上产生侧。
    """
    fakeredis = pytest.importorskip("fakeredis")
    import asyncio
    import json
    from unittest.mock import AsyncMock

    from plaita.event.core import Event, EventSubscription
    from plaita.event.memory import InMemoryEventSubscriptionStorage
    from plaita.server.event_filter import EventFilter
    from plaita.storage.base import ExecutionState
    from plaita.storage.memory import MemoryExecutionStorage

    flow = _flow()
    execution = FlowExecution(event_bus=_Bus())
    step1 = execution.run_distributed(flow)
    assert step1["is_suspend"] is True and step1["id"] == "waitA"
    sub_a = _node_state(step1, "waitA")["subscription_id"]

    execution_id = "exec-issue221"
    execution_storage = MemoryExecutionStorage()
    execution_storage.save_execution_state(
        execution_id,
        ExecutionState(
            execution_id=execution_id,
            flow_id="issue221",
            status="suspended",
            context=step1["context"],
        ),
    )
    subscription_storage = InMemoryEventSubscriptionStorage()
    subscription = EventSubscription(
        subscription_id=sub_a,
        event_type="a.done",
        correlation_id=execution_id,
        flow_id="issue221",
        node_id="waitA",
        timeout=1.0,
    )
    asyncio.run(subscription_storage.store_subscription(subscription))

    queue = "test:issue221-queue"
    redis_client = fakeredis.FakeRedis(decode_responses=True)
    event_filter = EventFilter(
        execution_storage=execution_storage,
        subscription_storage=subscription_storage,
        redis_client=redis_client,
        event_bus=AsyncMock(),
        queue_name=queue,
    )

    def _tasks():
        tasks = []
        for _mid, fields in redis_client.xrange(queue, min="-", max="+"):
            raw = fields.get("payload")
            if isinstance(raw, bytes):
                raw = raw.decode()
            tasks.append(json.loads(raw))
        return tasks

    # 事件链路先入队（event 去重键）。
    asyncio.run(event_filter.handle_event(Event(
        event_type="a.done", correlation_id=execution_id, data={"who": "A"},
    )))
    assert [t["resume_type"] for t in _tasks()] == ["event"]

    # 同一订阅的超时链路再入队（timeout 去重键与 event 键不同，不被挡住）。
    asyncio.run(event_filter._on_subscription_timeout(subscription))
    tasks = _tasks()
    assert [t["resume_type"] for t in tasks] == ["event", "timeout"]
    assert all(t["data"]["subscription_id"] == sub_a for t in tasks)

    # 先消费 event：waitA 完成，流程推进到 waitB 重新挂起。
    step2 = _resume(execution, flow, step1, "event", tasks[0]["data"])
    assert _node_state(step2, "waitA")["status"] == "completed"
    step3 = execution.run_distributed(flow, saved_context=step2["context"])
    assert step3["is_suspend"] is True and step3["id"] == "waitB"
    baseline = _node_state(step3, "waitB")

    # 后到的 timeout（仍带 sub_a）是陈旧消息：拒绝且不写 waitB。
    with pytest.raises(Exception) as ei:
        _resume(execution, flow, step3, "timeout", tasks[1]["data"])
    assert _guard_in_chain(ei.value) is not None
    assert _node_state(step3, "waitB") == baseline
