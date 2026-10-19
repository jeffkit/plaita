"""plaita#258：流内推进后 event/timeout resume 重投被 ack，running 执行永久悬挂。

## 缺陷

事件/超时 resume 在流内已经推进并落过 ``step_persist`` 之后（``PERSIST_EVERY_N_STEPS=1``），
若发生瞬态 ``StatePersistError``（Redis 瞬断的常规落点）或 worker 进程崩溃，
消息不 ack、按 at-least-once 重投；但重投消息仍携带 ``resume_type=event/timeout``。
此时盘上是 ``status=running`` + ``$LAST_NODE=非挂起节点``，策略层 ``_handle_resume``
抛 ``ResumeGuardError``（"Node ... is not an EventNode"）→ ``ResumeProtocolError``
→ 旧 ``run()`` 分支**无条件 ack** 唯一载体。订阅已注销、无 timeout 订阅、无队列
条目：执行永远停在 running，只能等人工收尸。

## 修复契约

1. **入口归一化**：取得租约并重读状态后，若盘上 ``running`` 且 ``event/timeout``
   载体已错过挂起点（``$LAST_NODE`` 非 pending 挂起点），把本次意图改写为
   ``continue``，从 checkpoint 续跑；已完成节点不重放。
2. **run() 兜底**：``ResumeProtocolError`` 分支 ack 前做状态感知——同一判据命中
   则**先入队**一份 ``resume_type=continue`` 新副本，再 ack 原条；入队失败/状态
   读取瞬断不 ack，消息留 pending。
3. **真 #33 错配零变化**：``suspended + continue`` 仍走 ``already_suspended``
   幂等短路；陈旧 event resume 打在**新的挂起点**上仍被订阅绑定守卫拒绝；
   running + 仍处于 pending 挂起点的 continue/event 仍按既有语义 ack + poison。

## 负控（还原修复后必须精确红）

- 还原入口归一化：``test_transient_persist_retry_resumes_from_checkpoint`` 在
  重投那一步直接抛 ``ResumeProtocolError``（旧行为），断言不到 completed；
- 还原 run() 分支：``test_run_loop_requeues_continue_instead_of_acking`` 的
  ``enqueued_bodies`` 为空（旧分支只 ack + poison）。
"""
import logging
import threading

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.node.mock import Mock
from plaita.server.flow_worker import (
    FlowWorker,
    RedisFlowWorker,
    ResumeProtocolError,
    StatePersistError,
)
from plaita.server.task_queue import RedisStreamTaskQueue, StreamTask
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

EXEC = "exec-issue258"
QUEUE = "test:issue258-stranded-resume"

FLOW = {
    "flow_id": "f1",
    "version": "v1",
    "nodes": [
        {"id": "start", "type": "start", "next": "evt"},
        {"id": "evt", "type": "event", "event_type": "go", "next": "m1"},
        {"id": "m1", "type": "mock", "value": 1, "next": "m2"},
        {"id": "m2", "type": "mock", "value": 2, "next": "m3"},
        {"id": "m3", "type": "mock", "value": 3, "next": "end"},
        {"id": "end", "type": "end"},
    ],
}

STALE_FLOW = {
    "flow_id": "f2",
    "version": "v1",
    "nodes": [
        {"id": "start", "type": "start", "next": "evt1"},
        {"id": "evt1", "type": "event", "event_type": "go", "next": "m1"},
        {"id": "m1", "type": "mock", "value": 1, "next": "evt2"},
        {"id": "evt2", "type": "event", "event_type": "go2", "next": "end"},
        {"id": "end", "type": "end"},
    ],
}


class _TransientPersistStorage(MemoryExecutionStorage):
    """在指定第 N 次 save 时抛一次 ``StatePersistError``（模拟 Redis 瞬断）。"""

    def __init__(self):
        super().__init__()
        self.calls = 0
        self.fail_on_call = None

    def save_execution_state(self, execution_id, state):
        self.calls += 1
        if self.fail_on_call == self.calls:
            self.fail_on_call = None
            raise StatePersistError("simulated transient persist failure")
        return super().save_execution_state(execution_id, state)


class _LoadBlipStorage(MemoryExecutionStorage):
    """状态读取瞬断：run() 的状态感知不得 ack 丢载体。"""

    def load_execution_state(self, execution_id):
        raise RuntimeError("redis down")


def _suspended_state(execution_id=EXEC, flow_id="f1", subscription_id="sub-1"):
    return ExecutionState(
        execution_id=execution_id,
        flow_id=flow_id,
        flow_version="v1",
        status="suspended",
        context={
            "$LAST_NODE": "evt",
            "$NODE": {
                "evt": {
                    "status": "pending",
                    "subscription_id": subscription_id,
                    "event_type": "go",
                }
            },
        },
    )


def _worker(flow=FLOW, storage=None):
    flows = MemoryFlowStorage()
    flows.save_flow(flow)
    storage = storage if storage is not None else MemoryExecutionStorage()
    return FlowWorker(execution_storage=storage, flow_storage=flows), storage


def _spy_mock_executions(monkeypatch):
    """记录 Mock 节点真实执行次数（证明已完成节点不重放）。"""
    calls = []
    real_execute = Mock.execute

    def _spy(self, execution):
        calls.append(self.id)
        return real_execute(self, execution)

    monkeypatch.setattr(Mock, "execute", _spy)
    return calls


class TestCrashWindowRecovery:
    """崩溃窗口：step_persist 后重投同一条 event/timeout 载体。"""

    @pytest.mark.parametrize("resume_type", ["event", "timeout"])
    def test_transient_persist_retry_resumes_from_checkpoint(
        self, resume_type, monkeypatch, caplog
    ):
        calls = _spy_mock_executions(monkeypatch)
        worker, storage = _worker(storage=_TransientPersistStorage())
        storage.save_execution_state(EXEC, _suspended_state())
        # seed 是第 1 次 save；第 3 次 = m2 的 step_persist。
        storage.fail_on_call = 3

        with pytest.raises(StatePersistError):
            worker.resume_flow(
                "f1", EXEC, resume_type, {"subscription_id": "sub-1"}
            )

        durable = storage.load_execution_state(EXEC)
        assert durable.status == "running"
        assert durable.context["$LAST_NODE"] == "m1"
        assert durable.context["$NODE"]["m1"] == 1

        # 同一条载体按原样重投（订阅已注销；resume_type 未改写）。
        with caplog.at_level(logging.WARNING, logger="plaita.server.flow_worker"):
            result = worker.resume_flow(
                "f1", EXEC, resume_type, {"subscription_id": "sub-1"}
            )

        assert result.get("is_end") is True, "重投必须从 m1 续跑到 end，而不是被拒"
        restored = storage.load_execution_state(EXEC)
        assert restored.status == "completed"
        assert restored.error is None
        # m1 的 step_persist 已成功落盘：重投只允许重跑「未落盘」的 m2。
        assert calls.count("m1") == 1, "已落盘的 m1 不得重放"
        assert calls.count("m3") == 1
        assert calls.count("start") == 0
        assert any(
            "归一化为 continue" in record.getMessage() for record in caplog.records
        ), "载体归一化必须留 WARNING 可观测痕迹"

    def test_suspended_continue_still_short_circuits(self):
        """#33 回归：suspended + continue 不取推进语义、状态原样。"""
        worker, storage = _worker()
        storage.save_execution_state(EXEC, _suspended_state())

        result = worker.resume_flow("f1", EXEC, "continue")

        assert result.get("already_suspended") is True
        kept = storage.load_execution_state(EXEC)
        assert kept.status == "suspended"
        assert kept.context["$LAST_NODE"] == "evt"

    def test_stale_event_resume_on_new_suspension_stays_rejected(self):
        """#221/#258 交叉：陈旧 event resume 打在新挂起点 → 拒绝且不双推进。"""
        worker, storage = _worker(flow=STALE_FLOW)
        storage.save_execution_state(
            EXEC,
            ExecutionState(
                execution_id=EXEC,
                flow_id="f2",
                flow_version="v1",
                status="suspended",
                context={
                    "$LAST_NODE": "evt2",
                    "$NODE": {
                        "evt1": {"status": "completed", "subscription_id": "sub-1"},
                        "m1": {"value": 1},
                        "evt2": {
                            "status": "pending",
                            "subscription_id": "sub-2",
                            "event_type": "go2",
                        },
                    },
                },
            ),
        )

        with pytest.raises(ResumeProtocolError) as exc_info:
            worker.resume_flow(
                "f2", EXEC, "event",
                {"subscription_id": "sub-1", "event_type": "go"},
            )

        assert "Stale event resume" in str(exc_info.value)
        kept = storage.load_execution_state(EXEC)
        assert kept.status == "suspended"
        assert kept.context["$LAST_NODE"] == "evt2"
        assert kept.context["$NODE"]["evt2"]["status"] == "pending"


class _OneShotQueue(RedisStreamTaskQueue):
    """消费循环只喂一条预置任务，第二次 read 触发停机。"""

    def __init__(self, *args, task, on_exhausted, **kwargs):
        super().__init__(*args, **kwargs)
        self._task = task
        self._on_exhausted = on_exhausted
        self.acked_ids = []
        self.enqueued_bodies = []
        self.poison = 0

    def read(self, block_ms=0):
        if self._task is not None:
            task, self._task = self._task, None
            return task
        if self._on_exhausted is not None:
            callback, self._on_exhausted = self._on_exhausted, None
            callback()
        return None

    def ack(self, message_id):
        self.acked_ids.append(message_id)
        super().ack(message_id)

    def enqueue(self, task, *, schema_version=None):
        self.enqueued_bodies.append(dict(task))
        return super().enqueue(task, schema_version=schema_version)

    def note_poison(self):
        self.poison += 1
        super().note_poison()


def _redis_worker(fake, storage=None):
    flows = MemoryFlowStorage()
    flows.save_flow(FLOW)
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name=QUEUE,
        execution_storage=storage if storage is not None else MemoryExecutionStorage(),
        flow_storage=flows,
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
        claim_min_idle_ms=1,
        read_block_ms=100,
    )


def _run_one_task(worker, fake, body, monkeypatch, storage=None):
    """让真实 run() 消费一条预置任务，返回 (queue, mid)。"""
    group = "g"
    seed = RedisStreamTaskQueue(
        fake, QUEUE, group_name=group, consumer_name="seed", claim_min_idle_ms=1
    )
    seed.ensure_group()
    mid = seed.enqueue(dict(body))
    first = seed.read(block_ms=100)
    assert first is not None and first.message_id == mid

    queue = _OneShotQueue(
        fake,
        QUEUE,
        group_name=group,
        consumer_name="worker",
        claim_min_idle_ms=1,
        max_deliveries=5,
        task=StreamTask(message_id=mid, body=dict(body), delivery_count=1),
        on_exhausted=worker.stop,
    )

    def _boom(*args, **kwargs):
        raise ResumeProtocolError("stranded carrier guard", execution_id=body["execution_id"])

    monkeypatch.setattr(worker, "_dispatch_task", _boom)
    worker._task_queue = queue
    thread = threading.Thread(target=worker.run, daemon=True)
    thread.start()
    thread.join(timeout=10.0)
    assert not thread.is_alive(), "run() 未在队列停机回调后退出"
    return queue, mid


class TestRunLoopStateAwareAck:
    """run() 的 ResumeProtocolError 分支：ack 前状态感知。"""

    @pytest.mark.parametrize("resume_type", ["event", "timeout"])
    def test_run_loop_requeues_continue_instead_of_acking(self, resume_type, monkeypatch):
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        worker = _redis_worker(fake, storage)
        storage.save_execution_state(
            EXEC,
            ExecutionState(
                execution_id=EXEC,
                flow_id="f1",
                flow_version="v1",
                status="running",
                context={"$LAST_NODE": "m1", "$NODE": {"m1": {"value": 1}}},
            ),
        )
        body = {
            "type": "resume",
            "flow_id": "f1",
            "execution_id": EXEC,
            "resume_type": resume_type,
            "tenant_id": "default",
        }

        queue, mid = _run_one_task(worker, fake, body, monkeypatch)

        assert queue.acked_ids == [mid], "原载体在完成重投后必须 ack"
        assert queue.poison == 0, "可续跑的载体不是 poison"
        assert len(queue.enqueued_bodies) == 1
        requeued = queue.enqueued_bodies[0]
        assert requeued["resume_type"] == "continue"
        assert requeued["execution_id"] == EXEC
        assert requeued["flow_id"] == "f1"
        assert requeued["type"] == "resume"
        kept = storage.load_execution_state(EXEC)
        assert kept.status == "running", "分支只切换载体，不改执行状态"

    def test_run_loop_true_protocol_mismatch_keeps_ack_semantics(self, monkeypatch):
        """真 #33 错配（running + 挂起节点 pending + continue）：原样 ack + poison。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        worker = _redis_worker(fake, storage)
        storage.save_execution_state(
            EXEC,
            ExecutionState(
                execution_id=EXEC,
                flow_id="f1",
                flow_version="v1",
                status="running",
                context={
                    "$LAST_NODE": "evt",
                    "$NODE": {
                        "evt": {
                            "status": "pending",
                            "subscription_id": "sub-1",
                            "event_type": "go",
                        }
                    },
                },
            ),
        )
        body = {
            "type": "resume",
            "flow_id": "f1",
            "execution_id": EXEC,
            "resume_type": "continue",
            "tenant_id": "default",
        }

        queue, mid = _run_one_task(worker, fake, body, monkeypatch)

        assert queue.acked_ids == [mid]
        assert queue.poison == 1
        assert queue.enqueued_bodies == [], "不得把真协议错配改成 continue 自旋"
        kept = storage.load_execution_state(EXEC)
        assert kept.status == "running"
        assert kept.context["$NODE"]["evt"]["status"] == "pending"

    def test_run_loop_state_read_blip_leaves_pending(self, monkeypatch):
        """状态读取瞬断：不 ack、不 poison、不重投，消息留 pending。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _redis_worker(fake, _LoadBlipStorage())
        body = {
            "type": "resume",
            "flow_id": "f1",
            "execution_id": EXEC,
            "resume_type": "event",
            "tenant_id": "default",
        }

        queue, _mid = _run_one_task(worker, fake, body, monkeypatch)

        assert queue.acked_ids == [], "读盘瞬断绝不能丢唯一载体"
        assert queue.poison == 0
        assert queue.enqueued_bodies == []
        pending = fake.xpending(QUEUE, "g")
        count = int(pending["pending"] if isinstance(pending, dict) else pending[0])
        assert count == 1, "原消息必须留在 PEL 等回收"
