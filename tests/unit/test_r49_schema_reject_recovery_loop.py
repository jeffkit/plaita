"""A49-01：超版本拒收 × 死信守卫恢复分支 = 自持克隆-死信热环。

## 缺陷

``_read_once`` 的超版本拒收走 ``dead_letter_if_allowed`` → FlowWorker 的
``_dead_letter_guard``。守卫第三分支「执行非终态 + 租约空」会先重入队一份
**继承源 schema_version** 的副本（A44-02），再放行原条死信。副本仍是 v2，
本 build 下一轮读到后再次拒收、再次克隆：流内恒 1 条、DLQ 每轮 +1，
永不收敛（回滚 / 生产者先行 / 伪造信封场景）。

## 修复契约

1. 队列在版本拒收时给任务打 ``schema_rejected`` 标记；
2. FlowWorker 的守卫对该标记**不克隆**、不终态化，直接放行死信一次；
3. 租约仍在时仍拒绝死信（消息留 pending 等新 build）——活持有者优先；
4. 非版本拒收（如 ``max_deliveries``）的恢复克隆语义不变。

## 负控（还原修复后必须精确红）

- 去掉 ``_reject_unsupported_schema`` 的 ``replace(task, schema_rejected=True)``
  或守卫里的 ``schema_rejected`` 分支：``test_over_version_...`` 的
  ``xlen(dlq) == 1`` 变红（每轮 +1，实测 6 轮 6 条）且 ``xlen(stream) == 1``
  （克隆副本仍在流内）。
"""
import json

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.server.flow_worker import RedisFlowWorker
from plaita.server.task_queue import StreamTask, enqueue_task
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

EXEC = "exec-a49"
QUEUE = "test:a49-schema-reject-loop"

FLOW_DEF = {
    "flow_id": "f1",
    "version": "1",
    "nodes": [
        {"id": "start", "type": "start", "next": "end"},
        {"id": "end", "type": "end", "output": "ok"},
    ],
}

BODY = {
    "type": "resume",
    "flow_id": "f1",
    "execution_id": EXEC,
    "resume_type": "event",
    "tenant_id": "default",
}


def _worker(fake, storage=None) -> RedisFlowWorker:
    flows = MemoryFlowStorage()
    flows.save_flow(FLOW_DEF)
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name=QUEUE,
        execution_storage=storage if storage is not None else MemoryExecutionStorage(),
        flow_storage=flows,
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
    )


def _state(status: str) -> ExecutionState:
    return ExecutionState(
        execution_id=EXEC,
        flow_id="f1",
        flow_version="1",
        status=status,
        context={"$LAST_NODE": "start", "$NODE": {"start": {}}},
    )


def _task(**kwargs) -> StreamTask:
    base = dict(
        message_id="1-0",
        body=dict(BODY),
        delivery_count=5,
        schema_version=2,
    )
    base.update(kwargs)
    return StreamTask(**base)


def _pending_count(fake, queue) -> int:
    summary = fake.xpending(QUEUE, queue.group_name)
    return int(summary["pending"] if isinstance(summary, dict) else summary[0])


class TestSchemaRejectDoesNotSelfClone:
    @pytest.mark.parametrize("status", ["suspended", "running"])
    def test_over_version_message_converges_to_one_dlq_row(self, status):
        """非终态 + 租约空 + v2 载体：跑 6 轮只能有 1 条 DLQ、流内不留副本。

        修复前：每轮守卫克隆一份 v2 副本再死信原条 —— DLQ 1→6、流内恒 1。
        """
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        worker = _worker(fake, storage)
        storage.save_execution_state(EXEC, _state(status))
        queue = worker._get_task_queue()
        queue.ensure_group()
        enqueue_task(fake, QUEUE, dict(BODY), schema_version=2)

        for _ in range(6):
            assert queue.read(block_ms=10) is None

        assert fake.xlen(queue.dlq_key) == 1, "DLQ 不得随轮次线性增长（自持热环）"
        assert fake.xlen(QUEUE) == 0, "不得克隆出继承同一超版本的恢复副本"
        assert _pending_count(fake, queue) == 0
        assert queue.stats()["schema_rejected"] == 1
        assert queue.stats()["dead_lettered"] == 1
        envelope = json.loads(fake.xrange(queue.dlq_key)[0][1]["payload"])
        assert envelope["schema_version"] == 2, "DLQ 信封保留源版本"
        assert "unsupported schema_version=2" in envelope["reason"]
        # 拒收不终态化执行：suspended 仍可被后续 event/cancel/timeout 唤醒。
        assert storage.load_execution_state(EXEC).status == status

    def test_guard_does_not_terminalize_running_on_schema_reject(self):
        """版本拒收不是节点失败：即使重试计数已到预算，也不得终态化 error。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        worker = _worker(fake, storage)
        storage.save_execution_state(EXEC, _state("running"))
        fake.set(
            f"plaita:execution:noderetry:{EXEC}:a",
            str(worker._node_retry_budget()),
        )

        assert worker._dead_letter_guard(_task(schema_rejected=True)) is True

        assert storage.load_execution_state(EXEC).status == "running"
        assert fake.xlen(QUEUE) == 0

    def test_schema_reject_with_live_lease_stays_pending(self):
        """活租约优先：拒收消息留 pending 等新 build，不克隆、不死信。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        worker = _worker(fake, storage)
        storage.save_execution_state(EXEC, _state("running"))
        fake.set(f"plaita:execution:lease:{EXEC}", "holder")

        assert worker._dead_letter_guard(_task(schema_rejected=True)) is False

        assert fake.xlen(QUEUE) == 0
        assert fake.xlen(f"{QUEUE}:dlq") == 0

    def test_non_schema_recovery_clone_is_unchanged(self):
        """对照：普通超投递消息（非版本拒收）仍克隆恢复副本后放行死信。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        worker = _worker(fake, storage)
        storage.save_execution_state(EXEC, _state("suspended"))

        assert worker._dead_letter_guard(_task(schema_version=1)) is True

        entries = fake.xrange(QUEUE)
        assert len(entries) == 1, "非版本拒收的恢复路径必须保留"
        assert json.loads(entries[0][1]["payload"])["execution_id"] == EXEC
