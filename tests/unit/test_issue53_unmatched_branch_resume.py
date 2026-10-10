"""plaita#53 回归：假分支无后继不得悬死 running，running 执行 resume/retry 不得假受理。

## 基线缺陷（2026-10-09 值守实测两例，exec 5d3353b8… / 48163508…）

- 实测 A/B：条件节点（codeflow `if ok == False` → id `ok_eq_false`，同
  `passed_eq_false`）走假分支后执行再无任何节点启动、status=running 持续
  88 分钟，无 error、无终态；
- resume 假受理：对 running 执行 `POST /resume {"resume_type":"retry"}` 返回
  「已加入队列」，等待 3 分钟以上 node_timings / last_update_time 双双不动。

## 本组断言（工单验收 1/2 的可执行化）

1. 分支未命中（`if` 无 else：`else_next=""`，以及未改写占位默认值
   `else_next="false"`）⇒ 执行必须显式终态 error，绝不留在 running。
   引擎侧防御在 ``plaita/core/strategies.py`` 的 ``_get_next_from_last``
   （distributed）与 ``_advance_one``（normal/generator）——本地树已有；
   本组把它在 **引擎层 + worker 端到端** 两层钉住（工单验收 3 的
   「核对线上已含该防御」以此为准的代码侧证据）。
2. `status=running` + `resume_type=retry`：必须**真实重派**（有节点活动）或
   显式拒绝，不得返回既无推进、又自称终态的 ``already_terminal`` 形状。
   基线缺陷：`_deterministic_failure_exhausted` 短路未与 ``retry_wakeup``
   （status=error）绑定——计数键寿命长于状态行（终态化写盘失败等），于是
   「running + 计数达限」的执行收到 retry 会命中该分支 → 返回
   ``already_terminal=True`` 而一行不推进（假受理）。
3. 反证：error 态 + retry + 计数达限仍拒绝唤醒（#73 语义不得被本修复放宽）。

反证要求：把 ``retry_wakeup and`` 去掉，断言 2 的核心用例必须变红。
"""
import json

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.core.errors import FlowErrorException
from plaita.core.executor import FlowExecution
from plaita.core.flow import Flow
from plaita.server.flow_worker import RedisFlowWorker
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage


def _if_false_branch_flow(else_next: str) -> dict:
    """codeflow `if ok == False:` 无 else 的规范形态：id `ok_eq_false`。

    `next` = 真分支（条件成立），`else_next` = 假分支。无 else 时假分支没有
    可去之处，两种落法都必须显式失败：``""``（分支未命中）与 ``"false"``
    （if 节点占位默认值指向不存在的节点）。线上 `self-improve-v2-sbx` 两例
    具体属哪一种，工单列为「未证实」（需 dump 线上定义）——两种都覆盖。
    """
    return {
        "flow_id": "f53",
        "version": "1",
        "inputType": {"dataType": "object"},
        "nodes": [
            {"id": "start", "type": "start", "next": "ok_eq_false"},
            {
                "id": "ok_eq_false",
                "type": "if",
                "condition": {"field": "$INPUT.ok", "operator": "eq", "value": True},
                "next": "truthy_end",
                "else_next": else_next,
            },
            {"id": "truthy_end", "type": "end", "output": {"branch": "true"}},
        ],
    }


PARTIAL_FLOW = {
    "flow_id": "f53b",
    "version": "1",
    "inputType": {"dataType": "object"},
    "nodes": [
        {"id": "start", "type": "start", "next": "impl"},
        {"id": "impl", "type": "assignment", "output": {"step": 1}, "next": "end"},
        {"id": "end", "type": "end", "output": "ok"},
    ],
}

NOFAIL_KEY = "plaita:execution:nofail:exec-53"


def _worker(fake, flow_def: dict, storage=None) -> RedisFlowWorker:
    fs = MemoryFlowStorage()
    fs.save_flow(flow_def)
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:issue53",
        execution_storage=storage if storage is not None else MemoryExecutionStorage(),
        flow_storage=fs,
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
    )


class TestUnmatchedBranchTerminalizes:
    """验收 1：假分支无后继 ⇒ 显式终态，不悬死 running。"""

    @pytest.mark.parametrize("else_next", ["", "false"])
    def test_engine_distributed_step_raises_instead_of_silent_end(self, else_next):
        """引擎层：断点步进解析不出后继时抛错，不得合成 is_end 假成功。"""
        fl = Flow.model_validate(_if_false_branch_flow(else_next))
        execution = FlowExecution()
        execution.mode = "distributed"

        # 首步跑 start + if 节点（distributed 首步会连跑到首个"后继确定"的
        # 节点），此时分支未命中已写入 checkpoint。
        first = execution.run_distributed(fl, {"ok": False})
        assert first["id"] == "ok_eq_false"
        assert first["is_end"] is False

        with pytest.raises(FlowErrorException) as excinfo:
            execution.run_distributed(
                fl, saved_context=first["context"], resume_type="continue"
            )
        assert "ok_eq_false" in str(excinfo.value), (
            "报错必须点名条件节点（否则值守无从定位假分支）"
        )

    @pytest.mark.parametrize("else_next", ["", "false"])
    def test_worker_terminalizes_execution_as_error(self, else_next):
        """worker 端到端：执行落 error 终态，绝不停在 running。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        worker = _worker(fake, _if_false_branch_flow(else_next), storage)

        worker.start_flow("f53", {"ok": False}, execution_id="exec-53")

        state = storage.load_execution_state("exec-53")
        assert state.status == "error", (
            f"条件假分支无后继必须显式失败，实际 status={state.status}"
        )
        assert "ok_eq_false" in json.dumps(state.error, ensure_ascii=False), (
            "error 必须点名节点 id，值守/告警可据此定位"
        )
        assert state.end_time, "终态必须带 end_time（可被终态查询/看板识别）"


class TestRunningResumeRetry:
    """验收 2：running + retry 必须真实重派（有节点活动）或显式拒绝。"""

    @staticmethod
    def _running_state(status: str = "running") -> ExecutionState:
        return ExecutionState(
            execution_id="exec-53", flow_id="f53b", flow_version="1",
            status=status,
            context={"$LAST_NODE": "start", "$NODE": {"start": {}}},
        )

    def test_running_retry_redispatches(self):
        """基线语义（护栏）：running + retry 与 continue 同路，真实推进。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        worker = _worker(fake, PARTIAL_FLOW, storage)
        storage.save_execution_state("exec-53", self._running_state())

        result = worker.resume_flow("f53b", "exec-53", "retry")

        assert result["is_end"] is True, "running 执行的 retry 必须真实派发后继节点"
        assert storage.load_execution_state("exec-53").status == "completed"

    def test_running_retry_not_swallowed_by_exhausted_counter(self, caplog):
        """核心钉子（plaita#53）：计数达限不得把 running 执行的 retry 变成哑弹。

        计数键寿命长于状态行（终态化写盘失败、状态被回滚…）——一旦把它当
        「终态」判据，running 执行收到的 retry 就会被 ``already_terminal``
        谎报 + 一行不推进，正是工单里的「假受理」。
        """
        import logging

        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        worker = _worker(fake, PARTIAL_FLOW, storage)
        storage.save_execution_state("exec-53", self._running_state())
        fake.set(NOFAIL_KEY, worker.DETERMINISTIC_FAILURE_MAX)

        with caplog.at_level(logging.WARNING, logger="plaita.server.flow_worker"):
            result = worker.resume_flow("f53b", "exec-53", "retry")

        assert result.get("already_terminal") is not True, (
            "running 执行不得被回「已成终态」——那是谎报，且实际未推进"
        )
        assert result["is_end"] is True, "必须真实重派（有节点活动）"
        assert storage.load_execution_state("exec-53").status == "completed"
        assert "按「从断点续跑」处理" in caplog.text, (
            "running + retry 的语义必须留痕（值守据此区分重派与白等）"
        )

    def test_error_retry_still_refused_when_exhausted(self):
        """反证（#73 语义不得放宽）：error 态 + 达限仍幂等拒绝唤醒。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        worker = _worker(fake, PARTIAL_FLOW, storage)
        storage.save_execution_state(
            "exec-53", self._running_state(status="error")
        )
        fake.set(NOFAIL_KEY, worker.DETERMINISTIC_FAILURE_MAX)

        result = worker.resume_flow("f53b", "exec-53", "retry")

        assert result["already_terminal"] is True
        assert result["deterministic_failure_exhausted"] is True
        state = storage.load_execution_state("exec-53")
        assert state.status == "error", "被拒的唤醒必须保留原 error 终态"


class TestResumeDroppedOnLiveLease:
    """验收 2 的另一半：resume 不生效时必须**有告警**，不许静默丢弃。

    租约被在册 worker 持有时，resume 消息按既有语义 ack 释放（#50/#52：留
    pending 会烧 delivery 并产生假死信）。但「ack 丢弃」对调用方是不可见的
    ——BFF 已回「恢复请求已加入队列」，值守看到的是「已救活」与「白等」无法
    区分（工单「假受理」）。此处钉子：resume 被丢必须留 WARNING，且日志里
    说清「本次 resume 未生效」与下一步动作。
    """

    def test_resume_dropped_by_lease_conflict_logs_warning(self, caplog):
        import logging
        import threading

        from plaita.server.execution_lease import ExecutionLeaseError
        from plaita.server.task_queue import StreamTask

        task = StreamTask(
            message_id="1-0",
            body={"type": "resume", "execution_id": "exec-53", "resume_type": "retry"},
        )

        class _Queue:
            def __init__(self):
                self.acked = []
                self.pending = task

            def read(self, block_ms=None):
                if self.pending is not None:
                    pending, self.pending = self.pending, None
                    worker._running = False  # 只跑一轮
                    return pending
                return None

            def ack(self, mid):
                self.acked.append(mid)

            def note_lease_conflict(self):
                pass

            def ensure_group(self):
                pass

        worker = RedisFlowWorker.__new__(RedisFlowWorker)
        worker._running = True
        worker._enable_registry = False
        worker._active_task_count = 0
        worker._active_count_lock = threading.Lock()
        worker._residue_sweep_interval_seconds = 0  # 本轮不扫残留（与缺陷无关）
        worker._last_residue_sweep = None
        worker._residue_sweep_lock = threading.Lock()
        worker.read_block_ms = 1
        worker._lease_conflict_ack_safe = lambda body: True
        worker._dispatch_task = lambda body, delivery_count=None: (_ for _ in ()).throw(
            ExecutionLeaseError("leased by another worker")
        )

        queue = _Queue()
        with caplog.at_level(logging.WARNING, logger="plaita.server.flow_worker"):
            worker._consume_loop(queue)

        assert queue.acked == ["1-0"], "既有语义（#50）不变：冲突时 ack 释放"
        assert "本次 resume 未生效" in caplog.text, (
            "resume 被 ack 丢弃必须留 WARNING 明说未生效（否则值守分不清白等）"
        )
