"""plaita#234 回归：start 载体重投覆写 checkpoint 并整条 flow 重放。

基线缺陷（P1）：``start_flow`` 完全不看盘上状态——固定构造 ``context={}`` 的
新 ``ExecutionState`` 并**无条件覆写**盘上 checkpoint，然后
``run_distributed(flow, params=params, execution_id=...)`` **不传
``saved_context``** ⇒ 整条 flow 从首节点重跑：

- 已执行节点的外部副作用（发消息/下单/写库/调外部 API）最多重放
  ``max_deliveries`` 次；
- 原 checkpoint（``$LAST_NODE`` / ``$NODE`` / ``$INPUT``）被永久抹成 ``{}``，
  此后连正确的 event/continue resume 也失去续跑现场。

触发面是 at-least-once 的**正常路径**：worker 崩溃/滚动重启、``StatePersistError``
（设计上就要不 ack 重投）、``ServiceDispatchError``（#233）、兜底异常后，XCLAIM
回收/死信重入队拿到的都是**同一条原始 start body**——它不经 #91 的
``_requeue_body_for_retry``。

修复契约（本测试钉住）：

- start 载体发现盘上已有断点 ⇒ **转 resume 语义**（决策表）：
  running + 非空 checkpoint → 从断点续跑（``saved_context`` = 原 checkpoint）；
  suspended → #33 幂等短路 + #233 补派，不重跑挂起节点；
  终态 → 幂等回报现状，不重跑；
- 落 running 行之前**不得**用 ``context={}`` 覆写已有 checkpoint；
- 首投（盘上无行）与「running + 空 checkpoint」窗口行为逐字不变；
- flow_id 不一致 → 按首投处理（转 resume 会被流程 ID 校验打成 poison ack）。
"""
import json
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.server.flow_worker import RedisFlowWorker
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

TEST_FLOW = {
    "flow_id": "f1",
    "version": "1",
    "nodes": [
        {"id": "start", "type": "start", "next": "a"},
        {"id": "a", "type": "assignment", "result": {"step": 1}, "next": "b"},
        {"id": "b", "type": "assignment", "result": {"step": 2}, "next": "end"},
        {"id": "end", "type": "end", "output": "ok"},
    ],
}
EXEC = "exec-replay"
CHECKPOINT = {
    "$LAST_NODE": "b",
    "$NODE": {"a": {"step": 1}, "b": {"step": 2}},
    "$INPUT": {"order_id": "O-1"},
}


def _worker():
    fake = fakeredis.FakeRedis(decode_responses=True)
    storage = MemoryExecutionStorage()
    flow_storage = MemoryFlowStorage()
    flow_storage.save_flow(TEST_FLOW)
    worker = RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:issue234-start-replay",
        execution_storage=storage,
        flow_storage=flow_storage,
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
    )
    return worker, storage


def _seed(storage, status="running", context=None, flow_id="f1", execution_id=EXEC):
    storage.save_execution_state(
        execution_id,
        ExecutionState(
            execution_id=execution_id,
            flow_id=flow_id,
            flow_version="1",
            status=status,
            context=dict(CHECKPOINT) if context is None else context,
        ),
    )


def _capture_run(worker, storage, execution_id=EXEC):
    """patch 执行器：记录 run_distributed 的 kwargs 与**调用瞬间**的盘上 context。"""
    captured = {}

    def fake_run_distributed(flow, params=None, execution_id=None, **kwargs):
        snapshot = storage.load_execution_state(execution_id)
        captured["context_at_call"] = dict(snapshot.context) if snapshot else None
        captured["kwargs"] = dict(kwargs)
        captured["params"] = params
        captured["flow"] = flow
        captured["execution_id"] = execution_id
        return {
            "execution_id": execution_id,
            "is_end": True,
            "context": {"$LAST_NODE": "end", "$NODE": {"a": {}, "b": {}}},
        }

    patcher = patch("plaita.server.flow_worker.FlowExecution")
    FE = patcher.start()
    FE.return_value.run_distributed.side_effect = fake_run_distributed
    return patcher, FE, captured


class TestStartRedeliveryResumesFromCheckpoint:
    """盘上 running + 非空 checkpoint：start 重投必须从断点续跑。"""

    def test_uses_saved_checkpoint_and_does_not_overwrite(self):
        worker, storage = _worker()
        _seed(storage, status="running", context=dict(CHECKPOINT))
        patcher, FE, captured = _capture_run(worker, storage)
        try:
            out = worker.start_flow(
                "f1", {"order_id": "O-1"}, version="1", execution_id=EXEC
            )
        finally:
            patcher.stop()

        assert captured["kwargs"].get("saved_context") == CHECKPOINT, (
            "start 重投必须把盘上 checkpoint 作为 saved_context 传回引擎（否则整条重放）"
        )
        assert captured["context_at_call"] == CHECKPOINT, (
            "落 running 行/开跑之前不得用 context={} 覆写盘上 checkpoint"
        )
        assert out.get("is_end") is True
        assert FE.return_value.run_distributed.call_count == 1

    def test_resume_type_is_continue(self):
        """载体转换走 resume 语义（不是又一次 start）。"""
        worker, storage = _worker()
        _seed(storage, status="running")
        patcher, FE, captured = _capture_run(worker, storage)
        try:
            worker.start_flow("f1", {}, version="1", execution_id=EXEC)
        finally:
            patcher.stop()
        assert captured["kwargs"].get("resume_type") == "continue"

    def test_checkpoint_survives_until_engine_call(self):
        """验收 (b) 的直接断言：盘上 $NODE keys 在调用瞬间仍然完整。"""
        worker, storage = _worker()
        _seed(storage, status="running")
        patcher, FE, captured = _capture_run(worker, storage)
        try:
            worker.start_flow("f1", {}, version="1", execution_id=EXEC)
        finally:
            patcher.stop()
        assert set(captured["context_at_call"]["$NODE"]) == {"a", "b"}
        assert captured["context_at_call"]["$INPUT"] == {"order_id": "O-1"}


class TestStartRedeliveryTerminalAndSuspended:
    """终态/挂起：start 载体幂等回报现状，绝不重跑（重放会覆写终态行）。"""

    @pytest.mark.parametrize("status", ["completed", "error", "cancelled"])
    def test_terminal_row_is_idempotent(self, status):
        worker, storage = _worker()
        _seed(storage, status=status)
        patcher, FE, captured = _capture_run(worker, storage)
        try:
            out = worker.start_flow("f1", {}, version="1", execution_id=EXEC)
        finally:
            patcher.stop()

        FE.return_value.run_distributed.assert_not_called()
        assert out["already_terminal"] is True
        assert out["status"] == status
        assert storage.load_execution_state(EXEC).status == status

    def test_suspended_row_is_not_replayed(self):
        worker, storage = _worker()
        _seed(storage, status="suspended")
        patcher, FE, captured = _capture_run(worker, storage)
        try:
            out = worker.start_flow("f1", {}, version="1", execution_id=EXEC)
        finally:
            patcher.stop()

        FE.return_value.run_distributed.assert_not_called()
        assert out["already_suspended"] is True
        state = storage.load_execution_state(EXEC)
        assert state.status == "suspended"
        assert state.context == CHECKPOINT, "挂起 checkpoint 不得被 start 载体覆写"


class TestFirstDeliveryUnchanged:
    """首投（盘上无行）与「running + 空 checkpoint」窗口：行为逐字不变。"""

    def test_first_delivery_does_not_pass_saved_context(self):
        worker, storage = _worker()
        patcher, FE, captured = _capture_run(worker, storage)
        try:
            out = worker.start_flow("f1", {"order_id": "O-1"}, version="1")
        finally:
            patcher.stop()

        assert "saved_context" not in captured["kwargs"]
        assert captured["params"] == {"order_id": "O-1"}
        assert out.get("is_end") is True
        new_id = out["execution_id"]
        assert storage.load_execution_state(new_id).status == "completed"

    def test_running_row_with_empty_checkpoint_keeps_fresh_start(self):
        """start→resume 转换的空 checkpoint 窗口（首节点尚未成功过）不变。"""
        worker, storage = _worker()
        _seed(storage, status="running", context={})
        patcher, FE, captured = _capture_run(worker, storage)
        try:
            worker.start_flow("f1", {"order_id": "O-1"}, version="1", execution_id=EXEC)
        finally:
            patcher.stop()

        assert "saved_context" not in captured["kwargs"]
        assert captured["execution_id"] == EXEC

    def test_flow_id_mismatch_falls_back_to_fresh_start(self):
        """盘上 flow_id 不一致：不转 resume（那会被流程ID校验打成 poison ack）。"""
        worker, storage = _worker()
        _seed(storage, status="running", flow_id="other-flow")
        patcher, FE, captured = _capture_run(worker, storage)
        try:
            worker.start_flow("f1", {}, version="1", execution_id=EXEC)
        finally:
            patcher.stop()

        assert "saved_context" not in captured["kwargs"]
        assert FE.return_value.run_distributed.call_count == 1

    def test_no_existing_row_created_at_start(self):
        """首投仍先落 running 行（P0 可见性契约不退化）。"""
        worker, storage = _worker()
        storage.save_execution_state  # noqa: B018 —— 只是确认 storage 可用
        assert storage.load_execution_state(EXEC) is None
        patcher, FE, captured = _capture_run(worker, storage)
        try:
            worker.start_flow("f1", {}, version="1", execution_id=EXEC)
        finally:
            patcher.stop()
        assert storage.load_execution_state(EXEC) is not None


class TestStartCarrierCompensationTogether:
    """#234 × #233 组合：start 转 resume 后不得又被 #33 幂等短路 ack 掉补派。

    这是两单的接缝：start 载体若只「转 resume」而 resume 的 suspended 短路
    仍什么都不做，落盘凭据照样没人处理——所以 start 转 resume 必须与
    resume 的挂起补派共用同一入口（本断言钉死）。
    """

    def test_start_carrier_without_dedup_compensates_via_resume(self):
        worker, storage = _worker()
        storage.save_execution_state(
            EXEC,
            ExecutionState(
                execution_id=EXEC,
                flow_id="f1",
                flow_version="1",
                status="suspended",
                context=dict(CHECKPOINT),
                dispatch_pending={
                    "pending": True,
                    "service_type": "delay",
                    "service_config": {"type": "delay", "delay_ms": 1000},
                    "node_id": "b",
                    "recorded_at": "2026-10-18T00:00:00",
                },
            ),
        )
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            result = worker.start_flow("f1", {}, version="1", execution_id=EXEC)

        FE.return_value.run_distributed.assert_not_called()
        assert result["already_suspended"] is True
        assert result["dispatch_compensated"] is True
        queued = [
            json.loads(item)
            for item in worker.redis_client.lrange("plaita:delay:queue", 0, -1)
        ]
        assert len(queued) == 1
        state = storage.load_execution_state(EXEC)
        assert state.status == "suspended"
        assert state.context == CHECKPOINT, "start 载体不得覆写挂起 checkpoint"
