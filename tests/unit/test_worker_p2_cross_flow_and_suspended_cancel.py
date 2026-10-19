"""worker P2 批（台账第三十一轮 V1 + 第三十四轮 ④）回归。

① cross-flow execution_id 覆写守卫（第三十一轮 V1）
   ``start_flow(flow_id=B, execution_id=X)`` 遇到盘上 X 属于 flow A 时，旧
   行为把 flow_id 不一致当「首投」，新建 ``context={}`` 的 B running 行
   **覆写 X**——A 的 checkpoint/状态永久丢失。修复契约：具名
   ``ExecutionIdConflictError`` 拒绝，不落任何行、A 原样保留；异常继承
   ``ValueError``，run() 按畸形载体 poison ack（换 flow 重投不会自愈）。

② suspended 早退路径补取消检查（第三十四轮 ④）
   suspended + continue/retry 在 ``_load_and_guard_resume_state`` 的幂等
   短路处早退，早于 resume 入口的取消检查。若挂起前已写下取消标志键，消息
   被 ack、盘上保持 suspended、flag 仍在——独立 cancel 载体丢失时执行带着
   取消意图长期挂起。修复契约：早退分支命中 ``_cancel_requested`` 即不短路，
   转正常路径（取租约 → 复核 → 取消终态化）；无取消意图时 #33 的
   「suspended + continue 不取租约」契约逐字不变。

负控：
- 还原 ①（flow_id 不一致 return None）→ ``CrossFlowExecutionIdGuard`` 红：
  start 返回、盘上变成 fB/completed、A checkpoint 丢失；
- 还原 ②（早退分支不查 flag）→ ``SuspendedCancelEarlyReturn`` 红：resume
  返回 ``already_suspended``、盘上仍 suspended、无 end_time。
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.server.flow_worker import ExecutionIdConflictError, RedisFlowWorker
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

FLOW_A = {
    "flow_id": "fA",
    "version": "1",
    "nodes": [
        {"id": "start", "type": "start", "next": "end"},
        {"id": "end", "type": "end", "output": "A"},
    ],
}
FLOW_B = {
    "flow_id": "fB",
    "version": "1",
    "nodes": [
        {"id": "start", "type": "start", "next": "end"},
        {"id": "end", "type": "end", "output": "B"},
    ],
}
EXEC = "exec-worker-p2"
CHECKPOINT = {"$LAST_NODE": "wait", "$NODE": {"x": 1}, "$INPUT": {"order": "O-1"}}
CANCEL_KEY = f"plaita:execution:cancel:{EXEC}"
LEASE_KEY = f"plaita:execution:lease:{EXEC}"


def _worker():
    fake = fakeredis.FakeRedis(decode_responses=True)
    storage = MemoryExecutionStorage()
    flow_storage = MemoryFlowStorage()
    flow_storage.save_flow(FLOW_A)
    flow_storage.save_flow(FLOW_B)
    worker = RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:worker-p2",
        execution_storage=storage,
        flow_storage=flow_storage,
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
    )
    return worker, storage, fake


def _seed(storage, *, flow_id="fA", status="suspended", context=None):
    storage.save_execution_state(
        EXEC,
        ExecutionState(
            execution_id=EXEC,
            flow_id=flow_id,
            flow_version="1",
            status=status,
            context=dict(CHECKPOINT) if context is None else context,
        ),
    )


class TestCrossFlowExecutionIdGuard:
    """① 同 execution_id 跨 flow：拒绝且不覆写原状态。"""

    def test_rejected_and_original_checkpoint_preserved(self):
        worker, storage, _ = _worker()
        _seed(storage, flow_id="fA", status="suspended")

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            with pytest.raises(ExecutionIdConflictError) as excinfo:
                worker.start_flow("fB", {}, version="1", execution_id=EXEC)

        # 引擎绝不启动（旧实现会跑完 B 并覆写 X）
        FE.return_value.run_distributed.assert_not_called()
        message = str(excinfo.value)
        assert EXEC in message and "fA" in message and "fB" in message

        state = storage.load_execution_state(EXEC)
        assert state.flow_id == "fA"
        assert state.status == "suspended"
        assert state.context == CHECKPOINT, (
            "A 的 checkpoint 被 B 的 context={} 覆写了（负控主判据）"
        )

    def test_conflict_is_valueerror_for_poison_ack(self):
        """换 flow 重投不会自愈：必须落到 run() 的 ValueError poison ack 分支。"""
        assert issubclass(ExecutionIdConflictError, ValueError)

    def test_newly_claimed_dedup_key_is_released(self):
        """本次认领的幂等映射不得指向另一条 flow 的 execution_id。"""
        worker, storage, fake = _worker()
        _seed(storage, flow_id="fA", status="suspended")
        dedup_key = "dedup-cross-flow"
        mapping_key = f"plaita:start-dedup:{dedup_key}"

        with patch("plaita.server.flow_worker.FlowExecution"):
            with pytest.raises(ExecutionIdConflictError):
                worker.start_flow(
                    "fB", {}, version="1", execution_id=EXEC,
                    dedup_key=dedup_key,
                )

        assert fake.get(mapping_key) is None, (
            "冲突 start 认领的 dedup 映射必须释放，否则 7 天内同 key 指向别家执行"
        )

    def test_same_flow_start_carrier_still_converts_to_resume(self):
        """不过度收紧：flow_id 一致时仍走既有 start→resume 载体转换。"""
        worker, storage, _ = _worker()
        _seed(storage, flow_id="fA", status="suspended")

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            out = worker.start_flow("fA", {}, version="1", execution_id=EXEC)

        FE.return_value.run_distributed.assert_not_called()
        assert out["already_suspended"] is True
        assert storage.load_execution_state(EXEC).context == CHECKPOINT


class TestSuspendedCancelEarlyReturn:
    """② suspended 早退路径必须先看取消意图。"""

    @pytest.mark.parametrize("resume_type", ["continue", "retry"])
    def test_cancel_flag_terminalizes_instead_of_early_return(self, resume_type):
        worker, storage, fake = _worker()
        _seed(storage, flow_id="fA", status="suspended")
        fake.set(CANCEL_KEY, "2026-10-19T00:00:00", ex=7 * 86400)

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            out = worker.resume_flow("fA", EXEC, resume_type)

        FE.return_value.run_distributed.assert_not_called()
        assert out["status"] == "cancelled"
        assert out["cancelled_at_resume"] is True
        assert "already_suspended" not in out

        state = storage.load_execution_state(EXEC)
        assert state.status == "cancelled", (
            "取消意图被挂起幂等早退吞掉（负控主判据：旧实现仍 suspended）"
        )
        assert state.end_time is not None
        assert state.context == CHECKPOINT, "取消终态化必须保留取消点前 checkpoint"
        assert fake.get(LEASE_KEY) is None, "取消终态化后租约必须释放"

    def test_no_cancel_flag_keeps_33_early_return_without_lease(self):
        """#33 契约不变：无取消意图的 suspended+continue 仍不取租约。"""
        worker, storage, fake = _worker()
        _seed(storage, flow_id="fA", status="suspended")

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            out = worker.resume_flow("fA", EXEC, "continue")

        FE.return_value.run_distributed.assert_not_called()
        assert out["already_suspended"] is True
        assert out["status"] == "suspended"
        assert storage.load_execution_state(EXEC).status == "suspended"
        assert fake.get(LEASE_KEY) is None, "suspended+continue 不得取租约（#33）"

    def test_start_carrier_with_cancel_flag_also_terminalizes(self):
        """start 重投载体转 resume 后同样吃取消意图，不只 continue 直投路径。"""
        worker, storage, fake = _worker()
        _seed(storage, flow_id="fA", status="suspended")
        fake.set(CANCEL_KEY, "2026-10-19T00:00:00", ex=7 * 86400)

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            out = worker.start_flow("fA", {}, version="1", execution_id=EXEC)

        FE.return_value.run_distributed.assert_not_called()
        assert out["status"] == "cancelled"
        assert storage.load_execution_state(EXEC).status == "cancelled"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
