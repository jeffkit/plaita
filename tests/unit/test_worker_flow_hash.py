"""resume 的 flow 定义指纹校验（2026-10 第二波 Track P1 任务②）。

基线缺陷：worker 流程定义 TTLCache 300s + console engine_sync 可直接覆盖
Redis 定义。挂起执行 resume 时用 latest 版本定义，若定义自启动后被改，
``_get_next_from_last`` 找不到节点/走错分支，**无任何告警**。

修复契约：
- ``ExecutionState`` 新增可选字段 ``flow_hash``（storage/base.py）：start 时
  对实际加载执行的 Flow 计算指纹（sha256 of ``model_dump(mode="json")`` +
  sort_keys 规范化 JSON）写入；
- resume_flow 在取得租约后（与活 worker 的推进写串行化，防误终态化他人
  正在推进的执行）比对指纹：不一致 → 终态化 error（message 写明「flow
  定义自启动后已变更，hash 不匹配，执行无法安全续跑」）+ raise ValueError
  → run() poison ack。**故意选可观测的终态而不是重投风暴**：定义被改是
  确定性不一致，重投 N 次结果相同，只会制造 DLQ 噪音；
- 老状态缺字段 → None → 跳过校验（零回归）；
- 各后端 round-trip：memory / redis（model_dump/model_validate 全量透传）、
  fenced（save 序列化 + load 透传）自动兼容；sqlalchemy 显式列需补
  （``**state_dict`` 展开进模型构造器，缺列会 TypeError → save False →
  StatePersistError）。
"""
import hashlib
import json
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.core.flow import Flow
from plaita.server.flow_worker import FlowWorker, RedisFlowWorker
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage


TEST_FLOW = {
    "flow_id": "f1",
    "version": "1",
    "nodes": [
        {"id": "start", "type": "start", "next": "end"},
        {"id": "end", "type": "end", "output": "ok"},
    ],
}

MODIFIED_FLOW = {
    "flow_id": "f1",
    "version": "1",
    "nodes": [
        {"id": "start", "type": "start", "next": "b"},
        {"id": "b", "type": "end", "output": "changed"},
    ],
}


def _expected_hash(flow_def: dict) -> str:
    return hashlib.sha256(
        json.dumps(
            Flow.model_validate(flow_def).model_dump(mode="json"),
            sort_keys=True,
            ensure_ascii=False,
            default=str,
        ).encode("utf-8")
    ).hexdigest()


def _base_worker(storage=None, flow_storage=None) -> FlowWorker:
    return FlowWorker(
        execution_storage=storage if storage is not None else MemoryExecutionStorage(),
        flow_storage=flow_storage if flow_storage is not None else MemoryFlowStorage(),
    )


def _redis_worker(fake, storage=None, flow_storage=None) -> RedisFlowWorker:
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:worker-flow-hash",
        execution_storage=storage if storage is not None else MemoryExecutionStorage(),
        flow_storage=flow_storage if flow_storage is not None else MemoryFlowStorage(),
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
    )


def _state(execution_id="exec-1", status="running", **kwargs) -> ExecutionState:
    # running 默认态：resume+continue 走完整校验/推进路径（#33 起 suspended+
    # continue 在校验之前被挂起幂等短路，测不到指纹校验）。
    return ExecutionState(
        execution_id=execution_id,
        flow_id="f1",
        flow_version="1",
        status=status,
        context={"$LAST_NODE": "start", "$NODE": {}},
        **kwargs,
    )


# ---------- ExecutionState 字段与后端 round-trip ----------


class TestFlowHashRoundTrip:
    def test_field_defaults_to_none(self):
        """新字段可选且默认 None（老构造点零改动）。"""
        state = ExecutionState(execution_id="e", context={})
        assert state.flow_hash is None

    def test_memory_backend_round_trip(self):
        storage = MemoryExecutionStorage()
        storage.save_execution_state("e", _state(flow_hash="abc123"))
        assert storage.load_execution_state("e").flow_hash == "abc123"

    def test_redis_backend_round_trip(self):
        """redis 后端 model_dump/model_validate 全量透传新字段。"""
        from plaita.storage.redis import RedisExecutionStorage

        storage = RedisExecutionStorage(
            client=fakeredis.FakeRedis(decode_responses=True)
        )
        storage.save_execution_state("e", _state(flow_hash="abc123"))
        assert storage.load_execution_state("e").flow_hash == "abc123"

    def test_redis_backend_old_state_without_field_loads_none(self):
        """老状态（序列化时无 flow_hash 键）→ model_validate 补 None → 跳过校验。"""
        from plaita.storage.redis import RedisExecutionStorage

        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = RedisExecutionStorage(client=fake)
        old = _state().model_dump()
        old.pop("flow_hash")
        fake.set("plaita:execution:e", json.dumps(old))
        loaded = storage.load_execution_state("e")
        assert loaded.flow_hash is None

    def test_fenced_wrapper_preserves_field(self):
        """fenced 包装器（未持 fence token → 普通写路径）保存/加载保留字段。"""
        from plaita.storage.fenced import FencedExecutionStorage

        inner = MemoryExecutionStorage()
        storage = FencedExecutionStorage(inner)
        storage.save_execution_state("e", _state(flow_hash="abc123"))
        assert storage.load_execution_state("e").flow_hash == "abc123"

    def test_sqlalchemy_model_has_column(self):
        """sqlalchemy 后端显式列必须存在：save 走 ``**state_dict`` 展开，
        缺列会在 INSERT 时 TypeError → save False → StatePersistError。

        只测 flow_hash 自身的兼容（列存在 + 构造可接受）：该后端 INSERT
        路径本就有两个先存 bug（execution_id 重复传参、tenant_id 缺列，
        均与本任务无关、后端已下架），不在本任务修。
        """
        pytest.importorskip("sqlalchemy")
        from plaita.storage.sqlalchemy import ExecutionStateModel

        assert hasattr(ExecutionStateModel, "flow_hash")
        model = ExecutionStateModel(
            execution_id="e", flow_id="f1", context={}, status="running",
            flow_hash="abc123",
        )
        assert model.flow_hash == "abc123"



# ---------- start 写入指纹 ----------


class TestStartStoresFlowHash:
    def test_start_flow_persists_hash_of_executed_flow(self):
        """start_flow 落盘的 state.flow_hash == 实际执行 Flow 的指纹。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        flow_storage = MemoryFlowStorage()
        flow_storage.save_flow(TEST_FLOW)
        worker = _redis_worker(fake, storage, flow_storage)

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.execution_id = "exec-new"
            inst.run_distributed.return_value = {
                "execution_id": "exec-new",
                "is_end": True,
                "context": {"done": True},
            }
            worker.start_flow("f1", {}, version="1")

        state = storage.load_execution_state("exec-new")
        assert state.flow_hash == _expected_hash(TEST_FLOW)

    def test_hash_changes_when_definition_changes(self):
        """定义变更 → 指纹变化（校验能感知改定义）。"""
        assert _expected_hash(TEST_FLOW) != _expected_hash(MODIFIED_FLOW)

    def test_hash_deterministic_across_validations(self):
        """同一定义多次 validate/dump 指纹稳定（跨进程可比）。"""
        assert _expected_hash(TEST_FLOW) == _expected_hash(TEST_FLOW)


# ---------- resume 校验 ----------


class TestResumeFlowHashEnforcement:
    def _worker_with_state(self, fake, flow_def, state):
        storage = MemoryExecutionStorage()
        flow_storage = MemoryFlowStorage()
        flow_storage.save_flow(flow_def)
        worker = _redis_worker(fake, storage, flow_storage)
        storage.save_execution_state(state.execution_id, state)
        return worker, storage

    def test_hash_mismatch_terminalizes_error_and_raises_value_error(self):
        """指纹不一致 → 终态化 error（message 写明变更）+ ValueError（poison ack）。

        选可观测终态而非重投风暴：定义被改是确定性不一致，重投 N 次结果
        相同，重投只会制造 DLQ 噪音与延迟。
        """
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker, storage = self._worker_with_state(
            fake, MODIFIED_FLOW, _state(flow_hash=_expected_hash(TEST_FLOW))
        )
        with pytest.raises(ValueError, match="hash 不匹配"):
            worker.resume_flow("f1", "exec-1", "continue")
        state = storage.load_execution_state("exec-1")
        assert state.status == "error"
        assert "已变更" in state.error["message"]
        assert state.error["stored_flow_hash"] == _expected_hash(TEST_FLOW)
        assert state.error["current_flow_hash"] == _expected_hash(MODIFIED_FLOW)

    def test_hash_match_proceeds(self):
        """指纹一致 → 正常推进（不被误伤）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker, storage = self._worker_with_state(
            fake, TEST_FLOW, _state(flow_hash=_expected_hash(TEST_FLOW))
        )
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.run_distributed.return_value = {
                "execution_id": "exec-1",
                "is_end": True,
                "context": {"done": True},
            }
            result = worker.resume_flow("f1", "exec-1", "continue")
        assert result["is_end"] is True
        assert storage.load_execution_state("exec-1").status == "completed"

    def test_missing_hash_skips_validation(self):
        """老状态 flow_hash=None → 跳过校验（升级期零回归）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker, storage = self._worker_with_state(
            fake, MODIFIED_FLOW, _state(flow_hash=None)
        )
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.run_distributed.return_value = {
                "execution_id": "exec-1",
                "is_end": True,
                "context": {"done": True},
            }
            worker.resume_flow("f1", "exec-1", "continue")
        assert storage.load_execution_state("exec-1").status == "completed"

    def test_mismatch_persist_failure_propagates_state_persist_error(self):
        """指纹不一致但终态化写盘失败 → StatePersistError 冒出（消息不 ack
        走重投），不被当成校验成功继续推进。"""
        fake = fakeredis.FakeRedis(decode_responses=True)

        class FalseSaveStorage(MemoryExecutionStorage):
            def save_execution_state(self, execution_id, state) -> bool:
                super().save_execution_state(execution_id, state)
                return False

        storage = FalseSaveStorage()
        flow_storage = MemoryFlowStorage()
        flow_storage.save_flow(MODIFIED_FLOW)
        worker = _redis_worker(fake, storage, flow_storage)
        storage.save_execution_state(
            "exec-1", _state(flow_hash=_expected_hash(TEST_FLOW))
        )
        from plaita.server.flow_worker import StatePersistError

        with pytest.raises(StatePersistError):
            worker.resume_flow("f1", "exec-1", "continue")
