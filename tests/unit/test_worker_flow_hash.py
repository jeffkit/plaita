"""resume 的 flow 定义指纹校验（2026-10 第二波 Track P1 任务②）。

基线缺陷：worker 流程定义 TTLCache 300s + console engine_sync 可直接覆盖
Redis 定义。挂起执行 resume 时用 latest 版本定义，若定义自启动后被改，
``_get_next_from_last`` 找不到节点/走错分支，**无任何告警**。

修复契约：
- ``ExecutionState`` 新增可选字段 ``flow_hash``（storage/base.py）：start 时
  对**存储中的原始流程定义**（``model_validate`` 之前）计算指纹
  （sha256 of sort_keys 规范化 JSON）写入；
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

plaita#36（口径 v2）：指纹从「解析后 ``Flow.model_dump``」改为「原始定义
JSON」。理由与守卫见 ``TestFlowHashStableAcrossEngineVersions``——对解析后的
模型求指纹会随模型字段演进漂移，滚动升级窗口把在途 run 批量终态化；本文件
的稳定性守卫必须先能红（旧口径下同一存储定义、不同引擎模型 → 指纹不同），
再在本口径下恒等。
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
from plaita.server.flow_worker import (
    FLOW_HASH_ALGO,
    FLOW_HASH_ALGO_V1,
    FlowWorker,
    RedisFlowWorker,
)
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
    """口径 v2（plaita#36）：**原始定义** JSON 的 sha256，不经 ``Flow`` 解析。

    刻意独立于产品实现重写一遍（而不是调 ``compute_flow_definition_hash``）：
    测试要能独立证伪产品实现，不能与实现同源。
    """
    return hashlib.sha256(
        json.dumps(
            flow_def,
            sort_keys=True,
            ensure_ascii=False,
            default=str,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _v1_hash(flow_def: dict) -> str:
    """口径 v1（旧实现）：解析后 ``model_dump(mode="json")`` 的 sha256。

    只用于构造「老 worker 写下的状态」——滚动升级窗口里 stored_algo=v1 +
    v1 指纹的执行必须被判成 ``upgrade_suspected``，而不是当成定义变更。
    """
    return hashlib.sha256(
        json.dumps(
            Flow.model_validate(flow_def).model_dump(mode="json"),
            sort_keys=True,
            ensure_ascii=False,
            default=str,
        ).encode("utf-8")
    ).hexdigest()


class _EvolvedFlow(Flow):
    """模拟「新版本引擎」：给 ``Flow`` 增加一个带默认值的序列化字段。

    真实版本演进（给模型增删字段、物化默认值）就是 plaita#36 的漂移源：
    同一份存储定义在新旧引擎下 ``model_dump`` 形态不同。
    """

    added_in_new_version: str = "default-value"


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
        """start_flow 落盘的 state.flow_hash == 存储原始定义的指纹（口径 v2）。"""
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
        assert state.flow_hash_algo == FLOW_HASH_ALGO

    def test_hash_changes_when_definition_changes(self):
        """定义变更 → 指纹变化（校验能感知改定义）。"""
        assert _expected_hash(TEST_FLOW) != _expected_hash(MODIFIED_FLOW)

    def test_hash_deterministic_across_computations(self):
        """同一定义多次计算指纹稳定（跨进程可比）。"""
        assert _expected_hash(TEST_FLOW) == _expected_hash(TEST_FLOW)


# ---------- plaita#36：指纹跨引擎版本稳定 ----------


class TestFlowHashStableAcrossEngineVersions:
    """口径 v2 的稳定性守卫。

    「假守卫」判定标准（与 tests/unit/test_flow_worker_scenarios.py 同款，
    写在此处供后人照抄）：一条守卫在满足任一条时即为**假守卫**——
    ① 零断言（只证明「没抛异常」）；
    ② 断言不可达（写在 ``try`` 里被同函数的 ``except`` 吞掉）；
    ③ 断言的对象无法区分名字承诺的行为（如只断言 ``is not None``）。
    真守卫必须**能红**：改坏被守卫的实现（变异）后必须失败。
    """

    def _start_and_read_hash(self, fake, flow_storage, execution_id):
        storage = MemoryExecutionStorage()
        worker = _redis_worker(fake, storage, flow_storage)
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.execution_id = execution_id
            inst.run_distributed.return_value = {
                "execution_id": execution_id,
                "is_end": True,
                "context": {"done": True},
            }
            worker.start_flow("f1", {}, version="1")
        return storage.load_execution_state(execution_id).flow_hash

    def test_hash_stable_when_engine_model_gains_field(self, monkeypatch):
        """同一存储定义、不同引擎模型（版本演进加字段）→ 指纹必须不变。

        变异点：把 ``_compute_flow_hash`` 改回 ``flow.model_dump(...)``（v1
        口径）→ 本用例立刻红（两次 start 的指纹不同，且与 #36 线上形态一致）。
        """
        import plaita.server.flow_worker as fw

        fake = fakeredis.FakeRedis(decode_responses=True)
        flow_storage = MemoryFlowStorage()
        flow_storage.save_flow(TEST_FLOW)

        hash_old_engine = self._start_and_read_hash(
            fake, flow_storage, "exec-old-engine"
        )

        # 「滚动升级」：另一台 worker 跑的是给 Flow 加过字段的新引擎版本，
        # 读同一份存储定义。
        monkeypatch.setattr(fw, "Flow", _EvolvedFlow)
        hash_new_engine = self._start_and_read_hash(
            fake, flow_storage, "exec-new-engine"
        )

        assert hash_new_engine == hash_old_engine, (
            "同一份存储定义在不同引擎模型下指纹漂移——滚动升级会把在途 run "
            "批量终态化（plaita#36）"
        )

    def test_compute_hash_rejects_parsed_model(self):
        """指纹入口只接受**原始定义**；传解析后的 ``Flow`` 必须响亮 TypeError。

        变异点：删掉 ``_compute_flow_hash`` 的入口类型守卫 → 本用例红
        （旧口径会静默接受 ``Flow`` 对象，正是漂移入口）。
        """
        worker = _base_worker()
        with pytest.raises(TypeError, match="原始流程定义"):
            worker._compute_flow_hash(Flow.model_validate(TEST_FLOW))

    def test_legacy_v1_state_is_classified_as_upgrade_suspected(self):
        """老 worker（v1 口径）写下的状态：哈希不同但必须判成「疑似升级」并
        给出 ``allow_flow_hash_change`` 显式放行提示，而不是含糊的「定义变更」。

        变异点：``FLOW_HASH_ALGO`` 不 bump（退回 v1 标签）→ 本用例红
        （``upgrade_suspected`` 为 False、``hint`` 为 None）。
        """
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        flow_storage = MemoryFlowStorage()
        flow_storage.save_flow(TEST_FLOW)
        worker = _redis_worker(fake, storage, flow_storage)
        storage.save_execution_state(
            "exec-legacy",
            _state(
                execution_id="exec-legacy",
                flow_hash=_v1_hash(TEST_FLOW),
                flow_hash_algo=FLOW_HASH_ALGO_V1,
            ),
        )

        with pytest.raises(ValueError, match="hash 不匹配"):
            worker.resume_flow("f1", "exec-legacy", "continue")

        state = storage.load_execution_state("exec-legacy")
        assert state.status == "error"
        assert state.error["upgrade_suspected"] is True
        assert "allow_flow_hash_change" in state.error["hint"]
        assert state.error["stored_flow_hash_algo"] == FLOW_HASH_ALGO_V1
        assert state.error["current_flow_hash_algo"] == FLOW_HASH_ALGO

    def test_negative_control_genuine_change_has_no_upgrade_hint(self):
        """负向对照：**真改定义**（算法标记不变）必须仍判 mismatch，且不得打上
        ``upgrade_suspected`` / 给放行提示——口径稳定化不能把守卫变成「永远放行」。
        """
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        flow_storage = MemoryFlowStorage()
        flow_storage.save_flow(MODIFIED_FLOW)
        worker = _redis_worker(fake, storage, flow_storage)
        storage.save_execution_state(
            "exec-changed",
            _state(
                execution_id="exec-changed",
                flow_hash=_expected_hash(TEST_FLOW),
                flow_hash_algo=FLOW_HASH_ALGO,
            ),
        )

        with pytest.raises(ValueError, match="hash 不匹配"):
            worker.resume_flow("f1", "exec-changed", "continue")

        state = storage.load_execution_state("exec-changed")
        assert state.status == "error"
        assert state.error["upgrade_suspected"] is False
        assert state.error["hint"] is None
        assert state.error["stored_flow_hash"] == _expected_hash(TEST_FLOW)
        assert state.error["current_flow_hash"] == _expected_hash(MODIFIED_FLOW)


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
        # plaita#93：本处终态化不由异常驱动，用引擎通用 flow 级错误码 + worker
        # 级判据名给出机器可判别身份（下游不必解析中文 message 分类）。
        assert state.error["code"] == -500
        assert state.error["error_type"] == "flow_hash_mismatch"

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
