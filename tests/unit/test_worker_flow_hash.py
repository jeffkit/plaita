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

#36 改口径（2026-10-10）：指纹从「解析后 ``model_dump``」改成**原始存储定义
JSON**。旧口径把引擎的序列化字段集当成定义的一部分——引擎任一次增删序列化
字段、pydantic 升版都会让同一份存储定义算出不同指纹，混部舰队（滚升窗口 /
``PLAITA_PYTHON`` 指向未同步 venv）下在途 run 被批量终态化。契约变为：

- 同一份存储定义在任何引擎版本下同指纹（``FLOW_HASH_ALGO`` = raw-json-v2）；
- 同口径下失配 = 定义真被改 → 仍是终态化 error，``category`` =
  ``flow_definition_changed``（engine_version 也变了只是佐证，不冒充成因），
  提示明说 ``allow_flow_hash_change`` 对同口径失配无效；
- 口径标记不同的失配 = 状态由别的构建/口径写下（混部/滚升）→ 终态化 error，
  ``category`` = ``engine_version_drift`` + 「可显式放行」的可执行提示；
- 存量状态（旧口径标记或完全没有标记）与当前口径**不可互比** → 一次性重基线
  放行（WARNING + ``plaita_resume_guard_total``），不终态化。
"""
import hashlib
import json
import threading
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.core.flow import Flow
from plaita.server.flow_worker import (
    ALLOW_FLOW_HASH_CHANGE_KEY,
    FLOW_HASH_ALGO,
    LEGACY_FLOW_HASH_ALGO,
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
    """当前口径：**原始存储定义** JSON 的 sha256（#36）。"""
    return hashlib.sha256(
        json.dumps(flow_def, sort_keys=True, ensure_ascii=False, default=str).encode("utf-8")
    ).hexdigest()


def _legacy_hash(flow_def: dict) -> str:
    """旧口径：解析后 ``model_dump(mode="json")`` 的 sha256（#36 前的指纹）。"""
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
    # flow_hash_algo 默认当前口径 = 「本引擎写下的状态」；测存量/漂移场景时按需覆盖。
    kwargs.setdefault("flow_hash_algo", FLOW_HASH_ALGO)
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
        """start_flow 落盘的 state.flow_hash == 原始存储定义的指纹 + 当前口径标记。"""
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
        # 与旧口径不同：指纹算在原始 JSON 上，不是解析后 dump
        assert state.flow_hash != _legacy_hash(TEST_FLOW)

    def test_hash_changes_when_definition_changes(self):
        """定义变更 → 指纹变化（校验能感知改定义）。"""
        assert _expected_hash(TEST_FLOW) != _expected_hash(MODIFIED_FLOW)

    def test_hash_deterministic_across_validations(self):
        """同一定义多次取指纹稳定（跨进程可比）。"""
        assert _expected_hash(TEST_FLOW) == _expected_hash(TEST_FLOW)

    def test_hash_ignores_model_evolution(self):
        """#36 核心：引擎给模型增删序列化字段不改同一份定义的指纹。

        旧口径把 ``Flow.model_dump`` 的输出当定义——这里把 dump 换成完全不同的
        内容模拟「pydantic/plaita 版本演进改了字段集」，新口径不受影响。
        """
        raw = dict(TEST_FLOW)
        fake = fakeredis.FakeRedis(decode_responses=True)
        flow_storage = MemoryFlowStorage()
        flow_storage.save_flow(TEST_FLOW)
        worker = _redis_worker(fake, MemoryExecutionStorage(), flow_storage)
        flow = worker.get_flow_definition("f1", "1")

        with patch.object(
            Flow, "model_dump", lambda self, **kw: {"evolved": "field added later"}
        ):
            assert worker._compute_flow_hash(raw) == _expected_hash(TEST_FLOW)
            assert worker._definition_fingerprint("f1", "1", flow) == (
                _expected_hash(TEST_FLOW),
                FLOW_HASH_ALGO,
            )
            # 旧口径在这种「模型演进」下必然漂移——这正是 #36 报的缺陷
            assert worker._compute_flow_hash(flow) != _expected_hash(TEST_FLOW)

    def test_fingerprint_falls_back_to_legacy_algo_when_raw_missing(self):
        """拿不到原始定义（调用方注入 Flow）→ 退回旧口径并**按旧标记落盘**，
        不冒用新标记（两种口径不可互比）。"""
        worker = _base_worker()
        flow = Flow.model_validate(TEST_FLOW)
        assert worker._definition_fingerprint("f1", "1", flow) == (
            _legacy_hash(TEST_FLOW),
            LEGACY_FLOW_HASH_ALGO,
        )


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
        """同口径指纹不一致 → 终态化 error（message 写明变更）+ ValueError（poison ack）。

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
        # 同引擎（无 engine_version 记录）→ 成因按「定义真被改」
        assert state.error["category"] == "flow_definition_changed"

    def test_same_algo_mismatch_stays_definition_change_even_with_engine_drift(self):
        """同口径失配 = 定义真被改：engine_version 也变了只是佐证，不冒充成因。

        v2 指纹算在原始存储定义上、与引擎版本无关，故同口径失配**不可能**是
        版本漂移所致；且 engine_version 不随 resume 覆写，按它分类会让这条执行
        此后每次失配都被永久归到「混部」，把值班视线从真正的定义变更上引开。
        """
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker, storage = self._worker_with_state(
            fake,
            MODIFIED_FLOW,
            _state(flow_hash=_expected_hash(TEST_FLOW), engine_version="0.0.1"),
        )
        with pytest.raises(ValueError, match="hash 不匹配"):
            worker.resume_flow("f1", "exec-1", "continue")
        state = storage.load_execution_state("exec-1")
        assert state.error["category"] == "flow_definition_changed"
        # 版本漂移照旧留痕（佐证），但不冒充成因
        assert state.error["stored_engine_version"] == "0.0.1"
        assert state.error["upgrade_suspected"] is False
        # 提示必须可执行：判定表里同口径失配**不认** allow_flow_hash_change，
        # 提示若指向它就等于让值班照做后撞回同一条错误
        assert "改回定义" in state.error["hint"]
        assert "同口径下无效" in state.error["hint"]
        assert (
            'plaita_resume_guard_total{category="flow_definition_changed",decision="mismatch"} 1'
            in worker.metrics_text()
        )

    def test_unknown_algo_mismatch_is_engine_version_drift_with_workable_hint(self):
        """口径标记不同（状态由别的口径/构建写下）→ engine_version_drift +
        可执行提示：照提示带 allow_flow_hash_change 确实能放行（与判定表一致）。
        """
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker, storage = self._worker_with_state(
            fake,
            MODIFIED_FLOW,
            _state(
                flow_hash=_expected_hash(TEST_FLOW),
                flow_hash_algo="flow-raw-json-sortkeys-v3",
                engine_version="0.0.1",
            ),
        )
        with pytest.raises(ValueError, match="hash 不匹配"):
            worker.resume_flow("f1", "exec-1", "continue")
        state = storage.load_execution_state("exec-1")
        assert state.error["category"] == "engine_version_drift"
        assert state.error["upgrade_suspected"] is True
        assert ALLOW_FLOW_HASH_CHANGE_KEY in state.error["hint"]
        assert (
            'plaita_resume_guard_total{category="engine_version_drift",decision="mismatch"} 1'
            in worker.metrics_text()
        )

        # 提示可执行：error 态 retry + 显式放行 → accepted 并刷新为当前口径
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.run_distributed.return_value = {
                "execution_id": "exec-1",
                "is_end": True,
                "context": {"done": True},
            }
            result = worker.resume_flow(
                "f1", "exec-1", "retry", data={ALLOW_FLOW_HASH_CHANGE_KEY: True}
            )
            assert result["is_end"] is True
        state = storage.load_execution_state("exec-1")
        assert state.flow_hash == _expected_hash(MODIFIED_FLOW)
        assert state.flow_hash_algo == FLOW_HASH_ALGO
        assert (
            'plaita_resume_guard_total{category="engine_version_drift",decision="accepted"} 1'
            in worker.metrics_text()
        )

    def test_legacy_hash_is_rebaselined_not_terminalized(self):
        """#36 核心：存量状态（旧口径指纹）resume 时不被终态化。

        跨口径比大小得不出「定义变没变」的结论——据它终态化就是滚动升级窗口里
        在途 run 被批量杀掉的现场。一次性重基线到当前口径后继续推进。
        """
        fake = fakeredis.FakeRedis(decode_responses=True)
        for legacy_algo in (None, LEGACY_FLOW_HASH_ALGO):
            worker, storage = self._worker_with_state(
                fake,
                MODIFIED_FLOW,
                _state(
                    flow_hash=_legacy_hash(TEST_FLOW),
                    flow_hash_algo=legacy_algo,
                    engine_version="0.6.1",
                ),
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
            state = storage.load_execution_state("exec-1")
            assert state.status == "completed"
            # 重基线：指纹与标记都换成当前口径（下次 resume 直接 match）
            assert state.flow_hash == _expected_hash(MODIFIED_FLOW)
            assert state.flow_hash_algo == FLOW_HASH_ALGO
            assert (
                'plaita_resume_guard_total{category="legacy_algo",decision="rebaseline"} 1'
                in worker.metrics_text()
            )

    def test_same_algo_mismatch_ignores_allow_change(self):
        """同口径哈希不同 = 定义真的变了：allow_flow_hash_change 不该放行。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker, storage = self._worker_with_state(
            fake, MODIFIED_FLOW, _state(flow_hash=_expected_hash(TEST_FLOW))
        )
        with pytest.raises(ValueError, match="hash 不匹配"):
            worker.resume_flow(
                "f1", "exec-1", "continue", data={ALLOW_FLOW_HASH_CHANGE_KEY: True}
            )
        assert storage.load_execution_state("exec-1").status == "error"


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


class TestResumeGuardMetricsSnapshot:
    """``plaita_resume_guard_total`` 的读写在并发下必须安全（#36 review）。

    ``metrics_text`` 由 ``ThreadingHTTPServer`` 的 handler 线程调用，
    ``_record_resume_guard`` 由消费线程写：不在同一把锁下取快照会命中
    ``RuntimeError: dictionary changed size during iteration``，``/metrics``
    被兜底成空 500——监控静默失明，恰是本次要修的可观测面。
    """

    def test_scrape_snapshots_under_the_record_lock(self):
        """确定性检查（并发撞窗口是概率事件）：持裁决锁时抓取必须阻塞。"""
        worker = _redis_worker(fakeredis.FakeRedis(decode_responses=True))
        worker._record_resume_guard("mismatch", "flow_definition_changed")
        scraped = threading.Event()

        def scrape():
            worker.metrics_text()
            scraped.set()

        worker._resume_guard_lock.acquire()
        try:
            thread = threading.Thread(target=scrape, daemon=True)
            thread.start()
            assert not scraped.wait(0.3), "抓取绕过了锁，直接迭代 _resume_guard_counts"
        finally:
            worker._resume_guard_lock.release()
        assert scraped.wait(5)
        thread.join(timeout=5)
        assert (
            'plaita_resume_guard_total{category="flow_definition_changed",decision="mismatch"} 1'
            in worker.metrics_text()
        )

    def test_concurrent_record_and_scrape_never_raise(self):
        """并发烟囱测试：写入新 (decision, category) 键的同时反复抓取。"""
        worker = _redis_worker(fakeredis.FakeRedis(decode_responses=True))
        stop = threading.Event()

        def write():
            i = 0
            while not stop.is_set():
                worker._record_resume_guard("mismatch", f"category-{i % 64}")
                i += 1

        writer = threading.Thread(target=write, daemon=True)
        writer.start()
        try:
            for _ in range(200):
                assert isinstance(worker.metrics_text(), str)
        finally:
            stop.set()
            writer.join(timeout=5)
        assert "plaita_resume_guard_total" in worker.metrics_text()
