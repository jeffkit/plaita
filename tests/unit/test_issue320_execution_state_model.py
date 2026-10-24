"""plaita#320 负控：A1 执行状态模型——pending 引用 + generation + progress_at
+ next_step（write-ahead state）。

规格源：#320 正文 + A1 r138 实施前评审（五窗映射/兼容义务/写序不变量/字段
细则/验收补充全部并入）。对照关系（A1 评审映射表即本文件的格清单）：

- **窗① ack 后崩溃 / 窗③ 挂起提交窗**：write-ahead 顺序格——挂起落盘（含
  pending）先于载体物化；「落盘→rpush」窗口被杀 ⇒ pending 在盘、重投补偿
  （新形状行走 ``effective_pending``，与 legacy 行同路径）。
- **窗② 租约冲突 ack**：状态行重建读源格——running 行带 next_step +
  progress_at（#321「running 无租约 → continue」的读法基础）。
- **窗④ 事件先于订阅**：补强选型 (b) deadline 必填格——event 引用恒带
  deadline（节点 subscription_timeout 优先，缺省/环境覆盖兜底），
  「不失联（可检可终局）」的数据前提；重建/回放（位标方案）不在本单。
- **窗⑤ 长节点卡死**：progress_at 与 last_update_time 正交双信号格——
  看门狗心跳只刷活性、不刷前进。
- **写序不变量 1**：终态迁移与 pending 清除同一次 CAS——机械化在落盘
  咽喉点（终态写自动清 pending/dispatch_pending/next_step）。
- **写序不变量 2**（补发/重投侧 enqueue 前重读非终态+generation）：义务落
  #321，本单以「补偿派发持租约+持锁重读」为既有先例锁定（#233/#243 家族）。
- **兼容四义务**：dispatch_pending 显式退役（停写+只读别名+双清）；#312
  词汇表正交（快照锁）；A125 收容不 touch 状态行（推导对载体在押无感=
  设计如此，coordination 义务在 #321）；NS 帧栈对齐（pending.node_id=最内
  帧挂起节点，node_state 逐帧查）+ 挂起守卫不被 pending 弱化。
- **A1 §五.2 终态×pending 原子性格** + §四.1-§四.4 字段细则格
  （创建=now/非步迁移不刷/推导在读面不回写/回滚=旧 worker 物理清除）。

基建：#233 同款（MemoryExecutionStorage + fakeredis + RedisFlowWorker，
patch FlowExecution 控制引擎返回）；fenced 盖章格用
RedisExecutionStorage+FencedExecutionStorage 套 fakeredis 走真实 Lua。
"""

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.core.errors import ExecutionLeaseError
from plaita.server.flow_worker import RedisFlowWorker, ServiceDispatchError
from plaita.storage.base import (
    ExecutionState,
    PENDING_EVENT_DEADLINE_DEFAULT_SECONDS,
    PENDING_KINDS,
    pending_event_deadline_default_seconds,
    validate_pending_ref,
)
from plaita.storage.fenced import (
    FencedExecutionStorage,
    reset_current_fence_token,
    set_current_fence_token,
)
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage
from plaita.storage.redis import (
    RedisExecutionStorage,
    execution_fence_generation_key,
    execution_fence_key,
)

TEST_FLOW = {
    "flow_id": "f1",
    "version": "1",
    "nodes": [
        {"id": "start", "type": "start", "next": "a"},
        {"id": "a", "type": "assignment", "result": {"step": 1}, "next": "end"},
        {"id": "end", "type": "end", "output": "ok"},
    ],
}
QUEUE = "plaita:delay:queue"
EXEC = "exec-issue320"
SERVICE_CONFIG = {"type": "delay", "delay_ms": 1000}


def _checkpoint(node_state=None, frames=None):
    """构造 checkpoint dict：$LAST_NODE/$NODE（+可选 $SUSPEND_FRAMES）。"""
    ctx = {"$LAST_NODE": "a", "$NODE": {"a": dict(node_state or {})}}
    if frames is not None:
        ctx["$SUSPEND_FRAMES"] = frames
    return ctx


def _worker():
    fake = fakeredis.FakeRedis(decode_responses=True)
    storage = MemoryExecutionStorage()
    flow_storage = MemoryFlowStorage()
    flow_storage.save_flow(TEST_FLOW)
    worker = RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:issue320-state-model",
        execution_storage=storage,
        flow_storage=flow_storage,
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
    )
    return worker, storage, fake


def _seed_running(storage, execution_id=EXEC):
    storage.save_execution_state(
        execution_id,
        ExecutionState(
            execution_id=execution_id,
            flow_id="f1",
            flow_version="1",
            status="running",
            context=_checkpoint(),
        ),
    )


def _suspend_result(execution_id=EXEC, context=None, service=True):
    result = {
        "execution_id": execution_id,
        "is_suspend": True,
        "context": dict(context or _checkpoint()),
    }
    if service:
        result["service_config"] = dict(SERVICE_CONFIG)
    return result


def _spy_saves(storage):
    """包装 storage.save_execution_state：记录每次落盘的 model_dump 快照。"""
    saved = []
    real_save = storage.save_execution_state
    storage.save_execution_state = MagicMock(
        side_effect=lambda eid, s: (saved.append(s.model_dump()), real_save(eid, s))[1]
    )
    return saved


def _queued(fake):
    return [json.loads(item) for item in fake.lrange(QUEUE, 0, -1)]


# ---------------------------------------------------------------------------
# schema 与回滚语义（A1 §四.4：旧 worker 全对象 save 物理清除新字段）
# ---------------------------------------------------------------------------


class TestSchemaAndRollbackSemantics:
    def test_new_fields_default_zero_regression(self):
        state = ExecutionState(execution_id="e", context={})
        assert state.pending is None
        assert state.generation == 0
        assert state.progress_at is None
        assert state.next_step is None
        assert state.dispatch_pending is None

    def test_model_dump_roundtrip_carries_new_fields(self):
        ref = {"kind": "event", "ref_id": "s1", "event_type": "go",
               "deadline": "2026-11-22T00:00:00", "node_id": "a"}
        state = ExecutionState(
            execution_id="e", context={}, pending=ref, generation=7,
            progress_at="2026-10-23T00:00:00",
            next_step={"node_id": "b", "step_id": None, "parent_step_id": None},
        )
        restored = ExecutionState.model_validate(
            json.loads(json.dumps(state.model_dump()))
        )
        assert restored.pending == ref
        assert restored.generation == 7
        assert restored.progress_at == "2026-10-23T00:00:00"
        assert restored.next_step == {"node_id": "b", "step_id": None, "parent_step_id": None}

    def test_old_worker_output_loads_and_rolls_back_fields(self):
        """回滚真语义（A1 §四.4）：旧 worker load（extra 忽略）→ 全对象 save
        ⇒ 新字段物理清除。新 worker 读回=字段缺省，零崩溃（验收「回滚后新
        字段仍在」不成立——按实写）。"""
        new_row = ExecutionState(
            execution_id="e", context={}, status="running",
            pending={"kind": "event", "ref_id": "s1", "event_type": "go",
                     "deadline": "2026-11-22T00:00:00"},
            generation=3, progress_at="2026-10-23T00:00:00",
            next_step={"node_id": "b", "step_id": None, "parent_step_id": None},
        )
        dumped = new_row.model_dump()
        old_worker_row = {
            k: v for k, v in dumped.items()
            if k not in ("pending", "generation", "progress_at", "next_step")
        }
        rolled_back = ExecutionState.model_validate(old_worker_row)
        assert rolled_back.pending is None and rolled_back.generation == 0
        assert rolled_back.progress_at is None and rolled_back.next_step is None

    def test_pending_kinds_vocabulary(self):
        assert PENDING_KINDS == frozenset(
            {"event", "timer", "service", "start", "cancel"}
        )


# ---------------------------------------------------------------------------
# 读面惰性推导（#320 迁移条款；A1 §四.3：推导在读面、不回写）
# ---------------------------------------------------------------------------


class TestEffectivePendingDerivation:
    def test_explicit_pending_wins(self):
        state = ExecutionState(
            execution_id="e", context=_checkpoint(), status="suspended",
            pending={"kind": "event", "ref_id": "s1", "event_type": "go",
                     "deadline": "2026-11-22T00:00:00"},
            dispatch_pending={"pending": True, "service_type": "delay",
                              "service_config": dict(SERVICE_CONFIG)},
        )
        assert state.effective_pending()["kind"] == "event"

    def test_legacy_dispatch_pending_derives_service_readonly(self):
        state = ExecutionState(
            execution_id="e", context=_checkpoint(), status="suspended",
            dispatch_pending={"pending": True, "service_type": "delay",
                              "service_config": dict(SERVICE_CONFIG),
                              "node_id": "a", "recorded_at": "2026-10-01T00:00:00"},
        )
        ref = state.effective_pending()
        assert ref["kind"] == "service"
        assert ref["migrated"] is True, "推导产物必须带 migrated 标记（写侧必填豁免依据）"
        assert ref["payload"]["service_config"] == SERVICE_CONFIG
        assert ref["payload"]["service_type"] == "delay"
        # 不回写：推导后行对象原样（A1 §四.3——回写须走同款 CAS，读面禁写）
        assert state.pending is None

    def test_suspended_with_subscription_derives_event(self):
        state = ExecutionState(
            execution_id="e", status="suspended",
            context=_checkpoint({"subscription_id": "sub-1", "event_type": "evt.go"}),
        )
        ref = state.effective_pending()
        assert ref["kind"] == "event"
        assert ref["migrated"] is True
        assert ref["ref_id"] == "sub-1"
        assert ref["event_type"] == "evt.go"
        assert ref["deadline"] is None, "老行推不出节点超时配置，不编造 deadline"

    def test_non_suspended_legacy_row_derives_none(self):
        state = ExecutionState(
            execution_id="e", status="running",
            context=_checkpoint({"subscription_id": "sub-1"}),
        )
        assert state.effective_pending() is None

    def test_orphan_suspended_row_derives_none(self):
        state = ExecutionState(execution_id="e", status="suspended", context=_checkpoint())
        assert state.effective_pending() is None


# ---------------------------------------------------------------------------
# 写侧 fail-closed 校验
# ---------------------------------------------------------------------------


class TestValidatePendingRef:
    def test_none_passthrough(self):
        assert validate_pending_ref(None) is None

    def test_unknown_kind_rejected(self):
        with pytest.raises(ValueError, match="pending.kind 非法"):
            validate_pending_ref({"kind": "quantum"})

    def test_event_requires_deadline(self):
        with pytest.raises(ValueError, match="deadline"):
            validate_pending_ref({"kind": "event", "event_type": "go", "ref_id": "s1"})

    def test_event_requires_event_type(self):
        with pytest.raises(ValueError, match="event_type"):
            validate_pending_ref({"kind": "event", "deadline": "2026-11-22T00:00:00"})

    def test_event_tolerates_missing_ref_id(self):
        """简化 bus 替身可能不回订阅 id——与 #221「缺字段放行」同款容忍。"""
        ref = validate_pending_ref(
            {"kind": "event", "ref_id": None, "event_type": "go",
             "deadline": "2026-11-22T00:00:00"}
        )
        assert ref["ref_id"] is None

    def test_service_requires_payload_config(self):
        with pytest.raises(ValueError, match="service_config"):
            validate_pending_ref(
                {"kind": "service", "payload": {"service_type": "delay"}}
            )

    def test_timer_requires_due_at(self):
        with pytest.raises(ValueError, match="due_at"):
            validate_pending_ref({"kind": "timer"})

    def test_migrated_rows_exempt_from_write_side_required_fields(self):
        ref = validate_pending_ref(
            {"kind": "event", "event_type": "go", "deadline": None, "migrated": True}
        )
        assert ref["migrated"] is True


# ---------------------------------------------------------------------------
# generation 盖章（复用 fence 世代；fenced CAS 路径）
# ---------------------------------------------------------------------------


class TestGenerationStamp:
    def _fenced(self, client):
        return FencedExecutionStorage(
            RedisExecutionStorage(client=client, namespace="plaita")
        )

    def _set_fence(self, client, execution_id, generation):
        client.set(execution_fence_key("plaita", execution_id), str(generation))
        client.set(
            execution_fence_generation_key("plaita", execution_id), str(generation)
        )

    def test_fenced_save_stamps_generation(self):
        client = fakeredis.FakeRedis(decode_responses=True)
        storage = self._fenced(client)
        self._set_fence(client, "e", 3)
        token = set_current_fence_token(3)
        try:
            state = ExecutionState(execution_id="e", context={}, status="running")
            assert storage.save_execution_state("e", state) is True
            assert state.generation == 3, "盖章与本写同一 CAS（先盖章后序列化）"
            on_disk = json.loads(client.get("plaita:execution:e"))
            assert on_disk["generation"] == 3
        finally:
            reset_current_fence_token(token)

    def test_unfenced_save_leaves_generation_untouched(self):
        client = fakeredis.FakeRedis(decode_responses=True)
        storage = self._fenced(client)
        state = ExecutionState(execution_id="e", context={}, status="running")
        assert storage.save_execution_state("e", state) is True
        assert state.generation == 0

    def test_first_fenced_write_is_mutually_exclusive_with_legacy_row(self):
        """A1 §四.3：老行 generation=0 首写 0→N 天然互斥——世代不符的旧
        worker 写被 CAS 拒绝，迁移零写风暴。"""
        client = fakeredis.FakeRedis(decode_responses=True)
        storage = self._fenced(client)
        state = ExecutionState(execution_id="e", context={}, status="running")
        assert storage.save_execution_state("e", state) is True  # 未持租约老写

        self._set_fence(client, "e", 5)
        token = set_current_fence_token(5)
        try:
            newer = ExecutionState.model_validate(
                json.loads(client.get("plaita:execution:e"))
            )
            newer.status = "suspended"
            assert storage.save_execution_state("e", newer) is True
            assert newer.generation == 5
        finally:
            reset_current_fence_token(token)

        # 旧世代 worker（gen=4）写同一行 → fenced CAS 拒绝（raise 失租语义）
        stale = ExecutionState.model_validate(
            json.loads(client.get("plaita:execution:e"))
        )
        stale.status = "completed"
        token_stale = set_current_fence_token(4)
        try:
            self._set_fence(client, "e", 5)
            with pytest.raises(ExecutionLeaseError, match="fencing token mismatch"):
                storage.save_execution_state("e", stale)
        finally:
            reset_current_fence_token(token_stale)


# ---------------------------------------------------------------------------
# 窗①/窗③：write-ahead 顺序与「落盘→物化」窗口补偿（service 半边）
# ---------------------------------------------------------------------------


class TestSuspendWriteAheadService:
    def test_persist_precedes_materialization_and_carries_pending(self):
        """写序格：挂起落盘（含 pending）先于 rpush；派发失败时 pending 已在盘。"""
        worker, storage, fake = _worker()
        _seed_running(storage)
        order = []
        real_save = storage.save_execution_state

        def _spy_save(eid, s):
            order.append("save")
            return real_save(eid, s)

        storage.save_execution_state = MagicMock(side_effect=_spy_save)

        def _fail_rpush(*args, **kwargs):
            order.append("rpush")
            raise ConnectionError("redis down during rpush")

        fake.rpush = MagicMock(side_effect=_fail_rpush)

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            FE.return_value.run_distributed.return_value = _suspend_result()
            with pytest.raises(ServiceDispatchError):
                worker.resume_flow("f1", EXEC, "continue")

        assert order.index("save") < order.index("rpush"), (
            "write-ahead：状态（含 pending 引用）必须先于载体物化落盘"
        )
        state = storage.load_execution_state(EXEC)
        assert state.status == "suspended"
        assert state.dispatch_pending is None, "退役字段停写"
        ref = state.pending
        assert ref["kind"] == "service"
        assert ref["payload"]["service_config"] == SERVICE_CONFIG
        assert ref["node_id"] == "a"

    def test_crash_window_compensates_from_new_shape_row(self):
        """窗①/③ 闭合格：落盘后、rpush 前被杀（=rpush 注入失败）→ 重投载体
        从**新形状** pending 补派恰一次 → 双清（#320 退役面）。"""
        worker, storage, fake = _worker()
        _seed_running(storage)
        orig_rpush = fake.rpush
        fake.rpush = MagicMock(side_effect=ConnectionError("crash window"))
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            FE.return_value.run_distributed.return_value = _suspend_result()
            with pytest.raises(ServiceDispatchError):
                worker.resume_flow("f1", EXEC, "continue")
        assert fake.llen(QUEUE) == 0
        fake.rpush = orig_rpush

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            result = worker.resume_flow("f1", EXEC, "retry")

        assert result["already_suspended"] is True
        assert result["dispatch_compensated"] is True
        assert len(_queued(fake)) == 1
        state = storage.load_execution_state(EXEC)
        assert state.pending is None and state.dispatch_pending is None, "补派后双清"
        assert state.status == "suspended"

    def test_non_service_event_suspend_builds_event_ref_and_no_dispatch(self):
        """窗④ 数据面：纯事件等待挂起 → pending(kind=event) 带订阅引用与
        deadline；不物化服务载体、无补偿二写。"""
        worker, storage, fake = _worker()
        _seed_running(storage)
        context = _checkpoint(
            {
                "subscription_id": "sub-320",
                "event_type": "evt.approved",
                "subscription_timeout": 3600,
            }
        )
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            FE.return_value.run_distributed.return_value = _suspend_result(
                context=context, service=False
            )
            worker.resume_flow("f1", EXEC, "continue")

        state = storage.load_execution_state(EXEC)
        assert state.status == "suspended"
        assert state.dispatch_pending is None
        ref = state.pending
        assert ref["kind"] == "event"
        assert ref["ref_id"] == "sub-320"
        assert ref["event_type"] == "evt.approved"
        assert ref["node_id"] == "a"
        assert ref["deadline"], "deadline 必填（④窗补强选型 b）"
        assert fake.llen(QUEUE) == 0

    def test_event_deadline_default_when_node_unconfigured(self):
        worker, storage, _ = _worker()
        _seed_running(storage)
        context = _checkpoint({"subscription_id": "s", "event_type": "e"})
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            FE.return_value.run_distributed.return_value = _suspend_result(
                context=context, service=False
            )
            worker.resume_flow("f1", EXEC, "continue")
        ref = storage.load_execution_state(EXEC).pending
        assert ref["deadline"] > ref["recorded_at"], (
            "缺省 deadline=recorded_at+缺省秒数（宽默认，不改存量挂起存活语义）"
        )

    def test_env_override_for_default_deadline(self, monkeypatch):
        monkeypatch.setenv("PLAITA_PENDING_EVENT_DEADLINE_SECONDS", "120")
        assert pending_event_deadline_default_seconds() == 120.0

    def test_env_override_invalid_falls_back(self, monkeypatch):
        default = float(PENDING_EVENT_DEADLINE_DEFAULT_SECONDS)
        monkeypatch.setenv("PLAITA_PENDING_EVENT_DEADLINE_SECONDS", "-5")
        assert pending_event_deadline_default_seconds() == default
        monkeypatch.setenv("PLAITA_PENDING_EVENT_DEADLINE_SECONDS", "abc")
        assert pending_event_deadline_default_seconds() == default

    def test_no_references_suspends_without_pending(self):
        """无订阅无服务的简化替身挂起 → pending=None，行形同老行（零回归）。"""
        worker, storage, fake = _worker()
        _seed_running(storage)
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            FE.return_value.run_distributed.return_value = _suspend_result(
                context=_checkpoint(), service=False
            )
            worker.resume_flow("f1", EXEC, "continue")
        state = storage.load_execution_state(EXEC)
        assert state.status == "suspended"
        assert state.pending is None and state.dispatch_pending is None


# ---------------------------------------------------------------------------
# NS 帧栈对齐（A1 §二.4：pending.node_id = 最内层挂起节点）
# ---------------------------------------------------------------------------


class TestNestedFrameAlignment:
    INNER_FRAMES = [
        {
            "node_id": "child_delay",
            "flow_id": "child-flow",
            "context": {
                "$LAST_NODE": "child_delay",
                "$NODE": {"child_delay": {"subscription_id": "sub-inner",
                                          "event_type": "evt.inner",
                                          "subscription_timeout": 60}},
            },
        },
        {"node_id": "child_call", "flow_id": "f1", "context": None},
    ]

    def test_pending_node_id_is_innermost_frame_node(self):
        worker, storage, _ = _worker()
        _seed_running(storage)
        context = _checkpoint(
            {"subscription_id": "sub-root-stale", "event_type": "evt.root"},
            frames=self.INNER_FRAMES,
        )
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            FE.return_value.run_distributed.return_value = _suspend_result(
                context=context, service=False
            )
            worker.resume_flow("f1", EXEC, "continue")

        ref = storage.load_execution_state(EXEC).pending
        assert ref["kind"] == "event"
        assert ref["node_id"] == "child_delay", "帧栈对齐：取最内帧挂起节点"
        assert ref["ref_id"] == "sub-inner", "node_state 逐帧查（不在根 $NODE）"
        assert ref["event_type"] == "evt.inner"

    def test_service_pending_node_id_aligns_frames_too(self):
        worker, storage, fake = _worker()
        _seed_running(storage)
        context = _checkpoint(
            {},
            frames=[
                {"node_id": "inner_delay", "flow_id": "child", "context": None},
                {"node_id": "call", "flow_id": "f1", "context": None},
            ],
        )
        # rpush 注入失败：让 armed pending 留在盘上供观测（派发成功路径会
        # 立即双清——见 #233 成功路径契约）。
        fake.rpush = MagicMock(side_effect=ConnectionError("observe window"))
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            FE.return_value.run_distributed.return_value = _suspend_result(context=context)
            with pytest.raises(ServiceDispatchError):
                worker.resume_flow("f1", EXEC, "continue")
        ref = storage.load_execution_state(EXEC).pending
        assert ref["kind"] == "service"
        assert ref["node_id"] == "inner_delay"

    def test_armed_pending_does_not_weaken_suspend_guard(self):
        """pending 不是绕过 #33/#311 守卫的通道：suspended+continue 仍幂等短路，
        行原样（pending 不被消费）。"""
        worker, storage, _fake = _worker()
        _seed_running(storage)
        context = _checkpoint({"subscription_id": "s", "event_type": "e"})
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            FE.return_value.run_distributed.return_value = _suspend_result(
                context=context, service=False
            )
            worker.resume_flow("f1", EXEC, "continue")
        before = storage.load_execution_state(EXEC)

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            result = worker.resume_flow("f1", EXEC, "continue")

        FE.assert_not_called()
        assert result["already_suspended"] is True
        after = storage.load_execution_state(EXEC)
        assert after.pending == before.pending


# ---------------------------------------------------------------------------
# 窗②：状态行重建读源（#321「running 无租约 → continue」）
# ---------------------------------------------------------------------------


def _step_result(last_node, context, **extra):
    payload = {
        "execution_id": EXEC, "is_end": False, "is_suspend": False,
        "id": last_node, "context": dict(context),
    }
    payload.update(extra)
    return payload


class TestRebuildReadModel:
    def test_step_persist_declares_next_step_and_progress(self):
        """步提交落盘的 payload 内联断言：progress_at 刷新 + next_step 预声明。

        循环语义：step_persist 在「最新 result 为非终态步」后触发、context 取
        该 result——两发同位步（$LAST_NODE=a）+一发终态，第一发 step_persist
        的 next_step 即 flow.next_node(a)=end。"""
        worker, storage, _fake = _worker()
        _seed_running(storage)
        ctx_a = _checkpoint()
        results = [
            _step_result("a", ctx_a),
            _step_result("a", ctx_a),   # 同位步：step_persist 在此发之后触发
            {"execution_id": EXEC, "is_end": True, "id": "end", "context": ctx_a},
        ]
        saved = _spy_saves(storage)
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            FE.return_value.run_distributed.side_effect = results
            with patch.object(RedisFlowWorker, "_cancel_requested", return_value=False):
                worker.resume_flow("f1", EXEC, "continue")

        step_saves = [p for p in saved if p["status"] == "running"]
        assert step_saves, "步提交必须有 running 落盘"
        for payload in step_saves:
            assert payload["progress_at"], "步提交刷新 progress_at"
        # plaita#322 改种：step token 从「键位预留恒 None」升级为铸造的分配
        # 决定（B1 所有权 v1）；parent_step_id=声明链锚（此处无先前 token →
        # None）。schema 形状仍由本格钉住：三键齐全、node_id 权威。
        declarations = [
            p["next_step"] for p in step_saves if p.get("next_step") is not None
        ]
        assert any(
            d["node_id"] == "end"
            and isinstance(d["step_id"], str)
            and d["step_id"]
            and d["parent_step_id"] is None
            for d in declarations
        ), "步提交预声明 next_step=flow.next_node（step_id 为铸造 token）"

    def test_terminal_row_freezes_progress_and_clears_next_step(self):
        worker, storage, _fake = _worker()
        _seed_running(storage)
        ctx = _checkpoint()
        results = [
            _step_result("a", ctx),
            _step_result("a", ctx),  # 同位步：保证 step_persist（progress_at）发生
            {"execution_id": EXEC, "is_end": True, "id": "end", "context": ctx},
        ]
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            FE.return_value.run_distributed.side_effect = results
            with patch.object(RedisFlowWorker, "_cancel_requested", return_value=False):
                worker.resume_flow("f1", EXEC, "continue")

        state = storage.load_execution_state(EXEC)
        assert state.status == "completed"
        assert state.next_step is None, "终态不得声明未完成的工作"
        assert state.pending is None
        assert state.progress_at is not None, "终态冻结在最后一步提交时刻"


# ---------------------------------------------------------------------------
# 写序不变量 1：终态×pending 同一次 CAS（A1 §五.2 直接断言）
# ---------------------------------------------------------------------------


class TestTerminalPendingAtomicity:
    @staticmethod
    def _armed_state(status="running"):
        return ExecutionState(
            execution_id=EXEC,
            flow_id="f1",
            status=status,
            context=_checkpoint(),
            pending={"kind": "event", "ref_id": "s", "event_type": "e",
                     "deadline": "2026-11-22T00:00:00"},
            dispatch_pending={"pending": True, "service_type": "delay",
                              "service_config": dict(SERVICE_CONFIG)},
            next_step={"node_id": "b", "step_id": None, "parent_step_id": None},
        )

    @pytest.mark.parametrize("terminal", ["completed", "cancelled", "error"])
    def test_terminal_write_clears_pending_in_same_save(self, terminal):
        worker, storage, _fake = _worker()
        state = self._armed_state(status=terminal)
        state.end_time = "2026-10-23T00:00:00"

        saved = _spy_saves(storage)
        worker._persist_state_or_raise(EXEC, state, "terminal-test")

        assert len(saved) == 1
        payload = saved[0]
        assert payload["status"] == terminal
        assert payload["pending"] is None, "终态写内清 pending——同一次 CAS"
        assert payload["dispatch_pending"] is None
        assert payload["next_step"] is None
        on_disk = storage.load_execution_state(EXEC)
        assert on_disk.pending is None and on_disk.next_step is None

    def test_non_terminal_write_keeps_pending(self):
        worker, storage, _fake = _worker()
        state = self._armed_state(status="running")
        worker._persist_state_or_raise(EXEC, state, "step_persist")
        on_disk = storage.load_execution_state(EXEC)
        assert on_disk.pending is not None, "非终态写不动 armed pending"

    def test_event_consumption_clears_pending_with_running_migration(self):
        """A1 §一.③ resume 方向：事件消费后 pending 清除与 running 迁移同一
        次 CAS——event 决议挂起，第一步 running 落盘的 payload 里 pending 已
        是 None。"""
        worker, storage, _fake = _worker()
        storage.save_execution_state(
            EXEC,
            ExecutionState(
                execution_id=EXEC, flow_id="f1", flow_version="1",
                status="suspended",
                context=_checkpoint({"subscription_id": "sub-320",
                                     "event_type": "evt.go"}),
                pending={"kind": "event", "ref_id": "sub-320",
                         "event_type": "evt.go", "node_id": "a",
                         "deadline": "2026-11-22T00:00:00"},
            ),
        )
        ctx = _checkpoint({"subscription_id": "sub-320", "event_type": "evt.go"})
        results = [
            _step_result("b", ctx),
            _step_result("b", ctx),  # 同位步：保证 step_persist（running）发生
            {"execution_id": EXEC, "is_end": True, "id": "end", "context": ctx},
        ]
        saved = _spy_saves(storage)
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            FE.return_value.run_distributed.side_effect = results
            with patch.object(RedisFlowWorker, "_cancel_requested", return_value=False):
                worker.resume_flow(
                    "f1", EXEC, "event",
                    data={"subscription_id": "sub-320", "event_type": "evt.go"},
                )

        running_saves = [p for p in saved if p["status"] == "running"]
        assert running_saves, "事件消费后必须有 running 迁移落盘"
        assert all(p["pending"] is None for p in running_saves), (
            "running 迁移与 pending 清除同一次 CAS（payload 内联断言）"
        )

    def test_clear_path_goes_through_fenced_cas_write(self):
        """不变量 1 的「无独立清除路径」半边：补偿派发后的清除必须经
        ``_persist_state_or_raise``（带 generation CAS 的状态行写）。"""
        worker, storage, _fake = _worker()
        state = self._armed_state(status="suspended")
        worker._persist_state_or_raise(EXEC, state, "seed")
        real_persist = worker._persist_state_or_raise
        worker._persist_state_or_raise = MagicMock(
            side_effect=lambda *a, **k: real_persist(*a, **k)
        )
        worker._clear_suspend_dispatch_pending(EXEC, state, source="test")
        worker._persist_state_or_raise.assert_called_once()
        on_disk = storage.load_execution_state(EXEC)
        assert on_disk.pending is None and on_disk.dispatch_pending is None


# ---------------------------------------------------------------------------
# 窗⑤：progress_at 与 last_update_time 正交双信号
# ---------------------------------------------------------------------------


class TestProgressAtOrthogonality:
    def test_watchdog_heartbeat_refreshes_liveness_not_progress(self):
        """看门狗心跳（_publish_node_progress）只刷 last_update_time（活性），
        progress_at（前进）不动——mode 5 双信号检测面的数据前提。"""
        worker, storage, _fake = _worker()
        storage.save_execution_state(
            EXEC,
            ExecutionState(
                execution_id=EXEC, flow_id="f1", status="running",
                context=_checkpoint(),
                progress_at="2026-10-23T00:00:00",
                last_update_time="2026-10-23T00:00:00",
            ),
        )
        timing = MagicMock()
        timing.snapshot.return_value = {"impl": {"started_at": "x"}}
        worker._node_timings[EXEC] = timing

        worker._publish_node_progress(EXEC, fence_token=None, tenant_id=None)

        state = storage.load_execution_state(EXEC)
        assert state.last_update_time != "2026-10-23T00:00:00", "活性信号已刷新"
        assert state.progress_at == "2026-10-23T00:00:00", "前进信号不被心跳触碰"

    def test_retry_wakeup_does_not_touch_progress_at(self):
        """A1 §四.2：非步迁移（error→running 唤醒）不刷 progress_at——单调
        停滞语义才成立。"""
        worker, storage, _fake = _worker()
        storage.save_execution_state(
            EXEC,
            ExecutionState(
                execution_id=EXEC, flow_id="f1", flow_version="1",
                status="error", context=_checkpoint(),
                progress_at="2026-10-23T00:00:00",
                error={"message": "boom"},
            ),
        )
        saved = _spy_saves(storage)
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            FE.return_value.run_distributed.return_value = {
                "execution_id": EXEC, "is_end": True, "id": "end",
                "context": _checkpoint(),
            }
            worker.resume_flow("f1", EXEC, "retry")

        running_saves = [p for p in saved if p["status"] == "running"]
        assert running_saves, "retry 唤醒应有 running 迁移写（retry_wakeup）"
        for payload in running_saves:
            assert payload["progress_at"] == "2026-10-23T00:00:00", (
                "非步迁移不得刷新 progress_at"
            )


# ---------------------------------------------------------------------------
# 兼容格：#312 词汇表正交 / A125 收容无感 / #316 Stage 0 容忍
# ---------------------------------------------------------------------------


class TestCompatGrid:
    def test_epistemic_vocabulary_orthogonal(self):
        """#312 认识论分流词汇表不被本单触碰：消息侧处置词汇保持原集。"""
        from plaita.server.tenant_status import EPISTEMIC_REJECT_REASONS

        assert EPISTEMIC_REJECT_REASONS == frozenset(
            {"status_read_failed", "generation_read_failed"}
        )

    def test_derivation_is_custody_blind_by_design(self):
        """A125 义务面：隔离流在押（第四存放处 custody）时 pending 仍 armed、
        推导照常返回引用——收容不 touch 状态行；「重放 vs 重建两通道不并发」
        的协调义务在 #321（本单推导不判载体在场与否）。"""
        state = ExecutionState(
            execution_id="e", status="suspended", context=_checkpoint(),
            pending={"kind": "event", "ref_id": "s", "event_type": "e",
                     "deadline": "2026-11-22T00:00:00"},
        )
        assert state.effective_pending()["kind"] == "event"

    def test_carrier_audit_stage0_tolerates_new_fields(self, tmp_path):
        """#316 Stage 0 只读审计不动（实施范围⑤）：带新字段的行扫描结果与
        老行全等——新字段不制造幻影载体格。"""
        import importlib.util
        import sys
        from pathlib import Path

        from plaita.tenant_context import (
            DEFAULT_TENANT_ID,
            reset_current_tenant,
            set_current_tenant,
            tenant_namespace,
        )

        repo_root = Path(__file__).resolve().parents[2]
        spec = importlib.util.spec_from_file_location(
            "carrier_audit_320", repo_root / "scripts" / "carrier_audit.py"
        )
        audit = importlib.util.module_from_spec(spec)
        sys.modules["carrier_audit_320"] = audit
        spec.loader.exec_module(audit)

        now = 1761198000.0
        iso = datetime.fromtimestamp(now, tz=timezone.utc).isoformat()

        def scan_with(extra_fields):
            client = fakeredis.FakeRedis(decode_responses=True)
            token = set_current_tenant(DEFAULT_TENANT_ID)
            try:
                store = RedisExecutionStorage(
                    client=client, namespace=tenant_namespace(DEFAULT_TENANT_ID)
                )
                state = ExecutionState(
                    execution_id="e1", flow_id="f1", tenant_id=DEFAULT_TENANT_ID,
                    context={}, status="suspended",
                    start_time=iso, last_update_time=iso,
                    **extra_fields,
                )
                assert store.save_execution_state("e1", state) is True
            finally:
                reset_current_tenant(token)
            config = audit.load_config(
                redis_url="redis://localhost:6379/9",
                tenants="default",
                streams="plaita:flow:queue",
                timer_queues="plaita:delay:queue",
                output_dir=str(tmp_path),
            )
            return audit.scan_round(audit.ReadOnlyRedis(client), config, now=now)

        legacy_result = scan_with({})
        new_result = scan_with(
            {
                "pending": {"kind": "event", "ref_id": "s", "event_type": "e",
                            "deadline": iso},
                "generation": 3,
                "progress_at": iso,
                "next_step": {"node_id": "b", "step_id": None,
                              "parent_step_id": None},
            }
        )
        assert legacy_result.cells == new_result.cells, (
            "Stage 0 载体判据不受新字段影响（suspended 行无载体 ⇒ 同格计数）"
        )
