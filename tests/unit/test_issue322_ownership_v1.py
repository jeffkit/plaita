"""plaita#322 负控：B1 所有权 v1——step token 预分配 + 连接器幂等键
+ 进度租约 + report_progress。

规格源：#322 正文（正文即规格）+ A1 r138 评审注记（#320 ①②重建副作用
安全依赖本单——义务声明落 WORKER_PROTOCOL.md 与 ownership 模块 docstring）。

格清单（对应正文建议/验收）：

- **step_id 预分配**：状态行 next_step 的 step token 从「键位预留恒 None」
  升级为「分配决定」——start 首落 / retry_wakeup / step_persist 三个铸造点
  （C 组）；执行对象消费行声明（bind_step），重投/重试读同一行 ⇒ 同 token
  （C4 重放稳定格）；terminal 咽喉清声明（#320 写序不变量 1 的 step 角，
  与 pending 同一次 CAS——三向格 E 组）。
- **连接器幂等键**：``execution.idempotency_key = f"{execution_id}:{step_id}"``
  （attempt 不进键）；子调用经 ``get_child_execution(sub_key=…)`` /
  ``derive_key(index)`` 派生（循环 10 次迭代 10 个不同 key）；内置 http
  连接器对非安全方法自动带 ``Idempotency-Key``（用户显式给的头不覆盖；
  无声明/安全方法不带）。子执行 ``$EXECUTION_ID`` 每次构造随机
  （core/context.py:311）⇒ 键的执行段必须取**根**执行 id（A 组钉死）。
- **进度租约**：``last_report_at``（report_progress 单点写）与 progress_at
  （#320 钉死：仅步提交刷新）正交；租约过期谓词
  ``now - max(progress_at, last_report_at) > step_timeout``——step_timeout
  按节点 ``timeout`` 解析、未声明用环境缺省（默认宽）。看门狗行为不变
  （任务书③裁量：租约到期=载体丢失信号，供 #321 对账器消费，非本单动作）。
- **report_progress**：引擎面 API（子执行沿父链找到根 reporter）；worker 面
  单点写 last_report_at（节流；终态行拒写；不触碰 progress_at /
  last_update_time）。

基建：#320/#233 同款（MemoryExecutionStorage + fakeredis + RedisFlowWorker，
patch FlowExecution 控制引擎返回）。
"""

import json
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.core.executor import FlowExecution
from plaita.core.ownership import (
    DEFAULT_STEP_LEASE_TIMEOUT_SECONDS,
    IDEMPOTENCY_HEADER,
    STEP_LEASE_TIMEOUT_ENV,
    bind_child_step,
    compose_idempotency_key,
    connector_idempotency_key,
    declared_step_id,
    default_step_lease_timeout_seconds,
    new_step_id,
    resolve_step_timeout_seconds,
    step_lease_expired,
)
from plaita.node.http import HTTP, HttpExecutor
from plaita.server.flow_worker import RedisFlowWorker
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

TEST_FLOW = {
    "flow_id": "f1",
    "version": "1",
    "nodes": [
        {"id": "start", "type": "start", "next": "a"},
        {"id": "a", "type": "assignment", "result": {"step": 1}, "next": "end"},
        {"id": "end", "type": "end", "output": "ok"},
    ],
}
EXEC = "exec-issue322"


def _checkpoint(node_state=None):
    return {"$LAST_NODE": "a", "$NODE": {"a": dict(node_state or {})}}


def _worker():
    fake = fakeredis.FakeRedis(decode_responses=True)
    storage = MemoryExecutionStorage()
    flow_storage = MemoryFlowStorage()
    flow_storage.save_flow(TEST_FLOW)
    worker = RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:issue322-ownership",
        execution_storage=storage,
        flow_storage=flow_storage,
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
    )
    return worker, storage, fake


def _seed_running(storage, execution_id=EXEC, next_step=None, status="running",
                  progress_at="2026-10-24T00:00:00", **extra):
    storage.save_execution_state(
        execution_id,
        ExecutionState(
            execution_id=execution_id,
            flow_id="f1",
            flow_version="1",
            status=status,
            context=_checkpoint(),
            progress_at=progress_at,
            next_step=next_step,
            **extra,
        ),
    )


def _step_result(node_id="a", context=None):
    return {
        "execution_id": EXEC,
        "is_end": False,
        "id": node_id,
        "context": dict(context or _checkpoint()),
    }


def _end_result(context=None):
    return {
        "execution_id": EXEC,
        "is_end": True,
        "id": "end",
        "context": dict(context or _checkpoint()),
    }


def _spy_saves(storage):
    saved = []
    real_save = storage.save_execution_state
    storage.save_execution_state = MagicMock(
        side_effect=lambda eid, s: (saved.append(s.model_dump()), real_save(eid, s))[1]
    )
    return saved


def _bound_execution(execution_id=EXEC, step_id="s-1"):
    """构造带 run 身份与 step 声明的真实 FlowExecution（引擎面键模型用）。"""
    execution = FlowExecution()
    execution._ctx.set_state("$EXECUTION_ID", execution_id)
    execution.bind_step(step_id)
    return execution


def _http_node(method="POST", node_id="h1", headers=None):
    kwargs = dict(id=node_id, url="http://target.invalid/api", method=method)
    if headers is not None:
        kwargs["headers"] = headers
    return HTTP(**kwargs)


# ---------------------------------------------------------------------------
# A. 键模型（引擎面，纯）：step token → 幂等键
# ---------------------------------------------------------------------------


class TestKeyModel:
    def test_fresh_execution_is_tokenless(self):
        execution = FlowExecution()
        execution._ctx.set_state("$EXECUTION_ID", EXEC)
        assert execution.step_id is None
        assert execution.idempotency_key is None, (
            "无声明即无所有权主张——连接器不得编造幂等键"
        )
        assert execution.derive_key(0) is None

    def test_bind_step_composes_idempotency_key(self):
        execution = _bound_execution()
        assert execution.step_id == "s-1"
        assert execution.idempotency_key == f"{EXEC}:s-1"
        assert execution.derive_key(3) == f"{EXEC}:s-1:3"

    def test_key_uses_root_execution_id_not_child_uuid(self):
        """子执行 $EXECUTION_ID 每次构造随机（core/context.py:311）——键的
        执行段必须取根执行 id，否则子流程内连接器键跨重放不稳定。"""
        parent = _bound_execution(step_id="s-1")
        child = parent.get_child_execution()
        assert child.execution_id != parent.execution_id, (
            "前提：子执行确实换了随机 id（否则本格测不到根回退）"
        )
        assert child.idempotency_key == f"{EXEC}:s-1", "子执行继承父作用域"
        grandchild = child.get_child_execution()
        assert grandchild.idempotency_key == f"{EXEC}:s-1"

    def test_child_with_sub_key_derives_scoped_key(self):
        """复合容器两步式：get_child_execution()（签名零变更，替身友好）+
        bind_child_step 派生作用域。"""
        parent = _bound_execution(step_id="s-1")
        child = parent.get_child_execution()
        assert child.idempotency_key == f"{EXEC}:s-1", "未绑定子执行沿父链继承"
        bind_child_step(parent, child, 7)
        assert child.idempotency_key == f"{EXEC}:s-1:7"

    def test_loop_ten_iterations_ten_distinct_keys(self):
        """验收：循环 10 次迭代产生 10 个不同 key；同迭代重放同 key。"""
        parent = _bound_execution(step_id="s-1")
        keys = []
        for i in range(10):
            child = parent.get_child_execution()
            bind_child_step(parent, child, i)
            keys.append(child.idempotency_key)
        assert len(set(keys)) == 10
        replay_child = parent.get_child_execution()
        bind_child_step(parent, replay_child, 4)
        assert replay_child.idempotency_key == keys[4], "同迭代重放必须复现同一 key"

    def test_bind_child_step_defensive_on_plain_double(self):
        """替身子执行缺 bind_step（或 bind_step 抛错）→ 静默跳过——所有权面
        绝不打断节点执行。"""
        parent = _bound_execution(step_id="s-1")
        bind_child_step(parent, object(), 0)  # 不得抛

        class _Boom:
            def bind_step(self, scope):
                raise RuntimeError("double refuses")

        bind_child_step(parent, _Boom(), 0)  # 不得抛

    def test_rebind_to_none_returns_tokenless(self):
        execution = _bound_execution()
        execution.bind_step(None)
        assert execution.idempotency_key is None

    def test_compose_and_declared_helpers(self):
        assert compose_idempotency_key("e1", "s1") == "e1:s1"
        assert compose_idempotency_key("e1", None) is None
        assert compose_idempotency_key("", "s1") is None, "无 run 身份不主张所有权"
        assert declared_step_id({"node_id": "a", "step_id": "s1"}) == "s1"
        assert declared_step_id({"node_id": "a", "step_id": None}) is None
        assert declared_step_id(None) is None
        assert declared_step_id({"step_id": 42}) is None, "非串 token 不认"

    def test_new_step_id_shape_and_uniqueness(self):
        tokens = {new_step_id() for _ in range(50)}
        assert len(tokens) == 50
        assert all(isinstance(t, str) and t for t in tokens)


# ---------------------------------------------------------------------------
# B. 连接器幂等键（内置 http 连接器自动带 Idempotency-Key）
# ---------------------------------------------------------------------------


class TestConnectorHeader:
    def test_post_carries_step_scoped_key(self):
        execution = _bound_execution(step_id="s-1")
        executor = _http_node("POST").new_executor(execution)
        assert executor.headers[IDEMPOTENCY_HEADER] == f"{EXEC}:s-1:h1"

    def test_safe_methods_do_not_carry_header(self):
        """副作用收据只给能产生副作用的方法（GET/HEAD/OPTIONS/TRACE 语义安全）。"""
        execution = _bound_execution(step_id="s-1")
        for method in ("GET", "HEAD", "OPTIONS", "TRACE"):
            executor = _http_node(method).new_executor(execution)
            assert IDEMPOTENCY_HEADER not in executor.headers, method

    def test_unsafe_methods_all_carry_header(self):
        execution = _bound_execution(step_id="s-1")
        for method in ("POST", "PUT", "PATCH", "DELETE"):
            executor = _http_node(method).new_executor(execution)
            assert executor.headers[IDEMPOTENCY_HEADER] == f"{EXEC}:s-1:h1", method

    def test_user_supplied_header_wins_case_insensitive(self):
        execution = _bound_execution(step_id="s-1")
        executor = _http_node("POST", headers={"idempotency-key": "user-key"}).new_executor(
            execution
        )
        assert executor.headers["idempotency-key"] == "user-key"
        assert IDEMPOTENCY_HEADER not in executor.headers

    def test_tokenless_execution_carries_no_header(self):
        execution = FlowExecution()
        execution._ctx.set_state("$EXECUTION_ID", EXEC)
        executor = _http_node("POST").new_executor(execution)
        assert IDEMPOTENCY_HEADER not in executor.headers

    def test_odd_execution_context_is_defensive(self):
        """测试/第三方常传 MagicMock 当 execution（#190 同款教训）——读键失败
        必须退化为「不带头」，绝不炸节点。"""
        executor = _http_node("POST").new_executor(MagicMock())
        assert IDEMPOTENCY_HEADER not in executor.headers

    def test_retry_replay_same_step_same_key_single_side_effect(self):
        """验收：同一逻辑步骤重试 20 次，外部系统只收到 1 次副作用——
        以键稳定性为机制、外部去重为语义的最小模型。"""
        external_seen = {}

        def external_apply(key):
            if key in external_seen:
                return "duplicate"
            external_seen[key] = True
            return "applied"

        execution = _bound_execution(step_id="s-1")
        node = _http_node("POST")
        keys = [
            connector_idempotency_key(execution, node.id) for _ in range(20)
        ]
        outcomes = [external_apply(k) for k in keys]
        assert outcomes.count("applied") == 1
        assert outcomes.count("duplicate") == 19

    def test_helper_none_when_key_missing(self):
        assert connector_idempotency_key(None, "h1") is None
        tokenless = FlowExecution()
        tokenless._ctx.set_state("$EXECUTION_ID", EXEC)
        assert connector_idempotency_key(tokenless, "h1") is None


# ---------------------------------------------------------------------------
# C. worker 侧 step token 预分配与消费
# ---------------------------------------------------------------------------


class TestStepTokenAllocation:
    def test_start_first_persist_declares_minted_step_token(self):
        """start 首落声明即将执行的 start 节点，step_id 为铸造值。"""
        worker, storage, _fake = _worker()
        saved = _spy_saves(storage)
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            FE.return_value.run_distributed.return_value = _end_result()
            with patch.object(RedisFlowWorker, "_cancel_requested", return_value=False):
                worker.start_flow("f1", {}, execution_id=EXEC)

        start_saves = [p for p in saved if p["status"] == "running"]
        assert start_saves, "start 首落 running 行必须发生"
        first = start_saves[0]
        assert first["next_step"]["node_id"] == "start"
        assert isinstance(first["next_step"]["step_id"], str) and first["next_step"]["step_id"]
        assert first["next_step"]["parent_step_id"] is None

    def test_step_persist_mints_fresh_token_with_parent_chain(self):
        """step_persist 铸造新 token 声明下一步；parent_step_id=刚消费的 token
        （声明链审计锚）。

        驱动序列三发（#320 同款）：step_persist 在「最新 result 为非终态步」
        之后触发——[a, a, end]：第二发 a 之后落 step_persist（声明 next=end）。"""
        worker, storage, _fake = _worker()
        _seed_running(
            storage,
            next_step={"node_id": "a", "step_id": "s-seed", "parent_step_id": None},
        )
        saved = _spy_saves(storage)
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            FE.return_value.run_distributed.side_effect = [
                _step_result("a"),
                _step_result("a"),
                _end_result(),
            ]
            with patch.object(RedisFlowWorker, "_cancel_requested", return_value=False):
                worker.resume_flow("f1", EXEC, "continue")

        step_saves = [p for p in saved if p["status"] == "running" and p.get("next_step")]
        assert step_saves, "step_persist 必须发生"
        declaration = step_saves[0]["next_step"]
        assert declaration["node_id"] == "end"
        assert isinstance(declaration["step_id"], str) and declaration["step_id"]
        assert declaration["step_id"] != "s-seed", "每次声明铸造新 token"
        assert declaration["parent_step_id"] == "s-seed", "声明链指向刚消费的 token"

    def test_bound_token_is_row_declaration_and_replay_stable(self):
        """执行对象消费的是行声明：恢复入口与步循环首步读同一行 ⇒ 同 token；
        step_persist 后的下一步消费**新**声明（铸造值 ≠ 种子）。"""
        worker, storage, _fake = _worker()
        _seed_running(
            storage,
            next_step={"node_id": "a", "step_id": "s-seed", "parent_step_id": None},
        )
        saved = _spy_saves(storage)
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            FE.return_value.run_distributed.side_effect = [
                _step_result("a"),
                _step_result("a"),
                _end_result(),
            ]
            with patch.object(RedisFlowWorker, "_cancel_requested", return_value=False):
                worker.resume_flow("f1", EXEC, "continue")
            bind_calls = [c.args[0] for c in FE.return_value.bind_step.call_args_list]
        minted = next(
            p["next_step"]["step_id"] for p in saved
            if p["status"] == "running" and p.get("next_step")
        )
        assert bind_calls[0] == "s-seed", "恢复入口消费行声明"
        assert bind_calls[1] == "s-seed", "步循环首步：行尚未重写，仍是同一声明"
        assert bind_calls[2] == minted, "step_persist 后的下一步消费新铸造的声明"
        assert minted != "s-seed"

    def test_replayed_row_yields_identical_token(self):
        """载体重投（行未变）→ 二次执行绑定同一 token——「同 step_id 二次
        执行」的 worker 侧语义。崩溃模拟用 ExecutionLeaseError（原样上抛、
        不 ack、不写状态——正是重投路径的异常族）。"""
        from plaita.core.errors import ExecutionLeaseError

        worker, storage, _fake = _worker()
        _seed_running(
            storage,
            next_step={"node_id": "a", "step_id": "s-seed", "parent_step_id": None},
        )
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            with patch.object(RedisFlowWorker, "_cancel_requested", return_value=False):
                # 第一次投递：节点执行后、落盘前载体消失（模拟崩溃窗口）
                FE.return_value.run_distributed.side_effect = ExecutionLeaseError(
                    "simulated loss before persist"
                )
                with pytest.raises(ExecutionLeaseError):
                    worker.resume_flow("f1", EXEC, "continue")
                # 重投：同一行、同一声明
                FE.return_value.run_distributed.side_effect = [
                    _step_result("a"),
                    _step_result("a"),
                    _end_result(),
                ]
                worker.resume_flow("f1", EXEC, "continue")
            bind_calls = [c.args[0] for c in FE.return_value.bind_step.call_args_list]
        assert len(bind_calls) >= 2
        assert bind_calls[0] == bind_calls[1] == "s-seed", (
            "首投与重投读同一行 ⇒ 同 token（连接器幂等键稳定的前提）"
        )

    def test_retry_wakeup_mints_token_for_rerun_node(self):
        """error→running 唤醒写（retry_wakeup）铸造 re-run 节点的 token——
        write-ahead：先声明工作再执行；progress_at 不动（#320 §四.2）。"""
        worker, storage, _fake = _worker()
        _seed_running(
            storage,
            status="error",
            progress_at="2026-10-24T00:00:00",
            error={"message": "boom"},
        )
        saved = _spy_saves(storage)
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            FE.return_value.run_distributed.return_value = _end_result()
            with patch.object(RedisFlowWorker, "_cancel_requested", return_value=False):
                worker.resume_flow("f1", EXEC, "retry")

        wakeups = [p for p in saved if p.get("status") == "running"]
        assert wakeups, "retry 唤醒应有 running 迁移写"
        declaration = wakeups[0]["next_step"]
        assert declaration["node_id"] == "a", "声明 re-run 节点（$LAST_NODE）"
        assert isinstance(declaration["step_id"], str) and declaration["step_id"]
        assert wakeups[0]["progress_at"] == "2026-10-24T00:00:00", (
            "唤醒写不得刷新 progress_at（#320 钉死语义）"
        )

    def test_suspend_row_declares_no_step_token(self):
        """挂起行的工作声明由 pending 承载，next_step 同一次写置 None
        （#320 语义，step token 随之不存在）。"""
        worker, storage, _fake = _worker()
        _seed_running(
            storage,
            next_step={"node_id": "a", "step_id": "s-seed", "parent_step_id": None},
        )
        saved = _spy_saves(storage)
        suspend_result = {
            "execution_id": EXEC,
            "is_suspend": True,
            "context": _checkpoint(),
            "service_config": {"type": "delay", "delay_ms": 1000},
        }
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            FE.return_value.run_distributed.return_value = suspend_result
            with patch.object(RedisFlowWorker, "_cancel_requested", return_value=False):
                worker.resume_flow("f1", EXEC, "continue")

        suspended = [p for p in saved if p["status"] == "suspended"]
        assert suspended
        assert suspended[0]["next_step"] is None
        assert suspended[0]["pending"]["kind"] == "service"


# ---------------------------------------------------------------------------
# E. 三向格：step_id × generation × pending（#320 字段组合不变量）
# ---------------------------------------------------------------------------


class TestThreeWayGrid:
    def test_terminal_clears_declaration_in_same_cas_as_pending(self):
        """终态行不得带 armed 声明：step token 与 pending 同一次 CAS 清除
        （#320 写序不变量 1 的 step 角）。"""
        worker, storage, _fake = _worker()
        state = ExecutionState(
            execution_id=EXEC,
            flow_id="f1",
            status="completed",
            context=_checkpoint(),
            pending={"kind": "event", "ref_id": "s", "event_type": "e",
                     "deadline": "2026-12-24T00:00:00"},
            next_step={"node_id": "b", "step_id": "s-armed",
                       "parent_step_id": None},
        )
        worker._persist_state_or_raise(EXEC, state, "terminal-test")
        assert state.next_step is None
        assert state.pending is None

    def test_stale_generation_write_cannot_rollback_declaration(self):
        """fenced CAS：老世代写不得回滚新世代声明（声明的不可逆性——
        接管者的 token 不会被失租者覆盖）。"""
        from plaita.storage.fenced import (
            FencedExecutionStorage,
            reset_current_fence_token,
            set_current_fence_token,
        )
        from plaita.storage.redis import (
            RedisExecutionStorage,
            execution_fence_generation_key,
            execution_fence_key,
        )

        client = fakeredis.FakeRedis(decode_responses=True)
        storage = FencedExecutionStorage(
            RedisExecutionStorage(client=client, namespace="plaita")
        )
        client.set(execution_fence_key("plaita", EXEC), "5")
        client.set(execution_fence_generation_key("plaita", EXEC), "5")
        token = set_current_fence_token(5)
        try:
            state = ExecutionState(
                execution_id=EXEC, flow_id="f1", status="running",
                context=_checkpoint(),
                next_step={"node_id": "b", "step_id": "s-new-owner",
                           "parent_step_id": None},
            )
            assert storage.save_execution_state(EXEC, state) is True
        finally:
            reset_current_fence_token(token)

        stale = ExecutionState.model_validate(
            json.loads(client.get(f"plaita:execution:{EXEC}"))
        )
        stale.next_step = {"node_id": "b", "step_id": "s-stale",
                           "parent_step_id": None}
        stale_token = set_current_fence_token(4)
        try:
            from plaita.core.errors import ExecutionLeaseError
            with pytest.raises(ExecutionLeaseError):
                storage.save_execution_state(EXEC, stale)
        finally:
            reset_current_fence_token(stale_token)

        on_disk = json.loads(client.get(f"plaita:execution:{EXEC}"))
        assert on_disk["next_step"]["step_id"] == "s-new-owner", (
            "声明未被老世代写回滚"
        )


# ---------------------------------------------------------------------------
# D. 进度租约：last_report_at + 过期谓词 + report_progress
# ---------------------------------------------------------------------------


class TestProgressLease:
    def test_last_report_at_field_roundtrip(self):
        state = ExecutionState(
            execution_id=EXEC, flow_id="f1", status="running",
            context={}, last_report_at="2026-10-24T01:00:00",
        )
        restored = ExecutionState.model_validate(state.model_dump())
        assert restored.last_report_at == "2026-10-24T01:00:00"
        assert ExecutionState(execution_id=EXEC, context={}).last_report_at is None

    def test_expiry_predicate_fresh_and_stale(self):
        now = datetime(2026, 10, 24, 2, 0, 0)
        fresh = (now - timedelta(seconds=30)).isoformat()
        stale = (now - timedelta(seconds=3600)).isoformat()
        assert step_lease_expired(fresh, None, 60.0, now=now) is False
        assert step_lease_expired(stale, None, 60.0, now=now) is True
        # max(progress_at, last_report_at)：任一新鲜即租约有效
        assert step_lease_expired(stale, fresh, 60.0, now=now) is False
        assert step_lease_expired(fresh, stale, 60.0, now=now) is False
        # 无基线（老行）不判过期——宁可漏报不误杀
        assert step_lease_expired(None, None, 60.0, now=now) is False

    def test_report_refresh_extends_lease(self):
        now = datetime(2026, 10, 24, 2, 0, 0)
        step_commit = (now - timedelta(seconds=120)).isoformat()
        reported = (now - timedelta(seconds=10)).isoformat()
        assert step_lease_expired(step_commit, None, 60.0, now=now) is True
        assert step_lease_expired(step_commit, reported, 60.0, now=now) is False, (
            "report_progress 刷新租约（长步内活性+前进合并信号）"
        )

    def test_state_level_convenience(self):
        state = ExecutionState(
            execution_id=EXEC, flow_id="f1", status="running", context={},
            progress_at="2026-10-24T01:00:00", last_report_at=None,
        )
        assert state.step_lease_expired(60.0, now=datetime(2026, 10, 24, 1, 30)) is True
        assert state.step_lease_expired(3600.0, now=datetime(2026, 10, 24, 1, 30)) is False

    def test_resolve_step_timeout_from_node_timeout(self):
        from plaita.node.basic import Node

        default = 86400.0
        assert resolve_step_timeout_seconds(Node(id="n", timeout="3000"), default) == 3.0
        assert resolve_step_timeout_seconds(Node(id="n", timeout="PT2S"), default) == 2.0
        assert resolve_step_timeout_seconds(Node(id="n", timeout=""), default) == default, (
            "未声明用默认"
        )
        assert resolve_step_timeout_seconds(None, default) == default

    def test_default_step_lease_timeout_env(self, monkeypatch):
        monkeypatch.delenv(STEP_LEASE_TIMEOUT_ENV, raising=False)
        assert default_step_lease_timeout_seconds() == DEFAULT_STEP_LEASE_TIMEOUT_SECONDS
        monkeypatch.setenv(STEP_LEASE_TIMEOUT_ENV, "600")
        assert default_step_lease_timeout_seconds() == 600.0
        monkeypatch.setenv(STEP_LEASE_TIMEOUT_ENV, "not-a-number")
        assert default_step_lease_timeout_seconds() == DEFAULT_STEP_LEASE_TIMEOUT_SECONDS, (
            "非法值回退默认（#151 掩形纪律由 runtime_config 共享实现承担）"
        )

    def test_report_progress_calls_bound_reporter(self):
        execution = FlowExecution()
        reporter = MagicMock()
        execution.bind_progress_reporter(reporter)
        execution.report_progress("halfway")
        reporter.assert_called_once()
        child = execution.get_child_execution()
        child.report_progress()
        assert reporter.call_count == 2, "子执行沿父链找到根 reporter"

    def test_report_progress_without_reporter_is_noop(self):
        execution = FlowExecution()
        execution.report_progress("nobody listening")  # 不得抛

    def test_report_progress_swallows_reporter_failure(self):
        execution = FlowExecution()
        reporter = MagicMock(side_effect=RuntimeError("storage down"))
        execution.bind_progress_reporter(reporter)
        execution.report_progress()  # 进度上报绝不打断节点执行


class TestWorkerProgressReporter:
    def test_reporter_writes_last_report_at_single_point(self):
        worker, storage, _fake = _worker()
        _seed_running(storage)
        worker._report_step_progress(EXEC, "halfway")
        state = storage.load_execution_state(EXEC)
        assert state.last_report_at is not None, "单点写 last_report_at"
        assert state.progress_at == "2026-10-24T00:00:00", "不触碰 progress_at"
        assert state.last_update_time is None, "不触碰活性信号（看门狗域）"

    def test_reporter_refuses_terminal_rows(self):
        worker, storage, _fake = _worker()
        _seed_running(storage, status="completed")
        worker._report_step_progress(EXEC)
        state = storage.load_execution_state(EXEC)
        assert state.last_report_at is None

    def test_reporter_throttled_within_min_interval(self, monkeypatch):
        monkeypatch.setenv("PLAITA_PROGRESS_REPORT_MIN_INTERVAL_SECONDS", "60")
        worker, storage, _fake = _worker()
        _seed_running(storage)
        saved = _spy_saves(storage)
        worker._report_step_progress(EXEC, "first")
        worker._report_step_progress(EXEC, "second")
        assert len(saved) == 1, "节流窗内的第二次上报不产生第二行写"
        monkeypatch.setenv("PLAITA_PROGRESS_REPORT_MIN_INTERVAL_SECONDS", "0")
        worker._report_step_progress(EXEC, "third")
        assert len(saved) == 2, "节流关闭（0）时每次上报都落行"

    def test_missing_row_is_noop(self):
        worker, storage, _fake = _worker()
        worker._report_step_progress("exec-not-there")  # 不得抛

    def test_watchdog_heartbeat_stays_orthogonal(self):
        """看门狗心跳不写 last_report_at（三信号正交：活性/前进/上报）。"""
        worker, storage, _fake = _worker()
        storage.save_execution_state(
            EXEC,
            ExecutionState(
                execution_id=EXEC, flow_id="f1", status="running",
                context=_checkpoint(),
                progress_at="2026-10-24T00:00:00",
                last_update_time="2026-10-24T00:00:00",
            ),
        )
        timing = MagicMock()
        timing.snapshot.return_value = {"impl": {"started_at": "x"}}
        worker._node_timings[EXEC] = timing
        worker._publish_node_progress(EXEC, fence_token=None, tenant_id=None)
        state = storage.load_execution_state(EXEC)
        assert state.last_report_at is None, "心跳不冒充进度上报"
