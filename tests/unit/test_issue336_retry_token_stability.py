"""plaita#336 负控：失败重试路径连接器幂等键漂移——error 终态咽喉 ×
retry_wakeup 铸造点。

规格源：#336 正文（A1 r139 实弹探针）+ owner 裁决（案 b）：token 存
``state.error`` 扩展键随失败证据落盘，retry 唤醒复用。#322 的核心承诺
「同逻辑步骤重试 N 次 = 至多 1 次副作用」的稳定性锚是「重投读同一行 ⇒
同 token」——error 终态化清 ``next_step``（#320 写序不变量 1）后该锚在
error→retry 主路径失效：retry_wakeup 必然重铸新 token，外部去重失效。

修法（本单）：咽喉清声明**前**把行上声明转存 ``state.error["failed_step"]``
（形态与 #320 ``next_step`` 字段设计一致：``{node_id, step_id,
parent_step_id}``）；retry_wakeup 铸造点先读该键——重跑节点（$LAST_NODE）
与转存声明同节点 ⇒ 复用同 token；无证据/节点漂移 ⇒ 照铸新（首投/其它
路径不变）。案 a（error 行豁免清声明）不采：#320 不变量不开口子；
案 c（可推导 token）不采：与 ``new_step_id`` 随机防伪造设计冲突。

格清单：

- **A 咽喉转存**（真 ``_persist_state_or_raise``）：error 终态清声明前
  转存证据（同一次落盘）；无声明不造键；error=None 兜底造 dict；
  completed/cancelled 不转存（retry 只唤醒 error 行）；再终态化幂等
  （已存证据不被清除）；与既有 error 扩展键（``failed_node``/``node_retries``
  等）共存不覆写。
- **B retry 复用**（真 ``resume_flow``）：漂移探针全链（声明→真咽喉终态化
  →retry 绑原 token，修前两 token/修后同 token）；连续两次 retry 同 token
  （G1 预算=2 内）；无证据照铸新（守恒）；节点漂移不复用（守恒/防御）；
  连接器键跨失败-重试稳定（真 ``connector_idempotency_key``）。

基建：#322 同款（MemoryExecutionStorage + fakeredis + RedisFlowWorker，
patch FlowExecution 控制引擎返回）。声明转存/复用半边的行为格直接以字面
键 ``"failed_step"`` 断言（线面格式即契约，不经新符号 import）。
"""

from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.core.executor import FlowExecution
from plaita.core.ownership import (
    connector_idempotency_key,
    new_step_id,
)
from plaita.server.flow_worker import (
    NodeFailureTerminalizedError,
    RedisFlowWorker,
)
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
EXEC = "exec-issue336"
EVIDENCE_KEY = "failed_step"


def _checkpoint():
    return {"$LAST_NODE": "a", "$NODE": {"a": {}}}


def _worker():
    fake = fakeredis.FakeRedis(decode_responses=True)
    storage = MemoryExecutionStorage()
    flow_storage = MemoryFlowStorage()
    flow_storage.save_flow(TEST_FLOW)
    worker = RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:issue336-retry-token",
        execution_storage=storage,
        flow_storage=flow_storage,
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
    )
    return worker, storage, fake


def _declaration(step_id):
    return {"node_id": "a", "step_id": step_id, "parent_step_id": None}


def _seed_row(storage, *, status, next_step=None, error=None):
    storage.save_execution_state(
        EXEC,
        ExecutionState(
            execution_id=EXEC,
            flow_id="f1",
            flow_version="1",
            status=status,
            context=_checkpoint(),
            next_step=next_step,
            error=error,
        ),
    )
    return storage.load_execution_state(EXEC)


def _end_result():
    return {
        "execution_id": EXEC,
        "is_end": True,
        "id": "end",
        "context": dict(_checkpoint()),
    }


def _spy_saves(storage):
    saved = []
    real_save = storage.save_execution_state
    storage.save_execution_state = MagicMock(
        side_effect=lambda eid, s: (saved.append(s.model_dump()), real_save(eid, s))[1]
    )
    return saved


def _retry_wakeup(worker, storage, *, result=None, side_effect=None):
    saved = _spy_saves(storage)
    with patch("plaita.server.flow_worker.FlowExecution") as FE:
        if side_effect is not None:
            FE.return_value.run_distributed.side_effect = side_effect
        else:
            FE.return_value.run_distributed.return_value = (
                result if result is not None else _end_result()
            )
        with patch.object(RedisFlowWorker, "_cancel_requested", return_value=False):
            if side_effect is not None:
                # 确定性失败在步循环终态化 error 后以
                # NodeFailureTerminalizedError 上抛（消息 poison ack）——
                # 绑定发生在 run_distributed 前，读点不受影响。
                with pytest.raises(NodeFailureTerminalizedError):
                    worker.resume_flow("f1", EXEC, "retry")
            else:
                worker.resume_flow("f1", EXEC, "retry")
        bound = FE.return_value.bind_step.call_args_list[0].args[0]
    return bound, saved


def _key_for(token):
    """真键模型：绑定 token 的根执行在节点 a 上的连接器外发键。"""
    execution = FlowExecution()
    execution._ctx.set_state("$EXECUTION_ID", EXEC)
    execution.bind_step(token)
    return connector_idempotency_key(execution, "a")


# ---------------------------------------------------------------------------
# A. 咽喉转存：error 终态清声明前 token 随失败证据落盘
# ---------------------------------------------------------------------------


class TestTerminalEvidencePreservation:
    def test_error_terminal_stores_declaration_as_evidence(self):
        """error 终态：next_step 照清（#320 不变量保持）+ 声明转存进
        error 扩展键（同一次落盘）。"""
        worker, storage, _fake = _worker()
        t1 = new_step_id()
        state = _seed_row(storage, status="running", next_step=_declaration(t1))
        state.status = "error"
        state.error = {"message": "POST ok, response parse failed"}
        saved = _spy_saves(storage)
        worker._persist_state_or_raise(EXEC, state, "error_state")

        assert len(saved) == 1, "转存与清声明须同一次落盘（#320 同 CAS 纪律）"
        assert saved[0]["next_step"] is None, "终态不得声明未完成的工作"
        evidence = saved[0]["error"].get(EVIDENCE_KEY)
        assert evidence == _declaration(t1), "行上 token 随失败证据落盘"
        assert evidence["node_id"] == "a"
        assert evidence["parent_step_id"] is None

    def test_error_terminal_without_declaration_stores_nothing(self):
        """无声明（老行/挂起后 error）不造键：无证据 ⇒ retry 照铸新。"""
        worker, storage, _fake = _worker()
        state = _seed_row(storage, status="running", next_step=None)
        state.status = "error"
        state.error = {"message": "boom"}
        worker._persist_state_or_raise(EXEC, state, "error_state")
        assert EVIDENCE_KEY not in state.error

    def test_error_terminal_malformed_declaration_stores_nothing(self):
        """畸形声明（缺 token）不转存：读者按缺省铸新，不编造。"""
        worker, storage, _fake = _worker()
        state = _seed_row(
            storage, status="running",
            next_step={"node_id": "a", "parent_step_id": None},
        )
        state.status = "error"
        state.error = {"message": "boom"}
        worker._persist_state_or_raise(EXEC, state, "error_state")
        assert state.next_step is None
        assert EVIDENCE_KEY not in state.error

    def test_error_terminal_with_error_none_creates_evidence_dict(self):
        """error 行无失败证据 dict 时兜底造 dict 存证据：token 证据不因
        载体缺失而丢（错误详情缺失是另一回事，不在本单放大）。"""
        worker, storage, _fake = _worker()
        t1 = new_step_id()
        state = _seed_row(storage, status="running", next_step=_declaration(t1))
        state.status = "error"
        state.error = None
        worker._persist_state_or_raise(EXEC, state, "error_state")
        assert isinstance(state.error, dict)
        assert state.error.get(EVIDENCE_KEY) == _declaration(t1)

    def test_completed_terminal_does_not_preserve(self):
        """completed 不转存：retry 只唤醒 error 行，终态收据残留无消费方
        （观测残留与意图声明不同律——last_report_at 先例）。"""
        worker, storage, _fake = _worker()
        state = _seed_row(
            storage, status="running", next_step=_declaration(new_step_id())
        )
        state.status = "completed"
        state.error = None
        worker._persist_state_or_raise(EXEC, state, "completed")
        assert state.next_step is None
        assert state.error is None

    def test_cancelled_terminal_does_not_preserve(self):
        """cancelled 同上：不转存。"""
        worker, storage, _fake = _worker()
        state = _seed_row(
            storage, status="running", next_step=_declaration(new_step_id())
        )
        state.status = "cancelled"
        state.error = None
        worker._persist_state_or_raise(EXEC, state, "cancelled")
        assert state.next_step is None
        assert state.error is None

    def test_reterminalization_keeps_existing_evidence(self):
        """再终态化幂等：已终态行（声明已清）再过咽喉不覆写既有证据。"""
        worker, storage, _fake = _worker()
        t1 = new_step_id()
        state = _seed_row(
            storage, status="error", next_step=None,
            error={"message": "boom", EVIDENCE_KEY: _declaration(t1)},
        )
        worker._persist_state_or_raise(EXEC, state, "already_terminal")
        assert state.error[EVIDENCE_KEY] == _declaration(t1)

    def test_evidence_coexists_with_existing_error_keys(self):
        """与节点失败路径既有扩展键（failed_node/node_retries/error_type）
        共存：转存只加键，不覆写失败证据。"""
        worker, storage, _fake = _worker()
        t1 = new_step_id()
        state = _seed_row(storage, status="running", next_step=_declaration(t1))
        state.status = "error"
        state.error = {
            "message": "节点执行失败（重试 2 次后仍失败）: boom",
            "node_retries": 2,
            "error_type": "NodeExecutionError",
            "failed_node": "a",
        }
        worker._persist_state_or_raise(EXEC, state, "error_state")
        assert state.error["node_retries"] == 2
        assert state.error["failed_node"] == "a"
        assert state.error[EVIDENCE_KEY] == _declaration(t1)


# ---------------------------------------------------------------------------
# B. retry 复用：唤醒面同 token（漂移探针 + 守恒/防御格）
# ---------------------------------------------------------------------------


class TestRetryTokenReuse:
    def _staged_failure(self, storage, worker, t1):
        """漂移探针公共舞台：running 行带声明 → 真咽喉 error 终态化。"""
        state = _seed_row(storage, status="running", next_step=_declaration(t1))
        state.status = "error"
        state.error = {"message": "POST ok, response parse failed"}
        worker._persist_state_or_raise(EXEC, state, "error_state")
        row = storage.load_execution_state(EXEC)
        assert row.status == "error" and row.next_step is None
        return row

    def test_failure_retry_rebinds_same_token(self):
        """A1 r139 漂移探针（真组件复刻）：首跑带 T1 失败 → retry 唤醒
        重跑绑定 token == T1（修前铸新必红）。"""
        worker, storage, _fake = _worker()
        t1 = new_step_id()
        self._staged_failure(storage, worker, t1)

        bound, saved = _retry_wakeup(worker, storage)

        assert bound == t1, "重试必须复用原 token——同逻辑步骤同幂等键"
        wakeups = [p for p in saved if p.get("status") == "running"]
        assert wakeups and wakeups[0]["next_step"]["step_id"] == t1
        assert wakeups[0]["error"] is None, "唤醒写清失败证据（翻 running）"

    def test_retry_twice_still_same_token(self):
        """连续两次 retry（G1 预算=2 内）token 恒定：重试 N 次 = 同键。"""
        worker, storage, _fake = _worker()
        t1 = new_step_id()
        self._staged_failure(storage, worker, t1)
        for attempt in (1, 2):
            bound, _saved = _retry_wakeup(
                worker, storage, side_effect=ValueError(f"确定性失败 #{attempt}")
            )
            assert bound == t1, f"第 {attempt} 次 retry 仍须复用 T1"
            row = storage.load_execution_state(EXEC)
            assert row.status == "error", "确定性失败终态化 error"
            assert row.error[EVIDENCE_KEY] == _declaration(t1), (
                "再终态化把 T1 重新落进证据（下轮 retry 继续复用）"
            )

    def test_retry_without_evidence_mints_fresh(self):
        """守恒：无证据（老 error 行/首投形态）照铸新 token。"""
        worker, storage, _fake = _worker()
        _seed_row(storage, status="error", next_step=None,
                  error={"message": "boom"})
        bound, saved = _retry_wakeup(worker, storage)
        wakeups = [p for p in saved if p.get("status") == "running"]
        assert wakeups and wakeups[0]["next_step"]["step_id"] == bound
        assert isinstance(bound, str) and bound

    def test_retry_node_drift_does_not_reuse(self):
        """防御：证据声明节点 ≠ 重跑节点（$LAST_NODE 漂移）不复用——
        陈旧声明从未消费过，照铸新。"""
        worker, storage, _fake = _worker()
        stale = new_step_id()
        _seed_row(
            storage, status="error", next_step=None,
            error={"message": "boom", EVIDENCE_KEY: _declaration(stale)},
        )
        # $LAST_NODE 仍是 a，证据节点改造成 b ⇒ 漂移
        row = storage.load_execution_state(EXEC)
        row.error[EVIDENCE_KEY]["node_id"] = "b"
        storage.save_execution_state(EXEC, row)

        bound, saved = _retry_wakeup(worker, storage)
        assert bound != stale, "节点漂移不得复用陈旧 token"
        wakeups = [p for p in saved if p.get("status") == "running"]
        assert wakeups[0]["next_step"]["node_id"] == "a"

    def test_connector_key_stable_across_failure_retry(self):
        """端到端键面：失败尝试与重试的外发键逐字节相同（外部去重生效）。"""
        worker, storage, _fake = _worker()
        t1 = new_step_id()
        self._staged_failure(storage, worker, t1)
        key_first = _key_for(t1)

        bound, _saved = _retry_wakeup(worker, storage)
        key_retry = _key_for(bound)

        assert key_first == key_retry
        assert key_first == f"{EXEC}:{t1}:a"
