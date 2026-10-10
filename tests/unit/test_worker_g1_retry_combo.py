"""G1 resume-retry（43828aa）× 波次二任务①自动重试 的组合语义钉子。

rebase 组合（2026-10 二波 P1 × keeper 迁移 G1）两条互补机制必须显式协同：

- **G1 retry = error 后的人工唤醒**：终态短路放行 ``error + resume_type=retry``，
  翻 running 落盘、error 清空、从断点步进；
- **任务① = 节点失败的自动有界重试**：``__cause__`` 链含 NodeExecutionError
  时不终态化、消息重投重跑失败节点，预算=重试计数键
  ``{ns}:execution:noderetry:{id}:{node_id}``（**按节点维度隔离**，
  plaita#123——此前只含 execution_id，而成功推进清零整个执行 ⇒
  「前序节点稳成功 + 某节点稳失败」的 flow 预算永不耗尽 ⇒ 无限重投）。

组合契约（本文件钉住）：

1. 自动重试走的是 running 态的步进失败路径，**不**经过 G1 的 error 放行
   （retry_wakeup 判据要求 status==error，running+continue 走正常处理）；
2. G1 retry 唤醒**清零**计数键——预算耗尽终态化的执行经人工唤醒后拿全新
   预算，否则唤醒后第一次失败立刻再耗尽，G1 形同虚设。翻转落盘成功才清零
   （落盘失败抛 StatePersistError 走重投，唤醒未生效不清预算）；
3. 计数语义 = 「当前节点的**连续**失败次数」：任一节点成功推进即清零，
   不同节点的失败不共享预算（节点 A 抖一次花掉的预算不让节点 B 的第一次
   失败就少一次机会）；
4. flow 定义指纹校验在 retry 唤醒**之前**：定义被改时执行保持 error（可再
   retry），不翻 running。
"""
import json
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.core.errors import FlowErrorException, NodeExecutionError
from plaita.server.flow_worker import (
    FlowHashMismatchError,
    NodeExecutionRetryableError,
    RedisFlowWorker,
    StatePersistError,
)
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage


FLOW_DEF = {
    "flow_id": "f1",
    "version": "1",
    "nodes": [
        {"id": "start", "type": "start", "next": "a"},
        {"id": "a", "type": "assignment", "result": {"step": 1}, "next": "end"},
        {"id": "end", "type": "end", "output": "ok"},
    ],
}

COUNTER_KEY = "plaita:execution:noderetry:exec-1:a"
LEGACY_COUNTER_KEY = "plaita:execution:noderetry:exec-1"


def _node_failure_exc() -> FlowErrorException:
    """分布式路径真实形态：FlowErrorException(__cause__=NodeExecutionError)。"""
    err = NodeExecutionError("执行节点a出错了: ConnectionError: blip", node="a")
    wrapped = FlowErrorException(str(err))
    wrapped.__cause__ = err
    return wrapped


def _make_worker(fake, storage=None, flow_storage=None) -> RedisFlowWorker:
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:g1-retry-combo",
        execution_storage=storage if storage is not None else MemoryExecutionStorage(),
        flow_storage=flow_storage if flow_storage is not None else MemoryFlowStorage(),
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
        lease_ttl_seconds=60,
    )


def _flow_storage():
    fs = MemoryFlowStorage()
    fs.save_flow(FLOW_DEF)
    return fs


def _state(status="running", **kwargs) -> ExecutionState:
    state = ExecutionState(
        execution_id="exec-1",
        flow_id="f1",
        flow_version="1",
        status=status,
        context={"$LAST_NODE": "start", "$NODE": {"start": {}}},
        **kwargs,
    )
    if status == "error":
        state.error = {"message": "boom"}
        state.end_time = "2026-10-02T00:00:00"
    return state


# ---------- 1：自动重试不误触 G1 的 error 放行 ----------


class TestAutoRetryDoesNotTouchG1Wakeup:
    def test_running_plus_continue_node_failure_goes_auto_retry_path(self):
        """自动重试钉子：running + continue + 节点失败 → NodeExecutionRetryableError，
        状态保持 running、无 error 落盘——全程不经过 G1 的 retry_wakeup
        （那个分支要求 status==error）也不触发终态化。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        worker = _make_worker(fake, storage, _flow_storage())
        storage.save_execution_state("exec-1", _state(status="running"))
        fake.set(COUNTER_KEY, "0")  # 即使计数键存在也走自动路径

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.run_distributed.side_effect = _node_failure_exc()
            with pytest.raises(NodeExecutionRetryableError):
                worker.resume_flow("f1", "exec-1", "continue")

        state = storage.load_execution_state("exec-1")
        assert state.status == "running"
        assert state.error is None
        assert state.end_time is None
        # G1 wakeup 翻转（running 化 + 清 error）从未发生——状态本来就 running
        assert fake.get(COUNTER_KEY) == "1"  # 自动路径正常计数

    def test_error_plus_continue_stays_error_no_auto_retry(self):
        """error 态 + continue（非 retry）→ already_terminal 短路，既不放行
        也不自动重试——自动重试只服务消息处理中的步进失败，不复活 error。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        worker = _make_worker(fake, storage, _flow_storage())
        storage.save_execution_state("exec-1", _state(status="error"))

        result = worker.resume_flow("f1", "exec-1", "continue")
        assert result["already_terminal"] is True
        assert result["status"] == "error"
        assert storage.load_execution_state("exec-1").status == "error"


# ---------- 2：G1 retry 唤醒清零计数键 ----------


class TestRetryWakeupClearsCounter:
    def _error_worker_with_exhausted_counter(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        worker = _make_worker(fake, storage, _flow_storage())
        storage.save_execution_state("exec-1", _state(status="error"))
        fake.set(COUNTER_KEY, "5")  # 预算（5）耗尽时的遗留计数（节点 a）
        # 另造两个键，验证唤醒清的是**该执行下所有节点**的键（#123 起键按节点隔离）：
        # 另一个节点 + 存量旧键（无节点维度）。
        fake.set("plaita:execution:noderetry:exec-1:other", "3")
        fake.set(LEGACY_COUNTER_KEY, "2")
        return fake, storage, worker

    def test_wakeup_deletes_counter_and_next_failure_gets_fresh_budget(self):
        """预算耗尽 error（计数=5）→ G1 retry 唤醒清零计数 → 唤醒后的第一次
        节点失败重新拿满预算（第 1/5 次重试），而不是立刻再耗尽。

        判据对比：若唤醒未清零，本次失败 INCR→6 ≥ 5 → 预算耗尽 → RuntimeError
        终态化（G1 形同虚设）；清零生效 → INCR→1 → 可重试且标注「第 1/5」。

        唤醒是**执行级**意图 ⇒ 必须清掉该执行下**所有**节点的键（含存量旧键）。
        """
        fake, storage, worker = self._error_worker_with_exhausted_counter()

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.run_distributed.side_effect = _node_failure_exc()
            with pytest.raises(NodeExecutionRetryableError) as ei:
                worker.resume_flow("f1", "exec-1", "retry")

        assert "第 1/5" in str(ei.value)  # 全新预算（未清零会是预算耗尽 RuntimeError）
        assert fake.get(COUNTER_KEY) == "1"
        assert fake.get("plaita:execution:noderetry:exec-1:other") is None, (
            "唤醒必须清该执行下**所有节点**的计数键（不只是失败节点）"
        )
        assert fake.get(LEGACY_COUNTER_KEY) is None, (
            "存量旧键（无节点维度）也应被唤醒一并清掉，避免遗留脏数据"
        )
        # G1 唤醒翻转生效且自动重试未终态化：行停在 running，可继续重投/再 retry
        state = storage.load_execution_state("exec-1")
        assert state.status == "running"
        assert state.error is None

    def test_wakeup_persist_failure_keeps_counter(self):
        """唤醒翻转落盘失败（StatePersistError）→ 唤醒未生效，计数**不**清零
        （消息重投后重新走 wakeup，预算语义不被半途破坏）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)

        class FalseSaveStorage(MemoryExecutionStorage):
            def save_execution_state(self, execution_id, state) -> bool:
                super().save_execution_state(execution_id, state)
                return False

        storage = FalseSaveStorage()
        worker = _make_worker(fake, storage, _flow_storage())
        storage.save_execution_state("exec-1", _state(status="error"))
        fake.set(COUNTER_KEY, "5")

        with pytest.raises(StatePersistError):
            worker.resume_flow("f1", "exec-1", "retry")
        assert fake.get(COUNTER_KEY) == "5"


# ---------- 3：成功步进重置计数（每节点独立预算） ----------


class TestCounterResetsOnSuccessfulAdvance:
    def test_fail_then_success_then_fail_counts_per_node(self):
        """显式三步：a 失败（计数 a=1）→ a 成功（**只**清 a）→ a 再失败
        （重新从 1 计），证明成功步进确实清零、且新预算是满的。

        键按节点维度（plaita#123）⇒ 断言落在 ``…:exec-1:a``。
        """
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake, MemoryExecutionStorage(), _flow_storage())
        state = _state(status="running")
        execution = MagicMock()

        seq = [
            # 步1：节点 a 失败（自动重试路径，计数 0→1）
            _node_failure_exc(),
            # 步2（重投后重跑）：a 成功推进 —— 成功步进清 a 的计数
            {"execution_id": "exec-1", "id": "a", "is_end": False, "is_suspend": False,
             "context": {"$LAST_NODE": "a", "$NODE": {"start": {}, "a": {}}}},
            # 步3：节点 a 再次失败 —— 应重新从 1 计（预算已清零）
            _node_failure_exc(),
        ]

        def scripted(flow, **kwargs):
            outcome = seq.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

        execution.run_distributed.side_effect = scripted

        # 第一次调用：步1 失败 → 自动重试放行（计数=1，状态不终态化）
        with pytest.raises(NodeExecutionRetryableError):
            worker._process_execution_result(
                _flow(), _initial_result(), state, execution
            )
        assert fake.get(COUNTER_KEY) == "1"

        # 第二次调用（模拟消息重投后重新处理）：步2 成功 → a 的计数清零；
        # 步3 失败 → 计数重新 =1，仍是可重试异常（而非 2→逼近耗尽）
        with pytest.raises(NodeExecutionRetryableError) as ei:
            worker._process_execution_result(
                _flow(), _initial_result(), state, execution
            )
        assert "第 1/" in str(ei.value)  # 全新预算：第 1 次重试
        assert fake.get(COUNTER_KEY) == "1"

    def test_success_of_other_node_does_not_clear_failing_node(self):
        """★ plaita#123 核心语义：**他节点**成功不得清掉失败节点的计数。

        序列：a 失败（计数 a=1）→ **b 成功推进**（b 的键被清，a 的保持）
        → a 再失败 ⇒ a 的计数必须是 **2**（累积），而不是被 b 的成功抹回 1。
        修复前：成功推进清**整个执行** ⇒ 永远是 1 ⇒ 预算永不耗尽 ⇒ 无限重投。
        """
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake, MemoryExecutionStorage(), _flow_storage())
        state = _state(status="running")
        execution = MagicMock()

        seq = [
            _node_failure_exc(),  # a 失败 → a=1
            # b 成功推进（他节点成功）——只应清 b 的键，不得碰 a
            {"execution_id": "exec-1", "id": "b", "is_end": False, "is_suspend": False,
             "context": {"$LAST_NODE": "b", "$NODE": {"start": {}, "a": {}, "b": {}}}},
            _node_failure_exc(),  # a 再失败 → a 应为 2
            # b 又成功推进
            {"execution_id": "exec-1", "id": "b", "is_end": False, "is_suspend": False,
             "context": {"$LAST_NODE": "b", "$NODE": {"start": {}, "a": {}, "b": {}}}},
            _node_failure_exc(),  # a 再失败 → a 应为 3
        ]

        def scripted(flow, **kwargs):
            outcome = seq.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

        execution.run_distributed.side_effect = scripted

        attempts = []
        for _ in range(3):
            try:
                worker._process_execution_result(
                    _flow(), _initial_result(), state, execution
                )
            except NodeExecutionRetryableError as e:
                attempts.append(e.attempt)
                continue
            break

        assert attempts == [1, 2, 3], (
            f"a 的连续失败计数必须累积（[1, 2, 3]），实际 {attempts}——"
            f"被 b 的成功清零即 #123 的无限重投形态"
        )
        assert fake.get(COUNTER_KEY) == "3"
        assert fake.get("plaita:execution:noderetry:exec-1:b") is None, (
            "b 成功推进应对 b 的键做幂等清零（不存在则 no-op）"
        )


# ---------- 4：指纹校验在 retry 唤醒之前 ----------


class TestHashCheckBeforeRetryWakeup:
    def test_hash_mismatch_on_retry_keeps_error_reretryable(self):
        """error + retry + 定义已改 → 指纹校验先于唤醒：执行保持 error（带
        hash 不匹配信息），修复定义后仍可再 retry——G1 语义不被指纹校验吞掉。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        flow_storage = _flow_storage()
        worker = _make_worker(fake, storage, flow_storage)
        state = _state(status="error")
        state.flow_hash = "0" * 64  # 与当前定义不符
        storage.save_execution_state("exec-1", state)

        with pytest.raises(FlowHashMismatchError):
            worker.resume_flow("f1", "exec-1", "retry")

        saved = storage.load_execution_state("exec-1")
        assert saved.status == "error"  # 未被翻转成 running
        assert "hash 不匹配" in saved.error["message"]

        # 修复「定义」（这里直接把状态指纹对齐）后再 retry → 正常唤醒推进
        saved.flow_hash = worker._compute_flow_hash(_flow())
        storage.save_execution_state("exec-1", saved)
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.run_distributed.return_value = {
                "execution_id": "exec-1", "is_end": True, "is_suspend": False,
                "result": "ok", "context": {"$LAST_NODE": "end", "$NODE": {}},
            }
            worker.resume_flow("f1", "exec-1", "retry")
        assert storage.load_execution_state("exec-1").status == "completed"


# ---------- 共用小工具 ----------


def _flow():
    from plaita.core.flow import Flow

    return Flow.model_validate(FLOW_DEF)


def _initial_result():
    return {
        "execution_id": "exec-1",
        "is_end": False,
        "is_suspend": False,
        "context": {"$LAST_NODE": "start", "$NODE": {"start": {}}},
    }
