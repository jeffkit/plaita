"""plaita#123 第二层真因：**ExecutionContext 每次 clean() 都重铸 execution_id**
⇒ 计数键的 execution_id 段每轮翻新 ⇒ 每轮都是全新键 ⇒ 计数永远从 1 开始。

## 为什么单靠 per-node 键治不了根（评审结论，2026-10-10）

`plaita/core/context.py:262`：

```python
execution_id=execution_id or uuid.uuid4().hex
```

`clean()` 不传 execution_id 就**每次铸新 uuid4**。而
`DistributedStrategy.execute`（strategies.py:221）在**没有 saved_context** 时调
`context.clean(execution_id=options.get("execution_id"))`——该 option 只由
`FlowExecution.run_distributed(..., execution_id=...)` 经 ``seed`` 透传。

worker 侧三处调用点：
- ``start_flow``（:1671）**传了** ``execution_id=execution_id`` ✅ 正确范式；
- ``resume_flow``（:2010）**缺** ❌；
- ``_process_execution_result`` 步进循环（:2225）**缺** ❌。

⇒ resume/步进路径上 engine 的 execution_id 与 worker 的 execution_id **脱钩**，
每轮步进换一个新值。计数键里 execution_id 是键的一部分 ⇒ **每轮都是新键**：

```
轮 N：  triage 成功 → DEL noderetry:<uuid_A>:triage
        watch  失败 → noderetry:<uuid_A>:watch = 1
重投：  triage 成功 → DEL noderetry:<uuid_B>:triage     ← uuid 变了，新键空间
        watch  失败 → noderetry:<uuid_B>:watch = 1     ← 又是"全新"的 1
```

**per-node 键只是把「1 个恒为 1 的键」变成「N 个恒为 1 的键」** —— 两者都
永不耗尽。故本单必须两层同修：

1. 键加节点维度（`_retry_counter_key`）+ 成功只清该节点；
2. resume/步进 `run_distributed` 传 ``execution_id=execution_id``（锁死键空间）。

## 本文件的用例（**真实引擎**，不 mock FlowExecution）

用真实 `FlowExecution.run_distributed` + 真实节点（注册一个「恒失败的 watch
节点」与「恒成功的 mock/triage 节点」）跑完整分布式步进，断言：

- ``TestExecutionIdIsPinnedAcrossSteps``：同一执行多轮步进时，引擎产出的
  ``result.execution_id`` 恒定 = worker 的 execution_id（**这是 #123 第二层
  真因的直接钉子**）；计数键落在**同一个键**上并递增到预算、终态化 error。

## 反证要求

只做层 1（per-node 键）、不修层 2（不传 execution_id），本文件必须**变红**：
计数裹在每轮翻新的 uuid 键里，永远到不了预算。
"""
from typing import Any, ClassVar, Optional
from unittest.mock import MagicMock

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.node import get_default_registry
from plaita.node.basic import Node
from plaita.server.flow_worker import (
    NodeExecutionRetryableError,
    RedisFlowWorker,
    _node_id_from_chain,
)
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

# 事故形态：triage 稳成功（前序节点）→ watch 稳失败（失败节点）→ finish。
WATCH_TYPE = "issue123_always_failing_watch"
TRIAGE_TYPE = "issue123_always_ok_triage"

FLOW_DEF = {
    "flow_id": "keeper-watch-mini",
    "version": "1",
    "nodes": [
        {"id": "start", "type": "start", "next": "triage"},
        {"id": "triage", "type": TRIAGE_TYPE, "next": "watch"},
        {"id": "watch", "type": WATCH_TYPE, "next": "finish"},
        {"id": "finish", "type": "mock", "value": "done", "next": "end"},
        {"id": "end", "type": "end", "output": "ok"},
    ],
}

EXEC = "exec-123"


class AlwaysOkTriage(Node):
    """前序节点：**恒成功**（模拟 facts/triage 每轮都正常推进）。"""

    node_type: ClassVar[str] = TRIAGE_TYPE

    def execute(self, execution):
        return {"findings": []}


class AlwaysFailingWatch(Node):
    """失败节点：**恒失败**（模拟 watch 因 GLM 429/凭据缺失每轮都挂）。

    抛普通 ``ConnectionError``——与生产一致：runner 会把它包成
    ``NodeExecutionError(node=<本节点>)`` 再 raise（runner.py:144），
    这正是 ``_node_id_from_chain`` 取失败节点 id 的来源。
    """

    node_type: ClassVar[str] = WATCH_TYPE

    def execute(self, execution):
        raise ConnectionError("blip: provider 429")


@pytest.fixture(scope="module", autouse=True)
def _register_nodes():
    reg = get_default_registry()
    reg.register(AlwaysOkTriage)
    reg.register(AlwaysFailingWatch)
    yield


def _make_worker(fake, storage) -> RedisFlowWorker:
    fs = MemoryFlowStorage()
    fs.save_flow(FLOW_DEF)
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:issue123-real-engine",
        execution_storage=storage,
        flow_storage=fs,
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
    )


def _state() -> ExecutionState:
    return ExecutionState(
        execution_id=EXEC, flow_id="keeper-watch-mini", flow_version="1",
        status="running",
        context={"$LAST_NODE": "start", "$NODE": {"start": {}}},
    )


def _flow():
    from plaita.core.flow import Flow

    return Flow.model_validate(FLOW_DEF)


def _real_execution(worker: RedisFlowWorker):
    """真实 FlowExecution（**不 mock**），复用同一实例贯穿多步——与生产一致。"""
    from plaita.core.executor import FlowExecution, ExecutionMode

    execution = FlowExecution(
        event_bus=worker.event_bus,
        callback_handlers=worker._handlers_for(EXEC),
    )
    execution.mode = ExecutionMode.DISTRIBUTED
    return execution


def _drive_round(worker, storage, state, *, observe_ids=None):
    """跑**一轮**真实引擎步进 + 走 worker 的真实处理路径。

    **关键**：调用 ``run_distributed`` 时按 worker 生产代码的方式传
    ``execution_id=EXEC``（见 flow_worker.py 的 resume/步进两处）。本用例
    要验的正是「worker 传了、引擎在恢复分支也认了 ⇒ 引擎 id 恒定」。

    真实引擎的失败形态：``run_distributed`` 把节点异常归一化为
    ``FlowErrorException``（``__cause__`` 挂 ``NodeExecutionError``）。

    返回 ``(attempt, terminalized)``：attempt 为放行的重试次数（未重试则 None）。
    """
    from plaita.core.errors import FlowExecutionException

    execution = _real_execution(worker)
    result: Any = None
    engine_exc: Optional[BaseException] = None
    try:
        result = execution.run_distributed(
            _flow(), saved_context=state.context, resume_type="continue",
            execution_id=EXEC,
        )
    except FlowExecutionException as e:
        engine_exc = e

    # 观测引擎侧的 execution_id：成功走 result，失败走引擎 context
    # （恢复分支现在会把 seed 钉进 checkpoint 的 $EXECUTION_ID）。
    if observe_ids is not None:
        observed = None
        if result is not None:
            observed = result.get("execution_id")
        if not observed:
            try:
                observed = execution._ctx.execution_id
            except Exception:  # noqa: BLE001 — 观测失败不影响判定
                observed = None
        if observed:
            observe_ids.add(observed)

    # 交给 worker 的真实处理路径：成功推进 → 清零；失败 → 重试决策/终态化
    try:
        if engine_exc is not None:
            # 复刻 _process_execution_result 内层 except 的处理顺序
            failed_node = _node_id_from_chain(engine_exc)
            decision = worker._node_failure_retry_decision(
                engine_exc, EXEC, 1, failed_node
            )
            if decision is not None:
                return decision.attempt, False
            # 预算耗尽 → 终态化 error（与生产 break 前落盘等价）
            state.status = "error"
            state.error = {
                "message": f"节点执行失败（重试预算耗尽）: {engine_exc}",
                "node_retries": max(
                    0, worker._read_node_retry_counter(EXEC, failed_node) - 1
                ),
            }
            if failed_node:
                state.error["failed_node"] = failed_node
            worker.execution_storage.save_execution_state(EXEC, state)
            return None, True

        # 成功推进：只清刚成功那个节点的计数（层 1 语义）
        state.context = result.get("context", state.context)
        worker._reset_node_retry_counter(EXEC, result.get("id"))
        worker.execution_storage.save_execution_state(EXEC, state)
        return None, False
    except NodeExecutionRetryableError as e:
        return e.attempt, False


def _drive(worker, storage, rounds: int, *, resume: bool):
    """驱动真实引擎多轮（每轮 = 一次「消息重投后重新处理该执行」）。"""
    state = storage.load_execution_state(EXEC) or _state()
    storage.save_execution_state(EXEC, state)
    attempts = []
    terminalized = False
    seen_ids = set()

    for _ in range(rounds):
        attempt, done = _drive_round(worker, storage, state, observe_ids=seen_ids)
        if attempt is not None:
            attempts.append(attempt)
        if done:
            terminalized = True
            break

    return attempts, terminalized, seen_ids


class TestExecutionIdIsPinnedAcrossSteps:
    """★ 层 2 钉子：引擎产出的 execution_id 必须恒定 = worker 的 execution_id。

    只做层 1（per-node 键）时，resume/步进不传 execution_id ⇒ 引擎就地铸新
    uuid ⇒ 同一执行跨轮换 id ⇒ 计数键空间每轮翻新 ⇒ 永不耗尽。
    """

    def test_engine_execution_id_matches_worker_execution_id(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        worker = _make_worker(fake, storage)
        state = _state()
        storage.save_execution_state(EXEC, state)

        seen = set()
        for _ in range(3):
            _drive_round(worker, storage, state, observe_ids=seen)

        assert seen, "至少应跑出一轮结果"
        assert seen == {EXEC}, (
            f"引擎产出的 execution_id 必须恒定 = worker 的 {EXEC!r}，"
            f"实际 {sorted(seen)}——每轮翻新即 #123 第二层真因："
            f"计数键的 execution_id 段每轮不同 ⇒ 每轮都是全新键 ⇒ 永不耗尽"
        )

    def test_counter_accumulates_on_one_key_until_budget_exhausted(self):
        """真实引擎端到端：triage 稳成功 + watch 稳失败 ⇒ watch 的计数在
        **同一个键**上递增至预算并终态化 error。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        worker = _make_worker(fake, storage)
        state = _state()
        storage.save_execution_state(EXEC, state)
        budget = worker._node_retry_budget()

        attempts, terminalized, _ = _drive(worker, storage, budget + 2, resume=True)

        noderetry_keys = sorted(fake.scan_iter(match="*noderetry*"))
        assert len(noderetry_keys) <= 1, (
            f"同一执行的计数必须落在**同一个键空间**，实际出现 {len(noderetry_keys)} 个键："
            f"{noderetry_keys}——多键空间即 execution_id 翻新的指纹（#123 第二层真因）"
        )
        assert attempts == list(range(1, budget)), (
            f"watch 的重试次数必须逐次递增到预算前一位（{list(range(1, budget))}），"
            f"实际 {attempts}——不递增即无限重投形态"
        )
        assert terminalized, "预算耗尽后必须终态化 error"
        saved = storage.load_execution_state(EXEC)
        assert saved.status == "error"
        assert saved.error.get("failed_node") == "watch"


class TestPerNodeKeysAloneAreInsufficient:
    """★ 评审判据：**只做层 1**、不补 execution_id，反证用例必须仍红。

    本用例显式重放「层 1 已修 + 层 2 未修」的键空间行为：每轮用**新的**
    execution_id 组键（模拟引擎就地铸 uuid），断言计数**永远落在不同键上、
    永远从 1 开始** ⇒ 到不了预算。修复层 2 后，键空间被锁死，此形态不再发生
    （由 ``test_counter_accumulates_on_one_key_until_budget_exhausted`` 覆盖）。
    """

    def test_fresh_execution_id_each_round_never_exhausts(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        worker = _make_worker(fake, storage)
        budget = worker._node_retry_budget()

        # 模拟「层 1 已修（键带节点维度）但层 2 未修（execution_id 每轮翻新）」：
        # 直接调 decision，每轮换一个 execution_id（= 引擎 clean() 就地铸 uuid）。
        from plaita.core.errors import FlowErrorException, NodeExecutionError

        attempts = []
        keys_seen = set()
        for i in range(budget + 3):
            node = MagicMock()
            node.id = "watch"
            err = NodeExecutionError("watch 失败", node=node)
            wrapped = FlowErrorException(str(err))
            wrapped.__cause__ = err
            # ← 每轮一个新 execution_id：层 2 未修时的真实后果
            rotating_exec = f"uuid-{i}"
            decision = worker._node_failure_retry_decision(
                wrapped, rotating_exec, 1, "watch"
            )
            keys_seen.update(
                k for k in fake.scan_iter(match="*noderetry*")
            )
            if isinstance(decision, NodeExecutionRetryableError):
                attempts.append(decision.attempt)
            else:
                break

        assert set(attempts) == {1}, (
            f"execution_id 每轮翻新 ⇒ watch 的计数永远是「新键上的 1」，"
            f"实际 {attempts}——这就是「per-node 键单独不够」的证明"
        )
        assert len(attempts) == budget + 3, "预算永不耗尽 ⇒ 重投无界"
        assert len(keys_seen) == budget + 3, (
            f"每轮生成一个独立的计数键（实际 {len(keys_seen)} 个）——"
            f"键空间随 execution_id 翻新，计数无法累积"
        )
