"""plaita#123 根因：重试计数键按**执行**维度，却被「成功推进」整执行清零
⇒ 「前序节点稳定成功 + 某节点稳定失败」的 flow 预算永不耗尽 ⇒ 无限重投。

## 缺陷（2026-10-10 值守实测）

`_retry_counter_key` 只含 execution_id：

```
{ns}:execution:noderetry:{execution_id}          # 执行维度
```

而 `_process_execution_result` 的步进成功分支在每个节点推进后调
`_reset_node_retry_counter(execution_id)`，把**整个执行**的计数 DEL 掉。
注释却声称语义是「当前节点的**连续**失败次数，不同节点不共享预算」——
键粒度与声称语义不匹配。

**机制（2026-10-10 独立验收员用真实引擎重放确认，勿照抄早期误述）**：
`strategies.py` docstring 是 *"Execute one node per call"* —— 一次
`run_distributed` **只推进一个节点**。所以**不是**「跨轮被前序节点抹平」，
而是 **同一轮内**：

```
同一轮消息处理中：
  facts/triage 成功 → DEL noderetry（清整个执行）
  watch       失败 → INCR → 1
下一轮重投：三步重演 ⇒ 又多一条「第 1/5」
```

验收员按旧代码顺序重放 102 轮得 `[1,1,1,1,…]` 全 1，精确复现生产日志的
**「第 1/5」×102、「第 5/5」×0**。

⇒ 预算永不耗尽 ⇒ 永不终态化 ⇒ **无限重投**。

**实证（数字均为 2026-10-10 本机 Redis db1 快照，随时间变化，勿当常量）**：
`keeper-watch`（facts/triage 稳成功、watch 因 GLM 429 稳失败）累积 **667 个**
卡在 `running` 的悬停执行、最老 40 小时；对照全库 `noderetry=5` 的 39 个执行
全部正确终态化 `error`（证明「耗尽即终态化」本身没坏，坏的是计数到不了耗尽）。
采集命令：`SCAN plaita:execution:*` + 逐键读 `status`/`node_timings`。

## 反转验证（可复现步骤，**勿用 `git stash`**）

实现与测试**都已在提交内** ⇒ `git stash` 是 **no-op**，照做会拿到假的
「全绿」（静默无操作 + 测试本就绿）。正确做法：回退实现文件、**保留本测试**：

```bash
git checkout <pre-fix-sha> -- plaita/server/flow_worker.py \
    plaita/core/strategies.py plaita-console/backend/api/executions.py
env -u http_proxy -u https_proxy -u HTTP_PROXY -u HTTPS_PROXY \
  .venv/bin/python -m pytest tests/unit/test_issue123_node_scoped_retry_budget.py -q
# 期望：17 failed, 1 passed
#  （含 assert [1,1,1,1,1,1] == [1,2,3,4]、status 停在 running）
git checkout <fix-sha> -- plaita/server/flow_worker.py \
    plaita/core/strategies.py plaita-console/backend/api/executions.py
# 期望：21 passed（连同 test_issue123_execution_id_pinning.py）
```

注：`test_issue123_execution_id_pinning.py` 在反转态是 collection ImportError
（旧实现无 `_node_id_from_chain`），**不计入** 17。

## 修复契约

计数键加**节点维度**（失败节点 id 取自异常链的 `NodeExecutionError.node`），
成功推进只清**刚成功那一个节点**的键：

```
{ns}:execution:noderetry:{execution_id}:{node_id}
```

## 反证要求

本文件核心用例（``TestFailNodeBudgetExhaustsDespiteSuccesses``）构造
「节点 A 恒成功 + 节点 B 恒失败」的步进序列，断言 B 的计数**连续递增到
预算**并终态化 `error`。把实现退回「执行维度键 + 整执行清零」，该用例必须
变红（复现无限重投）。
"""
from unittest.mock import MagicMock

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.core.errors import FlowErrorException, NodeExecutionError
from plaita.server.flow_worker import (
    NodeExecutionRetryableError,
    RedisFlowWorker,
)
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

# 事故形态的最小复刻：A 是「稳定成功的前序节点」，B 是「稳定失败的节点」。
FLOW_DEF = {
    "flow_id": "f1",
    "version": "1",
    "nodes": [
        {"id": "start", "type": "start", "next": "a"},
        {"id": "a", "type": "assignment", "result": {"step": 1}, "next": "b"},
        {"id": "b", "type": "assignment", "result": {"step": 2}, "next": "end"},
        {"id": "end", "type": "end", "output": "ok"},
    ],
}

EXEC = "exec-1"
BASE_KEY = f"plaita:execution:noderetry:{EXEC}"


def _node_b_failure() -> FlowErrorException:
    """生产真实形态：B 节点执行失败，原始异常归一化后挂 ``__cause__``。

    ``NodeExecutionError.node`` 是 Flow Node 对象（runner.py:144），
    失败节点 id 从这里取——**不能**用 checkpoint 的 ``$LAST_NODE``：
    失败节点不写 context，那个值是**上一个成功节点**（正是本单的根因形态）。
    """
    node = MagicMock()
    node.id = "b"
    node.name = "b"
    err = NodeExecutionError("执行节点b出错了: ConnectionError: blip", node=node)
    wrapped = FlowErrorException(str(err))
    wrapped.__cause__ = err
    return wrapped


def _a_succeeded_result():
    """A 成功推进的那一步：distributed 每步只跑一个节点，result 带该步 id。"""
    return {
        "execution_id": EXEC,
        "id": "a",
        "is_end": False,
        "is_suspend": False,
        "context": {"$LAST_NODE": "a", "$NODE": {"start": {}, "a": {"step": 1}}},
    }


def _initial_result():
    return {
        "execution_id": EXEC,
        "id": "start",
        "is_end": False,
        "is_suspend": False,
        "context": {"$LAST_NODE": "start", "$NODE": {"start": {}}},
    }


def _make_worker(fake, storage=None) -> RedisFlowWorker:
    fs = MemoryFlowStorage()
    fs.save_flow(FLOW_DEF)
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:issue123-node-scope",
        execution_storage=storage if storage is not None else MemoryExecutionStorage(),
        flow_storage=fs,
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
    )


def _state() -> ExecutionState:
    return ExecutionState(
        execution_id=EXEC, flow_id="f1", flow_version="1", status="running",
        context={"$LAST_NODE": "start", "$NODE": {"start": {}}},
    )


def _flow():
    from plaita.core.flow import Flow

    return Flow.model_validate(FLOW_DEF)


class TestFailNodeBudgetExhaustsDespiteSuccesses:
    """★ 反证用例：A 恒成功、B 恒失败 ⇒ B 必须自己把预算烧到耗尽并终态化。"""

    def test_b_counter_reaches_budget_and_terminalizes(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        worker = _make_worker(fake, storage)
        state = _state()
        storage.save_execution_state(EXEC, state)
        execution = MagicMock()
        budget = worker._node_retry_budget()

        # 每一步：A 成功推进（触发清零）→ B 失败（触发计数）。
        # 序列长度 = 预算 + 1：预算内的每一步都应放行重试，第 budget 次失败
        # 必须终态化。
        seq = []
        for _ in range(budget + 1):
            seq.append(_a_succeeded_result())
            seq.append(_node_b_failure())

        def scripted(flow, **kwargs):
            outcome = seq.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

        execution.run_distributed.side_effect = scripted

        attempts = []
        for _ in range(budget + 2):
            if not seq:
                break
            try:
                # 每次调用模拟「消息重投后重新处理该执行」：从 checkpoint 继续。
                worker._process_execution_result(
                    _flow(), _initial_result(), state, execution=execution
                )
            except NodeExecutionRetryableError as e:
                attempts.append(e.attempt)
                continue
            # 步进循环正常返回（无异常）= 该轮已终态化 error 并 break。
            # 注：`_process_execution_result` 的终态化路径是 **break 正常返回**
            # （plaita#73 的对称性钉子，见 test_issue73_terminalize_ack），只有
            # `resume_flow` 才抛 NodeFailureTerminalizedError。
            break

        assert attempts == list(range(1, budget)), (
            f"B 的重试次数必须逐次递增到预算前一位（{list(range(1, budget))}），"
            f"实际 {attempts}——不递增即「每轮被前序节点成功清零」的无限重投形态"
        )
        saved = storage.load_execution_state(EXEC)
        assert saved.status == "error", (
            "B 的计数耗尽后必须终态化 error，否则执行永久悬停 running、"
            "消息无限重投（plaita#123 事故本体）"
        )
        assert saved.error.get("node_retries") == budget - 1
        assert saved.error.get("failed_node") == "b"

    def test_per_node_scope_is_exactly_what_makes_it_converge(self):
        """收敛性归因：把「成功时清整执行」的旧行为**显式重放**在同一序列上，
        计数永远停在 1 ⇒ 永不耗尽。这就是 #123 的红色形态。

        做法：先按新语义写键（节点维度），再模拟旧代码的「成功推进清整执行」
        （DEL 执行维度键 + 遍历删除所有子键）——即修复前的清零范围。断言此时
        计数不再累积。核心用例（上一条）依赖的正是「清零范围收窄到单节点」，
        两者互为对照：清零范围一旦回到执行维度，预算就烧不完。
        """
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        budget = worker._node_retry_budget()

        attempts = []
        for _ in range(budget + 3):
            # ① 前序节点 A 成功推进 —— 修复前的清零范围 = 整个执行
            fake.delete(BASE_KEY, *fake.scan_iter(match=f"{BASE_KEY}:*"))
            # ② 失败节点 B 计数
            decision = worker._node_failure_retry_decision(
                _node_b_failure(), EXEC, 1, "b"
            )
            if isinstance(decision, NodeExecutionRetryableError):
                attempts.append(decision.attempt)
            else:
                break

        assert len(attempts) == budget + 3, (
            "整执行清零下 B 永远不会耗尽预算（这正是无限重投的机制）"
        )
        assert set(attempts) == {1}, (
            f"整执行清零 ⇒ B 的计数每轮被抹平、恒为 1，实际 {attempts}。"
            f"反过来说：只有把清零范围收窄到「刚成功的那一个节点」，"
            f"计数才可能累积到预算——这就是本单的修复点"
        )

    def test_per_node_keys_are_isolated(self):
        """键必须带节点维度：B 的失败写 ``…:{exec}:b``，**不**写执行维度键。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        worker._node_failure_retry_decision(_node_b_failure(), EXEC, 1, "b")
        assert fake.get(f"{BASE_KEY}:b") == "1"
        assert fake.get(BASE_KEY) is None, (
            "节点维度键必须与执行维度键分离——共用执行维度键就是本单根因"
        )

    def test_success_resets_only_that_node(self):
        """A 成功只清 A 的键，B 的计数不受影响（不同节点不共享预算）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        fake.set(f"{BASE_KEY}:a", "3")
        fake.set(f"{BASE_KEY}:b", "4")

        worker._reset_node_retry_counter(EXEC, "a")

        assert fake.get(f"{BASE_KEY}:a") is None
        assert fake.get(f"{BASE_KEY}:b") == "4", (
            "A 的成功不得清掉 B 的失败计数——那正是 #123 的无限重投根因"
        )

    def test_failure_without_resolvable_node_falls_back_to_execution_dimension(self):
        """失败节点定位不到（链中无 NodeExecutionError / node=None）→ 退化执行
        维度键，保守但绝不比修复前差。"""
        node = MagicMock()
        node.id = None
        node.name = None
        err = NodeExecutionError("boom", node=node)
        wrapped = FlowErrorException(str(err))
        wrapped.__cause__ = err

        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        worker._node_failure_retry_decision(wrapped, EXEC, 1, None)
        assert fake.get(BASE_KEY) == "1"


class TestG1WakeupStillResetsWholeExecution:
    """G1 retry 唤醒是**执行级**意图：必须清掉该执行下所有节点键。"""

    def test_wakeup_clears_every_per_node_key(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        fake.set(BASE_KEY, "1")
        fake.set(f"{BASE_KEY}:a", "2")
        fake.set(f"{BASE_KEY}:b", "5")

        worker._reset_node_retry_counter(EXEC)  # node_id 缺省 = 全清

        assert fake.get(BASE_KEY) is None
        assert fake.get(f"{BASE_KEY}:a") is None
        assert fake.get(f"{BASE_KEY}:b") is None

    def test_wakeup_does_not_touch_other_executions(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        fake.set(f"{BASE_KEY}:b", "5")
        fake.set("plaita:execution:noderetry:other-exec:b", "5")

        worker._reset_node_retry_counter(EXEC)

        assert fake.get(f"{BASE_KEY}:b") is None
        assert fake.get("plaita:execution:noderetry:other-exec:b") == "5"


class TestLegacyExecutionScopedKeysDoNotAffectNewLogic:
    """存量旧键（无节点维度）不迁移、靠 TTL 过期；**不得**影响新逻辑。"""

    def test_legacy_key_does_not_grant_budget_to_new_node_key(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        fake.set(BASE_KEY, "4")  # 存量旧键（旧版本写下的）

        out = worker._node_failure_retry_decision(_node_b_failure(), EXEC, 1, "b")

        assert isinstance(out, NodeExecutionRetryableError)
        assert out.attempt == 1, (
            "旧键不得参与新语义的预算判定——否则旧执行被错误地少给预算"
        )
        assert fake.get(f"{BASE_KEY}:b") == "1"

    def test_legacy_key_alone_does_not_exhaust_budget(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        fake.set(BASE_KEY, "5")  # 旧键已达预算值

        out = worker._node_failure_retry_decision(_node_b_failure(), EXEC, 1, "b")
        assert isinstance(out, NodeExecutionRetryableError), (
            "旧键不应让新节点一上来就『预算耗尽』而立即终态化"
        )


class TestDeadLetterGuardSeesPerNodeKeys:
    """死信守卫的后备判据：取该执行下所有节点键的最大值。"""

    def test_max_counter_uses_per_node_keys(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        fake.set(f"{BASE_KEY}:a", "2")
        fake.set(f"{BASE_KEY}:b", "5")
        assert worker._max_node_retry_counter(EXEC) == 5

    def test_guard_finalizes_when_per_node_counter_exhausted(self):
        from plaita.server.task_queue import StreamTask

        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        worker = _make_worker(fake, storage)
        storage.save_execution_state(EXEC, _state())
        fake.set(f"{BASE_KEY}:b", str(worker._node_retry_budget()))

        task = StreamTask(
            message_id="m1",
            body={"type": "resume", "execution_id": EXEC, "tenant_id": "default"},
            delivery_count=5,
        )
        assert worker._dead_letter_guard(task) is True
        assert storage.load_execution_state(EXEC).status == "error"


class TestRetryableSignalCarriesFailedNodeObservability:
    """可观测：日志/异常能指出「哪个节点在反复失败」。"""

    def test_decision_logs_node_id(self, caplog):
        import logging

        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        with caplog.at_level(logging.WARNING):
            worker._node_failure_retry_decision(_node_b_failure(), EXEC, 1, "b")
        assert "节点 b 执行失败" in caplog.text


class TestFallbackTotalRetryCap:
    """★ 兜底上限（评审建议）：**不看键名、只看次数**。

    节点维度预算依赖「键里的节点段稳定」。任何键空间漂移（节点 id 动态变化、
    flow 定义被改、将来又把 execution_id 引回键）都会让单节点预算失效 ⇒
    退回无界重投。该计数只按 execution_id 累计，达上限即终态化——保证
    「预算机制整体失效」时仍有界。
    """

    def test_total_counter_increments_and_has_ttl(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        n1 = worker._record_node_retry_total(EXEC)
        n2 = worker._record_node_retry_total(EXEC)
        assert (n1, n2) == (1, 2)
        key = "plaita:execution:noderefetch:exec-1"
        assert 0 < fake.ttl(key) <= 7 * 86400

    def test_cap_exhaustion_blocks_retry_even_with_rotating_node_ids(self):
        """键空间漂移（每轮换一个 node_id）时，单节点预算永远烧不完 ——
        兜底总上限必须把执行拦住。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        cap = worker.NODE_RETRY_TOTAL_MAX

        # 预置到上限前一位：下一轮 decision 自增到 cap → 必须返回 None
        fake.set("plaita:execution:noderefetch:exec-1", str(cap - 1))
        # 每轮都换 node_id ⇒ 单节点键永远是新的 1，走不到节点预算
        out = worker._node_failure_retry_decision(
            _node_b_failure(), EXEC, 1, "rotating-node-999"
        )
        assert out is None, (
            f"兜底总上限（{cap}）达限后必须拦下重试——否则键空间漂移即无界重投"
        )
        assert worker._node_retry_total_exhausted(EXEC) is True

    def test_below_cap_still_allows_retry(self):
        """未达兜底上限时不干预正常路径（不误伤）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        out = worker._node_failure_retry_decision(_node_b_failure(), EXEC, 1, "b")
        assert isinstance(out, NodeExecutionRetryableError)
        assert worker._node_retry_total_exhausted(EXEC) is False

    def test_no_redis_client_never_blocks(self):
        """无 redis（内存 worker/单测）→ 读不到计数，永不判达限（保守）。"""
        worker = _make_worker(fakeredis.FakeRedis(decode_responses=True))
        worker.redis_client = None
        assert worker._node_retry_total_exhausted(EXEC) is False

    def test_guard_finalizes_on_fallback_cap_alone(self):
        """死信守卫：**单节点键都没达预算**，但兜底总上限已达 ⇒ 仍须终态化
        后放行死信（消息层最后一道闸不能只有会失效的那条判据）。"""
        from plaita.server.task_queue import StreamTask

        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        worker = _make_worker(fake, storage)
        storage.save_execution_state(EXEC, _state())
        fake.set("plaita:execution:noderefetch:exec-1", str(worker.NODE_RETRY_TOTAL_MAX))
        # 注意：**不**预置任何 per-node 达预算的键
        assert worker._max_node_retry_counter(EXEC) == 0

        task = StreamTask(
            message_id="m1",
            body={"type": "resume", "execution_id": EXEC, "tenant_id": "default"},
            delivery_count=5,
        )
        assert worker._dead_letter_guard(task) is True
        assert storage.load_execution_state(EXEC).status == "error"

    def test_guard_below_both_caps_reenqueues(self):
        """两条判据都未达限 → 维持重入队恢复路径（不误杀）。"""
        from plaita.server.task_queue import StreamTask

        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        worker = _make_worker(fake, storage)
        storage.save_execution_state(EXEC, _state())
        fake.set("plaita:execution:noderefetch:exec-1", "3")

        task = StreamTask(
            message_id="m1",
            body={"type": "resume", "execution_id": EXEC, "tenant_id": "default"},
            delivery_count=5,
        )
        assert worker._dead_letter_guard(task) is True
        assert storage.load_execution_state(EXEC).status == "running"
        assert fake.xlen("test:issue123-node-scope") == 1  # 已重入队
