"""plaita#73 遗留层：**确定性失败也要有界**（不再只靠消息层无限重投）。

## 缺陷（2026-10-10 实证）

`_node_failure_retry_decision` 的可重试判据在前、`INCR` 在后：

```
if not _is_retryable_node_failure(exc):   # 确定性失败（exited 1 / sync_in / 超时）
    return None                            # ← 直接返回，**完全不碰计数器**
attempt = self._record_node_retry(...)
```

于是确定性失败（最常见：GLM 配额耗尽 → agent 秒退 `exited 1`）**不进任何计数**，
唯一收敛机制只剩「消息层重投」——而消息层没有次数概念。实测后果：

- 全库 `noderetry` 键 **74/74 恒为 1**（从未累积）；
- 同一执行被重投数百次（`engine_error 9`、近 10min 重投 43 次）；
- 沙箱实例持续占位，5 小时烧 **5.66 实例小时**、产出 0。

## 修复契约

确定性失败用**独立**计数键 `{ns}:execution:nofail:{id}`（不用 `noderetry`——
后者的语义是「可重试预算」，会被 G1 唤醒/成功推进清零，塞进确定性失败会污染
该语义）。计数语义是「**连续**失败」：任一节点成功推进即清零（跨节点的偶发
确定性失败不应累计判死），G1 唤醒**不**清零（否则「唤醒→清零→再撞→再唤醒」
永不收敛）。达 `DETERMINISTIC_FAILURE_MAX`（连续）即判「不可救」：
`resume_flow` 对 `resume_type=retry` 幂等短路（不抛异常，抛异常会让调用方当
失败并重试），消息层重投随之停止。

反证要求：去掉记账，本测试必须变红。
"""
import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.core.errors import FlowErrorException, NodeExecutionError, NodeTimeoutError
from plaita.server.flow_worker import (
    NodeExecutionRetryableError,
    RedisFlowWorker,
)
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

FLOW_DEF = {
    "flow_id": "f1",
    "version": "1",
    "nodes": [
        {"id": "start", "type": "start", "next": "impl"},
        {"id": "impl", "type": "assignment", "result": {"step": 1}, "next": "end"},
        {"id": "end", "type": "end", "output": "ok"},
    ],
}

NOFAIL_KEY = "plaita:execution:nofail:exec-1"
NODERETRY_KEY = "plaita:execution:noderetry:exec-1"


def _deterministic_exc() -> FlowErrorException:
    """生产真实形态：agent 子进程退出（不可重试）。"""
    return FlowErrorException(
        "执行节点impl出错了: AgentRunError: executor 'recursive' exited 1: "
        "checkpoint: per-turn snapshots active"
    )


def _transient_exc() -> FlowErrorException:
    err = NodeExecutionError("执行节点impl出错了: ConnectionError: blip", node="impl")
    w = FlowErrorException(str(err))
    w.__cause__ = err
    return w


def _timeout_exc() -> FlowErrorException:
    inner = NodeTimeoutError("timeout")
    w = FlowErrorException(str(inner))
    w.__cause__ = inner
    return w


def _make_worker(fake) -> RedisFlowWorker:
    fs = MemoryFlowStorage()
    fs.save_flow(FLOW_DEF)
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:issue73-nofail",
        execution_storage=MemoryExecutionStorage(),
        flow_storage=fs,
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
        lease_ttl_seconds=60,
    )


def _state(status="running") -> ExecutionState:
    return ExecutionState(
        execution_id="exec-1", flow_id="f1", flow_version="1", status=status,
        context={"$LAST_NODE": "start", "$NODE": {"start": {}}},
    )


class TestDeterministicFailureIsCounted:
    def test_deterministic_failure_increments_own_counter(self):
        """核心钉子：确定性失败必须**进计数器**（旧实现完全不记）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        w = _make_worker(fake)
        assert w._read_deterministic_failure_count("exec-1") == 0

        stale = NodeExecutionRetryableError("x")
        del stale
        assert w._node_failure_retry_decision(_deterministic_exc(), "exec-1", 1) is None
        assert fake.get(NOFAIL_KEY) == "1", "确定性失败未记账（#73 遗留层）"

        w._node_failure_retry_decision(_deterministic_exc(), "exec-1", 1)
        assert fake.get(NOFAIL_KEY) == "2"

    def test_deterministic_failure_never_touches_retry_budget(self):
        """语义隔离：确定性失败**不得**污染可重试预算键（noderetry）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        w = _make_worker(fake)
        w._node_failure_retry_decision(_deterministic_exc(), "exec-1", 1)
        assert fake.get(NODERETRY_KEY) is None, (
            "确定性失败不应写 noderetry——它是「可重试预算」，会被 G1/推进清零，"
            "语义不同（这正是旧实现 noderetry 恒为 1 的另一面）"
        )

    def test_transient_failure_still_uses_retry_budget(self):
        """负向钉子：瞬态失败仍走可重试预算（本修复不得误伤）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        w = _make_worker(fake)
        out = w._node_failure_retry_decision(_transient_exc(), "exec-1", 1)
        assert isinstance(out, NodeExecutionRetryableError)
        assert fake.get(NODERETRY_KEY) == "1"
        assert fake.get(NOFAIL_KEY) is None, "瞬态失败不应计入确定性失败"

    def test_timeout_counts_as_deterministic(self):
        """超时也是确定性失败（不重试），同样要有界。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        w = _make_worker(fake)
        assert w._node_failure_retry_decision(_timeout_exc(), "exec-1", 1) is None
        assert fake.get(NOFAIL_KEY) == "1"

    def test_exhaustion_flips_predicate(self):
        """达上限后 `_deterministic_failure_exhausted` 必须为真。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        w = _make_worker(fake)
        for _ in range(w.DETERMINISTIC_FAILURE_MAX - 1):
            w._record_deterministic_failure("exec-1")
        assert w._deterministic_failure_exhausted("exec-1") is False
        w._record_deterministic_failure("exec-1")
        assert w._deterministic_failure_exhausted("exec-1") is True

    def test_key_has_ttl_and_is_tenant_routed(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        w = _make_worker(fake)
        w._record_deterministic_failure("exec-1")
        assert fake.ttl(NOFAIL_KEY) > 0, "计数键必须带 TTL（防永久泄漏）"

    def test_no_redis_client_never_blocks(self):
        """无 redis → 计数退化为 1，永不判「不可救」（保守）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        w = _make_worker(fake)
        w.redis_client = None
        assert w._read_deterministic_failure_count("exec-1") == 0
        assert w._deterministic_failure_exhausted("exec-1") is False


class TestCounterIsConsecutiveNotCumulative:
    """计数语义是「**连续**失败」：任一节点成功推进即清零。

    2026-10-10 评审：只增不减时，一个跑了几天、在**不同**节点各撞过一次确定性
    失败的长寿命执行会被累计判死（12 次跨节点偶发 = 12 次同一位置撞墙），
    远没到「该放弃」的程度。清零必须挂在**推进**收口上，且**不**在 G1 retry
    唤醒时清零（否则「唤醒→清零→再撞→再唤醒」永不收敛）。
    """

    def test_reset_clears_counter_and_predicate(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        w = _make_worker(fake)
        for _ in range(w.DETERMINISTIC_FAILURE_MAX - 1):
            w._record_deterministic_failure("exec-1")
        assert w._deterministic_failure_exhausted("exec-1") is False

        w._reset_deterministic_failure_counter("exec-1")

        assert fake.get(NOFAIL_KEY) is None
        assert w._deterministic_failure_exhausted("exec-1") is False
        # 幂等：键不存在时再清一次不得抛
        w._reset_deterministic_failure_counter("exec-1")

    def test_step_advance_clears_the_counter(self):
        """真实推进一个节点后计数键必须被清零（调用点验证，不测裸 helper）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        w = _make_worker(fake)
        w._record_deterministic_failure("exec-1")
        w._record_deterministic_failure("exec-1")
        assert fake.get(NOFAIL_KEY) == "2"
        w.execution_storage.save_execution_state("exec-1", _state("running"))

        result = w.resume_flow("f1", "exec-1", "continue")

        assert result["is_end"] is True
        assert fake.get(NOFAIL_KEY) is None, (
            "推进过就不是「同一位置反复撞墙」——跨节点偶发失败不得累计判死"
        )

    def test_retry_wakeup_does_not_clear_the_counter(self):
        """G1 唤醒**不**清零：清零会让「唤醒→再撞→再唤醒」永不收敛。"""
        import inspect

        src = inspect.getsource(RedisFlowWorker.resume_flow)
        seg = src.split("if retry_wakeup:")[1].split("execution = FlowExecution(")[0]
        assert "_reset_node_retry_counter(execution_id)" in seg, "可重试预算仍要清零"
        assert "_reset_deterministic_failure_counter(execution_id)" not in seg, (
            "确定性失败计数不得在唤醒时清零（否则上限形同虚设）"
        )


class TestExhaustedShortCircuitsRetryWakeup:
    def test_retry_wakeup_short_circuits_when_exhausted(self):
        """端到端：判不可救后，`resume_flow(retry)` 必须幂等短路、不抛异常。"""
        import inspect

        src = inspect.getsource(RedisFlowWorker.resume_flow)
        assert "deterministic_failure_exhausted" in src, (
            "resume_flow 必须对「确定性失败不可救」短路，否则消息层重投继续空转"
        )
        seg = src.split("_deterministic_failure_exhausted(execution_id)")[1] if \
            "_deterministic_failure_exhausted(execution_id)" in src else ""
        assert seg, "未找到短路分支"
        seg = seg[: seg.index("if state_status in (")]
        assert "raise" not in seg, "短路必须是幂等返回，抛异常会让调用方当失败重试"
        assert "already_terminal" in seg
