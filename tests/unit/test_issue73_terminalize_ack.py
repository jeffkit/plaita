"""plaita#73 回归：节点失败**终态化后必须 poison ack**，不得无限重投。

## 缺陷（2026-10-10 值守 Agent 实证）

``resume_flow`` 的终态化路径先落盘 ``status=error``，然后抛**裸
``RuntimeError``**（旧代码 ``flow_worker.py:1736``）。而 ``run()`` 的
poison ack 分支只认 ``ValueError``（``:2847``，"畸形消息：ack 掉避免
poison pill 无限重投"），``RuntimeError`` 落到**兜底 ``except Exception``**
——那条分支**不 ack**、留 pending 等租约超时回收 ⇒ 消息被反复重投 ⇒
``resume_flow`` 见 ``status=error`` 只放行 ``resume_type=retry`` ⇒ 再撞
同一确定性失败 ⇒ 再次终态化 + 再次 ``RuntimeError``……

**状态早已 error，消息却永远在转**：实测同一执行重投 598 次、沙箱实例
持续占位 5.66 实例小时（产出 0）。

## 实测证据（静态快照，一条命令定性）

```
$ redis-cli --scan --pattern "*noderetry*" | while read k; do redis-cli GET $k; done | sort | uniq -c
     74 1          ← 全库 74 个计数键无例外全为 1；TTL 各异 ⇒ 每键只 INCR 过 1 次
```

原因：``INCR`` 在 ``_node_failure_retry_decision`` 内、位于
``_is_retryable_node_failure`` 判据**之后**；确定性失败（``exited 1`` /
``AgsError: sync_in`` / 超时）判 False ⇒ **根本不碰计数键** ⇒
「预算耗尽」分支从未触发 ⇒ 重投无界。

## 修复契约

终态化路径抛 ``NodeFailureTerminalizedError``（``ValueError`` 子类，与既有
``FlowHashMismatchError`` 同族）：语义是「**raise 前状态已终态化**」，
``run()`` 按 ``ValueError`` poison ack —— 终态已可观测，重投只会命中
``already_terminal`` 短路，纯属浪费。

反证要求：把实现退回裸 ``RuntimeError``，本测试必须变红。
"""
import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.core.errors import FlowErrorException, NodeExecutionError
from plaita.server.flow_worker import (
    FlowHashMismatchError,
    NodeFailureTerminalizedError,
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


def _deterministic_failure() -> FlowErrorException:
    """生产真实形态：agent 子进程退出（确定性失败，非 NodeExecutionError）。

    实测该类异常被 ``_is_retryable_node_failure`` 判 False（不进预算计数）。
    """
    return FlowErrorException(
        "执行节点impl出错了: AgentRunError: executor 'recursive' exited 1: "
        "checkpoint: per-turn snapshots active"
    )


def _transient_failure() -> FlowErrorException:
    """瞬态失败（可重试）：链中有 NodeExecutionError。"""
    err = NodeExecutionError("执行节点impl出错了: ConnectionError: blip", node="impl")
    wrapped = FlowErrorException(str(err))
    wrapped.__cause__ = err
    return wrapped


def _make_worker(fake) -> RedisFlowWorker:
    fs = MemoryFlowStorage()
    fs.save_flow(FLOW_DEF)
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:issue73-terminalize",
        execution_storage=MemoryExecutionStorage(),
        flow_storage=fs,
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
        lease_ttl_seconds=60,
    )


def _state(status="running") -> ExecutionState:
    return ExecutionState(
        execution_id="exec-1",
        flow_id="f1",
        flow_version="1",
        status=status,
        context={"$LAST_NODE": "start", "$NODE": {"start": {}}},
    )


class TestTerminalizedFailureIsPoisonAcked:
    def test_terminal_class_is_valueerror_family(self):
        """核心钉子：终态化信号必须是 ValueError 族，否则 run() 不 poison ack。

        实现修复前该处抛裸 RuntimeError → 落到兜底 except（不 ack）→ 无限重投。
        """
        assert issubclass(NodeFailureTerminalizedError, ValueError), (
            "必须是 ValueError 族：run() 只有 except ValueError 才 poison ack "
            "（:2847「畸形消息：ack 掉避免 poison pill 无限重投」）"
        )
        # 与既有同族信号对齐（FlowHashMismatchError 是既有的正确范式）
        assert issubclass(FlowHashMismatchError, ValueError)
        # 且**不得**是 RuntimeError：那会落进兜底不-ack 分支（本单根因）
        assert not issubclass(NodeFailureTerminalizedError, RuntimeError)

    def test_deterministic_failure_does_not_reach_budget_counter(self):
        """机制钉子：确定性失败不进预算计数（解释全库 74 键为何恒为 1）。"""
        from plaita.server.flow_worker import _is_retryable_node_failure

        assert _is_retryable_node_failure(_transient_failure()) is True
        assert _is_retryable_node_failure(_deterministic_failure()) is False, (
            "确定性失败（exited 1 / sync_in）判不可重试 ⇒ INCR 不执行 ⇒ "
            "计数键恒为 1 ⇒ 预算耗尽分支永不触发"
        )

    def test_resume_flow_terminal_path_raises_poison_signal(self):
        """关键钉子：**resume_flow** 终态化 error 后必须抛 ValueError 族。

        两路径不对称（本单根因）：
        - ``_process_execution_result``（start 首轮）终态化后 **break 正常返回**
          → run() 照常 ack ✓；
        - ``resume_flow``（续跑轮）终态化后 **raise** → 旧代码抛裸 RuntimeError
          → run() 兜底 except（不 ack）→ 重投 → status=error 只放行 retry
          → 再撞同一确定性失败 → **无限循环**。

        这里直接对源做断言（resume_flow 的 raise 站点），因为构造完整
        resume_flow 需要租约/恢复上下文；同时用可执行单测钉住异常类的族别。
        """
        import inspect
        import re

        src = inspect.getsource(RedisFlowWorker.resume_flow)
        # 终态化后的 raise 必须是新信号，不能是裸 RuntimeError
        raises = re.findall(r"raise (\w+)\(", src)
        assert "NodeFailureTerminalizedError" in raises, (
            "resume_flow 终态化路径必须抛 NodeFailureTerminalizedError，"
            "否则 run() 不 poison ack ⇒ 消息无限重投（#73）"
        )
        assert "RuntimeError" not in raises, (
            "终态化后不得抛裸 RuntimeError：它会落到 run() 的兜底 except（不 ack）"
        )

    def test_process_execution_result_terminal_path_acks_by_returning(self):
        """对称性钉子：start 首轮的终态化路径**靠正常返回**让 run() ack。

        该路径终态化后 ``break``（不 raise）——所以它没有 #73 那个泄漏；
        本测试防止后人「为了对称」把它也改成 raise（那会引入新的不-ack）。
        """
        import inspect

        src = inspect.getsource(RedisFlowWorker._process_execution_result)
        assert '_persist_state_or_raise(execution_id, state, "error_state")' in src
        # 该站位之后应直接 break（本函数收尾），不得跟 raise
        tail = src.split('_persist_state_or_raise(execution_id, state, "error_state")')[1]
        assert tail.lstrip().startswith("break"), (
            "start 首轮终态化后应 break 正常返回（run() 据此 ack）；"
            "改成 raise 会复现 #73 的不-ack 重投循环"
        )


def _flow():
    from plaita.core.flow import Flow

    return Flow(**FLOW_DEF)


def _initial_result():
    return {
        "execution_id": "exec-1",
        "is_end": False,
        "is_suspend": False,
        "context": {"$LAST_NODE": "start", "$NODE": {"start": {}}},
    }
