"""plaita#71：distributed 档的流程级超时必须与 NORMAL/GENERATOR 对齐。

背景（issue 正文 + 实跑基线）：

* ``FlowExecution.run_distributed`` 把调用期原始 ``timeout`` 直送策略，既不
  ``_parse_timeout`` 也不与 ``flow.timeout`` 取更严者 ⇒ ``flow.timeout`` 在
  worker / console 的唯一生产路径上完全失效；调用方照文档传 ISO 时长还会在首
  节点炸成与真因无关的 ``TypeError: unsupported operand type(s) for -: 'str'
  and 'int'``。
* ``DistributedStrategy.execute`` 从第 2 步起不再把 ``max_timeout_ms`` 传给
  ``_execute_current_node`` ⇒ 即便预算算出来了，也只有首步的 start 节点吃到，
  逐步路径上节点超时预算归零。

本文件钉住：合并/解析（spy 第 6 位形参）、ISO 调用期时长不再炸 ``TypeError``、
以及逐步路径上后续步的预算不丢。
"""

import time
import unittest
from typing import ClassVar
from unittest.mock import patch

from plaita.core import types
from plaita.core.errors import FlowErrorType, FlowExecutionException
from plaita.core.executor import FlowExecution
from plaita.core.flow import Flow
from plaita.core.strategies import DistributedStrategy
from plaita.io import Property
from plaita.node import End, Node, Start


class _SleepNode(Node):
    """固定睡眠的同步节点：超时预算是否生效由此节点耗时暴露。"""

    node_type: ClassVar[str] = "sleep_issue71"
    node_name: ClassVar[str] = "睡眠"
    seconds: float = 0.0

    def run(self, execution):
        time.sleep(self.seconds)
        return f"slept {self.seconds}s"


def _flow(nodes, timeout=""):
    return Flow(
        flow_id="issue71",
        version="1.0",
        runtime="python",
        timeout=timeout,
        output_type=Property(data_type=types.STRING, name="r"),
        nodes=nodes,
    )


def _capture_strategy_timeout():
    """Patch ``DistributedStrategy.execute`` to record its 6th positional arg."""
    captured = []
    original = DistributedStrategy.execute

    async def spy(strategy, flow, context, runner, callback_manager,
                  params=None, timeout_ms=None, **options):
        captured.append(timeout_ms)
        return await original(
            strategy, flow, context, runner, callback_manager,
            params, timeout_ms, **options,
        )

    return captured, patch.object(DistributedStrategy, "execute", spy)


class TestDistributedFlowTimeoutIsMergedAndParsed(unittest.TestCase):
    def test_flow_timeout_applies_and_strategy_receives_parsed_ms(self):
        """flow.timeout='PT0.05S' + 0.3s 节点：必须超时，且策略收到 50 而非 None。"""
        flow = _flow([
            Start(id="n0", next="n1"),
            _SleepNode(id="n1", next="e", seconds=0.3),
            End(id="e", **{"resultType": "success", "output": "$NODE.n1"}),
        ], timeout="PT0.05S")
        execution = FlowExecution()
        captured, patcher = _capture_strategy_timeout()

        started = time.time()
        with patcher:
            with self.assertRaises(FlowExecutionException) as ctx:
                execution.run_distributed(flow)
        elapsed = time.time() - started

        self.assertEqual(ctx.exception.error_type, FlowErrorType.FLOW_ERROR)
        self.assertIn("Timeout", str(ctx.exception))
        self.assertLess(elapsed, 0.25, "0.3s 的节点在 50ms 预算下不该跑满")
        self.assertEqual(captured, [50],
                         "distributed 策略应收到解析后的 50ms，而不是 None / 'PT0.05S'")

    def test_flow_timeout_tighter_than_caller_wins(self):
        """flow.timeout=50ms 与调用期 200ms 取更严者：策略收到 50。"""
        flow = _flow([
            Start(id="n0", next="n1"),
            _SleepNode(id="n1", next="e", seconds=0.3),
            End(id="e", **{"resultType": "success", "output": "$NODE.n1"}),
        ], timeout="PT0.05S")
        execution = FlowExecution()
        captured, patcher = _capture_strategy_timeout()

        with patcher:
            with self.assertRaises(FlowExecutionException):
                execution.run_distributed(flow, timeout=200)

        self.assertEqual(captured, [50])

    def test_caller_timeout_tighter_than_flow_wins(self):
        """调用期 50ms 比 flow 的 5s 更严：策略收到 50。"""
        flow = _flow([
            Start(id="n0", next="n1"),
            _SleepNode(id="n1", next="e", seconds=0.3),
            End(id="e", **{"resultType": "success", "output": "$NODE.n1"}),
        ], timeout="PT5S")
        execution = FlowExecution()
        captured, patcher = _capture_strategy_timeout()

        with patcher:
            with self.assertRaises(FlowExecutionException):
                execution.run_distributed(flow, timeout=50)

        self.assertEqual(captured, [50])

    def test_iso_caller_timeout_is_parsed_not_leaked_as_str(self):
        """ISO 调用期时长不得炸 TypeError；应等价 500ms 预算。"""
        flow = _flow([
            Start(id="n0", next="n1"),
            _SleepNode(id="n1", next="e", seconds=0.3),
            End(id="e", **{"resultType": "success", "output": "$NODE.n1"}),
        ])
        execution = FlowExecution()
        captured, patcher = _capture_strategy_timeout()

        with patcher:
            result = execution.run_distributed(flow, timeout="PT0.5S")

        self.assertEqual(captured, [500], "ISO 时长应被解析成 500ms 预算")
        self.assertFalse(result["is_end"])

    def test_instance_timeout_alone_is_applied(self):
        """实例级 ``execution.timeout``（调用期不传 timeout）也必须参与取严。"""
        flow = _flow([
            Start(id="n0", next="n1"),
            _SleepNode(id="n1", next="e", seconds=0.3),
            End(id="e", **{"resultType": "success", "output": "$NODE.n1"}),
        ])
        execution = FlowExecution()
        execution.timeout = 50
        captured, patcher = _capture_strategy_timeout()

        with patcher:
            with self.assertRaises(FlowExecutionException):
                execution.run_distributed(flow)

        self.assertEqual(captured, [50])

    def test_instance_timeout_iso_string_is_parsed(self):
        """实例级 timeout 同样支持 ISO 时长（经 _parse_timeout 归一）。"""
        flow = _flow([
            Start(id="n0", next="n1"),
            _SleepNode(id="n1", next="e", seconds=0.3),
            End(id="e", **{"resultType": "success", "output": "$NODE.n1"}),
        ])
        execution = FlowExecution()
        execution.timeout = "PT0.05S"
        captured, patcher = _capture_strategy_timeout()

        with patcher:
            with self.assertRaises(FlowExecutionException):
                execution.run_distributed(flow)

        self.assertEqual(captured, [50])

    def test_stepwise_budget_reaches_later_steps(self):
        """逐步路径：首步成功后的下一步仍必须拿到流程级预算（此前第 2 步起为 None）。"""
        flow = _flow([
            Start(id="s", next="n1"),
            _SleepNode(id="n1", next="n2", seconds=0.05),
            _SleepNode(id="n2", next="e", seconds=0.3),
            End(id="e", **{"resultType": "success", "output": "$NODE.n2"}),
        ])
        execution = FlowExecution()
        captured, patcher = _capture_strategy_timeout()

        with patcher:
            first = execution.run_distributed(flow, timeout=200)
            self.assertFalse(first["is_end"])
            started = time.time()
            with self.assertRaises(FlowExecutionException):
                execution.run_distributed(
                    flow, timeout=200, saved_context=first["context"]
                )
            elapsed = time.time() - started

        self.assertEqual(captured, [200, 200])
        self.assertLess(elapsed, 0.25, "第二步的 0.3s 节点应在 200ms 预算内被打断")

    def test_stepwise_two_slow_nodes_time_out(self):
        """issue 验收 #3 形状：逐步流程的每步预算必须生效（此前两步都"成功"）。

        偏离说明（照实测量）：issue 给的「节点 0.2s / 预算 200ms」是 sleep 恰好
        等于 budget 的临界巧合——``asyncio.wait_for`` 的 200ms 定时器与节点线程
        ``set_result`` 之间存在真实竞态，实测同一用例约 1/10 次「节点先返回」而
        不超时。这里把节点耗时抬到 0.25s（> 预算），判据不变（每步都吃预算 →
        超时），结果确定；临界形态在 pre-fix 下是 100% 不超时（预算根本没下发），
        见 ``test_stepwise_budget_reaches_later_steps`` 对第 2 步预算的逐步断言。
        """
        flow = _flow([
            Start(id="s", next="n1"),
            _SleepNode(id="n1", next="n2", seconds=0.25),
            _SleepNode(id="n2", next="e", seconds=0.25),
            End(id="e", **{"resultType": "success", "output": "$NODE.n2"}),
        ])
        execution = FlowExecution()
        with self.assertRaises(FlowExecutionException):
            execution.run_distributed(flow, timeout=200)


class TestExistingTimeoutGuardIsStrengthened(unittest.TestCase):
    """既有 ``test_flow_distributed.py::test_timeout_handling`` 是假守卫。

    历史用例只断言 ``_parse_timeout(flow.timeout) == 100``（纯解析），分布式
    策略是否真的收到该值是零覆盖——这正是缺陷长期潜伏的原因。这里补上「策略
    确实收到 100」的断言。
    """

    def test_existing_shape_receives_tighter_budget(self):
        flow = _flow([
            Start(id="start", next="node1"),
            _SleepNode(id="node1", next="node2", seconds=0.0),
            _SleepNode(id="node2", next="end", seconds=0.0),
            End(id="end"),
        ], timeout="PT0.1S")
        execution = FlowExecution()
        captured, patcher = _capture_strategy_timeout()

        with patcher:
            result = execution.run_distributed(flow, timeout=200)

        self.assertEqual(result["id"], "node1")
        self.assertEqual(captured, [100], "flow 的 100ms 比调用期 200ms 更严")


if __name__ == "__main__":
    unittest.main()
