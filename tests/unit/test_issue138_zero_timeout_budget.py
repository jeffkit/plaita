"""plaita#138：``0`` / ``PT0S`` 是**显式零预算**（最严），不是"无限额"。

``run_distributed`` 把 flow 级 / 实例级 / 调用期 timeout 取**最严（min）**。
但最严的那个值过去被静默丢掉：``_parse_timeout`` 把 ``0`` 与零时长 ISO 串
映射成 ``None``（= 不限额），于是 min-merge 永远看不到零预算——
"取最严"在取到最严值（0）时反而完全失效：``flow.timeout="PT0S"`` /
``run_distributed(..., timeout=0)`` 都会让一个 0.309s 的节点跑完。

本文件钉住三件事：

1. ``0`` / ``0.0`` / ``False`` / ``"0"`` / ``"PT0S"`` / ``"PT"`` ⇒ 毫秒预算 ``0``；
2. 该预算在三条入口上都**真的生效**（flow 级 / 调用期 / 节点自身），不是只改解析；
3. 负向对照：``None`` / ``""`` 仍是"未提供超时"（不能把"没给"也变成 0）。
"""

import time
import unittest
from typing import ClassVar
from unittest.mock import patch

from plaita.core import types
from plaita.core.errors import FlowErrorType, FlowExecutionException
from plaita.core.executor import FlowExecution
from plaita.core.flow import Flow
from plaita.core.runner import NodeRunner
from plaita.core.strategies import DistributedStrategy
from plaita.io import Property
from plaita.node import End, Node, Start

ZERO_FORMS = (0, 0.0, False, "0", "PT0S", "PT")


class _SleepNode(Node):
    node_type: ClassVar[str] = "sleep_issue138"
    seconds: float = 0.3

    def run(self, execution):
        time.sleep(self.seconds)
        return "slept"


def _flow(seconds: float = 0.3, timeout: str = ""):
    return Flow(
        flow_id="issue138",
        version="1.0",
        runtime="python",
        timeout=timeout,
        output_type=Property(data_type=types.STRING, name="r"),
        nodes=[
            Start(id="s", next="n1"),
            _SleepNode(id="n1", next="e", seconds=seconds),
            End(id="e", **{"resultType": "success", "output": "$NODE.n1"}),
        ],
    )


def _capture_strategy_timeout():
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


class TestZeroFormsParseToZero(unittest.TestCase):
    """RED：修复前 ``_parse_timeout(0)`` / ``("PT0S")`` 都返回 ``None``。"""

    def test_zero_number_is_zero_budget(self):
        for value in (0, 0.0, False):
            with self.subTest(value=value):
                self.assertEqual(
                    NodeRunner._parse_timeout(value), 0,
                    f"{value!r} 是显式零预算，必须归成 0 而不是 None（不限额）",
                )

    def test_zero_duration_string_is_zero_budget(self):
        for value in ("0", "PT0S", "PT"):
            with self.subTest(value=value):
                self.assertEqual(
                    NodeRunner._parse_timeout(value), 0,
                    f"{value!r} 是显式零预算，必须归成 0 而不是 None（不限额）",
                )

    def test_positive_semantics_unchanged(self):
        """本单不改变任何正超时语义（阴性对照的正向半边）。"""
        for value, expected in ((300, 300), ("300", 300), ("PT0.3S", 300), (500, 500)):
            with self.subTest(value=value):
                self.assertEqual(NodeRunner._parse_timeout(value), expected)


class TestNoLimitIsStillNoLimit(unittest.TestCase):
    """负向对照：0 变"零预算"不得把"未提供超时"也一并变成 0。"""

    def test_none_and_empty_string_stay_no_limit(self):
        for value in (None, ""):
            with self.subTest(value=value):
                self.assertIsNone(NodeRunner._parse_timeout(value))

    def test_none_still_means_no_limit_end_to_end(self):
        captured, patcher = _capture_strategy_timeout()
        with patcher:
            result = FlowExecution().run_distributed(_flow(seconds=0.0), timeout=None)
        self.assertEqual(captured, [None], "None 仍是「未给预算」，不能变成 0")
        self.assertFalse(result["is_end"])


class TestZeroBudgetBoundsTheRun(unittest.TestCase):
    """RED：修复前这些入口全部静默跑满 0.3s（无限额）。"""

    def test_flow_level_zero_duration_bounds_the_run(self):
        started = time.time()
        with self.assertRaises(FlowExecutionException) as ctx:
            _flow(timeout="PT0S").run()
        self.assertIn("Timeout", str(ctx.exception))
        self.assertLess(time.time() - started, 0.25,
                        "flow.timeout='PT0S' 必须立即触发超时，而不是跑满 0.3s")

    def test_call_level_zero_bounds_the_run(self):
        started = time.time()
        with self.assertRaises(FlowExecutionException):
            FlowExecution().run(_flow(), timeout=0)
        self.assertLess(time.time() - started, 0.25,
                        "run(timeout=0) 必须立即触发超时，而不是跑满 0.3s")

    def test_instance_level_zero_bounds_the_run(self):
        execution = FlowExecution()
        execution.timeout = 0
        started = time.time()
        with self.assertRaises(FlowExecutionException):
            execution.run_distributed(_flow())
        self.assertLess(time.time() - started, 0.25,
                        "execution.timeout=0 必须立即触发超时")

    def test_node_level_zero_duration_bounds_and_is_a_node_error(self):
        """节点自带零预算 ⇒ 节点级超时（NODE_ERROR），不是"流程级强加"。"""
        flow = _flow()
        flow.nodes[1].timeout = "PT0S"
        started = time.time()
        with self.assertRaises(FlowExecutionException) as ctx:
            flow.run()
        self.assertLess(time.time() - started, 0.25,
                        "node.timeout='PT0S' 必须立即触发超时，而不是跑满 0.3s")
        self.assertEqual(ctx.exception.error_type, FlowErrorType.NODE_ERROR,
                         "节点自身预算是节点级限额，超时归属应为 NODE_ERROR")


class TestZeroParticipatesInStrictestWins(unittest.TestCase):
    """RED：修复前 0 被过滤掉，min-merge 看不到它。"""

    CASES = [
        # (flow.timeout, execution.timeout, 调用期 timeout, 期望毫秒)
        ("PT0S", None, None, 0),
        ("PT5S", None, 0, 0),
        ("", 0, None, 0),
        ("PT5S", 100, 0, 0),
        ("PT5S", 100, "PT0.2S", 100),
    ]

    def test_zero_wins_as_the_strictest_budget(self):
        for flow_timeout, instance_timeout, call_timeout, expected in self.CASES:
            with self.subTest(flow=flow_timeout, instance=instance_timeout,
                              call=call_timeout):
                execution = FlowExecution()
                if instance_timeout is not None:
                    execution.timeout = instance_timeout
                captured, patcher = _capture_strategy_timeout()
                with patcher:
                    with self.assertRaises(FlowExecutionException):
                        execution.run_distributed(
                            _flow(timeout=flow_timeout), timeout=call_timeout
                        )
                self.assertEqual(captured, [expected])


if __name__ == "__main__":
    unittest.main()
