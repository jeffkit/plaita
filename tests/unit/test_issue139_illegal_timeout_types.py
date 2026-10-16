"""plaita#139：非法 timeout 类型/值必须**响亮失败**，不得静默当成"无限额"。

``_parse_timeout`` 历史上对非 ``str``/非数值的输入直接 ``return None``
（= 不限额），于是调用方的预算被无声丢弃：

- ``FlowExecution.run(flow, timeout=[1,2])`` 静默跑满 0.3s 的节点；
  ``flow.timeout = [1,2]`` 同样无声失效；
- ``-5`` 被原样返回（负数预算），``-0.5`` 更被 ``int()`` 截成 ``0``；
- ``float('inf')`` 走到 ``int()`` 抛**裸 OverflowError**——既不是具名拒绝，
  又击穿了「离开 ``run_distributed`` 的一切失败都是 ``FlowErrorException``」；
- ``10**400`` 被当合法预算接受，随后在引擎 ``timeout_ms / 1000.0`` 处内部
  ``OverflowError``（流程已经开始跑了才炸）。

本文件钉住：非法类型/负值/非有限 float/超界整数 ⇒ ``ValueError``（点名违规值与
受支持形状）；distributed 入口仍把一切失败归一成 ``FlowErrorException``；负向
对照：合法 ``int``/ISO 串仍按最严预算生效。
"""

import math
import sys
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


class _BadTimeout:
    def __repr__(self) -> str:  # 稳定 repr，便于断言"消息带违规值"
        return "<bad-timeout>"


class _SleepNode(Node):
    node_type: ClassVar[str] = "sleep_issue139_types"
    seconds: float = 0.3

    def run(self, execution):
        time.sleep(self.seconds)
        return "slept"


def _flow(seconds: float = 0.3, timeout=""):
    return Flow(
        flow_id="issue139_types",
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


class TestParseTimeoutRejectsIllegalInputs(unittest.TestCase):
    """RED：这些输入在修复前返回 None（静默无限额）或抛错的类型不对。"""

    def test_illegal_types_raise_named_value_error(self):
        for value, type_name in (
            ([1, 2], "list"), ({"a": 1}, "dict"), ({1, 2}, "set"),
            (("a",), "tuple"), (b"300", "bytes"), (_BadTimeout(), "_BadTimeout"),
        ):
            with self.subTest(value=value):
                with self.assertRaises(ValueError) as ctx:
                    NodeRunner._parse_timeout(value)
                message = str(ctx.exception)
                self.assertIn(repr(value), message, "必须点名违规值")
                self.assertIn(type_name, message, "必须点名实际类型")
                self.assertIn("ISO 8601", message, "必须列出受支持形状")

    def test_negative_budget_is_rejected(self):
        """-5 过去被原样返回，-0.5 更被 ``int()`` 截成 0（静默错值）。"""
        for value in (-5, -0.5, -1):
            with self.subTest(value=value):
                with self.assertRaises(ValueError) as ctx:
                    NodeRunner._parse_timeout(value)
                self.assertIn(repr(value), str(ctx.exception))

    def test_non_finite_floats_are_rejected_as_value_errors(self):
        """``inf`` 过去抛裸 OverflowError；``nan`` 抛不点名的 CPython ValueError。"""
        for value in (float("inf"), float("-inf"), float("nan")):
            with self.subTest(value=value):
                with self.assertRaises(ValueError) as ctx:
                    NodeRunner._parse_timeout(value)
                self.assertIn(repr(value), str(ctx.exception))

    def test_oversized_int_is_rejected(self):
        """超过 ``sys.float_info.max`` 的毫秒预算会在引擎 ``/1000.0`` 处内部炸。"""
        huge = 10 ** 400
        with self.assertRaises(ValueError) as ctx:
            NodeRunner._parse_timeout(huge)
        self.assertIn(str(huge), str(ctx.exception))
        # 边界内的极大预算仍合法（不误伤）
        self.assertEqual(NodeRunner._parse_timeout(10 ** 18), 10 ** 18)
        self.assertEqual(NodeRunner._parse_timeout(math.floor(sys.float_info.max)),
                         math.floor(sys.float_info.max))

    def test_non_fixed_length_iso_durations_are_rejected(self):
        """``P1Y``/``P1M`` 没有固定秒数：``total_seconds()`` 会把年/月静默丢掉。

        旧行为是 ``0 → None``（"无限额"，静默丢弃预算）；朴素的 #138 改法会让
        它变成 ``0``（"立即超时"）——两者都不是调用方要的预算。响亮拒绝。
        """
        for value in ("P1Y", "P1M", "P1YT1H"):
            with self.subTest(value=value):
                with self.assertRaises(ValueError) as ctx:
                    NodeRunner._parse_timeout(value)
                self.assertIn(repr(value), str(ctx.exception))
                self.assertIn("year/month", str(ctx.exception))

    def test_negative_iso_duration_is_rejected(self):
        """``-PT1S`` 解析成负时长，旧实现原样返回 -1000ms（负预算）。"""
        with self.assertRaises(ValueError) as ctx:
            NodeRunner._parse_timeout("-PT1S")
        self.assertIn("'-PT1S'", str(ctx.exception))

    def test_valid_inputs_unchanged(self):
        """负向对照：合法形状语义一字不变（含 #138 的 0 = 零预算）。"""
        for value, expected in (
            (None, None), ("", None),
            (300, 300), ("300", 300), ("PT0.3S", 300),
            (0, 0), ("PT0S", 0),
            (True, 1), (False, 0),
            ("P1D", 86_400_000), ("P1WT1H", 608_400_000),
        ):
            with self.subTest(value=value):
                self.assertEqual(NodeRunner._parse_timeout(value), expected)


class TestNormalModeEntryFailsLoudly(unittest.TestCase):
    """RED：修复前这些调用静默跑满 0.3s，或把负数变成"立即超时"。"""

    def test_call_timeout_illegal_type_fails_before_running(self):
        started = time.time()
        with self.assertRaises(ValueError):
            FlowExecution().run(_flow(), timeout=[1, 2])
        self.assertLess(time.time() - started, 0.15,
                        "非法 timeout 必须在开跑前就失败，而不是静默无限额跑满")

    def test_flow_level_illegal_type_fails(self):
        flow = _flow()
        flow.timeout = {"not": "a timeout"}
        started = time.time()
        with self.assertRaises(ValueError):
            flow.run()
        self.assertLess(time.time() - started, 0.15,
                        "flow.timeout 非法类型必须响亮失败，而不是无声丢弃预算")

    def test_negative_call_timeout_is_rejected_not_an_immediate_timeout(self):
        """负数过去经 ``int(-0.5) == 0`` 或负值预算变成"神秘立即超时"。"""
        with self.assertRaises(ValueError):
            FlowExecution().run(_flow(), timeout=-5)


class TestDistributedContractHolds(unittest.TestCase):
    """distributed 入口：一切失败归一成 ``FlowErrorException``（#139 契约）。"""

    def test_malformed_types_still_normalize_to_flow_error(self):
        for value in ([1, 2], {"a": 1}, b"300", _BadTimeout()):
            with self.subTest(value=value):
                with self.assertRaises(FlowExecutionException) as ctx:
                    FlowExecution().run_distributed(_flow(seconds=0.0), timeout=value)
                self.assertEqual(ctx.exception.error_type, FlowErrorType.FLOW_ERROR)
                self.assertIn("from run_distributed(timeout=...)", str(ctx.exception))

    def test_non_finite_and_oversized_normalize_at_validation_time(self):
        """RED：``inf`` 逃出裸 OverflowError；``10**400`` 先跑流程再内部炸。"""
        for value, needle in (
            (float("inf"), "inf"), (float("nan"), "nan"),
            (10 ** 400, None),
        ):
            with self.subTest(value=value):
                started = time.time()
                with self.assertRaises(FlowExecutionException) as ctx:
                    FlowExecution().run_distributed(_flow(), timeout=value)
                self.assertEqual(ctx.exception.error_type, FlowErrorType.FLOW_ERROR)
                self.assertIn("from run_distributed(timeout=...)", str(ctx.exception))
                if needle is not None:
                    self.assertIn(needle, str(ctx.exception), "必须点名违规值")
                self.assertLess(time.time() - started, 0.15,
                                "必须在校验期立即失败，而不是进入执行后才炸")

    def test_valid_budget_still_applies_as_strictest(self):
        """负向对照：合法 int/ISO 串仍按最严者下发并生效。"""
        for value, expected in ((50, 50), ("PT0.05S", 50)):
            with self.subTest(value=value):
                captured, patcher = _capture_strategy_timeout()
                started = time.time()
                with patcher:
                    with self.assertRaises(FlowExecutionException):
                        FlowExecution().run_distributed(_flow(seconds=0.3), timeout=value)
                self.assertEqual(captured, [expected])
                self.assertLess(time.time() - started, 0.25)


if __name__ == "__main__":
    unittest.main()
