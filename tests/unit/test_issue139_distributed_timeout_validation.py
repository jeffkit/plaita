"""plaita#139：distributed 入口的畸形 timeout 必须**硬错**，不得静默降级。

背景：#71 给 ``run_distributed`` 加了「flow.timeout / 实例 timeout / 调用期
timeout 取严」的解析后，若直接沿用 ``NodeRunner._parse_timeout`` 的宽容口径
（非 str/number → ``None``），非法类型会变成「无限额」——比 #71 之前的
``FlowErrorException(TypeError)`` **更糟**（响亮失败 → 静默丢预算）；而非法
ISO 串抛的裸 ``ValueError`` 又逃出了「distributed 一切异常归一为
``FlowErrorException``/``-500``」的既有契约。

本文件钉住：畸形表（list/dict/负数/非法串/任意对象）一律 ``FlowErrorException``
且消息含违规值；合法 int/ISO 仍按最严者生效；显式 ``None``/空串仍是"无限额"。
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


class _BadTimeout:
    def __repr__(self) -> str:  # 稳定 repr，便于断言"消息带违规值"
        return "<bad-timeout>"


class _SleepNode(Node):
    node_type: ClassVar[str] = "sleep_issue139"
    seconds: float = 0.0

    def run(self, execution):
        time.sleep(self.seconds)
        return "slept"


def _flow(seconds: float = 0.2, timeout: str = ""):
    return Flow(
        flow_id="issue139",
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


MALFORMED = [
    ([1, 2], "[1, 2]"),
    ({"a": 1}, "{'a': 1}"),
    (-5, "-5"),
    ("garbage", "'garbage'"),
    ("3 seconds", "'3 seconds'"),
    (_BadTimeout(), "<bad-timeout>"),
]


class TestMalformedTimeoutIsAFlowError(unittest.TestCase):
    def test_malformed_call_timeout_raises_flow_error_with_value(self):
        for value, needle in MALFORMED:
            with self.subTest(value=value):
                execution = FlowExecution()
                started = time.time()
                with self.assertRaises(FlowExecutionException) as ctx:
                    execution.run_distributed(_flow(), timeout=value)
                elapsed = time.time() - started
                exc = ctx.exception
                self.assertEqual(exc.error_type, FlowErrorType.FLOW_ERROR)
                self.assertEqual(exc.code, -500)
                self.assertIn(needle, str(exc), "错误消息必须带违规值")
                self.assertIn("from run_distributed(timeout=...)", str(exc),
                              "错误消息必须指明违规输入来自哪个入口")
                self.assertLess(elapsed, 0.15,
                                "畸形 timeout 必须在校验期立即失败，不得降级成无限额跑完整条流程")

    def test_malformed_instance_timeout_raises_flow_error(self):
        execution = FlowExecution()
        execution.timeout = {"not": "a timeout"}
        with self.assertRaises(FlowExecutionException) as ctx:
            execution.run_distributed(_flow())
        self.assertEqual(ctx.exception.error_type, FlowErrorType.FLOW_ERROR)
        self.assertIn("{'not': 'a timeout'}", str(ctx.exception))
        self.assertIn("from execution.timeout", str(ctx.exception))

    def test_malformed_flow_timeout_raises_flow_error(self):
        with self.assertRaises(FlowExecutionException) as ctx:
            FlowExecution().run_distributed(_flow(timeout="PT-nope"))
        self.assertEqual(ctx.exception.error_type, FlowErrorType.FLOW_ERROR)
        self.assertIn("'PT-nope'", str(ctx.exception))
        self.assertIn("from flow.timeout", str(ctx.exception))

    def test_non_numeric_object_message_names_the_type(self):
        """任意对象分支：消息要带实际类型名（否则排查时不知道收到了什么）。"""
        with self.assertRaises(FlowExecutionException) as ctx:
            FlowExecution().run_distributed(_flow(), timeout=_BadTimeout())
        message = str(ctx.exception)
        self.assertIn("<bad-timeout>", message)
        self.assertIn("got _BadTimeout", message)

    def test_error_is_not_a_bare_value_error(self):
        """distributed 契约：异常类型必须是 FlowExecutionException 子类。"""
        with self.assertRaises(FlowExecutionException):
            FlowExecution().run_distributed(_flow(), timeout="garbage")

    def test_validation_error_still_reports_the_flow_to_callbacks(self):
        """归一化必须带上真实 flow（不是 None）——``on_flow_end`` 的既有契约。"""
        from plaita.core.callback import FlowCallback

        flow = _flow()
        seen = []

        class _Recorder(FlowCallback):
            def on_flow_end(self, flow, result=None, error=None, exception=None, **kwargs):
                seen.append(flow)

        execution = FlowExecution(callback_handlers=[_Recorder()])
        with self.assertRaises(FlowExecutionException):
            execution.run_distributed(flow, timeout=[1, 2])
        self.assertEqual(seen, [flow])


class TestValidTimeoutStillWinsStrictest(unittest.TestCase):
    """正向对照：合法 int/ISO 一律按最严者下发（不得因校验改动而失守）。"""

    CASES = [
        # (flow.timeout, execution.timeout, 调用期 timeout, 期望毫秒)
        ("", None, 50, 50),
        ("PT0.05S", None, 200, 50),
        ("PT5S", "PT0.05S", 200, 50),
        ("PT5S", 100, "PT0.2S", 100),
        ("200", "300", "100", 100),
    ]

    def test_strictest_of_int_and_iso_wins(self):
        for flow_timeout, instance_timeout, call_timeout, expected in self.CASES:
            with self.subTest(flow=flow_timeout, instance=instance_timeout, call=call_timeout):
                execution = FlowExecution()
                if instance_timeout is not None:
                    execution.timeout = instance_timeout
                captured, patcher = _capture_strategy_timeout()
                with patcher:
                    with self.assertRaises(FlowExecutionException):
                        execution.run_distributed(
                            _flow(seconds=0.3, timeout=flow_timeout), timeout=call_timeout
                        )
                self.assertEqual(captured, [expected])

    def test_none_and_empty_string_still_mean_no_limit(self):
        for value in (None, ""):
            with self.subTest(value=value):
                captured, patcher = _capture_strategy_timeout()
                with patcher:
                    result = FlowExecution().run_distributed(
                        _flow(seconds=0.0), timeout=value
                    )
                self.assertEqual(captured, [None])
                self.assertFalse(result["is_end"])

    def test_zero_is_a_zero_budget_not_unlimited(self):
        """0 / 零时长（``PT``/``PT0S``）是**显式零预算**（plaita#138 起）。

        #139 的边界是"畸形输入不得静默降级"；零预算不是畸形，但"0 = 无限额"
        的旧口径正是兄弟单 #138 的缺陷本身——它让"取最严"在取到最严值（0）
        时反而完全失效。此处按 #138 的结论钉住：0 必须作为最严预算下发，
        且流程被立即打断。
        """
        for value in (0, "PT", "PT0S"):
            with self.subTest(value=value):
                captured, patcher = _capture_strategy_timeout()
                started = time.time()
                with patcher:
                    with self.assertRaises(FlowExecutionException):
                        FlowExecution().run_distributed(_flow(seconds=0.3), timeout=value)
                self.assertEqual(captured, [0],
                                 "0/零时长必须作为最严预算（0ms）下发，而不是 None")
                self.assertLess(time.time() - started, 0.25,
                                "零预算必须立即超时，而不是跑满 0.3s 的节点")


if __name__ == "__main__":
    unittest.main()
