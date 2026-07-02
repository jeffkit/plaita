"""B9 复现: ErrorHandler.strategy 应使用 ErrorStrategy 枚举, 而非裸字符串。

2026-07 重构落地: ``strategy`` 字段类型已从 ``Optional[str]`` 改为
``ErrorStrategy``, 字符串输入在 validator 里自动 coerce 为 enum。
比较处不再需要 ``_strategy_eq``, 直接 ``==`` 即可。
"""

import unittest

from plaita.core.errors import ErrorHandler, ErrorStrategy, RecoverableErrorHandler


class TestErrorHandlerStrategyEnum(unittest.TestCase):
    def test_default_strategy_value(self):
        h = ErrorHandler()
        # 字段存为 ErrorStrategy enum; == enum 与 == enum.value 都成立
        self.assertEqual(h.strategy, ErrorStrategy.ABORT)
        self.assertEqual(h.strategy.value, "abort")

    def test_string_input_validated_and_coerced(self):
        h = ErrorHandler(strategy="continue")
        self.assertEqual(h.strategy, ErrorStrategy.CONTINUE)
        h2 = ErrorHandler(strategy="continue-with")
        self.assertEqual(h2.strategy, ErrorStrategy.CONTINUE_WITH)
        # 下划线别名 continue_with 也应归一化为 continue-with
        h3 = ErrorHandler(strategy="continue_with")
        self.assertEqual(h3.strategy, ErrorStrategy.CONTINUE_WITH)

    def test_invalid_strategy_rejected(self):
        from pydantic import ValidationError
        with self.assertRaises(ValidationError):
            ErrorHandler(strategy="bogus")

    def test_handle_abort_raises_timeout(self):
        h = ErrorHandler(strategy=ErrorStrategy.ABORT)
        with self.assertRaises(TimeoutError):
            h.handle()

    def test_handle_continue_returns_none(self):
        h = ErrorHandler(strategy="continue")
        self.assertIsNone(h.handle())

    def test_handle_continue_with_returns_default(self):
        h = ErrorHandler(strategy="continue-with", defaultValue={"x": 1})
        self.assertEqual(h.handle(), {"x": 1})

    def test_recoverable_handler_inherits_enum_strategy(self):
        h = RecoverableErrorHandler(strategy="continue", retryTimes=2)
        self.assertEqual(h.strategy, ErrorStrategy.CONTINUE)
        self.assertEqual(h.retry_times, 2)


class TestRunnerUsesEnumStrategy(unittest.TestCase):
    """runner 用真实 ErrorHandler(枚举) 时, continue-with 应返回默认值。"""

    def test_continue_with_returns_default_via_runner(self):
        from unittest.mock import Mock
        from plaita.core.executor import FlowExecution
        from plaita.node import Node

        node = Mock(spec=Node)
        node.id = "n"
        node.name = "n"
        node.branching = False
        node.timeout = None
        node.timeout_handler = None
        node.run.side_effect = RuntimeError("boom")
        node.error_handler = RecoverableErrorHandler(
            strategy=ErrorStrategy.CONTINUE_WITH, defaultValue={"fallback": 1}, retryTimes=0
        )

        execution = FlowExecution()
        from plaita.core.flow import Flow
        flow = Flow(flow_id="t", version="1", runtime="python")
        result, branch = execution._process_node(flow, node, False)
        self.assertEqual(result, {"fallback": 1})


if __name__ == "__main__":
    unittest.main()
