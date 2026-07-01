"""B9 复现: ErrorHandler.strategy 应使用 ErrorStrategy 枚举, 而非裸字符串。

当前 ``strategy: Optional[str] = Field(ErrorStrategy.ABORT.value)`` 把枚举值
拆成字符串存储, 类型安全丢失, 比较处处 ``== ErrorStrategy.X.value``。
应为枚举类型, 同时保持对字符串输入的兼容与现有行为正确。
"""

import unittest

from plaita.core.errors import ErrorHandler, ErrorStrategy, RecoverableErrorHandler


class TestErrorHandlerStrategyEnum(unittest.TestCase):
    def test_default_strategy_value(self):
        h = ErrorHandler()
        # use_enum_values: 存储为枚举值字符串, 保持向后兼容 (== "abort")
        self.assertEqual(h.strategy, "abort")
        self.assertEqual(h.strategy, ErrorStrategy.ABORT.value)

    def test_string_input_validated_and_coerced(self):
        h = ErrorHandler(strategy="continue")
        self.assertEqual(h.strategy, "continue")
        h2 = ErrorHandler(strategy="continue-with")
        self.assertEqual(h2.strategy, "continue-with")

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
        self.assertEqual(h.strategy, "continue")
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
