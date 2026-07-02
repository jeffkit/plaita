"""FlowExecution facade 属性代理行为测试。

strict_attrs=True（默认）: 未知公共属性写入抛 AttributeError，防止拼写错误静默落入 context。
strict_attrs=False: 未知公共属性写入 context（向后兼容模式，显式 opt-in）。
"""

import unittest

from plaita.core.executor import FlowExecution


class TestFacadeNoPhantomAttrs(unittest.TestCase):
    def test_unknown_public_attr_lands_on_context(self):
        # strict_attrs=False: 未知属性落入 context 而不是 facade.__dict__
        execution = FlowExecution(strict_attrs=False)
        execution.my_marker = 42

        self.assertEqual(execution._ctx.get_state("my_marker"), 42)
        self.assertNotIn("my_marker", execution.__dict__)

    def test_unknown_attr_round_trips(self):
        # strict_attrs=False: 读写对称
        execution = FlowExecution(strict_attrs=False)
        execution.temp_value = "hello"
        self.assertEqual(execution.temp_value, "hello")
        self.assertEqual(execution._ctx.get_state("temp_value"), "hello")

    def test_real_attrs_stay_on_facade(self):
        execution = FlowExecution()
        execution.mode = "normal"
        execution.timeout = 100
        self.assertEqual(execution.mode, "normal")
        self.assertEqual(execution.timeout, 100)
        # mode/timeout 不应污染 context state
        self.assertNotIn("mode", execution._ctx.context)
        self.assertNotIn("timeout", execution._ctx.context)

    def test_strict_attrs_rejects_unknown_writes(self):
        """strict_attrs=True 时拼写错误不再静默落到 context state。"""
        execution = FlowExecution(strict_attrs=True)
        with self.assertRaises(AttributeError):
            execution.tiemout = 100  # 拼写错误的 timeout
        # 真实属性仍可正常写入
        execution.timeout = 100
        self.assertEqual(execution.timeout, 100)
        # context 已有属性(如 express_prefix)仍可写
        execution.express_prefix = "#"
        self.assertEqual(execution.express_prefix, "#")


if __name__ == "__main__":
    unittest.main()
