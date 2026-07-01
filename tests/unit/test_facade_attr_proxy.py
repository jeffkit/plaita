"""A2 复现: FlowExecution facade 不应产生"幻影属性"。

当前 ``__setattr__`` 对既不在 _REAL_ATTRS、又不以 _ 开头、且 context 上没有
的属性, 会落到 ``object.__setattr__`` 挂在 facade 实例上。这类属性:
- 不进入 ``_ctx._context``, 分布式持久化(to_dict)时丢失;
- 与"facade 是 context 的薄代理"的契约不符。

期望: 未知公共属性应写入 context, 使其可被持久化、且读写对称。
"""

import unittest

from plaita.core.executor import FlowExecution


class TestFacadeNoPhantomAttrs(unittest.TestCase):
    def test_unknown_public_attr_lands_on_context(self):
        execution = FlowExecution()
        execution.my_marker = 42

        # 应该写到 context, 而不是 facade 的 __dict__
        self.assertEqual(execution._ctx.get_state("my_marker"), 42)
        self.assertNotIn("my_marker", execution.__dict__)

    def test_unknown_attr_round_trips(self):
        execution = FlowExecution()
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
