"""plaita#137：``Flow.run/arun/debug`` 的**保留关键字**必须具名报错，不得静默当成 $INPUT。

既有契约（README「``flow.run(name="kongjie")``」/ ``_coerce_input_value``）：
``flow.run(key=value)`` 把**每个**关键字映射成 ``$INPUT`` 字段。所以这里
**不存在**"任意未知关键字"——``mode``/``params``/``timeout`` 之类的兄弟入口
参数名同样是合法的 ``$INPUT`` 字段（仓库既有测试即如此用），按它们一刀切拒绝
会误伤既有用法。

真正的缺陷是"看起来是 API 参数、实际永远到不了调用方以为的位置"的名字：

- ``inputs_data`` / ``input_data``（历史输入包装别名）：被静默塞进 ``$INPUT``，
  于是 ``flow.run(inputs_data={"ok": True})`` 返回 ``None``——与"流程跑完没有
  输出"无法区分（#137 的实测实例，曾让一份 issue 复现脚本的 true branch 从未执行）。
- ``lazy``：``run_compatible(self, flow, lazy, ...)`` 的形参，永远到不了
  ``$INPUT``（现状是 Python 原生的 "got multiple values for argument" 报错，
  不点名修法）。

本文件钉住：保留名 → 具名 ``TypeError``（点名违规键 + 给出 dict 形式写法）；
其它任何关键字仍是合法 ``$INPUT`` 字段（负向对照，防误伤）。
"""

import asyncio
import unittest

from plaita.core import types
from plaita.core.executor import FlowExecution
from plaita.core.flow import Flow
from plaita.io import Property
from plaita.node import End, Start

RESERVED = ("inputs_data", "input_data", "lazy")


def _echo_flow():
    """``$INPUT`` 原样回显的 flow：任何"到不了 $INPUT"的关键字都会显形。"""
    return Flow(
        flow_id="q137_echo",
        version="1.0",
        runtime="python",
        input_type=Property(data_type=types.OBJECT, name="in"),
        output_type=Property(data_type=types.OBJECT, name="out"),
        nodes=[
            Start(id="s", next="e"),
            End(id="e", **{"resultType": "success", "output": "$INPUT"}),
        ],
    )


class TestReservedRunKeywordsAreRejected(unittest.TestCase):
    """RED：``inputs_data=`` 在修复前被静默当成 ``$INPUT`` 字段（返回 None）。"""

    def test_inputs_data_keyword_is_rejected(self):
        flow = _echo_flow()
        with self.assertRaises(TypeError) as ctx:
            flow.run(inputs_data={"ok": True})
        self.assertIn("inputs_data", str(ctx.exception))

    def test_input_data_keyword_is_rejected(self):
        with self.assertRaises(TypeError) as ctx:
            _echo_flow().run(input_data={"ok": True})
        self.assertIn("input_data", str(ctx.exception))

    def test_arun_rejects_reserved_keyword(self):
        with self.assertRaises(TypeError) as ctx:
            asyncio.run(_echo_flow().arun(inputs_data={"ok": True}))
        self.assertIn("inputs_data", str(ctx.exception))

    def test_debug_rejects_reserved_keyword(self):
        with self.assertRaises(TypeError) as ctx:
            _echo_flow().debug(inputs_data={"ok": True})
        self.assertIn("inputs_data", str(ctx.exception))

    def test_lazy_keyword_gets_named_rejection(self):
        """``lazy`` 现在抛 Python 原生 "multiple values" —— 改为点名 + 指路。"""
        with self.assertRaises(TypeError) as ctx:
            _echo_flow().run(lazy=True)
        message = str(ctx.exception)
        self.assertIn("lazy", message)
        self.assertIn("$INPUT", message)

    def test_rejection_names_available_shape_and_dict_escape_hatch(self):
        """错误必须点名违规值 + 给出"要用同名输入字段就走 dict 形式"的修法。"""
        with self.assertRaises(TypeError) as ctx:
            _echo_flow().run(inputs_data={"ok": True})
        message = str(ctx.exception)
        self.assertIn("inputs_data", message)
        self.assertIn("$INPUT", message, "必须说明 kwargs 的既有语义是映射成 $INPUT 字段")
        self.assertIn("flow.run({'inputs_data': ...})", message,
                      "必须给出 dict 形式写法")


class TestReservedRejectionDoesNotOverreach(unittest.TestCase):
    """负向对照：修复不得把合法用法（kwargs-as-input）一起拒掉。"""

    def test_sibling_entry_point_names_stay_input_fields(self):
        """``mode``/``params``/``timeout`` 是兄弟入口的参数名，但在 Flow.run 里
        它们是普通 ``$INPUT`` 字段（``tests/unit/test_builder_mutations.py``
        的 ``lb.run(params={})`` / ``fb.run(greeting=...)`` 钉住）。"""
        result = _echo_flow().run(mode="x", params={"p": 1}, timeout=123)
        self.assertEqual(result, {"mode": "x", "params": {"p": 1}, "timeout": 123})

    def test_positive_control_kwargs_still_reach_input(self):
        result = _echo_flow().run(ok=True, extra="v")
        self.assertEqual(result, {"ok": True, "extra": "v"})

    def test_dict_positional_input_still_works(self):
        self.assertEqual(_echo_flow().run({"ok": True}), {"ok": True})

    def test_reserved_name_usable_as_input_field_via_dict(self):
        """保留名不是"禁用名"：dict 形式仍然可以把它当普通输入字段。"""
        self.assertEqual(
            _echo_flow().run({"inputs_data": {"ok": True}}),
            {"inputs_data": {"ok": True}},
        )
        self.assertEqual(
            _echo_flow().run({"lazy": True}),
            {"lazy": True},
        )


class TestExecuteUnknownOptions(unittest.TestCase):
    """同族：``FlowExecution.execute(**options)`` 历史上接了 options 却从不消费。"""

    def test_unknown_option_is_rejected(self):
        with self.assertRaises(TypeError) as ctx:
            FlowExecution().execute(_echo_flow(), {}, nonsense=1)
        message = str(ctx.exception)
        self.assertIn("nonsense", message)
        self.assertIn("timeout", message, "错误必须列出可用参数")

    def test_known_parameters_still_work(self):
        result = FlowExecution().execute(_echo_flow(), {"ok": True}, lazy=False)
        self.assertEqual(result, {"ok": True})


if __name__ == "__main__":
    unittest.main()
