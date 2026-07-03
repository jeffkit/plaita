"""FlowExecution facade 属性代理行为测试。

重构后 FlowExecution 不再用 ``__getattr__``/``__setattr__`` 兜底代理：
context / express_* / execution_id / event_bus / cancel_event 都是**显式**
property，state 访问走显式 ``set_state``/``get_state``/``evaluate`` 等方法。
未声明的属性就是普通 Python 实例属性——不会再静默落进 context state，
也不会再凭空代理到 context 的任意方法。
"""

import unittest

from plaita.core.executor import FlowExecution


class TestFacadeExplicitDelegation(unittest.TestCase):
    def test_real_attrs_stay_on_facade(self):
        execution = FlowExecution()
        execution.mode = "normal"  # 字符串入口, setter coerce 成 enum
        execution.timeout = 100
        # 0.5.0 起 execution.mode 是 ExecutionMode enum (内部统一), 公共入口
        # 仍接受字符串。字符串 "normal" coerce 后等价于 ExecutionMode.NORMAL。
        from plaita.core.executor import ExecutionMode
        self.assertEqual(execution.mode, ExecutionMode.NORMAL)
        self.assertEqual(execution.timeout, 100)
        # mode/timeout 不应污染 context state
        self.assertNotIn("mode", execution._ctx.context)
        self.assertNotIn("timeout", execution._ctx.context)

    def test_context_property_round_trips_to_underlying_ctx(self):
        execution = FlowExecution()
        execution.context = {"$INPUT": {"x": 1}}
        self.assertEqual(execution._ctx.context, {"$INPUT": {"x": 1}})
        self.assertEqual(execution.context, {"$INPUT": {"x": 1}})

    def test_express_prefix_property_delegates_to_ctx(self):
        execution = FlowExecution()
        execution.express_prefix = "#"
        self.assertEqual(execution.express_prefix, "#")
        self.assertEqual(execution._ctx.express_prefix, "#")

    def test_unknown_attr_does_not_leak_into_context_state(self):
        # 没有 strict_attrs 开关了：拼写错误就是普通实例属性，
        # 关键保证是它**不会**静默落进 context state 造成持久化污染。
        execution = FlowExecution()
        execution.tiemout = 100  # 拼写错误的 timeout
        self.assertEqual(execution.tiemout, 100)
        self.assertNotIn("tiemout", execution._ctx.context)
        # 真实属性照常工作
        execution.timeout = 100
        self.assertEqual(execution.timeout, 100)

    def test_no_phantom_context_method_delegation(self):
        # context 上有的方法不应自动出现在 facade 上（除非显式声明）。
        # 这里用一个 context 独有、facade 未声明的方法验证。
        execution = FlowExecution()
        self.assertFalse(hasattr(execution, "child"))
        # 显式声明的 delegate 仍然在
        self.assertTrue(hasattr(execution, "evaluate"))
        self.assertTrue(hasattr(execution, "set_state"))
        self.assertTrue(hasattr(execution, "get_state"))

    def test_execution_id_is_read_only_property(self):
        execution = FlowExecution()
        eid = execution.execution_id
        self.assertEqual(eid, execution._ctx.execution_id)
        with self.assertRaises(AttributeError):
            execution.execution_id = "hacked"


if __name__ == "__main__":
    unittest.main()
