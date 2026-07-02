from unittest import TestCase

from plaita.core.flow import Flow
from plaita.core import types
from plaita.io import Property
from plaita.node import End, Start
from plaita.node.calculate import Calculate
from plaita.node.calculate import FUNCTIONS, Call


class CalculateTestCase(TestCase):

    def test_add(self):
        flow = Flow(
            flow_id="plus-one",
            version="1",
            runtime="python",
            input_type=Property(data_type=types.OBJECT, is_required=True),
            output_type=Property(data_type=types.INTEGER),
            nodes=[
                Start(id="start", next="cal"),
                Calculate(
                    id="cal",
                    expression={"function_name": "add", "params": {"left": "$INPUT.x", "right": 1}},
                    next="end",
                ),
                End(id="end", **{"resultType": "success", "output": "$NODE.cal"}),
            ],
        )
        self.assertEqual(2, flow.run({"x": 1}))

    def test_greeting(self):
        flow = Flow(
            flow_id="hello-world",
            version="1",
            runtime="python",
            input_type=Property(data_type=types.OBJECT, is_required=True),
            output_type=Property(data_type=types.STRING),
            nodes=[
                Start(id="start", next="cal"),
                Calculate(
                    id="cal",
                    expression={"function_name": "concat", "params": {"left": "hello ", "right": "$INPUT.name"}},
                    next="end",
                ),
                End(id="end", **{"resultType": "success", "output": "$NODE.cal"}),
            ],
        )
        self.assertEqual("hello KongJie", flow.run({"name": "KongJie"}))

    def test_embed(self):
        flow = Flow(
            flow_id="plus-twice",
            version="1",
            runtime="python",
            input_type=Property(data_type=types.OBJECT, is_required=True),
            output_type=Property(data_type=types.INTEGER),
            nodes=[
                Start(id="start", next="cal"),
                Calculate(
                    id="cal",
                    expression={
                        "function_name": "add",
                        "params": {
                            "left": "$INPUT.x",
                            "right": {"function_name": "multiply", "params": {"left": "$INPUT.x", "right": 10}},
                        },
                    },
                    next="end",
                ),
                End(id="end", **{"resultType": "success", "output": "$NODE.cal"}),
            ],
        )
        self.assertEqual(11, flow.run({"x": 1}))


# ---------------------------------------------------------------------------
# 强化：精确断言 register_function 注册的元数据 + Call.from_json 的错误分支。
# 杀死仅靠「能跑通」蒙混的变异点（label/description/param_type/return_type 被改成
# None 或下标错位、assert 文案/格式化符被改、缺省值被改）。
# ---------------------------------------------------------------------------


class TestRegisteredFunctionMetadata(TestCase):
    """逐函数精确断言 FUNCTIONS 里每个 Function 的元数据与行为。"""

    def test_all_expected_functions_registered(self):
        self.assertEqual(
            set(FUNCTIONS.keys()),
            {"add", "sub", "multiply", "div", "concat", "replace"},
        )

    def test_add_metadata_and_behavior(self):
        f = FUNCTIONS["add"]
        self.assertEqual(f.name, "add")
        self.assertEqual(f.label, "加法")
        self.assertEqual(f.description, "返回两个数值类型的参数相加的结果")
        self.assertEqual(f.func(2, 3), 5)
        self.assertEqual(set(f.param_type.keys()), {"left", "right"})
        self.assertEqual(f.param_type["left"].data_type, types.INTEGER)
        self.assertEqual(f.param_type["right"].data_type, types.INTEGER)
        self.assertEqual(f.return_type.data_type, types.INTEGER)

    def test_sub_metadata_and_behavior(self):
        f = FUNCTIONS["sub"]
        self.assertEqual(f.label, "减法")
        self.assertEqual(f.description, "返回两个数值类型的参数相减的结果")
        self.assertEqual(f.func(5, 3), 2)
        self.assertEqual(f.return_type.data_type, types.INTEGER)

    def test_multiply_metadata_and_behavior(self):
        f = FUNCTIONS["multiply"]
        self.assertEqual(f.label, "乘法")
        self.assertEqual(f.description, "返回两个数值类型的参数相乘的结果")
        self.assertEqual(f.func(2, 3), 6)
        self.assertEqual(f.return_type.data_type, types.INTEGER)

    def test_div_metadata_and_behavior(self):
        f = FUNCTIONS["div"]
        self.assertEqual(f.label, "除法")
        self.assertEqual(f.description, "返回两个数值类型的参数相除的结果")
        self.assertEqual(f.func(6, 3), 2.0)
        self.assertEqual(f.return_type.data_type, types.INTEGER)

    def test_concat_metadata_and_behavior(self):
        f = FUNCTIONS["concat"]
        self.assertEqual(f.label, "拼接字符串")
        self.assertEqual(f.description, "")
        self.assertEqual(f.func("a", "b"), "ab")
        self.assertEqual(set(f.param_type.keys()), {"left", "right"})
        self.assertEqual(f.param_type["left"].data_type, types.STRING)
        self.assertEqual(f.param_type["right"].data_type, types.STRING)
        self.assertEqual(f.return_type.data_type, types.STRING)

    def test_replace_metadata_and_behavior(self):
        f = FUNCTIONS["replace"]
        self.assertEqual(f.label, "字符串替换")
        self.assertEqual(f.description, "")
        self.assertEqual(f.func("hello", "l", "L"), "heLLo")
        self.assertEqual(set(f.param_type.keys()), {"source", "which", "target"})
        self.assertEqual(f.param_type["source"].data_type, types.STRING)
        self.assertEqual(f.param_type["which"].data_type, types.STRING)
        self.assertEqual(f.param_type["target"].data_type, types.STRING)
        self.assertEqual(f.return_type.data_type, types.STRING)


class TestCallFromJsonErrors(TestCase):
    """Call.from_json 的 assert 文案 / 格式化符 / 缺省值必须精确。"""

    def test_none_returns_none(self):
        self.assertIsNone(Call.from_json(None))

    def test_passthrough_call_instance(self):
        call = Call(function_name="add", params={})
        self.assertIs(Call.from_json(call), call)

    def test_non_dict_raises_with_exact_message(self):
        # 非 dict/str/Call/None —— 触发 isinstance(content, dict) assert。
        # 把 `%` 改成 `/` 的变异会抛 TypeError 而非 AssertionError；文案变异会改消息
        with self.assertRaises(AssertionError) as cm:
            Call.from_json(123)
        self.assertEqual(str(cm.exception), "unknown format of call config : 123")

    def test_missing_function_name_raises_with_exact_message(self):
        with self.assertRaises(AssertionError) as cm:
            Call.from_json({"not_function_name": 1})
        self.assertEqual(str(cm.exception), "function_name is required for call config")

    def test_unregistered_function_raises_with_exact_message(self):
        with self.assertRaises(AssertionError) as cm:
            Call.from_json({"function_name": "does_not_exist"})
        self.assertEqual(str(cm.exception), "function does_not_exist not registered ")

    def test_missing_params_defaults_to_empty_dict(self):
        # 缺 params 键时默认必须是 {} 而非 None —— 改默认值的变异会让 None.items() 抛错
        call = Call.from_json({"function_name": "add"})
        self.assertEqual(call.function_name, "add")
        self.assertEqual(call.params, {})

    def test_nested_call_param_parsed(self):
        call = Call.from_json({
            "function_name": "add",
            "params": {
                "left": 1,
                "right": {"function_name": "add", "params": {"left": 2, "right": 3}},
            },
        })
        self.assertEqual(call.params["left"], 1)
        self.assertIsInstance(call.params["right"], Call)
        self.assertEqual(call.params["right"].function_name, "add")


class TestCallInvoke(TestCase):
    """Call.invoke 走 Function.__call__，参数经 evaluate 解析。"""

    def test_invoke_resolves_variable_params(self):
        call = Call(function_name="add", params={"left": "$X", "right": 1})
        self.assertEqual(call.invoke({"$X": 2}), 3)

    def test_invoke_nested_call(self):
        inner = Call(function_name="multiply", params={"left": "$X", "right": 10})
        call = Call(function_name="add", params={"left": "$X", "right": inner})
        self.assertEqual(call.invoke({"$X": 1}), 11)
