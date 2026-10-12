from unittest import TestCase
from unittest.mock import MagicMock

import plaita.core.errors as plaita_errors
from plaita.core.flow import Flow
from plaita.dsl import validate_flow_ir
from plaita.core import types
from plaita.io import Property
from plaita.node import End, Start
from plaita.node.assignment import Assignment
from plaita.node.decide import Bool, Condition, ConditionGroup, Logic, Switch, SwitchLegacy, condition_from_json
from plaita.node.decide import _create_condition_group, _parse_condition_content


class ConditionTestCse(TestCase):

    def test_condition(self):
        cond = {
            "relation": "and",
            "conditions": [
                {"field": "$output.data.total", "operator": "gt", "value": 0},
                {"field": "$output.code", "operator": "eq", "value": 0},
            ],
        }
        output = {
            "code": 0,
            "cost": 73,
            "message": "ok",
            "msg": "success",
            "success": True,
            "data": {"list": [], "current": 1, "pageSize": 0, "total": 0},
        }
        self.assertFalse(condition_from_json(cond).match({"$output": output}, "$"))


class BoolTestCase(TestCase):

    def test_switch_node(self):

        flow = Flow(
            flow_id="test_switch",
            version="1",
            runtime="python",
            input_type=Property(
                data_type=types.OBJECT,
                is_required=True,
                children={
                    "name": Property(data_type=types.STRING, is_required=True),
                    "age": Property(data_type=types.INTEGER, is_required=True),
                },
            ),
            output_type=Property(data_type=types.INTEGER, is_required=True),
            nodes=[
                Start(id="start", name="开始", next="test"),
                # 是否18岁以上
                Switch(
                    id="test",
                    branches=[
                        {"next": "gte_18", "condition": {"field": "$INPUT.age", "operator": "gte", "value": 18}},
                        {"next": "lt_18", "isDefault": True},
                    ],
                ),
                End(id="gte_18", **{"resultType": "success", "output": 0}),
                End(id="lt_18", **{"resultType": "error", "error": {"code": 500, "message": "test-error"}}),
            ],
        )

        self.assertEqual(0, flow.run(name="kong", age=18))
        with self.assertRaises(plaita_errors.FlowExecutionException):
            result = flow.run(name="kong", age=17)
            print("result: ", result)

    def test_bool_node(self):
        flow = Flow(
            flow_id="test_bool",
            version="1",
            runtime="python",
            input_type=Property(
                data_type=types.OBJECT,
                is_required=True,
                children={
                    "f0_str": Property(data_type=types.STRING, is_required=True),
                    "f1_str": Property(data_type=types.STRING, is_required=True),
                    "f2_str": Property(data_type=types.STRING, is_required=True),
                    "f3_array": Property(
                        data_type=types.ARRAY, is_required=True, item_type=Property(data_type=types.STRING)
                    ),
                    "f4_int": Property(data_type=types.INTEGER, is_required=True),
                    "f5_int": Property(data_type=types.INTEGER, is_required=True),
                },
            ),
            output_type=Property(data_type=types.INTEGER, is_required=True),
            nodes=[
                Start(id="start", name="开始", next="test"),
                Bool(
                    id="test",
                    next="ok",
                    else_next="fault",
                    condition=ConditionGroup(
                        relation="and",
                        conditions=[
                            Condition(field="$INPUT.f0_str", operator="eq", value="$INPUT.f1_str"),
                            ConditionGroup(
                                relation="or",
                                conditions=[
                                    Condition(field="$INPUT.f0_str", operator="in", value="$INPUT.f3_array"),
                                    Condition(field="$INPUT.f3_array", operator="contains", value="$INPUT.f2_str"),
                                ],
                            ),
                            Condition(field="$INPUT.f4_int", operator="eq", value=0),
                            Condition(field="$INPUT.f4_int", operator="ne", value=1),
                            ConditionGroup(relation="or", conditions=[]),
                        ],
                    ),
                ),
                End(id="ok", **{"resultType": "success", "output": 0}),  # 或者没有value字段
                End(id="fault", **{"resultType": "error", "error": {"code": 500, "message": "test-error"}}),
            ],
        )

        self.assertEqual(
            0,
            flow.run(
                **{
                    "f0_str": "kong",
                    "f1_str": "kong",
                    "f2_str": "jie",
                    "f3_array": ["kong", "kit"],
                    "f4_int": 0,
                    "f5_int": 8,
                }
            ),
        )

        self.assertEqual(
            0,
            flow.run(
                **{
                    "f0_str": "kong",
                    "f1_str": "kong",
                    "f2_str": "jie",
                    "f3_array": ["jie", "kit"],
                    "f4_int": 0,
                    "f5_int": 8,
                }
            ),
        )

        with self.assertRaises(plaita_errors.FlowExecutionException):
            flow.run(
                **{
                    "f0_str": "kong",
                    "f1_str": "kong",
                    "f2_str": "jie",
                    "f3_array": ["kong", "kit"],
                    "f4_int": 10,
                    "f5_int": 8,
                }
            )

        with self.assertRaises(plaita_errors.FlowExecutionException):
            flow.run(
                **{
                    "f0_str": "kong",
                    "f1_str": "kong",
                    "f2_str": "jie",
                    "f3_array": ["fat", "kit"],
                    "f4_int": 0,
                    "f5_int": 8,
                }
            )


class SwitchTestCase(TestCase):

    def test_switch_node_simple_type(self):
        # 简单类型的switch
        flow = Flow(
            flow_id="switch",
            version="1.0",
            runtime="python",
            input_type=Property(
                data_type=types.OBJECT,
                is_required=True,
                children={
                    "value": Property(data_type=types.INTEGER, is_required=True),
                    "default": Property(data_type=types.INTEGER, default_value=0),
                },
            ),
            output_type=Property(data_type=types.INTEGER, is_required=True),
            nodes=[
                Start(id="start", name="开始", next="switch-node"),
                SwitchLegacy(
                    id="switch-node",
                    target="$INPUT.value",
                    cases=[
                        {"id": "default", "name": "默认", "value": "$INPUT.default"},
                        {"id": "min", "name": "默认", "value": -1},
                        {"id": "max", "name": "默认", "value": 1},
                    ],
                ),
                End(id="default", **{"resultType": "success", "output": 0}),
                End(id="min", **{"resultType": "success", "output": -1}),
                End(id="max", **{"resultType": "success", "output": 1}),
            ],
        )
        self.assertEqual(0, flow.run(value=0, default=0))
        self.assertEqual(1, flow.run(value=1, default=0))
        self.assertEqual(-1, flow.run(value=-1, default=0))
        self.assertEqual(0, flow.run(value=9))


class LogicTestCase(TestCase):

    def test_logic_node(self):
        flow = Flow(
            flow_id="logic",
            version="1.0",
            runtime="python",
            input_type=Property(
                data_type=types.OBJECT,
                is_required=True,
                children={
                    "name": Property(data_type=types.STRING, is_required=True),
                    "age": Property(data_type=types.INTEGER, default_value=0),
                },
            ),
            output_type=Property(data_type=types.INTEGER, is_required=True),
            nodes=[
                Start(id="start", name="开始", next="branch-node"),
                Logic(
                    id="branch-node",
                    branches=[
                        {
                            "name": "name_age",
                            "condition": {
                                "relation": "and",
                                "conditions": [
                                    {"field": "$INPUT.name", "operator": "eq", "value": "KongJie"},
                                    {"field": "$INPUT.age", "operator": "eq", "value": 28},
                                ],
                            },
                            "priority": 3,
                        },
                        {
                            "name": "name_or_age",
                            "condition": {
                                "relation": "or",
                                "conditions": [
                                    {"field": "$INPUT.name", "operator": "eq", "value": "KongJie"},
                                    {"field": "$INPUT.age", "operator": "eq", "value": 28},
                                ],
                            },
                            "priority": 2,
                        },
                        {
                            "name": "default",
                            "condition": None,
                            "priority": 1,
                            "is_default": True,
                        },
                    ],
                ),
                End(id="default", **{"resultType": "success", "output": 0}),
                End(id="name_age", **{"resultType": "success", "output": 1}),
                End(id="name_or_age", **{"resultType": "success", "output": -1}),
            ],
        )
        self.assertEqual(1, flow.run(name="KongJie", age=28))
        self.assertEqual(0, flow.run(name="JieJie", age=20))  # default
        self.assertEqual(-1, flow.run(name="KongJie", age=20))  # 命中一个
        self.assertEqual(-1, flow.run(name="Jie", age=28))  # 命中一个

        # class User(object):
        #     def __init__(self, name, age):
        #         self.name = name
        #         self.age = age
        # self.assertEqual(1, flow.run(User('KongJie', 28)))  TODO: 支持对象类型


class AssignmentTestCase(TestCase):

    def test_assignment_node(self):
        # 正常，无分支
        flow = Flow(
            flow_id="assignment",
            version="1.0",
            runtime="python",
            output_type=Property(data_type=types.STRING),
            nodes=[
                Start(id="start", next="assign"),
                Assignment(
                    id="assign", outputType={"dataType": "string", "required": True}, output="KongJie", next="end"
                ),
                End(id="end", **{"resultType": "success", "output": "$NODE.assign"}),
            ],
        )
        self.assertEqual("KongJie", flow.run())

    def test_assignment_mul_upstream(self):
        flow = Flow(
            flow_id="assignment",
            version="1.0",
            runtime="python",
            input_type=Property(data_type=types.OBJECT, is_required=True),
            output_type=Property(data_type=types.STRING),
            nodes=[
                Start(id="start", next="bool"),
                Bool(
                    id="bool",
                    condition={"field": "$INPUT.flag", "operator": "eq", "value": True},
                    next="assign-KongJie",
                    else_next="assign-others",
                ),
                Assignment(
                    id="assign-KongJie",
                    outputType={"dataType": "string", "required": True},
                    output="KongJie",
                    next="assign-join",
                ),
                Assignment(
                    id="assign-others",
                    outputType={"dataType": "string", "required": True},
                    output="nobody",
                    next="assign-join",
                ),
                Assignment(
                    id="assign-join",
                    outputType={"dataType": "string", "required": True},
                    upstreamOutput=[
                        {"upstream": "assign-KongJie", "value": "$NODE.assign-KongJie"},
                        {"upstream": "assign-others", "value": "$NODE.assign-others"},
                    ],
                    next="end",
                ),
                End(id="end", **{"resultType": "success", "output": "$NODE.assign-join"}),
            ],
        )
        self.assertEqual("KongJie", flow.run({"flag": True}))
        self.assertEqual("nobody", flow.run({"flag": False}))


class AssignmentSourceValidationTestCase(TestCase):
    """plaita#111：``validate()`` 第二个断言 ``len(...) >= 0`` 恒真，
    「必须有 output」从未生效——缺 output 时构建期静默放行、运行期
    ``execute()`` 落入 ``else: return None`` 静默产出 None。
    """

    @staticmethod
    def _prop(data_type=types.STRING):
        return Property(data_type=data_type)

    # ---- 构建期：非法配置必须报错（ValueError，带节点 id） ----

    def test_missing_output_and_upstream_raises_at_validate(self):
        node = Assignment(id="assign-missing", output_type=self._prop())
        with self.assertRaises(ValueError) as ctx:
            node.validate()
        # 只钉「承重信息」：节点 id + 两类来源字段名。不逐字匹配整段文案——独立验收用
        # 「只改文案、不改行为」的手工变异证明过精确整段断言属**过拟合**（行为没变也红）。
        msg = str(ctx.exception)
        self.assertIn("'assign-missing'", msg)
        self.assertIn("output", msg)
        self.assertIn("upstream_output", msg)

    def test_missing_output_type_raises_at_validate(self):
        # 第一处断言同样换成显式 ValueError（python -O 下 assert 会整体消失）
        node = Assignment(id="assign-no-type", output="x")
        with self.assertRaises(ValueError) as ctx:
            node.validate()
        msg = str(ctx.exception)
        self.assertIn("'assign-no-type'", msg)
        self.assertIn("output_type", msg)

    # ---- 合法配置不得被误伤 ----

    def test_falsy_literal_output_passes_validation(self):
        # 假值字面量是 ``is not None`` 的合法 output，绝不能被真值判断当成缺失
        for value, dtype in ((0, types.INTEGER), (False, types.BOOL), ("", types.STRING)):
            with self.subTest(value=value):
                node = Assignment(id="assign-falsy", output_type=self._prop(dtype), output=value)
                node.validate()  # 不得抛错

    def test_falsy_literal_output_executes(self):
        # 走 `self.output is not None` 分支，值原样返回（不因假值被吞成 None）。
        # 注意 "" 用 types.ANY：``output_type=string`` + 空串会撞下面那条**既有**怪癖。
        for value, dtype in ((0, types.INTEGER), (False, types.BOOL), ("", types.ANY)):
            with self.subTest(value=value):
                node = Assignment(id="assign-falsy", output_type=self._prop(dtype), output=value)
                execution = MagicMock()
                execution.evaluate.return_value = value
                self.assertEqual(node.execute(execution), value)
                # evaluate 必须收到 output 字面量本身（杀死「把 value 置 None」类变异）
                execution.evaluate.assert_called_once_with(value)

    def test_empty_string_with_string_type_is_a_preexisting_quirk(self):
        """记录性断言：``output=""`` + ``output_type=string`` 会被类型检查拒绝。

        根因是 ``plaita/io.py`` 的 string 判定写成 ``isinstance(obj, str) and obj``
        ——空串被当假值。**基线（ae33c01）与本补丁行为一致**，属既有语义而非 #111 引入；
        上面那条假值用例因此对 ``""`` 用 ``types.ANY``。钉住它，免得日后被误判成回归。
        """
        node = Assignment(id="assign-empty-str",
                          output_type=self._prop(types.STRING), output="")
        execution = MagicMock()
        execution.evaluate.return_value = ""
        with self.assertRaises(ValueError) as ctx:
            node.execute(execution)
        self.assertIn("output_type", str(ctx.exception))

    def test_upstream_output_only_passes_validation(self):
        node = Assignment(
            id="assign-upstream",
            output_type=self._prop(),
            upstream_output=[{"upstream": "u", "value": "$NODE.u"}],
        )
        node.validate()  # 不得抛错

    def test_single_upstream_output_branch_unchanged(self):
        node = Assignment(
            id="asg-1up",
            output_type=self._prop(),
            upstream_output=[{"upstream": "u", "value": "$NODE.u"}],
        )
        execution = MagicMock()
        execution.evaluate.return_value = "X"
        self.assertEqual(node.execute(execution), "X")
        # value 必须是 upstream_output[0]["value"]（杀死「置 None」类变异）
        execution.evaluate.assert_called_once_with("$NODE.u")

    def test_multi_upstream_output_selects_last_node(self):
        node = Assignment(
            id="asg-2up",
            output_type=self._prop(),
            upstream_output=[
                {"upstream": "u1", "value": "$NODE.u1"},
                {"upstream": "u2", "value": "$NODE.u2"},
            ],
        )
        execution = MagicMock()
        execution.state.last_node_id = "u2"
        execution.evaluate.return_value = "B"
        self.assertEqual(node.execute(execution), "B")
        execution.evaluate.assert_called_once_with("$NODE.u2")

    # ---- 运行期：保留历史兼容行为（本 issue 明确允许的 fallback 选择） ----

    def test_execute_missing_both_sources_still_returns_none_for_compat(self):
        """构建期已由 validate() 拦下；运行期暂保留 ``return None``。

        plaita#111 的修复方向③（运行期改抛错）会把无 output 的 assignment 从
        「静默 None」变成硬失败——实测撞红 3 个既有用例，且仓内 7+ 个测试文件
        用无 output 的 assignment 仅做「推进节点」用途。按 issue「若担心兼容性，
        至少保证构建期可见」取最小修法，运行期语义**不变**；本用例把这一选择
        钉住：日后若决定改成抛错，这里会红，逼迫有意识地同步改动。
        """
        node = Assignment(id="assign-runtime", output_type=self._prop())
        self.assertIsNone(node.execute(MagicMock()))


# ---------------------------------------------------------------------------
# 强化：精确断言 Condition / ConditionGroup / parse / Switch.execute 的边界语义。
# 杀死仅靠「能跑通」蒙混的变异点（prefix 透传、None 处理分支、parse 的 and/or、
# 日志文案、_create_condition_group 缺省值）。
# ---------------------------------------------------------------------------


class _FakeExecution:
    """最小执行上下文，供 Switch.execute 直接调用。"""

    def __init__(self, context, express_prefix="$"):
        self.context = context
        self.express_prefix = express_prefix


class TestConditionMatchSemantics(TestCase):
    """Condition.match 的 prefix 透传与 None 处理分支。"""

    def test_default_prefix_resolves_variable(self):
        # 不传 prefix 时使用默认 "$" —— 变异默认前缀会令变量无法解析
        cond = Condition(field="$X", operator="eq", value=5)
        self.assertIs(cond.match({"$X": 5}), True)
        self.assertIs(cond.match({"$X": 6}), False)

    def test_custom_prefix_propagates_to_field(self):
        # prefix="#" 必须透传给 evaluate(field) —— 丢前缀会让 #X 不解析
        cond = Condition(field="#X", operator="eq", value=5)
        self.assertIs(cond.match({"#X": 5}, "#"), True)

    def test_custom_prefix_propagates_to_value(self):
        # prefix="#" 必须透传给 evaluate(value) —— 丢前缀会让 #X 不解析
        cond = Condition(field=5, operator="eq", value="#X")
        self.assertIs(cond.match({"#X": 5}, "#"), True)

    def test_none_left_with_non_eq_operator_returns_false(self):
        # left=None, right 非 None, op=gt: 原逻辑走 None 分支返回 False；
        # 把 `or` 改成 `and` 的变异会跳过分支并对 None>5 抛 TypeError
        cond = Condition(field="$A", operator="gt", value=5)
        self.assertIs(cond.match({"$A": None}, "$"), False)

    def test_both_none_eq_returns_true(self):
        # 两侧都 None + eq => True（`left is right`）。
        # 把 == 改成 != 的变异会落进 else 返回 False；把 `is` 改成 `is not` 同理
        cond = Condition(field="$A", operator="eq", value="$B")
        self.assertIs(cond.match({"$A": None, "$B": None}, "$"), True)

    def test_both_none_ne_returns_false(self):
        # 两侧都 None + ne => `left is not right` => False
        cond = Condition(field="$A", operator="ne", value="$B")
        self.assertIs(cond.match({"$A": None, "$B": None}, "$"), False)

    def test_none_left_eq_against_value_returns_false(self):
        # left=None, right=5, eq => None is 5 => False
        cond = Condition(field="$A", operator="eq", value=5)
        self.assertIs(cond.match({"$A": None}, "$"), False)


class TestConditionGroupMatchSemantics(TestCase):
    """ConditionGroup.match 的 prefix 透传与空条件。"""

    def test_empty_conditions_returns_true(self):
        group = ConditionGroup(relation="and", conditions=[])
        self.assertIs(group.match({}, "$"), True)

    def test_custom_prefix_propagates_to_conditions(self):
        # prefix="#" 必须透传给每个 condition.match —— 丢前缀会让 #X 不解析
        group = ConditionGroup(
            relation="and",
            conditions=[Condition(field="#X", operator="eq", value=5)],
        )
        self.assertIs(group.match({"#X": 5}, "#"), True)

    def test_or_relation_any_matches(self):
        group = ConditionGroup(
            relation="or",
            conditions=[
                Condition(field="$A", operator="eq", value=1),
                Condition(field="$B", operator="eq", value=2),
            ],
        )
        self.assertIs(group.match({"$A": 99, "$B": 2}, "$"), True)
        self.assertIs(group.match({"$A": 99, "$B": 99}, "$"), False)


class TestParseConditionContent(TestCase):
    """_parse_condition_content 的 and/or 分支必须精确。"""

    def test_relation_only_without_conditions_returns_none(self):
        # 只有 relation 没有 conditions —— `and` 改 `or` 的变异会误判成 group
        self.assertIsNone(condition_from_json({"relation": "and"}))

    def test_field_operator_without_value_returns_none(self):
        # field+operator 但缺 value —— `and` 改 `or` 的变异会误判成 condition
        self.assertIsNone(condition_from_json({"field": "x", "operator": "eq"}))

    def test_operator_value_without_field_returns_none(self):
        # operator+value 但缺 field —— 同理
        self.assertIsNone(condition_from_json({"operator": "eq", "value": 5}))

    def test_full_group_parsed(self):
        result = condition_from_json({"relation": "and", "conditions": []})
        self.assertIsInstance(result, ConditionGroup)
        self.assertEqual(result.relation, "and")

    def test_full_condition_parsed(self):
        result = condition_from_json({"field": "x", "operator": "eq", "value": 5})
        self.assertIsInstance(result, Condition)
        self.assertEqual(result.field, "x")
        self.assertEqual(result.operator, "eq")
        self.assertEqual(result.value, 5)


class TestCreateConditionGroupDefault(TestCase):
    """_create_condition_group 缺 conditions 键时必须回退到空列表而非 None。"""

    def test_missing_conditions_key_defaults_to_empty(self):
        # 直接调用 _create_condition_group，缺 conditions 键。
        # 默认值改成 None 的变异会让 map(None) 抛 TypeError
        group = _create_condition_group({"relation": "and"})
        self.assertEqual(group.relation, "and")
        self.assertEqual(group.conditions, [])

    def test_conditions_key_present_uses_it(self):
        group = _create_condition_group({
            "relation": "or",
            "conditions": [{"field": "x", "operator": "eq", "value": 1}],
        })
        self.assertEqual(len(group.conditions), 1)


class TestSwitchExecuteLogging(TestCase):
    """Switch.execute 命中分支 / 默认分支时的日志文案必须精确。"""

    def _make_switch(self):
        return Switch(
            id="sw",
            branches=[
                {"name": "hit", "next": "next_hit",
                 "condition": {"field": "$INPUT.x", "operator": "eq", "value": 1}},
                {"name": "def", "isDefault": True, "next": "next_def"},
            ],
        )

    def test_matching_branch_logs_exact_message(self):
        sw = self._make_switch()
        with self.assertLogs("plaita", level="INFO") as cm:
            result = sw.execute(_FakeExecution({"$INPUT": {"x": 1}}, "$"))
        self.assertEqual(result, "next_hit")
        self.assertTrue(
            any("test branches, hit, next_hit" in m for m in cm.output),
            f"expected branch-hit log not found in {cm.output}",
        )

    def test_default_branch_logs_exact_message(self):
        sw = self._make_switch()
        with self.assertLogs("plaita", level="INFO") as cm:
            result = sw.execute(_FakeExecution({"$INPUT": {"x": 999}}, "$"))
        self.assertEqual(result, "next_def")
        self.assertTrue(
            any("default branches def, next_def" in m for m in cm.output),
            f"expected default-branch log not found in {cm.output}",
        )

class CaseWithoutNameTestCase(TestCase):
    """#112：``case`` 的 ``name`` 是可选展示名，缺失时不得裸 KeyError。"""

    def test_case_without_name_constructs_and_label_is_none(self):
        node = SwitchLegacy(
            id="c", target="$INPUT.k", cases=[{"id": "a", "value": 1}], default="e")
        self.assertEqual("a", node.branches[0].name)
        self.assertEqual("a", node.branches[0].next)
        self.assertIsNone(node.branches[0].label,
                          "缺 name 时 label 为 None（Branch.label 本就 Optional）")
        self.assertTrue(node.branches[-1].is_default)
        # 有 name 时必须原样透传到 label（钉住取值点，防「读了别的键」仍绿）
        named = SwitchLegacy(
            id="c", target="$INPUT.k", cases=[{"id": "a", "name": "甲", "value": 1}], default="e")
        self.assertEqual("甲", named.branches[0].label)

    def test_case_without_name_passes_ir_validate_and_model_validate(self):
        ir = {
            "flow_id": "case_no_name",
            "version": "1.0",
            "runtime": "python",
            "nodes": [
                {"type": "start", "id": "s", "next": "c"},
                {"type": "case", "id": "c", "target": "$INPUT.k", "default": "e",
                 "cases": [{"id": "a", "value": 1}]},
                # case 分支的 next 恒为 case 的 id ⇒ 必须有同名节点承接
                {"type": "end", "id": "a", "resultType": "success"},
                {"type": "end", "id": "e", "resultType": "success"},
            ],
        }
        validate_flow_ir(ir)                      # 修复前：KeyError('name')
        flow = Flow.model_validate(ir)            # 修复前：KeyError('name')
        self.assertEqual("a", flow.nodes[1].branches[0].name)

    def test_case_missing_id_or_value_still_value_error(self):
        with self.assertRaises(ValueError) as cm:
            SwitchLegacy(id="c", target="$INPUT.k", cases=[{"value": 1}], default="e")
        msg = str(cm.exception)
        self.assertIn("cases[0]", msg)
        self.assertIn("'id'", msg)
        with self.assertRaises(ValueError) as cm2:
            SwitchLegacy(id="c", target="$INPUT.k", cases=[{"id": "a"}], default="e")
        msg2 = str(cm2.exception)
        self.assertIn("cases[0]", msg2)
        self.assertIn("'value'", msg2)
