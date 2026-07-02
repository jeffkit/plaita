from unittest import TestCase

import plaita.core.errors as plaita_errors
from plaita.core.flow import Flow
from plaita.core import types
from plaita.io import Property
from plaita.node import End, Start
from plaita.node.assignment import Assignment
from plaita.node.decide import Bool, Condition, ConditionGroup, Logic, Switch, SwitchLegacy, condition_from_json


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
            input_type=Property(data_type=types.BOOL, is_required=True),
            output_type=Property(data_type=types.STRING),
            nodes=[
                Start(id="start", next="bool"),
                Bool(
                    id="bool",
                    condition={"field": "$INPUT", "operator": "eq", "value": True},
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
        self.assertEqual("KongJie", flow.run(True))
        self.assertEqual("nobody", flow.run(False))
