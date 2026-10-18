"""plaita#220-L2：parallel 分支 ``input`` / ``condition`` 双链往返保真。

第八轮清单把 L2 记为「@flow 只发 ``branches={name: child}``，分支
``input``/``condition`` 全丢」。基础表达面已由 9c8820a 补齐；本模块锁定该面
的**残余**——同一对字段在两条链上仍有两类可观测丢失：

1. ``@flow`` 链：分支条件算子 ``contains`` / ``notContains`` 无法发射
   （``_invert_cond`` 只认中缀比较算子）⇒ 整条 flow ``EmitError``，分支条件
   不是「丢失」而是「不可表达」。
2. ``sexpr`` 链：分支 ``input`` 与条件 ``value`` 走表达式发射器，裸原子约定
   把字面字符串 ``"true"`` / ``"42"`` / ``"nil"`` 读回成 ``True`` / ``42`` /
   ``None``；嵌套 dict/list 内的同类字符串同样漂移（与 #220/L6 的
   headers/body 漂移同根，但这是分支字段自己的两个槽位）。

每个用例都是「IR → 两链往返 → 字段逐项相等」；负控口径：在修复前，
``@flow`` 用例以 ``EmitError: 不支持的条件算子`` 变红，``sexpr`` 用例以
``'true' != True`` 变红。
"""
from __future__ import annotations

import unittest

from plaita.dsl.codeflow import compile_source, emit_source
from plaita.dsl.sexpr import compile_sexpr, flow_to_sexpr


def _child(output="$INPUT.item"):
    return {
        "runtime": "python",
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "cs", "next": "ce"},
            {"type": "end", "id": "ce", "output": output, "resultType": "success"},
        ],
    }


def _flow(branches, flow_id="t220l2"):
    return {
        "flow_id": flow_id,
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "s", "next": "p"},
            {"type": "parallel", "id": "p", "branches": branches, "next": "e"},
            {"type": "end", "id": "e", "output": "done", "resultType": "success"},
        ],
    }


def _branch(condition=None, input_value="$INPUT.item", name="a"):
    branch = {"name": name, "flow": _child()}
    if input_value is not None:
        branch["input"] = input_value
    if condition is not None:
        branch["condition"] = condition
    return branch


def _emit_rt(ir):
    source = emit_source(ir)
    return compile_source(source), source


def _sexpr_rt(ir):
    source = flow_to_sexpr(ir)
    return compile_sexpr(source), source


def _branch_of(back):
    parallel = [n for n in back["nodes"] if n["type"] == "parallel"]
    assert len(parallel) == 1, back
    return parallel[0]["branches"][0]


#: 全部 12 个条件算子的规范 IR 形态。``truthy`` / ``falsy`` 的 ``value`` 不参与
#: 引擎比较（``Condition.match`` 直接 ``bool(left)`` / ``not bool(left)``），编译
#: 产物恒为 ``True``；这里也写成 ``True``，让「字段逐项相等」的口径成立。
_OPERATORS = (
    ("eq", "a"),
    ("ne", "a"),
    ("gt", 1),
    ("gte", 1),
    ("lt", 1),
    ("lte", 1),
    ("in", ["a", "b"]),
    ("notIn", ["a", "b"]),
    ("contains", "a"),
    ("notContains", "a"),
    ("truthy", True),
    ("falsy", True),
)


class TestFlowChainBranchConditionOperators(unittest.TestCase):
    """@flow：分支条件必须覆盖引擎支持的全部算子（含 contains/notContains）。"""

    def test_every_operator_round_trips_with_branch_input(self):
        for operator, value in _OPERATORS:
            with self.subTest(operator=operator):
                condition = {
                    "field": "$INPUT.x",
                    "operator": operator,
                    "value": value,
                }
                back, source = _emit_rt(_flow([_branch(condition=condition)]))
                branch = _branch_of(back)
                self.assertEqual(branch.get("input"), "$INPUT.item", source)
                self.assertEqual(branch.get("condition"), condition, source)

    def test_contains_uses_f_contains_spelling(self):
        """contains 的 @flow 拼写是 ``F.contains(field, value)``（b in a）。"""
        condition = {"field": "$INPUT.x", "operator": "contains", "value": "a"}
        source = emit_source(_flow([_branch(condition=condition)]))
        self.assertIn("F.contains(INPUT.x, 'a')", source)

    def test_not_contains_uses_negated_spelling(self):
        condition = {"field": "$INPUT.x", "operator": "notContains", "value": "a"}
        source = emit_source(_flow([_branch(condition=condition)]))
        self.assertIn("not F.contains(INPUT.x, 'a')", source)

    def test_compound_condition_round_trips(self):
        condition = {
            "relation": "and",
            "conditions": [
                {"field": "$INPUT.x", "operator": "eq", "value": 1},
                {
                    "relation": "or",
                    "conditions": [
                        {"field": "$INPUT.y", "operator": "contains", "value": "k"},
                        {"field": "$INPUT.z", "operator": "notContains", "value": "j"},
                    ],
                },
            ],
        }
        back, source = _emit_rt(_flow([_branch(condition=condition)]))
        self.assertEqual(_branch_of(back).get("condition"), condition, source)


class TestFlowChainBranchInputTypes(unittest.TestCase):
    """@flow：分支 input 的 Python 字面量类型不得漂移（对照面）。"""

    def test_literal_scalars_and_containers_round_trip(self):
        values = (
            "$INPUT.item",
            "true",
            "42",
            "nil",
            "a b",
            42,
            True,
            None,
            {"a": 1, "b": "true"},
            ["a", 1, True],
        )
        for value in values:
            with self.subTest(value=value):
                back, source = _emit_rt(
                    _flow([_branch(input_value=value, condition=None)]))
                self.assertEqual(_branch_of(back).get("input"), value, source)


class TestSexprChainBranchInputTypes(unittest.TestCase):
    """sexpr：分支 input 是**数据**槽位，标量形状字符串必须恒加引号。"""

    def test_scalar_looking_strings_stay_strings(self):
        for value in ("true", "false", "42", "-7", "nil"):
            with self.subTest(value=value):
                back, source = _sexpr_rt(
                    _flow([_branch(input_value=value, condition=None)]))
                self.assertIs(type(_branch_of(back).get("input")), str, source)
                self.assertEqual(_branch_of(back).get("input"), value, source)

    def test_nested_containers_keep_string_elements(self):
        value = {"a": "true", "b": ["42", 1, {"c": "nil"}]}
        back, source = _sexpr_rt(
            _flow([_branch(input_value=value, condition=None)]))
        self.assertEqual(_branch_of(back).get("input"), value, source)

    def test_expression_and_native_scalars_round_trip(self):
        for value in ("$INPUT.item", 42, True, None, ["a", 1]):
            with self.subTest(value=value):
                back, source = _sexpr_rt(
                    _flow([_branch(input_value=value, condition=None)]))
                self.assertEqual(_branch_of(back).get("input"), value, source)

    def test_emitted_input_is_quoted_for_scalar_string(self):
        source = flow_to_sexpr(_flow([_branch(input_value="true")]))
        self.assertIn(':input "true"', source)
        self.assertNotIn(":input true", source)


class TestSexprChainBranchConditionValueTypes(unittest.TestCase):
    """sexpr：条件 value 是**数据**槽位，类型必须与 IR 逐项相等。"""

    def test_scalar_looking_strings_stay_strings(self):
        for value in ("true", "false", "42", "-7", "nil"):
            with self.subTest(value=value):
                condition = {
                    "field": "$INPUT.x", "operator": "eq", "value": value,
                }
                back, source = _sexpr_rt(_flow([_branch(condition=condition)]))
                self.assertEqual(
                    _branch_of(back).get("condition"), condition, source)

    def test_nested_containers_keep_string_elements(self):
        for value in (["a", "true", 1], {"a": "true", "b": ["nil", 2]}):
            with self.subTest(value=value):
                condition = {
                    "field": "$INPUT.x", "operator": "in", "value": value,
                }
                back, source = _sexpr_rt(_flow([_branch(condition=condition)]))
                self.assertEqual(
                    _branch_of(back).get("condition"), condition, source)

    def test_every_operator_round_trips(self):
        for operator, value in _OPERATORS:
            with self.subTest(operator=operator):
                condition = {
                    "field": "$INPUT.x", "operator": operator, "value": value,
                }
                back, source = _sexpr_rt(_flow([_branch(condition=condition)]))
                self.assertEqual(
                    _branch_of(back).get("condition"), condition, source)

    def test_compound_condition_round_trips(self):
        condition = {
            "relation": "or",
            "conditions": [
                {"field": "$INPUT.x", "operator": "eq", "value": "true"},
                {
                    "relation": "and",
                    "conditions": [
                        {"field": "$INPUT.y", "operator": "contains",
                         "value": "42"},
                        {"field": "$INPUT.z", "operator": "notIn",
                         "value": ["nil", "x"]},
                    ],
                },
            ],
        }
        back, source = _sexpr_rt(_flow([_branch(condition=condition)]))
        self.assertEqual(_branch_of(back).get("condition"), condition, source)

    def test_emitted_value_is_quoted_for_scalar_string(self):
        condition = {"field": "$INPUT.x", "operator": "eq", "value": "true"}
        source = flow_to_sexpr(_flow([_branch(condition=condition)]))
        self.assertIn('eq "true"', source)
        self.assertNotIn("eq true", source)


class TestBothChainsBranchesWithoutExtras(unittest.TestCase):
    """没有 input/condition 的分支保持既有形状（防回归）。"""

    def test_plain_branch_round_trips(self):
        ir = _flow([
            {"name": "a", "flow": _child()},
            {"name": "b", "flow": _child()},
        ])
        for chain, rt in (("flow", _emit_rt), ("sexpr", _sexpr_rt)):
            with self.subTest(chain=chain):
                back, source = rt(ir)
                branches = [
                    n for n in back["nodes"] if n["type"] == "parallel"
                ][0]["branches"]
                self.assertEqual([b.get("name") for b in branches], ["a", "b"],
                                 source)
                for branch in branches:
                    self.assertNotIn("input", branch, source)
                    self.assertNotIn("condition", branch, source)

    def test_input_only_and_condition_only(self):
        cases = (
            _branch(input_value="$INPUT.x", condition=None, name="input_only"),
            _branch(input_value=None, name="condition_only",
                    condition={"field": "$INPUT.flag", "operator": "truthy",
                               "value": True}),
        )
        for branch_ir in cases:
            for chain, rt in (("flow", _emit_rt), ("sexpr", _sexpr_rt)):
                with self.subTest(branch=branch_ir["name"], chain=chain):
                    back, source = rt(_flow([branch_ir]))
                    branch = _branch_of(back)
                    for field in ("input", "condition"):
                        if field in branch_ir:
                            self.assertEqual(branch.get(field), branch_ir[field],
                                             source)
                        else:
                            self.assertNotIn(field, branch, source)


if __name__ == "__main__":
    unittest.main()
