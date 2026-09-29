"""assignment 节点作为表达式载体 + 表达式位置比较/and/or/not/三元 的行为测试。

配套 _expr.py「表达式位置放开」（feat/expr-in-assignment）：
  - 注册表新增比较函数（eq/ne/gt/...）与 ifelse，函数名 == if 节点条件算子名；
  - and/or/比较/not/三元在表达式位置（赋值右侧、节点参数）编译为 $F.*；
  - if 条件位置不受影响，仍编译为结构化 Condition/ConditionGroup。
"""
from __future__ import annotations

import unittest

from plaita.dsl.codeflow import compile_source, flow
from plaita.core.flow import Flow


@flow("expr_coalesce")
def expr_coalesce(INPUT):
    cmd = INPUT.test_command or "true"
    return cmd


@flow("expr_ternary")
def expr_ternary(INPUT):
    label = "high" if INPUT.risk == "high" else "low"
    return label


@flow("expr_predicate")
def expr_predicate(INPUT):
    need = INPUT.mode == "human" and INPUT.risk == "high"
    neg = not INPUT.closed
    chain = 0 < INPUT.n < 10
    return {"need": need, "neg": neg, "chain": chain}


@flow("expr_condition_position_unchanged")
def expr_condition_position_unchanged(INPUT):
    if INPUT.mode == "human" and INPUT.risk == "high":
        return "need_human"
    return "auto"


class TestExprPositionCompilation(unittest.TestCase):
    """@flow 源码 -> IR 形状断言。"""

    def test_coalesce_compiles_to_F_or(self):
        ir = compile_source("""
from plaita.dsl.codeflow import flow
@flow("t")
def t(INPUT):
    cmd = INPUT.test_command or "true"
    return cmd
""")
        out = [n["output"] for n in ir["nodes"] if n["type"] == "assignment"]
        self.assertEqual(out, ['$F.or($INPUT.test_command, "true")'])

    def test_ternary_compiles_to_F_ifelse(self):
        ir = compile_source("""
from plaita.dsl.codeflow import flow
@flow("t")
def t(INPUT):
    label = "high" if INPUT.risk == "high" else "low"
    return label
""")
        out = [n["output"] for n in ir["nodes"] if n["type"] == "assignment"]
        self.assertEqual(out, ['$F.ifelse($F.eq($INPUT.risk, "high"), "high", "low")'])

    def test_and_chain_folds_left(self):
        # a and b and c 的 Python 语义是 (a and b) and c —— 返回第一个假值本身
        ir = compile_source("""
from plaita.dsl.codeflow import flow
@flow("t")
def t(INPUT):
    v = INPUT.a and INPUT.b and INPUT.c
    return v
""")
        out = [n["output"] for n in ir["nodes"] if n["type"] == "assignment"]
        self.assertEqual(out, ["$F.and($F.and($INPUT.a, $INPUT.b), $INPUT.c)"])

    def test_chained_compare_compiles_to_and_of_ops(self):
        ir = compile_source("""
from plaita.dsl.codeflow import flow
@flow("t")
def t(INPUT):
    v = 0 < INPUT.n < 10
    return v
""")
        out = [n["output"] for n in ir["nodes"] if n["type"] == "assignment"]
        self.assertEqual(out, ["$F.and($F.lt(0, $INPUT.n), $F.lt($INPUT.n, 10))"])

    def test_condition_position_still_structured_group(self):
        # 条件位置不受放开影响：仍是结构化 ConditionGroup（console 编辑器依赖）
        ir = compile_source("""
from plaita.dsl.codeflow import flow
@flow("t")
def t(INPUT):
    if INPUT.mode == "human" and INPUT.risk == "high":
        return "need_human"
    return "auto"
""")
        cond = [n["condition"] for n in ir["nodes"] if n["type"] == "if"][0]
        self.assertEqual(cond["relation"], "and")
        self.assertEqual(len(cond["conditions"]), 2)
        self.assertEqual(cond["conditions"][0],
                         {"field": "$INPUT.mode", "operator": "eq", "value": "human"})


class TestExprEndToEnd(unittest.TestCase):
    """IR -> 引擎真实执行。"""

    def run_flow(self, flow_obj, **params):
        return flow_obj.run(**params)

    def test_coalesce_end_to_end(self):
        self.assertEqual(self.run_flow(expr_coalesce, test_command=""), "true")
        self.assertEqual(self.run_flow(expr_coalesce, test_command="cargo test"), "cargo test")

    def test_ternary_end_to_end(self):
        self.assertEqual(self.run_flow(expr_ternary, risk="high"), "high")
        self.assertEqual(self.run_flow(expr_ternary, risk="low"), "low")

    def test_predicate_end_to_end(self):
        r = self.run_flow(expr_predicate, mode="human", risk="high", closed=False, n=5)
        self.assertEqual(r, {"need": True, "neg": True, "chain": True})
        r2 = self.run_flow(expr_predicate, mode="auto", risk="high", closed=True, n=50)
        self.assertEqual(r2, {"need": False, "neg": False, "chain": False})

    def test_condition_position_end_to_end(self):
        self.assertEqual(
            self.run_flow(expr_condition_position_unchanged, mode="human", risk="high"),
            "need_human",
        )
        self.assertEqual(
            self.run_flow(expr_condition_position_unchanged, mode="auto", risk="high"),
            "auto",
        )


class TestAssignmentNodeSemantics(unittest.TestCase):
    """assignment 节点自身的求值/类型校验行为（hand-written IR）。"""

    IR_TMPL = {
        "name": "asg-sem",
        "nodes": [
            {"type": "start", "id": "start", "next": "asg"},
            {"type": "assignment", "id": "asg", "output": "__OUTPUT__", "next": "end"},
            {"type": "end", "id": "end", "output": {"v": "$NODE.asg"}},
        ],
    }

    def _run(self, output, output_type=None, params=None):
        import copy
        ir = copy.deepcopy(self.IR_TMPL)
        ir["nodes"][1]["output"] = output
        if output_type:
            ir["nodes"][1]["output_type"] = output_type
        fl = Flow.model_validate(ir)
        return fl.run(params or {})

    def test_falsy_literal_passthrough(self):
        # 假值字面量曾是合法赋值却被真值判断吞成 None
        self.assertEqual(self._run(0), {"v": 0})
        self.assertEqual(self._run(False), {"v": False})
        self.assertEqual(self._run(""), {"v": ""})

    def test_output_type_matches_evaluated_value(self):
        # output 是表达式串时，类型校验必须针对求值结果而非表达式字符串本身
        ir_type = {"type": "integer"}
        self.assertEqual(self._run("$F.add(1, 2)", output_type=ir_type), {"v": 3})

    def test_output_type_mismatch_raises_loudly(self):
        # 曾静默返回 None；现在大声失败（与仓内「消灭静默降级」基调一致）。
        # 引擎会把节点异常包成 NodeExecutionError，但原始消息保留在链上。
        from plaita.core.errors import NodeExecutionError
        with self.assertRaises(NodeExecutionError) as ctx:
            self._run("$F.concat('abc')", output_type={"type": "integer"})
        self.assertIn("assignment node 'asg'", str(ctx.exception))
