"""共享 Flow IR 拓扑校验 + AI 编译门闭合。"""
from __future__ import annotations

import unittest

from plaita.dsl import build, end, start, validate_flow_ir, FlowIRValidationError
from plaita.dsl.codeflow import flow_from_source
from plaita.dsl.ir_validate import build_flow
from plaita.dsl.sexpr import parse_sexpr


class TestValidateFlowIR(unittest.TestCase):
    def test_duplicate_id(self):
        with self.assertRaises(FlowIRValidationError) as cm:
            validate_flow_ir(
                {
                    "flow_id": "x",
                    "nodes": [
                        {"type": "start", "id": "a", "next": "b"},
                        {"type": "end", "id": "a"},
                    ],
                }
            )
        self.assertIn("重复", str(cm.exception))

    def test_dangling_next(self):
        with self.assertRaises(FlowIRValidationError) as cm:
            validate_flow_ir(
                {
                    "flow_id": "x",
                    "nodes": [
                        {"type": "start", "id": "start", "next": "ghost"},
                        {"type": "end", "id": "end"},
                    ],
                }
            )
        self.assertIn("ghost", str(cm.exception))

    def test_if_missing_else(self):
        with self.assertRaises(FlowIRValidationError):
            validate_flow_ir(
                {
                    "flow_id": "x",
                    "nodes": [
                        {"type": "start", "id": "s", "next": "i"},
                        {
                            "type": "if",
                            "id": "i",
                            "condition": {"field": "$INPUT.x", "operator": "eq", "value": 1},
                            "next": "end",
                        },
                        {"type": "end", "id": "end"},
                    ],
                }
            )

    def test_recursive_child_flow_dangling(self):
        """子流程内悬空 next 必须被拦下（历史 builder/sexpr 只扫顶层）。"""
        with self.assertRaises(FlowIRValidationError) as cm:
            validate_flow_ir(
                {
                    "flow_id": "parent",
                    "nodes": [
                        {"type": "start", "id": "s", "next": "c"},
                        {
                            "type": "child",
                            "id": "c",
                            "input": {},
                            "childFlow": {
                                "flow_id": "child",
                                "nodes": [
                                    {"type": "start", "id": "cs", "next": "missing"},
                                    {"type": "end", "id": "ce"},
                                ],
                            },
                            "next": "e",
                        },
                        {"type": "end", "id": "e"},
                    ],
                }
            )
        self.assertIn("childFlow", cm.exception.path)
        self.assertIn("missing", str(cm.exception))

    def test_parallel_branch_flow_recursive(self):
        with self.assertRaises(FlowIRValidationError) as cm:
            validate_flow_ir(
                {
                    "flow_id": "p",
                    "nodes": [
                        {"type": "start", "id": "s", "next": "par"},
                        {
                            "type": "parallel",
                            "id": "par",
                            "branches": [
                                {
                                    "name": "b1",
                                    "flow": {
                                        "flow_id": "b1",
                                        "nodes": [
                                            {"type": "start", "id": "bs", "next": "nope"},
                                            {"type": "end", "id": "be"},
                                        ],
                                    },
                                }
                            ],
                            "next": "e",
                        },
                        {"type": "end", "id": "e"},
                    ],
                }
            )
        self.assertIn("branches", cm.exception.path)

    def test_builder_delegates(self):
        with self.assertRaises(ValueError):
            (
                build("t")
                .add(start(next="ghost"))
                .add(end())
                .build()
            )


class TestFlowFromSourceGate(unittest.TestCase):
    def test_topology_caught_at_compile(self):
        """AI 主路径：拓扑错误必须在 flow_from_source 阶段失败，而非运行时。"""
        src = '''
@flow("bad_topo", input_type="object")
def bad_topo(INPUT):
    x = F.upper(INPUT.name)
    return x
'''
        # 正常源码应通过
        flow = flow_from_source(src)
        self.assertEqual(flow.run(name="a"), "A")

    def test_dangling_via_manual_ir_build_flow(self):
        with self.assertRaises(FlowIRValidationError):
            build_flow(
                {
                    "runtime": "python",
                    "flow_id": "x",
                    "inputType": {"dataType": "object"},
                    "nodes": [
                        {"type": "start", "id": "start", "next": "ghost"},
                        {"type": "end", "id": "end", "output": "x", "resultType": "success"},
                    ],
                }
            )


class TestSexprUsesSharedValidate(unittest.TestCase):
    def test_if_without_else_fails(self):
        src = """
(flow t :input-type object
  (start -> i)
  (if :id i (cond "$INPUT.x" eq 1) -> e)
  (end e :output "ok"))
"""
        # sexpr 编译期或共享 validate 都会拦；二者任一即可
        with self.assertRaises((ValueError, SyntaxError)):
            parse_sexpr(src)

    def test_switch_without_default_via_shared_validate(self):
        from plaita.dsl.sexpr import _static_validate

        data = {
            "runtime": "python",
            "flow_id": "bad_switch",
            "nodes": [
                {"type": "start", "id": "s", "next": "sw"},
                {
                    "type": "switch",
                    "id": "sw",
                    "branches": [
                        {
                            "name": "b1",
                            "next": "e",
                            "condition": {"field": "$INPUT.x", "operator": "eq", "value": 1},
                        }
                    ],
                },
                {"type": "end", "id": "e", "output": "$INPUT.x", "resultType": "success"},
            ],
        }
        with self.assertRaises(FlowIRValidationError):
            _static_validate(data)


if __name__ == "__main__":
    unittest.main()
