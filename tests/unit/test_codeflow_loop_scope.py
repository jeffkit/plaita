"""codeflow DSL 集合循环（MAP/FILTER/FIND/LOOP/REDUCE）作用域：编译形态 + 端到端。

钉死的语义（jeffkit/plaita#16 内核侧修复）：
- 循环目标名映射子流程输入（$INPUT.item / $INPUT[0..1]）；
- 外层已赋值变量自动映射 ``$PARENT.NODE.<名>`` 快照引用——与 while 的
  ``_while_child_flow`` 同一约定。体内可读集合节点执行前已确定的父侧变量；
- 子流程写不回父 context，循环目标名遮蔽外层同名（与 Python 遮蔽规则一致）；
- 嵌套集合循环：外层目标名在内层体里经 ``$PARENT.INPUT.*`` 快照可达。
"""
from __future__ import annotations

import ast
import unittest

from plaita.dsl.codeflow import F, MAP, NODE, PARENT, REDUCE, flow, flow_from_source
from plaita.dsl.codeflow._common import _CompileCtx
from plaita.dsl.codeflow._stmt import _compile_block

import plaita.node  # noqa: F401  # 注册默认节点


def _stmts(code: str, lineno: int = 7) -> list[ast.stmt]:
    tree = ast.parse(code.strip())
    for i, stmt in enumerate(tree.body):
        ln = lineno + i
        for node in ast.walk(stmt):
            if isinstance(node, ast.AST):
                node.lineno = ln  # type: ignore[attr-defined]
    return tree.body


def _child_nodes(spec: dict) -> dict:
    return {n["id"]: n for n in spec["childFlow"]["nodes"]}


class TestCollectionLoopParentScopeCompile(unittest.TestCase):
    def test_map_body_reads_outer_assignment(self):
        ctx = _CompileCtx()
        _compile_block(_stmts(
            "limit = INPUT.base\n"
            "for x in MAP(INPUT.items, id=\"mp\"):\n"
            "    return F.mul(x, limit)\n"
            "return NODE.mp\n"
        ), ctx, None)
        spec = next(n for n in ctx.nodes if n["type"] == "map")
        body = _child_nodes(spec)
        ends = [n for n in body.values() if n["type"] == "end"]
        self.assertEqual(ends[0]["output"], "$F.mul($INPUT.item, $PARENT.NODE.limit)")

    def test_reduce_body_reads_outer_assignment(self):
        ctx = _CompileCtx()
        _compile_block(_stmts(
            "limit = INPUT.base\n"
            "for acc, it in REDUCE(INPUT.items, initial=[], id=\"rd\"):\n"
            "    return F.add(acc, F.mul(it, limit))\n"
            "return NODE.rd\n"
        ), ctx, None)
        spec = next(n for n in ctx.nodes if n["type"] == "reduce")
        body = _child_nodes(spec)
        ends = [n for n in body.values() if n["type"] == "end"]
        self.assertEqual(
            ends[0]["output"],
            "$F.add($INPUT[0], $F.mul($INPUT[1], $PARENT.NODE.limit))",
        )

    def test_loop_target_shadows_outer_name(self):
        """循环目标名遮蔽外层同名：体内用 $INPUT.item，块外回退外层赋值。"""
        ctx = _CompileCtx()
        _compile_block(_stmts(
            "x = INPUT.base\n"
            "for x in MAP(INPUT.items, id=\"mp\"):\n"
            "    return x\n"
            "return F.concat(NODE.mp, \"-\", x)\n"
        ), ctx, None)
        spec = next(n for n in ctx.nodes if n["type"] == "map")
        body = _child_nodes(spec)
        ends = [n for n in body.values() if n["type"] == "end"]
        self.assertEqual(ends[0]["output"], "$INPUT.item")
        tail = next(n for n in ctx.nodes if n["type"] == "end")
        self.assertEqual(tail["output"], "$F.concat($NODE.mp, \"-\", $NODE.x)")

    def test_nested_loop_outer_target_reachable(self):
        """嵌套集合循环：外层目标名经 $PARENT.INPUT.* 快照在内层体可达。"""
        ctx = _CompileCtx()
        _compile_block(_stmts(
            "for a in MAP(INPUT.groups, id=\"m1\"):\n"
            "    for b in MAP(a.members, id=\"m2\"):\n"
            "        return b\n"
            "    return NODE.m2\n"
            "return NODE.m1\n"
        ), ctx, None)
        outer = next(n for n in ctx.nodes if n["id"] == "m1")
        inner = next(
            n for n in outer["childFlow"]["nodes"] if n.get("type") == "map")
        # 内层 collection 在外层子流程执行体求值：a 即外层子流程的 $INPUT.item
        self.assertEqual(inner["collection"], "$INPUT.item.members")
        body = _child_nodes(inner)
        ends = [n for n in body.values() if n["type"] == "end"]
        # b 是内层循环目标，仍映射内层子流程输入
        self.assertEqual(ends[0]["output"], "$INPUT.item")


class TestCollectionLoopParentScopeRuntime(unittest.TestCase):
    def test_reduce_aggregates_with_outer_variable(self):
        @flow("loop_scope_reduce")
        def loop_scope_reduce(INPUT):
            limit = F.mul(INPUT.base, 100)
            for acc, it in REDUCE(INPUT.items, initial=[], id="rd"):
                return F.append(acc, F.mul(it, limit))
            return NODE.rd

        self.assertEqual(
            loop_scope_reduce.run(base=2, items=[1, 2, 3]), [200, 400, 600])

    def test_map_body_reads_outer_variable(self):
        @flow("loop_scope_map")
        def loop_scope_map(INPUT):
            scale = INPUT.base
            for it in MAP(INPUT.items, id="mp"):
                return F.mul(it, scale)
            return NODE.mp

        self.assertEqual(loop_scope_map.run(base=10, items=[1, 2, 3]),
                         [10, 20, 30])

    def test_nested_loops_end_to_end(self):
        @flow("loop_scope_nested")
        def loop_scope_nested(INPUT):
            for a in MAP(INPUT.groups, id="m1"):
                # a 映射进内层体为 $PARENT.INPUT.item（外层子流程快照的输入）
                for b in MAP(a.members, id="m2"):
                    return F.concat(a.tag, ":", b)
                return NODE.m2
            return NODE.m1

        self.assertEqual(
            loop_scope_nested.run(
                groups=[{"tag": "t", "members": ["x", "y"]},
                        {"tag": "u", "members": ["z"]}]),
            [["t:x", "t:y"], ["u:z"]],
        )


if __name__ == "__main__":
    unittest.main()
