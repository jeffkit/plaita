"""codeflow DSL ``while`` 语句：编译形态 + 端到端语义。

钉死的引擎语义（2026-09-30 探针实证）：
- 循环体编译为子流程，必须以 return 结束——return 值即下一轮 item（首轮 None），
  也是循环结束后 While 节点的输出；子流程状态与父隔离。
- 条件可用 ``item``（$LOOP-ITEM）/``rounds``（$LOOP-INDEX）；体内同名走
  $INPUT.item/$INPUT.index。
- 条件组求值不短路，但点路径打 None 优雅返回 None、None 参与比较不炸
  （引擎吞 TypeError 记 False），故 ``item == None or item.left > 0`` 首轮安全。
"""
from __future__ import annotations

import ast
import unittest

from plaita.dsl.codeflow import WHILE, flow
from plaita.dsl.codeflow._common import _CodeflowError, _CompileCtx
from plaita.dsl.codeflow._stmt import _compile_block


def _stmts(code: str, lineno: int = 7) -> list[ast.stmt]:
    tree = ast.parse(code.strip())
    for i, stmt in enumerate(tree.body):
        ln = lineno + i
        for node in ast.walk(stmt):
            if isinstance(node, ast.AST):
                node.lineno = ln  # type: ignore[attr-defined]
    return tree.body


class TestWhileCompileShape(unittest.TestCase):
    def test_while_spec_shape(self):
        ctx = _CompileCtx()
        entry = _compile_block(_stmts(
            "while item == None or item.left > 0:\n"
            "    return {\"left\": 1}\n"
            "return NODE.w\n"
        ), ctx, None)
        spec = next(n for n in ctx.nodes if n["type"] == "while")
        assert entry == spec["id"]
        # 条件编译为 or 组：eq($LOOP-INDEX,0) / gt($LOOP-ITEM.left,0)
        cond = spec["condition"]
        assert cond["relation"] == "or"
        ops = [(c["field"], c["operator"], c["value"]) for c in cond["conditions"]]
        assert ("$LOOP-ITEM", "eq", None) in ops
        assert ("$LOOP-ITEM.left", "gt", 0) in ops
        # 子流程：start 入口 + return end；体引用 item 走 $INPUT.item
        cf = spec["child_flow"]
        assert cf["inputType"] == {"dataType": "object"}
        assert cf["nodes"][0]["type"] == "start"
        end = cf["nodes"][-1]
        assert end["type"] == "end"
        assert end["output"] == {"left": 1}
        assert spec.get("source_line") == 7

    def test_while_wires_next(self):
        ctx = _CompileCtx()
        _compile_block(_stmts(
            "while rounds < 3:\n"
            "    return rounds\n"
            "x = 1\n"
            "return NODE.w\n"
        ), ctx, None)
        w = next(n for n in ctx.nodes if n["type"] == "while")
        a = next(n for n in ctx.nodes if n["id"] == "x")
        assert w["next"] == "x"
        assert a["type"] == "assignment"

    def test_while_body_refs(self):
        ctx = _CompileCtx()
        _compile_block(_stmts(
            "base = INPUT.n\n"
            "while rounds < 3:\n"
            "    n = F.add(item, rounds)\n"
            "    m = F.add(base, item)\n"
            "    return n\n"
            "return NODE.w\n"
        ), ctx, None)
        w = next(n for n in ctx.nodes if n["type"] == "while")
        body = {n["id"]: n for n in w["child_flow"]["nodes"]}
        # 体内 item/rounds 映射到子流程输入；外层赋值名自动走 $PARENT 快照
        assert body["n"]["output"] == "$F.add($INPUT.item, $INPUT.index)"
        assert body["m"]["output"] == "$F.add($PARENT.NODE.base, $INPUT.item)"

    def test_while_empty_body_rejected(self):
        with self.assertRaises(_CodeflowError) as cm:
            _compile_block(_stmts("while item != None:\n    pass\n"), _CompileCtx(), None)
        assert "return" in str(cm.exception)

    def test_while_else_rejected(self):
        code = "while item != None:\n    return item\nelse:\n    return 0\n"
        with self.assertRaises(_CodeflowError) as cm:
            _compile_block(_stmts(code), _CompileCtx(), None)
        assert "while-else" in str(cm.exception)

    def test_while_after_dangling_rejected(self):
        code = "while item != None:\n    return item\n"
        with self.assertRaises(_CodeflowError) as cm:
            _compile_block(_stmts(code), _CompileCtx(), None)
        assert "悬空" in str(cm.exception)


# ---------------------------------------------------------------------------
# for-head WHILE：节点可命名（id=），循环最终态经 NODE.<id> 引用
# ---------------------------------------------------------------------------

@flow("while_poll_state")
def while_poll_state(INPUT):
    base = INPUT.base
    for st in WHILE(rounds == 0 or item.go != False, id="wps"):
        n = F.add(base, rounds)
        return {"go": False, "seen": n}
    return NODE.wps


@flow("while_countdown")
def while_countdown(INPUT):
    seed = INPUT.n
    for st in WHILE(item == None or item.left > 0, id="wcd", max_iterations=50):
        # F.ifelse 急切求值（两支都会算）：None 不得作为算术操作数，
        # 被减数/减数都用 ifelse 兜底
        n = F.sub(F.ifelse(item == None, seed, item.left), F.ifelse(item == None, 0, 1))
        return {"left": n}
    return NODE.wcd


@flow("while_stmt_runs")
def while_stmt_runs(INPUT):
    while rounds < 2:
        return rounds
    return "done"


class TestWhileEndToEnd(unittest.TestCase):
    def test_poll_state_threading(self):
        # 首轮强制进入（rounds==0），体 return 的 dict 穿线程为下一轮 item，
        # 循环结束输出 = 最后一轮 return 值，经 NODE.<id> 可达
        self.assertEqual(while_poll_state.run(base=40), {"go": False, "seen": 40})

    def test_countdown(self):
        self.assertEqual(while_countdown.run(n=3), {"left": 0})
        self.assertEqual(while_countdown.run(n=0), {"left": 0})

    def test_statement_form_runs(self):
        # 语句形态（自动 id，输出不可命名引用）执行语义一致
        self.assertEqual(while_stmt_runs.run(), "done")


class TestWhileForHeadCompile(unittest.TestCase):
    def test_for_head_while_spec(self):
        ctx = _CompileCtx()
        _compile_block(_stmts(
            "base = INPUT.n\n"
            "for st in WHILE(item.go != False, id=\"w\", max_iterations=7):\n"
            "    return item\n"
            "return NODE.w\n"
        ), ctx, None)
        w = next(n for n in ctx.nodes if n["type"] == "while")
        self.assertEqual(w["id"], "w")
        self.assertEqual(w["max_iterations"], 7)
        self.assertEqual(w["condition"]["field"], "$LOOP-ITEM.go")
        self.assertEqual(w["condition"]["operator"], "ne")
        # 体内 for 目标 st 映射 $INPUT.item；外层 base 映射 $PARENT.NODE.base
        body = {n["id"]: n for n in w["child_flow"]["nodes"]}
        end = body[[k for k in body if body[k]["type"] == "end"][0]]
        self.assertEqual(end["output"], "$INPUT.item")
        # next 接到 while 之后的节点
        self.assertIsNotNone(w["next"])


if __name__ == "__main__":
    unittest.main()
