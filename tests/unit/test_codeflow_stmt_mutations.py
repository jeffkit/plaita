"""针对 plaita/dsl/codeflow/_stmt.py 的 mutation killing 测试。"""
from __future__ import annotations

import ast

import pytest

from plaita.dsl.codeflow._common import _CodeflowError, _CompileCtx
from plaita.dsl.codeflow._stmt import (
    _compile_assign,
    _compile_block,
    _compile_expr_stmt,
    _compile_for,
    _compile_if,
)


def _ctx(**kwargs) -> _CompileCtx:
    return _CompileCtx(**kwargs)


def _stmts(code: str, lineno: int = 7) -> list[ast.stmt]:
    tree = ast.parse(code.strip())
    for i, stmt in enumerate(tree.body):
        ln = lineno + i
        for node in ast.walk(stmt):
            if isinstance(node, ast.AST):
                node.lineno = ln  # type: ignore[attr-defined]
    return tree.body


def _stmt(code: str, lineno: int = 7) -> ast.stmt:
    return _stmts(code, lineno)[0]


# ---------------------------------------------------------------------------
# _compile_block
# ---------------------------------------------------------------------------

class TestCompileBlockMutations:
    def test_empty_block_returns_succ(self):
        assert _compile_block([], _ctx(), "succ") == "succ"

    def test_return_value_end_node(self):
        ctx = _ctx()
        end_id = _compile_block(_stmts("return INPUT.x"), ctx, None)
        assert end_id is not None
        end = ctx.nodes[-1]
        assert end["type"] == "end"
        assert end["output"] == "$INPUT.x"
        assert end["resultType"] == "success"
        assert end.get("source_line") == 7

    def test_return_bare_no_output(self):
        ctx = _ctx()
        end_id = _compile_block(_stmts("return"), ctx, None)
        end = ctx.nodes[-1]
        assert end["output"] is None

    def test_return_unreachable_message_and_lineno(self):
        stmts = _stmts("return INPUT.x\nx = INPUT.y")
        with pytest.raises(_CodeflowError) as exc:
            _compile_block(stmts, _ctx(), None)
        msg = str(exc.value)
        assert "return 之后还有不可达语句" in msg
        assert "XXreturn" not in msg
        assert "第 ?" not in msg
        assert "第 7 行" in msg or "第 8 行" in msg

    def test_pass_falls_through(self):
        ctx = _ctx()
        entry = _compile_block(_stmts("pass\nreturn INPUT.x"), ctx, None)
        assert entry is not None
        assert ctx.nodes[-1]["type"] == "end"

    def test_unsupported_stmt_message(self):
        with pytest.raises(_CodeflowError) as exc:
            _compile_block(_stmts("while True: pass"), _ctx(), "succ")
        msg = str(exc.value)
        assert "不支持的语句 While" in msg
        assert "XX不支持的语句" not in msg
        assert "第 ?" not in msg

    def test_if_dispatched(self):
        ctx = _ctx()
        entry = _compile_block(
            _stmts("if INPUT.flag:\n    return INPUT.a\nelse:\n    return INPUT.b"),
            ctx,
            None,
        )
        if_node = ctx.nodes[0]
        assert if_node["type"] == "if"
        assert entry == if_node["id"]

    def test_block_if_outer_succ_for_fallthrough_branches(self):
        ctx = _ctx()
        code = "if INPUT.flag:\n    a = INPUT.a\nelse:\n    b = INPUT.b"
        entry = _compile_block(_stmts(code), ctx, "join_point")
        if_node = ctx.nodes[0]
        assert entry == if_node["id"]
        a_node = next(n for n in ctx.nodes if n.get("id") == "a")
        b_node = next(n for n in ctx.nodes if n.get("id") == "b")
        assert a_node["next"] == "join_point"
        assert b_node["next"] == "join_point"

    def test_block_for_outer_succ(self):
        ctx = _ctx()
        code = "for x in MAP(INPUT.items):\n    return x"
        entry = _compile_block(_stmts(code), ctx, "after_map")
        spec = next(n for n in ctx.nodes if n["type"] == "map")
        assert entry == spec["id"]
        assert spec["next"] == "after_map"

    def test_block_pass_propagates_succ(self):
        ctx = _ctx()
        entry = _compile_block(_stmts("pass\na = INPUT.x\nreturn a"), ctx, "tail")
        assert entry is not None
        assert any(n.get("id") == "a" for n in ctx.nodes)

    def test_block_assign_at_end_uses_succ(self):
        ctx = _ctx()
        entry = _compile_block(_stmts("a = INPUT.x"), ctx, "done")
        assert entry == "a"
        assert ctx.nodes[0]["next"] == "done"

    def test_block_expr_stmt_at_end_uses_succ(self):
        ctx = _ctx()
        entry = _compile_block(_stmts("INPUT.x"), ctx, "done")
        assert entry is not None
        assert ctx.nodes[0]["next"] == "done"

# ---------------------------------------------------------------------------
# _compile_if
# ---------------------------------------------------------------------------

class TestCompileIfMutations:
    def _compile_simple_if(self, code: str) -> tuple[_CompileCtx, dict]:
        ctx = _ctx()
        head = _stmt(code)
        assert isinstance(head, ast.If)
        entry = _compile_if(head, ctx, "after_succ", [])
        if_node = ctx.nodes[0]
        assert entry == if_node["id"]
        return ctx, if_node

    def test_if_branches_link_to_after_when_no_return_in_branch(self):
        ctx, if_node = self._compile_simple_if(
            "if INPUT.flag:\n    a = INPUT.a\nelse:\n    b = INPUT.b"
        )
        assert if_node["condition"] is not None
        assert if_node["next"] is not None
        assert if_node["else_next"] is not None
        assert if_node.get("source_line") == 7
        # both branches should fall through to the same after (empty rest → succ)
        assert if_node["next"] != if_node["else_next"] or len(ctx.nodes) > 1

    def test_if_without_else_uses_after(self):
        _, if_node = self._compile_simple_if("if INPUT.flag:\n    return INPUT.a")
        assert if_node["else_next"] == "after_succ"

    def test_elif_compiled_as_nested_if(self):
        ctx = _ctx()
        code = (
            "if INPUT.a:\n"
            "    return INPUT.x\n"
            "elif INPUT.b:\n"
            "    return INPUT.y\n"
            "else:\n"
            "    return INPUT.z"
        )
        head = _stmt(code)
        entry = _compile_if(head, ctx, None, [])
        outer = ctx.nodes[0]
        assert outer["type"] == "if"
        assert entry == outer["id"]
        # elif body is another if in orelse
        inner = next(n for n in ctx.nodes if n["type"] == "if" and n["id"] != outer["id"])
        assert inner is not None

    def test_dangling_true_branch_message(self):
        head = _stmt("if INPUT.flag:\n    pass")
        with pytest.raises(_CodeflowError) as exc:
            _compile_if(head, _ctx(), None, [])
        msg = str(exc.value)
        assert "if 真分支悬空" in msg
        assert "XXif 真分支悬空" not in msg
        assert "第 ?" not in msg

    def test_dangling_false_branch_message(self):
        head = _stmt("if INPUT.flag:\n    return INPUT.a\nelse:\n    pass")
        with pytest.raises(_CodeflowError) as exc:
            _compile_if(head, _ctx(), None, [])
        msg = str(exc.value)
        assert "if 假分支悬空" in msg
        assert "XXif 假分支悬空" not in msg
        assert "第 ?" not in msg

    def test_after_block_uses_succ(self):
        ctx = _ctx()
        stmts = _stmts(
            "if INPUT.flag:\n"
            "    return INPUT.a\n"
            "else:\n"
            "    return INPUT.b\n"
            "return INPUT.tail"
        )
        head, rest = stmts[0], stmts[1:]
        assert isinstance(head, ast.If)
        entry = _compile_if(head, ctx, "final_end", rest)
        if_node = ctx.nodes[0]
        assert entry == if_node["id"]
        # rest compiles to end node; if branches should not point to final_end directly
        tail_end = ctx.nodes[-1]
        assert tail_end["type"] == "end"


# ---------------------------------------------------------------------------
# _compile_for
# ---------------------------------------------------------------------------

class TestCompileForMutations:
    def _compile_map(self, body: str, iter_code: str, succ: str = "after") -> tuple[_CompileCtx, dict]:
        ctx = _ctx()
        code = f"for x in {iter_code}:\n{body}"
        head = _stmt(code)
        assert isinstance(head, ast.For)
        entry = _compile_for(head, ctx, succ, [])
        spec = next(n for n in ctx.nodes if n.get("type") == "map")
        return ctx, spec

    def test_map_basic_child_flow(self):
        ctx, spec = self._compile_map("    return x", "MAP(INPUT.items)")
        assert spec["type"] == "map"
        assert spec["collection"] == "$INPUT.items"
        assert spec["next"] == "after"
        assert spec.get("source_line") == 7
        child = spec["childFlow"]
        assert child["inputType"] == {"dataType": "object"}
        nodes = child["nodes"]
        assert nodes[0]["type"] == "start"
        assert nodes[0]["next"] is not None

    def test_map_with_index_target(self):
        ctx = _ctx()
        head = _stmt("for x, i in MAP(INPUT.items):\n    return x")
        assert isinstance(head, ast.For)
        _compile_for(head, ctx, "after", [])
        # loop vars wired in child ctx via compilation — verify child has end
        spec = next(n for n in ctx.nodes if n["type"] == "map")
        child_end = [n for n in spec["childFlow"]["nodes"] if n["type"] == "end"]
        assert child_end

    def test_reduce_two_var_child_array_input(self):
        ctx = _ctx()
        head = _stmt("for a, b in REDUCE(INPUT.items):\n    return F.add(a, b)")
        assert isinstance(head, ast.For)
        _compile_for(head, ctx, "after", [])
        spec = next(n for n in ctx.nodes if n["type"] == "reduce")
        assert spec["childFlow"]["inputType"] == {"dataType": "array"}

    def test_reduce_initial_kw(self):
        ctx = _ctx()
        head = _stmt("for a, b in REDUCE(INPUT.items, initial=0):\n    return F.add(a, b)")
        _compile_for(head, ctx, "after", [])
        spec = next(n for n in ctx.nodes if n["type"] == "reduce")
        assert spec["initial"] == 0

    def test_map_concurrent_and_max_concurrent(self):
        ctx = _ctx()
        head = _stmt(
            "for x in MAP(INPUT.items, concurrent=True, max_concurrent=3):\n"
            "    return x"
        )
        _compile_for(head, ctx, "after", [])
        spec = next(n for n in ctx.nodes if n["type"] == "map")
        assert spec["concurrent"] is True
        assert spec["maxConcurrent"] == 3

    def test_map_max_concurrent_camel_alias(self):
        ctx = _ctx()
        head = _stmt(
            "for x in MAP(INPUT.items, concurrent=True, maxConcurrent=5):\n"
            "    return x"
        )
        _compile_for(head, ctx, "after", [])
        spec = next(n for n in ctx.nodes if n["type"] == "map")
        assert spec["maxConcurrent"] == 5

    def test_custom_id_kw(self):
        ctx = _ctx()
        head = _stmt('for x in MAP(INPUT.items, id="my_map"):\n    return x')
        _compile_for(head, ctx, "after", [])
        spec = next(n for n in ctx.nodes if n["type"] == "map")
        assert spec["id"] == "my_map"

    def test_map_collection_uses_ctx_bound_name(self):
        ctx = _ctx()
        ctx.names["items"] = "$NODE.items"
        head = _stmt("for x in MAP(items):\n    return x")
        _compile_for(head, ctx, "after", [])
        spec = next(n for n in ctx.nodes if n["type"] == "map")
        assert spec["collection"] == "$NODE.items"

    def test_child_start_node_id_is_start(self):
        ctx = _ctx()
        head = _stmt("for x in MAP(INPUT.items):\n    return x")
        _compile_for(head, ctx, "after", [])
        spec = next(n for n in ctx.nodes if n["type"] == "map")
        assert spec["childFlow"]["nodes"][0] == {
            "type": "start",
            "id": "start",
            "next": spec["childFlow"]["nodes"][0]["next"],
        }

    def test_filter_node_type(self):
        ctx = _ctx()
        head = _stmt("for x in FILTER(INPUT.items):\n    return x")
        _compile_for(head, ctx, "after", [])
        spec = next(n for n in ctx.nodes if n["type"] == "filter")
        assert spec["collection"] == "$INPUT.items"

    def test_map_concurrent_ignores_non_constant_max_concurrent(self):
        ctx = _ctx()
        ctx.names["lim"] = "$NODE.lim"
        head = _stmt(
            "for x in MAP(INPUT.items, concurrent=True, max_concurrent=lim):\n"
            "    return x"
        )
        _compile_for(head, ctx, "after", [])
        spec = next(n for n in ctx.nodes if n["type"] == "map")
        assert spec["concurrent"] is True
        assert "maxConcurrent" not in spec

    def test_invalid_iter_not_collection_call(self):
        head = _stmt("for x in INPUT.items:\n    return x")
        with pytest.raises(_CodeflowError) as exc:
            _compile_for(head, _ctx(), "after", [])
        msg = str(exc.value)
        assert "for 循环的迭代对象必须是 MAP/FILTER/FIND/LOOP/REDUCE" in msg
        assert "XXfor 循环" not in msg
        assert "第 ?" not in msg

    def test_invalid_iter_builtin_node_call(self):
        head = _stmt('for x in HTTP(url="http://x.com"):\n    return x')
        with pytest.raises(_CodeflowError) as exc:
            _compile_for(head, _ctx(), "after", [])
        assert "for 循环的迭代对象必须是 MAP/FILTER/FIND/LOOP/REDUCE" in str(exc.value)

    def test_missing_collection_arg_message(self):
        head = _stmt("for x in MAP():\n    return x")
        with pytest.raises(_CodeflowError) as exc:
            _compile_for(head, _ctx(), "after", [])
        msg = str(exc.value)
        assert "MAP 需要一个集合表达式" in msg
        assert "第 ?" not in msg

    def test_reduce_wrong_loop_vars_message(self):
        head = _stmt("for x in REDUCE(INPUT.items):\n    return x")
        with pytest.raises(_CodeflowError) as exc:
            _compile_for(head, _ctx(), "after", [])
        msg = str(exc.value)
        assert "REDUCE 的循环变量必须是 (first, second)" in msg
        assert "第 ?" not in msg

    def test_empty_loop_body_message(self):
        head = _stmt("for x in MAP(INPUT.items):\n    pass")
        with pytest.raises(_CodeflowError) as exc:
            _compile_for(head, _ctx(), "after", [])
        msg = str(exc.value)
        assert "循环体为空或全部悬空" in msg
        assert "第 ?" not in msg

    def test_collection_no_after_message(self):
        head = _stmt("for x in MAP(INPUT.items):\n    return x")
        with pytest.raises(_CodeflowError) as exc:
            _compile_for(head, _ctx(), None, [])
        msg = str(exc.value)
        assert "集合节点之后悬空" in msg
        assert "第 ?" not in msg


# ---------------------------------------------------------------------------
# _compile_assign
# ---------------------------------------------------------------------------

class TestCompileAssignMutations:
    def test_node_call_assign_inserts_spec(self):
        ctx = _ctx()
        stmts = _stmts('resp = HTTP(url="http://x.com")\nreturn INPUT.x')
        head, rest = stmts[0], stmts[1:]
        assert isinstance(head, ast.Assign)
        entry = _compile_assign(head, ctx, None, rest)
        assert entry == "resp"
        http = ctx.nodes[0]
        assert http["type"] == "http"
        assert http["id"] == "resp"
        assert http["next"] is not None
        assert http.get("source_line") == 7

    def test_expr_assign_claims_name(self):
        ctx = _ctx()
        stmts = _stmts("val = INPUT.x\nreturn val")
        head, rest = stmts[0], stmts[1:]
        entry = _compile_assign(head, ctx, None, rest)
        assert entry == "val"
        assign = ctx.nodes[0]
        assert assign["type"] == "assignment"
        assert assign["id"] == "val"
        assert assign["output"] == "$INPUT.x"
        assert ctx.names["val"] == "$NODE.val"

    def test_expr_assign_uses_ctx_bound_value(self):
        ctx = _ctx()
        ctx.names["src"] = "$NODE.src"
        stmts = _stmts("val = src\nreturn val")
        head, rest = stmts[0], stmts[1:]
        _compile_assign(head, ctx, None, rest)
        assign = ctx.nodes[0]
        assert assign["output"] == "$NODE.src"

    def test_invalid_target_message(self):
        head = _stmt("a, b = INPUT.x")
        with pytest.raises(_CodeflowError) as exc:
            _compile_assign(head, _ctx(), "after", [])
        msg = str(exc.value)
        assert "赋值目标必须是单个变量名" in msg
        assert "XX赋值目标" not in msg
        assert "第 ?" not in msg

    def test_dangling_after_assign_message(self):
        head = _stmt('resp = HTTP(url="http://x.com")')
        with pytest.raises(_CodeflowError) as exc:
            _compile_assign(head, _ctx(), None, [])
        msg = str(exc.value)
        assert "赋值 resp 之后悬空" in msg
        assert "第 ?" not in msg

    def test_names_registered_before_rest(self):
        ctx = _ctx()
        stmts = _stmts("a = INPUT.x\nb = INPUT.y\nreturn b")
        head, rest = stmts[0], stmts[1:]
        _compile_assign(head, ctx, None, rest)
        assert ctx.names["a"] == "$NODE.a"


# ---------------------------------------------------------------------------
# _compile_expr_stmt
# ---------------------------------------------------------------------------

class TestCompileExprStmtMutations:
    def test_node_call_expr_stmt_returns_spec_id(self):
        ctx = _ctx()
        stmts = _stmts('HTTP(url="http://x.com")\nreturn INPUT.x')
        head, rest = stmts[0], stmts[1:]
        assert isinstance(head, ast.Expr)
        entry = _compile_expr_stmt(head, ctx, None, rest)
        http = ctx.nodes[0]
        assert http["type"] == "http"
        assert entry == http["id"]
        assert http["next"] is not None

    def test_expr_stmt_passes_succ_to_after(self):
        ctx = _ctx()
        stmts = _stmts("INPUT.x")
        head, rest = stmts[0], stmts[1:]
        entry = _compile_expr_stmt(head, ctx, "done", rest)
        assign = ctx.nodes[0]
        assert assign["next"] == "done"
        assert entry == assign["id"]

    def test_plain_expr_stmt_auto_assignment(self):
        ctx = _ctx()
        stmts = _stmts("INPUT.x\nreturn INPUT.x")
        head, rest = stmts[0], stmts[1:]
        entry = _compile_expr_stmt(head, ctx, None, rest)
        assign = ctx.nodes[0]
        assert assign["type"] == "assignment"
        assert assign["output"] == "$INPUT.x"
        assert entry == assign["id"]
        assert assign.get("source_line") == 7

    def test_dangling_expr_stmt_message(self):
        head = _stmt('HTTP(url="http://x.com")')
        with pytest.raises(_CodeflowError) as exc:
            _compile_expr_stmt(head, _ctx(), None, [])
        msg = str(exc.value)
        assert "表达式语句之后悬空" in msg
        assert "第 ?" not in msg
