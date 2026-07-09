"""Coverage4: targets remaining missing lines in plaita/dsl/codeflow/.

Covers:
_nodes.py : 28-30, 56, 76, 85, 90, 99, 116, 146, 149, 153, 156, 159, 172, 174,
            206, 214, 220, 223, 226, 241
_expr.py  : 48, 53, 59, 61, 72, 94, 176, 189, 196
_stmt.py  : 84, 88, 93, 95, 115, 127, 140, 180, 191, 201, 230
_source.py: 29, 63, 100, 195, 202-203, 218
_common.py: 144, 155, 277, 280
"""
from __future__ import annotations

import ast
import unittest
from typing import Any, ClassVar, Optional

from plaita import Node
from plaita.node import NodeRegistry
from plaita.dsl.codeflow import (
    ErrorHandler,
    compile_func,
    compile_source,
    flow,
    childflow,
    CHILD,
    CODE,
    EVENT,
    F,
    HTTP,
    INPUT,
    MAP,
    NODE,
    PARALLEL,
    REDUCE,
)
from plaita.dsl.codeflow._common import _CodeflowError, _CompileCtx  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# 测试辅助：自定义节点注册
# ---------------------------------------------------------------------------

class _CoverNode(Node):
    node_type: ClassVar[str] = "codeflow_cover4_node"
    text: Optional[str] = None

    def execute(self, execution):
        return execution.evaluate(self.text) if self.text else ""


def _make_reg() -> NodeRegistry:
    reg = NodeRegistry()
    reg.register(_CoverNode)
    return reg


def _compile_with_reg(src: str, reg: NodeRegistry) -> dict:
    """使用指定 registry 的已知类型集编译源码。"""
    from plaita.dsl import codeflow as cf
    import textwrap as _tw

    known = set(reg.list_types())
    mod = ast.parse(_tw.dedent(src))
    chosen = None
    for stmt in mod.body:
        if isinstance(stmt, ast.FunctionDef):
            chosen = stmt
            break
    assert chosen is not None
    return cf._compile_fdef(chosen, "test", {}, known_node_types=known)  # type: ignore


# ---------------------------------------------------------------------------
# _common.py : line 144 — _Placeholder.__repr__
# ---------------------------------------------------------------------------

class TestPlaceholderRepr(unittest.TestCase):
    def test_placeholder_repr(self):
        """Line 144 in _common.py: __repr__ of _Placeholder."""
        from plaita.dsl.codeflow import INPUT  # _Placeholder
        r = repr(INPUT)
        self.assertIn("INPUT", r)
        self.assertIn("placeholder", r)


# ---------------------------------------------------------------------------
# _common.py : line 155 — _raise_if_unregistered_custom silent return
# _nodes.py  : lines 28-30 — builtin-handled uppercase name as node call
# ---------------------------------------------------------------------------

class TestBuiltinHandledUppercaseNodeCall(unittest.TestCase):
    """START(...) / END(...) — uppercase but in _BUILTIN_HANDLED_TYPES.

    _raise_if_unregistered_custom returns silently (line 155), then
    _compile_node_call raises "不支持的节点调用" (line 30).
    """

    def _compile_builtin_as_node(self, call_src: str) -> None:
        from plaita.dsl.codeflow._nodes import _compile_node_call

        ctx = _CompileCtx(known_node_types=set())
        mod = ast.parse(call_src)
        call_node: ast.Call = mod.body[0].value  # type: ignore[assignment]
        with self.assertRaises(_CodeflowError):
            _compile_node_call(call_node, ctx, None)

    def test_start_as_node_call_raises(self):
        """Lines 28-30 + _common.py 155: START(...) as node call."""
        self._compile_builtin_as_node("START(INPUT)")

    def test_end_as_node_call_raises(self):
        """Same path: END(...) as node call."""
        self._compile_builtin_as_node("END(INPUT)")

    def test_if_as_node_call_raises(self):
        """Same path: IF(...) as node call."""
        self._compile_builtin_as_node("IF(INPUT)")


# ---------------------------------------------------------------------------
# _common.py : line 277 — _unpack_names nested non-Name element
# _common.py : line 280 — _unpack_names non-Name/Tuple/List target
# ---------------------------------------------------------------------------

class TestUnpackNamesErrors(unittest.TestCase):
    def test_nested_tuple_in_loop_var_raises(self):
        """Line 277: for (x, (a, b)) in MAP(...) → nested non-Name → raise."""
        with self.assertRaises(Exception) as ctx:
            compile_source("""
from plaita.dsl.codeflow import flow, MAP, F, INPUT, NODE
@flow("t")
def t(INPUT):
    for x, (a, b) in MAP(INPUT.items):
        return x
    return NODE.t
""")
        self.assertIn("循环变量解包必须是纯名字", str(ctx.exception))

    def test_attribute_loop_target_raises(self):
        """Line 280: for x.attr in MAP(...) → not Name/Tuple/List → raise."""
        with self.assertRaises(Exception) as ctx:
            compile_source("""
from plaita.dsl.codeflow import flow, MAP, INPUT, NODE
@flow("t")
def t(INPUT):
    for INPUT.item in MAP(INPUT.items):
        return INPUT.item
    return NODE.t
""")
        self.assertIn("不支持的循环变量形式", str(ctx.exception))


# ---------------------------------------------------------------------------
# _nodes.py : line 56 — HTTP with headers=
# ---------------------------------------------------------------------------

class TestHttpHeaders(unittest.TestCase):
    def test_http_with_headers(self):
        """Line 56: HTTP(url=..., headers=...) → spec includes headers."""
        def _func(INPUT):
            resp = HTTP(url=INPUT.url, headers={"X-Token": "abc"})
            return resp

        ir = compile_func(_func, "test_http_headers")
        http = next(n for n in ir["nodes"] if n.get("type") == "http")
        self.assertIn("headers", http)
        self.assertEqual(http["headers"]["X-Token"], "abc")


# ---------------------------------------------------------------------------
# _nodes.py : line 76 — CODE without code/lang
# ---------------------------------------------------------------------------

class TestCodeNodeMissingArgs(unittest.TestCase):
    def test_code_no_args_raises(self):
        """Line 76: CODE() without code= and lang= raises."""
        with self.assertRaises(Exception) as ctx:
            def _func(INPUT):
                r = CODE()
                return r
            compile_func(_func, "test_code_no_args")
        self.assertIn("CODE 需要 code 和 lang", str(ctx.exception))

    def test_code_with_lang_only_raises(self):
        """Line 76: CODE(lang='python') without code= raises."""
        with self.assertRaises(Exception) as ctx:
            def _func(INPUT):
                r = CODE(lang="python")
                return r
            compile_func(_func, "test_code_no_code")
        self.assertIn("CODE 需要 code 和 lang", str(ctx.exception))


# ---------------------------------------------------------------------------
# _nodes.py : line 85 — EVENT with non-constant positional arg
# _nodes.py : line 90 — EVENT with filter=
# ---------------------------------------------------------------------------

class TestEventNode(unittest.TestCase):
    def test_event_positional_expression_arg(self):
        """Line 85: EVENT(INPUT.event_type) → etype from _compile_expr."""
        def _func(INPUT):
            ev = EVENT(INPUT.event_type)
            return ev

        ir = compile_func(_func, "test_event_expr_pos")
        ev = next(n for n in ir["nodes"] if n.get("type") == "event")
        self.assertEqual(ev["eventType"], "$INPUT.event_type")

    def test_event_with_filter(self):
        """Line 90: EVENT("user.login", filter=INPUT.cond) → spec includes eventFilter."""
        def _func(INPUT):
            ev = EVENT("user.login", filter=INPUT.cond)
            return ev

        ir = compile_func(_func, "test_event_filter")
        ev = next(n for n in ir["nodes"] if n.get("type") == "event")
        self.assertIn("eventFilter", ev)
        self.assertEqual(ev["eventFilter"], "$INPUT.cond")


# ---------------------------------------------------------------------------
# _nodes.py : line 99 — CHILD/REFERENCE without flow=
# ---------------------------------------------------------------------------

class TestChildWithoutFlow(unittest.TestCase):
    def test_child_missing_flow_raises(self):
        """Line 99: CHILD(INPUT.x) without flow= raises."""
        with self.assertRaises(Exception) as ctx:
            def _func(INPUT):
                r = CHILD(INPUT.x)
                return r
            compile_func(_func, "test_child_no_flow")
        self.assertIn("CHILD 需要 flow=", str(ctx.exception))


# ---------------------------------------------------------------------------
# _nodes.py : line 116 — PARALLEL with conditional=True
# ---------------------------------------------------------------------------

_PARALLEL_COND_SRC = """
from plaita.dsl.codeflow import flow, childflow, PARALLEL, INPUT

@childflow
def cf_cond(INPUT):
    return INPUT.x

@flow("test_parallel_conditional")
def test_parallel_conditional(INPUT):
    par = PARALLEL(branches={"a": cf_cond}, conditional=True)
    return par
"""


class TestParallelConditional(unittest.TestCase):
    def test_parallel_conditional_true(self):
        """Line 116: PARALLEL(..., conditional=True) → isConditional=True in IR."""
        ir = compile_source(_PARALLEL_COND_SRC)
        par = next(n for n in ir["nodes"] if n.get("type") == "parallel")
        self.assertTrue(par.get("isConditional"))


# ---------------------------------------------------------------------------
# _nodes.py : line 146 — custom node id= non-string constant
# _nodes.py : line 149 — custom node auto_id (no assign, no id=)
# _nodes.py : line 153 — custom node timeout=
# _nodes.py : line 156 — custom node on_error=
# _nodes.py : line 159 — custom node **kwargs unpack
# ---------------------------------------------------------------------------

class TestCustomNodeMissingLines(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.reg = _make_reg()

    def test_custom_node_id_not_string_raises(self):
        """Line 146: custom node id= must be a string constant; int raises."""
        with self.assertRaises(_CodeflowError) as ctx:
            _compile_with_reg("""
@flow("t")
def t(INPUT):
    CODEFLOW_COVER4_NODE(id=42)
    return "done"
""", self.reg)
        self.assertIn("id= 必须是字符串常量", str(ctx.exception))

    def test_custom_node_auto_id_when_no_assign_no_id(self):
        """Line 149: expression-stmt custom node without id= uses auto_id."""
        ir = _compile_with_reg("""
@flow("t")
def t(INPUT):
    CODEFLOW_COVER4_NODE(text="hi")
    return "done"
""", self.reg)
        node = next(n for n in ir["nodes"] if n.get("type") == "codeflow_cover4_node")
        # id should be auto-generated (e.g., _n1)
        self.assertIsNotNone(node.get("id"))
        self.assertTrue(str(node["id"]).startswith("_n"))

    def test_custom_node_with_timeout(self):
        """Line 153: custom node with timeout= sets timeout field."""
        ir = _compile_with_reg("""
@flow("t")
def t(INPUT):
    a = CODEFLOW_COVER4_NODE(text=INPUT.msg, timeout=30)
    return a
""", self.reg)
        node = next(n for n in ir["nodes"] if n.get("type") == "codeflow_cover4_node")
        self.assertEqual(node.get("timeout"), 30)

    def test_custom_node_with_on_error(self):
        """Line 156: custom node with on_error= sets errorHandler field."""
        ir = _compile_with_reg("""
@flow("t")
def t(INPUT):
    a = CODEFLOW_COVER4_NODE(text=INPUT.msg, on_error=ErrorHandler("abort"))
    return a
""", self.reg)
        node = next(n for n in ir["nodes"] if n.get("type") == "codeflow_cover4_node")
        self.assertIn("errorHandler", node)
        self.assertEqual(node["errorHandler"]["strategy"], "abort")

    def test_custom_node_kwargs_unpack_raises(self):
        """Line 159: custom node with **kwargs raises."""
        with self.assertRaises(_CodeflowError) as ctx:
            _compile_with_reg("""
@flow("t")
def t(INPUT):
    a = CODEFLOW_COVER4_NODE(**INPUT.opts)
    return a
""", self.reg)
        self.assertIn("不支持 **kwargs 解包", str(ctx.exception))


# ---------------------------------------------------------------------------
# _nodes.py : line 172 — ErrorHandler with retry_times
# _nodes.py : line 174 — ErrorHandler with invalid strategy
# ---------------------------------------------------------------------------

class TestErrorHandlerMissingLines(unittest.TestCase):
    def test_error_handler_retry_times(self):
        """Line 172 (retryTimes) + line 172 strategy=kw path: ErrorHandler with retry_times."""
        def _func(INPUT):
            resp = HTTP(url=INPUT.url, on_error=ErrorHandler("abort", retry_times=3))
            return resp

        ir = compile_func(_func, "test_eh_retry")
        http = next(n for n in ir["nodes"] if n.get("type") == "http")
        self.assertEqual(http["errorHandler"].get("retryTimes"), 3)

    def test_error_handler_strategy_as_keyword_arg(self):
        """Line 172: ErrorHandler(strategy='continue') — strategy via kw → strategy field."""
        def _func(INPUT):
            resp = HTTP(url=INPUT.url, on_error=ErrorHandler(strategy="continue"))
            return resp

        ir = compile_func(_func, "test_eh_strategy_kw")
        http = next(n for n in ir["nodes"] if n.get("type") == "http")
        self.assertEqual(http["errorHandler"].get("strategy"), "continue")

    def test_error_handler_invalid_strategy_raises(self):
        """Line 174: ErrorHandler('bad_strategy') raises _CodeflowError."""
        with self.assertRaises(Exception) as ctx:
            def _func(INPUT):
                resp = HTTP(url=INPUT.url, on_error=ErrorHandler("bad_strategy"))
                return resp
            compile_func(_func, "test_eh_bad_strategy")
        self.assertIn("unknown error handler strategy", str(ctx.exception))


# ---------------------------------------------------------------------------
# _nodes.py : line 206 — childflow arg not ast.Name
# ---------------------------------------------------------------------------

_CHILDFLOW_NOT_NAME_SRC = """
from plaita.dsl.codeflow import flow, childflow, CHILD, INPUT

@childflow
def cf_call(INPUT):
    return INPUT.x

@flow("test_cf_not_name")
def test_cf_not_name(INPUT):
    result = CHILD(INPUT.x, flow=cf_call())
    return result
"""


class TestChildflowArgNotName(unittest.TestCase):
    def test_childflow_call_arg_raises(self):
        """Line 206: CHILD(flow=cf()) — cf() is a Call, not a Name → raise."""
        with self.assertRaises(Exception) as ctx:
            compile_source(_CHILDFLOW_NOT_NAME_SRC)
        self.assertIn("不支持的 childflow 参数", str(ctx.exception))


# ---------------------------------------------------------------------------
# _nodes.py : lines 214, 220, 223, 226 — PARALLEL branch errors
# _nodes.py : line 241 — join list non-string element
# ---------------------------------------------------------------------------

_PARALLEL_DICT_BAD_KEY_SRC = """
from plaita.dsl.codeflow import flow, childflow, PARALLEL, INPUT

@childflow
def cf_dk(INPUT):
    return INPUT.x

@flow("test_par_bad_key")
def test_par_bad_key(INPUT):
    par = PARALLEL(branches={INPUT.x: cf_dk})
    return par
"""

_PARALLEL_LIST_NOT_TUPLE_SRC = """
from plaita.dsl.codeflow import flow, childflow, PARALLEL, INPUT

@childflow
def cf_lt(INPUT):
    return INPUT.x

@flow("test_par_list_not_tuple")
def test_par_list_not_tuple(INPUT):
    par = PARALLEL(branches=["branch_a"])
    return par
"""

_PARALLEL_LIST_TUPLE_BAD_NAME_SRC = """
from plaita.dsl.codeflow import flow, childflow, PARALLEL, INPUT

@childflow
def cf_bn(INPUT):
    return INPUT.x

@flow("test_par_tuple_bad_name")
def test_par_tuple_bad_name(INPUT):
    par = PARALLEL(branches=[(INPUT.name, cf_bn)])
    return par
"""

_PARALLEL_BRANCHES_NOT_DICT_LIST_SRC = """
from plaita.dsl.codeflow import flow, childflow, PARALLEL, INPUT

@childflow
def cf_ndl(INPUT):
    return INPUT.x

@flow("test_par_not_dict_list")
def test_par_not_dict_list(INPUT):
    par = PARALLEL(branches=INPUT.branches)
    return par
"""

_PARALLEL_JOIN_NOT_STRING_SRC = """
from plaita.dsl.codeflow import flow, childflow, PARALLEL, INPUT

@childflow
def cf_jns(INPUT):
    return INPUT.x

@flow("test_par_join_non_str")
def test_par_join_non_str(INPUT):
    par = PARALLEL(branches={"a": cf_jns}, join=[INPUT.x])
    return par
"""


class TestParallelBranchErrors(unittest.TestCase):
    def test_parallel_dict_non_string_key_raises(self):
        """Line 214: PARALLEL(branches={INPUT.x: cf}) — non-string key → raise."""
        with self.assertRaises(Exception) as ctx:
            compile_source(_PARALLEL_DICT_BAD_KEY_SRC)
        self.assertIn("分支名必须是字符串常量", str(ctx.exception))

    def test_parallel_list_non_tuple_raises(self):
        """Line 220: PARALLEL(branches=["str"]) — list element not tuple → raise."""
        with self.assertRaises(Exception) as ctx:
            compile_source(_PARALLEL_LIST_NOT_TUPLE_SRC)
        self.assertIn("分支需要 (name, flow) 元组", str(ctx.exception))

    def test_parallel_list_tuple_non_string_name_raises(self):
        """Line 223: PARALLEL(branches=[(INPUT.name, cf)]) — name not string → raise."""
        with self.assertRaises(Exception) as ctx:
            compile_source(_PARALLEL_LIST_TUPLE_BAD_NAME_SRC)
        self.assertIn("分支名必须是字符串常量", str(ctx.exception))

    def test_parallel_branches_not_dict_or_list_raises(self):
        """Line 226: PARALLEL(branches=INPUT.branches) — not dict/list → raise."""
        with self.assertRaises(Exception) as ctx:
            compile_source(_PARALLEL_BRANCHES_NOT_DICT_LIST_SRC)
        self.assertIn("branches 需要是 dict 字面量或", str(ctx.exception))

    def test_parallel_join_non_string_element_raises(self):
        """Line 241: PARALLEL(join=[INPUT.x]) — non-string element → raise."""
        with self.assertRaises(Exception) as ctx:
            compile_source(_PARALLEL_JOIN_NOT_STRING_SRC)
        self.assertIn("join 列表元素必须是字符串常量", str(ctx.exception))


# ---------------------------------------------------------------------------
# _expr.py : line 48 — unsupported UnaryOp (not USub or Not)
# ---------------------------------------------------------------------------

class TestUnsupportedUnaryOp(unittest.TestCase):
    def test_uadd_op_raises(self):
        """Line 48: +INPUT.x (UAdd) is not USub or Not → raises."""
        with self.assertRaises(Exception) as ctx:
            compile_source("""
from plaita.dsl.codeflow import flow, INPUT
@flow("t")
def t(INPUT):
    return +INPUT.x
""")
        self.assertIn("不支持的一元运算", str(ctx.exception))


# ---------------------------------------------------------------------------
# _expr.py : line 53 — subscript base not str
# ---------------------------------------------------------------------------

class TestSubscriptBaseNotStr(unittest.TestCase):
    def test_list_literal_subscript_raises(self):
        """Line 53: [1, 2, 3][0] — base is list, not str → raises."""
        with self.assertRaises(Exception) as ctx:
            compile_source("""
from plaita.dsl.codeflow import flow, INPUT
@flow("t")
def t(INPUT):
    return [1, 2, 3][0]
""")
        self.assertIn("下标访问的基底必须是命名空间", str(ctx.exception))


# ---------------------------------------------------------------------------
# _expr.py : line 59 — dict ** unpack raises
# _expr.py : line 61 — dict non-constant key raises
# ---------------------------------------------------------------------------

class TestDictErrors(unittest.TestCase):
    def test_dict_double_star_unpack_raises(self):
        """Line 59: {**INPUT.extra} — ** unpack in dict → raises."""
        with self.assertRaises(Exception) as ctx:
            compile_source("""
from plaita.dsl.codeflow import flow, INPUT
@flow("t")
def t(INPUT):
    return {**INPUT.extra}
""")
        self.assertIn("不支持 ** 解包", str(ctx.exception))

    def test_dict_non_constant_key_raises(self):
        """Line 61: {INPUT.key: "v"} — non-constant key → raises."""
        with self.assertRaises(Exception) as ctx:
            compile_source("""
from plaita.dsl.codeflow import flow, INPUT
@flow("t")
def t(INPUT):
    return {INPUT.key: "value"}
""")
        self.assertIn("dict 的 key 必须是常量", str(ctx.exception))


# ---------------------------------------------------------------------------
# _expr.py : line 72 — footgun hints (IfExp / JoinedStr / etc.)
# ---------------------------------------------------------------------------

class TestFootgunHints(unittest.TestCase):
    def test_ternary_expression_raises_with_hint(self):
        """Line 71: ternary `a if c else b` — footgun hint for IfExp."""
        with self.assertRaises(Exception) as ctx:
            compile_source("""
from plaita.dsl.codeflow import flow, INPUT
@flow("t")
def t(INPUT):
    return INPUT.x if INPUT.flag else INPUT.y
""")
        self.assertIn("三元表达式", str(ctx.exception))

    def test_fstring_raises_with_hint(self):
        """Line 71: f-string — footgun hint for JoinedStr."""
        with self.assertRaises(Exception) as ctx:
            compile_source("""
from plaita.dsl.codeflow import flow, INPUT
@flow("t")
def t(INPUT):
    return f"hello {INPUT.name}"
""")
        self.assertIn("f-string", str(ctx.exception))

    def test_list_comprehension_raises_with_hint(self):
        """Line 71: list comprehension — footgun hint for ListComp."""
        with self.assertRaises(Exception) as ctx:
            compile_source("""
from plaita.dsl.codeflow import flow, INPUT
@flow("t")
def t(INPUT):
    return [x for x in INPUT.items]
""")
        self.assertIn("列表推导式", str(ctx.exception))

    def test_yield_expression_raises_generic(self):
        """Line 72: `yield` is not in _FOOTGUN_HINTS → generic 不支持的表达式 raise."""
        with self.assertRaises(Exception) as ctx:
            compile_source("""
from plaita.dsl.codeflow import flow, INPUT
@flow("t")
def t(INPUT):
    yield INPUT.x
    return 0
""")
        self.assertIn("不支持的表达式", str(ctx.exception))


# ---------------------------------------------------------------------------
# _expr.py : line 94 — ast.Index py<3.9 compat (direct call)
# ---------------------------------------------------------------------------

class TestAstIndexCompat(unittest.TestCase):
    @unittest.skipUnless(hasattr(ast, "Index"), "ast.Index not available in this Python version")
    def test_eval_subscript_index_with_ast_index(self):
        """Line 94: _eval_subscript_index handles py<3.9 ast.Index nodes."""
        from plaita.dsl.codeflow._expr import _eval_subscript_index  # type: ignore

        ctx = _CompileCtx(known_node_types=set())
        inner = ast.Constant(value=7)
        try:
            # Python 3.9-3.11: Index(value=...) as positional
            idx_node = ast.Index(inner)  # type: ignore[attr-defined]
        except TypeError:
            # Some versions may differ; skip gracefully
            self.skipTest("Cannot construct ast.Index in this runtime")
        result = _eval_subscript_index(idx_node, ctx)
        self.assertEqual(result, "7")


# ---------------------------------------------------------------------------
# _expr.py : line 176 — unsupported comparison op (is / is not)
# ---------------------------------------------------------------------------

class TestUnsupportedCompareOp(unittest.TestCase):
    def test_is_none_comparison_raises(self):
        """Line 176: `INPUT.x is None` — ast.Is not in _COMPARE_OP → raises."""
        with self.assertRaises(Exception) as ctx:
            compile_source("""
from plaita.dsl.codeflow import flow, INPUT
@flow("t")
def t(INPUT):
    if INPUT.x is None:
        return 1
    return 2
""")
        self.assertIn("不支持的比较运算", str(ctx.exception))

    def test_is_not_comparison_raises(self):
        """Line 176: `INPUT.x is not None` — ast.IsNot not in _COMPARE_OP → raises."""
        with self.assertRaises(Exception) as ctx:
            compile_source("""
from plaita.dsl.codeflow import flow, INPUT
@flow("t")
def t(INPUT):
    if INPUT.x is not None:
        return 1
    return 2
""")
        self.assertIn("不支持的比较运算", str(ctx.exception))


# ---------------------------------------------------------------------------
# _expr.py : line 189 — not on non-negatable operator (direct call)
# _expr.py : line 196 — not on invalid operand (direct call)
# ---------------------------------------------------------------------------

class TestNegateConditionErrors(unittest.TestCase):
    def test_negate_non_negatable_op_raises(self):
        """Line 189: _negate_condition on cond with operator not in _NEGATE_OP → raise."""
        from plaita.dsl.codeflow._expr import _negate_condition  # type: ignore

        node = ast.Constant(value=1)
        cond = {"field": "$INPUT.x", "operator": "contains", "value": "hello"}
        with self.assertRaises(_CodeflowError) as ctx:
            _negate_condition(cond, node)
        self.assertIn("not 无法翻转运算符", str(ctx.exception))

    def test_negate_invalid_operand_raises(self):
        """Line 196: _negate_condition on cond with no operator/relation → raise."""
        from plaita.dsl.codeflow._expr import _negate_condition  # type: ignore

        node = ast.Constant(value=1)
        # No "operator" and no valid "relation"
        cond = {"field": "$INPUT.x"}
        with self.assertRaises(_CodeflowError) as ctx:
            _negate_condition(cond, node)
        self.assertIn("not 的操作数不合法", str(ctx.exception))


# ---------------------------------------------------------------------------
# _stmt.py : lines 84/93 — if body → None AND after → None (dangling true branch)
# _stmt.py : lines 88/95 — if body returns, else → None AND after → None (dangling false branch)
# ---------------------------------------------------------------------------

class TestIfBranchDangling(unittest.TestCase):
    def test_if_pass_body_dangling_raises(self):
        """Lines 84+93: if INPUT.flag: pass with no after → 真分支悬空."""
        with self.assertRaises(Exception) as ctx:
            compile_source("""
from plaita.dsl.codeflow import flow, INPUT
@flow("t")
def t(INPUT):
    if INPUT.flag:
        pass
""")
        self.assertIn("真分支悬空", str(ctx.exception))

    def test_if_else_pass_dangling_raises(self):
        """Lines 88+95: if body returns, else: pass with no after → 假分支悬空."""
        with self.assertRaises(Exception) as ctx:
            compile_source("""
from plaita.dsl.codeflow import flow, INPUT
@flow("t")
def t(INPUT):
    if INPUT.flag:
        return INPUT.x
    else:
        pass
""")
        self.assertIn("假分支悬空", str(ctx.exception))


# ---------------------------------------------------------------------------
# _stmt.py : line 115 — collection node without positional arg
# ---------------------------------------------------------------------------

class TestCollectionNodeNoArg(unittest.TestCase):
    def test_map_no_collection_arg_raises(self):
        """Line 115: MAP() with no collection arg → raises."""
        with self.assertRaises(Exception) as ctx:
            compile_source("""
from plaita.dsl.codeflow import flow, MAP, INPUT
@flow("t")
def t(INPUT):
    for x in MAP(id="m"):
        return x
    return INPUT.x
""")
        self.assertIn("MAP 需要一个集合表达式", str(ctx.exception))

    def test_filter_no_collection_arg_raises(self):
        """Line 115: FILTER() with no collection arg → raises."""
        with self.assertRaises(Exception) as ctx:
            compile_source("""
from plaita.dsl.codeflow import flow, FILTER, INPUT
@flow("t")
def t(INPUT):
    for x in FILTER():
        return x
    return INPUT.x
""")
        self.assertIn("FILTER 需要一个集合表达式", str(ctx.exception))


# ---------------------------------------------------------------------------
# _stmt.py : line 127 — REDUCE with wrong number of loop vars
# ---------------------------------------------------------------------------

class TestReduceWrongLoopVars(unittest.TestCase):
    def test_reduce_three_vars_raises(self):
        """Line 127: REDUCE with 3 loop vars (need exactly 2) → raises."""
        with self.assertRaises(Exception) as ctx:
            compile_source("""
from plaita.dsl.codeflow import flow, REDUCE, F, INPUT
@flow("t")
def t(INPUT):
    for a, b, c in REDUCE(INPUT.items):
        return F.add(a, b)
    return INPUT.x
""")
        self.assertIn("REDUCE 的循环变量必须是 (first, second)", str(ctx.exception))

    def test_reduce_one_var_raises(self):
        """Line 127: REDUCE with 1 loop var → raises."""
        with self.assertRaises(Exception) as ctx:
            compile_source("""
from plaita.dsl.codeflow import flow, REDUCE, F, INPUT
@flow("t")
def t(INPUT):
    for x in REDUCE(INPUT.items):
        return x
    return INPUT.x
""")
        self.assertIn("REDUCE 的循环变量必须是 (first, second)", str(ctx.exception))


# ---------------------------------------------------------------------------
# _stmt.py : line 140 — empty/pass loop body
# ---------------------------------------------------------------------------

class TestEmptyLoopBody(unittest.TestCase):
    def test_map_pass_body_raises(self):
        """Line 140: for x in MAP(...): pass — child body empty → raises."""
        with self.assertRaises(Exception) as ctx:
            compile_source("""
from plaita.dsl.codeflow import flow, MAP, INPUT
@flow("t")
def t(INPUT):
    for x in MAP(INPUT.items):
        pass
    return INPUT.x
""")
        self.assertIn("循环体为空或全部悬空", str(ctx.exception))


# ---------------------------------------------------------------------------
# _stmt.py : line 180 — collection node no after (no return/next)
# ---------------------------------------------------------------------------

class TestCollectionNodeNoAfter(unittest.TestCase):
    def test_map_no_return_after_raises(self):
        """Line 180: MAP node at end of function with no return → 集合节点之后悬空."""
        with self.assertRaises(Exception) as ctx:
            compile_source("""
from plaita.dsl.codeflow import flow, MAP, INPUT
@flow("t")
def t(INPUT):
    for x in MAP(INPUT.items):
        return x
""")
        self.assertIn("集合节点之后悬空", str(ctx.exception))


# ---------------------------------------------------------------------------
# _stmt.py : line 191 — assignment target not single Name
# ---------------------------------------------------------------------------

class TestAssignmentTargetNotName(unittest.TestCase):
    def test_tuple_unpack_assign_raises(self):
        """Line 191: a, b = HTTP(...) — tuple target not single Name → raises."""
        with self.assertRaises(Exception) as ctx:
            compile_source("""
from plaita.dsl.codeflow import flow, HTTP, INPUT
@flow("t")
def t(INPUT):
    a, b = HTTP(url=INPUT.url)
    return a
""")
        self.assertIn("赋值目标必须是单个变量名", str(ctx.exception))

    def test_augmented_assign_raises(self):
        """Line 191: multiple assignment targets raises."""
        with self.assertRaises(Exception) as ctx:
            compile_source("""
from plaita.dsl.codeflow import flow, INPUT
@flow("t")
def t(INPUT):
    a = b = INPUT.x
    return a
""")
        self.assertIn("赋值目标必须是单个变量名", str(ctx.exception))


# ---------------------------------------------------------------------------
# _stmt.py : line 201 — assignment with no after (no return after assign)
# ---------------------------------------------------------------------------

class TestAssignmentNoAfter(unittest.TestCase):
    def test_assign_at_end_no_return_raises(self):
        """Line 201: x = HTTP(url=...) at end of function with no return → raises."""
        with self.assertRaises(Exception) as ctx:
            compile_source("""
from plaita.dsl.codeflow import flow, HTTP, INPUT
@flow("t")
def t(INPUT):
    x = HTTP(url=INPUT.url)
""")
        self.assertIn("之后悬空", str(ctx.exception))


# ---------------------------------------------------------------------------
# _stmt.py : line 230 — expression stmt with no after
# ---------------------------------------------------------------------------

class TestExprStmtNoAfter(unittest.TestCase):
    def test_node_call_stmt_at_end_no_return_raises(self):
        """Line 230: HTTP(...) as stmt at end of function with no return → raises."""
        with self.assertRaises(Exception) as ctx:
            compile_source("""
from plaita.dsl.codeflow import flow, HTTP, INPUT
@flow("t")
def t(INPUT):
    HTTP(url=INPUT.url)
""")
        self.assertIn("之后悬空", str(ctx.exception))


# ---------------------------------------------------------------------------
# _source.py : line 29 — _func_ast on async function (no FunctionDef in AST)
# ---------------------------------------------------------------------------

class TestFuncAstNoFunctionDef(unittest.TestCase):
    def test_async_function_raises(self):
        """Line 29: compile_func on async function → no FunctionDef found → raises."""
        with self.assertRaises(Exception) as ctx:
            async def async_flow(INPUT):
                return INPUT.x

            compile_func(async_flow, "test_async")
        self.assertIn("找不到函数定义", str(ctx.exception))


# ---------------------------------------------------------------------------
# _source.py : line 63 — _compile_fdef with pass-only function body
# ---------------------------------------------------------------------------

class TestCompileFdefEmptyBody(unittest.TestCase):
    def test_pass_only_function_raises(self):
        """Line 63: @flow function with only `pass` → 函数体为空."""
        with self.assertRaises(Exception) as ctx:
            compile_source("""
from plaita.dsl.codeflow import flow, INPUT
@flow("t")
def t(INPUT):
    pass
""")
        self.assertIn("函数体为空", str(ctx.exception))


# ---------------------------------------------------------------------------
# _source.py : line 100 — _compile_childflow_fdef with pass-only body
# ---------------------------------------------------------------------------

class TestCompileChildflowEmptyBody(unittest.TestCase):
    def test_pass_only_childflow_raises(self):
        """Line 100: @childflow function with only `pass` → 子流程函数体为空."""
        with self.assertRaises(Exception) as ctx:
            compile_source("""
from plaita.dsl.codeflow import flow, childflow, CHILD, INPUT

@childflow
def empty_cf(INPUT):
    pass

@flow("t")
def t(INPUT):
    result = CHILD(INPUT.x, flow=empty_cf)
    return result
""")
        self.assertIn("子流程函数体为空", str(ctx.exception))


# ---------------------------------------------------------------------------
# _source.py : line 195 — _deco_name returns None (Attribute decorator)
# ---------------------------------------------------------------------------

class TestDecoNameAttributeDecorator(unittest.TestCase):
    def test_attribute_decorator_is_ignored(self):
        """Line 195: @some_module.flow → _deco_name returns None, decorator ignored."""
        ir = compile_source("""
@some_module.flow
def my_func(INPUT):
    return 42
""")
        # Compiles successfully — decorator was silently ignored
        self.assertIsNotNone(ir)
        self.assertEqual(ir["flow_id"], "my_func")


# ---------------------------------------------------------------------------
# _source.py : lines 202-203 — _eval_deco_value returns None (non-literal)
# ---------------------------------------------------------------------------

class TestEvalDecoValueNonLiteral(unittest.TestCase):
    def test_flow_id_from_variable_is_ignored(self):
        """Lines 202-203: @flow(some_var) — arg is Name, literal_eval fails → None."""
        ir = compile_source("""
from plaita.dsl.codeflow import flow, INPUT
some_var = "my_id"

@flow(some_var)
def named_flow(INPUT):
    return INPUT.x
""")
        # pos_id is None (non-literal), falls back to function name
        self.assertEqual(ir["flow_id"], "named_flow")


# ---------------------------------------------------------------------------
# _source.py : line 218 — _extract_deco_opts skips kw with arg=None (**kwargs)
# ---------------------------------------------------------------------------

class TestExtractDecoOptsKwargStar(unittest.TestCase):
    def test_flow_decorator_with_star_kwargs_is_handled(self):
        """Line 218: @flow(**some_dict) — keyword with arg=None → continue (skip)."""
        ir = compile_source("""
from plaita.dsl.codeflow import flow, INPUT
some_dict = {}

@flow(**some_dict)
def star_flow(INPUT):
    return INPUT.x
""")
        self.assertIsNotNone(ir)
        self.assertEqual(ir["flow_id"], "star_flow")


if __name__ == "__main__":
    unittest.main()
