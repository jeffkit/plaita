"""Extended tests for plaita/dsl/codeflow.py — covers uncovered branches.

Target coverage gaps (lines 92, 95, 128-138, 205-206, 218, 229, 258, 264,
313-314, 320, 357, 362-367, 369-374, 376-380, 385-402, 410-413, 419-423,
433-438, 454-455, 490-508, 515-522, 553, 568-571, 590, 615, 641, 675-686,
697, 707, 750-766, 786-827, 882-960, ...).
"""
from __future__ import annotations

import unittest

from plaita.dsl.codeflow import (
    ErrorHandler,
    _Placeholder,
    compile_func,
    compile_source,
    flow,
    childflow,
    CHILD,
    CODE,
    EVENT,
    F,
    FILTER,
    FIND,
    HTTP,
    INPUT,
    LOOP,
    MAP,
    NODE,
    PARALLEL,
    REDUCE,
    flow_from_source,
)


# ---------------------------------------------------------------------------
# _Placeholder
# ---------------------------------------------------------------------------

class TestPlaceholder(unittest.TestCase):
    def test_call_returns_self(self):
        """Line 92: __call__ returns self-Placeholder."""
        p = _Placeholder("TEST")
        result = p(1, 2, key="val")
        self.assertIsInstance(result, _Placeholder)

    def test_repr(self):
        """Line 95: __repr__ shows placeholder name."""
        p = _Placeholder("MY_PH")
        r = repr(p)
        self.assertIn("MY_PH", r)
        self.assertIn("placeholder", r.lower())


# ---------------------------------------------------------------------------
# ErrorHandler.__init__ with all optional fields
# ---------------------------------------------------------------------------

class TestErrorHandler(unittest.TestCase):
    def test_default_strategy(self):
        """Default strategy is 'abort'."""
        eh = ErrorHandler()
        self.assertEqual(eh.spec["strategy"], "abort")

    def test_continue_strategy(self):
        eh = ErrorHandler("continue")
        self.assertEqual(eh.spec["strategy"], "continue")

    def test_retry_times(self):
        """Line 131-132: retry_times sets retryTimes."""
        eh = ErrorHandler("continue", retry_times=3)
        self.assertEqual(eh.spec["retryTimes"], 3)

    def test_default_value(self):
        """Lines 133-134: default_value sets defaultValue."""
        eh = ErrorHandler("continue_with", default_value={"err": True})
        self.assertEqual(eh.spec["defaultValue"], {"err": True})

    def test_error_code(self):
        """Lines 135-136: error_code sets errorCode."""
        eh = ErrorHandler("abort", error_code=500)
        self.assertEqual(eh.spec["errorCode"], 500)

    def test_error_message(self):
        """Lines 137-138: error_message sets errorMessage."""
        eh = ErrorHandler("abort", error_message="custom msg")
        self.assertEqual(eh.spec["errorMessage"], "custom msg")

    def test_invalid_strategy_raises(self):
        """Line 128-129: invalid strategy raises ValueError."""
        with self.assertRaises(ValueError):
            ErrorHandler("invalid_strategy")


# ---------------------------------------------------------------------------
# Expression errors via @flow
# ---------------------------------------------------------------------------

class TestExpressionErrors(unittest.TestCase):
    def test_unsupported_binop(self):
        """Line 364: unsupported binary operator (FloorDiv) in expression."""
        with self.assertRaises(Exception) as ctx:
            @flow("test_binop_error")
            def _test(INPUT):
                x = INPUT.a // INPUT.b  # noqa: F841
                return x

    def test_unary_usub(self):
        """Line 370-371: unary negation -INPUT.x → $F.sub(0, ...)."""
        @flow("test_usub")
        def test_neg(INPUT):
            return -INPUT.value

        result = test_neg.run(value=5)
        self.assertEqual(result, -5)

    def test_unary_not_in_expr_raises(self):
        """Line 373: 'not' in non-condition position raises."""
        with self.assertRaises(Exception):
            @flow("test_not_expr")
            def _test(INPUT):
                x = not INPUT.flag  # noqa: F841
                return x

    def test_subscript_access(self):
        """Lines 376-380: subscript access INPUT.items[0]."""
        @flow("test_subscript")
        def test_sub(INPUT):
            return INPUT.items[0]

        result = test_sub.run(items=[10, 20, 30])
        self.assertEqual(result, 10)

    def test_dict_literal_in_return(self):
        """Line 381-389: dict literal compiled and returned."""
        @flow("test_dict_literal")
        def test_dict(INPUT):
            return {"key": INPUT.value, "static": 42}

        result = test_dict.run(value="hello")
        self.assertEqual(result, {"key": "hello", "static": 42})

    def test_list_literal_in_return(self):
        """Line 390-391: list literal compiled and returned."""
        @flow("test_list_literal")
        def test_list(INPUT):
            return [INPUT.a, INPUT.b, 99]

        result = test_list.run(a=1, b=2)
        self.assertEqual(result, [1, 2, 99])

    def test_boolop_in_expr_raises(self):
        """Line 393: BoolOp in expression position raises."""
        with self.assertRaises(Exception):
            @flow("test_boolop_expr")
            def _test(INPUT):
                return INPUT.a and INPUT.b  # BoolOp in expression

    def test_footgun_lambda_raises(self):
        """Line 397: lambda in @flow raises with readable hint."""
        with self.assertRaises(Exception) as ctx:
            @flow("test_lambda")
            def _test(INPUT):
                f = lambda x: x  # noqa: F841
                return f

    def test_footgun_fstring_raises(self):
        """Line 397: f-string in @flow raises with readable hint."""
        code = '''
from plaita.dsl.codeflow import flow, INPUT

@flow("test_fstring_error")
def _test(INPUT):
    return f"hello {INPUT.name}"
'''
        with self.assertRaises(Exception):
            compile_source(code)

    def test_unknown_name_raises(self):
        """Lines 413: unknown name in expression raises."""
        with self.assertRaises(Exception):
            @flow("test_unknown_name")
            def _test(INPUT):
                return UNKNOWN_VAR  # noqa: F821 (intentional)

    def test_builtin_name_alone_raises(self):
        """Line 412: builtin like 'len' used as name (not called) raises."""
        with self.assertRaises(Exception):
            from plaita.dsl.codeflow import compile_source as cs
            cs("""
from plaita.dsl.codeflow import flow, INPUT
@flow("test_bare_builtin")
def _test(INPUT):
    return len
""")


# ---------------------------------------------------------------------------
# _eval_subscript_index error
# ---------------------------------------------------------------------------

class TestSubscriptErrors(unittest.TestCase):
    def test_non_integer_subscript_raises(self):
        """Line 423: subscript with string key raises."""
        with self.assertRaises(Exception):
            @flow("test_str_subscript")
            def _test(INPUT):
                return INPUT.data["key"]  # string subscript not supported


# ---------------------------------------------------------------------------
# _render_arg edge cases
# ---------------------------------------------------------------------------

class TestRenderArg(unittest.TestCase):
    def test_none_renders_to_null(self):
        """Line 435: None renders to 'null' in $F call context."""
        from plaita.dsl.codeflow import _render_arg
        self.assertEqual(_render_arg(None), "null")

    def test_bool_true_renders(self):
        from plaita.dsl.codeflow import _render_arg
        self.assertEqual(_render_arg(True), "true")

    def test_bool_false_renders(self):
        from plaita.dsl.codeflow import _render_arg
        self.assertEqual(_render_arg(False), "false")

    def test_dict_raises(self):
        """Line 438: dict value can't be rendered as $F arg."""
        from plaita.dsl.codeflow import _render_arg
        with self.assertRaises(Exception):
            _render_arg({"key": "val"})


# ---------------------------------------------------------------------------
# Node call in expression position (lines 454-455)
# ---------------------------------------------------------------------------

class TestNodeCallInExpression(unittest.TestCase):
    def test_http_in_expression_raises(self):
        """Lines 454-455: HTTP(...) inside another expression raises."""
        with self.assertRaises(Exception):
            @flow("test_node_in_expr")
            def _test(INPUT):
                return F.upper(HTTP(url="http://x.com"))  # noqa


# ---------------------------------------------------------------------------
# _compile_condition: bare expression truthiness (line 490-491)
# ---------------------------------------------------------------------------

class TestConditionBareExpr(unittest.TestCase):
    def test_bare_variable_as_condition(self):
        """Lines 490-491: bare variable used as condition → (expr != False)."""
        @flow("test_bare_cond")
        def test_truthy(INPUT):
            if INPUT.flag:
                return "yes"
            return "no"

        self.assertEqual(test_truthy.run(flag=True), "yes")
        self.assertEqual(test_truthy.run(flag=False), "no")

    def test_not_condition_with_compare(self):
        """Lines 515-516: not of a compare operator → negated op."""
        @flow("test_not_cond")
        def test_not(INPUT):
            if not INPUT.x == 5:
                return "not_five"
            return "five"

        self.assertEqual(test_not.run(x=5), "five")
        self.assertEqual(test_not.run(x=3), "not_five")

    def test_not_of_and_condition(self):
        """Lines 517-519: not(a and b) → De Morgan → or(not a, not b)."""
        @flow("test_not_and")
        def test_de_morgan(INPUT):
            if not (INPUT.x > 0 and INPUT.y > 0):
                return "fail"
            return "pass"

        self.assertEqual(test_de_morgan.run(x=1, y=1), "pass")
        self.assertEqual(test_de_morgan.run(x=-1, y=1), "fail")

    def test_chained_compare(self):
        """Lines 502, 508: chained comparison a < b < c."""
        @flow("test_chained")
        def test_chain(INPUT):
            if 0 < INPUT.x < 10:
                return "in_range"
            return "out"

        self.assertEqual(test_chain.run(x=5), "in_range")
        self.assertEqual(test_chain.run(x=15), "out")


# ---------------------------------------------------------------------------
# Unreachable statements after return
# ---------------------------------------------------------------------------

class TestUnreachableStatements(unittest.TestCase):
    def test_statement_after_return_raises(self):
        """Line 553: code after return raises _CodeflowError."""
        with self.assertRaises(Exception):
            @flow("test_unreachable")
            def _test(INPUT):
                return INPUT.x
                y = INPUT.y  # noqa: F841 (dead code)


# ---------------------------------------------------------------------------
# For loop compilation: LOOP, REDUCE
# ---------------------------------------------------------------------------

class TestLoopNodeCompilation(unittest.TestCase):
    def test_loop_node(self):
        """LOOP compilation with condition."""
        @flow("test_loop")
        def test_loop_flow(INPUT):
            for item in LOOP(INPUT.items, id="lp"):
                if item > 3:
                    return "stop"
                return item
            return NODE.lp

        result = test_loop_flow.run(items=[1, 2, 3, 4, 5])
        self.assertIsNotNone(result)

    def test_reduce_node(self):
        """REDUCE compilation with two-tuple loop vars."""
        @flow("test_reduce")
        def test_reduce_flow(INPUT):
            for acc, item in REDUCE(INPUT.nums, id="rd"):
                return F.add(acc, item)
            return NODE.rd

        result = test_reduce_flow.run(nums=[1, 2, 3, 4])
        self.assertEqual(result, 10)  # sum


# ---------------------------------------------------------------------------
# CODE node compilation
# ---------------------------------------------------------------------------

class TestCodeNodeCompilation(unittest.TestCase):
    def test_code_method_syntax(self):
        """Line 807-808: CODE.python(code) syntax — get IR without Flow validation."""
        def _func(INPUT):
            result = CODE.python("def run(x): return x['val'] * 2")
            return result

        ir = compile_func(_func, "test_code_method")
        code_nodes = [n for n in ir["nodes"] if n.get("type") == "code"]
        self.assertEqual(len(code_nodes), 1)
        self.assertEqual(code_nodes[0]["language"], "python")

    def test_code_missing_raises(self):
        """Line 811-812: CODE without code argument raises."""
        with self.assertRaises(Exception):
            def _func(INPUT):
                return CODE(lang="python")  # missing code
            compile_func(_func, "test_code_missing")


# ---------------------------------------------------------------------------
# EVENT node compilation
# ---------------------------------------------------------------------------

class TestEventNodeCompilation(unittest.TestCase):
    def test_event_basic(self):
        """Lines 818-827: EVENT node compilation — get raw IR."""
        def _func(INPUT):
            ev = EVENT(type="user.login")
            return ev

        ir = compile_func(_func, "test_event_flow")
        event_nodes = [n for n in ir["nodes"] if n.get("type") == "event"]
        self.assertEqual(len(event_nodes), 1)
        self.assertEqual(event_nodes[0]["eventType"], "user.login")

    def test_event_missing_type_raises(self):
        """Line 822-823: EVENT without type raises."""
        with self.assertRaises(Exception):
            def _func(INPUT):
                ev = EVENT()  # missing type
                return ev
            compile_func(_func, "test_event_missing")


# ---------------------------------------------------------------------------
# HTTP with ErrorHandler in @flow
# ---------------------------------------------------------------------------

class TestHttpWithErrorHandler(unittest.TestCase):
    def test_http_with_on_error(self):
        """Lines 799-801, 907-920: HTTP node with ErrorHandler — get raw IR."""
        def _func(INPUT):
            resp = HTTP(
                url=INPUT.url,
                on_error=ErrorHandler("continue_with", default_value={"error": True})
            )
            return resp

        ir = compile_func(_func, "test_http_error")
        http_nodes = [n for n in ir["nodes"] if n.get("type") == "http"]
        self.assertEqual(len(http_nodes), 1)
        self.assertIn("errorHandler", http_nodes[0])
        self.assertEqual(http_nodes[0]["errorHandler"]["strategy"], "continue_with")
        self.assertEqual(http_nodes[0]["errorHandler"]["defaultValue"], {"error": True})

    def test_http_with_retry(self):
        """ErrorHandler retry_times and error_code via AST."""
        def _func(INPUT):
            resp = HTTP(
                url=INPUT.url,
                on_error=ErrorHandler("abort", retry_times=2, error_code=503)
            )
            return resp

        ir = compile_func(_func, "test_http_retry")
        http_node = next(n for n in ir["nodes"] if n.get("type") == "http")
        eh = http_node["errorHandler"]
        self.assertEqual(eh["retryTimes"], 2)
        self.assertEqual(eh["errorCode"], 503)


# ---------------------------------------------------------------------------
# compile_source: multiple candidates and explicit flow_id
# ---------------------------------------------------------------------------

class TestCompileSource(unittest.TestCase):
    def test_compile_single_function(self):
        """compile_source with a single function (no decorator)."""
        src = """
def my_flow(INPUT):
    return INPUT.value
"""
        ir = compile_source(src)
        self.assertEqual(ir["flow_id"], "my_flow")

    def test_compile_with_explicit_flow_id(self):
        """Lines 1248-1252: explicit flow_id selects function."""
        src = """
def alpha(INPUT):
    return 1

def beta(INPUT):
    return 2
"""
        ir = compile_source(src, flow_id="beta")
        self.assertEqual(ir["flow_id"], "beta")

    def test_compile_no_function_raises(self):
        """Lines 1259-1260: no function in source raises."""
        with self.assertRaises(Exception):
            compile_source("x = 1\ny = 2\n")

    def test_compile_multiple_candidates_raises(self):
        """Lines 1261-1263: multiple candidates without flow_id raises."""
        with self.assertRaises(Exception):
            compile_source("""
def alpha(INPUT):
    return 1

def beta(INPUT):
    return 2
""")

    def test_compile_unknown_flow_id_raises(self):
        """Lines 1253-1254: flow_id not found raises."""
        with self.assertRaises(Exception):
            compile_source("""
def my_func(INPUT):
    return 1
""", flow_id="nonexistent")

    def test_compile_with_childflow(self):
        """compile_source with childflow definition."""
        src = """
from plaita.dsl.codeflow import flow, childflow, CHILD, INPUT, NODE

@childflow
def my_child(INPUT):
    return INPUT.x

@flow("main")
def main_flow(INPUT):
    result = CHILD(INPUT.x, flow=my_child)
    return NODE.result
"""
        ir = compile_source(src)
        self.assertEqual(ir["flow_id"], "main")


# ---------------------------------------------------------------------------
# flow_from_source
# ---------------------------------------------------------------------------

class TestFlowFromSource(unittest.TestCase):
    def test_basic_flow_from_source(self):
        """flow_from_source creates a runnable Flow."""
        src = """
def double(INPUT):
    return INPUT.value * 2
"""
        f = flow_from_source(src)
        result = f.run(value=7)
        self.assertEqual(result, 14)


# ---------------------------------------------------------------------------
# pass statement in @flow
# ---------------------------------------------------------------------------

class TestPassStatement(unittest.TestCase):
    def test_pass_statement_compiles(self):
        """Lines 568-569: pass statement is a no-op in @flow."""
        @flow("test_pass")
        def test_p(INPUT):
            pass  # not a return, so succ is used
            return INPUT.x

        result = test_p.run(x=99)
        self.assertEqual(result, 99)


# ---------------------------------------------------------------------------
# _default_known_node_types exception path
# ---------------------------------------------------------------------------

class TestDefaultKnownNodeTypes(unittest.TestCase):
    def test_exception_returns_empty_set(self):
        """Lines 205-206: if get_default_registry raises, returns empty set."""
        from unittest.mock import patch
        with patch("plaita.dsl.codeflow._default_known_node_types",
                   side_effect=Exception("registry error")):
            # We need to call the internal function directly
            pass  # Just verify the module doesn't crash

        from plaita.dsl.codeflow import _default_known_node_types
        with patch("plaita.node.get_default_registry",
                   side_effect=Exception("fail")):
            result = _default_known_node_types()
        self.assertEqual(result, set())


if __name__ == "__main__":
    unittest.main()
