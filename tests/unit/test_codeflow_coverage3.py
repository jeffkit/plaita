"""Extra coverage tests for plaita/dsl/codeflow.py — targets remaining gaps.

Covers:
- _describe_call Name path (line 258) and nested-call path (line 229)
- auto_id collision (lines 313-314)
- claim collision (line 320)
- _compile_expr attribute base not str (line 357)
- _negate_condition or relation (lines 519-522)
- unsupported statement (line 571)
- for loop not MAP/FILTER (line 615)
- MAP with 2 loop vars/index (line 641)
- MAP concurrent + maxConcurrent (lines 675-678)
- REDUCE with initial (line 682)
- non-node expression statement (lines 750-752)
- HTTP method override via keyword + with body + with input (lines 786, 792, 798)
- PARALLEL node with dict branches and join (lines 841-853)
- _eval_error_handler with errorMessage + non-ErrorHandler (lines 919, 921)
- _eval_childflow_arg not in childflows (lines 933-935)
- _eval_parallel_branches list of tuples + error (lines 939-960)
- _eval_join with list and error (lines 964-972)
- _unpack_names tuple (line 982)
- _compile_fdef global_context + metadata (lines 1055, 1058)
- _compile_childflow_fdef desc (line 1085)
- compile_source with @flow opts containing global_context/metadata/desc
- _deco_name fallback (line 1185)
- _eval_deco_value exception (lines 1192-1193)
- _extract_deco_opts skip unknown kw (lines 1208, 1211)
"""
from __future__ import annotations

import unittest

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
# _describe_call Name path (line 258)
# ---------------------------------------------------------------------------

class TestDescribeCallName(unittest.TestCase):
    def test_unsupported_lowercase_func_gives_readable_error(self):
        """Line 258: calling unsupported lowercase func triggers _describe_call Name path."""
        with self.assertRaises(Exception) as ctx:
            @flow("test_describe_name")
            def _test(INPUT):
                return some_func(INPUT.x)  # noqa: F821

        self.assertIn("some_func", str(ctx.exception))


class TestDescribeCallNested(unittest.TestCase):
    def test_chained_call_gives_readable_error(self):
        """Line 229: chained call like obj_factory().method() triggers recursive _describe_call."""
        with self.assertRaises(Exception) as ctx:
            @flow("test_describe_nested")
            def _test(INPUT):
                return get_factory().do_thing(INPUT.x)  # noqa: F821

        # Just verifies it raises (not assert specific message since line may vary)
        self.assertIsInstance(ctx.exception, Exception)


# ---------------------------------------------------------------------------
# auto_id collision (lines 313-314)
# ---------------------------------------------------------------------------

class TestAutoIdCollision(unittest.TestCase):
    def test_autoid_skips_claimed_n1(self):
        """Lines 313-314: auto_id skips over _n1 if it is already claimed."""
        from plaita.dsl.codeflow import _CompileCtx
        ctx = _CompileCtx()
        # Manually claim _n1
        ctx._claimed.add("_n1")
        ctx._counter = 0  # reset so next auto_id tries _n1 first
        nid = ctx.auto_id()
        # Should have skipped _n1 and picked _n2 (or higher)
        self.assertNotEqual(nid, "_n1")
        self.assertTrue(nid.startswith("_n"))


# ---------------------------------------------------------------------------
# claim collision (line 320)
# ---------------------------------------------------------------------------

class TestClaimCollision(unittest.TestCase):
    def test_duplicate_variable_name_raises(self):
        """Line 320: using the same variable name twice in @flow raises ValueError."""
        with self.assertRaises(Exception):
            def _func(INPUT):
                result = HTTP(url=INPUT.url)
                result = HTTP(url=INPUT.url2)  # duplicate id → ValueError  # noqa: F841
                return result

            compile_func(_func, "test_dup_id")


# ---------------------------------------------------------------------------
# _compile_expr attribute base not str (line 357)
# ---------------------------------------------------------------------------

class TestAttributeBaseNotStr(unittest.TestCase):
    def test_attribute_of_list_literal_raises(self):
        """Line 357: [INPUT.x].count — base compiles to list, not str → raises."""
        with self.assertRaises(Exception):
            compile_source("""
from plaita.dsl.codeflow import flow, INPUT
@flow("test_attr_not_str")
def _test(INPUT):
    return [INPUT.x].count
""")


# ---------------------------------------------------------------------------
# _negate_condition or relation (lines 519-522)
# ---------------------------------------------------------------------------

class TestNegateOrCondition(unittest.TestCase):
    def test_not_of_or_condition(self):
        """Lines 519-522: not(a == 1 or b == 2) → De Morgan → and(a != 1, b != 2)."""
        @flow("test_not_or")
        def test_not_or_flow(INPUT):
            if not (INPUT.x == 1 or INPUT.y == 2):
                return "neither"
            return "one_or_both"

        self.assertEqual(test_not_or_flow.run(x=1, y=5), "one_or_both")
        self.assertEqual(test_not_or_flow.run(x=5, y=2), "one_or_both")
        self.assertEqual(test_not_or_flow.run(x=5, y=5), "neither")


# ---------------------------------------------------------------------------
# Unsupported statement (line 571)
# ---------------------------------------------------------------------------

class TestUnsupportedStatement(unittest.TestCase):
    def test_while_loop_raises(self):
        """Line 571: while loop in @flow raises _CodeflowError."""
        with self.assertRaises(Exception):
            @flow("test_while")
            def _test(INPUT):
                while INPUT.x > 0:  # not supported  # noqa: F821
                    return INPUT.x
                return 0


# ---------------------------------------------------------------------------
# For loop not MAP/FILTER (line 615)
# ---------------------------------------------------------------------------

class TestForLoopNotCollection(unittest.TestCase):
    def test_for_loop_plain_iter_raises(self):
        """Line 615: iterating over a non-MAP/FILTER variable raises."""
        with self.assertRaises(Exception):
            @flow("test_for_plain")
            def _test(INPUT):
                for x in INPUT.items:  # must be MAP/FILTER/etc.
                    return x
                return None


# ---------------------------------------------------------------------------
# MAP with 2 loop vars — index (line 641)
# ---------------------------------------------------------------------------

class TestMapWithIndex(unittest.TestCase):
    def test_map_two_loop_vars(self):
        """Line 641: MAP with 2 loop vars sets both item and index."""
        def _func(INPUT):
            for item, idx in MAP(INPUT.items, id="m1"):
                return F.add(item, idx)
            return NODE.m1

        ir = compile_func(_func, "test_map_index")
        map_node = next(n for n in ir["nodes"] if n.get("type") == "map")
        # Check child flow has item/index loop vars wired in
        child_nodes = map_node["childFlow"]["nodes"]
        self.assertTrue(any(n.get("type") == "start" for n in child_nodes))


# ---------------------------------------------------------------------------
# MAP concurrent + maxConcurrent (lines 675-678)
# ---------------------------------------------------------------------------

class TestMapConcurrent(unittest.TestCase):
    def test_map_concurrent_with_max(self):
        """Lines 675-678: MAP concurrent=True, max_concurrent=5."""
        def _func(INPUT):
            for item in MAP(INPUT.items, concurrent=True, max_concurrent=5, id="m2"):
                return item
            return NODE.m2

        ir = compile_func(_func, "test_map_concurrent")
        map_node = next(n for n in ir["nodes"] if n.get("type") == "map")
        self.assertTrue(map_node.get("concurrent"))
        self.assertEqual(map_node.get("maxConcurrent"), 5)

    def test_map_concurrent_without_max(self):
        """Line 675: MAP concurrent=True without max_concurrent."""
        def _func(INPUT):
            for item in MAP(INPUT.items, concurrent=True, id="m3"):
                return item
            return NODE.m3

        ir = compile_func(_func, "test_map_concurrent_no_max")
        map_node = next(n for n in ir["nodes"] if n.get("type") == "map")
        self.assertTrue(map_node.get("concurrent"))
        self.assertNotIn("maxConcurrent", map_node)


# ---------------------------------------------------------------------------
# REDUCE with initial (line 682)
# ---------------------------------------------------------------------------

class TestReduceWithInitial(unittest.TestCase):
    def test_reduce_initial_value(self):
        """Line 682: REDUCE with initial= sets initial field in IR."""
        def _func(INPUT):
            for acc, item in REDUCE(INPUT.items, initial=0, id="rd2"):
                return F.add(acc, item)
            return NODE.rd2

        ir = compile_func(_func, "test_reduce_initial")
        reduce_node = next(n for n in ir["nodes"] if n.get("type") == "reduce")
        self.assertEqual(reduce_node.get("initial"), 0)


# ---------------------------------------------------------------------------
# Non-node expression statement (lines 750-752)
# ---------------------------------------------------------------------------

class TestNonNodeExprStmt(unittest.TestCase):
    def test_f_call_as_statement(self):
        """Lines 750-752: F.xxx() used as standalone statement (not assigned)."""
        def _func(INPUT):
            F.upper(INPUT.name)  # expression statement, not a node call
            return INPUT.name

        ir = compile_func(_func, "test_f_stmt")
        # Should have an assignment node wrapping the F.upper call
        assign_nodes = [n for n in ir["nodes"] if n.get("type") == "assignment"]
        self.assertTrue(len(assign_nodes) >= 1)


# ---------------------------------------------------------------------------
# HTTP method override via keyword (line 786)
# ---------------------------------------------------------------------------

class TestHttpMethodOverride(unittest.TestCase):
    def test_http_method_via_keyword(self):
        """Line 786: HTTP(url=..., method='DELETE') — method via keyword arg."""
        def _func(INPUT):
            resp = HTTP(url=INPUT.url, method="DELETE")
            return resp

        ir = compile_func(_func, "test_http_delete")
        http_node = next(n for n in ir["nodes"] if n.get("type") == "http")
        self.assertEqual(http_node["method"], "DELETE")

    def test_http_with_body(self):
        """Line 792: HTTP(url=..., body=INPUT.data)."""
        def _func(INPUT):
            resp = HTTP(url=INPUT.url, body=INPUT.payload)
            return resp

        ir = compile_func(_func, "test_http_body")
        http_node = next(n for n in ir["nodes"] if n.get("type") == "http")
        self.assertIn("body", http_node)

    def test_http_with_input(self):
        """Line 798: HTTP(url=..., input=INPUT.data)."""
        def _func(INPUT):
            resp = HTTP(url=INPUT.url, input=INPUT.data)
            return resp

        ir = compile_func(_func, "test_http_input")
        http_node = next(n for n in ir["nodes"] if n.get("type") == "http")
        self.assertIn("input", http_node)

    def test_http_missing_url_raises(self):
        """Line 789: HTTP without URL raises."""
        with self.assertRaises(Exception):
            def _func(INPUT):
                resp = HTTP()
                return resp
            compile_func(_func, "test_http_no_url")


# ---------------------------------------------------------------------------
# PARALLEL node with dict branches (lines 841-853)
# ---------------------------------------------------------------------------

_PARALLEL_SRC_DICT = """
from plaita.dsl.codeflow import flow, childflow, PARALLEL, INPUT

@childflow
def branch_a(INPUT):
    return INPUT.x

@childflow
def branch_b(INPUT):
    return INPUT.y

@flow("test_parallel_dict")
def test_parallel_dict(INPUT):
    par = PARALLEL(branches={"a": branch_a, "b": branch_b})
    return par
"""

_PARALLEL_SRC_MODE = """
from plaita.dsl.codeflow import flow, childflow, PARALLEL, INPUT

@childflow
def branch_c(INPUT):
    return INPUT.x

@flow("test_parallel_mode")
def test_parallel_mode(INPUT):
    par = PARALLEL(branches={"c": branch_c}, mode="process")
    return par
"""


class TestParallelNode(unittest.TestCase):
    def test_parallel_with_dict_branches(self):
        """Lines 841-853: PARALLEL(branches={name: childflow_func})."""
        ir = compile_source(_PARALLEL_SRC_DICT)
        par_node = next(n for n in ir["nodes"] if n.get("type") == "parallel")
        self.assertEqual(len(par_node["branches"]), 2)
        branch_names = {b["name"] for b in par_node["branches"]}
        self.assertIn("a", branch_names)
        self.assertIn("b", branch_names)

    def test_parallel_with_mode(self):
        """Line 847: PARALLEL with mode='process'."""
        ir = compile_source(_PARALLEL_SRC_MODE)
        par_node = next(n for n in ir["nodes"] if n.get("type") == "parallel")
        self.assertEqual(par_node.get("mode"), "process")


# ---------------------------------------------------------------------------
# _eval_error_handler: errorMessage (line 919) + non-ErrorHandler (line 921)
# ---------------------------------------------------------------------------

class TestEvalErrorHandler(unittest.TestCase):
    def test_error_handler_with_error_message(self):
        """Line 919: ErrorHandler with error_message sets errorMessage field."""
        def _func(INPUT):
            resp = HTTP(
                url=INPUT.url,
                on_error=ErrorHandler("abort", error_message="Custom error msg")
            )
            return resp

        ir = compile_func(_func, "test_eh_msg")
        http_node = next(n for n in ir["nodes"] if n.get("type") == "http")
        eh = http_node["errorHandler"]
        self.assertEqual(eh.get("errorMessage"), "Custom error msg")

    def test_error_handler_not_error_handler_call_raises(self):
        """Line 921: on_error that's not ErrorHandler(...) raises."""
        with self.assertRaises(Exception):
            def _func(INPUT):
                resp = HTTP(url=INPUT.url, on_error=F.upper("abort"))
                return resp
            compile_func(_func, "test_eh_not_handler")


# ---------------------------------------------------------------------------
# _eval_childflow_arg: not in childflows (lines 933-935)
# ---------------------------------------------------------------------------

class TestEvalChildflowArg(unittest.TestCase):
    def test_unknown_childflow_raises(self):
        """Lines 933-934: CHILD(flow=unknown_var) raises _CodeflowError."""
        with self.assertRaises(Exception):
            def _func(INPUT):
                result = CHILD(INPUT.x, flow=unknown_childflow)  # noqa: F821
                return result
            compile_func(_func, "test_unknown_childflow")


# ---------------------------------------------------------------------------
# _eval_parallel_branches: list of tuples (lines 946-954) + error (lines 939, 955, 959)
# ---------------------------------------------------------------------------

class TestEvalParallelBranches(unittest.TestCase):
    def test_parallel_list_of_tuples(self):
        """Lines 946-954: PARALLEL(branches=[("a", childflow), ("b", childflow)])."""
        src = """
from plaita.dsl.codeflow import flow, childflow, PARALLEL, INPUT

@childflow
def cf_a(INPUT):
    return INPUT.x

@childflow
def cf_b(INPUT):
    return INPUT.y

@flow("test_parallel_list_tuples")
def test_parallel_list_tuples(INPUT):
    par = PARALLEL(branches=[("a", cf_a), ("b", cf_b)])
    return par
"""
        ir = compile_source(src)
        par_node = next(n for n in ir["nodes"] if n.get("type") == "parallel")
        self.assertEqual(len(par_node["branches"]), 2)

    def test_parallel_missing_branches_raises(self):
        """Line 843: PARALLEL without branches raises."""
        with self.assertRaises(Exception):
            def _func(INPUT):
                par = PARALLEL()
                return par
            compile_func(_func, "test_parallel_no_branches")


# ---------------------------------------------------------------------------
# _eval_join (lines 964-972)
# ---------------------------------------------------------------------------

class TestEvalJoin(unittest.TestCase):
    def test_parallel_with_join(self):
        """Lines 964-972: PARALLEL with join=['a', 'b']."""
        src = """
from plaita.dsl.codeflow import flow, childflow, PARALLEL, INPUT

@childflow
def cf_j1(INPUT):
    return INPUT.x

@childflow
def cf_j2(INPUT):
    return INPUT.y

@flow("test_parallel_join")
def test_parallel_join(INPUT):
    par = PARALLEL(branches={"a": cf_j1, "b": cf_j2}, join=["a"])
    return par
"""
        ir = compile_source(src)
        par_node = next(n for n in ir["nodes"] if n.get("type") == "parallel")
        self.assertIn("joinBranches", par_node)
        self.assertEqual(par_node["joinBranches"], ["a"])

    def test_parallel_join_not_list_raises(self):
        """Line 972: join that's not a list literal raises."""
        with self.assertRaises(Exception):
            compile_source("""
from plaita.dsl.codeflow import flow, childflow, PARALLEL, INPUT

@childflow
def cf(INPUT):
    return INPUT.x

@flow("test_parallel_join_bad")
def test_parallel_join_bad(INPUT):
    par = PARALLEL(branches={"x": cf}, join=INPUT.branches)
    return par
""")


# ---------------------------------------------------------------------------
# _unpack_names: tuple target (line 982)
# ---------------------------------------------------------------------------

class TestUnpackNamesTuple(unittest.TestCase):
    def test_for_reduce_two_vars(self):
        """Line 982: REDUCE with tuple (first, second) loop vars."""
        def _func(INPUT):
            for first, second in REDUCE(INPUT.nums, id="rd3"):
                return F.add(first, second)
            return NODE.rd3

        ir = compile_func(_func, "test_reduce_tuple")
        reduce_node = next(n for n in ir["nodes"] if n.get("type") == "reduce")
        # Child flow should have nodes referencing $INPUT[0] and $INPUT[1]
        child_nodes = reduce_node["childFlow"]["nodes"]
        self.assertTrue(len(child_nodes) >= 1)


# ---------------------------------------------------------------------------
# _compile_fdef: global_context (line 1055), metadata (line 1058)
# ---------------------------------------------------------------------------

class TestCompileFdefOpts(unittest.TestCase):
    def test_flow_with_global_context(self):
        """Line 1055: compile_func with global_context sets globalContext in IR."""
        def _test_gc(INPUT):
            return INPUT.x

        ir = compile_func(_test_gc, "test_gc", global_context={"db_url": "postgres://..."})
        self.assertIn("globalContext", ir)
        self.assertEqual(ir["globalContext"]["db_url"], "postgres://...")

    def test_flow_with_metadata(self):
        """Line 1058: compile_func with metadata sets metadata in IR."""
        def _test_meta(INPUT):
            return INPUT.x

        ir = compile_func(_test_meta, "test_meta", metadata={"owner": "team-a", "sla": "99.9%"})
        self.assertIn("metadata", ir)
        self.assertEqual(ir["metadata"]["owner"], "team-a")


# ---------------------------------------------------------------------------
# _compile_childflow_fdef: desc (line 1085)
# ---------------------------------------------------------------------------

class TestCompileChildflowDesc(unittest.TestCase):
    def test_childflow_with_desc(self):
        """Line 1085: @childflow(desc=...) sets desc in IR."""
        @childflow(desc="helper for computing discount")
        def discount_calc(INPUT):
            return F.mul(INPUT.price, INPUT.rate)

        ir = discount_calc.__codeflow_ir__
        self.assertIn("desc", ir)
        self.assertEqual(ir["desc"], "helper for computing discount")


# ---------------------------------------------------------------------------
# compile_source with global_context/metadata/desc in @flow decorator
# ---------------------------------------------------------------------------

class TestCompileSourceWithDecoOpts(unittest.TestCase):
    def test_flow_decorator_with_global_context_in_source(self):
        """Lines 1055, 1185, 1192, 1208: compile_source with @flow(global_context=...)."""
        src = """
from plaita.dsl.codeflow import flow, INPUT

@flow("my_gc_flow", global_context={"env": "prod"})
def my_gc_flow(INPUT):
    return INPUT.value
"""
        ir = compile_source(src)
        self.assertIn("globalContext", ir)
        self.assertEqual(ir["globalContext"]["env"], "prod")

    def test_flow_decorator_with_metadata_in_source(self):
        """Lines 1058: compile_source with @flow(metadata=...)."""
        src = """
from plaita.dsl.codeflow import flow, INPUT

@flow("my_meta_flow", metadata={"version": "2.0"})
def my_meta_flow(INPUT):
    return INPUT.value
"""
        ir = compile_source(src)
        self.assertIn("metadata", ir)

    def test_flow_decorator_desc_alias(self):
        """Line 1185: @flow with desc= in source mode."""
        src = """
from plaita.dsl.codeflow import flow, INPUT

@flow("desc_flow", desc="A test flow")
def desc_flow(INPUT):
    return INPUT.value
"""
        ir = compile_source(src)
        self.assertIn("desc", ir)
        self.assertEqual(ir["desc"], "A test flow")

    def test_compile_source_with_unknown_decorator_keyword(self):
        """Lines 1208, 1211: @flow with unknown keyword is silently ignored."""
        src = """
from plaita.dsl.codeflow import flow, INPUT

@flow("unknown_kw_flow", unknown_option="ignored")
def unknown_kw_flow(INPUT):
    return INPUT.value
"""
        ir = compile_source(src)
        self.assertEqual(ir["flow_id"], "unknown_kw_flow")
        # unknown_option is not in the IR
        self.assertNotIn("unknown_option", ir)

    def test_compile_source_childflow_with_desc(self):
        """Line 1085: compile_source with @childflow(desc=...)."""
        src = """
from plaita.dsl.codeflow import flow, childflow, CHILD, INPUT, NODE

@childflow(desc="My helper")
def helper(INPUT):
    return INPUT.x

@flow("main")
def main_flow(INPUT):
    result = CHILD(INPUT.x, flow=helper)
    return NODE.result
"""
        ir = compile_source(src)
        self.assertEqual(ir["flow_id"], "main")


if __name__ == "__main__":
    unittest.main()
