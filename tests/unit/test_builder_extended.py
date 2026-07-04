"""Extended tests for plaita/dsl/builder.py — covers uncovered branches.

Target coverage gaps (lines 83, 87, 89, 135, 149, 222, 240, 278-280, 315-318,
329, 340, 350-355, 368-380, 392-395, 410-419, 429-432, 486, 529, 574-586,
884, 910, 933, 942, 952, 962, 973, 986, 999, 1015, 1027, 1036-1044, 1072-1073,
1080-1081, 1088, 1091, 1094, 1097).
"""
from __future__ import annotations

import asyncio
import unittest

import plaita.dsl.builder as dsl
from plaita.dsl.builder import (
    FlowBuilder,
    LinearBuilder,
    NodeSpec,
    assignment,
    branch,
    build,
    case,
    child_flow,
    code,
    cond,
    cond_group,
    end,
    error_handler,
    event,
    find,
    filter,
    http,
    if_,
    linear,
    loop,
    map,
    parallel,
    parallel_branch,
    reduce,
    reference,
    start,
    switch,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _echo_child() -> FlowBuilder:
    """Child flow: echo $INPUT (as-is, used as inline/reference child)."""
    return (
        FlowBuilder(input_type="any")
        .add(start(next="e"))
        .add(end("e", output="$INPUT"))
    )


def _echo_item_child() -> FlowBuilder:
    """Child flow: echo $INPUT.item (for map/loop/filter/find/reduce)."""
    return (
        FlowBuilder(input_type="object")
        .add(start(next="e"))
        .add(end("e", output="$INPUT.item"))
    )


def _gt2_child() -> FlowBuilder:
    """Child flow: return True if $INPUT.item > 2, else False (for filter/find)."""
    return (
        FlowBuilder(input_type="object")
        .add(start(id="s", next="c"))
        .add(if_("c", cond("$INPUT.item", ">", 2), next="t", else_next="f"))
        .add(end("t", output=True))
        .add(end("f", output=False))
    )


# ---------------------------------------------------------------------------
# error_handler() optional params (lines 83, 87, 89)
# ---------------------------------------------------------------------------

class TestErrorHandlerFunction(unittest.TestCase):
    def test_retry_times(self):
        """Line 83: retry_times → retryTimes."""
        eh = error_handler("abort", retry_times=3)
        self.assertEqual(eh["retryTimes"], 3)

    def test_default_value(self):
        """Line 87: default_value → defaultValue."""
        eh = error_handler("continue_with", default_value={"ok": False})
        self.assertEqual(eh["defaultValue"], {"ok": False})

    def test_error_code(self):
        """Line 89 (error_code branch check)."""
        eh = error_handler("abort", error_code=400)
        self.assertEqual(eh["errorCode"], 400)

    def test_error_message(self):
        eh = error_handler("abort", error_message="bad input")
        self.assertEqual(eh["errorMessage"], "bad input")

    def test_invalid_strategy_raises(self):
        with self.assertRaises(ValueError):
            error_handler("bad_strategy")


# ---------------------------------------------------------------------------
# end() with error param (line 135)
# ---------------------------------------------------------------------------

class TestEndNodeFactory(unittest.TestCase):
    def test_end_with_error_dict(self):
        """Line 135: end with error dict sets error field."""
        n = end(id="e", error={"code": 404, "message": "Not found"})
        self.assertEqual(n["error"], {"code": 404, "message": "Not found"})


# ---------------------------------------------------------------------------
# assignment() with output_type (line 149)
# ---------------------------------------------------------------------------

class TestAssignmentNodeFactory(unittest.TestCase):
    def test_assignment_with_output_type(self):
        """Line 149: assignment with output_type."""
        n = assignment("a", "$INPUT.x", output_type={"dataType": "string"})
        self.assertEqual(n["outputType"], {"dataType": "string"})


# ---------------------------------------------------------------------------
# case() validation (line 222)
# ---------------------------------------------------------------------------

class TestCaseNodeFactory(unittest.TestCase):
    def test_case_missing_next_raises(self):
        """Line 222: case branch with no id and no next → ValueError."""
        with self.assertRaises(ValueError) as ctx:
            case("c", "$INPUT.status", [{"value": 1}])  # no next, no id
        self.assertIn("next", str(ctx.exception))

    def test_case_with_next_normalizes_to_id(self):
        """Happy path: case branch with next → id set from next."""
        n = case("c", "$INPUT.status", [{"value": 1, "next": "n1"}])
        self.assertEqual(n["cases"][0]["id"], "n1")


# ---------------------------------------------------------------------------
# _collection_node() ValueError (line 240)
# ---------------------------------------------------------------------------

class TestCollectionNodeFactory(unittest.TestCase):
    def test_loop_no_child_flow_raises(self):
        """Line 240: loop without child_flow raises ValueError."""
        with self.assertRaises(ValueError):
            loop("lp", "$INPUT.items", child_flow=None)


# ---------------------------------------------------------------------------
# map() with max_concurrent (lines 278-280)
# ---------------------------------------------------------------------------

class TestMapNodeFactory(unittest.TestCase):
    def test_map_max_concurrent(self):
        """Lines 278-280: map with concurrent=True and max_concurrent."""
        n = map("m", "$INPUT.items", _echo_child(), concurrent=True, max_concurrent=4)
        self.assertTrue(n["concurrent"])
        self.assertEqual(n["maxConcurrent"], 4)


# ---------------------------------------------------------------------------
# reduce() with initial (lines 315-318)
# ---------------------------------------------------------------------------

class TestReduceNodeFactory(unittest.TestCase):
    def test_reduce_with_initial(self):
        """Lines 315-318: reduce with initial value."""
        n = reduce("r", "$INPUT.nums", _echo_child(), initial=0)
        self.assertEqual(n["initial"], 0)


# ---------------------------------------------------------------------------
# reference() (line 340)
# ---------------------------------------------------------------------------

class TestReferenceNodeFactory(unittest.TestCase):
    def test_reference_node(self):
        """Line 340: reference node factory."""
        n = reference("ref", "$INPUT.val", _echo_child())
        self.assertEqual(n["type"], "reference")
        self.assertEqual(n["input"], "$INPUT.val")


# ---------------------------------------------------------------------------
# parallel_branch() with input and condition (lines 329, 350-355)
# ---------------------------------------------------------------------------

class TestParallelBranchFactory(unittest.TestCase):
    def test_branch_with_condition(self):
        """Lines 329, 353-354: branch with condition."""
        b = parallel_branch("b1", _echo_child(), condition=cond("$INPUT.flag", "==", True))
        self.assertIn("condition", b)
        self.assertEqual(b["condition"]["operator"], "eq")

    def test_branch_with_input_and_condition(self):
        """Lines 350-355: branch with both input and condition."""
        b = parallel_branch("b2", _echo_child(), input="$INPUT.val",
                            condition=cond("$INPUT.active", "==", True))
        self.assertEqual(b["input"], "$INPUT.val")
        self.assertIn("condition", b)


# ---------------------------------------------------------------------------
# parallel() with join_branches and is_conditional (lines 368-380)
# ---------------------------------------------------------------------------

class TestParallelNodeFactory(unittest.TestCase):
    def test_parallel_with_join_branches(self):
        """Lines 376-377: parallel with join_branches."""
        n = parallel("p", [parallel_branch("b1", _echo_child())],
                     join_branches=["b1"])
        self.assertEqual(n["joinBranches"], ["b1"])

    def test_parallel_is_conditional(self):
        """Lines 378-379: parallel with is_conditional."""
        n = parallel("p", [parallel_branch("b1", _echo_child())],
                     is_conditional=True)
        self.assertTrue(n["isConditional"])


# ---------------------------------------------------------------------------
# code() with input (lines 392-395)
# ---------------------------------------------------------------------------

class TestCodeNodeFactory(unittest.TestCase):
    def test_code_with_input(self):
        """Lines 392-395: code node with input."""
        n = code("c", "python", "def run(x): return x", input="$INPUT")
        self.assertEqual(n["input"], "$INPUT")


# ---------------------------------------------------------------------------
# http() with optional fields (lines 410-419)
# ---------------------------------------------------------------------------

class TestHttpNodeFactory(unittest.TestCase):
    def test_http_all_optional_fields(self):
        """Lines 410-419: http with headers, body, timeout, error_handler."""
        eh = error_handler("continue_with", default_value={"err": True})
        n = http("h", "POST", "http://example.com",
                 headers={"X-Key": "val"},
                 body={"data": "$INPUT.payload"},
                 timeout="PT5S",
                 error_handler=eh)
        self.assertEqual(n["headers"], {"X-Key": "val"})
        self.assertEqual(n["body"], {"data": "$INPUT.payload"})
        self.assertEqual(n["timeout"], "PT5S")
        self.assertEqual(n["errorHandler"]["strategy"], "continue_with")


# ---------------------------------------------------------------------------
# event() with event_filter (lines 429-432)
# ---------------------------------------------------------------------------

class TestEventNodeFactory(unittest.TestCase):
    def test_event_with_filter(self):
        """Lines 429-432: event with event_filter."""
        n = event("ev", "user.login", event_filter={"user_id": "$INPUT.uid"})
        self.assertEqual(n["eventFilter"], {"user_id": "$INPUT.uid"})


# ---------------------------------------------------------------------------
# FlowBuilder.add() TypeError (line 486)
# ---------------------------------------------------------------------------

class TestFlowBuilderAdd(unittest.TestCase):
    def test_add_non_node_spec_raises(self):
        """Line 486: add non-NodeSpec raises TypeError."""
        fb = FlowBuilder()
        with self.assertRaises(TypeError):
            fb.add("not a node spec")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# FlowBuilder.reroute() with else_next (line 529)
# ---------------------------------------------------------------------------

class TestFlowBuilderReroute(unittest.TestCase):
    def test_reroute_else_next(self):
        """Line 529: reroute sets else_next."""
        fb = (
            FlowBuilder("test", input_type="object")
            .add(start(next="if1"))
            .add(dsl.if_("if1", cond("$INPUT.x", ">", 0),
                         next="pos", else_next="neg"))
            .add(end("pos", output="positive"))
            .add(end("neg", output="negative"))
        )
        fb.reroute("if1", else_next="pos")  # reroute else to pos
        if_node = next(n for n in fb._nodes if n.get("id") == "if1")
        self.assertEqual(if_node["else_next"], "pos")

    def test_reroute_missing_node_raises(self):
        """KeyError when node_id not found."""
        fb = FlowBuilder()
        with self.assertRaises(KeyError):
            fb.reroute("nonexistent", next="x")


# ---------------------------------------------------------------------------
# FlowBuilder.to_dict() optional fields (lines 574-586)
# ---------------------------------------------------------------------------

class TestFlowBuilderToDict(unittest.TestCase):
    def test_all_optional_fields_in_to_dict(self):
        """Lines 574-586: all optional metadata fields included in dict."""
        fb = FlowBuilder(
            "test_flow",
            input_type="object",
            output_type={"dataType": "string"},
            desc="My flow",
            version="1.0",
            author="Alice",
            timeout="PT10S",
            global_context={"env": "prod"},
            metadata={"team": "platform"},
        )
        d = fb.to_dict()
        self.assertEqual(d["outputType"]["dataType"], "string")
        self.assertEqual(d["desc"], "My flow")
        self.assertEqual(d["version"], "1.0")
        self.assertEqual(d["author"], "Alice")
        self.assertEqual(d["timeout"], "PT10S")
        self.assertEqual(d["globalContext"]["env"], "prod")
        self.assertEqual(d["metadata"]["team"], "platform")


# ---------------------------------------------------------------------------
# FlowBuilder.validate() error cases (884-1044)
# ---------------------------------------------------------------------------

class TestFlowBuilderValidation(unittest.TestCase):
    def _fb_with_nodes(self, *nodes):
        fb = FlowBuilder("test", input_type="object")
        for n in nodes:
            fb.add(n)
        return fb

    def test_duplicate_id_raises(self):
        """Line 630: duplicate node ids → ValueError."""
        fb = self._fb_with_nodes(
            start(id="start", next="e"),
            end("start"),  # duplicate id
        )
        with self.assertRaises(ValueError) as ctx:
            fb.validate()
        self.assertIn("重复", str(ctx.exception))

    def test_missing_next_target_raises(self):
        """Lines 638-641: next points to non-existent node."""
        fb = self._fb_with_nodes(
            start(id="start", next="nonexistent"),
            end("e"),
        )
        with self.assertRaises(ValueError) as ctx:
            fb.validate()
        self.assertIn("nonexistent", str(ctx.exception))

    def test_if_missing_else_raises(self):
        """Lines 655-658: if node without else_next."""
        fb = self._fb_with_nodes(
            start(id="start", next="if1"),
            dsl.if_("if1", cond("$INPUT.x", ">", 0), next="end_node"),
            end("end_node"),
        )
        with self.assertRaises(ValueError) as ctx:
            fb.validate()
        self.assertIn("else", str(ctx.exception))

    def test_if_missing_then_raises(self):
        """Lines 651-654: if node without next/then."""
        fb = self._fb_with_nodes(
            start(id="start", next="if1"),
            dsl.if_("if1", cond("$INPUT.x", ">", 0), else_next="end_node"),
            end("end_node"),
        )
        with self.assertRaises(ValueError) as ctx:
            fb.validate()
        self.assertIn("next", str(ctx.exception))

    def test_switch_no_default_raises(self):
        """Lines 667-671: switch without isDefault branch."""
        fb = self._fb_with_nodes(
            start(id="start", next="sw"),
            switch("sw", branches=[
                branch("b1", next="end_node",
                       condition=cond("$INPUT.x", "==", 1)),
                # no is_default branch
            ]),
            end("end_node"),
        )
        with self.assertRaises(ValueError) as ctx:
            fb.validate()
        self.assertIn("isDefault", str(ctx.exception))


# ---------------------------------------------------------------------------
# LinearBuilder methods (lines 884-1097)
# ---------------------------------------------------------------------------

class TestLinearBuilderMethods(unittest.TestCase):
    def test_case_method(self):
        """Line 884-887: LinearBuilder.case."""
        flow = (
            linear("test_case", input_type="object")
            .start()
            .case("$INPUT.n", [{"name": "case1", "value": 1, "next": "e1"},
                               {"name": "case2", "value": 2, "next": "e2"}],
                  default="e1")
            .end("e1", output="one")
            .end("e2", output="two")
            .build()
        )
        self.assertEqual(flow.run(n=1), "one")
        self.assertEqual(flow.run(n=2), "two")
        self.assertEqual(flow.run(n=99), "one")  # default

    def test_loop_method(self):
        """Line 910-911: LinearBuilder.loop."""
        flow = (
            linear("test_loop", input_type="object")
            .start()
            .loop("$INPUT.items", _echo_item_child(), id="lp")
            .end(output="$NODE.lp")
            .build()
        )
        result = flow.run(items=[1, 2, 3])
        self.assertEqual(result, 3)  # last item

    def test_filter_method(self):
        """Line 933: LinearBuilder.filter."""
        flow = (
            linear("test_filter", input_type="object")
            .start()
            .filter("$INPUT.nums", _gt2_child(), id="ft")
            .end(output="$NODE.ft")
            .build()
        )
        result = flow.run(nums=[1, 2, 3, 4])
        self.assertEqual(result, [3, 4])

    def test_find_method(self):
        """Line 942: LinearBuilder.find."""
        flow = (
            linear("test_find", input_type="object")
            .start()
            .find("$INPUT.nums", _gt2_child(), id="fd")
            .end(output="$NODE.fd")
            .build()
        )
        result = flow.run(nums=[1, 2, 3, 4])
        self.assertEqual(result, 3)  # first item > 2

    def test_reduce_method(self):
        """Line 952-953: LinearBuilder.reduce."""
        # reduce child receives {"first": acc, "second": item}; output second each time
        reduce_child = (
            FlowBuilder(input_type="object")
            .add(start(id="s", next="e"))
            .add(end("e", output="$INPUT.second"))
        )
        flow = (
            linear("test_reduce", input_type="object")
            .start()
            .reduce("$INPUT.nums", reduce_child, id="rd", initial=0)
            .end(output="$NODE.rd")
            .build()
        )
        result = flow.run(nums=[1, 2, 3])
        self.assertEqual(result, 3)  # last item becomes final accumulator

    def test_child_method(self):
        """Line 962-964: LinearBuilder.child."""
        inner = (
            FlowBuilder(input_type="any")
            .add(start(id="s", next="e"))
            .add(end("e", output="$INPUT"))
        )
        flow = (
            linear("test_child", input_type="object")
            .start()
            .child("$INPUT.val", inner, id="ch")
            .end(output="$NODE.ch")
            .build()
        )
        result = flow.run(val=42)
        self.assertEqual(result, 42)

    def test_parallel_method(self):
        """Line 986-989: LinearBuilder.parallel with join_branches."""
        b1_flow = FlowBuilder().add(start(id="s", next="e")).add(end("e", output=1))
        b2_flow = FlowBuilder().add(start(id="s", next="e")).add(end("e", output=2))
        flow = (
            linear("test_par", input_type="object")
            .start()
            .parallel(
                [parallel_branch("b1", b1_flow), parallel_branch("b2", b2_flow)],
                id="par", join_branches=["b1", "b2"], mode="thread",
            )
            .end(output="$NODE.par")
            .build()
        )
        result = flow.run()
        self.assertIn("b1", result)
        self.assertIn("b2", result)

    def test_validate_and_to_dict(self):
        """Lines 1072-1073, 1075-1077: LinearBuilder.validate and to_dict."""
        lb = (
            linear("test_validate", input_type="object")
            .start()
            .end(output="done")
        )
        lb.validate()  # should not raise
        d = lb.to_dict()
        self.assertEqual(d["flow_id"], "test_validate")

    def test_to_json(self):
        """Lines 1080-1081: LinearBuilder.to_json."""
        lb = (
            linear("test_json", input_type="object")
            .start()
            .end(output="x")
        )
        j = lb.to_json()
        self.assertIn("test_json", j)

    def test_run(self):
        """Line 1088: LinearBuilder.run."""
        lb = (
            linear("test_run", input_type="object")
            .start()
            .end(output="$INPUT.value")
        )
        result = lb.run(value=99)
        self.assertEqual(result, 99)

    def test_arun(self):
        """Line 1091: LinearBuilder.arun."""
        async def _run():
            lb = (
                linear("test_arun", input_type="object")
                .start()
                .end(output="$INPUT.v")
            )
            return await lb.arun(v=77)

        result = asyncio.run(_run())
        self.assertEqual(result, 77)

    def test_context_manager(self):
        """Lines 1094, 1097: LinearBuilder __enter__/__exit__."""
        with linear("ctx_test", input_type="object").start().end(output="ok") as lb:
            result = lb.run()
        self.assertEqual(result, "ok")

    def test_linear_add_method(self):
        """Lines 1036-1044: LinearBuilder.add with explicit id."""
        lb = linear("test_add", input_type="object")
        lb.add(NodeSpec(start(id="s", next="e")))
        lb.add(NodeSpec(end("e", output="done")))
        result = lb.run()
        self.assertEqual(result, "done")

    def test_linear_add_non_spec_raises(self):
        """Line 1037: add non-NodeSpec raises TypeError."""
        lb = linear("test_add_err")
        with self.assertRaises(TypeError):
            lb.add("not a spec")  # type: ignore[arg-type]

    def test_linear_add_duplicate_id_raises(self):
        """Line 1042: add node with duplicate id raises ValueError."""
        lb = linear("test_dup", input_type="object")
        lb.add(NodeSpec(start(id="s", next="e")))
        with self.assertRaises(ValueError):
            lb.add(NodeSpec(start(id="s")))  # duplicate id


# ---------------------------------------------------------------------------
# FlowBuilder.__enter__/__exit__
# ---------------------------------------------------------------------------

class TestFlowBuilderContextManager(unittest.TestCase):
    def test_context_manager(self):
        """Lines 695-700: FlowBuilder __enter__/__exit__."""
        with build("test_ctx", input_type="object") as fb:
            fb.add(start(next="e"))
            fb.add(end("e", output="hello"))
        result = fb.run()
        self.assertEqual(result, "hello")


# ---------------------------------------------------------------------------
# child_flow decorator
# ---------------------------------------------------------------------------

class TestChildFlowDecorator(unittest.TestCase):
    def test_child_flow_decorator(self):
        """child_flow decorator creates FlowBuilder."""
        @child_flow(input_type="object")
        def echo_flow(c):
            c.add(start(next="e"))
            c.add(end("e", output="$INPUT.item"))

        self.assertIsInstance(echo_flow, FlowBuilder)

        main_flow = (
            build("test_child_deco", input_type="object")
            .add(start(next="m"))
            .add(map("m", "$INPUT.items", echo_flow, next="end"))
            .add(end("end", output="$NODE.m"))
        )
        result = main_flow.run(items=[10, 20, 30])
        self.assertEqual(result, [10, 20, 30])


if __name__ == "__main__":
    unittest.main()
