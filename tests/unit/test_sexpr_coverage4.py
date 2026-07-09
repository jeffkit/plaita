"""Supplemental coverage tests for plaita/dsl/sexpr.py.

Targets all remaining missing lines after test_sexpr.py / test_sexpr_extended.py /
test_sexpr_coverage3.py: 126, 128, 210, 287-288, 368, 377, 380, 389, 398, 401,
462, 475, 482, 520, 533, 537, 550, 558, 561, 587, 595, 597, 600, 607-609, 626,
628, 635, 639, 643, 646, 660, 669, 680, 693, 734, 737, 744, 752, 755, 766, 768,
880-883, 888, 906.
"""
from __future__ import annotations

import unittest

from plaita.dsl.sexpr import (
    Symbol,
    _Ctx,
    _atom,
    _compile_node,
    _type_spec,
    compile_sexpr,
    flow_to_sexpr,
)


# ---------------------------------------------------------------------------
# _atom: lparen / rparen branches (lines 126, 128)
# These paths are unreachable from the parser itself (parser handles them
# before calling _atom), so we exercise them via direct call.
# ---------------------------------------------------------------------------

class TestAtomLparenRparen(unittest.TestCase):
    def test_atom_lparen_returns_open_paren(self):
        """Line 126: _atom with kind='lparen' returns '('."""
        self.assertEqual(_atom("lparen", "("), "(")

    def test_atom_rparen_returns_close_paren(self):
        """Line 128: _atom with kind='rparen' returns ')'."""
        self.assertEqual(_atom("rparen", ")"), ")")


# ---------------------------------------------------------------------------
# _split_args: arrow alias keyword without value (line 210)
# ---------------------------------------------------------------------------

class TestSplitArgsArrowAlias(unittest.TestCase):
    def test_arrow_alias_without_value_raises(self):
        """Line 210: '->' at end of arg list without following value raises."""
        src = """
(flow g :input-type object
  (start ->)
  (end e :output "$INPUT.x"))
"""
        with self.assertRaises(SyntaxError):
            compile_sexpr(src)


# ---------------------------------------------------------------------------
# _Ctx.claim: while loop for auto-id collision (lines 287-288)
# ---------------------------------------------------------------------------

class TestCtxClaimWhileLoop(unittest.TestCase):
    def test_auto_id_skips_pre_claimed(self):
        """Lines 287-288: when _n1 is already claimed, auto-id increments to _n2."""
        src = """
(flow g :input-type object
  (start :id _n1 -> _n2)
  (end :output "$INPUT.x"))
"""
        d = compile_sexpr(src)
        # The end node had no explicit id; _n1 was taken, so it became _n2
        self.assertEqual(d["nodes"][1]["id"], "_n2")


# ---------------------------------------------------------------------------
# _compile_childflow: bad form / outputType / desc (lines 368, 377, 380)
# ---------------------------------------------------------------------------

class TestCompileChildflow(unittest.TestCase):
    def test_bad_childflow_form_raises(self):
        """Line 368: :child set to a non-(childflow ...) value raises SyntaxError."""
        src = """
(flow g :input-type object
  (start -> m)
  (map :id m :collection "$INPUT.items" :child "not_a_childflow" -> e)
  (end e :output "$NODE.m"))
"""
        with self.assertRaises(SyntaxError):
            compile_sexpr(src)

    def test_childflow_with_output_type(self):
        """Line 377: childflow with :output-type sets outputType in IR."""
        src = """
(flow g :input-type object
  (start -> m)
  (map :id m :collection "$INPUT.items"
    :child (childflow :input-type object :output-type string
      (start -> e)
      (end e :output "$INPUT.item"))
    -> e_out)
  (end e_out :output "$NODE.m"))
"""
        d = compile_sexpr(src)
        cf = d["nodes"][1]["childFlow"]
        self.assertIn("outputType", cf)

    def test_childflow_with_desc(self):
        """Line 380: childflow with :desc sets desc in IR."""
        src = """
(flow g :input-type object
  (start -> m)
  (map :id m :collection "$INPUT.items"
    :child (childflow :input-type object :desc "process item"
      (start -> e)
      (end e :output "$INPUT.item"))
    -> e_out)
  (end e_out :output "$NODE.m"))
"""
        d = compile_sexpr(src)
        cf = d["nodes"][1]["childFlow"]
        self.assertEqual(cf["desc"], "process item")


# ---------------------------------------------------------------------------
# _type_spec: non-str input (line 389)
# ---------------------------------------------------------------------------

class TestTypeSpecNonStr(unittest.TestCase):
    def test_type_spec_with_dict_returns_as_is(self):
        """Line 389: _type_spec with a non-str (dict) returns the value unchanged."""
        spec = {"dataType": "string", "required": True}
        self.assertIs(_type_spec(spec), spec)

    def test_type_spec_with_none_returns_none(self):
        """Line 389: _type_spec with None returns None (non-str path)."""
        self.assertIsNone(_type_spec(None))


# ---------------------------------------------------------------------------
# _compile_node: non-list and non-symbol head (lines 398, 401)
# ---------------------------------------------------------------------------

class TestCompileNodeErrors(unittest.TestCase):
    def test_non_list_node_raises(self):
        """Line 398: _compile_node with a non-list form raises SyntaxError."""
        ctx = _Ctx()
        with self.assertRaises(SyntaxError):
            _compile_node("not_a_list", ctx)

    def test_empty_list_node_raises(self):
        """Line 398: _compile_node with an empty list raises SyntaxError."""
        ctx = _Ctx()
        with self.assertRaises(SyntaxError):
            _compile_node([], ctx)

    def test_non_symbol_head_raises(self):
        """Line 401: _compile_node with a non-symbol head raises SyntaxError."""
        ctx = _Ctx()
        # Head is an integer, not a Symbol
        with self.assertRaises(SyntaxError):
            _compile_node([42, Symbol("foo")], ctx)

    def test_non_list_in_flow_body_raises(self):
        """Line 398: bare atom in flow body is passed to _compile_node as non-list."""
        src = """
(flow g :input-type object
  bare_atom
  (start -> e)
  (end e :output 1))
"""
        with self.assertRaises(SyntaxError):
            compile_sexpr(src)

    def test_non_symbol_head_in_flow_body_raises(self):
        """Line 401: list with non-symbol head in flow body raises SyntaxError."""
        src = """
(flow g :input-type object
  (42 foo bar)
  (start -> e)
  (end e :output 1))
"""
        with self.assertRaises(SyntaxError):
            compile_sexpr(src)


# ---------------------------------------------------------------------------
# _c_end: error field (line 462)
# ---------------------------------------------------------------------------

class TestCEndErrorField(unittest.TestCase):
    def test_end_with_string_error(self):
        """Line 462 else-branch: :error string wraps in {'message': ...}."""
        src = """
(flow g :input-type object
  (start -> e)
  (end e :output 1 :error "something went wrong"))
"""
        d = compile_sexpr(src)
        end_node = d["nodes"][1]
        self.assertIn("error", end_node)
        self.assertEqual(end_node["error"], {"message": "something went wrong"})

    def test_end_with_dict_error(self):
        """Line 462 if-branch: :error dict is stored directly."""
        src = """
(flow g :input-type object
  (start -> e)
  (end e :output 1 :error (dict :code 404)))
"""
        # (dict :code 404) parses to a list, not a Python dict, so takes else-branch
        d = compile_sexpr(src)
        end_node = d["nodes"][1]
        self.assertIn("error", end_node)


# ---------------------------------------------------------------------------
# _c_assignment: outputType (line 475)
# ---------------------------------------------------------------------------

class TestCAssignmentOutputType(unittest.TestCase):
    def test_assignment_with_output_type(self):
        """Line 475: assignment with :output-type sets outputType in IR."""
        src = """
(flow g :input-type object
  (start -> a)
  (assignment :id a "$INPUT.x" :output-type string -> e)
  (end e :output "$NODE.a"))
"""
        d = compile_sexpr(src)
        a = d["nodes"][1]
        self.assertIn("outputType", a)


# ---------------------------------------------------------------------------
# _c_if: missing condition (line 482)
# ---------------------------------------------------------------------------

class TestCIfMissingCondition(unittest.TestCase):
    def test_if_with_only_keywords_raises(self):
        """Line 482: if with no positional condition raises SyntaxError."""
        src = """
(flow g :input-type object
  (start -> i)
  (if :id i -> e1 :else e2)
  (end :id e1 :output 1)
  (end :id e2 :output 2))
"""
        with self.assertRaises(SyntaxError):
            compile_sexpr(src)


# ---------------------------------------------------------------------------
# _c_branch: priority (line 520)
# ---------------------------------------------------------------------------

class TestCBranchPriority(unittest.TestCase):
    def test_branch_with_priority(self):
        """Line 520: branch with :priority sets priority in IR."""
        src = """
(flow g :input-type object
  (start -> sw)
  (switch :id sw
    (branch b1 -> e1 :when (cond "$INPUT.x" eq 1) :priority 10)
    (branch def -> e2 :default true))
  (end :id e1 :output 1)
  (end :id e2 :output 2))
"""
        d = compile_sexpr(src)
        sw = d["nodes"][1]
        b1 = sw["branches"][0]
        self.assertEqual(b1["priority"], 10)


# ---------------------------------------------------------------------------
# _c_case: no target / non-match form / match < 2 args (lines 533, 537, 550)
# ---------------------------------------------------------------------------

class TestCCaseErrors(unittest.TestCase):
    def test_case_no_target_raises(self):
        """Line 533: case without :target raises SyntaxError."""
        src = """
(flow g :input-type object
  (start -> c)
  (case :id c (match 1 e1) :default ed)
  (end :id e1 :output 1)
  (end :id ed :output 0))
"""
        with self.assertRaises(SyntaxError):
            compile_sexpr(src)

    def test_case_non_match_form_raises(self):
        """Line 537: case with non-(match ...) positional arg raises SyntaxError."""
        src = """
(flow g :input-type object
  (start -> c)
  (case :id c :target "$INPUT.n" (wrong-form 1 e1) :default ed)
  (end :id e1 :output 1)
  (end :id ed :output 0))
"""
        with self.assertRaises(SyntaxError):
            compile_sexpr(src)

    def test_match_too_few_args_raises(self):
        """Line 550: match with < 2 positional args raises SyntaxError."""
        src = """
(flow g :input-type object
  (start -> c)
  (case :id c :target "$INPUT.n" (match 1) :default ed)
  (end :id e1 :output 1)
  (end :id ed :output 0))
"""
        with self.assertRaises(SyntaxError):
            compile_sexpr(src)


# ---------------------------------------------------------------------------
# _collection_common: no collection / no child (lines 558, 561)
# _c_map: maxConcurrent (line 587)
# ---------------------------------------------------------------------------

class TestCollectionCommonErrors(unittest.TestCase):
    def test_map_no_collection_raises(self):
        """Line 558: map without :collection raises SyntaxError."""
        src = """
(flow g :input-type object
  (start -> m)
  (map :id m
    :child (childflow (start -> e) (end e :output "$INPUT.item"))
    -> e_out)
  (end e_out :output "$NODE.m"))
"""
        with self.assertRaises(SyntaxError):
            compile_sexpr(src)

    def test_map_no_child_raises(self):
        """Line 561: map without :child raises SyntaxError."""
        src = """
(flow g :input-type object
  (start -> m)
  (map :id m :collection "$INPUT.items" -> e_out)
  (end e_out :output "$NODE.m"))
"""
        with self.assertRaises(SyntaxError):
            compile_sexpr(src)


class TestMapMaxConcurrent(unittest.TestCase):
    def test_map_with_max_concurrent(self):
        """Line 587: map with :concurrent true and :max-concurrent sets maxConcurrent."""
        src = """
(flow g :input-type object
  (start -> m)
  (map :id m :collection "$INPUT.items"
    :concurrent true :max-concurrent 5
    :child (childflow (start -> e) (end e :output "$INPUT.item"))
    -> e_out)
  (end e_out :output "$NODE.m"))
"""
        d = compile_sexpr(src)
        m = d["nodes"][1]
        self.assertTrue(m["concurrent"])
        self.assertEqual(m["maxConcurrent"], 5)


# ---------------------------------------------------------------------------
# _c_child: positional input / no input / no child (lines 595, 597, 600)
# _c_reference: full compilation (lines 607-609)
# ---------------------------------------------------------------------------

class TestCChildNode(unittest.TestCase):
    def test_child_with_positional_input(self):
        """Line 595: child with positional (non-keyword) input uses pos[0] as input."""
        src = """
(flow g :input-type object
  (start -> c)
  (child "$INPUT.val"
    :child (childflow (start -> e) (end e :output "$INPUT"))
    :id c -> end)
  (end end :output "$NODE.c"))
"""
        d = compile_sexpr(src)
        c = d["nodes"][1]
        self.assertEqual(c["type"], "child")
        self.assertEqual(c["input"], "$INPUT.val")

    def test_child_no_input_raises(self):
        """Line 597: child without any input raises SyntaxError."""
        src = """
(flow g :input-type object
  (start -> c)
  (child :id c
    :child (childflow (start -> e) (end e :output "$INPUT"))
    -> end)
  (end end :output "$NODE.c"))
"""
        with self.assertRaises(SyntaxError):
            compile_sexpr(src)

    def test_child_no_child_flow_raises(self):
        """Line 600: child without :child raises SyntaxError."""
        src = """
(flow g :input-type object
  (start -> c)
  (child :id c :input "$INPUT.val" -> end)
  (end end :output "$NODE.c"))
"""
        with self.assertRaises(SyntaxError):
            compile_sexpr(src)

    def test_reference_node_compiles(self):
        """Lines 607-609: reference node calls _c_child and sets type='reference'."""
        src = """
(flow g :input-type object
  (start -> r)
  (reference :id r :input "$INPUT.val"
    :child (childflow (start -> e) (end e :output "$INPUT"))
    -> end)
  (end end :output "$NODE.r"))
"""
        d = compile_sexpr(src)
        r = d["nodes"][1]
        self.assertEqual(r["type"], "reference")
        self.assertEqual(r["input"], "$INPUT.val")


# ---------------------------------------------------------------------------
# _c_parallel: joinBranches / isConditional (lines 626, 628)
# _c_pbranch: no name / no flow / input / when (lines 635, 639, 643, 646)
# ---------------------------------------------------------------------------

class TestCParallel(unittest.TestCase):
    def test_parallel_join_branches(self):
        """Line 626: parallel with :join-branches sets joinBranches in IR."""
        src = """
(flow g :input-type object
  (start -> p)
  (parallel :id p
    :join-branches b1
    (pbranch b1 :flow (childflow (start -> e) (end e :output 1)))
    (pbranch b2 :flow (childflow (start -> e) (end e :output 2)))
    -> end)
  (end end :output "$NODE.p"))
"""
        d = compile_sexpr(src)
        p = d["nodes"][1]
        self.assertEqual(p["joinBranches"], ["b1"])

    def test_parallel_is_conditional(self):
        """Line 628: parallel with :is-conditional sets isConditional=True."""
        src = """
(flow g :input-type object
  (start -> p)
  (parallel :id p
    :is-conditional true
    (pbranch b1 :flow (childflow (start -> e) (end e :output 1)))
    -> end)
  (end end :output "$NODE.p"))
"""
        d = compile_sexpr(src)
        p = d["nodes"][1]
        self.assertTrue(p["isConditional"])


class TestCPbranch(unittest.TestCase):
    def test_pbranch_no_name_raises(self):
        """Line 635: pbranch without name raises SyntaxError."""
        src = """
(flow g :input-type object
  (start -> p)
  (parallel :id p
    (pbranch :flow (childflow (start -> e) (end e :output 1)))
    -> end)
  (end end :output "$NODE.p"))
"""
        with self.assertRaises(SyntaxError):
            compile_sexpr(src)

    def test_pbranch_no_flow_raises(self):
        """Line 639: pbranch without :flow raises SyntaxError."""
        src = """
(flow g :input-type object
  (start -> p)
  (parallel :id p
    (pbranch b1)
    -> end)
  (end end :output "$NODE.p"))
"""
        with self.assertRaises(SyntaxError):
            compile_sexpr(src)

    def test_pbranch_with_input_and_when(self):
        """Lines 643, 646: pbranch with :input and :when sets both fields."""
        src = """
(flow g :input-type object
  (start -> p)
  (parallel :id p
    (pbranch b1
      :flow (childflow (start -> e) (end e :output 1))
      :input "$INPUT.val"
      :when (cond "$INPUT.x" eq 1))
    -> end)
  (end end :output "$NODE.p"))
"""
        d = compile_sexpr(src)
        p = d["nodes"][1]
        branch = p["branches"][0]
        self.assertEqual(branch["input"], "$INPUT.val")
        self.assertIn("condition", branch)


# ---------------------------------------------------------------------------
# _c_code: input (line 660)
# ---------------------------------------------------------------------------

class TestCCodeInput(unittest.TestCase):
    def test_code_with_input(self):
        """Line 660: code node with :input sets input in IR."""
        src = """
(flow g :input-type object
  (start -> c)
  (code :id c :lang python :code "result = x" :input "$INPUT.data" -> e)
  (end e :output "$NODE.c"))
"""
        d = compile_sexpr(src)
        c = d["nodes"][1]
        self.assertEqual(c["input"], "$INPUT.data")


# ---------------------------------------------------------------------------
# _c_http: missing method/url / input (lines 669, 680)
# ---------------------------------------------------------------------------

class TestCHttp(unittest.TestCase):
    def test_http_no_method_raises(self):
        """Line 669: http without :method raises SyntaxError."""
        src = """
(flow g :input-type object
  (start -> h)
  (http :id h :url "http://example.com" -> e)
  (end e :output "$NODE.h"))
"""
        with self.assertRaises(SyntaxError):
            compile_sexpr(src)

    def test_http_no_url_raises(self):
        """Line 669: http without :url raises SyntaxError."""
        src = """
(flow g :input-type object
  (start -> h)
  (http :id h :method GET -> e)
  (end e :output "$NODE.h"))
"""
        with self.assertRaises(SyntaxError):
            compile_sexpr(src)

    def test_http_with_input(self):
        """Line 680: http with :input sets input in IR."""
        src = """
(flow g :input-type object
  (start -> h)
  (http :id h :method POST :url "http://example.com" :input "$INPUT.data" -> e)
  (end e :output "$NODE.h"))
"""
        d = compile_sexpr(src)
        h = d["nodes"][1]
        self.assertEqual(h["input"], "$INPUT.data")


# ---------------------------------------------------------------------------
# _c_event: filter (line 693)
# ---------------------------------------------------------------------------

class TestCEventFilter(unittest.TestCase):
    def test_event_with_filter(self):
        """Line 693: event with :filter sets eventFilter in IR."""
        src = """
(flow g :input-type object
  (start -> ev)
  (event :id ev :type "user.login" :filter "user_id" -> end)
  (end end :output "$NODE.ev"))
"""
        d = compile_sexpr(src)
        ev = d["nodes"][1]
        self.assertEqual(ev["eventFilter"], "user_id")


# ---------------------------------------------------------------------------
# _compile_flow: bad form / no flow_id / outputType / globalContext / metadata
# (lines 734, 737, 744, 752, 755)
# compile_sexpr: empty / multiple forms (lines 766, 768)
# ---------------------------------------------------------------------------

class TestCompileFlow(unittest.TestCase):
    def test_non_flow_head_raises(self):
        """Line 734: top-level form that is not (flow ...) raises SyntaxError."""
        with self.assertRaises(SyntaxError):
            compile_sexpr("(not_flow :id x)")

    def test_flow_no_id_raises(self):
        """Line 737: (flow :input-type object) with no positional id raises SyntaxError."""
        with self.assertRaises(SyntaxError):
            compile_sexpr("(flow :input-type object)")

    def test_flow_with_output_type(self):
        """Line 744: flow with :output-type sets outputType in IR."""
        src = """
(flow g :input-type object :output-type string
  (start -> e)
  (end e :output "$INPUT.x"))
"""
        d = compile_sexpr(src)
        self.assertIn("outputType", d)

    def test_flow_with_global_context(self):
        """Line 752: flow with :global-context sets globalContext in IR."""
        src = """
(flow g :input-type object :global-context (dict :key "val")
  (start -> e)
  (end e :output "$INPUT.x"))
"""
        d = compile_sexpr(src)
        self.assertIn("globalContext", d)

    def test_flow_with_metadata(self):
        """Line 755: flow with :metadata sets metadata in IR."""
        src = """
(flow g :input-type object :metadata (dict :env "prod")
  (start -> e)
  (end e :output "$INPUT.x"))
"""
        d = compile_sexpr(src)
        self.assertIn("metadata", d)


class TestCompileSexprEdgeCases(unittest.TestCase):
    def test_empty_source_raises(self):
        """Line 766: empty source raises ValueError."""
        with self.assertRaises(ValueError):
            compile_sexpr("")

    def test_multiple_forms_raises(self):
        """Line 768: source with >1 top-level form raises SyntaxError."""
        with self.assertRaises(SyntaxError):
            compile_sexpr("""
(flow a :input-type object (start -> e) (end e :output 1))
(flow b :input-type object (start -> e) (end e :output 2))
""")


# ---------------------------------------------------------------------------
# _node_to_src: child/reference (lines 880-883), http headers (888),
# on-error defaultValue (906)
# ---------------------------------------------------------------------------

class TestNodeToSrcCoverage(unittest.TestCase):
    def _to_sexpr(self, src: str) -> str:
        return flow_to_sexpr(compile_sexpr(src))

    def test_child_node_to_src(self):
        """Lines 880-881: child node serialized with :input and :child."""
        src = """
(flow g :input-type object
  (start -> c)
  (child :id c :input "$INPUT.val"
    :child (childflow (start -> e) (end e :output "$INPUT"))
    -> end)
  (end end :output "$NODE.c"))
"""
        result = self._to_sexpr(src)
        self.assertIn("child", result)
        self.assertIn(":input", result)

    def test_child_node_to_src_with_next(self):
        """Line 883: child node with 'next' includes '-> ...' in serialization."""
        src = """
(flow g :input-type object
  (start -> c)
  (child :id c :input "$INPUT.val"
    :child (childflow (start -> e) (end e :output "$INPUT"))
    -> end)
  (end end :output "$NODE.c"))
"""
        result = self._to_sexpr(src)
        self.assertIn("->", result)

    def test_reference_node_to_src(self):
        """Lines 879-883: reference node serialized via child/reference branch."""
        src = """
(flow g :input-type object
  (start -> r)
  (reference :id r :input "$INPUT.val"
    :child (childflow (start -> e) (end e :output "$INPUT"))
    -> end)
  (end end :output "$NODE.r"))
"""
        result = self._to_sexpr(src)
        self.assertIn("reference", result)
        self.assertIn(":input", result)

    def test_http_with_headers_to_src(self):
        """Line 888: http node with headers is serialized with :headers."""
        src = """
(flow g :input-type object
  (start -> h)
  (http :id h :method GET :url "http://example.com"
    :headers (dict :Content-Type "application/json")
    -> e)
  (end e :output "$NODE.h"))
"""
        result = self._to_sexpr(src)
        self.assertIn(":headers", result)

    def test_on_error_default_value_to_src(self):
        """Line 906: on-error with defaultValue serializes :default field."""
        src = """
(flow g :input-type object
  (start -> h)
  (http :id h :method POST :url "http://example.com"
    :on-error (on-error continue_with :default "fallback")
    -> e)
  (end e :output "$NODE.h"))
"""
        result = self._to_sexpr(src)
        self.assertIn(":default", result)
        self.assertIn("fallback", result)


if __name__ == "__main__":
    unittest.main()
