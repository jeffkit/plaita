"""Extended tests for plaita.dsl.sexpr — covers uncovered branches.

Targets the following gap areas identified from coverage:
- Parser: unrecognized char, float atoms, unbalanced parens
- _value: list literal compilation
- _compile_dict_literal: tuple-style entries and errors
- _Ctx.claim: duplicate id error
- _compile_condition: or/not variants and error cases
- Node compilers: assignment, loop+condition, map concurrent,
  child, reference, parallel, code, event, reduce+initial
- flow_to_sexpr: reverse-direction serialization for all node types
"""

from __future__ import annotations

import unittest

from plaita.dsl.sexpr import compile_sexpr, parse_sexpr, flow_to_sexpr, read_forms


# ---------------------------------------------------------------------------
# Lexer/parser edge cases
# ---------------------------------------------------------------------------

class TestParserEdgeCases(unittest.TestCase):
    def test_unbalanced_open_paren_mid_form_raises(self):
        """Line 167: unclosed inner paren inside outer form raises SyntaxError."""
        with self.assertRaises(SyntaxError):
            read_forms("(flow x (start")

    def test_float_atom(self):
        """Line 140: float token is converted to Python float."""
        src = """
        (flow g :input-type object
          (start -> e)
          (end e :output 3.14))
        """
        d = compile_sexpr(src)
        end_node = d["nodes"][-1]
        self.assertAlmostEqual(end_node["output"], 3.14)

    def test_unbalanced_open_paren_raises(self):
        """Line 167: unclosed '(' raises SyntaxError."""
        with self.assertRaises(SyntaxError):
            read_forms("(flow x")

    def test_extra_close_paren_raises(self):
        """Line 170: extra ')' raises SyntaxError."""
        with self.assertRaises(SyntaxError):
            read_forms("(flow x) )")


# ---------------------------------------------------------------------------
# _value: list literal
# ---------------------------------------------------------------------------

class TestValueList(unittest.TestCase):
    def test_list_literal(self):
        """Lines 241-242: (list 1 2 "$INPUT.x") compiles to Python list."""
        src = """
        (flow g :input-type object
          (start -> e)
          (end e :output (list 1 2 "$INPUT.x")))
        """
        d = compile_sexpr(src)
        self.assertEqual(d["nodes"][-1]["output"], [1, 2, "$INPUT.x"])

    def test_dict_literal_keyword_style(self):
        """Lines 254-256: (dict :key val) compiles to Python dict."""
        src = """
        (flow g :input-type object
          (start -> h)
          (http :id h :method GET :url "http://x.com"
            :headers (dict :Content-Type "application/json")
            -> e)
          (end e :output "$NODE.h"))
        """
        d = compile_sexpr(src)
        h = d["nodes"][1]
        self.assertEqual(h["headers"], {"Content-Type": "application/json"})

    def test_dict_literal_tuple_style(self):
        """Lines 258-260: (dict ("key" val)) compiles to Python dict."""
        src = """
        (flow g :input-type object
          (start -> h)
          (http :id h :method GET :url "http://x.com"
            :headers (dict ("X-Custom" "val"))
            -> e)
          (end e :output "$NODE.h"))
        """
        d = compile_sexpr(src)
        h = d["nodes"][1]
        self.assertEqual(h["headers"], {"X-Custom": "val"})

    def test_dict_literal_invalid_entry_raises(self):
        """Line 262: invalid dict entry raises SyntaxError."""
        src = """
        (flow g :input-type object
          (start -> h)
          (http :id h :method GET :url "http://x.com"
            :headers (dict invalid_bare_sym)
            -> e)
          (end e :output "$NODE.h"))
        """
        with self.assertRaises(SyntaxError):
            compile_sexpr(src)


# ---------------------------------------------------------------------------
# _Ctx.claim: duplicate id
# ---------------------------------------------------------------------------

class TestCtxDuplicateId(unittest.TestCase):
    def test_duplicate_node_id_raises(self):
        """Lines 282: _Ctx.claim raises ValueError on duplicate id."""
        src = """
        (flow g :input-type object
          (start :id dup -> e)
          (end :id dup :output "x"))
        """
        with self.assertRaises((SyntaxError, ValueError)):
            compile_sexpr(src)


# ---------------------------------------------------------------------------
# Condition edge cases
# ---------------------------------------------------------------------------

class TestConditionEdgeCases(unittest.TestCase):
    def test_or_condition(self):
        """Line 315: (or ...) compiles to relation=or."""
        src = """
        (flow g :input-type object
          (start -> c)
          (if :id c (or (cond "$INPUT.x" > 10) (cond "$INPUT.x" < 0))
            -> big :else mid)
          (end big :output "extreme")
          (end mid :output "normal"))
        """
        f = parse_sexpr(src)
        self.assertEqual(f.run(x=15), "extreme")
        self.assertEqual(f.run(x=-5), "extreme")
        self.assertEqual(f.run(x=5), "normal")

    def test_not_of_and_becomes_or(self):
        """Lines 325-327: (not (and ...)) = De Morgan → or."""
        src = """
        (flow g :input-type object
          (start -> c)
          (if :id c (not (and (cond "$INPUT.a" >= 1) (cond "$INPUT.b" >= 1)))
            -> fail :else ok)
          (end fail :output "fail")
          (end ok :output "ok"))
        """
        f = parse_sexpr(src)
        self.assertEqual(f.run(a=0, b=1), "fail")  # not(a>=1 and b>=1)
        self.assertEqual(f.run(a=1, b=1), "ok")

    def test_not_of_or_becomes_and(self):
        """Lines 328-332: (not (or ...)) = De Morgan → and."""
        src = """
        (flow g :input-type object
          (start -> c)
          (if :id c (not (or (cond "$INPUT.a" == 1) (cond "$INPUT.b" == 1)))
            -> neither :else has_one)
          (end neither :output "neither")
          (end has_one :output "has_one"))
        """
        f = parse_sexpr(src)
        self.assertEqual(f.run(a=0, b=0), "neither")
        self.assertEqual(f.run(a=1, b=0), "has_one")

    def test_condition_non_symbol_head_raises(self):
        """Line 303: non-symbol head in condition form raises SyntaxError."""
        from plaita.dsl.sexpr import _compile_condition
        with self.assertRaises(SyntaxError):
            _compile_condition(["cond", "$INPUT.x", ">", 1])  # valid
        # Non-symbol head (not a Symbol):
        from plaita.dsl.sexpr import Symbol
        with self.assertRaises(SyntaxError):
            _compile_condition([42, "$INPUT.x", ">", 1])

    def test_cond_wrong_arg_count_raises(self):
        """Line 308: cond with wrong number of args raises SyntaxError."""
        from plaita.dsl.sexpr import _compile_condition, Symbol
        with self.assertRaises(SyntaxError):
            _compile_condition([Symbol("cond"), "$INPUT.x"])  # missing op and value

    def test_not_multi_args_raises(self):
        """Line 318: (not cond1 cond2) with >1 arg raises SyntaxError."""
        from plaita.dsl.sexpr import _compile_condition, Symbol
        with self.assertRaises(SyntaxError):
            _compile_condition([Symbol("not"),
                                [Symbol("cond"), "$INPUT.x", Symbol(">"), 1],
                                [Symbol("cond"), "$INPUT.y", Symbol(">"), 1]])

    def test_not_unknown_operator_raises(self):
        """Line 323: (not (cond f custom_op v)) raises if operator can't be negated."""
        from plaita.dsl.sexpr import _compile_condition, Symbol
        with self.assertRaises(SyntaxError):
            _compile_condition([Symbol("not"),
                                [Symbol("cond"), Symbol("$INPUT.x"),
                                 Symbol("custom_op"), 1]])

    def test_not_condition_unknown_structure_raises(self):
        """_negate_condition raises for a compiled dict with no operator/relation."""
        from plaita.dsl.sexpr import _negate_condition
        with self.assertRaises(SyntaxError):
            _negate_condition({"unknown_key": "value"})


# ---------------------------------------------------------------------------
# Node type compilations
# ---------------------------------------------------------------------------

class TestAssignmentNode(unittest.TestCase):
    def test_assignment_compiles_and_runs(self):
        """Lines 465-474: assignment node."""
        src = """
        (flow g :input-type object
          (start -> a)
          (assign :id a "$F.mul($INPUT.x, 2)" -> e)
          (end e :output "$NODE.a"))
        """
        f = parse_sexpr(src)
        self.assertEqual(f.run(x=5), 10)


class TestLoopWithCondition(unittest.TestCase):
    def test_loop_with_when_condition(self):
        """Lines 570-574: loop node with :when condition."""
        src = """
        (flow g :input-type object
          (start -> lp)
          (loop :id lp :collection "$INPUT.items"
            :child (childflow :input-type object
              (start -> e)
              (end e :output "$F.mul($INPUT.item, 2)"))
            :when (cond "$LOOP-RESULT" < 8)
            -> end)
          (end end :output "$NODE.lp"))
        """
        f = parse_sexpr(src)
        result = f.run(items=[1, 2, 3, 4, 5])
        self.assertEqual(result, 8)  # stops at item=4 where result=8 (not < 8)


class TestMapConcurrent(unittest.TestCase):
    def test_map_concurrent(self):
        """Lines 582-585: concurrent map."""
        src = """
        (flow g :input-type object
          (start -> m)
          (map :id m :collection "$INPUT.nums"
            :concurrent true
            :child (childflow :input-type object
              (start -> e)
              (end e :output "$F.mul($INPUT.item, 3)"))
            -> end)
          (end end :output "$NODE.m"))
        """
        f = parse_sexpr(src)
        result = sorted(f.run(nums=[1, 2, 3]))
        self.assertEqual(result, [3, 6, 9])


class TestReduceNode(unittest.TestCase):
    def test_reduce_with_initial(self):
        """Lines 695-700: reduce with :initial value."""
        src = """
        (flow g :input-type object
          (start -> r)
          (reduce :id r :collection "$INPUT.nums"
            :initial 100
            :child (childflow :input-type object
              (start -> e)
              (end e :output "$F.add($INPUT.first, $INPUT.second)"))
            -> end)
          (end end :output "$NODE.r"))
        """
        f = parse_sexpr(src)
        self.assertEqual(f.run(nums=[1, 2, 3]), 106)


class TestChildNode(unittest.TestCase):
    def test_child_node_compiles(self):
        """Lines 590-600: child node with explicit input."""
        src = """
        (flow g :input-type object
          (start -> c)
          (child :id c :input "$INPUT.val"
            :child (childflow :input-type any
              (start -> e)
              (end e :output "$INPUT"))
            -> end)
          (end end :output "$NODE.c"))
        """
        d = compile_sexpr(src)
        c = d["nodes"][1]
        self.assertEqual(c["type"], "child")
        self.assertEqual(c["input"], "$INPUT.val")


class TestCodeNode(unittest.TestCase):
    def test_code_node_compiles(self):
        """Lines 648-658: code node compilation."""
        src = """
        (flow g :input-type object
          (start -> c)
          (code :id c :lang python :code "result = $INPUT.x + 1" -> e)
          (end e :output "$NODE.c"))
        """
        d = compile_sexpr(src)
        c = d["nodes"][1]
        self.assertEqual(c["type"], "code")
        self.assertEqual(c["language"], "python")
        self.assertEqual(c["code"], "result = $INPUT.x + 1")

    def test_code_missing_lang_raises(self):
        """Lines 652-653: missing :lang raises SyntaxError."""
        src = """
        (flow g :input-type object
          (start -> c)
          (code :id c :code "x = 1" -> e)
          (end e :output "$NODE.c"))
        """
        with self.assertRaises(SyntaxError):
            compile_sexpr(src)


class TestEventNode(unittest.TestCase):
    def test_event_node_compiles(self):
        """Lines 682-692: event node compilation."""
        src = """
        (flow g :input-type object
          (start -> ev)
          (event :id ev :type "user.login" -> end)
          (end end :output "$NODE.ev"))
        """
        d = compile_sexpr(src)
        ev = d["nodes"][1]
        self.assertEqual(ev["type"], "event")
        self.assertEqual(ev["eventType"], "user.login")

    def test_event_missing_type_raises(self):
        """Line 685-686: missing :type raises SyntaxError."""
        src = """
        (flow g :input-type object
          (start -> ev)
          (event :id ev -> end)
          (end end :output "$NODE.ev"))
        """
        with self.assertRaises(SyntaxError):
            compile_sexpr(src)


class TestParallelNode(unittest.TestCase):
    def test_parallel_node_compiles(self):
        """Lines 610-627: parallel node with pbranches."""
        src = """
        (flow g :input-type object
          (start -> p)
          (parallel :id p
            (pbranch b1 :flow (childflow :input-type object
              (start -> e) (end e :output 1)))
            (pbranch b2 :flow (childflow :input-type object
              (start -> e) (end e :output 2)))
            -> end)
          (end end :output "$NODE.p"))
        """
        d = compile_sexpr(src)
        p = d["nodes"][1]
        self.assertEqual(p["type"], "parallel")
        self.assertEqual(len(p["branches"]), 2)

    def test_parallel_no_pbranch_raises(self):
        """Lines 614-615, 501: parallel with invalid branch raises."""
        src = """
        (flow g :input-type object
          (start -> p)
          (parallel :id p
            (branch b1 -> e)
            -> end)
          (end end :output "$NODE.p"))
        """
        with self.assertRaises(SyntaxError):
            compile_sexpr(src)

    def test_parallel_mode_and_join(self):
        """Lines 619-626: parallel with mode and joinBranches."""
        src = """
        (flow g :input-type object
          (start -> p)
          (parallel :id p
            :mode any
            (pbranch b1 :flow (childflow (start -> e) (end e :output 1)))
            (pbranch b2 :flow (childflow (start -> e) (end e :output 2)))
            -> end)
          (end end :output "$NODE.p"))
        """
        d = compile_sexpr(src)
        p = d["nodes"][1]
        self.assertEqual(p.get("mode"), "any")


# ---------------------------------------------------------------------------
# flow_to_sexpr round-trip for various node types
# ---------------------------------------------------------------------------

class TestFlowToSexprRoundtrip(unittest.TestCase):
    def _roundtrip(self, src: str) -> str:
        """Compile to IR dict, convert to sexpr, re-compile, return sexpr string."""
        d = compile_sexpr(src)
        back = flow_to_sexpr(d)
        # Should be parseable
        re_d = compile_sexpr(back)
        return back

    def test_assignment_roundtrip(self):
        src = """
        (flow g :input-type object
          (start -> a) (assign :id a "$INPUT.x" -> e) (end e :output "$NODE.a"))
        """
        back = self._roundtrip(src)
        self.assertIn("assignment", back)

    def test_if_roundtrip(self):
        src = """
        (flow g :input-type object
          (start -> c)
          (if :id c (cond "$INPUT.n" > 0) -> ok :else no)
          (end ok :output "pos") (end no :output "neg"))
        """
        back = self._roundtrip(src)
        self.assertIn("if", back)

    def test_switch_roundtrip(self):
        src = """
        (flow g :input-type object
          (start -> s)
          (switch :id s
            (branch a -> ea :when (cond "$INPUT.t" == "A"))
            (branch d -> ed :default true))
          (end ea :output "A") (end ed :output "D"))
        """
        back = self._roundtrip(src)
        self.assertIn("switch", back)

    def test_case_roundtrip(self):
        src = """
        (flow g :input-type object
          (start -> c)
          (case :id c :target "$INPUT.n" (match 1 e1) :default ed)
          (end e1 :output "one") (end ed :output "other"))
        """
        back = self._roundtrip(src)
        self.assertIn("case", back)

    def test_map_roundtrip(self):
        src = """
        (flow g :input-type object
          (start -> m)
          (map :id m :collection "$INPUT.items"
            :child (childflow :input-type object (start -> e) (end e :output "$INPUT.item"))
            -> end)
          (end end :output "$NODE.m"))
        """
        back = self._roundtrip(src)
        self.assertIn("map", back)

    def test_http_roundtrip(self):
        src = """
        (flow g :input-type object
          (start -> h)
          (http :id h :method GET :url "http://example.com/api" -> e)
          (end e :output "$NODE.h"))
        """
        back = self._roundtrip(src)
        self.assertIn("http", back)

    def test_code_roundtrip(self):
        src = """
        (flow g :input-type object
          (start -> c)
          (code :id c :lang python :code "result = 42" -> e)
          (end e :output "$NODE.c"))
        """
        back = self._roundtrip(src)
        self.assertIn("code", back)

    def test_flow_to_sexpr_with_desc(self):
        """Line 958-959: flow with :desc in serialization."""
        d = compile_sexpr("""
        (flow g :input-type object :desc "My flow"
          (start -> e) (end e :output 1))
        """)
        back = flow_to_sexpr(d)
        self.assertIn(":desc", back)


# ---------------------------------------------------------------------------
# _expr_to_src edge cases
# ---------------------------------------------------------------------------

class TestExprToSrc(unittest.TestCase):
    def test_empty_string(self):
        """Line 827: empty string → double-quoted."""
        from plaita.dsl.sexpr import _expr_to_src
        self.assertEqual(_expr_to_src(""), '""')

    def test_string_with_spaces_quoted(self):
        """Line 831: string with spaces gets quoted."""
        from plaita.dsl.sexpr import _expr_to_src
        self.assertIn('"hello world"', _expr_to_src("hello world"))

    def test_bool_true(self):
        from plaita.dsl.sexpr import _expr_to_src
        self.assertEqual(_expr_to_src(True), "true")

    def test_bool_false(self):
        from plaita.dsl.sexpr import _expr_to_src
        self.assertEqual(_expr_to_src(False), "false")

    def test_none_to_nil(self):
        from plaita.dsl.sexpr import _expr_to_src
        self.assertEqual(_expr_to_src(None), "nil")

    def test_int_value(self):
        from plaita.dsl.sexpr import _expr_to_src
        self.assertEqual(_expr_to_src(42), "42")

    def test_float_value(self):
        from plaita.dsl.sexpr import _expr_to_src
        self.assertEqual(_expr_to_src(3.14), "3.14")

    def test_list_value(self):
        from plaita.dsl.sexpr import _expr_to_src
        result = _expr_to_src([1, 2, 3])
        self.assertEqual(result, "(1 2 3)")

    def test_dict_value(self):
        from plaita.dsl.sexpr import _expr_to_src
        result = _expr_to_src({"key": "val"})
        self.assertIn("dict", result)
        self.assertIn(":key", result)


if __name__ == "__main__":
    unittest.main()
