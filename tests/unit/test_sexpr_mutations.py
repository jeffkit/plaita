"""Mutation-targeted tests for plaita/dsl/sexpr.py.

Focus on survived mutants after initial mutmut run. Primary targets:
  - _node_to_src (93 survived): flow_to_sexpr output assertions
  - _compile_flow (43 survived): flow metadata fields
  - _compile_error_handler (26 survived): all optional EH fields
  - _c_parallel / _c_pbranch: mode, joinBranches, isConditional
  - _c_if: then/else_next stored correctly
  - _c_event: eventType key
  - _atom: float/keyword parsing
  - _decode_string: escape sequences
  - _compile_condition: all operators, and/or/not
  - _common_fields: timeout, errorHandler on nodes
  - flow_to_sexpr / _flow_inner_to_src: outputType roundtrip
"""
from __future__ import annotations

import unittest

from plaita.dsl.sexpr import (
    _atom,
    _compile_condition,
    _decode_string,
    _negate_condition,
    compile_sexpr,
    flow_to_sexpr,
    Symbol,
    Keyword,
)


# ---------------------------------------------------------------------------
# Helper: compile then decompile to sexpr
# ---------------------------------------------------------------------------

def _roundtrip(src: str) -> str:
    return flow_to_sexpr(compile_sexpr(src))


# ---------------------------------------------------------------------------
# _decode_string: escape sequences
# ---------------------------------------------------------------------------

class TestDecodeString(unittest.TestCase):
    """_decode_string covers lines 147-148. Mutations change body/slice bounds."""

    def test_plain_string_no_escape(self):
        result = _decode_string('"hello world"')
        self.assertEqual(result, "hello world")

    def test_string_with_backslash_n(self):
        result = _decode_string('"hello\\nworld"')
        self.assertEqual(result, "hello\nworld")

    def test_string_with_backslash_t(self):
        result = _decode_string('"a\\tb"')
        self.assertEqual(result, "a\tb")

    def test_string_with_escaped_quote(self):
        result = _decode_string('"say \\"hi\\""')
        self.assertIn("hi", result)

    def test_empty_string(self):
        result = _decode_string('""')
        self.assertEqual(result, "")

    def test_strips_outer_quotes(self):
        result = _decode_string('"abc"')
        self.assertFalse(result.startswith('"'))
        self.assertFalse(result.endswith('"'))


# ---------------------------------------------------------------------------
# _atom: number/keyword/bool/none parsing
# ---------------------------------------------------------------------------

class TestAtomParsing(unittest.TestCase):
    """_atom lines 130-142: parsing of various atom kinds."""

    def test_atom_true_lowercase(self):
        self.assertIs(_atom("atom", "true"), True)

    def test_atom_true_titlecase(self):
        self.assertIs(_atom("atom", "True"), True)

    def test_atom_false_lowercase(self):
        self.assertIs(_atom("atom", "false"), False)

    def test_atom_false_titlecase(self):
        self.assertIs(_atom("atom", "False"), False)

    def test_atom_nil(self):
        self.assertIsNone(_atom("atom", "nil"))

    def test_atom_none(self):
        self.assertIsNone(_atom("atom", "None"))

    def test_atom_null(self):
        self.assertIsNone(_atom("atom", "null"))

    def test_atom_integer_positive(self):
        v = _atom("atom", "42")
        self.assertEqual(v, 42)
        self.assertIsInstance(v, int)

    def test_atom_integer_negative(self):
        v = _atom("atom", "-7")
        self.assertEqual(v, -7)
        self.assertIsInstance(v, int)

    def test_atom_float(self):
        v = _atom("atom", "3.14")
        self.assertAlmostEqual(v, 3.14)
        self.assertIsInstance(v, float)

    def test_atom_float_negative(self):
        v = _atom("atom", "-0.5")
        self.assertAlmostEqual(v, -0.5)
        self.assertIsInstance(v, float)

    def test_atom_keyword(self):
        v = _atom("atom", ":foo")
        self.assertIsInstance(v, Keyword)
        self.assertEqual(str(v), "foo")

    def test_atom_symbol(self):
        v = _atom("atom", "my-node")
        self.assertIsInstance(v, Symbol)
        self.assertEqual(str(v), "my-node")

    def test_atom_string_kind_calls_decode(self):
        v = _atom("string", '"hello"')
        self.assertEqual(v, "hello")

    def test_atom_lparen_returns_open_paren(self):
        self.assertEqual(_atom("lparen", "("), "(")

    def test_atom_rparen_returns_close_paren(self):
        self.assertEqual(_atom("rparen", ")"), ")")


# ---------------------------------------------------------------------------
# _compile_condition: all operators and nested forms
# ---------------------------------------------------------------------------

class TestCompileCondition(unittest.TestCase):
    """_compile_condition tests use Symbol() for form heads (as the parser produces)."""

    def test_cond_eq(self):
        c = _compile_condition([Symbol("cond"), "$INPUT.x", Symbol("eq"), 5])
        self.assertEqual(c["operator"], "eq")
        self.assertEqual(c["field"], "$INPUT.x")
        self.assertEqual(c["value"], 5)

    def test_cond_op_alias_eq_symbol(self):
        c = _compile_condition([Symbol("cond"), "$INPUT.x", Symbol("=="), 5])
        self.assertEqual(c["operator"], "eq")

    def test_cond_op_ne(self):
        c = _compile_condition([Symbol("cond"), "$INPUT.x", Symbol("!="), 3])
        self.assertEqual(c["operator"], "ne")

    def test_cond_op_gt(self):
        c = _compile_condition([Symbol("cond"), "$INPUT.age", Symbol(">"), 18])
        self.assertEqual(c["operator"], "gt")

    def test_cond_op_gte(self):
        c = _compile_condition([Symbol("cond"), "$INPUT.age", Symbol(">="), 18])
        self.assertEqual(c["operator"], "gte")

    def test_cond_op_lt(self):
        c = _compile_condition([Symbol("cond"), "$INPUT.age", Symbol("<"), 18])
        self.assertEqual(c["operator"], "lt")

    def test_cond_op_lte(self):
        c = _compile_condition([Symbol("cond"), "$INPUT.age", Symbol("<="), 18])
        self.assertEqual(c["operator"], "lte")

    def test_cond_op_in(self):
        c = _compile_condition([Symbol("cond"), "$INPUT.x", Symbol("in"), [1, 2]])
        self.assertEqual(c["operator"], "in")

    def test_cond_op_notIn_symbol(self):
        c = _compile_condition([Symbol("cond"), "$INPUT.x", Symbol("notIn"), [1, 2]])
        self.assertEqual(c["operator"], "notIn")

    def test_cond_op_contains(self):
        c = _compile_condition([Symbol("cond"), "$INPUT.x", Symbol("contains"), "hello"])
        self.assertEqual(c["operator"], "contains")

    def test_cond_op_notContains(self):
        c = _compile_condition([Symbol("cond"), "$INPUT.x", Symbol("notContains"), "hello"])
        self.assertEqual(c["operator"], "notContains")

    def test_and_condition(self):
        c = _compile_condition([Symbol("and"),
            [Symbol("cond"), "$INPUT.x", Symbol("eq"), 1],
            [Symbol("cond"), "$INPUT.y", Symbol("eq"), 2]])
        self.assertEqual(c["relation"], "and")
        self.assertEqual(len(c["conditions"]), 2)

    def test_or_condition(self):
        c = _compile_condition([Symbol("or"),
            [Symbol("cond"), "$INPUT.x", Symbol("eq"), 1],
            [Symbol("cond"), "$INPUT.y", Symbol("eq"), 2]])
        self.assertEqual(c["relation"], "or")
        self.assertEqual(len(c["conditions"]), 2)

    def test_not_condition_negates_operator(self):
        c = _compile_condition([Symbol("not"), [Symbol("cond"), "$INPUT.x", Symbol("eq"), 5]])
        self.assertEqual(c["operator"], "ne")

    def test_not_de_morgan_and(self):
        c = _compile_condition([Symbol("not"), [Symbol("and"),
            [Symbol("cond"), "$INPUT.x", Symbol("gt"), 0],
            [Symbol("cond"), "$INPUT.y", Symbol("lt"), 10]]])
        self.assertEqual(c["relation"], "or")

    def test_not_de_morgan_or(self):
        c = _compile_condition([Symbol("not"), [Symbol("or"),
            [Symbol("cond"), "$INPUT.x", Symbol("gt"), 0],
            [Symbol("cond"), "$INPUT.y", Symbol("lt"), 10]]])
        self.assertEqual(c["relation"], "and")

    def test_not_unknowable_op_raises(self):
        with self.assertRaises(SyntaxError):
            _compile_condition([Symbol("not"), [Symbol("cond"), "$INPUT.x", Symbol("custom_op"), [1]]])


# ---------------------------------------------------------------------------
# _negate_condition: operator lookup exact values
# ---------------------------------------------------------------------------

class TestNegateCondition(unittest.TestCase):
    def test_negate_eq(self):
        c = _negate_condition({"field": "x", "operator": "eq", "value": 1})
        self.assertEqual(c["operator"], "ne")

    def test_negate_ne(self):
        c = _negate_condition({"field": "x", "operator": "ne", "value": 1})
        self.assertEqual(c["operator"], "eq")

    def test_negate_gt(self):
        c = _negate_condition({"field": "x", "operator": "gt", "value": 1})
        self.assertEqual(c["operator"], "lte")

    def test_negate_gte(self):
        c = _negate_condition({"field": "x", "operator": "gte", "value": 1})
        self.assertEqual(c["operator"], "lt")

    def test_negate_lt(self):
        c = _negate_condition({"field": "x", "operator": "lt", "value": 1})
        self.assertEqual(c["operator"], "gte")

    def test_negate_lte(self):
        c = _negate_condition({"field": "x", "operator": "lte", "value": 1})
        self.assertEqual(c["operator"], "gt")

    def test_negate_in(self):
        c = _negate_condition({"field": "x", "operator": "in", "value": [1]})
        self.assertEqual(c["operator"], "notIn")

    def test_negate_notIn(self):
        c = _negate_condition({"field": "x", "operator": "notIn", "value": [1]})
        self.assertEqual(c["operator"], "in")

    def test_negate_contains(self):
        c = _negate_condition({"field": "x", "operator": "contains", "value": "x"})
        self.assertEqual(c["operator"], "notContains")

    def test_negate_notContains(self):
        c = _negate_condition({"field": "x", "operator": "notContains", "value": "x"})
        self.assertEqual(c["operator"], "contains")

    def test_negate_unknown_op_raises(self):
        with self.assertRaises(SyntaxError):
            _negate_condition({"field": "x", "operator": "unknown", "value": 1})

    def test_negate_preserves_field_and_value(self):
        c = _negate_condition({"field": "my.field", "operator": "gt", "value": 42})
        self.assertEqual(c["field"], "my.field")
        self.assertEqual(c["value"], 42)


# ---------------------------------------------------------------------------
# _compile_flow: metadata fields (desc, version, author, timeout)
# ---------------------------------------------------------------------------

class TestCompileFlowFields(unittest.TestCase):
    def test_flow_desc_in_ir(self):
        src = """(flow g :desc "check age"
  (start -> e) (end e :output 1))"""
        d = compile_sexpr(src)
        self.assertEqual(d["desc"], "check age")

    def test_flow_version_in_ir(self):
        src = """(flow g :version "1.2.0"
  (start -> e) (end e :output 1))"""
        d = compile_sexpr(src)
        self.assertEqual(d["version"], "1.2.0")

    def test_flow_author_in_ir(self):
        src = """(flow g :author "alice"
  (start -> e) (end e :output 1))"""
        d = compile_sexpr(src)
        self.assertEqual(d["author"], "alice")

    def test_flow_timeout_in_ir(self):
        src = """(flow g :timeout 30
  (start -> e) (end e :output 1))"""
        d = compile_sexpr(src)
        self.assertEqual(d["timeout"], 30)

    def test_flow_id_string_in_ir(self):
        src = """(flow my_flow
  (start -> e) (end e :output 1))"""
        d = compile_sexpr(src)
        self.assertEqual(d["flow_id"], "my_flow")

    def test_runtime_is_python(self):
        src = """(flow g (start -> e) (end e :output 1))"""
        d = compile_sexpr(src)
        self.assertEqual(d["runtime"], "python")

    def test_flow_inputType_dataType(self):
        src = """(flow g :input-type object (start -> e) (end e :output 1))"""
        d = compile_sexpr(src)
        self.assertEqual(d["inputType"]["dataType"], "object")


# ---------------------------------------------------------------------------
# _compile_error_handler: all optional fields
# ---------------------------------------------------------------------------

class TestCompileErrorHandler(unittest.TestCase):
    def _eh_src(self, eh_str: str) -> dict:
        src = f"""(flow g :input-type object
  (start -> h)
  (http :id h :method GET :url "http://a.b" :on-error {eh_str} -> e)
  (end e :output "$NODE.h"))"""
        d = compile_sexpr(src)
        return d["nodes"][1]["errorHandler"]

    def test_strategy_abort(self):
        eh = self._eh_src("(on-error abort)")
        self.assertEqual(eh["strategy"], "abort")

    def test_strategy_continue(self):
        eh = self._eh_src("(on-error continue)")
        self.assertEqual(eh["strategy"], "continue")

    def test_strategy_continue_with(self):
        eh = self._eh_src("(on-error continue_with)")
        self.assertEqual(eh["strategy"], "continue_with")

    def test_retry_times_field(self):
        eh = self._eh_src("(on-error abort :retry 3)")
        self.assertEqual(eh["retryTimes"], 3)

    def test_retry_times_alt_key(self):
        eh = self._eh_src("(on-error abort :retry-times 5)")
        self.assertEqual(eh["retryTimes"], 5)

    def test_default_value_field(self):
        eh = self._eh_src('(on-error continue_with :default "fallback")')
        self.assertEqual(eh["defaultValue"], "fallback")

    def test_default_value_alt_key(self):
        eh = self._eh_src('(on-error continue_with :default-value "fb")')
        self.assertEqual(eh["defaultValue"], "fb")

    def test_error_code_field(self):
        eh = self._eh_src("(on-error abort :code 404)")
        self.assertEqual(eh["errorCode"], 404)

    def test_error_message_field(self):
        eh = self._eh_src('(on-error abort :msg "bad request")')
        self.assertEqual(eh["errorMessage"], "bad request")

    def test_error_message_alt_key_message(self):
        eh = self._eh_src('(on-error abort :message "err")')
        self.assertEqual(eh["errorMessage"], "err")

    def test_error_message_alt_key_error_message(self):
        eh = self._eh_src('(on-error abort :error-message "oops")')
        self.assertEqual(eh["errorMessage"], "oops")

    def test_unknown_strategy_raises(self):
        with self.assertRaises(SyntaxError):
            self._eh_src("(on-error unknown)")

    def test_no_strategy_raises(self):
        with self.assertRaises(SyntaxError):
            self._eh_src("(on-error)")

    def test_non_list_form_raises(self):
        with self.assertRaises(SyntaxError):
            self._eh_src("abort")

    def test_wrong_head_raises(self):
        with self.assertRaises(SyntaxError):
            self._eh_src("(error abort)")


# ---------------------------------------------------------------------------
# _c_if: specific field values in IR
# ---------------------------------------------------------------------------

class TestCIfFields(unittest.TestCase):
    def _get_if_node(self, src: str) -> dict:
        d = compile_sexpr(src)
        return next(n for n in d["nodes"] if n["type"] == "if")

    def test_if_then_next_value(self):
        src = """(flow g :input-type object
  (start -> i)
  (if :id i (cond "$INPUT.x" eq 1) -> yes :else no)
  (end :id yes :output 1)
  (end :id no :output 0))"""
        n = self._get_if_node(src)
        self.assertEqual(n["next"], "yes")

    def test_if_else_next_value(self):
        src = """(flow g :input-type object
  (start -> i)
  (if :id i (cond "$INPUT.x" eq 1) -> yes :else no)
  (end :id yes :output 1)
  (end :id no :output 0))"""
        n = self._get_if_node(src)
        self.assertEqual(n["else_next"], "no")

    def test_if_condition_field_and_operator(self):
        src = """(flow g :input-type object
  (start -> i)
  (if :id i (cond "$INPUT.age" gte 18) -> adult :else minor)
  (end :id adult :output "adult")
  (end :id minor :output "minor"))"""
        n = self._get_if_node(src)
        self.assertEqual(n["condition"]["field"], "$INPUT.age")
        self.assertEqual(n["condition"]["operator"], "gte")
        self.assertEqual(n["condition"]["value"], 18)

    def test_if_missing_then_raises(self):
        src = """(flow g :input-type object
  (start -> i)
  (if :id i (cond "$INPUT.x" eq 1) :else no)
  (end :id no :output 0))"""
        with self.assertRaises(SyntaxError):
            compile_sexpr(src)

    def test_if_missing_else_raises(self):
        src = """(flow g :input-type object
  (start -> i)
  (if :id i (cond "$INPUT.x" eq 1) -> yes)
  (end :id yes :output 1))"""
        with self.assertRaises(SyntaxError):
            compile_sexpr(src)


# ---------------------------------------------------------------------------
# _c_event: eventType stored correctly
# ---------------------------------------------------------------------------

class TestCEventFields(unittest.TestCase):
    def _get_event_node(self, src: str) -> dict:
        d = compile_sexpr(src)
        return next(n for n in d["nodes"] if n["type"] == "event")

    def test_event_type_stored(self):
        src = """(flow g :input-type object
  (start -> ev)
  (event :id ev :type "user.login" -> end)
  (end end :output "$NODE.ev"))"""
        n = self._get_event_node(src)
        self.assertEqual(n["eventType"], "user.login")

    def test_event_type_alt_key(self):
        src = """(flow g :input-type object
  (start -> ev)
  (event :id ev :event-type "order.created" -> end)
  (end end :output "$NODE.ev"))"""
        n = self._get_event_node(src)
        self.assertEqual(n["eventType"], "order.created")

    def test_event_type_alt_key2(self):
        src = """(flow g :input-type object
  (start -> ev)
  (event :id ev :eventType "payment.done" -> end)
  (end end :output "$NODE.ev"))"""
        n = self._get_event_node(src)
        self.assertEqual(n["eventType"], "payment.done")

    def test_event_filter_stored(self):
        src = """(flow g :input-type object
  (start -> ev)
  (event :id ev :type "user.login" :filter "user_id" -> end)
  (end end :output "$NODE.ev"))"""
        n = self._get_event_node(src)
        self.assertEqual(n["eventFilter"], "user_id")

    def test_event_no_type_raises(self):
        src = """(flow g :input-type object
  (start -> ev)
  (event :id ev -> end)
  (end end :output "$NODE.ev"))"""
        with self.assertRaises(SyntaxError):
            compile_sexpr(src)


# ---------------------------------------------------------------------------
# _c_parallel / _c_pbranch: mode, joinBranches list, isConditional
# ---------------------------------------------------------------------------

class TestCParallelFields(unittest.TestCase):
    def _get_parallel_node(self, src: str) -> dict:
        d = compile_sexpr(src)
        return next(n for n in d["nodes"] if n["type"] == "parallel")

    def test_parallel_mode_field(self):
        src = """(flow g :input-type object
  (start -> p)
  (parallel :id p :mode fast
    (pbranch b1 :flow (childflow (start -> e) (end e :output 1)))
    -> end)
  (end end :output "$NODE.p"))"""
        p = self._get_parallel_node(src)
        self.assertEqual(p["mode"], "fast")

    def test_parallel_join_branches_list(self):
        src = """(flow g :input-type object
  (start -> p)
  (parallel :id p :join-branches b1
    (pbranch b1 :flow (childflow (start -> e) (end e :output 1)))
    (pbranch b2 :flow (childflow (start -> e) (end e :output 2)))
    -> end)
  (end end :output "$NODE.p"))"""
        p = self._get_parallel_node(src)
        self.assertIn("joinBranches", p)
        self.assertEqual(p["joinBranches"], ["b1"])

    def test_parallel_is_conditional_true(self):
        src = """(flow g :input-type object
  (start -> p)
  (parallel :id p :is-conditional true
    (pbranch b1 :flow (childflow (start -> e) (end e :output 1)))
    -> end)
  (end end :output "$NODE.p"))"""
        p = self._get_parallel_node(src)
        self.assertTrue(p["isConditional"])

    def test_pbranch_name_stored(self):
        src = """(flow g :input-type object
  (start -> p)
  (parallel :id p
    (pbranch my_branch :flow (childflow (start -> e) (end e :output 1)))
    -> end)
  (end end :output "$NODE.p"))"""
        p = self._get_parallel_node(src)
        self.assertEqual(p["branches"][0]["name"], "my_branch")

    def test_pbranch_input_stored(self):
        src = """(flow g :input-type object
  (start -> p)
  (parallel :id p
    (pbranch b1 :flow (childflow (start -> e) (end e :output 1)) :input "$INPUT.x")
    -> end)
  (end end :output "$NODE.p"))"""
        p = self._get_parallel_node(src)
        self.assertEqual(p["branches"][0]["input"], "$INPUT.x")

    def test_pbranch_condition_stored(self):
        src = """(flow g :input-type object
  (start -> p)
  (parallel :id p
    (pbranch b1
      :flow (childflow (start -> e) (end e :output 1))
      :when (cond "$INPUT.x" eq 1))
    -> end)
  (end end :output "$NODE.p"))"""
        p = self._get_parallel_node(src)
        self.assertIn("condition", p["branches"][0])
        self.assertEqual(p["branches"][0]["condition"]["operator"], "eq")

    def test_parallel_non_pbranch_raises(self):
        src = """(flow g :input-type object
  (start -> p)
  (parallel :id p
    (not-pbranch b1)
    -> end)
  (end end :output "$NODE.p"))"""
        with self.assertRaises(SyntaxError):
            compile_sexpr(src)


# ---------------------------------------------------------------------------
# _c_switch / _c_branch: specific field values
# ---------------------------------------------------------------------------

class TestCSwitchFields(unittest.TestCase):
    def _get_switch_node(self, src: str) -> dict:
        d = compile_sexpr(src)
        return next(n for n in d["nodes"] if n["type"] == "switch")

    def test_switch_branch_name(self):
        src = """(flow g :input-type object
  (start -> sw)
  (switch :id sw
    (branch case_a -> e1 :when (cond "$INPUT.x" eq 1))
    (branch default_b -> e2 :default true))
  (end :id e1 :output 1)
  (end :id e2 :output 0))"""
        sw = self._get_switch_node(src)
        self.assertEqual(sw["branches"][0]["name"], "case_a")

    def test_switch_branch_next(self):
        src = """(flow g :input-type object
  (start -> sw)
  (switch :id sw
    (branch case_a -> e1 :when (cond "$INPUT.x" eq 1))
    (branch def -> e2 :default true))
  (end :id e1 :output 1)
  (end :id e2 :output 0))"""
        sw = self._get_switch_node(src)
        self.assertEqual(sw["branches"][0]["next"], "e1")

    def test_switch_branch_condition(self):
        src = """(flow g :input-type object
  (start -> sw)
  (switch :id sw
    (branch case_a -> e1 :when (cond "$INPUT.x" eq 5))
    (branch def -> e2 :default true))
  (end :id e1 :output 1)
  (end :id e2 :output 0))"""
        sw = self._get_switch_node(src)
        self.assertIn("condition", sw["branches"][0])
        self.assertEqual(sw["branches"][0]["condition"]["value"], 5)

    def test_switch_branch_is_default(self):
        src = """(flow g :input-type object
  (start -> sw)
  (switch :id sw
    (branch def -> e1 :default true))
  (end :id e1 :output 1))"""
        sw = self._get_switch_node(src)
        self.assertTrue(sw["branches"][0]["isDefault"])

    def test_switch_no_branches_raises(self):
        src = """(flow g :input-type object
  (start -> sw)
  (switch :id sw)
  (end :output 0))"""
        with self.assertRaises(SyntaxError):
            compile_sexpr(src)

    def test_switch_non_branch_raises(self):
        src = """(flow g :input-type object
  (start -> sw)
  (switch :id sw (not-branch x -> e))
  (end :id e :output 0))"""
        with self.assertRaises(SyntaxError):
            compile_sexpr(src)


# ---------------------------------------------------------------------------
# _c_end: resultType field
# ---------------------------------------------------------------------------

class TestCEndResultType(unittest.TestCase):
    def test_end_default_result_type_is_success(self):
        src = """(flow g (start -> e) (end :id e :output 1))"""
        d = compile_sexpr(src)
        end = d["nodes"][1]
        self.assertEqual(end["resultType"], "success")

    def test_end_failure_result_type(self):
        src = """(flow g
  (start -> e)
  (end :id e :output 1 :result-type failure))"""
        d = compile_sexpr(src)
        end = d["nodes"][1]
        self.assertEqual(end["resultType"], "failure")

    def test_end_output_value_stored(self):
        src = """(flow g (start -> e) (end :id e :output "done"))"""
        d = compile_sexpr(src)
        self.assertEqual(d["nodes"][1]["output"], "done")


# ---------------------------------------------------------------------------
# _c_assignment: output expression
# ---------------------------------------------------------------------------

class TestCAssignmentExact(unittest.TestCase):
    def test_assignment_output_is_expression(self):
        src = """(flow g :input-type object
  (start -> a)
  (assignment :id a "$INPUT.x" -> e)
  (end :id e :output "$NODE.a"))"""
        d = compile_sexpr(src)
        self.assertEqual(d["nodes"][1]["output"], "$INPUT.x")

    def test_assignment_next_stored(self):
        src = """(flow g :input-type object
  (start -> a)
  (assignment :id a "$INPUT.x" -> next_node)
  (end :id next_node :output "$NODE.a"))"""
        d = compile_sexpr(src)
        self.assertEqual(d["nodes"][1]["next"], "next_node")


# ---------------------------------------------------------------------------
# _node_to_src (flow_to_sexpr): exact substring assertions
# ---------------------------------------------------------------------------

class TestNodeToSrcExact(unittest.TestCase):
    """Targets _node_to_src mutations by asserting exact string content."""

    def test_end_success_result_type_not_in_output(self):
        """resultType == 'success' must NOT emit :result-type in output."""
        src = """(flow g (start -> e) (end :id e :output 1))"""
        result = _roundtrip(src)
        self.assertNotIn(":result-type", result)

    def test_end_failure_result_type_in_output(self):
        """resultType != 'success' MUST emit :result-type in output."""
        src = """(flow g
  (start -> e)
  (end :id e :output 1 :result-type failure))"""
        result = _roundtrip(src)
        self.assertIn(":result-type", result)
        self.assertIn("failure", result)

    def test_assignment_output_in_sexpr(self):
        """Assignment node's output expression must appear in output."""
        src = """(flow g :input-type object
  (start -> a)
  (assignment :id a "$INPUT.value" -> e)
  (end :id e :output "$NODE.a"))"""
        result = _roundtrip(src)
        self.assertIn("$INPUT.value", result)

    def test_assignment_next_arrow_in_sexpr(self):
        """Assignment node with next shows '-> next_id'."""
        src = """(flow g :input-type object
  (start -> a)
  (assignment :id a "$INPUT.value" -> e)
  (end :id e :output "$NODE.a"))"""
        result = _roundtrip(src)
        self.assertIn("->", result)

    def test_if_condition_in_sexpr(self):
        """if node: condition, -> then, :else else_next all appear."""
        src = """(flow g :input-type object
  (start -> i)
  (if :id i (cond "$INPUT.age" gte 18) -> adult :else minor)
  (end :id adult :output "adult")
  (end :id minor :output "minor"))"""
        result = _roundtrip(src)
        self.assertIn(":else", result)
        self.assertIn("->", result)
        self.assertIn("gte", result)

    def test_switch_branches_in_sexpr(self):
        """switch node serializes all branches."""
        src = """(flow g :input-type object
  (start -> sw)
  (switch :id sw
    (branch high -> hi :when (cond "$INPUT.n" gt 10))
    (branch def -> lo :default true))
  (end :id hi :output "hi")
  (end :id lo :output "lo"))"""
        result = _roundtrip(src)
        self.assertIn("branch", result)
        self.assertIn("->", result)
        self.assertIn(":when", result)
        self.assertIn(":default", result)

    def test_loop_with_when_in_sexpr(self):
        """loop node with condition emits ':when' in output."""
        src = """(flow g :input-type object
  (start -> lp)
  (loop :id lp
    :collection "$INPUT.items"
    :when (cond "$INPUT.count" lt 10)
    :child (childflow (start -> e) (end e :output "$INPUT.item"))
    -> done)
  (end :id done :output "$NODE.lp"))"""
        result = _roundtrip(src)
        self.assertIn(":when", result)
        self.assertIn(":collection", result)
        self.assertIn(":child", result)

    def test_map_concurrent_in_sexpr(self):
        """map with concurrent=True emits ':concurrent'."""
        src = """(flow g :input-type object
  (start -> m)
  (map :id m :collection "$INPUT.items"
    :concurrent true :max-concurrent 3
    :child (childflow (start -> e) (end e :output "$INPUT.item"))
    -> done)
  (end :id done :output "$NODE.m"))"""
        result = _roundtrip(src)
        self.assertIn(":concurrent", result)

    def test_reduce_initial_in_sexpr(self):
        """reduce with initial emits ':initial'."""
        src = """(flow g :input-type object
  (start -> r)
  (reduce :id r :collection "$INPUT.items"
    :initial 0
    :child (childflow (start -> e) (end e :output "$INPUT.item"))
    -> done)
  (end :id done :output "$NODE.r"))"""
        result = _roundtrip(src)
        self.assertIn(":initial", result)

    def test_http_method_url_in_sexpr(self):
        """http node serializes :method and :url."""
        src = """(flow g :input-type object
  (start -> h)
  (http :id h :method POST :url "http://api.example.com" -> e)
  (end :id e :output "$NODE.h"))"""
        result = _roundtrip(src)
        self.assertIn(":method", result)
        self.assertIn(":url", result)
        self.assertIn("POST", result)

    def test_http_body_in_sexpr(self):
        """http node with body emits ':body'."""
        src = """(flow g :input-type object
  (start -> h)
  (http :id h :method POST :url "http://a.b"
    :body "$INPUT.data" -> e)
  (end :id e :output "$NODE.h"))"""
        result = _roundtrip(src)
        self.assertIn(":body", result)

    def test_code_lang_code_in_sexpr(self):
        """code node serializes :lang and :code."""
        src = """(flow g :input-type object
  (start -> c)
  (code :id c :lang python :code "result = x + 1" -> e)
  (end :id e :output "$NODE.c"))"""
        result = _roundtrip(src)
        self.assertIn(":lang", result)
        self.assertIn(":code", result)
        self.assertIn("python", result)

    def test_event_type_in_sexpr(self):
        """event node serializes :type."""
        src = """(flow g :input-type object
  (start -> ev)
  (event :id ev :type "user.login" -> end)
  (end :id end :output "$NODE.ev"))"""
        result = _roundtrip(src)
        self.assertIn(":type", result)
        self.assertIn("user.login", result)

    def test_error_handler_retry_in_sexpr(self):
        """on-error with retry emits ':retry'."""
        src = """(flow g :input-type object
  (start -> h)
  (http :id h :method GET :url "http://a.b"
    :on-error (on-error abort :retry 3) -> e)
  (end :id e :output "$NODE.h"))"""
        result = _roundtrip(src)
        self.assertIn(":retry", result)
        self.assertIn("3", result)

    def test_case_target_in_sexpr(self):
        """case node serializes :target and match arms."""
        src = """(flow g :input-type object
  (start -> c)
  (case :id c :target "$INPUT.code"
    (match 200 e200)
    (match 404 e404)
    :default e0)
  (end :id e200 :output "ok")
  (end :id e404 :output "not found")
  (end :id e0 :output "other"))"""
        result = _roundtrip(src)
        self.assertIn(":target", result)
        self.assertIn("match", result)

    def test_flow_to_sexpr_desc_in_output(self):
        """flow with desc emits :desc in sexpr output."""
        src = """(flow g :desc "test flow"
  (start -> e) (end :id e :output 1))"""
        result = _roundtrip(src)
        self.assertIn(":desc", result)
        self.assertIn("test flow", result)

    def test_flow_to_sexpr_input_type_in_output(self):
        """flow with inputType emits :input-type in sexpr output."""
        src = """(flow g :input-type object (start -> e) (end :id e :output 1))"""
        result = _roundtrip(src)
        self.assertIn(":input-type", result)
        self.assertIn("object", result)


# ---------------------------------------------------------------------------
# _flow_inner_to_src: childflow inputType roundtrip
# ---------------------------------------------------------------------------

class TestFlowInnerToSrc(unittest.TestCase):
    def test_childflow_input_type_in_sexpr(self):
        """_flow_inner_to_src emits :input-type when childFlow has inputType."""
        src = """(flow g :input-type object
  (start -> m)
  (map :id m :collection "$INPUT.items"
    :child (childflow :input-type object
      (start -> e) (end :id e :output "$INPUT.item"))
    -> done)
  (end :id done :output "$NODE.m"))"""
        result = _roundtrip(src)
        self.assertIn(":input-type", result)

    def test_childflow_nodes_in_sexpr(self):
        """_flow_inner_to_src includes child nodes."""
        src = """(flow g :input-type object
  (start -> m)
  (map :id m :collection "$INPUT.items"
    :child (childflow
      (start -> e) (end :id e :output "$INPUT.item"))
    -> done)
  (end :id done :output "$NODE.m"))"""
        result = _roundtrip(src)
        self.assertIn("childflow", result)
        self.assertIn("start", result)


# ---------------------------------------------------------------------------
# _common_fields: timeout on nodes
# ---------------------------------------------------------------------------

class TestCommonFieldsTimeout(unittest.TestCase):
    def test_node_timeout_stored(self):
        src = """(flow g :input-type object
  (start -> h)
  (http :id h :method GET :url "http://a.b" :timeout 10 -> e)
  (end :id e :output "$NODE.h"))"""
        d = compile_sexpr(src)
        h = d["nodes"][1]
        self.assertEqual(h["timeout"], 10)


# ---------------------------------------------------------------------------
# _compile_childflow: runtime is python
# ---------------------------------------------------------------------------

class TestCompileChildflowFields(unittest.TestCase):
    def test_childflow_runtime_python(self):
        src = """(flow g :input-type object
  (start -> m)
  (map :id m :collection "$INPUT.items"
    :child (childflow (start -> e) (end :id e :output "$INPUT.item"))
    -> done)
  (end :id done :output "$NODE.m"))"""
        d = compile_sexpr(src)
        cf = d["nodes"][1]["childFlow"]
        self.assertEqual(cf["runtime"], "python")

    def test_childflow_input_type_stored(self):
        src = """(flow g :input-type object
  (start -> m)
  (map :id m :collection "$INPUT.items"
    :child (childflow :input-type string
      (start -> e) (end :id e :output "$INPUT.item"))
    -> done)
  (end :id done :output "$NODE.m"))"""
        d = compile_sexpr(src)
        cf = d["nodes"][1]["childFlow"]
        self.assertEqual(cf["inputType"]["dataType"], "string")


# ---------------------------------------------------------------------------
# _c_loop / _c_map / _reduce: specific fields
# ---------------------------------------------------------------------------

class TestCollectionNodes(unittest.TestCase):
    def test_loop_condition_stored(self):
        src = """(flow g :input-type object
  (start -> lp)
  (loop :id lp
    :collection "$INPUT.items"
    :when (cond "$INPUT.count" lt 10)
    :child (childflow (start -> e) (end :id e :output "$INPUT.item"))
    -> done)
  (end :id done :output "$NODE.lp"))"""
        d = compile_sexpr(src)
        lp = d["nodes"][1]
        self.assertIn("condition", lp)
        self.assertEqual(lp["condition"]["operator"], "lt")

    def test_map_max_concurrent_stored(self):
        src = """(flow g :input-type object
  (start -> m)
  (map :id m :collection "$INPUT.items"
    :concurrent true :max-concurrent 4
    :child (childflow (start -> e) (end :id e :output "$INPUT.item"))
    -> done)
  (end :id done :output "$NODE.m"))"""
        d = compile_sexpr(src)
        m = d["nodes"][1]
        self.assertEqual(m["maxConcurrent"], 4)

    def test_reduce_initial_stored(self):
        src = """(flow g :input-type object
  (start -> r)
  (reduce :id r :collection "$INPUT.items"
    :initial 0
    :child (childflow (start -> e) (end :id e :output "$INPUT.item"))
    -> done)
  (end :id done :output "$NODE.r"))"""
        d = compile_sexpr(src)
        r = d["nodes"][1]
        self.assertEqual(r["initial"], 0)

    def test_filter_type_stored(self):
        src = """(flow g :input-type object
  (start -> f)
  (filter :id f :collection "$INPUT.items"
    :child (childflow (start -> e) (end :id e :output "$INPUT.item"))
    -> done)
  (end :id done :output "$NODE.f"))"""
        d = compile_sexpr(src)
        self.assertEqual(d["nodes"][1]["type"], "filter")

    def test_find_type_stored(self):
        src = """(flow g :input-type object
  (start -> f)
  (find :id f :collection "$INPUT.items"
    :child (childflow (start -> e) (end :id e :output "$INPUT.item"))
    -> done)
  (end :id done :output "$NODE.f"))"""
        d = compile_sexpr(src)
        self.assertEqual(d["nodes"][1]["type"], "find")


# ---------------------------------------------------------------------------
# _c_http: headers and body in IR
# ---------------------------------------------------------------------------

class TestCHttpFields(unittest.TestCase):
    def test_http_headers_stored(self):
        src = """(flow g :input-type object
  (start -> h)
  (http :id h :method GET :url "http://a.b"
    :headers (dict :Content-Type "application/json") -> e)
  (end :id e :output "$NODE.h"))"""
        d = compile_sexpr(src)
        h = d["nodes"][1]
        self.assertIn("headers", h)
        self.assertIn("Content-Type", h["headers"])

    def test_http_body_stored(self):
        src = """(flow g :input-type object
  (start -> h)
  (http :id h :method POST :url "http://a.b"
    :body "$INPUT.data" -> e)
  (end :id e :output "$NODE.h"))"""
        d = compile_sexpr(src)
        h = d["nodes"][1]
        self.assertEqual(h["body"], "$INPUT.data")

    def test_http_method_stored(self):
        src = """(flow g :input-type object
  (start -> h)
  (http :id h :method DELETE :url "http://a.b" -> e)
  (end :id e :output "$NODE.h"))"""
        d = compile_sexpr(src)
        self.assertEqual(d["nodes"][1]["method"], "DELETE")

    def test_http_url_stored(self):
        src = """(flow g :input-type object
  (start -> h)
  (http :id h :method GET :url "https://example.com/api" -> e)
  (end :id e :output "$NODE.h"))"""
        d = compile_sexpr(src)
        self.assertEqual(d["nodes"][1]["url"], "https://example.com/api")


# ---------------------------------------------------------------------------
# _c_code: language and code fields
# ---------------------------------------------------------------------------

class TestCCodeFields(unittest.TestCase):
    def test_code_language_stored(self):
        src = """(flow g :input-type object
  (start -> c)
  (code :id c :lang javascript :code "result = x + 1" -> e)
  (end :id e :output "$NODE.c"))"""
        d = compile_sexpr(src)
        self.assertEqual(d["nodes"][1]["language"], "javascript")

    def test_code_code_stored(self):
        src = """(flow g :input-type object
  (start -> c)
  (code :id c :lang python :code "result = x * 2" -> e)
  (end :id e :output "$NODE.c"))"""
        d = compile_sexpr(src)
        self.assertEqual(d["nodes"][1]["code"], "result = x * 2")


# ---------------------------------------------------------------------------
# _c_case: target and default
# ---------------------------------------------------------------------------

class TestCCaseFields(unittest.TestCase):
    def test_case_target_stored(self):
        src = """(flow g :input-type object
  (start -> c)
  (case :id c :target "$INPUT.status"
    (match 200 e_ok)
    :default e_def)
  (end :id e_ok :output "ok")
  (end :id e_def :output "other"))"""
        d = compile_sexpr(src)
        c = d["nodes"][1]
        self.assertEqual(c["target"], "$INPUT.status")

    def test_case_default_stored(self):
        src = """(flow g :input-type object
  (start -> c)
  (case :id c :target "$INPUT.code"
    (match 1 e1)
    :default e_def)
  (end :id e1 :output 1)
  (end :id e_def :output 0))"""
        d = compile_sexpr(src)
        c = d["nodes"][1]
        self.assertEqual(c["default"], "e_def")

    def test_case_match_value_stored(self):
        src = """(flow g :input-type object
  (start -> c)
  (case :id c :target "$INPUT.n"
    (match 42 e42)
    :default e0)
  (end :id e42 :output 42)
  (end :id e0 :output 0))"""
        d = compile_sexpr(src)
        match = d["nodes"][1]["cases"][0]
        self.assertEqual(match["value"], 42)
        self.assertEqual(match["id"], "e42")


if __name__ == "__main__":
    unittest.main()
