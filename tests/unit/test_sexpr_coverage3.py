"""Extra coverage tests for plaita/dsl/sexpr.py — targets remaining gaps.

Covers:
- tokenize unrecognized char (line 114)
- _split_args keyword without value (lines 204, 211)
- _compile_dict_literal keyword without value (line 254)
- _Ctx.claim collision (lines 288-289)
- _compile_condition not a list (line 314), unknown form (line 335)
- _compile_error_handler branches (lines 344-363)
- _c_assignment no output (line 471)
- _c_if missing then (line 489), missing else (line 491)
- _c_switch wrong args (line 502), empty (line 505)
- _c_branch no name (line 514)
- _static_validate if/switch errors (lines 800-809)
- _expr_to_src / _node_to_src edge cases in flow_to_sexpr (lines 835-941)
"""
from __future__ import annotations

import unittest

from plaita.dsl.sexpr import compile_sexpr, parse_sexpr, flow_to_sexpr, read_forms


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _simple_flow(extra_node: str = "") -> str:
    return f"""
(flow simple-flow :input-type object
  (start -> e)
  {extra_node}
  (end e :output "$INPUT.x"))
"""


# ---------------------------------------------------------------------------
# Tokenize: unrecognized character (line 114)
# ---------------------------------------------------------------------------

class TestTokenizeError(unittest.TestCase):
    def test_unclosed_string_raises(self):
        """Line 114: unclosed string — '\"' is not part of atom pattern, string match fails."""
        with self.assertRaises(SyntaxError):
            read_forms('"unclosed string without closing quote')


# ---------------------------------------------------------------------------
# _split_args: keyword without value (lines 204, 211)
# ---------------------------------------------------------------------------

class TestSplitArgsErrors(unittest.TestCase):
    def test_keyword_at_end_without_value_raises(self):
        """Line 204: :key at end of args without following value raises."""
        src = """
(flow g :input-type object
  (start -> e)
  (end e :output "$INPUT.x" :result-type))
"""
        with self.assertRaises(SyntaxError):
            compile_sexpr(src)


# ---------------------------------------------------------------------------
# _compile_dict_literal: keyword without value (line 254)
# ---------------------------------------------------------------------------

class TestDictLiteralErrors(unittest.TestCase):
    def test_dict_keyword_without_value_raises(self):
        """Line 254: (dict :key) missing value raises SyntaxError."""
        src = """
(flow g :input-type object
  (start -> h)
  (http :id h :method GET :url "http://x.com"
    :headers (dict :Content-Type)
    -> e)
  (end e :output "$NODE.h"))
"""
        with self.assertRaises(SyntaxError):
            compile_sexpr(src)


# ---------------------------------------------------------------------------
# _Ctx.claim: duplicate id (lines 288-289)
# ---------------------------------------------------------------------------

class TestCtxClaimCollision(unittest.TestCase):
    def test_duplicate_node_id_raises(self):
        """Lines 288-289: two nodes with the same id raises ValueError."""
        src = """
(flow g :input-type object
  (start :id dup -> e)
  (end :id dup :output "$INPUT.x"))
"""
        with self.assertRaises(ValueError):
            compile_sexpr(src)


# ---------------------------------------------------------------------------
# _compile_condition: not a list (line 314) / unknown form (line 335)
# ---------------------------------------------------------------------------

class TestCompileConditionErrors(unittest.TestCase):
    def test_condition_not_a_list_raises(self):
        """Line 314: non-list condition raises SyntaxError."""
        src = """
(flow g :input-type object
  (start -> i)
  (if :id i "not_a_condition" -> e :else e2)
  (end e :output 1)
  (end e2 :output 2))
"""
        with self.assertRaises(SyntaxError):
            compile_sexpr(src)

    def test_unknown_condition_form_raises(self):
        """Line 335: unknown condition head like 'xor' raises SyntaxError."""
        src = """
(flow g :input-type object
  (start -> i)
  (if :id i (xor (cond "$INPUT.x" eq 1) (cond "$INPUT.y" eq 2)) -> e :else e2)
  (end e :output 1)
  (end e2 :output 2))
"""
        with self.assertRaises(SyntaxError):
            compile_sexpr(src)


# ---------------------------------------------------------------------------
# _compile_error_handler branches (lines 344-363)
# ---------------------------------------------------------------------------

class TestCompileErrorHandler(unittest.TestCase):
    def test_not_on_error_form_raises(self):
        """Line 344: on-error not in (on-error ...) form raises SyntaxError."""
        src = """
(flow g :input-type object
  (start -> h)
  (http :id h :method GET :url "http://x.com"
    :on-error (bad-handler abort)
    -> e)
  (end e :output "$NODE.h"))
"""
        with self.assertRaises(SyntaxError):
            compile_sexpr(src)

    def test_on_error_missing_strategy_raises(self):
        """Line 347: (on-error) without strategy raises."""
        src = """
(flow g :input-type object
  (start -> h)
  (http :id h :method GET :url "http://x.com"
    :on-error (on-error)
    -> e)
  (end e :output "$NODE.h"))
"""
        with self.assertRaises(SyntaxError):
            compile_sexpr(src)

    def test_on_error_invalid_strategy_raises(self):
        """Line 350: unknown strategy raises."""
        src = """
(flow g :input-type object
  (start -> h)
  (http :id h :method GET :url "http://x.com"
    :on-error (on-error invalid_strategy)
    -> e)
  (end e :output "$NODE.h"))
"""
        with self.assertRaises(SyntaxError):
            compile_sexpr(src)

    def test_on_error_with_retry_times(self):
        """Lines 353-354: on-error with :retry sets retryTimes."""
        src = """
(flow g :input-type object
  (start -> h)
  (http :id h :method POST :url "http://x.com"
    :on-error (on-error continue :retry 3)
    -> e)
  (end e :output "$NODE.h"))
"""
        d = compile_sexpr(src)
        http_node = next(n for n in d["nodes"] if n.get("type") == "http")
        self.assertEqual(http_node["errorHandler"]["retryTimes"], 3)

    def test_on_error_with_default_value(self):
        """Lines 355-357: on-error with :default sets defaultValue."""
        src = """
(flow g :input-type object
  (start -> h)
  (http :id h :method POST :url "http://x.com"
    :on-error (on-error continue_with :default (dict :err true))
    -> e)
  (end e :output "$NODE.h"))
"""
        d = compile_sexpr(src)
        http_node = next(n for n in d["nodes"] if n.get("type") == "http")
        self.assertEqual(http_node["errorHandler"]["defaultValue"], {"err": True})

    def test_on_error_with_error_code(self):
        """Lines 358-360: on-error with :code sets errorCode."""
        src = """
(flow g :input-type object
  (start -> h)
  (http :id h :method POST :url "http://x.com"
    :on-error (on-error abort :code 503)
    -> e)
  (end e :output "$NODE.h"))
"""
        d = compile_sexpr(src)
        http_node = next(n for n in d["nodes"] if n.get("type") == "http")
        self.assertEqual(http_node["errorHandler"]["errorCode"], 503)

    def test_on_error_with_error_message(self):
        """Lines 361-363: on-error with :msg sets errorMessage."""
        src = """
(flow g :input-type object
  (start -> h)
  (http :id h :method POST :url "http://x.com"
    :on-error (on-error abort :msg "Service unavailable")
    -> e)
  (end e :output "$NODE.h"))
"""
        d = compile_sexpr(src)
        http_node = next(n for n in d["nodes"] if n.get("type") == "http")
        self.assertEqual(http_node["errorHandler"]["errorMessage"], "Service unavailable")


# ---------------------------------------------------------------------------
# _c_assignment: no output (line 471)
# ---------------------------------------------------------------------------

class TestCAssignmentErrors(unittest.TestCase):
    def test_assignment_no_output_raises(self):
        """Line 471: assignment without output expression raises."""
        src = """
(flow g :input-type object
  (start -> a)
  (assignment :id a -> e)
  (end e :output "$INPUT.x"))
"""
        with self.assertRaises(SyntaxError):
            compile_sexpr(src)


# ---------------------------------------------------------------------------
# _c_if: missing then (line 489) / missing else (line 491)
# ---------------------------------------------------------------------------

class TestCIfErrors(unittest.TestCase):
    def test_if_missing_then_raises(self):
        """Line 489: if without :then raises SyntaxError."""
        src = """
(flow g :input-type object
  (start -> i)
  (if :id i (cond "$INPUT.x" eq 1) :else e2)
  (end :id e1 :output 1)
  (end :id e2 :output 2))
"""
        with self.assertRaises(SyntaxError):
            compile_sexpr(src)

    def test_if_missing_else_raises(self):
        """Line 491: if without :else raises SyntaxError."""
        src = """
(flow g :input-type object
  (start -> i)
  (if :id i (cond "$INPUT.x" eq 1) -> e1)
  (end :id e1 :output 1))
"""
        with self.assertRaises(SyntaxError):
            compile_sexpr(src)


# ---------------------------------------------------------------------------
# _c_switch: wrong arg type (line 502) / empty (line 505)
# ---------------------------------------------------------------------------

class TestCSwitchErrors(unittest.TestCase):
    def test_switch_wrong_arg_raises(self):
        """Line 502: switch with non-branch arg raises."""
        src = """
(flow g :input-type object
  (start -> sw)
  (switch :id sw (not-a-branch foo -> e1) (branch b2 -> e2 :default true))
  (end :id e1 :output 1)
  (end :id e2 :output 2))
"""
        with self.assertRaises(SyntaxError):
            compile_sexpr(src)

    def test_switch_empty_raises(self):
        """Line 505: switch with no branches raises."""
        src = """
(flow g :input-type object
  (start -> sw)
  (switch :id sw)
  (end :id e :output 1))
"""
        with self.assertRaises(SyntaxError):
            compile_sexpr(src)


# ---------------------------------------------------------------------------
# _c_branch: no name (line 514)
# ---------------------------------------------------------------------------

class TestCBranchErrors(unittest.TestCase):
    def test_branch_no_name_raises(self):
        """Line 514: branch without name raises."""
        src = """
(flow g :input-type object
  (start -> sw)
  (switch :id sw (branch -> e1 :default true))
  (end :id e1 :output 1))
"""
        with self.assertRaises(SyntaxError):
            compile_sexpr(src)


# ---------------------------------------------------------------------------
# _static_validate: if node missing next (line 800)
# ---------------------------------------------------------------------------

class TestStaticValidateErrors(unittest.TestCase):
    def test_if_without_next_fails_validate(self):
        """Line 800: if node with no 'next' field fails static validation."""
        # Build a raw IR dict with a broken if node
        data = {
            "runtime": "python",
            "flow_id": "bad_flow",
            "nodes": [
                {"type": "start", "id": "s", "next": "i"},
                # if node missing 'next' (true branch)
                {"type": "if", "id": "i", "condition": {"field": "$INPUT.x", "operator": "eq", "value": 1}},
                {"type": "end", "id": "e", "output": "$INPUT.x", "resultType": "success"},
            ]
        }
        from plaita.dsl.sexpr import _static_validate
        with self.assertRaises(ValueError):
            _static_validate(data)

    def test_switch_without_default_fails_validate(self):
        """Line 809: switch without default branch fails static validation."""
        data = {
            "runtime": "python",
            "flow_id": "bad_switch",
            "nodes": [
                {"type": "start", "id": "s", "next": "sw"},
                {
                    "type": "switch",
                    "id": "sw",
                    "branches": [
                        {"name": "b1", "next": "e", "condition": {"field": "$INPUT.x", "operator": "eq", "value": 1}}
                    ]
                },
                {"type": "end", "id": "e", "output": "$INPUT.x", "resultType": "success"},
            ]
        }
        from plaita.dsl.sexpr import _static_validate
        with self.assertRaises(ValueError):
            _static_validate(data)


# ---------------------------------------------------------------------------
# flow_to_sexpr: various node types in serialization
# ---------------------------------------------------------------------------

class TestFlowToSexprEdgeCases(unittest.TestCase):
    def _compile_and_round_trip(self, src: str) -> str:
        """Compile sexpr to IR dict, then call flow_to_sexpr."""
        data = compile_sexpr(src)
        return flow_to_sexpr(data)

    def test_end_with_non_success_result_type(self):
        """Line 879: end node with resultType != 'success' includes :result-type."""
        src = """
(flow g :input-type object
  (start -> e)
  (end e :output "$INPUT.x" :result-type error))
"""
        result = self._compile_and_round_trip(src)
        self.assertIn("result-type", result)

    def test_expr_to_src_empty_string(self):
        """Line 830: empty string renders as '""'."""
        from plaita.dsl.sexpr import _expr_to_src
        self.assertEqual(_expr_to_src(""), '""')

    def test_expr_to_src_bool_true(self):
        """Line 836: bool True renders as 'true'."""
        from plaita.dsl.sexpr import _expr_to_src
        self.assertEqual(_expr_to_src(True), "true")

    def test_expr_to_src_bool_false(self):
        """Line 836: bool False renders as 'false'."""
        from plaita.dsl.sexpr import _expr_to_src
        self.assertEqual(_expr_to_src(False), "false")

    def test_expr_to_src_int(self):
        """Line 838: int renders as string."""
        from plaita.dsl.sexpr import _expr_to_src
        self.assertEqual(_expr_to_src(42), "42")

    def test_expr_to_src_none(self):
        """Line 840: None renders as 'nil'."""
        from plaita.dsl.sexpr import _expr_to_src
        self.assertEqual(_expr_to_src(None), "nil")

    def test_expr_to_src_list(self):
        """Line 842: list renders as (...)."""
        from plaita.dsl.sexpr import _expr_to_src
        result = _expr_to_src([1, 2, "x"])
        self.assertEqual(result, "(1 2 x)")

    def test_expr_to_src_dict(self):
        """Line 844: dict renders as (dict ...)."""
        from plaita.dsl.sexpr import _expr_to_src
        result = _expr_to_src({"k": "v"})
        self.assertIn("dict", result)
        self.assertIn(":k", result)

    def test_expr_to_src_fallback_to_str(self):
        """Line 845: non-standard value renders via str()."""
        from plaita.dsl.sexpr import _expr_to_src

        class CustomVal:
            def __str__(self):
                return "custom_repr"

        result = _expr_to_src(CustomVal())
        self.assertEqual(result, "custom_repr")

    def test_cond_to_src_with_relation(self):
        """Lines 859-860: _cond_to_src with relation dict renders (and/or ...)."""
        from plaita.dsl.sexpr import _cond_to_src
        cond = {
            "relation": "and",
            "conditions": [
                {"field": "$INPUT.x", "operator": "gt", "value": 0},
                {"field": "$INPUT.y", "operator": "lt", "value": 10},
            ]
        }
        result = _cond_to_src(cond)
        self.assertTrue(result.startswith("(and"))
        self.assertIn("cond", result)

    def test_cond_to_src_non_dict(self):
        """Line 863: _cond_to_src with non-dict value falls back to _expr_to_src."""
        from plaita.dsl.sexpr import _cond_to_src
        result = _cond_to_src("$INPUT.flag")
        self.assertEqual(result, "$INPUT.flag")

    def test_flow_to_sexpr_assignment_node(self):
        """Lines 880-883: assignment node in flow_to_sexpr."""
        src = """
(flow g :input-type object
  (start -> a)
  (assignment :id a "$INPUT.x" -> e)
  (end e :output "$NODE.a"))
"""
        result = self._compile_and_round_trip(src)
        self.assertIn("assignment", result)

    def test_flow_to_sexpr_http_with_body(self):
        """Lines 922-924: http node with body in flow_to_sexpr."""
        src = """
(flow g :input-type object
  (start -> h)
  (http :id h :method POST :url "http://x.com"
    :body (dict :key "$INPUT.data")
    -> e)
  (end e :output "$NODE.h"))
"""
        result = self._compile_and_round_trip(src)
        self.assertIn(":body", result)

    def test_flow_to_sexpr_event_node(self):
        """Lines 932-933: event node in flow_to_sexpr."""
        src = """
(flow g :input-type object
  (start -> ev)
  (event :id ev :type "user.login" -> e)
  (end e :output "$NODE.ev"))
"""
        result = self._compile_and_round_trip(src)
        self.assertIn("event", result)
        self.assertIn("user.login", result)

    def test_flow_to_sexpr_node_with_error_handler(self):
        """Lines 935-941: node with errorHandler in flow_to_sexpr."""
        src = """
(flow g :input-type object
  (start -> h)
  (http :id h :method POST :url "http://x.com"
    :on-error (on-error continue :retry 2)
    -> e)
  (end e :output "$NODE.h"))
"""
        result = self._compile_and_round_trip(src)
        self.assertIn("on-error", result)
        self.assertIn("continue", result)

    def test_flow_to_sexpr_code_node(self):
        """Lines 927-931: code node in flow_to_sexpr."""
        src = """
(flow g :input-type object
  (start -> c)
  (code :id c :lang python :code "result = x * 2" -> e)
  (end e :output "$NODE.c"))
"""
        result = self._compile_and_round_trip(src)
        self.assertIn(":lang", result)
        self.assertIn(":code", result)

    def test_flow_to_sexpr_if_node(self):
        """Lines 884-887: if node in flow_to_sexpr."""
        src = """
(flow g :input-type object
  (start -> i)
  (if :id i (cond "$INPUT.x" eq 1) -> e1 :else e2)
  (end :id e1 :output 1)
  (end :id e2 :output 2))
"""
        result = self._compile_and_round_trip(src)
        self.assertIn("if", result)
        self.assertIn(":else", result)

    def test_flow_to_sexpr_switch_with_condition(self):
        """Lines 888-895: switch node with condition in flow_to_sexpr."""
        src = """
(flow g :input-type object
  (start -> sw)
  (switch :id sw
    (branch b1 -> e1 :when (cond "$INPUT.x" eq 1))
    (branch b2 -> e2 :default true))
  (end :id e1 :output 1)
  (end :id e2 :output 2))
"""
        result = self._compile_and_round_trip(src)
        self.assertIn("switch", result)
        self.assertIn(":when", result)
        self.assertIn(":default true", result)

    def test_flow_to_sexpr_loop_with_condition(self):
        """Lines 905-906: loop node with condition in flow_to_sexpr."""
        src = """
(flow g :input-type object
  (start -> lp)
  (loop :id lp
    :collection "$INPUT.items"
    :child (childflow
      (start -> e_inner)
      (end e_inner :output "$INPUT.item"))
    :when (cond "$INPUT.item" gt 0)
    -> e_out)
  (end e_out :output "$NODE.lp"))
"""
        result = self._compile_and_round_trip(src)
        self.assertIn(":when", result)

    def test_flow_to_sexpr_map_concurrent(self):
        """Line 907-908: map node with concurrent=True in flow_to_sexpr."""
        src = """
(flow g :input-type object
  (start -> m)
  (map :id m
    :collection "$INPUT.items"
    :child (childflow
      (start -> e_inner)
      (end e_inner :output "$INPUT.item"))
    :concurrent true
    -> e_out)
  (end e_out :output "$NODE.m"))
"""
        result = self._compile_and_round_trip(src)
        self.assertIn(":concurrent true", result)

    def test_flow_to_sexpr_reduce_with_initial(self):
        """Lines 909-910: reduce node with initial in flow_to_sexpr."""
        src = """
(flow g :input-type object
  (start -> r)
  (reduce :id r
    :collection "$INPUT.items"
    :child (childflow
      (start -> e_inner)
      (end e_inner :output "$INPUT[0]"))
    :initial 0
    -> e_out)
  (end e_out :output "$NODE.r"))
"""
        result = self._compile_and_round_trip(src)
        self.assertIn(":initial", result)


if __name__ == "__main__":
    unittest.main()
