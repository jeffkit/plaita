"""plaita#136 regression: round-trip matrix for three one-sided sexpr gaps.

In ``plaita/dsl/sexpr.py`` the writer (``flow_to_sexpr`` / ``_node_to_src``)
and the reader (``compile_sexpr`` / ``_c_*`` / ``_common_fields``) disagree:
the reader has always read these, the writer never wrote them (or wrote them in
the wrong position) --

1. **``end.error``** (the issue itself): the writer emits only ``:output`` and
   ``:result-type``, never ``:error``. After a round-trip the payload is gone
   and the terminal state changes from ``ErrorResultException``
   (error_result/4001) to ``NodeExecutionError`` (node_error/-9527).
2. **``:timeout``**: ``_common_fields`` reads it for **every** node type while
   the writer emitted it nowhere, so every node silently lost its timeout.
3. **``errorHandler``**: the writer emitted it as a **trailing positional**
   ``(on-error abort :retry 2)``, which the reader swallowed as ``output`` ->
   ``output = ['on-error', 'abort', 'retry', 2]`` with ``retryTimes`` reset to
   0. On an ``assignment`` this is even a hard ``SyntaxError`` (the trailing
   form becomes a second positional argument).

All three share one root (the same writer/reader pair), so one round-trip
matrix covers them. Every case exercises **both** paths: ``compile_sexpr`` (IR)
and ``parse_sexpr`` (Flow model); where runtime semantics exist the terminal
state quadruple is compared before/after.

This file also pins the maintainer's two recent fixes as a **must-not-break**
regression corpus:

* ``7d0add9``: all keywords must precede ``-> next`` (a ``:kw`` after ``->``
  degrades into a positional argument);
* ``87e4dd7``: ``(event ... :filter (dict :a 1 :b x) -> e)`` must still yield
  ``event_filter == {'a': 1, 'b': 'x'}`` with ``next == 'e'`` after
  ``Flow.model_validate``.
"""

import pytest

from plaita.core.errors import FlowExecutionException
from plaita.dsl.ir_validate import build_flow
from plaita.dsl.sexpr import compile_sexpr, flow_to_sexpr, parse_sexpr


def _node(ir, node_id):
    for n in ir["nodes"]:
        if n.get("id") == node_id:
            return n
    raise AssertionError(f"no node {node_id} in IR: {ir['nodes']}")


def _model_node(flow, node_id):
    for n in flow.nodes:
        if getattr(n, "id", None) == node_id:
            return n
    raise AssertionError(f"no node {node_id} in Flow")


def _emit(ir):
    return flow_to_sexpr(ir)


def _paren_segment(text, start):
    """Return the balanced ``(...)`` segment beginning at ``start``.

    ``text.index(")")`` is not usable here: after #136 a node segment nests
    ``(on-error ...)`` and ``(dict ...)``, so the first ``)`` sits inside the
    nested form.
    """
    depth = 0
    for i in range(start, len(text)):
        if text[i] == "(":
            depth += 1
        elif text[i] == ")":
            depth -= 1
            if depth == 0:
                return text[start:i + 1]
    raise AssertionError(f"unbalanced parens: {text[start:]}")


def _node_segment(text, marker):
    """Balanced ``(marker ...)`` segment (marker e.g. ``"event"``)."""
    return _paren_segment(text, text.index("(" + marker))


def _roundtrip_ir(ir):
    """IR -> sexpr -> IR via ``compile_sexpr``; returns (text, IR)."""
    text = _emit(ir)
    return text, compile_sexpr(text)


def _outcome(ir):
    """Run IR; returns ('ok', value) or (exc name, flow_error_type, code, msg)."""
    try:
        return ("ok", build_flow(ir).run())
    except FlowExecutionException as exc:
        error_type = getattr(exc.error_type, "value", exc.error_type)
        return ("exc", type(exc).__name__, error_type, exc.code, exc.message)


# ===========================================================================
# Item 1: end.error
# ===========================================================================

def _end_ir(result_type="success", *, output=None, error=None, nid="e"):
    node = {"type": "end", "id": nid, "resultType": result_type}
    if output is not None:
        node["output"] = output
    if error is not None:
        node["error"] = error
    return {
        "runtime": "python", "flow_id": "f", "version": "1",
        "nodes": [
            {"type": "start", "id": "s", "next": nid},
            node,
        ],
    }


class TestItem1EndErrorRoundTrip:
    """``end.error`` must be emitted, read back, and keep the same terminal state."""

    PAYLOAD = {"code": 4001, "message": "bad"}

    def test_error_is_emitted(self):
        text = _emit(_end_ir("error", output="no", error=self.PAYLOAD))
        assert ":error (dict :code 4001 :message bad)" in text, text

    def test_error_survives_compile_sexpr(self):
        text, back = _roundtrip_ir(_end_ir("error", output="no", error=self.PAYLOAD))
        assert _node(back, "e")["error"] == self.PAYLOAD, (_node(back, "e"), text)

    def test_error_survives_parse_sexpr_model(self):
        text = _emit(_end_ir("error", output="no", error=self.PAYLOAD))
        node = _model_node(parse_sexpr(text), "e")
        assert node.error == self.PAYLOAD, (node.error, text)
        assert node.result_type == "error"

    def test_runtime_terminal_state_is_identical(self):
        """Same terminal quadruple (exception class / flow_error_type / code /
        message) before and after the round-trip.

        Before the fix the payload was gone, so ``End.execute`` took the
        ``self.error.get(...)`` None branch -> ``NodeExecutionError(-9527)``.
        """
        ir = _end_ir("error", output="no", error=self.PAYLOAD)
        _, back = _roundtrip_ir(ir)
        before = _outcome(ir)
        after = _outcome(back)
        assert before == after, (before, after, _emit(ir))
        assert before == ("exc", "ErrorResultException", "error_result", 4001, "bad")

    @pytest.mark.parametrize("result_type,output,error", [
        ("success", "x", None),                            # success exit
        ("error", None, {"code": 7, "message": "m"}),      # error without output
        ("error", "x", {"code": 4001, "message": "bad"}),  # error + payload
        ("nop", "x", None),                                # silent exit
    ])
    def test_four_end_shapes_roundtrip(self, result_type, output, error):
        ir = _end_ir(result_type, output=output, error=error)
        text, back = _roundtrip_ir(ir)
        assert _node(back, "e") == _node(ir, "e"), (text, _node(back, "e"))

    def test_empty_error_dict_is_data_not_absence(self):
        """An empty ``error={}`` is data: dropping it turns ERROR_RESULT into
        NODE_ERROR.

        ``End.execute`` distinguishes "error exit with code/message None" from
        "no error exit at all" (the latter makes ``.get`` raise AttributeError).
        The emit guard must therefore be ``is not None``, not truthiness.
        """
        ir = _end_ir("error", output="x", error={})
        text, back = _roundtrip_ir(ir)
        assert _node(back, "e")["error"] == {}, (text, _node(back, "e"))
        before, after = _outcome(ir), _outcome(back)
        assert before == after, (before, after, text)
        assert before == ("exc", "ErrorResultException", "error_result", None, None)

    def test_scalar_error_still_wraps_as_message(self):
        """The pre-existing scalar semantics ``:error boom`` -> ``{"message": ...}``
        must not change."""
        d = compile_sexpr('(flow f (start -> e) (end e :output 1 :error boom))')
        assert _node(d, "e")["error"] == {"message": "boom"}

    def test_dict_literal_error_is_not_a_bare_list(self):
        """``(dict :code 404)`` must compile to a dict, not stay a list that the
        else-branch then wraps into a message string."""
        d = compile_sexpr(
            '(flow f (start -> e) (end e :output 1 :result-type error '
            ':error (dict :code 404 :message gone)))'
        )
        assert _node(d, "e")["error"] == {"code": 404, "message": "gone"}


# ===========================================================================
# Item 2: :timeout (every node type)
# ===========================================================================

def _timeout_ir(node_type, timeout):
    """Minimal ``start -> <node> -> end`` flow whose node carries a timeout."""
    node = {"type": node_type, "id": "n", "next": "e", "timeout": timeout}
    if node_type == "assignment":
        node["output"] = "1"
    elif node_type == "event":
        node["eventType"] = "t"
    elif node_type == "code":
        node["language"] = "python"
        node["code"] = "x = 1"
    elif node_type == "http":
        node["method"] = "GET"
        node["url"] = "http://x"
    return {
        "runtime": "python", "flow_id": "f", "version": "1",
        "nodes": [
            {"type": "start", "id": "s", "next": "n"},
            node,
            {"type": "end", "id": "e"},
        ],
    }


class TestItem2NodeTimeoutRoundTrip:
    """``:timeout`` must be emitted and read back for **every** node type
    (before the fix it was emitted nowhere at all)."""

    @pytest.mark.parametrize("node_type", [
        "end", "assignment", "event", "code", "http", "if", "switch",
    ])
    def test_timeout_survives_roundtrip(self, node_type):
        if node_type == "end":
            ir = {
                "runtime": "python", "flow_id": "f", "version": "1",
                "nodes": [
                    {"type": "start", "id": "s", "next": "n"},
                    {"type": "end", "id": "n", "timeout": "PT30S"},
                ],
            }
        elif node_type == "if":
            ir = {
                "runtime": "python", "flow_id": "f", "version": "1",
                "nodes": [
                    {"type": "start", "id": "s", "next": "n"},
                    {"type": "if", "id": "n", "timeout": "PT5S",
                     "condition": {"field": "x", "operator": "eq", "value": 1},
                     "next": "e", "else_next": "e"},
                    {"type": "end", "id": "e"},
                ],
            }
        elif node_type == "switch":
            ir = {
                "runtime": "python", "flow_id": "f", "version": "1",
                "nodes": [
                    {"type": "start", "id": "s", "next": "n"},
                    {"type": "switch", "id": "n", "timeout": "PT5S",
                     "branches": [{"name": "a", "next": "e", "isDefault": True}]},
                    {"type": "end", "id": "e"},
                ],
            }
        else:
            ir = _timeout_ir(node_type, "PT10S")
        text, back = _roundtrip_ir(ir)
        assert ":timeout" in text, (node_type, text)
        assert _node(back, "n")["timeout"] == ir["nodes"][1]["timeout"], (
            node_type, text, _node(back, "n"),
        )

    def test_timeout_survives_parse_sexpr_model(self):
        text = _emit(_timeout_ir("assignment", "PT10S"))
        node = _model_node(parse_sexpr(text), "n")
        assert node.timeout == "PT10S", (node.timeout, text)

    def test_timeout_precedes_arrow_for_nodes_with_next(self):
        """Common keywords must precede ``-> next`` (otherwise they degrade into
        positional arguments)."""
        text = _emit(_timeout_ir("assignment", "PT10S"))
        node_part = _node_segment(text, "assignment")
        assert node_part.index(":timeout") < node_part.index("->"), node_part

    def test_absent_timeout_emits_nothing(self):
        ir = _timeout_ir("assignment", None)
        del ir["nodes"][1]["timeout"]
        text = _emit(ir)
        assert ":timeout" not in text, text
        assert "timeout" not in _node(_roundtrip_ir(ir)[1], "n"), text

    def test_integer_timeout_roundtrips(self):
        text, back = _roundtrip_ir(_timeout_ir("assignment", 30))
        assert _node(back, "n")["timeout"] == 30, (text, _node(back, "n"))


# ===========================================================================
# Item 3: errorHandler
# ===========================================================================

class TestItem3ErrorHandlerRoundTrip:
    """``errorHandler`` must be emitted as the **value** of ``:on-error`` and
    must never be swallowed into ``output``."""

    @pytest.mark.parametrize("handler", [
        {"strategy": "abort"},
        {"strategy": "continue", "retryTimes": 3},
        {"strategy": "continue_with", "defaultValue": "fallback"},
        {"strategy": "abort", "retryTimes": 2},
        {"strategy": "abort", "errorCode": 503},
        {"strategy": "abort", "errorMessage": "boom"},
        {"strategy": "continue_with", "retryTimes": 2,
         "defaultValue": {"err": True}, "errorCode": 7, "errorMessage": "m"},
    ])
    def test_handler_survives_roundtrip(self, handler):
        ir = _timeout_ir("http", None)
        del ir["nodes"][1]["timeout"]
        ir["nodes"][1]["errorHandler"] = handler
        text, back = _roundtrip_ir(ir)
        assert _node(back, "n")["errorHandler"] == handler, (
            text, _node(back, "n"),
        )

    def test_handler_is_not_swallowed_into_output(self):
        """The exact pre-fix symptom: the handler became
        ``output = ['on-error', ...]``."""
        ir = _timeout_ir("end", None)
        del ir["nodes"][1]["timeout"]
        ir["nodes"][1]["output"] = "x"
        ir["nodes"][1]["errorHandler"] = {"strategy": "abort", "retryTimes": 2}
        text, back = _roundtrip_ir(ir)
        node = _node(back, "n")
        assert node["output"] == "x", (text, node)
        assert node["errorHandler"] == {"strategy": "abort", "retryTimes": 2}, (
            text, node,
        )

    def test_assignment_handler_does_not_break_compilation(self):
        """Before the fix the trailing handler was a second positional argument
        on ``assignment`` -> ``SyntaxError``."""
        ir = _timeout_ir("assignment", None)
        del ir["nodes"][1]["timeout"]
        ir["nodes"][1]["errorHandler"] = {"strategy": "continue_with", "retryTimes": 2}
        text, back = _roundtrip_ir(ir)
        assert _node(back, "n")["errorHandler"] == {
            "strategy": "continue_with", "retryTimes": 2,
        }, (text, _node(back, "n"))
        assert _node(back, "n")["output"] == 1, (text, _node(back, "n"))

    def test_handler_retry_times_reach_the_model(self):
        """``retry_times`` used to reset to 0 at the model layer (the symptom
        stated in the issue)."""
        ir = _timeout_ir("http", None)
        del ir["nodes"][1]["timeout"]
        ir["nodes"][1]["errorHandler"] = {"strategy": "abort", "retryTimes": 2}
        node = _model_node(parse_sexpr(_emit(ir)), "n")
        assert node.error_handler.retry_times == 2, node.error_handler

    def test_handler_precedes_arrow(self):
        ir = _timeout_ir("http", None)
        del ir["nodes"][1]["timeout"]
        ir["nodes"][1]["errorHandler"] = {"strategy": "abort", "retryTimes": 2}
        text = _emit(ir)
        node_part = _node_segment(text, "http")
        assert node_part.index(":on-error") < node_part.index("->"), node_part

    def test_absent_handler_emits_nothing(self):
        text = _emit(_timeout_ir("http", None))
        assert "on-error" not in text, text

    def test_handler_and_timeout_together(self):
        """Both common keywords emitted at once must not interfere, and both
        must precede ``-> next``."""
        ir = _timeout_ir("http", "PT5S")
        ir["nodes"][1]["errorHandler"] = {"strategy": "abort", "retryTimes": 2}
        text, back = _roundtrip_ir(ir)
        node = _node(back, "n")
        assert node["timeout"] == "PT5S", (text, node)
        assert node["errorHandler"] == {"strategy": "abort", "retryTimes": 2}, (
            text, node,
        )
        assert node["next"] == "e", (text, node)
        node_part = _node_segment(text, "http")
        assert node_part.index("->") > node_part.index(":on-error"), node_part

    def test_full_handler_literal_shape(self):
        """The emitted text must be exactly the nested
        ``:on-error (on-error ...)`` form."""
        ir = _timeout_ir("http", None)
        del ir["nodes"][1]["timeout"]
        ir["nodes"][1]["errorHandler"] = {"strategy": "abort", "retryTimes": 2}
        text = _emit(ir)
        assert ":on-error (on-error abort :retry 2)" in text, text

    def test_handler_without_strategy_defaults_to_abort(self):
        """Hand-written / legacy IR may omit ``strategy``; the writer must fall
        back to the documented default ``abort``.

        ``_compile_error_handler`` always fills ``strategy`` in, so this only
        shows up for IR that reaches ``flow_to_sexpr`` directly (hand-written
        JSON flows). The fallback must not become ``None`` or a cased variant.
        """
        ir = _timeout_ir("http", None)
        del ir["nodes"][1]["timeout"]
        ir["nodes"][1]["errorHandler"] = {"retryTimes": 1}
        text = _emit(ir)
        assert ":on-error (on-error abort :retry 1)" in text, text
        back = compile_sexpr(text)
        assert _node(back, "n")["errorHandler"] == {
            "strategy": "abort", "retryTimes": 1,
        }, (text, _node(back, "n"))


# ===========================================================================
# Must-not-break: maintainer's 7d0add9 + 87e4dd7 (regression corpus)
# ===========================================================================

FILTER_SRC = (
    '(flow f (start :id s -> ev) '
    '(event :id ev :type t :filter (dict :a 1 :b x) -> e) (end :id e))'
)


class TestMaintainerRegressionCorpus:
    """``7d0add9`` (keyword position) and ``87e4dd7`` (filter value) stay green."""

    def test_filter_dict_value_survives_compile(self):
        ir = compile_sexpr(FILTER_SRC)
        text = flow_to_sexpr(ir)
        back = compile_sexpr(text)
        assert _node(ir, "ev")["eventFilter"] == {"a": 1, "b": "x"}
        assert _node(back, "ev")["eventFilter"] == {"a": 1, "b": "x"}, (text, _node(back, "ev"))
        assert _node(back, "ev")["next"] == "e", (text, _node(back, "ev"))

    def test_filter_dict_value_survives_model_validation(self):
        node = _model_node(parse_sexpr(flow_to_sexpr(compile_sexpr(FILTER_SRC))), "ev")
        assert node.event_filter == {"a": 1, "b": "x"}, node.event_filter
        assert node.next == "e"

    def test_keywords_still_precede_arrow(self):
        text = flow_to_sexpr(compile_sexpr(FILTER_SRC))
        ev_part = _node_segment(text, "event")
        assert ev_part.index(":filter") < ev_part.index("->"), ev_part

    def test_event_next_survives(self):
        ir = compile_sexpr('(flow f (start :id s -> ev) (event :id ev :type t -> e) (end :id e))')
        back = compile_sexpr(flow_to_sexpr(ir))
        assert _node(back, "ev")["next"] == "e"

    def test_emitter_is_idempotent(self):
        text = flow_to_sexpr(compile_sexpr(FILTER_SRC))
        assert flow_to_sexpr(compile_sexpr(text)) == text


class TestKeywordOrderingGuard:
    """Keyword position, pinned to what the reader actually does.

    Measured correction (2026-10-13, adversarial review): ``_split_args`` scans
    the whole flat argument list, so a flat ``:kw value`` pair is read on
    **either** side of ``-> next``. Keyword position is therefore not what makes
    a field readable -- a *nested list form* outside a keyword slot is. These
    tests pin both halves, plus the ``if`` / ``switch`` exception where the
    branch keywords legitimately follow the arrow.
    """

    def test_common_keywords_never_follow_arrow(self):
        """The emitter's chosen shape: common keywords precede ``-> next``.

        This is a stability/shape assertion, not a correctness requirement --
        see `test_flat_keyword_after_arrow_is_still_read` for the reader's real
        behaviour.
        """
        cases = [
            ("assignment", _timeout_ir("assignment", "PT10S")),
            ("http", _timeout_ir("http", "PT10S")),
            ("event", _timeout_ir("event", "PT10S")),
        ]
        for marker, ir in cases:
            ir["nodes"][1]["errorHandler"] = {"strategy": "abort", "retryTimes": 2}
            text = _emit(ir)
            seg = _node_segment(text, marker)
            arrow = seg.index("->")
            assert ":" not in seg[arrow:], seg

    def test_flat_keyword_after_arrow_is_still_read(self):
        """A flat ``:kw value`` after ``-> next`` is still a keyword.

        ``_split_args`` walks the whole argument list, so ``:timeout`` /
        ``:filter`` / ``:on-error`` placed after the arrow are recognised
        normally. Pinned so an over-broad reading of ``7d0add9`` ("a keyword
        after ``->`` is a positional") does not lead someone to rewrite the
        reader or to fear the emitter's shape.
        """
        d = compile_sexpr(
            '(flow f (start :id s -> a) (assignment :id a 1 -> e :timeout 5) (end :id e))')
        assert _node(d, "a")["timeout"] == 5, _node(d, "a")

        d = compile_sexpr(
            '(flow f (start :id s -> ev) (event :id ev :type t -> e '
            ':filter (dict :a 1) :on-error (on-error abort :retry 2)) (end :id e))')
        assert _node(d, "ev")["eventFilter"] == {"a": 1}, _node(d, "ev")
        assert _node(d, "ev")["errorHandler"] == {
            "strategy": "abort", "retryTimes": 2}, _node(d, "ev")

    def test_nested_form_outside_keyword_slot_is_not_a_keyword(self):
        """The real degradation: a *nested list form* in a positional slot.

        This is the mechanism behind the pre-#136 corruption; the fix is to
        emit the handler as the **value** of ``:on-error``, not to move
        keywords around.
        """
        with pytest.raises(SyntaxError):
            compile_sexpr(
                '(flow f (start :id s -> a) (assignment :id a 1 -> e '
                '(on-error abort :retry 2)) (end :id e))')

        d = compile_sexpr(
            '(flow f (start :id s -> e) (end :id e :output x '
            '(on-error abort :retry 2)))')
        assert _node(d, "e")["output"] == ["on-error", "abort", "retry", 2], _node(d, "e")

    def test_if_and_switch_branch_keywords_follow_arrow(self):
        """``if`` / ``switch`` are the documented exception.

        Their branch keywords are emitted **after** the arrow by grammar --
        ``(if :id i (cond x eq 1) -> t :else e)`` and
        ``(branch b -> e :when (cond x eq 1))`` -- and the reader accepts them
        there. ``7d0add9``'s generic guard regex only covers
        ``start|end|event|http|assignment|code``, so it never sees these two.
        Pin the real shape and its round-trip.
        """
        if_text = _emit(compile_sexpr(
            '(flow f (start :id s -> i) (if :id i (cond x eq 1) -> t :else e) '
            '(end :id t) (end :id e))'))
        if_seg = _node_segment(if_text, "if")
        assert if_seg.index("->") < if_seg.index(":else"), if_seg
        back = compile_sexpr(if_text)
        assert _node(back, "i")["next"] == "t", _node(back, "i")
        assert _node(back, "i")["else_next"] == "e", _node(back, "i")

        sw_text = _emit(compile_sexpr(
            '(flow f (start :id s -> sw) '
            '(switch :id sw (branch b -> e :when (cond x eq 1)) :default e) '
            '(end :id e))'))
        assert "-> e :when" in sw_text, sw_text
        back = compile_sexpr(sw_text)
        branch = _node(back, "sw")["branches"][0]
        assert branch["next"] == "e", branch
        assert branch["condition"] == {
            "field": "x", "operator": "eq", "value": 1}, branch
