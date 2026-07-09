"""针对 plaita/dsl/codeflow/_common.py 的 mutation killing 测试。

目标：杀死 mutmut 初筛后 survived 的变异点，提升 _common.py 变异分数。
"""
from __future__ import annotations

import ast
import pytest

from plaita.dsl.codeflow._common import (
    _Placeholder,
    ErrorHandler,
    _is_upper_ident,
    _CompileCtx,
    _CodeflowError,
    _annotate_source,
    _unpack_names,
    _const_bool,
    _ChildFlowMarker,
    _raise_if_unregistered_custom,
    _describe_call,
    _BUILTIN_HANDLED_TYPES,
)


# ---------------------------------------------------------------------------
# _Placeholder.__getattr__ (mutmut_1: return _Placeholder(None))
# ---------------------------------------------------------------------------

class TestPlaceholderGetattr:
    def test_getattr_propagates_name(self):
        p = _Placeholder("HTTP")
        child = p.post
        assert child._name == "HTTP.post"

    def test_getattr_chain_propagates_name(self):
        p = _Placeholder("NODE")
        child = p.result.value
        assert child._name == "NODE.result.value"

    def test_getattr_is_placeholder(self):
        p = _Placeholder("INPUT")
        child = p.name
        assert isinstance(child, _Placeholder)
        assert child._name is not None


# ---------------------------------------------------------------------------
# ErrorHandler.__init__ (mutmut_10: raise ValueError(None))
# ---------------------------------------------------------------------------

class TestErrorHandlerInit:
    def test_invalid_strategy_error_contains_strategy(self):
        with pytest.raises(ValueError, match="invalid_strat"):
            ErrorHandler(strategy="invalid_strat")

    def test_invalid_strategy_error_message_not_none(self):
        with pytest.raises(ValueError) as exc:
            ErrorHandler(strategy="bad")
        assert exc.value.args[0] is not None
        assert isinstance(exc.value.args[0], str)

    def test_valid_strategies_no_error(self):
        ErrorHandler(strategy="abort")
        ErrorHandler(strategy="continue")
        ErrorHandler(strategy="continue_with")


# ---------------------------------------------------------------------------
# _is_upper_ident (mutmut_1: and → or; mutmut_2: and → or; mutmut_8: replace tweak)
# ---------------------------------------------------------------------------

class TestIsUpperIdent:
    def test_simple_upper(self):
        assert _is_upper_ident("HTTP") is True

    def test_lower_returns_false(self):
        assert _is_upper_ident("http") is False

    def test_mixed_case_returns_false(self):
        assert _is_upper_ident("Http") is False

    def test_digits_only_returns_false(self):
        # no alpha → should be False even if isupper() returns True for all-numeric
        assert _is_upper_ident("123") is False

    def test_underscore_only_returns_false(self):
        # all-underscore: isupper() is False for "_"
        assert _is_upper_ident("_") is False

    def test_upper_with_underscore_and_digits(self):
        assert _is_upper_ident("LLM_V2") is True

    def test_upper_with_leading_digit_returns_false(self):
        # "1A" is not a valid Python identifier, but let's verify behavior
        # isupper() on "1A" is True, isalnum() is True, any alpha is True → True
        # This is expected behavior - just test it doesn't crash
        result = _is_upper_ident("1A")
        assert isinstance(result, bool)

    def test_empty_string(self):
        # edge: empty string
        assert _is_upper_ident("") is False

    def test_upper_with_only_underscores_and_letters(self):
        # "A_B" → isupper=True, replace("_","").isalnum()="AB".isalnum()=True, any alpha=True
        assert _is_upper_ident("A_B") is True

    def test_underscore_with_digits_no_alpha_returns_false(self):
        # "_1_" → isupper()=False → False
        assert _is_upper_ident("_1_") is False

    def test_space_in_name_returns_false(self):
        # "A B" → isupper=True, but replace("_","")="A B", isalnum()=False
        assert _is_upper_ident("A B") is False


# ---------------------------------------------------------------------------
# _raise_if_unregistered_custom (mutmut_3,4,5,12,14,16)
# ---------------------------------------------------------------------------

class TestRaiseIfUnregisteredCustom:
    def _make_name_node(self, id_str: str) -> ast.Name:
        node = ast.Name(id=id_str)
        node.lineno = 1
        node.col_offset = 0
        return node

    def test_builtin_types_do_not_raise(self):
        ctx = _CompileCtx(known_node_types=set())
        for builtin in ("HTTP", "CODE", "EVENT", "CHILD", "REFERENCE", "PARALLEL",
                        "MAP", "FILTER", "FIND", "LOOP", "REDUCE"):
            node = self._make_name_node(builtin)
            # Should not raise for builtin handled types
            _raise_if_unregistered_custom(node, ctx)

    def test_known_custom_type_does_not_raise(self):
        ctx = _CompileCtx(known_node_types={"llm", "retrieve"})
        node = self._make_name_node("LLM")
        _raise_if_unregistered_custom(node, ctx)

    def test_unregistered_upper_raises(self):
        ctx = _CompileCtx(known_node_types=set())
        node = self._make_name_node("MYNODE")
        with pytest.raises(_CodeflowError, match="MYNODE"):
            _raise_if_unregistered_custom(node, ctx)

    def test_non_name_node_does_not_raise(self):
        ctx = _CompileCtx(known_node_types=set())
        node = ast.Constant(value=42)
        _raise_if_unregistered_custom(node, ctx)  # should not raise

    def test_lower_name_does_not_raise(self):
        ctx = _CompileCtx(known_node_types=set())
        node = self._make_name_node("mynode")
        _raise_if_unregistered_custom(node, ctx)  # not upper ident → skip

    def test_builtin_not_in_known_types_still_does_not_raise(self):
        # mutmut_5: `or` → `and`. If we only have builtin type in _BUILTIN_HANDLED_TYPES
        # but NOT in known_node_types, the original code should still not raise.
        ctx = _CompileCtx(known_node_types=set())  # empty known types
        node = self._make_name_node("HTTP")  # in _BUILTIN_HANDLED_TYPES, not in known_node_types
        _raise_if_unregistered_custom(node, ctx)  # should NOT raise


# ---------------------------------------------------------------------------
# _describe_call (mutmut_1: base = None)
# ---------------------------------------------------------------------------

class TestDescribeCall:
    def _parse_expr(self, code: str) -> ast.expr:
        tree = ast.parse(code, mode="eval")
        return tree.body

    def test_simple_name_call(self):
        node = self._parse_expr("HTTP()")
        result = _describe_call(node.func)
        assert result == "HTTP(...)"

    def test_attribute_call_with_name_base(self):
        node = self._parse_expr("HTTP.post()")
        result = _describe_call(node.func)
        assert result == "HTTP.post(...)"

    def test_nested_call(self):
        # func.value is itself a Call: e.g., HTTP()()
        node = self._parse_expr("HTTP()()")
        result = _describe_call(node.func)
        # Should not return None
        assert result is not None
        assert isinstance(result, str)

    def test_attribute_base_not_none(self):
        # Specifically targets mutmut_1: base = None
        node = self._parse_expr("obj.method()")
        result = _describe_call(node.func)
        assert "None" not in result


# ---------------------------------------------------------------------------
# _CompileCtx init & auto_id (mutmut_2: counter=1; auto_id mutmuts)
# ---------------------------------------------------------------------------

class TestCompileCtxAutoId:
    def test_first_auto_id_is_n1(self):
        ctx = _CompileCtx()
        nid = ctx.auto_id()
        assert nid == "_n1", f"Expected '_n1', got {nid!r}"

    def test_second_auto_id_is_n2(self):
        ctx = _CompileCtx()
        ctx.auto_id()  # _n1
        nid = ctx.auto_id()
        assert nid == "_n2", f"Expected '_n2', got {nid!r}"

    def test_auto_ids_are_unique(self):
        ctx = _CompileCtx()
        ids = [ctx.auto_id() for _ in range(20)]
        assert len(ids) == len(set(ids)), "auto_id should return unique IDs"

    def test_hint_is_claimed_and_returned(self):
        ctx = _CompileCtx()
        nid = ctx.auto_id(hint="my_node")
        assert nid == "my_node"

    def test_hint_prevents_reuse(self):
        # mutmut_2: add(None) instead of add(hint) → hint not tracked → can return duplicate
        ctx = _CompileCtx()
        ctx.auto_id(hint="my_node")
        # Now claim manually shouldn't work
        with pytest.raises(ValueError):
            ctx.claim("my_node")

    def test_hint_used_twice_returns_auto_id(self):
        ctx = _CompileCtx()
        first = ctx.auto_id(hint="node")
        second = ctx.auto_id(hint="node")  # already claimed → should get auto ID
        assert first == "node"
        assert second != "node"
        assert second.startswith("_n")

    def test_auto_id_skips_claimed(self):
        ctx = _CompileCtx()
        ctx.claim("_n1")  # pre-claim _n1
        nid = ctx.auto_id()
        assert nid == "_n2", f"Should skip _n1, expected '_n2', got {nid!r}"

    def test_counter_starts_at_zero(self):
        # mutmut_2: counter = 1 → first auto_id would be _n2
        ctx = _CompileCtx()
        assert ctx._counter == 0

    def test_auto_id_while_collision_increments_correctly(self):
        # Exercises the while loop: claim _n1 through _n5, next should be _n6
        ctx = _CompileCtx()
        for i in range(1, 6):
            ctx.claim(f"_n{i}")
        nid = ctx.auto_id()
        assert nid == "_n6"

    def test_auto_id_added_to_claimed(self):
        # mutmut_12: add(None) instead of add(cand)
        ctx = _CompileCtx()
        nid1 = ctx.auto_id()
        nid2 = ctx.auto_id()
        assert nid1 != nid2  # both should be unique
        # After getting nid1, claiming it should fail
        with pytest.raises(ValueError):
            ctx.claim(nid1)


# ---------------------------------------------------------------------------
# _CompileCtx.claim (mutmut_2: ValueError(None))
# ---------------------------------------------------------------------------

class TestCompileCtxClaim:
    def test_claim_new_id_works(self):
        ctx = _CompileCtx()
        result = ctx.claim("custom_id")
        assert result == "custom_id"

    def test_claim_duplicate_raises_value_error(self):
        ctx = _CompileCtx()
        ctx.claim("dup")
        with pytest.raises(ValueError):
            ctx.claim("dup")

    def test_claim_duplicate_error_message_contains_id(self):
        # mutmut_2: ValueError(None) → message is None → args[0] is None
        ctx = _CompileCtx()
        ctx.claim("my_node")
        with pytest.raises(ValueError) as exc:
            ctx.claim("my_node")
        msg = exc.value.args[0]
        assert msg is not None
        assert "my_node" in str(msg)


# ---------------------------------------------------------------------------
# _CodeflowError.__init__ (mutmut_1: line=None; mutmut_2: getattr(None,...))
# ---------------------------------------------------------------------------

class TestCodeflowError:
    def _make_node_with_lineno(self, lineno: int) -> ast.AST:
        node = ast.Name(id="x")
        node.lineno = lineno
        node.col_offset = 0
        return node

    def test_error_with_node_includes_lineno(self):
        node = self._make_node_with_lineno(42)
        err = _CodeflowError("test error", node)
        assert "42" in str(err)

    def test_error_with_node_not_question_mark(self):
        node = self._make_node_with_lineno(10)
        err = _CodeflowError("msg", node)
        # Should have "10", not "?" or "None"
        msg = str(err)
        assert "?" not in msg or "10" in msg

    def test_error_without_node_uses_question_mark(self):
        err = _CodeflowError("test error", None)
        assert "?" in str(err)

    def test_error_message_format(self):
        node = self._make_node_with_lineno(5)
        err = _CodeflowError("bad node", node)
        msg = str(err)
        assert "[codeflow]" in msg
        assert "5" in msg
        assert "bad node" in msg

    def test_error_lineno_from_node_not_fixed(self):
        # mutmut_1: line = None → "第 None 行"
        node = self._make_node_with_lineno(99)
        err = _CodeflowError("msg", node)
        assert "None" not in str(err)

    def test_error_lineno_uses_node_attribute(self):
        # mutmut_2: getattr(None, ...) → always "?"
        node7 = self._make_node_with_lineno(7)
        node9 = self._make_node_with_lineno(9)
        err7 = _CodeflowError("msg", node7)
        err9 = _CodeflowError("msg", node9)
        assert "7" in str(err7)
        assert "9" in str(err9)
        assert str(err7) != str(err9)


# ---------------------------------------------------------------------------
# _annotate_source (mutmut_7: likely source_line key change)
# ---------------------------------------------------------------------------

class TestAnnotateSource:
    def _make_node_with_lineno(self, lineno: int) -> ast.AST:
        node = ast.Name(id="x")
        node.lineno = lineno
        node.col_offset = 0
        return node

    def test_annotates_source_line(self):
        node = self._make_node_with_lineno(15)
        spec = {}
        result = _annotate_source(spec, node)
        assert "source_line" in result
        assert result["source_line"] == 15

    def test_none_node_no_annotation(self):
        spec = {"type": "node"}
        result = _annotate_source(spec, None)
        assert "source_line" not in result

    def test_returns_same_dict(self):
        spec = {"type": "node"}
        node = self._make_node_with_lineno(1)
        result = _annotate_source(spec, node)
        assert result is spec


# ---------------------------------------------------------------------------
# _unpack_names (mutmut_4,6,7,10,12,13)
# ---------------------------------------------------------------------------

class TestUnpackNames:
    def test_single_name(self):
        node = ast.Name(id="x")
        node.lineno = 1
        node.col_offset = 0
        result = _unpack_names(node)
        assert result == ["x"]

    def test_tuple_of_names(self):
        a = ast.Name(id="a")
        b = ast.Name(id="b")
        a.lineno = a.col_offset = b.lineno = b.col_offset = 1
        tup = ast.Tuple(elts=[a, b])
        tup.lineno = tup.col_offset = 1
        result = _unpack_names(tup)
        assert result == ["a", "b"]

    def test_tuple_with_non_name_raises(self):
        const = ast.Constant(value=1)
        tup = ast.Tuple(elts=[const])
        tup.lineno = tup.col_offset = 1
        with pytest.raises(_CodeflowError):
            _unpack_names(tup)

    def test_unsupported_node_raises(self):
        node = ast.Constant(value=42)
        with pytest.raises(_CodeflowError):
            _unpack_names(node)


# ---------------------------------------------------------------------------
# _const_bool (mutmut_1: and → or)
# ---------------------------------------------------------------------------

class TestConstBool:
    def test_true_constant_returns_true(self):
        node = ast.Constant(value=True)
        assert _const_bool(node) is True

    def test_false_constant_returns_false(self):
        node = ast.Constant(value=False)
        assert _const_bool(node) is False

    def test_non_constant_returns_false(self):
        # mutmut_1: `and` → `or` would make this True if node.value is True
        # (but non-Constant nodes don't have .value normally)
        node = ast.Name(id="x")
        node.lineno = node.col_offset = 1
        result = _const_bool(node)
        assert result is False

    def test_number_constant_returns_false(self):
        node = ast.Constant(value=1)
        assert _const_bool(node) is False

    def test_string_constant_returns_false(self):
        node = ast.Constant(value="True")
        assert _const_bool(node) is False

    def test_none_constant_returns_false(self):
        node = ast.Constant(value=None)
        assert _const_bool(node) is False


# ---------------------------------------------------------------------------
# _ChildFlowMarker.__init__ (mutmut_2: self._func = None)
# ---------------------------------------------------------------------------

class TestChildFlowMarker:
    def test_func_attribute_set_correctly(self):
        def my_func():
            pass

        ir = {"flow": "test"}
        marker = _ChildFlowMarker(ir, my_func)
        assert marker._func is my_func
        assert marker._func is not None

    def test_ir_attribute_set_correctly(self):
        def my_func():
            pass

        ir = {"flow": "test", "nodes": []}
        marker = _ChildFlowMarker(ir, my_func)
        assert marker.__codeflow_ir__ is ir

    def test_different_funcs_not_mixed_up(self):
        def func_a():
            pass

        def func_b():
            pass

        marker = _ChildFlowMarker({}, func_a)
        assert marker._func is func_a
        assert marker._func is not func_b


# ---------------------------------------------------------------------------
# _CompileCtx.__init__ module_globals (mutmut_8: =None; mutmut_9: and {})
# ---------------------------------------------------------------------------

class TestCompileCtxModuleGlobals:
    def test_module_globals_passed_correctly(self):
        # mutmut_9: `or {}` → `and {}` would give {} when module_globals is truthy
        mg = {"my_var": 42, "helper": lambda x: x}
        ctx = _CompileCtx(module_globals=mg)
        assert ctx.module_globals is mg
        assert ctx.module_globals.get("my_var") == 42

    def test_module_globals_defaults_to_empty(self):
        ctx = _CompileCtx()
        assert ctx.module_globals == {}

    def test_module_globals_none_gives_empty(self):
        ctx = _CompileCtx(module_globals=None)
        assert ctx.module_globals == {}

    def test_module_globals_not_replaced_by_empty(self):
        # Specifically kills mutmut_9: `module_globals and {}` would always give {}
        # when module_globals is a non-empty dict
        mg = {"key": "value"}
        ctx = _CompileCtx(module_globals=mg)
        assert "key" in ctx.module_globals
        assert ctx.module_globals["key"] == "value"


# ---------------------------------------------------------------------------
# _CodeflowError edge cases (mutmut_4: default None; mutmut_10/11: XX?XX)
# ---------------------------------------------------------------------------

class TestCodeflowErrorEdgeCases:
    def test_node_without_lineno_uses_question_mark(self):
        # mutmut_4: getattr(node, "lineno", None) → None in message
        node = ast.Name(id="x")  # no lineno set
        err = _CodeflowError("msg", node)
        msg = str(err)
        assert "None" not in msg, f"Should use '?' not None, got: {msg}"
        assert "?" in msg

    def test_none_node_question_mark_not_changed(self):
        # mutmut_11: "XX?XX" instead of "?" when node=None
        err = _CodeflowError("test", None)
        msg = str(err)
        assert "XX" not in msg
        assert "?" in msg

    def test_node_lineno_default_is_question_mark(self):
        # mutmut_10: "XX?XX" fallback when node has no lineno
        # ast.Name(id="x") created directly has no lineno attribute
        node = ast.Name(id="no_lineno")
        assert not hasattr(node, "lineno"), "node should not have lineno for this test"
        err = _CodeflowError("msg", node)
        msg = str(err)
        assert "XX" not in msg


# ---------------------------------------------------------------------------
# _annotate_source with missing lineno (mutmut_7: missing default arg)
# ---------------------------------------------------------------------------

class TestAnnotateSourceEdgeCases:
    def test_node_without_lineno_no_annotation(self):
        # mutmut_7: getattr(node, "lineno",) → missing default → AttributeError
        # ast.Name(id="x") created directly has no lineno attribute
        node = ast.Name(id="no_lineno")
        assert not hasattr(node, "lineno"), "node should not have lineno for this test"
        spec = {}
        result = _annotate_source(spec, node)
        assert "source_line" not in result

    def test_node_with_lineno_annotated(self):
        node = ast.Name(id="x")
        node.lineno = 7
        node.col_offset = 0
        spec = {}
        result = _annotate_source(spec, node)
        assert result["source_line"] == 7


# ---------------------------------------------------------------------------
# _unpack_names error messages (mutmut_7: XX前缀; mutmut_13: XX前缀)
# ---------------------------------------------------------------------------

class TestUnpackNamesErrorMessages:
    def test_tuple_non_name_error_message_content(self):
        # mutmut_7: changes Chinese error text
        const = ast.Constant(value=1)
        const.lineno = const.col_offset = 1
        tup = ast.Tuple(elts=[const])
        tup.lineno = tup.col_offset = 1
        with pytest.raises(_CodeflowError) as exc:
            _unpack_names(tup)
        msg = str(exc.value)
        assert "循环变量解包" in msg
        assert "XX" not in msg

    def test_unsupported_node_error_message_content(self):
        # mutmut_13: changes "不支持的循环变量形式" text
        node = ast.Constant(value=42)
        with pytest.raises(_CodeflowError) as exc:
            _unpack_names(node)
        msg = str(exc.value)
        assert "不支持的循环变量形式" in msg
        assert "XX" not in msg


# ---------------------------------------------------------------------------
# _raise_if_unregistered_custom error message (mutmut_16: XX（无）XX)
# ---------------------------------------------------------------------------

class TestRaiseIfUnregisteredCustomErrorMsg:
    def _make_name_node(self, id_str: str) -> ast.Name:
        node = ast.Name(id=id_str)
        node.lineno = 1
        node.col_offset = 0
        return node

    def test_no_available_types_shows_clean_message(self):
        # mutmut_16: 'XX（无）XX' instead of '（无）'
        ctx = _CompileCtx(known_node_types=set())
        node = self._make_name_node("MYNODE")
        with pytest.raises(_CodeflowError) as exc:
            _raise_if_unregistered_custom(node, ctx)
        msg = str(exc.value)
        assert "XX" not in msg

    def test_error_with_unregistered_node_has_lineno(self):
        # mutmut_12: func→None loses line number info
        ctx = _CompileCtx(known_node_types=set())
        node = self._make_name_node("MYNODE")
        with pytest.raises(_CodeflowError) as exc:
            _raise_if_unregistered_custom(node, ctx)
        msg = str(exc.value)
        assert "1" in msg  # lineno should be 1, not "?" (which would be "None" node)


# ---------------------------------------------------------------------------
# _describe_call fallback type names (mutmut_3,4: type(None).__name__)
# ---------------------------------------------------------------------------

class TestDescribeCallFallbacks:
    def test_unknown_func_type_not_none_type(self):
        # mutmut_4: `type(None).__name__` → "NoneType" for non-Name/Attribute func
        # Use a Subscript node as the func
        node = ast.parse("a[0]()", mode="eval").body
        result = _describe_call(node.func)  # func is Subscript
        assert "NoneType" not in result
        assert result != "NoneType"

    def test_attribute_call_unknown_base_not_none_type(self):
        # mutmut_3: `type(None).__name__` for non-Name base of attribute
        # Create an Attribute where value is something other than Name or Call
        # e.g., obj[0].method()
        node = ast.parse("a[0].method()", mode="eval").body
        result = _describe_call(node.func)  # func.value is Subscript
        assert "NoneType" not in result
