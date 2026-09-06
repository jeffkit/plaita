"""Golden test set for the expression engine.

These tests pin the public behavior of ``plaita.io.evaluate`` /
``parse_function`` so that the unified-parser refactor (C1) can be verified
against an invariant baseline.  They cover:

* plain strings and passthrough of non-string values
* variable paths (root, dotted, bracketed, negative root index, mixed)
* ``{% ... %}`` interpolation (with/without spaces, multi-line, multi-match,
  wrong-prefix passthrough, unclosed-brace passthrough)
* function calls (flat, nested, with variable args, with string/number/bool
  args, trailing-comma default, variadic)
* unknown-function fallback (``"undefined"``)
* custom / scoped registry resolution
* non-string containers (list / dict recursion)

Only supported, non-quirk behaviors are locked here.  Edge cases that are
historical bugs (e.g. negative index on a non-root bracket segment) are
intentionally omitted so the refactor is free to fix them.
"""

import unittest

from plaita.io import evaluate, parse_function
from plaita.core.expression import (
    ExpressionEvaluator,
    ExpressionRegistry,
    FunctionCategory,
)


class GoldenPlainAndPassthrough(unittest.TestCase):
    def test_plain_string(self):
        self.assertEqual(evaluate("hello", None), "hello")

    def test_bare_braces_passthrough(self):
        # "{...}" without {% ... %} is NOT interpolation
        self.assertEqual(evaluate("{user.name}", {"user": {"name": "tom"}}), "{user.name}")

    def test_int_passthrough(self):
        self.assertEqual(evaluate(42, {}), 42)

    def test_none_passthrough(self):
        self.assertIsNone(evaluate(None, {}))

    def test_bool_passthrough(self):
        self.assertIs(evaluate(True, {}), True)

    def test_list_recursion(self):
        ctx = {"$INPUT": 42}
        self.assertEqual(evaluate(["$INPUT", "static"], ctx), [42, "static"])

    def test_dict_recursion(self):
        ctx = {"$INPUT": {"name": "Alice"}}
        self.assertEqual(evaluate({"greeting": "$INPUT.name"}, ctx), {"greeting": "Alice"})

    def test_nested_dict_recursion(self):
        ctx = {"$INPUT": {"a": {"b": 1}}}
        self.assertEqual(evaluate({"outer": {"inner": "$INPUT.a.b"}}, ctx),
                         {"outer": {"inner": 1}})


class GoldenVariablePaths(unittest.TestCase):
    def test_root_variable(self):
        self.assertEqual(evaluate("$INPUT", {"$INPUT": "hello"}), "hello")

    def test_root_bracket_index(self):
        self.assertEqual(evaluate("$INPUT[0]", {"$INPUT": ["hello"]}), "hello")

    def test_root_dot_digit_index(self):
        self.assertEqual(evaluate("$INPUT.0", {"$INPUT": ["hello"]}), "hello")

    def test_root_bracket_then_field(self):
        ctx = {"$INPUT": [{"name": "hello"}]}
        self.assertEqual(evaluate("$INPUT[0].name", ctx), "hello")

    def test_root_negative_bracket_then_field(self):
        ctx = {"$INPUT": [{"name": "hello"}]}
        self.assertEqual(evaluate("$INPUT[-1].name", ctx), "hello")

    def test_field_then_bracket(self):
        ctx = {"$INPUT": {"names": ["hello"]}}
        self.assertEqual(evaluate("$INPUT.names[0]", ctx), "hello")

    def test_root_dot_digit_then_field(self):
        ctx = {"$INPUT": [{"name": "hello"}]}
        self.assertEqual(evaluate("$INPUT.0.name", ctx), "hello")

    def test_field_then_dot_digit(self):
        ctx = {"$INPUT": {"users": [{"name": "hello"}]}}
        self.assertEqual(evaluate("$INPUT.users.0", ctx), {"name": "hello"})

    def test_deep_dotted_path(self):
        ctx = {"$INPUT": {"a": {"b": {"c": "deep"}}}}
        self.assertEqual(evaluate("$INPUT.a.b.c", ctx), "deep")

    def test_missing_nested_returns_none(self):
        ctx = {"$INPUT": {}}
        self.assertIsNone(evaluate("$INPUT.missing", ctx), None)

    def test_node_path_multiline(self):
        ctx = {"$NODE": {"step1": {"data": [{"url": "http://example.com"}]}}}
        express = "img: ![]({%$NODE.step1.data.0.url%})"
        self.assertEqual(evaluate(express, ctx), "img: ![](http://example.com)")


class GoldenInterpolation(unittest.TestCase):
    def test_basic_interpolation(self):
        ctx = {"$INPUT": [{"name": "hello"}]}
        self.assertEqual(evaluate("my name is {%$INPUT[0].name%}", ctx), "my name is hello")

    def test_interpolation_with_spaces(self):
        ctx = {"$INPUT": 3}
        self.assertEqual(evaluate("v: {% $INPUT %}", ctx), "v: 3")

    def test_wrong_prefix_passthrough(self):
        ctx = {"$INPUT": [{"name": "hello"}]}
        self.assertEqual(evaluate("x {%@INPUT[0].name%} y", ctx), "x {%@INPUT[0].name%} y")

    def test_no_prefix_inside_braces_passthrough(self):
        # {%world%} does not start with $ -> not interpolated
        self.assertEqual(evaluate("hi {%world%}", {"world": "x"}), "hi {%world%}")

    def test_function_inside_interpolation(self):
        ctx = {"$INPUT": 3}
        self.assertEqual(evaluate("let see: {%$F.add(3, $INPUT)%}", ctx), "let see: 6")
        self.assertEqual(evaluate("let see: {% $F.add(3, $INPUT) %}", ctx), "let see: 6")

    def test_multi_interpolation(self):
        ctx = {"$A": "x", "$B": "y"}
        self.assertEqual(evaluate("{%$A%} and {%$B%}", ctx), "x and y")

    def test_unclosed_brace_passthrough(self):
        self.assertEqual(evaluate("a {% b", {}), "a {% b")
        ctx = {"$INPUT": "x"}
        self.assertEqual(evaluate("a {% $INPUT %} b {%", ctx), "a x b {%")


class GoldenFunctionCalls(unittest.TestCase):
    def test_flat_add(self):
        self.assertEqual(evaluate("$F.add(1, 2)", {}), 3)

    def test_flat_add_spaces(self):
        self.assertEqual(evaluate("$F.add( 1, 2 )", {}), 3)

    def test_nested_function(self):
        self.assertEqual(evaluate("$F.add($F.add(1, 2), 3)", {}), 6)

    def test_nested_function_deep(self):
        self.assertEqual(evaluate("$F.add($F.mul(2, 3), 4)", {}), 10)

    def test_nested_function_with_variable(self):
        ctx = {"$INPUT": {"value": 3}}
        self.assertEqual(evaluate("$F.add($F.add(1, 2), $INPUT.value)", ctx), 6)

    def test_function_with_root_variable_arg(self):
        self.assertEqual(evaluate("$F.add(3, $INPUT)", {"$INPUT": 3}), 6)

    def test_function_with_string_args(self):
        self.assertEqual(evaluate('$F.concat("1", "2", "3", "4")', {}), "1234")

    def test_function_with_deep_variable_arg(self):
        ctx = {"$INPUT": {"test": {"value": 3}}}
        self.assertEqual(evaluate("$F.sub($INPUT.test.value, 2)", ctx), 1)

    def test_function_default_arg(self):
        ctx = {"$INPUT": {"test": {"value": 3}}}
        self.assertEqual(evaluate("$F.getDictValue($INPUT.test, 'value1', 2)", ctx), 2)

    def test_function_no_default(self):
        ctx = {"$INPUT": {"test": {"value": 3}}}
        self.assertEqual(evaluate("$F.getDictValue($INPUT.test, 'value')", ctx), 3)

    def test_function_trailing_comma_default(self):
        ctx = {"$INPUT": {"test": {"value": 3}}}
        self.assertEqual(evaluate("$F.getDictValue($INPUT.test, 'value',)", ctx), 3)

    def test_function_variadic_with_variable(self):
        ctx = {"$INPUT": {"test": {"value": "1"}}}
        self.assertEqual(evaluate("$F.concat($INPUT.test.value, '2', '3')", ctx), "123")

    def test_function_or_variadic(self):
        ctx = {"$INPUT": {"test": {"value": ""}}}
        self.assertEqual(evaluate("$F.or($INPUT.test.value, 0, '3')", ctx), "3")

    def test_unknown_function_returns_undefined(self):
        """默认注册表未命中 → NameError 带 did-you-mean（LLM 作者模拟 P0-3：
        返回 'undefined' 字符串会让错误值静默流入下游）；scoped registry 仍返回
        'undefined'（既有契约）。"""
        from plaita.core.expression import ExpressionRegistry
        with self.assertRaises(NameError) as cm:
            evaluate("$F.does_not_exist(1, 2)", {})
        self.assertIn("does_not_exist", str(cm.exception))

        scoped = ExpressionRegistry()
        self.assertEqual(evaluate("$F.does_not_exist(1, 2)", {}, registry=scoped), "undefined")

    def test_parse_function_non_function_returns_input(self):
        # parse_function only handles $F.* ; a bare variable path is returned as-is
        self.assertEqual(parse_function("$INPUT.value", {}, "$"), "$INPUT.value")

    def test_parse_function_nested(self):
        self.assertEqual(parse_function("$F.add($F.add(1, 2), $F.mul(3, 4))", {}, "$"), 15)


class GoldenScopedRegistry(unittest.TestCase):
    def test_custom_registry_resolves(self):
        reg = ExpressionRegistry()
        reg.register("double", lambda x: x * 2, FunctionCategory.MATH)
        evaluator = ExpressionEvaluator(registry=reg)
        self.assertEqual(evaluator.evaluate("$F.double(21)", {}), 42)

    def test_custom_registry_falls_back_to_undefined(self):
        reg = ExpressionRegistry()
        reg.register("double", lambda x: x * 2, FunctionCategory.MATH)
        evaluator = ExpressionEvaluator(registry=reg)
        # 'add' is not in the scoped registry -> undefined sentinel
        self.assertEqual(evaluator.evaluate("$F.add(1, 2)", {}), "undefined")

    def test_default_registry_builtin(self):
        evaluator = ExpressionEvaluator()
        self.assertEqual(evaluator.evaluate('$F.upper("hello")', {}), "HELLO")


if __name__ == "__main__":
    unittest.main()
