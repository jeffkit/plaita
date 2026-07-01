"""Unit tests for plaita.core.expression — Expression Engine (Phase 4)."""

import json
import warnings
from datetime import date, datetime
from unittest import TestCase

from plaita.core.expression import (
    ExpressionEvaluator,
    ExpressionRegistry,
    FunctionCategory,
    FunctionDescriptor,
    get_default_expression_registry,
)


# ---------------------------------------------------------------------------
# T061 — FunctionCategory enum and FunctionDescriptor dataclass
# ---------------------------------------------------------------------------


class TestFunctionCategory(TestCase):

    def test_all_categories_exist(self):
        expected = {"MATH", "STRING", "LOGIC", "ARRAY", "DICT", "DATETIME", "JSON", "TYPE"}
        self.assertEqual(expected, {c.name for c in FunctionCategory})

    def test_category_values(self):
        self.assertEqual(FunctionCategory.MATH.value, "math")
        self.assertEqual(FunctionCategory.STRING.value, "string")
        self.assertEqual(FunctionCategory.LOGIC.value, "logic")
        self.assertEqual(FunctionCategory.ARRAY.value, "array")
        self.assertEqual(FunctionCategory.DICT.value, "dict")
        self.assertEqual(FunctionCategory.DATETIME.value, "datetime")
        self.assertEqual(FunctionCategory.JSON.value, "json")
        self.assertEqual(FunctionCategory.TYPE.value, "type")

    def test_category_is_str_enum(self):
        self.assertIsInstance(FunctionCategory.MATH, str)


class TestFunctionDescriptor(TestCase):

    def test_creation(self):
        fn = lambda a, b: a + b
        desc = FunctionDescriptor(
            name="add", func=fn, category=FunctionCategory.MATH,
        )
        self.assertEqual(desc.name, "add")
        self.assertIs(desc.func, fn)
        self.assertEqual(desc.category, FunctionCategory.MATH)
        self.assertFalse(desc.has_side_effects)
        self.assertEqual(desc.description, "")

    def test_frozen(self):
        fn = lambda a: a
        desc = FunctionDescriptor(name="test", func=fn, category=FunctionCategory.STRING)
        with self.assertRaises(AttributeError):
            desc.name = "changed"

    def test_with_side_effects(self):
        fn = lambda a, b: a.pop(b)
        desc = FunctionDescriptor(
            name="pop", func=fn, category=FunctionCategory.ARRAY,
            has_side_effects=True, description="Pop item",
        )
        self.assertTrue(desc.has_side_effects)
        self.assertEqual(desc.description, "Pop item")


# ---------------------------------------------------------------------------
# T060 — ExpressionRegistry
# ---------------------------------------------------------------------------


class TestExpressionRegistry(TestCase):

    def _make_registry(self):
        reg = ExpressionRegistry()
        reg.register("add", lambda a, b: a + b, FunctionCategory.MATH, description="Add")
        reg.register("lower", lambda a: a.lower(), FunctionCategory.STRING)
        reg.register("pop", lambda a, b: a.pop(b), FunctionCategory.ARRAY, has_side_effects=True)
        return reg

    def test_register_and_get(self):
        reg = self._make_registry()
        desc = reg.get("add")
        self.assertIsNotNone(desc)
        self.assertEqual(desc.name, "add")
        self.assertEqual(desc.category, FunctionCategory.MATH)
        self.assertEqual(desc.func(3, 4), 7)

    def test_get_nonexistent(self):
        reg = ExpressionRegistry()
        self.assertIsNone(reg.get("nope"))

    def test_get_callable(self):
        reg = self._make_registry()
        fn = reg.get_callable("lower")
        self.assertEqual(fn("HELLO"), "hello")

    def test_get_callable_nonexistent(self):
        reg = ExpressionRegistry()
        self.assertIsNone(reg.get_callable("nope"))

    def test_register_empty_name(self):
        reg = ExpressionRegistry()
        with self.assertRaises(ValueError):
            reg.register("", lambda: None, FunctionCategory.MATH)

    def test_register_duplicate(self):
        reg = ExpressionRegistry()
        reg.register("add", lambda a, b: a + b, FunctionCategory.MATH)
        with self.assertRaises(ValueError):
            reg.register("add", lambda a, b: a + b, FunctionCategory.MATH)

    def test_by_category(self):
        reg = self._make_registry()
        math_fns = reg.by_category(FunctionCategory.MATH)
        self.assertEqual(len(math_fns), 1)
        self.assertEqual(math_fns[0].name, "add")
        string_fns = reg.by_category(FunctionCategory.STRING)
        self.assertEqual(len(string_fns), 1)

    def test_side_effect_functions(self):
        reg = self._make_registry()
        se = reg.side_effect_functions()
        self.assertEqual(len(se), 1)
        self.assertEqual(se[0].name, "pop")
        self.assertTrue(se[0].has_side_effects)

    def test_all_functions(self):
        reg = self._make_registry()
        all_fns = reg.all_functions()
        self.assertEqual(len(all_fns), 3)
        self.assertIn("add", all_fns)
        self.assertIn("lower", all_fns)
        self.assertIn("pop", all_fns)

    def test_contains(self):
        reg = self._make_registry()
        self.assertIn("add", reg)
        self.assertNotIn("xyz", reg)

    def test_len(self):
        reg = self._make_registry()
        self.assertEqual(len(reg), 3)


# ---------------------------------------------------------------------------
# T062 — Regression: all 90+ functions through refactored engine
# ---------------------------------------------------------------------------


class TestDefaultRegistryCompleteness(TestCase):
    """Verify all 90+ built-in functions are registered and produce
    identical results to the original REGISTERED_FUNCTIONS dict."""

    def setUp(self):
        self.reg = get_default_expression_registry()

    def test_math_functions(self):
        self.assertEqual(self.reg.get_callable("add")(5, 3), 8)
        self.assertEqual(self.reg.get_callable("sub")(5, 3), 2)
        self.assertEqual(self.reg.get_callable("mul")(5, 3), 15)
        self.assertEqual(self.reg.get_callable("div")(6, 3), 2)
        self.assertEqual(self.reg.get_callable("mod")(5, 3), 2)
        self.assertEqual(self.reg.get_callable("pow")(2, 3), 8)
        self.assertEqual(self.reg.get_callable("abs")(-5), 5)
        self.assertEqual(self.reg.get_callable("ceil")(2.3), 3)
        self.assertEqual(self.reg.get_callable("floor")(2.7), 2)
        self.assertEqual(self.reg.get_callable("round")(2.3456, 0), 2)
        self.assertEqual(self.reg.get_callable("round")(2.3456, 2), 2.35)
        self.assertEqual(self.reg.get_callable("trunc")(2.3456), 2)
        self.assertEqual(self.reg.get_callable("sqrt")(4), 2)

    def test_string_functions(self):
        self.assertEqual(self.reg.get_callable("lower")("HELLO"), "hello")
        self.assertEqual(self.reg.get_callable("upper")("hello"), "HELLO")
        self.assertEqual(self.reg.get_callable("capitalize")("hello"), "Hello")
        self.assertEqual(self.reg.get_callable("title")("hello world"), "Hello World")
        self.assertEqual(self.reg.get_callable("strip")("  hello  "), "hello")
        self.assertEqual(self.reg.get_callable("lstrip")("  hello  "), "hello  ")
        self.assertEqual(self.reg.get_callable("rstrip")("  hello  "), "  hello")
        self.assertEqual(self.reg.get_callable("replace")("hello world", "world", "Python"), "hello Python")
        self.assertEqual(self.reg.get_callable("split")("a,b,c", ","), ["a", "b", "c"])
        self.assertEqual(self.reg.get_callable("join")(["a", "b"], ","), "a,b")
        self.assertTrue(self.reg.get_callable("startswith")("hello", "he"))
        self.assertTrue(self.reg.get_callable("endswith")("hello", "lo"))
        self.assertEqual(self.reg.get_callable("concat")("hello", " ", "world"), "hello world")
        self.assertTrue(self.reg.get_callable("isDigit")("123"))

    def test_logic_functions(self):
        self.assertTrue(self.reg.get_callable("and")(True, True))
        self.assertFalse(self.reg.get_callable("and")(True, False))
        self.assertTrue(self.reg.get_callable("or")(False, True))
        self.assertFalse(self.reg.get_callable("or")(False, False))
        self.assertTrue(self.reg.get_callable("or")(False, False, True))
        self.assertTrue(self.reg.get_callable("not")(False))
        self.assertFalse(self.reg.get_callable("not")(True))

    def test_array_functions(self):
        lst = [1, 2, 3]
        self.assertEqual(self.reg.get_callable("len")(lst), 3)
        self.assertEqual(self.reg.get_callable("length")(lst), 3)
        self.assertEqual(self.reg.get_callable("index")(lst, 2), 1)
        self.assertEqual(self.reg.get_callable("slice")(lst, 1, 3), [2, 3])
        self.assertEqual(self.reg.get_callable("append")(lst, 4), [1, 2, 3, 4])
        self.assertEqual(self.reg.get_callable("extend")(lst, [4, 5]), [1, 2, 3, 4, 5])
        self.assertEqual(self.reg.get_callable("insert")(lst, 1, 2), [1, 2, 2, 3])
        self.assertEqual(self.reg.get_callable("remove")(lst, 3), [1, 2])
        self.assertEqual(self.reg.get_callable("reverse")(lst), [3, 2, 1])
        self.assertEqual(self.reg.get_callable("sort")(lst), [1, 2, 3])
        self.assertEqual(self.reg.get_callable("sort")(lst, None, True), [3, 2, 1])
        lst_dicts = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 20}]
        self.assertEqual(
            self.reg.get_callable("sort")(lst_dicts, "age"),
            [{"name": "Bob", "age": 20}, {"name": "Alice", "age": 25}],
        )
        lst = [1, 2, 3]
        self.assertEqual(self.reg.get_callable("getListItem")(lst, 1), 2)
        self.reg.get_callable("setListItem")(lst, 1, 4)
        self.assertEqual(lst, [1, 4, 3])
        self.assertEqual(self.reg.get_callable("addListItem")(lst, 5), [1, 4, 3, 5])
        self.reg.get_callable("delListItem")(lst, 1)
        self.assertEqual(lst, [1, 3])
        self.assertEqual(self.reg.get_callable("pop")(lst, 0), 1)
        self.assertEqual(lst, [3])
        self.assertEqual(self.reg.get_callable("insertListItem")([1, 3], 1, 2), [1, 2, 3])

    def test_dict_functions(self):
        dct = {"a": 1, "b": 2}
        self.assertEqual(list(self.reg.get_callable("keys")(dct)), ["a", "b"])
        self.assertEqual(list(self.reg.get_callable("values")(dct)), [1, 2])
        self.assertEqual(list(self.reg.get_callable("items")(dct)), [("a", 1), ("b", 2)])
        self.assertEqual(self.reg.get_callable("get")(dct, "a"), 1)
        self.assertEqual(self.reg.get_callable("get")(dct, "c", "default"), "default")
        self.reg.get_callable("set")(dct, "b", 3)
        self.assertEqual(dct, {"a": 1, "b": 3})
        self.reg.get_callable("delete")(dct, "b")
        self.assertEqual(dct, {"a": 1})
        self.reg.get_callable("clear")(dct)
        self.assertEqual(dct, {})
        dct = {"a": 1, "b": 2}
        self.assertEqual(self.reg.get_callable("getDictValue")(dct, "a"), 1)
        self.assertEqual(self.reg.get_callable("getDictValue")(dct, "c", "default"), "default")
        self.reg.get_callable("setDictValue")(dct, "b", 3)
        self.assertEqual(dct, {"a": 1, "b": 3})
        self.reg.get_callable("delDictValue")(dct, "b")
        self.assertEqual(dct, {"a": 1})
        self.assertEqual(list(self.reg.get_callable("getDictKeys")(dct)), ["a"])
        self.assertEqual(list(self.reg.get_callable("getDictValues")(dct)), [1])
        self.reg.get_callable("clearDict")(dct)
        self.assertEqual(dct, {})

    def test_datetime_functions(self):
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.assertEqual(self.reg.get_callable("now")(), now)
        today = date.today().strftime("%Y-%m-%d")
        self.assertEqual(self.reg.get_callable("today")(), today)

    def test_json_functions(self):
        obj = {"key": "value"}
        json_str = '{"key": "value"}'
        self.assertEqual(self.reg.get_callable("json_loads")(json_str), obj)
        self.assertEqual(self.reg.get_callable("json_dumps")(obj), json_str)

    def test_total_function_count(self):
        total = len(self.reg)
        self.assertGreaterEqual(total, 50, f"Expected >= 50 functions, got {total}")

    def test_all_categories_populated(self):
        for cat in [
            FunctionCategory.MATH,
            FunctionCategory.STRING,
            FunctionCategory.LOGIC,
            FunctionCategory.ARRAY,
            FunctionCategory.DICT,
            FunctionCategory.DATETIME,
            FunctionCategory.JSON,
        ]:
            fns = self.reg.by_category(cat)
            self.assertTrue(len(fns) > 0, f"Category {cat.value} has no functions")


# ---------------------------------------------------------------------------
# T063 — Side-effect labeling
# ---------------------------------------------------------------------------


class TestSideEffectLabeling(TestCase):
    """Verify the correct functions are marked has_side_effects=True."""

    def setUp(self):
        self.reg = get_default_expression_registry()

    def test_side_effect_array_functions(self):
        expected = {"pop", "delListItem", "setListItem"}
        se_names = {d.name for d in self.reg.side_effect_functions() if d.category == FunctionCategory.ARRAY}
        self.assertEqual(expected, se_names)

    def test_side_effect_dict_functions(self):
        expected = {"set", "delete", "clear", "setDictValue", "delDictValue", "clearDict"}
        se_names = {d.name for d in self.reg.side_effect_functions() if d.category == FunctionCategory.DICT}
        self.assertEqual(expected, se_names)

    def test_pure_functions_not_marked(self):
        pure_names = {"add", "sub", "lower", "upper", "len", "keys", "get", "getDictValue", "now"}
        for name in pure_names:
            desc = self.reg.get(name)
            self.assertIsNotNone(desc, f"Function '{name}' not found")
            self.assertFalse(desc.has_side_effects, f"'{name}' should not have side effects")

    def test_all_side_effect_functions_count(self):
        se_fns = self.reg.side_effect_functions()
        self.assertEqual(len(se_fns), 9)


# ---------------------------------------------------------------------------
# T064 — Backward-compatible REGISTERED_FUNCTIONS dict proxy
# ---------------------------------------------------------------------------


class TestRegisteredFunctionsProxy(TestCase):
    """Verify the backward-compat proxy at plaita.io.REGISTERED_FUNCTIONS."""

    def test_read_access(self):
        from plaita.io import REGISTERED_FUNCTIONS
        fn = REGISTERED_FUNCTIONS["add"]
        self.assertEqual(fn(3, 4), 7)

    def test_get_with_default(self):
        from plaita.io import REGISTERED_FUNCTIONS
        fn = REGISTERED_FUNCTIONS.get("add")
        self.assertEqual(fn(3, 4), 7)
        default_fn = REGISTERED_FUNCTIONS.get("nonexistent", lambda: "fallback")
        self.assertEqual(default_fn(), "fallback")

    def test_contains(self):
        from plaita.io import REGISTERED_FUNCTIONS
        self.assertIn("add", REGISTERED_FUNCTIONS)
        self.assertNotIn("nonexistent_xyz", REGISTERED_FUNCTIONS)

    def test_len(self):
        from plaita.io import REGISTERED_FUNCTIONS
        self.assertGreaterEqual(len(REGISTERED_FUNCTIONS), 50)

    def test_iter(self):
        from plaita.io import REGISTERED_FUNCTIONS
        names = list(REGISTERED_FUNCTIONS)
        self.assertIn("add", names)
        self.assertIn("lower", names)

    def test_setitem_emits_deprecation_warning(self):
        from plaita.io import REGISTERED_FUNCTIONS
        custom_fn = lambda x: x * 2
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            REGISTERED_FUNCTIONS["_test_custom_fn_"] = custom_fn
            self.assertTrue(any(issubclass(x.category, DeprecationWarning) for x in w))
        self.assertEqual(REGISTERED_FUNCTIONS["_test_custom_fn_"](5), 10)
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            del REGISTERED_FUNCTIONS["_test_custom_fn_"]

    def test_register_function_emits_deprecation_warning(self):
        from plaita.io import register_function, REGISTERED_FUNCTIONS
        custom_fn = lambda x: x * 3
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            register_function("_test_reg_fn_", custom_fn)
            self.assertTrue(any(issubclass(x.category, DeprecationWarning) for x in w))
        self.assertEqual(REGISTERED_FUNCTIONS["_test_reg_fn_"](5), 15)
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            del REGISTERED_FUNCTIONS["_test_reg_fn_"]

    def test_keyerror_on_missing(self):
        from plaita.io import REGISTERED_FUNCTIONS
        with self.assertRaises(KeyError):
            _ = REGISTERED_FUNCTIONS["definitely_not_a_function"]


# ---------------------------------------------------------------------------
# T065 — ExpressionEvaluator
# ---------------------------------------------------------------------------


class TestExpressionEvaluator(TestCase):
    """Verify ExpressionEvaluator delegates correctly to plaita.io.evaluate."""

    def setUp(self):
        self.evaluator = ExpressionEvaluator()

    def test_evaluate_plain_string(self):
        result = self.evaluator.evaluate("hello", {})
        self.assertEqual(result, "hello")

    def test_evaluate_variable(self):
        result = self.evaluator.evaluate("$INPUT", {"$INPUT": "hello"})
        self.assertEqual(result, "hello")

    def test_evaluate_nested_dict(self):
        ctx = {"$INPUT": {"name": "Alice"}}
        result = self.evaluator.evaluate({"greeting": "$INPUT.name"}, ctx)
        self.assertEqual(result, {"greeting": "Alice"})

    def test_evaluate_list(self):
        ctx = {"$INPUT": 42}
        result = self.evaluator.evaluate(["$INPUT", "static"], ctx)
        self.assertEqual(result, [42, "static"])

    def test_evaluate_function_call(self):
        result = self.evaluator.evaluate("$F.add(1, 2)", {})
        self.assertEqual(result, 3)

    def test_evaluate_nested_function(self):
        result = self.evaluator.evaluate("$F.add($F.mul(2, 3), 4)", {})
        self.assertEqual(result, 10)

    def test_evaluate_with_variable_in_function(self):
        result = self.evaluator.evaluate("$F.add($INPUT, 5)", {"$INPUT": 10})
        self.assertEqual(result, 15)

    def test_evaluate_template_expression(self):
        result = self.evaluator.evaluate("result is {%$F.add(3, 4)%}", {})
        self.assertEqual(result, "result is 7")

    def test_evaluate_non_string(self):
        self.assertEqual(self.evaluator.evaluate(42, {}), 42)
        self.assertIsNone(self.evaluator.evaluate(None, {}))
        self.assertTrue(self.evaluator.evaluate(True, {}))

    def test_custom_registry(self):
        reg = ExpressionRegistry()
        reg.register("double", lambda x: x * 2, FunctionCategory.MATH)
        evaluator = ExpressionEvaluator(registry=reg)
        self.assertIs(evaluator.registry, reg)

    def test_custom_registry_drives_evaluation(self):
        """A custom registry must actually resolve $F calls — not be ignored."""
        reg = ExpressionRegistry()
        reg.register("double", lambda x: x * 2, FunctionCategory.MATH)
        evaluator = ExpressionEvaluator(registry=reg)

        # double 只存在于自定义 registry, 默认 registry 没有
        self.assertEqual(evaluator.evaluate("$F.double(21)", {}), 42)

        # 默认 registry 里的 add 不在自定义 registry 中, 应回退为 "undefined"
        self.assertEqual(evaluator.evaluate("$F.add(1, 2)", {}), "undefined")

    def test_default_registry_still_works(self):
        """Behavior-preserving: default evaluator resolves built-in functions."""
        self.assertEqual(self.evaluator.evaluate('$F.upper("hello")', {}), "HELLO")

    def test_registry_property(self):
        self.assertIsInstance(self.evaluator.registry, ExpressionRegistry)
