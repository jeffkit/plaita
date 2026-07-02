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
        # or 的 falsy 回退必须是 False 而不是 None —— 精确断言类型
        self.assertIs(self.reg.get_callable("or")(False, False), False)
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
        # remove 用 4 元素列表：mutant 把 a[index(b)+1:] 改成 a[index(b)+2:] 时
        # 在 3 元素列表上仍可能巧合得到同样结果，故用 4 元素拉开差异
        self.assertEqual(self.reg.get_callable("remove")([1, 2, 3, 4], 2), [1, 3, 4])
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


# ---------------------------------------------------------------------------
# 强化：精确断言 default registry 的元数据（name/category/side-effect/description）
# 与 register/unregister/evaluator 的边界语义。杀死仅靠「能跑通」蒙混的变异点。
# ---------------------------------------------------------------------------


# default registry 里每个函数的精确元数据 (category, has_side_effects, description)。
# 任何字符串常量/分类/副作用标记被变异都应被这表精确命中。
EXPECTED_DEFAULT_FUNCTIONS = {
    # math
    "add": (FunctionCategory.MATH, False, "Add two values"),
    "sub": (FunctionCategory.MATH, False, "Subtract b from a"),
    "mul": (FunctionCategory.MATH, False, "Multiply two values"),
    "div": (FunctionCategory.MATH, False, "Divide a by b"),
    "mod": (FunctionCategory.MATH, False, "Modulo a by b"),
    "pow": (FunctionCategory.MATH, False, "Raise a to the power of b"),
    "abs": (FunctionCategory.MATH, False, "Absolute value"),
    "ceil": (FunctionCategory.MATH, False, "Ceiling"),
    "floor": (FunctionCategory.MATH, False, "Floor"),
    "round": (FunctionCategory.MATH, False, "Round to n digits"),
    "trunc": (FunctionCategory.MATH, False, "Truncate to integer"),
    "sqrt": (FunctionCategory.MATH, False, "Square root"),
    # string
    "lower": (FunctionCategory.STRING, False, "Lowercase"),
    "upper": (FunctionCategory.STRING, False, "Uppercase"),
    "capitalize": (FunctionCategory.STRING, False, "Capitalize first char"),
    "title": (FunctionCategory.STRING, False, "Title-case"),
    "strip": (FunctionCategory.STRING, False, "Strip whitespace"),
    "lstrip": (FunctionCategory.STRING, False, "Strip leading whitespace"),
    "rstrip": (FunctionCategory.STRING, False, "Strip trailing whitespace"),
    "replace": (FunctionCategory.STRING, False, "Replace substring"),
    "split": (FunctionCategory.STRING, False, "Split string"),
    "join": (FunctionCategory.STRING, False, "Join iterable with separator"),
    "startswith": (FunctionCategory.STRING, False, "Check string prefix"),
    "endswith": (FunctionCategory.STRING, False, "Check string suffix"),
    "concat": (FunctionCategory.STRING, False, "Concatenate values as strings"),
    "isDigit": (FunctionCategory.STRING, False, "Check if string is all digits"),
    # array — pure
    "len": (FunctionCategory.ARRAY, False, "Length of sequence"),
    "length": (FunctionCategory.ARRAY, False, "Length of sequence (alias)"),
    "index": (FunctionCategory.ARRAY, False, "Find index of element"),
    "slice": (FunctionCategory.ARRAY, False, "Slice list"),
    "append": (FunctionCategory.ARRAY, False, "Append element (returns new list)"),
    "extend": (FunctionCategory.ARRAY, False, "Extend list (returns new list)"),
    "insert": (FunctionCategory.ARRAY, False, "Insert element (returns new list)"),
    "remove": (FunctionCategory.ARRAY, False, "Remove first occurrence (returns new list)"),
    "reverse": (FunctionCategory.ARRAY, False, "Reverse list (returns new list)"),
    "sort": (FunctionCategory.ARRAY, False, "Sort list (returns new list)"),
    "getListItem": (FunctionCategory.ARRAY, False, "Get item by index"),
    "addListItem": (FunctionCategory.ARRAY, False, "Add item (returns new list)"),
    "insertListItem": (FunctionCategory.ARRAY, False, "Insert item (returns new list)"),
    # array — side-effect
    "pop": (FunctionCategory.ARRAY, True, "Pop item at index (mutates list)"),
    "delListItem": (FunctionCategory.ARRAY, True, "Delete item at index (mutates list)"),
    "setListItem": (FunctionCategory.ARRAY, True, "Set item at index (mutates list)"),
    # dict — pure
    "keys": (FunctionCategory.DICT, False, "Dict keys"),
    "values": (FunctionCategory.DICT, False, "Dict values"),
    "items": (FunctionCategory.DICT, False, "Dict items"),
    "get": (FunctionCategory.DICT, False, "Get value with default"),
    "getDictValue": (FunctionCategory.DICT, False, "Get dict value with default"),
    "getDictKeys": (FunctionCategory.DICT, False, "Get dict keys"),
    "getDictValues": (FunctionCategory.DICT, False, "Get dict values"),
    # dict — side-effect
    "set": (FunctionCategory.DICT, True, "Set dict value (mutates dict)"),
    "delete": (FunctionCategory.DICT, True, "Delete dict key (mutates dict)"),
    "clear": (FunctionCategory.DICT, True, "Clear all dict entries (mutates dict)"),
    "setDictValue": (FunctionCategory.DICT, True, "Set dict value (mutates dict)"),
    "delDictValue": (FunctionCategory.DICT, True, "Delete dict key (mutates dict)"),
    "clearDict": (FunctionCategory.DICT, True, "Clear all dict entries (mutates dict)"),
    # logic
    "and": (FunctionCategory.LOGIC, False, "Logical AND"),
    "or": (FunctionCategory.LOGIC, False, "Logical OR (returns first truthy value)"),
    "not": (FunctionCategory.LOGIC, False, "Logical NOT"),
    # datetime
    "now": (FunctionCategory.DATETIME, False, "Current datetime formatted"),
    "today": (FunctionCategory.DATETIME, False, "Current date formatted"),
    # json
    "json_loads": (FunctionCategory.JSON, False, "Parse JSON string"),
    "json_dumps": (FunctionCategory.JSON, False, "Serialize to JSON string"),
}


class TestDefaultRegistryMetadata(TestCase):
    """逐函数精确断言 default registry 的元数据 —— 杀死 _register_* 中
    对描述字符串 / 分类 / 副作用标记的字符串常量变异。"""

    def setUp(self):
        self.reg = get_default_expression_registry()

    def test_function_count(self):
        self.assertEqual(len(self.reg), len(EXPECTED_DEFAULT_FUNCTIONS))

    def test_no_unexpected_functions(self):
        names = set(self.reg.all_functions().keys())
        self.assertEqual(names, set(EXPECTED_DEFAULT_FUNCTIONS.keys()))

    def test_each_function_metadata(self):
        for name, (cat, se, desc) in EXPECTED_DEFAULT_FUNCTIONS.items():
            d = self.reg.get(name)
            self.assertIsNotNone(d, f"function {name!r} not registered")
            self.assertEqual(d.name, name, f"name mismatch for {name!r}")
            self.assertEqual(d.category, cat, f"category mismatch for {name!r}")
            self.assertEqual(d.has_side_effects, se, f"side-effect flag mismatch for {name!r}")
            self.assertEqual(d.description, desc, f"description mismatch for {name!r}")

    def test_categories_function_counts(self):
        self.assertEqual(len(self.reg.by_category(FunctionCategory.MATH)), 12)
        self.assertEqual(len(self.reg.by_category(FunctionCategory.STRING)), 14)
        self.assertEqual(len(self.reg.by_category(FunctionCategory.ARRAY)), 16)
        self.assertEqual(len(self.reg.by_category(FunctionCategory.DICT)), 13)
        self.assertEqual(len(self.reg.by_category(FunctionCategory.LOGIC)), 3)
        self.assertEqual(len(self.reg.by_category(FunctionCategory.DATETIME)), 2)
        self.assertEqual(len(self.reg.by_category(FunctionCategory.JSON)), 2)
        # TYPE 分类在 default registry 中未使用
        self.assertEqual(len(self.reg.by_category(FunctionCategory.TYPE)), 0)

    def test_repr(self):
        self.assertEqual(repr(self.reg), f"<ExpressionRegistry functions={len(self.reg)}>")


class TestRegisterSemantics(TestCase):
    """精确断言 register 的错误文案、默认 description、override 行为。"""

    def test_empty_name_error_message(self):
        reg = ExpressionRegistry()
        with self.assertRaises(ValueError) as cm:
            reg.register("", lambda: None, FunctionCategory.MATH)
        self.assertEqual(str(cm.exception), "Function name must not be empty")

    def test_duplicate_name_error_message(self):
        reg = ExpressionRegistry()
        reg.register("add", lambda a, b: a + b, FunctionCategory.MATH)
        with self.assertRaises(ValueError) as cm:
            reg.register("add", lambda a, b: a - b, FunctionCategory.MATH)
        self.assertEqual(str(cm.exception), "Function 'add' is already registered")

    def test_default_description_is_empty_string(self):
        # register 不传 description 时必须落成 ""（而不是 None / "XXXX"）
        reg = ExpressionRegistry()
        reg.register("foo", lambda: 1, FunctionCategory.MATH)
        self.assertEqual(reg.get("foo").description, "")

    def test_explicit_description_recorded(self):
        reg = ExpressionRegistry()
        reg.register("foo", lambda: 1, FunctionCategory.MATH, description="hi there")
        self.assertEqual(reg.get("foo").description, "hi there")

    def test_override_replaces_existing(self):
        reg = ExpressionRegistry()
        reg.register("add", lambda a, b: a + b, FunctionCategory.MATH, description="orig")
        # override=True 不抛错，且替换 func / description
        reg.register("add", lambda a, b: a * b, FunctionCategory.MATH,
                     description="replaced", override=True)
        self.assertEqual(reg.get_callable("add")(3, 4), 12)
        self.assertEqual(reg.get("add").description, "replaced")
        self.assertEqual(len(reg), 1)

    def test_override_false_does_not_touch_existing(self):
        reg = ExpressionRegistry()
        reg.register("add", lambda a, b: a + b, FunctionCategory.MATH, description="orig")
        # override=False 对同名注册抛错，原有 registration 不变
        with self.assertRaises(ValueError):
            reg.register("add", lambda a, b: a * b, FunctionCategory.MATH, override=False)
        self.assertEqual(reg.get_callable("add")(3, 4), 7)
        self.assertEqual(reg.get("add").description, "orig")


class TestUnregisterSemantics(TestCase):
    """unregister 必须按名移除，且对未注册名是 no-op（不抛 KeyError）。"""

    def test_unregister_removes_by_name(self):
        reg = ExpressionRegistry()
        reg.register("add", lambda a, b: a + b, FunctionCategory.MATH)
        reg.register("sub", lambda a, b: a - b, FunctionCategory.MATH)
        reg.unregister("add")
        self.assertNotIn("add", reg)
        self.assertIn("sub", reg)  # 别的函数不受影响
        self.assertEqual(len(reg), 1)

    def test_unregister_missing_is_noop(self):
        reg = ExpressionRegistry()
        reg.register("add", lambda a, b: a + b, FunctionCategory.MATH)
        # 对未注册名调用 unregister 不能抛 KeyError
        reg.unregister("definitely_not_here")
        self.assertIn("add", reg)
        self.assertEqual(len(reg), 1)


class TestDefaultRegistryCaching(TestCase):
    """get_default_expression_registry 懒加载并缓存单例。"""

    def test_returns_same_instance(self):
        import plaita.core.expression as mod
        # 重置缓存以隔离测试
        original = mod._default_registry
        try:
            mod._default_registry = None
            first = get_default_expression_registry()
            second = get_default_expression_registry()
            self.assertIs(first, second)
        finally:
            mod._default_registry = original

    def test_caches_built_registry(self):
        import plaita.core.expression as mod
        original = mod._default_registry
        try:
            mod._default_registry = None
            reg = get_default_expression_registry()
            self.assertIs(mod._default_registry, reg)
            self.assertIn("add", reg)
        finally:
            mod._default_registry = original


class TestEvaluatorPrefixPropagation(TestCase):
    """evaluate 必须把 prefix 透传给 plaita.io.evaluate —— 非 $ 前缀要生效。"""

    def test_custom_prefix_resolves_variable(self):
        evaluator = ExpressionEvaluator()
        ctx = {"#INPUT": "hello"}
        # prefix="#" 时 #INPUT 应被解析为变量值
        self.assertEqual(evaluator.evaluate("#INPUT", ctx, prefix="#"), "hello")

    def test_custom_prefix_function_call(self):
        evaluator = ExpressionEvaluator()
        # prefix="#" 时函数调用前缀也应是 #F
        self.assertEqual(evaluator.evaluate("#F.add(1, 2)", {}, prefix="#"), 3)
