"""变异测试专项断言 — plaita.core.expression_parser

这批测试针对 mutmut baseline sweep 中 survived 的 175 个变异点，
采取三项关键策略来确保有效性：

1. **setUp 清空缓存**：`ExpressionParser._instances.clear()` 强制每个测试
   都触发全新的 `_build_grammar()` 构建，防止 mutmut in-process 跑多轮时
   复用旧缓存（这是 138 个 `_build_grammar` 变异全部幸存的根因）。

2. **精确断言结果值**：只断言"能运行"是不够的，必须断言返回值的精确语义
   （bool vs str，42 vs None，等等）。

3. **专项构造测试输入**：每个测试输入都经过设计，能够区分原始代码与特定
   变异代码的行为差异。
"""
from __future__ import annotations

import logging
import unittest
from unittest import TestCase

from plaita.core.expression import ExpressionRegistry, FunctionCategory
from plaita.core.expression_parser import ExpressionParser, _get_attr


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _clear_parser_cache() -> None:
    """清空 ExpressionParser 类级实例缓存，强制重新构建语法。"""
    ExpressionParser._instances.clear()


def _fresh(prefix: str = "$") -> ExpressionParser:
    """清空缓存后返回新 parser 实例（确保 _build_grammar 被调用）。"""
    _clear_parser_cache()
    return ExpressionParser.for_prefix(prefix)


def _registry(**funcs) -> ExpressionRegistry:
    """构建仅含指定函数的最小 ExpressionRegistry。"""
    reg = ExpressionRegistry()
    for name, fn in funcs.items():
        reg.register(name, fn, FunctionCategory.MATH)
    return reg


# ===========================================================================
# 1. __init__ 与 for_prefix 变异
# ===========================================================================

class TestExpressionParserConstruction(TestCase):
    """杀灭 __init__ 和 for_prefix 相关变异。"""

    def setUp(self):
        _clear_parser_cache()

    def tearDown(self):
        _clear_parser_cache()

    # ── __init__ ────────────────────────────────────────────────────────────

    def test_default_prefix_is_dollar(self):
        """__init____mutmut_1: default prefix 'XX$XX' → must be '$'."""
        p = ExpressionParser()
        self.assertEqual(p.prefix, "$")

    def test_custom_prefix_stored(self):
        """__init____mutmut_2: self.prefix = None → must equal passed arg."""
        p = ExpressionParser("@")
        self.assertEqual(p.prefix, "@")

    def test_dollar_prefix_stored(self):
        """__init____mutmut_2: ExpressionParser('$').prefix must be '$'."""
        p = ExpressionParser("$")
        self.assertEqual(p.prefix, "$")

    # ── for_prefix ──────────────────────────────────────────────────────────

    def test_for_prefix_returns_expression_parser(self):
        """for_prefix__mutmut_5: inst = None → must return ExpressionParser."""
        p = ExpressionParser.for_prefix("$")
        self.assertIsInstance(p, ExpressionParser)

    def test_for_prefix_stores_correct_prefix(self):
        """for_prefix__mutmut_6: cls(None) → prefix must equal arg."""
        p = ExpressionParser.for_prefix("$")
        self.assertEqual(p.prefix, "$")

    def test_for_prefix_no_args_default_dollar(self):
        """for_prefix__mutmut_1: default 'XX$XX' → must be '$' with no arg."""
        p = ExpressionParser.for_prefix()
        self.assertIsInstance(p, ExpressionParser)
        self.assertEqual(p.prefix, "$")

    def test_for_prefix_caches_same_instance(self):
        """for_prefix__mutmut_4: if inst is not None → second call must reuse."""
        p1 = ExpressionParser.for_prefix("$")
        p2 = ExpressionParser.for_prefix("$")
        self.assertIs(p1, p2)

    def test_for_prefix_stores_in_cache(self):
        """for_prefix__mutmut_7: _instances[prefix]=None → must be reused."""
        p1 = ExpressionParser.for_prefix("$")
        p2 = ExpressionParser.for_prefix("$")
        self.assertIs(p1, p2)
        self.assertIsNotNone(p2)

    def test_for_prefix_different_prefix_different_instance(self):
        """for_prefix__mutmut_3: get(None) → different prefixes different instances."""
        p1 = ExpressionParser.for_prefix("$")
        p2 = ExpressionParser.for_prefix("@")
        self.assertIsNot(p1, p2)
        self.assertEqual(p1.prefix, "$")
        self.assertEqual(p2.prefix, "@")

    def test_for_prefix_with_custom_prefix_evaluates(self):
        """for_prefix__mutmut_6: cls(None) → must use given prefix to parse."""
        p = ExpressionParser.for_prefix("#")
        result = p.evaluate("#INPUT", {"#INPUT": 99})
        self.assertEqual(result, 99)


# ===========================================================================
# 2. _build_grammar — 布尔关键字
# ===========================================================================

class TestBuildGrammarBooleans(TestCase):
    """杀灭 _build_grammar True/False/true/false 关键字变异。"""

    def setUp(self):
        _clear_parser_cache()

    def tearDown(self):
        _clear_parser_cache()

    def test_True_evaluates_to_python_True(self):
        """_build_grammar__mutmut_10: 'True' → 'TRUE' — 必须解析出 bool True。"""
        reg = _registry(identity=lambda x: x)
        p = _fresh()
        self.assertIs(p.evaluate("$F.identity(True)", {}, reg), True)

    def test_False_evaluates_to_python_False(self):
        """_build_grammar__mutmut_20: 'False' → 'FALSE'。"""
        reg = _registry(identity=lambda x: x)
        p = _fresh()
        self.assertIs(p.evaluate("$F.identity(False)", {}, reg), False)

    def test_lowercase_true_evaluates_to_True(self):
        """_build_grammar: 'true' 关键字必须解析为 bool True。"""
        reg = _registry(identity=lambda x: x)
        p = _fresh()
        self.assertIs(p.evaluate("$F.identity(true)", {}, reg), True)

    def test_lowercase_false_evaluates_to_False(self):
        """_build_grammar: 'false' 关键字必须解析为 bool False。"""
        reg = _registry(identity=lambda x: x)
        p = _fresh()
        self.assertIs(p.evaluate("$F.identity(false)", {}, reg), False)

    def test_boolean_result_is_bool_not_string(self):
        """结果类型必须是 bool，不能是 str（区分等价变异）。"""
        reg = _registry(identity=lambda x: x)
        p = _fresh()
        result = p.evaluate("$F.identity(True)", {}, reg)
        self.assertIsInstance(result, bool)
        self.assertTrue(result)

    def test_boolean_in_conditional_function(self):
        """在条件函数中布尔值必须保持正确语义。"""
        reg = _registry(ternary=lambda c, a, b: a if c else b)
        p = _fresh()
        self.assertEqual(p.evaluate("$F.ternary(True, 1, 0)", {}, reg), 1)
        self.assertEqual(p.evaluate("$F.ternary(False, 1, 0)", {}, reg), 0)
        self.assertEqual(p.evaluate("$F.ternary(true, 1, 0)", {}, reg), 1)
        self.assertEqual(p.evaluate("$F.ternary(false, 1, 0)", {}, reg), 0)


# ===========================================================================
# 3. _build_grammar — 字符串字面量
# ===========================================================================

class TestBuildGrammarStrings(TestCase):
    """杀灭 _build_grammar 单/双引号字符串变异。"""

    def setUp(self):
        _clear_parser_cache()

    def tearDown(self):
        _clear_parser_cache()

    def test_double_quoted_string_arg(self):
        """双引号字符串字面量必须被解析。"""
        reg = _registry(identity=lambda x: x)
        p = _fresh()
        self.assertEqual(p.evaluate('$F.identity("hello")', {}, reg), "hello")

    def test_single_quoted_string_arg(self):
        """单引号字符串字面量必须被解析。"""
        reg = _registry(identity=lambda x: x)
        p = _fresh()
        self.assertEqual(p.evaluate("$F.identity('world')", {}, reg), "world")

    def test_string_with_spaces(self):
        """含空格的字符串字面量不被截断。"""
        reg = _registry(identity=lambda x: x)
        p = _fresh()
        self.assertEqual(p.evaluate('$F.identity("hello world")', {}, reg), "hello world")

    def test_empty_string_arg(self):
        """空字符串字面量。"""
        reg = _registry(identity=lambda x: x)
        p = _fresh()
        self.assertEqual(p.evaluate('$F.identity("")', {}, reg), "")


# ===========================================================================
# 4. _build_grammar — name_token / identifier 字符集
# ===========================================================================

class TestBuildGrammarNameTokens(TestCase):
    """杀灭 name_token、identifier = None 等变异。"""

    def setUp(self):
        _clear_parser_cache()

    def tearDown(self):
        _clear_parser_cache()

    def test_field_with_underscore(self):
        """name_token 必须包含 '_'。"""
        p = _fresh()
        self.assertEqual(p.evaluate("$INPUT.user_name", {"$INPUT": {"user_name": "Bob"}}), "Bob")

    def test_field_with_hyphen(self):
        """name_token 必须包含 '-'。"""
        p = _fresh()
        self.assertEqual(p.evaluate("$INPUT.user-id", {"$INPUT": {"user-id": 123}}), 123)

    def test_field_with_dollar_sign(self):
        """name_token 必须包含 '$'（特殊根节点嵌套路径）。"""
        p = _fresh()
        ctx = {"$INPUT": {"$GLOBAL": "value"}}
        self.assertEqual(p.evaluate("$INPUT.$GLOBAL", ctx), "value")

    def test_root_variable_resolved_from_context(self):
        """root 语法必须能识别 $<name> 格式。"""
        p = _fresh()
        self.assertEqual(p.evaluate("$INPUT", {"$INPUT": 42}), 42)

    def test_identifier_in_function_name(self):
        """identifier 必须包含字母数字下划线（函数名）。"""
        reg = _registry(get_value=lambda: 99)
        p = _fresh()
        self.assertEqual(p.evaluate("$F.get_value()", {}, reg), 99)


# ===========================================================================
# 5. _build_grammar — 索引与点分路径
# ===========================================================================

class TestBuildGrammarIndexing(TestCase):
    """杀灭 index_seg、dot_int、dot_field 以及 signed_int 变异。"""

    def setUp(self):
        _clear_parser_cache()

    def tearDown(self):
        _clear_parser_cache()

    def test_bracket_index_zero(self):
        """index_seg: $LIST[0] 必须返回第一个元素。"""
        p = _fresh()
        self.assertEqual(p.evaluate("$LIST[0]", {"$LIST": ["a", "b", "c"]}), "a")

    def test_bracket_index_negative(self):
        """signed_int: $LIST[-1] 必须返回最后一个元素。"""
        p = _fresh()
        self.assertEqual(p.evaluate("$LIST[-1]", {"$LIST": [10, 20, 30]}), 30)

    def test_bracket_index_middle(self):
        """index_seg: 中间索引。"""
        p = _fresh()
        self.assertEqual(p.evaluate("$LIST[1]", {"$LIST": ["x", "y", "z"]}), "y")

    def test_dot_integer_index(self):
        """dot_int: $LIST.0 必须作为索引（不是字段 '0'）。"""
        p = _fresh()
        self.assertEqual(p.evaluate("$LIST.0", {"$LIST": ["first", "second"]}), "first")

    def test_dot_integer_index_nonzero(self):
        """dot_int: $LIST.2 索引。"""
        p = _fresh()
        self.assertEqual(p.evaluate("$LIST.2", {"$LIST": [0, 1, 2, 3]}), 2)

    def test_dot_field_access(self):
        """dot_field: $INPUT.name 字段访问。"""
        p = _fresh()
        self.assertEqual(p.evaluate("$INPUT.name", {"$INPUT": {"name": "Alice"}}), "Alice")

    def test_chained_field_then_index(self):
        """组合路径：字段访问后跟索引。"""
        p = _fresh()
        ctx = {"$INPUT": {"items": ["x", "y", "z"]}}
        self.assertEqual(p.evaluate("$INPUT.items[2]", ctx), "z")

    def test_chained_index_then_field(self):
        """组合路径：索引后跟字段访问。"""
        p = _fresh()
        ctx = {"$INPUT": [{"name": "first"}, {"name": "second"}]}
        self.assertEqual(p.evaluate("$INPUT[0].name", ctx), "first")

    def test_deep_chained_path(self):
        """深层路径访问。"""
        p = _fresh()
        ctx = {"$INPUT": {"users": [{"profile": {"age": 30}}, {"profile": {"age": 25}}]}}
        self.assertEqual(p.evaluate("$INPUT.users[1].profile.age", ctx), 25)


# ===========================================================================
# 6. _build_grammar — 函数调用语法
# ===========================================================================

class TestBuildGrammarFunctionCalls(TestCase):
    """杀灭函数调用 grammar 相关变异（func_head、arg_list、Forward 等）。"""

    def setUp(self):
        _clear_parser_cache()

    def tearDown(self):
        _clear_parser_cache()

    def test_function_call_no_args(self):
        """无参函数调用。"""
        reg = _registry(get_pi=lambda: 3)
        p = _fresh()
        self.assertEqual(p.evaluate("$F.get_pi()", {}, reg), 3)

    def test_function_call_single_arg(self):
        """单参数函数调用。"""
        reg = _registry(double=lambda x: x * 2)
        p = _fresh()
        self.assertEqual(p.evaluate("$F.double(21)", {}, reg), 42)

    def test_function_call_two_args(self):
        """双参数函数调用。"""
        reg = _registry(add=lambda a, b: a + b)
        p = _fresh()
        self.assertEqual(p.evaluate("$F.add(3, 4)", {}, reg), 7)

    def test_function_call_three_args(self):
        """三参数函数调用（arg_list 分隔符）。"""
        reg = _registry(add3=lambda a, b, c: a + b + c)
        p = _fresh()
        self.assertEqual(p.evaluate("$F.add3(1, 2, 3)", {}, reg), 6)

    def test_function_call_trailing_comma(self):
        """arg_list 末尾逗号（pp.Optional(',')）不会导致解析失败。

        注意：trailing comma 会在 token 列表末尾产生一个 ',' 字符串，
        因此使用 varargs 函数过滤掉非数值参数。"""
        reg = _registry(
            sum_nums=lambda *args: sum(a for a in args if isinstance(a, (int, float)))
        )
        p = _fresh()
        result = p.evaluate("$F.sum_nums(1, 2,)", {}, reg)
        self.assertEqual(result, 3)

    def test_function_call_with_variable_arg(self):
        """函数参数含变量引用（expr 顺序：function_call before variable）。"""
        reg = _registry(double=lambda x: x * 2)
        p = _fresh()
        self.assertEqual(p.evaluate("$F.double($INPUT.x)", {"$INPUT": {"x": 5}}, reg), 10)

    def test_nested_function_calls(self):
        """函数调用嵌套（Forward 递归）。"""
        reg = _registry(add=lambda a, b: a + b, double=lambda x: x * 2)
        p = _fresh()
        self.assertEqual(p.evaluate("$F.add($F.double(3), 1)", {}, reg), 7)

    def test_function_with_string_and_variable(self):
        """函数参数混合字符串和变量。"""
        reg = _registry(concat=lambda a, b: a + b)
        p = _fresh()
        result = p.evaluate('$F.concat("Hello ", $INPUT.name)', {"$INPUT": {"name": "World"}}, reg)
        self.assertEqual(result, "Hello World")

    def test_function_with_boolean_arg(self):
        """函数参数含布尔值（验证 expr = constant | function_call | variable）。"""
        reg = _registry(identity=lambda x: x)
        p = _fresh()
        self.assertIs(p.evaluate("$F.identity(True)", {}, reg), True)

    def test_function_call_f_separator_is_dot(self):
        """$F. 分隔符必须是点号（不能是其他字符）。"""
        reg = _registry(noop=lambda: "ok")
        p = _fresh()
        self.assertEqual(p.evaluate("$F.noop()", {}, reg), "ok")
        # 格式必须是 $F.func_name(
        with self.assertRaises(Exception):
            p.evaluate("$Fnoop()", {}, reg)  # 缺少 '.'


# ===========================================================================
# 7. _build_grammar — 模板插值
# ===========================================================================

class TestBuildGrammarInterpolation(TestCase):
    """杀灭插值语法变异（interpolation、self._interpolation 等）。"""

    def setUp(self):
        _clear_parser_cache()

    def tearDown(self):
        _clear_parser_cache()

    def test_template_single_expression(self):
        """基本 {% expr %} 插值。"""
        reg = _registry(add=lambda a, b: a + b)
        p = _fresh()
        self.assertEqual(p.evaluate("result: {% $F.add(1, 2) %}", {}, reg), "result: 3")

    def test_template_multiple_expressions(self):
        """多个 {% %} 块（杀灭 _eval_template__mutmut_4,5 matched=None/True）。"""
        reg = _registry(add=lambda a, b: a + b)
        p = _fresh()
        result = p.evaluate("a={% $F.add(1, 0) %}, b={% $F.add(2, 0) %}", {}, reg)
        self.assertEqual(result, "a=1, b=2")

    def test_template_no_match_unchanged(self):
        """无 {% %} 时原字符串不变（matched=True 会导致空结果）。"""
        p = _fresh()
        s = "just a plain string without any template"
        self.assertEqual(p.evaluate(s, {}), s)

    def test_template_with_variable(self):
        """{% $VAR %} 插值含变量。"""
        p = _fresh()
        result = p.evaluate("Hello {% $INPUT.name %}!", {"$INPUT": {"name": "World"}})
        self.assertEqual(result, "Hello World!")

    def test_template_leading_text_preserved(self):
        """_eval_template last=None 变异：前缀文本必须保留（杀灭 mutmut_2）。"""
        reg = _registry(add=lambda a, b: a + b)
        p = _fresh()
        result = p.evaluate("start-{% $F.add(2, 3) %}-end", {}, reg)
        self.assertEqual(result, "start-5-end")

    def test_template_only_expression(self):
        """{% %} 就是整个字符串时。"""
        reg = _registry(identity=lambda x: x)
        p = _fresh()
        self.assertEqual(p.evaluate("{% $F.identity(42) %}", {}, reg), "42")

    def test_template_suppresses_delimiters(self):
        """结果中不包含 {%、%} 分隔符。"""
        p = _fresh()
        result = p.evaluate("val={% $INPUT %}", {"$INPUT": 99})
        self.assertEqual(result, "val=99")
        self.assertNotIn("{%", result)
        self.assertNotIn("%}", result)


# ===========================================================================
# 8. _eval_variable 变异
# ===========================================================================

class TestEvalVariable(TestCase):
    """杀灭 _eval_variable 中的幸存变异。"""

    def setUp(self):
        _clear_parser_cache()

    def tearDown(self):
        _clear_parser_cache()

    def test_root_variable_simple(self):
        """根变量直接解析。"""
        p = _fresh()
        self.assertEqual(p.evaluate("$INPUT", {"$INPUT": 42}), 42)

    def test_root_variable_missing_raises_key_error(self):
        """缺失根变量抛出 KeyError。"""
        p = _fresh()
        with self.assertRaises(KeyError):
            p.evaluate("$MISSING", {})

    def test_field_access_returns_correct_value(self):
        """_eval_variable__mutmut_9: partition → rpartition（对单冒号标签等价）。

        字段访问必须返回正确值，kind 分支必须是 'field'。"""
        p = _fresh()
        self.assertEqual(p.evaluate("$INPUT.x", {"$INPUT": {"x": 7}}), 7)

    def test_index_access_returns_correct_value(self):
        """索引访问（kind='index'）必须正确分发。"""
        p = _fresh()
        self.assertEqual(p.evaluate("$LIST[-1]", {"$LIST": [10, 20, 30]}), 30)

    def test_recursive_field_eval_uses_parent_as_context(self):
        """_eval_variable__mutmut_25: context→None — 递归 evaluate 必须用父对象作 context。

        attr 是一个 $-表达式字符串，必须用父对象（obj）作为 context 来解析。
        若 context=None 则 context[$key] 抛出 TypeError。"""
        p = _fresh()
        ctx = {
            "$INPUT": {
                "$INPUT": {"value": 42},   # 父对象的 $INPUT 键
                "ptr": "$INPUT.value",      # 字段值是表达式，需用父对象解析
            }
        }
        result = p.evaluate("$INPUT.ptr", ctx)
        self.assertEqual(result, 42)

    def test_recursive_field_eval_uses_registry(self):
        """_eval_variable__mutmut_26: registry→None — 递归 evaluate 必须传递 registry。

        若 registry=None，自定义函数 xyzzy_fn 回退到 default registry（不存在），返回 'undefined'。"""
        reg = _registry(xyzzy_fn=lambda s: "XYZZY:" + s)
        p = _fresh()
        ctx = {
            "$INPUT": {
                "func_result": "$F.xyzzy_fn('test')",
            }
        }
        result = p.evaluate("$INPUT.func_result", ctx, reg)
        self.assertEqual(result, "XYZZY:test")

    def test_recursive_field_eval_with_missing_registry_differs(self):
        """_eval_variable__mutmut_29: evaluate(attr, obj, ) — 丢弃 registry 与 mutmut_26 等价。

        验证：registry 缺失时，xyzzy_fn 必须返回 'undefined'（自定义函数不在 default 中）。"""
        reg = _registry(xyzzy_fn=lambda s: "XYZZY:" + s)
        p = _fresh()
        ctx = {"$INPUT": {"func_result": "$F.xyzzy_fn('test')"}}
        result_with_reg = p.evaluate("$INPUT.func_result", ctx, reg)
        self.assertEqual(result_with_reg, "XYZZY:test")

        _clear_parser_cache()
        p2 = _fresh()
        # 不传 registry → 使用 default registry，xyzzy_fn 不存在 → "undefined"
        result_no_reg = p2.evaluate("$INPUT.func_result", ctx)
        self.assertEqual(result_no_reg, "undefined")


# ===========================================================================
# 9. _eval_function_call 变异
# ===========================================================================

class TestEvalFunctionCall(TestCase):
    """杀灭 _eval_function_call 中的幸存变异。"""

    def setUp(self):
        _clear_parser_cache()

    def tearDown(self):
        _clear_parser_cache()

    def test_function_name_extracted_correctly(self):
        """head.split('.')[1].split('(')[0] 必须提取正确函数名。"""
        reg = _registry(add=lambda a, b: a + b)
        p = _fresh()
        self.assertEqual(p.evaluate("$F.add(3, 4)", {}, reg), 7)

    def test_function_name_with_underscores(self):
        """含下划线的函数名正确提取。"""
        reg = _registry(my_func=lambda x: x + 1)
        p = _fresh()
        self.assertEqual(p.evaluate("$F.my_func(10)", {}, reg), 11)

    def test_unknown_function_returns_undefined(self):
        """未注册函数返回 'undefined'（不抛出异常）。"""
        p = _fresh()
        self.assertEqual(p.evaluate("$F.nonexistent(1)", {}), "undefined")

    def test_unregistered_function_logs_warning_with_func_name(self):
        """_eval_function_call__mutmut_29: func_name→None — warning 日志必须含函数名。

        mutmut_30: registry→None — warning 中 registry 参数必须正确记录。
        注意：logger 使用 getLogger('plaita')，需用 'plaita' 捕获日志。"""
        p = _fresh()
        with self.assertLogs("plaita", level=logging.WARNING) as cm:
            result = p.evaluate("$F.definitely_not_in_registry_abc(42)", {})
        self.assertEqual(result, "undefined")
        # warning 必须包含实际函数名（不能是 None）
        self.assertTrue(
            any("definitely_not_in_registry_abc" in line for line in cm.output),
            msg=f"Function name missing in warning log: {cm.output}",
        )

    def test_function_result_correct_type(self):
        """函数结果类型必须是实际计算值，不能是 None 或 'undefined'。"""
        reg = _registry(mul=lambda a, b: a * b)
        p = _fresh()
        result = p.evaluate("$F.mul(6, 7)", {}, reg)
        self.assertEqual(result, 42)
        self.assertNotEqual(result, "undefined")
        self.assertIsNotNone(result)

    def test_function_with_custom_registry_vs_default(self):
        """_eval_function_call__mutmut_29/30: registry 必须被传递。

        xyzzy 只在自定义 registry 中，若传 None 则返回 'undefined'。"""
        reg = _registry(xyzzy=lambda: "xyzzy!")
        p = _fresh()
        self.assertEqual(p.evaluate("$F.xyzzy()", {}, reg), "xyzzy!")
        # 不传 registry 时，相同函数名 → undefined
        _clear_parser_cache()
        p2 = _fresh()
        self.assertEqual(p2.evaluate("$F.xyzzy()", {}), "undefined")


# ===========================================================================
# 10. _eval_prefix 变异
# ===========================================================================

class TestEvalPrefix(TestCase):
    """杀灭 _eval_prefix 中的幸存变异（parse_all=True/False/None）。"""

    def setUp(self):
        _clear_parser_cache()

    def tearDown(self):
        _clear_parser_cache()

    def test_parse_all_true_rejects_partial_match(self):
        """_eval_prefix__mutmut_6: parse_all=False → partial match silently succeeds.

        parse_all=True 时，$INPUT extra_garbage 必须失败（extra_garbage 不被消耗）。"""
        p = _fresh()
        with self.assertRaises(Exception):
            p.evaluate("$INPUT extra_garbage", {"$INPUT": 1})

    def test_prefix_expression_returns_correct_value(self):
        """_eval_prefix__mutmut_3,5: parse_all=None 或缺失时行为异常。"""
        p = _fresh()
        self.assertEqual(p.evaluate("$INPUT", {"$INPUT": 42}), 42)

    def test_function_expression_parse_all(self):
        """$F.func() 也受 parse_all 约束。"""
        reg = _registry(add=lambda a, b: a + b)
        p = _fresh()
        self.assertEqual(p.evaluate("$F.add(1, 2)", {}, reg), 3)
        with self.assertRaises(Exception):
            p.evaluate("$F.add(1, 2) junk", {}, reg)


# ===========================================================================
# 11. _eval_template 变异
# ===========================================================================

class TestEvalTemplate(TestCase):
    """杀灭 _eval_template 中的幸存变异（matched、last 等）。"""

    def setUp(self):
        _clear_parser_cache()

    def tearDown(self):
        _clear_parser_cache()

    def test_no_template_string_unchanged(self):
        """_eval_template__mutmut_5: matched=True → 无模板时也会 join → 空字符串。

        正确行为：无 {% %} 匹配时，原字符串原样返回。"""
        p = _fresh()
        original = "no template here just a plain sentence"
        self.assertEqual(p.evaluate(original, {}), original)

    def test_template_prefix_text_preserved(self):
        """_eval_template__mutmut_2: last=None → value[None:start] == value[:start] (等价)。

        但通过精确匹配前缀和后缀来验证字符串拼接正确性。"""
        p = _fresh()
        result = p.evaluate("PREFIX-{% $INPUT %}-SUFFIX", {"$INPUT": "MID"})
        self.assertEqual(result, "PREFIX-MID-SUFFIX")

    def test_template_empty_prefix(self):
        """前缀为空时（last=0 情形：value[0:0] = ''）。"""
        p = _fresh()
        result = p.evaluate("{% $INPUT %}tail", {"$INPUT": "head"})
        self.assertEqual(result, "headtail")

    def test_template_between_matches_preserved(self):
        """两个模板块之间的文字必须保留。"""
        p = _fresh()
        result = p.evaluate("{% $A %} AND {% $B %}", {"$A": 1, "$B": 2})
        self.assertEqual(result, "1 AND 2")

    def test_non_template_prefix_string_short(self):
        """不以 $ 开头的短字符串（走 _eval_template 路径）不变。"""
        p = _fresh()
        for s in ["", "hello", "  spaces  ", "123"]:
            self.assertEqual(p.evaluate(s, {}), s)


# ===========================================================================
# 12. parse_function 变异
# ===========================================================================

class TestParseFunction(TestCase):
    """杀灭 parse_function 中的幸存变异。"""

    def setUp(self):
        _clear_parser_cache()

    def tearDown(self):
        _clear_parser_cache()

    def test_non_function_returns_unchanged(self):
        """不含 $F. 的表达式原样返回（不 evaluate）。"""
        p = _fresh()
        self.assertEqual(p.parse_function("plain string", {}), "plain string")

    def test_function_with_context_evaluates(self):
        """parse_function__mutmut_3: context→None — context 必须传递。"""
        reg = _registry(add=lambda a, b: a + b)
        p = _fresh()
        result = p.parse_function("$F.add($INPUT.x, 1)", {"$INPUT": {"x": 9}}, reg)
        self.assertEqual(result, 10)

    def test_function_with_registry_evaluates(self):
        """parse_function__mutmut_4: registry→None — registry 必须传递。

        xyzzy 只在自定义 registry 中，若 registry=None 则返回 'undefined'。"""
        reg = _registry(xyzzy_func=lambda: "found!")
        p = _fresh()
        result = p.parse_function("$F.xyzzy_func()", {}, reg)
        self.assertEqual(result, "found!")

    def test_function_without_registry_uses_default(self):
        """不传 registry 时使用 default registry（upper 函数存在）。"""
        p = _fresh()
        result = p.parse_function("$F.upper('hello')", {})
        self.assertEqual(result, "HELLO")

    def test_bad_function_syntax_returns_unchanged(self):
        """parse_function 对 ParseException 返回原字符串（不抛出）。

        使用 '$F.(invalid)' 格式（identifier 不以 '(' 开头），
        触发 function_call 和 variable 都失败时的 ParseException。
        context 中提供 '$F' 防止 _eval_variable 内 KeyError。"""
        p = _fresh()
        bad = "$F.(invalid)"
        result = p.parse_function(bad, {"$F": None})
        self.assertEqual(result, bad)

    def test_function_result_matches_evaluate(self):
        """parse_function__mutmut_6: evaluate(expr, registry) 少传 context。

        验证 parse_function 与 evaluate 产生相同结果。"""
        reg = _registry(double=lambda x: x * 2)
        p = _fresh()
        pf_result = p.parse_function("$F.double(5)", {}, reg)
        ev_result = p.evaluate("$F.double(5)", {}, reg)
        self.assertEqual(pf_result, ev_result)
        self.assertEqual(pf_result, 10)


# ===========================================================================
# 13. evaluate 变异
# ===========================================================================

class TestEvaluate(TestCase):
    """杀灭 evaluate__mutmut_3（_push_frame prefix→None）。"""

    def setUp(self):
        _clear_parser_cache()

    def tearDown(self):
        _clear_parser_cache()

    def test_prefix_used_in_frame(self):
        """evaluate__mutmut_3: _push_frame 传 None 而非 self.prefix。

        在 _eval_variable 中，special root 字段名用 prefix 拼接。
        若 prefix=None，则 f'{None}INPUT' = 'NoneINPUT' 导致解析错误。"""
        p = ExpressionParser.for_prefix("$")
        # 通过特殊根路径验证 prefix 被正确使用
        ctx = {"$INPUT": {"$GLOBAL": "global_val"}}
        result = p.evaluate("$INPUT.$GLOBAL", ctx)
        self.assertEqual(result, "global_val")

    def test_custom_prefix_evaluates_correctly(self):
        """自定义前缀下 evaluate 必须正确工作。"""
        p = ExpressionParser.for_prefix("#")
        result = p.evaluate("#VAR", {"#VAR": "custom"})
        self.assertEqual(result, "custom")

    def test_evaluate_list_element(self):
        """evaluate 对 list 类型逐元素求值。"""
        p = _fresh()
        ctx = {"$X": 10, "$Y": 20}
        result = p.evaluate(["$X", "$Y", "plain"], ctx)
        self.assertEqual(result, [10, 20, "plain"])

    def test_evaluate_dict_values(self):
        """evaluate 对 dict 类型逐值求值。"""
        p = _fresh()
        ctx = {"$X": 10}
        result = p.evaluate({"key": "$X", "literal": "abc"}, ctx)
        self.assertEqual(result, {"key": 10, "literal": "abc"})

    def test_evaluate_non_string_passthrough(self):
        """非字符串类型直接透传（int, float, None）。"""
        p = _fresh()
        for v in [42, 3.14, None, True]:
            self.assertEqual(p.evaluate(v, {}), v)


# ===========================================================================
# 14. _get_attr 变异
# ===========================================================================

class TestGetAttr(TestCase):
    """杀灭 _get_attr 幸存变异（__get_attr__mutmut_3 等价变异不可杀）。"""

    def test_dict_key_present(self):
        """dict 有 key 时返回对应值。"""
        self.assertEqual(_get_attr({"k": "v"}, "k"), "v")

    def test_dict_key_missing_returns_none(self):
        """dict 无 key 时返回 None（obj.get(path, None) 与 obj.get(path,) 等价）。"""
        self.assertIsNone(_get_attr({"k": "v"}, "missing"))

    def test_dict_like_object_uses_get(self):
        """dict-like 对象（有 __getitem__ 和 get）走 dict 分支。"""
        class DictLike:
            def __getitem__(self, k):
                return "item"
            def get(self, k, default=None):
                return f"got:{k}"
        self.assertEqual(_get_attr(DictLike(), "mykey"), "got:mykey")

    def test_attribute_object_uses_getattr(self):
        """普通对象走 getattr 分支。"""
        class Obj:
            name = "attr_value"
        self.assertEqual(_get_attr(Obj(), "name"), "attr_value")

    def test_attribute_object_missing_attr_returns_none(self):
        """普通对象缺失属性返回 None。"""
        class Obj:
            pass
        self.assertIsNone(_get_attr(Obj(), "nonexistent"))

    def test_primitive_returns_none(self):
        """无 __dict__、无 get 的基本类型返回 None。"""
        self.assertIsNone(_get_attr(42, "anything"))
        self.assertIsNone(_get_attr(None, "x"))

    def test_list_returns_none_for_field(self):
        """list 对象字段访问返回 None（用索引访问才有效）。"""
        self.assertIsNone(_get_attr([1, 2, 3], "name"))


if __name__ == "__main__":
    unittest.main()
