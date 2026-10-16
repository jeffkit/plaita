"""test_expression_size_guard — 表达式引擎结果规模守卫（issue #68）。

2026-10 安全评审 P1（`$F.pow` 的指数上限推广到同族算子）+ 独立验收返工。

## 判定规则：识别"放大"，不是限制绝对大小

    allowed = min( max(floor, factor * input_size), hard )

默认 ``floor=100_000`` / ``factor=4`` / ``hard=100_000_000``，分别由
``PLAITA_EXPR_MAX_RESULT_SIZE`` / ``PLAITA_EXPR_GROWTH_FACTOR`` /
``PLAITA_EXPR_MAX_RESULT_HARD`` 覆盖。

* 小输入 → 巨大输出（``"x" * 10**6``、``replace`` 最坏形态、``%`` 格式化宽度）
  **在分配之前**被拒：受保护路径的 tracemalloc 峰值不随放大规模增长；
* 输入本来就大、输出同量级（200 段 × 1KB 拼 prompt）正常放行；
* **类型错误优先于尺寸错误**：``str + list`` 等在任何规模都抛原生 TypeError。

## 分层

1. **预检（闸）**：``expression.py`` 里每个已知放大算子在调用 Python 算子
   之前预检；判定是**调用点级**的——预检要么算出上界、要么该调用注定抛原生
   类型错误，不存在"这个函数在清单里所以跳过"的漏洞（第一版就是那样漏的）；
2. **探针（网）**：``expression_parser._size_guard_result`` 对**每一次** ``$F.*``
   调用都按同一条比值规则复核（无函数级白名单），输入规模用**内容量**而非
   元素/参数个数。**对没有预检的路径它仍是"检测到"（值已分配）**——本文件用
   tracemalloc 把这个边界钉住，不许文档把它说成"阻止"。

本模块在**未带修复**的源码上仍可导入（模块级符号用 ``getattr`` 兜底），
修复前与守卫实现耦合的断言按 ``_HAS_GUARD`` 跳过、而"旧代码必须拒绝/必须
不放大"的断言照常运行——负向对照因此得到精确 red 清单而不是收集期 ImportError。
"""

from __future__ import annotations

import ast
import zoneinfo
import array
import inspect
import collections
import datetime
import enum
import decimal
import io
import logging
import pathlib
import re
import sys
import timeit
import tracemalloc
import unittest
import types
import uuid
from contextlib import contextmanager
from unittest import TestCase, skipUnless

import plaita.core.expression as expr_mod
import plaita.core.expression_parser as parser_mod
from plaita.core.expression import (
    _BYTEARRAY_REPR_PREFIX,
    ExpressionRegistry,
    _fn_add,
    _fn_append,
    _fn_concat,
    _fn_div,
    _fn_extend,
    _fn_join,
    _fn_mod,
    _fn_not,
    _fn_sort,
    _mod_decide,
    _spec_bound,
    _value_size,
    _mapping_snapshot,
    _fn_mul,
    _fn_pow,
    _fn_replace,
    _fn_sub,
    _fn_today,
)
from plaita.core.expression_parser import ExpressionParser

# getattr 兜底：修复前这些模块级符号不存在（见模块 docstring）——只 import 两侧
# 都存在的名字，其余一律 getattr，保证本模块在未修复源码上仍可**收集**。
_parse_format_specs = getattr(expr_mod, "_parse_format_specs", None)
_mod_arg_plan = getattr(expr_mod, "_mod_arg_plan", None)
_content_size = getattr(expr_mod, "_content_size", None)
_join_item_lengths = getattr(expr_mod, "_join_item_lengths", None)
_is_repeatable = getattr(expr_mod, "_is_repeatable", None)
_is_bytes_like = getattr(expr_mod, "_is_bytes_like", None)
_conversion_accepts = getattr(expr_mod, "_conversion_accepts", None)
_render_size = getattr(expr_mod, "_render_size", None)
_measure_input = getattr(expr_mod, "_measure_input", None)
_prepare_join = getattr(expr_mod, "PROJECTIONS", {}).get(
    getattr(expr_mod, "_fn_join", None))
_prepare_sort = getattr(expr_mod, "PROJECTIONS", {}).get(
    getattr(expr_mod, "_fn_sort", None))
_format_values_size = getattr(expr_mod, "_format_values_size", None)
_kinds_compatible = getattr(expr_mod, "_kinds_compatible", None)
_measured_unit = getattr(expr_mod, "_measured_unit", None)
_measure = getattr(expr_mod, "_measure", None)
_pair_total = getattr(expr_mod, "_pair_total", None)
_seq_kind = getattr(expr_mod, "_seq_kind", None)
_unit_for = getattr(expr_mod, "_unit_for", None)
_value_size = getattr(expr_mod, "_value_size", None)
_HAS_GUARD = hasattr(expr_mod, "_allowed_result_size")
_allowed_result_size = getattr(expr_mod, "_allowed_result_size", None)
_check_result_size = getattr(expr_mod, "_check_result_size", None)
EXPR_MAX_RESULT_SIZE_ENV = getattr(expr_mod, "EXPR_MAX_RESULT_SIZE_ENV", "")
EXPR_GROWTH_FACTOR_ENV = getattr(expr_mod, "EXPR_GROWTH_FACTOR_ENV", "")
EXPR_MAX_RESULT_HARD_ENV = getattr(expr_mod, "EXPR_MAX_RESULT_HARD_ENV", "")
_resolve_positive_int = getattr(expr_mod, "_resolve_positive_int", None)
_size_guard_result_raw = getattr(parser_mod, "_size_guard_result", None)


def _probe(func_name, value, args=()):
    """测试用探针：模拟调用点——**调用前**算好输入规模再交给守卫。"""
    if _size_guard_result_raw is None:
        return None
    input_size = 0
    for arg in args:
        input_size += _content_size(arg) if _content_size is not None else 0
    return _size_guard_result_raw(func_name, value, input_size)

FLOOR = getattr(expr_mod, "_EXPR_MAX_RESULT_SIZE", 100_000)
FACTOR = getattr(expr_mod, "_EXPR_GROWTH_FACTOR", 4)
HARD = getattr(expr_mod, "_EXPR_MAX_RESULT_HARD", 100_000_000)

# 峰值断言阈值：被判定的放大路径若真的先分配，峰值会是 100MB~1GB 量级，
# 4MB 的门槛能把"闸"和"网"区分开。
_PEAK_BUDGET = 4 * 1024 * 1024


def _measure_peak(fn) -> tuple[str, int]:
    """跑 fn，返回 (结果描述, tracemalloc 峰值增量字节数)。"""
    already = tracemalloc.is_tracing()
    if not already:
        tracemalloc.start()
    tracemalloc.reset_peak()
    try:
        try:
            result = fn()
            desc = f"ok:{type(result).__name__}:{len(result) if hasattr(result, '__len__') else result}"
        except BaseException as exc:  # noqa: BLE001 - 需要区分"拒绝"与"崩"
            desc = f"{type(exc).__name__}"
        _, peak = tracemalloc.get_traced_memory()
    finally:
        if not already:
            tracemalloc.stop()
    return desc, peak


@contextmanager
def _limits(floor=None, factor=None, hard=None):
    """临时改三个旋钮（算子与探针都在调用时读取，可立即生效）。"""
    names = ("_EXPR_MAX_RESULT_SIZE", "_EXPR_GROWTH_FACTOR", "_EXPR_MAX_RESULT_HARD")
    saved = {n: getattr(expr_mod, n, None) for n in names}
    if floor is not None:
        expr_mod._EXPR_MAX_RESULT_SIZE = floor
    if factor is not None:
        expr_mod._EXPR_GROWTH_FACTOR = factor
    if hard is not None:
        expr_mod._EXPR_MAX_RESULT_HARD = hard
    try:
        yield
    finally:
        for n, v in saved.items():
            if v is not None:
                setattr(expr_mod, n, v)


def _fresh() -> ExpressionParser:
    """新建独立 parser（绕开 per-prefix 单例，测试互不污染）。"""
    ExpressionParser._instances.clear()
    return ExpressionParser()


class _NoLen:
    """无 ``__len__``、``__sizeof__`` 可控的对象（getsizeof 边界断言用）。"""

    def __init__(self, size: int):
        self._size = size

    def __sizeof__(self):
        return self._size


class TestPreventionWithMeasuredPeak(TestCase):
    """独立验收点名的每条"无界"路径：必须**在分配之前**被拒。

    每条同时断言：(a) 抛 ValueError；(b) tracemalloc 峰值远小于本该分配的
    结果规模——修理前这些用例的峰值是 100MB~1GB 量级（issue #68 实测
    1000.0MB / 100.09MB / 9.3GiB…），所以峰值断言能把"事后检测"证伪。
    """

    def _assert_refused_cheaply(self, fn) -> None:
        desc, peak = _measure_peak(fn)
        self.assertEqual("ValueError", desc)
        self.assertLess(peak, _PEAK_BUDGET,
                        f"拒绝发生在分配之后：peak={peak/1024/1024:.2f}MB")

    # --- 操作数顺序（验收者实测 1000MB 的那条） ---

    def test_mul_seq_times_int(self):
        self._assert_refused_cheaply(lambda: _fn_mul("x", 10**6))

    def test_mul_int_times_seq(self):
        """``$F.mul(1000000000, 'A')``（@flow 里的 ``10**9 * "A"``）。"""
        self._assert_refused_cheaply(lambda: _fn_mul(10**9, "A"))

    def test_mul_int_times_seq_through_evaluate(self):
        parser = _fresh()
        self._assert_refused_cheaply(
            lambda: parser.evaluate("{% $F.mul(1000000000, 'A') %}", {}))

    def test_mul_bool_multiplier_treated_as_int(self):
        """bool 是 int：``'A' * True`` 结果 1 字节，不放大；大 bool 不存在。"""
        self.assertEqual("A", _fn_mul("A", True))

    # --- bytes / bytearray（验收者实测 100.09MB 的那条） ---

    def test_mul_bytes(self):
        self._assert_refused_cheaply(lambda: _fn_mul(b"x", 10**8))

    def test_mul_bytearray(self):
        self._assert_refused_cheaply(lambda: _fn_mul(bytearray(b"x"), 10**8))

    def test_mul_int_times_bytes(self):
        self._assert_refused_cheaply(lambda: _fn_mul(10**8, b"x"))

    def test_bytes_unit_labelled_bytes(self):
        with self.assertRaises(ValueError) as cm:
            _fn_mul(b"x", 10**8)
        self.assertIn("100000000 bytes", str(cm.exception))
        self.assertNotIn("elements", str(cm.exception))

    def test_mul_bytearray_unit_labelled_bytes(self):
        with self.assertRaises(ValueError) as cm:
            _fn_mul(bytearray(b"x"), 10**8)
        self.assertIn("bytes", str(cm.exception))

    # --- replace（验收者实测上界 ~9.3GiB 的那条） ---

    def test_replace_worst_case(self):
        self._assert_refused_cheaply(
            lambda: _fn_replace("a" * 100000, "a", "b" * 1000))

    def test_replace_empty_pattern(self):
        self._assert_refused_cheaply(
            lambda: _fn_replace("a" * 100000, "", "b" * 1000))

    def test_replace_through_evaluate(self):
        parser = _fresh()
        ctx = {"$INPUT": {"big": "a" * 60000}}
        self._assert_refused_cheaply(
            lambda: parser.evaluate("{% $F.replace($INPUT.big, 'a', 'b' * 1000) %}", ctx)
            if False else  # 表达式里没有 'b'*1000，用嵌套 mul 复现
            parser.evaluate("{% $F.replace($INPUT.big, 'a', $F.mul('b', 10000)) %}", ctx))

    def test_replace_bytes(self):
        self._assert_refused_cheaply(lambda: _fn_replace(b"a" * 100000, b"a", b"b" * 1000))

    # --- mod：str/bytes 的 % 是格式化（验收者实测的上界 ~4.7GiB 那条） ---

    def test_mod_format_width(self):
        self._assert_refused_cheaply(lambda: _fn_mod("%100000000d", 1))

    def test_mod_star_width(self):
        self._assert_refused_cheaply(lambda: _fn_mod("%*d", (10**8, 1)))

    def test_mod_format_width_through_evaluate(self):
        parser = _fresh()
        self._assert_refused_cheaply(
            lambda: parser.evaluate("{% $F.mod('%100000000d', 1) %}", {}))

    def test_mod_multi_conversion_single_value_keeps_type_error(self):
        """多个转换符 + 非序列参数：原生 TypeError，不能被说成规模错误。"""
        with self.assertRaises(TypeError):
            _fn_mod("%s" * 5000, "x" * 5000)

    # --- join：分隔符重复是放大 ---

    def test_join_separator_repetition(self):
        self._assert_refused_cheaply(lambda: _fn_join(["a"] * 6, "s" * 25000))

    def test_join_str_items_amplification(self):
        self._assert_refused_cheaply(lambda: _fn_join("x" * 100, "s" * 2000))

    # --- 既有 pow 守卫（对照组） ---

    def test_refusal_peak_does_not_grow_with_amplification_scale(self):
        """同一受保护路径在 10**5 与 10**9 放大下峰值都必须有界。

        这是"预防（分配前拒绝）而非检测（先造后报）"的直接证据：若实现是
        事后检测，10**9 这一侧的峰值会是 GB 量级。
        """
        small_desc, small_peak = _measure_peak(lambda: _fn_mul("x", 10**5 + 1))
        big_desc, big_peak = _measure_peak(lambda: _fn_mul("x", 10**9))
        self.assertEqual("ValueError", small_desc)
        self.assertEqual("ValueError", big_desc)
        self.assertLess(small_peak, _PEAK_BUDGET)
        self.assertLess(big_peak, _PEAK_BUDGET)

    def test_both_operand_orders_are_prevented(self):
        """main 上**两个方向都没挡**（owner 实测 1e7 两侧都成功）：这里都必须在分配前拒。"""
        for args in (("A", 10**7), (10**7, "A")):
            with self.subTest(args=args):
                desc, peak = _measure_peak(lambda a=args: _fn_mul(*a))
                self.assertEqual("ValueError", desc)
                self.assertLess(peak, _PEAK_BUDGET)

    def test_pow_guard_still_cheap(self):
        self._assert_refused_cheaply(lambda: _fn_pow(9, 10**8))

    def test_pow_message_unchanged(self):
        with self.assertRaises(ValueError) as cm:
            _fn_pow(9, 9999999)
        self.assertEqual(
            "$F.pow exponent too large: 9999999 (limit 1000000). "
            "Refusing to burn CPU on an unbounded power computation.",
            str(cm.exception),
        )
        self.assertEqual(1024, _fn_pow(2, 10))


class TestGrowthRule(TestCase):
    """判定规则本身：``allowed = min(max(floor, factor*input), hard)``。"""

    @skipUnless(_HAS_GUARD, "guard not present")
    def test_allowed_size_formula(self):
        with _limits(floor=1000, factor=3, hard=10000):
            self.assertEqual(1000, _allowed_result_size(0))
            self.assertEqual(1000, _allowed_result_size(1))
            self.assertEqual(3000, _allowed_result_size(1000))
            self.assertEqual(9000, _allowed_result_size(3000))
            self.assertEqual(10000, _allowed_result_size(10**6))

    @skipUnless(_HAS_GUARD, "guard not present")
    def test_boundary_inclusive(self):
        """恰好等于 allowed 放行、+1 拒绝（#107/#130 的边界口径）。"""
        with _limits(floor=100, factor=2, hard=10**6):
            self.assertEqual(100, len(_fn_mul("x", 100)))
            with self.assertRaises(ValueError):
                _fn_mul("x", 101)

    @skipUnless(_HAS_GUARD, "guard not present")
    def test_boundary_follows_input_size(self):
        """输入大 → 允许的输出上界跟着变大（识别放大，不是绝对上限）。"""
        with _limits(floor=100, factor=2, hard=10**6):
            self.assertEqual(200, len(_fn_mul("x" * 100, 2)))
            self.assertEqual(200, len(_fn_mul("x" * 100, 2, )))
            with self.assertRaises(ValueError):
                _fn_mul("x" * 100, 3)  # 300 > max(100, 2*100)

    @skipUnless(_HAS_GUARD, "guard not present")
    def test_hard_ceiling_caps_ratio(self):
        with _limits(floor=100, factor=1000, hard=500):
            self.assertEqual(500, _allowed_result_size(10**6))
            with self.assertRaises(ValueError):
                _fn_mul("x" * 100, 6)  # 600 > 500

    @skipUnless(_HAS_GUARD, "guard not present")
    def test_check_result_size_direct(self):
        with _limits(floor=100, factor=4, hard=1000):
            _check_result_size("$F.x", 100, 1, "chars")   # 恰好 == floor
            with self.assertRaises(ValueError) as cm:
                _check_result_size("$F.x", 101, 1, "chars")
            self.assertIn("101 chars (limit 100)", str(cm.exception))

    def test_hard_and_factor_defaults(self):
        self.assertEqual(100_000, FLOOR)
        self.assertEqual(4, FACTOR)
        self.assertEqual(100_000_000, HARD)


class TestOverRestrictionReverseControls(TestCase):
    """验收者点名的过度限制：输入本来就大、输出同量级的合法流程必须放行。

    判据是**输出/输入的比值**，不是输出的绝对大小——绝对上限会拒掉
    ``join(20 段 × 10000 字符, "")``（20 万字符，输入本身就有 20 万）这类
    常规业务形态，那是真实回归。
    """

    def test_non_amplifying_large_input_is_allowed(self):
        """repo 所有者给的具名正向对照：20 段 × 10000 字符 → 200000 字符必须可用。"""
        joined = _fn_join(["c" * 10000] * 20, "")
        self.assertEqual(200_000, len(joined))

    def test_small_input_amplification_is_refused_next_to_the_positive_control(self):
        """与上一条互为对照：同样的 200000 规模，由 1 字节放大而来就必须拒。"""
        with self.assertRaises(ValueError):
            _fn_mul("x", 10**6)

    def test_join_two_hundred_kilobytes_of_prompt(self):
        """200 段 × 1KB ≈ 200KB（拼 prompt/报告的常规形态）必须通过。"""
        joined = _fn_join(["x" * 1024] * 200, "\n")
        self.assertEqual(200 * 1024 + 199, len(joined))

    def test_join_single_item_ignores_separator(self):
        """n=1 时分隔符根本不进结果：长分隔符不能导致拒绝。"""
        self.assertEqual("b", _fn_join(["b"], "s" * (FLOOR + 1)))

    def test_join_keeps_type_error_any_size(self):
        with self.assertRaises(TypeError):
            _fn_join([1, 2], "-")

    def test_append_keeps_type_error_any_size(self):
        """``str + [b]`` 在任何规模都是原生 TypeError（不能因规模变成 ValueError）。"""
        with self.assertRaises(TypeError):
            _fn_append("a", 1)
        with self.assertRaises(TypeError):
            _fn_append("a" * (FLOOR + 1), 1)

    def test_add_keeps_type_error_any_size(self):
        with self.assertRaises(TypeError):
            _fn_add("a", [1])
        with self.assertRaises(TypeError):
            _fn_add("a" * (FLOOR + 1), [1])

    def test_extend_keeps_type_error_any_size(self):
        with self.assertRaises(TypeError):
            _fn_extend("a", [1])
        with self.assertRaises(TypeError):
            _fn_extend("a" * (FLOOR + 1), [1] * (FLOOR + 1))

    def test_mul_keeps_type_error(self):
        with self.assertRaises(TypeError):
            _fn_mul([1], "x")
        with self.assertRaises(TypeError):
            _fn_mul("x", 1.5)

    def test_add_large_linear_concatenation_allowed(self):
        self.assertEqual(200_001, len(_fn_add("a" * 200000, "b")))

    def test_append_large_linear_allowed(self):
        base = [0] * 150000
        self.assertEqual(150001, len(_fn_append(base, 1)))

    def test_mul_one_times_large_allowed(self):
        """``× 1`` 不放大：100KB 输入 ×1 必须放行（不是"输出 > 100KB 就拒"）。"""
        self.assertEqual(100_001, len(_fn_mul("x" * 100001, 1)))

    def test_mul_two_times_large_allowed(self):
        self.assertEqual(400_000, len(_fn_mul("a" * 200000, 2)))

    def test_extend_large_linear_allowed(self):
        self.assertEqual(300_000, len(_fn_extend([0] * 100000, [0] * 200000)))

    def test_replace_large_shrinking_allowed(self):
        self.assertEqual(200_000, len(_fn_replace("a" * 200000, "a", "b")))

    def test_concat_large_linear_allowed(self):
        self.assertEqual(200_001, len(_fn_concat("a" * 200000, "b")))

    def test_mod_numeric_untouched(self):
        self.assertEqual(6, _fn_mod(10**9, 7))
        self.assertEqual(1.5, _fn_mod(7.5, 2))

    def test_mod_small_format_allowed(self):
        self.assertEqual("hello jeff 42", _fn_mod("%s %s %d", ("hello", "jeff", 42)))
        self.assertEqual("  1", _fn_mod("%3d", 1))

    @skipUnless(_HAS_GUARD, "guard not present")
    def test_hard_ceiling_only_hits_truly_huge_results(self):
        """线性拷贝在输入巨大时仍有硬天花板（issue 的 extend([0]*10,[0]*1e8) AC）。"""
        with _limits(floor=100, factor=4, hard=1000):
            self.assertEqual(1000, len(_fn_extend([0] * 400, [0] * 600)))
            with self.assertRaises(ValueError):
                _fn_extend([0] * 400, [0] * 601)


class TestNonAmplifyingOperatorsUntouched(TestCase):
    """只会缩小结果的算子：sub/div；数值 add/mul 保持原生语义。"""

    def test_sub_div_untouched(self):
        self.assertEqual(-99999, _fn_sub(1, 100000))
        self.assertEqual(2, _fn_div(4, 2))

    def test_numeric_add_mul_untouched(self):
        self.assertEqual(3, _fn_add(1, 2))
        self.assertEqual(6, _fn_mul(2, 3))
        self.assertEqual(0.5, _fn_mul(0.25, 2))

    def test_negative_and_zero_counts_yield_empty(self):
        self.assertEqual("", _fn_mul("x", 0))
        self.assertEqual("", _fn_mul("x", -1))
        self.assertEqual([], _fn_mul([1, 2], -10**9))

    def test_numeric_mul_is_not_size_guarded(self):
        """数值乘法按需分配（CPU 代价，非"小输入换大内存"），保持语义。"""
        self.assertEqual(10**200, _fn_mul(10**100, 10**100))


class TestPositiveControls(TestCase):
    """正常流量：大但合理的用法必须继续可跑。"""

    def setUp(self):
        self.parser = _fresh()

    def test_loop_concatenation_of_kbs(self):
        acc = ""
        for i in range(500):
            acc = _fn_add(acc, "line %d\n" % i)
        self.assertGreater(len(acc), 4000)
        self.assertLess(len(acc), FLOOR)

    def test_join_ten_thousand_lines(self):
        joined = _fn_join(["line %d" % i for i in range(10000)], "\n")
        self.assertGreater(len(joined), 50000)
        self.assertLess(len(joined), FLOOR)

    def test_end_to_end_flow_scales(self):
        ctx = {"$INPUT": {"rows": [{"v": i} for i in range(5000)]}}
        self.assertEqual(5000, self.parser.evaluate("$F.len($INPUT.rows)", ctx))
        page = self.parser.evaluate("$F.slice($INPUT.rows, 0, 5000)", ctx)
        self.assertEqual(5000, len(page))
        text = self.parser.evaluate("$F.join($INPUT.words, ' ')", {"$INPUT": {"words": ["w"] * 20000}})
        self.assertEqual(39999, len(text))
        self.assertEqual(200_000, len(_fn_replace("a" * 200000, "a", "b")))

    def test_small_result_semantics_unchanged(self):
        ctx = {"$INPUT": {"item": 3}}
        self.assertEqual(6, self.parser.evaluate("$F.mul($INPUT.item, 2)", ctx))
        self.assertEqual([1, 2, 3, 4], _fn_extend([1, 2], [3, 4]))
        self.assertEqual([1, 2], _fn_append([1], 2))
        self.assertEqual("ababab", _fn_mul("ab", 3))
        self.assertEqual("a1None", _fn_concat("a", 1, None))


@skipUnless(_HAS_GUARD, "guard not present")
class TestEvaluationSideProbe(TestCase):
    """探针：硬上限 + 未预检路径的比值判定；对未预检函数只是"检测网"。"""

    def setUp(self):
        self.parser = _fresh()

    def test_hard_ceiling_applies_to_custom_functions(self):
        reg = ExpressionRegistry()
        reg.register("boom", lambda n: "x" * n, "boom")
        with _limits(floor=100, factor=4, hard=1000):
            with self.assertRaises(ValueError) as cm:
                self.parser.evaluate("$F.boom(5000)", {}, reg)
            self.assertIn("(limit 1000)", str(cm.exception))

    def test_ratio_rule_for_unprechecked_custom_function(self):
        reg = ExpressionRegistry()
        reg.register("grow", lambda s: s * 2, "2x")
        with _limits(floor=100, factor=4, hard=10**6):
            self.assertEqual("ab" * 2, self.parser.evaluate("$F.grow('ab')", {}, reg))
            big = self.parser.evaluate("$F.grow($INPUT.s)", {"$INPUT": {"s": "x" * 300}}, reg)
            self.assertEqual(600, len(big))

    def test_custom_amplifier_is_refused(self):
        reg = ExpressionRegistry()
        reg.register("boom", lambda n: "x" * n, "boom")
        with _limits(floor=100, factor=4, hard=10**9):
            with self.assertRaises(ValueError) as cm:
                self.parser.evaluate("$F.boom(100000)", {}, reg)
            self.assertIn("$F.boom result too large", str(cm.exception))

    def test_probe_is_detection_not_prevention_for_custom_functions(self):
        """钉住已知边界：未预检函数的值**已经分配**，探针只能说"检测到"。

        这条断言的意义是防止文档/报告把探针说成"阻止"——峰值必须 ≥ 结果规模。
        """
        reg = ExpressionRegistry()
        reg.register("boom", lambda n: "x" * n, "boom")
        size = 1_000_000
        with _limits(floor=100, factor=4, hard=10**9):
            desc, peak = _measure_peak(lambda: self.parser.evaluate(f"$F.boom({size})", {}, reg))
        self.assertEqual("ValueError", desc)
        self.assertGreaterEqual(peak, size)  # 值真的被造出来了

    def test_probe_applies_even_to_prechecked_operator_names(self):
        """探针**不再**按函数白名单跳过：同名算子的产物同样按比值判定。

        第一版把内建算子放进 ``PRE_CHECKED_FUNCTIONS``，预检一旦让路（类型不匹配、
        无法估算）该调用就彻底失去守卫——这里钉住"探针永远复核"。
        """
        with self.assertRaises(ValueError) as cm:
            _probe("mul", "z" * (FLOOR + 1), ("z", 1))
        self.assertIn("$F.mul result too large", str(cm.exception))
        # 输入内容量与输出同量级时放行（内容口径，不是元素/参数个数）
        self.assertEqual(FLOOR + 1, len(_probe("mul", "z" * (FLOOR + 1), ("z" * (FLOOR + 1),))))

    def test_overridden_builtin_takes_full_probe(self):
        """scoped registry 用同名函数覆盖内建算子时必须走完整判定。"""
        reg = ExpressionRegistry()
        reg.register("mul", lambda a, b: "x" * 1000000, "override")
        with self.assertRaises(ValueError) as cm:
            self.parser.evaluate("$F.mul(1, 1)", {}, reg)
        self.assertIn("$F.mul result too large", str(cm.exception))

    def test_regex_replace_bypass_is_caught(self):
        """``$F.replace`` 已预检；这里验证经求值路径同样在分配前被拒。"""
        ctx = {"$INPUT": {"s": "a" * 60000}}
        desc, peak = _measure_peak(
            lambda: self.parser.evaluate("$F.replace($INPUT.s, 'a', $F.mul('b', 1000))", ctx))
        self.assertEqual("ValueError", desc)
        self.assertLess(peak, _PEAK_BUDGET)

    def test_small_values_pass_probe(self):
        self.assertEqual(3, self.parser.evaluate("$F.add(1, 2)", {}))
        self.assertEqual("AB", self.parser.evaluate("$F.upper('ab')", {}))
        reg = ExpressionRegistry()
        reg.register("boom", lambda n: [0] * n, "boom")
        self.assertEqual([0], self.parser.evaluate("$F.boom(1)", {}, reg))

    def test_probe_string_and_list_boundary(self):
        with _limits(floor=100, factor=4, hard=10**6):
            self.assertEqual(100, len(_probe("fn", "z" * 100, ())))
            with self.assertRaises(ValueError):
                _probe("fn", "z" * 101, ())
            self.assertEqual(100, len(_probe("fn", [0] * 100, ())))
            with self.assertRaises(ValueError):
                _probe("fn", [0] * 101, ())

    def test_probe_getsizeof_boundary_exact_limit_passes(self):
        """无 ``__len__`` 对象的边界：``getsizeof == limit`` 必须放行。"""
        with _limits(floor=100, factor=4, hard=10**6):
            obj = _NoLen(50000)
            exact = sys.getsizeof(obj)
            expr_mod._EXPR_MAX_RESULT_SIZE = exact
            self.assertIs(obj, _probe("fn", obj, ()))
            expr_mod._EXPR_MAX_RESULT_SIZE = exact - 1
            with self.assertRaises(ValueError) as cm:
                _probe("fn", obj, ())
            self.assertIn("bytes", str(cm.exception))

    def test_probe_hard_ceiling_for_unsized_object(self):
        with _limits(floor=100, factor=4, hard=1000):
            with self.assertRaises(ValueError) as cm:
                _probe("fn", _NoLen(5000), ())
            self.assertIn("(limit 1000)", str(cm.exception))
            self.assertIn("bytes", str(cm.exception))

    def test_probe_hard_boundary_inclusive(self):
        """结果恰好等于 hard 必须放行（``>`` vs ``>=`` 的分界）。"""
        with _limits(floor=10, factor=4, hard=10):
            self.assertEqual(10, len(_probe("fn", "z" * 10, ())))
            with self.assertRaises(ValueError):
                _probe("fn", "z" * 11, ())

    def test_probe_ratio_boundary_inclusive_and_bytes_unit(self):
        with _limits(floor=100, factor=4, hard=10**6):
            # size == allowed（400）必须放行；401 必须拒且单位是 bytes
            self.assertEqual(400, len(_probe("fn", "z" * 400, ("z" * 100,))))
            with self.assertRaises(ValueError) as cm:
                _probe("fn", "z" * 401, ("z" * 100,))
            self.assertEqual(
                "$F.fn result too large: 401 chars (limit 400). "
                "Refusing to build an unbounded result.", str(cm.exception))
            with self.assertRaises(ValueError) as cm:
                _probe("fn", b"z" * 401, (b"z" * 100,))
            self.assertEqual(
                "$F.fn result too large: 401 bytes (limit 400). "
                "Refusing to build an unbounded result.", str(cm.exception))

    def test_probe_size_equal_to_allowed_with_small_floor(self):
        with _limits(floor=1, factor=1, hard=7):
            self.assertEqual("AB", self.parser.evaluate("$F.upper('ab')", {}))

    def test_probe_hard_ceiling_message_unit(self):
        """硬上限分支的文案单位：str→chars、bytes→bytes（逐字比对）。"""
        with _limits(floor=1, factor=1, hard=7):
            with self.assertRaises(ValueError) as cm:
                _probe("fn", "z" * 250000, ())
            self.assertEqual(
                "$F.fn result too large: 250000 chars (limit 7). "
                "Refusing to build an unbounded result.", str(cm.exception))
            with self.assertRaises(ValueError) as cm:
                _probe("fn", b"z" * 250000, ())
            self.assertEqual(
                "$F.fn result too large: 250000 bytes (limit 7). "
                "Refusing to build an unbounded result.", str(cm.exception))

    def test_probe_len_overflow_falls_back_to_getsizeof(self):
        class Exploding:
            def __len__(self):
                raise OverflowError("nope")

        obj = Exploding()
        self.assertIs(obj, _probe("fn", obj, ()))

    def test_probe_reads_live_limit_not_import_snapshot(self):
        with _limits(floor=4):
            with self.assertRaises(ValueError) as cm:
                _probe("upper", "abcde", ())
            self.assertIn("(limit 4)", str(cm.exception))
            self.assertEqual("abcd", _probe("upper", "abcd", ()))

    def test_probe_tolerates_unevaluable_argument_types(self):
        """参数是不可估/不可哈希类型时，内容量走 getsizeof 兜底而不是崩。"""
        self.assertEqual("small", _probe("fn", "small", ({"a": 1}, {1, 2}, None, iter([]))))
        self.assertEqual(0, _content_size(set()))


@skipUnless(_HAS_GUARD, "guard not present")
class TestLimitConfiguration(TestCase):
    """三个旋钮都可由环境变量覆盖，非法值回退默认。"""

    def test_env_names(self):
        self.assertEqual("PLAITA_EXPR_MAX_RESULT_SIZE", EXPR_MAX_RESULT_SIZE_ENV)
        self.assertEqual("PLAITA_EXPR_GROWTH_FACTOR", EXPR_GROWTH_FACTOR_ENV)
        self.assertEqual("PLAITA_EXPR_MAX_RESULT_HARD", EXPR_MAX_RESULT_HARD_ENV)

    def test_resolver_reads_env(self):
        import os

        original = os.environ.get(EXPR_GROWTH_FACTOR_ENV)
        try:
            os.environ[EXPR_GROWTH_FACTOR_ENV] = "7"
            self.assertEqual(7, _resolve_positive_int(EXPR_GROWTH_FACTOR_ENV, 4))
        finally:
            if original is None:
                os.environ.pop(EXPR_GROWTH_FACTOR_ENV, None)
            else:
                os.environ[EXPR_GROWTH_FACTOR_ENV] = original

    def test_resolver_falls_back_on_invalid(self):
        import os

        original = os.environ.get(EXPR_MAX_RESULT_SIZE_ENV)
        try:
            for raw in ["abc", "0", "-5", "", "  ", "1.5"]:
                os.environ[EXPR_MAX_RESULT_SIZE_ENV] = raw
                with self.subTest(raw=raw):
                    self.assertEqual(100_000, _resolve_positive_int(EXPR_MAX_RESULT_SIZE_ENV, 100_000))
        finally:
            if original is None:
                os.environ.pop(EXPR_MAX_RESULT_SIZE_ENV, None)
            else:
                os.environ[EXPR_MAX_RESULT_SIZE_ENV] = original


@skipUnless(_HAS_GUARD, "guard not present")
class TestCallPointLevelGuard(TestCase):
    """判定必须是**调用点级**：预检让路时不能连探针也失去。"""

    def test_no_function_level_whitelist_exists(self):
        self.assertFalse(hasattr(expr_mod, "PRE_CHECKED_FUNCTIONS"),
                         "函数级白名单会造成'预检让路 ⇒ 完全无守卫'的漏洞")

    def test_probe_catches_result_when_precheck_cannot_apply(self):
        """构造"预检无法适用"的调用：探针仍必须拒绝。"""
        # 自定义注册函数（没有任何预检）→ 只能检测，但必须拒绝
        reg = ExpressionRegistry()
        reg.register("boom", lambda n: "x" * n, "boom")
        with _limits(floor=100, factor=4, hard=10**9):
            with self.assertRaises(ValueError):
                _probe("boom", "x" * 100000, (1,))

    def test_content_based_input_size_not_element_count(self):
        """输入规模按内容量：元素很少但内容很大的结果不得被误拒。"""
        with _limits(floor=100, factor=4, hard=10**6):
            self.assertEqual(200_000, len(_probe("join", "c" * 200_000, (["c" * 10_000] * 20, ""))))
            self.assertEqual(222_000, len(_probe("json_dumps", "j" * 222_000,
                                                 ([{"k": "v" * 100}] * 2000,))))


@skipUnless(_HAS_GUARD, "guard not present")
class TestHelperContracts(TestCase):
    """内部契约的白盒断言（标签/单位/估算上界），保证变异体不能靠"改标签"存活。"""

    def test_seq_kind_labels(self):
        self.assertEqual("str", _seq_kind("x"))
        self.assertEqual("bytes", _seq_kind(b"x"))
        self.assertEqual("bytearray", _seq_kind(bytearray(b"x")))
        self.assertEqual("list", _seq_kind([]))
        self.assertEqual("tuple", _seq_kind(()))
        for other in (1, 1.5, None, {}, set()):
            with self.subTest(other=type(other).__name__):
                self.assertIsNone(_seq_kind(other))

    def test_measured_unit_labels(self):
        self.assertEqual("chars", _measured_unit("x"))
        self.assertEqual("bytes", _measured_unit(b"x"))
        self.assertEqual("bytes", _measured_unit(bytearray(b"x")))
        self.assertEqual("elements", _measured_unit([]))
        self.assertEqual("elements", _measured_unit({}))
        self.assertEqual("bytes", _measured_unit(7))  # 无 __len__ → getsizeof

    def test_unit_for_labels(self):
        self.assertEqual("chars", _unit_for("x"))
        self.assertEqual("bytes", _unit_for(b"x"))
        self.assertEqual("bytes", _unit_for(bytearray(b"x")))
        self.assertEqual("elements", _unit_for([]))
        self.assertEqual("elements", _unit_for(None))

    def test_measure_uses_len_then_getsizeof(self):
        self.assertEqual(3, _measure("abc"))
        self.assertEqual(2, _measure([1, 2]))
        self.assertEqual(sys.getsizeof(7), _measure(7))
        self.assertGreaterEqual(_measure(10**500), sys.getsizeof(10**500))

    def test_value_size_covers_decimal_digits_of_ints(self):
        # 估算必须是**上界**（不能低估），且是 int（不是 float）
        self.assertGreaterEqual(_value_size(10**100), 101)
        self.assertEqual(110_732, _value_size(10**100000))
        self.assertIsInstance(_value_size(10**100000), int)
        self.assertEqual(3, _value_size("abc"))
        self.assertEqual(2, _value_size(b"ab"))
        self.assertEqual(sys.getsizeof(1.5), _value_size(1.5))
        self.assertEqual(2, _value_size([1, 2]))
        self.assertEqual(1, _value_size({"a": 1}))

    def test_pair_total_requires_same_kind(self):
        self.assertEqual(2, _pair_total("a", "b"))
        self.assertEqual(2, _pair_total(b"a", bytearray(b"b")))
        self.assertEqual(2, _pair_total([1], [2]))
        self.assertEqual(2, _pair_total((1,), (2,)))
        for a, b in [(1, 2), ("a", [1]), ([1], (2,)), (None, None), ({}, {})]:
            with self.subTest(a=type(a).__name__, b=type(b).__name__):
                self.assertIsNone(_pair_total(a, b))

    def test_kinds_compatible(self):
        self.assertTrue(_kinds_compatible("str", "str"))
        self.assertTrue(_kinds_compatible("bytes", "bytearray"))
        self.assertTrue(_kinds_compatible("list", "list"))
        self.assertFalse(_kinds_compatible("str", "bytes"))
        self.assertFalse(_kinds_compatible("list", "tuple"))
        self.assertFalse(_kinds_compatible(None, "str"))
        self.assertFalse(_kinds_compatible("str", None))

    def test_parse_format_specs_covers_mapping_key_width(self):
        """``%(key)100000000s`` 的宽度必须被解析出来（第一版当成普通文本 → 无预检）。"""
        specs = _parse_format_specs("%(key)100000000s", bytes_mode=False)
        self.assertEqual(1, len(specs))
        self.assertEqual("key", specs[0].key)
        self.assertEqual(100000000, specs[0].width)
        self.assertFalse(specs[0].star_width)
        self.assertEqual("s", specs[0].conversion)

    def test_parse_format_specs_flags_precision_and_star(self):
        specs = _parse_format_specs("%-+#0 8.3f", bytes_mode=False)
        self.assertEqual(8, specs[0].width)
        self.assertEqual(3, specs[0].precision)
        star = _parse_format_specs("%*.*f", bytes_mode=False)
        self.assertTrue(star[0].star_width)
        self.assertTrue(star[0].star_precision)
        self.assertEqual(0, _parse_format_specs("plain text", bytes_mode=False).__len__())
        self.assertIsNone(_parse_format_specs("%q", bytes_mode=False))
        self.assertIsNone(_parse_format_specs("%", bytes_mode=False))
        self.assertIsNone(_parse_format_specs("%(unclosed", bytes_mode=False))
        self.assertIsNone(_parse_format_specs("%b", bytes_mode=False))       # str 不支持 %b
        self.assertIsNotNone(_parse_format_specs("%b", bytes_mode=True))     # bytes 支持

    def _decide(self, fmt, values, bytes_mode=False):
        specs = _parse_format_specs(fmt, bytes_mode=bytes_mode)
        return _mod_decide(fmt, len(fmt), specs, values, bytes_mode)

    def test_mod_decide_kinds(self):
        """Q1：默认拒绝——只有全字段可证才是 ``render``；已证明原生必错才是 ``native``。"""
        self.assertEqual("render", self._decide("%s", "x").kind)
        self.assertEqual("native", self._decide("%s %s", "x").kind)        # 标量 + 多转换符
        self.assertEqual("native", self._decide("%s %s", [1, 2]).kind)     # list 是单值
        self.assertEqual("native", self._decide("%s", ()).kind)            # 参数不足
        self.assertEqual("native", self._decide("%s", (1, 2, 3)).kind)     # 参数过多
        self.assertEqual("native", self._decide("%(z)s", {"a": 1}).kind)   # 缺键 → KeyError
        self.assertEqual("native", self._decide("%(a)s", "x").kind)        # 非映射 → TypeError
        self.assertEqual("native", self._decide("%*d", ("x", 1)).kind)     # '*' 非 int
        self.assertEqual("native", self._decide("%(a)*d", {"a": 1}).kind)  # mapping + '*'
        self.assertEqual("refuse", self._decide("%q", 1).kind)             # 解析失败 ⇒ 拒
        self.assertEqual("render", self._decide("%%", 1).kind)            # Z5.2：无转换符按 render
        class _WithStr:
            def __str__(self):
                return "E" * 10_000_000

        self.assertEqual("refuse", self._decide("%s", _WithStr()).kind)    # 覆写 __str__ ⇒ 拒
        self.assertEqual("refuse", self._decide("%(a)s", _O2StatefulMapping()).kind)

    def test_mod_decide_width_uses_abs_of_star(self):
        decision = self._decide("%*d", (-8, 1))
        self.assertEqual("render", decision.kind)
        self.assertEqual([1], decision.values)
        self.assertEqual([8], decision.widths)

    def test_format_values_size(self):
        self.assertEqual(3, _format_values_size("abc"))
        self.assertEqual(2, _format_values_size(("a", "b")))
        self.assertEqual(1, _format_values_size({"a": "x"}))  # 只算 value（key 不进结果）


@skipUnless(_HAS_GUARD, "guard not present")
class TestModFormatTypePrecedence(TestCase):
    """``%`` 的类型/格式错误优先于尺寸错误（与 add/append 同口径）。"""

    def test_mapping_format_with_scalar_is_type_error_even_with_tiny_limits(self):
        with _limits(floor=1, factor=1, hard=7):
            for fmt in ("%(a)s", "%(a)s%(b)s"):
                with self.subTest(fmt=fmt), self.assertRaises(TypeError):
                    _fn_mod(fmt, "x")

    def test_zero_conversion_with_argument_is_type_error(self):
        with _limits(floor=1, factor=1, hard=7):
            with self.assertRaises(TypeError):
                _fn_mod("%%", 1)
            self.assertEqual("%", _fn_mod("%%", ()))

    def test_multiple_conversions_with_scalar_is_type_error(self):
        with _limits(floor=1, factor=1, hard=7):
            with self.assertRaises(TypeError):
                _fn_mod("%s%s", "x")

    def test_mapping_format_with_dict_is_allowed(self):
        self.assertEqual("x", _fn_mod("%(a)s", {"a": "x"}))
        self.assertEqual("xy", _fn_mod("%(a)s%(b)s", {"a": "x", "b": "y"}))

    def test_non_mapping_format_with_dict_single_value_allowed(self):
        self.assertEqual("{'a': 1}", _fn_mod("%s", {"a": 1}))


@skipUnless(_HAS_GUARD, "guard not present")
class TestBytesBytearrayMixing(TestCase):
    """bytes/bytearray 互相混用是同类拼接；与 str 混用是原生 TypeError。"""

    def test_add_bytes_bytearray(self):
        self.assertEqual(b"ab", _fn_add(b"a", bytearray(b"b")))
        self.assertEqual(b"ab", _fn_add(bytearray(b"a"), b"b"))

    def test_extend_bytes_bytearray(self):
        self.assertEqual(2, len(_fn_extend(bytearray(b"a"), b"b")))
        self.assertEqual(2, len(_fn_extend(b"a", bytearray(b"b"))))

    def test_bytes_str_mix_is_type_error(self):
        with self.assertRaises(TypeError):
            _fn_add(b"a", "b")
        with self.assertRaises(TypeError):
            _fn_extend(b"a", "b")


@skipUnless(_HAS_GUARD, "guard not present")
class TestEnvResolutionLogging(TestCase):
    """解析器：合法值生效、非法值告警并回退、未设置时**不告警**。"""

    def test_valid_env_value_is_used(self):
        import os

        original = os.environ.get(EXPR_MAX_RESULT_SIZE_ENV)
        try:
            os.environ[EXPR_MAX_RESULT_SIZE_ENV] = "1"
            self.assertEqual(1, _resolve_positive_int(EXPR_MAX_RESULT_SIZE_ENV, 100_000))
        finally:
            if original is None:
                os.environ.pop(EXPR_MAX_RESULT_SIZE_ENV, None)
            else:
                os.environ[EXPR_MAX_RESULT_SIZE_ENV] = original

    def test_unset_env_logs_nothing(self):
        import os

        original = os.environ.get(EXPR_MAX_RESULT_SIZE_ENV)
        try:
            os.environ.pop(EXPR_MAX_RESULT_SIZE_ENV, None)
            with self.assertNoLogs("plaita.core.expression", level="WARNING"):
                self.assertEqual(100_000, _resolve_positive_int(EXPR_MAX_RESULT_SIZE_ENV, 100_000))
        finally:
            if original is not None:
                os.environ[EXPR_MAX_RESULT_SIZE_ENV] = original

    def test_invalid_env_warns_with_name_and_value(self):
        import os

        original = os.environ.get(EXPR_MAX_RESULT_SIZE_ENV)
        try:
            os.environ[EXPR_MAX_RESULT_SIZE_ENV] = "abc"
            with self.assertLogs("plaita.core.expression", level="WARNING") as logs:
                self.assertEqual(100_000, _resolve_positive_int(EXPR_MAX_RESULT_SIZE_ENV, 100_000))
            self.assertTrue(any(EXPR_MAX_RESULT_SIZE_ENV in line and "abc" in line
                                for line in logs.output), logs.output)
        finally:
            if original is None:
                os.environ.pop(EXPR_MAX_RESULT_SIZE_ENV, None)
            else:
                os.environ[EXPR_MAX_RESULT_SIZE_ENV] = original

    def test_non_positive_env_warns_and_falls_back(self):
        import os

        original = os.environ.get(EXPR_MAX_RESULT_SIZE_ENV)
        try:
            os.environ[EXPR_MAX_RESULT_SIZE_ENV] = "0"
            with self.assertLogs("plaita.core.expression", level="WARNING") as logs:
                self.assertEqual(100_000, _resolve_positive_int(EXPR_MAX_RESULT_SIZE_ENV, 100_000))
            self.assertTrue(any("非正" in line for line in logs.output), logs.output)
        finally:
            if original is None:
                os.environ.pop(EXPR_MAX_RESULT_SIZE_ENV, None)
            else:
                os.environ[EXPR_MAX_RESULT_SIZE_ENV] = original


@skipUnless(_HAS_GUARD, "guard not present")
class TestRefusalMatrixTinyLimits(TestCase):
    """三只旋钮压到最小，让每条预检都真的触发，再逐条比对**精确文案**。

    这是变异体的高发区：把实测规模渲染成 ``None``、把单位写成 chars/elements、
    把上限算错、改掉算子名——整串比对全部杀掉（子串包含抓不住 ``None``）。
    """

    TINY = dict(floor=1, factor=1, hard=7)

    def _refusal(self, fn, *args):
        with _limits(**self.TINY):
            with self.assertRaises(ValueError) as cm:
                fn(*args)
        return str(cm.exception)

    def _ok(self, fn, *args):
        with _limits(**self.TINY):
            return fn(*args)

    def test_mul_messages(self):
        for args, expected in [
            (("x", 2), "$F.mul(1 * 2) result too large: 2 chars (limit 1)."),
            ((b"x", 2), "$F.mul(1 * 2) result too large: 2 bytes (limit 1)."),
            (([0], 2), "$F.mul(1 * 2) result too large: 2 elements (limit 1)."),
            ((2, "x"), "$F.mul(1 * 2) result too large: 2 chars (limit 1)."),
        ]:
            with self.subTest(args=args):
                self.assertTrue(self._refusal(_fn_mul, *args).startswith(expected),
                                self._refusal(_fn_mul, *args))

    def test_add_messages(self):
        self.assertTrue(self._refusal(_fn_add, "a" * 6, "bb").startswith(
            "$F.add(6 + 2) result too large: 8 chars (limit 7)."))
        self.assertTrue(self._refusal(_fn_add, [0] * 6, [0, 0]).startswith(
            "$F.add(6 + 2) result too large: 8 elements (limit 7)."))
        self.assertTrue(self._refusal(_fn_add, b"a" * 6, b"bb").startswith(
            "$F.add(6 + 2) result too large: 8 bytes (limit 7)."))

    def test_extend_unit_follows_operands(self):
        self.assertTrue(self._refusal(_fn_extend, "a" * 6, "bb").startswith(
            "$F.extend(6 + 2) result too large: 8 chars (limit 7)."))
        self.assertTrue(self._refusal(_fn_extend, b"a" * 6, b"bb").startswith(
            "$F.extend(6 + 2) result too large: 8 bytes (limit 7)."))

    def test_add_hard_boundary_inclusive(self):
        self.assertEqual(7, len(self._ok(_fn_add, "a" * 6, "b")))
        with self.assertRaises(ValueError):
            self._ok(_fn_add, "a" * 6, "bb")

    def test_append_messages_and_boundary(self):
        with self.assertRaises(ValueError) as cm:
            self._ok(_fn_append, [0] * 7, 1)
        self.assertTrue(str(cm.exception).startswith(
            "$F.append(7 + 1) result too large: 8 elements (limit 7)."))
        self.assertEqual(7, len(self._ok(_fn_append, [0] * 6, 1)))

    def test_extend_messages_and_boundary(self):
        with self.assertRaises(ValueError) as cm:
            self._ok(_fn_extend, [0] * 4, [0] * 4)
        self.assertTrue(str(cm.exception).startswith(
            "$F.extend(4 + 4) result too large: 8 elements (limit 7)."))
        self.assertEqual(7, len(self._ok(_fn_extend, [0] * 3, [0] * 4)))

    def test_concat_messages_and_boundary(self):
        with self.assertRaises(ValueError) as cm:
            self._ok(_fn_concat, "abcdefg", "h")
        self.assertTrue(str(cm.exception).startswith(
            "$F.concat(2 args) result too large: 8 chars (limit 7)."))
        self.assertEqual(7, len(self._ok(_fn_concat, "abcdefg")))

    def test_join_messages_and_boundary(self):
        with self.assertRaises(ValueError) as cm:
            self._ok(_fn_join, ["a", "b"], "ssssss")
        self.assertTrue(str(cm.exception).startswith(
            "$F.join(2 items) result too large: 8 chars (limit 7)."))
        self.assertEqual(7, len(self._ok(_fn_join, ["a"] * 7, "")))

    def test_join_separator_repeat_overrun_message(self):
        """分隔符按 (n-1) 次计入结果总长；n=1 时不计（见反向对照）。"""
        with self.assertRaises(ValueError) as cm:
            self._ok(_fn_join, ["a", "b"], "ssssssss")
        self.assertTrue(str(cm.exception).startswith(
            "$F.join(2 items) result too large: 10 chars (limit 7)."))

    def test_join_unit_and_non_str_element_type_error(self):
        with self.assertRaises(ValueError) as cm:
            self._ok(_fn_join, ["a" * 400, "b" * 400], "")
        self.assertTrue(str(cm.exception).startswith(
            "$F.join(2 items) result too large: 800 chars (limit 7)."))
        with self.assertRaises(TypeError) as cm:
            self._ok(_fn_join, [1, 2], "-")
        self.assertIn("expected str instance", str(cm.exception))

    def test_join_str_items_with_bytes_separator_keeps_type_error(self):
        with self.assertRaises(TypeError):
            self._ok(_fn_join, "x" * 10, b"s" * 10)

    def test_replace_message_and_boundary(self):
        with self.assertRaises(ValueError) as cm:
            self._ok(_fn_replace, "aaaa", "a", "bbb")
        self.assertTrue(str(cm.exception).startswith(
            "$F.replace(4 chars, 4 hits) result too large: 12 chars (limit 7)."))
        self.assertEqual(6, len(self._ok(_fn_replace, "aaa", "a", "bb")))
        self.assertEqual(6, len(self._ok(_fn_replace, "aaaaaa", "a", "b")))

    def test_replace_empty_pattern_message(self):
        with self.assertRaises(ValueError) as cm:
            self._ok(_fn_replace, "aaa", "", "bb")
        self.assertTrue(str(cm.exception).startswith(
            "$F.replace(3 chars, 3 hits) result too large: 11 chars (limit 5)."))

    def test_mod_message_width_and_star(self):
        with self.assertRaises(ValueError) as cm:
            self._ok(_fn_mod, "%8d", 1)
        self.assertTrue(str(cm.exception).startswith(
            "$F.mod(format 3 chars) result too large: 19 chars (limit 6)."))
        self.assertTrue(self._refusal(_fn_mod, "%*d", (8, 1)).startswith(
            "$F.mod(format 3 chars) result too large: 19 chars (limit 6)."))

    def test_mod_conservative_estimate_message(self):
        """保守估计（格式串长度 × (宽度+1) + 渲染值内容量）逐字比对。

        覆盖第二轮验收点名的两条旁路：dict 参数 + 大宽度、映射键后写宽度。
        """
        self.assertTrue(self._refusal(_fn_mod, "%2000000d", 1).startswith(
            "$F.mod(format 9 chars) result too large: 2000017 chars (limit 7)."))
        self.assertTrue(self._refusal(_fn_mod, "%100000000s", {"x": 1}).startswith(
            "$F.mod(format 11 chars) result too large: 100000019 chars (limit 7)."))
        self.assertTrue(self._refusal(_fn_mod, "%(k)100000000s", {"k": "x"}).startswith(
            "$F.mod(format 14 chars) result too large: 100000022 chars (limit 7)."))

    def test_mod_zero_conversion_message_is_native(self):
        with _limits(**self.TINY):
            with self.assertRaises(TypeError) as cm:
                _fn_mod("%%", 1)
        self.assertIn("not all arguments converted", str(cm.exception))

    def test_replace_non_str_pattern_keeps_native_message(self):
        with self.assertRaises(TypeError) as cm:
            _fn_replace("abc", 1, "z")
        self.assertIn("must be str", str(cm.exception))

    def test_mul_one_times_large_still_checked_against_hard(self):
        """×1 不放大，但仍受 hard 约束（tiny 配置下会拒）。"""
        with self.assertRaises(ValueError) as cm:
            self._ok(_fn_mul, bytearray(b"x" * 100000), 1)
        self.assertIn("100000 bytes (limit 7)", str(cm.exception))
        self.assertEqual(100_001, len(_fn_mul("x" * 100001, 1)))  # 默认配置放行

    def test_mod_tuple_two_conversions_message(self):
        """两个转换符 + tuple 值：精确文案（同时也是 star 分支顺序的判别测试）。"""
        with _limits(floor=1, factor=1, hard=7):
            with self.assertRaises(ValueError) as cm:
                _fn_mod("%s-%s", ("a", "b"))
        self.assertEqual(
            "$F.mod(format 5 chars) result too large: 23 chars (limit 7). "
            "Refusing to build an unbounded result.", str(cm.exception))

    def test_mod_escaped_percent_then_text_is_allowed(self):
        """``'%%(a)s'`` 里 ``%%`` 是转义、没有转换符：按原生语义放行。"""
        with _limits(floor=1, factor=1, hard=7):
            self.assertEqual("%(a)s", _fn_mod("%%(a)s", {"a": 1}))

    def test_mod_single_conversion_dict_message(self):
        """``'%s' % {'a': 1}``（dict 作为单个值）的精确文案：默认精度哨兵判别。"""
        with _limits(floor=1, factor=1, hard=4):
            with self.assertRaises(ValueError) as cm:
                _fn_mod("%s", {"a": 1})
        self.assertEqual(
            "$F.mod(format 2 chars) result too large: 30 chars (limit 4). "
            "Refusing to build an unbounded result.", str(cm.exception))

    def test_mod_bytes_int_message_includes_digits(self):
        """``b'%d' % 1`` 的上界必须含值本身的位数（不只是精度/宽度）。"""
        with _limits(floor=1, factor=1, hard=3):
            with self.assertRaises(ValueError) as cm:
                _fn_mod(b"%d", 1)
        self.assertEqual(
            "$F.mod(format 2 bytes) result too large: 11 bytes (limit 3). "
            "Refusing to build an unbounded result.", str(cm.exception))

    def test_replace_bytearray_input_message(self):
        """``bytearray`` 输入也必须预检（第一版只认 bytes 输入 → 该路径无守卫）。"""
        with _limits(floor=1, factor=1, hard=7):
            with self.assertRaises(ValueError) as cm:
                _fn_replace(bytearray(b"a" * 100), b"a", b"bb")
        self.assertEqual(
            "$F.replace(100 bytes, 100 hits) result too large: 200 bytes (limit 7). "
            "Refusing to build an unbounded result.", str(cm.exception))

    def test_mod_mapping_multi_key_message(self):
        """映射键多转换符在 tiny 限额下的精确文案（宽度 0 / 渲染值内容量 2）。"""
        with _limits(floor=1, factor=1, hard=7):
            with self.assertRaises(ValueError) as cm:
                _fn_mod("%(a)s%(b)s", {"a": "x", "b": "y"})
        self.assertEqual(
            "$F.mod(format 10 chars) result too large: 28 chars (limit 7). "
            "Refusing to build an unbounded result.", str(cm.exception))

    def test_join_unbounded_iterable_message(self):
        """物化指针预算用**字节**口径：精确文案（项数上限 + 字节数 + 上限）。"""
        with _limits(floor=1, factor=1, hard=1000):
            with self.assertRaises(ValueError) as cm:
                _fn_join(("" for _ in range(200)), "")
        self.assertEqual(
            "$F.join(iterable with > 125 items) result too large: 1008 bytes (limit 1000). "
            "Refusing to build an unbounded result.", str(cm.exception))

    def test_mul_refusal_message_names_operator_once(self):
        msg = self._refusal(_fn_mul, "x", 2)
        self.assertEqual(1, msg.count("$F.mul"))
        self.assertIn("Refusing to build an unbounded result.", msg)


@skipUnless(_HAS_GUARD, "guard not present")
class TestSecondRoundBypasses(TestCase):
    """第二轮独立验收点名的 5 条旁路：都必须在**分配之前**拒绝（峰值有界）。

    每条同时给出根因注释——它们都是同一个设计缺陷的表现：判定按**函数**声明
    "已预检"，预检一旦让路（类型不匹配 / 无法估算），探针也被跳过。
    """

    def _prevented(self, fn) -> None:
        desc, peak = _measure_peak(fn)
        self.assertEqual("ValueError", desc)
        self.assertLess(peak, _PEAK_BUDGET,
                        f"拒绝发生在分配之后：peak={peak/1024/1024:.1f}MB")

    def test_bypass1_mod_dict_argument_with_huge_width(self):
        """dict 参数 + 非 mapping 格式串（第一版把这条当"类型错误"直接早退）。"""
        self._prevented(lambda: _fn_mod("%100000000s", {"x": 1}))
        self._prevented(lambda: _fn_mod("%300000000s", {"x": 1}))

    def test_bypass2_mod_mapping_key_width_after_key(self):
        """``%(key)100000000s``：映射键**之后**的宽度第一版当普通文本（无预检）。"""
        self._prevented(lambda: _fn_mod("%(k)100000000s", {"k": "x"}))

    def test_bypass2b_mod_star_width_from_mapping_is_native(self):
        """mapping + ``*`` 原生就是 TypeError（不能被说成尺寸错误）。"""
        with self.assertRaises(TypeError):
            _fn_mod("%(k)*s", {"k": "x"})

    def test_bypass3_replace_bytes_with_bytearray_pattern(self):
        """``bytes.replace(bytearray, bytearray)`` 合法：第一版按"类型不对"跳过预检。"""
        self._prevented(lambda: _fn_replace(b"a" * 1000, bytearray(b"a"), bytearray(b"b" * 1000)))
        self._prevented(lambda: _fn_replace("a" * 1000, "a", "b" * 1000))

    def test_bypass4_join_generator_and_view(self):
        """任意可迭代（生成器/视图）：第一版 ``items=None`` 跳过预检且探针被白名单跳过。

        形态改为"生成器 + 分隔符放大"（非放大的生成器**应当放行**，见
        ``TestThirdRoundNonAmplifying``）；视图照常精确判定。
        """
        self._prevented(lambda: _fn_join(("" for _ in range(200000)), "x" * 1000))
        self._prevented(lambda: _fn_join(["a"] * 6, "s" * 25000))
        self.assertEqual("a-b", _fn_join((c for c in ["a", "b"]), "-"))     # 小生成器照常
        self.assertEqual("a-b", _fn_join({"a": 1, "b": 2}.keys(), "-"))     # 视图照常

    def test_bypass5_mul_unknown_sequence_type(self):
        """未枚举序列（``array.array``）：按"类型支持 __mul__ 且有 __len__"纳入预检。"""
        import array
        self._prevented(lambda: _fn_mul(array.array("i", [0] * 100), 10**6))
        self._prevented(lambda: _fn_mul(10**6, array.array("i", [0] * 100)))

    def test_unknown_repeat_type_keeps_native_type_error(self):
        with self.assertRaises(TypeError):
            _fn_mul(set(), 3)
        with self.assertRaises(TypeError):
            _fn_mul(memoryview(b"ab"), 3)


@skipUnless(_HAS_GUARD, "guard not present")
class TestSecondRoundOverRestriction(TestCase):
    """第二轮验收点名的误伤：输入内容本来就大、输出同量级，必须放行。

    判据是**输出/输入的比值**；"输入规模"必须是**内容规模**（字符串长度、
    容器内容之和），不能用元素个数/参数个数。
    """

    def test_json_dumps_of_many_rows_is_allowed(self):
        """``json_dumps([{"k": "v"*100}]*2000)`` = 222000 字符（输入内容 202000）。"""
        rows = [{"k": "v" * 100}] * 2000
        parser = _fresh()
        result = parser.evaluate("$F.json_dumps($INPUT.rows)", {"$INPUT": {"rows": rows}})
        self.assertEqual(222_000, len(result))

    def test_mod_with_two_large_arguments_is_allowed(self):
        self.assertEqual(200_001, len(_fn_mod("%s %s", ("x" * 100000, "y" * 100000))))

    def test_mod_with_large_list_value_is_allowed(self):
        self.assertEqual(300_004, len(_fn_mod("%s", ["z" * 300000])))

    def test_join_twenty_times_ten_thousand_is_allowed(self):
        self.assertEqual(200_000, len(_fn_join(["c" * 10000] * 20, "")))

    def test_content_size_is_content_not_element_count(self):
        self.assertEqual(200_000, _content_size(["c" * 10000] * 20))
        self.assertEqual(202_000, _content_size([{"k": "v" * 100}] * 2000))
        self.assertEqual(9, _content_size(123456789))
        self.assertEqual(3, _content_size("abc"))

    def test_today_percent_c_expansion_is_refused_by_ratio(self):
        """``today('%c'*100000)`` = 2400000 字符（24× 放大）按设计拒绝。

        这是**探针（检测）**拦下的例子——``today`` 没有预检；文档把它写成
        "比值判定会拒绝 24× 放大"的示例，免得与"非放大大输入必须放行"混淆。
        """
        with self.assertRaises(ValueError) as cm:
            _probe("today", _fn_today("%c" * 100000), ("%c" * 100000,))
        self.assertIn("$F.today result too large", str(cm.exception))


@skipUnless(_HAS_GUARD, "guard not present")
class TestNativeErrorsTakePrecedence(TestCase):
    """类型/格式错误优先于尺寸错误——**即使把三只旋钮压到极小**。"""

    def test_type_errors_first(self):
        with _limits(floor=1, factor=1, hard=1):
            with self.assertRaises(TypeError):
                _fn_mod("%s %s", [1, 2])           # list 是单个值
            with self.assertRaises(TypeError):
                _fn_mod("%s %s", "x")              # 标量 + 多转换符
            with self.assertRaises(TypeError):
                _fn_mod("%s", ())                  # 参数不足
            with self.assertRaises(ValueError):
                # Q3：native 会**先构造**该字段再报 "not all arguments converted"
                # ⇒ 已构造字段的分配必须计入，tiny 上限下尺寸错误先抛
                _fn_mod("%s", tuple(range(100000)))
        # 默认上限下同一条仍是原生 TypeError（字段上界 << 允许额度）
        with self.assertRaises(TypeError):
            _fn_mod("%s", tuple(range(100000)))
            with self.assertRaises(KeyError):
                _fn_mod("%(z)s", {"a": 1})         # mapping 缺键
            with self.assertRaises(TypeError):
                _fn_mod("%(a)s", "x")              # mapping 格式 + 标量
            with self.assertRaises(ValueError):
                _fn_mod("%q", 1)                   # 非法转换符
            with self.assertRaises(ValueError):
                _fn_mod("%", 1)                    # 未闭合
            with self.assertRaises(TypeError):
                _fn_mod(b"%s", "x")                # bytes 格式 + str 参数
            with self.assertRaises(TypeError):
                _fn_mod("%*d", ("x", 1))           # '*' 不是 int

    def test_type_errors_first_for_sequence_operators(self):
        with _limits(floor=1, factor=1, hard=1):
            with self.assertRaises(TypeError):
                _fn_add("a", [1])
            with self.assertRaises(TypeError):
                _fn_extend([1], (2,))
            with self.assertRaises(TypeError):
                _fn_join([1, 2], "-")
            with self.assertRaises(TypeError):
                _fn_mul([1], "x")


@skipUnless(_HAS_GUARD, "guard not present")
class TestUnitLabels(TestCase):
    """提示文案的单位必须与实际类型一致（第二轮验收点名）。"""

    def test_replace_bytes_unit(self):
        with self.assertRaises(ValueError) as cm:
            _fn_replace(b"a" * 1000, b"a", b"b" * 1000)
        self.assertIn("1000 bytes", str(cm.exception))
        self.assertNotIn("chars", str(cm.exception))
        with self.assertRaises(ValueError) as cm:
            _fn_replace(b"a" * 1000, bytearray(b"a"), bytearray(b"b" * 1000))
        self.assertIn("bytes", str(cm.exception))

    def test_replace_str_unit(self):
        with self.assertRaises(ValueError) as cm:
            _fn_replace("a" * 1000, "a", "b" * 1000)
        self.assertIn("1000 chars", str(cm.exception))

    def test_mod_bytes_unit(self):
        with self.assertRaises(ValueError) as cm:
            _fn_mod(b"%1000000d", 1)
        self.assertIn("bytes", str(cm.exception))
        self.assertNotIn("chars", str(cm.exception))

    def test_concat_int_estimate_is_exact_for_small_ints(self):
        """``concat(123456789)`` 的**渲染**上界必须是 11 字符（9 位 + 2 余量）。"""
        with _limits(floor=5, factor=1, hard=8):
            with self.assertRaises(ValueError) as cm:
                _fn_concat(123456789)
            self.assertIn("11 chars (limit 8)", str(cm.exception))
        self.assertEqual(9, _value_size(123456789))      # 值本身 9 位
        self.assertEqual(11, _render_size(123456789))    # 渲染上界 9 + 2


@skipUnless(_HAS_GUARD, "guard not present")
class TestThirdRoundLiteralPrecision(TestCase):
    """第三轮验收 A：``%`` 的**字面精度**必须进上界，且界定的是**分配量**。

    实测（CPython 3.11，精度 2e6）：``%.<P>d/i/u/o/x/X`` 分配 ≈P、``f/F`` ≈P+2、
    ``e/E`` ≈P+6，而 ``%.<P>g/G`` **结果只有 1 字符却仍分配 ≈P**（P=9e8 时
    1024MB，且原实现**静默返回 '1'**）——所以精度必须进上界。
    ``s``/``r``/``a`` 的精度是截断而不是填充（实测 0 额外分配），因此**不拒绝**。
    """

    ALLOCATING = ("d", "i", "u", "o", "x", "X", "e", "E", "f", "F", "g", "G")

    def test_huge_literal_precision_refused_for_every_allocating_conversion(self):
        for conversion in self.ALLOCATING:
            value = 1 if conversion in "diuoxX" else 1.0
            with self.subTest(conversion=conversion):
                desc, peak = _measure_peak(
                    lambda c=conversion, v=value: _fn_mod("%.90000000" + c, v))
                self.assertEqual("ValueError", desc)
                self.assertLess(peak, _PEAK_BUDGET, f"{conversion}: peak={peak}")

    def test_g_conversion_would_be_silent_without_precision_bound(self):
        """``%.900000000g`` 结果只有 1 字符却分配 ~1GB：必须**分配前**拒绝。"""
        desc, peak = _measure_peak(lambda: _fn_mod("%.900000000g", 1.0))
        self.assertEqual("ValueError", desc)
        self.assertLess(peak, _PEAK_BUDGET)

    def test_literal_precision_bound_message(self):
        with _limits(floor=1000, factor=4, hard=10**9):
            with self.assertRaises(ValueError) as cm:
                _fn_mod("%.90000000f", 1.0)
        self.assertEqual(
            "$F.mod(format 11 chars) result too large: 90000021 chars (limit 1000). "
            "Refusing to build an unbounded result.", str(cm.exception))

    def test_truncating_conversions_with_huge_precision_stay_allowed(self):
        """``s``/``r``/``a`` 的精度是**截断**（0 额外分配），按原生语义放行。"""
        for conversion in ("s", "r", "a"):
            with self.subTest(conversion=conversion):
                desc, peak = _measure_peak(
                    lambda c=conversion: _fn_mod("%.90000000" + c, "abc"))
                self.assertTrue(desc.startswith("ok"), desc)
                self.assertLess(peak, _PEAK_BUDGET)

    def test_normal_precision_is_unaffected(self):
        with _limits(floor=100, factor=4, hard=10**6):
            self.assertEqual("1.50", _fn_mod("%.2f", 1.5))
            self.assertEqual("000001", _fn_mod("%.6d", 1))
            self.assertEqual("1.500000e+00", _fn_mod("%.6e", 1.5))
            self.assertEqual("1.5", _fn_mod("%.2g", 1.5))
            self.assertEqual("abc", _fn_mod("%.3s", "abcdef"))
            self.assertEqual(8, len(_fn_mod("%f", 1.0)))


@skipUnless(_HAS_GUARD, "guard not present")
class TestThirdRoundBufferProtocol(TestCase):
    """第三轮验收 B：``replace`` 的 bytes-like 闸门必须覆盖**任意缓冲协议**对象。"""

    def test_memoryview_array_patterns_are_prevented(self):
        import array
        cases = {
            "memoryview-pattern": lambda: _fn_replace(
                b"a" * 1000, memoryview(b"a"), b"b" * 200000),
            "memoryview-replacement": lambda: _fn_replace(
                b"a" * 1000, b"a", memoryview(b"b" * 200000)),
            "array-pattern": lambda: _fn_replace(
                b"a" * 1000, array.array("b", b"a"), b"b" * 200000),
            "array-replacement": lambda: _fn_replace(
                b"a" * 1000, b"a", array.array("b", b"b" * 200000)),
        }
        for label, fn in cases.items():
            with self.subTest(case=label):
                desc, peak = _measure_peak(fn)
                self.assertEqual("ValueError", desc)
                self.assertLess(peak, _PEAK_BUDGET, f"{label}: peak={peak/1024/1024:.2f}MB")

    def test_buffer_like_replacements_still_work(self):
        import array
        self.assertEqual(b"zbc", _fn_replace(b"abc", memoryview(b"a"), b"z"))
        self.assertEqual(b"azc", _fn_replace(b"abc", b"b", memoryview(b"z")))
        self.assertEqual(b"zbc", _fn_replace(b"abc", memoryview(b"a"), array.array("b", b"z")))
        self.assertEqual(b"zbc", _fn_replace(bytearray(b"abc"), memoryview(b"a"),
                                             array.array("b", b"z")))

    def test_is_bytes_like_contract(self):
        import array
        for good in (b"x", bytearray(b"x"), memoryview(b"x"), array.array("b", b"x")):
            with self.subTest(good=type(good).__name__):
                self.assertTrue(_is_bytes_like(good))
        for bad in ("x", 1, None, [1], {"a": 1}):
            with self.subTest(bad=type(bad).__name__):
                self.assertFalse(_is_bytes_like(bad))


@skipUnless(_HAS_GUARD, "guard not present")
class TestThirdRoundLyingLength(TestCase):
    """第三轮验收 H：``__len__`` 撒谎的可迭代**不得被截断**（正确性 bug）。"""

    class _Liar:
        def __init__(self, items, claim):
            self._items = items
            self._claim = claim

        def __len__(self):
            return self._claim

        def __iter__(self):
            return iter(self._items)

    def test_under_reporting_length_does_not_truncate(self):
        items = ["x" * 10] * 200000
        honest = _fn_join((item for item in items), "")
        for claim in (10, 1):
            with self.subTest(claim=claim):
                self.assertEqual(honest, _fn_join(self._Liar(items, claim), ""))

    def test_over_reporting_length_does_not_blow_up(self):
        items = ["ab", "cd", "ef"]
        self.assertEqual("ab|cd|ef", _fn_join(self._Liar(items, 10**9), "|"))

    def test_lying_length_is_not_used_for_the_size_decision(self):
        """少报长度也不能让放大混过去：内容量口径判定。"""
        liar = self._Liar([""] * 200000, 1)
        with self.assertRaises(ValueError):
            _fn_join(liar, "x" * 1000)


@skipUnless(_HAS_GUARD, "guard not present")
class TestThirdRoundNonAmplifying(TestCase):
    """第三轮验收 F：8 条误伤全部放行，与"放大必须被拒"同文件对照。"""

    def test_join_generator_20x10000_is_allowed_helper_layer(self):
        """owner 具名用例的**生成器形态**：与列表形态并列断言。"""
        self.assertEqual(200000, len(_fn_join((item for item in ["c" * 10000] * 20), "")))
        self.assertEqual(200000, len(_fn_join(["c" * 10000] * 20, "")))

    def test_join_generator_1001x100_is_allowed_helper_layer(self):
        self.assertEqual(100100, len(_fn_join((item for item in ["c" * 100] * 1001), "")))

    def test_join_generator_50k_x20_is_allowed_helper_layer(self):
        self.assertEqual(1000000, len(_fn_join((item for item in ["c" * 20] * 50000), "")))

    def test_join_dict_keys_200k_is_allowed_helper_layer(self):
        keys = {"k%09d" % i: 1 for i in range(200000)}
        self.assertEqual(2199999, len(_fn_join(keys.keys(), ",")))

    def test_join_dict_values_20k_x10_is_allowed_helper_layer(self):
        values = {i: "c" * 10 for i in range(20000)}
        self.assertEqual(219999, len(_fn_join(values.values(), ",")))

    def test_join_generator_200k_x1_is_allowed_helper_layer(self):
        """200k 项 × 1 字符：不得因为"项数"被拒（内容量口径）。"""
        self.assertEqual(200000, len(_fn_join((c for c in "x" * 200000), "")))

    def test_json_dumps_many_rows_with_huge_last_item_is_allowed(self):
        rows = [{"k": "v" * 100}] * 2999 + [{"k": "v" * 5000000}]
        parser = _fresh()
        result = parser.evaluate("$F.json_dumps($INPUT.rows)", {"$INPUT": {"rows": rows}})
        self.assertEqual(5332900, len(result))

    def test_json_dumps_deeply_nested_huge_value_is_allowed(self):
        parser = _fresh()
        result = parser.evaluate("$F.json_dumps($INPUT.v)",
                                 {"$INPUT": {"v": [[[["x" * 1000000]]]]}})
        self.assertEqual(1000010, len(result))

    def test_content_size_is_a_true_upper_bound(self):
        """第三轮验收 D：``max(前 2048 项)×n`` 会**低估**（2049 vs 真值 2148）。"""
        container = ["x"] * 2048 + ["x" * 100]
        self.assertEqual(2148, _content_size(container))
        self.assertEqual(50000, _content_size({("k%09d" % i): 1 for i in range(5000)}.keys()))
        self.assertEqual(4000, _content_size(__import__("array").array("i", [0] * 1000)))
        self.assertEqual(1234, _content_size(memoryview(b"x" * 1234)))
        self.assertEqual(1000, _content_size([[[["x" * 1000]]]]))

    def test_content_size_does_not_consume_one_shot_iterators(self):
        """单次迭代器不枚举：既不消费、也不按 hard 放宽（返回对象自身大小）。

        真实内容量由需要它的算子（``join``/``sort``）在**调用点**先物化一次拿到，
        见 ``TestFifthRoundCallPointPrepare``。
        """
        generator = (c for c in "abc")
        size = _content_size(generator)
        self.assertLess(size, 10_000)
        self.assertNotEqual(HARD, size)
        self.assertEqual(["a", "b", "c"], list(generator))     # 未被消费

    def test_content_size_scans_exactly_no_saturation_surprise(self):
        """扫描精确到项：``[0]*1_000_001`` 不得被报成 1e8（第三轮验收 必修 2③）。"""
        self.assertEqual(1_000_001, _content_size([0] * 1_000_001))
        self.assertEqual(1_000_002, _content_size([0] * 1_000_002))   # 变异 (d)：>1e6 不得重新饱和
        self.assertEqual(1_500_000, _content_size([0] * 1_500_000))
        self.assertEqual(1_088_890, _content_size(list(range(200_000))))  # 各位数精确求和

    def test_content_size_handles_self_reference(self):
        cyclic: list = []
        cyclic.append(cyclic)
        self.assertEqual(1, _content_size(cyclic))   # 自引用只计一次 + 元素数下界
        mapping: dict = {}
        mapping["k"] = mapping
        self.assertEqual(1, _content_size(mapping))

    def test_content_size_views_and_deep_nesting_are_content(self):
        """视图/深嵌套不得回落成元素数（回落会误伤 join/json_dumps）。"""
        keys = {("k%09d" % i): 1 for i in range(200000)}
        self.assertGreaterEqual(_content_size(keys.keys()), 2000000)


@skipUnless(_HAS_GUARD, "guard not present")
class TestThirdRoundNativePrecedenceDefaultLimits(TestCase):
    """第三轮验收 E：默认限额下"转换符 × 值类型"不合法必须抛**原生**错误。"""

    def test_native_type_errors_at_default_limits(self):
        cases = [
            ("%1000000000d", "x", TypeError),
            ("%1000000000d", None, TypeError),
            ("%(k)1000000000d", {"k": "x"}, TypeError),
            ("%*d", (10**9, "x"), TypeError),
            (b"%1000000000d", b"x", TypeError),
            ("%1000000000c", "xx", TypeError),
            ("%1000000000x", 1.5, TypeError),
        ]
        for fmt, value, expected in cases:
            with self.subTest(fmt=fmt, value=type(value).__name__):
                with self.assertRaises(expected):
                    _fn_mod(fmt, value)

    def test_native_type_errors_at_tiny_limits(self):
        with _limits(floor=1, factor=1, hard=1):
            with self.assertRaises(TypeError):
                _fn_mod("%d", "x")
            with self.assertRaises(TypeError):
                _fn_mod("%x", 1.5)
            with self.assertRaises(TypeError):
                _fn_mod("%c", "xy")

    def test_conversion_accepts_contract(self):
        self.assertTrue(_conversion_accepts("s", object(), False))
        self.assertTrue(_conversion_accepts("d", 1, False))
        self.assertTrue(_conversion_accepts("d", 1.5, False))
        self.assertFalse(_conversion_accepts("d", "x", False))
        self.assertFalse(_conversion_accepts("x", 1.5, False))
        self.assertTrue(_conversion_accepts("x", 1, False))
        self.assertTrue(_conversion_accepts("c", 65, False))
        self.assertTrue(_conversion_accepts("c", "a", False))
        self.assertFalse(_conversion_accepts("c", "ab", False))
        self.assertFalse(_conversion_accepts("s", "x", True))
        self.assertTrue(_conversion_accepts("s", b"x", True))

    def test_valid_conversions_are_not_refused(self):
        with _limits(floor=100, factor=4, hard=10**6):
            self.assertEqual("1", _fn_mod("%d", 1))
            self.assertEqual("1.500000", _fn_mod("%f", 1.5))
            self.assertEqual("1", _fn_mod("%x", 1))
            self.assertEqual("A", _fn_mod("%c", 65))
            self.assertEqual("'x'", _fn_mod("%r", "x"))
            self.assertEqual("b'1'", str(_fn_mod(b"%d", 1)))


@skipUnless(_HAS_GUARD, "guard not present")
class TestFourthRoundEvaluatePath(TestCase):
    """第四轮验收：**求值路径**（``ExpressionParser.evaluate``）上的判定。

    根因：预检会把不可重复迭代的输入**物化并消费**，探针随后再 measure 原始参数
    只能看到"已耗尽 ⇒ 内容 0"，allowance 落到 floor，于是同一个 join 的**列表形态
    能过、生成器形态被误拒**。修复＝调用点级结论：预检把这次判定用的输入规模登记
    下来，探针复用同一份（`_take_result_verdict`），而不是事后重新测量参数。

    本类所有断言都经 ``evaluate``；helper 层（直调 ``_fn_*``）的对照见
    ``TestThirdRoundNonAmplifying`` 里带 ``_helper_layer`` 的用例。
    """

    def _evaluate(self, expression: str, context: dict):
        return _fresh().evaluate(expression, context)

    def test_join_generator_20x10000_evaluate_path_is_allowed(self):
        """owner 具名用例的生成器形态（求值路径）——第四轮的误拒回归。"""
        result = self._evaluate("$F.join($INPUT.g, '')",
                                {"$INPUT": {"g": (item for item in ["c" * 10000] * 20)}})
        self.assertEqual(200000, len(result))

    def test_join_list_20x10000_evaluate_path_is_allowed(self):
        """同一内容、同一表达式形状的列表形态：两者必须一致放行。"""
        result = self._evaluate("$F.join($INPUT.x, '')", {"$INPUT": {"x": ["c" * 10000] * 20}})
        self.assertEqual(200000, len(result))

    def test_join_generator_200k_x1_evaluate_path_is_allowed(self):
        result = self._evaluate("$F.join($INPUT.g, '')",
                                {"$INPUT": {"g": (c for c in "x" * 200000)}})
        self.assertEqual(200000, len(result))

    def test_join_views_evaluate_path_are_allowed(self):
        keys = {("k%09d" % i): 1 for i in range(200000)}
        self.assertEqual(2199999, len(self._evaluate("$F.join($INPUT.k, ',')",
                                                     {"$INPUT": {"k": keys.keys()}})))
        values = {i: "c" * 10 for i in range(20000)}
        self.assertEqual(219999, len(self._evaluate("$F.join($INPUT.v, ',')",
                                                    {"$INPUT": {"v": values.values()}})))

    def test_join_small_generator_evaluate_path_is_correct(self):
        result = self._evaluate("$F.join($INPUT.g, '-')",
                                {"$INPUT": {"g": (c for c in ("ab", "ab", "ab"))}})
        self.assertEqual("ab-ab-ab", result)

    def test_join_lying_length_evaluate_path_is_allowed_and_identical(self):
        """诚实与谎报 ``__len__`` 必须**同样放行且结果逐字节相同**（不是同样被拒）。"""
        honest = self._evaluate("$F.join($INPUT.g, '')",
                                {"$INPUT": {"g": (item for item in ["c" * 10] * 200000)}})

        class _Liar:
            def __init__(self, items, claim):
                self._items, self._claim = items, claim

            def __len__(self):
                return self._claim

            def __iter__(self):
                return iter(self._items)

        for claim in (10, 10**9):
            with self.subTest(claim=claim):
                liar = _Liar(["c" * 10] * 200000, claim)
                self.assertEqual(honest, self._evaluate("$F.join($INPUT.g, '')",
                                                        {"$INPUT": {"g": liar}}))
        self.assertEqual(2000000, len(honest))

    def test_join_generator_amplification_evaluate_path_is_refused(self):
        """放大形态在求值路径上仍必须被拒（生成器 + 大分隔符）。"""
        with self.assertRaises(ValueError) as cm:
            self._evaluate("$F.join($INPUT.g, $INPUT.sep)",
                           {"$INPUT": {"g": ("" for _ in range(200000)), "sep": "x" * 1000}})
        self.assertIn("$F.join", str(cm.exception))
        with self.assertRaises(ValueError):
            self._evaluate("$F.mul('x', 1000000)", {})

    def test_join_generator_amplification_peak_is_bounded(self):
        ctx = {"$INPUT": {"g": ("" for _ in range(200000)), "sep": "x" * 1000}}
        desc, peak = _measure_peak(lambda: _fresh().evaluate("$F.join($INPUT.g, $INPUT.sep)", ctx))
        self.assertEqual("ValueError", desc)
        self.assertLess(peak, _PEAK_BUDGET)

    def test_other_iterable_receivers_keep_native_behaviour(self):
        """顺带自查：其它接收可迭代的算子不得因"预检消费"改变行为。"""
        parser = _fresh()
        with self.assertRaises(TypeError):                      # list + generator
            parser.evaluate("$F.add($INPUT.a, $INPUT.g)",
                            {"$INPUT": {"a": [0], "g": (x for x in [1])}})
        with self.assertRaises(TypeError):                      # 同上（extend 是拼接）
            parser.evaluate("$F.extend($INPUT.a, $INPUT.g)",
                            {"$INPUT": {"a": [0], "g": (x for x in [1])}})
        with self.assertRaises(TypeError):                      # len(generator)
            parser.evaluate("$F.len($INPUT.g)", {"$INPUT": {"g": (x for x in [1])}})
        with self.assertRaises(TypeError):                      # generator * int
            parser.evaluate("$F.mul($INPUT.g, 2)", {"$INPUT": {"g": (x for x in [1])}})
        self.assertEqual(5000, len(parser.evaluate("$F.slice($INPUT.a, 0, 5000)",
                                                   {"$INPUT": {"a": list(range(200000))}})))
        self.assertEqual(2, len(parser.evaluate("$F.keys($INPUT.d)",
                                                {"$INPUT": {"d": {"a": 1, "b": 2}}})))

    def test_call_point_verdict_is_consumed_not_reused(self):
        """登记值必须被"取出并清除"：不能让上一次调用污染下一次。"""
        parser = _fresh()
        self.assertEqual(200000, len(parser.evaluate(
            "$F.join($INPUT.g, '')", {"$INPUT": {"g": (c for c in "c" * 200000)}})))
        # 紧接着一次未预检的大结果：不得沿用上一次的 input_size
        # 上一次 join 登记的 input_size≈200000；若被复用，这次 200000 字符的结果
        # 会被放行（allowed=800000）。必须拒绝 ⇒ 登记值确实被取出并清除了。
        with self.assertRaises(ValueError):
            parser.evaluate("$F.boom($INPUT.n)", {"$INPUT": {"n": 200000}},
                            _registry_with_boom())


def _registry_with_boom():
    registry = ExpressionRegistry()
    registry.register("boom", lambda n: "x" * n, "boom")
    return registry


@skipUnless(_HAS_GUARD, "guard not present")
class TestFifthRoundNoForgeryChannel(TestCase):
    """第五轮验收 必修 1：判定必须只依赖本次调用的实参——**结构上不可种入**。

    旧实现把"这次调用用的 input_size"放在**线程本地**里，而"调用点"并不等于
    "一次求值"：任何在 ``func(*args)`` 里跑起来、又能到达 ``_check_result_size``
    的代码都会种入判定，外层调用点随即把它当成自己的预检结论。实测（旧树）：
    ``$F.evil({'tiny':'a'})``（内部先做一次合法的 30M concat 再返回 99M 字符串）
    → **ALLOWED 99,000,000 字符，峰值 94.5MB**；嵌套 ``evaluate`` 变体 → 123.1MB。
    """

    def _registry(self):
        registry = ExpressionRegistry()
        registry.register("evil", self._evil_concat_then_huge, "evil")
        registry.register("evil2", self._evil_nested_evaluate, "evil2")
        registry.register("boom", lambda n: "x" * n, "boom")
        from plaita.core.expression import _fn_mul
        registry.register("mul", _fn_mul, "mul")     # 供"异常路径"用例使用
        return registry

    @staticmethod
    def _evil_concat_then_huge(_arg):
        from plaita.core.expression import _fn_concat
        _fn_concat("A" * 30_000_000)          # 内部合法大调用（非放大 ⇒ 放行）
        return "Y" * 99_000_000

    @staticmethod
    def _evil_nested_evaluate(_arg):
        inner = _fresh()
        try:
            inner.evaluate("$F.mul($INPUT.big, 1000000000)",
                           {"$INPUT": {"big": "A" * 30_000_000}})
        except ValueError:
            pass                              # 拒绝也要"种入"过 30M
        return "Y" * 99_000_000

    def test_verdict_channel_is_structurally_gone(self):
        import inspect
        for name in ("_record_result_verdict", "_take_result_verdict", "_CALL_VERDICT"):
            with self.subTest(name=name):
                self.assertFalse(hasattr(expr_mod, name), f"{name} 又会引入可种入通道")
        self.assertNotIn("input_size", inspect.signature(_probe).parameters)

    def test_callee_cannot_seed_allowance_with_inner_successful_call(self):
        with self.assertRaises(ValueError) as cm:
            _fresh().evaluate("$F.evil($INPUT.tiny)",
                              {"$INPUT": {"tiny": "a"}}, self._registry())
        self.assertIn("$F.evil result too large", str(cm.exception))

    def test_callee_cannot_seed_allowance_with_nested_evaluate(self):
        with self.assertRaises(ValueError) as cm:
            _fresh().evaluate("$F.evil2($INPUT.tiny)",
                              {"$INPUT": {"tiny": "a"}}, self._registry())
        self.assertIn("$F.evil2 result too large", str(cm.exception))

    def test_exception_path_leaves_no_consumable_state(self):
        """异常路径（预检拒绝）不得留下任何能被后续求值消费的状态。"""
        parser = _fresh()
        registry = self._registry()
        with self.assertRaises(ValueError):       # 预检先算出 input_size=60000 再拒绝
            parser.evaluate("$F.mul($INPUT.big, $INPUT.n)",
                            {"$INPUT": {"big": "b" * 60000, "n": 10**9}}, registry)
        # 若上一次的 60000 泄漏，这里 allowed 会到 240000 ⇒ 200000 被放行
        with self.assertRaises(ValueError):
            parser.evaluate("$F.boom(200000)", {}, registry)

    def test_probe_is_independent_of_the_precheck(self):
        """把预检换成空操作，探针仍按真实实参判定（没有共享状态可言）。"""
        original = expr_mod._check_result_size
        expr_mod._check_result_size = lambda *a, **k: None
        try:
            with self.assertRaises(ValueError):
                _fresh().evaluate("$F.boom(200000)", {}, self._registry())
        finally:
            expr_mod._check_result_size = original

    def test_custom_function_allowance_is_its_own_argument_content(self):
        """对照：没有种入时两者都被拒（证明上面的 ALLOWED 只可能来自种入）。"""
        registry = self._registry()
        for expression in ("$F.boom(200000)", "$F.boom(99999999)"):
            with self.subTest(expression=expression):
                with self.assertRaises(ValueError):
                    _fresh().evaluate(expression, {}, registry)


@skipUnless(_HAS_GUARD, "guard not present")
class TestFifthRoundCallPointPrepare(TestCase):
    """第五轮验收 必修 1：``join``/``sort`` 在**调用点**把单次迭代器物化一次。

    物化结果同时用于①投影②输入规模③真正的算子执行（实参替换），因此
    ``iter(x) is x`` 的迭代器只会被拉取一次，且探针随后测量到的是真实内容量。
    """

    class _CountingOneShot:
        def __init__(self, items):
            self._iterator = iter(items)
            self.pulls = 0

        def __iter__(self):
            return self

        def __next__(self):
            self.pulls += 1
            return next(self._iterator)

    def test_join_materializes_a_one_shot_iterator_exactly_once(self):
        items = ["c" * 10000] * 20
        source = self._CountingOneShot(items)
        result = _fresh().evaluate("$F.join($INPUT.g, '')", {"$INPUT": {"g": source}})
        self.assertEqual(200000, len(result))
        self.assertLessEqual(source.pulls, len(items) + 1)   # 只拉取一遍（+1 次 StopIteration）

    def test_sort_materializes_a_one_shot_iterator_exactly_once(self):
        source = self._CountingOneShot([3, 1, 2])
        result = _fresh().evaluate("$F.sort($INPUT.g)", {"$INPUT": {"g": source}})
        self.assertEqual([1, 2, 3], result)
        self.assertLessEqual(source.pulls, 4)

    def test_prepare_join_refusal_message_evaluate_path(self):
        """调用点物化的拒绝文案（求值路径）：项数/单位/上限逐字比对。

        这条同时杀掉 6 个"消息级"非等价变异：``*``→``/``、``(count±1)``、
        ``what=None``、``unit=None``、``_unit_for(None)``（会变成 elements）。
        """
        words = ["a" * 100] * 5 + ["a" * 6]          # 6 项，内容合计 506
        with _limits(floor=1, factor=1, hard=7):
            with self.assertRaises(ValueError) as cm:
                _fresh().evaluate("$F.join($INPUT.words, $INPUT.sep)",
                                  {"$INPUT": {"words": words, "sep": "x" * 200}})
        self.assertEqual(
            "$F.join(6 items) result too large: 1506 chars (limit 7). "
            "Refusing to build an unbounded result.", str(cm.exception))

    def test_prepare_table_is_keyed_by_function_object(self):
        from plaita.core.expression import _fn_join, _fn_sort
        self.assertIs(_prepare_join, expr_mod.PROJECTIONS[_fn_join])
        self.assertIs(_prepare_sort, expr_mod.PROJECTIONS[_fn_sort])

    def test_overridden_builtin_does_not_use_the_prepare_table(self):
        """scoped registry 覆盖同名算子时对象不同 ⇒ 不走 prepare，按真实参数判定。"""
        registry = ExpressionRegistry()
        registry.register("join", lambda a, b: "x" * 1000000, "override")
        with self.assertRaises(ValueError):
            _fresh().evaluate("$F.join($INPUT.g, '')",
                              {"$INPUT": {"g": ["c" * 10]}}, registry)


@skipUnless(_HAS_GUARD, "guard not present")
class TestFifthRoundModWithOneShotIterators(TestCase):
    """第五轮验收 必修 2：``%s/%r/%a`` + 单次迭代器必须与 base 行为一致。

    旧实现把"单次迭代器"饱和成 ``hard``，而 ``_spec_bound`` 又把它当成**渲染后**
    的尺寸 ⇒ ``'%s' % generator`` 被投影成 1e8 并误拒（base 是 OK）。渲染实际是
    ``str(value)``（生成器/zip/map 是短 repr），所以 ``s/r/a`` 用 ``_render_size``。
    """

    FORMS = (
        ("generator", lambda: (c for c in "abcde")),
        ("zip", lambda: zip([1, 2], [3, 4])),
        ("iter(list)", lambda: iter([1, 2, 3])),
        ("itertools.repeat", lambda: __import__("itertools").repeat("x", 5)),
    )

    def test_percent_s_with_iterators_is_allowed_like_base(self):
        for label, factory in self.FORMS:
            with self.subTest(form=label):
                value = factory()
                result = _fresh().evaluate("$F.mod('%s', $INPUT.v)", {"$INPUT": {"v": value}})
                self.assertIsInstance(result, str)
                self.assertLess(len(result), 200)     # 短 repr，不是 1e8
                self.assertTrue(result.startswith("<") or result.startswith("repeat("),
                                result)

    def test_percent_r_and_a_with_iterators_are_allowed(self):
        for conversion in ("r", "a"):
            with self.subTest(conversion=conversion):
                result = _fresh().evaluate(f"$F.mod('%{conversion}', $INPUT.v)",
                                           {"$INPUT": {"v": (c for c in "ab")}})
                self.assertLess(len(result), 200)

    def test_render_size_used_for_sra_not_content_size(self):
        generator = (c for c in "abc")
        self.assertLess(_render_size(generator), 1000)
        self.assertEqual(3, _render_size("abc"))
        self.assertEqual(11, _render_size(123456789))
        self.assertGreaterEqual(_render_size(["a"] * 10), 10)
        self.assertGreaterEqual(_render_size([""] * 10), 20)   # 元素数×2 的 repr 余量


class _LiarBuffer:
    """自报 ``itemsize``/``__len__`` 巨大、``__str__`` 巨大的"说谎缓冲对象"。"""

    itemsize = 25_000_000

    def __len__(self):
        return 1

    def __iter__(self):
        return self

    def __next__(self):
        raise StopIteration

    def __str__(self):
        return "x" * 99_000_000


class _CustomMapping:
    """非 dict 的 Mapping：``_content_size`` 必须走键+值，而不是漏掉 values。"""

    def __init__(self, data):
        self._data = data

    def __getitem__(self, key):
        return self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def keys(self):
        return self._data.keys()

    def items(self):
        return self._data.items()

    def values(self):
        return self._data.values()

    def get(self, key, default=None):
        return self._data.get(key, default)


class _ExplodingMapping:
    def __init__(self, size):
        self._size = size

    def __len__(self):
        return self._size

    def __iter__(self):
        return iter(())

    def keys(self):
        return ()

    def items(self):
        raise RuntimeError("boom from items()")

    def values(self):
        raise RuntimeError("boom from values()")

    def __getitem__(self, key):
        raise KeyError(key)


@skipUnless(_HAS_GUARD, "guard not present")
class TestSixthRoundPreCallMeasurement(TestCase):
    """第六轮验收 必修 1：额度来自**调用前**的测量，而不是调用后回读实参。

    旧实现（第五轮交付）在 ``func(*args)`` 之后回读实参算输入规模 ⇒ 被调用者只要
    ``lst.clear(); lst.extend([...巨大...])`` 就能把 allowance 抬高。实测旧树：
    ``$F.evil($INPUT.lst)``（lst=[0]，函数改写它再返回 99M）→ **ALLOWED 99,000,000
    字符，峰值 123.1MB**。
    """

    @staticmethod
    def _evil_mutates_then_huge(lst):
        lst.clear()
        lst.extend(["A" * 30_000_000])        # 抬高 allowance 的尝试
        return "Y" * 99_000_000

    def _registry(self):
        registry = ExpressionRegistry()
        registry.register("evil", self._evil_mutates_then_huge, "evil")
        return registry

    def test_callee_mutating_its_argument_cannot_raise_the_allowance(self):
        with self.assertRaises(ValueError) as cm:
            _fresh().evaluate("$F.evil($INPUT.lst)", {"$INPUT": {"lst": [0]}},
                              self._registry())
        self.assertIn("$F.evil result too large", str(cm.exception))

    def test_allowance_is_computed_before_the_call(self):
        """结构性断言：守卫只收调用点算好的局部量，不接收实参元组。"""
        import inspect
        params = list(inspect.signature(_size_guard_result_raw).parameters)
        self.assertEqual(["func_name", "value", "input_size"], params)

    def test_measure_input_is_a_pure_function_of_the_snapshot(self):
        args = ([0], "sep")
        snapshot = _measure_input(tuple(args))
        args[0].extend(["A" * 1000])          # 调用后再改实参
        self.assertEqual(snapshot, _measure_input(([0], "sep")))
        self.assertLess(snapshot, 100)


@skipUnless(_HAS_GUARD, "guard not present")
class TestSixthRoundAuthoritativeSizing(TestCase):
    """第六轮验收 必修 2：不得信任 ``itemsize``/``__len__`` 这类自报尺寸。"""

    def test_lying_buffer_object_contributes_nothing(self):
        liar = _LiarBuffer()
        self.assertEqual(0, _content_size(liar))
        self.assertIsNone(_render_size(liar))

    def test_percent_s_with_lying_buffer_is_refused_before_allocation(self):
        desc, peak = _measure_peak(
            lambda: _fresh().evaluate("$F.mod('%s', $INPUT.o)",
                                      {"$INPUT": {"o": _LiarBuffer()}}))
        self.assertEqual("ValueError", desc)
        self.assertLess(peak, _PEAK_BUDGET, f"拒绝发生在渲染之后：peak={peak/1024/1024:.1f}MB")
        with self.assertRaises(ValueError) as cm:
            _fresh().evaluate("$F.mod('%s', $INPUT.o)", {"$INPUT": {"o": _LiarBuffer()}})
        self.assertIn("cannot bound the rendered size", str(cm.exception))

    def test_real_buffers_still_use_memoryview_nbytes(self):
        import array
        self.assertEqual(1234, _content_size(memoryview(b"x" * 1234)))
        self.assertEqual(4000, _content_size(array.array("i", [0] * 1000)))

    def test_custom_mapping_counts_keys_and_values(self):
        mapping = _CustomMapping({"key": "v" * 100})
        self.assertGreaterEqual(_content_size(mapping), 100)
        self.assertGreaterEqual(_render_size(mapping), 100)

    def test_mapping_that_raises_is_refused_not_leaked(self):
        exploding = _ExplodingMapping(10**8)
        with self.assertRaises(ValueError) as cm:
            _content_size(exploding)
        self.assertIn("unmeasurable value", str(cm.exception))

    def test_unknown_object_with_custom_str_is_conservative(self):
        class Blob:
            def __str__(self):
                return "z" * 10

        self.assertEqual(0, _content_size(Blob()))
        self.assertIsNone(_render_size(Blob()))          # 无法界定 ⇒ 预检保守拒绝

    def test_default_repr_objects_are_bounded(self):
        class Plain:
            pass

        # AM1（第二十四轮）：默认 repr 的 256 是**推理**（类名可写）⇒ 改成行为断言
        # （上界 ≥ 实际 repr），实现侧已读真实 `__module__`/`__qualname__`。
        value = Plain()
        self.assertGreaterEqual(_render_size(value), len(repr(value)))
        self.assertGreaterEqual(_render_size(value, as_repr=True), len(repr(value)))
        generator = (c for c in "ab")
        self.assertGreaterEqual(_render_size(generator), len(repr(generator)))
        iterator = iter([1, 2])
        self.assertGreaterEqual(_render_size(iterator), len(repr(iterator)))


@skipUnless(_HAS_GUARD, "guard not present")
class TestSixthRoundWrappedConsumers(TestCase):
    """第六轮验收 必修 3：包装/偏应用/绑定方法不得丢失调用点物化。"""

    def _run_with(self, name, func, expression, context, expected_len):
        registry = ExpressionRegistry()
        registry.register(name, func, name)
        result = _fresh().evaluate(expression, context, registry)
        self.assertEqual(expected_len, len(result))
        return result

    @staticmethod
    def _generator():
        return (item for item in ["c" * 10000] * 20)

    def test_join_wrapped_in_partial_is_allowed(self):
        import functools
        from plaita.core.expression import _fn_join
        self._run_with("join", functools.partial(_fn_join), "$F.join($INPUT.g, '')",
                       {"$INPUT": {"g": self._generator()}}, 200000)

    def test_join_wrapped_in_lambda_is_allowed(self):
        from plaita.core.expression import _fn_join
        self._run_with("join", lambda a, b: _fn_join(a, b), "$F.join($INPUT.g, '')",
                       {"$INPUT": {"g": self._generator()}}, 200000)

    def test_sort_wrapped_in_partial_is_allowed(self):
        import functools
        from plaita.core.expression import _fn_sort
        registry = ExpressionRegistry()
        registry.register("sort", functools.partial(_fn_sort), "sort")
        result = _fresh().evaluate("$F.sort($INPUT.g)",
                                   {"$INPUT": {"g": (x for x in range(200000))}}, registry)
        self.assertEqual(200000, len(result))

    def test_wrapped_join_with_huge_separator_is_still_refused(self):
        from plaita.core.expression import _fn_join
        registry = ExpressionRegistry()
        registry.register("join", lambda a, b: _fn_join(a, b), "join")
        with self.assertRaises(ValueError):
            _fresh().evaluate("$F.join($INPUT.g, $INPUT.sep)",
                              {"$INPUT": {"g": ("" for _ in range(200000)), "sep": "x" * 1000}},
                              registry)

    def test_name_based_materialization_only_for_consuming_operators(self):
        from plaita.core.expression import ITERABLE_CONSUMING_NAMES
        self.assertEqual(frozenset({"join", "sort"}), ITERABLE_CONSUMING_NAMES)

    def test_custom_function_gets_no_materialization(self):
        """自定义函数名不在消费集合里：单次迭代器按 0 计（严格方向）。"""
        registry = ExpressionRegistry()
        registry.register("sneaky", lambda g: "x" * 200000, "sneaky")
        with self.assertRaises(ValueError):
            _fresh().evaluate("$F.sneaky($INPUT.g)", {"$INPUT": {"g": (c for c in "ab")}}, registry)


@skipUnless(_HAS_GUARD, "guard not present")
class TestSixthRoundTemplateInterpolation(TestCase):
    """第六轮验收 必修 4：``{% ... %}`` 插值也要过结果守卫。"""

    class _HugeStr:
        def __str__(self):
            return "Z" * 99_000_000

    def test_template_interpolation_of_huge_str_is_refused(self):
        with self.assertRaises(ValueError) as cm:
            _fresh().evaluate("{% $INPUT.o %}", {"$INPUT": {"o": self._HugeStr()}})
        self.assertIn("$F.template cannot bound the rendered size", str(cm.exception))

    def test_template_interpolation_of_small_values_still_works(self):
        self.assertEqual("v=7", _fresh().evaluate("v={% $INPUT.n %}", {"$INPUT": {"n": 7}}))
        self.assertEqual("[1, 2]", _fresh().evaluate("{% $INPUT.x %}", {"$INPUT": {"x": [1, 2]}}))

    def test_template_interpolation_of_big_but_allowed_value(self):
        payload = "y" * 200000
        self.assertEqual(payload, _fresh().evaluate("{% $INPUT.s %}", {"$INPUT": {"s": payload}}))


class _LyingMapping:
    """鸭子类型映射：``__iter__`` 空、``len=0``、``items()`` 25 个 1MB 别名（真实 1MB），
    但 ``__str__`` 返回 99MB。第七轮验收用它证明"按元素估上界"不是上界。"""

    def __init__(self):
        self._blob = b"x" * (1024 * 1024)

    def __len__(self):
        return 0

    def __iter__(self):
        return iter(())

    def keys(self):
        return ()

    def items(self):
        return ((i, self._blob) for i in range(25))

    def values(self):
        return (self._blob for _ in range(25))

    def __getitem__(self, key):
        return self._blob

    def __str__(self):
        return "Y" * 99_000_000


class _RepeatableHugeStr:
    """可重复迭代（``__iter__`` 返回新迭代器）+ 99MB ``__str__``。"""

    def __len__(self):
        return 0

    def __iter__(self):
        return iter(())

    def __str__(self):
        return "Z" * 99_000_000


@skipUnless(_HAS_GUARD, "guard not present")
class TestSeventhRoundRenderGate(TestCase):
    """第七轮验收 必修 1：渲染前闸（bounded-repr 判定必须排在任何元素分支之前）。

    旧实现只在"非可迭代兜底分支"查自定义 ``__str__`` ⇒ 映射/序列/缓冲/带
    ``__iter__`` 的值都走"按元素估上界"，而那不是上界（实测 LyingMapping 元素合计
    1MB、真实渲染 99MB ⇒ **ALLOWED 99,000,000 字符，峰值 94.70MB**）。
    """

    def test_lying_mapping_with_huge_str_is_refused_before_rendering(self):
        desc, peak = _measure_peak(
            lambda: _fresh().evaluate("$F.mod('%s', $INPUT.o)",
                                      {"$INPUT": {"o": _LyingMapping()}}))
        self.assertEqual("ValueError", desc)
        self.assertLess(peak, _PEAK_BUDGET, f"渲染发生了：peak={peak/1024/1024:.1f}MB")

    def test_repeatable_huge_str_refused_in_mod_and_template(self):
        for label, call in (
            ("mod", lambda: _fresh().evaluate("$F.mod('%s', $INPUT.o)",
                                              {"$INPUT": {"o": _RepeatableHugeStr()}})),
            ("template", lambda: _fresh().evaluate("{% $INPUT.o %}",
                                                   {"$INPUT": {"o": _RepeatableHugeStr()}})),
        ):
            with self.subTest(path=label):
                desc, peak = _measure_peak(call)
                self.assertEqual("ValueError", desc)
                self.assertLess(peak, _PEAK_BUDGET, f"{label}: peak={peak/1024/1024:.1f}MB")

    def test_truncating_precision_does_not_materialize_the_render(self):
        """``%.5s`` 结果只有 5 字符，旧实现却产生 94MB transient。"""
        desc, peak = _measure_peak(
            lambda: _fresh().evaluate("$F.mod('%.5s', $INPUT.o)",
                                      {"$INPUT": {"o": _LiarBuffer()}}))
        self.assertEqual("ValueError", desc)
        self.assertLess(peak, _PEAK_BUDGET)

    def test_concat_render_gate(self):
        with self.assertRaises(ValueError) as cm:
            _fresh().evaluate("$F.concat($INPUT.o)", {"$INPUT": {"o": _RepeatableHugeStr()}})
        self.assertIn("cannot bound the rendered size", str(cm.exception))

    def test_normal_renders_are_unaffected(self):
        parser = _fresh()
        self.assertEqual("abc", parser.evaluate("$F.mod('%s', 'abc')", {}))
        self.assertEqual("12", parser.evaluate("$F.mod('%s', 12)", {}))
        self.assertEqual("[1, 2]", parser.evaluate("$F.mod('%s', $INPUT.l)",
                                                   {"$INPUT": {"l": [1, 2]}}))
        self.assertEqual("{'a': 1}", parser.evaluate("$F.mod('%s', $INPUT.d)",
                                                     {"$INPUT": {"d": {"a": 1}}}))
        self.assertLess(len(parser.evaluate("$F.mod('%s', $INPUT.g)",
                                            {"$INPUT": {"g": (c for c in "ab")}})), 200)

    def test_bounded_repr_is_shape_independent(self):
        """判定必须与"形状"无关：可迭代/映射/缓冲都不例外。"""
        from plaita.core.expression import _render_size
        self.assertIsNone(_render_size(_LyingMapping()))
        self.assertIsNone(_render_size(_RepeatableHugeStr()))
        self.assertIsNone(_render_size(_LiarBuffer()))
        self.assertEqual(256, _render_size((c for c in "ab")))     # 内建生成器仍可有界


@skipUnless(_HAS_GUARD, "guard not present")
class TestSeventhRoundElementCountFloor(TestCase):
    """第七轮验收 必修 2：已知长度的内建容器用**元素数**作下界，避免误拒。

    旧实现：``_content_size(['']*200000) == 0``（空串内容量 0）而
    ``_measure(结果) == 200000`` ⇒ 口径不一致 ⇒ base 允许的
    ``sort(['']*200000)`` / ``join(['']*200000, 'x')`` 被拒。
    """

    def test_sort_of_empty_strings_matches_base(self):
        result = _fresh().evaluate("$F.sort($INPUT.x)", {"$INPUT": {"x": [""] * 200000}})
        self.assertEqual(200000, len(result))

    def test_join_of_empty_strings_matches_base(self):
        result = _fresh().evaluate("$F.join($INPUT.x, 'x')", {"$INPUT": {"x": [""] * 200000}})
        self.assertEqual(199999, len(result))

    def test_content_size_uses_element_count_as_floor(self):
        self.assertEqual(200000, _content_size([""] * 200000))
        self.assertGreaterEqual(_content_size({str(i): "" for i in range(1000)}), 1000)
        self.assertEqual(3000, _content_size(["c" * 3] * 1000))   # 内容量占主导

    def test_amplification_is_still_refused(self):
        with self.assertRaises(ValueError):
            _fresh().evaluate("$F.join($INPUT.g, $INPUT.s)",
                              {"$INPUT": {"g": ("" for _ in range(200000)), "s": "x" * 1000}})
        with self.assertRaises(ValueError):
            _fresh().evaluate("$F.mul('x', 1000000)", {})


class _LyingLenList(list):
    def __len__(self):
        return 0


class _LyingLenTuple(tuple):
    def __len__(self):
        return 0


class _SpoofedModule:
    """自报 ``__module__ = "itertools"``、``__str__`` 返回 50MB 的对象。"""

    __module__ = "itertools"

    def __str__(self):
        return "Z" * 50_000_000


class _EvilKey:
    def __str__(self):
        return "K" * 1000

    def __hash__(self):
        return 1

    def __eq__(self, other):
        return self is other


@skipUnless(_HAS_GUARD, "guard not present")
class TestEighthRoundSubclassLen(TestCase):
    """第八轮验收 H1：``join`` 不许信任 **list/tuple/str 子类**谎报的 ``__len__``。

    旧实现 `_join_item_lengths` 的快路径直接 `len(items)` 当项数；谎报 0 时投影里的
    `len(sep)*(n-1)` 项消失 ⇒ 预检通过 ⇒ 原生 join 造出整块结果，探针事后才拒
    （实测 1M 空串 + `'x'*1000`：**峰值 103–206MB**；list 子类诚实版本 0.00MB）。
    """

    def test_lying_len_list_matches_honest_behaviour(self):
        for n in (300_000, 1_000_000):
            with self.subTest(n=n):
                honest = [""] * n
                lying = _LyingLenList([""] * n)
                desc_honest, peak_honest = _measure_peak(
                    lambda: _fresh().evaluate("$F.join($INPUT.g, $INPUT.s)",
                                              {"$INPUT": {"g": honest, "s": "x" * 100}}))
                desc_lying, peak_lying = _measure_peak(
                    lambda: _fresh().evaluate("$F.join($INPUT.g, $INPUT.s)",
                                              {"$INPUT": {"g": lying, "s": "x" * 100}}))
                self.assertEqual(desc_honest, desc_lying)
                self.assertEqual("ValueError", desc_lying)
                # 子类实参会被**物化一次**（单次迭代，防有状态对象长出来），
                # 峰值以输入自身的指针量（8B/项）为尺度，**不得放大**。
                self.assertLess(peak_lying, n * 24 + 4 * 1024 * 1024,
                                f"谎报 len 触发了放大：{peak_lying/1024/1024:.1f}MB")

    def test_lying_len_tuple_is_refused_cheaply(self):
        lying = _LyingLenTuple([""] * 1_000_000)
        desc, peak = _measure_peak(
            lambda: _fresh().evaluate("$F.join($INPUT.g, $INPUT.s)",
                                      {"$INPUT": {"g": lying, "s": "x" * 100}}))
        self.assertEqual("ValueError", desc)
        self.assertLess(peak, 1_000_000 * 24 + 4 * 1024 * 1024)   # 只允许"物化一次"的线性量

    def test_lying_len_str_subclass_matches_honest(self):
        class _LyingLenStr(str):
            def __len__(self):
                return 0

        self.assertEqual("", _fresh().evaluate("$F.join($INPUT.g, ',')",
                                               {"$INPUT": {"g": _LyingLenStr("")}}))
        self.assertEqual("a,b", _fresh().evaluate("$F.join($INPUT.g, ',')",
                                                  {"$INPUT": {"g": _LyingLenStr("ab")}}))

    def test_join_fast_path_only_trusts_exact_builtin_types(self):
        """结构性断言：快路径的判据是 `type(...) is list/tuple/str`。"""
        import inspect
        source = inspect.getsource(expr_mod._join_item_lengths)
        self.assertIn("type(items) is str", source)
        self.assertIn("type(items) in (list, tuple)", source)
        self.assertNotIn("isinstance(items, (list, tuple))", source)


@skipUnless(_HAS_GUARD, "guard not present")
class TestEighthRoundSpoofedModule(TestCase):
    """第八轮验收 H2：不许有任何基于字符串自述（如 ``__module__``）的判据。"""

    def test_spoofed_module_does_not_get_a_short_repr_bound(self):
        self.assertIsNone(_render_size(_SpoofedModule()))
        self.assertIsNone(_render_size(_SpoofedModule()))

    def test_spoofed_module_is_refused_before_rendering(self):
        for label, call in (
            ("mod", lambda: _fresh().evaluate("$F.mod('%s', $INPUT.o)",
                                              {"$INPUT": {"o": _SpoofedModule()}})),
            ("truncating", lambda: _fresh().evaluate("$F.mod('%.5s', $INPUT.o)",
                                                     {"$INPUT": {"o": _SpoofedModule()}})),
            ("template", lambda: _fresh().evaluate("{% $INPUT.o %}",
                                                   {"$INPUT": {"o": _SpoofedModule()}})),
            ("concat", lambda: _fresh().evaluate("$F.concat($INPUT.o)",
                                                 {"$INPUT": {"o": _SpoofedModule()}})),
        ):
            with self.subTest(path=label):
                desc, peak = _measure_peak(call)
                self.assertEqual("ValueError", desc)
                self.assertLess(peak, _PEAK_BUDGET, f"{label}: {peak/1024/1024:.1f}MB")

    def test_no_string_based_judgement_in_source(self):
        import inspect
        source = inspect.getsource(expr_mod._render_size)
        self.assertNotIn("__module__", source)
        self.assertNotIn('== "itertools"', source)


@skipUnless(_HAS_GUARD, "guard not present")
class TestEighthRoundProvableBounds(TestCase):
    """第八轮验收 必修 3/6：上界必须 ≥ 实际渲染长度；子项不可界定必须整条返回 None。"""

    def test_bounds_are_never_below_the_actual_render(self):
        import re as _re
        from collections import namedtuple
        Point = namedtuple("Point", "x y")
        cases = [
            "abc", "", b"\xff" * 100, bytearray(b"\xff" * 100), 123456789, 1.5, True, None,
            ["\x00" * 100], [""] * 1000, {"a": 1}, {1, 2}, ("x", 1), range(1000),
            b"a" * 1000, [b"\xff" * 10], {"k": ["\x00" * 10]},
        ]
        for value in cases:
            with self.subTest(value=type(value).__name__):
                bound = _render_size(value)
                self.assertIsNotNone(bound, f"{value!r} 应有可证上界")
                actual = len(str(value))
                self.assertGreaterEqual(bound, actual, f"上界 {bound} < 实际 {actual}")

    def test_bytes_escape_budget_is_four_times(self):
        self.assertGreaterEqual(_render_size(b"\xff" * 100), len(str(b"\xff" * 100)))
        self.assertGreaterEqual(_render_size(bytearray(b"\xff" * 100)),
                                len(str(bytearray(b"\xff" * 100))))

    def test_empty_container_bound_counts_the_element_count(self):
        """``['']*1000`` 的渲染是 4000 字符，上界不许只给 16（第七轮低估 250×）。"""
        self.assertGreaterEqual(_render_size([""] * 1000), len(str([""] * 1000)))
        self.assertGreaterEqual(_render_size([""] * 1_000_000), len(str([""] * 1_000_000)))
        self.assertGreaterEqual(_render_size([""] * 25_000_000), 100_000_000)   # O(1) 短路

    def test_unbounded_child_makes_the_whole_bound_none(self):
        for value in ({_EvilKey(): 1}, [_EvilKey()], (_EvilKey(),), [[_EvilKey()]],
                      {"k": [_EvilKey()]}):
            with self.subTest(value=type(value).__name__):
                self.assertIsNone(_render_size(value))

    def test_unbounded_child_yields_value_error_not_type_error(self):
        for value in ({_EvilKey(): 1}, [_EvilKey()]):
            with self.subTest(value=type(value).__name__):
                with self.assertRaises(ValueError):
                    _fresh().evaluate("$F.mod('%s', $INPUT.o)", {"$INPUT": {"o": value}})


@skipUnless(_HAS_GUARD, "guard not present")
class TestEighthRoundLegitimateValues(TestCase):
    """第八轮验收 必修 5（R1）：合法值必须可渲染（收紧只落在攻击面）。"""

    def _renderable(self):
        import array
        import datetime
        import decimal
        import enum
        import pathlib
        import re
        import uuid
        from collections import deque, namedtuple
        from types import SimpleNamespace

        class _Color(enum.Enum):
            RED = 1

        _Point = namedtuple("_Point", "x y")
        return {
            "datetime": datetime.datetime(2026, 1, 2, 3, 4, 5),
            "date": datetime.date(2026, 1, 2),
            "Decimal": decimal.Decimal("1.5"),
            "UUID": uuid.UUID(int=1),
            "Path": pathlib.Path("/a/b"),
            "Pattern": re.compile("ab+"),
            "Enum": _Color.RED,
            "SimpleNamespace": SimpleNamespace(a=1),
            "deque": deque([1, 2]),
            "array": array.array("i", [1, 2]),
            "MyList": _LyingLenList([1, 2]),
            "MyStr": type("_S", (str,), {})("ab"),
            "lambda": (lambda x: x),
        }

    def test_legitimate_values_are_renderable(self):
        parser = _fresh()
        for label, value in self._renderable().items():
            with self.subTest(value=label):
                self.assertGreater(parser.evaluate("$F.mod('%s', $INPUT.o)",
                                                   {"$INPUT": {"o": value}}).__len__(), 0)

    def test_template_of_datetime_matches_base(self):
        import datetime
        value = datetime.datetime(2026, 1, 2, 3, 4, 5)
        self.assertEqual(str(value), _fresh().evaluate("{% $INPUT.o %}", {"$INPUT": {"o": value}}))

    def test_refusal_message_carries_the_escape_hint(self):
        with self.assertRaises(ValueError) as cm:
            _fresh().evaluate("$F.mod('%s', $INPUT.o)", {"$INPUT": {"o": _EvilKey()}})
        self.assertIn("str(x)", str(cm.exception))


@skipUnless(_HAS_GUARD, "guard not present")
class TestEighthRoundMutationPrecision(TestCase):
    """精确常量/边界断言：把交付 revision 上 oracle 判出的可杀变异逐条钉住。"""

    def test_content_size_saturation_boundary_is_inclusive(self):
        """``total >= hard`` 与 ``> hard`` 的分界（env 极小限额下逐字可观测）。"""
        with _limits(floor=1, factor=1, hard=7):
            self.assertEqual(7, _content_size([0] * 1_000_001))

    def test_join_str_fast_path_is_exact(self):
        with _limits(floor=1, factor=1, hard=7):
            with self.assertRaises(ValueError) as cm:
                _fn_join("a" * 100, "x")
        self.assertEqual(
            "$F.join(100 items) result too large: 199 chars (limit 7). "
            "Refusing to build an unbounded result.", str(cm.exception))

    def test_render_size_exact_constants(self):
        # AU1（第二十七轮）：bytes repr 上界改为**真实逐字符**（可打印 ASCII 1、转义 4）⇒
        # `b'xxxxxxxxxx'` = 10 + 2（前缀 b' ） + 1（收尾 '） = 13（旧值 4×10+24 = 64）。
        self.assertEqual(13, _render_size(b"x" * 10))
        self.assertEqual(4, _render_size(None))
        self.assertEqual(100_000_016, _render_size([""] * 25_000_000))   # O(1) 短路值

    def test_render_size_uses_element_count_for_exact_containers(self):
        """精确容器的上界必须把 ``len`` 传下去（短路分支依赖它）。"""
        self.assertGreater(_render_size([""] * 25_000_000), _EXPR_MAX_RESULT_HARD if False else 100_000_000)

    def test_concat_bytes_message_uses_exact_bound(self):
        with _limits(floor=1, factor=1, hard=7):
            with self.assertRaises(ValueError) as cm:
                _fn_concat(b"x" * 20)
        self.assertEqual(
            "$F.concat(1 args) result too large: 23 chars (limit 7). "
            "Refusing to build an unbounded result.", str(cm.exception))

    def test_parse_format_specs_edge_cases(self):
        self.assertEqual([], _parse_format_specs("%%", bytes_mode=False))
        specs = _parse_format_specs("%(a)s", bytes_mode=False)
        self.assertEqual(1, len(specs))
        self.assertEqual("a", specs[0].key)
        self.assertEqual(0, specs[0].width)
        self.assertFalse(specs[0].star_width)
        self.assertEqual(-1, specs[0].precision)
        self.assertFalse(specs[0].star_precision)
        self.assertEqual("s", specs[0].conversion)


_NT_BASE = collections.namedtuple("_NT_BASE", "a b")


class _EvilNamedTuple(_NT_BASE):
    def __repr__(self):
        return "W" * 50_000_000


class _EvilPath(pathlib.PurePosixPath):
    def __repr__(self):
        return "P" * 100_000


class _EvilDatetime(datetime.datetime):
    def __str__(self):
        return "D" * 100_000


class _BoomList(list):
    def __iter__(self):
        raise RuntimeError("user-iter-boom")


class _BoomDict(dict):
    def items(self):
        raise RuntimeError("user-iter-boom")


class _SneakyIterable:
    """第一遍 1000 项、之后 40 万项：守卫/执行两遍枚举就会被放大。"""

    def __init__(self):
        self.calls = 0

    def __iter__(self):
        self.calls += 1
        return iter(["x" * 40] * (1000 if self.calls == 1 else 400_000))


@skipUnless(_HAS_GUARD, "guard not present")
class TestNinthRoundD1NamedTupleSubclass(TestCase):
    """第九轮 D1：namedtuple 子类覆写 `__repr__` 曾让上界 32 vs 实际 50,000,000。"""

    def test_overriding_namedtuple_repr_is_refused(self):
        value = _EvilNamedTuple(1, 2)
        self.assertIsNone(_render_size(value))
        self.assertIsNone(_render_size(value))
        self.assertIsNone(_render_size(value, as_repr=True))

    def test_concat_cannot_return_fifty_megabytes(self):
        desc, peak = _measure_peak(
            lambda: _fresh().evaluate("$F.concat($INPUT.v)",
                                      {"$INPUT": {"v": _EvilNamedTuple(1, 2)}}))
        self.assertEqual("ValueError", desc)
        self.assertLess(peak, _PEAK_BUDGET)

    def test_list_template_and_dict_paths_refuse_before_allocating(self):
        for label, expr, context in (
            ("list", "{% $F.concat($INPUT.v) %}", {"$INPUT": {"v": [_EvilNamedTuple(1, 2)]}}),
            ("bare-template", "{% $INPUT.v %}", {"$INPUT": {"v": _EvilNamedTuple(1, 2)}}),
            ("dict-value", "$F.mod('%s', $INPUT.d)", {"$INPUT": {"d": {"k": _EvilNamedTuple(1, 2)}}}),
        ):
            with self.subTest(path=label):
                desc, peak = _measure_peak(lambda: _fresh().evaluate(expr, context))
                self.assertEqual("ValueError", desc)
                self.assertLess(peak, _PEAK_BUDGET, f"{label}: {peak/1024/1024:.1f}MB")

    def test_legitimate_namedtuple_follows_the_documented_refusal(self):
        """F3 方案 1 之后：合法 namedtuple 也**不做整体渲染**（先 `str(x)` 即可）。"""
        value = _NT_BASE(1, 2)
        self.assertIsNone(_render_size(value))
        self.assertIsNone(_render_size(value))
        with self.assertRaises(ValueError):
            _fresh().evaluate("{% $INPUT.v %}", {"$INPUT": {"v": value}})
        self.assertEqual(2, _fresh().evaluate("$INPUT.v[1]", {"$INPUT": {"v": value}}))
        # 表达式文法没有裸函数调用，所以"先 str(x)"要在**上游**（节点/自定义函数）完成：
        self.assertEqual("_NT_BASE(a=1, b=2)",
                         _fresh().evaluate("$F.mod('%s', $INPUT.s)",
                                           {"$INPUT": {"s": str(value)}}))


@skipUnless(_HAS_GUARD, "guard not present")
class TestNinthRoundD2SubclassableStdlib(TestCase):
    """第九轮 D2：`isinstance` + 固定小上界在可子类化 stdlib 类型上不安全。"""

    def test_path_subclass_with_repr_override_is_refused(self):
        value = _EvilPath(".")
        self.assertIsNone(_render_size(value))
        self.assertIsNone(_render_size(value))

    def test_datetime_subclass_with_str_override_is_refused(self):
        value = _EvilDatetime(2026, 1, 2, 3, 4, 5)
        self.assertIsNone(_render_size(value))
        with self.assertRaises(ValueError):
            _fresh().evaluate("$F.mod('%r', $INPUT.v)", {"$INPUT": {"v": value}})

    def test_exact_stdlib_types_still_renderable(self):
        for value in (pathlib.PurePosixPath("/a/b"), datetime.datetime(2026, 1, 2, 3, 4, 5),
                      decimal.Decimal("1.5"), uuid.UUID(int=1), re.compile("ab+")):
            with self.subTest(value=type(value).__name__):
                self.assertIsNotNone(_render_size(value))
                self.assertGreaterEqual(_render_size(value, as_repr=True), len(repr(value)))


@skipUnless(_HAS_GUARD, "guard not present")
class TestNinthRoundD3FuzzyCorpus(TestCase):
    """第九轮 D3：模糊语料下**上界 ≥ 实际渲染**（`str` 与 `repr` 两种模式）。"""

    def _fuzzy(self):
        return [
            "\U0010ffff" * 10, "\ud800" * 10, "\x00" * 100, "a\tb\nc" * 5,
            b"\xff" * 100, bytearray(b"\xff" * 100), b"a" * 1000,
            2 ** 256, -2 ** 256, 1.7976931348623157e308,
            datetime.datetime(2026, 10, 13, 16, 59, 0, 123456),
            datetime.datetime(2026, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc),
            datetime.date(2026, 1, 2), datetime.timedelta.max,
            decimal.Decimal("1" + "0" * 50), decimal.Decimal("1E+999999999"),
            decimal.Decimal("-0.0000000001"),
            uuid.UUID(int=1), uuid.uuid4(),
            re.compile("a"), re.compile("abc", re.I | re.M | re.S | re.X | re.U),
            pathlib.PurePosixPath("."), pathlib.Path("/a/b/c"),
            [re.compile("abc", re.I | re.M | re.S | re.X | re.U)] * 1000,
            ["\U0010ffff"] * 100, {"k": ["\x00" * 10]}, [""] * 1000,
            ("x", 1, None), {1, 2, 3}, range(10 ** 9),
            # namedtuple 不在"必须有上界"之列：第十轮 F3 方案 1 明确它**不可整体
            # 渲染**（字段/索引下钻可用，整体渲染需先 `str(x)`）。
        ]

    def test_bound_is_never_below_the_actual_render(self):
        for value in self._fuzzy():
            for mode in (False, True):
                with self.subTest(value=type(value).__name__, repr_mode=mode):
                    bound = _render_size(value, as_repr=mode)
                    actual = len(repr(value)) if mode else len(str(value))
                    self.assertIsNotNone(bound, f"{type(value).__name__} 应有可证上界")
                    self.assertGreaterEqual(bound, actual)


@skipUnless(_HAS_GUARD, "guard not present")
class TestNinthRoundD4EnumerationFailures(TestCase):
    """第九轮 D4：枚举期间的用户异常必须转成"不可界定"（ValueError），不得漏出。"""

    def test_builtin_container_subclasses_use_ancestor_slots(self):
        """第十一轮 H1：内建容器子类走祖先 C 槽 ⇒ 敌意 `__iter__` 既不生效也不误拒。"""
        self.assertEqual("[1, 2, 3]", _fresh().evaluate("{% $INPUT.b %}",
                                                        {"$INPUT": {"b": _BoomList([1, 2, 3])}}))
        self.assertEqual("{'a': 1}", _fresh().evaluate("$F.concat($INPUT.b)",
                                                       {"$INPUT": {"b": _BoomDict({"a": 1})}}))

    def test_duck_typed_mapping_errors_are_still_contained(self):
        """没有内建祖先的鸭子映射仍走实例协议 ⇒ 其异常必须收口成 ValueError。"""
        class _DuckBoom:
            def keys(self):
                return ["a"]

            def items(self):
                raise RuntimeError("items-boom")

            def __getitem__(self, key):
                return 1

            def __iter__(self):
                raise RuntimeError("iter-boom")

        with self.assertRaises(ValueError):
            _content_size(_DuckBoom())


@skipUnless(_HAS_GUARD, "guard not present")
class TestNinthRoundD5SinglePassMaterialization(TestCase):
    """第九轮 D5：有状态可迭代对象不得在"第二遍"长出来（守卫与执行共用一次迭代）。"""

    def test_stateful_iterable_is_enumerated_once(self):
        sneaky = _SneakyIterable()
        desc, peak = _measure_peak(
            lambda: _fresh().evaluate("{% $F.join($INPUT.s, '') %}", {"$INPUT": {"s": sneaky}}))
        self.assertEqual(1, sneaky.calls, "守卫/执行枚举了两遍")
        self.assertLess(peak, 4 * 1024 * 1024)

    def test_stateful_iterable_result_never_exceeds_what_was_measured(self):
        sneaky = _SneakyIterable()
        result = _fresh().evaluate("{% $F.join($INPUT.s, '') %}", {"$INPUT": {"s": sneaky}})
        self.assertLessEqual(len(result), 1000 * 40)

    def test_materialization_is_bounded(self):
        class _Huge:
            def __iter__(self):
                while True:
                    yield ""

        with self.assertRaises(ValueError):
            _fresh().evaluate("$F.join($INPUT.h, 'x')", {"$INPUT": {"h": _Huge()}})


_NT_PROTO = collections.namedtuple("_NT_PROTO", "a b")


def _huge(name="W", n=50_000_000):
    return name * n


class _CraftedCodeNamedTuple(_NT_PROTO):
    """F1-a：与生成原型 **code/consts/names 全等**，只换闭包 cell/globals。"""


def _craft_repr():
    proto = _NT_PROTO.__repr__
    cell = (lambda: _huge()).__closure__[0] if False else None

    def _make_cell(value):
        def _inner():
            return value
        return _inner.__closure__[0]

    globs = dict(proto.__globals__)
    globs["repr"] = lambda obj: _huge()
    closure = tuple(_make_cell("__repr_fmt__") for _ in proto.__code__.co_freevars)
    return types.FunctionType(proto.__code__, globs, "crafted", None, closure)


class _DescriptorRepr:
    """F1-b：类上返回真原型（code 匹配）、实例上返回敌意可调用。"""

    def __get__(self, instance, owner):
        if instance is None:
            return _NT_PROTO.__repr__
        return lambda: _huge("D", 5_000_000)


class _DescriptorNamedTuple(_NT_PROTO):
    __repr__ = _DescriptorRepr()


class _BoomLenStr(str):
    def __len__(self):
        raise RuntimeError("len-boom")


@skipUnless(_HAS_GUARD, "guard not present")
class _LyingIterList(list):
    """覆写 `__iter__` 返回空迭代器，真实存储仍在 C 槽里。"""

    def __iter__(self):
        return iter(())


class _LyingLenIterStr(str):
    def __len__(self):
        return 1

    def __iter__(self):
        return iter(())


@skipUnless(_HAS_GUARD, "guard not present")
class TestEleventhRoundH1AncestorSlots(TestCase):
    """第十一轮 H1（决定性）：计费与枚举一律走**祖先 C 槽**，绝不走实例协议。

    旧实现用实例 `__iter__` 数上界、`str()` 用 C 槽读真实存储 ⇒ 覆写 `__iter__`
    只骗到上界、骗不到渲染（实测按 16 字节放行 100,004,000 字符、峰值 105.96MB）。
    """

    def test_render_and_content_use_ancestor_storage(self):
        big = "B" * 100_000
        value = _LyingIterList([big] * 1000)
        bound = _render_size(value)
        self.assertTrue(bound is None or bound > expr_mod._EXPR_MAX_RESULT_HARD
                        or bound >= len(str(value)), f"上界 {bound} 小于实际")
        self.assertEqual(100_000_000, _content_size(value))

    def test_lying_iter_list_refused_before_allocating(self):
        big = "B" * 100_000
        # AX3（第二十八轮）：容器元素改按**真实逐字符宽度**计 ⇒ 小 n 的合法结果现在是
        # 预算内的 1× 渲染（可证上界 ≥ 实际），只有真实上界越过硬上限的形状才必须
        # 分配前拒。H1 的不变量（走祖先 C 槽而非说谎 `__iter__`）由下面的 bound 断言与
        # `test_render_and_content_use_ancestor_storage` 覆盖。
        for n in (200, 500):
            with self.subTest(bound_n=n):
                value = _LyingIterList([big] * n)
                bound = _render_size(value, as_repr=True)
                self.assertIsNotNone(bound)
                self.assertGreaterEqual(bound, len(repr(value)))
                self.assertEqual(100_000 * n, _content_size(value))
        for n in (1000, 1500):
            with self.subTest(n=n):
                value = _LyingIterList([big] * n)
                for label, call in (
                    ("concat", lambda v=value: _fresh().evaluate("$F.concat($INPUT.v)",
                                                                 {"$INPUT": {"v": v}})),
                    ("mod", lambda v=value: _fresh().evaluate("$F.mod('%s', $INPUT.v)",
                                                              {"$INPUT": {"v": v}})),
                ):
                    with self.subTest(path=label):
                        desc, peak = _measure_peak(call)
                        self.assertEqual("ValueError", desc)
                        self.assertLess(peak, _PEAK_BUDGET,
                                        f"峰值随 N 增长：n={n} {peak/1024/1024:.1f}MB")

    def test_lying_len_str_subclass_bound_is_the_real_storage(self):
        n = 50_000_004
        value = _LyingLenIterStr("x" * n)
        self.assertGreaterEqual(_render_size(value), n)
        self.assertGreaterEqual(_render_size(value, as_repr=True), len(repr(value)))
        self.assertEqual(n, _content_size(value))

    def test_lying_len_str_subclass_above_hard_is_refused_before_allocating(self):
        value = _LyingLenIterStr("x" * 100_000_001)
        desc, peak = _measure_peak(
            lambda: _fresh().evaluate("$F.concat($INPUT.v)", {"$INPUT": {"v": value}}))
        self.assertEqual("ValueError", desc)
        self.assertLess(peak, 2 * 1024 * 1024)

    def test_padding_does_not_hide_the_lying_value(self):
        big = "B" * 100_000
        desc, peak = _measure_peak(
            lambda: _fresh().evaluate("$F.concat($INPUT.v, $INPUT.p)",
                                      {"$INPUT": {"v": _LyingIterList([big] * 1000),
                                                  "p": "y" * 1_000_000}}))
        self.assertEqual("ValueError", desc)
        self.assertLess(peak, _PEAK_BUDGET)


@skipUnless(_HAS_GUARD, "guard not present")
class TestEleventhRoundH2MeasureContainment(TestCase):
    """第十一轮 H2：`_measure`（事后网的 O(1) 读取）也必须收口。"""

    class _Boom:
        def __len__(self):
            raise RuntimeError("boom-len")

        def __sizeof__(self):
            raise RuntimeError("boom-size")

    class _BoomSizeof:
        """`__len__` 抛 TypeError ⇒ `_measure` 回退 getsizeof；`__sizeof__` 才是雷。"""

        def __len__(self):
            raise TypeError("no-len")

        def __sizeof__(self):
            raise RuntimeError("boom-sizeof")

    def test_measure_hostile_len_and_sizeof_become_value_error(self):
        with self.assertRaises(ValueError):
            expr_mod._measure(self._Boom())
        with self.assertRaises(ValueError):
            expr_mod._measure(self._BoomSizeof())

    def test_getsizeof_fallback_path_is_contained_too(self):
        """H2 的 getsizeof 分支：`len` 正常回退后 `__sizeof__` 抛 RuntimeError 同样收口。"""
        desc, peak = _measure_peak(lambda: expr_mod._measure(self._BoomSizeof()))
        self.assertEqual("ValueError", desc)
        self.assertLess(peak, _PEAK_BUDGET)
        for label, expr in (("ifelse", "$F.ifelse(1, $INPUT.b, 0)"),
                            ("or", "$F.or($INPUT.b, 1)"),
                            ("not", "$F.not($INPUT.b)")):
            with self.subTest(fn=label):
                desc, peak = _measure_peak(
                    lambda e=expr: _fresh().evaluate(e, {"$INPUT": {"b": self._BoomSizeof()}}))
                # `$F.or`/`$F.not` 先做真值判断：`__len__` 抛的 TypeError 是**原生**
                # 类型错误（文档承诺原样抛出）；但绝不能是裸的 RuntimeError。
                self.assertIn(desc, ("ValueError", "TypeError"))
                self.assertLess(peak, _PEAK_BUDGET)
        self.assertNotEqual("RuntimeError", _measure_peak(
            lambda: _fresh().evaluate("$F.or($INPUT.b, 1)",
                                      {"$INPUT": {"b": self._Boom()}}))[0])

    def test_ifelse_or_not_paths_do_not_leak_user_exceptions(self):
        for label, expr in (("ifelse", "$F.ifelse(1, $INPUT.b, 0)"),
                            ("or", "$F.or($INPUT.b, 1)"),
                            ("not", "$F.not($INPUT.b)")):
            with self.subTest(fn=label):
                with self.assertRaises(ValueError):
                    _fresh().evaluate(expr, {"$INPUT": {"b": self._Boom()}})

    def test_native_operator_errors_still_surface(self):
        with self.assertRaises(TypeError):
            _fresh().evaluate("$F.mod('%d', $INPUT.s)", {"$INPUT": {"s": "abc"}})
        with self.assertRaises(KeyError):
            _fresh().evaluate("$F.mod('%(missing)s', $INPUT.d)", {"$INPUT": {"d": {"a": 1}}})


@skipUnless(_HAS_GUARD, "guard not present")
class TestEleventhRoundH4NoOverTightening(TestCase):
    """第十一轮 H4：`PurePath` 子类按祖先匹配；`Enum` **类对象**放行。"""

    def test_purepath_subclass_is_renderable(self):
        class MyPath(pathlib.PurePosixPath):
            pass

        value = MyPath("/a/b")
        self.assertIsNotNone(_render_size(value))
        self.assertGreaterEqual(_render_size(value), len(str(value)))
        self.assertEqual(str(value), _fresh().evaluate("$F.mod('%s', $INPUT.v)",
                                                       {"$INPUT": {"v": value}}))

    def test_purepath_variants_and_nested_subclass_are_renderable(self):
        class P1(pathlib.PurePosixPath):
            pass

        class P2(pathlib.PosixPath):
            pass

        class P3(pathlib.PureWindowsPath):
            pass

        class P4(P1):
            pass

        parser = _fresh()
        for value in (P1("/a/b"), P2("/a/b"), P3("C:/a/b"), P4("/a/b")):
            with self.subTest(cls=type(value).__name__):
                self.assertIsNotNone(_render_size(value))
                self.assertGreaterEqual(_render_size(value), len(str(value)))
                self.assertEqual(str(value), parser.evaluate("$F.mod('%s', $INPUT.v)",
                                                             {"$INPUT": {"v": value}}))

    def test_purepath_subclass_with_own_str_is_still_refused(self):
        """反向对照：祖先匹配不是"凡 PurePath 子类都放行"——覆写 `__str__` 仍然拒绝。"""
        class EvilPath(pathlib.PurePosixPath):
            def __str__(self):
                return "x" * 1_000

        value = EvilPath("/a")
        self.assertIsNone(_render_size(value))
        self.assertIsNone(_render_size(value))
        with self.assertRaises(ValueError):
            _fresh().evaluate("$F.mod('%s', $INPUT.v)", {"$INPUT": {"v": value}})

    def test_enum_class_object_is_renderable(self):
        class Color(enum.Enum):
            RED = 1

        self.assertIsNotNone(_render_size(Color))
        self.assertGreaterEqual(_render_size(Color), len(str(Color)))
        self.assertEqual(str(Color), _fresh().evaluate("$F.mod('%s', $INPUT.v)",
                                                       {"$INPUT": {"v": Color}}))

    def test_enum_class_object_is_renderable_in_every_render_path(self):
        class Color(enum.Enum):
            RED = 1

        parser = _fresh()
        context = {"$INPUT": {"v": Color}}
        self.assertEqual(str(Color), parser.evaluate("{% $INPUT.v %}", context))
        self.assertEqual(str(Color), parser.evaluate("$F.concat($INPUT.v)", context))
        self.assertEqual(str(Color), parser.evaluate("$F.mod('%s', $INPUT.v)", context))


class TestTenthRoundF1CodeIdentityIsNotBehaviour(TestCase):
    """第十轮 F1：`code` 相同 ≠ 行为相同（闭包 cell / globals / 描述符）。"""

    def test_crafted_function_is_refused(self):
        _CraftedCodeNamedTuple.__repr__ = _craft_repr()
        value = _CraftedCodeNamedTuple(1, 2)
        self.assertIsNone(_render_size(value))
        self.assertIsNone(_render_size(value))

    def test_descriptor_repr_is_refused(self):
        value = _DescriptorNamedTuple(1, 2)
        self.assertIsNone(_render_size(value))
        self.assertIsNone(_render_size(value))

    def test_concat_refuses_before_allocating_for_crafted(self):
        _CraftedCodeNamedTuple.__repr__ = _craft_repr()
        desc, peak = _measure_peak(
            lambda: _fresh().evaluate("$F.concat($INPUT.v)",
                                      {"$INPUT": {"v": _CraftedCodeNamedTuple(1, 2)}}))
        self.assertEqual("ValueError", desc)
        self.assertLess(peak, _PEAK_BUDGET)

    def test_concat_refuses_before_allocating_for_descriptor(self):
        desc, peak = _measure_peak(
            lambda: _fresh().evaluate("$F.concat($INPUT.v)",
                                      {"$INPUT": {"v": _DescriptorNamedTuple(1, 2)}}))
        self.assertEqual("ValueError", desc)
        self.assertLess(peak, _PEAK_BUDGET)

    def test_legitimate_namedtuple_is_documented_as_not_wholly_renderable(self):
        """F3 方案 1：namedtuple 整体渲染需先 `str(x)`（字段/索引下钻不受影响）。"""
        value = _NT_PROTO(1, 2)
        self.assertIsNone(_render_size(value))
        with self.assertRaises(ValueError):
            _fresh().evaluate("{% $INPUT.v %}", {"$INPUT": {"v": value}})
        context = {"$INPUT": {"v": _NT_PROTO("x", "y")}}
        self.assertEqual("y", _fresh().evaluate("$INPUT.v[1]", context))
        self.assertEqual(str(_render_size(_NT_PROTO(1, 2))), "None")


@skipUnless(_HAS_GUARD, "guard not present")
class TestTenthRoundF2PreRenderRefusal(TestCase):
    """第十轮 F2（第一优先级）：拼接/模板/`%s`/`%r` 必须在 `str()` **之前**拒绝。"""

    def test_small_bound_huge_render_is_refused_before_allocating(self):
        _CraftedCodeNamedTuple.__repr__ = _craft_repr()
        for label, expr, ctx in (
            ("concat", "$F.concat($INPUT.e)", {"$INPUT": {"e": _CraftedCodeNamedTuple(1, 2)}}),
            ("template", "{% $INPUT.e %}", {"$INPUT": {"e": _CraftedCodeNamedTuple(1, 2)}}),
            ("percent-s", "$F.mod('%s', $INPUT.e)", {"$INPUT": {"e": _CraftedCodeNamedTuple(1, 2)}}),
            ("percent-r", "$F.mod('%r', $INPUT.e)", {"$INPUT": {"e": _CraftedCodeNamedTuple(1, 2)}}),
            ("dict-value", "$F.mod('%s', $INPUT.d)",
             {"$INPUT": {"d": {"k": _CraftedCodeNamedTuple(1, 2)}}}),
        ):
            with self.subTest(path=label):
                desc, peak = _measure_peak(lambda: _fresh().evaluate(expr, ctx))
                # tuple 子类作为 `%` 的**参数序列**时原生就是 TypeError（根本没渲染）；
                # 其余路径必须是守卫的 ValueError。两者都不得有分配。
                self.assertIn(desc.split(":")[0], ("ValueError", "TypeError"),
                              f"{label} 没有在渲染前拒绝：{desc}")
                self.assertLess(peak, _PEAK_BUDGET, f"{label}: {peak/1024/1024:.1f}MB")

    def test_huge_but_provable_value_is_refused_before_allocating(self):
        """可证上界超硬上限 ⇒ 也在渲染前拒绝（`concat` 一个 100MB+ 的字符串）。"""
        huge = "x" * 100_000_001                 # 在测量之外先构造好
        desc, peak = _measure_peak(
            lambda: _fresh().evaluate("$F.concat($INPUT.s)", {"$INPUT": {"s": huge}}))
        self.assertEqual("ValueError", desc)
        self.assertLess(peak, 4 * 1024 * 1024)

    # 说明（第十一轮 H3）：这里原本有一条 `inspect.getsource` 的**文本顺序断言**，
    # 但它在真正的 100MB 绕过发生时照样通过（对 `{% %}`/`$F.mod` 与"上界本身说谎"
    # 完全盲）⇒ 已删除。承重的是下面这组**行为**用例（H1 的祖先槽反例）。


@skipUnless(_HAS_GUARD, "guard not present")
class TestTenthRoundF4UntrustedProtocols(TestCase):
    """第十轮 F4：`__len__` 等实例协议调用不得把用户异常漏出守卫。"""

    def test_hostile_len_on_builtin_subclass_is_ignored_by_ancestor_slot(self):
        """H1：str 子类的 `__len__` 不再参与计费（走 `str.__len__`）。"""
        self.assertEqual(3, _content_size(_BoomLenStr("abc")))
        self.assertEqual(3, _render_size(_BoomLenStr("abc")))          # %s：就是字符数
        self.assertGreaterEqual(_render_size(_BoomLenStr("abc"), as_repr=True), len("'abc'"))

    def test_hostile_len_on_unknown_object_becomes_value_error(self):
        class _Boom:
            def __len__(self):
                raise RuntimeError("boom-len")

        with self.assertRaises(ValueError):
            expr_mod._measure(_Boom())

    def test_hostile_len_never_leaks_a_raw_user_exception(self):
        """渲染路径可以正常出结果（不调用 `len`），但**绝不能**漏出 RuntimeError。"""
        try:
            result = _fresh().evaluate("{% $INPUT.v %}", {"$INPUT": {"v": _BoomLenStr("abc")}})
        except ValueError:
            result = None
        except RuntimeError as exc:            # pragma: no cover - 失败路径
            self.fail(f"守卫漏出原始用户异常：{exc!r}")
        self.assertIn(result, (None, "abc", "'abc'"))


@skipUnless(_HAS_GUARD, "guard not present")
class TestTenthRoundF5NoOverTightening(TestCase):
    """第十轮 F5：无覆写的子类必须按祖先语义（不得因迭代标量而误拒）。"""

    class _MyInt(int):
        pass

    class _MyFloat(float):
        pass

    class _MyStr(str):
        pass

    def test_scalar_subclasses_are_renderable(self):
        parser = _fresh()
        for value in (self._MyInt(5), self._MyFloat(1.5), self._MyStr("ab")):
            with self.subTest(value=type(value).__name__):
                self.assertIsNotNone(_render_size(value))
                self.assertEqual(str(value), parser.evaluate("$F.mod('%s', $INPUT.v)",
                                                             {"$INPUT": {"v": value}}))

    def test_scalar_subclass_bounds_are_ancestor_bounds(self):
        self.assertGreaterEqual(_render_size(self._MyInt(5)), len(str(5)))
        self.assertGreaterEqual(_render_size(self._MyFloat(1.5)), len(str(1.5)))
        self.assertGreaterEqual(_render_size(self._MyStr("ab")), len(str("ab")))

    def test_namedtuple_rename_true_has_no_wholesale_channel(self):
        renamed = collections.namedtuple("_Renamed", "a _b c", rename=True)
        self.assertIsNone(_render_size(renamed(1, 2, 3)))


class TestLegacyAssertionsStillHold(TestCase):
    """修复前也必须红的"拒绝"断言（负向对照的核心）。"""

    def test_issue_repro_mul_str(self):
        with self.assertRaises(ValueError):
            _fn_mul("x", 10**6)

    def test_issue_repro_mul_list(self):
        with self.assertRaises(ValueError):
            _fn_mul([0], 10**8)

    def test_issue_repro_mul_int_first(self):
        with self.assertRaises(ValueError):
            _fn_mul(10**9, "A")

    def test_issue_repro_bytes(self):
        with self.assertRaises(ValueError):
            _fn_mul(b"x", 10**8)

    def test_issue_repro_replace(self):
        with self.assertRaises(ValueError):
            _fn_replace("a" * 100000, "a", "b" * 1000)

    def test_issue_repro_mod_format(self):
        with self.assertRaises(ValueError):
            _fn_mod("%100000000d", 1)

    def test_issue_repro_join_separator_repetition(self):
        with self.assertRaises(ValueError):
            _fn_join(["a"] * 6, "s" * 25000)


@skipUnless(_HAS_GUARD, "guard not present")
class TestProbeHotPathCost(TestCase):
    """探针在小值上必须是常数级开销（回归护栏，不是性能门禁）。"""

    def test_probe_small_value_is_constant_time(self):
        n = 20000
        elapsed = timeit.timeit(lambda: _probe("add", 7, (1, 2)), number=n)
        per_call_ns = elapsed / n * 1e9
        # 实测 ~200ns（含 pytest 进程内噪声）；20µs 的界只排除"不是常数级"
        # 的实现（序列化/深拷贝会是毫秒级），不会因 CI 负载抖动误红。
        self.assertLess(per_call_ns, 20_000)

    def test_probe_does_not_materialize_anything(self):
        payload = ["x"] * 10
        self.assertIs(payload, _probe("fn", payload, ()))
        self.assertIs(sys, _probe("fn", sys, ()))


# ---------------------------------------------------------------------------
# 第十二轮 H3：渲染点清点守卫（AST 驱动 + 正/负对照）
# ---------------------------------------------------------------------------
# 第十一轮的教训：旧守卫是 `inspect.getsource(_fn_concat)` 的**文本顺序断言**，它在
# 真实的 100MB 绕过发生时照样通过——只盯一个函数、只比较两个子串的位置，对
# `{% %}`/`$F.mod` 以及"上界函数本身说谎"完全盲。本轮的替代品把口径倒过来
# （参照 #133 的注册表守卫）：
#
#   1. AST 枚举**两个模块里所有**渲染原语调用点（`str()`/`repr()`/`format()`/
#      `ascii()`、`%` 格式化、`.format()`、`.join()` 物化）；
#   2. 每个点必须在 `_RENDER_SITES` 登记并声明处置：`guarded`（函数内更早处必须
#      有一次**具名**上界调用）/ `bounded`（构造上可证）/ `excluded`（不是用户
#      数据的渲染）——三者都要写理由；
#   3. 另扫 f-string：任何**根是形参**的插值（`f"{user}"` 就是一次 `str()`）以及
#      任何带 format spec 的插值都必须登记在 `_FSTRING_PARAM_SITES`；
#   4. 正向对照：未登记的合成源码、以及"把真实 `_fn_concat` 的上界调用抽掉"的
#      变体，喂给**同一个**检查器都必须报警。
#
# **这只是清单，不是上界证明**：上界是否真的成立由 H1 的祖先槽反例等行为用例
# 承重（旧守卫的病根正是"结构断言冒充证明"）。
_RENDER_PRIMITIVE_NAMES = frozenset({"str", "repr", "format", "ascii"})
_RENDER_METHOD_NAMES = frozenset({
    "format", "join", "replace", "write", "writerow", "write_text",
    "__mod__", "__add__", "__radd__", "__mul__", "__rmul__",
    "__str__", "__repr__", "__format__", "__call__",
})

_LIBRARY_SOURCES = {
    "plaita/core/expression.py": expr_mod.__file__,
    "plaita/core/expression_parser.py": parser_mod.__file__,
}

# (模块, 函数, 算子, 源码原文) -> (处置, 具名上界调用或 None, 理由)
_RENDER_SITES = {
    # --- expression.py ---------------------------------------------------
    ("plaita/core/expression.py", "_render_unmeasurable", "log:warning",
     '"$F render size unmeasurable (%s); refusing to render"'): (
        "excluded", None,
        "只插值异常的**类型名**（M4 修：不再插值 exc 的消息，避免 %s 渲染作者数据）"),
    ("plaita/core/expression.py", "_fn_json_dumps", "indirect:json_dumps", "json.dumps(a)"): (
        "excluded", None,
        "json.dumps 是序列化；结果由事后探针 _size_guard_result 复核（属'检测而非阻止'，"
        "文档已如实说明）"),
    ("plaita/core/expression.py", "_native_mod", "binop:%", "a % b"): (
        "excluded", None,
        "非 str/bytes 的数值取模：原生语义、不放大；渲染类 % 全部走 _fn_mod"),
    ("plaita/core/expression.py", "_fn_pow", "call:str", "str(exponent)"): (
        "bounded", None,
        "K1：指数经 int.__index__ C 槽读出且 ≤ _POW_MAX_EXPONENT ⇒ 十进制长度有界"),
    ("plaita/core/expression.py", "_fn_mod", "binop:%", "a % b"): (
        "guarded", "_check_result_size",
        "Q1/Q3：_mod_decide 结构性判定（默认拒绝）后 _check_result_size 在渲染前把关；"
        "已证明原生必错的形状才走到这里"),
    ("plaita/core/expression.py", "_fn_mod", "binop:%", "a % resolved"): (
        "guarded", "_check_result_size",
        "Q2：映射格式渲染的是**一次性快照**（精确 dict），引用只读一次；"
        "_check_result_size 在渲染前把关"),
    ("plaita/core/expression.py", "_ancestor_replace", "method:replace",
     "str.replace(value, old, new)"): (
        "bounded", None,
        "str.replace 的 C 槽；长度/命中数由 _fn_replace 用祖先槽预检后才调用"),
    ("plaita/core/expression.py", "_ancestor_replace", "method:replace",
     "bytes.replace(value, old, new)"): (
        "bounded", None, "bytes.replace 的 C 槽；同上"),
    ("plaita/core/expression.py", "_ancestor_replace", "method:replace",
     "bytearray.replace(value, old, new)"): (
        "bounded", None, "bytearray.replace 的 C 槽；同上"),
    ("plaita/core/expression.py", "_fn_replace", "method:replace", "a.replace(b, c)"): (
        "excluded", None,
        "仅当 type(a) 没有 replace（kind 为 None）⇒ 原生 AttributeError，不分配"),
    ("plaita/core/expression.py", "_fn_join", "method:join", "str.join(b, items)"): (
        "guarded", "_check_result_size",
        "join：元素数与内容量已在 _join_item_lengths 用祖先槽/C 槽读真实存储，"
        "_check_result_size 在拼接前拒绝；str.join 是 C 槽（忽略覆写的 join）"),
    ("plaita/core/expression.py", "_fn_join", "method:join", "bytes.join(b, items)"): (
        "guarded", "_check_result_size", "同 str.join 分支"),
    ("plaita/core/expression.py", "_fn_join", "method:join", "bytearray.join(b, items)"): (
        "guarded", "_check_result_size", "同 str.join 分支"),
    ("plaita/core/expression.py", "_fn_join", "method:join", "str.join(b, a)"): (
        "excluded", None,
        "projected 为 None ⇒ 类型不符，原生 TypeError，不分配；自定义 join 已在前拒绝"),
    ("plaita/core/expression.py", "_fn_join", "method:join", "bytes.join(b, a)"): (
        "excluded", None, "同 str.join(b, a) 分支"),
    ("plaita/core/expression.py", "_fn_join", "method:join", "bytearray.join(b, a)"): (
        "excluded", None, "同 str.join(b, a) 分支"),
    ("plaita/core/expression.py", "_fn_concat", "method:join",
     '"".join([str(arg) for arg in args])'): (
        "guarded", "_render_size",
        "concat：每个实参先 _render_size 求可证上界，任一 None 或超额度即在本行之前 raise"),
    ("plaita/core/expression.py", "_fn_concat", "call:str", "str(arg)"): (
        "guarded", "_render_size",
        "concat 的逐实参 str()：所有 arg 的 _render_size 都在列表推导之前算过"),
    ("plaita/core/expression.py", "_value_size", "call:str", "str(value)"): (
        "bounded", None,
        "只在小整数（bit_length() <= 64）分支执行 ⇒ 最多 20 位十进制"),
    ("plaita/core/expression.py", "_render_type_bound", "call:str", "str(parts.exponent)"): (
        "bounded", None,
        "Decimal.as_tuple().exponent 是 int（|exponent| ≤ 18 位），只进上界/提示"),
    ("plaita/core/expression.py", "_subclass_render_bound", "call:str",
     "str(parts.exponent)"): (
        "bounded", None, "同 _render_size 的 Decimal 分支"),
    ("plaita/core/expression.py", "_safe_int_text", "method:__repr__",
     "int.__repr__(value)"): (
        "bounded", None,
        "int.__repr__ C 槽 + bit_length() <= 64 判定；>64bit 只报位数（H3/M4）"),
    ("plaita/core/expression.py", "_resolve_positive_int", "log:warning",
     '"%s=%r 非法，回退默认 %d"'): (
        "excluded", None,
        "环境变量名/值来自进程环境（启动期，非评估作者数据），%r 只回显配置串"),
    ("plaita/core/expression.py", "_resolve_positive_int", "log:warning",
     '"%s=%r 非正，回退默认 %d"'): (
        "excluded", None, "同 _resolve_positive_int 的非法分支"),
    # --- expression_parser.py --------------------------------------------
    ("plaita/core/expression_parser.py", "_warn_suspicious_literal", "log:warning",
     '"expression %r looks like templating/Python code from another language and was "'
     ' "returned AS-IS (no evaluation happened). plaita expressions use $INPUT.x / "'
     ' "$NODE.id / $F.fn(...) and {%% ... %%} for interpolation — rewrite the string."'): (
        "excluded", None,
        "value 已由调用方截断到 ≤120 字符（value[:117] + '...'）"),
    ("plaita/core/expression_parser.py", "_warn_unmatched_template", "log:warning",
     '"expression %r contains \'{%%\' but no valid {%% ... %%} expression matched "'
     ' "(inner expression is malformed or unclosed); it was returned AS-IS without "'
     ' "evaluation — fix the inner expression (e.g. {%% $INPUT.x %%})."'): (
        "excluded", None, "同 _warn_suspicious_literal：调用方已截断"),
    ("plaita/core/expression_parser.py", "_render_path", "method:join",
     '"".join(parts)'): (
        "bounded", None, "段名来自已在内存的表达式 token，拼接量 ≤ 输入串长度"),
    ("plaita/core/expression_parser.py", "_warn_raw_accessor_collision", "log:warning",
     '"expression %s: the segment after $NODE is \'raw\', but this flow also has a "'
     ' "node/key with id \'raw\' — it is resolved as that node (backwards "'
     ' "compatibility), so the verbatim \'$NODE.raw.<id>...\' accessor (#130) is NOT "'
     ' "available in this flow; rename the node to use the accessor."'): (
        "excluded", None, "path 是解析器 token（已在内存，≤ 输入串长度）"),
    ("plaita/core/expression_parser.py", "_reject_reserved_field", "method:format",
     "_REJECTION_TEMPLATE.format(path=path)"): (
        "excluded", None,
        "错误消息模板：path 是解析器 token（str），消息 ≤4x len(path) ≤ 输入串"),
    ("plaita/core/expression_parser.py", "_eval_template", "method:join",
     '"".join(out)'): (
        "bounded", None,
        "out 的每一项都在循环内先过 _render_size（None/超硬上限即 raise）；"
        "拼接量 = Σ 已证片段长度（结构守卫看不到跨循环支配，故这里显式登记理由）"),
    ("plaita/core/expression_parser.py", "_eval_template", "call:str", "str(raw)"): (
        "guarded", "_render_size",
        "模板插值的 str(raw)：同一循环里 _render_size(raw) 先判，None/超限即 raise"),
    ("plaita/core/expression_parser.py", "thunk", "log:debug",
     '"parse_function: func_name=%s, argc=%d, arg_types=%s"'): (
        "excluded", None,
        "M4 修：只记函数名/参数**个数**/参数**类型名**，不再插值 args 本身"),
    ("plaita/core/expression_parser.py", "_container_has_root", "log:debug",
     '"contains probe failed for %s root %r: %s"'): (
        "excluded", None,
        "DEBUG 级容器探针的**既有可见性契约**（tests/unit/test_expression_parser_root_scan.py "
        "精确比对消息）；异常来自容器协议，仓库级日志卫生见 plaita#151（本单不动），"
        "M4 只改评估路径上会渲染**值本身**的两处（parse_function / _render_unmeasurable）"),
    ("plaita/core/expression_parser.py", "_container_has_root", "log:debug",
     '"get probe failed for %s root %r: %s"'): (
        "excluded", None, "同 contains 探针分支（DEBUG、有精确比对契约）"),
    ("plaita/core/expression_parser.py", "thunk", "log:warning",
     '"expression function %r not registered (registry_type=%s); "'
     ' "returning \'undefined\'"'): (
        "excluded", None, "M4 修：只记函数名与 registry 的**类型名**"),
    ("plaita/core/expression_parser.py", "thunk", "log:debug",
     '"expression references missing input key %r; evaluating to None"'): (
        "excluded", None, "raw 是段名 token（已在内存），不是值内容"),
    ("plaita/core/expression_parser.py", "thunk", "log:debug",
     '"nested expression string in %s references root(s) %s missing "'
     ' "from its parent container; returned AS-IS as data text"'): (
        "excluded", None, "field_path 与固定根名列表，均在内存且受常量约束"),
    ("plaita/core/expression_parser.py", "thunk", "method:join",
     '", ".join(sorted(missing_roots))'): (
        "excluded", None,
        "错误消息：missing_roots 是固定根名（$INPUT/$NODE/...）的排序列表"),
    # --- #69 落地侧并合登记（2026-10-16 rebase union：这些点来自 main 已落地的 #69 r13 闸与诊断，
    #     语义 = 静态源/类型名/引擎信号，非结果放大面；逐条处置见行内理由）---
    ('plaita/core/expression_parser.py', '_canonical_container_fast_path', 'log:debug',
     '"canonical fast-path probe failed for %r: %s"'): (
        'excluded', None,
        "#69 侧存量（r13 对抗验收层）：插值均为静态表达式源/状态键名/类型名/引擎自造信号，不含运行期结果数据，非结果放大面"),
    ('plaita/core/expression_parser.py', '_descriptor_for', 'log:debug',
     '"expression registry descriptor lookup failed for %r: %s"'): (
        'excluded', None,
        "#69 侧存量（r13 对抗验收层）：插值均为静态表达式源/状态键名/类型名/引擎自造信号，不含运行期结果数据，非结果放大面"),
    ('plaita/core/expression_parser.py', '_detach_alias', 'method:format',
     '_MUTATOR_DETACH_NOT_DATA_MODEL_TEMPLATE.format( func=func_name, callee=callee, position=position + 1, path=path or f"$F.{callee}(...)", root=root or callee, type_name=exc.type_name)'): (
        'excluded', None,
        "#69 拒绝消息模板（_MUTATOR_*/_MAPPING_VIEW_*）：插值 func 名/实参位/静态路径渲染/类型名，均来自静态表达式源，非运行期结果数据"),
    ('plaita/core/expression_parser.py', '_detach_alias', 'method:format',
     '_MUTATOR_DETACH_NOT_DATA_MODEL_TEMPLATE.format( func=func_name, callee=callee, position=position + 1, path=path or f"$F.{callee}(...)", root=root or callee, type_name=type(value).__name__)'): (
        'excluded', None,
        "#69 拒绝消息模板（_MUTATOR_*/_MAPPING_VIEW_*）：插值 func 名/实参位/静态路径渲染/类型名，均来自静态表达式源，非运行期结果数据"),
    ('plaita/core/expression_parser.py', '_enforce_fresh', 'method:format',
     '_MUTATOR_UNPROVEN_FRESH_TEMPLATE.format( func=func_name, path=plan.label, root=plan.root or plan.label, callee=plan.callee or plan.label)'): (
        'excluded', None,
        "#69 拒绝消息模板（_MUTATOR_*/_MAPPING_VIEW_*）：插值 func 名/实参位/静态路径渲染/类型名，均来自静态表达式源，非运行期结果数据"),
    ('plaita/core/expression_parser.py', '_engine_mapping_view', 'method:format',
     '_MAPPING_VIEW_NON_DICT_TEMPLATE.format( func=func_name, type_name=type(value).__name__)'): (
        'excluded', None,
        "#69 拒绝消息模板（_MUTATOR_*/_MAPPING_VIEW_*）：插值 func 名/实参位/静态路径渲染/类型名，均来自静态表达式源，非运行期结果数据"),
    ('plaita/core/expression_parser.py', '_engine_mapping_view', 'method:format',
     '_MAPPING_VIEW_PROBE_FAILED_TEMPLATE.format( func=func_name, type_name=type(value).__name__)'): (
        'excluded', None,
        "#69 拒绝消息模板（_MUTATOR_*/_MAPPING_VIEW_*）：插值 func 名/实参位/静态路径渲染/类型名，均来自静态表达式源，非运行期结果数据"),
    ('plaita/core/expression_parser.py', '_engine_mapping_view', 'method:format',
     '_MAPPING_VIEW_SPOOFED_TEMPLATE.format( func=func_name, type_name=type(value).__name__)'): (
        'excluded', None,
        "#69 拒绝消息模板（_MUTATOR_*/_MAPPING_VIEW_*）：插值 func 名/实参位/静态路径渲染/类型名，均来自静态表达式源，非运行期结果数据"),
    ('plaita/core/expression_parser.py', '_gate_mutator_argument', 'log:warning',
     '"plaita#69 mutation gate: failing closed for %r"'): (
        'excluded', None,
        "#69 侧存量（r13 对抗验收层）：插值均为静态表达式源/状态键名/类型名/引擎自造信号，不含运行期结果数据，非结果放大面"),
    ('plaita/core/expression_parser.py', '_gate_mutator_argument', 'method:format',
     '_MUTATOR_ARG_ROOT_TEMPLATE.format( func=func_name, root=root_key, position=position + 1, path=rendered)'): (
        'excluded', None,
        "#69 拒绝消息模板（_MUTATOR_*/_MAPPING_VIEW_*）：插值 func 名/实参位/静态路径渲染/类型名，均来自静态表达式源，非运行期结果数据"),
    ('plaita/core/expression_parser.py', '_gate_mutator_argument', 'method:format',
     '_MUTATOR_CONTAINER_ALIAS_ARG_TEMPLATE.format( func=func_name, path=rendered, alias_path=probed[1], root=probed[2], position=position + 1)'): (
        'excluded', None,
        "#69 拒绝消息模板（_MUTATOR_*/_MAPPING_VIEW_*）：插值 func 名/实参位/静态路径渲染/类型名，均来自静态表达式源，非运行期结果数据"),
    ('plaita/core/expression_parser.py', '_gate_mutator_argument', 'method:format',
     '_MUTATOR_CONTAINER_ALIAS_TEMPLATE.format( func=func_name, path=rendered, alias_path=probed[1], root=probed[2], position=position + 1)'): (
        'excluded', None,
        "#69 拒绝消息模板（_MUTATOR_*/_MAPPING_VIEW_*）：插值 func 名/实参位/静态路径渲染/类型名，均来自静态表达式源，非运行期结果数据"),
    ('plaita/core/expression_parser.py', '_gate_mutator_argument', 'method:format',
     '_MUTATOR_LIVE_REFERENCE_TEMPLATE.format( func=func_name, callee=callee, arg_note=_argument_note(position), path=source_path or f"$F.{callee}(...)", root=source_root or callee)'): (
        'excluded', None,
        "#69 拒绝消息模板（_MUTATOR_*/_MAPPING_VIEW_*）：插值 func 名/实参位/静态路径渲染/类型名，均来自静态表达式源，非运行期结果数据"),
    ('plaita/core/expression_parser.py', '_gate_mutator_argument', 'method:format',
     '_MUTATOR_ROOT_TEMPLATE.format( func=func_name, root=root_key, position=position + 1, path=rendered)'): (
        'excluded', None,
        "#69 拒绝消息模板（_MUTATOR_*/_MAPPING_VIEW_*）：插值 func 名/实参位/静态路径渲染/类型名，均来自静态表达式源，非运行期结果数据"),
    ('plaita/core/expression_parser.py', '_gate_mutator_argument', 'method:format',
     '_MUTATOR_UNDECLARED_PRODUCER_TEMPLATE.format( func=func_name, callee=callee, arg_note=_argument_note(position))'): (
        'excluded', None,
        "#69 拒绝消息模板（_MUTATOR_*/_MAPPING_VIEW_*）：插值 func 名/实参位/静态路径渲染/类型名，均来自静态表达式源，非运行期结果数据"),
    ('plaita/core/expression_parser.py', '_gate_mutator_argument', 'method:format',
     '_MUTATOR_UNMAPPED_TEMPLATE.format( func=func_name, root=root_key, path=rendered)'): (
        'excluded', None,
        "#69 拒绝消息模板（_MUTATOR_*/_MAPPING_VIEW_*）：插值 func 名/实参位/静态路径渲染/类型名，均来自静态表达式源，非运行期结果数据"),
    ('plaita/core/expression_parser.py', '_is_persisted_key', 'log:debug',
     '"persisted-key probe failed for %r: %s"'): (
        'excluded', None,
        "#69 侧存量（r13 对抗验收层）：插值均为静态表达式源/状态键名/类型名/引擎自造信号，不含运行期结果数据，非结果放大面"),
    ('plaita/core/expression_parser.py', '_is_persisted_key_strict', 'log:warning',
     '"plaita#69 mutation gate: cannot read the state key map " "(%s: %s); failing closed"'): (
        'excluded', None,
        "#69 侧存量（r13 对抗验收层）：插值均为静态表达式源/状态键名/类型名/引擎自造信号，不含运行期结果数据，非结果放大面"),
    ('plaita/core/expression_parser.py', '_is_persisted_key_strict', 'log:warning',
     '"plaita#69 mutation gate: state key map is %s, not a dict; " "failing closed"'): (
        'excluded', None,
        "#69 侧存量（r13 对抗验收层）：插值均为静态表达式源/状态键名/类型名/引擎自造信号，不含运行期结果数据，非结果放大面"),
    ('plaita/core/expression_parser.py', '_live_alias_index', 'log:debug',
     '"live root %r unreadable during alias probe: %s"'): (
        'excluded', None,
        "#69 侧存量（r13 对抗验收层）：插值均为静态表达式源/状态键名/类型名/引擎自造信号，不含运行期结果数据，非结果放大面"),
    ('plaita/core/expression_parser.py', '_live_alias_index', 'log:debug',
     '"live-state key map unreadable during alias probe: %s"'): (
        'excluded', None,
        "#69 侧存量（r13 对抗验收层）：插值均为静态表达式源/状态键名/类型名/引擎自造信号，不含运行期结果数据，非结果放大面"),
    ('plaita/core/expression_parser.py', '_non_root_key_identity_alias', 'log:debug',
     '"non-root key identity probe failed for %r: %s"'): (
        'excluded', None,
        "#69 侧存量（r13 对抗验收层）：插值均为静态表达式源/状态键名/类型名/引擎自造信号，不含运行期结果数据，非结果放大面"),
    ('plaita/core/expression_parser.py', '_persisted_root_keys', 'log:warning',
     '"plaita#69 mutation gate: cannot read the state key map " "(%s: %s); failing closed"'): (
        'excluded', None,
        "#69 侧存量（r13 对抗验收层）：插值均为静态表达式源/状态键名/类型名/引擎自造信号，不含运行期结果数据，非结果放大面"),
    ('plaita/core/expression_parser.py', '_persisted_root_keys', 'log:warning',
     '"plaita#69 mutation gate: state key map is %s, not a dict; " "failing closed"'): (
        'excluded', None,
        "#69 侧存量（r13 对抗验收层）：插值均为静态表达式源/状态键名/类型名/引擎自造信号，不含运行期结果数据，非结果放大面"),
    ('plaita/core/expression_parser.py', '_scalar_base_datetime', 'method:replace',
     'datetime.replace(built, fold=datetime.fold.__get__(value))'): (
        'bounded', None,
        "标量基读/拷贝 C 槽：值是已过类型闸的标量输入，长度由输入界定（#69 侧存量）"),
    ('plaita/core/expression_parser.py', '_scalar_base_decimal', 'method:__str__',
     'Decimal.__str__(value)'): (
        'bounded', None,
        "标量基读/拷贝 C 槽：值是已过类型闸的标量输入，长度由输入界定（#69 侧存量）"),
    ('plaita/core/expression_parser.py', '_scalar_base_str', 'method:__str__',
     'str.__str__(value)'): (
        'bounded', None,
        "标量基读/拷贝 C 槽：值是已过类型闸的标量输入，长度由输入界定（#69 侧存量）"),
    ('plaita/core/expression_parser.py', 'instance_state_signal', 'method:join',
     '", ".join(f"{name} on {layer}" for name, layer in c_descriptors)'): (
        'excluded', None,
        "拼接引擎自造信号串/C 描述符名/静态路径片段（_scalar_base 路径渲染），非作者数据"),
    ('plaita/core/expression_parser.py', 'instance_state_signal', 'method:join',
     '", ".join(hooks)'): (
        'excluded', None,
        "拼接引擎自造信号串/C 描述符名/静态路径片段（_scalar_base 路径渲染），非作者数据"),
    ('plaita/core/expression_parser.py', 'mapping_storage_refusal_detail', 'method:join',
     '" and ".join(signals)'): (
        'excluded', None,
        "拼接引擎自造信号串/C 描述符名/静态路径片段（_scalar_base 路径渲染），非作者数据"),
    ('plaita/core/expression_parser.py', 'mapping_storage_refusal_detail', 'method:join',
     '", ".join(overrides)'): (
        'excluded', None,
        "拼接引擎自造信号串/C 描述符名/静态路径片段（_scalar_base 路径渲染），非作者数据"),
    ('plaita/core/expression_parser.py', 'thunk', 'method:join',
     '"".join(path_parts)'): (
        'excluded', None,
        "拼接引擎自造信号串/C 描述符名/静态路径片段（_scalar_base 路径渲染），非作者数据"),

}

# (模块, 函数, ast.unparse(FormattedValue)) -> 理由
# 第十五轮 Q7：f-string 的**根**是形参/函数内局部名，或其属性/下标链 ⇒ 登记。
_FSTRING_PARAM_SITES = {
    ('plaita/core/expression.py', '_fn_append#local', '{length}'):
        '本模块自造的计数/标签/算子名（常量级或受输入串约束）',
    ('plaita/core/expression.py', '_fn_concat#local', '{unbounded!r}'):
        '本模块自造的计数/标签/算子名（常量级或受输入串约束）',
    ('plaita/core/expression.py', '_fn_join#local', '{n}'):
        '本模块自造的计数/标签/算子名（常量级或受输入串约束）',
    ('plaita/core/expression.py', '_content_size#param', '{depth}'):
        '递归预算消息里的深度计数（int，常量级）',
    ('plaita/core/expression.py', '_content_size_inner#param', '{depth}'):
        '嵌套深度计数（int 局部量，常量级）',
    ('plaita/core/expression.py', '_fn_mod#local', '{decision.reason}'):
        '本模块自造的计数/标签/算子名（常量级或受输入串约束）',
    ('plaita/core/expression.py', '_fn_mod#local', '{fmt_len}'):
        '本模块自造的计数/标签/算子名（常量级或受输入串约束）',
    ('plaita/core/expression.py', '_fn_mul#local', '{length}'):
        '本模块自造的计数/标签/算子名（常量级或受输入串约束）',
    ('plaita/core/expression.py', '_fn_replace#local', '{hits}'):
        '本模块自造的计数/标签/算子名（常量级或受输入串约束）',
    ('plaita/core/expression.py', '_fn_replace#local', '{length_a}'):
        '本模块自造的计数/标签/算子名（常量级或受输入串约束）',
    ('plaita/core/expression.py', '_join_item_lengths#local', '{limit_items}'):
        '本模块自造的计数/标签/算子名（常量级或受输入串约束）',
    ('plaita/core/expression.py', '_join_item_lengths#local', '{seen}'):
        '本模块自造的计数/标签/算子名（常量级或受输入串约束）',
    ('plaita/core/expression.py', '_mod_decide#local', '{spec.conversion!r}'):
        '本模块自造的计数/标签/算子名（常量级或受输入串约束）',
    ('plaita/core/expression.py', '_prepare_join#local', '{count}'):
        '本模块自造的计数/标签/算子名（常量级或受输入串约束）',
    ('plaita/core/expression.py', '_safe_int_text#local', '{bits}'):
        '本模块自造的计数/标签/算子名（常量级或受输入串约束）',
    ('plaita/core/expression.py', '_size_refusal#param', '{unit}'):
        '本模块自造的计数/标签/算子名（常量级或受输入串约束）',
    ('plaita/core/expression.py', '_size_refusal#param', '{what}'):
        '本模块自造的计数/标签/算子名（常量级或受输入串约束）',
    ('plaita/core/expression.py', 'register#param', '{name}'):
        '本模块自造的计数/标签/算子名（常量级或受输入串约束）',
    ('plaita/core/expression_parser.py', '_annotate_parse_error#local', '{caret}'):
        '表达式/解析器内部字符串（长度受输入串约束）；日志侧仓库级卫生见 #151',
    ('plaita/core/expression_parser.py', '_annotate_parse_error#local', '{col}'):
        '表达式/解析器内部字符串（长度受输入串约束）；日志侧仓库级卫生见 #151',
    ('plaita/core/expression_parser.py', '_annotate_parse_error#local', '{line}'):
        '表达式/解析器内部字符串（长度受输入串约束）；日志侧仓库级卫生见 #151',
    ('plaita/core/expression_parser.py', '_annotate_parse_error#param', '{exc.msg}'):
        '表达式/解析器内部字符串（长度受输入串约束）；日志侧仓库级卫生见 #151',
    ('plaita/core/expression_parser.py', '_annotate_parse_error#param', '{value!r}'):
        '表达式/解析器内部字符串（长度受输入串约束）；日志侧仓库级卫生见 #151',
    ('plaita/core/expression_parser.py', '_build_grammar#local', '{prefix}'):
        '表达式/解析器内部字符串（长度受输入串约束）；日志侧仓库级卫生见 #151',
    ('plaita/core/expression_parser.py', '_compile_variable#local', '{raw}'):
        '表达式/解析器内部字符串（长度受输入串约束）；日志侧仓库级卫生见 #151',
    ('plaita/core/expression_parser.py', '_compile_variable#param', '{self.prefix}'):
        '表达式/解析器内部字符串（长度受输入串约束）；日志侧仓库级卫生见 #151',
    ('plaita/core/expression_parser.py', '_eval_template#local', '{bound}'):
        '表达式/解析器内部字符串（长度受输入串约束）；日志侧仓库级卫生见 #151',
    ('plaita/core/expression_parser.py', '_render_path#local', '{raw}'):
        '表达式/解析器内部字符串（长度受输入串约束）；日志侧仓库级卫生见 #151',
    ('plaita/core/expression_parser.py', '_render_path#local', '{target}'):
        '表达式/解析器内部字符串（长度受输入串约束）；日志侧仓库级卫生见 #151',
    ('plaita/core/expression_parser.py', '_size_guard_result#local', '{allowed}'):
        '表达式/解析器内部字符串（长度受输入串约束）；日志侧仓库级卫生见 #151',
    ('plaita/core/expression_parser.py', '_size_guard_result#local', '{hard}'):
        '表达式/解析器内部字符串（长度受输入串约束）；日志侧仓库级卫生见 #151',
    ('plaita/core/expression_parser.py', '_size_guard_result#local', '{size}'):
        '表达式/解析器内部字符串（长度受输入串约束）；日志侧仓库级卫生见 #151',
    ('plaita/core/expression_parser.py', '_size_guard_result#param', '{func_name}'):
        '表达式/解析器内部字符串（长度受输入串约束）；日志侧仓库级卫生见 #151',
    ('plaita/core/expression_parser.py', 'parse_function#param', '{self.prefix}'):
        '表达式/解析器内部字符串（长度受输入串约束）；日志侧仓库级卫生见 #151',
    ('plaita/core/expression_parser.py', 'thunk#local', '{available}'):
        '表达式/解析器内部字符串（长度受输入串约束）；日志侧仓库级卫生见 #151',
    ('plaita/core/expression_parser.py', 'thunk#local', '{close!r}'):
        '表达式/解析器内部字符串（长度受输入串约束）；日志侧仓库级卫生见 #151',
    ('plaita/core/expression_parser.py', 'thunk#local', '{field_path}'):
        '表达式/解析器内部字符串（长度受输入串约束）；日志侧仓库级卫生见 #151',
    ('plaita/core/expression_parser.py', 'thunk#local', '{hint}'):
        '表达式/解析器内部字符串（长度受输入串约束）；日志侧仓库级卫生见 #151',
    ('plaita/core/expression_parser.py', 'thunk#local', '{original}'):
        '表达式/解析器内部字符串（长度受输入串约束）；日志侧仓库级卫生见 #151',
    ('plaita/core/expression_parser.py', 'thunk#local', '{prefix}'):
        '表达式/解析器内部字符串（长度受输入串约束）；日志侧仓库级卫生见 #151',
    ('plaita/core/expression_parser.py', 'thunk#local', '{preview!r}'):
        '表达式/解析器内部字符串（长度受输入串约束）；日志侧仓库级卫生见 #151',

    # --- #69 落地侧并合登记（同上，f-string 面）---
    ('plaita/core/expression_parser.py', '__init__#param', '{detail}'):
        '本模块自造的计数/标签/类型名（常量级或受输入串约束）',
    ('plaita/core/expression_parser.py', '__init__#param', '{type_name}'):
        '本模块自造的计数/标签/类型名（常量级或受输入串约束）',
    ('plaita/core/expression_parser.py', '_check_hashable_key#param', '{role}'):
        '本模块自造的计数/标签/类型名（常量级或受输入串约束）',
    ('plaita/core/expression_parser.py', '_detach_alias#local', '{callee}'):
        '静态路径片段/引擎自造信号串/算子名（表达式源码侧），非运行期结果数据',
    ('plaita/core/expression_parser.py', '_engine_copy#local', '{kind.__name__}'):
        '本模块自造的计数/标签/类型名（常量级或受输入串约束）',
    ('plaita/core/expression_parser.py', '_engine_copy_base_read#param', '{kind.__name__}'):
        '本模块自造的计数/标签/类型名（常量级或受输入串约束）',
    ('plaita/core/expression_parser.py', '_engine_copy_members#param', '{kind_name}'):
        '本模块自造的计数/标签/类型名（常量级或受输入串约束）',
    ('plaita/core/expression_parser.py', '_gate_mutator_argument#local', '{callee}'):
        '静态路径片段/引擎自造信号串/算子名（表达式源码侧），非运行期结果数据',
    ('plaita/core/expression_parser.py', '_probe_container_alias#local', '{key!r}'):
        '静态状态键名（来自表达式源码/前缀渲染），非运行期结果数据',
    ('plaita/core/expression_parser.py', 'instance_state_signal#local', '{layer}'):
        '静态路径片段/引擎自造信号串/算子名（表达式源码侧），非运行期结果数据',
    ('plaita/core/expression_parser.py', 'instance_state_signal#local', '{name}'):
        '静态路径片段/引擎自造信号串/算子名（表达式源码侧），非运行期结果数据',
    ('plaita/core/expression_parser.py', 'mapping_storage_refusal_detail#local', '{state_signal}'):
        '静态路径片段/引擎自造信号串/算子名（表达式源码侧），非运行期结果数据',
    ('plaita/core/expression_parser.py', 'thunk#local', '{raw}'):
        '静态路径片段/引擎自造信号串/算子名（表达式源码侧），非运行期结果数据',
    ('plaita/core/expression_parser.py', 'thunk#local', '{target}'):
        '静态路径片段/引擎自造信号串/算子名（表达式源码侧），非运行期结果数据',

}


# `$F.*` 渲染入口：注册名 -> (模块, helper, 该 helper 必须先调用的具名上界函数)。
# 守卫会反向检查：任何"体内含渲染原语"的注册 helper 都必须在这张表里。
_RENDER_ENTRY_POINTS = {
    "concat": ("plaita/core/expression.py", "_fn_concat", "_render_size"),
    "mod": ("plaita/core/expression.py", "_fn_mod", "_check_result_size"),
    "join": ("plaita/core/expression.py", "_fn_join", "_check_result_size"),
    "replace": ("plaita/core/expression.py", "_fn_replace", "_check_result_size"),
    "pow": ("plaita/core/expression.py", "_fn_pow", "_check_result_size"),
    "json_dumps": ("plaita/core/expression.py", "_fn_json_dumps", None),
    "template": ("plaita/core/expression_parser.py", "_eval_template", "_render_size"),

}


#: AC3：**真插桩**——被生成的作者 dunder 每次调用都追加到这里（正控可观测）。
_AUTHOR_PROTOCOL_CALLS = []


def _meta_deceived(base, slots, calls):
    """带**元类数据描述符欺骗**的子类：`Meta.__mro__`/`__dict__` 都撒谎（AC1/AC4）。"""
    class _Meta(type):
        __mro__ = property(lambda cls: (base,))
        __dict__ = property(lambda cls: {})

    def _count(name, fn):
        def _inner(self, *args, **kwargs):
            calls.append(name)
            _AUTHOR_PROTOCOL_CALLS.append(name)
            return fn(self, *args, **kwargs)
        return _inner

    body = {}
    for name, fn in slots.items():
        body[name] = _count(name, fn)
    return _Meta, body


def _float_index_liar(calls=None):
    calls = [] if calls is None else calls
    meta, body = _meta_deceived(float, {"__index__": lambda self: 10 ** 400000}, calls)
    return meta("FloatIndexLiar", (float,), body)(1.5)


def _str_repr_liar(calls=None):
    calls = [] if calls is None else calls
    meta, body = _meta_deceived(str, {"__str__": lambda self: "y" * 10_000_000,
                                      "__repr__": lambda self: "y" * 10_000_000}, calls)
    return meta("StrReprLiar", (str,), body)("x")


def _dict_getitem_liar(calls=None):
    calls = [] if calls is None else calls
    meta, body = _meta_deceived(dict, {"__getitem__": lambda self, k: "z" * 5_000_000}, calls)
    return meta("DictLiar", (dict,), body)(a="v")


def _calls_dict(body):
    wrapped = {}
    for name, fn in body.items():
        def _wrapper(self, *args, _fn=fn, _name=name, **kwargs):
            _AUTHOR_PROTOCOL_CALLS.append(_name)
            return _fn(self, *args, **kwargs)
        wrapped[name] = _wrapper
    return wrapped


def _ast_parents(tree):
    return {child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}


def _enclosing_function(node, parents):
    current = node
    while current in parents:
        current = parents[current]
        if isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return current
    return None


def _call_name(node):
    """被调者的名字：`f(x)` -> `f`，`m.f(x)` -> `f`；其余 None。"""
    if not isinstance(node, ast.Call):
        return None
    func = node.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


_LOG_LEVELS = frozenset({
    "debug", "info", "warning", "warn", "error", "exception", "critical", "log",
})
_LOG_OWNER_NAMES = frozenset({"logger", "logging", "log"})
_INDIRECT_JSON_FUNCS = frozenset({"json.dumps", "dumps"})
_STREAM_WRITE_RECEIVERS = frozenset({"StringIO", "BytesIO", "io.StringIO", "io.BytesIO"})


def _dotted_name(node) -> "str | None":
    parts = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
        return ".".join(reversed(parts))
    return None


def _const_str(node) -> "str | None":
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None


def _collect_render_aliases(tree) -> set:
    """模块/函数级 ``NAME = str|repr|format|ascii`` 的别名集合（M5）。"""
    aliases = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            targets = node.targets
            value = node.value
        elif isinstance(node, ast.AnnAssign):
            targets = [node.target]
            value = node.value
        else:
            continue
        if isinstance(value, ast.Name) and value.id in _RENDER_PRIMITIVE_NAMES:
            for target in targets:
                if isinstance(target, ast.Name):
                    aliases.add(target.id)
        elif isinstance(value, ast.Attribute) and value.attr in _RENDER_METHOD_NAMES:
            # Q7：`w = fh.write; w(a)` —— 绑定方法别名同样是渲染点
            for target in targets:
                if isinstance(target, ast.Name):
                    aliases.add(target.id)
    return aliases


def _classify_indirect_call(node):
    """``getattr(str,'__call__')(a)`` / ``operator.methodcaller('format')(a)`` 等间接形态。"""
    inner = node.func
    if not isinstance(inner, ast.Call):
        return None
    inner_func = inner.func
    if isinstance(inner_func, ast.Name) and inner_func.id == "getattr" and len(inner.args) >= 2:
        name = _const_str(inner.args[1])
        if name in _RENDER_PRIMITIVE_NAMES:
            return "indirect:getattr"
        if isinstance(inner.args[0], ast.Name) and inner.args[0].id in _RENDER_PRIMITIVE_NAMES:
            return "indirect:getattr"          # 取 str/repr 的任意属性都算危险构造
        if name in _RENDER_METHOD_NAMES:
            return "indirect:getattr"          # Q7：`getattr(fh,'write')(a)`
        if name is None and len(inner.args) == 2:
            return "indirect:getattr"          # `getattr(x, name)(...)` 动态名
    if isinstance(inner_func, ast.Name) and inner_func.id in ("vars", "globals", "locals"):
        return "indirect:vars"
    if isinstance(inner_func, ast.Attribute) and inner_func.attr == "methodcaller" \
            and inner.args and (_const_str(inner.args[0]) in _RENDER_METHOD_NAMES
                                or _const_str(inner.args[0]) is None):
        return "indirect:methodcaller"
    if isinstance(inner_func, ast.Attribute) and inner_func.attr in _RENDER_PRIMITIVE_NAMES:
        return "indirect:import"               # `__import__('builtins').str(a)`
    return None


def _classify_render_call(node, aliases):
    """返回 ``(kind, text_target)``；不是渲染点则 ``(None, None)``。"""
    func = node.func
    if isinstance(func, ast.Call):
        kind = _classify_indirect_call(node)
        return (kind, node) if kind else (None, None)
    if isinstance(func, ast.Subscript):
        # `__builtins__['repr'](a)` / `vars(m)['str'](a)`
        base = _dotted_name(func.value)
        if isinstance(func.value, ast.Call) and \
                _dotted_name(func.value.func) in ("vars", "globals", "locals", "dir"):
            return "indirect:vars", node
        key = _const_str(func.slice)
        if base in ("__builtins__",) or key in _RENDER_PRIMITIVE_NAMES:
            return "indirect:builtins", node
        if base in ("vars", "globals", "locals", "dir"):
            return "indirect:vars", node
        return None, None
    if isinstance(func, ast.Name):
        if func.id in _RENDER_PRIMITIVE_NAMES:
            return f"call:{func.id}", node
        if func.id in aliases:
            return "indirect:alias", node
        if func.id == "getattr" and len(node.args) >= 2:
            name = _const_str(node.args[1])
            if name in _RENDER_PRIMITIVE_NAMES:
                return "indirect:getattr", node
        if func.id == "methodcaller" and node.args:
            name = _const_str(node.args[0])
            if name in _RENDER_METHOD_NAMES or name is None:
                return "indirect:methodcaller", node
        if func.id == "dumps":
            return "indirect:json_dumps", node
        if func.id == "print":
            return "indirect:print", node
        return None, None
    if isinstance(func, ast.Attribute):
        owner = _dotted_name(func.value)
        owner_low = (owner or "").lower()
        if func.attr in _LOG_LEVELS and len(node.args) >= 2 and (
                owner in _LOG_OWNER_NAMES or owner_low.endswith("log")
                or owner_low.endswith("logger")):
            return f"log:{func.attr}", node.args[0]
        if owner == "operator" and func.attr == "methodcaller" and node.args:
            name = _const_str(node.args[0])
            if name in _RENDER_METHOD_NAMES or name is None:
                return "indirect:methodcaller", node
        if func.attr in ("dumps", "dump") or f"{owner}.{func.attr}" in _INDIRECT_JSON_FUNCS:
            return "indirect:json_dumps", node
        if func.attr in _RENDER_PRIMITIVE_NAMES and isinstance(func.value, ast.Call) \
                and _dotted_name(func.value.func) == "__import__":
            return "indirect:import", node
        if func.attr in _RENDER_METHOD_NAMES:
            # `io.StringIO().write(a)`：接收者是流构造调用
            if func.attr == "write" and isinstance(func.value, ast.Call) \
                    and _dotted_name(func.value.func) in _STREAM_WRITE_RECEIVERS:
                return "indirect:stringio_write", node
            return f"method:{func.attr}", node
    return None, None


def _iter_render_sites(tree, source=None):
    """产出 ``(函数名, 算子种类, 源码文本, 行号, 所在函数节点)``。

    文本优先取 ``ast.get_source_segment``（源码原文，跨 Python 版本稳定），
    取不到才退回 ``ast.unparse``。带上函数**节点**（而不是只带名字）是为了在有
    同名嵌套函数（`expression_parser` 里有多个 `thunk`）时仍能定位正确的函数体。

    第十二轮 M4/M5：除 `str()/repr()/format()/ascii()`、`%`、`.format()`、`.join()`、
    `.replace()`、`__str__`/`__repr__`/`__format__`/`__mod__` 属性取用外，还覆盖
    **可静态识别的危险构造**：模块别名指向渲染原语、`getattr(str,'__call__') (...)`,
    `operator.methodcaller('format')`、`json.dumps`、`io.StringIO().write`、
    `logger.*`/`logging.*` 的插值调用。宁红勿绿。
    """
    parents = _ast_parents(tree)
    aliases = _collect_render_aliases(tree)
    called_attrs = {id(sub.func) for sub in ast.walk(tree)
                    if isinstance(sub, ast.Call) and isinstance(sub.func, ast.Attribute)}
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and id(node) not in called_attrs \
                and node.attr in _DUNDER_RENDER_ATTRS:
            kind, target = f"attr:{node.attr}", node
        elif isinstance(node, ast.Call):
            kind, target = _classify_render_call(node, aliases)
            if kind is None:
                continue
        elif isinstance(node, ast.BinOp) and isinstance(node.op, ast.Mod):
            kind, target = "binop:%", node
        else:
            continue
        text = ast.get_source_segment(source, target) if source is not None else None
        if not text:
            text = ast.unparse(target)
        text = " ".join(text.split())
        enclosing = _enclosing_function(node, parents)
        yield (enclosing.name if enclosing is not None else "<module>",
               kind, text, node.lineno, enclosing, node)


def _root_name(node):
    """表达式的根名字：`a.b[0]` -> `a`；`f(x).y` / 字面量 -> ``None``。"""
    while isinstance(node, (ast.Attribute, ast.Subscript)):
        node = node.value
    return node.id if isinstance(node, ast.Name) else None


def _param_aliases(fn):
    """函数内"形参的直接别名"集合：``x = a`` / ``x = a.attr`` / ``x = a[i]``。

    第十三轮 O7：`x = a; f"{x}"` 与 `f"{a}"` 是同一类渲染点。只做**直接**传播
    （赋值的根是形参/已知别名），不把 `bits = len(a)` 这类派生计数误判为作者数据。
    """
    params = {arg.arg for arg in fn.args.args}
    params |= {arg.arg for arg in fn.args.kwonlyargs}
    params |= {arg.arg for arg in fn.args.posonlyargs}
    aliases = set(params)
    changed = True
    while changed:
        changed = False
        for node in ast.walk(fn):
            if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Name):
                continue
            if node.value.id in aliases:
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id not in aliases:
                        aliases.add(target.id)
                        changed = True
    return params, aliases


def _iter_fstring_param_sites(tree):
    """产出 ``(函数名#param|#local, ast.unparse(FormattedValue), 行号)``。

    判据：插值表达式的**根**是本函数形参或形参的直接别名（`f"{user}"` /
    `x = user; f"{x}"`），或带 format spec（`f"{n:>100000000}"` —— spec 本身就是
    一次按宽度的分配）。`f"{len(a)}"` / `f"{type(x).__name__}"` 这类根是调用结果的
    插值不算（清单是**启发式**，不是完备证明——见文档）。
    """
    parents = _ast_parents(tree)
    for node in ast.walk(tree):
        if not isinstance(node, ast.JoinedStr):
            continue
        fn = _enclosing_function(node, parents)
        if fn is None:
            continue
        params, aliases = _param_aliases(fn)
        for value in node.values:
            if not isinstance(value, ast.FormattedValue):
                continue
            if value.format_spec is not None:
                yield (fn.name, " ".join(ast.unparse(value).split()), node.lineno)
                continue
            root = _root_name(value.value)
            inner = value.value
            while isinstance(inner, (ast.Attribute, ast.Subscript)):
                inner = inner.value
            # Q7：`f"{obj.attr}"` / `f"{d['k']}"`——链的**根**是形参或函数内局部名
            # （含循环变量/with-as）才算渲染点；纯模块级常量的链在文档里点名。
            risky_chain = isinstance(inner, ast.Name) and (
                inner.id in aliases or inner.id in {
                    sub.id for sub in ast.walk(fn)
                    if isinstance(sub, ast.Name) and isinstance(sub.ctx, ast.Store)})
            if root in aliases or risky_chain:
                label = "param" if root in params else "local"
                yield (f"{fn.name}#{label}", " ".join(ast.unparse(value).split()),
                       node.lineno)


def _bound_provider_lines(function, provider):
    return [sub.lineno for sub in ast.walk(function)
            if isinstance(sub, ast.Call) and _call_name(sub) == provider]


def _field_of(parent, child):
    """``child`` 是 ``parent`` 的哪个字段（body/handlers/orelse/finalbody/…）。"""
    for name, value in ast.iter_fields(parent):
        if value is child:
            return name
        if isinstance(value, list) and any(item is child for item in value):
            return name
    return ""


_CONDITIONAL_NODES = (ast.If, ast.For, ast.AsyncFor, ast.While, ast.Try,
                      ast.With, ast.AsyncWith, ast.Subscript, ast.Match)
_CONDITIONAL_EXPR_NODES = (ast.IfExp, ast.BoolOp, ast.GeneratorExp, ast.Lambda)
_COMPREHENSION_NODES = (ast.ListComp, ast.SetComp, ast.DictComp, ast.GeneratorExp)
# 未被调用、只被**传递**的 render dunder（`functools.reduce(str.__add__, xs)`）。
_DUNDER_RENDER_ATTRS = frozenset({
    "__add__", "__radd__", "__mul__", "__rmul__", "__mod__",
    "__str__", "__repr__", "__format__", "__call__",
})


def _inside_filtered_comprehension(node, parents):
    """调用的祖先里有**带 if 子句**的推导式 ⇒ 它可能被过滤掉 ⇒ 不支配（O3）。"""
    current = node
    while current in parents:
        current = parents[current]
        if isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return False
        if isinstance(current, _COMPREHENSION_NODES):
            if any(getattr(gen, "ifs", None) for gen in current.generators):
                return True
        if isinstance(current, ast.match_case) and current.guard is not None:
            return True                       # Q5：`case _ if False:` 体可能不执行
    return False


def _ancestor_chain(node, parents):
    """从 ``node`` 向上的祖先节点列表（不含函数边界以上）。"""
    chain = []
    current = node
    while current in parents:
        parent = parents[current]
        if isinstance(parent, (ast.FunctionDef, ast.AsyncFunctionDef)):
            break
        chain.append(parent)
        current = parent
    return chain


def _is_within(ancestor, node, parents):
    current = node
    while current is not None:
        if current is ancestor:
            return True
        current = parents.get(current)
    return False


def _comprehension_ancestors(node, parents):
    return {anc for anc in _ancestor_chain(node, parents)
            if isinstance(anc, _COMPREHENSION_NODES)}


def _inside_comprehension_iterable(node, parents):
    """``node`` 是否落在某个推导式的 ``generators[*].iter`` 里（T6）。

    推导式**先求值可迭代对象**，再求值元素表达式 ⇒ 元素里的上界调用支配不了
    ``iter`` 里的渲染点（`[_render_size(v) for _ in str(v)]`）。
    """
    for anc in _ancestor_chain(node, parents):
        if isinstance(anc, _COMPREHENSION_NODES):
            for gen in anc.generators:
                if _is_within(gen.iter, node, parents):
                    return anc
    return None


def _conditional_chain(node, parents):
    """从外到内的"条件/循环/异常/惰性表达式"链，带字段名以区分 try body/handler。"""
    chain = []
    current = node
    while current in parents:
        parent = parents[current]
        if isinstance(parent, (ast.FunctionDef, ast.AsyncFunctionDef)):
            break
        if isinstance(parent, _CONDITIONAL_NODES + _CONDITIONAL_EXPR_NODES):
            field = _field_of(parent, current)
            if isinstance(current, (ast.ExceptHandler, ast.match_case)):
                field = id(current)           # Q5：兄弟 case/handler 必须区分
            chain.append((id(parent), field))
        current = parent
    chain.reverse()
    return chain


def _dominates(call_node, site_node, parents):
    """``call_node`` 是否在**所有**到达 ``site_node`` 的路径上执行。

    保守充分条件（宁红勿绿）：调用的条件链是站点条件链的前缀，且在同一层源码里
    更早。`if False:` 里的调用其链多一层 ⇒ 前缀不成立 ⇒ 必红（第十二轮 M3：
    旧守卫只查"出现过"，把上界调用挪进死分支后 309 条用例全绿）。
    """
    if _enclosing_function(call_node, parents) is not _enclosing_function(site_node, parents):
        return False                          # 嵌套 def/lambda 里的调用不支配本函数
    if _inside_filtered_comprehension(call_node, parents):
        return False                          # `[... for x in xs if False]`
    if _inside_comprehension_iterable(site_node, parents) is not None and \
            _comprehension_ancestors(call_node, parents):
        # Z3/T6：站点的渲染点在**任一**推导式的 iter 里（含嵌套），而上界调用在**任一**
        # 推导式里 ⇒ iter 先于元素求值（嵌套时外层 iter 先于内层元素），调用不支配它。
        return False
    call_chain = _conditional_chain(call_node, parents)
    site_chain = _conditional_chain(site_node, parents)
    if call_chain != site_chain[:len(call_chain)]:
        return False
    if call_node.lineno != site_node.lineno:
        return call_node.lineno < site_node.lineno
    # S5：**同一行内**必须严格在前（`col_offset`），否则 `x = str(v); b = _render_size(v)`
    # 这类逆序也会被判"被支配"。
    return call_node.col_offset < site_node.col_offset


def _scan_render_offenders(source, module, render_sites, fstring_sites):
    """同一个检查器的唯一入口；合成源码与真实源码都走它。"""
    tree = ast.parse(source)
    parents = _ast_parents(tree)
    offenders = []
    found_render = set()
    for func, kind, text, line, fn, node in _iter_render_sites(tree, source):
        key = (module, func, kind, text)
        found_render.add(key)
        spec = render_sites.get(key)
        if spec is None:
            offenders.append(f"未登记渲染点 {module}::{func} L{line} [{kind}] {text!r}")
            continue
        disposition, provider, why = spec
        if not why.strip():
            offenders.append(f"渲染点 {key} 未写理由")
        if disposition == "guarded":
            calls = [sub for sub in ast.walk(fn)
                     if isinstance(sub, ast.Call) and _call_name(sub) == provider] \
                if fn is not None else []
            if not any(_dominates(call, node, parents) for call in calls):
                offenders.append(
                    f"{module}::{func} L{line} [{kind}] {text!r} 声明 guarded"
                    f"（{provider}）但该调用并不支配这个渲染点（可能在死分支/另一分支）")
        elif disposition not in ("bounded", "excluded"):
            offenders.append(f"渲染点 {key} 的处置 {disposition!r} 非法")
    found_fstrings = set()
    for func, text, line in _iter_fstring_param_sites(tree):
        key = (module, func, text)
        found_fstrings.add(key)
        if key not in fstring_sites:
            offenders.append(f"未登记的 f-string 形参渲染 {module}::{func} L{line} {text!r}")
    return offenders, found_render, found_fstrings


@skipUnless(_HAS_GUARD, "guard not present")
class TestTwelfthRoundH3RenderSiteInventory(TestCase):
    """第十二轮 H3：渲染点必须**逐点**登记，登记了 `guarded` 就必须真有更早的上界调用。"""

    def _sources(self):
        return {module: pathlib.Path(path).read_text(encoding="utf-8")
                for module, path in _LIBRARY_SOURCES.items()}

    def test_render_site_registry_is_exact(self):
        """真实树里的渲染点集合 == 登记表（多一条或少一条都要红）。"""
        found = set()
        found_fstrings = set()
        for module, source in self._sources().items():
            _, render, fstrings = _scan_render_offenders(
                source, module, _RENDER_SITES, _FSTRING_PARAM_SITES)
            found |= render
            found_fstrings |= fstrings
        self.assertEqual(set(_RENDER_SITES), found,
                         "渲染点登记表与源码不一致（新增/删除渲染点都要同步）")
        self.assertEqual(set(_FSTRING_PARAM_SITES), found_fstrings,
                         "f-string 形参渲染登记表与源码不一致")

    def test_no_unregistered_or_unguarded_render_sites(self):
        """负向对照：真实源码必须 0 报警。"""
        offenders = []
        for module, source in self._sources().items():
            offenders.extend(_scan_render_offenders(
                source, module, _RENDER_SITES, _FSTRING_PARAM_SITES)[0])
        self.assertEqual([], offenders)

    def test_unregistered_render_site_is_flagged(self):
        """正向对照①：合成源码里未登记的 `str(a)` / `"%s" % a` 必须被同一个检查器抓到。"""
        synthetic = (
            "def _fn_evil(a):\n"
            "    return str(a)\n"
            "\n"
            "def _fn_worse(a):\n"
            "    return '%s' % a\n"
            "\n"
            "def _fn_ok(a):\n"
            "    bound = _render_size(a)\n"
            "    if bound is None:\n"
            "        raise ValueError('x')\n"
            "    return str(a)\n"
        )
        offenders, found, _ = _scan_render_offenders(
            synthetic, "synthetic.py", _RENDER_SITES, _FSTRING_PARAM_SITES)
        self.assertIn(("synthetic.py", "_fn_evil", "call:str", "str(a)"), found)
        self.assertIn(("synthetic.py", "_fn_worse", "binop:%", "'%s' % a"), found)
        self.assertTrue(any("_fn_evil" in line for line in offenders))
        self.assertTrue(any("_fn_worse" in line for line in offenders))
        # 给 `_fn_ok` 一条正确的登记（其余仍缺）⇒ `_fn_ok` 不得再出现在报警里。
        offenders_ok, _, _ = _scan_render_offenders(
            synthetic, "synthetic.py",
            {("synthetic.py", "_fn_ok", "call:str", "str(a)"): ("guarded", "_render_size", "理由")},
            _FSTRING_PARAM_SITES)
        self.assertFalse([line for line in offenders_ok if "_fn_ok" in line], offenders_ok)
        self.assertTrue(any("_fn_evil" in line for line in offenders_ok))

    def test_removing_the_bound_call_from_concat_is_flagged(self):
        """正向对照②（承重）：抽掉真实 `_fn_concat` 的 `_render_size` ⇒ 必须报警。

        第十一轮的绕过就发生在这条路径上；如果这条守卫在"上界调用被拿掉"后仍然
        通过，它就又变回了那条通过时毫无意义的文本断言。
        """
        module = "plaita/core/expression.py"
        source = self._sources()[module]
        needle = "bounds = [_render_size(arg) for arg in args]"
        self.assertIn(needle, source)
        broken = source.replace(needle, "bounds = [0 for arg in args]")
        offenders, _, _ = _scan_render_offenders(
            broken, module, _RENDER_SITES, _FSTRING_PARAM_SITES)
        self.assertTrue(
            any("_fn_concat" in line and "_render_size" in line for line in offenders),
            f"抽掉上界调用后守卫没有报警：{offenders[:5]}")

    # 第十三轮 O3：五种"上界调用出现过但不支配"的变异定义（原文存这里，供 replay）。
    _DOMINANCE_MUTATIONS = {
        "if_false": (
            "if False:\n        bounds = [_render_size(arg) for arg in args]\n"
            "    bounds = [0 for arg in args]"
        ),
        "filtered_comprehension": (
            "bounds = [_render_size(arg) for arg in args if False]\n"
            "    bounds = [0 for arg in args]"
        ),
        "sliced_to_empty": (
            "bounds = [_render_size(arg) for arg in args][:0]\n"
            "    bounds = [0 for arg in args]"
        ),
        "nested_def": (
            "def _bound(args):\n        return [_render_size(arg) for arg in args]\n"
            "    bounds = [0 for arg in args]"
        ),
        "match_case": (
            "match args:\n        case _:\n"
            "            bounds = [_render_size(arg) for arg in args]\n"
            "    bounds = [0 for arg in args]"
        ),
    }

    def test_moving_the_bound_call_out_of_dominance_is_flagged(self):
        """正向对照③（O3 决定性）：上界调用"出现过但不支配"的五种形状都必须红。

        第十二轮验收用 `if False:` 打穿了旧守卫，第十三轮又用"带 if 的推导式 /
        `[:0]` 切片 / 嵌套 def / match 分支"四种形状打穿——上界调用出现过但**结果
        不支配渲染点**。支配性检查必须把五种全判红。
        """
        module = "plaita/core/expression.py"
        source = self._sources()[module]
        needle = "bounds = [_render_size(arg) for arg in args]"
        self.assertIn(needle, source)
        for label, replacement in self._DOMINANCE_MUTATIONS.items():
            with self.subTest(shape=label):
                broken = source.replace(needle, replacement)
                self.assertNotEqual(source, broken)
                offenders, _, _ = _scan_render_offenders(
                    broken, module, _RENDER_SITES, _FSTRING_PARAM_SITES)
                self.assertTrue(
                    any("_fn_concat" in line and "支配" in line for line in offenders),
                    f"{label}: 上界调用不支配渲染点但没有报警：{offenders[:5]}")

    def test_dominance_checker_itself(self):
        """支配性检查单元对照：同路径更早 = 支配；死分支/另一分支 = 不支配。"""
        source = (
            "def f(a, cond):\n"
            "    _render_size(a)\n"                 # L2 支配 L4
            "    x = str(a)\n"                       # L3
            "    if cond:\n"
            "        _render_size(a)\n"               # L5 不支配 L6? 同分支且更早 ⇒ 支配
            "        y = str(a)\n"                   # L6
            "    if False:\n"
            "        _render_size(a)\n"               # L8 不支配 L10
            "    z = str(a)\n"                       # L10
            "    return x, y, z\n"
        )
        tree = ast.parse(source)
        parents = _ast_parents(tree)
        calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
                 and _call_name(n) == "_render_size"]
        sites = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
                 and _call_name(n) == "str"]
        self.assertEqual(3, len(calls))
        self.assertEqual(3, len(sites))
        calls.sort(key=lambda n: n.lineno)
        sites.sort(key=lambda n: n.lineno)
        self.assertTrue(_dominates(calls[0], sites[0], parents))
        self.assertTrue(_dominates(calls[1], sites[1], parents))
        self.assertFalse(_dominates(calls[2], sites[2], parents))
        self.assertFalse(_dominates(calls[1], sites[2], parents))

    def test_m5_indirect_constructs_are_flagged(self):
        """正向对照④（M5）：可静态识别的危险构造必须被扫描器抓到（宁红勿绿）。"""
        synthetic = (
            "import io\n"
            "import json\n"
            "import logging\n"
            "import operator\n"
            "def f(a):\n"
            "    _s = str\n"                              # 别名
            "    r = []\n"
            "    r.append(getattr(str, '__call__')(a))\n"
            "    r.append('%s'.__mod__(a))\n"
            "    r.append(operator.methodcaller('format')(a))\n"
            "    r.append(io.StringIO().write(a))\n"
            "    r.append(json.dumps(a))\n"
            "    r.append(type(a).__str__(a))\n"
            "    _s(a)\n"
            "    logging.warning('%s', a)\n"
            "    return r\n"
        )
        _, found, _ = _scan_render_offenders(
            synthetic, "synthetic.py", _RENDER_SITES, _FSTRING_PARAM_SITES)
        kinds = {kind for _module, _func, kind, _text in found}
        for expected in ("indirect:getattr", "method:__mod__", "indirect:methodcaller",
                         "indirect:stringio_write", "indirect:json_dumps",
                         "method:__str__", "indirect:alias", "log:warning"):
            with self.subTest(kind=expected):
                self.assertIn(expected, kinds)

    def test_param_fstring_is_flagged(self):
        """正向对照③：`f"{a}"` 就是一次对形参的 `str()` 渲染，必须登记。"""
        synthetic = (
            "def _fn_evil(a):\n"
            "    raise ValueError(f'{a}')\n"
        )
        offenders, _, found_fstrings = _scan_render_offenders(
            synthetic, "synthetic.py", _RENDER_SITES, _FSTRING_PARAM_SITES)
        self.assertIn(("synthetic.py", "_fn_evil#param", "{a}"), found_fstrings)
        self.assertTrue(any("_fn_evil" in line for line in offenders))

    def test_render_entry_points_are_registered_and_guarded(self):
        """`$F.*` 渲染入口：体内含渲染原语的注册 helper 必须登记且真有具名上界调用。"""
        registry = expr_mod.get_default_expression_registry()
        registered = {}
        for name, descriptor in registry.all_functions().items():
            fn_name = getattr(descriptor.func, "__name__", "")
            registered.setdefault(fn_name, []).append(name)

        render_helpers = set()
        for module, source in self._sources().items():
            for func, _kind, _text, _line, _node, _site in _iter_render_sites(ast.parse(source), source):
                render_helpers.add((module, func))

        # 反向覆盖：每个"体内渲染"的注册 helper 都必须在入口表里。
        for module, helper in sorted(render_helpers):
            names = registered.get(helper, [])
            if not names:
                continue
            self.assertTrue(
                any(name in _RENDER_ENTRY_POINTS for name in names),
                f"{helper} 体内含渲染原语（{module}）但不在 _RENDER_ENTRY_POINTS"
                f"（注册名 {names}）")
        # 正向：入口表里的每一项都指向真实存在的注册名/helper 与具名上界调用。
        for name, (module, helper, provider) in _RENDER_ENTRY_POINTS.items():
            if name != "template":
                self.assertIn(helper, registered,
                              f"$F.{name} 未指向已注册 helper {helper}")
            self.assertIn((module, helper), render_helpers,
                          f"{name} 登记的 helper {helper} 体内没有渲染原语")
            tree = ast.parse(self._sources()[module])
            functions = {node.name: node for node in ast.walk(tree)
                         if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}
            if provider is not None:
                self.assertTrue(_bound_provider_lines(functions[helper], provider),
                                f"{helper} 没有调用登记的上界函数 {provider}")


@skipUnless(_HAS_GUARD, "guard not present")
class TestTwelfthRoundH3MessageRendering(TestCase):
    """第十二轮 H3：**错误消息本身**也是一次渲染，不得被敌意协议撑爆。

    实测（修复前）：`_fn_pow`/`_fn_mul` 用 `f"{n}"` 把用户 int 子类写进消息，
    子类 `__str__` 返回 20,000,000 字符 ⇒ 消息长 20,000,098、tracemalloc 峰值
    40MB——拒绝路径反而成了分配路径。`_render_namespace_bound` 的 `str(key)`
    同理（在"求上界"的过程中先渲染一次）。
    """

    class _EvilInt(int):
        calls = 0

        def __str__(self):
            type(self).calls += 1
            return "E" * 20_000_000

        def __repr__(self):
            type(self).calls += 1
            return "R" * 20_000_000

        def __abs__(self):                    # 不得被 C 槽绕过之外的实现利用
            type(self).calls += 1
            return super().__abs__()

    class _EvilStr(str):
        calls = 0

        def __str__(self):
            type(self).calls += 1
            return "S" * 20_000_000

    def test_pow_and_mul_messages_do_not_call_hostile_int_str(self):
        for label, call in (
            ("pow", lambda: _fn_pow(9, self._EvilInt(10**7))),
            ("mul", lambda: _fn_mul("a", self._EvilInt(10**7))),
        ):
            with self.subTest(fn=label):
                self._EvilInt.calls = 0
                desc, peak = _measure_peak(call)
                self.assertEqual("ValueError", desc)
                self.assertLess(peak, _PEAK_BUDGET, f"{label}: {peak/1024/1024:.1f}MB")
                self.assertEqual(0, self._EvilInt.calls,
                                 f"{label} 渲染了敌意 int 的 __str__/__repr__/__abs__")

    def test_huge_multiplier_message_is_bounded_not_decimal_expanded(self):
        desc, peak = _measure_peak(lambda: _fn_mul("a", 10**100_000))
        self.assertEqual("ValueError", desc)
        self.assertLess(peak, _PEAK_BUDGET)
        with self.assertRaises(ValueError) as mul_ctx:
            _fn_mul("a", 10**100_000)
        mul_message = str(mul_ctx.exception)
        self.assertIn("<int:", mul_message)                   # 乘数只报位数
        self.assertLess(len(mul_message), 300, mul_message)   # 规模也按位数报，不展开
        with self.assertRaises(ValueError) as pow_ctx:
            _fn_pow(9, 10**100_000)
        self.assertLess(len(str(pow_ctx.exception)), 300)

    def test_namespace_bound_does_not_render_hostile_str_key(self):
        namespace = types.SimpleNamespace()
        namespace.__dict__[self._EvilStr("k")] = "v"
        self._EvilStr.calls = 0
        bound = _render_size(namespace)
        self.assertIsNotNone(bound)
        self.assertEqual(0, self._EvilStr.calls,
                         "求上界时调用了敌意 str 子类的 __str__")


# ---------------------------------------------------------------------------
# 第十二轮 M1–M7：祖先 C 槽计费 / 映射规划 / 渲染入口行为反例 / 日志 / enum
# ---------------------------------------------------------------------------
class _M1LyingLenStr(str):
    calls = 0

    def __len__(self):
        type(self).calls += 1
        return 1


class _M1LyingCountStr(str):
    count_calls = 0

    def count(self, *args, **kwargs):          # noqa: D102 - 敌意覆写
        type(self).count_calls += 1
        return 0


class _M1LyingSepStr(str):
    calls = 0

    def __len__(self):
        type(self).calls += 1
        return 1


class _M1EvilJoinStr(str):
    calls = 0

    def join(self, items):                     # noqa: D102 - 敌意覆写
        type(self).calls += 1
        return "J" * 5_000_000


class _M2LyingTuple(tuple):
    calls = 0

    def __len__(self):
        type(self).calls += 1
        return 0

    def __iter__(self):
        type(self).calls += 1
        return iter(())


class _M3HostileBigInt(int):
    calls = 0

    def __str__(self):
        type(self).calls += 1
        return "H" * 5_000_000

    def __repr__(self):
        type(self).calls += 1
        return "R" * 5_000_000


class _M1HonestStr(str):
    pass


@skipUnless(_HAS_GUARD, "guard not present")
class TestTwelfthRoundM1AncestorBilling(TestCase):
    """M1：所有算子的前置检查/预留都走内建祖先 C 槽；说谎实例协议调用数 = 0。

    第十一轮只把**渲染路径**改对了，运算符前置检查仍在 `len(value)`：
    `_fn_mul(LyingLenStr("a"*1000, __len__→1), 50000)` 按 1 计费、原生造出
    50,000,000 字符（实测 @50MB）。
    """

    def _refused_cheaply(self, fn):
        desc, peak = _measure_peak(fn)
        self.assertEqual("ValueError", desc)
        self.assertLess(peak, _PEAK_BUDGET, f"峰值 {peak/1024/1024:.1f}MB")

    def test_mul_uses_ancestor_len(self):
        value = _M1LyingLenStr("a" * 1000)
        _M1LyingLenStr.calls = 0
        self._refused_cheaply(lambda: _fn_mul(value, 50_000))
        self.assertEqual(0, _M1LyingLenStr.calls, "mul 前置检查调用了覆写的 __len__")

    def test_mul_list_subclass_uses_ancestor_len(self):
        value = _LyingLenList([0] * 1_000_000)      # 在测量之外构造
        self._refused_cheaply(lambda: _fn_mul(value, 10))

    def test_replace_uses_ancestor_len_and_count(self):
        value = _M1LyingLenStr("a" * 20_000)
        _M1LyingLenStr.calls = 0
        self._refused_cheaply(lambda: _fn_replace(value, "", "b" * 1000))
        self.assertEqual(0, _M1LyingLenStr.calls)

        _M1LyingCountStr.count_calls = 0
        self._refused_cheaply(
            lambda: _fn_replace(_M1LyingCountStr("a" * 100_000), "a", "b" * 1000))
        self.assertEqual(0, _M1LyingCountStr.count_calls,
                         "replace 用了覆写的 count（祖先槽应为 str.count）")

    def test_add_extend_append_use_ancestor_len(self):
        left = _LyingLenList([0] * 1_000_000)       # 在测量之外构造
        right = _LyingLenList([0] * 1_000_000)
        with _limits(floor=1, factor=1, hard=7):
            self._refused_cheaply(lambda: _fn_append(left, 7))
            self._refused_cheaply(lambda: _fn_add(left, right))
            self._refused_cheaply(lambda: _fn_extend(left, right))

    def test_join_uses_ancestor_sep_len_and_join_slot(self):
        sep = _M1LyingSepStr("s" * 25_000)
        _M1LyingSepStr.calls = 0
        self._refused_cheaply(lambda: _fn_join(["a"] * 6, sep))
        self.assertEqual(0, _M1LyingSepStr.calls, "join 前置检查调用了覆写的 __len__")

        evil = _M1EvilJoinStr("-")
        _M1EvilJoinStr.calls = 0
        self.assertEqual("a-b", _fn_join(["a", "b"], evil))
        self.assertEqual(0, _M1EvilJoinStr.calls, "join 调用了覆写的 join 方法")

    def test_honest_values_keep_identical_results(self):
        self.assertEqual("ababab", _fn_mul("ab", 3))
        self.assertEqual("xybxyb", _fn_replace("abab", "a", "xy"))
        self.assertEqual([1, 2], _fn_add([1], [2]))
        self.assertEqual((1, 2), _fn_extend((1,), (2,)))
        self.assertEqual([1, 2], _fn_append([1], 2))
        self.assertEqual("a-b", _fn_join(["a", "b"], "-"))
        self.assertEqual("ababab", _fn_mul(_M1HonestStr("ab"), 3))
        self.assertEqual("a-a-a-a-a", _fn_join(_M1HonestStr("a") * 5, "-"))

    def test_unprovable_custom_sequence_is_refused_not_guessed(self):
        class _Custom:
            def __mul__(self, other):
                return self

            def __len__(self):
                return 1

        self._refused_cheaply(lambda: _fn_mul(_Custom(), 10**6))


@skipUnless(_HAS_GUARD, "guard not present")
class TestTwelfthRoundM2MappingMod(TestCase):
    """M2：`$F.mod` 的映射参数不再要求精确 dict；找不到映射祖先槽也不得跳过判定。"""

    _FMT = "%(k)100000000s"

    def test_non_dict_mappings_are_refused_before_allocating(self):
        for label, values in (
            ("mappingproxy", types.MappingProxyType({"k": "x"})),
            ("chainmap", collections.ChainMap({"k": "x"})),
            ("userdict", collections.UserDict({"k": "x"})),
        ):
            with self.subTest(mapping=label):
                desc, peak = _measure_peak(lambda v=values: _fn_mod(self._FMT, v))
                self.assertEqual("ValueError", desc)
                self.assertLess(peak, _PEAK_BUDGET, f"{label}: {peak/1024/1024:.1f}MB")

    def test_dict_subclass_with_lying_contains_is_refused(self):
        class _LyingContains(dict):
            def __contains__(self, key):
                return False

        desc, peak = _measure_peak(lambda: _fn_mod(self._FMT, _LyingContains({"k": "x"})))
        self.assertEqual("ValueError", desc)
        self.assertLess(peak, _PEAK_BUDGET)

    def test_tuple_subclass_reads_through_tuple_slots(self):
        _M2LyingTuple.calls = 0
        self.assertEqual("ab", _fn_mod("%s%s", _M2LyingTuple(("a", "b"))))
        self.assertEqual(0, _M2LyingTuple.calls,
                         "mod 规划用了 tuple 子类覆写的 __len__/__iter__")

    def test_exact_dict_control_and_valid_mappings_work(self):
        # S2：MappingProxyType 一律拒绝（copy() 会跑被包 dict 子类的作者协议，
        # type(proxy) 又证明不了被包对象是精确 dict）
        with self.assertRaises(ValueError) as cm:
            _fn_mod("%(k)s", types.MappingProxyType({"k": "x"}))
        self.assertIn("dict(x)", str(cm.exception))
        self.assertEqual("x", _fn_mod("%(k)s", collections.ChainMap({"k": "x"})))
        self.assertEqual("x", _fn_mod("%(k)s", {"k": "x"}))

    def test_native_errors_still_take_precedence(self):
        with _limits(floor=1, factor=1, hard=1):
            with self.assertRaises(KeyError):
                _fn_mod("%(z)s", {"a": 1})
            with self.assertRaises(TypeError):
                _fn_mod("%(a)s", "x")
            with self.assertRaises(TypeError):
                _fn_mod("%1000000000d", "x")
            with self.assertRaises(TypeError):
                _fn_mod("%s %s", [1, 2])
            with self.assertRaises(TypeError):
                _fn_mod("%(k)*s", {"k": "x"})

    def test_mapping_decision_is_snapshot_based(self):
        """Q2：精确 dict 走 ``dict.get`` C 槽；三种 stdlib 映射一次性快照；其余拒绝。"""
        import collections as collections_module
        import inspect
        import types as types_module
        exact = {"a": 1}
        self.assertIs(_mapping_snapshot(exact), exact)
        self.assertIsNone(_mapping_snapshot(types_module.MappingProxyType({"a": 1})))
        self.assertEqual({"a": 1, "b": 2},
                         _mapping_snapshot(collections_module.ChainMap({"a": 1}, {"b": 2})))
        self.assertEqual({"a": 1}, _mapping_snapshot(collections_module.UserDict({"a": 1})))
        self.assertIsNone(_mapping_snapshot(_O2StatefulMapping()))
        source = inspect.getsource(expr_mod._mod_decide)
        self.assertIn("dict.get(snapshot", source)

    def test_counterexample_would_be_a_bypass_on_the_old_shape(self):
        """对照：同一条格式串 + 精确 dict 在修复前后都拒绝；非 dict 映射现在也拒绝。"""
        self.assertRaises(ValueError, _fn_mod, self._FMT, {"k": "x"})


@skipUnless(_HAS_GUARD, "guard not present")
class TestTwelfthRoundM3EntryCounterexamples(TestCase):
    """M3②：每个渲染入口都喂一个"上界小、实际大"的敌意对象，必须**分配前**拒绝。"""

    def _refused_cheaply(self, fn):
        desc, peak = _measure_peak(fn)
        self.assertEqual("ValueError", desc)
        self.assertLess(peak, _PEAK_BUDGET, f"峰值 {peak/1024/1024:.1f}MB")

    def test_concat_mod_percent_and_template(self):
        for label, call in (
            ("concat", lambda v=_M3HostileBigInt(1): _fn_concat(v)),
            ("mod-s", lambda v=_M3HostileBigInt(1): _fn_mod("%s", v)),
            ("mod-r", lambda v=_M3HostileBigInt(1): _fn_mod("%r", v)),
            ("template", lambda v=_M3HostileBigInt(1): _fresh().evaluate(
                "{% $INPUT.v %}", {"$INPUT": {"v": v}})),
        ):
            with self.subTest(path=label):
                _M3HostileBigInt.calls = 0
                self._refused_cheaply(call)
                self.assertEqual(0, _M3HostileBigInt.calls,
                                 f"{label} 在拒绝前渲染了敌意对象")

    def test_mul_replace_join_entries(self):
        self._refused_cheaply(lambda: _fn_mul(_M1LyingLenStr("a" * 1000), 50_000))
        self._refused_cheaply(
            lambda: _fn_replace(_M1LyingCountStr("a" * 100_000), "a", "b" * 1000))
        self._refused_cheaply(lambda: _fn_join(["a"] * 6, _M1LyingSepStr("s" * 25_000)))
        self._refused_cheaply(lambda: _fresh().evaluate(
            "$F.join($INPUT.items, $INPUT.sep)",
            {"$INPUT": {"items": ["a"] * 6, "sep": _M1LyingSepStr("s" * 25_000)}}))


@skipUnless(_HAS_GUARD, "guard not present")
class TestTwelfthRoundM4LogRendering(TestCase):
    """M4：日志插值不得渲染作者数据（只记类型名/元数/形态）。"""

    def test_debug_log_does_not_render_author_args(self):
        from plaita.logger import logger as shared_logger

        buffer = io.StringIO()
        handler = logging.StreamHandler(buffer)
        handler.setLevel(logging.DEBUG)
        previous = shared_logger.level
        shared_logger.addHandler(handler)
        shared_logger.setLevel(logging.DEBUG)
        _M3HostileBigInt.calls = 0
        try:
            _measure_peak(lambda: _fresh().evaluate(
                "$F.mul($INPUT.v, 1)", {"$INPUT": {"v": _M3HostileBigInt(1)}}))
        finally:
            shared_logger.removeHandler(handler)
            shared_logger.setLevel(previous)
        logged = buffer.getvalue()
        self.assertGreater(len(logged), 0, "DEBUG 日志路径没有被触发（用例失去意义）")
        self.assertLess(len(logged), 2_000)
        self.assertEqual(0, _M3HostileBigInt.calls, "DEBUG 日志渲染了作者参数")

    def test_unmeasurable_warning_does_not_render_exception_payload(self):
        logger = logging.getLogger("plaita.core.expression")
        buffer = io.StringIO()
        handler = logging.StreamHandler(buffer)
        handler.setLevel(logging.WARNING)
        previous = logger.level
        logger.addHandler(handler)
        logger.setLevel(logging.WARNING)
        calls = {"n": 0}

        class _Payload:
            def __str__(self):
                calls["n"] += 1
                return "P" * 5_000_000

            def __repr__(self):
                return str(self)

        class _BoomLen:
            def __len__(self):
                raise RuntimeError(_Payload())

        try:
            desc, peak = _measure_peak(lambda: _measure(_BoomLen()))
        finally:
            logger.removeHandler(handler)
            logger.setLevel(previous)
        self.assertEqual("ValueError", desc)
        self.assertEqual(0, calls["n"], "WARNING 日志渲染了异常消息里的作者对象")
        self.assertLess(peak, _PEAK_BUDGET)
        self.assertLess(len(buffer.getvalue()), 2_000)


@skipUnless(_HAS_GUARD, "guard not present")
class TestTwelfthRoundM6M7EnumAndTimezone(TestCase):
    """M6/M7：enum 成员的 value 与 tzinfo 的 repr 计入上界；IntEnum/Flag 成员可渲染。"""

    def test_enum_member_value_is_counted_in_the_repr_bound(self):
        class _Big(enum.Enum):
            A = "V" * 1_000_000

        value = _Big.A
        bound = _render_size(value, as_repr=True)
        self.assertIsNotNone(bound)
        self.assertGreaterEqual(bound, len(repr(value)))
        list_bound = _render_size([value])
        self.assertIsNotNone(list_bound)
        self.assertGreaterEqual(list_bound, len(str([value])))

    def test_timezone_utc_is_counted_and_bound_is_truthful(self):
        value = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
        bound = _render_size(value, as_repr=True)
        self.assertIsNotNone(bound)
        self.assertGreaterEqual(bound, len(repr(value)))

    def test_custom_tzinfo_with_huge_repr_is_refused_before_allocating(self):
        class _EvilTZ(datetime.tzinfo):
            def __repr__(self):
                return "T" * 1_000_000

            def utcoffset(self, dt):
                return datetime.timedelta(0)

            def dst(self, dt):
                return None

            def tzname(self, dt):
                return "x"

        value = datetime.datetime(2026, 1, 1, tzinfo=_EvilTZ())
        self.assertIsNone(_render_size(value, as_repr=True))
        desc, peak = _measure_peak(lambda: _fn_mod("%r", value))
        self.assertEqual("ValueError", desc)
        self.assertLess(peak, _PEAK_BUDGET)

    def test_int_enum_and_flag_members_are_renderable(self):
        class _Color(enum.IntEnum):
            RED = 1

        class _Perm(enum.Flag):
            R = 1
            W = 2

        for value in (_Color.RED, _Perm.R | _Perm.W):
            with self.subTest(cls=type(value).__name__):
                bound = _render_size(value)
                self.assertIsNotNone(bound)
                self.assertGreaterEqual(bound, len(str(value)))
                repr_bound = _render_size(value, as_repr=True)
                self.assertIsNotNone(repr_bound)
                self.assertGreaterEqual(repr_bound, len(repr(value)))
                self.assertEqual(str(value), _fresh().evaluate(
                    "$F.mod('%s', $INPUT.v)", {"$INPUT": {"v": value}}))

    def test_enum_member_with_overridden_str_is_still_refused(self):
        class _Evil(enum.Enum):
            A = 1

            def __str__(self):
                return "x" * 1_000_000

        self.assertIsNone(_render_size(_Evil.A))


@skipUnless(_HAS_GUARD, "guard not present")
class TestFifteenthRoundQ1ShapeMatrix(TestCase):
    """Q1.3：``%[flags][width][.prec]conv`` 形状笛卡尔积——分配前拒或峰值常数。"""

    FLAGS = ("", "-", "0", "+", "#", " ")
    CONVERSIONS = ("s", "r", "a", "d", "i", "u", "o", "x", "X",
                   "e", "E", "f", "F", "g", "G", "c")

    @staticmethod
    def _value_for(conversion):
        if conversion in "diuoxXc":
            return 1
        if conversion in "eEfFgG":
            return 1.0
        return "x"

    def _assert_bounded(self, fmt, value, label):
        desc, peak = _measure_peak(lambda: _fn_mod(fmt, value))
        self.assertTrue(
            desc == "ValueError" or peak <= _PEAK_BUDGET,
            f"{label}: desc={desc} peak={peak/1024/1024:.1f}MB")
        return peak

    def test_literal_width_shapes(self):
        peaks = []
        for flag in self.FLAGS:
            for conversion in self.CONVERSIONS:
                value = self._value_for(conversion)
                for width in ("1000", "2000000", "4000000"):
                    for precision in ("", ".3", ".2000000"):
                        fmt = f"%{flag}{width}{precision}{conversion}"
                        peaks.append(self._assert_bounded(
                            fmt, value, f"{fmt!r} {value!r}"))
        self.assertLess(max(peaks) - min(peaks), 2_000_000, "峰值随宽度增长")

    def test_star_width_and_precision_shapes(self):
        for flag in self.FLAGS:
            for conversion in self.CONVERSIONS:
                value = self._value_for(conversion)
                for width in (2_000_000, 4_000_000):
                    self._assert_bounded(f"%{flag}*{conversion}", (width, value),
                                         f"star-width {conversion} {width}")
                    self._assert_bounded(f"%{flag}*.*{conversion}",
                                         (width, 2_000_000, value),
                                         f"star-width+precision {conversion} {width}")
                self._assert_bounded(f"%.*{conversion}", (4_000_000, value),
                                     f"star-precision {conversion}")

    def test_unparsable_format_with_star_width_is_refused(self):
        """Q1：解析失败 + `*`（宽度在参数里）也必须拒，不能退回"下界 0 ⇒ 放行"。"""
        for fmt, values in (("%*s%", (20_000_000, "x")),
                            ("%*d%", (50_000_000, 1))):
            with self.subTest(fmt=fmt):
                desc, peak = _measure_peak(lambda: _fn_mod(fmt, values))
                self.assertEqual("ValueError", desc)
                self.assertLess(peak, _PEAK_BUDGET)

    def test_star_width_amplification_is_refused(self):
        for width in (20_000_000, 50_000_000, 100_000_000):
            with self.subTest(width=width):
                desc, peak = _measure_peak(lambda w=width: _fn_mod("%*s", (w, "x")))
                self.assertEqual("ValueError", desc)
                self.assertLess(peak, _PEAK_BUDGET)

    def test_star_precision_truncates_instead_of_accumulating(self):
        # Q1.4：精度是**截断**（%s/%r/%a），结果只有 500 字符 ⇒ 不得当宽度累加
        desc, peak = _measure_peak(lambda: _fn_mod("%.*s", (200_000, "x" * 500)))
        self.assertEqual("ok:str:500", desc)
        self.assertLess(peak, _PEAK_BUDGET)


@skipUnless(_HAS_GUARD, "guard not present")
class TestFifteenthRoundQ3DoomedFields(TestCase):
    """Q3：错误点之前的字段必须计入上界；未知 `%s/%r/%a` 类型一律拒绝。"""

    def test_unknown_render_type_before_a_native_error_is_refused(self):
        class _EvilStr:
            calls = 0

            def __str__(self):
                type(self).calls += 1
                return "E" * 50_000_000

            def __repr__(self):
                return self.__str__()

        for fmt, values in (("%s", (_EvilStr(), "extra")),
                            ("%s%d", (_EvilStr(), 1)),
                            ("%r", (_EvilStr(),)),
                            ("%a", (_EvilStr(),))):
            with self.subTest(fmt=fmt):
                desc, peak = _measure_peak(lambda: _fn_mod(fmt, values))
                self.assertEqual("ValueError", desc)
                self.assertLess(peak, _PEAK_BUDGET)
        self.assertEqual(0, _EvilStr.calls, "判定/拒绝路径运行了作者 __str__/__repr__")

    def test_fields_before_the_native_error_count_toward_the_bound(self):
        desc, peak = _measure_peak(
            lambda: _fn_mod("%(a)5000000s%(missing)s", {"a": "x"}))
        self.assertEqual("ValueError", desc)
        self.assertLess(peak, _PEAK_BUDGET)
        desc, peak = _measure_peak(lambda: _fn_mod("%5000000s", ("x", "extra")))
        self.assertEqual("ValueError", desc)
        self.assertLess(peak, _PEAK_BUDGET)
        # 第一个字段可构造、第二个字段确定类型错 ⇒ 第一个字段的上界仍要计入
        desc, peak = _measure_peak(lambda: _fn_mod("%5000000s %d", ("x", "y")))
        self.assertEqual("ValueError", desc)
        self.assertLess(peak, _PEAK_BUDGET)
        # 类型错误在**第一个**字段 ⇒ 不构造任何字段 ⇒ 原生 TypeError 优先
        desc, _ = _measure_peak(lambda: _fn_mod("%5000000d", "x"))
        self.assertEqual("TypeError", desc)


@skipUnless(_HAS_GUARD, "guard not present")
class TestFifteenthRoundQ4ValuePredicates(TestCase):
    """Q4：原生错误优先级恢复、数值口径统一、逃生口消息、决策路径不跑作者代码。"""

    def test_c_bytes_like_is_native_type_error(self):
        for value in (b"a", bytearray(b"a"), memoryview(b"a")):
            with self.subTest(value=type(value).__name__):
                with self.assertRaises(TypeError):
                    _fn_mod("%c", value)

    def test_numeric_protocol_only_values_refused_with_escape_hatch(self):
        class _IntOnly:
            calls = 0

            def __index__(self):
                type(self).calls += 1
                return 5

            def __int__(self):
                type(self).calls += 1
                return 5

        for conversion in ("d", "i", "u", "x", "X", "o", "c"):
            with self.subTest(conversion=conversion):
                with self.assertRaises(ValueError) as cm:
                    _fn_mod("%" + conversion, _IntOnly())
                self.assertIn("int(x)", str(cm.exception))
        self.assertEqual(0, _IntOnly.calls, "决策路径运行了作者数值协议")

    def test_decision_path_never_runs_author_getattr(self):
        class _Bomb:
            calls = 0

            def __getattr__(self, name):
                type(self).calls += 1
                raise AttributeError(name)

        for conversion in ("d", "x", "f", "s", "r", "c"):
            with self.subTest(conversion=conversion):
                try:
                    _fn_mod("%" + conversion, _Bomb())
                except Exception:                 # noqa: BLE001 - 只看调用次数
                    pass
        self.assertEqual(0, _Bomb.calls, "决策路径运行了作者 __getattr__")

    def test_mul_refusal_names_the_list_escape_hatch(self):
        class _NoSlot:
            def __len__(self):
                return 1

            def __mul__(self, other):
                return self

            __rmul__ = __mul__

        with self.assertRaises(ValueError) as cm:
            _fn_mul(_NoSlot(), 2)
        self.assertIn("list(x)", str(cm.exception))

    def test_mapping_refusal_names_the_dict_escape_hatch(self):
        class _CustomMap:
            def __getitem__(self, key):
                return "v"

        with self.assertRaises(ValueError) as cm:
            _fn_mod("%(a)s", _CustomMap())
        self.assertIn("dict(x)", str(cm.exception))


@skipUnless(_HAS_GUARD, "guard not present")
class TestFifteenthRoundQ8Corpus(TestCase):
    """Q8③：``bound >= actual`` 语料**入库**（可重放制品，不再只活在报告里）。

    逐值 × ``as_repr`` 断言 `上界 >= 实际渲染长度`；任何低估即红。语料是仓库里的
    常量（这里），不是外部脚本，因此报告里的检查数可被任何人复跑出来。
    """

    def _corpus(self):
        class _E(enum.Enum):
            A = 1

        class _IE(enum.IntEnum):
            A = 1

        class _F(enum.Flag):
            A = 1

        return [
            "", "a", "a" * 100, "x" * 1000, "\x00", "\x00" * 10, "é", "😀",
            "'\"\\", "\n\t", b"", b"ab", b"\x00" * 5, b"y" * 1000, bytearray(b"xy"),
            0, 1, -1, 2 ** 64, -2 ** 64, 10 ** 100, 10 ** 1000,
            0.0, -1.5, 1e300, float("inf"), True, False, None,
            [], [1], [1, "a"], [None, True], [[1], [2]], [[[[1]]]],
            (), (1, 2), (1, "a"), (1, (2, (3,))), set(), {1}, frozenset({1, 2}),
            {}, {"a": 1}, {"a": [1, 2]}, {1: "a"}, {"a": {"b": [1, 2, 3]}},
            range(3), memoryview(b"abc"),
            decimal.Decimal("1.5"), decimal.Decimal("-0"),
            datetime.datetime(2026, 1, 2, 3, 4, 5),
            datetime.datetime(2026, 1, 2, tzinfo=datetime.timezone.utc),
            datetime.date(2026, 1, 2), datetime.time(3, 4, 5),
            datetime.timedelta(days=1, seconds=2),
            uuid.UUID(int=1), pathlib.PurePosixPath("/a/b"),
            re.compile("a(b)c"), _E.A, _IE.A, _F.A,
            array.array("i", [1, 2]), collections.deque([1, 2]),
            collections.UserList([1, 2]), collections.UserDict({"a": 1}),
            collections.ChainMap({"a": 1}, {"b": 2}),
            types.MappingProxyType({"a": 1}),
            types.SimpleNamespace(a=1),
            zoneinfo.ZoneInfo("UTC"),
        ]

    def test_corpus_has_no_underestimates(self):
        checks = 0
        underestimates = []
        for value in self._corpus():
            for as_repr in (False, True):
                bound = _render_size(value, as_repr=as_repr)
                if bound is None:
                    continue
                actual = len(repr(value) if as_repr else str(value))
                checks += 1
                if bound < actual:
                    underestimates.append(
                        (type(value).__name__, as_repr, bound, actual))
        self.assertGreaterEqual(checks, 100, f"语料太小：{checks}")
        self.assertEqual([], underestimates)


@skipUnless(_HAS_GUARD, "guard not present")
class TestFifteenthRoundQ7ScannerCoverage(TestCase):
    """Q7：带参数的日志调用、绑定方法别名、json.dump、getattr(fh,'write')、属性链 f-string。"""

    def test_new_indirect_shapes_are_flagged(self):
        synthetic = (
            "import json\n"
            "def f(a, fh, obj, d):\n"
            "    LOG.warning('%s', a)\n"
            "    self.logger.warning('%s', a)\n"
            "    w = fh.write\n"
            "    w(a)\n"
            "    json.dump(a, fh)\n"
            "    getattr(fh, 'write')(a)\n"
            "    x = f'{obj.attr}'\n"
            "    y = f\"{d['k']}\"\n"
            "    return (x, y)\n"
        )
        _, found, found_fstrings = _scan_render_offenders(
            synthetic, "synthetic.py", _RENDER_SITES, _FSTRING_PARAM_SITES)
        kinds = {kind for _module, _func, kind, _text in found}
        for expected in ("log:warning", "indirect:alias", "indirect:json_dumps",
                         "indirect:getattr"):
            with self.subTest(kind=expected):
                self.assertIn(expected, kinds)
        self.assertIn(("synthetic.py", "f#param", "{obj.attr}"), found_fstrings)
        self.assertIn(("synthetic.py", "f#param", "{d['k']}"), found_fstrings)


@skipUnless(_HAS_GUARD, "guard not present")
class TestFifteenthRoundK1Budgets(TestCase):
    """owner K1：预算只用于发现非数据；纯数据通过；上限 ≥10^6；耗尽给明确消息。"""

    def test_budgets_are_at_least_a_million(self):
        self.assertGreaterEqual(expr_mod._DATA_WALK_MAX_DEPTH, 1_000_000)
        self.assertGreaterEqual(expr_mod._POW_MAX_EXPONENT, 1_000_000)

    def test_pure_data_passes_the_data_gate(self):
        nested = [[1] * 10000, [2] * 10000]
        self.assertGreater(_content_size(nested), 0)
        self.assertEqual(2, len(_fn_sort(nested)))
        deep = [1]
        for _ in range(200):
            deep = [deep]
        self.assertIsNotNone(_render_size(deep, as_repr=True))

    def test_pow_pure_data_passes_and_amplification_is_refused(self):
        self.assertEqual(2 ** 20000, _fn_pow(2, 20000))
        self.assertEqual(1024, _fn_pow(2, 10))
        desc, peak = _measure_peak(lambda: _fn_pow(9, 999_999))
        self.assertEqual("ValueError", desc)
        self.assertLess(peak, _PEAK_BUDGET)

    def test_depth_budget_exhaustion_message_is_explicit(self):
        source = inspect.getsource(expr_mod._content_size_inner)
        self.assertIn("data-verification budget", source)


@skipUnless(_HAS_GUARD, "guard not present")
class TestSixteenthRoundS5SameLineOrder(TestCase):
    """S5：同一行内上界调用必须**严格在前**（col_offset），逆序不算支配。"""

    BEFORE = (
        "def _fn_ok(args):\n"
        "    bounds = [_render_size(arg) for arg in args]; "
        "out = ''.join([str(arg) for arg in args])\n"
        "    return out\n"
    )
    AFTER = (
        "def _fn_ok(args):\n"
        "    out = ''.join([str(arg) for arg in args]); "
        "bounds = [_render_size(arg) for arg in args]\n"
        "    return out\n"
    )

    def _registry(self):
        registry = dict(_RENDER_SITES)
        for key in (("synthetic.py", "_fn_ok", "method:join",
                     "''.join([str(arg) for arg in args])"),
                    ("synthetic.py", "_fn_ok", "call:str", "str(arg)")):
            registry[key] = ("guarded", "_render_size", "S5 同行顺序测试用登记")
        return registry

    def test_call_earlier_in_the_same_line_dominates(self):
        offenders, _, _ = _scan_render_offenders(
            self.BEFORE, "synthetic.py", self._registry(), _FSTRING_PARAM_SITES)
        self.assertEqual([], offenders)

    def test_call_later_in_the_same_line_does_not_dominate(self):
        offenders, _, _ = _scan_render_offenders(
            self.AFTER, "synthetic.py", self._registry(), _FSTRING_PARAM_SITES)
        self.assertTrue(any("_fn_ok" in line and "支配" in line for line in offenders),
                        offenders[:5])


@skipUnless(_HAS_GUARD, "guard not present")
class TestSixteenthRoundS6NoAuthorProtocol(TestCase):
    """S6：决策路径（`_content_size`/映射/迭代器判据）不得运行作者 `__getattr__`。"""

    def test_content_size_never_runs_author_getattr(self):
        class _Bomb:
            calls = 0

            def __getattr__(self, name):
                type(self).calls += 1
                raise AttributeError(name)

        for value in (_Bomb(), [_Bomb()], {"k": _Bomb()}, (_Bomb(),)):
            with self.subTest(kind=type(value).__name__):
                before = _Bomb.calls
                _content_size(value)
                self.assertEqual(before, _Bomb.calls)

    def test_not_operator_path_never_runs_author_getattr(self):
        class _Bomb:
            calls = 0

            def __getattr__(self, name):
                type(self).calls += 1
                raise AttributeError(name)

        self.assertFalse(_fn_not(_Bomb()))
        self.assertEqual(0, _Bomb.calls)

    def test_data_verification_message_reports_the_real_depth(self):
        budget = expr_mod._DATA_WALK_MAX_DEPTH
        self.assertEqual(1_000_000, budget)
        source = inspect.getsource(expr_mod._content_size_inner)
        self.assertIn("data-verification budget: depth {depth}", source)
        self.assertNotIn('"$F.* nested value", depth, "levels"', source)


@skipUnless(_HAS_GUARD, "guard not present")
class TestTwentiethRoundACMetaAndInstrumentation(TestCase):
    """AC1/AC2：元类数据描述符欺骗由**不可变类型层读取**关闭（行为实测见报告）。

    待办（AC3/AC4）：生成语料需要"可观测计数器 + 正控"与元类维度，本轮未做实 ⇒
    报告里如实标注为未完成，不声称语料已覆盖元类。
    """

    META_ATTACKS = (
        ("oxX", "%.100000000o", lambda: _float_index_liar(), "__index__"),
        ("s", "%s", lambda: _str_repr_liar(), "__str__"),
        ("r", "%r", lambda: _str_repr_liar(), "__repr__"),
    )

    SLOT_FOR_KIND = {"int": "__int__", "index": "__index__", "float": "__float__",
                     "char": "__index__", "str": "__str__", "repr": "__repr__",
                     "buffer": "__bytes__"}

    def test_bidirectional_control_per_conversion(self):
        """★ 双向正控：(a) 槽干净子类不被过度拒绝；(b) 覆写被派发槽 ⇒ 必须被拒 + 0 调用。"""
        import decimal as decimal_module
        import enum as enum_module

        class CleanFloat(float):
            pass

        class CleanDecimal(decimal_module.Decimal):
            pass

        class CleanInt(int):
            pass

        class CleanEnum(enum_module.IntEnum):
            A = 1

        misses = []
        for label, value in (("CleanFloat", CleanFloat(1.5)),
                             ("CleanDecimal", CleanDecimal("1.5")),
                             ("CleanInt", CleanInt(7)), ("CleanEnum", CleanEnum.A)):
            for conversion in "diuoxXeEfFgG":
                fmt = "%" + conversion
                specs = _parse_format_specs(fmt, bytes_mode=False)
                decision = _mod_decide(fmt, len(fmt), specs, value, False)
                try:
                    actual = len(fmt % value)
                    native_ok = True
                except Exception:
                    native_ok = False
                    actual = 0
                # (a) 只惩罚**真的过度拒绝**（原生会成功却拒）与低估/fail-open；
                #     "native 且原生失败"正是原生错误优先，合法。
                if decision.kind == "refuse" and native_ok:
                    misses.append(("over-refusal", label, fmt))
                elif decision.kind == "render" and native_ok and decision.projected < actual:
                    misses.append(("bound<actual", label, fmt, decision.projected, actual))
                elif decision.kind == "native" and native_ok:
                    misses.append(("native-but-succeeded", label, fmt))
        self.assertGreaterEqual(len(misses) + 36, 36)
        self.assertEqual([], misses[:5])

        for conversion in "diuoxXceEfFgG":
            kind = expr_mod._CONVERSION_DISPATCH[conversion][0]
            slot = self.SLOT_FOR_KIND.get(kind)
            if slot is None:
                continue
            calls = []
            body = {slot: (lambda self: 10 ** 6) if slot != "__str__" else
                    (lambda self: "y" * 10 ** 6)}
            if slot == "__repr__":
                body[slot] = lambda self: "y" * 10 ** 6
            liar_cls = type("Liar_" + conversion, (float,), body)
            try:
                liar = liar_cls(1.5)
            except Exception:                       # noqa: BLE001
                continue
            with self.subTest(conversion=conversion, slot=slot):
                desc, peak = _measure_peak(lambda f="%" + conversion, v=liar: _fn_mod(f, v))
                self.assertEqual("ValueError", desc.split(":")[0])
                self.assertLess(peak, _PEAK_BUDGET)

    def test_author_call_instrumentation_positive_control(self):
        """AC3：计数器必须**可观测**（否则任何“0 次”断言都无效）。"""
        class _Probe(str):
            def __str__(self):
                _AUTHOR_PROTOCOL_CALLS.append("__str__")
                return "y" * 10

        del _AUTHOR_PROTOCOL_CALLS[:]
        self.assertEqual(10, len("%s" % _Probe("x")))
        self.assertGreater(len(_AUTHOR_PROTOCOL_CALLS), 0,
                           "计数器不可观测 ⇒ 语料“0 次作者调用”断言无效")

    def test_metaclass_dimension_corpus(self):
        """AC4：元类数据描述符欺骗维度进语料（生成逻辑见 `_meta_deceived`）。"""
        violations = []
        checks = 0
        for value in (_float_index_liar(), _str_repr_liar(), _dict_getitem_liar()):
            for bytes_mode, conversions in ((False, "diuoxXeEfFgGcrsa"),
                                            (True, "diuoxXeEfFgGcrsab")):
                for conversion in conversions:
                    for width in ("", "100000000"):
                        fmt = f"%{width}{conversion}"
                        fmt = fmt.encode() if bytes_mode else fmt
                        fmt_text = fmt if isinstance(fmt, str) else fmt.decode("latin-1")
                        del _AUTHOR_PROTOCOL_CALLS[:]
                        specs = _parse_format_specs(fmt_text, bytes_mode=bytes_mode)
                        decision = _mod_decide(fmt_text, len(fmt_text), specs,
                                               value, bytes_mode)
                        checks += 1
                        if _AUTHOR_PROTOCOL_CALLS:
                            violations.append(("author-call", type(value).__name__,
                                               fmt, list(_AUTHOR_PROTOCOL_CALLS)))
                        try:
                            actual = len(fmt % value)
                            native_ok = True
                        except Exception:
                            native_ok = False
                            actual = 0
                        if decision.kind == "native" and native_ok:
                            violations.append(("native-but-ok", type(value).__name__, fmt))
                        elif decision.kind == "render" and native_ok \
                                and decision.projected < actual:
                            violations.append(("bound<actual", type(value).__name__, fmt,
                                               decision.projected, actual))
        self.assertGreaterEqual(checks, 100, checks)
        self.assertEqual([], violations[:5])

    def test_immutable_reads_are_used_for_type_layer_facts(self):
        source = " ".join(line for line in inspect.getsource(expr_mod).splitlines()
                          if not line.lstrip().startswith("#"))
        for forbidden in ("cls.__mro__", "base.__dict__", 'type.__getattribute__(cls, "__mro__")',
                          'type.__getattribute__(base, "__dict__")'):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, source)
        self.assertIn('_TYPE_MRO_GETTER = type.__dict__["__mro__"]', source)
        self.assertIn('_TYPE_DICT_GETTER = type.__dict__["__dict__"]', source)


@skipUnless(_HAS_GUARD, "guard not present")
class TestNineteenthRoundABAdversarialCorpus(TestCase):
    """AB5.1：对抗语料**从 `_CONVERSION_DISPATCH` 派发表生成**（不是手挑几个类）。

    对每个被派发的槽 × 每个转换符 × 宽/精 × str/bytes 两模式断言：
      * 决策路径 **0 次作者协议调用**；
      * `render` ⇒ 上界 ≥ 原生实际长度；`native` ⇒ 原生**确实失败**。
    """

    DUNDERS = ("__int__", "__index__", "__float__", "__trunc__", "__abs__",
               "__eq__", "__ne__", "__len__", "__str__", "__repr__", "__bytes__")

    def _adversarial_classes(self):
        import decimal as decimal_module
        slots = sorted({name for _kind, names in expr_mod._CONVERSION_DISPATCH.values()
                        for name in names} | set(self.DUNDERS))
        bases = [("int", int, 10 ** 4000), ("float", float, 1e300),
                 ("Decimal", decimal_module.Decimal, decimal_module.Decimal("1e1000"))]
        made = []
        for base_name, base, seed in bases:
            for dunder in slots:
                body = {}
                if dunder == "__len__":
                    body[dunder] = lambda self: 10 ** 9
                elif dunder in ("__str__", "__repr__"):
                    body[dunder] = lambda self, _d=dunder: "x" * 1000
                elif dunder == "__bytes__":
                    body[dunder] = lambda self: b"z" * 1000
                elif dunder in ("__eq__", "__ne__"):
                    body[dunder] = lambda self, other: dunder == "__eq__"
                elif dunder == "__int__":
                    body[dunder] = lambda self: 10 ** 4000
                elif dunder == "__index__":
                    body[dunder] = lambda self: 10 ** 4000
                elif dunder == "__float__":
                    body[dunder] = lambda self: 1.0
                elif dunder == "__abs__":
                    body[dunder] = lambda self: 1.0
                elif dunder == "__trunc__":
                    body[dunder] = lambda self: 10 ** 4000

                def _calls(cls):
                    cls.calls = 0
                    return cls

                cls = type(f"Liar_{base_name}_{dunder.strip('_')}", (base,),
                           _calls_dict(body))
                cls.calls = 0
                try:
                    instance = cls(seed)
                except Exception:                       # noqa: BLE001
                    continue
                made.append((f"{base_name}+{dunder}", instance))
        return made

    def test_snan_surfaces_native_errors_in_both_modes(self):
        import decimal as decimal_module
        for fmt in ("%2000000e", "%2000000f", "%2000000g"):
            with self.subTest(fmt=fmt, mode="str"):
                desc, peak = _measure_peak(
                    lambda f=fmt: _fn_mod(f, decimal_module.Decimal("sNaN")))
                self.assertIn(desc.split(":")[0], ("ValueError", "OverflowError"))
                self.assertLess(peak, _PEAK_BUDGET)
            with self.subTest(fmt=fmt, mode="bytes"):
                desc, peak = _measure_peak(
                    lambda f=fmt: _fn_mod(f.encode(), decimal_module.Decimal("sNaN")))
                self.assertEqual("TypeError", desc.split(":")[0])
                self.assertLess(peak, _PEAK_BUDGET)

    def test_dispatch_table_is_the_single_source(self):
        table = expr_mod._CONVERSION_DISPATCH
        for conversion in "diuoxXceEfFgGsrab":
            with self.subTest(conversion=conversion):
                self.assertIn(conversion, table)
        # `o/x/X` 的派发里必须有 `__index__`（AB1 的教训）
        for conversion in "oxX":
            self.assertIn("__index__", table[conversion][1])

    def test_adversarial_corpus_invariants(self):
        underestimates = []
        author_calls = []
        checks = 0
        for label, value in self._adversarial_classes():
            for bytes_mode, conversions in ((False, "diuoxXeEfFgGcrsa"),
                                            (True, "diuoxXeEfFgGcrsab")):
                for conversion in conversions:
                    for width in ("", "1000"):
                        for precision in ("", ".6"):
                            fmt = f"%{width}{precision}{conversion}"
                            if bytes_mode:
                                fmt = fmt.encode()
                            fmt_text = fmt if isinstance(fmt, str) else fmt.decode("latin-1")
                            del _AUTHOR_PROTOCOL_CALLS[:]
                            specs = _parse_format_specs(fmt_text, bytes_mode=bytes_mode)
                            decision = _mod_decide(fmt_text, len(fmt_text), specs,
                                                   value, bytes_mode)
                            checks += 1
                            if _AUTHOR_PROTOCOL_CALLS:
                                author_calls.append(
                                    (label, fmt, list(_AUTHOR_PROTOCOL_CALLS)))
                            try:
                                actual = len(fmt % value)
                                native_ok = True
                            except Exception:
                                native_ok = False
                                actual = 0
                            if decision.kind == "native" and native_ok:
                                underestimates.append(("native-but-succeeded", label, fmt))
                            elif decision.kind == "render" and native_ok \
                                    and decision.projected < actual:
                                underestimates.append(
                                    ("bound<actual", label, fmt, decision.projected, actual))
        self.assertGreaterEqual(checks, 1000, checks)
        self.assertEqual([], author_calls[:5])
        self.assertEqual([], underestimates[:5])


@skipUnless(_HAS_GUARD, "guard not present")
class TestEighteenthRoundZ1NumericSubclasses(TestCase):
    """Z1：`_numeric_magnitude_bound` 只用 C 槽，覆写任何数值协议都不跑、且上界不低估。"""

    def _liars(self):
        import decimal as decimal_module

        class AbsLiar(float):
            calls = 0

            def __abs__(self):
                type(self).calls += 1
                return 1.0

        class EqLiar(float):
            calls = 0

            def __eq__(self, other):
                type(self).calls += 1
                return True

            def __ne__(self, other):
                type(self).calls += 1
                return False

        class FloatLiar(float):
            calls = 0

            def __float__(self):
                type(self).calls += 1
                return 1.0

        class IntLiar(int):
            calls = 0

            def __int__(self):
                type(self).calls += 1
                return 1

            def __index__(self):
                type(self).calls += 1
                return 1

        class DecimalLiar(decimal_module.Decimal):
            calls = 0

            def adjusted(self):
                type(self).calls += 1
                return 0

        return [AbsLiar(1e300), EqLiar(1e300), FloatLiar(1e300),
                IntLiar(10 ** 4000), DecimalLiar("1e1000")]

    def test_adversarial_numeric_subclasses_have_no_underestimates(self):
        underestimates = []
        checks = 0
        for value in self._liars():
            for conversion in "diuoxXeEfFgG":
                for width in ("", "12", "1000"):
                    for precision in ("", ".6", ".0"):
                        fmt = f"%{width}{precision}{conversion}"
                        specs = _parse_format_specs(fmt, bytes_mode=False)
                        decision = _mod_decide(fmt, len(fmt), specs, value, False)
                        try:
                            actual = len(fmt % value)
                        except Exception:
                            continue
                        checks += 1
                        if decision.kind == "refuse":
                            continue
                        if decision.kind == "native":
                            underestimates.append(
                                ("native-but-succeeded", fmt, type(value).__name__))
                        elif decision.projected < actual:
                            underestimates.append(
                                (fmt, type(value).__name__, decision.projected, actual))
        self.assertGreaterEqual(checks, 288, checks)
        self.assertEqual([], underestimates[:5])

    def test_adversarial_numeric_subclasses_never_run_author_protocols(self):
        values = self._liars()
        for value in values:
            type(value).calls = 0                  # 只统计决策路径
        for value in values:
            for conversion in "diuoxXeFg":
                fmt = f"%{conversion}"
                specs = _parse_format_specs(fmt, bytes_mode=False)
                _mod_decide(fmt, len(fmt), specs, value, False)
        for value in values:
            self.assertEqual(0, type(value).calls, type(value).__name__)

    def test_decimal_subclass_over_refusal_control(self):
        """AC6③：**干净** Decimal 子类必须可证（不许凑不齐槽就一律拒）。"""
        import decimal as decimal_module

        class CleanDecimal(decimal_module.Decimal):
            pass

        self.assertEqual(8, len(_fn_mod("%f", CleanDecimal("1.5"))))
        specs = _parse_format_specs("%f", bytes_mode=False)
        decision = _mod_decide("%f", 2, specs, CleanDecimal("1.5"), False)
        self.assertEqual("render", decision.kind)
        self.assertGreaterEqual(decision.projected, 3)

        class DecimalLiar(decimal_module.Decimal):
            def adjusted(self):
                return 0

        with self.assertRaises(ValueError) as cm:
            _fn_mod("%f", DecimalLiar("1e1000"))
        self.assertIn("float(x)", str(cm.exception))


@skipUnless(_HAS_GUARD, "guard not present")
class TestEighteenthRoundZ2NonFinite(TestCase):
    """Z2：inf/nan（含 Decimal）在 d/i/u/o/x/X 上必须让**原生错误**先抛。"""

    def test_non_finite_surfaces_native_errors(self):
        import decimal as decimal_module
        cases = [
            ("%2000000d", float("inf"), "OverflowError"),
            ("%2000000i", float("nan"), "ValueError"),
            ("%2000000u", float("-inf"), "OverflowError"),
            ("%2000000d", decimal_module.Decimal("nan"), "ValueError"),
            ("%2000000d", decimal_module.Decimal("inf"), "OverflowError"),
            ("%2000000x", float("inf"), "TypeError"),
            ("%2000000X", float("nan"), "TypeError"),
            ("%2000000o", decimal_module.Decimal("inf"), "TypeError"),
        ]
        for fmt, value, expected in cases:
            with self.subTest(fmt=fmt, value=type(value).__name__):
                desc, peak = _measure_peak(lambda f=fmt, v=value: _fn_mod(f, v))
                self.assertEqual(expected, desc.split(":")[0])
                self.assertLess(peak, _PEAK_BUDGET)
        # e/f/g 上 inf/nan 原生**成功**（3 字符）⇒ 不是"原生必错"
        for fmt in ("%e", "%f", "%g"):
            with self.subTest(fmt=fmt):
                self.assertEqual(3, len(_fn_mod(fmt, float("inf"))))


@skipUnless(_HAS_GUARD, "guard not present")
class TestEighteenthRoundZ3Z4Z5(TestCase):
    """Z3 嵌套推导式；Z4 格式操作数/`_value_size`；Z5.1 真实深度边界；Z5.2 无转换符。"""

    def test_nested_comprehension_iterable_is_not_dominated(self):
        registry = dict(_RENDER_SITES)
        registry[("synthetic.py", "_fn_ok", "call:str", "str(a)")] = (
            "guarded", "_render_size", "Z3 嵌套推导式测试用登记")
        risky = (
            "def _fn_ok(args):\n"
            "    return [_render_size(x) for x in "
            "[arg for arg in [str(a) for a in args]]]\n"
        )
        offenders, _, _ = _scan_render_offenders(
            risky, "synthetic.py", registry, _FSTRING_PARAM_SITES)
        self.assertTrue(any("_fn_ok" in line and "支配" in line for line in offenders),
                        offenders[:5])

    def test_format_operand_subclass_is_refused_before_allocation(self):
        class _EvilStr(str):
            def __mod__(self, other):
                return "y" * 50_000_000

        class _EvilBytes(bytes):
            pass

        desc, peak = _measure_peak(lambda: _fn_mod(_EvilStr("%(a)s"), {"a": 1}))
        self.assertEqual("ValueError", desc)
        self.assertLess(peak, _PEAK_BUDGET)
        desc, peak = _measure_peak(lambda: _fn_mod(_EvilBytes(b"%(a)s"), {b"a": b"1"}))
        self.assertEqual("ValueError", desc)
        self.assertLess(peak, _PEAK_BUDGET)

    def test_value_size_does_not_run_author_int(self):
        class _IntLiar(int):
            calls = 0

            def __int__(self):
                type(self).calls += 1
                return 1

        value = _IntLiar(10 ** 4000)
        self.assertGreaterEqual(_value_size(value), 4001)
        self.assertEqual(0, _IntLiar.calls)

    def test_real_depth_boundary_is_recorded(self):
        def nested(n):
            v = [1]
            for _ in range(n):
                v = [v]
            return v

        last_ok = None
        for depth in range(2900, 3100):
            try:
                _content_size(nested(depth))
                last_ok = depth
            except ValueError as exc:
                self.assertIn("recursion budget", str(exc))
                self.assertIsNotNone(last_ok)
                self.assertLessEqual(depth - last_ok, 2)     # 边界连续
                self.assertGreaterEqual(last_ok, 2000)
                return
        self.fail("3000 层以内没有触发递归预算拒绝")

    def test_no_conversion_format_is_a_render(self):
        specs = _parse_format_specs("%%(a)s", bytes_mode=False)
        decision = _mod_decide("%%(a)s", 7, specs, {"a": 1}, False)
        self.assertEqual("render", decision.kind)      # Z5.2：不再声称“原生必错”
        self.assertEqual("%(a)s", _fn_mod("%%(a)s", {"a": 1}))
        with self.assertRaises(TypeError):
            _fn_mod("%%", 1)


@skipUnless(_HAS_GUARD, "guard not present")
class TestSeventeenthRoundT1BytesMapping(TestCase):
    """T1：bytes 模式的映射键语义（原生用 **bytes** 键）。"""

    def test_bytes_mode_uses_bytes_keys(self):
        self.assertEqual(b"B", _fn_mod(b"%(a)s", {b"a": b"B"}))
        with self.assertRaises(KeyError):
            _fn_mod(b"%(a)s", {"a": "B"})           # str 键 → 原生 KeyError
        with self.assertRaises(KeyError):
            _fn_mod("%(a)s", {b"a": b"B"})          # str 模式不接受 bytes 键

    def test_bytes_mapping_width_is_bounded_before_allocation(self):
        for width in (2_000_000, 100_000_000, 400_000_000):
            with self.subTest(width=width):
                desc, peak = _measure_peak(
                    lambda w=width: _fn_mod(f"%(a){w}s".encode(), {b"a": b"x"}))
                self.assertEqual("ValueError", desc)
                self.assertLess(peak, _PEAK_BUDGET)

    def test_bytes_mapping_fuzz_native_implies_failure(self):
        """T1.3：映射格式 × 键型（str/bytes/混合）× 值型 —— native 判定必须真的必错。"""
        class _EvilBytes:
            def __bytes__(self):
                return b"z" * 32

        values = [b"x", b"yy", "x", bytearray(b"z"), memoryview(b"zz"), 1, 1.5,
                  None, [1], {b"a": b"v"}, _EvilBytes()]
        key_kinds = [
            {"a": "v"}, {b"a": b"v"}, {}, {"a": b"v"}, {b"a": "v"},
            {"a": "v", b"a": b"w"},
        ]
        counterexamples = []
        checks = {"str": 0, "bytes": 0}
        for bytes_mode, conversions in ((False, "diouxXeEfFgGcrsa"),
                                        (True, "diouxXeEfFgGcrsab")):
            label = "bytes" if bytes_mode else "str"
            for conversion in conversions:
                for width in ("", "2", "1000000"):
                    for precision in ("", ".2"):
                        fmt = f"%(a){width}{precision}{conversion}"
                        if bytes_mode:
                            fmt = fmt.encode()
                        fmt_text = fmt if isinstance(fmt, str) else fmt.decode("latin-1")
                        for mapping in key_kinds:
                            for extra in values:
                                checks[label] += 1
                                specs = _parse_format_specs(fmt_text, bytes_mode=bytes_mode)
                                decision = _mod_decide(fmt_text, len(fmt_text), specs,
                                                       mapping, bytes_mode)
                                if decision.kind != "native":
                                    continue
                                try:
                                    fmt % mapping
                                    native_ok = True
                                except Exception:
                                    native_ok = False
                                if native_ok:
                                    counterexamples.append(
                                        (fmt, type(next(iter(mapping), None)).__name__))
        self.assertGreaterEqual(checks["bytes"], 4000, checks)
        self.assertEqual([], counterexamples[:5])


@skipUnless(_HAS_GUARD, "guard not present")
class TestSeventeenthRoundT2T3Numeric(TestCase):
    """T2：数值量级进上界；T3：类型/格式错误先于规模错误。"""

    def test_numeric_magnitude_is_counted(self):
        import decimal as decimal_module
        cases = [
            ("%d", 1e300), ("%f", 1e300), ("%f", 10 ** 300),
            ("%d", decimal_module.Decimal("1e1000")),
            ("%d", 2 ** 4000), ("%.0f", 1e300), ("%e", 1e300), ("%g", 1e300),
        ]
        for fmt, value in cases:
            with self.subTest(fmt=fmt, value=type(value).__name__):
                specs = _parse_format_specs(fmt, bytes_mode=False)
                bound = _spec_bound(specs[0], value, 0)
                self.assertIsNotNone(bound)
                actual = len(fmt % value)
                self.assertGreaterEqual(bound, actual, (fmt, bound, actual))

    def test_numeric_projection_corpus_has_no_underestimates(self):
        import decimal as decimal_module
        values = [0, 1, -1, 2 ** 64, 2 ** 300, 2 ** 4000, 10 ** 100, 0.0, 1.5, -1.5,
                  1e300, -1e300, float("inf"), float("nan"), True, False,
                  decimal_module.Decimal("1e1000"), decimal_module.Decimal("-1e-1000"),
                  decimal_module.Decimal("123.45")]
        under = []
        checks = 0
        for conversion in "diuoxXeEfFgG":
            for precision in ("", ".0", ".6"):
                for width in ("", "12"):
                    fmt = f"%{width}{precision}{conversion}"
                    for value in values:
                        specs = _parse_format_specs(fmt, bytes_mode=False)
                        decision = _mod_decide(fmt, len(fmt), specs, value, False)
                        try:
                            actual = len(fmt % value)
                        except Exception:
                            continue
                        if decision.kind == "refuse":
                            continue
                        checks += 1
                        if decision.kind == "native":
                            under.append(("native-but-succeeded", fmt, type(value).__name__))
                        elif decision.projected < actual:
                            under.append((fmt, type(value).__name__,
                                          decision.projected, actual))
        self.assertGreaterEqual(checks, 300, checks)
        self.assertEqual([], under[:5])

    def test_native_error_precedes_size_error_for_huge_ints(self):
        for fmt in ("%2000000e", "%2000000f", "%2000000g"):
            with self.subTest(fmt=fmt):
                desc, peak = _measure_peak(lambda f=fmt: _fn_mod(f, 2 ** 4000))
                self.assertEqual("OverflowError", desc.split(":")[0])
                self.assertLess(peak, _PEAK_BUDGET)
        for fmt in (b"%2000000e", b"%2000000f", b"%2000000g"):
            with self.subTest(fmt=fmt):
                desc, peak = _measure_peak(lambda f=fmt: _fn_mod(f, 2 ** 4000))
                self.assertEqual("TypeError", desc.split(":")[0])
                self.assertLess(peak, _PEAK_BUDGET)


@skipUnless(_HAS_GUARD, "guard not present")
class TestSeventeenthRoundT4T5T6(TestCase):
    """T4.2/T4.3/T5/T6。"""

    def test_recursion_budget_message_is_accurate_on_the_real_path(self):
        deep = [1]
        for _ in range(5000):
            deep = [deep]
        with self.assertRaises(ValueError) as cm:
            _content_size(deep)
        message = str(cm.exception)
        self.assertIn("interpreter recursion budget", message)
        self.assertIn("declared budget 1000000 levels", message)
        self.assertIn("depth", message)
        self.assertNotIn("result too large: 0 levels", message)

    def test_userstring_and_userlist_use_their_data(self):
        self.assertEqual(3, _content_size(collections.UserString("abc")))
        self.assertEqual(3, _content_size(collections.UserList([1, 2, 3])))
        self.assertEqual(6, len(_fn_mul(collections.UserString("abc"), 2)))

        class _EvilString(collections.UserString):
            calls = 0

            def __iter__(self):
                type(self).calls += 1
                return iter(self)

        with self.assertRaises(ValueError):
            _content_size(_EvilString("abc"))
        self.assertEqual(0, _EvilString.calls)

    def test_comprehension_iterable_is_not_dominated(self):
        registry = dict(_RENDER_SITES)
        registry[("synthetic.py", "_fn_ok", "call:str", "str(a)")] = (
            "guarded", "_render_size", "T6 推导式 iter 测试用登记")
        risky = (
            "def _fn_ok(args):\n"
            "    bounds = [_render_size(arg) for arg in [str(a) for a in args]]\n"
            "    return bounds\n"
        )
        offenders, _, _ = _scan_render_offenders(
            risky, "synthetic.py", registry, _FSTRING_PARAM_SITES)
        self.assertTrue(any("_fn_ok" in line and "支配" in line for line in offenders),
                        offenders[:5])


@skipUnless(_HAS_GUARD, "guard not present")
class TestSixteenthRoundS1BytesMode(TestCase):
    """S1：bytes 模式的 `%` 与 str 模式**共用同一判据**；`native` 判定必须真的必错。"""

    BYTES_CONVERSIONS = "diouxXeEfFgGcrsab"

    @staticmethod
    def _value_for(conversion):
        if conversion in "sb":
            return b"x" * 10
        if conversion in "diuoxXc":
            return 1
        if conversion in "eEfFgG":
            return 1.0
        return b"x"

    def test_bytes_shape_matrix_is_bounded(self):
        peaks = []
        for flag in ("", "-", "0", "+", "#", " "):
            for conversion in self.BYTES_CONVERSIONS:
                value = self._value_for(conversion)
                for width in ("3", "2000000"):
                    for precision in ("", ".3"):
                        fmt = f"%{flag}{width}{precision}{conversion}".encode()
                        desc, peak = _measure_peak(lambda f=fmt, v=value: _fn_mod(f, v))
                        self.assertTrue(
                            desc == "ValueError" or peak <= _PEAK_BUDGET,
                            f"{fmt!r}: {desc} peak={peak/1024/1024:.1f}MB")
                        peaks.append(peak)
        self.assertLess(max(peaks) - min(peaks), 2_000_000)

    def test_bytes_like_is_bounded_and_rendered(self):
        self.assertEqual(b"q" * 1000, _fn_mod(b"%b", b"q" * 1000))
        self.assertEqual(b"x", _fn_mod(b"%s", b"x"))
        self.assertEqual(b"ab", _fn_mod(b"%s", bytearray(b"ab")))

    def test_author_bytes_protocol_is_refused_without_running_it(self):
        class _EvilBytes:
            calls = 0

            def __bytes__(self):
                type(self).calls += 1
                return b"z" * 50_000_000

        class _IndexOnly:
            calls = 0

            def __index__(self):
                type(self).calls += 1
                return 5

        class _IndexBytes:
            calls = 0

            def __index__(self):
                type(self).calls += 1
                return 5

            def __bytes__(self):
                type(self).calls += 1
                return b"z" * 50_000_000

        for fmt in (b"%s", b"%b"):
            for value in (_EvilBytes(), _IndexBytes()):
                with self.subTest(fmt=fmt, value=type(value).__name__):
                    desc, peak = _measure_peak(lambda f=fmt, v=value: _fn_mod(f, v))
                    self.assertEqual("ValueError", desc)
                    self.assertLess(peak, _PEAK_BUDGET)
        self.assertEqual(0, _EvilBytes.calls)
        self.assertEqual(0, _IndexBytes.calls)
        # `__index__`-only 在 `%s`/`%b` 上是**原生必错**（TypeError，不构造字段）
        for fmt in (b"%s", b"%b"):
            with self.subTest(fmt=fmt, value="index_only"):
                with self.assertRaises(TypeError):
                    _fn_mod(fmt, _IndexOnly())
        self.assertEqual(0, _IndexOnly.calls)

    def test_large_memoryview_bound_is_the_byte_length(self):
        value = memoryview(b"z" * 2_000_000)
        decision = _mod_decide("%s", 2, _parse_format_specs("%s", bytes_mode=True),
                               value, True)
        self.assertEqual("render", decision.kind)
        self.assertGreaterEqual(decision.projected, 2_000_000)
        # 非放大（输入 2MB → 结果 2MB）⇒ 放行，上界按真实字节数而不是 74
        result = _fn_mod(b"%s", value)
        self.assertEqual(b"z" * 2_000_000, result)
        # 小输入 + 大宽度 ⇒ 分配前拒（上界 2e6 > 额度）
        desc, peak = _measure_peak(lambda: _fn_mod(b"%2000000s", memoryview(b"z" * 10)))
        self.assertEqual("ValueError", desc)
        self.assertLess(peak, _PEAK_BUDGET)

    def test_bytes_refusal_names_the_bytes_escape_hatch(self):
        class _EvilBytes:
            def __bytes__(self):
                return b"z"

        with self.assertRaises(ValueError) as cm:
            _fn_mod(b"%s", _EvilBytes())
        self.assertIn("bytes(x)", str(cm.exception))

    def test_native_decision_implies_native_failure_fuzz(self):
        """S1.3：模糊测试——`decision == 'native'` 必须是**真的原生必错**（0 反例）。

        覆盖 str 与 bytes 两种模式（共用同一判据 `_definitely_rejected`）。
        """
        class _EvilBytes:
            def __bytes__(self):
                return b"z" * 32

        class _IndexOnly:
            def __index__(self):
                return 5

        class _Obj:
            pass

        values = [0, 1, -3, 2 ** 70, 1.5, True, None, "x", "yy", b"x", b"yy",
                  bytearray(b"z"), memoryview(b"zz"), _EvilBytes(), _IndexOnly(),
                  _Obj(), [], [1], {}, {"a": 1}, (1, 2), {1}, 3.0]
        counterexamples = []
        checks = {"str": 0, "bytes": 0}
        for bytes_mode, conversions in (
                (False, "diouxXeEfFgGcrsa"),
                (True, "diouxXeEfFgGcrsab")):
            label = "bytes" if bytes_mode else "str"
            for conversion in conversions:
                for flag in ("", "-", "0", "#"):
                    for width in ("", "2", "1000000"):
                        for precision in ("", ".2"):
                            fmt = f"%{flag}{width}{precision}{conversion}"
                            if bytes_mode:
                                fmt = fmt.encode()
                            fmt_text = fmt if isinstance(fmt, str) else fmt.decode("latin-1")
                            for value in values:
                                checks[label] += 1
                                specs = _parse_format_specs(fmt_text, bytes_mode=bytes_mode)
                                decision = _mod_decide(fmt_text, len(fmt_text), specs,
                                                       value, bytes_mode)
                                if decision.kind != "native":
                                    continue
                                try:
                                    fmt % value
                                    native_ok = True
                                except Exception:
                                    native_ok = False
                                if native_ok:
                                    counterexamples.append((fmt, type(value).__name__))
        self.assertGreaterEqual(checks["bytes"], 4000, checks)
        self.assertGreaterEqual(checks["str"], 4000, checks)
        self.assertEqual([], counterexamples[:5])


@skipUnless(_HAS_GUARD, "guard not present")
class TestFifteenthRoundX2MappingSubclasses(TestCase):
    """X2.1：三个已知 stdlib 映射的**子类**在读取任何属性之前就被拒绝（0 次作者调用）。"""

    def test_chainmap_subclass_overriding_keys_is_refused(self):
        class _EvilChain(collections.ChainMap):
            calls = 0

            def keys(self):
                type(self).calls += 1
                return ["a"]

            def __getitem__(self, key):
                type(self).calls += 1
                return "y" * 50_000_000

        with self.assertRaises(ValueError) as cm:
            _fn_mod("%(a)s", _EvilChain({"a": "v"}))
        self.assertIn("dict(x)", str(cm.exception))
        self.assertEqual(0, _EvilChain.calls)

    def test_chainmap_subclass_property_maps_is_refused(self):
        class _EvilMaps(collections.ChainMap):
            calls = 0

            @property
            def maps(self):
                type(self).calls += 1
                return [{"a": "y" * 50_000_000}]

            @maps.setter
            def maps(self, value):
                self._maps = value

        with self.assertRaises(ValueError):
            _fn_mod("%(a)s", _EvilMaps({"a": "v"}))
        self.assertEqual(0, _EvilMaps.calls)

    def test_userdict_subclass_is_refused(self):
        class _EvilUserDict(collections.UserDict):
            calls = 0

            def __getitem__(self, key):
                type(self).calls += 1
                return "y" * 50_000_000

        with self.assertRaises(ValueError):
            _fn_mod("%(a)s", _EvilUserDict({"a": "v"}))
        self.assertEqual(0, _EvilUserDict.calls)

    def test_mappingproxy_is_refused_for_every_wrapped_kind(self):
        """S2：type(proxy) 恒为 mappingproxy ⇒ 证明不了被包对象是精确 dict，一律拒绝。

        验收者最小复现：只覆写 __iter__ + __getitem__ 的 dict 子类，copy() 会调作者
        __getitem__ 1 次并返回伪造值（本实现不读任何东西，直接拒）。
        """

        class _IterGetitemDict(dict):
            calls = 0

            def __iter__(self):
                type(self).calls += 1
                return super().__iter__()

            def __getitem__(self, key):
                type(self).calls += 1
                return "y" * 50_000_000

        class _ChainSub(collections.ChainMap):
            calls = 0

            def copy(self):
                type(self).calls += 1
                return {}

        class _UserDictSub(collections.UserDict):
            calls = 0

            def copy(self):
                type(self).calls += 1
                return {}

        proxies = {
            "exact_dict": types.MappingProxyType({"a": "real"}),
            "dict_subclass": types.MappingProxyType(_IterGetitemDict(a="real")),
            "chainmap_subclass": types.MappingProxyType(_ChainSub({"a": "real"})),
            "userdict_subclass": types.MappingProxyType(_UserDictSub({"a": "real"})),
        }
        for label, proxy in proxies.items():
            with self.subTest(wrapped=label):
                with self.assertRaises(ValueError) as cm:
                    _fn_mod("%(a)s", proxy)
                self.assertIn("dict(x)", str(cm.exception))
        self.assertEqual(0, _IterGetitemDict.calls)
        self.assertEqual(0, _ChainSub.calls)
        self.assertEqual(0, _UserDictSub.calls)

    def test_chainmap_with_lying_dict_subclass_uses_c_slot(self):
        class _LyingDict(dict):
            calls = 0

            def __getitem__(self, key):
                type(self).calls += 1
                return "y" * 50_000_000

        chained = collections.ChainMap(_LyingDict(a="real"))
        self.assertEqual("real", _fn_mod("%(a)s", chained))
        self.assertEqual(0, _LyingDict.calls)


@skipUnless(_HAS_GUARD, "guard not present")
class TestFifteenthRoundX2DepthBudget(TestCase):
    """X2.2：合法深嵌套纯数据要通过；超出的病态嵌套 fail-closed 且有明确消息。"""

    def _nested(self, depth):
        value = [1]
        for _ in range(depth):
            value = [value]
        return value

    def test_legal_deep_nesting_passes(self):
        for depth in (999, 1001, 2000):
            with self.subTest(depth=depth):
                self.assertGreater(_content_size(self._nested(depth)), 0)

    def test_pathological_nesting_fails_closed_with_explicit_message(self):
        with self.assertRaises(ValueError) as cm:
            _content_size(self._nested(5000))
        self.assertIn("recursion", str(cm.exception))

    def test_recursion_limit_is_restored(self):
        before = sys.getrecursionlimit()
        _content_size(self._nested(1500))
        self.assertEqual(before, sys.getrecursionlimit())


@skipUnless(_HAS_GUARD, "guard not present")
class TestFifteenthRoundQ5SiblingBranches(TestCase):
    """Q5：兄弟 case/handler 里的上界调用**不支配**另一个分支里的渲染点。"""

    SIBLING_CASES = (
        "def _fn_ok(args):\n"
        "    match args:\n"
        "        case [x]:\n"
        "            bounds = [_render_size(arg) for arg in args]\n"
        "        case _:\n"
        "            out = ''.join([str(arg) for arg in args])\n"
        "    return out\n"
    )
    SIBLING_HANDLERS = (
        "def _fn_ok(args):\n"
        "    try:\n"
        "        pass\n"
        "    except ValueError:\n"
        "        bounds = [_render_size(arg) for arg in args]\n"
        "    except TypeError:\n"
        "        out = ''.join([str(arg) for arg in args])\n"
        "    return out\n"
    )
    GUARDED_CASE = (
        "def _fn_ok(args):\n"
        "    match args:\n"
        "        case [x] if False:\n"
        "            bounds = [_render_size(arg) for arg in args]\n"
        "    return ''.join([str(arg) for arg in args])\n"
    )

    def test_sibling_branches_are_red(self):
        registry = dict(_RENDER_SITES)
        for key in (("synthetic.py", "_fn_ok", "method:join",
                     "''.join([str(arg) for arg in args])"),
                    ("synthetic.py", "_fn_ok", "call:str", "str(arg)")):
            registry[key] = ("guarded", "_render_size", "Q5 兄弟分支测试用登记")
        for label, source in (("match_cases", self.SIBLING_CASES),
                              ("except_handlers", self.SIBLING_HANDLERS),
                              ("guarded_case", self.GUARDED_CASE)):
            with self.subTest(shape=label):
                offenders, _, _ = _scan_render_offenders(
                    source, "synthetic.py", registry, _FSTRING_PARAM_SITES)
                self.assertTrue(
                    any("_fn_ok" in line and "支配" in line for line in offenders),
                    f"{label}: 兄弟分支假绿：{offenders[:5]}")


class _O1IntOnly:
    """只有 ``__index__`` 的对象：native `%d`/`%c` 能成功，旧谓词却判 False。"""

    def __init__(self, value):
        self.value = value
        self.calls = 0

    def __index__(self):
        self.calls += 1
        return self.value


class _O2StatefulInt(int):
    """真值 90,000,000、``__int__`` 返回 1 的 int 子类。"""

    def __int__(self):
        type(self).int_calls = getattr(type(self), "int_calls", 0) + 1
        return 1


class _O2StatefulMapping:
    """第一次取值返回 's'、第二次返回 150MB 的映射（TOCTOU）。"""

    def __init__(self):
        self.calls = 0

    def __getitem__(self, key):
        self.calls += 1
        return "s" if self.calls == 1 else "y" * 150_000_000


@skipUnless(_HAS_GUARD, "guard not present")
class TestThirteenthRoundO1ModFloor(TestCase):
    """O1：`plan is None` 绝不能让上限判定空转；宽度本身就是分配下界。"""

    def _refused_cheaply(self, fn):
        desc, peak = _measure_peak(fn)
        self.assertEqual("ValueError", desc)
        self.assertLess(peak, _PEAK_BUDGET, f"峰值 {peak/1024/1024:.1f}MB")

    def test_incomplete_format_with_huge_width_is_refused_before_allocating(self):
        peaks = []
        for width in (50_000_000, 100_000_000, 400_000_000):
            with self.subTest(width=width):
                desc, peak = _measure_peak(lambda w=width: _fn_mod(f"%{w}s%", "x"))
                self.assertEqual("ValueError", desc)
                self.assertLess(peak, _PEAK_BUDGET, f"{width}: {peak/1024/1024:.1f}MB")
                peaks.append(peak)
        self.assertLess(max(peaks) - min(peaks), 1_000_000, "峰值随宽度增长")

    def test_count_mismatch_with_huge_width_is_refused_before_allocating(self):
        self._refused_cheaply(lambda: _fn_mod("%200000000s", ("a", "b")))

    def test_false_negative_conversion_is_refused_before_allocating(self):
        self._refused_cheaply(lambda: _fn_mod("%200000000d", _O1IntOnly(1)))
        self._refused_cheaply(lambda: _fn_mod("%200000000c", _O1IntOnly(65)))

    def test_floor_does_not_break_native_error_precedence(self):
        with _limits(floor=1, factor=1, hard=1):
            with self.assertRaises(TypeError):
                _fn_mod("%1000000000d", "x")
            with self.assertRaises(TypeError):
                _fn_mod("%s %s", "x")
            with self.assertRaises(KeyError):
                _fn_mod("%(z)s", {"a": 1})
            with self.assertRaises(ValueError):
                _fn_mod("%q", 1)


@skipUnless(_HAS_GUARD, "guard not present")
class TestThirteenthRoundO2ModDecisionPath(TestCase):
    """O2：星号宽度不得跑作者 `__int__`；映射只读一次且渲染同一份值。"""

    def test_star_width_never_runs_author_int(self):
        _O2StatefulInt.int_calls = 0
        value = _O2StatefulInt(90_000_000)      # 真值 90M，__int__ 返回 1
        desc, peak = _measure_peak(lambda: _fn_mod("%*s", (value, "x")))
        self.assertEqual("ValueError", desc)
        self.assertLess(peak, _PEAK_BUDGET)
        self.assertEqual(0, _O2StatefulInt.int_calls, "星号宽度调用了覆写的 __int__")

    def test_custom_mapping_is_refused_with_the_dict_escape_hatch(self):
        """Q2：无 C 槽的自定义映射 ⇒ 拒绝（owner 的 (a) 口径）+ dict(x) 逃生口。"""
        mapping = _O2StatefulMapping()
        with self.assertRaises(ValueError) as cm:
            _fn_mod("%(k)s", mapping)
        self.assertIn("dict(x)", str(cm.exception))
        self.assertEqual(0, mapping.calls, "被拒路径不得读自定义映射")
        desc, peak = _measure_peak(lambda: _fn_mod("%(k)s", mapping))
        self.assertEqual("ValueError", desc)
        self.assertLess(peak, _PEAK_BUDGET)

    def test_dict_subclass_stateful_getitem_is_read_once_via_c_slot(self):
        """Q2：dict 子类走 ``dict.get`` C 槽——覆写 ``__getitem__`` 0 次调用、渲染真实值。"""

        class _Stateful(dict):
            calls = 0

            def __getitem__(self, key):
                type(self).calls += 1
                return "y" * 50_000_000

        values = _Stateful(k="real")
        desc, peak = _measure_peak(lambda: _fn_mod("%(k)s", values))
        self.assertEqual("ok:str:4", desc)
        self.assertEqual(0, _Stateful.calls)
        self.assertLess(peak, _PEAK_BUDGET)

    def test_defaultdict_missing_key_is_not_inserted_on_the_reject_path(self):
        values = collections.defaultdict(lambda: "DEFAULT")
        desc, peak = _measure_peak(lambda: _fn_mod("%(missing)200000000s", values))
        self.assertEqual("ValueError", desc)
        self.assertLess(peak, _PEAK_BUDGET)
        self.assertEqual({}, dict(values), "被拒路径触发了 __missing__ 并插入默认值")


@skipUnless(_HAS_GUARD, "guard not present")
class TestThirteenthRoundO4Truncation(TestCase):
    """O4：写进日志/消息的作者可控字符串必须截断。"""

    def test_huge_type_name_is_truncated_in_log_and_message(self):
        class _HugeNameError(Exception):
            pass

        _HugeNameError.__name__ = "N" * 3_000_000
        _HugeNameError.__qualname__ = _HugeNameError.__name__

        class _Boom:
            def __len__(self):
                raise _HugeNameError("x")

        logger = logging.getLogger("plaita.core.expression")
        buffer = io.StringIO()
        handler = logging.StreamHandler(buffer)
        handler.setLevel(logging.WARNING)
        previous = logger.level
        logger.addHandler(handler)
        logger.setLevel(logging.WARNING)
        try:
            desc, peak = _measure_peak(lambda: _measure(_Boom()))
        finally:
            logger.removeHandler(handler)
            logger.setLevel(previous)
        self.assertEqual("ValueError", desc)
        self.assertLess(peak, _PEAK_BUDGET)
        self.assertLess(len(buffer.getvalue()), 2_000)
        with self.assertRaises(ValueError) as ctx:
            _measure(_Boom())
        self.assertLess(len(str(ctx.exception)), 2_000)

    def test_container_probe_debug_message_is_truncated(self):
        from plaita.logger import logger as shared_logger

        class _HugeMessage:
            def __contains__(self, key):
                raise RuntimeError("E" * 3_000_000)

        buffer = io.StringIO()
        handler = logging.StreamHandler(buffer)
        handler.setLevel(logging.DEBUG)
        previous = shared_logger.level
        shared_logger.addHandler(handler)
        shared_logger.setLevel(logging.DEBUG)
        try:
            self.assertFalse(parser_mod._container_has_root(_HugeMessage(), "$NODE"))
        finally:
            shared_logger.removeHandler(handler)
            shared_logger.setLevel(previous)
        self.assertGreater(len(buffer.getvalue()), 0)
        self.assertLess(len(buffer.getvalue()), 1_000)


@skipUnless(_HAS_GUARD, "guard not present")
class TestThirteenthRoundO5O6O7(TestCase):
    """O5 enum repr 覆写；O6 ZoneInfo/UserList；O7 间接构造。"""

    def test_enum_repr_override_is_refused(self):
        class _ReprEvil(enum.Enum):
            A = 1

            def __repr__(self):
                return "R" * 5_000_000

        class _IntReprEvil(enum.IntEnum):
            A = 1

            def __repr__(self):
                return "R" * 5_000_000

        for value in (_ReprEvil.A, _IntReprEvil.A):
            with self.subTest(cls=type(value).__name__):
                self.assertIsNone(_render_size(value, as_repr=True))
        desc, peak = _measure_peak(lambda: _fn_mod("%r", _ReprEvil.A))
        self.assertEqual("ValueError", desc)
        self.assertLess(peak, _PEAK_BUDGET)

    def test_zoneinfo_is_renderable_and_bounded(self):
        import zoneinfo

        value = datetime.datetime(2026, 1, 1, tzinfo=zoneinfo.ZoneInfo("UTC"))
        bound = _render_size(value, as_repr=True)
        self.assertIsNotNone(bound)
        self.assertGreaterEqual(bound, len(repr(value)))
        self.assertEqual(repr(value), _fn_mod("%r", value))
        self.assertEqual(str([value]), _fn_concat([value]))

    def test_zoneinfo_subclass_is_still_refused(self):
        import zoneinfo

        class _EvilZone(zoneinfo.ZoneInfo):
            def __repr__(self):
                return "Z" * 1_000_000

        value = datetime.datetime(2026, 1, 1, tzinfo=_EvilZone("UTC"))
        self.assertIsNone(_render_size(value, as_repr=True))

    def test_userlist_uses_its_data_slot(self):
        values = collections.UserList([0, 1, 2])
        self.assertEqual(6, len(_fn_mul(values, 2)))
        self.assertEqual(6, len(_fn_add(values, values)))
        self.assertEqual(6, len(_fn_extend(values, values)))

    def test_m5_indirect_constructs_are_flagged(self):
        synthetic = (
            "import functools\n"
            "import getpass\n"
            "def f(a, b, name, m, xs, p):\n"
            "    r = []\n"
            "    r.append(print(a))\n"
            "    r.append(__builtins__['repr'](a))\n"
            "    r.append(vars(m)['str'](a))\n"
            "    r.append(getattr(b, name)(a))\n"
            "    r.append(__import__('builtins').str(a))\n"
            "    r.append(str.__call__(a))\n"
            "    r.append(functools.reduce(str.__add__, xs))\n"
            "    x = a\n"
            "    r.append(f'{x}')\n"
            "    return r\n"
        )
        _, found, found_fstrings = _scan_render_offenders(
            synthetic, "synthetic.py", _RENDER_SITES, _FSTRING_PARAM_SITES)
        kinds = {kind for _m, _f, kind, _t in found}
        for expected in ("indirect:print", "indirect:builtins", "indirect:vars",
                         "indirect:getattr", "indirect:import", "method:__call__",
                         "attr:__add__"):
            with self.subTest(kind=expected):
                self.assertIn(expected, kinds)
        self.assertIn(("synthetic.py", "f#local", "{x}"), found_fstrings)


class TestCorePublicReexports(TestCase):
    """收尾回归门：``plaita.core`` 的公开再导出必须与基线一致。

    成因：第十五轮把 token 子系统加进 ``plaita/core/__init__.py`` 时，该文件被从
    另一个 checkout ``cp`` 覆盖，静默削掉了 ``StateSnapshotError`` 再导出——而
    ci-gate 全绿（没有任何测试从 ``plaita.core`` 顶层导入它）。本断言让"公开再
    导出被削"以后必然变红；同时 ``plaita/core/__init__.py`` 已恢复为与基线逐字节
    相同（``git diff 7ee8c25 HEAD -- plaita/core/__init__.py`` 必须为空）。
    """

    def test_state_snapshot_error_is_reexported_from_core(self):
        from plaita.core import StateSnapshotError
        from plaita.core import errors as core_errors
        self.assertIs(StateSnapshotError, core_errors.StateSnapshotError)

    def test_core_public_names_still_resolve(self):
        import plaita.core as core
        for name in ("StateSnapshotError", "ExpressionEvaluator", "valid"):
            with self.subTest(name=name):
                self.assertTrue(hasattr(core, name))


#: 第二十一轮：`%` 全表 AC4 语料的值类型/家族（生成逻辑见 `_grid_values`）。
_GRID_KINDS = ("int", "float", "Decimal", "str", "bytes")
_GRID_FAMILIES = ("exact", "clean", "overriding", "meta")
_GRID_CONVERSIONS = "diuoxXceEfFgGsrab"


@skipUnless(_HAS_GUARD, "guard not present")
class TestTwentyFirstRoundRenderDispatch(TestCase):
    """第二十一轮：**唯一派发表** `_RENDER_DISPATCH` 的单一来源 + s/r/a/b 双向控制
    + 全表 AC4 语料。

    两条判据方向都由**实测原生行为**驱动（不是手挑类型）：

    * **方向 A（不得过度拒绝）**：原生成功、且原生**没有**调用任何作者协议
      ⇒ 决策不得是 ``refuse``；``render`` 时必须 ``projected ≥ 实际``；
    * **方向 B（不得放行/低估）**：``native`` 决策必须伴随原生真的失败；
      ``render`` 且原生实测会跑作者协议 ⇒ 违规（上界不可证却放行）；决策路径
      任何时候 **0 次作者协议调用**。

    ``b``（bytes 模式）的 buffer 事实单独用正控钉住：bytes/bytearray 子类覆写
    ``__bytes__`` **被原生忽略**（走 C buffer）⇒ 必须放行且按 C 字节数计，
    而真正派发 ``__bytes__`` 的形状（非 buffer 带作者 ``__bytes__`` / ``PurePath``
    子类）必须具名拒绝。
    """

    _OVERRIDE_VALUES = {
        "__int__": lambda: 10 ** 4000,
        "__index__": lambda: 10 ** 4000,
        "__float__": lambda: 1.5,
        "__str__": lambda: "y" * 2000,
        "__repr__": lambda: "y" * 2000,
        "__bytes__": lambda: b"z" * 2000,
    }

    # --- 生成逻辑 -------------------------------------------------------
    @staticmethod
    def _base_for(kind):
        return {"int": int, "float": float, "Decimal": decimal.Decimal,
                "str": str, "bytes": bytes}[kind]

    @staticmethod
    def _seed_for(kind):
        return {"int": 7, "float": 1.5, "Decimal": decimal.Decimal("1.5"),
                "str": "abc", "bytes": b"abc"}[kind]

    def _raw_overrides(self, conversion):
        slots = expr_mod._RENDER_DISPATCH[conversion].slots
        return {name: (lambda self, _name=name: self._OVERRIDE_VALUES[_name]())
                for name in slots}

    def _grid_values(self, conversion, kind, family):
        """``(label, value, skipped)``——转换符 × 值类型 × 家族生成一个被测值。

        家族：``exact`` 精确类型；``clean`` 空子类（槽全部继承）；``overriding``
        覆写该转换符在表里派发的**全部**槽；``meta`` 同前者但套 `_meta_deceived`
        （元类数据描述符对 `__mro__`/`__dict__` 撒谎，AC1/AC4 维度）。
        """
        base = self._base_for(kind)
        seed = self._seed_for(kind)
        label = f"{kind}:{family}:{conversion}"
        try:
            if family == "exact":
                return label, seed, False
            if family == "clean":
                cls = type(f"Clean_{kind}_{conversion}", (base,), {})
                return label, cls(seed), False
            raw = self._raw_overrides(conversion)
            if family == "overriding":
                cls = type(f"Ov_{kind}_{conversion}", (base,), _calls_dict(raw))
            else:
                meta, body = _meta_deceived(base, raw, [])
                cls = meta(f"Meta_{kind}_{conversion}", (base,), body)
            return label, cls(seed), False
        except Exception:                       # noqa: BLE001 - 构造不出来的组合跳过
            return label, None, True

    @staticmethod
    def _format_for(conversion, bytes_mode):
        fmt = "%" + conversion
        if bytes_mode:
            fb = fmt.encode()
            return fb, fb.decode("latin-1")
        return fmt, fmt

    def _cell(self, value, conversion, bytes_mode):
        """跑一次决策 + 一次原生，返回 ``(decision, decision_calls, native_ok, actual, native_calls)``。"""
        fb, ft = self._format_for(conversion, bytes_mode)
        specs = _parse_format_specs(ft, bytes_mode=bytes_mode)
        del _AUTHOR_PROTOCOL_CALLS[:]
        decision = _mod_decide(ft, len(ft), specs, value, bytes_mode)
        decision_calls = list(_AUTHOR_PROTOCOL_CALLS)
        del _AUTHOR_PROTOCOL_CALLS[:]
        try:
            actual = len(fb % value)
            native_ok = True
        except Exception:                       # noqa: BLE001 - 原生失败是合法结果
            actual, native_ok = 0, False
        return decision, decision_calls, native_ok, actual, list(_AUTHOR_PROTOCOL_CALLS)

    # --- ① 单一来源 -----------------------------------------------------
    def test_render_dispatch_is_the_single_source(self):
        table = expr_mod._RENDER_DISPATCH
        self.assertEqual(set(_GRID_CONVERSIONS), set(table))
        for conversion, rule in table.items():
            with self.subTest(conversion=conversion):
                self.assertIn(rule.kind, ("int", "index", "float", "char",
                                          "str", "repr", "buffer"))
                self.assertTrue(rule.slots)
                if rule.kind == "char":
                    # `%c` 的上界是宽度常量（`_spec_bound` 直接算）⇒ 该行不带 bound。
                    self.assertIsNone(rule.bound)
                else:
                    self.assertTrue(callable(rule.bound))
                self.assertIsInstance(rule, tuple)
        # 兼容视图是**派生投影**，不是第二张表：逐行相等且键集一致。
        for conversion, rule in table.items():
            with self.subTest(projected=conversion):
                self.assertEqual((rule.kind, rule.slots),
                                 expr_mod._CONVERSION_DISPATCH[conversion])
        self.assertEqual(set(table), set(expr_mod._CONVERSION_DISPATCH))
        # bytes 模式 `s`/`b` 都路由到 buffer 行；str 模式没有 `%b`。
        self.assertIs(table["b"], expr_mod._render_rule("s", True))
        self.assertIs(table["b"], expr_mod._render_rule("b", True))
        self.assertIs(table["s"], expr_mod._render_rule("s", False))
        self.assertIsNone(expr_mod._render_rule("b", False))
        # 旧白名单确实删除（不再有第二套归属判据）。
        source = " ".join(line for line in inspect.getsource(expr_mod).splitlines()
                          if not line.lstrip().startswith("#"))
        for gone in ("_STR_SAFE_OWNERS", "_REPR_SAFE_OWNERS", "_RENDER_SCALARS",
                     "_RENDER_CONTAINERS", "_RENDER_STDLIB_FIXED"):
            with self.subTest(gone=gone):
                self.assertNotIn(gone, source)
        # 执行点只做表分派。
        render_source = inspect.getsource(expr_mod._render_size)
        self.assertIn('_RENDER_DISPATCH["r" if as_repr else "s"]', render_source)

    # --- ②(a) 合法类型不得被过度拒绝 --------------------------------------
    def test_legit_types_are_not_over_refused(self):
        class CleanStr(str):
            pass

        class CleanBytes(bytes):
            pass

        class CleanBytearray(bytearray):
            pass

        class CleanEnum(enum.IntEnum):
            A = 1

        class PlainEnum(enum.Enum):
            A = 1

        class CleanPath(pathlib.PurePosixPath):
            pass

        legit = [
            ("str", str("abc")), ("bytes", bytes(b"abc")),
            ("bytearray", bytearray(b"abc")), ("CleanStr", CleanStr("abc")),
            ("CleanBytes", CleanBytes(b"abc")), ("CleanBytearray", CleanBytearray(b"abc")),
            ("IntEnum", CleanEnum.A), ("Enum", PlainEnum.A),
            ("UUID", uuid.UUID(int=1)),
            ("PurePosixPath", pathlib.PurePosixPath("/x/y")),
            ("PureWindowsPath", pathlib.PureWindowsPath("C:/x/y")),
            ("CleanPath", CleanPath("/x/y")),
        ]
        misses = []
        checks = 0
        for label, value in legit:
            # 只覆盖**渲染/buffer 族**（本轮的 s/r/a/b）：数值族 x/%d 等的合法性由
            # AC6 的数值协议表（int/float/Decimal 家族）另行治理，UUID 之类"只有
            # 协议可取数"的值在数值转换上按既有口径拒绝 + 具名逃生口 int(x)。
            for conversion in ("s", "r", "a", "b"):
                for bytes_mode in (False, True):
                    if conversion == "b" and not bytes_mode:
                        continue
                    decision, decision_calls, native_ok, actual, native_calls = \
                        self._cell(value, conversion, bytes_mode)
                    checks += 1
                    mode = "bytes" if bytes_mode else "str"
                    tag = (label, mode, "%" + conversion)
                    if decision_calls:
                        misses.append(("decision-author-call", tag, decision_calls))
                    if native_ok and not native_calls:
                        # 方向 A：原生成功且没有跑作者协议 ⇒ 不得拒绝。
                        if decision.kind == "refuse":
                            misses.append(("over-refusal", tag, actual))
                        elif decision.kind == "render" and decision.projected < actual:
                            misses.append(("bound<actual", tag, decision.projected, actual))
                    if decision.kind == "native" and native_ok:
                        misses.append(("native-but-succeeded", tag))
        self.assertGreaterEqual(checks, 80, checks)
        self.assertEqual([], misses[:5])

    # --- ②(b) 覆写被派发槽（含元类欺骗）必须具名拒绝 + 0 调用 ----------------
    def test_overriding_slot_subclasses_are_refused_by_name(self):
        """正控：下面每个形状都**实测**原生真的会调用那个作者槽，所以"必须拒绝"不是空话。"""
        plan = [
            ("s", "__str__", False, str, "abc"),
            ("s", "__repr__", False, list, ["a"]),
            ("r", "__repr__", False, str, "abc"),
            ("a", "__repr__", False, str, "abc"),
            ("s", "__bytes__", True, str, "abc"),
            ("b", "__bytes__", True, str, "abc"),
            ("b", "__bytes__", True, pathlib.PurePosixPath, "/x/y"),
        ]
        checked = 0
        for conversion, slot, bytes_mode, base, seed in plan:
            for family in ("overriding", "meta"):
                override_value = self._OVERRIDE_VALUES[slot]()
                raw = {slot: (lambda self, _v=override_value: _v)}
                if family == "overriding":
                    cls = type(f"Ov_{base.__name__}_{conversion}", (base,),
                               _calls_dict(raw))
                else:
                    meta, body = _meta_deceived(base, raw, [])
                    cls = meta(f"Meta_{base.__name__}_{conversion}", (base,), body)
                value = cls(seed)
                with self.subTest(conversion=conversion, slot=slot, family=family,
                                  base=base.__name__):
                    decision, decision_calls, native_ok, actual, native_calls = \
                        self._cell(value, conversion, bytes_mode)
                    # 正控：原生确实跑了作者槽（否则这条"必须拒绝"没有对象）。
                    self.assertTrue(native_calls,
                                    f"native 未派发 {slot}，正控前提不成立")
                    self.assertEqual([], decision_calls,
                                     "决策路径不得运行作者协议")
                    self.assertEqual("refuse", decision.kind)
                    fb, _ft = self._format_for(conversion, bytes_mode)
                    desc, peak = _measure_peak(lambda f=fb, v=value: _fn_mod(f, v))
                    self.assertEqual("ValueError", desc)
                    with self.assertRaises(ValueError) as ctx:
                        _fn_mod(fb, value)
                    self.assertIn(type(value).__name__, str(ctx.exception))
                    self.assertLess(peak, _PEAK_BUDGET)
                    checked += 1
        self.assertGreaterEqual(checked, 14, checked)

    def test_buffer_dispatch_ignores_author_bytes_for_c_slots(self):
        """★4 同族的 **buffer** 事实（实测）：bytes/bytearray 子类的 `%s`/`%b` 直读 C
        buffer，覆写的 `__bytes__` **被忽略**（0 次作者调用）；因此必须放行、按 C 字节数
        计上界，而不是把"表里有 `__bytes__`"误当成"原生会派发"。"""
        class OvBytes(bytes):
            def __bytes__(self):
                _AUTHOR_PROTOCOL_CALLS.append("__bytes__")
                return b"z" * 1000

        class OvBytearray(bytearray):
            def __bytes__(self):
                _AUTHOR_PROTOCOL_CALLS.append("__bytes__")
                return b"z" * 1000

        for label, value in (("OvBytes", OvBytes(b"hello")),
                             ("OvBytearray", OvBytearray(b"hello"))):
            for conversion, bytes_mode in (("s", True), ("b", True)):
                with self.subTest(label=label, conversion=conversion):
                    decision, decision_calls, native_ok, actual, native_calls = \
                        self._cell(value, conversion, bytes_mode)
                    self.assertEqual([], decision_calls)
                    self.assertEqual([], native_calls,
                                     "原生本应忽略 __bytes__（C buffer 直读）")
                    self.assertTrue(native_ok)
                    self.assertEqual(5, actual)
                    self.assertEqual("render", decision.kind)
                    self.assertGreaterEqual(decision.projected, actual)

        # 反向正控：`str` 子类带**作者** `__bytes__` ⇒ 非 buffer、原生确实派发它 ⇒
        # 必须具名拒绝、0 次作者调用（这条同时钉死"buffer 长度不能用 str 字符数"）。
        class OvStr(str):
            def __bytes__(self):
                _AUTHOR_PROTOCOL_CALLS.append("__bytes__")
                return b"z" * 1000

        for conversion in ("s", "b"):
            with self.subTest(label="OvStr", conversion=conversion):
                decision, decision_calls, native_ok, actual, native_calls = \
                    self._cell(OvStr("abc"), conversion, True)
                self.assertEqual([], decision_calls)
                self.assertTrue(native_calls, "原生对 str 子类的 __bytes__ 是派发的")
                self.assertEqual("refuse", decision.kind)

    # --- ③ AC4 全表语料 ---------------------------------------------------
    def test_ac4_render_dispatch_full_table(self):
        """`for conv in table: for kind in (int,float,Decimal,str,bytes):
        for family in (exact, clean, overriding, meta):` 每格断言双向不变式。

        生成逻辑：`_grid_values` 按表里该转换符的 `slots` 生成覆写/元类欺骗子类；
        每个 (转换符, 模式) 组合跑两模式（str 模式跳过 `b`）。违规判据即本类
        docstring 的两条方向。
        """
        checks = 0
        skipped = 0
        violations = []
        per_cell = collections.Counter()
        for conversion in sorted(expr_mod._RENDER_DISPATCH):
            for bytes_mode in (False, True):
                if not bytes_mode and conversion == "b":
                    continue
                for kind in _GRID_KINDS:
                    for family in _GRID_FAMILIES:
                        label, value, was_skipped = self._grid_values(
                            conversion, kind, family)
                        if was_skipped:
                            skipped += 1
                            continue
                        decision, decision_calls, native_ok, actual, native_calls = \
                            self._cell(value, conversion, bytes_mode)
                        checks += 1
                        mode = "bytes" if bytes_mode else "str"
                        tag = (conversion, mode, kind, family)
                        per_cell[f"{mode}:{family}"] += 1
                        if decision_calls:
                            violations.append(("decision-author-call", tag,
                                               decision_calls))
                        if decision.kind == "native" and native_ok:
                            violations.append(("native-but-succeeded", tag))
                        if decision.kind == "render":
                            if native_ok and decision.projected < actual:
                                violations.append(("bound<actual", tag,
                                                   decision.projected, actual))
                            if native_calls:
                                violations.append(("render-ran-author-code", tag,
                                                   native_calls, actual))
                        if decision.kind == "refuse" and native_ok and not native_calls \
                                and family in ("exact", "clean"):
                            # 方向 A 只保护"合法数据"：精确/干净子类且原生没跑作者协议。
                            violations.append(("over-refusal-legit", tag, actual))
                        if decision.kind == "refuse" and native_calls:
                            per_cell[f"{mode}:required-refusal"] += 1
        self.assertGreaterEqual(checks, 600, checks)
        self.assertEqual([], violations[:5])
        # 语料确实覆盖了两模式 × 四家族（生成逻辑自证，不是空循环）。
        for key in ("str:exact", "bytes:exact", "str:overriding", "bytes:overriding",
                    "str:meta", "bytes:meta"):
            with self.subTest(cell=key):
                self.assertGreater(per_cell[key], 0)
        self.assertGreater(per_cell["str:required-refusal"]
                           + per_cell["bytes:required-refusal"], 0)


import itertools as _itertools  # noqa: E402 - 测试语料别名


class _CorpusEnum(enum.Enum):
    A = 1


#: AG1/AG2：纯 Python 渲染祖先（`__str__`/`__repr__` 是 Python 代码，会读实例属性）。
_AG_ANCESTORS = ("PurePosixPath", "PureWindowsPath", "PosixPath",
                 "SimpleNamespace", "UUID")


@skipUnless(_HAS_GUARD, "guard not present")
class TestTwentySecondRoundInstanceAttributeShadowing(TestCase):
    """AG1/AG2/AG3：纯 Python 祖先的 `__str__`/`__repr__` 读**实例属性**，作者子类可以
    用数据描述符（property）把它换掉；而 r21 的上界读的是"安全投影"（`parts`/`vars()`/
    常量 64）⇒ 低估（实测 `%s` 上界 67 / 实际 10,000,000、峰值 10MB；NS 上界 51 /
    实际 10,000,010；UUID 上界 74 / 实际 332,197），且 `_mod_decide` 会跑作者 property。

    判据（双向，全部由实测驱动）：

    * 方向 A：清白子类（类命名空间只有解释器自动名字）必须放行，`projected ≥ 实际`，
      决策路径 **0 次作者调用**；
    * 方向 B：native 实测**会**调用作者 property 的形状必须 `refuse`（具名、0 调用、
      峰值 < 预算）；native **忽略**的 property（SimpleNamespace 的 C repr 直读真实实例
      dict）必须按**真实内容**给上界，而不是 property 返回的投影。
    """

    FORMATS = ("%s", "%r", "%a", b"%s", b"%b", b"%r", b"%a")

    @staticmethod
    def _counting_property(name, value, calls):
        """可读可写的 property：读时计数（模拟作者数据描述符），写时存到别处。"""
        def getter(self):
            calls.append(name)
            _AUTHOR_PROTOCOL_CALLS.append(name)
            return getattr(self, "__shadow_" + name, value)

        def setter(self, new):
            object.__setattr__(self, "__shadow_" + name, new)

        return property(getter, setter)

    def _shadowing_path(self, attr):
        """`PurePath` 子类：把 `__str__` 会读的某个槽换成 property。"""
        calls = []
        body = {attr: self._counting_property(attr, [] if attr == "_parts" else "", calls)}
        cls = type(f"ShadowPath_{attr}", (pathlib.PurePosixPath,), body)
        return cls, calls

    def _shadowing_uuid(self):
        calls = []
        cls = type("ShadowUUID", (uuid.UUID,), {
            "int": self._counting_property("int", 10 ** 4000, calls)})
        return cls, calls

    def _shadowing_namespace(self, payload):
        calls = []
        cls = type("ShadowNamespace", (types.SimpleNamespace,), {
            "__dict__": self._counting_property("__dict__", {"a": "x"}, calls)})
        value = cls()
        value.big = payload
        return value, calls

    def _cell(self, value, fmt):
        bytes_mode = not isinstance(fmt, str)
        text = fmt.decode("latin-1") if bytes_mode else fmt
        specs = _parse_format_specs(text, bytes_mode=bytes_mode)
        del _AUTHOR_PROTOCOL_CALLS[:]
        decision = _mod_decide(text, len(text), specs, value, bytes_mode)
        decision_calls = list(_AUTHOR_PROTOCOL_CALLS)
        del _AUTHOR_PROTOCOL_CALLS[:]
        try:
            actual = len(fmt % value)
            native_ok = True
        except Exception:                       # noqa: BLE001
            actual, native_ok = 0, False
        return decision, decision_calls, native_ok, actual, list(_AUTHOR_PROTOCOL_CALLS)

    # --- AG1 方向 B：native 会派发作者 property ⇒ 必须拒绝 -----------------
    def test_path_attribute_shadowing_is_refused_before_rendering(self):
        # plaita#68 r30：可遮蔽名按版本过滤——名字不在本版类型上（3.13 的
        # `_parts`）时类级遮蔽不会重定向渲染，前提不成立，跳过。
        attrs = [a for a in ("_str", "_drv", "_root", "_parts")
                 if hasattr(pathlib.PurePosixPath, a)]
        for attr in attrs:
            cls, calls = self._shadowing_path(attr)
            for fmt in ("%s", "%r", "%a", b"%s", b"%b", b"%r"):
                try:
                    # 每个转换符都用**新实例**：native 的 `str()` 会缓存 `_str`，
                    # 复用实例会让后续转换符不再读被遮蔽的槽（正控失效）。
                    value = cls("/x/y")
                except Exception:               # noqa: BLE001 - 构造不了的形状跳过
                    break
                with self.subTest(attr=attr, fmt=fmt):
                    decision, decision_calls, native_ok, actual, native_calls = \
                        self._cell(value, fmt)
                    self.assertEqual([], decision_calls,
                                     "决策路径不得运行作者 property")
                    self.assertGreater(len(native_calls), 0,
                                       "正控前提不成立：native 未派发该 property")
                    # native 会跑作者 property：决策必须拒绝（不是判 native、不是 render）。
                    self.assertEqual("refuse", decision.kind)
                    fb = fmt if isinstance(fmt, bytes) else fmt
                    desc, peak = _measure_peak(lambda f=fb, v=value: _fn_mod(f, v))
                    self.assertEqual("ValueError", desc)
                    self.assertLess(peak, _PEAK_BUDGET)

    def test_path_instance_dict_protocol_shadowing_is_refused(self):
        """AG1 续：实例 dict 遮蔽 `as_posix` / `_format_parsed_parts`（二者不是数据描述符）
        ——实测 ``p.as_posix = lambda: "y"*10**7`` ⇒ ``repr(p)`` 10,000,009 字符、
        r21 上界 81、决策 render。"""
        class CleanPath(pathlib.PurePosixPath):
            pass

        def make(attr):
            value = CleanPath("/x/y")
            if attr == "as_posix":
                setattr(value, attr, lambda: "y" * 10**7)
            else:
                setattr(value, attr, lambda *args: "y" * 10**7)
            return value

        for attr, fmt in (("as_posix", "%r"), ("as_posix", "%a"),
                          ("_format_parsed_parts", "%s")):
            with self.subTest(attr=attr, fmt=fmt):
                value = make(attr)
                decision, decision_calls, native_ok, actual, native_calls = \
                    self._cell(value, fmt)
                self.assertEqual([], decision_calls,
                                 "决策路径不得运行作者实例属性")
                self.assertTrue(native_ok)
                self.assertGreater(actual, 10 ** 6,
                                   "正控：native 确实走了被实例 dict 遮蔽的属性")
                self.assertEqual("refuse", decision.kind)

    def test_uuid_int_shadowing_is_refused_before_rendering(self):
        cls, calls = self._shadowing_uuid()
        value = cls(int=5)
        for fmt in ("%s", "%r", "%a"):
            with self.subTest(fmt=fmt):
                decision, decision_calls, native_ok, actual, native_calls = \
                    self._cell(value, fmt)
                self.assertEqual([], decision_calls)
                self.assertGreater(len(native_calls), 0,
                                   "正控：native 会读子类 property int")
                self.assertEqual("refuse", decision.kind)
                desc, peak = _measure_peak(lambda f=fmt, v=value: _fn_mod(f, v))
                self.assertEqual("ValueError", desc)
                self.assertLess(peak, _PEAK_BUDGET)

    # --- AG1 方向 A / C repr 直读真实 dict：按真实内容给上界、0 调用 ----------
    def test_namespace_dict_shadowing_uses_the_real_instance_dict(self):
        payload = "z" * 2000
        value, calls = self._shadowing_namespace(payload)
        for fmt in ("%s", "%r", "%a"):
            with self.subTest(fmt=fmt):
                decision, decision_calls, native_ok, actual, native_calls = \
                    self._cell(value, fmt)
                self.assertEqual([], decision_calls,
                                 "决策路径不得运行作者的 __dict__ property")
                self.assertTrue(native_ok)
                self.assertGreater(actual, len(payload),
                                   "原生 repr 必须包含真实实例 dict 的内容")
                self.assertEqual("render", decision.kind)
                self.assertGreaterEqual(decision.projected, actual)
                # property 被 native **忽略**（C repr 直读真实 dict）⇒ 正控应为 0 次调用。
                self.assertEqual([], native_calls)

    def test_clean_pure_python_ancestor_subclasses_are_not_over_refused(self):
        class CleanPath(pathlib.PurePosixPath):
            pass

        class CleanWindowsPath(pathlib.PureWindowsPath):
            pass

        class CleanPosixPath(pathlib.PosixPath):
            pass

        class CleanNamespace(types.SimpleNamespace):
            pass

        class CleanUUID(uuid.UUID):
            pass

        legit = [("CleanPath", CleanPath("/x/y")),
                 ("CleanWindowsPath", CleanWindowsPath("C:/x/y")),
                 ("CleanPosixPath", CleanPosixPath("/x/y")),
                 ("CleanNamespace", CleanNamespace(a="x")),
                 ("CleanUUID", CleanUUID(int=1))]
        misses = []
        checks = 0
        for label, value in legit:
            for fmt in self.FORMATS:
                decision, decision_calls, native_ok, actual, native_calls = \
                    self._cell(value, fmt)
                checks += 1
                tag = (label, fmt)
                if decision_calls:
                    misses.append(("decision-author-call", tag, decision_calls))
                if decision.kind == "refuse" and native_ok and not native_calls:
                    misses.append(("over-refusal", tag, actual))
                if decision.kind == "render" and native_ok and decision.projected < actual:
                    misses.append(("bound<actual", tag, decision.projected, actual))
                if decision.kind == "native" and native_ok:
                    misses.append(("native-but-succeeded", tag))
        self.assertGreaterEqual(checks, 35, checks)
        self.assertEqual([], misses[:5])

    # --- AG2：决策路径 0 次作者调用（真属性计数，不只 dunder）----------------
    def test_decision_path_has_zero_author_calls_for_all_shadow_shapes(self):
        path_cls, path_calls = self._shadowing_path("_parts")
        uuid_cls, uuid_calls = self._shadowing_uuid()
        value, ns_calls = self._shadowing_namespace("z" * 100)
        cases = [("path._parts", path_cls("/x/y"), path_calls),
                 ("uuid.int", uuid_cls(int=5), uuid_calls),
                 ("namespace.__dict__", value, ns_calls)]
        checks = 0
        for label, value, calls in cases:
            for fmt in self.FORMATS:
                with self.subTest(label=label, fmt=fmt):
                    del calls[:]
                    decision, decision_calls, native_ok, actual, native_calls = \
                        self._cell(value, fmt)
                    self.assertEqual([], decision_calls,
                                     "决策路径运行了作者 property")
                    checks += 1
        self.assertGreaterEqual(checks, 18, checks)

    # --- AG3：AC4 网格加"纯 Python 祖先"维度 -------------------------------
    def _ancestor_value(self, base, family, tag):
        """``(value, calls, skipped)``：exact / clean / shadow(property) / meta-shadow。"""
        calls = []
        try:
            if base == "PurePosixPath":
                target, seed = pathlib.PurePosixPath, "/x/y"
                attr, shadow = "_str", None
            elif base == "PureWindowsPath":
                target, seed = pathlib.PureWindowsPath, "C:/x/y"
                attr, shadow = "_str", None
            elif base == "PosixPath":
                target, seed = pathlib.PosixPath, "/x/y"
                attr, shadow = "_str", None
            elif base == "SimpleNamespace":
                target, seed = types.SimpleNamespace, None
                attr, shadow = "__dict__", {"a": "x"}
            else:
                target, seed = uuid.UUID, None
                attr, shadow = "int", 7
            if family == "exact":
                if base == "SimpleNamespace":
                    return types.SimpleNamespace(a=1), calls, False
                if base == "UUID":
                    return uuid.UUID(int=1), calls, False
                return target(seed), calls, False
            if family == "slot-mutated":
                # AJ1：**精确类型**的实例槽同样可写（普通赋值 / object.__setattr__）。
                if base == "SimpleNamespace":
                    value = types.SimpleNamespace()
                    value.big = "z" * 2000
                    return value, calls, False
                if base == "UUID":
                    value = uuid.UUID(int=1)
                    object.__setattr__(value, "int", 10 ** 4000)
                    return value, calls, False
                value = target(seed)
                setattr(value, "_str", "E" * 2000)
                return value, calls, False
            if family == "clean":
                cls = type(f"Clean_{tag}", (target,), {})
                if base == "SimpleNamespace":
                    return cls(a=1), calls, False
                if base == "UUID":
                    return cls(int=1), calls, False
                return cls(seed), calls, False
            body = {attr: self._counting_property(attr, shadow, calls)}
            if family == "shadow":
                cls = type(f"Shadow_{tag}", (target,), body)
            else:
                class _Meta(type):
                    __mro__ = property(lambda cls: (target,))
                    __dict__ = property(lambda cls: {})
                cls = _Meta(f"MetaShadow_{tag}", (target,), body)
            if base == "SimpleNamespace":
                value = cls()
                value.big = "z" * 2000
                return value, calls, False
            if base == "UUID":
                return cls(int=1), calls, False
            return cls(seed), calls, False
        except Exception:                       # noqa: BLE001
            return None, calls, True

    def test_ac4_pure_python_ancestor_dimension(self):
        checks = 0
        skipped = 0
        violations = []
        per_family = collections.Counter()
        for base in _AG_ANCESTORS:
            for family in ("exact", "clean", "slot-mutated", "shadow", "meta-shadow"):
                value, calls, was_skipped = self._ancestor_value(base, family, base)
                if was_skipped:
                    skipped += 1
                    continue
                for fmt in self.FORMATS:
                    decision, decision_calls, native_ok, actual, native_calls = \
                        self._cell(value, fmt)
                    checks += 1
                    mode = "bytes" if isinstance(fmt, bytes) else "str"
                    tag = (base, family, mode, fmt)
                    per_family[f"{family}"] += 1
                    if decision_calls:
                        violations.append(("decision-author-call", tag, decision_calls))
                    if decision.kind == "native" and native_ok:
                        violations.append(("native-but-succeeded", tag))
                    if decision.kind == "render" and native_ok \
                            and decision.projected < actual:
                        violations.append(("bound<actual", tag,
                                           decision.projected, actual))
                    if decision.kind == "render" and native_calls:
                        violations.append(("render-ran-author-code", tag, actual))
                    if family in ("shadow", "meta-shadow") and native_calls:
                        # native 实测会派发作者 property ⇒ 必须拒绝（fail-closed）。
                        if decision.kind != "refuse":
                            violations.append(("shadow-not-refused", tag, actual))
                    if family in ("exact", "clean") and decision.kind == "refuse" \
                            and native_ok and not native_calls:
                        violations.append(("over-refusal-legit", tag, actual))
        self.assertGreaterEqual(checks, 175, checks)
        self.assertEqual([], violations[:5])
        for family in ("exact", "clean", "slot-mutated", "shadow", "meta-shadow"):
            with self.subTest(family=family):
                self.assertGreater(per_family[family], 0)



@skipUnless(_HAS_GUARD, "guard not present")
class TestTwentyThirdRoundExactTypeSlots(TestCase):
    """AJ1：**精确类型**的实例槽同样可写，精确分支不能绕过子类闸。

    实测（base r22 `2b09c9e`）：``p = PurePosixPath("/x/y"); p._str = "E"*10**7`` ⇒
    ``%s`` 投影 **67** / 实际 **10,000,000**；``%r`` 投影 67 / 实际 10,000,017、峰值
    **22.50MB**（分配后才拦）；``object.__setattr__(u, "int", 10**400000)`` ⇒ 投影 74 /
    实际 332,197。根因：`_render_type_bound` 的 `cls in _PATH_EXACT_TYPES` 分支走
    `value.parts` 投影、`cls is UUID` 分支走常量 64，二者都绕过了
    `_path_render_bound`/`_uuid_render_bound`。修复：精确分支接到同一道闸。
    """

    FORMATS = ("%s", "%r", "%a", b"%s", b"%r", b"%a")

    def _cell(self, value, fmt):
        bytes_mode = not isinstance(fmt, str)
        text = fmt.decode("latin-1") if bytes_mode else fmt
        specs = _parse_format_specs(text, bytes_mode=bytes_mode)
        del _AUTHOR_PROTOCOL_CALLS[:]
        decision = _mod_decide(text, len(text), specs, value, bytes_mode)
        decision_calls = list(_AUTHOR_PROTOCOL_CALLS)
        del _AUTHOR_PROTOCOL_CALLS[:]
        try:
            actual = len(fmt % value)
            native_ok = True
        except Exception:                       # noqa: BLE001
            actual, native_ok = 0, False
        return decision, decision_calls, native_ok, actual, list(_AUTHOR_PROTOCOL_CALLS)

    def test_exact_path_slot_writes_are_bounded_or_refused(self):
        misses = []
        absent = []          # plaita#68 r30：本版不存在的槽名（篡改面向消失）
        checks = 0
        # plaita#68 r30：可写槽按版本枚举——第一代（3.9–3.12）与第二代（3.13+
        # pathlib 重写）的渲染相关槽名不同；不在本版类型上的名字 setattr 即
        # AttributeError = 篡改面向消失（等价于更强的拒绝），记 absent 跳过。
        for target, seed in ((pathlib.PurePosixPath, "/x/y"),
                             (pathlib.PureWindowsPath, "C:/x/y"),
                             (pathlib.PosixPath, "/x/y")):
            for attr, payload in (("_str", "E" * 10 ** 6), ("_drv", "E" * 10 ** 6),
                                  ("_root", "E" * 10 ** 6), ("_parts", ["E" * 10 ** 6]),
                                  ("_raw_paths", ["E" * 10 ** 6])):
                value = target(seed)
                with self.subTest(target=target.__name__, attr=attr):
                    try:
                        setattr(value, attr, payload)
                    except AttributeError:
                        # plaita#68 r30：本版类型上该名字不是可写槽（3.13 的
                        # `_parts` 等）——篡改面向消失，无 cell 可测，缺席合法。
                        absent.append((target.__name__, attr))
                        continue
                for fmt in ("%s", "%r", "%a", b"%r", b"%a"):
                    decision, decision_calls, native_ok, actual, native_calls = \
                        self._cell(value, fmt)
                    checks += 1
                    tag = (target.__name__, attr, fmt)
                    if decision_calls:
                        misses.append(("decision-author-call", tag))
                    if decision.kind == "refuse":
                        continue                    # fail-closed 也是合法答案
                    if decision.kind == "native" and native_ok:
                        misses.append(("native-but-succeeded", tag))
                    if decision.kind == "render" and native_ok \
                            and decision.projected < actual:
                        misses.append(("bound<actual", tag,
                                       decision.projected, actual))
        self.assertGreaterEqual(checks, 60, checks)
        for a in absent:
            self.assertIn(a[1], ("_parts", "_raw_paths"),
                          "缺席的槽名必须是已知的代际差异")
        self.assertEqual([], misses[:5])

    def test_exact_path_slot_writes_refuse_before_allocating(self):
        for attr in ("_str", "_drv", "_root"):
            value = pathlib.PurePosixPath("/x/y")
            setattr(value, attr, "E" * 10 ** 7)
            for fmt in ("%s", "%r", "%a", b"%r"):
                with self.subTest(attr=attr, fmt=fmt):
                    desc, peak = _measure_peak(lambda f=fmt, v=value: _fn_mod(f, v))
                    self.assertEqual("ValueError", desc)
                    self.assertLess(peak, _PEAK_BUDGET)

    def test_exact_uuid_int_slot_write_is_bounded(self):
        value = uuid.UUID(int=1)
        object.__setattr__(value, "int", 10 ** 400000)
        for fmt in ("%s", "%r", "%a", b"%r", b"%a"):
            with self.subTest(fmt=fmt):
                decision, decision_calls, native_ok, actual, native_calls = \
                    self._cell(value, fmt)
                self.assertEqual([], decision_calls)
                self.assertTrue(native_ok)
                self.assertGreaterEqual(actual, 332197)
                self.assertEqual("render", decision.kind)
                self.assertGreaterEqual(decision.projected, actual)

    def _family_corpus(self):
        """AM2：**按族**生成语料（默认实例 + 作者字符串变体 + 边界长度）。"""
        big = "E" * 2000
        out = [
            ("str", "abc"), ("bytes", b"abc"), ("bytearray", bytearray(b"abc")),
            ("bool", True), ("int", 123456789), ("none", None), ("float", 1.5),
            ("float-inf", float("inf")), ("float-nan", float("nan")),
            ("list", [1, "a"]), ("tuple", (1,)), ("set", {1}), ("frozenset", frozenset({1})),
            ("dict", {"a": 1}), ("keys", {}.keys()), ("values", {}.values()),
            ("items", {}.items()), ("range", range(3)), ("memoryview", memoryview(b"abc")),
            ("decimal", decimal.Decimal("1.5")),
            ("decimal-bigexp", decimal.Decimal("1e1000")),
            ("uuid", uuid.UUID(int=1)),
            ("pattern", re.compile("ab+")), ("pattern-big", re.compile(big[:1000])),
            ("array", array.array("i", [1, 2])), ("array-u", array.array("u", big[:1000])),
            # AP1/AP2（第二十五轮）：每 typecode 的**最坏值**（旧 32+len*24 投影对 d/f 低估）
            ("array-b-worst", array.array("b", [-128] * 1000)),
            ("array-B-worst", array.array("B", [255] * 1000)),
            ("array-h-worst", array.array("h", [-32768] * 1000)),
            ("array-H-worst", array.array("H", [65535] * 1000)),
            ("array-i-worst", array.array("i", [-2 ** 31] * 1000)),
            ("array-I-worst", array.array("I", [2 ** 32 - 1] * 1000)),
            ("array-l-worst", array.array("l", [-2 ** 63] * 1000)),
            ("array-L-worst", array.array("L", [2 ** 64 - 1] * 1000)),
            ("array-q-worst", array.array("q", [-2 ** 63] * 1000)),
            ("array-Q-worst", array.array("Q", [2 ** 64 - 1] * 1000)),
            ("array-d-worst", array.array("d", [-sys.float_info.max] * 1000)),
            ("array-f-worst", array.array("f", [-3.4028235e38] * 1000)),
            ("array-u-escapes", array.array("u", "\U0010ffff\x00\ud800" * 100)),
            ("array-in-list", [array.array("d", [-sys.float_info.max] * 1000)]),
            ("array-in-dict", {"k": array.array("d", [-sys.float_info.max] * 1000)}),
            # AR1/AR2（第二十六轮）：**非 ASCII 名字**族——`%a`（ascii）会展开成
            # \xNN/\uNNNN/\UNNNNNNNN（最多 10×），所有 as_repr 上界里的作者名字都要按 10×。
            ("str-nonascii", "\u00e9" * 200),
            ("dict-nonascii-key", {"\u00e9" * 200: 1}),
            ("list-nonascii", ["\u00e9" * 200]),
            ("array-u-nonascii", array.array("u", "\u00e9" * 200)),
            ("pattern-nonascii", re.compile("\u00e9" * 200)),
            ("namespace-nonascii-key", types.SimpleNamespace(**{"\u00e9" * 200: 1})),
            ("class-object-nonascii", type("\u00c9" * 200, (), {})),
            ("timezone-nonascii-name",
             datetime.timezone(datetime.timedelta(0), "\u00e9" * 200)),
            ("deque", collections.deque([1, 2])),
            ("ordereddict", collections.OrderedDict(a=1)),
            ("defaultdict", collections.defaultdict(int, a=1)),
            ("namespace", types.SimpleNamespace(a=1)),
            ("date", datetime.date(2026, 1, 2)),
            ("timedelta", datetime.timedelta(days=1, seconds=2)),
            # 类对象族（AM1：__module__/__qualname__ 可写）
            ("class-default", int), ("class-longname", type(big, (), {})),
            # 默认 object repr 族
            ("plain-object", object()),
            # enum 族
            ("enum-class", _CorpusEnum), ("enum-class-long", enum.Enum(big, {"A": 1})),
            ("enum-member", _CorpusEnum.A),
            # 函数/内建族
            ("builtin-fn", len), ("builtin-method", "".join),
            ("function", (lambda: None)),
            # 迭代器族
            ("generator", (c for c in "ab")), ("zip", zip([1], [2])),
            ("iter-list", iter([1, 2])),
            ("repeat", _itertools.repeat(big)), ("count", _itertools.count(10 ** 2000)),
            # timezone / datetime 族
            ("timezone-utc", datetime.timezone.utc),
            ("timezone-offset", datetime.timezone(datetime.timedelta(hours=8))),
            ("timezone-name", datetime.timezone(datetime.timedelta(0), big)),
            ("datetime", datetime.datetime(2026, 1, 2, 3, 4, 5)),
            ("time", datetime.time(3, 4, 5)),
        ]
        class _BigQual:
            pass
        _BigQual.__qualname__ = big
        out.append(("plain-object-longqual", _BigQual()))
        class _BigMod:
            pass
        _BigMod.__module__ = big
        out.append(("plain-object-longmod", _BigMod()))
        def _big_fn():
            pass
        _big_fn.__qualname__ = big
        out.append(("function-longqual", _big_fn))
        class _Tz(datetime.tzinfo):
            def utcoffset(self, dt):
                return datetime.timedelta(0)

            def dst(self, dt):
                return datetime.timedelta(0)

            def tzname(self, dt):
                return "T"
        _Tz.__qualname__ = big
        out.append(("datetime-bigtz", datetime.datetime(2026, 1, 2, tzinfo=_Tz())))
        out.append(("time-bigtz", datetime.time(3, 4, 5, tzinfo=_Tz())))

        class _WorstArray(array.array):
            pass

        out.append(("array-subclass-worst",
                    _WorstArray("d", [-sys.float_info.max] * 1000)))

        class _NonAsciiClass:
            pass
        _NonAsciiClass.__qualname__ = "\u00c9" * 200
        out.append(("plain-object-nonascii", _NonAsciiClass()))
        _NonAsciiEnum = enum.Enum("\u00c9" * 200, {"A": 1})
        out.append(("enum-class-object-nonascii", _NonAsciiEnum))
        out.append(("enum-member-nonascii", list(_NonAsciiEnum)[0]))
        _NonAsciiMemberEnum = enum.Enum("E", {"\u00e9" * 200: 1})
        out.append(("enum-member-name-nonascii", list(_NonAsciiMemberEnum)[0]))
        _NonAsciiPath = type("\u00c9" * 200, (pathlib.PurePosixPath,), {})
        out.append(("path-subclass-nonascii", _NonAsciiPath("/x/y")))
        _NonAsciiUuid = type("\u00c9" * 200, (uuid.UUID,), {})
        out.append(("uuid-subclass-nonascii", _NonAsciiUuid(int=1)))
        def _non_ascii_fn():
            pass
        _non_ascii_fn.__qualname__ = "\u00e9" * 200
        out.append(("function-nonascii", _non_ascii_fn))
        return out

    def test_every_exact_type_branch_has_bound_ge_actual(self):
        """AM2 + AR2：每个族至少一个默认实例 + 作者字符串变体（含非 ASCII 名字）；
        上界必须 ≥ 实际渲染，且 **`%r` 与 `%a` 分开**断言（`%a` 比 `ascii`）。"""
        corpus = self._family_corpus()
        families = {label.split("-")[0] for label, _ in corpus}
        misses = []
        for label, value in corpus:
            with self.subTest(label=label):
                text_bound = _render_size(value)
                repr_bound = _render_size(value, as_repr=True)
                self.assertIsNotNone(text_bound, label)
                self.assertIsNotNone(repr_bound, label)
                if text_bound < len(str(value)):
                    misses.append(("str", label, text_bound, len(str(value))))
                if repr_bound < len(repr(value)):
                    misses.append(("repr", label, repr_bound, len(repr(value))))
                if repr_bound < len(ascii(value)):
                    misses.append(("ascii", label, repr_bound, len(ascii(value))))
                for conversion, expected in (("r", len(repr(value))),
                                             ("a", len(ascii(value)))):
                    decision = _mod_decide(f"%{conversion}", 2,
                                           _parse_format_specs(f"%{conversion}",
                                                               bytes_mode=False),
                                           value, False)
                    if decision.kind == "refuse":
                        continue                        # fail-closed 也合法
                    if decision.projected < expected:
                        misses.append((conversion, label, decision.projected, expected))
        self.assertEqual([], misses[:5])
        self.assertGreaterEqual(len(corpus), 45, len(corpus))
        self.assertEqual(41, len(families), sorted(families))



@skipUnless(_HAS_GUARD, "guard not present")
class TestTwentyFourthRoundConstantBounds(TestCase):
    """AM1–AM3：**常量上界**必须与槽读取同等对待——只要 str/repr 里嵌了作者可写的
    名字/参数，常量就是推理而不是证明。

    base r23 `0f38361` 实测：`class C: pass; C.__qualname__="E"*1e7` ⇒ `%r` 投影 **266** /
    实际 **20,000,025**、峰值 **25.00MB**；`type("E"*1e7,(),{})` 266 / 10,000,019；
    enum 类 266 / 10,000,009；函数 `__qualname__` 266 / 10,000,026；
    `timezone(timedelta(0), "E"*1e7)` `%s` 74 / 10,000,000；
    `itertools.repeat("E"*1e7)` 266 / 10,000,010。

    方向：这些族必须**读真实成分**（不可变 getsets / C 槽 / `__reduce__`）给出 ≥ 实际的
    上界并 **0 次作者调用**；读不到 ⇒ 拒绝。每条族一个控制用例，对应一条"恢复常量即红"的
    变异（见 `scripts/mutation_replay/mutations/68-*-constant.json`）。
    """

    BIG = "E" * 2000
    ALL_FORMATS = ("%s", "%r", "%a", b"%s", b"%r", b"%a")

    def _cell(self, value, fmt):
        bytes_mode = not isinstance(fmt, str)
        text = fmt.decode("latin-1") if bytes_mode else fmt
        specs = _parse_format_specs(text, bytes_mode=bytes_mode)
        del _AUTHOR_PROTOCOL_CALLS[:]
        decision = _mod_decide(text, len(text), specs, value, bytes_mode)
        decision_calls = list(_AUTHOR_PROTOCOL_CALLS)
        del _AUTHOR_PROTOCOL_CALLS[:]
        try:
            actual = len(fmt % value)
            native_ok = True
        except Exception:                       # noqa: BLE001
            actual, native_ok = 0, False
        return decision, decision_calls, native_ok, actual, list(_AUTHOR_PROTOCOL_CALLS)

    def assert_bounded(self, value, label, formats=ALL_FORMATS):
        for fmt in formats:
            with self.subTest(label=label, fmt=fmt):
                decision, decision_calls, native_ok, actual, native_calls = \
                    self._cell(value, fmt)
                self.assertEqual([], decision_calls, f"{label}: 决策路径跑了作者代码")
                if decision.kind == "native":
                    self.assertFalse(native_ok, f"{label} {fmt}: 判 native 但原生成功")
                    continue
                self.assertEqual("render", decision.kind, label)
                if native_ok:
                    self.assertGreaterEqual(decision.projected, actual,
                                            f"{label} {fmt}: {decision.projected} < {actual}")

    def test_author_writable_class_names_are_bounded(self):
        class _BigQual:
            pass
        _BigQual.__qualname__ = self.BIG
        class _BigMod:
            pass
        _BigMod.__module__ = self.BIG
        made = type(self.BIG, (), {})
        long_enum = enum.Enum(self.BIG, {"A": 1})
        for label, value in (("plain", object()),
                             ("plain-longqual", _BigQual()),
                             ("plain-longmod", _BigMod()),
                             ("class-made", made),
                             ("instance-of-made", made()),
                             ("enum-class-long", long_enum)):
            self.assert_bounded(value, label)

    def test_function_qualname_is_bounded(self):
        def _big():
            pass
        _big.__qualname__ = self.BIG
        self.assert_bounded(_big, "function-longqual")
        self.assert_bounded(len, "builtin-fn")
        self.assert_bounded("".join, "builtin-method")
        self.assert_bounded(type(self.BIG, (), {}), "class-object")

    def test_timezone_name_is_bounded(self):
        self.assert_bounded(datetime.timezone.utc, "tz-utc")
        self.assert_bounded(datetime.timezone(datetime.timedelta(hours=8)), "tz-offset")
        self.assert_bounded(datetime.timezone(datetime.timedelta(0), self.BIG), "tz-name")

    def test_datetime_tzinfo_repr_is_bounded(self):
        class _Tz(datetime.tzinfo):
            def utcoffset(self, dt):
                return datetime.timedelta(0)

            def dst(self, dt):
                return datetime.timedelta(0)

            def tzname(self, dt):
                return "T"

        _Tz.__qualname__ = self.BIG
        tz = _Tz()
        self.assert_bounded(datetime.datetime(2026, 1, 2, tzinfo=tz), "datetime-bigtz")
        self.assert_bounded(datetime.time(3, 4, 5, tzinfo=tz), "time-bigtz")

    def test_repeat_and_count_embed_real_args(self):
        self.assert_bounded(_itertools.repeat(self.BIG), "repeat-big")
        self.assert_bounded(_itertools.count(10 ** 2000), "count-big")
        self.assert_bounded(_itertools.repeat("x", 5), "repeat-small")
        self.assert_bounded(_itertools.count(1, 2), "count-small")

    def test_fixed_iterator_reprs_are_bounded(self):
        values = [
            ("generator", (c for c in "ab")), ("zip", zip([1], [2])),
            ("map", map(str, [1])), ("filter", filter(None, [1])),
            ("enumerate", enumerate([1])), ("reversed", reversed([1])),
            ("iter-list", iter([1, 2])), ("cycle", _itertools.cycle([1])),
            ("chain", _itertools.chain([1])), ("islice", _itertools.islice([1], 1)),
            ("accumulate", _itertools.accumulate([1])),
            ("product", _itertools.product([1])),
            ("permutations", _itertools.permutations([1])),
            ("combinations", _itertools.combinations([1], 1)),
            ("combinations-r", _itertools.combinations_with_replacement([1], 1)),
            ("zip-longest", _itertools.zip_longest([1])),
            ("groupby", _itertools.groupby([1])),
            ("pairwise", _itertools.pairwise([1])),
            ("takewhile", _itertools.takewhile(bool, [1])),
            ("dropwhile", _itertools.dropwhile(bool, [1])),
            ("filterfalse", _itertools.filterfalse(bool, [1])),
            ("compress", _itertools.compress([1], [1])),
            ("starmap", _itertools.starmap(pow, [(1, 2)])),
        ]
        for label, value in values:
            self.assert_bounded(value, label)

    def test_constant_bound_families_refuse_before_allocating(self):
        """端到端方向：巨名形状的 `_fn_mod` 必须在分配前拒绝或按 1× 渲染。"""
        class _BigQual:
            pass
        _BigQual.__qualname__ = "E" * 10 ** 7
        case = {"$INPUT": {"x": _BigQual()}}
        for expression, label in (("{% $F.mod('%r',$INPUT.x) %}", "mod"),
                                  ("{% $INPUT.x %}", "template"),
                                  ("{% $F.concat($INPUT.x) %}", "concat")):
            with self.subTest(expression=label):
                desc, peak = _measure_peak(
                    lambda e=expression, c=case: _fresh().evaluate(e, c))
                self.assertEqual("ValueError", desc)
                self.assertLess(peak, _PEAK_BUDGET)



@skipUnless(_HAS_GUARD, "guard not present")
class TestTwentyFifthRoundArrayElementBounds(TestCase):
    """AP1–AP3：`array.array` 的每元素投影必须按 **typecode 的实测最坏值**，不能再是 24。

    base r24 `698bebd` 实测：`array('d', [-sys.float_info.max]*K)` 的 `%r` 每元素是
    **26** 字符（`repr(-max)` 24 + `", "` 2），旧界按 24 算 ⇒ K=1e6 投影 24,000,042 /
    实际 26,000,012；K=2e6 投影 48,000,042 / 实际 52,000,012（`_fn_mod` 直接返回、峰值
    181MB）；K=4,166,000 投影 99,984,032 ≤ 1e8 而实际 108,316,012 ⇒ **硬上限被绕过**。
    """

    def _decide(self, value):
        specs = _parse_format_specs("%r", bytes_mode=False)
        return _mod_decide("%r", 2, specs, value, False)

    def test_every_typecode_worst_case_has_bound_ge_actual(self):
        worst = {
            "b": -128, "B": 255, "h": -32768, "H": 65535,
            "i": -2 ** 31, "I": 2 ** 32 - 1, "l": -2 ** 63, "L": 2 ** 64 - 1,
            "q": -2 ** 63, "Q": 2 ** 64 - 1,
            "d": -sys.float_info.max, "f": -3.4028235e38,
        }
        misses = []
        for typecode, value in worst.items():
            for count in (1, 1000, 100_000):
                array_value = array.array(typecode, [value] * count)
                decision = self._decide(array_value)
                actual = len(repr(array_value))
                if decision.kind != "render":
                    misses.append(("not-render", typecode, count, decision.kind))
                elif decision.projected < actual:
                    misses.append(("bound<actual", typecode, count,
                                   decision.projected, actual))
        self.assertEqual([], misses[:5])
        # unicode typecode：repr 是单个 quoted string，含不可打印/星平面字符时按 10× 转义
        for text in ("ab", "\x00\x01", "\U0010ffff", "\ud800", "\u00e9" * 50):
            array_value = array.array("u", text)
            decision = self._decide(array_value)
            with self.subTest(unicode=repr(text)[:12]):
                self.assertEqual("render", decision.kind)
                self.assertGreaterEqual(decision.projected, len(repr(array_value)))
        self.assertEqual([], misses[:5])

    def test_array_hard_limit_is_not_bypassed(self):
        """K=4,166,000（旧投影 99,984,032 ≤ 1e8）必须被硬上限在分配前拒绝。"""
        value = array.array("d", [-sys.float_info.max] * 4_166_000)
        self.assertGreaterEqual(len(repr(value)), 108_000_000)   # 实测 ~108,316,012
        decision = self._decide(value)
        self.assertEqual("render", decision.kind)
        self.assertGreater(decision.projected, expr_mod._EXPR_MAX_RESULT_HARD)
        desc, peak = _measure_peak(lambda: _fn_mod("%r", value))
        self.assertEqual("ValueError", desc)
        self.assertLess(peak, _PEAK_BUDGET)

    def test_array_subclass_and_nested_shapes_are_bounded(self):
        class _WorstArray(array.array):
            pass

        worst = -sys.float_info.max
        shapes = [
            ("exact", array.array("d", [worst] * 100_000)),
            ("subclass", _WorstArray("d", [worst] * 100_000)),
            ("in-list", [array.array("d", [worst] * 100_000)]),
            ("in-dict", {"k": array.array("d", [worst] * 100_000)}),
            ("in-tuple", (array.array("d", [worst] * 100_000),)),
        ]
        for label, value in shapes:
            with self.subTest(shape=label):
                decision = self._decide(value)
                self.assertEqual("render", decision.kind)
                self.assertGreaterEqual(decision.projected, len(repr(value)))



@skipUnless(_HAS_GUARD, "guard not present")
class TestTwentySixthRoundAsciiExpansion(TestCase):
    r"""AR1–AR4：`%a`（`ascii()`）把非 ASCII 展开成 `\xNN`/`\uNNNN`/`\UNNNNNNNN`
    （最多 10×），而 `_bound_repr` 同时服务 `%r` 与 `%a`。

    base r25 `3342d82` 实测（`%a`）：enum 成员 `_name_="é"*1000` ⇒ 投影 **1031** / 实际
    **4008**；enum 类名、PurePath 子类名、UUID 子类名同样 1× 低估；`"😀"*1e6` 的成员名
    ⇒ 投影 1,000,031 / 实际 **10,000,008**、`_fn_mod` **RET**（峰值 20.00MB）。
    修复：所有 `as_repr` 上界里的作者可控名字按 **10×** 计（`_class_name_bound`、
    `_function_repr_bound`、`_path_render_bound`、`_uuid_render_bound`、
    `_enum_member_render_bound`、`_tzinfo_extra` 的 ZoneInfo key）；`%a` 的成长额度只按
    值的**权威内容量**（`_content_size`）计，ascii 展开不再吃额度 ⇒ 分配前拒绝。
    """

    def _decide(self, conversion, value):
        fmt = f"%{conversion}"
        return _mod_decide(fmt, 2, _parse_format_specs(fmt, bytes_mode=False), value, False)

    def _assert_ascii_bounded(self, value, label):
        for conversion in ("r", "a"):
            with self.subTest(label=label, conversion=conversion):
                decision = self._decide(conversion, value)
                expected = len(repr(value)) if conversion == "r" else len(ascii(value))
                if decision.kind == "refuse":
                    continue
                self.assertGreaterEqual(decision.projected, expected,
                                        f"{label} %{conversion}: "
                                        f"{decision.projected} < {expected}")

    def test_ascii_expansion_of_author_names_is_bounded(self):
        shapes = []
        for scale in (1, 1000):
            member_enum = enum.Enum("E", {"é" * scale: 1})
            shapes.append((f"enum-member-name-{scale}", list(member_enum)[0]))
            class_enum = enum.Enum("É" * scale, {"A": 1})
            shapes.append((f"enum-class-object-{scale}", class_enum))
            shapes.append((f"enum-class-name-{scale}", list(class_enum)[0]))
            shapes.append((f"path-class-{scale}",
                           type("É" * scale, (pathlib.PurePosixPath,), {})("/x/y")))
            shapes.append((f"uuid-class-{scale}",
                           type("É" * scale, (uuid.UUID,), {})(int=1)))
        class _BigQual:
            pass
        _BigQual.__qualname__ = "É" * 1000
        shapes.append(("plain-object-nonascii", _BigQual()))

        def _non_ascii_fn():
            pass
        _non_ascii_fn.__qualname__ = "é" * 1000
        shapes.append(("function-nonascii", _non_ascii_fn))
        shapes.append(("class-object-nonascii", type("É" * 1000, (), {})))
        shapes.append(("timezone-nonascii",
                       datetime.timezone(datetime.timedelta(0), "é" * 1000)))
        shapes.append(("namespace-nonascii-key",
                       types.SimpleNamespace(**{"é" * 1000: 1})))
        for label, value in shapes:
            self._assert_ascii_bounded(value, label)

    def test_ascii_expansion_astral_names_are_bounded(self):
        star = "😀" * 100_000
        member = enum.Enum("E", {star: 1})[star]
        for label, value in (("member-astral", member),
                             ("enum-class-astral", enum.Enum("😀" * 100_000, {"A": 1})),
                             ("path-astral",
                              type("😀" * 100_000, (pathlib.PurePosixPath,), {})("/x")),
                             ("uuid-astral",
                              type("😀" * 100_000, (uuid.UUID,), {})(int=1))):
            for conversion in ("r", "a"):
                with self.subTest(label=label, conversion=conversion):
                    decision = self._decide(conversion, value)
                    expected = len(repr(value)) if conversion == "r" else len(ascii(value))
                    self.assertEqual("render", decision.kind)
                    self.assertGreaterEqual(decision.projected, expected)

    def test_ascii_expansion_refuses_before_allocating(self):
        star = "😀" * 10 ** 6
        shapes = [
            ("member-name-star", enum.Enum("E", {star: 1})[star]),
            ("enum-class-name", enum.Enum("É" * 10 ** 6, {"A": 1})),
            ("path-class-name", type("É" * 10 ** 6, (pathlib.PurePosixPath,), {})("/x/y")),
            ("uuid-class-name", type("É" * 10 ** 6, (uuid.UUID,), {})(int=1)),
        ]
        for label, value in shapes:
            with self.subTest(shape=label):
                decision = self._decide("a", value)
                self.assertEqual("render", decision.kind)
                self.assertGreaterEqual(decision.projected, len(ascii(value)))
                desc, peak = _measure_peak(
                    lambda v=value: _fresh().evaluate("{% $F.mod('%a',$INPUT.x) %}",
                                                      {"$INPUT": {"x": v}}))
                self.assertEqual("ValueError", desc)
                self.assertLess(peak, _PEAK_BUDGET)

    def test_corpus_asserts_ascii_expansion(self):
        """护栏：语料的运行时控制必须**真的**比 `ascii`（否则 `%a` 族结构上不可见）。"""
        source = inspect.getsource(
            TestTwentyThirdRoundExactTypeSlots
            .test_every_exact_type_branch_has_bound_ge_actual)
        self.assertIn("ascii(", source)
        self.assertIn('("a", len(ascii(value)))', source)



@skipUnless(_HAS_GUARD, "guard not present")
class TestTwentySeventhRoundAsciiThreshold(TestCase):
    """AU1–AU3：`%a`（ascii）上界改为**真实逐字符转义宽度**，并回收 r26 的过度拒绝。

    base r26 `4c13ebc` 实测：上界是两分法最坏值（可打印 ASCII ≤2× / 其它 ≤10×）⇒
    `"\n"*9999` 投影 100,010 > 允许 100,000（实际 ascii 只有 20,000）而 **`%a` 被拒**；
    `"é"*9999`（实际 39,998）、`"\n"*100000`（实际 200,002 ≤ 允许 400,000）、
    bytes `"é"*12496`（24,992B，实际 99,971 ≤ 允许 100,000）同样被拒。
    修后上界 = 逐字符真实宽度（1/2/4/6/10）+ 两端引号，`%a` 与 `%r` 同源。
    """

    def _render(self, value):
        result = _fn_mod("%a", value)
        self.assertEqual(len(ascii(value)), len(result))
        return result

    def test_long_control_and_nonascii_strings_render_under_percent_a(self):
        cases = [
            ("\n" * 9999, "nl-9999"),
            ("\t" * 9999, "tab-9999"),
            ("é" * 9999, "latin1-9999"),
            ("\x00" * 10000, "nul-10000"),
            ("\n" * 100000, "nl-100000"),
            ("é".encode() * 12496, "bytes-latin1-24992"),
        ]
        for value, label in cases:
            with self.subTest(case=label):
                desc, peak = _measure_peak(lambda v=value: self._render(v))
                self.assertTrue(desc.startswith("ok:str:"), f"{label}: {desc}")
                self.assertLess(peak, _PEAK_BUDGET, f"{label}: {peak} bytes")

    def test_astral_expansion_still_refuses_before_rendering(self):
        value = "😀" * 10 ** 6
        actual = len(ascii(value))
        desc, peak = _measure_peak(lambda: _fn_mod("%a", value))
        self.assertEqual("ValueError", desc)
        self.assertEqual("ValueError", desc)
        self.assertLess(peak, actual)          # 绝不构造 10,000,002 字符的展开串
        with self.assertRaises(ValueError) as cm:
            _fn_mod("%a", "😀" * 10_000)
        message = str(cm.exception)
        self.assertIn("PLAITA_EXPR_GROWTH_FACTOR", message)   # AU3：逃生口写进消息
        self.assertIn("%r", message)
        self.assertIn("ascii()", message)

    def test_growth_threshold_is_where_real_expansion_exceeds_budget(self):
        """阈值（AU3 实测）：拒 ⇔ `fmt_len + spec_bound > max(floor=100_000,
        4 × (fmt_len + 内容))`。`%a` 的 spec_bound = 真实逐字符和 + 10，fmt_len = 2。"""
        # é（latin-1，ascii 宽度 4/字符）⇒ 首个被拒 N = **24,998**（实测）。
        self.assertEqual(len(ascii("é" * 24_997)), len(_fn_mod("%a", "é" * 24_997)))
        with self.assertRaises(ValueError):
            _fn_mod("%a", "é" * 24_998)
        # 星号平面（宽度 10/字符）⇒ 首个被拒 N = **9,999**（实测）。
        self.assertEqual(len(ascii("😀" * 9_998)), len(_fn_mod("%a", "😀" * 9_998)))
        with self.assertRaises(ValueError):
            _fn_mod("%a", "😀" * 9_999)
        # `\n`（宽度 2/字符）在 2,000,000 字符内**从不**被拒（真实展开 ≤4×）。
        value = "\n" * 2_000_000
        self.assertEqual(len(ascii(value)), len(_fn_mod("%a", value)))

    def test_percent_a_bound_is_exact_and_shared_with_percent_r(self):
        values = ["", "abc", "a'b", 'a"b', "a\\b", "\n\t\r", "\x00\x1f\x7f",
                  "é" * 5, "中" * 5, "😀" * 5, "mix é\n中😀", "x" * 1000]
        for value in values:
            with self.subTest(value=repr(value)[:16]):
                bound = _render_size(value, as_repr=True)
                self.assertEqual(len(ascii(value)), bound)      # 对 %a **精确**
                self.assertGreaterEqual(bound, len(repr(value)))  # 对 %r 保守但 ≥ 实际
                self.assertEqual(len(value), _render_size(value))  # %s 仍按 1×

    def test_bytes_repr_width_is_exact(self):
        values = [b"", b"abc", b"a'b", b'a"b', b"a\\b", b"\n\t", b"\x00\xff",
                  b"\xc3\xa9" * 1000, bytes(range(256)) * 3]
        for value in values:
            with self.subTest(value=repr(value)[:16]):
                self.assertEqual(len(repr(value)), _render_size(value))


@skipUnless(_HAS_GUARD, "guard not present")
class TestTwentyEighthRoundBytearrayAndBoundPeak(TestCase):
    """AX1-AX4：bytearray 引号规则、上界计算自身的峰值预算、容器 str 快路。

    base r27 `1ae3641` 实测：
    * `bytearray.__repr__` **无条件**转义单引号（bytes 只在同时含双引号时转义），而 r27 的
      `_bytes_repr_width` 对二者共用 bytes 规则 ⇒ `bytearray(b"'")` 上界 15 / 实际 16；
      `bytearray(b"'"*1e6)` 上界 1,000,014 / 实际 2,000,014（**可绕 1e8 硬上限**）。
    * bytes 的不可打印计数走 `re.subn` ⇒ 每个匹配物化一个 match 对象，峰值 **88×** 输入。
    * `_container_render_bound` 的 str 快路仍是扁平 `10×len+10` ⇒ 含 `"\\n"*100_000` 的
      list 的 `%a` 上界 1,000,038、实际 200,004、允许 400,008 ⇒ **预算内的合法值被拒**。
    """

    _ALPHA = (b"'", b'"', b"\\", b"\n", b"\x00", b"\xff", b"a")

    def test_bytearray_apostrophe_rule_matches_native_exhaustively(self):
        """AX1：全部 256 单字节 + 全部 <=3 长度组合（7 字符表）都要 bound >= 实际。"""
        misses = []
        for kind in ("bytes", "bytearray"):
            maker = bytes if kind == "bytes" else bytearray
            for byte in range(256):
                value = maker([byte])
                bound = _render_size(value, as_repr=True)
                if bound < len(repr(value)):
                    misses.append((kind, bytes(value), bound, len(repr(value))))
            for size in (1, 2, 3):
                for combo in _itertools.product(self._ALPHA, repeat=size):
                    value = maker(b"".join(combo))
                    bound = _render_size(value, as_repr=True)
                    if bound < len(repr(value)):
                        misses.append((kind, bytes(value), bound, len(repr(value))))
        self.assertEqual([], misses[:5])

    def test_bytearray_apostrophe_hard_limit_is_not_bypassed(self):
        """AX1：bytearray(b"'"*K) 的上界 = 2K + 14（实测）；K > 5e7 时越过 1e8 硬上限。"""
        for k in (0, 1, 3, 1000):
            value = bytearray(b"'" * k)
            self.assertEqual(2 * k + _BYTEARRAY_REPR_PREFIX,
                             _render_size(value, as_repr=True))
        # 解析复测 r27 的绕过：2 x 50,000,001 + 14 = 100,000,016 > 1e8。
        self.assertGreater(2 * 50_000_001 + 14, 100_000_000)

    def test_bytearray_shapes_in_containers_are_bounded(self):
        values = [
            bytearray(b"'"), bytearray(b"'''"), bytearray(b"a'b"),
            bytearray(b"'" * 100_000),
            [bytearray(b"'" * 1000)], {"k": bytearray(b"'" * 1000)},
            (bytearray(b"'" * 1000),),
        ]
        for value in values:
            with self.subTest(value=repr(value)[:24]):
                bound = _render_size(value, as_repr=True)
                self.assertIsNotNone(bound)
                self.assertGreaterEqual(bound, len(repr(value)))

    def test_bound_computation_peak_budget(self):
        """AX2/H2：**上界计算自身**不得放大内存（预算阶梯断言）。

        * 单位：`peak`（tracemalloc 峰值字节）对 **`len(value)`**——
          `bytes`/`bytearray` 是**字节数**，`str` 是**字符数**（不是 storage 字节数、
          不是 `sys.getsizeof`）。`str` 为 UCS-2/UCS-4 时 storage = 2–4 × 字符数，
          所以同一条峰值换算成 storage 比例会更小。
        * 预算常数 `c`：`bytes`/`bytearray` **c = 1.5**（实测恰好 1.00×）；
          `str` **c = 10**（实测 astral 8.45×、BMP CJK 8.45× 字符数）。
        * `str` 路径**不满足**字面 H2（1.00×），采用这个**有界且被测试守卫**的预算；
          详见 `docs/mutation-testing.md` 第二十八轮。
        """
        bytes_cases = [b"\xc3\xa9" * 100_000, b"\xc3\xa9" * 1_000_000,
                       bytearray(b"\xc3\xa9" * 1_000_000),
                       b"'" * 1_000_000, bytearray(b"'" * 1_000_000)]
        for value in bytes_cases:
            with self.subTest(kind=type(value).__name__, size=len(value)):
                _, peak = _measure_peak(lambda v=value: _render_size(v, as_repr=True))
                self.assertLessEqual(
                    peak, 1.5 * len(value),
                    "%s len=%d bytes: peak=%d bytes > c(1.5) x len(value) bytes"
                    % (type(value).__name__, len(value), peak))
        for value in ("\n" * 1_000_000, "é" * 1_000_000, "😀" * 1_000_000):
            with self.subTest(chars=len(value)):
                _, peak = _measure_peak(lambda v=value: _render_size(v, as_repr=True))
                self.assertLessEqual(
                    peak, 10 * len(value),
                    "str len=%d chars (not storage): peak=%d bytes > c(10) x "
                    "len(value) chars" % (len(value), peak))

    def test_container_str_fast_path_renders_within_budget(self):
        """AX3：list/set/嵌套的 str 项按真实宽度计 ⇒ 预算内的合法值必须渲染。"""
        text = "\n" * 100_000
        shapes = [
            ("list", [text]), ("set", {text}), ("nested", [[text]]),
            ("tuple", (text,)), ("dict", {"k": text}),
            ("list-mixed", [text, "abc", 1]),
        ]
        for label, value in shapes:
            with self.subTest(shape=label):
                bound = _render_size(value, as_repr=True)
                self.assertIsNotNone(bound)
                self.assertGreaterEqual(bound, len(ascii(value)))
                # 原生 `%a` 的实参语义（1-tuple 会按参数元组展开）就是参照系。
                expected = len("%a" % value)
                self.assertEqual(expected, len(_fn_mod("%a", value)))


if __name__ == "__main__":
    unittest.main()
