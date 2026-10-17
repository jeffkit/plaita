"""plaita#69 r6 —— 表达式求值的 µs 级守卫（O(1) 性质 + **常量因子**预算）。

背景：``tests/unit/test_performance_benchmark.py`` 全是整流程/事件级粗阈值
（``avg_time < 0.1s``），抓不到 µs 级回归。verifier 实测 r4 相对 base 把
``$INPUT.a.b.c`` 从 1.995µs 拉到 3.250µs（+63%）、r5 把
``$F.set($VAR,'k','v')`` 打到 23.5µs（≈6.8× 裸路径）而无人发现。

本文件有两层判据：

1. **可判定的性质**（与机器绝对速度无关）：单次表达式求值的成本**不得随上下文规模
   增长**（default-deny 的查询是 O(1)）。r6 起这条也覆盖 **mutator 路径**（目标位
   freshness plan 的 allow / default-deny 两条分支）。
2. **常量因子预算**（r6 新增）：mutator 路径相对裸路径 ``$INPUT.a.b.c`` 的**比值**与
   ``scripts/issue69_mutation_matrix/perf_baseline.json`` 里的 checked-in 基线比，
   超出 ``tolerance`` 即红。比值与机器速度无关（分子分母同步缩放）。

负控（两层各一）：

* 人为在热路径上加一次"全量遍历" ⇒ O(1) 性质必须不成立；
* 人为把 mutator 路径放慢固定 µs ⇒ 常量因子预算必须红。
"""
from __future__ import annotations

import json
import pathlib
import time
import unittest

import pytest

import plaita.core.expression_parser as parser_mod
from plaita.core.context import ExecutionContext
from plaita.core.expression_parser import ExpressionParser

ITERATIONS = 20000
REPEATS = 5

_BASELINE_PATH = (pathlib.Path(__file__).resolve().parents[2]
                  / "scripts" / "issue69_mutation_matrix" / "perf_baseline.json")
_BASELINE = json.loads(_BASELINE_PATH.read_text(encoding="utf-8"))
_REFERENCE = _BASELINE["reference"]
_RATIOS = _BASELINE["ratios"]
_TOLERANCE = _BASELINE["tolerance"]

#: mutator 路径（r6 起纳入守卫）：
#: * ``_MUT_ALLOW``：目标位是 DEEP producer ⇒ 分类保证 fresh，不走 plan；
#: * ``_MUT_PLAN_ALLOW``：目标位是**同一次求值**里由嵌套 producer 造出的对象
#:   （嵌套表达式串共享作用域）⇒ 走 plan 且放行；
#: * ``_MUT_PLAN_REFUSE``：目标位是**上一次求值**物化的 ``$VAR`` ⇒ 走 plan 且
#:   default-deny 拒绝（verifier 测的 23.5µs 那条）。
_MUT_ALLOW = "$F.set($F.json_loads('{\"q\":0}'),'q',1)"
_MUT_PLAN_ALLOW = "$F.set($PROD.v,'k','v')"
_MUT_PLAN_REFUSE = "$F.set($VAR,'k','v')"


class _Cfg:
    expose_env = None
    global_context: dict = {}
    flow_id = "perf69"


def _big_payload(width: int) -> dict:
    payload = {"a": {"b": {"c": 1}}, "raw": '{"q": 0}'}
    for i in range(width):
        payload[f"k{i}"] = {"deep": [i, {"x": i}]}
    return payload


def _context(payload: dict) -> ExecutionContext:
    ctx = ExecutionContext()
    ctx.setup_flow(_Cfg(), ({},), {})
    ctx.set_state("$INPUT", payload)
    ctx.set_state("$VAR", ctx.evaluate({"a": 1}))
    # 嵌套表达式串挂在普通键下：`$F.set($PROD.v,…)` 让 plan-allow 分支可达。
    ctx.set_state("$PROD", {"v": "$F.json_loads('{}')"})
    return ctx


def _mutable_ids(obj, out: set) -> None:
    if isinstance(obj, dict):
        out.add(id(obj))
        for value in obj.values():
            _mutable_ids(value, out)
    elif isinstance(obj, (list, set, tuple, frozenset)):
        out.add(id(obj))
        for value in obj:
            _mutable_ids(value, out)


def _us_per_call(parser, expr: str, ctx: ExecutionContext,
                 iterations: int = ITERATIONS, repeats: int = REPEATS) -> float:
    best = float("inf")
    for _ in range(repeats):
        start = time.perf_counter()
        for _ in range(iterations):
            try:
                parser.evaluate(expr, ctx.context)
            except ValueError:
                pass                       # default-deny 分支：抛出也是被测路径的一部分
        best = min(best, (time.perf_counter() - start) / iterations * 1e6)
    return best


def _busy_wait_us(microseconds: float) -> None:
    end = time.perf_counter() + microseconds / 1e6
    while time.perf_counter() < end:
        pass


class TestExpressionPerfDoesNotScaleWithContext(unittest.TestCase):
    WIDTH = 4000

    @classmethod
    def setUpClass(cls) -> None:
        cls.parser = ExpressionParser.for_prefix("$")
        cls.small = _context({"a": {"b": {"c": 1}}, "raw": '{"q": 0}'})
        cls.big = _context(_big_payload(cls.WIDTH))

    def _assert_flat(self, expr: str) -> tuple:
        small = _us_per_call(self.parser, expr, self.small)
        big = _us_per_call(self.parser, expr, self.big)
        # O(1) 判据：大上下文不得把单次求值显著推高（给足 3x + 1µs 余量抗噪声）。
        self.assertLess(big, small * 3 + 1.0,
                        f"{expr}: us/call small={small:.3f} big={big:.3f} — "
                        "单次求值成本随上下文规模增长（无界证明回归）")
        return small, big

    def test_path_lookup_is_flat(self):
        small, big = self._assert_flat("$INPUT.a.b.c")
        print(f"\n[perf] $INPUT.a.b.c small={small:.3f}us big={big:.3f}us")

    def test_call_over_path_is_flat(self):
        small, big = self._assert_flat("$F.add($INPUT.a.b.c,1)")
        print(f"\n[perf] $F.add($INPUT.a.b.c,1) small={small:.3f}us big={big:.3f}us")

    def test_mutator_producer_target_is_flat(self):
        small, big = self._assert_flat(_MUT_ALLOW)
        print(f"\n[perf] {_MUT_ALLOW} small={small:.3f}us big={big:.3f}us")

    def test_mutator_freshness_plan_allow_is_flat(self):
        small, big = self._assert_flat(_MUT_PLAN_ALLOW)
        print(f"\n[perf] {_MUT_PLAN_ALLOW} small={small:.3f}us big={big:.3f}us")

    def test_mutator_default_deny_is_flat(self):
        small, big = self._assert_flat(_MUT_PLAN_REFUSE)
        print(f"\n[perf] {_MUT_PLAN_REFUSE} small={small:.3f}us big={big:.3f}us")

    def test_negative_control_full_traversal_trips_the_predicate(self):
        """人为在热路径上加一次全量遍历 ⇒ '成本不随规模增长' 必须不成立。"""
        saved = parser_mod._is_persisted_key_strict

        def _traversal_probe(context, root_key):
            seen: set = set()
            try:
                for key in list(context.keys()):
                    _mutable_ids(context[key], seen)
            except Exception:                     # noqa: BLE001 - 负控只需要成本
                pass
            return saved(context, root_key)

        parser_mod._is_persisted_key_strict = _traversal_probe
        try:
            small = _us_per_call(self.parser, _MUT_PLAN_REFUSE, self.small,
                                 iterations=200, repeats=2)
            big = _us_per_call(self.parser, _MUT_PLAN_REFUSE, self.big,
                               iterations=200, repeats=2)
        finally:
            parser_mod._is_persisted_key_strict = saved
        print(f"\n[perf-neg] small={small:.3f}us big={big:.3f}us")
        self.assertFalse(big < small * 3 + 1.0,
                         "negative control did not trip the flatness predicate — "
                         "the guard has no discriminating power")


class TestExpressionPerfConstantFactorBudget(unittest.TestCase):
    """r6：mutator 路径的**常量因子**预算（checked-in 基线 × tolerance）。

    比值 ``expr_us / $INPUT.a.b.c_us`` 与机器绝对速度无关。基线在
    ``scripts/issue69_mutation_matrix/perf_baseline.json``（含 producing 说明）。
    """

    @classmethod
    def setUpClass(cls) -> None:
        cls.parser = ExpressionParser.for_prefix("$")
        cls.ctx = _context({"a": {"b": {"c": 1}}, "raw": '{"q": 0}'})

    def _ratio(self, expr: str, iterations: int = ITERATIONS,
               repeats: int = REPEATS):
        reference = _us_per_call(self.parser, _REFERENCE, self.ctx,
                                 iterations=iterations, repeats=repeats)
        measured = _us_per_call(self.parser, expr, self.ctx,
                                iterations=iterations, repeats=repeats)
        return reference, measured, measured / reference

    @pytest.mark.timeout(900)  # 预算测量含 #203 数据面白名单检查开销，CI 2 核超 150s 默认门（marker 覆盖不动判据）
    def test_mutator_paths_stay_within_the_checked_in_budget(self):
        reference, _ref_us, _ = self._ratio(_REFERENCE)
        self.assertGreater(reference, 0.0)
        misses = []
        for expr, baseline in sorted(_RATIOS.items()):
            _ref, measured, ratio = self._ratio(expr)
            ceiling = baseline * _TOLERANCE
            print(f"\n[perf-budget] {expr}: {measured:.3f}us "
                  f"ratio={ratio:.2f} ceiling={ceiling:.2f}")
            if ratio > ceiling:
                misses.append((expr, round(ratio, 2), round(ceiling, 2)))
        self.assertEqual([], misses,
                         f"常量因子预算超限（baseline={_RATIOS} tol={_TOLERANCE}）: {misses}")

    def test_negative_control_slower_mutator_reddens_the_budget(self):
        """人为把目标位热路径放慢 ⇒ 比值必须越过 ceiling（守卫有区分力）。

        注入量按**实测 reference 单次成本的比例**（20×）取，而非固定 µs：
        固定 40µs 在 ~2µs/op 的开发机上 = 20×（比值 ~20 ≫ ceiling），但在
        ~48µs/op 的 2 核 CI 上只有 0.84×（实测比值 2.06 < ceiling 3.2，
        run de1fc13 假红）。注入与机器同速缩放后信噪比恒定；迭代数相应
        收缩以控时长（200 次 × ~1ms × 2 轮在 CI ≈ 0.4s）。
        """
        saved = parser_mod._is_persisted_key_strict
        reference, ref_us, _ = self._ratio(_REFERENCE)
        delay_us = max(40.0, 20.0 * ref_us)

        def _slow_probe(context, root_key):
            _busy_wait_us(delay_us)
            return saved(context, root_key)

        parser_mod._is_persisted_key_strict = _slow_probe
        try:
            _ref, measured, ratio = self._ratio(_MUT_PLAN_REFUSE,
                                                iterations=200, repeats=2)
        finally:
            parser_mod._is_persisted_key_strict = saved
        ceiling = _RATIOS[_MUT_PLAN_REFUSE] * _TOLERANCE
        print(f"\n[perf-budget-neg] reference={reference:.3f}us "
              f"delay={delay_us:.0f}us slow_mutator={measured:.3f}us "
              f"ratio={ratio:.2f} ceiling={ceiling:.2f}")
        self.assertGreater(ratio, ceiling,
                           "negative control did not redden the constant-factor budget — "
                           "the budget has no discriminating power")


if __name__ == "__main__":
    unittest.main()
