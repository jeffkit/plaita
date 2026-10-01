"""test_expression_cache — compile-to-thunk 缓存的正确性回归。

2026-10 BFF 热路径评审要求的护栏（防缓存投毒）：

1. 同一表达式串在不同 context 下连续求值必须各自正确（thunk 只捕获结构、
   不捕获 context 值）——跨 context 串扰是本设计最致命的失效形态；
2. 「嵌套表达式字符串」递归在缓存命中路径上仍以父对象为 context 且 registry
   透传；
3. 多线程并发命中同一 thunk 不串扰（parallel 分支场景）；
4. 超长字符串（> _MAX_CACHED_LEN）不入缓存但求值正确；
5. ParseException（编译失败）不缓存——parse_function 的「失败原样返回」
   契约在重复调用上保持；
6. scoped registry 的 "undefined" 哨兵在缓存命中路径上依然生效；
7. LRU 容量上限生效。
"""

from __future__ import annotations

import threading
import unittest
from unittest import TestCase

from plaita.core.expression import ExpressionRegistry, FunctionCategory
from plaita.core.expression_parser import ExpressionParser


def _fresh() -> ExpressionParser:
    """新建独立 parser（绕开 per-prefix 单例，测试互不污染）。"""
    ExpressionParser._instances.clear()
    return ExpressionParser()


def _registry(**fns) -> ExpressionRegistry:
    reg = ExpressionRegistry()
    for name, fn in fns.items():
        reg.register(name, fn, FunctionCategory.TYPE)
    return reg


class TestCrossContextNoInterference(TestCase):
    """缓存投毒主护栏：同串跨 context 求值不串扰。"""

    def setUp(self):
        self.p = _fresh()

    def tearDown(self):
        ExpressionParser._instances.clear()

    def test_same_string_two_contexts(self):
        """先 A 后 B：第二次命中缓存，结果必须仍是 B 自己的。"""
        ctx_a = {"$INPUT": {"who": "Alice"}}
        ctx_b = {"$INPUT": {"who": "Bob"}}
        expr = "$INPUT.who"
        self.assertEqual(self.p.evaluate(expr, ctx_a), "Alice")
        self.assertEqual(self.p.evaluate(expr, ctx_b), "Bob")  # cache hit path
        self.assertEqual(self.p.evaluate(expr, ctx_a), "Alice")

    def test_same_function_call_two_contexts(self):
        ctx_a = {"$INPUT": {"n": 1}}
        ctx_b = {"$INPUT": {"n": 41}}
        reg = _registry(add=lambda a, b: a + b)
        expr = "$F.add($INPUT.n, 1)"
        self.assertEqual(self.p.evaluate(expr, ctx_a, reg), 2)
        self.assertEqual(self.p.evaluate(expr, ctx_b, reg), 42)

    def test_template_two_contexts(self):
        ctx_a = {"$INPUT": {"name": "X"}}
        ctx_b = {"$INPUT": {"name": "Y"}}
        expr = "hi {% $INPUT.name %}!"
        self.assertEqual(self.p.evaluate(expr, ctx_a), "hi X!")
        self.assertEqual(self.p.evaluate(expr, ctx_b), "hi Y!")

    def test_missing_key_after_hit_raises_same(self):
        """命中路径上 KeyError 语义与冷路径一致（root 缺失）。"""
        ctx_ok = {"$INPUT": {"x": 1}}
        ctx_bad = {"$OTHER": 1}
        expr = "$INPUT.x"
        self.assertEqual(self.p.evaluate(expr, ctx_ok), 1)
        with self.assertRaises(KeyError):
            self.p.evaluate(expr, ctx_bad)

    def test_registry_swap_on_hit(self):
        """同串先 scoped 后 default：函数解析必须按次取 registry。"""
        expr = "$F.only_in_scoped()"
        scoped = _registry(only_in_scoped=lambda: "scoped")
        # 冷路径：scoped 命中
        self.assertEqual(self.p.evaluate(expr, {}, scoped), "scoped")
        # 热路径换 default registry：应 NameError（default registry 拼写错误硬失败）
        with self.assertRaises(NameError):
            self.p.evaluate(expr, {}, None)
        # 再换回 scoped：仍正确
        self.assertEqual(self.p.evaluate(expr, {}, scoped), "scoped")


class TestNestedRecursionOnCacheHit(TestCase):
    """嵌套表达式字符串递归在缓存命中路径的行为。"""

    def setUp(self):
        self.p = _fresh()

    def tearDown(self):
        ExpressionParser._instances.clear()

    def test_nested_attr_uses_parent_context_on_hit(self):
        """属性值本身是表达式串：必须以父对象为 context 求值（第二次调用走缓存）。

        语义与历史一致：嵌套串引用的根由**包含它的对象**提供
        （参见 test_expression_parser_path_recursion.py 的 canonical 形态）。
        """
        reg = _registry(upper=lambda s: s.upper())
        leaf = {"first": "ada"}
        parent = {"$INPUT": {"name": "$NODE.first", "$NODE": leaf}}
        expr = "$INPUT.name"
        for _ in range(3):  # 第 2、3 次命中编译缓存
            self.assertEqual(self.p.evaluate(expr, parent, reg), "ada")

    def test_nested_recursion_registry_passthrough(self):
        """外层 registry 必须透传进嵌套求值（命中路径）。"""
        reg = _registry(upper=lambda s: s.upper())
        parent = {
            "$NODE": {"a": {"greeting": "$F.upper($GLOBAL.raw)",
                            "$GLOBAL": {"raw": "nested"}}}
        }
        expr = "$NODE.a.greeting"
        for _ in range(2):
            self.assertEqual(self.p.evaluate(expr, parent, reg), "NESTED")

    def test_nested_recursion_tracks_changing_parent(self):
        """父对象的嵌套串变化后，命中路径上取的是新值。"""
        leaf = {"first": "v1"}
        parent = {"$INPUT": {"name": "$NODE.first", "$NODE": leaf}}
        expr = "$INPUT.name"
        self.assertEqual(self.p.evaluate(expr, parent), "v1")
        leaf["first"] = "v2"
        self.assertEqual(self.p.evaluate(expr, parent), "v2")


class TestConcurrentCacheHits(TestCase):
    """多线程并发命中同一 thunk（parallel 分支场景）。"""

    def tearDown(self):
        ExpressionParser._instances.clear()

    def test_threads_different_contexts_same_string(self):
        p = _fresh()
        reg = _registry(add=lambda a, b: a + b)
        expr = "$F.add($INPUT.n, 1)"
        errors: list = []

        def worker(n):
            ctx = {"$INPUT": {"n": n}}
            try:
                for _ in range(200):
                    got = p.evaluate(expr, ctx, reg)
                    if got != n + 1:
                        errors.append(f"thread n={n}: got {got}")
                        return
            except Exception as e:  # noqa: BLE001
                errors.append(f"thread n={n}: {type(e).__name__}: {e}")

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])


class TestCacheBoundaries(TestCase):
    """缓存边界行为。"""

    def setUp(self):
        self.p = _fresh()

    def tearDown(self):
        ExpressionParser._instances.clear()

    def test_long_string_evaluated_correctly_uncached(self):
        """> _MAX_CACHED_LEN 的串不入缓存，但求值正确。"""
        long_body = "x" * 5000
        template = f"head {{% $INPUT.v %}} {long_body}"
        self.assertEqual(
            self.p.evaluate(template, {"$INPUT": {"v": 7}}),
            f"head 7 {long_body}",
        )
        self.assertNotIn(template, self.p._compile_cache)

    def test_parse_exception_not_cached(self):
        """编译失败不缓存：parse_function 反复调用反复原样返回。"""
        bad = "$F.(invalid)"
        for _ in range(3):
            self.assertEqual(self.p.parse_function(bad, {}), bad)
        self.assertNotIn(bad, self.p._compile_cache)

    def test_lru_eviction(self):
        """超过 maxsize 后最旧条目被逐出，功能不受影响。"""
        p = _fresh()
        old_max = ExpressionParser._MAX_CACHE_ENTRIES
        ExpressionParser._MAX_CACHE_ENTRIES = 4
        try:
            for i in range(10):
                p.evaluate(f"$INPUT.k{i}", {"$INPUT": {"k0": 0}})
            self.assertLessEqual(len(p._compile_cache), 4)
            # 被逐出的旧 key 再次求值仍正确（重新编译）
            self.assertEqual(p.evaluate("$INPUT.k0", {"$INPUT": {"k0": 0}}), 0)
        finally:
            ExpressionParser._MAX_CACHE_ENTRIES = old_max

    def test_scoped_registry_undefined_on_hit(self):
        """scoped registry 未注册函数：命中路径上仍返回 'undefined' 哨兵。"""
        scoped = _registry()
        expr = "$F.not_registered()"
        for _ in range(2):
            self.assertEqual(self.p.evaluate(expr, {}, scoped), "undefined")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
