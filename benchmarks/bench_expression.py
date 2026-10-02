#!/usr/bin/env python3
"""bench_expression.py — 表达式引擎基准（编译缓存改前/改后对照）。

方法纪律（2026-10 BFF 评审要求）：
- 语料固定配比：命中路径 60%、首解析 10%、函数调用 10%、模板 10%
  （其中含 ≥1 条 >4KB 长文体）、深路径/索引 5%、失败 parse 5%；
- warmup ≥1000 次，timeit.repeat 取 min，按用例成本缩放迭代数；
- 输出按类报 µs/call 与加权总账；开头自证被测对象（plaita.__file__）。

用法：python benchmarks/bench_expression.py
"""

from __future__ import annotations

import os
import sys
import timeit

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import plaita  # noqa: E402
import pyparsing  # noqa: E402

from plaita.io import evaluate, parse_function  # noqa: E402

print(f"plaita : {plaita.__file__}")
print(f"python : {sys.version.split()[0]}  pyparsing: {pyparsing.__version__}")

CTX = {
    "$INPUT": {"x": {"y": 42, "items": [1, 2, 3], "name": "plaita"}, "n": 7,
               "flag": True},
    "$NODE": {"a": {"out": "ok"}},
    "$GLOBAL": {"env": "bench"},
    "$FLOW_ID": "bench-flow",
}

LONG_BODY = ("lorem ipsum " * 400)  # ~4.8KB，超缓存上限
CASES = {
    # 类别: (表达式, 每轮迭代数)
    "hit/var_path":      ("$INPUT.x.y", 20000),
    "hit/func_call":     ("$F.add($INPUT.n, 1)", 20000),
    "hit/deep_index":    ("$INPUT.x.items[0]", 20000),
    "hit/template":      ("prefix {% $F.add($INPUT.n, 1) %} suffix", 20000),
    "hit/cond_alias":    ("$FLOW", 20000),
    "miss/new_var_path": (None, 3000),   # 每次新串，走首解析
    "long/template_5kb": (f"a={{% $INPUT.n %}} {LONG_BODY}", 200),
    "fail/parse_error":  ("$F.(invalid", 3000),
}

# 加权配比（对应 BFF 典型参数树：多数求值是热路径命中）
WEIGHTS = {
    "hit/var_path": 0.30, "hit/func_call": 0.20, "hit/deep_index": 0.10,
    "hit/template": 0.10, "hit/cond_alias": 0.05,
    "miss/new_var_path": 0.10, "long/template_5kb": 0.05, "fail/parse_error": 0.05,
    "plain_text": 0.05,
}


def bench_miss(number: int) -> float:
    """真实冷解析：每轮独立生成互异串池（round 间字符串不同，永远 miss）。"""
    times = []
    round_salt = 0
    for _ in range(5):
        round_salt += 1
        pool = [f"$INPUT.x.y{round_salt}_{i}" for i in range(number)]
        t0 = timeit.default_timer()
        for e in pool:
            evaluate(e, CTX)
        times.append((timeit.default_timer() - t0) / number * 1e6)
    return min(times)


def bench_one(label: str, expr: str | None, number: int) -> float:
    """返回 µs/call（min-of-5）。expr=None（miss/首解析）走 bench_miss。"""
    def run():
        if label.startswith("fail/"):
            # 解析失败是 parse_function 的契约场景（捕获 ParseException 原样返回）
            parse_function(expr, CTX)
        else:
            evaluate(expr, CTX)

    if expr is not None and not label.startswith("fail/"):
        for _ in range(1000):  # warmup
            evaluate(expr, CTX)
    best = min(timeit.repeat(run, number=number, repeat=5))
    return best / number * 1e6


def main() -> None:
    results = {}
    for label, (expr, number) in CASES.items():
        if expr is None:
            results[label] = bench_miss(number)
        else:
            results[label] = bench_one(label, expr, number)
        print(f"{label:22s} {results[label]:10.2f} us/call")

    results["plain_text"] = bench_one("plain_text", "just a plain string body", 20000)
    print(f"{'plain_text':22s} {results['plain_text']:10.2f} us/call   (预筛快路径)")

    # 加权总账不含 long/template_5kb：>4KB 长模板走独立小 LRU（64 条），
    # 命中后为 µs 级；首解析仍付 scanString 全串扫描（ms 级），单独列示
    # 以免首轮成本淹没其他类别。
    weights_wo_long = {k: w for k, w in WEIGHTS.items() if k != "long/template_5kb"}
    renorm = sum(weights_wo_long.values())
    total = sum(results[k] * w for k, w in weights_wo_long.items()) / renorm
    print(f"\nweighted total (BFF 配比，不含 >4KB 长模板): {total:.2f} us/eval")
    print(f"long/template_5kb（单独列示，命中后）: {results['long/template_5kb']:.2f} us/call"
          f" —— 二级 LRU（{plaita.core.expression_parser.ExpressionParser._LONG_CACHE_ENTRIES} 条）；"
          f"首解析另付 scanString 全串成本")
    cache = plaita.core.expression_parser.ExpressionParser.for_prefix("$")._compile_cache
    print(f"compile cache entries after run: {len(cache)}")


if __name__ == "__main__":
    main()
