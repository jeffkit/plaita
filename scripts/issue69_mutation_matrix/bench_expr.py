#!/usr/bin/env python3
"""plaita#69 r6 —— 表达式求值的 **µs 级**基准（base / r4 / r5 / r6 四列同源）。

用法（在每个 revision 的 checkout 内跑同一条命令）::

    cd <checkout> && PYTHONPATH=. python scripts/issue69_mutation_matrix/bench_expr.py

它只测 ``ExpressionParser.evaluate`` 的热路径表达式（不经节点/事件层）：
``$INPUT.a.b.c``、``$F.add($INPUT.a.b.c,1)``、``$F.eq($INPUT.a.b.c,1)``。
输出每调用微秒数（取 ``REPEATS`` 轮的最小值，抗调度噪声）。

r5 的 freshness 登记只发生在**函数调用产生容器结果**与**容器重建**上（O(1) 外壳 /
DEEP producer 的闭包），路径表达式不登记、不查询 —— 因此 r5 不应比 r4 慢。
"""
from __future__ import annotations

import os
import sys
import time

import plaita

_ROOT = os.environ.get("PLAITA_BENCH_ROOT")
if _ROOT:
    real = os.path.realpath(plaita.__file__)
    assert real.startswith(os.path.realpath(_ROOT)), (real, _ROOT)

from plaita.core.context import ExecutionContext
from plaita.core.expression_parser import ExpressionParser

EXPRESSIONS = (
    "$INPUT.a.b.c",
    "$F.add($INPUT.a.b.c,1)",
    "$F.eq($INPUT.a.b.c,1)",
    "$F.get($INPUT.a.b,'b')",
    "$F.json_loads($INPUT.raw)",
    # r6：mutator 路径（目标位分类 allow / freshness plan allow / default-deny）
    "$F.set($F.json_loads('{\"q\":0}'),'q',1)",
    "$F.set($PROD.v,'k','v')",
    "$F.set($VAR,'k','v')",
)
ITERATIONS = 30000
REPEATS = 7


class _Cfg:
    expose_env = None
    global_context: dict = {}
    flow_id = "bench69"


def _context() -> ExecutionContext:
    ctx = ExecutionContext()
    ctx.setup_flow(_Cfg(), ({},), {})
    ctx.set_state("$INPUT", {"a": {"b": {"c": 1}}, "raw": '{"q": 0}'})
    # r6：`$VAR` 是**上一次**求值物化的对象（default-deny 路径）；
    # `$PROD.v` 是同一次求值里由嵌套 producer 造出的对象（plan-allow 路径）。
    ctx.set_state("$VAR", ctx.evaluate({"a": 1}))
    ctx.set_state("$PROD", {"v": "$F.json_loads('{}')"})
    return ctx


def main() -> int:
    parser = ExpressionParser.for_prefix("$")
    ctx = _context()
    print(f"# plaita={os.path.realpath(plaita.__file__)}")
    print(f"# iterations={ITERATIONS} repeats={REPEATS} (min per-call µs)")
    print("| expression | us/call |")
    print("|---|---|")
    for expr in EXPRESSIONS:
        best = float("inf")
        for _ in range(REPEATS):
            start = time.perf_counter()
            for _ in range(ITERATIONS):
                try:
                    parser.evaluate(expr, ctx.context)
                except ValueError:
                    pass                   # default-deny 分支：抛出也是被测路径的一部分
            best = min(best, (time.perf_counter() - start) / ITERATIONS * 1e6)
        print(f"| `{expr}` | {best:.3f} |")
    return 0


if __name__ == "__main__":
    sys.exit(main())
