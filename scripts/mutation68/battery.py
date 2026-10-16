"""差分 oracle 的被测体：穷举调用并输出可比较的 JSON。

规模刻意压在"守卫若被破坏，最多分配几十 MB"的范围内——否则破坏守卫的变异体
会真的去造 25GB，既慢又可能 OOM。判定倍数不受影响：小尺寸 + 小阈值（环境变量
配置）足以暴露行为差异。
"""
import json
import os
import sys

out = {}
try:
    import plaita.core.expression as E
    import plaita.core.expression_parser as P

    def _n(x):
        return len(x) if hasattr(x, "__len__") else 0

    cases = []
    kinds = {
        "str": lambda n: "x" * n,
        "bytes": lambda n: b"x" * n,
        "bytearray": lambda n: bytearray(b"x" * n),
        "list": lambda n: [0] * n,
        "tuple": lambda n: (0,) * n,
    }
    # 大尺寸只配小倍数；大倍数只配极小尺寸（乘积 ≤ ~2e5）
    combos = [(0, 1), (0, 100_001), (1, 1), (1, 2), (1, 4), (2, 2),
              (100_000, 1), (100_000, 2), (100_001, 1)]
    for kname, maker in kinds.items():
        for size, n in combos:
            cases.append((f"mul:{kname}{size}*{n}", lambda m=maker, s=size, c=n: E._fn_mul(m(s), c)))
            cases.append((f"mul:{n}*{kname}{size}", lambda m=maker, s=size, c=n: E._fn_mul(c, m(s))))
    import array as _array
    for av in (_array.array("i", [0] * 10), _array.array("i", [0] * 100)):
        for n in (1, 2, 10**4, 10**6):
            cases.append((f"mul:array{len(av)}*{n}", lambda x=av, c=n: E._fn_mul(x, c)))
            cases.append((f"mul:{n}*array{len(av)}", lambda x=av, c=n: E._fn_mul(c, x)))
    for a in (0, 1, 2, 3, 10**6, -3, True, False):
        for b in (0, 1, 2, 7, 10**9, 0.5, "x", b"x", [1], None):
            cases.append((f"mul:num{a}*{b!r}", lambda x=a, y=b: E._fn_mul(x, y)))
    for a, b in [("a", "b"), ("a"*100, "b"*100), ("a"*100_000, "b"), ("a"*100_001, "b"),
                 ([1], [2]), ([0]*100_000, [0]*2), ((1,), (2,)), (b"a", b"b"),
                 (b"a", bytearray(b"b")), (bytearray(b"a"), b"b"),
                 ("a", [1]), (1, 2), (1, "a"), (None, None), ({}, {}), (set(), set()),
                 ([1], (2,)), ((1,), [2]), (bytearray(b"a"), b"b"), (b"a", "b")]:
        cases.append((f"add:{type(a).__name__}{_n(a)}+{type(b).__name__}",
                      lambda x=a, y=b: E._fn_add(x, y)))
        cases.append((f"extend:{type(a).__name__}{_n(a)}+{type(b).__name__}",
                      lambda x=a, y=b: E._fn_extend(x, y)))
    for a in ([1], [], "a", b"a", (1,), 1):
        for b in (0, 1, "z", [2]):
            cases.append((f"append:{type(a).__name__}+{type(b).__name__}",
                          lambda x=a, y=b: E._fn_append(x, y)))
    for items in ([], ["a"], ["a", "b"], ["b"], ["a"]*6, ["a"]*300, ["x"*1024]*200,
                  ["a"*100_000, "b"*100_000], "abc", "x"*100, b"ab", (b"a", b"b"),
                  ["a"*25000]*6, [1, 2]):
        for sep in ("", "-", "s"*300, "s"*25000, "s"*100_001, b"-", bytearray(b"-")):
            cases.append((f"join:{type(items).__name__}{_n(items)}:{_n(sep)}",
                          lambda i=items, s=sep: E._fn_join(i, s)))
    gen_items = [(c for c in ["a", "b"]), (c for c in ["a"]*20)]
    for gi in gen_items:
        cases.append((f"join:gen{len(list(gi))}",
                      lambda i=gi: E._fn_join(list(i) if False else (c for c in ["a"]*20), "-")))
    for s, old, new in [("abc", "a", "z"), ("a"*20_000, "a", "b"), ("a"*20_000, "a", "b"*1000),
                        ("a"*20_000, "", "b"*1000), ("a"*20_000, "", ""), ("abc", "", "-"),
                        (b"a"*20_000, b"a", b"b"*1000), (bytearray(b"a"*100), b"a", b"bb"),
                        (b"a"*1000, bytearray(b"a"), bytearray(b"b"*1000)),
                        (b"a"*1000, bytearray(b"a"), b"b"),
                        ("a"*100, "a", "b"*40), ("abc", 1, "z")]:
        cases.append((f"replace:{type(s).__name__}{_n(s)}:{_n(old)}:{_n(new)}",
                      lambda x=s, y=old, z=new: E._fn_replace(x, y, z)))
    for fmt, val in [("%100000000s", {"x": 1}), ("%(k)100000000s", {"k": "x"}),
                     ("%*d", (-100000000, 1)), ("%s", "a"), ("%s-%s", ("a", "b")),
                     ("%(a)s", {"a": "x"}), ("%(a)s%(b)s", {"a": "x", "b": "y"}),
                     ("%(a)s%(b)s", "x"), ("%s", {"a": 1}), ("%s%s", ["a", "b"]),
                     ("%s%s", "x"), ("%%(a)s", {"a": 1}), ("%%", 1),
                     ("%2000000d", 1), ("%*d", (2_000_000, 1)), ("%*d", (3, 1)),
                     ("%s"*3, "x"), ("%s"*3000, ["a"]*3000), ("%.2f", 3.14159),
                     ("order 12345: %s", "abc"), ("%5d", 1), ("abc", ()),
                     (b"%d", 1), (b"%*d", (2_000_000, 1))]:
        cases.append((f"mod:{fmt!r}:{type(val).__name__}", lambda f=fmt, v=val: E._fn_mod(f, v)))
    for a, b in [(10**9, 7), (7.5, 2), (10**50, 7)]:
        cases.append((f"mod:num{a}:{b}", lambda x=a, y=b: E._fn_mod(x, y)))
    for args in [("a", 1, None), ("a"*20_000, "b"), (10**100000,), (b"a", b"b"), (),
                 (123456789,), (["c"*10_000]*20,)]:
        cases.append((f"concat:{len(args)}", lambda x=args: E._fn_concat(*x)))
    for v in ["s", b"s", bytearray(b"s"), [1], (1,), 1, None, 1.5, {"a": 1}, {1},
              ["c"*100]*5, [{"k": "v"*100}]*5, 123456789]:
        cases.append((f"priv:seqkind:{type(v).__name__}", lambda x=v: E._seq_kind(x)))
        cases.append((f"priv:unit:{type(v).__name__}", lambda x=v: E._unit_for(x)))
        cases.append((f"priv:measuredunit:{type(v).__name__}", lambda x=v: E._measured_unit(x)))
        cases.append((f"priv:measure:{type(v).__name__}", lambda x=v: E._measure(x)))
        cases.append((f"priv:valuesize:{type(v).__name__}", lambda x=v: E._value_size(x)))
    for a, b in [("a", "b"), ([1], [2]), (1, 2), ("a", [1]), (b"a", bytearray(b"b")), (None, None)]:
        cases.append((f"priv:pair:{type(a).__name__}+{type(b).__name__}",
                      lambda x=a, y=b: E._pair_total(x, y)))
    for k1, k2 in [("str", "str"), ("str", "bytes"), ("bytes", "bytearray"),
                   ("list", "list"), ("list", "tuple"), (None, "str")]:
        cases.append((f"priv:compat:{k1}+{k2}", lambda x=k1, y=k2: E._kinds_compatible(x, y)))
    for n in (0, 1, 2, 3, 5, 100, 101, 400, 401, 1000, 100_000):
        cases.append((f"priv:allowed:{n}", lambda x=n: E._allowed_result_size(x)))
    # 当前实现的结构解析 / 计划（取代了已删除的 _format_width_bound/_conversion_count/_star_width）
    for fmt in ["%s", "%100d", "%.5f", "%*d", "%(a)s", "%%", "abc", "%5.2f", "order 999: %s"]:
        cases.append((f"priv:specs:{fmt}", lambda f=fmt: E._parse_format_specs(f, False)))
        cases.append((f"priv:bounds:{fmt}", lambda f=fmt: [
            E._spec_bound(sp, val, sp.width)
            for sp, val in zip(E._parse_format_specs(f, False) or [], [1, 2, 3])
        ]))
    for v in [(1, "a"), {"a": "b"}, "abc", 5, ["x"*10, 3]]:
        cases.append((f"priv:valsize:{type(v).__name__}", lambda x=v: E._format_values_size(x)))
    if hasattr(P, "_size_guard_result"):
        for label, func, value, args in [
            ("prechecked", E._fn_mul, "z" * 250_000, ()),
            ("custom-small", None, "abc", ("abc",)),
            ("custom-big", None, "z" * 250_000, ("z" * 250_000,)),
            ("custom-amp", None, "z" * 250_000, ("a",)),
            ("bytes", None, b"z" * 250_000, (b"z" * 250_000,)),
            ("unsized", None, 10**500000, ()),
        ]:
            cases.append((f"probe:{label}",
                          lambda v=value, a=args: P._size_guard_result(
                              "fn", v, E._measure_input(tuple(a)) if hasattr(E, "_measure_input") else 0)))
    parser = P.ExpressionParser.for_prefix("$")
    ctx = {"$INPUT": {"lst": [0], "s": "x" * 100_000, "words": ["w"] * 6, "sep": "s" * 300}}
    for expr in ["{% $F.mul('x', 100001) %}", "{% $F.mul(100001, 'x') %}",
                 "{% $F.mul($INPUT.lst, 100001) %}", "{% $F.add(1, 2) %}",
                 "{% $F.replace($INPUT.s, 'x', 'yy') %}", "{% $F.mod('%5d', 1) %}",
                 "{% $F.join($INPUT.words, $INPUT.sep) %}", "$F.upper('ab')",
                 "$F.concat('a', 1)"]:
        cases.append((f"eval:{expr[:36]}", lambda e=expr: parser.evaluate(e, ctx)))

    # --- 第六轮验收 必修 6.2：电池要能区分"非等价但行为不同"的变异 ---
    # (a) 被调用者改写实参后返回巨大结果（额度必须来自调用前的快照）
    class _Mutating:
        @staticmethod
        def mutate_and_return(lst):
            lst.clear()
            lst.extend(["A" * 30_000_000])
            return "Y" * 99_000_000

    registry = E.ExpressionRegistry()
    registry.register("evil", _Mutating.mutate_and_return, "evil")
    cases.append(("eval:mutating-callee",
                  lambda: parser.evaluate("$F.evil($INPUT.lst)", {"$INPUT": {"lst": [0]}}, registry)))

    # (b) 自报 itemsize/__len__ 的说谎缓冲对象 + %s
    class _LiarBuffer:
        itemsize = 25_000_000

        def __len__(self):
            return 1

        def __iter__(self):
            return self

        def __next__(self):
            raise StopIteration

        def __str__(self):
            return "x" * 99_000_000

    cases.append(("priv:content:liar-buffer", lambda: E._content_size(_LiarBuffer())))
    cases.append(("priv:render:liar-buffer", lambda: E._render_size(_LiarBuffer())))
    cases.append(("eval:liar-buffer-percent-s",
                  lambda: parser.evaluate("$F.mod('%s', $INPUT.o)", {"$INPUT": {"o": _LiarBuffer()}})))

    # (c) 自定义 __str__ 的未知对象（渲染投影必须保守）
    class _CustomStr:
        def __str__(self):
            return "z" * 99_000_000

    cases.append(("priv:render:custom-str", lambda: E._render_size(_CustomStr())))
    cases.append(("eval:template-interpolation",
                  lambda: parser.evaluate("{% $INPUT.o %}", {"$INPUT": {"o": _CustomStr()}})))

    # (f) 第七轮决定性漏洞：自定义 __str__ 的**映射/可重复迭代**对象（渲染前闸）
    class _LyingMapping:
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
        def __len__(self):
            return 0

        def __iter__(self):
            return iter(())

        def __str__(self):
            return "Z" * 99_000_000

    cases.append(("priv:render:lying-mapping", lambda: E._render_size(_LyingMapping())))
    cases.append(("priv:render:repeatable-huge-str", lambda: E._render_size(_RepeatableHugeStr())))
    cases.append(("eval:lying-mapping-percent-s",
                  lambda: parser.evaluate("$F.mod('%s', $INPUT.o)", {"$INPUT": {"o": _LyingMapping()}})))
    cases.append(("eval:repeatable-huge-str-template",
                  lambda: parser.evaluate("{% $INPUT.o %}", {"$INPUT": {"o": _RepeatableHugeStr()}})))
    cases.append(("eval:repeatable-huge-str-concat",
                  lambda: parser.evaluate("$F.concat($INPUT.o)", {"$INPUT": {"o": _RepeatableHugeStr()}})))

    # (g) 第七轮误拒回归：已知长度容器的元素数下界
    cases.append(("eval:sort-empty-strings",
                  lambda: parser.evaluate("$F.sort($INPUT.e)", {"$INPUT": {"e": [""] * 200000}})))
    cases.append(("eval:join-empty-strings",
                  lambda: parser.evaluate("$F.join($INPUT.e, 'x')", {"$INPUT": {"e": [""] * 200000}})))

    # (d) >1e6 项的容器不得饱和
    for n in (1_000_001, 1_000_002, 1_500_000):
        cases.append((f"priv:content:list-{n}", lambda k=n: E._content_size([0] * k)))

    # (e) 包装形态的消费型算子（partial/lambda）
    import functools as _functools
    for label, func in (("partial", _functools.partial(E._fn_join)),
                        ("lambda", lambda a, b: E._fn_join(a, b))):
        wrapped = E.ExpressionRegistry()
        wrapped.register("join", func, "join")
        cases.append((f"eval:wrapped-join-{label}",
                      lambda r=wrapped: parser.evaluate(
                          "$F.join($INPUT.g, '')",
                          {"$INPUT": {"g": (x for x in ["c" * 10000] * 20)}}, r)))

    for label, fn in cases:
        try:
            r = fn()
            out[label] = f"ok:{type(r).__name__}:{r!r}"[:200]
        except BaseException as exc:  # noqa: BLE001
            out[label] = f"{type(exc).__name__}:{exc}"[:200]
except BaseException:  # noqa: BLE001
    import traceback
    out["_import_error"] = traceback.format_exc()[-1200:]
print(json.dumps(out, sort_keys=True, ensure_ascii=False))
