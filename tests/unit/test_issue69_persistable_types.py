"""plaita#69 r7 —— **可持久化标量**的表驱动守卫（Finding 2）。

r6 的 ``_engine_copy`` 用一份 ad-hoc 白名单（``str/bytes/int/float/bool/complex/
NoneType``）判标量，于是 ``Decimal`` / ``datetime`` / ``date``（``plaita/core/types.py``
的 ``native_types`` 明确声明为 DSL 可持久化类型，``plaita/io.py`` 的
``Property.default_value`` 也用它们）在**值参去别名**时被具名拒绝：

    $INPUT.amount 是 Decimal
    $F.set($F.json_loads('{}'),'k',$F.add($INPUT.amount,1))   # r5 OK → r6 ValueError

r7 把接受/不可变集合**从仓内类型表推导**（``native_types``），并用本文件机械钉住：

* ``PERSISTABLE_SCALAR_TYPES`` 的推导部分 == ``native_types`` 展平去容器；
* ``io.py`` 的 ``Property.default_value`` 注解里的每个具体类型都在数据模型里
  （将来 io.py 加类型而 types.py 没跟上 ⇒ 本测试红）；
* 表驱动：``native_types`` 的每个条目都能通过 ``_engine_copy`` 且**值参去别名**
  （真实 DSL 表达式 ``$F.set($V,'k',$F.get($INPUT,'v'))`` 走 ``_detach_alias``）；
* 每个**有 DSL 生成表达式**的标量再走一次 ``$F.set($V,'k',<expr>)``；
* 反面：真·非数据模型的可变对象仍具名拒绝（fail-closed）。
"""
from __future__ import annotations

import typing
import unittest

from plaita.core import types
from plaita.core.context import ExecutionContext
from plaita.core.expression_parser import (
    ExpressionParser,
    _EngineCopyRefused,
    _engine_copy,
    freshness_scope,
)

PARSER = ExpressionParser.for_prefix("$")


class _Cfg:
    expose_env = None
    global_context: dict = {}
    flow_id = "r7-types"


def _flatten(declared):
    return declared if isinstance(declared, tuple) else (declared,)


def _native_scalars():
    found = []
    for declared in types.native_types.values():
        for candidate in _flatten(declared):
            if candidate in types.PERSISTABLE_CONTAINER_TYPES:
                continue
            if candidate not in found:
                found.append(candidate)
    return found


def _native_containers():
    found = []
    for declared in types.native_types.values():
        for candidate in _flatten(declared):
            if candidate in types.PERSISTABLE_CONTAINER_TYPES and candidate not in found:
                found.append(candidate)
    return found


def _context() -> ExecutionContext:
    ctx = ExecutionContext()
    ctx.setup_flow(_Cfg(), ({},), {})
    return ctx


#: 每种 ``native_types`` 类型一个样本值 + （可选）一个 DSL 里能生成该类型的表达式。
def _sample_for(native_type):
    from datetime import date, datetime
    from decimal import Decimal
    if native_type is str:
        return "text", "$F.concat('a','b')"
    if native_type is bool:
        return True, "$F.eq(1,1)"
    if native_type is int:
        return 7, "$F.add(1,1)"
    if native_type is float:
        return 1.5, "$F.div(3,2)"
    if native_type is Decimal:
        return Decimal("1.5"), "$F.add($INPUT.amount,1)"
    if native_type is datetime:
        return datetime(2020, 1, 2, 3, 4, 5), None
    if native_type is date:
        return date(2020, 1, 2), None
    if native_type is type(None):
        return None, "$F.json_loads('null')"
    if native_type is bytes:
        return b"b", None
    if native_type is complex:
        return 3 + 4j, None
    raise AssertionError(f"no sample for {native_type!r}")


def _sample_for_container(container_type):
    if container_type is dict:
        return {"a": 1}, '$F.json_loads(\'{"a": 1}\')'
    if container_type is list:
        return [1, 2], "$F.split('a,b',',')"
    if container_type is tuple:
        return (1, 2), None
    if container_type is set:
        return {1, 2}, None
    if container_type is frozenset:
        return frozenset([1]), None
    raise AssertionError(f"no sample for {container_type!r}")


def _all_samples():
    """``(type, sample_value, dsl_producer_or_None, is_container)`` for the whole model."""
    out = []
    for candidate in dict.fromkeys(_native_scalars() + [bytes, complex]):
        value, expr = _sample_for(candidate)
        out.append((candidate, value, expr, False))
    for candidate in _native_containers():
        value, expr = _sample_for_container(candidate)
        out.append((candidate, value, expr, True))
    return out


class TestIssue69PersistableTypeTable(unittest.TestCase):
    def test_scalar_table_is_derived_from_native_types(self):
        derived = _native_scalars()
        self.assertEqual(tuple(derived), types.PERSISTABLE_SCALAR_TYPES[:len(derived)])
        # 额外项只能是 r6 起就接受的不可变类型（无法成为就地写穿通道）。
        self.assertEqual((bytes, complex), types.PERSISTABLE_SCALAR_TYPES[len(derived):])

    def test_every_native_type_is_in_the_data_model(self):
        for declared in types.native_types.values():
            for candidate in _flatten(declared):
                with self.subTest(native=candidate.__name__):
                    self.assertIn(candidate, types.PERSISTABLE_TYPES)

    def test_io_default_value_types_are_a_subset_of_the_data_model(self):
        """``plaita/io.py`` 的 ``Property.default_value`` 注解必须被数据模型覆盖。"""
        from plaita.io import Property
        annotation = Property.model_fields["default_value"].annotation
        args = typing.get_args(annotation)
        self.assertTrue(args, "default_value annotation has no Union args")
        for arg in args:
            if arg is type(None):
                continue
            if arg in (list, typing.List) or typing.get_origin(arg) is list:
                self.assertIn(list, types.PERSISTABLE_CONTAINER_TYPES)
                continue
            if arg in (dict, typing.Dict) or typing.get_origin(arg) is dict:
                self.assertIn(dict, types.PERSISTABLE_CONTAINER_TYPES)
                continue
            with self.subTest(io_type=getattr(arg, "__name__", repr(arg))):
                self.assertIn(arg, types.PERSISTABLE_TYPES)

    def test_every_native_type_survives_the_engine_copier(self):
        for candidate, value, _expr, _is_container in _all_samples():
            with self.subTest(native=candidate.__name__):
                copied = _engine_copy(value)
                self.assertEqual(value, copied)

    def test_every_native_type_survives_a_value_argument_detach(self):
        """``_detach_alias``（值参去别名的唯一实现）必须接受表里的每个类型。"""
        from plaita.core.expression_parser import _detach_alias

        for candidate, value, _expr, is_container in _all_samples():
            with self.subTest(native=candidate.__name__):
                detached = _detach_alias(
                    value, "slice", (0, 1, "$INPUT.v", "$INPUT"))
                self.assertEqual(value, detached)
                if is_container and candidate in types.PERSISTABLE_MUTABLE_TYPES:
                    self.assertIsNot(value, detached)
                    self.assertIs(type(detached), candidate)

    def test_shallow_producer_value_over_a_root_is_detached_end_to_end(self):
        """真实 DSL 浅 producer（``add``/``mul``/``slice``）值参去别名：r6 误拒的回归面。"""
        from decimal import Decimal
        cases = (
            ("Decimal amount", {"amount": Decimal("1.5")},
             "$F.set($F.json_loads('{}'),'k',$F.add($INPUT.amount,1))"),
            ("list mul", {"v": [1, 2]},
             "$F.set($F.json_loads('{}'),'k',$F.mul($INPUT.v,1))"),
            ("str slice", {"v": "abc"},
             "$F.set($F.json_loads('{}'),'k',$F.slice($INPUT.v,0,1))"),
        )
        for label, payload, expr in cases:
            with self.subTest(case=label):
                ctx = _context()
                ctx.set_state("$INPUT", payload)
                with freshness_scope():
                    self.assertIsNone(PARSER.evaluate(expr, ctx.context))

    def test_every_producible_native_type_survives_a_value_argument_producer(self):
        """``$F.set($V,'k',<expr producing that type>)``：有生成表达式者逐个跑。"""
        covered = []
        for candidate, value, expr, _is_container in _all_samples():
            if expr is None:
                continue
            with self.subTest(native=candidate.__name__):
                ctx = _context()
                ctx.set_state("$INPUT", {"v": value,
                                         "amount": value if candidate.__name__ == "Decimal" else 1})
                with freshness_scope():
                    ctx.set_state("$V", ctx.evaluate({}))
                    ctx.set_state("$V0", ctx.evaluate({}))
                    PARSER.evaluate(f"$F.set($V,'k',{expr})", ctx.context)
                    self.assertIn("k", ctx.get_state("$V"))
                covered.append(candidate)
        # 无 DSL 生成表达式的类型：tuple/set/frozenset/datetime/date/bytes/complex
        # 由去别名用例覆盖（文档明说 DSL 数组归一为 list）。
        self.assertIn(dict, covered)
        self.assertIn(list, covered)
        self.assertIn(float, covered)

    def test_decimal_scalar_accumulation_is_restored(self):
        """Finding 2 的最锐利用例：``Decimal`` 是不可变标量，r5 OK / r6 误拒。"""
        from decimal import Decimal
        ctx = _context()
        ctx.set_state("$INPUT", {"amount": Decimal("1.5")})
        with freshness_scope():
            self.assertIsNone(PARSER.evaluate(
                "$F.set($F.json_loads('{}'),'k',$F.add($INPUT.amount,1))", ctx.context))
        # 表达式返回 None（set 的语义）；用同一形状把它存进 $V 再看值。
        with freshness_scope():
            ctx.set_state("$V", ctx.evaluate({}))
            PARSER.evaluate("$F.set($V,'k',$F.add($INPUT.amount,1))", ctx.context)
            self.assertEqual(Decimal("2.5"), ctx.get_state("$V")["k"])
            self.assertIs(type(ctx.get_state("$V")["k"]), Decimal)

    def test_genuine_non_data_model_mutables_are_still_refused(self):
        from collections.abc import MutableMapping

        class MyMapping(MutableMapping):
            def __init__(self):
                self._d = {"a": 1}

            def __getitem__(self, key):
                return self._d[key]

            def __setitem__(self, key, value):
                self._d[key] = value

            def __delitem__(self, key):
                del self._d[key]

            def __iter__(self):
                return iter(self._d)

            def __len__(self):
                return len(self._d)

        for value in (MyMapping(), bytearray(b"x")):
            with self.subTest(value=type(value).__name__):
                with self.assertRaises(_EngineCopyRefused) as cm:
                    _engine_copy(value)
                self.assertEqual(type(value).__name__, cm.exception.type_name)


if __name__ == "__main__":
    unittest.main()
