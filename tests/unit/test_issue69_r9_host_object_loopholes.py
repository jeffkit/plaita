"""plaita#69 **r9** —— r8 复核（PASS-WITH-GAPS）报出的 A/B/C/D/E 五个真实缺陷。

r8 的快照路径已确认干净（0 宿主钩子、精确基类容器、绝不 ``is`` 活对象），但复核
又钉住五类**规则级/承诺级**漏洞：

* **A（最严重，规则级）** 标量归一化用内建构造器（``str(value)`` / ``bytes(value)``
  / ``bool(value)`` / ``datetime(value.year, …)``）—— 它们会落回**宿主覆写**：
  ``__str__`` 返回 ``'<LiarStr SECRET=…>'`` 的 ``str`` 子类被**静默**写进数据模型
  （宿主 repr 里的秘密变成数据，无任何错误）；伪造的 ``bool`` 触发宿主 ``__len__``
  并漏出裸 ``RuntimeError``；伪造的 ``bytes`` 触发宿主 ``__iter__`` 同理。
* **B（规则级）** ``class HashableDict(dict): __hash__ = …`` 做 ``set`` 成员被归一化成
  普通 ``dict`` ⇒ 集合重建抛裸 ``TypeError: unhashable type: 'dict'``（DSL 可达：
  循环条件引用含该集合的 ``$INPUT``）。
* **C（承诺级）** r8 只把**值**路径改成基类协议；``dict`` 的**键**与 ``set`` 的
  **成员**仍按引用进快照（``key_is_live`` / ``el_is_live``，无错误）。
* **D（承诺级）** ``class M(MutableMapping, dict)``（数据在 ``_d``，dict 存储空）：
  r7 快照 ``{'m': {'a': 1}}`` → r8 ``{'m': {}}`` —— 静默丢数据。
* **E（文档级）** 文档承诺的逃生口 ``$F.json_loads($F.json_dumps(<part>))`` 有损。

本文件按**测试**钉住修复后的契约（A/B/C/D），E 的注记由
``TestIssue69R9EscapeHatchIsLossy`` 同时钉住"类型行为 + 两处文档文字"。
"""
from __future__ import annotations

import datetime
import enum
import math
import pathlib
import unittest
from collections import OrderedDict, defaultdict
from collections.abc import MutableMapping
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from plaita.core.errors import StateSnapshotError
from plaita.core.expression_parser import (
    _EngineCopyRefused,
    _engine_copy,
    freshness_scope,
)
from plaita.core.state import _snapshot_value
from tests.unit.test_issue69_r8_snapshot_discipline import _run_loop

#: 宿主钩子计数器（跨测试清空）：任何一次调用都说明标量归一化没走基类协议。
HOOKS: list = []

#: r10：DST 歧义墙钟时间（PEP 495 ``fold``）必须能观察到的真实时区。
NY = ZoneInfo("America/New_York")


class LiarStr(str):
    """``__str__``/``__repr__`` 都返回秘密 —— r8 下 ``str(value)`` 会调用前者。"""

    SECRET = "<LiarStr SECRET=TOPSECRET>"

    def __str__(self):
        HOOKS.append("str.__str__")
        return self.SECRET

    def __repr__(self):
        HOOKS.append("str.__repr__")
        return self.SECRET


class HookedBytes(bytes):
    def __bytes__(self):
        HOOKS.append("bytes.__bytes__")
        raise RuntimeError("host __bytes__ must never be called")

    def __iter__(self):
        HOOKS.append("bytes.__iter__")
        raise RuntimeError("host __iter__ must never be called")

    def __len__(self):
        HOOKS.append("bytes.__len__")
        raise RuntimeError("host __len__ must never be called")

    def __repr__(self):
        HOOKS.append("bytes.__repr__")
        return "<LiarBytes SECRET=TOPSECRET>"


class HookedInt(int):
    def __int__(self):
        HOOKS.append("int.__int__")
        raise RuntimeError("host __int__ must never be called")

    def __repr__(self):
        HOOKS.append("int.__repr__")
        return "<LiarInt SECRET=TOPSECRET>"


class HookedFloat(float):
    def __float__(self):
        HOOKS.append("float.__float__")
        raise RuntimeError("host __float__ must never be called")


class HookedComplex(complex):
    def __complex__(self):
        HOOKS.append("complex.__complex__")
        raise RuntimeError("host __complex__ must never be called")

    def __repr__(self):
        HOOKS.append("complex.__repr__")
        return "<LiarComplex SECRET=TOPSECRET>"


class HookedDecimal(Decimal):
    def __str__(self):
        HOOKS.append("Decimal.__str__")
        return "999999"

    def __repr__(self):
        HOOKS.append("Decimal.__repr__")
        return "<LiarDecimal SECRET=TOPSECRET>"


class HookedDateTime(datetime.datetime):
    @property
    def year(self):
        HOOKS.append("datetime.year")
        raise RuntimeError("host year property must never be read")

    def __repr__(self):
        HOOKS.append("datetime.__repr__")
        return "<LiarDateTime SECRET=TOPSECRET>"


class HookedDate(datetime.date):
    @property
    def toordinal(self):
        HOOKS.append("date.toordinal")
        raise RuntimeError("host toordinal must never be called")

    def __repr__(self):
        HOOKS.append("date.__repr__")
        return "<LiarDate SECRET=TOPSECRET>"


class HashableDict(dict):
    """病态但是合法的宿主类：``dict`` 子类 + 可哈希 ⇒ 能当 ``set`` 成员 / 字典键。"""

    __hash__ = object.__hash__


class MappingBackedDict(MutableMapping, dict):
    """``class M(MutableMapping, dict)``：数据存在 ``_d``，**dict 存储恒空**（r9 D）。"""

    def __init__(self, **kwargs):
        self._d = dict(kwargs)

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


def _float_key(value: float) -> tuple:
    """NaN/±0.0 也要能比较：``==`` 对 NaN 恒假、对 ``0.0``/``-0.0`` 看不出符号。"""
    if math.isnan(value):
        return ("nan", math.copysign(1.0, value))
    return ("num", value, math.copysign(1.0, value))


def _base_datetime_twin(value: Any) -> datetime.datetime:
    """用**基类描述符**把任意 ``datetime``（含宿主子类）重建为精确 ``datetime``。

    这是"值保真"的**定义式**：拷贝必须与它逐字段相等。派生量
    （``timestamp()``/``utcoffset()``/``tzname()``）在宿主子类上可能被覆写，所以
    一律从这个 twin 上取，判定工具本身不能成为漏洞。
    """
    dt = datetime.datetime
    return dt(dt.year.__get__(value), dt.month.__get__(value), dt.day.__get__(value),
              dt.hour.__get__(value), dt.minute.__get__(value),
              dt.second.__get__(value), dt.microsecond.__get__(value),
              tzinfo=dt.tzinfo.__get__(value), fold=dt.fold.__get__(value))


def _base_date_twin(value: Any) -> datetime.date:
    kind = datetime.date
    return kind(kind.year.__get__(value), kind.month.__get__(value),
                kind.day.__get__(value))


def observable_fields(value: Any) -> tuple:
    """标量的**全部可观察字段**，一律经**基类描述符**读（宿主覆写不影响判定）。

    plaita#69 **r10**：``type(copy) is base`` + ``==`` **不足以**判决 A —— DST 歧义
    墙钟时间的 ``fold`` 被丢掉时这两个判据**仍然成立**（``type`` 是精确 ``datetime``，
    而 ``copy == value`` 与 ``hash()`` 对 fold 全盲：同一墙钟的 fold=0/fold=1 实测
    ``==`` 为 True、``hash()`` 相等，见
    ``TestIssue69R10FoldIsValuePreserving::test_equality_exact_type_and_hash_are_fold_blind``）。
    r9 的 ``assertEqual(expected, copied)`` 正是这样漏掉了 ``fold``。

    每个标量的可观察面穷举成可比较的元组：

    * ``datetime``：7 个分量 + ``tzinfo`` 对象 + ``fold`` + ``timestamp()`` +
      ``utcoffset()`` + ``tzname()``（``tzinfo`` 的**身份**另由
      :func:`assert_value_preserving` 断）；
    * ``date``：``year/month/day`` + ``toordinal()``；
    * ``Decimal``：``is_signed`` + ``as_tuple``（符号/系数/指数）+ ``str()``；
    * ``str``/``bytes``：完整内容；``int``/``float``/``complex``：完整数值（含 ±0 与 NaN）。
    """
    if isinstance(value, datetime.datetime):
        twin = _base_datetime_twin(value)
        return ("datetime", twin.year, twin.month, twin.day, twin.hour, twin.minute,
                twin.second, twin.microsecond, twin.tzinfo, twin.fold,
                twin.timestamp(), twin.utcoffset(), twin.tzname())
    if isinstance(value, datetime.date):
        twin = _base_date_twin(value)
        return ("date", twin.year, twin.month, twin.day, twin.toordinal())
    if isinstance(value, Decimal):
        return ("Decimal", Decimal.is_signed(value), Decimal.as_tuple(value),
                Decimal.__str__(value))
    if isinstance(value, bool):
        return ("bool", bool(value))
    if isinstance(value, int):
        return ("int", int.__int__(value))
    if isinstance(value, float):
        return ("float", _float_key(float.__float__(value)))
    if isinstance(value, complex):
        return ("complex",
                tuple(_float_key(part) for part in complex.__getnewargs__(value)))
    if isinstance(value, str):
        return ("str", str.__str__(value))
    if isinstance(value, bytes):
        return ("bytes", bytes.__getitem__(value, slice(None)))
    return ("other", type(value).__name__, repr(value))


def assert_value_preserving(test_case: unittest.TestCase, original: Any,
                            kind: type) -> Any:
    """A 的**完整**判决：精确基类 + 全部可观察字段 + ``tzinfo`` 身份。"""
    copied = _engine_copy(original)
    test_case.assertIs(type(copied), kind)
    test_case.assertEqual(observable_fields(original), observable_fields(copied))
    if isinstance(original, datetime.datetime):
        dt = datetime.datetime
        test_case.assertIs(dt.tzinfo.__get__(copied), dt.tzinfo.__get__(original))
        twin = _base_datetime_twin(original)
        test_case.assertEqual(twin.fold, copied.fold)
        test_case.assertEqual(twin.timestamp(), copied.timestamp())
        test_case.assertEqual(twin.utcoffset(), copied.utcoffset())
        test_case.assertEqual(twin.tzname(), copied.tzname())
    return copied


def _liar(base: type, name: str):
    """``__class__`` 伪造对象：能骗过 ``isinstance``，过不了 C 基类描述符。"""

    class Liar:
        __class__ = base

        def __len__(self):
            HOOKS.append("liar.__len__")
            raise RuntimeError("host __len__ must never be called")

        def __iter__(self):
            HOOKS.append("liar.__iter__")
            raise RuntimeError("host __iter__ must never be called")

        def __str__(self):
            HOOKS.append("liar.__str__")
            return "<Liar SECRET=TOPSECRET>"

        def __repr__(self):
            HOOKS.append("liar.__repr__")
            return "<Liar SECRET=TOPSECRET>"

    Liar.__name__ = name
    Liar.__qualname__ = name
    return Liar()


class TestIssue69R9ScalarNormalisationUsesBaseProtocols(unittest.TestCase):
    """Finding A：标量归一化只走基类协议，宿主 ``__repr__``/``__str__``/``__len__``/
    ``__iter__`` 覆写**零调用**；读不出来的一律具名拒绝。"""

    def setUp(self) -> None:
        HOOKS.clear()

    def test_host_repr_secret_never_becomes_data(self):
        """最锐的一条：``__str__``/``__repr__`` 返回秘密的 ``str`` 子类。"""
        value = LiarStr("base-content")
        copied = _engine_copy(value)
        self.assertIs(type(copied), str)
        self.assertEqual("base-content", copied)
        self.assertNotIn("SECRET", copied)
        self.assertEqual([], HOOKS, HOOKS)

    def test_dsl_value_argument_detach_never_writes_the_host_repr(self):
        """DSL 面：浅 producer 的值参去别名把宿主 ``__str__`` 的返回值写进 ``$VAR``。"""
        from tests.unit.test_issue69_mutation_gate import _alias_context

        with freshness_scope():
            ctx = _alias_context()
            ctx.set_state("$INPUT", {"items": [LiarStr("base-content")]})
            ctx.set_state("$VAR", ctx.evaluate({}))
            ctx.evaluate("$F.set($VAR,'k',$F.slice($INPUT.items,0,1))")
            stored = ctx.get_state("$VAR")["k"]
        self.assertEqual(["base-content"], stored)
        self.assertNotIn("SECRET", repr(stored))
        self.assertEqual([], HOOKS, HOOKS)

    def test_every_scalar_subclass_normalises_to_the_exact_base_with_zero_hooks(self):
        """r10：判决面从 ``type is`` + ``==`` 扩到**全部可观察字段**（含 ``fold``）。

        r9 的旧判据漏掉了 PEP 495 ``fold``：``HookedDateTime`` 的 naive ``fold=0``
        样本与 ``assertEqual(expected, copied)`` 都是 fold 盲的（见
        ``TestIssue69R10ScalarFieldPreservation`` 的 r9 红灯记录）。
        """
        cases = (
            (LiarStr("base-content"), str, "base-content"),
            (HookedBytes(b"abc"), bytes, b"abc"),
            (HookedInt(7), int, 7),
            (HookedFloat(1.5), float, 1.5),
            (HookedComplex(1, 2), complex, complex(1, 2)),
            (HookedDecimal("1.5E+3"), Decimal, Decimal("1.5E+3")),
            (HookedDateTime(2020, 1, 2, 3, 4, 5, 6),
             datetime.datetime, datetime.datetime(2020, 1, 2, 3, 4, 5, 6)),
            # r10：DST 歧义墙钟时间（fold=1）—— ``==`` 与旧读法都看不出丢 fold。
            (HookedDateTime(2020, 11, 1, 1, 30, tzinfo=NY, fold=1),
             datetime.datetime,
             datetime.datetime(2020, 11, 1, 1, 30, tzinfo=NY, fold=1)),
            (HookedDate(2020, 1, 2), datetime.date, datetime.date(2020, 1, 2)),
        )
        for index, (value, kind, expected) in enumerate(cases):
            with self.subTest(kind=kind.__name__, case=index):
                copied = assert_value_preserving(self, value, kind)
                self.assertEqual(expected, copied)
        self.assertEqual([], HOOKS, HOOKS)

    def test_enum_member_still_normalises_to_its_plain_base(self):
        class Level(enum.IntEnum):
            LOW = 1

        copied = _engine_copy(Level.LOW)
        self.assertIs(type(copied), int)
        self.assertEqual(1, copied)
        self.assertEqual([], HOOKS, HOOKS)

    def test_spoofed_scalars_are_named_refused_not_bare_runtimeerror(self):
        """伪造 ``bool`` 触发宿主 ``__len__``、伪造 ``bytes`` 触发宿主 ``__iter__``
        —— r8 下都是裸 ``RuntimeError``；现在必须是具名 ``_EngineCopyRefused``。"""
        for base, name in ((bool, "Liar_bool"), (bytes, "Liar_bytes"), (int, "Liar_int"),
                           (str, "Liar_str"), (float, "Liar_float"),
                           (complex, "Liar_complex"), (Decimal, "Liar_decimal"),
                           (datetime.datetime, "Liar_datetime"),
                           (datetime.date, "Liar_date"),
                           (type(None), "Liar_none")):
            value = _liar(base, name)
            with self.subTest(base=base.__name__):
                self.assertTrue(isinstance(value, base))
                with self.assertRaises(_EngineCopyRefused) as cm:
                    _engine_copy(value)
                self.assertEqual(name, cm.exception.type_name)
        self.assertEqual([], HOOKS, HOOKS)

    def test_spoofed_bool_and_bytes_are_named_at_the_dsl_boundary(self):
        """DSL 面：具名拒绝翻译成文档承诺的 ``not part of the persistable data model``。"""
        from tests.unit.test_issue69_mutation_gate import _alias_context

        for base, name in ((bool, "Liar_bool"), (bytes, "Liar_bytes")):
            with self.subTest(base=base.__name__):
                with freshness_scope():
                    ctx = _alias_context()
                    ctx.set_state("$INPUT", {"items": [_liar(base, name)]})
                    ctx.set_state("$VAR", ctx.evaluate({}))
                    with self.assertRaises(ValueError) as cm:
                        ctx.evaluate("$F.set($VAR,'k',$F.slice($INPUT.items,0,1))")
                self.assertIn("not part of the persistable data model", str(cm.exception))
                self.assertIn(name, str(cm.exception))
        self.assertEqual([], HOOKS, HOOKS)


class TestIssue69R9HashableMemberIsNamedRefused(unittest.TestCase):
    """Finding B：归一化后不可哈希的成员/键必须**具名**拒绝，绝不漏裸 ``TypeError``。"""

    def test_engine_copy_names_the_set_member_and_the_dict_key(self):
        member = HashableDict({"a": 1})
        with self.assertRaises(_EngineCopyRefused) as cm:
            _engine_copy({member})
        self.assertEqual("HashableDict", cm.exception.type_name)
        self.assertIn("set member", str(cm.exception))
        with self.assertRaises(_EngineCopyRefused) as cm:
            _engine_copy({member: 1})
        self.assertEqual("HashableDict", cm.exception.type_name)
        self.assertIn("dict key", str(cm.exception))
        with self.assertRaises(_EngineCopyRefused) as cm:
            _engine_copy(frozenset({member}))
        self.assertEqual("HashableDict", cm.exception.type_name)
        self.assertIn("frozenset member", str(cm.exception))

    def test_snapshot_path_names_the_same_shape(self):
        for payload, role in (({"k": {HashableDict({"a": 1})}}, "set member"),
                              ({HashableDict({"a": 1}): 1}, "dict key")):
            with self.subTest(role=role):
                with self.assertRaises(StateSnapshotError) as cm:
                    _snapshot_value(payload)
                self.assertIn("HashableDict", cm.exception.message)
                self.assertIn(role, cm.exception.message)

    def test_dsl_loop_condition_refusal_is_named_and_writes_nothing(self):
        """DSL 级回归（r8 下是裸 ``TypeError: unhashable type: 'dict'``）。"""
        payload = {"s": {HashableDict({"a": 1})}, "cfg": {"a": 1}}
        raised, detail = _run_loop(payload, "$F.set($INPUT.cfg,'k','v')")
        self.assertIsNotNone(raised, "整根不可拷贝 ⇒ 必须具名拒绝")
        self.assertNotIn("unhashable type", detail)
        self.assertIn("plaita#69", detail)
        self.assertEqual({"a": 1}, payload["cfg"])               # 活对象零写穿
        self.assertEqual(1, len(payload["s"]))

    def test_exact_hashable_members_and_keys_are_still_accepted(self):
        """负控：普通数据模型值的键/成员照常通过（不因本修复过拒）。"""
        payload = {"s": {1, "x", (1, 2), frozenset({3})}, "d": {1: "v", (2, 3): "t"}}
        snapshot = _snapshot_value(payload)
        self.assertEqual(payload["s"], snapshot["s"])
        self.assertEqual(payload["d"], snapshot["d"])
        self.assertIs(type(snapshot["s"]), set)
        self.assertIs(type(snapshot["d"]), dict)


class TestIssue69R9SnapshotKeyAndMemberAliases(unittest.TestCase):
    """Finding C：键/成员不再按引用进快照（选择 (a)：归一化 or 具名拒绝）。"""

    def test_spoofed_class_as_member_or_key_is_named_refused(self):
        with self.assertRaises(StateSnapshotError) as cm:
            _snapshot_value({"k": {_liar(set, "Liar_set")}})
        self.assertIn("Liar_set", cm.exception.message)
        with self.assertRaises(StateSnapshotError) as cm:
            _snapshot_value({_liar(str, "Liar_str"): 1})
        self.assertIn("Liar_str", cm.exception.message)

    def test_host_str_subclass_key_is_normalised_not_shared(self):
        live_key = LiarStr("base-key")
        snapshot = _snapshot_value({live_key: "v"})
        key = next(iter(snapshot))
        self.assertIsNot(key, live_key)
        self.assertIs(type(key), str)
        self.assertEqual("base-key", key)
        self.assertNotIn("SECRET", key)

    def test_host_enum_member_is_normalised_not_shared(self):
        class Level(enum.IntEnum):
            LOW = 1

        live = Level.LOW
        snapshot = _snapshot_value({"s": {live}})
        member = next(iter(snapshot["s"]))
        self.assertIsNot(member, live)
        self.assertIs(type(member), int)

    def test_host_set_subclass_members_are_not_shared(self):
        class HostSet(set):
            pass

        live = HostSet({HashableStrKey("a"), HashableStrKey("b")})
        snapshot = _snapshot_value({"s": live})
        self.assertIs(type(snapshot["s"]), set)
        self.assertIsNot(snapshot["s"], live)
        for member in snapshot["s"]:
            self.assertIs(type(member), str)


class HashableStrKey(str):
    """``str`` 子类：可哈希、能当 ``set`` 成员，归一化后是普通 ``str``。"""


class TestIssue69R9MappingOutsideDictStorage(unittest.TestCase):
    """Finding D：``MutableMapping`` + ``dict`` 病态形状具名拒绝（不再静默 ``{}``）。"""

    def test_snapshot_refuses_the_shape_at_root_and_nested(self):
        with self.assertRaises(StateSnapshotError) as cm:
            _snapshot_value(MappingBackedDict(a=1))
        self.assertIn("MappingBackedDict", cm.exception.message)
        self.assertIn("real dict storage", cm.exception.message)
        with self.assertRaises(StateSnapshotError) as cm:
            _snapshot_value({"m": MappingBackedDict(a=1)})
        self.assertIn("MappingBackedDict", cm.exception.message)
        self.assertIn("$['m']", cm.exception.message)

    def test_engine_copy_refuses_the_shape(self):
        with self.assertRaises(_EngineCopyRefused) as cm:
            _engine_copy({"m": MappingBackedDict(a=1)})
        self.assertEqual("MappingBackedDict", cm.exception.type_name)
        self.assertIn("Mapping", str(cm.exception))

    def test_dsl_loop_condition_refusal_is_named(self):
        """DSL 级回归（r7/r8 下这个形状让条件副本静默变成 ``{}`` ⇒ 假"成功"）。"""
        payload = {"m": MappingBackedDict(a=1), "cfg": {"a": 1}}
        raised, detail = _run_loop(payload, "$F.set($INPUT.cfg,'k','v')")
        self.assertIsNotNone(raised, "不可保真拷贝 ⇒ 必须具名拒绝")
        self.assertIn("plaita#69", detail)
        self.assertEqual({"a": 1}, payload["cfg"])

    def test_ordinary_dict_subclasses_are_still_normalised_negative_control(self):
        """负控：``OrderedDict``/``defaultdict``/普通 ``dict`` 子类不受影响。"""
        from plaita.core.expression_parser import reads_mapping_outside_dict_storage

        for value in (OrderedDict(a=1), defaultdict(int, {"a": 1}), _PlainDict(a=1)):
            with self.subTest(kind=type(value).__name__):
                self.assertEqual({"a": 1}, _snapshot_value({"m": value})["m"])
                self.assertEqual({"a": 1}, _engine_copy({"m": value})["m"])
        self.assertFalse(reads_mapping_outside_dict_storage(OrderedDict()))
        self.assertFalse(reads_mapping_outside_dict_storage({}))
        self.assertTrue(reads_mapping_outside_dict_storage(MappingBackedDict()))


class _PlainDict(dict):
    """纯 ``dict`` 子类（没有第二条 ``Mapping`` 基类）。"""


class TestIssue69R9EscapeHatchIsLossy(unittest.TestCase):
    """Finding E：文档承诺的逃生口 ``$F.json_loads($F.json_dumps(<part>))`` 有损。

    一半钉**类型行为**（``tuple`` → ``list``、五类 ``TypeError``），一半钉**文档文字**
    （两处注记必须逐类型写明损失与替代，否则测试红）——注记不许腐烂。
    """

    #: 逃生口在文档里的字面量（两处引用必须紧跟注记）。
    ESCAPE_HATCH = "$F.json_loads($F.json_dumps("
    #: 注记窗口内必须出现的类型/替代关键词。
    REQUIRED_TOKENS = ("Decimal", "datetime", "date", "bytes", "set", "tuple", "list",
                       "TypeError", "ctx.evaluate")

    @staticmethod
    def _callable(name: str):
        from plaita.core.expression import get_default_expression_registry

        return get_default_expression_registry().get_callable(name)

    def test_tuple_silently_becomes_a_list(self):
        round_tripped = self._callable("json_loads")(self._callable("json_dumps")((1, 2)))
        self.assertEqual([1, 2], round_tripped)
        self.assertIs(type(round_tripped), list)          # 形状丢失：tuple → list

    def test_non_json_types_raise_typeerror(self):
        dumps = self._callable("json_dumps")
        for value, name in ((Decimal("1.5"), "Decimal"),
                            (datetime.datetime(2020, 1, 2), "datetime"),
                            (datetime.date(2020, 1, 2), "date"),
                            (b"x", "bytes"),
                            ({1, 2}, "set"),
                            (frozenset({1}), "frozenset")):
            with self.subTest(kind=name):
                with self.assertRaises(TypeError) as cm:
                    dumps(value)
                self.assertIn(name, str(cm.exception))

    def test_decimal_escape_hatch_is_unusable_at_the_dsl_level(self):
        payload = {"amount": Decimal("1.5"), "cfg": {"a": 1}}
        raised, detail = _run_loop(
            payload,
            "$F.set($F.json_loads($F.json_dumps($INPUT.amount)),'k','v')")
        self.assertIsNotNone(raised, "Decimal 进不了 json_dumps ⇒ 逃生口不可用")
        self.assertIn("Decimal", detail)
        self.assertIn("not JSON serializable", detail)

    def test_the_note_is_present_next_to_both_escape_hatch_mentions(self):
        """任一处逃生口引用后面必须紧跟**逐类型**的损失与替代注记。"""
        root = pathlib.Path(__file__).resolve().parents[2]
        for relpath in ("docs-site/docs/guide/expressions.md", "MIGRATION.md"):
            with self.subTest(doc=relpath):
                text = (root / relpath).read_text(encoding="utf-8")
                windows = []
                at = text.find(self.ESCAPE_HATCH)
                while at != -1:
                    windows.append(text[at:at + 2600])
                    at = text.find(self.ESCAPE_HATCH, at + 1)
                self.assertTrue(windows, f"{relpath}: 找不到逃生口引用")
                self.assertTrue(
                    any(all(token in window for token in self.REQUIRED_TOKENS)
                        for window in windows),
                    f"{relpath}: 没有一处逃生口引用带着完整注记"
                    f"（需要 {self.REQUIRED_TOKENS}）")


if __name__ == "__main__":
    unittest.main()
