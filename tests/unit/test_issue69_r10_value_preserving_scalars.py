"""plaita#69 **r10** —— r9 的 A 修复引入的**静默数据损坏**（PEP 495 ``fold``）收口
+ 逐标量「可观察字段」审计。

r9 把标量归一化从内建构造器换成基类协议，方向正确，但 ``datetime`` 那一格选了
``datetime(*datetime.__reduce__(value)[1])`` —— ``__reduce__`` 是 **protocol 2** 形态，
它打包 7 个分量 + ``tzinfo``，**不含 ``fold``**（3.9.6 / 3.10.21 / 3.11.16 / 3.12.x /
3.13.15 实测一致；只有 ``__reduce_ex__(v, 4)`` 把 fold 位写进打包字节）。后果是
DST 歧义墙钟时间被**静默**改成 ``fold=0``：

    ZoneInfo("America/New_York"), 2020-11-01 01:30, fold=1
      r9 读数： fold=0  timestamp()=1604208600.0  utcoffset=-4:00(EDT)
      正确读数： fold=1  timestamp()=1604212200.0  utcoffset=-5:00(EST)
      （``type(copy) is datetime`` 与 ``copy == value`` 都仍成立 ⇒ 无任何错误）

本文件钉住三件事：

1. **fold 保真**（:class:`TestIssue69R10FoldIsValuePreserving`）：DST 歧义时刻的
   ``fold``/``timestamp()``/``utcoffset()``/``tzinfo`` 身份、fold 不影响 ``timestamp()``
   的非歧义对照、DSL 面（``$F.set($VAR,'k',$F.slice($INPUT.items,0,1))``）、快照路径
   （同一墙钟的两个 fold 是 ``set`` 的两个成员，r9 下会**静默塌成一个**）、以及宿主
   ``fold`` property / ``replace`` / ``__reduce__`` 覆写（含"宿主流谎返回 0"）都进不去。
2. **逐标量字段审计**（:class:`TestIssue69R10ScalarFieldPreservation`）：``datetime``
   （7 分量 + ``tzinfo`` 身份 + ``fold`` + ``timestamp()`` + ``utcoffset()``）、``date``
   （``year/month/day/toordinal``）、``Decimal``（``is_signed`` + 符号/系数/指数 +
   ``str()``，含 ``-0``/``1E+3``/``sNaN123``/``Infinity``）、``str``/``bytes``/``int``/
   ``float``/``complex`` 的完整值，全部用**基类描述符**读，宿主覆写零进入。
3. r10 复核发现的**第二个同类洞**：``bytes(memoryview(v))`` 在 **3.12+** 会调用宿主
   Python 级 ``__buffer__``（PEP 688）—— 实测 3.13.15 上
   ``class EvilBytes(bytes): def __buffer__(self, flags): return memoryview(b"SECRET")``
   让旧读数变成 ``b'SECRET'``。r10 改走 C 基类描述符 ``bytes.__getitem__(v, slice(None))``，
   并且本文件用"``memoryview`` 被禁用后仍能读"的**结构性**判据（不依赖 PEP 688 是否
   存在）钉住它。
"""
from __future__ import annotations

import builtins
import datetime
import math
import unittest
from collections import OrderedDict, defaultdict, Counter
from collections.abc import Mapping, MutableMapping
from decimal import Decimal
from zoneinfo import ZoneInfo

from plaita.core.errors import StateSnapshotError
from plaita.core.expression_parser import (
    _EngineCopyRefused,
    _canonical_persistable_kind,
    _engine_copy,
    dict_storage_protocol_overrides,
    freshness_scope,
    reads_mapping_outside_dict_storage,
)
from plaita.core.state import _snapshot_value
from tests.unit.test_issue69_r8_snapshot_discipline import _run_loop
from tests.unit.test_issue69_r9_host_object_loopholes import (
    NY,
    _base_datetime_twin,
    assert_value_preserving,
    observable_fields,
)

#: 宿主钩子计数器（跨测试清空）：任何一次调用都说明归一化落回了宿主覆写。
HOOKS: list = []

#: r10 的 DST 歧义样本：2020-11-01 01:30 America/New_York 出现两次。
AMBIGUOUS = (2020, 11, 1, 1, 30)
#: fold=1 时的权威读数（EST, UTC-5）。
FOLD1_TIMESTAMP = 1604212200.0
#: fold=0 时的权威读数（EDT, UTC-4）—— r9 会静默给出这个。
FOLD0_TIMESTAMP = 1604208600.0


class SubDT(datetime.datetime):
    """普通 ``datetime`` 子类（数据模型里应被归一化成精确 ``datetime``）。"""


class HostileFold(datetime.datetime):
    """把读取器可能碰到的**每一个**宿主钩子都覆写成抛异常。"""

    @property
    def year(self):
        HOOKS.append("datetime.year")
        raise RuntimeError("host year property must never be read")

    @property
    def month(self):
        HOOKS.append("datetime.month")
        raise RuntimeError("host month property must never be read")

    @property
    def day(self):
        HOOKS.append("datetime.day")
        raise RuntimeError("host day property must never be read")

    @property
    def hour(self):
        HOOKS.append("datetime.hour")
        raise RuntimeError("host hour property must never be read")

    @property
    def minute(self):
        HOOKS.append("datetime.minute")
        raise RuntimeError("host minute property must never be read")

    @property
    def second(self):
        HOOKS.append("datetime.second")
        raise RuntimeError("host second property must never be read")

    @property
    def microsecond(self):
        HOOKS.append("datetime.microsecond")
        raise RuntimeError("host microsecond property must never be read")

    @property
    def tzinfo(self):
        HOOKS.append("datetime.tzinfo")
        raise RuntimeError("host tzinfo property must never be read")

    @property
    def fold(self):
        HOOKS.append("datetime.fold")
        raise RuntimeError("host fold property must never be read")

    @property
    def replace(self):
        HOOKS.append("datetime.replace")
        raise RuntimeError("host replace must never be read")

    def __reduce__(self):
        HOOKS.append("datetime.__reduce__")
        raise RuntimeError("host __reduce__ must never be called")

    def __reduce_ex__(self, protocol):
        HOOKS.append("datetime.__reduce_ex__")
        raise RuntimeError("host __reduce_ex__ must never be called")


class LyingFold(datetime.datetime):
    """宿主 ``fold`` property 返回**谎言**（真实 C 字段是 1，它说 0）。"""

    @property
    def fold(self):
        HOOKS.append("LyingFold.fold")
        return 0


class HostileDate(datetime.date):
    @property
    def year(self):
        HOOKS.append("date.year")
        raise RuntimeError("host year property must never be read")

    @property
    def month(self):
        HOOKS.append("date.month")
        raise RuntimeError("host month property must never be read")

    @property
    def day(self):
        HOOKS.append("date.day")
        raise RuntimeError("host day property must never be read")

    @property
    def toordinal(self):
        HOOKS.append("date.toordinal")
        raise RuntimeError("host toordinal must never be read")

    def __reduce__(self):
        HOOKS.append("date.__reduce__")
        raise RuntimeError("host __reduce__ must never be called")


class HostileDecimal(Decimal):
    def __str__(self):
        HOOKS.append("Decimal.__str__")
        return "999999"

    def __repr__(self):
        HOOKS.append("Decimal.__repr__")
        return "<LiarDecimal SECRET=TOPSECRET>"

    def __format__(self, spec):
        HOOKS.append("Decimal.__format__")
        return "999999"

    def as_tuple(self):
        HOOKS.append("Decimal.as_tuple")
        raise RuntimeError("host as_tuple must never be called")

    def is_signed(self):
        HOOKS.append("Decimal.is_signed")
        raise RuntimeError("host is_signed must never be called")


class HostileStr(str):
    def __str__(self):
        HOOKS.append("str.__str__")
        return "<LiarStr SECRET=TOPSECRET>"

    def __repr__(self):
        HOOKS.append("str.__repr__")
        return "<LiarStr SECRET=TOPSECRET>"

    def __format__(self, spec):
        HOOKS.append("str.__format__")
        return "<LiarStr SECRET=TOPSECRET>"


class HostileBytes(bytes):
    """PEP 688（3.12+）的 Python 级 ``__buffer__`` + 全部旧宿主钩子。

    ``__buffer__`` 在 3.11 及更早只是普通方法（不会被 ``memoryview()`` 采纳），
    因此本类在两个版本带上都合法；区别只在 3.12+ 的读数。
    """

    def __buffer__(self, flags):
        HOOKS.append("bytes.__buffer__")
        return memoryview(b"SECRET")

    def __bytes__(self):
        HOOKS.append("bytes.__bytes__")
        return b"SECRET"

    def __index__(self):
        HOOKS.append("bytes.__index__")
        return 0

    def __iter__(self):
        HOOKS.append("bytes.__iter__")
        raise RuntimeError("host __iter__ must never be called")

    def __len__(self):
        HOOKS.append("bytes.__len__")
        return 0

    def __getitem__(self, item):
        HOOKS.append("bytes.__getitem__")
        return b"S"

    def __repr__(self):
        HOOKS.append("bytes.__repr__")
        return "<LiarBytes SECRET=TOPSECRET>"


class HostileInt(int):
    def __int__(self):
        HOOKS.append("int.__int__")
        raise RuntimeError("host __int__ must never be called")

    def __index__(self):
        HOOKS.append("int.__index__")
        raise RuntimeError("host __index__ must never be called")

    def __repr__(self):
        HOOKS.append("int.__repr__")
        return "<LiarInt SECRET=TOPSECRET>"


class HostileFloat(float):
    def __float__(self):
        HOOKS.append("float.__float__")
        raise RuntimeError("host __float__ must never be called")

    def __repr__(self):
        HOOKS.append("float.__repr__")
        return "<LiarFloat SECRET=TOPSECRET>"


class HostileComplex(complex):
    def __complex__(self):
        HOOKS.append("complex.__complex__")
        raise RuntimeError("host __complex__ must never be called")

    def __getnewargs__(self):
        HOOKS.append("complex.__getnewargs__")
        raise RuntimeError("host __getnewargs__ must never be called")

    def __repr__(self):
        HOOKS.append("complex.__repr__")
        return "<LiarComplex SECRET=TOPSECRET>"


class TestIssue69R10FoldIsValuePreserving(unittest.TestCase):
    """PEP 495 ``fold`` 是**值**：DST 歧义墙钟时间必须逐字段保真。"""

    def setUp(self) -> None:
        HOOKS.clear()

    def test_dst_ambiguous_wall_time_keeps_fold_timestamp_and_offset(self):
        """核心回归：r9 下 fold=1 → fold=0，``timestamp()`` 差 3600s（静默）。"""
        expected = {0: FOLD0_TIMESTAMP, 1: FOLD1_TIMESTAMP}
        offsets = {0: datetime.timedelta(hours=-4), 1: datetime.timedelta(hours=-5)}
        for fold in (0, 1):
            with self.subTest(fold=fold):
                value = SubDT(*AMBIGUOUS, tzinfo=NY, fold=fold)
                copied = assert_value_preserving(self, value, datetime.datetime)
                self.assertEqual(fold, datetime.datetime.fold.__get__(copied))
                self.assertIs(NY, copied.tzinfo)
                self.assertEqual(expected[fold], copied.timestamp())
                self.assertEqual(offsets[fold], copied.utcoffset())
                # 同一墙钟的两个 fold 是**不同的时刻**（这才是 fold 的后果）
                self.assertNotEqual(copied.timestamp(),
                                    SubDT(*AMBIGUOUS, tzinfo=NY,
                                          fold=1 - fold).timestamp())
        self.assertEqual([], HOOKS, HOOKS)

    def test_unambiguous_time_is_fold_blind_yet_fold_still_carried(self):
        """负控：``fold`` 不改变 ``timestamp()`` 的时刻也要原样带过 fold。

        证明修复不是"只要 fold=1 就加一小时"的特判 —— 用的是同一个基类描述符，
        fold 只是被**搬运**，不参与任何换算。
        """
        for fold in (0, 1):
            with self.subTest(fold=fold):
                value = SubDT(2020, 11, 1, 3, 30, tzinfo=NY, fold=fold)
                copied = assert_value_preserving(self, value, datetime.datetime)
                self.assertEqual(fold, datetime.datetime.fold.__get__(copied))
                self.assertEqual(1604219400.0, copied.timestamp())
                self.assertEqual(datetime.timedelta(hours=-5), copied.utcoffset())
        self.assertEqual([], HOOKS, HOOKS)

    def test_fold_is_carried_for_naive_and_fixed_offset_forms_too(self):
        """验收者实测**三种形态全丢 fold**：naive / ``timezone`` / ``zoneinfo``。

        naive 与固定偏移 ``timezone`` 下 ``fold`` 不影响 ``timestamp()``（没有歧义），
        但它仍是 PEP 495 的**存储字段**，``datetime.fold`` 必须为 1 —— r9 的
        ``__reduce__`` 读数在这三种形态上都会给 0。
        """
        fixed = datetime.timezone(datetime.timedelta(hours=-5))
        cases = (("naive", None), ("timezone", fixed), ("zoneinfo", NY))
        for label, tzinfo in cases:
            for fold in (0, 1):
                with self.subTest(label=label, fold=fold):
                    value = SubDT(2020, 11, 1, 1, 30, tzinfo=tzinfo, fold=fold)
                    copied = assert_value_preserving(self, value, datetime.datetime)
                    self.assertEqual(fold, datetime.datetime.fold.__get__(copied))
                    self.assertIs(tzinfo, copied.tzinfo)
                    self.assertEqual(
                        _base_datetime_twin(value).tzinfo, copied.tzinfo)
        self.assertEqual([], HOOKS, HOOKS)

    def test_equality_exact_type_and_hash_are_fold_blind(self):
        """**r9 为什么漏掉**：``==``/``hash()``/精确 ``type`` 对 fold 全盲。

        判定必须看字段（``observable_fields``）：同一墙钟、同一 ``ZoneInfo`` 的
        fold=0 与 fold=1 实测 ``==`` 为 True、``hash()`` 相等、``set`` 只收一个成员
        —— 所以丢了 fold 的副本**看起来完全一样**。
        """
        zero = SubDT(*AMBIGUOUS, tzinfo=NY, fold=0)
        one = SubDT(*AMBIGUOUS, tzinfo=NY, fold=1)
        self.assertEqual(zero, one)                    # CPython 对 fold 盲
        self.assertEqual(hash(zero), hash(one))        # hash 也盲
        self.assertEqual(1, len({zero, one}))
        self.assertIs(type(zero), type(one))
        self.assertNotEqual(zero.timestamp(), one.timestamp())
        self.assertNotEqual(observable_fields(zero), observable_fields(one))
        # r9 的读数（fold 丢掉）与 fold=0 逐字段相同 —— 正是静默所在
        r9_like = datetime.datetime(*datetime.datetime.__reduce__(one)[1])
        self.assertEqual(observable_fields(r9_like), observable_fields(zero))
        self.assertEqual(zero, r9_like)

    def test_dsl_level_fold_survives_the_detach_copy(self):
        """DSL 面：浅 producer 的值参去别名把 fold=1 的 ``datetime`` 复制进 ``$VAR``。"""
        from tests.unit.test_issue69_mutation_gate import _alias_context

        with freshness_scope():
            ctx = _alias_context()
            ctx.set_state("$INPUT", {
                "items": [SubDT(*AMBIGUOUS, tzinfo=NY, fold=1)]})
            ctx.set_state("$VAR", ctx.evaluate({}))
            ctx.evaluate("$F.set($VAR,'k',$F.slice($INPUT.items,0,1))")
            stored = ctx.get_state("$VAR")["k"][0]
        self.assertIs(type(stored), datetime.datetime)
        self.assertEqual(1, datetime.datetime.fold.__get__(stored))
        self.assertEqual(FOLD1_TIMESTAMP, stored.timestamp())
        self.assertEqual(datetime.timedelta(hours=-5), stored.utcoffset())
        self.assertIs(NY, stored.tzinfo)
        self.assertEqual([], HOOKS, HOOKS)

    def test_snapshot_path_keeps_fold_for_members_and_keys(self):
        """快照路径：``set`` 成员 / ``dict`` 键经同一归一化入口，fold 必须带过。

        （``==``/``hash()`` 对 fold 盲，所以"两个 fold 塌成一个"在 Python 层就发生，
        不能当判据；这里直接钉成员的 ``fold``/``timestamp()``。）
        """
        one = SubDT(*AMBIGUOUS, tzinfo=NY, fold=1)
        snapshot = _snapshot_value({"s": {one}, "d": {one: "v"}})
        (member,) = snapshot["s"]
        (key,) = snapshot["d"]
        for label, value in (("member", member), ("key", key)):
            with self.subTest(role=label):
                self.assertIs(type(value), datetime.datetime)
                self.assertEqual(1, datetime.datetime.fold.__get__(value))
                self.assertEqual(FOLD1_TIMESTAMP, value.timestamp())
                self.assertEqual(datetime.timedelta(hours=-5), value.utcoffset())
                self.assertIs(NY, value.tzinfo)
        self.assertEqual([], HOOKS, HOOKS)

    def test_hostile_fold_property_replace_and_reduce_are_never_entered(self):
        """宿主覆写 ``fold``/``replace``/``__reduce__`` 一律进不去（基类描述符）。"""
        value = HostileFold(*AMBIGUOUS, tzinfo=NY, fold=1)
        copied = _engine_copy(value)
        self.assertIs(type(copied), datetime.datetime)
        self.assertEqual(1, datetime.datetime.fold.__get__(copied))
        self.assertEqual(FOLD1_TIMESTAMP, copied.timestamp())
        self.assertEqual(datetime.timedelta(hours=-5), copied.utcoffset())
        self.assertIs(NY, copied.tzinfo)
        self.assertEqual((*AMBIGUOUS,), (copied.year, copied.month, copied.day,
                                         copied.hour, copied.minute))
        self.assertEqual([], HOOKS, HOOKS)

    def test_hostile_fold_property_that_lies_is_ignored(self):
        """宿主流谎（``value.fold`` 说 0）时，基类描述符仍读到真实 C 字段 1。"""
        value = LyingFold(*AMBIGUOUS, tzinfo=NY, fold=1)
        self.assertEqual(0, value.fold)                       # 宿主谎言
        self.assertEqual(1, datetime.datetime.fold.__get__(value))   # 真实字段
        copied = _engine_copy(value)
        self.assertEqual(1, datetime.datetime.fold.__get__(copied))
        self.assertEqual(FOLD1_TIMESTAMP, copied.timestamp())
        self.assertIn("LyingFold.fold", HOOKS)                # 只有宿主的谎言被读过
        self.assertEqual(1, len(HOOKS), HOOKS)

    def test_datetime_is_never_routed_to_the_date_reader(self):
        """``datetime`` 必须命中 ``datetime`` 那一格（``date.__reduce__`` 只带日期）。"""
        from plaita.core.types import PERSISTABLE_SCALAR_TYPES

        self.assertLess(PERSISTABLE_SCALAR_TYPES.index(datetime.datetime),
                        PERSISTABLE_SCALAR_TYPES.index(datetime.date))
        value = SubDT(*AMBIGUOUS, tzinfo=NY, fold=1)
        self.assertIs(datetime.datetime, _canonical_persistable_kind(value))
        self.assertEqual(4, len(datetime.date.__reduce__(value)[1][0]),
                         "date.__reduce__ 只打包 year/month/day 四个字节")
        copied = assert_value_preserving(self, value, datetime.datetime)
        self.assertEqual((1, 30), (copied.hour, copied.minute))
        self.assertEqual(1, copied.fold)


class TestIssue69R10ScalarFieldPreservation(unittest.TestCase):
    """逐标量「全可观察字段」审计：每个读取器的字段集合都被穷举并验证保真。"""

    def setUp(self) -> None:
        HOOKS.clear()

    def test_datetime_all_fields_including_fold_and_tzinfo_identity(self):
        cases = (
            SubDT(2020, 1, 2, 3, 4, 5, 6),
            SubDT(2020, 1, 2, 3, 4, 5, 6, tzinfo=NY),
            SubDT(*AMBIGUOUS, tzinfo=NY, fold=0),
            SubDT(*AMBIGUOUS, tzinfo=NY, fold=1),
            SubDT(2020, 11, 1, 3, 30, tzinfo=NY, fold=1),
            HostileFold(2020, 1, 2, 3, 4, 5, 6, tzinfo=NY, fold=1),
            HostileFold(*AMBIGUOUS, tzinfo=NY, fold=0),
        )
        for index, value in enumerate(cases):
            with self.subTest(case=index):
                assert_value_preserving(self, value, datetime.datetime)
        # 尾部再加一条**非 subTest** 的单点判决：`mutation_replay` 的 killer 解析只认
        # `FAILED`/`ERROR` 行，subTest 失败只出 `SUBFAILED(` 行（工具已知限制）。
        micro = SubDT(2020, 1, 2, 3, 4, 5, 6, tzinfo=NY, fold=1)
        copied_micro = _engine_copy(micro)
        self.assertEqual(
            (6, 1),
            (datetime.datetime.microsecond.__get__(copied_micro),
             datetime.datetime.fold.__get__(copied_micro)))
        self.assertEqual(_base_datetime_twin(micro).timestamp(),
                         copied_micro.timestamp())
        self.assertEqual([], HOOKS, HOOKS)

    def test_date_year_month_day(self):
        """``date`` 的字段只有 ``year/month/day``（+``toordinal``）；宿主覆写零进入。"""
        for value in (HostileDate(1, 1, 1), HostileDate(2020, 1, 2),
                      HostileDate(9999, 12, 31)):
            with self.subTest(value=observable_fields(value)[1:4]):
                copied = assert_value_preserving(self, value, datetime.date)
                self.assertEqual(observable_fields(value), observable_fields(copied))
        self.assertEqual([], HOOKS, HOOKS)

    def test_decimal_sign_coefficient_exponent_and_str(self):
        """``Decimal`` 的全部可观察字段：符号 / 系数 digits / 指数 / ``str()``。

        ``Decimal`` 值**不带 context**（context 只参与算术），故保真面就是
        ``is_signed`` + ``as_tuple`` + ``str``；``str`` 往返连 ``-0``、指数记法、
        ``sNaN`` 载荷都逐字保留。
        """
        samples = ("0", "-0", "1.5", "1.5E+3", "-1.2300E-7", "0.000",
                   "123456789012345678901234567890", "1E+999999999999",
                   "NaN", "-NaN", "sNaN123", "Infinity", "-Infinity")
        for text in samples:
            with self.subTest(value=text):
                value = HostileDecimal(text)
                copied = assert_value_preserving(self, value, Decimal)
                self.assertIs(Decimal.is_signed(value), Decimal.is_signed(copied))
                self.assertEqual(Decimal.as_tuple(value), Decimal.as_tuple(copied))
                self.assertEqual(Decimal.__str__(value), Decimal.__str__(copied))
                self.assertEqual(text, str(copied))
        # 尾部非 subTest 单点判决（同上：`mutation_replay` 的 killer 解析只认
        # `FAILED`/`ERROR` 行，subTest 失败只出 `SUBFAILED(` 行）。
        exponent_case = HostileDecimal("1.5E+3")
        self.assertEqual(Decimal.as_tuple(Decimal("1.5E+3")),
                         Decimal.as_tuple(_engine_copy(exponent_case)))
        self.assertEqual([], HOOKS, HOOKS)

    def test_str_bytes_int_float_complex_full_value(self):
        cases = (
            (HostileStr(""), str), (HostileStr("abc"), str),
            (HostileStr("a\x00b"), str), (HostileStr("日本語"), str),
            (HostileBytes(b""), bytes), (HostileBytes(b"real"), bytes),
            (HostileBytes(bytes(range(256))), bytes),
            (HostileInt(0), int), (HostileInt(-7), int), (HostileInt(2 ** 200), int),
            (HostileFloat(-0.0), float), (HostileFloat(1.5), float),
            (HostileFloat(float("-inf")), float),
            (HostileComplex(1, 2), complex), (HostileComplex(-0.0, 0.0), complex),
            (HostileComplex(1e308, -1e-308), complex),
        )
        for index, (value, kind) in enumerate(cases):
            with self.subTest(kind=kind.__name__, case=index):
                assert_value_preserving(self, value, kind)
        self.assertEqual([], HOOKS, HOOKS)

    def test_float_and_complex_keep_signed_zero_and_nan_payload_bits(self):
        """``float``/``complex`` 的 ±0 与 NaN 位模式都在可观察字段里。"""
        for value in (HostileFloat(-0.0), HostileFloat(0.0),
                      HostileFloat(float("nan")), HostileFloat(float("inf"))):
            with self.subTest(value=repr(float.__float__(value))):
                copied = assert_value_preserving(self, value, float)
                self.assertEqual(
                    observable_fields(float.__float__(value)),
                    observable_fields(copied))
        for value in (HostileComplex(-0.0, 0.0), HostileComplex(0.0, -0.0),
                      HostileComplex(float("nan"), 1.0)):
            with self.subTest(value=repr(complex.__getnewargs__(value))):
                copied = assert_value_preserving(self, value, complex)
                original_parts = complex.__getnewargs__(value)
                copied_parts = complex.__getnewargs__(copied)
                self.assertEqual([math.copysign(1.0, part) for part in original_parts],
                                 [math.copysign(1.0, part) for part in copied_parts])
                self.assertEqual([math.isnan(part) for part in original_parts],
                                 [math.isnan(part) for part in copied_parts])
        self.assertEqual([], HOOKS, HOOKS)

    def test_bytes_reader_is_immune_to_the_pep688_buffer_hook(self):
        """3.12+ PEP 688：``memoryview()`` 会调用宿主 ``__buffer__``。

        3.9–3.11 没有该协议，因此本测试在旧版上"天然通过"；真正的版本无关判据是
        下面那条禁掉 ``memoryview`` 的结构性测试。这里同时钉住"内容没被宿主换掉"。
        """
        value = HostileBytes(b"real")
        copied = assert_value_preserving(self, value, bytes)
        self.assertEqual(b"real", copied)
        self.assertNotEqual(b"SECRET", copied)
        self.assertEqual([], HOOKS, HOOKS)

    def test_bytes_reader_does_not_depend_on_memoryview(self):
        """结构性判据（版本无关）：读取器不得调用 ``memoryview``。

        r9 的实现是 ``bytes(memoryview(value))``，把 ``memoryview`` 换成会炸的替身
        即可在**任何**版本上把它暴露出来（3.12+ 的 ``__buffer__`` 劫持只是该实现的
        另一面）。r10 的 ``bytes.__getitem__(value, slice(None))`` 不经过它。
        """
        original = builtins.memoryview

        def _forbidden(*args, **kwargs):
            raise AssertionError(
                "the bytes reader must not go through memoryview()/the Python "
                "buffer protocol: a 3.12+ subclass can hijack it via __buffer__")

        builtins.memoryview = _forbidden
        try:
            copied = _engine_copy(HostileBytes(b"real"))
        finally:
            builtins.memoryview = original
        self.assertIs(type(copied), bytes)
        self.assertEqual(b"real", copied)
        self.assertEqual([], HOOKS, HOOKS)

    def test_spoofed_scalars_are_still_named_refused(self):
        """回归：r10 改了 ``bytes``/``datetime`` 读取器，伪造对象仍须具名拒绝。"""

        class Liar:
            __class__ = bytes

            def __len__(self):
                raise RuntimeError("liar __len__")

            def __iter__(self):
                raise RuntimeError("liar __iter__")

            def __bytes__(self):
                raise RuntimeError("liar __bytes__")

        with self.assertRaises(_EngineCopyRefused) as cm:
            _engine_copy(Liar())
        self.assertEqual("Liar", cm.exception.type_name)
        self.assertEqual([], HOOKS, HOOKS)


class ItemsLiar(dict):
    """r10 D1 反例一：数据在 ``_d``，用 Python 覆写映射协议自述（无 ``Mapping`` 基类）。"""

    def __init__(self, **kwargs):
        self._d = dict(kwargs)

    def items(self):
        return self._d.items()

    def keys(self):
        return self._d.keys()

    def values(self):
        return self._d.values()

    def __iter__(self):
        return iter(self._d)

    def __len__(self):
        return len(self._d)

    def __getitem__(self, key):
        return self._d[key]


class DictFirstMapping(dict, Mapping):
    """r10 D1 反例二：``Mapping`` 排在 ``dict`` **之后**（镜像顺序），数据在 ``_d``。"""

    def __init__(self, **kwargs):
        self._d = dict(kwargs)

    def items(self):
        return self._d.items()

    def __iter__(self):
        return iter(self._d)

    def __len__(self):
        return len(self._d)

    def __getitem__(self, key):
        return self._d[key]


class ItemsOnlyLiar(dict):
    """残余形状（判据 3 覆盖）：只覆写 ``items()``，数据放在实例属性 ``_d``。"""

    def __init__(self, **kwargs):
        self._d = dict(kwargs)

    def items(self):
        return self._d.items()


class HookDictBenign(dict):
    """r8 G1 的良性形状：覆写 ``items()``，但数据**仍在**真实 dict 存储里。"""

    def items(self):                      # 覆写：绝不能进数据路径
        HOOKS.append("HookDictBenign.items")
        return dict.items(self)


class AttrDict(dict):
    """带实例属性的普通 ``dict`` 子类 —— 判据 3 的**过拒代价**（见测试文档）。"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.meta = "not data, but indistinguishable"


class PlainDict(dict):
    """纯 ``dict`` 子类（无覆写、无实例状态）。"""


class TestIssue69R10MappingStorageProtocol(unittest.TestCase):
    """r10 Finding D1：r9-D 的 MRO 判据漏掉**镜像形状**，两条路径都静默丢数据。

    r9 的 ``reads_mapping_outside_dict_storage`` 只看 ``Mapping`` 是否排在 ``dict``
    **之前**。``ItemsLiar(dict)``（数据在 ``_d``，用 Python 自述）与
    ``DictFirstMapping(dict, Mapping)``（``Mapping`` 在 ``dict`` 之后）都判 False ⇒
    ``_engine_copy``/``_snapshot_value`` 都静默产出 ``{}``，DSL ``_run_loop`` 报**成功**。

    判据扩成三条机械可判、零宿主钩子的析取（r9 MRO ∨ Python 覆写
    ``__iter__``/``__len__`` ∨ 实例自带 ``__dict__`` 状态），见
    :func:`~plaita.core.expression_parser.reads_mapping_outside_dict_storage`。
    """

    def setUp(self) -> None:
        HOOKS.clear()

    def test_mirror_shapes_are_named_refused_on_both_copy_paths(self):
        for cls in (ItemsLiar, DictFirstMapping, ItemsOnlyLiar):
            value = cls(a=1)
            with self.subTest(kind=cls.__name__):
                self.assertTrue(reads_mapping_outside_dict_storage(value))
                self.assertEqual([], list(dict.items(value)), "真实存储是空的")
                self.assertEqual([("a", 1)], list(value.items()), "宿主自述有一个键")
                with self.assertRaises(_EngineCopyRefused) as cm:
                    _engine_copy({"m": value})
                self.assertEqual(cls.__name__, cm.exception.type_name)
                self.assertIn("real dict storage", str(cm.exception))
                with self.assertRaises(StateSnapshotError) as cm:
                    _snapshot_value({"m": value})
                self.assertIn(cls.__name__, cm.exception.message)
                self.assertIn("real dict storage", cm.exception.message)
                self.assertIn("$['m']", cm.exception.message)

    def test_dsl_loop_condition_refusal_is_named_and_writes_nothing(self):
        """r9-D1 的用户可见面：以前是**报成功**（副本静默变 ``{}``），现在具名拒绝。"""
        for cls in (ItemsLiar, DictFirstMapping, ItemsOnlyLiar):
            with self.subTest(kind=cls.__name__):
                payload = {"m": cls(a=1), "cfg": {"a": 1}}
                raised, detail = _run_loop(payload, "$F.set($INPUT.cfg,'k','v')")
                self.assertIsNotNone(raised, "不可保真拷贝 ⇒ 必须拒绝，不能报成功")
                self.assertIn("plaita#69", detail)
                self.assertNotIn("unhashable type", detail)
                self.assertEqual({"a": 1}, payload["cfg"])      # 零写穿
                self.assertEqual({"a": 1}, dict(payload["m"]._d))

    def test_negative_controls_still_normalise_on_both_paths(self):
        """负控：``OrderedDict``/``defaultdict``/``Counter``/纯子类/精确 dict 不受影响。"""
        class _Cfg:
            expose_env = None
            global_context: dict = {}
            flow_id = "r10-d1"

        cases = (OrderedDict(a=1), defaultdict(int, {"a": 1}), Counter({"a": 1}),
                 PlainDict(a=1), {"a": 1})
        for value in cases:
            with self.subTest(kind=type(value).__name__):
                self.assertFalse(reads_mapping_outside_dict_storage(value))
                self.assertEqual({"a": 1}, _engine_copy(value))
                self.assertIs(type(_engine_copy(value)), dict)
                snapshot = _snapshot_value({"m": value})
                self.assertEqual({"a": 1}, snapshot["m"])
                self.assertIs(type(snapshot["m"]), dict)

    def test_engine_internal_dict_subclasses_are_not_refused(self):
        """引擎自有的 ``dict`` 子类（``NodeSpec`` / 隔离条件上下文）不得被误伤。"""
        from plaita.dsl.builder import NodeSpec
        from plaita.node.loop import _IsolatedConditionContext

        for value in (NodeSpec(), _IsolatedConditionContext({"a": 1})):
            with self.subTest(kind=type(value).__name__):
                self.assertFalse(reads_mapping_outside_dict_storage(value))

    def test_benign_items_override_is_normalised_not_refused(self):
        """**判断**：覆写 ``items()`` 而数据仍在真实存储 ⇒ 归一化（r7/r8 契约）。

        依据：引擎**不采信**宿主自述 —— 权威读数是真实存储（``dict.items``），而
        ``HookDictBenign`` 的数据就在那里，归一化**没有丢任何东西**（断言副本等于真实
        存储）；r8 G1 的 ``test_snapshot_value_rebuilds_hooked_subclasses_as_plain_containers``
        正是要求这个形状被归一化成精确 ``dict``、零宿主钩子、非活对象。把"六个协议
        名里覆写任何一个"一律拒绝会推翻这条已验证的契约，而它并不能比判据 3 多挡住
        任何**数据在别处**的形状（判据 3 抓实例状态，正是 ``_d`` 的载体）。
        """
        value = HookDictBenign({"a": 1})
        self.assertFalse(reads_mapping_outside_dict_storage(value))
        copied = _engine_copy(value)
        self.assertIs(type(copied), dict)
        self.assertEqual(dict(dict.items(value)), copied)
        self.assertEqual({"a": 1}, copied)
        self.assertIs(type(_snapshot_value({"m": value})["m"]), dict)
        self.assertEqual([], HOOKS, HOOKS)

    def test_instance_state_costs_an_over_refusal_and_that_is_declared(self):
        """判据 3 的**过拒代价**：带实例属性的良性 ``dict`` 子类也被具名拒绝。

        "实例状态是不是数据"无法在不调用宿主钩子的前提下判定，故取 fail-closed：
        要进数据模型先 ``dict(...)`` 展平（错误消息里点名了 ``__dict__``）。
        """
        value = AttrDict(a=1)
        self.assertEqual({"a": 1}, dict(value), "数据确实在真实存储里")
        self.assertTrue(reads_mapping_outside_dict_storage(value))
        with self.assertRaises(_EngineCopyRefused) as cm:
            _engine_copy(value)
        self.assertIn("instance state", str(cm.exception))
        self.assertEqual({"a": 1}, _engine_copy(dict(value)), "展平后照常通过")

    def test_only_the_identity_pair_or_instance_state_triggers_the_extension(self):
        """白盒：判据的三条析取各自可辨（避免"碰巧全真"的恒真式）。"""
        self.assertEqual(("items",), dict_storage_protocol_overrides(HookDictBenign))
        self.assertEqual(
            ("__getitem__", "__iter__", "__len__", "items", "keys", "values"),
            dict_storage_protocol_overrides(ItemsLiar))
        self.assertEqual((), dict_storage_protocol_overrides(OrderedDict))
        self.assertEqual((), dict_storage_protocol_overrides(PlainDict))
        self.assertEqual((), dict_storage_protocol_overrides(dict))
        self.assertFalse(reads_mapping_outside_dict_storage(HookDictBenign()))
        self.assertTrue(reads_mapping_outside_dict_storage(ItemsLiar(a=1)))
        self.assertFalse(reads_mapping_outside_dict_storage(OrderedDict(a=1)))

    def test_mutable_mapping_before_dict_shape_is_still_refused(self):
        """r9-D 原形状（``Mapping`` 在 ``dict`` 之前）回归：判据不因扩宽而丢原覆盖。"""
        class MappingBackedDict(MutableMapping, dict):
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

        value = MappingBackedDict(a=1)
        self.assertTrue(reads_mapping_outside_dict_storage(value))
        with self.assertRaises(_EngineCopyRefused) as cm:
            _engine_copy({"m": value})
        self.assertEqual("MappingBackedDict", cm.exception.type_name)
        with self.assertRaises(StateSnapshotError) as cm:
            _snapshot_value({"m": value})
        self.assertIn("MappingBackedDict", cm.exception.message)


class TestIssue69R10NestedOffenderNaming(unittest.TestCase):
    """r10：不可哈希的**嵌套** offender 也要点名真实宿主类型，不是外层容器名。

    r9 的具名拒绝用 ``type(original).__name__``：``{(HashableDict({'a': 1}),): 1}``
    的失败发生在 ``hash(copied)`` 那一跳，于是只报 ``'tuple'``（归因不到宿主类）。
    r10 用 :func:`~plaita.core.expression_parser.unhashable_offender` 沿 ``tuple``
    下标下钻到真实 offender。
    """

    @staticmethod
    def _hashable_dict():
        class HashableDict(dict):
            __hash__ = object.__hash__

        return HashableDict({"a": 1})

    def test_tuple_key_names_the_inner_host_class_on_both_paths(self):
        key = (self._hashable_dict(),)
        with self.assertRaises(_EngineCopyRefused) as cm:
            _engine_copy({key: 1})
        self.assertEqual("HashableDict", cm.exception.type_name)
        self.assertIn("dict key", str(cm.exception))
        with self.assertRaises(StateSnapshotError) as cm:
            _snapshot_value({key: 1})
        self.assertIn("HashableDict", cm.exception.message)
        self.assertNotIn("'tuple'", cm.exception.message)

    def test_deeply_nested_tuple_key_and_frozenset_member(self):
        inner = self._hashable_dict()
        with self.assertRaises(_EngineCopyRefused) as cm:
            _engine_copy({("x", (inner,)): 1})
        self.assertEqual("HashableDict", cm.exception.type_name)
        with self.assertRaises(_EngineCopyRefused) as cm:
            _engine_copy({frozenset({self._hashable_dict()}): 1})
        self.assertEqual("HashableDict", cm.exception.type_name)
        self.assertIn("frozenset member", str(cm.exception))

    def test_honest_tuple_keys_are_still_accepted(self):
        """负控：可哈希的嵌套键照常通过（不因归因改进而过拒）。"""
        self.assertEqual({(1, (2, 3)): "v", frozenset({4}): "w"},
                         _engine_copy({(1, (2, 3)): "v", frozenset({4}): "w"}))
        self.assertEqual({(1, (2, 3)): "v", frozenset({4}): "w"},
                         _snapshot_value({(1, (2, 3)): "v", frozenset({4}): "w"}))


if __name__ == "__main__":
    unittest.main()
