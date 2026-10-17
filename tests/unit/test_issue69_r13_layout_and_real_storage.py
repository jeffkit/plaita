"""plaita#69 **r13** —— clause (iii) 第 5 条从**名字**改成**布局**判据 + `$F.keys/items/values`
读**真实存储** + off-object/快照口径两处更正。

r12 的独立验收判 **FAIL**，反例 falsify 的是 **r12 自己的豁免理由**：

* r12 写"C 描述符只可能由**拥有 C 布局的类型**在自己的 ``tp_dict`` 里生成，Python 类体
  写不出 ``member_descriptor``"，并据此 ``if klass is cls: continue``。**假的** —— Python
  类体的 ``__slots__`` 就在该类**自己的** ``tp_dict`` 里生成 ``member_descriptor``；因为
  clause (iii).2 按**名字** ``__slots__`` 找槽，**隐藏声明、保留槽**就能恢复静默 ``{}`` /
  DSL 报成功：

      class HiddenSlotsLiar(dict):
          __slots__ = ("_store",)
          def __init__(self, d): self._store = dict(d)
          def items(self): return self._store.items()
      del HiddenSlotsLiar.__slots__

  pre（`56b7678` == `bddeb72`）实测：``declared_instance_slots(cls) == ()``、
  ``declared_c_storage_descriptors(cls) == ()``、``instance_state_signal(v) is None``、
  ``_engine_copy({'m': v}) == {'m': {}}``、``_snapshot_value({'m': v}) == {'m': {}}``、
  DSL ``raised=None``（零钩子）。三个等价形态都复现（``del`` / 事后 ``= ()`` / 元类
  ``__new__``）。子类 ``class Sub(HiddenSlotsLiar)`` **已被拒** ⇒ 只有定义层自己漏。

* r12 称"返回 ``['LIE']`` 的假视图不再可能被写成数据、零宿主钩子"，但 ``$F.keys`` 从来
  不是引擎自产视图 —— 它直接问宿主（``_fn_keys(a) = a.keys()``）：
  ``class LieKeysPlainList(dict): def keys(self): return ["LIE"]`` 在 pre 下
  ``$F.set($VAR,'k',$F.keys($INPUT.d))`` → ``raised=None``、``$VAR == {'k': ['LIE']}``、
  宿主 ``keys`` **被调用**（``items``/``values`` 同形）。

本文件钉五件事：

1. **布局判据**（:class:`TestIssue69R13HiddenSlotsLayoutRule`）：三个隐藏形态在两条拷贝
   路径 + DSL 上一律具名拒绝、零写穿、零宿主钩子；子类照旧被拒；负控（精确
   ``defaultdict``/``OrderedDict``/``Counter``/纯子类/``dict``/``NodeSpec``/
   ``_IsolatedConditionContext``/``BenignOD``/``BenignCounter``）逐字不变；
   声明的过拒面含 stdlib 的 ``importlib.metadata._collections.FreezableDefaultDict``。
2. **豁免按 ``Py_TPFLAGS_HEAPTYPE``**（值 ``1 << 9 == 0x200``，五版一致）：静态 C 类型
   （位清）自己的槽照常归一化；Python 类（位置）自己的槽一律计入。
3. **映射辅助读真实存储**（:class:`TestIssue69R13MappingViewReadsRealStorage`）：说谎的
   ``keys``/``items``/``values`` 得到真实存储、宿主钩子零调用；普通 ``dict`` 的行为逐字
   不变；非 ``dict`` 映射与 ``__class__`` 伪造形状具名拒绝。
4. **快照按引用共享的口径**（:class:`TestIssue69R13SnapshotSharesNonRebuiltValues`）：
   ``MappingProxyType``/``ChainMap``/``UserDict``/``os.environ`` 共享**同一对象**（它们是
   容器），而 DSL 路径 ``_engine_copy`` 对四者具名拒绝。
5. **文档口径**（:class:`TestIssue69R13RationaleIsCorrected`）：正确的布局规则与
   ``Py_TPFLAGS_HEAPTYPE``、off-object 类属性族的更正理由、r13 章节，三处（模块 /
   MIGRATION.md / docs-site）齐备。
"""
from __future__ import annotations

import os
import pathlib
import sys
import types
import unittest
from collections import ChainMap, Counter, OrderedDict, UserDict, defaultdict

from plaita.core.errors import StateSnapshotError
from plaita.core.expression_parser import (
    _EngineCopyRefused,
    _PY_TPFLAGS_HEAPTYPE,
    _PY_TPFLAGS_IMMUTABLETYPE,
    _engine_copy,
    _engine_mapping_view,
    _is_heap_type,
    _is_immutable_type,
    _is_python_defined_class,
    _type_flags,
    declared_c_storage_descriptors,
    declared_instance_slots,
    freshness_scope,
    instance_state_signal,
    reads_mapping_outside_dict_storage,
)
from plaita.core.state import _snapshot_value
from plaita.dsl.builder import NodeSpec
from plaita.node.loop import _IsolatedConditionContext
from tests.unit.test_issue69_mutation_gate import _alias_context
from tests.unit.test_issue69_r8_snapshot_discipline import _run_loop

ROOT = pathlib.Path(__file__).resolve().parents[2]

#: 宿主钩子计数器（跨测试清空）：任何一次调用都说明判定/读取落回了宿主代码。
HOOKS: list = []


# --------------------------------------------------------------------------- #
# 阻塞反例：三个"隐藏 __slots__ 声明、保留 member_descriptor"的等价形态
# --------------------------------------------------------------------------- #


class HiddenSlotsLiar(dict):
    """形态一：类体声明槽，之后 ``del cls.__slots__`` 抹掉声明名。"""

    __slots__ = ("_store",)

    def __init__(self, d):
        self._store = dict(d)

    def items(self):
        HOOKS.append("items")
        return self._store.items()

    def keys(self):
        HOOKS.append("keys")
        return self._store.keys()


del HiddenSlotsLiar.__slots__


class HiddenSlotsEmptiedLiar(dict):
    """形态二：类体声明槽，事后 ``cls.__slots__ = ()``。"""

    __slots__ = ("_store",)

    def __init__(self, d):
        self._store = dict(d)

    def items(self):
        HOOKS.append("items")
        return self._store.items()


HiddenSlotsEmptiedLiar.__slots__ = ()


class _HidingMeta(type):
    """形态三的元类：``__new__`` 里设置 ``__slots__`` 再删掉声明名。"""

    def __new__(mcls, name, bases, namespace):
        namespace["__slots__"] = ("_store",)
        cls = super().__new__(mcls, name, bases, namespace)
        del cls.__slots__
        return cls


class HiddenSlotsMetaclassLiar(dict, metaclass=_HidingMeta):
    def __init__(self, d):
        self._store = dict(d)

    def items(self):
        HOOKS.append("items")
        return self._store.items()


HIDING_FORMS = (HiddenSlotsLiar, HiddenSlotsEmptiedLiar,
                HiddenSlotsMetaclassLiar)

#: 布局判据点名的信号（声明层就是堆类型 ``cls`` 自己）。
SIGNALS = {
    HiddenSlotsLiar: "instance state (C descriptor _store on HiddenSlotsLiar)",
    HiddenSlotsEmptiedLiar: "instance state (C descriptor _store on HiddenSlotsEmptiedLiar)",
    HiddenSlotsMetaclassLiar: (
        "instance state (C descriptor _store on HiddenSlotsMetaclassLiar)"),
}


class SubclassOfHiddenSlotsLiar(HiddenSlotsLiar):
    """子类：槽的声明层是基类（≠ ``cls``），r12 下**已经**被拒。"""


# --------------------------------------------------------------------------- #
# 负控 / 过拒面
# --------------------------------------------------------------------------- #


class PlainDictSubclass(dict):
    """纯 ``dict`` 子类：自己的命名空间只有 ``__dict__``/``__weakref__`` 占位。"""


class BenignDefaultDictSubclass(defaultdict):
    """声明的过拒面：数据全在真实存储里、``__dict__`` 空的良性 ``defaultdict`` 子类。"""


class LyingKeysPlainList(dict):
    def keys(self):
        HOOKS.append("keys")
        return ["LIE"]


class LyingItemsPlainList(dict):
    def items(self):
        HOOKS.append("items")
        return [("LIE", "LIE")]


class LyingValuesPlainList(dict):
    def values(self):
        HOOKS.append("values")
        return ["LIE"]


class PlainSubclassWithHostKeys(dict):
    """真实存储与宿主自述一致（``keys()`` 覆写但数据在存储里）—— 规则不靠自述。"""

    def keys(self):
        HOOKS.append("keys")
        return super().keys()


class _SpoofedDict:
    """``isinstance(x, dict)`` 为真但真实类型不是 ``dict``（``__class__`` 伪造）。"""

    @property
    def __class__(self):
        return dict


class _RaisingGetattribute:
    """宿主 ``__getattribute__`` 抛异常：``isinstance`` 回落读 ``__class__`` 时命中。"""

    def __getattribute__(self, name):
        raise RuntimeError("host __getattribute__ boom")


class _LyingFlagsMeta(type):
    """宿主元类试图伪造 ``__flags__`` / 抛异常的 ``__getattribute__``。"""

    __flags__ = 0

    def __getattribute__(cls, name):
        if name == "__flags__":
            raise RuntimeError("host metaclass tried to lie about __flags__")
        return super().__getattribute__(name)


class HiddenSlotsWithLyingMetaclass(dict, metaclass=_LyingFlagsMeta):
    __slots__ = ("_store",)


class TestIssue69R13HiddenSlotsLayoutRule(unittest.TestCase):
    """clause (iii) 第 5 条按**布局**判：堆类型自己 ``tp_dict`` 里的槽描述符一律计入。"""

    def setUp(self) -> None:
        HOOKS.clear()

    def test_the_pre_fix_blind_spot_is_reproduced_by_every_form(self):
        """白盒复现 r12 的两个"看不见"（clause 2 按名、clause 5 跳过 cls）。"""
        for cls in HIDING_FORMS:
            with self.subTest(kind=cls.__name__):
                descriptor = cls.__dict__.get("_store")
                self.assertIsNotNone(descriptor, "槽描述符仍在命名空间里")
                self.assertIs(types.MemberDescriptorType, type(descriptor))
                self.assertEqual((), declared_instance_slots(cls),
                                 "clause 2 按**名字** __slots__ 找 ⇒ 看不见")
                value = cls({"a": 1})
                self.assertEqual({}, dict(dict.items(value)), "真实存储是空的")
                self.assertEqual({"a": 1}, dict(value.items()), "宿主自述有内容")

    def test_every_hiding_form_is_named_refused_on_both_copy_paths(self):
        for cls in HIDING_FORMS:
            with self.subTest(kind=cls.__name__):
                value = cls({"a": 1})
                self.assertEqual(SIGNALS[cls], instance_state_signal(value))
                self.assertTrue(reads_mapping_outside_dict_storage(value))
                with self.assertRaises(_EngineCopyRefused) as cm:
                    _engine_copy({"m": value})
                self.assertEqual(cls.__name__, cm.exception.type_name)
                self.assertIn(SIGNALS[cls], str(cm.exception))
                with self.assertRaises(StateSnapshotError) as cm:
                    _snapshot_value({"m": value})
                self.assertIn(cls.__name__, cm.exception.message)
                self.assertIn(SIGNALS[cls], cm.exception.message)
        self.assertEqual([], HOOKS, HOOKS)

    def test_every_hiding_form_refuses_the_dsl_loop_condition_with_zero_hooks(self):
        """用户可见面：pre 这里 ``raised=None``（副本静默变 ``{}``），r13 具名拒绝。"""
        for cls in HIDING_FORMS:
            with self.subTest(kind=cls.__name__):
                payload = {"m": cls({"a": 1}), "cfg": {"a": 1}}
                raised, detail = _run_loop(payload, "$F.set($INPUT.cfg,'k','v')")
                self.assertIsNotNone(raised, "不可保真拷贝 ⇒ 必须拒绝，不能报成功")
                self.assertIn("plaita#69", detail)
                self.assertNotIn("TypeError: descriptor", detail)
                self.assertEqual({"a": 1}, payload["cfg"])      # 零写穿
        self.assertEqual([], HOOKS, HOOKS)

    def test_the_defining_layer_is_caught_while_subclasses_stay_refused(self):
        """**非 subTest** 判决：修的是"只有定义层自己漏"这条缝。"""
        defining = HiddenSlotsLiar({"a": 1})
        self.assertIsNotNone(instance_state_signal(defining))
        self.assertTrue(reads_mapping_outside_dict_storage(defining))
        with self.assertRaises(_EngineCopyRefused):
            _engine_copy({"m": defining})
        inherited = SubclassOfHiddenSlotsLiar({"a": 1})
        self.assertIsNotNone(instance_state_signal(inherited))
        with self.assertRaises(_EngineCopyRefused):
            _engine_copy({"m": inherited})
        self.assertIn("on HiddenSlotsLiar", instance_state_signal(inherited),
                      "子类命中的仍是基类那层的声明")
        self.assertEqual([], HOOKS, HOOKS)

    def test_white_box_layout_rule_is_mechanically_discriminating(self):
        """白盒：第 5 条的豁免 =「声明层 ``is cls`` **且** ``cls`` 不是 Python 定义的类」。"""
        for cls in HIDING_FORMS:
            with self.subTest(kind=cls.__name__):
                self.assertEqual((("_store", cls.__name__),),
                                 declared_c_storage_descriptors(cls))
        self.assertEqual((("_store", "HiddenSlotsLiar"),),
                         declared_c_storage_descriptors(SubclassOfHiddenSlotsLiar))
        # 静态 C 类型**自己**声明的槽照常豁免（动机场景，不能推翻负控契约）。
        self.assertEqual((), declared_c_storage_descriptors(defaultdict))
        self.assertEqual((), declared_c_storage_descriptors(OrderedDict))
        self.assertEqual((), declared_c_storage_descriptors(Counter))
        self.assertIs(types.MemberDescriptorType,
                      type(defaultdict.__dict__["default_factory"]))

    def test_py_tpflags_heaptype_value_is_printed_and_pinned(self):
        """豁免靠"Python 定义的类"判据：打印并钉住两个 flag 的值与被测类型的读数。

        **裸 ``Py_TPFLAGS_HEAPTYPE`` 不够**（r13 实测）：3.12/3.13 的
        ``collections.defaultdict`` 自己是堆类型（``0x20405740``），只看 heap 位会把
        精确 ``defaultdict`` 误拒、推翻负控契约。正确判据是
        ``_is_python_defined_class = heap ∧ ¬immutable``。
        """
        print(f"Py_TPFLAGS_HEAPTYPE = {_PY_TPFLAGS_HEAPTYPE} "
              f"({_PY_TPFLAGS_HEAPTYPE:#x}); "
              f"Py_TPFLAGS_IMMUTABLETYPE = {_PY_TPFLAGS_IMMUTABLETYPE} "
              f"({_PY_TPFLAGS_IMMUTABLETYPE:#x})")
        self.assertEqual(1 << 9, _PY_TPFLAGS_HEAPTYPE)
        self.assertEqual(0x200, _PY_TPFLAGS_HEAPTYPE)
        self.assertEqual(1 << 8, _PY_TPFLAGS_IMMUTABLETYPE)
        self.assertEqual(0x100, _PY_TPFLAGS_IMMUTABLETYPE)
        dd_heap = sys.version_info >= (3, 12)
        cases = ((dict, False, False), (defaultdict, dd_heap, False),
                 (OrderedDict, False, False), (Counter, True, True),
                 (HiddenSlotsLiar, True, True), (NodeSpec, True, True),
                 (_IsolatedConditionContext, True, True))
        for cls, expected_heap, expected_python in cases:
            flags = _type_flags(cls)
            print(f"  {cls.__name__}: __flags__={flags:#x} "
                  f"heap={_is_heap_type(cls)} immutable={_is_immutable_type(cls)} "
                  f"python_defined={_is_python_defined_class(cls)}")
            with self.subTest(kind=cls.__name__):
                self.assertEqual(expected_heap, _is_heap_type(cls))
                self.assertEqual(expected_python, _is_python_defined_class(cls))

    def test_heap_bit_alone_would_misjudge_defaultdict_on_312_plus(self):
        """版本敏感的机械证据：3.12+ 的 ``defaultdict`` 是堆类型，但**不是** Python 定义的类。"""
        if sys.version_info >= (3, 12):
            self.assertTrue(_is_heap_type(defaultdict),
                            "3.12+ 的 defaultdict 自己是堆类型（实测 0x20405740）")
            self.assertTrue(_is_immutable_type(defaultdict),
                            "它是带 IMMUTABLETYPE 的 PyType_FromSpec 类型")
        else:
            self.assertFalse(_is_heap_type(defaultdict),
                             "3.11 及更早：defaultdict 是静态 C 类型")
        self.assertFalse(_is_python_defined_class(defaultdict),
                         "两种版本下都必须是 False ⇒ 精确 defaultdict 照常归一化")
        self.assertEqual((), declared_c_storage_descriptors(defaultdict))
        self.assertIsNone(instance_state_signal(defaultdict(int, {"a": 1})))

    def test_the_heap_flag_reader_resists_a_lying_metaclass(self):
        """宿主元类伪造 ``__flags__`` / 抛异常都进不来（读 ``tp_flags`` 槽）。"""
        self.assertTrue(_is_heap_type(HiddenSlotsWithLyingMetaclass))
        self.assertTrue(_is_python_defined_class(HiddenSlotsWithLyingMetaclass))
        self.assertEqual((("_store", "HiddenSlotsWithLyingMetaclass"),),
                         declared_c_storage_descriptors(
                             HiddenSlotsWithLyingMetaclass))

    def test_negative_controls_still_normalise(self):
        """负控逐字不变：精确三种 + 纯子类 + ``dict`` + ``NodeSpec`` + 隔离上下文。"""
        cases = (defaultdict(int, {"a": 1}), OrderedDict(a=1), Counter({"a": 1}),
                 PlainDictSubclass(a=1), {"a": 1}, NodeSpec(a=1),
                 _IsolatedConditionContext(a=1))
        for value in cases:
            with self.subTest(kind=type(value).__name__):
                self.assertIsNone(instance_state_signal(value))
                self.assertFalse(reads_mapping_outside_dict_storage(value))
                copied = _engine_copy(value)
                self.assertIs(type(copied), dict)
                self.assertEqual({"a": 1}, copied)
                snapshot = _snapshot_value({"m": value})
                self.assertEqual({"a": 1}, snapshot["m"])
                self.assertIs(type(snapshot["m"]), dict)

        class BenignOD(OrderedDict):
            pass

        class BenignCounter(Counter):
            pass

        for value in (BenignOD(a=1), BenignCounter({"a": 1})):
            with self.subTest(kind=type(value).__name__):
                self.assertEqual((), declared_c_storage_descriptors(type(value)))
                self.assertIsNone(instance_state_signal(value))
                self.assertEqual({"a": 1}, _engine_copy(value))

    def test_benign_defaultdict_subclass_is_the_declared_over_refusal(self):
        """过拒面（明说）：良性 ``defaultdict`` 子类数据在真实存储里，仍被拒。"""
        value = BenignDefaultDictSubclass(int, {"a": 1})
        self.assertEqual({"a": 1}, dict(dict.items(value)), "数据确实在真实存储里")
        self.assertEqual(
            "instance state (C descriptor default_factory on defaultdict)",
            instance_state_signal(value))
        self.assertTrue(reads_mapping_outside_dict_storage(value))
        with self.assertRaises(_EngineCopyRefused):
            _engine_copy(value)
        self.assertEqual({"a": 1}, _engine_copy(dict(dict.items(value))),
                         "显式展平（读真实存储）后照常通过")

    def test_stdlib_freezable_default_dict_is_the_real_over_refusal_example(self):
        """verifier 找到的 stdlib 真实例子（3.9 无该模块 ⇒ skip）。"""
        try:
            from importlib.metadata._collections import FreezableDefaultDict
        except ImportError:                     # CPython 3.9
            self.skipTest("importlib.metadata._collections 在 3.9 不存在")
        stdlib_value = FreezableDefaultDict(int, {"a": 1})
        self.assertEqual({"a": 1}, dict(dict.items(stdlib_value)),
                         "stdlib 这个子类的数据也在真实存储里")
        self.assertEqual(
            "instance state (C descriptor default_factory on defaultdict)",
            instance_state_signal(stdlib_value))
        with self.assertRaises(_EngineCopyRefused):
            _engine_copy(stdlib_value)

    def test_clause_5_hook_free_on_hostile_shapes(self):
        """零宿主钩子：判定与两条拒绝路径不调用宿主方法/``__getattribute__``。"""
        for cls in HIDING_FORMS:
            with self.subTest(kind=cls.__name__):
                value = cls({"a": 1})
                instance_state_signal(value)
                reads_mapping_outside_dict_storage(value)
                with self.assertRaises(_EngineCopyRefused):
                    _engine_copy({"m": value})
                with self.assertRaises(StateSnapshotError):
                    _snapshot_value({"m": value})
        self.assertEqual([], HOOKS, HOOKS)


class TestIssue69R13MappingViewReadsRealStorage(unittest.TestCase):
    """``$F.keys``/``$F.values``/``$F.items`` 读**真实** dict 存储，不采信宿主自述。"""

    def setUp(self) -> None:
        HOOKS.clear()

    def _dsl_value(self, cls, func: str):
        with freshness_scope():
            ctx = _alias_context()
            ctx.set_state("$INPUT", {"d": cls({"a": 1})})
            ctx.set_state("$VAR", ctx.evaluate({}))
            raised = None
            try:
                ctx.evaluate(f"$F.set($VAR,'k',$F.{func}($INPUT.d))")
            except Exception as exc:            # noqa: BLE001 - 判决即探针对象
                raised = exc
        return raised, ctx

    def test_lying_keys_items_values_yield_the_real_storage(self):
        """pre：``$VAR == {'k': ['LIE']}`` 且宿主钩子被调用；r13：真实存储 + 零钩子。"""
        cases = (("keys", LyingKeysPlainList, ["a"]),
                 ("items", LyingItemsPlainList, [("a", 1)]),
                 ("values", LyingValuesPlainList, [1]))
        for func, cls, expected in cases:
            with self.subTest(func=func):
                raised, ctx = self._dsl_value(cls, func)
                self.assertIsNone(raised)
                self.assertEqual({"k": expected}, ctx.get_state("$VAR"),
                                 "真实存储的物化结果，不是宿主自述的 LIE")
                self.assertEqual([], HOOKS, "宿主的覆写方法零调用")

    def test_the_alias_helpers_read_the_same_real_storage(self):
        for func, expected in (("getDictKeys", ["a"]), ("getDictValues", [1])):
            with self.subTest(func=func):
                raised, ctx = self._dsl_value(LyingKeysPlainList, func)
                self.assertIsNone(raised)
                self.assertEqual({"k": expected}, ctx.get_state("$VAR"))
                self.assertEqual([], HOOKS)

    def test_plain_dict_behaviour_is_byte_unchanged(self):
        """负控：普通 ``dict`` 的 ``dict_keys``/``dict_values``/``dict_items`` 物化照旧。"""
        with freshness_scope():
            ctx = _alias_context()
            ctx.set_state("$INPUT", {"d": {"a": 1}})
            ctx.set_state("$VAR", ctx.evaluate({}))
            for name, expr in (("k", "$F.keys($INPUT.d)"),
                               ("k2", "$F.values($INPUT.d)"),
                               ("k3", "$F.items($INPUT.d)"),
                               ("k4", "$F.getDictKeys($INPUT.d)"),
                               ("k5", "$F.getDictValues($INPUT.d)")):
                self.assertIsNone(
                    ctx.evaluate(f"$F.set($VAR,'{name}',{expr})"))
        self.assertEqual({"k": ["a"], "k2": [1], "k3": [("a", 1)],
                          "k4": ["a"], "k5": [1]}, ctx.get_state("$VAR"))
        self.assertEqual([], HOOKS)

    def test_direct_reader_returns_the_base_descriptor_views(self):
        """零宿主钩子地拿到 ``dict_keys``/``dict_values``/``dict_items``（真实布局）。"""
        value = LyingKeysPlainList({"a": 1})
        self.assertEqual(["a"], list(_engine_mapping_view("keys", value)))
        self.assertEqual([1], list(_engine_mapping_view("values",
                                                        LyingValuesPlainList({"a": 1}))))
        self.assertEqual([("a", 1)],
                         list(_engine_mapping_view("items",
                                                   LyingItemsPlainList({"a": 1}))))
        self.assertEqual(["a"], list(_engine_mapping_view("getDictKeys", value)))
        self.assertEqual([], HOOKS)

    def test_non_dict_mapping_is_named_refused(self):
        """非 ``dict`` 的映射读不到真实存储 ⇒ 具名 ``ValueError``（含 ``plaita#69``）。"""
        for value in (UserDict({"a": 1}), ChainMap({"a": 1}),
                      types.MappingProxyType({"a": 1}), os.environ):
            with self.subTest(kind=type(value).__name__):
                with self.assertRaises(ValueError) as cm:
                    _engine_mapping_view("keys", value)
                message = str(cm.exception)
                self.assertIn("plaita#69", message)
                self.assertIn("not a dict", message)
                self.assertIn(type(value).__name__, message)

    def test_spoofed_class_is_named_refused(self):
        """``__class__`` 伪造让 ``isinstance(dict)`` 为真 ⇒ 基类描述符拒绝 ⇒ 具名化。"""
        spoofed = _SpoofedDict()
        self.assertTrue(isinstance(spoofed, dict))
        for func in ("keys", "values", "items"):
            with self.subTest(func=func):
                with self.assertRaises(ValueError) as cm:
                    _engine_mapping_view(func, spoofed)
                self.assertIn("plaita#69", str(cm.exception))
                self.assertIn("spoofed __class__", str(cm.exception))

        with freshness_scope():
            ctx = _alias_context()
            ctx.set_state("$INPUT", {"d": _SpoofedDict()})
            ctx.set_state("$VAR", ctx.evaluate({}))
            with self.assertRaises(Exception) as cm:   # noqa: BLE001 - 判决即探针
                ctx.evaluate("$F.set($VAR,'k',$F.keys($INPUT.d))")
        message = str(cm.exception)
        # DSL 面先经过 #203 的容器闸（真实类型判定），伪造的 __class__ 被更早
        # 具名拒绝；直调 _engine_mapping_view 的 #69 spoofed 拒绝仍由上面钉住。
        self.assertTrue("plaita#203" in message or "plaita#69" in message)
        if "plaita#203" not in message:
            self.assertIn("spoofed __class__", message)

    def test_raising_getattribute_probe_is_named_not_bare_runtimeerror(self):
        """``isinstance`` 回落读 ``__class__`` 时宿主抛异常 ⇒ 具名拒绝，不漏裸 ``RuntimeError``。

        只直接钉 ``_engine_mapping_view``：DSL 面 ``$F.keys($INPUT.d)`` 会**先**经过
        **路径遍历**的 ``isinstance(attr, str)``。

        plaita#163（后续单）已补上那一跳：遍历里的探测改走
        ``expression_parser._probe_isinstance``，宿主 ``__class__`` 抛异常时同样具名
        拒绝（含 ``plaita#69``）。DSL 面的动态断言由
        ``tests/unit/test_issue163_path_traversal_probe.py`` 钉住；本测试继续只钉
        ``_engine_mapping_view`` 这一跳本身（两个跳各自有独立反例）。
        """
        value = _RaisingGetattribute()
        with self.assertRaises(ValueError) as cm:
            _engine_mapping_view("keys", value)
        message = str(cm.exception)
        self.assertIn("plaita#69", message)
        self.assertIn("_RaisingGetattribute", message)
        self.assertNotIn("__getattribute__ boom", message)


class TestIssue69R13SnapshotSharesNonRebuiltValues(unittest.TestCase):
    """快照**按引用**共享"不属于五种重建容器"的值 —— 其中包含**容器**。"""

    SHARED = ("mappingproxy", "ChainMap", "UserDict", "_Environ")

    def test_the_shared_by_reference_enumeration_is_pinned(self):
        values = (types.MappingProxyType({"a": 1}), ChainMap({"a": 1}),
                  UserDict({"a": 1}), os.environ)
        self.assertEqual(self.SHARED, tuple(type(v).__name__ for v in values),
                         "枚举本身就是文档承诺的那四种")
        for value in values:
            with self.subTest(kind=type(value).__name__):
                self.assertIs(value, _snapshot_value(value),
                              "非五种重建容器 ⇒ 按引用共享**同一对象**")
                self.assertIn(type(value).__name__, self.SHARED)

    def test_the_five_rebuilt_kinds_are_never_shared(self):
        for value in ({"a": 1}, [1], (1,), {1}, frozenset({1})):
            with self.subTest(kind=type(value).__name__):
                self.assertIsNot(value, _snapshot_value(value))

    def test_the_shared_types_are_named_refused_on_the_dsl_copy_path(self):
        """按引用共享**不是数据通道**：DSL 路径对这四个具名拒绝。"""
        for value in (types.MappingProxyType({"a": 1}), ChainMap({"a": 1}),
                      UserDict({"a": 1}), os.environ):
            with self.subTest(kind=type(value).__name__):
                with self.assertRaises(_EngineCopyRefused) as cm:
                    _engine_copy(value)
                self.assertEqual(type(value).__name__, cm.exception.type_name)


class TestIssue69R13RationaleIsCorrected(unittest.TestCase):
    """文档口径：布局规则 / heap flag / off-object 类属性族更正理由 / r13 章节。"""

    def test_module_states_the_layout_rule_and_the_heap_flag(self):
        module = (ROOT / "plaita/core/expression_parser.py").read_text(encoding="utf-8")
        for token in ("_is_heap_type", "_is_python_defined_class",
                      "_PY_TPFLAGS_IMMUTABLETYPE", "Py_TPFLAGS_HEAPTYPE", "0x200",
                      "0x100", "HiddenSlotsLiar", "布局判据", "类体写不出", "假的"):
            with self.subTest(token=token):
                self.assertIn(token, module)

    def test_module_no_longer_states_the_false_premise_as_fact(self):
        """旧措辞里的豁免理由必须以"被引用+判假"的形式存在，不能原样当事实。"""
        module = (ROOT / "plaita/core/expression_parser.py").read_text(encoding="utf-8")
        self.assertIn("r13 规则（只看**每个层命名空间里真实存在的描述符**）", module)
        self.assertNotIn("**为什么排除声明层就是 ``cls`` 自己的那种**", module)

    def test_migration_has_the_r13_section_with_all_readings(self):
        migration = (ROOT / "MIGRATION.md").read_text(encoding="utf-8")
        for token in ("### r13：", "HiddenSlotsLiar", "Py_TPFLAGS_HEAPTYPE",
                      "Py_TPFLAGS_IMMUTABLETYPE", "0x200", "0x100", "0x20405740",
                      "LieKeysPlainList", "FreezableDefaultDict",
                      "MappingProxyType", "ChainMap", "UserDict", "os.environ",
                      "常量/缓存", "r13 理由更正"):
            with self.subTest(token=token):
                self.assertIn(token, migration)

    def test_docs_site_has_the_r13_section(self):
        page = (ROOT / "docs-site/docs/guide/expressions.md").read_text(encoding="utf-8")
        for token in ("r13 补记", "HiddenSlotsLiar", "Py_TPFLAGS_HEAPTYPE",
                      "Py_TPFLAGS_IMMUTABLETYPE", "0x20405740",
                      "LieKeysPlainList", "MappingProxyType", "UserDict",
                      "r13 理由更正"):
            with self.subTest(token=token):
                self.assertIn(token, page)

    def test_the_snapshot_sharing_enumeration_is_documented(self):
        tokens = ("MappingProxyType", "ChainMap", "UserDict", "os.environ")
        for relpath in ("plaita/core/expression_parser.py", "MIGRATION.md",
                        "docs-site/docs/guide/expressions.md"):
            text = (ROOT / relpath).read_text(encoding="utf-8")
            for token in tokens:
                with self.subTest(doc=relpath, token=token):
                    self.assertIn(token, text)


if __name__ == "__main__":
    unittest.main()
