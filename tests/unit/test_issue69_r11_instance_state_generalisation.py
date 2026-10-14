"""plaita#69 **r11** —— D1 第三条判据从**代理**改成**性质**，外加三处收口。

r10 的第三次判定（``reads_mapping_outside_dict_storage`` clause iii）测的是
**代理**："实例有非空 ``__dict__``"；它宣称的**性质**是"对象可能在真实 ``dict``
存储之外持有状态"。``__slots__`` 形状因此漏网：

    class SlotsItemsLiar(dict):
        __slots__ = ("_d",)
        def __init__(self, d): self._d = dict(d)
        def items(self): return self._d.items()

r10 实测：``dict.items(v) == {}``（真实存储是空的）、``v.items() == {'a': 1}``（宿主
自述有一个键）、``reads_mapping_outside_dict_storage(v) is False``（判据判否）⇒
``_engine_copy(v) == {}``、``_snapshot_value({'m': v}) == {'m': {}}``，DSL 循环条件里
``$F.set($INPUT.cfg,'k','v')`` **报成功**（失数据但无错误）。同族还有
``SlotsItemsKeysValuesLiar`` / ``SlotsGetItemLiar`` / ``HidingDict``
（``@property def __dict__`` 遮蔽真实实例字典）。对照：``SlotsIterLiar``（同样的
``__slots__``，另加 ``__iter__``）**被拒** —— 两者只差**数据放在哪**。

本文件钉五件事：

1. **性质判据**（:class:`TestIssue69R11InstanceStateIsTheProperty`）：四条机械可判、
   零宿主钩子的析取（非空 ``__dict__`` ∨ MRO 非空 ``__slots__`` ∨ 遮蔽的 ``__dict__``
   ∨ 序列化钩子），全部形状在**两条拷贝路径**与 **DSL** 上都具名拒绝、零写穿；
   ``MissingLiar``（只有 ``__missing__``、没有数据）仍然**正确归一化**。类自身的读取
   走 C 描述符（``type.__dict__["__mro__"] / ["__dict__"]``），所以宿主**元类**
   覆写 ``__getattribute__``（抛异常或说谎）同样进不来 —— 说谎的元类否则能把声明了
   ``__slots__`` 的基类抹掉、让判据重新静默丢数据。
2. **零宿主钩子**（:class:`TestIssue69R11PredicateEntersNoHostHooks`）：判定过程不调用
   宿主 ``__getattribute__``/``items``/``__iter__``/``__len__``；r10 的
   ``instance_carries_own_state`` 会**进入**遮蔽的 ``__dict__`` property（红），r11
   只看类对象（绿）。
3. **负控与过拒代价**（:class:`TestIssue69R11NegativeControlsAndOverRefusalCost`）：
   ``OrderedDict``/``defaultdict``/``Counter``/纯子类/精确 ``dict``/``NodeSpec``/
   ``_IsolatedConditionContext``/``MissingLiar`` 不受影响；``__slots__ = ()`` 与只含
   ``__dict__``/``__weakref__`` 占位项的形态算**空**；带实例状态（``__dict__`` **或**
   ``__slots__``）的 ``dict`` 子类**被过拒**，且过拒代价在文档里明说。
4. **文档逃生口修正**（:class:`TestIssue69R11DictEscapeHatchIsLossy`）：文档推荐的
   ``dict(...)`` 展平**读的是真实存储**，对"数据在别处"的形状恰好返回 ``{}``（静默
   错误答案）；``dict(v.items())`` 采信宿主自述。recovery 必须是从已知良好数据重建 /
   ``ctx.evaluate``。
5. **r11 的两处独立收口**：
   * finding 5（:class:`TestIssue69R11RaisingGetattributeIsNamed`）：宿主
     ``__getattribute__`` 抛异常时，``isinstance`` 回落读 ``__class__`` ⇒ r6 起漏**裸**
     ``RuntimeError``；r11 具名 ``_EngineCopyRefused`` / ``StateSnapshotError``；
   * finding 6（:class:`TestIssue69R11ViewMaterialisationIsNarrowed`）：
     ``_detach_alias`` 的视图分支以前对宿主导出的 ABC 视图子类调用 ``list(value)``
     （实测钩子 ``['view.__iter__','view.__len__']``）并把宿主自述内容写成数据；r11
     只对 ``__iter__``/``__len__`` 都是 **C 槽描述符**的视图物化，其余具名拒绝。
6. **§P 权威口径**（:class:`TestIssue69R11PerformanceAuthorityIsCurrentRevision`）：
   文档里带 µs 的"权威读数"必须指向**当前 revision（r11）**，历史值必须标 revision。
"""
from __future__ import annotations

import pathlib
import unittest
from collections import Counter, OrderedDict, defaultdict
from collections.abc import KeysView, ItemsView, ValuesView

from plaita.core.errors import StateSnapshotError
from plaita.core.expression_parser import (
    _EngineCopyRefused,
    _canonical_persistable_kind,
    _detach_alias,
    _dict_view_is_materialisable,
    _engine_copy,
    _is_dict_view_type,
    _normalised_slot_names,
    declares_instance_state_hooks,
    declared_instance_slots,
    dict_storage_protocol_overrides,
    freshness_scope,
    instance_carries_own_state,
    instance_state_signal,
    reads_mapping_outside_dict_storage,
    shadows_instance_dict,
)
from plaita.core.state import _snapshot_value
from tests.unit.test_issue69_r8_snapshot_discipline import _run_loop

#: 宿主钩子计数器（跨测试清空）：任何一次调用都说明判定/物化落回了宿主代码。
HOOKS: list = []


# --------------------------------------------------------------------------- #
# 阻塞反例形状（r10 下两条路径都静默产出 {}，DSL 报成功）
# --------------------------------------------------------------------------- #
class SlotsItemsLiar(dict):
    """r11 阻塞反例：``__slots__`` 存数据、Python 覆写 ``items()`` 自述。"""

    __slots__ = ("_d",)

    def __init__(self, d):
        self._d = dict(d)

    def items(self):
        return self._d.items()


class SlotsItemsKeysValuesLiar(dict):
    """同族变体一：``items``/``keys``/``values`` 三个自述口。"""

    __slots__ = ("_d",)

    def __init__(self, d):
        self._d = dict(d)

    def items(self):
        return self._d.items()

    def keys(self):
        return self._d.keys()

    def values(self):
        return self._d.values()


class SlotsGetItemLiar(dict):
    """同族变体二：只覆写 ``__getitem__``（唯一自述口）。"""

    __slots__ = ("_d",)

    def __init__(self, d):
        self._d = dict(d)

    def __getitem__(self, key):
        return self._d[key]


class HidingDict(dict):
    """同族变体三：``@property def __dict__`` 遮蔽真实实例字典。"""

    def __init__(self, d):
        self._d = dict(d)

    @property
    def __dict__(self):
        return {}


class SlotsIterLiar(dict):
    """对照：同样的 ``__slots__``，**另加** ``__iter__``/``__len__``（clause ii 已拒）。"""

    __slots__ = ("_d",)

    def __init__(self, d):
        self._d = dict(d)

    def items(self):
        return self._d.items()

    def __iter__(self):
        return iter(self._d)

    def __len__(self):
        return len(self._d)


class MissingLiar(dict):
    """负控：只有 ``__missing__``、**没有**存储之外的数据 ⇒ ``{}`` 是**正确**读数。"""

    def __missing__(self, key):
        return 42


class SlotsStringLiar(dict):
    """字符串简写形态：``__slots__ = "_d"``。"""

    __slots__ = "_d"

    def __init__(self, d):
        self._d = dict(d)

    def items(self):
        return self._d.items()


class InheritedSlotsLiar(SlotsStringLiar):
    """继承/合并的槽：子类自己不声明 ``__slots__``。"""


class SlotsListFormLiar(dict):
    """列表形态：``__slots__ = ["_d"]``。"""

    __slots__ = ["_d"]

    def __init__(self, d):
        self._d = dict(d)

    def items(self):
        return self._d.items()


class GetStateLiar(dict):
    """序列化钩子：``__getstate__``（非 C 槽实现；数据仍在真实存储里）。"""

    def __getstate__(self):
        return dict(self)


class SetStateLiar(dict):
    """序列化钩子：``__setstate__``。"""

    def __setstate__(self, state):
        pass


class ReduceExLiar(dict):
    """序列化钩子：``__reduce_ex__``。"""

    def __reduce_ex__(self, protocol):
        return (dict, ())


class ReduceOnlyLiar(dict):
    """负控：``__reduce__`` **不是**信号（``Counter`` 就覆写它而数据在真实存储里）。"""

    def __reduce__(self):
        return (dict, ())


# --------------------------------------------------------------------------- #
# 负控 / 占位项 / 过拒代价
# --------------------------------------------------------------------------- #
class PlainDict(dict):
    """纯 ``dict`` 子类（无覆写、无实例状态）。"""


class EmptySlotsDict(dict):
    """``__slots__ = ()`` ⇒ 空（``NodeSpec`` 就是这形状）。"""

    __slots__ = ()


class WeakrefOnlySlotsDict(dict):
    """只声明 ``__weakref__`` ⇒ 空（不是额外存储）。"""

    __slots__ = ("__weakref__",)


class DictSlotPlaceholderDict(dict):
    """只声明 ``__dict__`` ⇒ 空（实例字典由 clause iii 第一条管）。"""

    __slots__ = ("__dict__",)


class BothPlaceholdersDict(dict):
    """``__dict__`` + ``__weakref__`` ⇒ 空。"""

    __slots__ = ("__dict__", "__weakref__")


class AttrDict(dict):
    """**过拒代价（r10 面）**：带实例属性的良性子类，数据确实在真实存储里。"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.meta = "not data, but indistinguishable"


class SlotsCacheDict(dict):
    """**过拒代价（r11 扩面）**：带非空 ``__slots__`` 的良性子类，数据在真实存储里。"""

    __slots__ = ("_cache",)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._cache = "not data, but indistinguishable"


class HookedHidingDict(dict):
    """遮蔽 ``__dict__`` 的钩子版本：进入 property 就记一笔。"""

    def __init__(self, d):
        self._d = dict(d)

    @property
    def __dict__(self):
        HOOKS.append("HidingDict.__dict__")
        return {}


class HookedStateDict(dict):
    """把判定可能碰到的每个宿主口都记一笔（容器/属性钩子）。"""

    __slots__ = ("_d",)

    def __init__(self, d):
        self._d = dict(d)

    def __getattribute__(self, name):
        if name not in ("__class__", "__init__"):
            HOOKS.append(f"__getattribute__({name})")
        return object.__getattribute__(self, name)

    def items(self):
        HOOKS.append("items")
        return self._d.items()

    def keys(self):
        HOOKS.append("keys")
        return self._d.keys()

    def values(self):
        HOOKS.append("values")
        return self._d.values()

    def __iter__(self):
        HOOKS.append("__iter__")
        return iter(self._d)

    def __len__(self):
        HOOKS.append("__len__")
        return len(self._d)

    def __getitem__(self, key):
        HOOKS.append("__getitem__")
        return self._d[key]


class RaisingGetattribute:
    """宿主 ``__getattribute__`` 抛异常（r11 finding 5 的形状）。"""

    def __getattribute__(self, name):
        raise RuntimeError("host __getattribute__ refuses " + name)


# --------------------------------------------------------------------------- #
# 元类面 + 非标准 __slots__ 载荷（r11：判据只看 C 描述符，不进元类属性协议）
# --------------------------------------------------------------------------- #
class _OpaqueSlotsPayload:
    """非标准 ``__slots__`` 载荷：一旦被迭代就记一笔宿主钩子。"""

    def __iter__(self):
        HOOKS.append("slots.__iter__")
        return iter(["_d"])


class OpaqueSlotsLiar(dict):
    """``__slots__`` 是任意可迭代对象（CPython 接受）⇒ 不能迭代它来判空。"""

    __slots__ = _OpaqueSlotsPayload()

    def __init__(self, d):
        self._d = dict(d)

    def items(self):
        return self._d.items()


class _HonestMeta(type):
    """普通元类（负控）。"""


class MetaSlotsLiar(dict, metaclass=_HonestMeta):
    """带普通元类的 ``__slots__`` 形状（读数不得因元类而变）。"""

    __slots__ = ("_d",)

    def __init__(self, d):
        self._d = dict(d)

    def items(self):
        return self._d.items()


class _RaisingMeta(type):
    """元类覆写 ``__getattribute__``：对 ``__mro__``/``__dict__`` 直接抛异常。"""

    def __getattribute__(self, name):
        if name in ("__mro__", "__dict__"):
            raise RuntimeError("hostile metaclass refuses " + name)
        return super().__getattribute__(name)


class RaisingMetaSlotsLiar(dict, metaclass=_RaisingMeta):
    """``__slots__`` 数据 + 拒绝属性协议的元类（r10/r11 判据会漏裸 ``RuntimeError``）。"""

    __slots__ = ("_d",)

    def __init__(self, d):
        self._d = dict(d)

    def items(self):
        return self._d.items()


class _LyingMeta(type):
    """元类说谎：``__mro__`` 换成 ``(dict, object)``、类命名空间换成 ``{}``。"""

    def __getattribute__(self, name):
        if name == "__mro__":
            HOOKS.append("meta.__mro__")
            return (dict, object)
        if name == "__dict__":
            HOOKS.append("meta.__dict__")
            return {}
        return super().__getattribute__(name)


class LyingMetaSlotsLiar(dict, metaclass=_LyingMeta):
    """说谎元类能"抹掉"声明了 ``__slots__`` 的基类 ⇒ 旧判据重新静默丢数据。"""

    __slots__ = ("_d",)

    def __init__(self, d):
        self._d = dict(d)

    def items(self):
        return self._d.items()


class HookDictClass(dict):
    """只覆写 ``items()``、数据在真实存储里的类（负控：仍须归一化）。"""

    def items(self):
        return dict.items(self)


class EvilKeysView(KeysView):
    """宿主导出的 ABC 视图子类：``__iter__``/``__len__`` 是 Python 实现。"""

    def __init__(self, mapping):
        self._m = mapping

    def __iter__(self):
        HOOKS.append("view.__iter__")
        return iter(["LIE"])

    def __len__(self):
        HOOKS.append("view.__len__")
        return 1


class ViewReturningDict(dict):
    """数据在真实存储里（可归一化），但 ``keys()`` 返回宿主自述的 ABC 视图。"""

    def keys(self):
        return EvilKeysView(dict(self))


#: 阻塞形状家族 → 期望信号：全部满足"真实 dict 存储是空的 + 宿主自述有内容"。
SILENT_LOSS_SHAPES = (
    (SlotsItemsLiar, "instance state (__slots__)"),
    (SlotsItemsKeysValuesLiar, "instance state (__slots__)"),
    (SlotsGetItemLiar, "instance state (__slots__)"),
    (HidingDict, "shadowed __dict__"),
    (SlotsStringLiar, "instance state (__slots__)"),
    (InheritedSlotsLiar, "instance state (__slots__)"),
    (SlotsListFormLiar, "instance state (__slots__)"),
)

#: 序列化钩子形状 → 声明的钩子名（数据可以就在真实存储里；判据仍然 fail-closed）。
STATE_HOOK_SHAPES = (
    (GetStateLiar, "__getstate__"),
    (SetStateLiar, "__setstate__"),
    (ReduceExLiar, "__reduce_ex__"),
)


class TestIssue69R11InstanceStateIsTheProperty(unittest.TestCase):
    """clause iii 按**性质**判定：存储之外的实例状态一律具名拒绝。"""

    def setUp(self) -> None:
        HOOKS.clear()

    # ---- 每条形状一个**非 subTest** 判决（变异重放器只认 FAILED/ERROR 行） ----

    def test_slots_items_liar_is_named_refused_on_both_copy_paths(self):
        value = SlotsItemsLiar({"a": 1})
        self.assertEqual({}, dict(dict.items(value)), "真实存储是空的")
        self.assertEqual({"a": 1}, dict(value.items()), "宿主自述有一个键")
        self.assertTrue(reads_mapping_outside_dict_storage(value))
        self.assertEqual("instance state (__slots__)", instance_state_signal(value))
        with self.assertRaises(_EngineCopyRefused) as cm:
            _engine_copy({"m": value})
        self.assertEqual("SlotsItemsLiar", cm.exception.type_name)
        self.assertIn("instance state (__slots__)", str(cm.exception))
        with self.assertRaises(StateSnapshotError) as cm:
            _snapshot_value({"m": value})
        self.assertIn("SlotsItemsLiar", cm.exception.message)
        self.assertIn("instance state (__slots__)", cm.exception.message)
        self.assertIn("$['m']", cm.exception.message)

    def test_hiding_dict_is_named_refused_on_both_copy_paths(self):
        value = HidingDict({"a": 1})
        self.assertEqual({}, dict(dict.items(value)), "真实存储是空的")
        self.assertEqual("shadowed __dict__", instance_state_signal(value))
        self.assertTrue(shadows_instance_dict(HidingDict))
        with self.assertRaises(_EngineCopyRefused) as cm:
            _engine_copy({"m": value})
        self.assertEqual("HidingDict", cm.exception.type_name)
        self.assertIn("shadowed __dict__", str(cm.exception))
        with self.assertRaises(StateSnapshotError) as cm:
            _snapshot_value({"m": value})
        self.assertIn("HidingDict", cm.exception.message)
        self.assertIn("shadowed __dict__", cm.exception.message)

    def test_state_hook_shapes_are_named_refused_on_both_copy_paths(self):
        for cls, hook in STATE_HOOK_SHAPES:
            with self.subTest(kind=cls.__name__):
                value = cls({"a": 1})
                self.assertIn(hook, declares_instance_state_hooks(cls))
                with self.assertRaises(_EngineCopyRefused) as cm:
                    _engine_copy({"m": value})
                self.assertEqual(cls.__name__, cm.exception.type_name)
                self.assertIn(f"instance state hooks ({hook})", str(cm.exception))
                with self.assertRaises(StateSnapshotError) as cm:
                    _snapshot_value({"m": value})
                self.assertIn(hook, cm.exception.message)

    def test_every_silent_loss_shape_is_refused_on_both_paths(self):
        """表驱动：全部形态（含字符串/列表/继承槽、遮蔽 ``__dict__``）两路径都具名拒绝。"""
        for cls, signal in SILENT_LOSS_SHAPES:
            with self.subTest(kind=cls.__name__):
                value = cls({"a": 1})
                self.assertEqual({}, dict(dict.items(value)), "真实存储是空的")
                self.assertEqual(signal, instance_state_signal(value))
                self.assertTrue(reads_mapping_outside_dict_storage(value))
                with self.assertRaises(_EngineCopyRefused) as cm:
                    _engine_copy({"m": value})
                self.assertEqual(cls.__name__, cm.exception.type_name)
                self.assertIn(signal, str(cm.exception))
                with self.assertRaises(StateSnapshotError) as cm:
                    _snapshot_value({"m": value})
                self.assertIn(cls.__name__, cm.exception.message)

    def test_host_self_description_differs_from_the_real_storage(self):
        """证据：这些形状的"自述内容"确实与真实存储不同（否则不是丢数据形状）。

        ``HidingDict`` 的自述口是**被遮蔽的 ``__dict__``**（返回 ``{}``）：它证明不了
        "自述有内容"，但能证明"真实实例字典被描述符挡住读不到" —— 用
        ``type.__dictoffset__ != 0`` 钉住它**确实有**真实实例字典槽。
        """
        self.assertEqual({"a": 1}, dict(SlotsItemsLiar({"a": 1}).items()))
        self.assertEqual({"a": 1}, dict(SlotsItemsKeysValuesLiar({"a": 1}).items()))
        self.assertEqual(1, SlotsGetItemLiar({"a": 1})["a"])
        for value in (SlotsItemsLiar({"a": 1}), SlotsItemsKeysValuesLiar({"a": 1}),
                      SlotsGetItemLiar({"a": 1})):
            with self.subTest(kind=type(value).__name__):
                self.assertEqual({}, dict(dict.items(value)), "真实存储是空的")
        self.assertNotEqual(0, HidingDict.__dictoffset__,
                            "HidingDict 确实有真实实例字典槽（被 property 挡住）")
        self.assertEqual(0, SlotsItemsLiar.__dictoffset__, "__slots__ 类型没有实例字典")

    def test_dsl_loop_condition_refusal_is_named_and_writes_nothing(self):
        """用户可见面：r10 这里**报成功**（副本静默变 ``{}``），r11 具名拒绝。"""
        for cls in (SlotsItemsLiar, SlotsItemsKeysValuesLiar, SlotsGetItemLiar,
                    HidingDict, SlotsStringLiar, InheritedSlotsLiar):
            with self.subTest(kind=cls.__name__):
                payload = {"m": cls({"a": 1}), "cfg": {"a": 1}}
                raised, detail = _run_loop(payload, "$F.set($INPUT.cfg,'k','v')")
                self.assertIsNotNone(raised, "不可保真拷贝 ⇒ 必须拒绝，不能报成功")
                self.assertIn("plaita#69", detail)
                self.assertNotIn("unhashable type", detail)
                self.assertEqual({"a": 1}, payload["cfg"])      # 零写穿

    def test_control_shapes_still_normalise(self):
        """负控：无存储之外状态的形状照常归一化（两路径 + 值相等）。"""
        cases = (OrderedDict(a=1), defaultdict(int, {"a": 1}), Counter({"a": 1}),
                 PlainDict(a=1), EmptySlotsDict(a=1), WeakrefOnlySlotsDict(a=1),
                 DictSlotPlaceholderDict(a=1), BothPlaceholdersDict(a=1),
                 ReduceOnlyLiar(a=1), MissingLiar({"a": 1}), {"a": 1})
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

    def test_missing_liar_yields_its_real_storage_which_is_correct(self):
        """``MissingLiar`` 的 ``{}``/``{'a': 1}`` 是**正确**读数，不是丢数据。"""
        value = MissingLiar({"a": 1})
        self.assertEqual({"a": 1}, dict(dict.items(value)))
        self.assertIsNone(instance_state_signal(value))
        self.assertEqual({"a": 1}, _engine_copy(value))
        self.assertEqual({"a": 1}, _snapshot_value({"m": value})["m"])
        payload = {"m": value, "cfg": {"a": 1}}
        raised, _detail = _run_loop(payload, "$F.set($INPUT.cfg,'k','v')")
        self.assertIsNone(raised, "数据确实在真实存储里 ⇒ 不应拒绝")

    def test_engine_internal_dict_subclasses_are_not_refused(self):
        """引擎自有的 ``dict`` 子类（空 ``__slots__`` / 无实例状态）不得被误伤。"""
        from plaita.dsl.builder import NodeSpec
        from plaita.node.loop import _IsolatedConditionContext

        for value in (NodeSpec(), _IsolatedConditionContext({"a": 1})):
            with self.subTest(kind=type(value).__name__):
                self.assertIsNone(instance_state_signal(value))
                self.assertFalse(reads_mapping_outside_dict_storage(value))

    def test_slots_iter_liar_is_refused_by_clause_two_or_three(self):
        """对照：``SlotsIterLiar`` 依旧被拒（r10 由 clause ii 拒，r11 clause iii 也命中）。"""
        value = SlotsIterLiar({"a": 1})
        self.assertEqual({}, dict(dict.items(value)))
        self.assertEqual({"a": 1}, dict(value.items()))
        self.assertEqual("instance state (__slots__)", instance_state_signal(value))
        overrides = dict_storage_protocol_overrides(SlotsIterLiar)
        self.assertIn("__iter__", overrides)
        self.assertIn("__len__", overrides)
        with self.assertRaises(_EngineCopyRefused):
            _engine_copy({"m": value})

    def test_white_box_signal_is_mechanically_discriminating(self):
        """白盒：四条判据各自可辨（避免"碰巧恒真"）。"""
        self.assertEqual(("_d",), declared_instance_slots(SlotsItemsLiar))
        self.assertEqual(("_d",), declared_instance_slots(SlotsStringLiar))
        self.assertEqual(("_d",), declared_instance_slots(InheritedSlotsLiar))
        self.assertEqual(("_d",), declared_instance_slots(SlotsListFormLiar))
        self.assertEqual((), declared_instance_slots(EmptySlotsDict))
        self.assertEqual((), declared_instance_slots(WeakrefOnlySlotsDict))
        self.assertEqual((), declared_instance_slots(DictSlotPlaceholderDict))
        self.assertEqual((), declared_instance_slots(BothPlaceholdersDict))
        self.assertEqual((), declared_instance_slots(OrderedDict))
        self.assertEqual((), declared_instance_slots(dict))
        self.assertTrue(shadows_instance_dict(HidingDict))
        self.assertFalse(shadows_instance_dict(PlainDict))
        self.assertFalse(shadows_instance_dict(EmptySlotsDict))
        self.assertEqual(("__getstate__",), declares_instance_state_hooks(GetStateLiar))
        self.assertEqual(("__setstate__",), declares_instance_state_hooks(SetStateLiar))
        self.assertEqual(("__reduce_ex__",), declares_instance_state_hooks(ReduceExLiar))
        self.assertEqual((), declares_instance_state_hooks(ReduceOnlyLiar))
        self.assertEqual((), declares_instance_state_hooks(Counter))
        self.assertEqual((), declares_instance_state_hooks(OrderedDict))
        self.assertEqual("instance state (__dict__)", instance_state_signal(AttrDict(a=1)))
        self.assertEqual("instance state (__slots__)", instance_state_signal(SlotsCacheDict(a=1)))
        self.assertTrue(instance_carries_own_state(AttrDict(a=1)))
        self.assertTrue(instance_carries_own_state(SlotsCacheDict(a=1)))

    def test_opaque_slots_payload_is_treated_as_state_without_iterating_it(self):
        """非标准 ``__slots__`` 载荷（含带钩子的自定义可迭代）不迭代 ⇒ fail-closed 当有状态。

        CPython 接受**任意可迭代**做 ``__slots__``（实测 iterator / 自定义可迭代都行），
        所以在不调用宿主钩子的前提下无法判空 —— 取"有额外存储"。
        """
        HOOKS.clear()
        value = OpaqueSlotsLiar({"a": 1})
        self.assertEqual(("<opaque __slots__>",), _normalised_slot_names(OpaqueSlotsLiar))
        self.assertEqual("instance state (__slots__)", instance_state_signal(value))
        self.assertTrue(reads_mapping_outside_dict_storage(value))
        with self.assertRaises(_EngineCopyRefused):
            _engine_copy({"m": value})
        self.assertEqual([], HOOKS, "判据不许迭代宿主的 __slots__ 载荷")
        self.assertEqual((), _normalised_slot_names(EmptySlotsDict))

    def test_lying_metaclass_cannot_hide_the_slots_base(self):
        """说谎的元类把 ``__mro__`` 换成 ``(dict, object)``、类命名空间换成 ``{}``。

        r11 实测（commit dec5b96）：判据读 ``cls.__mro__`` / ``vars(cls)`` 时被这两种
        谎话骗过 ⇒ ``reads_mapping_outside_dict_storage(v) is False``、``_engine_copy(v)
        == {}``、``_snapshot_value({'m': v}) == {'m': {}}``（同一条阻塞反例换了个通道）。
        判据现在走 C 描述符 ``type.__dict__["__mro__"] / ["__dict__"]``。
        """
        HOOKS.clear()
        value = LyingMetaSlotsLiar({"a": 1})
        self.assertEqual({}, dict(dict.items(value)), "真实存储是空的")
        self.assertEqual({"a": 1}, dict(value.items()), "宿主自述有内容")
        self.assertEqual((dict, object), LyingMetaSlotsLiar.__mro__, "元类确实在说谎")
        self.assertEqual("instance state (__slots__)", instance_state_signal(value))
        self.assertTrue(reads_mapping_outside_dict_storage(value))
        with self.assertRaises(_EngineCopyRefused) as cm:
            _engine_copy({"m": value})
        self.assertEqual("LyingMetaSlotsLiar", cm.exception.type_name)
        self.assertIn("instance state (__slots__)", str(cm.exception))
        with self.assertRaises(StateSnapshotError) as cm:
            _snapshot_value({"m": value})
        self.assertIn("LyingMetaSlotsLiar", cm.exception.message)

    def test_raising_metaclass_does_not_leak_a_bare_runtimeerror(self):
        """抛异常的元类（``__mro__``/``__dict__`` 直接 raise）⇒ 具名拒绝，不是裸异常。"""
        HOOKS.clear()
        value = RaisingMetaSlotsLiar({"a": 1})
        self.assertEqual({}, dict(dict.items(value)))
        self.assertEqual({"a": 1}, dict(value.items()))
        self.assertEqual("instance state (__slots__)", instance_state_signal(value))
        self.assertTrue(reads_mapping_outside_dict_storage(value))
        with self.assertRaises(_EngineCopyRefused) as cm:
            _engine_copy({"m": value})
        self.assertEqual("RaisingMetaSlotsLiar", cm.exception.type_name)
        with self.assertRaises(StateSnapshotError) as cm:
            _snapshot_value({"m": value})
        self.assertIn("RaisingMetaSlotsLiar", cm.exception.message)
        self.assertEqual([], HOOKS, HOOKS)

    def test_dsl_refuses_the_metaclass_shapes_too(self):
        """DSL 面：元类通道的形状同样具名拒绝、零写穿。"""
        for cls in (LyingMetaSlotsLiar, RaisingMetaSlotsLiar):
            with self.subTest(kind=cls.__name__):
                payload = {"m": cls({"a": 1}), "cfg": {"a": 1}}
                raised, detail = _run_loop(payload, "$F.set($INPUT.cfg,'k','v')")
                self.assertIsNotNone(raised)
                self.assertIn("plaita#69", detail)
                self.assertEqual({"a": 1}, payload["cfg"])

    def test_honest_metaclass_is_unaffected(self):
        """负控：普通元类（含 ABCMeta 的虚拟注册）读数逐字不变。"""
        self.assertEqual(("_d",), declared_instance_slots(MetaSlotsLiar))
        self.assertFalse(shadows_instance_dict(MetaSlotsLiar))
        self.assertEqual((), declares_instance_state_hooks(MetaSlotsLiar))
        self.assertEqual((), declared_instance_slots(OrderedDict))
        self.assertEqual(("items",), dict_storage_protocol_overrides(HookDictClass))
        self.assertTrue(_is_dict_view_type(type({"a": 1}.keys())))
        self.assertFalse(_is_dict_view_type(type({"a": 1})))


class TestIssue69R11PredicateEntersNoHostHooks(unittest.TestCase):
    """判定与物化都必须零宿主钩子（r11 的零钩子承诺）。"""

    def setUp(self) -> None:
        HOOKS.clear()

    def test_shadowed_dict_property_is_never_entered(self):
        """r10 的 ``instance_carries_own_state`` **会**进入遮蔽的 ``__dict__`` property。"""
        value = HookedHidingDict({"a": 1})
        self.assertEqual("shadowed __dict__", instance_state_signal(value))
        self.assertTrue(reads_mapping_outside_dict_storage(value))
        self.assertEqual([], HOOKS, HOOKS)

    def test_every_container_hook_stays_uncalled(self):
        value = HookedStateDict({"a": 1})
        self.assertEqual("instance state (__slots__)", instance_state_signal(value))
        self.assertTrue(reads_mapping_outside_dict_storage(value))
        self.assertEqual(
            ("__getitem__", "__iter__", "__len__", "items", "keys", "values"),
            dict_storage_protocol_overrides(HookedStateDict))
        self.assertEqual([], HOOKS, HOOKS)

    def test_refusal_paths_do_not_call_host_methods(self):
        for cls in (SlotsItemsLiar, SlotsItemsKeysValuesLiar, SlotsGetItemLiar):
            with self.subTest(kind=cls.__name__):
                HOOKS.clear()
                value = cls({"a": 1})
                with self.assertRaises(_EngineCopyRefused):
                    _engine_copy({"m": value})
                with self.assertRaises(StateSnapshotError):
                    _snapshot_value({"m": value})
                self.assertEqual([], HOOKS, HOOKS)


class TestIssue69R11NegativeControlsAndOverRefusalCost(unittest.TestCase):
    """负控不变 + 过拒代价明说（r11 把代价从 ``__dict__`` 扩到 ``__slots__``）。"""

    def setUp(self) -> None:
        HOOKS.clear()

    def test_negative_controls_are_all_three_clauses_false(self):
        cases = (OrderedDict(a=1), defaultdict(int, {"a": 1}), Counter({"a": 1}),
                 PlainDict(a=1), {"a": 1}, EmptySlotsDict(a=1),
                 WeakrefOnlySlotsDict(a=1), DictSlotPlaceholderDict(a=1),
                 MissingLiar({"a": 1}))
        for value in cases:
            with self.subTest(kind=type(value).__name__):
                self.assertIsNone(instance_state_signal(value))
                self.assertFalse(reads_mapping_outside_dict_storage(value))

    def test_instance_state_costs_an_over_refusal_and_that_is_declared(self):
        """过拒代价：数据在真实存储里，仍被具名拒绝（``__dict__`` 与 ``__slots__`` 两形状）。"""
        for shape, signal in ((AttrDict(a=1), "instance state (__dict__)"),
                              (SlotsCacheDict(a=1), "instance state (__slots__)")):
            with self.subTest(kind=type(shape).__name__):
                self.assertEqual({"a": 1}, dict(shape), "数据确实在真实存储里")
                self.assertEqual(signal, instance_state_signal(shape))
                self.assertTrue(reads_mapping_outside_dict_storage(shape))
                with self.assertRaises(_EngineCopyRefused) as cm:
                    _engine_copy(shape)
                self.assertIn("instance state", str(cm.exception))
                self.assertIn(signal, str(cm.exception))
                self.assertEqual({"a": 1}, _engine_copy(dict(shape)), "展平后照常通过")

    def test_benign_items_override_is_still_normalised(self):
        """r7/r8 契约（``HookDict``/``EvilDict``：只覆写 ``items`` 且无额外状态）不变。"""
        class HookDictBenign(dict):
            def items(self):
                HOOKS.append("HookDictBenign.items")
                return dict.items(self)

        value = HookDictBenign({"a": 1})
        self.assertIsNone(instance_state_signal(value))
        self.assertFalse(reads_mapping_outside_dict_storage(value))
        copied = _engine_copy(value)
        self.assertIs(type(copied), dict)
        self.assertEqual({"a": 1}, copied)
        self.assertEqual([], HOOKS, HOOKS)


class TestIssue69R11DictEscapeHatchIsLossy(unittest.TestCase):
    """``dict(...)`` 展平**读的是真实存储** ⇒ 对"数据在别处"的形状静默给错答案。"""

    def test_dict_of_a_refused_shape_is_empty(self):
        for cls in (SlotsItemsLiar, SlotsItemsKeysValuesLiar, SlotsGetItemLiar,
                    HidingDict, SlotsStringLiar, InheritedSlotsLiar):
            with self.subTest(kind=cls.__name__):
                value = cls({"a": 1})
                self.assertEqual({}, dict(value),
                                 "dict(...) 读真实存储 ⇒ 对丢数据形状返回 {}")
                self.assertEqual({}, dict(dict.items(value)))

    def test_dict_items_of_a_refused_shape_trusts_the_host(self):
        """``dict(v.items())`` 会**调用宿主 ``items()``**：拿到的不是引擎的读数。"""
        self.assertEqual({"a": 1}, dict(SlotsItemsLiar({"a": 1}).items()))
        self.assertEqual({}, dict(SlotsGetItemLiar({"a": 1})),
                         "没有 items 覆写 ⇒ 连自述都拿不到，dict(...) 仍是空")

    def test_dict_of_the_over_refused_shape_is_correct(self):
        """对照：过拒形状（数据在真实存储里）的 ``dict(...)`` 是**正确**的。"""
        self.assertEqual({"a": 1}, dict(AttrDict(a=1)))
        self.assertEqual({"a": 1}, dict(SlotsCacheDict(a=1)))

    def test_the_note_is_present_next_to_the_dict_recommendation(self):
        """两处 ``dict(...)`` 建议旁边必须写明它读真实存储、并给诚实的 recovery。"""
        root = pathlib.Path(__file__).resolve().parents[2]
        required = ("dict(...)", "real dict storage", "ctx.evaluate")
        for relpath in ("docs-site/docs/guide/expressions.md", "MIGRATION.md",
                        "docs/issue69-mutation-matrix.md"):
            with self.subTest(doc=relpath):
                text = (root / relpath).read_text(encoding="utf-8")
                windows = []
                at = text.find("dict(...)")
                while at != -1:
                    windows.append(text[max(0, at - 400):at + 1600])
                    at = text.find("dict(...)", at + 1)
                self.assertTrue(windows, f"{relpath}: 找不到 dict(...) 建议")
                self.assertTrue(
                    any(all(token in window for token in required) for window in windows),
                    f"{relpath}: 没有一处 dict(...) 建议带着完整警告（需要 {required}）")


class TestIssue69R11RaisingGetattributeIsNamed(unittest.TestCase):
    """finding 5：``isinstance`` 回落读 ``__class__`` ⇒ 宿主钩子异常必须具名。"""

    def test_engine_copy_names_the_raising_getattribute(self):
        with self.assertRaises(_EngineCopyRefused) as cm:
            _engine_copy(RaisingGetattribute())
        self.assertEqual("RaisingGetattribute", cm.exception.type_name)
        self.assertIn("__getattribute__", str(cm.exception))
        self.assertIn("isinstance", str(cm.exception))

    def test_canonical_kind_names_the_raising_getattribute(self):
        with self.assertRaises(_EngineCopyRefused) as cm:
            _canonical_persistable_kind(RaisingGetattribute())
        self.assertEqual("RaisingGetattribute", cm.exception.type_name)
        self.assertIn("RaisingGetattribute", str(cm.exception))

    def test_nested_and_snapshot_paths_name_it_too(self):
        with self.assertRaises(_EngineCopyRefused) as cm:
            _engine_copy({"m": RaisingGetattribute()})
        self.assertEqual("RaisingGetattribute", cm.exception.type_name)
        with self.assertRaises(StateSnapshotError) as cm:
            _snapshot_value(RaisingGetattribute())
        self.assertIn("RaisingGetattribute", cm.exception.message)
        self.assertIn("__getattribute__", cm.exception.message)
        with self.assertRaises(StateSnapshotError) as cm:
            _snapshot_value({"m": RaisingGetattribute()})
        self.assertIn("RaisingGetattribute", cm.exception.message)

    def test_dsl_loop_condition_refusal_is_named_and_writes_nothing(self):
        payload = {"m": RaisingGetattribute(), "cfg": {"a": 1}}
        raised, detail = _run_loop(payload, "$F.set($INPUT.cfg,'k','v')")
        self.assertIsNotNone(raised, "宿主钩子异常必须变成具名拒绝，不能报成功")
        self.assertIn("plaita#69", detail)
        self.assertEqual({"a": 1}, payload["cfg"])

    def test_honest_scalars_and_containers_are_unaffected(self):
        """负控：没有抛异常的宿主钩子时，判据逐字不变。"""
        self.assertIs(dict, _canonical_persistable_kind({"a": 1}))
        self.assertIs(list, _canonical_persistable_kind([1]))
        self.assertIs(str, _canonical_persistable_kind("x"))
        self.assertIs(dict, _canonical_persistable_kind(OrderedDict(a=1)))


class TestIssue69R11ViewMaterialisationIsNarrowed(unittest.TestCase):
    """finding 6：``_detach_alias`` 的视图分支只物化 C 槽视图，其余具名拒绝。"""

    def setUp(self) -> None:
        HOOKS.clear()

    def test_exact_engine_views_are_still_materialised(self):
        for view, expected in (({"a": 1}.keys(), ["a"]),
                               ({"a": 1}.values(), [1]),
                               ({"a": 1}.items(), [("a", 1)]),
                               (OrderedDict(a=1).keys(), ["a"]),
                               (OrderedDict(a=1).items(), [("a", 1)])):
            with self.subTest(kind=type(view).__name__):
                self.assertTrue(_is_dict_view_type(type(view)))
                self.assertTrue(_dict_view_is_materialisable(type(view)))
                detached = _detach_alias(view, "set", ("set", 1, "$VAR.k", "$VAR"))
                self.assertEqual(expected, detached)
        self.assertEqual([], HOOKS, HOOKS)

    def test_host_view_subclass_is_named_refused_with_zero_hooks(self):
        value = EvilKeysView({"a": 1})
        self.assertTrue(_is_dict_view_type(type(value)))
        self.assertFalse(_dict_view_is_materialisable(type(value)))
        with self.assertRaises(ValueError) as cm:
            _detach_alias(value, "set", ("set", 1, "$VAR.k", "$VAR"))
        self.assertIn("dict view", str(cm.exception))
        self.assertIn("__iter__", str(cm.exception))
        self.assertIn("plaita#69 r11", str(cm.exception))
        self.assertEqual([], HOOKS, "宿主的 __iter__/__len__ 一次都不能进")

    def test_abc_views_are_refused_even_without_an_override(self):
        for view in (KeysView({"a": 1}), ValuesView({"a": 1}), ItemsView({"a": 1})):
            with self.subTest(kind=type(view).__name__):
                self.assertFalse(_dict_view_is_materialisable(type(view)))
                with self.assertRaises(ValueError):
                    _detach_alias(view, "set", ("set", 1, "$VAR.k", "$VAR"))
        self.assertEqual([], HOOKS, HOOKS)

    def test_dsl_keys_never_calls_the_host_view_factory(self):
        """DSL 面（r13 加强）：``$F.keys`` 读**真实存储**，宿主的 ``keys()`` 零调用。

        r11/r12 的 ``$F.keys`` 会调用参数自己的 ``a.keys()``，于是宿主视图能到达
        mutator 值参并被物化闸门具名拒绝（本用例原名
        ``test_dsl_value_argument_from_a_host_view_is_named_refused``）。r13 把这条
        **通道本身关掉**：``$F.keys`` 对 ``dict`` 只走 ``dict.keys(v)``（基类 C
        描述符），宿主的 ``EvilKeysView`` 根本不会被造出来 ⇒ 结果是真实存储 ``['a']``、
        零钩子、无拒绝。视图物化闸门本身仍由本类的 ``_detach_alias`` 直调用例钉住。
        """
        from tests.unit.test_issue69_mutation_gate import _alias_context

        with freshness_scope():
            ctx = _alias_context()
            ctx.set_state("$INPUT", {"d": ViewReturningDict({"a": 1})})
            ctx.set_state("$VAR", ctx.evaluate({}))
            self.assertIsNone(ctx.evaluate("$F.set($VAR,'k',$F.keys($INPUT.d))"))
        self.assertEqual({"k": ["a"]}, ctx.get_state("$VAR"),
                         "真实存储的键，不是宿主自述的视图")
        self.assertEqual([], HOOKS, "宿主的 keys() 一次都不能进")

    def test_dsl_value_argument_from_a_plain_dict_view_still_works(self):
        from tests.unit.test_issue69_mutation_gate import _alias_context

        with freshness_scope():
            ctx = _alias_context()
            ctx.set_state("$INPUT", {"d": {"a": 1}})
            ctx.set_state("$VAR", ctx.evaluate({}))
            self.assertIsNone(ctx.evaluate("$F.set($VAR,'k',$F.keys($INPUT.d))"))
        self.assertEqual({"k": ["a"]}, ctx.get_state("$VAR"))
        self.assertEqual([], HOOKS)

    def test_the_view_residue_is_documented(self):
        root = pathlib.Path(__file__).resolve().parents[2]
        module = (root / "plaita/core/expression_parser.py").read_text(encoding="utf-8")
        self.assertIn("_dict_view_is_materialisable", module)
        self.assertIn("view.__iter__", module)
        for relpath in ("MIGRATION.md",):
            text = (root / relpath).read_text(encoding="utf-8")
            with self.subTest(doc=relpath):
                self.assertIn("_dict_view_is_materialisable", text)
                self.assertIn("view.__iter__", text)


class TestIssue69R11PerformanceAuthorityIsCurrentRevision(unittest.TestCase):
    """finding 7：§P 的"权威读数"只能指当前 revision（r11），历史值必须标 revision。"""

    DOCS = ("docs/issue69-mutation-matrix.md", "MIGRATION.md")

    def test_no_site_calls_r9_the_authority_any_more(self):
        root = pathlib.Path(__file__).resolve().parents[2]
        banned = ("权威 = r9", "权威读数 = r9", "权威读数是 r9", "权威 = **r9")
        for relpath in self.DOCS:
            text = (root / relpath).read_text(encoding="utf-8")
            with self.subTest(doc=relpath):
                for needle in banned:
                    self.assertNotIn(needle, text)

    def test_every_authority_line_with_a_reading_names_the_current_revision(self):
        """带 µs 的"权威"行必须指向 r11；写"历史值/不称权威"的行才算免责。"""
        root = pathlib.Path(__file__).resolve().parents[2]
        disclaimer = ("历史", "不称权威", "不得称权威")
        for relpath in self.DOCS:
            text = (root / relpath).read_text(encoding="utf-8")
            with self.subTest(doc=relpath):
                hits = 0
                for line in text.splitlines():
                    if "权威" in line and "µs" in line:
                        hits += 1
                        if any(token in line for token in disclaimer):
                            continue
                        self.assertIn("r11", line,
                                      f"{relpath}: 权威读数行未指向 r11: {line.strip()[:120]}")
                self.assertGreater(hits, 0, f"{relpath}: 找不到任何权威读数行")

    def test_the_unverifiable_whole_table_lift_claim_is_gone(self):
        root = pathlib.Path(__file__).resolve().parents[2]
        for relpath in self.DOCS:
            text = (root / relpath).read_text(encoding="utf-8")
            with self.subTest(doc=relpath):
                self.assertNotIn("~7–10%", text)
                self.assertNotIn("同向抬升 ~7", text)

    def test_the_cross_machine_caveat_is_present(self):
        root = pathlib.Path(__file__).resolve().parents[2]
        for relpath in self.DOCS:
            text = (root / relpath).read_text(encoding="utf-8")
            with self.subTest(doc=relpath):
                self.assertIn("跨机", text)


if __name__ == "__main__":
    unittest.main()
