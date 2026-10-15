"""plaita#69 **r12** —— clause (iii) 的第 5 条（C member/getset 描述符）+ 视图物化的
**真实布局**闸门 + 变异 JSON 契约 + off-object 残余的明说。

r11 的独立验收判 **FAIL**，给出的仍是**规则级**反例：clause (iii) 测的是**代理**（四条
*声明*形态：实例 ``__dict__`` / ``__slots__`` / 遮蔽 ``__dict__`` / 序列化钩子），不是它
宣称的**性质**（"对象可能在真实 ``dict`` 存储之外持有状态"）。这次通道是 **C 级成员槽**：

    class N_DDLiar(defaultdict):
        def __init__(self, d):
            super().__init__(None)
            self.default_factory = dict(d)     # member_descriptor 的 C 槽
        def items(self): return self.default_factory.items()

``defaultdict.default_factory`` 在 ``defaultdict.__dict__`` 里是 ``member_descriptor``
（CPython 3.9.6 / 3.10.21 / 3.11.16 / 3.12.14 / 3.13.15 全部实测），声明层 ``defaultdict``
**先于** ``dict``。r11 实测：``dict.items(v) == {}``（真实存储是空的）、
``v.items() == {'a': 1}``（宿主自述有内容）、
``object.__getattribute__(v,'__dict__') == {}``、``instance_state_signal(v) is None``、
``reads_mapping_outside_dict_storage(v) is False`` ⇒ ``_engine_copy({'m': v}) == {'m': {}}``、
``_snapshot_value({'m': v}) == {'m': {}}``、DSL ``$F.set($INPUT.cfg,'k','v')`` →
**``raised=None``（报成功）**，``cfg`` 未变。同族 ``N2_DDGetItem``（只覆写
``__getitem__``，``v['a'] == 1`` 而真实存储是 ``{}``）。r11 的引理
（``expression_parser.py`` 的 ``declared_instance_slots`` docstring："无槽 ⇒ 状态只能在实例
``__dict__`` 或真实 ``dict`` 存储里"）**是假的**。判据既然是机械可判的
（``types.MemberDescriptorType`` 已在 ``_C_SLOT_DESCRIPTOR_TYPES`` 里，判据本来就逐层走
``_class_namespace``），就该按**性质**关掉它。

本文件钉五件事：

1. **clause (iii) 第 5 条**（:class:`TestIssue69R12CStorageDescriptorIsTheProperty`）：
   MRO 里 ``dict`` 之前、**非 ``cls`` 自身**的层声明的 C member/getset 描述符
   （``declared_c_storage_descriptors``）⇒ 两条拷贝路径 + DSL 具名拒绝、零写穿；信号名
   ``instance state (C descriptor <名> on <层>)``。**负控**：精确
   ``defaultdict``/``OrderedDict``/``Counter``（声明层就是 ``cls`` 自己，或只有被排除的
   ``__dict__``/``__weakref__``）与纯 ``dict`` 子类照常归一化 —— 这正是"只在声明层
   ≠ ``cls`` 时拒绝"的机械后果。**声明的过拒面**：无数据在存储之外的良性 ``defaultdict``
   子类（``class BenignDD(defaultdict): pass``）也被拒，与 ``AttrDict``/``SlotsCacheDict``
   的过拒注记同级明说。
2. **零宿主钩子**：第 5 条只读类对象（``_real_mro``/``_class_namespace``），不调用宿主
   ``__getattribute__``/``items``/``__iter__``/``__len__``。
3. **视图物化的真实布局闸门**（:class:`TestIssue69R12ViewMaterialisationNeedsTheRealLayout`）：
   r11 只检查"解析到的实现是 C 描述符**类型**"，不看它**属不属于该对象的布局** ⇒ 两条
   旁路：``class V1(KeysView): __iter__ = dict.__iter__`` 过闸后 ``list(v1)`` 抛**裸**
   ``TypeError``（消息里没有 ``plaita#69``）；``KeysView.register(LieList)`` 让一个普通
   ``list`` 子类被当视图物化（实测拿到 ``['LIE']`` —— ABC 注册是宿主可控的），更尖锐的
   ``KeysView.register(LieBytes)`` 把 ``b'LIE'`` 物化成 ``[76, 73, 69]``（改变值种类）。
   r12：物化只允许**精确类型**是 :data:`_ENGINE_VIEW_TYPES` 的引擎自产视图，且
   ``list(value)`` 包在同一具名 fail-closed 包装里。
4. **变异 JSON 契约**（:class:`TestIssue69R12MutationJsonContract`）：每个
   ``scripts/mutation_replay/mutations/*.json`` 必须带重放器读的全部键（缺 ``issue`` 时
   ``replay.py --list`` 直接 ``KeyError``）且 ``old`` 在目标文件里**恰好出现 1 次**
   （锚点陈旧时重放器判"定义原文 0 次"）。
5. **off-object 残余明说**（:class:`TestIssue69R12OffObjectResidualIsDeclared`）：数据放在
   **类属性** / **闭包** / **模块级旁表** / **property/数据描述符** 后面时，零钩子静态判定
   **看不见** ⇒ 在模块 docstring 与 `MIGRATION.md` 里按**设计之外**（out of scope）明说并
   给出理由；同时记录本机五个解释器**都可用**（3.9.6 / 3.10.21 / 3.11.16 / 3.12.14 /
   3.13.15）与 C 成员描述符面在这五版上的读数。
"""
from __future__ import annotations

import json
import pathlib
import types
import unittest
from collections import Counter, OrderedDict, defaultdict
from collections.abc import KeysView

from plaita.core.errors import StateSnapshotError
from plaita.core.expression_parser import (
    _ENGINE_VIEW_TYPES,
    _EngineCopyRefused,
    _detach_alias,
    _dict_view_is_materialisable,
    _engine_copy,
    _is_dict_view_type,
    _is_python_defined_class,
    _resolved_slot_implementation,
    declared_c_storage_descriptors,
    declared_instance_slots,
    dict_storage_protocol_overrides,
    declares_instance_state_hooks,
    freshness_scope,
    instance_state_signal,
    reads_mapping_outside_dict_storage,
    shadows_instance_dict,
)
from plaita.core.state import _snapshot_value
from tests.unit.test_issue69_r8_snapshot_discipline import _run_loop

ROOT = pathlib.Path(__file__).resolve().parents[2]

#: 宿主钩子计数器（跨测试清空）：任何一次调用都说明判定/物化落回了宿主代码。
HOOKS: list = []

#: r11 实测的阻塞反例（r12 的规则级反例）：数据全部在 ``default_factory`` 的 C 槽里。
C_DESCRIPTOR_SIGNAL = "instance state (C descriptor default_factory on defaultdict)"


# --------------------------------------------------------------------------- #
# 阻塞反例形状（r11 下两条路径都静默产出 {}，DSL 报成功）
# --------------------------------------------------------------------------- #
class CStorageItemsLiar(defaultdict):
    """r12 阻塞反例：``member_descriptor`` 存数据、Python 覆写 ``items()`` 自述。"""

    def __init__(self, d):
        super().__init__(None)
        self.default_factory = dict(d)

    def items(self):
        return self.default_factory.items()


class CStorageGetItemLiar(defaultdict):
    """同族变体：只覆写 ``__getitem__``（唯一自述口），``v['a'] == 1`` 而存储为空。"""

    def __init__(self, d):
        super().__init__(None)
        self.default_factory = dict(d)

    def __getitem__(self, key):
        return self.default_factory[key]


class CStorageSlotsLiar(defaultdict):
    """同族变体二：``__slots__ = ()``（走不到 ``__slots__`` 判据），仍靠 C 槽存数据。"""

    __slots__ = ()

    def __init__(self, d):
        super().__init__(None)
        self.default_factory = dict(d)

    def items(self):
        return self.default_factory.items()


class CStorageIntermediate(defaultdict):
    """中间 Python 层（自身不声明 C 槽），声明层仍是更远处的 ``defaultdict``。"""

    def __init__(self, d):
        super().__init__(None)
        self.default_factory = dict(d)

    def items(self):
        return self.default_factory.items()


class CStorageLeafLiar(CStorageIntermediate):
    """两层继承：C 描述符的声明层 ``defaultdict`` 离 ``cls`` 有两层。"""


class BenignDefaultDictSubclass(defaultdict):
    """**声明的过拒面**：数据全在真实存储里、``__dict__`` 空的良性 ``defaultdict`` 子类。"""


class HookedCStorageLiar(defaultdict):
    """带宿主钩子计数器的 C 槽形状：判定/拒绝过程一次都不许进宿主代码。"""

    def __init__(self, d):
        super().__init__(None)
        self.default_factory = dict(d)

    def items(self):
        HOOKS.append("items")
        return self.default_factory.items()

    def keys(self):
        HOOKS.append("keys")
        return self.default_factory.keys()

    def __iter__(self):
        HOOKS.append("__iter__")
        return iter(self.default_factory)

    def __len__(self):
        HOOKS.append("__len__")
        return len(self.default_factory)

    def __getattribute__(self, name):
        if name in ("items", "keys", "values", "__iter__", "__len__", "__dict__"):
            HOOKS.append("__getattribute__:" + name)
        return object.__getattribute__(self, name)


class PlainDictSubclass(dict):
    """纯 ``dict`` 子类（无额外存储）—— 负控。"""


class V1KeysView(KeysView):
    """r12 finding：解析到 ``dict`` 的 C 描述符、但**不属于**自己的布局。"""

    __iter__ = dict.__iter__
    __len__ = dict.__len__


class RegisteredLieList(list):
    """r12 finding：ABC 虚拟注册（宿主可控）让普通 ``list`` 子类"自称是视图"。"""

    def __iter__(self):
        HOOKS.append("RegisteredLieList.__iter__")
        return list.__iter__(self)

    def __len__(self):
        HOOKS.append("RegisteredLieList.__len__")
        return list.__len__(self)


class RegisteredPlainList(list):
    """ABC 注册 + **不覆写**任何名字 ⇒ ``list.__iter__``/``list.__len__`` 是 C 描述符。"""


class RegisteredLieBytes(bytes):
    """更尖锐：``bytes`` 子类被注册成"视图" ⇒ 物化会把 ``b'LIE'`` 变成 ``[76, 73, 69]``。"""


KeysView.register(RegisteredLieList)
KeysView.register(RegisteredPlainList)
KeysView.register(RegisteredLieBytes)


class RegisteredViewReturningDict(dict):
    """数据在真实存储里（可归一化），但 ``keys()`` 返回宿主登记的**假视图**。

    r12 实测（pre，`56b7678`）：`$F.set($VAR,'k',$F.keys($INPUT.d))` 会把它物化成
    ``['LIE']`` 写进 ``$VAR`` —— `RegisteredPlainList` 不覆写任何名字，`list.__iter__`
    是 C 描述符，于是 r11 的闸门放行。
    """

    def keys(self):
        return RegisteredPlainList(["LIE"])


class TestIssue69R12CStorageDescriptorIsTheProperty(unittest.TestCase):
    """clause (iii) 第 5 条：非 ``cls`` 层声明的 C 布局存储一律具名拒绝。"""

    def setUp(self) -> None:
        HOOKS.clear()

    # ---- 每条形状一个**非 subTest** 判决（变异重放器只认 FAILED/ERROR 行） ----

    def test_c_member_descriptor_liar_is_named_refused_on_both_copy_paths(self):
        value = CStorageItemsLiar({"a": 1})
        self.assertEqual({}, dict(dict.items(value)), "真实存储是空的")
        self.assertEqual({"a": 1}, dict(value.items()), "宿主自述有一个键")
        self.assertEqual({}, object.__getattribute__(value, "__dict__"),
                         "实例 __dict__ 也是空的（r11 的代理判据看不见）")
        self.assertEqual(C_DESCRIPTOR_SIGNAL, instance_state_signal(value))
        self.assertTrue(reads_mapping_outside_dict_storage(value))
        with self.assertRaises(_EngineCopyRefused) as cm:
            _engine_copy({"m": value})
        self.assertEqual("CStorageItemsLiar", cm.exception.type_name)
        self.assertIn(C_DESCRIPTOR_SIGNAL, str(cm.exception))
        with self.assertRaises(StateSnapshotError) as cm:
            _snapshot_value({"m": value})
        self.assertIn("CStorageItemsLiar", cm.exception.message)
        self.assertIn(C_DESCRIPTOR_SIGNAL, cm.exception.message)
        self.assertIn("$['m']", cm.exception.message)

    def test_c_member_descriptor_getitem_liar_is_named_refused_on_both_copy_paths(self):
        value = CStorageGetItemLiar({"a": 1})
        self.assertEqual({}, dict(dict.items(value)), "真实存储是空的")
        self.assertEqual(1, value["a"], "宿主自述有一个键")
        self.assertEqual(C_DESCRIPTOR_SIGNAL, instance_state_signal(value))
        with self.assertRaises(_EngineCopyRefused) as cm:
            _engine_copy({"m": value})
        self.assertEqual("CStorageGetItemLiar", cm.exception.type_name)
        self.assertIn("__getitem__", str(cm.exception))          # clause ii 的自述口
        self.assertIn(C_DESCRIPTOR_SIGNAL, str(cm.exception))    # clause iii 第 5 条
        with self.assertRaises(StateSnapshotError) as cm:
            _snapshot_value({"m": value})
        self.assertIn(C_DESCRIPTOR_SIGNAL, cm.exception.message)

    def test_every_c_descriptor_variant_is_refused_on_both_paths(self):
        """表驱动：``__slots__ = ()`` 变体、中间层、两层继承的同族形状。"""
        for cls in (CStorageSlotsLiar, CStorageLeafLiar, CStorageIntermediate):
            with self.subTest(kind=cls.__name__):
                value = cls({"a": 1})
                self.assertEqual({}, dict(dict.items(value)), "真实存储是空的")
                self.assertEqual(C_DESCRIPTOR_SIGNAL, instance_state_signal(value))
                self.assertTrue(reads_mapping_outside_dict_storage(value))
                with self.assertRaises(_EngineCopyRefused) as cm:
                    _engine_copy({"m": value})
                self.assertEqual(cls.__name__, cm.exception.type_name)
                self.assertIn(C_DESCRIPTOR_SIGNAL, str(cm.exception))
                with self.assertRaises(StateSnapshotError) as cm:
                    _snapshot_value({"m": value})
                self.assertIn(cls.__name__, cm.exception.message)

    def test_dsl_loop_condition_refusal_is_named_and_writes_nothing(self):
        """用户可见面：r11 这里**报成功**（副本静默变 ``{}``），r12 具名拒绝。"""
        for cls in (CStorageItemsLiar, CStorageGetItemLiar, CStorageSlotsLiar,
                    CStorageLeafLiar):
            with self.subTest(kind=cls.__name__):
                payload = {"m": cls({"a": 1}), "cfg": {"a": 1}}
                raised, detail = _run_loop(payload, "$F.set($INPUT.cfg,'k','v')")
                self.assertIsNotNone(raised, "不可保真拷贝 ⇒ 必须拒绝，不能报成功")
                self.assertIn("plaita#69", detail)
                self.assertNotIn("TypeError", detail)
                self.assertEqual({"a": 1}, payload["cfg"])      # 零写穿

    def test_white_box_predicate_is_mechanically_discriminating(self):
        """白盒：第 5 条只按"声明层 ≠ ``cls`` 且是 C member/getset"命中（避免恒真）。"""
        self.assertEqual((("default_factory", "defaultdict"),),
                         declared_c_storage_descriptors(CStorageItemsLiar))
        self.assertEqual((("default_factory", "defaultdict"),),
                         declared_c_storage_descriptors(CStorageLeafLiar))
        for exact in (defaultdict, OrderedDict, Counter, dict, PlainDictSubclass):
            with self.subTest(kind=exact.__name__):
                self.assertEqual((), declared_c_storage_descriptors(exact),
                                 "声明层就是 cls 自己（或只有被排除的占位名）")
        # 声明层就是 ``cls`` 自己的 C 类型：``defaultdict.default_factory`` 是
        # ``member_descriptor``，但它由 ``defaultdict`` 自己的 ``tp_members`` 生成。
        self.assertIs(types.MemberDescriptorType,
                      type(defaultdict.__dict__["default_factory"]))
        self.assertIs(defaultdict, CStorageItemsLiar.__mro__[1],
                      "CStorageItemsLiar 的声明层是 defaultdict（≠ cls）")
        # 前四条对阻塞形状全体不命中（这正是 r11 漏掉的原因）。
        for value in (CStorageItemsLiar({"a": 1}), CStorageGetItemLiar({"a": 1})):
            with self.subTest(kind=type(value).__name__):
                self.assertFalse(shadows_instance_dict(type(value)))
                self.assertEqual((), declared_instance_slots(type(value)))
                self.assertEqual((), declares_instance_state_hooks(type(value)))

    def test_negative_controls_still_normalise(self):
        """负控：精确 ``defaultdict``/``OrderedDict``/``Counter`` + 纯子类逐字不变。"""
        cases = (defaultdict(int, {"a": 1}), OrderedDict(a=1), Counter({"a": 1}),
                 PlainDictSubclass(a=1), {"a": 1})
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

    def test_benign_defaultdict_subclass_is_the_declared_over_refusal(self):
        """过拒面（明说）：良性 ``defaultdict`` 子类数据在真实存储里，仍被拒。"""
        value = BenignDefaultDictSubclass(int, {"a": 1})
        self.assertEqual({"a": 1}, dict(dict.items(value)), "数据确实在真实存储里")
        self.assertIsNone(instance_state_signal(defaultdict(int, {"a": 1})),
                          "精确 defaultdict 不受影响")
        self.assertEqual(C_DESCRIPTOR_SIGNAL, instance_state_signal(value))
        self.assertTrue(reads_mapping_outside_dict_storage(value))
        with self.assertRaises(_EngineCopyRefused) as cm:
            _engine_copy(value)
        self.assertIn(C_DESCRIPTOR_SIGNAL, str(cm.exception))
        with self.assertRaises(StateSnapshotError):
            _snapshot_value({"m": value})
        self.assertEqual({"a": 1}, _engine_copy(dict(dict.items(value))),
                         "显式展平（读真实存储）后照常通过")

    def test_the_declaring_layer_being_cls_itself_is_exempt(self):
        """**非 subTest** 判决：扩展类型**自己** ``tp_dict`` 里的槽不构成额外状态。

        ``defaultdict.default_factory`` 是 ``member_descriptor``，但它由 ``defaultdict``
        自己的 ``tp_members`` 生成（``declared_c_storage_descriptors(defaultdict) == ()``）；
        精确 ``OrderedDict``/``Counter`` 同理（只有被排除的 ``__dict__``/``__weakref__``）。

        **r13 重述**：这条豁免不再靠"声明层是 `cls` 就跳过"（那个口径被 `HiddenSlotsLiar`
        反例推翻，见 :mod:`tests.unit.test_issue69_r13_layout_and_real_storage`），而是靠
        "声明层 `is cls` 且 `cls` **不是 Python 定义的类**"
        （:func:`_is_python_defined_class` = heap ∧ ¬immutable）。因此杀死本判决的是
        r12 的 `69-r12-c-storage-cls-layer-not-exempt`（去掉整条豁免）；r13 的
        `69-r13-hidden-slots-layout-rule-off`（退回"声明层是 `cls` 就跳过"）**不**杀它
        —— 精确扩展类型在两种口径下都豁免，由 r13 自己的布局判决杀死。
        """
        for exact in (defaultdict, OrderedDict):
            with self.subTest(kind=exact.__name__):
                self.assertFalse(_is_python_defined_class(exact), "扩展类型不是 Python 类")
                self.assertEqual((), declared_c_storage_descriptors(exact))
        self.assertEqual((), declared_c_storage_descriptors(Counter))
        self.assertIsNone(instance_state_signal(defaultdict(int, {"a": 1})))
        self.assertEqual({"a": 1}, _engine_copy(defaultdict(int, {"a": 1})))
        self.assertEqual({"a": 1},
                         _snapshot_value({"m": defaultdict(int, {"a": 1})})["m"])

    def test_benign_ordereddict_and_counter_subclasses_stay_normalised(self):
        """实测细化：这两个的**非 ``cls`` 层**只声明被排除的 ``__dict__``/``__weakref__``。"""
        class BenignOD(OrderedDict):
            pass

        class BenignCounter(Counter):
            pass

        for value in (BenignOD(a=1), BenignCounter({"a": 1})):
            with self.subTest(kind=type(value).__name__):
                self.assertEqual((), declared_c_storage_descriptors(type(value)))
                self.assertIsNone(instance_state_signal(value))
                self.assertEqual({"a": 1}, _engine_copy(value))
                self.assertEqual({"a": 1}, _snapshot_value({"m": value})["m"])

    def test_the_c_descriptor_path_enters_no_host_hooks(self):
        """零宿主钩子：判定与两条拒绝路径都不调用宿主方法/``__getattribute__``。"""
        value = HookedCStorageLiar({"a": 1})
        self.assertEqual(C_DESCRIPTOR_SIGNAL, instance_state_signal(value))
        self.assertEqual([], HOOKS, HOOKS)
        self.assertTrue(reads_mapping_outside_dict_storage(value))
        self.assertEqual([], HOOKS, HOOKS)
        with self.assertRaises(_EngineCopyRefused):
            _engine_copy({"m": value})
        with self.assertRaises(StateSnapshotError):
            _snapshot_value({"m": value})
        self.assertEqual([], HOOKS, HOOKS)

    def test_the_falsified_lemma_is_gone_from_the_module(self):
        """r11 的假引理（"无槽 ⇒ 状态只能在实例 __dict__ 或真实存储里"）必须被改写。"""
        module = (ROOT / "plaita/core/expression_parser.py").read_text(encoding="utf-8")
        self.assertNotIn("无槽 ⇒ 状态只能在实例", module)
        self.assertIn("declared_c_storage_descriptors", module)
        self.assertIn("C member/getset 描述符", module)

    def test_the_over_refusal_is_declared_next_to_the_existing_note(self):
        """过拒代价注记必须与 ``AttrDict``/``SlotsCacheDict`` 同级出现（模块 + 文档）。"""
        module = (ROOT / "plaita/core/expression_parser.py").read_text(encoding="utf-8")
        self.assertIn("BenignDD", module)
        self.assertIn("r12 起再扩一面", module)
        self.assertIn("FreezableDefaultDict", module,
                      "r13：把 verifier 找到的 stdlib 真实例子写进过拒注记")
        migration = (ROOT / "MIGRATION.md").read_text(encoding="utf-8")
        self.assertIn("BenignDD", migration)
        self.assertIn("FreezableDefaultDict", migration)


class TestIssue69R12ViewMaterialisationNeedsTheRealLayout(unittest.TestCase):
    """r12 finding：物化闸门必须看**真实布局**，不能只看"解析到 C 描述符类型"。"""

    def setUp(self) -> None:
        HOOKS.clear()

    def test_resolved_c_descriptor_from_another_layout_is_named_refused(self):
        """``V1(KeysView)`` 的 ``__iter__`` 是 ``dict`` 的 C 描述符 ⇒ r11 过闸后裸 ``TypeError``。"""
        self.assertTrue(_is_dict_view_type(V1KeysView))
        self.assertEqual("c", _resolved_slot_implementation(V1KeysView, "__iter__"))
        self.assertEqual("c", _resolved_slot_implementation(V1KeysView, "__len__"))
        self.assertFalse(_dict_view_is_materialisable(V1KeysView),
                         "C 描述符类型 ≠ 属于该对象布局")
        value = V1KeysView({"a": 1})
        with self.assertRaises(ValueError) as cm:
            _detach_alias(value, "set", ("set", 1, "$VAR.k", "$VAR"))
        message = str(cm.exception)
        self.assertIn("plaita#69", message)                 # 具名，不是裸 TypeError
        self.assertNotIn("requires a 'dict' object", message)
        self.assertIn("dict view", message)
        self.assertIn("__iter__", message)
        self.assertEqual([], HOOKS, HOOKS)

    def test_abc_registered_non_view_type_is_named_refused(self):
        """``KeysView.register(LieList)`` 让普通 ``list`` 子类"自称视图"（ABC 注册宿主可控）。"""
        self.assertTrue(_is_dict_view_type(RegisteredPlainList))
        self.assertNotIn(RegisteredPlainList, _ENGINE_VIEW_TYPES)
        self.assertFalse(_dict_view_is_materialisable(RegisteredPlainList))
        value = RegisteredPlainList(["LIE"])
        self.assertEqual(["LIE"], _engine_copy(value), "它本来就是数据模型里的 list")
        with self.assertRaises(ValueError) as cm:
            _detach_alias(value, "set", ("set", 1, "$VAR.k", "$VAR"))
        message = str(cm.exception)
        self.assertIn("plaita#69", message)
        self.assertIn("dict view", message)
        self.assertEqual([], HOOKS, "宿主登记的类型一个钩子都不许进")

    def test_abc_registered_bytes_subclass_cannot_change_the_value_kind(self):
        """尖锐面：r11 会把注册成"视图"的 ``bytes`` 子类物化成 ``[76, 73, 69]``。"""
        value = RegisteredLieBytes(b"LIE")
        self.assertEqual(b"LIE", _engine_copy(value), "数据模型口径：bytes 子类按值归一化")
        self.assertTrue(_is_dict_view_type(RegisteredLieBytes))
        self.assertFalse(_dict_view_is_materialisable(RegisteredLieBytes))
        with self.assertRaises(ValueError) as cm:
            _detach_alias(value, "set", ("set", 1, "$VAR.k", "$VAR"))
        message = str(cm.exception)
        self.assertIn("plaita#69", message)
        self.assertIn("RegisteredLieBytes", message)
        self.assertEqual([], HOOKS)

    def test_only_exact_engine_view_types_are_materialisable(self):
        """引擎自产视图照常物化；ABC 视图 / 宿主子类 / 登记类型一律不物化。"""
        engine_cases = ((dict(a=1).keys(), ["a"]),
                        (dict(a=1).values(), [1]),
                        (dict(a=1).items(), [("a", 1)]),
                        (OrderedDict(a=1).keys(), ["a"]),
                        (OrderedDict(a=1).values(), [1]),
                        (OrderedDict(a=1).items(), [("a", 1)]))
        for view, expected in engine_cases:
            with self.subTest(kind=type(view).__name__):
                self.assertIn(type(view), _ENGINE_VIEW_TYPES)
                self.assertTrue(_dict_view_is_materialisable(type(view)))
                self.assertEqual(
                    expected,
                    _detach_alias(view, "set", ("set", 1, "$VAR.k", "$VAR")))
        self.assertEqual([], HOOKS, HOOKS)
        for cls in (KeysView, V1KeysView, RegisteredLieList, RegisteredPlainList,
                    RegisteredLieBytes):
            with self.subTest(kind=cls.__name__):
                self.assertFalse(_dict_view_is_materialisable(cls))

    def test_engine_view_types_are_not_subclassable(self):
        """五版 CPython 实测：真视图类型不能做基类 ⇒ ABC 注册是唯一旁路。"""
        for view_type in _ENGINE_VIEW_TYPES:
            with self.subTest(kind=view_type.__name__):
                with self.assertRaises(TypeError):
                    type("HostSubclass", (view_type,), {})
        self.assertTrue(issubclass(RegisteredLieList, KeysView),
                        "ABC 虚拟注册确实让普通 list 子类自称视图")
        self.assertFalse(_dict_view_is_materialisable(RegisteredLieList))

    def test_engine_view_materialisation_failure_is_named(self):
        """物化那一跳**任何**失败都变成具名 ``plaita#69`` 拒绝（不是裸异常）。"""
        from plaita.core import expression_parser as module

        def exploding_list(value):
            raise TypeError("descriptor requires a 'dict' object")

        module.list = exploding_list                        # LOAD_GLOBAL 先看模块全局
        try:
            with self.assertRaises(ValueError) as cm:
                _detach_alias(dict(a=1).keys(), "set", ("set", 1, "$VAR.k", "$VAR"))
        finally:
            del module.list
        message = str(cm.exception)
        self.assertIn("plaita#69", message)
        self.assertIn("materialisation", message)
        self.assertNotIn("requires a 'dict' object", message)

    def test_dsl_keys_never_materialises_a_registered_abc_view(self):
        """DSL 面（r13 加强）：宿主登记的假视图**够不到** mutator 值参。

        r12 的这条用例原名 ``test_dsl_value_argument_from_a_registered_abc_view_is_named_refused``：
        r12 的 ``$F.keys`` 调用参数自己的 ``keys()``，于是 ``RegisteredPlainList(['LIE'])``
        到达物化闸门并被具名拒绝。r13 让 ``$F.keys`` 对 ``dict`` 只走 ``dict.keys(v)``
        ⇒ 宿主的 ``keys()`` **零调用**、假视图根本不会被造出来 ⇒ ``$VAR`` 拿到真实存储
        的键。物化闸门本身仍由 :class:`TestIssue69R12ViewMaterialisationNeedsTheRealLayout`
        的 ``_detach_alias`` 直调用例钉住。
        """
        from tests.unit.test_issue69_mutation_gate import _alias_context

        with freshness_scope():
            ctx = _alias_context()
            ctx.set_state("$INPUT", {"d": RegisteredViewReturningDict({"a": 1})})
            ctx.set_state("$VAR", ctx.evaluate({}))
            self.assertIsNone(ctx.evaluate("$F.set($VAR,'k',$F.keys($INPUT.d))"))
        self.assertEqual({"k": ["a"]}, ctx.get_state("$VAR"),
                         "真实存储的键；['LIE'] 的通道已被关闭")
        self.assertEqual([], HOOKS)

    def test_dsl_value_argument_from_a_plain_dict_view_still_works(self):
        """负控：引擎自产视图的 DSL 用法逐字不变。"""
        from tests.unit.test_issue69_mutation_gate import _alias_context

        with freshness_scope():
            ctx = _alias_context()
            ctx.set_state("$INPUT", {"d": {"a": 1}})
            ctx.set_state("$VAR", ctx.evaluate({}))
            self.assertIsNone(ctx.evaluate("$F.set($VAR,'k',$F.keys($INPUT.d))"))
        self.assertEqual({"k": ["a"]}, ctx.get_state("$VAR"))
        self.assertEqual([], HOOKS)


class TestIssue69R12MutationJsonContract(unittest.TestCase):
    """变异 JSON 必须带重放器读的全部键，且 ``old`` 锚点在目标文件里恰好出现 1 次。"""

    #: ``replay.py`` 读到的键：``--list`` 用 name/issue/concern/file/rationale，
    #: 重放用 file/old/new/expect_fail。
    REPLAYER_KEYS = ("name", "issue", "concern", "file", "rationale",
                     "old", "new", "expect_fail")

    def _specs(self):
        directory = ROOT / "scripts" / "mutation_replay" / "mutations"
        paths = sorted(directory.glob("*.json"))
        self.assertTrue(paths, f"{directory} 下没有变异定义")
        return [(path, json.loads(path.read_text(encoding="utf-8"))) for path in paths]

    def test_every_mutation_json_carries_the_keys_the_replayer_reads(self):
        """聚合判决（**非 subTest**）：缺键就让这条以普通 ``FAILED`` 行变红。"""
        problems = []
        for path, spec in self._specs():
            missing = [k for k in self.REPLAYER_KEYS if k not in spec]
            if missing:
                problems.append(f"{path.name}: 缺键 {missing}")
            # plaita#88：统一 store 下 mutations 目录是**多单元共享**的（88-* 也住这里），
            # `issue == "69"` 断言收窄到本单元自己的 69-*.json；键集与 expect_fail 检查
            # 保持全局（任何单元的变异都必须带齐 replayer 键、声明行不得为空）。
            if path.name.startswith("69-") and str(spec.get("issue")) != "69":
                problems.append(f"{path.name}: issue={spec.get('issue')!r}")
            if not spec.get("expect_fail"):
                problems.append(f"{path.name}: expect_fail 为空")
        self.assertEqual([], problems,
                         "变异 JSON 缺键 ⇒ replay.py --list 直接 KeyError")

    def test_every_mutation_anchor_appears_exactly_once(self):
        """聚合判决：锚点陈旧（0 次）或重复（>1 次）都是普通 ``FAILED`` 行。"""
        problems = []
        for path, spec in self._specs():
            target = ROOT / spec["file"]
            if not target.is_file():
                problems.append(f"{path.name}: 目标文件不存在 {spec['file']}")
                continue
            count = target.read_text(encoding="utf-8").count(spec["old"])
            if count != 1:
                problems.append(
                    f"{path.name}: old 在 {spec['file']} 出现 {count} 次（应为 1）")
        self.assertEqual([], problems)


class TestIssue69R12OffObjectResidualIsDeclared(unittest.TestCase):
    """off-object 家族（类属性/闭包/模块级旁表/数据描述符）按设计之外明说。"""

    FAMILY = ("class attribute", "closure", "module-level side table",
              "property/data descriptor")

    def test_module_docstring_declares_the_off_object_family(self):
        module = (ROOT / "plaita/core/expression_parser.py").read_text(encoding="utf-8")
        self.assertIn("Off-object state is out of scope by design", module)
        for token in self.FAMILY:
            with self.subTest(token=token):
                self.assertTrue(token in module, f"模块 docstring 缺 {token!r}")

    def test_migration_declares_the_off_object_family_and_the_reason(self):
        migration = (ROOT / "MIGRATION.md").read_text(encoding="utf-8")
        for token in self.FAMILY + ("out of scope", "不可判定", "零钩子"):
            with self.subTest(token=token):
                self.assertTrue(token in migration, f"MIGRATION.md 缺 {token!r}")

    #: docs-site 用中文族名（r13 补：旧 pin 只读模块与 MIGRATION.md，漏了用户文档页）。
    FAMILY_ZH = ("类属性", "闭包", "模块级旁表", "property 或数据")

    def test_docs_site_page_declares_the_off_object_family_and_the_corrected_reason(self):
        """r13：用户文档页也必须明说四族**与更正后的理由**（类属性族不是"不可见"）。"""
        page = (ROOT / "docs-site/docs/guide/expressions.md").read_text(encoding="utf-8")
        for token in self.FAMILY_ZH + ("设计之外", "零钩子", "不可判定",
                                       "常量/缓存", "r13 理由更正"):
            with self.subTest(token=token):
                self.assertTrue(token in page,
                                f"docs-site/docs/guide/expressions.md 缺 {token!r}")

    def test_the_class_attribute_reason_is_corrected_everywhere(self):
        """r13 finding：三处都要"更正后的理由"（类属性是"内容 vs 常量"不可判，不是"看不见"）。"""
        for relpath in ("plaita/core/expression_parser.py", "MIGRATION.md",
                        "docs-site/docs/guide/expressions.md"):
            text = (ROOT / relpath).read_text(encoding="utf-8")
            with self.subTest(doc=relpath):
                self.assertIn("常量", text,
                              "缺「内容 vs 常量/缓存」的更正理由")
                self.assertIn("类体写不出", text,
                              "被推翻的前提必须**被引用**出来（而不是留在原地当事实）")
                self.assertIn("假的", text, "被引用的前提必须显式标注为假")

    def test_the_five_local_interpreters_are_recorded(self):
        """可用性口径更正：五个解释器**全部**可用（r11 的注记说反了）。"""
        migration = (ROOT / "MIGRATION.md").read_text(encoding="utf-8")
        for token in ("3.9.6", "3.10.21", "3.11.16", "3.12.14", "3.13.15",
                      "五个解释器", "member_descriptor"):
            with self.subTest(token=token):
                self.assertTrue(token in migration, f"MIGRATION.md 缺 {token!r}")


if __name__ == "__main__":
    unittest.main()
