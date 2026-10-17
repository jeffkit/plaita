"""plaita#163 **同形残面收口**（W22-A）——遍历面上其余"宿主抛异常时裸抛"的探测站点。

``#163``（``fix/w14-a`` @ ``bd3954ea``）把**路径遍历的三处** ``isinstance`` 探测
具名化，并在交付的 RED-PROOF §4 登记了同一文件的其余同族站点。本文件钉住那次收口：

1. **属性探针具名化**（新增 :func:`plaita.core.expression_parser._probe_hasattr`，
   与 :func:`_probe_isinstance` 同一纪律）：宿主 ``__getattribute__`` 在
   ``hasattr`` 探测上抛非 ``AttributeError`` 时，原本漏**裸** ``RuntimeError``；现在
   是具名 ``ValueError``（含 ``plaita#69``、真实类名、点明探的是哪个属性；刻意
   **不回显**宿主文本）。
   * ``_get_attr``：``hasattr(obj, "__getitem__")`` / ``hasattr(obj, "get")`` /
     ``hasattr(obj, "__dict__")``
   * ``_container_has_root``：``isinstance(obj, dict)``（走 ``_probe_isinstance``）、
     ``hasattr(obj, "__contains__")`` / ``hasattr(obj, "__dict__")`` /
     ``hasattr(obj, root_key)``，以及 **§4 清单外补收**的
     ``getattr(obj, "get", None)``（走 ``_probe_getattr``）
   * ``_compile_function_call``：两处 tuple 型 ``isinstance(result, (dict, …))``
2. **负控**（:class:`TestSiblingProbeSemanticsUnchanged`）：普通 dict/str/自定义容器
   遍历、INPUT 缺键的静默 ``None``、嵌套串递归、数据文本原样返回、``$F.keys`` 读真实
   存储、``$F.*`` 的 freshness 登记 —— 逐字不变；``_probe_hasattr`` 与 ``hasattr``
   返回值逐字相同。
3. **未修站点**（:class:`TestReadSitesKeepTheirNativeContract`）：真实**读取**
   （``_get_attr`` 末段 ``getattr(obj, path, None)``、映射分支 ``_mapping_get`` →
   ``obj.get(path, None)``、下标段 ``obj[target]``、INPUT 缺键判据的
   ``target not in obj``）**保留现状**——那是宿主自己的操作错误，带原生异常类型
   契约；具名化会改写原生类型（且 ``obj[target]`` 的 ``__class__`` 半边已由
   ``#163`` 的 ``isinstance(attr, str)`` 探针覆盖）。

RED-PROOF（含逐站点前/后行为表）见 ``/tmp/w22-a-out/RED-PROOF.md``。
"""
from __future__ import annotations

import pathlib
import re
import unittest

import plaita.core.expression_parser as parser_mod
from plaita.core.expression import ExpressionRegistry, FunctionCategory
from plaita.core.expression_parser import (
    ExpressionParser,
    _CONTAINER_RESULT_KINDS,
    _container_has_root,
    _engine_mapping_view,
    _get_attr,
    _probe_getattr,
    _probe_hasattr,
    freshness_scope,
)

from tests.unit.test_issue69_mutation_gate import _alias_context

ROOT = pathlib.Path(__file__).resolve().parents[2]


# ---------------------------------------------------------------------------
# 探针宿主：刻意把"抛异常的那一跳"拆开，才能逐站点定位
# ---------------------------------------------------------------------------
class RefuseOn:
    """``__class__`` 正常回答；``__getattribute__`` 对列出的属性名抛 ``RuntimeError``。

    ``isinstance`` 因此**不会**炸（走不通 ``__class__`` 那一跳），异常只可能来自
    属性探针 —— 这正是 W22-A 要具名化的那一类。
    """

    def __init__(self, *names):
        object.__setattr__(self, "_refuse", frozenset(names))

    def __getattribute__(self, name):
        if name in object.__getattribute__(self, "_refuse"):
            raise RuntimeError(f"host refuses {name}")
        return object.__getattribute__(self, name)


class RefuseOnWithGetItem(RefuseOn):
    """同上，但有类级 ``__getitem__``（让 ``hasattr(obj, "__getitem__")`` 为真）。"""

    def __getitem__(self, target):
        return ("item", target)


class RefuseClass:
    """``__class__`` 抛 ``RuntimeError``（``#163`` 的形状；这里只用来对照）。"""

    def __getattribute__(self, name):
        if name == "__class__":
            raise RuntimeError("host refuses __class__")
        return object.__getattribute__(self, name)


class SubscriptHost:
    """``__class__`` 正常、``__getitem__``（类级槽）抛 ``RuntimeError``。"""

    def __getitem__(self, target):
        raise RuntimeError("host __getitem__ raises")


class KeyErrorHost:
    """``__class__`` 正常、``__getitem__`` 抛 **原生** ``KeyError``。"""

    def __getitem__(self, target):
        raise KeyError(target)


class ContainsRaisingDict(dict):
    """真 dict 子类：``__contains__`` 覆写为抛 ``RuntimeError``（``__class__`` 正常）。"""

    def __contains__(self, key):
        raise RuntimeError("host __contains__ raises")


def _named(exc: BaseException, *, type_name: str, needle: str) -> None:
    """具名拒绝的三件套：类型 + ``plaita#69`` + 真实类名 + 不回显宿主文本。"""
    assert isinstance(exc, ValueError), f"必须是具名 ValueError，实际 {exc!r}"
    assert not isinstance(exc, RuntimeError), f"漏了裸 RuntimeError：{exc!r}"
    message = str(exc)
    assert "plaita#69" in message
    assert type_name in message, f"归因要用真实类名：{message!r}"
    assert needle in message, f"要点明哪一跳探针：{message!r}"
    for host_text in ("host refuses", "host __getitem__ raises", "host __contains__ raises"):
        assert host_text not in message, f"刻意不回显宿主异常文本：{message!r}"


def _named_or_container_gate(exc: BaseException, *, type_name: str,
                             needle: str) -> None:
    """plaita#203 容器闸先于 #69 逐跳探针：两种具名拒绝都接受。

    `RefuseOn` / `RefuseOnWithGetItem` / `RaisingEverythingHost` 这类宿主在
    ``__getitem__`` / ``get`` 探针上抛异常，#203 的 fail-closed 判定会先具名
    拒绝，因此不再抵达 #69 的逐跳探针。两者都是 ``ValueError``、都不回显宿主
    文本；非 mapping-like 形状仍由 #69 覆盖（本文件的直调探针测试仍钉 #69）。
    """
    assert isinstance(exc, ValueError), f"必须是具名 ValueError，实际 {exc!r}"
    assert not isinstance(exc, RuntimeError), f"漏了裸 RuntimeError：{exc!r}"
    message = str(exc)
    assert type_name in message, f"归因要用真实类名：{message!r}"
    if "plaita#203" in message:
        for host_text in ("host refuses", "host __getitem__ raises",
                          "host __contains__ raises"):
            assert host_text not in message, f"刻意不回显宿主异常文本：{message!r}"
        return
    assert "plaita#69" in message
    assert needle in message, f"要点明哪一跳探针：{message!r}"
    for host_text in ("host refuses", "host __getitem__ raises", "host __contains__ raises"):
        assert host_text not in message, f"刻意不回显宿主异常文本：{message!r}"


class TestSiblingProbeSitesAreNamed(unittest.TestCase):
    """宿主 ``__class__``/``__getattribute__`` 抛异常 ⇒ 具名拒绝，不是裸异常。"""

    # --- _get_attr（遍历场的读取收口点） --------------------------------
    def test_get_attr_getitem_probe_is_named(self):
        with self.assertRaises(ValueError) as cm:
            _get_attr(RefuseOn("__getitem__", "get"), "x")
        _named_or_container_gate(cm.exception, type_name="RefuseOn", needle="'__getitem__'")

    def test_get_attr_get_probe_is_named(self):
        with self.assertRaises(ValueError) as cm:
            _get_attr(RefuseOnWithGetItem("get"), "x")
        _named_or_container_gate(cm.exception, type_name="RefuseOnWithGetItem", needle="'get'")

    def test_get_attr_dict_probe_is_named(self):
        with self.assertRaises(ValueError) as cm:
            _get_attr(RefuseOnWithGetItem("__dict__"), "x")
        _named(cm.exception, type_name="RefuseOnWithGetItem", needle="'__dict__'")

    def test_traversal_dsl_getitem_probe_is_named(self):
        """DSL 面：``$INPUT.d.x`` 的 ``d`` 是宿主 ⇒ 具名，不是裸 ``RuntimeError``。"""
        with freshness_scope():
            ctx = _alias_context()
            ctx.set_state("$INPUT", {"d": RefuseOn("__getitem__", "get")})
            with self.assertRaises(ValueError) as cm:
                ctx.evaluate("$INPUT.d.x")
        _named_or_container_gate(cm.exception, type_name="RefuseOn", needle="'__getitem__'")

    def test_traversal_dsl_dict_probe_is_named(self):
        with freshness_scope():
            ctx = _alias_context()
            ctx.set_state("$INPUT", {"d": RefuseOnWithGetItem("__dict__")})
            with self.assertRaises(ValueError) as cm:
                ctx.evaluate("$INPUT.d.x")
        _named(cm.exception, type_name="RefuseOnWithGetItem", needle="'__dict__'")

    # --- _container_has_root（#104 方案 A 的父容器探针） -----------------
    def test_container_has_root_isinstance_probe_is_named(self):
        with self.assertRaises(ValueError) as cm:
            _container_has_root(RefuseClass(), "$INPUT")
        _named(cm.exception, type_name="RefuseClass", needle="isinstance(dict)")

    def test_container_has_root_contains_probe_is_named(self):
        with self.assertRaises(ValueError) as cm:
            _container_has_root(RefuseOn("__contains__"), "$INPUT")
        _named(cm.exception, type_name="RefuseOn", needle="'__contains__'")

    def test_container_has_root_get_probe_is_named(self):
        """``getattr(obj, "get", None)`` 那一跳（§4 清单外的同形站点，本单补收）。"""
        with self.assertRaises(ValueError) as cm:
            _container_has_root(RefuseOn("get"), "$INPUT")
        _named(cm.exception, type_name="RefuseOn", needle="'get'")

    def test_container_has_root_dict_probe_is_named(self):
        with self.assertRaises(ValueError) as cm:
            _container_has_root(RefuseOn("__dict__"), "$INPUT")
        _named(cm.exception, type_name="RefuseOn", needle="'__dict__'")

    def test_container_has_root_root_key_probe_is_named(self):
        with self.assertRaises(ValueError) as cm:
            _container_has_root(RefuseOn("$INPUT"), "$INPUT")
        _named(cm.exception, type_name="RefuseOn", needle="'$INPUT'")

    def test_raw_modifier_dsl_is_named(self):
        """DSL 面可达：``$NODE.raw.x`` 的 raw 修饰判定先经过 ``_container_has_root``。"""
        for node, type_name, needle in (
            (RefuseClass(), "RefuseClass", "isinstance(dict)"),
            (RefuseOn("__contains__"), "RefuseOn", "'__contains__'"),
            (RefuseOn("get"), "RefuseOn", "'get'"),
        ):
            with self.subTest(node=type_name):
                with freshness_scope():
                    ctx = _alias_context()
                    ctx.set_state("$NODE", node)
                    with self.assertRaises(ValueError) as cm:
                        ctx.evaluate("$NODE.raw.x")
                _named_or_container_gate(cm.exception, type_name=type_name, needle=needle)

    # --- _compile_function_call 的 tuple 型 isinstance -------------------
    def test_function_result_tuple_isinstance_probe_is_named(self):
        """``$F.<host_fn>()`` 的返回值 ``__class__`` 抛异常 ⇒ 具名（3186）。"""
        registry = ExpressionRegistry()
        registry.register("host_result", RefuseClass, FunctionCategory.DICT)
        with freshness_scope():
            ctx = _alias_context()
            with self.assertRaises(ValueError) as cm:
                ExpressionParser().evaluate("$F.host_result()", ctx.context, registry)
        _named(cm.exception, type_name="RefuseClass", needle="isinstance(dict/list/set")

    def test_engine_view_result_is_always_a_plain_builtin(self):
        """3150 那一处（``_engine_mapping_view`` 的结果）**结构上不可达**。

        r13 的视图读者是 ``dict.keys/values/items`` 基类 C 描述符，只会交出
        ``dict_keys``/``dict_values``/``dict_items``；非真 dict 在视图入口就被具名
        拒绝。所以那里的具名化是**纵深防御**（无 RED 可证），由源码级守卫钉住。
        """
        plain = {"a": 1, "b": 2}
        produced = {
            name: type(_engine_mapping_view(name, plain)).__name__
            for name in ("keys", "values", "items", "getDictKeys", "getDictValues")
        }
        self.assertEqual(
            {"keys": "dict_keys", "values": "dict_values", "items": "dict_items",
             "getDictKeys": "dict_keys", "getDictValues": "dict_values"},
            produced,
        )
        with self.assertRaises(ValueError) as cm:
            _engine_mapping_view("keys", RefuseClass())
        self.assertIn("plaita#69", str(cm.exception))

    def test_probe_helpers_keep_the_isinstance_verdict_verbatim(self):
        """判据一字不改：能判出来时与 ``hasattr``/``isinstance`` 逐字相同。"""
        class Spoofed:
            @property
            def __class__(self):            # noqa: A003 - 刻意伪造（#163 同款）
                return dict

        cases = [
            ({"a": 1}, dict, "dict", True),
            ({"a": 1}, str, "str", False),
            ("s", str, "str", True),
            (Spoofed(), dict, "dict", True),          # 伪造 __class__ 照旧命中
            ({"a": 1}, _CONTAINER_RESULT_KINDS, "kinds", True),
            (SubscriptHost(), _CONTAINER_RESULT_KINDS, "kinds", False),
        ]
        for value, kinds, name, expected in cases:
            with self.subTest(value=type(value).__name__, kinds=name):
                self.assertIs(
                    parser_mod._probe_isinstance(value, kinds, kind_name=name, path="p"),
                    isinstance(value, kinds),
                )
                self.assertIs(
                    parser_mod._probe_isinstance(value, kinds, kind_name=name, path="p"),
                    expected,
                )


class TestSiblingProbeSemanticsUnchanged(unittest.TestCase):
    """负控：具名化没有顺手改判据、也没有误伤普通遍历。"""

    def test_probe_hasattr_matches_hasattr_verbatim(self):
        class Slots:
            __slots__ = ("x",)

        class Missing:
            def __getattr__(self, name):
                raise AttributeError(name)

        cases = [
            ({"a": 1}, "__getitem__", True),
            ({"a": 1}, "get", True),
            ({"a": 1}, "nope", False),
            ("s", "__contains__", True),
            ("s", "get", False),
            (Slots(), "x", True),
            (Slots(), "y", False),
            (Missing(), "anything", False),
            (object(), "__dict__", True),
            (1, "bit_length", True),
        ]
        for value, name, _expected in cases:
            with self.subTest(value=type(value).__name__, name=name):
                self.assertIs(
                    _probe_hasattr(value, name, path="p"),
                    hasattr(value, name),
                )

    def test_probe_hasattr_false_for_raising_getattr(self):
        """``__getattr__`` 抛 ``AttributeError`` 的形状照旧 ⇒ ``False``（不误拒）。"""
        class RaisesAttribute:
            def __getattr__(self, name):
                raise AttributeError(f"no {name}")

        self.assertFalse(_probe_hasattr(RaisesAttribute(), "whatever", path="p"))

    def test_probe_hasattr_names_non_attribute_errors(self):
        """非 ``AttributeError``（``hasattr`` 会裸抛）⇒ 具名 ``ValueError``。"""
        class RaisesNonAttribute:
            def __getattr__(self, name):
                raise TypeError("host is broken")

        host = RaisesNonAttribute()
        with self.assertRaises(TypeError):
            hasattr(host, "whatever")            # 裸 hasattr 的行为（对照）
        with self.assertRaises(ValueError) as cm:
            _probe_hasattr(host, "whatever", path="p")
        self.assertIn("plaita#69", str(cm.exception))
        self.assertNotIn("host is broken", str(cm.exception))

    def test_probe_getattr_matches_getattr_verbatim(self):
        """``_probe_getattr`` 的返回值与 ``getattr(..., default)`` 逐字相同。"""
        sentinel = object()
        cases = [
            ({"a": 1}, "get", sentinel),   # 存在（绑定方法）
            ("s", "get", sentinel),        # 存在
            ("s", "nope", sentinel),       # 缺席 ⇒ default
            (object(), "nope", None),      # 缺席 ⇒ None
            ({"a": 1}, "nope", []),        # 缺席 ⇒ 给定可变默认
        ]
        for value, name, default in cases:
            with self.subTest(value=type(value).__name__, name=name):
                probed = _probe_getattr(value, name, default, path="p")
                builtin = getattr(value, name, default)
                self.assertIs(type(probed), type(builtin))
                if callable(builtin):
                    # 绑定方法每次属性访问都是新对象 ⇒ 比较 self/名字（内建方法与
                    # Python 方法都成立）。
                    self.assertIs(getattr(probed, "__self__", None),
                                  getattr(builtin, "__self__", None))
                    self.assertEqual(probed.__name__, builtin.__name__)
                else:
                    self.assertIs(probed, builtin)

    def test_probe_getattr_names_non_attribute_errors(self):
        class RaisesNonAttribute:
            def __getattr__(self, name):
                raise RuntimeError("host is broken")

        with self.assertRaises(RuntimeError):
            getattr(RaisesNonAttribute(), "whatever", None)     # 裸 getattr（对照）
        with self.assertRaises(ValueError) as cm:
            _probe_getattr(RaisesNonAttribute(), "whatever", None, path="p")
        self.assertIn("plaita#69", str(cm.exception))
        self.assertNotIn("host is broken", str(cm.exception))

    def test_get_attr_plain_shapes_unchanged(self):
        from types import SimpleNamespace

        class Payload:
            text = "hello"

        class MappingLike(dict):
            """真实 dict 子类：plaita#203 后的数据面正控。"""

        self.assertEqual(_get_attr({"k": "v"}, "k"), "v")
        self.assertIsNone(_get_attr({"k": "v"}, "missing"))
        self.assertIsNone(_get_attr({"k": "v"}, "_k"))
        self.assertEqual(_get_attr(MappingLike({"_k": "v"}), "_k"), "v")
        self.assertEqual(_get_attr(Payload(), "text"), "hello")
        self.assertIsNone(_get_attr(Payload(), "missing"))
        self.assertEqual(_get_attr(SimpleNamespace(text="hi"), "text"), "hi")
        for primitive in (1, 1.5, None, b"b"):
            self.assertIsNone(_get_attr(primitive, "anything"))

    def test_container_has_root_plain_shapes_unchanged(self):
        from types import SimpleNamespace

        class Contains:
            def __contains__(self, key):
                return key == "$GLOBAL"

        class HostileContains:
            def __contains__(self, key):
                raise TypeError("probe not supported")

            def get(self, key, default=None):
                return "hit" if key == "$NODE" else default

        class BadGet:
            def get(self, key, default=None):
                raise RuntimeError("storage down")

        self.assertIs(_container_has_root({"$INPUT": 1}, "$INPUT"), True)
        self.assertIs(_container_has_root({"$INPUT": 1}, "$NODE"), False)
        self.assertIs(_container_has_root(Contains(), "$GLOBAL"), True)
        self.assertIs(_container_has_root(Contains(), "$INPUT"), False)
        self.assertIs(_container_has_root(HostileContains(), "$NODE"), True)
        self.assertIs(_container_has_root(HostileContains(), "$INPUT"), False)
        self.assertIs(_container_has_root(BadGet(), "$INPUT"), False)
        self.assertIs(_container_has_root(SimpleNamespace(**{"$NODE": 1}), "$NODE"), True)
        self.assertIs(_container_has_root(SimpleNamespace(**{"$NODE": 1}), "$INPUT"), False)
        self.assertIs(_container_has_root(object(), "$INPUT"), False)
        self.assertIs(_container_has_root(None, "$INPUT"), False)

    def test_dsl_traversal_unchanged(self):
        def evaluate(state, expression):
            with freshness_scope():
                ctx = _alias_context()
                ctx.set_state("$INPUT", state)
                return ctx.evaluate(expression)

        self.assertEqual(evaluate({"d": {"a": 1, "b": {"c": 2}}}, "$INPUT.d.a"), 1)
        self.assertEqual(evaluate({"d": {"a": 1, "b": {"c": 2}}}, "$INPUT.d.b.c"), 2)
        self.assertEqual(evaluate({"d": [10, 20]}, "$INPUT.d[1]"), 20)
        self.assertIsNone(evaluate({"d": {"a": 1}}, "$INPUT.nope"))
        self.assertEqual(list(evaluate({"d": {"a": 1}}, "$F.keys($INPUT.d)")), ["a"])
        # 嵌套表达式串照旧递归；引用缺席根的串照旧按数据文本原样返回
        self.assertEqual(evaluate({"d": {"a": "{% $F.add(1, 2) %}"}}, "$INPUT.d.a"), "3")
        self.assertEqual(
            evaluate({"d": {"a": "{% $NODE.x %}"}}, "$INPUT.d.a"), "{% $NODE.x %}"
        )

    def test_function_result_registration_still_works(self):
        """``$F.*`` 的正向面：真容器结果照旧走 freshness 登记 + 规模守卫。"""
        with freshness_scope():
            ctx = _alias_context()
            registry = None
            parser = ExpressionParser()
            self.assertEqual(
                parser.evaluate("$F.slice($INPUT.lst,0,2)", ctx.context, registry),
                [{"n": 1}, {"n": 2}],
            )
            self.assertEqual(
                parser.evaluate("$F.json_loads('{\"a\": 1}')", ctx.context, registry),
                {"a": 1},
            )


class TestReadSitesKeepTheirNativeContract(unittest.TestCase):
    """**未修站点**（保留现状）：真实读取的宿主异常带原生类型契约。

    这三条刻意把"保留现状"变成可执行的记录：将来若要改成具名/吞掉，必须先改这里
    （并解释原生异常类型契约怎么办），不会悄悄漂移。
    """

    # --- 2570 getattr(obj, path, None)：真实读取 ------------------------
    def test_getattr_read_keeps_host_error(self):
        host = RefuseOnWithGetItem("x")
        with self.assertRaises(RuntimeError) as cm:
            _get_attr(host, "x")
        self.assertNotIn("plaita#69", str(cm.exception))

    def test_getattr_read_keeps_host_error_via_dsl(self):
        with freshness_scope():
            ctx = _alias_context()
            ctx.set_state("$INPUT", {"d": RefuseOnWithGetItem("x")})
            with self.assertRaises(RuntimeError):
                ctx.evaluate("$INPUT.d.x")

    # --- _mapping_get → obj.get(path, None)：真实读取（清单外同形，保留） ---
    def test_mapping_get_host_container_is_gated(self):
        """#203：宿主 mapping-like 在容器闸拒绝，不再进入 ``obj.get``。

        旧契约「探针已证明 ``get`` 存在 ⇒ ``get`` 自己抛的异常原生透出」只适用
        于数据 mapping；#203 后非 dict 的 host mapping-like 在读取前具名拒绝，
        原生宿主错误不再可达（有意的攻击面收缩）。
        """
        class MappingGetRaises:
            def __getitem__(self, key):
                return 1

            def get(self, key, default=None):
                raise RuntimeError("host get raises")

        with self.assertRaises(ValueError) as cm:
            _get_attr(MappingGetRaises(), "x")
        message = str(cm.exception)
        self.assertIn("plaita#203", message)
        self.assertIn("MappingGetRaises", message)
        self.assertNotIn("host get raises", message)

    def test_mapping_get_probe_itself_is_named(self):
        """``get`` 属性探针失败 ⇒ #203 容器闸先具名拒绝（非 mapping-like 仍走 #69）。"""
        with freshness_scope():
            ctx = _alias_context()
            ctx.set_state("$INPUT", {"d": RefuseOnWithGetItem("get")})
            with self.assertRaises(ValueError) as cm:
                ctx.evaluate("$INPUT.d.x")
        _named_or_container_gate(
            cm.exception, type_name="RefuseOnWithGetItem", needle="'get'")

    # --- 2994 obj[target]：真实读取（下标协议） -------------------------
    def test_subscript_read_keeps_host_error(self):
        with freshness_scope():
            ctx = _alias_context()
            ctx.set_state("$INPUT", {"d": SubscriptHost()})
            with self.assertRaises(RuntimeError) as cm:
                ctx.evaluate("$INPUT.d[0]")
        self.assertEqual("host __getitem__ raises", str(cm.exception))

    def test_subscript_read_keeps_native_keyerror(self):
        """原生 ``KeyError``（自定义映射缺键）不得被具名化改写。"""
        with freshness_scope():
            ctx = _alias_context()
            ctx.set_state("$INPUT", {"d": KeyErrorHost()})
            with self.assertRaises(KeyError):
                ctx.evaluate("$INPUT.d[0]")

    def test_subscript_read_class_shape_is_already_named_by_163(self):
        """``$INPUT.d[0]`` 的宿主 class 形状在更早的容器闸具名拒绝。

        #203 在 ``_get_attr`` 入口的 fail-closed 探测先于 #163 的
        ``isinstance(attr, str)``；两者都是具名 ``ValueError``，且都不回显宿主
        文本。非 mapping-like 形状的 #69 逐跳探针仍由直调测试覆盖。
        """
        with freshness_scope():
            ctx = _alias_context()
            ctx.set_state("$INPUT", {"d": RefuseClass()})
            with self.assertRaises(ValueError) as cm:
                ctx.evaluate("$INPUT.d[0]")
        message = str(cm.exception)
        self.assertIn("RefuseClass", message)
        self.assertTrue("plaita#203" in message or "plaita#69" in message)

    # --- 3000 target not in obj：INPUT 缺键判据（仅 DEBUG 留痕） --------
    def test_input_missing_key_probe_keeps_host_error(self):
        with freshness_scope():
            ctx = _alias_context()
            ctx.set_state("$INPUT", ContainsRaisingDict({"d": 1}))
            with self.assertRaises(RuntimeError) as cm:
                ctx.evaluate("$INPUT.d")
        self.assertEqual("host __contains__ raises", str(cm.exception))

    def test_input_missing_key_probe_class_shape_never_triggers_there(self):
        """真 dict 子类 + ``__class__`` 抛异常：``isinstance`` 命中 C 槽 ⇒ 不读
        ``__class__``，缺键判据照常工作（``#163`` 的结构性理由）。"""
        class RaisingClassDict(dict):
            def __getattribute__(self, name):
                if name == "__class__":
                    raise RuntimeError("host refuses __class__")
                return dict.__getattribute__(self, name)

        with freshness_scope():
            ctx = _alias_context()
            ctx.set_state("$INPUT", RaisingClassDict({"d": 1}))
            self.assertEqual(ctx.evaluate("$INPUT.d"), 1)


class TestSiblingProbeSitesAreWrapped(unittest.TestCase):
    """源码级：本单指认的探测点都走具名化包装（防漂移）。"""

    def setUp(self):
        self.source = (ROOT / "plaita" / "core" / "expression_parser.py").read_text(
            encoding="utf-8"
        )

    def test_wrapped_sites_are_present_exactly_once(self):
        for needle in (
            '_probe_hasattr(obj, "__getitem__", path=path)',
            '_probe_hasattr(obj, "get", path=path)',
            '_probe_hasattr(obj, "__dict__", path=path)',
            '_probe_hasattr(obj, "__contains__", path=root_key)',
            '_probe_hasattr(obj, "__dict__", path=root_key)',
            '_probe_hasattr(obj, root_key, path=root_key)',
            '_probe_getattr(obj, "get", None, path=root_key)',
            '_probe_isinstance(obj, dict, kind_name="dict", path=root_key)',
        ):
            with self.subTest(needle=needle):
                self.assertEqual(self.source.count(needle), 1, "探测点缺失或重复")
        self.assertEqual(
            self.source.count("_probe_isinstance(result, _CONTAINER_RESULT_KINDS"),
            2,
            "tuple 型 isinstance 的两处调用点都要走具名化",
        )

    def test_no_bare_probe_shapes_left(self):
        for needle in (
            'hasattr(obj, "__getitem__")',
            'hasattr(obj, "get")',
            'hasattr(obj, "__contains__")',
            'hasattr(obj, root_key)',
            'get = getattr(obj, "get", None)',   # 代码形态（docstring 里的说明不算）
            'isinstance(result, (dict, list, set, frozenset, tuple))',
            "isinstance(obj, dict):",          # _container_has_root 的裸判据
        ):
            with self.subTest(needle=needle):
                self.assertNotIn(needle, self.source)
        self.assertIsNotNone(re.search(r"def _probe_hasattr\(", self.source))
        self.assertIsNotNone(re.search(r"def _probe_getattr\(", self.source))
        self.assertIsNotNone(re.search(r"def _probe_isinstance\(", self.source))

    #: 变异目标（源码里唯一 ⇒ 重放脚本不会打歪；见 RED-PROOF §4）。
    MUTATION_OLD = '_probe_hasattr(obj, "__dict__", path=path)'
    MUTATION_NEW = 'hasattr(obj, "__dict__")'

    def test_mutation_target_is_unique(self):
        self.assertEqual(self.source.count(self.MUTATION_OLD), 1)
        self.assertEqual(self.source.count(self.MUTATION_NEW), 0)


if __name__ == "__main__":
    unittest.main()
