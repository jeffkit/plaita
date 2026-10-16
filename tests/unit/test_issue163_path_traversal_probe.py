"""plaita#163 —— 路径遍历的 ``isinstance`` 探测在宿主 ``__class__`` 抛异常时漏裸异常。

``#69`` r13 的独立验收确认：路径遍历层（``_compile_variable`` 的 thunk 与
``_get_attr``）用 ``isinstance(attr, str)`` / ``isinstance(obj, dict)`` 判断"这是不是
引擎数据"。``isinstance(x, C)`` 在**真实类型不命中**时会回落读 ``x.__class__``
（``tp_getattro`` ⇒ 宿主 ``__getattribute__`` / ``property``）；宿主在那里抛异常时，
**裸** ``RuntimeError`` 直接冒到用户侧，而不是 ``#69`` 已建立的具名拒绝。

同一形状在 ``_engine_mapping_view`` 直调时已具名（r13 的
``_MAPPING_VIEW_PROBE_FAILED_TEMPLATE``），但先经过路径遍历就漏——本文件钉住这条
缺口已关：

1. **动态**（:class:`TestIssue163TraversalProbeIsNamed`）：``$F.keys($INPUT.d)`` 与
   ``$INPUT.d`` 在宿主 ``__class__`` 抛异常时得到**具名** ``ValueError``
   （消息含 ``plaita#69``），绝不漏裸 ``RuntimeError``；``_get_attr`` 那一跳
   （``$INPUT.d.x``）同样具名。
2. **负控**（:class:`TestIssue163TraversalSemanticsUnchanged`）：普通 dict/str 的
   路径遍历、INPUT 缺键的静默 None、嵌套表达式串的递归、``$F.keys`` 读真实存储，
   行为逐字不变；判据本身（含 ``__class__`` 伪造的既有语义）没有被换成
   ``type()`` 精确判据。
3. **源码级**（:class:`TestIssue163ProbeSitesAreWrapped`）：遍历里那几处探测都走
   :func:`plaita.core.expression_parser._probe_isinstance`；变异（还原成裸
   ``isinstance``）由普通 ``FAILED`` 行杀死（见 RED-PROOF.md 的变异记录）。
"""
from __future__ import annotations

import pathlib
import re
import unittest

from plaita.core.expression_parser import (
    ExpressionParser,
    _engine_mapping_view,
    _probe_isinstance,
)
from plaita.core.expression_parser import freshness_scope

from tests.unit.test_issue69_mutation_gate import _alias_context

ROOT = pathlib.Path(__file__).resolve().parents[2]


class RaisingClassHost(dict):
    """宿主 ``__class__`` 抛异常的真 ``dict`` 子类（#163 issue 正文的复现形状）。"""

    def __getattribute__(self, name):
        if name == "__class__":
            raise RuntimeError("host refuses __class__")
        return dict.__getattribute__(self, name)


class RaisingEverythingHost:
    """宿主把**所有**属性访问都炸掉（走 ``_get_attr`` 的非 dict 分支）。"""

    def __getattribute__(self, name):
        raise RuntimeError("host refuses everything")


class RaisingClassPlain:
    """非 ``dict`` 的宿主，``__class__`` 抛异常（``isinstance(x, dict)`` 必回落）。"""

    def __getattribute__(self, name):
        if name == "__class__":
            raise RuntimeError("host refuses __class__")
        return object.__getattribute__(self, name)


class SpoofedClassName:
    """``__class__`` 是 ``property``（不抛）：``isinstance`` 的既有语义必须保留。"""

    def __init__(self, claimed):
        self._claimed = claimed

    @property
    def __class__(self):            # noqa: A003 - 刻意伪造（r13 ``_SpoofedDict`` 同款）
        return self._claimed


def _dsl(url_expression: str):
    """在活 CheckpointState 上求值一条表达式，返回 ``(raised, ctx)``。"""
    with freshness_scope():
        ctx = _alias_context()
        ctx.set_state("$INPUT", {"d": RaisingClassHost({"a": 1})})
        ctx.set_state("$VAR", ctx.evaluate({}))
        raised = None
        try:
            ctx.evaluate(f"$F.set($VAR,'k',{url_expression})")
        except Exception as exc:            # noqa: BLE001 - 判决即探针对象
            raised = exc
    return raised, ctx


class TestIssue163TraversalProbeIsNamed(unittest.TestCase):
    """路径遍历的探测失败 ⇒ 具名 ``plaita#69`` 拒绝，不是裸异常。"""

    def test_dsl_keys_on_raising_class_host_is_named(self):
        """``$F.keys($INPUT.d)``：宿主 ``__class__`` 抛异常 ⇒ 具名拒绝。

        pre（`87b8ad0`）实测：``RuntimeError: host refuses __class__``，
        消息里没有 ``plaita#69``（裸异常）。断言**类型 + 前缀 + 真实类名**三件，
        并显式排除裸 ``RuntimeError``——这正是 #163 的验收判据 2。
        """
        raised, _ctx = _dsl("$F.keys($INPUT.d)")
        self.assertIsNotNone(raised, "宿主 __class__ 抛异常时必须拒绝，不得静默通过")
        self.assertNotIsInstance(
            raised, RuntimeError,
            f"漏了裸 RuntimeError（#163 的原始缺口）：{raised!r}",
        )
        self.assertIsInstance(raised, ValueError)
        message = str(raised)
        self.assertIn("plaita#69", message)
        self.assertIn("RaisingClassHost", message, "归因要用真实类名（type() 读 C 槽）")
        self.assertIn("isinstance(str)", message, "要点明是哪一跳的探测")
        # 刻意不回显宿主异常的文本（宿主可控，可能带内部状态/凭据）
        self.assertNotIn("host refuses __class__", message)

    def test_bare_traversal_of_raising_host_is_named(self):
        """最短复现：``$INPUT.d`` 本身就经过 ``isinstance(attr, str)``。"""
        with freshness_scope():
            ctx = _alias_context()
            ctx.set_state("$INPUT", {"d": RaisingClassHost({"a": 1})})
            with self.assertRaises(ValueError) as cm:
                ctx.evaluate("$INPUT.d")
        self.assertIn("plaita#69", str(cm.exception))
        self.assertNotIsInstance(cm.exception, RuntimeError)

    def test_get_attr_probe_on_raising_host_is_named(self):
        """``_get_attr`` 的 ``isinstance(obj, dict)`` 那一跳同样具名。

        ``$INPUT.d.x``：第二段以 ``d`` 为容器 ⇒ 走 ``_get_attr`` 的探测。
        """
        with freshness_scope():
            ctx = _alias_context()
            ctx.set_state("$INPUT", {"d": RaisingEverythingHost()})
            with self.assertRaises(ValueError) as cm:
                ctx.evaluate("$INPUT.d.x")
        message = str(cm.exception)
        self.assertIn("plaita#69", message)
        self.assertIn("RaisingEverythingHost", message)

    def test_helper_returns_isinstance_answers_verbatim(self):
        """判据**一字不改**：能判出来时与 ``isinstance`` 逐字相同（含伪造）。"""
        cases = [
            ({}, dict, "dict", True),
            ([], dict, "dict", False),
            ("s", str, "str", True),
            (b"s", str, "str", False),
            (SpoofedClassName(dict), dict, "dict", True),   # `__class__` 伪造照旧命中
            (SpoofedClassName(str), str, "str", True),
        ]
        for value, kinds, name, expected in cases:
            with self.subTest(value=type(value).__name__, kinds=name):
                self.assertIs(
                    _probe_isinstance(value, kinds, kind_name=name, path="d"),
                    expected,
                )
                self.assertIs(_probe_isinstance(value, kinds, kind_name=name, path="d"),
                              isinstance(value, kinds))

    def test_probe_failure_is_valueerror_with_cause(self):
        """具名拒绝用 ``from exc`` 保留因果链（诊断用），但**不回显**宿主文本。

        用**非** ``dict`` 的宿主：真 ``dict`` 子类走 ``PyObject_TypeCheck`` 就命中，
        CPython 根本不会去读 ``__class__``（这正是「触发即裸异常」的结构性理由）。
        """
        with self.assertRaises(ValueError) as cm:
            _probe_isinstance(RaisingClassPlain(), dict, kind_name="dict", path="d")
        self.assertIsInstance(cm.exception.__cause__, RuntimeError)
        self.assertNotIn("host refuses", str(cm.exception))


class TestIssue163TraversalSemanticsUnchanged(unittest.TestCase):
    """负控：探测**具名化**不改路径遍历的语义（能跑的照旧跑通）。"""

    def _evaluate(self, state, expression):
        with freshness_scope():
            ctx = _alias_context()
            ctx.set_state("$INPUT", state)
            return ctx.evaluate(expression)

    def test_plain_mapping_traversal_still_works(self):
        self.assertEqual(self._evaluate({"d": {"a": 1, "b": {"c": 2}}}, "$INPUT.d.a"), 1)
        self.assertEqual(
            self._evaluate({"d": {"a": 1, "b": {"c": 2}}}, "$INPUT.d.b.c"), 2
        )

    def test_missing_input_key_stays_silent_none(self):
        """``$INPUT.<缺键>`` 的静默 ``None`` 是历史语义（探测不得把它变成拒绝）。"""
        self.assertIsNone(self._evaluate({"d": {"a": 1}}, "$INPUT.nope"))

    def test_nested_expression_string_still_recurses(self):
        """字符串属性值仍会被当嵌套表达式串递归求值（#104 的既有语义）。"""
        self.assertEqual(
            self._evaluate({"d": {"a": "{% $F.add(1, 2) %}"}}, "$INPUT.d.a"), "3"
        )

    def test_data_text_is_still_returned_as_is(self):
        """引用不存在的根 ⇒ 按数据文本原样返回（#104 方案 A），不误判、不拒绝。"""
        self.assertEqual(
            self._evaluate({"d": {"a": "{% $NODE.x %}"}}, "$INPUT.d.a"),
            "{% $NODE.x %}",
        )

    def test_spoofed_class_still_reaches_r13_spoofed_refusal(self):
        """``__class__`` 伪造（不抛）的既有路径逐字保留：照旧走到 r13 的 spoofed 拒绝。

        负控的意义：把探测换成 ``type()`` 精确判据会**改变**这一格（伪造的
        ``__class__`` 本来能骗过 ``isinstance``）；这里钉住「具名化没有顺手改判据」
        ——遍历的 ``isinstance(attr, str)`` 对伪造 dict 仍是 False，``$F.keys`` 仍由
        r13 的 ``_MAPPING_VIEW_SPOOFED_TEMPLATE`` 具名拒绝。
        """
        with freshness_scope():
            ctx = _alias_context()
            ctx.set_state("$INPUT", {"d": SpoofedClassName(dict)})
            ctx.set_state("$VAR", ctx.evaluate({}))
            with self.assertRaises(ValueError) as cm:
                ctx.evaluate("$F.set($VAR,'k',$F.keys($INPUT.d))")
        message = str(cm.exception)
        self.assertIn("plaita#69", message)
        self.assertIn("spoofed __class__", message)

    def test_keys_on_plain_dict_reads_real_storage(self):
        """``$F.keys`` 在普通 dict 上照旧返回真实存储。"""
        value = self._evaluate({"d": {"a": 1}}, "$F.keys($INPUT.d)")
        self.assertEqual(list(value), ["a"])

    def test_engine_mapping_view_probe_still_named(self):
        """r13 的直调面（``_engine_mapping_view``）逐字不变。"""
        with self.assertRaises(ValueError) as cm:
            _engine_mapping_view("keys", RaisingEverythingHost())
        self.assertIn("plaita#69", str(cm.exception))

    def test_expression_parser_smoke(self):
        """模块级对象仍可用（import 面没被改动牵动）。"""
        self.assertIsInstance(ExpressionParser(), ExpressionParser)


class TestIssue163ProbeSitesAreWrapped(unittest.TestCase):
    """源码级：路径遍历里的探测都走 ``_probe_isinstance``（防漂移）。"""

    #: 变异目标：把这一处还原成裸 ``isinstance`` 即复现 #163（见 RED-PROOF.md）。
    MUTATION_OLD = '_probe_isinstance(attr, str, kind_name="str", path=raw)'
    MUTATION_NEW = "isinstance(attr, str)"

    def setUp(self):
        self.source = (ROOT / "plaita" / "core" / "expression_parser.py").read_text(
            encoding="utf-8"
        )

    def test_traversal_probe_sites_are_wrapped(self):
        for needle in (
            '_probe_isinstance(obj, dict, kind_name="dict", path=raw)',
            '_probe_isinstance(attr, str, kind_name="str", path=raw)',
            '_probe_isinstance(obj, dict, kind_name="dict", path=path)',
        ):
            with self.subTest(needle=needle):
                self.assertEqual(self.source.count(needle), 1, "探测点缺失或重复")

    def test_mutation_target_is_unique(self):
        """变异串在源码里**唯一**（``old`` 唯一 ⇒ 重放脚本不会打歪）。"""
        self.assertEqual(self.source.count(self.MUTATION_OLD), 1)
        self.assertEqual(self.source.count(self.MUTATION_NEW), 0)

    def test_no_bare_isinstance_on_the_traversal_probe_shapes(self):
        """遍历里不再有裸 ``isinstance(obj, dict)`` / ``isinstance(attr, str)``。"""
        self.assertNotIn('isinstance(obj, dict) and target not in obj', self.source)
        self.assertNotIn("not raw_mode and isinstance(attr, str)", self.source)
        self.assertIsNotNone(
            re.search(r"def _probe_isinstance\(", self.source),
            "具名化包装函数不见了",
        )


if __name__ == "__main__":
    unittest.main()
