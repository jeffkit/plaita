"""plaita#162 —— ``state._snapshot_value`` 的 docstring 必须与**实测**一致。

立案事实（#69 r13 验收裁定为「必须立案」的 GAP）：``_snapshot_value.__doc__`` 曾写

    唯一按引用共享的是**非容器宿主对象**（它们不是容器，重建语义不适用）

这是**事实错误**。实测 ``_snapshot_value(v) is v`` 对
``types.MappingProxyType`` / ``collections.ChainMap`` / ``collections.UserDict`` /
``os._Environ``（``os.environ``）四者全为 ``True``，而这四者**都是容器**
（``isinstance(v, collections.abc.Container)`` 全为 ``True``）。

本文件是 #162 验收第 2 条的 **doc-pin**：它同时钉住

* **措辞**——枚举必须留在 ``_snapshot_value.__doc__`` 里，且那句假话必须从
  ``state.py`` 全模块消失（否则措辞可以无声漂回去）；
* **事实**——枚举的四者确实按引用共享、确实是容器，且重建面确实只有
  ``dict``/``list``/``tuple``/``set``/``frozenset`` 五种。

只钉"文档里有这几行字"是不够的：实现一旦改成重建 ``ChainMap``，文档就必须跟着改，
所以本文件把**行为**一起钉进来（行为漂移 ⇒ 本文件红 ⇒ 强制回来改文档）。

不改变 ``_snapshot_value`` 的任何行为（#162 边界：不动 #69 判据、不改快照语义）。
"""

from __future__ import annotations

import collections
import os
import pathlib
import types
import unittest

import plaita.core.state as state_module
from plaita.core.expression_parser import _EngineCopyRefused, _engine_copy
from plaita.core.state import _snapshot_value

#: #69 r13 在 expression_parser / MIGRATION / docs-site 三处已改用**枚举**口径的四个
#: 反例类型；#162 要求 state.py 的用户可见 docstring 与之对齐。
SHARED_CONTAINER_CASES = (
    ("types.MappingProxyType", types.MappingProxyType({"a": 1})),
    ("collections.ChainMap", collections.ChainMap({"a": 1})),
    ("collections.UserDict", collections.UserDict({"a": 1})),
    ("os._Environ", os.environ),
)

#: 重建面（``_snapshot_value`` 会现造普通容器的那五种）。
REBUILT_KINDS = (
    ("dict", lambda: {"a": 1}),
    ("dict-subclass", lambda: type("Sub", (dict,), {})({"a": 1})),
    ("list", lambda: [1, 2]),
    ("tuple", lambda: (1, 2)),
    ("set", lambda: {1, 2}),
    ("frozenset", lambda: frozenset({1, 2})),
)

#: #162 点名的那句假话（原文）。它必须从 ``state.py`` **整份源码**里消失：留着就
#: 意味着某个用户可见的 docstring 仍在把共享面说成"非容器"。
FALSE_PHRASE = "非容器宿主对象"
FALSE_PHRASE_VARIANT = "非容器对象"


def _state_source() -> str:
    return pathlib.Path(state_module.__file__).read_text(encoding="utf-8")


class TestDocstringEnumeratesTheSharedSet(unittest.TestCase):
    """措辞面：枚举在 docstring 里，假话不在模块里。"""

    def test_snapshot_value_docstring_names_every_counterexample(self):
        doc = _snapshot_value.__doc__ or ""
        for token in ("MappingProxyType", "ChainMap", "UserDict", "os._Environ"):
            with self.subTest(token=token):
                self.assertIn(
                    token, doc,
                    f"_snapshot_value.__doc__ 必须枚举按引用共享的容器反例 {token!r}",
                )

    def test_docstring_states_why_sharing_is_safe(self):
        doc = _snapshot_value.__doc__ or ""
        # "为何共享安全"的口径（#69 r13 已定）：不是数据通道 —— _engine_copy 具名拒绝。
        self.assertIn("_engine_copy", doc)
        self.assertIn("_EngineCopyRefused", doc)

    def test_docstring_states_the_rebuild_face_is_exactly_five_kinds(self):
        doc = _snapshot_value.__doc__ or ""
        self.assertIn("重建面", doc)
        for token in ("dict", "list", "tuple", "set", "frozenset"):
            with self.subTest(kind=token):
                self.assertIn(token, doc)

    def test_false_non_container_claim_is_absent_from_the_whole_module(self):
        src = _state_source()
        self.assertNotIn(
            FALSE_PHRASE, src,
            "#162：'唯一按引用共享的是非容器宿主对象' 是事实错误，state.py 不得再出现该说法",
        )
        self.assertNotIn(FALSE_PHRASE_VARIANT, src)

    def test_no_universal_claim_that_sharing_is_only_non_containers(self):
        """回归守卫：不许用"唯一/只剩…非容器"这类无限量化措辞替代枚举。"""
        src = _state_source()
        for line_no, line in enumerate(src.splitlines(), start=1):
            if "非容器" not in line:
                continue
            with self.subTest(line=line_no):
                # 允许"（非容器）"这样的**部分**描述；不允许把共享面整体等同于非容器。
                self.assertNotRegex(
                    line, r"唯一.{0,16}非容器",
                    f"state.py:{line_no} 又把共享面收敛成'唯一…非容器'",
                )


class TestBehaviourMatchesTheDocstring(unittest.TestCase):
    """事实面：docstring 的每一句都要能在本进程里实测出来。"""

    def test_enumerated_counterexamples_are_shared_and_are_containers(self):
        for name, value in SHARED_CONTAINER_CASES:
            with self.subTest(case=name):
                self.assertIs(
                    _snapshot_value(value), value,
                    f"{name} 实测按引用共享（#162 的反例前提）",
                )
                self.assertIsInstance(value, collections.abc.Container)
                self.assertIsInstance(value, collections.abc.Mapping)

    def test_shared_counterexamples_are_not_a_dsl_write_channel(self):
        """docstring 的"不是数据通道"必须可实测：四者全被 ``_engine_copy`` 具名拒绝。"""
        for name, value in SHARED_CONTAINER_CASES:
            with self.subTest(case=name):
                with self.assertRaises(_EngineCopyRefused) as cm:
                    _engine_copy(value)
                self.assertEqual(cm.exception.type_name, type(value).__name__)

    def test_rebuild_face_is_exactly_the_five_kinds(self):
        for name, factory in REBUILT_KINDS:
            with self.subTest(kind=name):
                value = factory()
                out = _snapshot_value(value)
                self.assertIsNot(out, value)
                self.assertEqual(out, value)
                self.assertIn(
                    type(out),
                    (dict, list, tuple, set, frozenset),
                    "重建面之外的类型不得被重建（否则 docstring 的补集口径要改）",
                )

    def test_non_rebuilt_containers_are_the_complement_of_the_rebuild_face(self):
        """补集口径：`UserList`/`deque` 这类容器同样按引用共享（docstring 的"同型"）。"""
        for value in (collections.UserList([1]), collections.deque([1]),
                      bytearray(b"ab")):
            with self.subTest(kind=type(value).__name__):
                self.assertIsInstance(value, collections.abc.Container)
                self.assertIs(_snapshot_value(value), value)


if __name__ == "__main__":
    unittest.main()
