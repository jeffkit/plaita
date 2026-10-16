"""plaita#89：canonical 把 while 的 ``child_flow`` 改名成 ``childFlow``。

canonical（``python -m plaita build`` 的默认产物形态）与 ``compile_source``
直出的 IR 是两套键形：前者 camel（``_canonical.py`` 改名，与 console 发布的
definition 同构）、后者 snake。本文件钉住反向消费的三条对称性：

1. ``emit_source`` 对 canonical while 文档不抛异常，且 round-trip 是
   canonical 的**不动点**（``to_canonical(compile_source(emit_source(canon))) ==
   canon``，剥派生注解口径）；
2. ``count_nodes`` 两形同值（CLI ``--format ir`` 的 ``N nodes`` 摘要与
   ``--check`` 的 OK 行不能漏数循环体）；
3. ``Flow.model_validate(canonical)`` 不再误报
   ``unknown config keys ['childFlow']``（While.LEGACY_KEYS 覆写掉基类声明）。
"""

import copy
import json
import logging
import unittest

from plaita.core.flow import Flow
from plaita.dsl.codeflow import compile_source, count_nodes, emit_source, to_canonical

_WHILE_SRC = '''
def use_while(INPUT):
    while item.n > 0:
        return {"n": item.n - 1}
    return "done"
'''

_DERIVED_KEYS = ("name", "desc", "source_line")


def _strip_derived(scope):
    """剥派生注解（name/desc/source_line），递归 childFlow / branches。"""
    for node in scope.get("nodes", []):
        for key in _DERIVED_KEYS:
            node.pop(key, None)
        inner = node.get("childFlow") or node.get("child_flow")
        if isinstance(inner, dict):
            _strip_derived(inner)
        for branch in node.get("branches") or []:
            if isinstance(branch, dict) and isinstance(branch.get("flow"), dict):
                _strip_derived(branch["flow"])
    return scope


def _while_node(scope):
    return next(n for n in scope["nodes"] if n["type"] == "while")


class _Capture(logging.Handler):
    def __init__(self):
        super().__init__()
        self.messages = []

    def emit(self, record):
        self.messages.append(record.getMessage())


class TestCanonicalWhileRoundTrip(unittest.TestCase):
    def setUp(self):
        self.ir = compile_source(_WHILE_SRC, flow_id="use_while")
        self.canon = to_canonical(self.ir)

    def test_precondition_canonical_renames_while_child_flow(self):
        # canonical 的设计契约（不是缺陷面）：while 的子图键被改名成 camel。
        self.assertIn("child_flow", _while_node(self.ir))
        self.assertIn("childFlow", _while_node(self.canon))
        self.assertNotIn("child_flow", _while_node(self.canon))

    def test_emit_source_accepts_canonical_while(self):
        source = emit_source(self.canon)  # 修复前抛 EmitError: childFlow 缺 start 节点
        self.assertIn("WHILE(", source)
        self.assertIn("id='n_gt_0'", source)

    def test_canonical_roundtrip_is_fixpoint(self):
        again = to_canonical(compile_source(emit_source(self.canon)))
        self.assertEqual(_strip_derived(copy.deepcopy(self.canon)),
                         _strip_derived(copy.deepcopy(again)))

    def test_canonical_while_child_flow_semantics_preserved(self):
        again = to_canonical(compile_source(emit_source(self.canon)))
        before, after = _while_node(self.canon), _while_node(again)

        def _shape(scope):
            return sorted((n["id"], n["type"], n.get("next")) for n in scope["nodes"])

        self.assertEqual(_shape(before["childFlow"]), _shape(after["childFlow"]))
        self.assertEqual(before["condition"], after["condition"])

    def test_count_nodes_symmetric(self):
        self.assertEqual(count_nodes(self.ir), count_nodes(self.canon))
        # 主流程 3（start/while/end 的裸 return）+ 循环体 2——两形都必须报 5，
        # 而不是 IR 报 3（漏数循环体）。
        self.assertEqual(count_nodes(self.ir), 5)

    def test_no_unknown_key_warning_for_childflow(self):
        logger = logging.getLogger("plaita.node.schema")
        capture = _Capture()
        logger.addHandler(capture)
        old_level = logger.level
        logger.setLevel(logging.WARNING)
        try:
            Flow.model_validate(self.canon)
        finally:
            logger.removeHandler(capture)
            logger.setLevel(old_level)
        noise = [m for m in capture.messages
                 if "unknown config keys" in m and "childFlow" in m]
        self.assertEqual(noise, [])


if __name__ == "__main__":
    unittest.main()
