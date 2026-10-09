"""正典（console-canonical）序列化测试。

钉三件事：
1. 形态规则——``type``/``id`` 首位、顶层 None 剔除、``child_flow`` →
   ``childFlow`` 改名（while）、嵌套子图（CHILD / PARALLEL 分支）递归；
2. 语义边界——条件树里的 ``value: null`` 是合法比较，**不**被剔除；
3. 字节稳定——同源码连续两次「编译 → 正典 → 序列化」逐字节一致，序列化
   约定固定（ensure_ascii=False / indent=2 / 尾换行）。
"""

import json
import unittest

from plaita.dsl.codeflow import (
    compile_source,
    count_nodes,
    serialize_canonical,
    to_canonical,
)

# 含 while（child_flow 改名）、CHILD（childFlow 递归）、裸 return（output null）
# 三种形态的最小样例。
_MULTI_SHAPE_SRC = '''
@childflow
def sub(INPUT):
    return INPUT.x + 1

def drain(INPUT):
    c = CHILD(input={"x": INPUT.base}, flow=sub)
    while item.n > 0:
        return {"n": item.n - 1}
    return c.val
'''

_BARE_RETURN_SRC = '''
def bare(INPUT):
    if len(INPUT.items) > 0:
        return
    return "empty"
'''

_NULL_COND_SRC = '''
def null_cond(INPUT):
    if INPUT.x == None:
        return "is-null"
    return "set"
'''

_PARALLEL_SRC = '''
@childflow
def b1(INPUT):
    return 1

@childflow
def b2(INPUT):
    return 2

def fan(INPUT):
    p = PARALLEL(branches={"a": b1, "b": b2})
    return p.a
'''


def _find(nodes, ntype):
    return [n for n in nodes if n["type"] == ntype]


class TestToCanonicalShape(unittest.TestCase):
    def setUp(self):
        self.doc = to_canonical(compile_source(_MULTI_SHAPE_SRC, flow_id="drain"))

    def test_root_keys_keep_ir_order(self):
        self.assertEqual(list(self.doc.keys())[:3],
                         ["runtime", "flow_id", "inputType"])
        self.assertEqual(self.doc["flow_id"], "drain")

    def test_start_node_first_and_type_first_key(self):
        start = self.doc["nodes"][0]
        self.assertEqual(start["type"], "start")
        self.assertEqual(list(start.keys())[0], "type")

    def test_child_node_uses_childflow_camel(self):
        child = _find(self.doc["nodes"], "child")[0]
        self.assertIn("childFlow", child)
        self.assertNotIn("child_flow", child)
        sub_nodes = child["childFlow"]["nodes"]
        # 子图递归转换：start 在首，end 节点保留 resultType camel 别名
        self.assertEqual(sub_nodes[0]["type"], "start")
        end = _find(sub_nodes, "end")[0]
        self.assertEqual(end["resultType"], "success")

    def test_while_node_renames_child_flow(self):
        while_node = _find(self.doc["nodes"], "while")[0]
        self.assertIn("childFlow", while_node)
        self.assertNotIn("child_flow", while_node)
        self.assertEqual(while_node["childFlow"]["inputType"],
                         {"dataType": "object"})

    def test_node_top_level_null_stripped(self):
        # IR 里 output=None 的 end 节点（裸 return），正典剔除该键；
        # 有值的 end 保留。样例有效性：两条 end 一条 None 一条有值。
        ir = compile_source(_BARE_RETURN_SRC, flow_id="bare")
        doc = to_canonical(ir)
        ir_ends = {e["id"]: e for e in _find(ir["nodes"], "end")}
        self.assertEqual(len(ir_ends), 2)
        self.assertTrue(any(e.get("output") is None for e in ir_ends.values()))
        canon_ends = {e["id"]: e for e in _find(doc["nodes"], "end")}
        for eid, ir_e in ir_ends.items():
            if ir_e.get("output") is None:
                self.assertNotIn("output", canon_ends[eid])
            else:
                self.assertEqual(canon_ends[eid]["output"], ir_e["output"])

    def test_condition_null_value_preserved(self):
        # 条件树里的 value: null 是合法比较（INPUT.x == null），不剔除
        doc = to_canonical(compile_source(_NULL_COND_SRC, flow_id="null_cond"))
        if_node = _find(doc["nodes"], "if")[0]
        self.assertIsNone(if_node["condition"]["value"])

    def test_parallel_branch_subflows_recursed(self):
        doc = to_canonical(compile_source(_PARALLEL_SRC, flow_id="fan"))
        p = _find(doc["nodes"], "parallel")[0]
        for branch in p["branches"]:
            self.assertEqual(branch["flow"]["nodes"][0]["type"], "start")


class TestByteStability(unittest.TestCase):
    def test_serialize_twice_identical(self):
        texts = [serialize_canonical(to_canonical(compile_source(
            _MULTI_SHAPE_SRC, flow_id="drain"))) for _ in range(2)]
        self.assertEqual(texts[0], texts[1])

    def test_serialize_convention(self):
        doc = to_canonical(compile_source(_MULTI_SHAPE_SRC, flow_id="drain"))
        text = serialize_canonical(doc)
        self.assertTrue(text.endswith("}\n"))
        # ensure_ascii=False：中文注释性 desc 不被转义
        self.assertNotIn("\\u", text)
        self.assertEqual(text, json.dumps(doc, ensure_ascii=False, indent=2) + "\n")

    def test_canonical_idempotent(self):
        doc = to_canonical(compile_source(_MULTI_SHAPE_SRC, flow_id="drain"))
        again = to_canonical(doc)
        self.assertEqual(serialize_canonical(doc), serialize_canonical(again))


class TestCountNodes(unittest.TestCase):
    def test_counts_nested(self):
        doc = to_canonical(compile_source(_MULTI_SHAPE_SRC, flow_id="drain"))
        top = len(doc["nodes"])
        child = _find(doc["nodes"], "child")[0]
        while_node = _find(doc["nodes"], "while")[0]
        expected = (top + len(child["childFlow"]["nodes"])
                    + len(while_node["childFlow"]["nodes"]))
        self.assertEqual(count_nodes(doc), expected)
        self.assertEqual(count_nodes({"nodes": []}), 0)


if __name__ == "__main__":
    unittest.main()
