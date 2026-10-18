"""plaita#220：@flow / sexpr 往返未登记丢失面（本补丁覆盖 6 个面）。

每个面一个「全字段 IR → 往返 → 字段逐项相等」用例：

- L1  @flow 集合节点 id 未绑定（map/filter/find/loop/reduce 下游 `$NODE.<id>`）；
- L2  @flow parallel 分支 input/condition；
- L4  HTTP output/query/addressing/delegate（两链）；
- L5a assignment upstreamOutput（两链）；L5b assignment outputType（@flow 链）；
- L6  sexpr HTTP headers/body 字符串值类型不漂移。
"""
from __future__ import annotations

import unittest

from plaita.dsl.codeflow import compile_source, emit_source
from plaita.dsl.sexpr import compile_sexpr, flow_to_sexpr


def _child(output="$INPUT.item"):
    return {
        "runtime": "python",
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "cs", "next": "ce"},
            {"type": "end", "id": "ce", "output": output, "resultType": "success"},
        ],
    }


def _flow(nodes, flow_id="t220"):
    return {"flow_id": flow_id, "inputType": {"dataType": "object"}, "nodes": nodes}


def _start(nxt="x"):
    return {"type": "start", "id": "s", "next": nxt}


def _end(output="done"):
    return {"type": "end", "id": "e", "output": output, "resultType": "success"}


def _emit_rt(ir):
    source = emit_source(ir)
    return compile_source(source), source


def _sexpr_rt(ir):
    source = flow_to_sexpr(ir)
    return compile_sexpr(source), source


def _nodes_of(ir, ntype):
    return [n for n in ir.get("nodes", []) if n.get("type") == ntype]


class TestL1CollectionNodeId(unittest.TestCase):
    """@flow 集合节点的 id 必须在重编译时仍可被下游 `$NODE.<id>` 解析。"""

    def test_every_collection_type_supports_downstream_reference(self):
        for kind in ("map", "filter", "find", "loop", "reduce"):
            with self.subTest(kind=kind):
                child = _child("$INPUT[0]" if kind == "reduce" else "$INPUT.item")
                node = {"type": kind, "id": "m", "collection": "$INPUT.items",
                        "childFlow": child, "next": "e"}
                if kind == "reduce":
                    node["initial"] = 0
                back, source = _emit_rt(_flow([
                    _start(nxt="m"), node, _end(output="$NODE.m")]))
                ends = _nodes_of(back, "end")
                self.assertEqual(len(ends), 1, source)
                self.assertEqual(ends[0].get("output"), "$NODE.m",
                                 f"{kind}: 下游引用集合节点 id 丢失\n{source}")

    def test_while_for_head_id_supports_downstream_reference(self):
        """同族：`for x in WHILE(cond, id='w')` 的 id 同样没有赋值语句可借。"""
        ir = _flow([
            _start(nxt="w"),
            {"type": "while", "id": "w", "condition": {"field": "$INPUT.n", "operator": "gt", "value": 0},
             "childFlow": _child(), "next": "e"},
            _end(output="$NODE.w"),
        ])
        back, source = _emit_rt(ir)
        ends = _nodes_of(back, "end")
        self.assertEqual(ends[0].get("output"), "$NODE.w", source)


class TestL2ParallelBranchFields(unittest.TestCase):
    def test_branch_input_and_condition_round_trip_through_flow(self):
        condition = {"field": "$INPUT.flag", "operator": "truthy", "value": True}
        ir = _flow([
            _start(nxt="p"),
            {"type": "parallel", "id": "p", "mode": "thread",
             "joinBranches": ["a"], "isConditional": True,
             "branches": [
                 {"name": "a", "flow": _child(), "input": "$INPUT.x",
                  "condition": condition},
                 {"name": "b", "flow": _child()},
             ], "next": "e"},
            _end(),
        ])
        back, source = _emit_rt(ir)
        branch = _nodes_of(back, "parallel")[0]["branches"][0]
        self.assertEqual(branch.get("input"), "$INPUT.x", source)
        self.assertEqual(branch.get("condition"), condition, source)

    def test_sexpr_chain_keeps_branch_fields(self):
        """对照：sexpr 链本来就保留，防止回归。"""
        condition = {"field": "$INPUT.flag", "operator": "truthy", "value": True}
        ir = _flow([
            _start(nxt="p"),
            {"type": "parallel", "id": "p",
             "branches": [{"name": "a", "flow": _child(), "input": "$INPUT.x",
                           "condition": condition}], "next": "e"},
            _end(),
        ])
        back, _source = _sexpr_rt(ir)
        branch = _nodes_of(back, "parallel")[0]["branches"][0]
        self.assertEqual(branch.get("input"), "$INPUT.x")
        self.assertEqual(branch.get("condition"), condition)


# L3（sexpr code `sandbox_backend`）已由 88eb1340 落地，用例在
# `tests/unit/test_issue220_sexpr_code_sandbox.py`（他侧实现更严格：
# 非字符串 / 空串读写两侧 fail-closed），本补丁不重复覆盖，也不改回自己的写法。


_HTTP_FIELDS = ("output", "query", "addressing", "delegate")


class TestL4HttpSemanticFields(unittest.TestCase):
    def _ir(self):
        return _flow([
            _start(nxt="h"),
            {"type": "http", "id": "h", "method": "GET", "url": "https://example.com",
             "output": "$INPUT.pick", "query": {"q": "1"},
             "addressing": {"name": "domain"},
             "delegate": {"name": "proxy", "params": {"p": 1}}, "next": "e"},
            _end(),
        ])

    def test_flow_chain_keeps_all_four_fields(self):
        ir = self._ir()
        expected = ir["nodes"][1]
        back, source = _emit_rt(ir)
        node = _nodes_of(back, "http")[0]
        for field in _HTTP_FIELDS:
            self.assertEqual(node.get(field), expected[field], f"{field}\n{source}")

    def test_sexpr_chain_keeps_all_four_fields(self):
        ir = self._ir()
        expected = ir["nodes"][1]
        back, source = _sexpr_rt(ir)
        node = _nodes_of(back, "http")[0]
        for field in _HTTP_FIELDS:
            self.assertEqual(node.get(field), expected[field], f"{field}\n{source}")


class TestL5AssignmentFields(unittest.TestCase):
    def _ir(self):
        upstream = [{"upstream": "u", "value": "$NODE.prev"},
                    {"upstream": "v", "value": "literal"}]
        return _flow([
            _start(nxt="prev"),
            {"type": "assignment", "id": "prev", "output": "$INPUT.x", "next": "a"},
            {"type": "assignment", "id": "a", "output": "$INPUT.x",
             "outputType": {"dataType": "string"},
             "upstreamOutput": upstream, "next": "e"},
            _end(),
        ])

    def test_flow_chain_keeps_output_type_and_upstream_output(self):
        ir = self._ir()
        expected = ir["nodes"][2]
        back, source = _emit_rt(ir)
        node = _nodes_of(back, "assignment")[-1]
        self.assertEqual(node.get("outputType"), expected["outputType"], source)
        self.assertEqual(node.get("upstreamOutput"), expected["upstreamOutput"], source)

    def test_sexpr_chain_keeps_upstream_output(self):
        ir = self._ir()
        expected = ir["nodes"][2]
        back, source = _sexpr_rt(ir)
        node = _nodes_of(back, "assignment")[-1]
        self.assertEqual(node.get("upstreamOutput"), expected["upstreamOutput"], source)
        self.assertEqual(node.get("outputType"), expected["outputType"], source)

    def test_plain_assignment_emits_unchanged_statement_form(self):
        """无附加字段的赋值保持 `n = expr`（字节形态不引入 ASSIGN）。"""
        ir = _flow([
            _start(nxt="a"),
            {"type": "assignment", "id": "a", "output": "$INPUT.x", "next": "e"},
            _end(),
        ])
        source = emit_source(ir)
        self.assertIn("    a = INPUT.x\n", source)
        self.assertNotIn("ASSIGN(", source)


class TestL6HttpDataStringTypes(unittest.TestCase):
    def test_headers_and_body_string_values_do_not_drift(self):
        headers = {"X-Num": "42", "X-Bool": "true", "X-Str": "ok"}
        body = {"n": "1", "b": "false", "s": "42"}
        ir = _flow([
            _start(nxt="h"),
            {"type": "http", "id": "h", "method": "POST",
             "url": "https://example.com", "headers": headers, "body": body,
             "next": "e"},
            _end(),
        ])
        back, source = _sexpr_rt(ir)
        node = _nodes_of(back, "http")[0]
        self.assertEqual(node.get("headers"), headers, source)
        self.assertEqual(node.get("body"), body, source)

    def test_expression_strings_stay_expressions(self):
        ir = _flow([
            _start(nxt="h"),
            {"type": "http", "id": "h", "method": "POST", "url": "https://example.com",
             "body": {"k": "$INPUT.value"}, "next": "e"},
            _end(),
        ])
        back, source = _sexpr_rt(ir)
        self.assertEqual(_nodes_of(back, "http")[0].get("body"),
                         {"k": "$INPUT.value"}, source)


if __name__ == "__main__":
    unittest.main()
