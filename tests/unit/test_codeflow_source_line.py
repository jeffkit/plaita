"""Tests for @flow 源码行号回标。

校验两件事：
1. 编译期：``@flow`` 编译产物的 IR 节点带 ``source_line``，且 ``Flow.model_validate``
   后 ``Node.source_line`` 被正确吃下（含子流程/集合节点的嵌套节点）。
2. 运行期：节点抛错时，``NodeExecutionError`` 携带 ``source_line`` 且消息含源码行号，
   便于从运行期错误定位回用户书写的 Python 源码行。

JSON / S-expr / Builder 前端不产生 ``source_line``，对应 ``Node.source_line`` 为 None，
运行期错误消息不附加 ``(源码第 N 行)`` 后缀——这一行为也在此覆盖。
"""

import unittest

from plaita.core.errors import NodeExecutionError
from plaita.dsl.codeflow import compile_source, flow_from_source


class TestCodeflowSourceLineIR(unittest.TestCase):
    def test_nodes_carry_source_line(self):
        src = '''
def adult_check(INPUT):
    if INPUT.age >= 18:
        return "成年"
    return "未成年"
'''
        d = compile_source(src, flow_id="adult_check")
        # start 是合成节点，不带 source_line；if / end 应带行号。
        # 三引号首行是空行，故 def 在第 2 行、if 在第 3 行、两个 return 在 4/5 行。
        start = [n for n in d["nodes"] if n["type"] == "start"][0]
        if_node = [n for n in d["nodes"] if n["type"] == "if"][0]
        ends = [n for n in d["nodes"] if n["type"] == "end"]

        self.assertNotIn("source_line", start)
        self.assertEqual(if_node["source_line"], 3)
        self.assertEqual({e["source_line"] for e in ends}, {4, 5})

    def test_node_model_pickup_source_line(self):
        src = '''
def greet(INPUT):
    name = F.upper(INPUT.name)
    return F.concat("hi ", name)
'''
        flow = flow_from_source(src, flow_id="greet")
        assign = [n for n in flow.nodes if n.node_type == "assignment"][0]
        end = [n for n in flow.nodes if n.node_type == "end"][0]
        self.assertEqual(assign.source_line, 3)
        self.assertEqual(end.source_line, 4)

    def test_node_call_carries_call_site_line(self):
        src = '''
def create_user(INPUT):
    if INPUT.age >= 18:
        resp = HTTP.post(
            url="https://api.example.com/users",
            body={"name": INPUT.name},
        )
        return resp.data
    return "未成年"
'''
        d = compile_source(src, flow_id="create_user")
        http = [n for n in d["nodes"] if n["type"] == "http"][0]
        # 赋值 `resp = HTTP.post(...)` 起于第 4 行
        self.assertEqual(http["source_line"], 4)

    def test_collection_childflow_nodes_carry_source_line(self):
        src = '''
def double_numbers(INPUT):
    for x in MAP(INPUT.numbers, id="dbl"):
        return F.mul(x, 2)
    return NODE.dbl
'''
        d = compile_source(src, flow_id="double_numbers")
        map_node = [n for n in d["nodes"] if n["type"] == "map"][0]
        # for ... MAP(...) 在第 3 行
        self.assertEqual(map_node["source_line"], 3)
        child_end = [n for n in map_node["childFlow"]["nodes"] if n["type"] == "end"][0]
        # 子流程体里的 return 在第 4 行
        self.assertEqual(child_end["source_line"], 4)

    def test_json_frontend_has_no_source_line(self):
        from plaita.core.flow import Flow

        data = {
            "flow_id": "plain",
            "runtime": "python",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "s", "next": "e"},
                {"type": "end", "id": "e", "output": "ok"},
            ],
        }
        flow = Flow.model_validate(data)
        for n in flow.nodes:
            self.assertIsNone(n.source_line)


class TestCodeflowRuntimeErrorSourceLine(unittest.TestCase):
    def test_runtime_error_carries_source_line(self):
        # CODE 节点运行 1/0 触发 ZeroDivisionError -> NodeExecutionError
        src = (
            "def boom(INPUT):\n"
            "    x = CODE.python(\"def run(x):\\n    return 1/0\", input=INPUT.n)\n"
            "    return x\n"
        )
        flow = flow_from_source(src, flow_id="boom")
        # CODE 节点位于源码第 2 行
        code_node = [n for n in flow.nodes if n.node_type == "code"][0]
        self.assertEqual(code_node.source_line, 2)

        with self.assertRaises(NodeExecutionError) as cm:
            flow.run(n=1)
        err = cm.exception
        self.assertEqual(err.source_line, 2)
        self.assertIn("源码第 2 行", err.message)

    def test_json_frontend_runtime_error_has_no_source_line(self):
        # 非前端产生的 flow：节点没有 source_line，错误消息不应附加行号后缀
        from plaita.core.flow import Flow

        data = {
            "flow_id": "plain",
            "runtime": "python",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "s", "next": "c"},
                {
                    "type": "code", "id": "c", "language": "python",
                    "code": "def run(x):\n    return 1/0\n",
                    "input": "$INPUT.n", "next": "e",
                },
                {"type": "end", "id": "e", "output": "$NODE.c"},
            ],
        }
        flow = Flow.model_validate(data)
        with self.assertRaises(NodeExecutionError) as cm:
            flow.run(n=1)
        err = cm.exception
        self.assertIsNone(err.source_line)
        self.assertNotIn("源码第", err.message)


if __name__ == "__main__":
    unittest.main()
