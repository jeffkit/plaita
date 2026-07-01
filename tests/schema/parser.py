from unittest import TestCase

from plaita import parse


class ParserTestCase(TestCase):

    def test_parse_basic_structure(self):
        # 解释基本结构
        content = """
{
    "flow_id": "echo",
    "version": "0.1",
    "runtime": "python",
    "inputType": { "dataType": "object" },
    "nodes": [
        {
            "type": "start",
            "id": "start",
            "next": "end"
        },
        {
            "type": "end",
            "id": "end",
            "output": "$INPUT.name",
            "resultType": "success"
        }
    ]
}
        """
        flow = parse(content)
        self.assertEqual(flow.flow_id, "echo")
        self.assertEqual(len(flow.nodes), 2)
        # 节点连线通过每个节点的 next 字段表示
        self.assertEqual(flow.nodes[0].next, "end")


if __name__ == "__main__":
    import unittest
    unittest.main()
