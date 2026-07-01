"""Tests for plaita.io_format — JSON/YAML 自动识别与文件加载。"""

import json
import os
import unittest

from plaita.core.flow import Flow, parse, parse_and_run
from plaita.io_format import loads, load_file

try:
    import yaml  # noqa: F401

    _HAS_YAML = True
except ImportError:
    _HAS_YAML = False

_FIXTURE_DIR = os.path.join(os.path.dirname(__file__), os.pardir, "fixture")


class TestLoadsJson(unittest.TestCase):
    def test_loads_json_object(self):
        data = loads('{"flow_id": "x", "nodes": []}')
        self.assertEqual(data["flow_id"], "x")

    def test_loads_empty_returns_empty_dict(self):
        self.assertEqual(loads(""), {})
        self.assertEqual(loads("   \n"), {})

    def test_loads_invalid_json_raises(self):
        with self.assertRaises(Exception):
            loads("{not valid")


class TestLoadsYaml(unittest.TestCase):
    def setUp(self):
        if not _HAS_YAML:
            self.skipTest("PyYAML 未安装")

    def test_loads_yaml_mapping(self):
        data = loads("flow_id: x\nnodes: []\n")
        self.assertEqual(data["flow_id"], "x")
        self.assertEqual(data["nodes"], [])

    def test_loads_yaml_non_mapping_raises(self):
        with self.assertRaises(RuntimeError):
            loads("- 1\n- 2\n")

    def test_yaml_flow_executes(self):
        text = (
            "flow_id: adult_check\n"
            "inputType: { dataType: object }\n"
            "nodes:\n"
            "  - { type: start, id: start, next: check }\n"
            "  - type: if\n"
            "    id: check\n"
            '    condition: { field: "$INPUT.age", operator: gte, value: 18 }\n'
            "    next: adult\n"
            "    else_next: minor\n"
            '  - { type: end, id: adult, output: "成年", resultType: success }\n'
            '  - { type: end, id: minor, output: "未成年", resultType: success }\n'
        )
        flow = Flow.from_string(text)
        self.assertEqual(flow.run(age=20), "成年")
        self.assertEqual(flow.run(age=15), "未成年")


class TestParseEntrypoint(unittest.TestCase):
    def setUp(self):
        if not _HAS_YAML:
            self.skipTest("PyYAML 未安装")

    def test_parse_accepts_yaml_string(self):
        flow = parse("flow_id: echo\ninputType: { dataType: object }\nnodes:\n"
                     "  - { type: start, id: start, next: e }\n"
                     '  - { type: end, id: e, output: "$INPUT.x", resultType: success }\n')
        self.assertEqual(flow.run(x="hi"), "hi")

    def test_parse_still_accepts_json_string(self):
        flow = parse(json.dumps({
            "flow_id": "echo",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "e"},
                {"type": "end", "id": "e", "output": "$INPUT.x", "resultType": "success"},
            ],
        }))
        self.assertEqual(flow.run(x="hi"), "hi")

    def test_parse_accepts_dict(self):
        flow = parse({"flow_id": "echo", "nodes": []})
        self.assertEqual(flow.flow_id, "echo")

    def test_parse_empty_returns_none(self):
        self.assertIsNone(parse(""))
        self.assertIsNone(parse(None))

    def test_parse_and_run_yaml(self):
        text = (
            "flow_id: echo\n"
            "inputType: { dataType: object }\n"
            "nodes:\n"
            "  - { type: start, id: start, next: e }\n"
            '  - { type: end, id: e, output: "$INPUT.x", resultType: success }\n'
        )
        self.assertEqual(parse_and_run(text, x="yo"), "yo")


class TestLoadFile(unittest.TestCase):
    def setUp(self):
        if not _HAS_YAML:
            self.skipTest("PyYAML 未安装")

    def test_load_yaml_file_by_extension(self):
        path = os.path.join(_FIXTURE_DIR, "adult_check.yaml")
        flow = Flow.from_file(path)
        self.assertEqual(flow.flow_id, "adult_check")
        self.assertEqual(flow.run(age=20), "成年")
        self.assertEqual(flow.run(age=10), "未成年")

    def test_load_json_file_by_extension(self):
        path = os.path.join(_FIXTURE_DIR, "assigment.json")
        flow = Flow.from_file(path)
        self.assertEqual(flow.run({"bb": "123456"})["dd"], "123456")


if __name__ == "__main__":
    unittest.main()
