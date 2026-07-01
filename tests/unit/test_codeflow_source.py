"""Tests for plaita.dsl.codeflow — flow_from_source 源码模式（运行期生成）。"""

import unittest

from plaita.dsl.codeflow import flow_from_source, compile_source


class TestFlowFromSource(unittest.TestCase):
    def test_simple_if(self):
        src = '''
        def adult_check(INPUT):
            if INPUT.age >= 18:
                return "成年"
            return "未成年"
        '''
        f = flow_from_source(src, flow_id="adult_check", input_type="object")
        self.assertEqual(f.run(age=20), "成年")
        self.assertEqual(f.run(age=15), "未成年")

    def test_http_and_error_handler(self):
        src = '''
        def create_user(INPUT):
            if INPUT.age >= 18:
                resp = HTTP.post(
                    url="https://api.example.com/users",
                    body={"name": INPUT.name},
                    timeout="PT5S",
                    on_error=ErrorHandler("continue_with", default={"data": None}),
                )
                return resp.data
            return "未成年"
        '''
        d = compile_source(src, flow_id="create_user", input_type="object")
        h = [n for n in d["nodes"] if n["type"] == "http"][0]
        self.assertEqual(h["method"], "POST")
        self.assertEqual(h["body"], {"name": "$INPUT.name"})
        self.assertEqual(h["errorHandler"]["strategy"], "continue_with")
        self.assertEqual(h["errorHandler"]["defaultValue"], {"data": None})
        f = flow_from_source(src, flow_id="create_user", input_type="object")
        self.assertEqual(f.flow_id, "create_user")

    def test_childflow_reference(self):
        src = '''
        @childflow(input_type="object")
        def double_each(INPUT):
            return F.mul(INPUT.item, 2)

        @flow("double_via_child", input_type="object")
        def double_via_child(INPUT):
            r = CHILD(input={"item": INPUT.payload}, flow=double_each)
            return r
        '''
        f = flow_from_source(src)
        self.assertEqual(f.run(payload=21), 42)

    def test_decorator_opts_extracted(self):
        src = '''
        @flow("grade", input_type="object", desc="分级")
        def grade(INPUT):
            if INPUT.score >= 90:
                return "A"
            elif INPUT.score >= 60:
                return "B"
            else:
                return "C"
        '''
        f = flow_from_source(src)
        self.assertEqual(f.flow_id, "grade")
        self.assertEqual(f.desc, "分级")
        self.assertEqual(f.run(score=95), "A")
        self.assertEqual(f.run(score=70), "B")
        self.assertEqual(f.run(score=30), "C")

    def test_explicit_opts_override_decorator(self):
        src = '''
        @flow("grade", input_type="object", desc="装饰器里的描述")
        def grade(INPUT):
            return INPUT.x
        '''
        f = flow_from_source(src, desc="显式描述")
        self.assertEqual(f.desc, "显式描述")

    def test_multiple_candidates_requires_flow_id(self):
        src = '''
        def foo(INPUT):
            return INPUT.x
        def bar(INPUT):
            return INPUT.y
        '''
        with self.assertRaises(Exception):
            flow_from_source(src)

    def test_named_flow_id_picks_function(self):
        src = '''
        def foo(INPUT):
            return INPUT.x
        def bar(INPUT):
            return INPUT.y
        '''
        f = flow_from_source(src, flow_id="bar", input_type="object")
        self.assertEqual(f.run(y=5), 5)


if __name__ == "__main__":
    unittest.main()
