import unittest

from plaita.io import evaluate


class EvaluateTestCase(unittest.TestCase):

    def test_evaluate_string(self):
        ret = evaluate("hello", None)
        self.assertEqual(ret, "hello")

    def test_evaluate_variable(self):
        ret = evaluate("$INPUT", {"$INPUT": "hello"})
        self.assertEqual("hello", ret)

    def test_evaluate_variable_with_array(self):
        ret = evaluate("$INPUT[0]", {"$INPUT": ["hello"]})
        self.assertEqual("hello", ret)

    def test_evaluate_variable_with_array_by_dot(self):
        ret = evaluate("$INPUT.0", {"$INPUT": ["hello"]})
        self.assertEqual("hello", ret)

    def test_evaluate_deep_array_by_slice(self):
        ret = evaluate("$INPUT[0].name", {"$INPUT": [{"name": "hello"}]})
        self.assertEqual("hello", ret)

    def test_evaluate_deep_array_by_slice_negative(self):
        ret = evaluate("$INPUT[-1].name", {"$INPUT": [{"name": "hello"}]})
        self.assertEqual("hello", ret)

    def test_evaluate_depp_array_with_slice_end(self):
        ret = evaluate("$INPUT.names[0]", {"$INPUT": {"names": ["hello"]}})
        self.assertEqual("hello", ret)

    def test_evaluate_deep_array_by_dot(self):
        ret = evaluate("$INPUT.0.name", {"$INPUT": [{"name": "hello"}]})
        self.assertEqual("hello", ret)

    def test_evaluate_deep_array_by_dot_last(self):
        ret = evaluate("$INPUT.users.0", {"$INPUT": {"users": [{"name": "hello"}]}})
        self.assertEqual({"name": "hello"}, ret)

    def test_evaluate_expression(self):
        ret = evaluate("my name is {%$INPUT[0].name%}", {"$INPUT": [{"name": "hello"}]})
        self.assertEqual("my name is hello", ret)

    def test_evaluate_expression_wrong_prefix(self):
        ret = evaluate("my name is {%@INPUT[0].name%}", {"$INPUT": [{"name": "hello"}]})
        self.assertEqual("my name is {%@INPUT[0].name%}", ret)

    def test_evaluate_with_brace(self):
        express = "{user.name}"
        context = {"user": {"name": "tom"}}
        ret = evaluate(express, context)
        self.assertEqual("{user.name}", ret)

    def test_function_call(self):
        express = "$F.add(1, 2)"
        context = {}
        ret = evaluate(express, context)
        self.assertEqual(3, ret)

    def test_inner_function_call(self):
        express = "$F.add($F.add(1, 2), 3)"
        context = {}
        ret = evaluate(express, context)
        self.assertEqual(6, ret)

    def test_inner_function_call_with_variable(self):
        express = "$F.add($F.add(1, 2), $INPUT.value)"
        context = {"$INPUT": {"value": 3}}
        ret = evaluate(express, context)
        self.assertEqual(6, ret)

    def test_function_call_with_root_variable(self):
        express = "$F.add(3, $INPUT)"
        context = {"$INPUT": 3}
        ret = evaluate(express, context)
        self.assertEqual(6, ret)

    def test_function_with_template(self):
        express = "let see: {%$F.add(3, $INPUT)%}"
        context = {"$INPUT": 3}
        ret = evaluate(express, context)
        express_with_space = "let see: {% $F.add(3, $INPUT) %}"
        ret2 = evaluate(express_with_space, context)
        self.assertEqual("let see: 6", ret)
        self.assertEqual("let see: 6", ret2)

    def test_var_with_multiple_lines(self):
        express = """以下是生成的嫦娥奔月图片：
![]({%$NODE.step1.data.0.url%})
        """
        context = {"$NODE": {"step1": {"data": [{"url": "http://example.com"}]}}}
        ret = evaluate(express, context)
        self.assertEqual("以下是生成的嫦娥奔月图片：\n![](http://example.com)\n        ", ret)

    def test_function_with_variable_args(self):
        express = '$F.concat("1", "2", "3", "4")'
        ret = evaluate(express, {})
        self.assertEqual("1234", ret)

    def test_function_call_with_deep_variable(self):
        express = "$F.sub($INPUT.test.value, 2)"
        ret = evaluate(express, {"$INPUT": {"test": {"value": 3}}})
        self.assertEqual(1, ret)

    def test_function_call_with_optional_default_args(self):
        express_with_default = "$F.getDictValue($INPUT.test, 'value1', 2)"
        ret_with_default = evaluate(express_with_default, {"$INPUT": {"test": {"value": 3}}})
        self.assertEqual(2, ret_with_default)
        express_without_default = "$F.getDictValue($INPUT.test, 'value')"
        ret_without_default = evaluate(express_without_default, {"$INPUT": {"test": {"value": 3}}})
        self.assertEqual(3, ret_without_default)
        express_with_null_default = "$F.getDictValue($INPUT.test, 'value',)"
        ret_with_null_default = evaluate(express_with_null_default, {"$INPUT": {"test": {"value": 3}}})
        self.assertEqual(3, ret_with_null_default)

    def test_function_call_with_unfixed_args(self):
        express_concat = "$F.concat($INPUT.test.value, '2', '3')"
        ret_concat = evaluate(express_concat, {"$INPUT": {"test": {"value": "1"}}})
        self.assertEqual("123", ret_concat)
        express_or = "$F.or($INPUT.test.value, 0, '3')"
        ret_or = evaluate(express_or, {"$INPUT": {"test": {"value": ""}}})
        self.assertEqual("3", ret_or)
