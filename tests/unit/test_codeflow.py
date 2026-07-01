"""Tests for plaita.dsl.codeflow — AST 版 Python 前端。

注意：被 ``@flow`` / ``@childflow`` 装饰的函数必须定义在模块级，``inspect.getsource``
才能取到源码做 AST 编译。
"""

import unittest

from plaita.dsl.codeflow import (
    flow, childflow, MAP, FILTER, FIND, LOOP, CHILD, F, NODE, HTTP, ErrorHandler,
)


@flow("adult_check", input_type="object", desc="判断成年")
def adult_check(INPUT):
    if INPUT.age >= 18:
        return "成年"
    return "未成年"


@flow("grade", input_type="object")
def grade(INPUT):
    if INPUT.score >= 90:
        return "A"
    elif INPUT.score >= 60:
        return "B"
    else:
        return "C"


@flow("greet", input_type="object")
def greet(INPUT):
    name = F.upper(INPUT.name)
    return F.concat("hi ", name)


@flow("double_numbers", input_type="object")
def double_numbers(INPUT):
    for x in MAP(INPUT.numbers, id="dbl"):
        return F.mul(x, 2)
    return NODE.dbl


@flow("evens", input_type="object")
def evens(INPUT):
    for x in FILTER(INPUT.nums, id="flt"):
        if F.mod(x, 2) == 0:
            return True
        return False
    return NODE.flt


@flow("first_even", input_type="object")
def first_even(INPUT):
    for x in FIND(INPUT.nums, id="fd"):
        if F.mod(x, 2) == 0:
            return True
        return False
    return NODE.fd


@flow("loop_echo", input_type="object")
def loop_echo(INPUT):
    for x in LOOP(INPUT.nums, id="lp"):
        return x
    return NODE.lp


@childflow(input_type="object")
def double_each(INPUT):
    return F.mul(INPUT.item, 2)


@flow("double_via_child", input_type="object")
def double_via_child(INPUT):
    r = CHILD(input={"item": INPUT.payload}, flow=double_each)
    return r


@flow("with_and", input_type="object")
def with_and(INPUT):
    if INPUT.age >= 18 and INPUT.vip == True:  # noqa: E712
        return "通过"
    return "拒绝"


@flow("with_not", input_type="object")
def with_not(INPUT):
    if not (INPUT.role == "blocked"):
        return "通过"
    return "拒绝"


class TestCodeflowBasic(unittest.TestCase):
    def test_adult_check(self):
        self.assertEqual(adult_check.run(age=20), "成年")
        self.assertEqual(adult_check.run(age=15), "未成年")

    def test_elif_chain(self):
        self.assertEqual(grade.run(score=95), "A")
        self.assertEqual(grade.run(score=70), "B")
        self.assertEqual(grade.run(score=30), "C")

    def test_assignment_and_f_func(self):
        self.assertEqual(greet.run(name="alice"), "hi ALICE")


class TestCodeflowCollections(unittest.TestCase):
    def test_map(self):
        self.assertEqual(double_numbers.run(numbers=[1, 2, 3, 4]), [2, 4, 6, 8])

    def test_filter(self):
        self.assertEqual(evens.run(nums=[1, 2, 3, 4, 6]), [2, 4, 6])

    def test_find(self):
        self.assertEqual(first_even.run(nums=[1, 3, 4, 6]), 4)

    def test_loop(self):
        # loop 返回最后一次结果
        self.assertEqual(loop_echo.run(nums=[10, 20, 30]), 30)


class TestCodeflowChild(unittest.TestCase):
    def test_child_subflow(self):
        self.assertEqual(double_via_child.run(payload=21), 42)


class TestCodeflowConditions(unittest.TestCase):
    def test_and_group(self):
        self.assertEqual(with_and.run(age=20, vip=True), "通过")
        self.assertEqual(with_and.run(age=20, vip=False), "拒绝")
        self.assertEqual(with_and.run(age=15, vip=True), "拒绝")

    def test_not_negation(self):
        self.assertEqual(with_not.run(role="normal"), "通过")
        self.assertEqual(with_not.run(role="blocked"), "拒绝")


class TestCodeflowIR(unittest.TestCase):
    def test_compiles_to_flow_with_nodes(self):
        from plaita.dsl.codeflow import compile_func
        d = compile_func(adult_check.__wrapped__, "adult_check")
        types = [n["type"] for n in d["nodes"]]
        self.assertIn("start", types)
        self.assertIn("if", types)
        self.assertIn("end", types)

    def test_expression_compiles_to_dollar_form(self):
        from plaita.dsl.codeflow import compile_func
        d = compile_func(greet.__wrapped__, "greet")
        assign = [n for n in d["nodes"] if n.get("type") == "assignment"][0]
        self.assertEqual(assign["output"], "$F.upper($INPUT.name)")
        end = [n for n in d["nodes"] if n.get("type") == "end"][0]
        self.assertIn("$F.concat", end["output"])
        self.assertIn("$NODE.name", end["output"])


@flow("http_with_error_handler", input_type="object")
def http_with_error_handler(INPUT):
    if INPUT.age >= 18:
        resp = HTTP.post(
            url="https://api.example.com/users",
            body={"name": INPUT.name},
            timeout="PT5S",
            on_error=ErrorHandler("continue_with", default={"data": None}),
        )
        return resp.data
    return "未成年"


class TestCodeflowHttp(unittest.TestCase):
    def test_http_compiles_and_validates(self):
        # 编译 + Flow.model_validate 都通过（含 continue_with 错误处理）
        self.assertEqual(http_with_error_handler.flow_id, "http_with_error_handler")
        from plaita.dsl.codeflow import compile_func
        d = compile_func(http_with_error_handler.__wrapped__, "http_with_error_handler")
        http_nodes = [n for n in d["nodes"] if n.get("type") == "http"]
        self.assertEqual(len(http_nodes), 1)
        h = http_nodes[0]
        self.assertEqual(h["method"], "POST")
        self.assertEqual(h["url"], "https://api.example.com/users")
        self.assertEqual(h["body"], {"name": "$INPUT.name"})
        self.assertEqual(h["errorHandler"]["strategy"], "continue_with")
        self.assertEqual(h["errorHandler"]["defaultValue"], {"data": None})


if __name__ == "__main__":
    unittest.main()
