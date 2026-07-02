"""@flow 自定义节点占位符支持 —— 大写名 → registry 查 node_type。

覆盖：
- 自定义节点编译成 ``{"type": <node_type>, ...}`` IR，字段经表达式编译；
- 赋值变量名作为节点 id，``$NODE.<name>`` 引用可达；
- 表达式语句 + ``id=`` 命名；
- ``{% ... %}`` 模板字符串透传给节点 ``execute``；
- 错误场景：表达式位置调用、未注册大写名、位置参数、``id=`` 与赋值冲突；
- 内置占位符（HTTP 等）不回归。
"""
from __future__ import annotations

import unittest
from typing import Any, ClassVar, Optional

from plaita import Node
from plaita.node import NodeRegistry, get_default_registry
from plaita.dsl.codeflow import compile_source, flow_from_source, _CodeflowError  # type: ignore


# ---------------------------------------------------------------------------
# 测试用自定义节点
# ---------------------------------------------------------------------------

class EchoNode(Node):
    node_type: ClassVar[str] = "codeflow_test_echo"
    text: Optional[str] = None
    upper: bool = False

    def execute(self, execution):
        t = execution.evaluate(self.text) if self.text else ""
        return str(t).upper() if self.upper else str(t)


class SumNode(Node):
    node_type: ClassVar[str] = "codeflow_test_sum"
    a: Optional[Any] = None
    b: Optional[Any] = None

    def execute(self, execution):
        av = execution.evaluate(self.a) if self.a is not None else 0
        bv = execution.evaluate(self.b) if self.b is not None else 0
        return av + bv


def _register_test_nodes(reg: NodeRegistry) -> None:
    reg.register(EchoNode)
    reg.register(SumNode)


# ---------------------------------------------------------------------------
# 辅助：用一个隔离 registry 构造 _CompileCtx 已知类型集，避免依赖全局 registry 状态
# ---------------------------------------------------------------------------

def _compile(src: str, reg: NodeRegistry):
    """编译源码，使用给定 registry 的类型集识别自定义节点。"""
    from plaita.dsl import codeflow as cf

    known = set(reg.list_types())
    src_dedented = src
    import ast as _ast, textwrap as _tw
    mod = _ast.parse(_tw.dedent(src_dedented))
    # 找主函数（@flow 装饰或唯一函数）
    chosen = None
    for stmt in mod.body:
        if isinstance(stmt, _ast.FunctionDef):
            chosen = stmt
            break
    assert chosen is not None
    return cf._compile_fdef(chosen, "test", {}, known_node_types=known)  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# 测试
# ---------------------------------------------------------------------------

class TestCustomNodeCompile(unittest.TestCase):
    """编译期：IR 形态、id、字段、source_line。"""

    @classmethod
    def setUpClass(cls):
        cls.reg = NodeRegistry()
        _register_test_nodes(cls.reg)

    def test_assignment_uses_var_name_as_id(self):
        ir = _compile('''
            @flow("t")
            def t(INPUT):
                a = CODEFLOW_TEST_ECHO(text=INPUT.msg, upper=True)
                return a
        ''', self.reg)
        nodes = ir["nodes"]
        echo = next(n for n in nodes if n["type"] == "codeflow_test_echo")
        self.assertEqual(echo["id"], "a")
        self.assertEqual(echo["text"], "$INPUT.msg")
        self.assertIs(echo["upper"], True)
        # 引用 a 编译成 $NODE.a
        end = next(n for n in nodes if n["type"] == "end")
        self.assertEqual(end["output"], "$NODE.a")

    def test_expr_stmt_with_id_kwarg(self):
        ir = _compile('''
            @flow("t")
            def t(INPUT):
                CODEFLOW_TEST_ECHO(text="hi", id="greet")
                return "done"
        ''', self.reg)
        echo = next(n for n in ir["nodes"] if n["type"] == "codeflow_test_echo")
        self.assertEqual(echo["id"], "greet")
        self.assertEqual(echo["text"], "hi")

    def test_dict_field_compiled(self):
        ir = _compile('''
            @flow("t")
            def t(INPUT):
                r = CODEFLOW_TEST_SUM(a=INPUT.x, b=INPUT.y)
                return r
        ''', self.reg)
        s = next(n for n in ir["nodes"] if n["type"] == "codeflow_test_sum")
        self.assertEqual(s["a"], "$INPUT.x")
        self.assertEqual(s["b"], "$INPUT.y")

    def test_source_line_annotated(self):
        ir = _compile('''
            @flow("t")
            def t(INPUT):
                a = CODEFLOW_TEST_ECHO(text=INPUT.msg)
                return a
        ''', self.reg)
        echo = next(n for n in ir["nodes"] if n["type"] == "codeflow_test_echo")
        # 三引号首行换行 + @flow=第2行 + def=第3行 + ECHO=第4行
        self.assertEqual(echo["source_line"], 4)

    def test_template_string_passthrough(self):
        ir = _compile('''
            @flow("t")
            def t(INPUT):
                a = CODEFLOW_TEST_ECHO(text="x:{% $INPUT.msg %}")
                return a
        ''', self.reg)
        echo = next(n for n in ir["nodes"] if n["type"] == "codeflow_test_echo")
        # {% %} 模板字符串原样透传，由节点 execute 自行求值
        self.assertEqual(echo["text"], "x:{% $INPUT.msg %}")


class TestCustomNodeRun(unittest.TestCase):
    """运行期：端到端执行。"""

    @classmethod
    def setUpClass(cls):
        _register_test_nodes(get_default_registry())

    def test_run_echo(self):
        fl = flow_from_source('''
            @flow("echo_flow")
            def echo_flow(INPUT):
                a = CODEFLOW_TEST_ECHO(text=INPUT.msg, upper=True)
                return a
        ''')
        self.assertEqual(fl.run(msg="hello"), "HELLO")

    def test_run_two_nodes_and_reference(self):
        fl = flow_from_source('''
            @flow("two")
            def two(INPUT):
                s = CODEFLOW_TEST_SUM(a=INPUT.x, b=INPUT.y)
                e = CODEFLOW_TEST_ECHO(text="ok")
                return F.concat(s, "-", e)
        ''')
        self.assertEqual(fl.run(x=2, y=3), "5-ok")


class TestCustomNodeErrors(unittest.TestCase):
    """错误场景 + AI 自纠友好的报错。"""

    @classmethod
    def setUpClass(cls):
        cls.reg = NodeRegistry()
        _register_test_nodes(cls.reg)

    def test_expression_position_rejected(self):
        with self.assertRaises(_CodeflowError) as cm:
            _compile('''
                @flow("t")
                def t(INPUT):
                    return CODEFLOW_TEST_ECHO(text=INPUT.msg)
            ''', self.reg)
        self.assertIn("只能作为语句或赋值右侧", str(cm.exception))

    def test_unregistered_upper_lists_available(self):
        with self.assertRaises(_CodeflowError) as cm:
            _compile('''
                @flow("t")
                def t(INPUT):
                    TOTALLY_UNKNOWN_NODE(a=1)
                    return 1
            ''', self.reg)
        msg = str(cm.exception)
        self.assertIn("TOTALLY_UNKNOWN_NODE", msg)
        self.assertIn("codeflow_test_echo", msg)  # 列出可用类型

    def test_positional_args_rejected(self):
        with self.assertRaises(_CodeflowError) as cm:
            _compile('''
                @flow("t")
                def t(INPUT):
                    a = CODEFLOW_TEST_ECHO(INPUT.msg)
                    return a
            ''', self.reg)
        self.assertIn("只接受关键字参数", str(cm.exception))

    def test_id_kwarg_with_assignment_rejected(self):
        with self.assertRaises(_CodeflowError) as cm:
            _compile('''
                @flow("t")
                def t(INPUT):
                    a = CODEFLOW_TEST_ECHO(text="x", id="foo")
                    return a
            ''', self.reg)
        self.assertIn("不要同时传 id=", str(cm.exception))


class TestBuiltinNonRegression(unittest.TestCase):
    """内置占位符（HTTP/CODE/...）不被自定义路径误吞。"""

    def test_http_still_compiles(self):
        # HTTP 是内置专用占位符，不依赖 registry 自定义路径
        ir = compile_source('''
            @flow("h")
            def h(INPUT):
                r = HTTP.get(url="https://example.com")
                return r
        ''')
        http = next(n for n in ir["nodes"] if n["type"] == "http")
        self.assertEqual(http["method"], "GET")
        self.assertEqual(http["url"], "https://example.com")


if __name__ == "__main__":
    unittest.main()
