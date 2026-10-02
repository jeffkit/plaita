"""clean 修复包 C1-1 — codeflow 自引用赋值编译期拦截 + 重复赋值误导文案。

问题（2026-10 评审）：
1. ``x = x + 1`` 编译通过——RHS 的 ``x`` 解析成自指 ``$NODE.x``，运行期
   ``TypeError: NoneType + int``。现编译期显式拦截（含三元与显式 ``NODE.x``
   形态），报带行号的可操作错误。
2. ``x = 1; x = 2`` 报 ``ValueError: 节点 id 重复: 'x'``——赋值场景的重复
   被表述成节点 id 冲突，误导。现在按赋值语义单独措辞。

回归边界：合法链式赋值（y = x + 1）不受影响；跨分支同名赋值的既有拦截
语义不变（历史决策：codeflow 不支持跨分支给同一变量赋值，join 点引用
分支内赋值的名字仍是「未知名字」错误）；保留命名空间同名赋值
（``NODE = NODE.x``）不误报。
"""

import ast
import unittest

from plaita.dsl.codeflow import compile_source, flow_from_source
from plaita.dsl.codeflow._common import _CodeflowError


def _compile_body(src: str):
    """compile_source 包一层，错误消息更可读。"""
    return compile_source(src)


class SelfReferenceAssignTest(unittest.TestCase):
    def test_plain_self_reference_rejected_with_line(self):
        """x = x + 1 编译期报错，带行号与可操作建议。"""
        with self.assertRaises(_CodeflowError) as cm:
            _compile_body("def f(INPUT):\n    x = x + 1\n    return x")
        msg = str(cm.exception)
        self.assertIn("第 2 行", msg)
        self.assertIn("自引用", msg)
        self.assertIn("中间变量", msg)

    def test_ternary_self_reference_rejected(self):
        """三元自引用（x = x if ... else ...）同样拦截。"""
        with self.assertRaises(_CodeflowError) as cm:
            _compile_body("def f(INPUT):\n    x = 1 if INPUT.a else x\n    return x")
        self.assertIn("自引用", str(cm.exception))

        # 反向分支引用
        with self.assertRaises(_CodeflowError):
            _compile_body("def f(INPUT):\n    x = x if INPUT.a else 1\n    return x")

    def test_explicit_node_ref_self_reference_rejected(self):
        """显式 NODE.<name> 形态也是自引用（赋值节点 id 即变量名）。"""
        with self.assertRaises(_CodeflowError) as cm:
            _compile_body("def f(INPUT):\n    x = NODE.x + 1\n    return x")
        self.assertIn("自引用", str(cm.exception))

    def test_self_reference_inside_node_call_args_rejected(self):
        """节点调用参数里的自引用同样拦截（x = HTTP(input={"p": x})）。"""
        with self.assertRaises(_CodeflowError):
            _compile_body(
                'def f(INPUT):\n'
                '    x = HTTP(url="http://a.com", input={"p": x})\n'
                '    return x'
            )

    def test_runtime_typeerror_no_longer_possible_for_this_shape(self):
        """回归锚点：修复前该 flow 编译通过、运行期 NoneType + int 炸掉。"""
        fl = flow_from_source("def f(INPUT):\n    x = INPUT.n\n    y = x + 1\n    return y")
        self.assertEqual(fl.run({"n": 41}), 42)


class DuplicateAssignMessageTest(unittest.TestCase):
    def test_duplicate_plain_assign_new_message(self):
        """x = 1; x = 2 报赋值语义错误，不再是「节点 id 重复」。"""
        with self.assertRaises(_CodeflowError) as cm:
            _compile_body("def f(INPUT):\n    x = 1\n    x = 2\n    return x")
        msg = str(cm.exception)
        self.assertIn("被多次赋值", msg)
        self.assertIn("每个赋值是一个节点", msg)
        self.assertNotIn("节点 id 重复", msg)

    def test_duplicate_node_call_assign_new_message(self):
        """节点调用赋值重复（x = HTTP(...); x = HTTP(...)）同一文案。"""
        with self.assertRaises(_CodeflowError) as cm:
            _compile_body(
                'def f(INPUT):\n'
                '    x = HTTP(url="http://a.com")\n'
                '    x = HTTP(url="http://b.com")\n'
                '    return x'
            )
        self.assertIn("被多次赋值", str(cm.exception))

    def test_mixed_duplicate_assign_new_message(self):
        """普通赋值与节点调用赋值混用重复，同样命中新文案。"""
        with self.assertRaises(_CodeflowError):
            _compile_body(
                'def f(INPUT):\n'
                '    x = 1\n'
                '    x = HTTP(url="http://a.com")\n'
                '    return x'
            )


class LegitimateShapesUnaffectedTest(unittest.TestCase):
    def test_chain_assign_still_compiles_and_runs(self):
        """合法链式赋值（y = x + 1）不受自引用检测影响。"""
        fl = flow_from_source("def f(INPUT):\n    x = INPUT.n\n    y = x + 1\n    return y")
        self.assertEqual(fl.run({"n": 1}), 2)

    def test_same_name_across_scopes_still_works(self):
        """循环体内引用循环变量、体内外同名不同域，语义不变。"""
        fl = flow_from_source(
            'def f(INPUT):\n'
            '    for x in MAP(INPUT.items, id="mp"):\n'
            '        return x\n'
            '    return NODE.mp'
        )
        self.assertEqual(fl.run({"items": [1, 2]}), [1, 2])

    def test_reserved_namespace_assign_not_flagged(self):
        """NODE = NODE.x 写 $NODE.NODE，读的是 x 节点——非环形，不误报。

        （保留命名空间经 _resolve_name 先于 ctx.names 解析；此用例钉住
        检测不对保留命名空间作裸名误报。）
        """
        ir = compile_source("def f(INPUT):\n    NODE = NODE.zz\n    return NODE")
        assign = next(n for n in ir["nodes"] if n["id"] == "NODE")
        self.assertEqual(assign["output"], "$NODE.zz")

    def test_cross_branch_same_name_join_refusal_unchanged(self):
        """跨分支同名赋值的既有拦截语义不变（历史决策，勿破坏）。

        分支体内的赋值对 join 点不可见——join 处引用分支内名字仍是
        「未知名字」编译错误（分支内赋值不能漏到块外，与 Python 作用域不同）。
        """
        with self.assertRaises(_CodeflowError) as cm:
            _compile_body(
                "def f(INPUT):\n"
                "    if INPUT.a:\n"
                "        x = 1\n"
                "    else:\n"
                "        x = 2\n"
                "    return x"
            )
        self.assertIn("未知名字", str(cm.exception))

    def test_if_branches_without_join_reference_still_compile(self):
        """分支内赋值 + join 处不引用该名字：正常编译（既有能力不回退）。"""
        ir = _compile_body(
            "def f(INPUT):\n"
            "    if INPUT.a:\n"
            "        x = 1\n"
            "        return x\n"
            "    return 0"
        )
        self.assertTrue(any(n["id"] == "x" for n in ir["nodes"]))

    def test_self_reference_check_is_pure_ast_walk(self):
        """检测函数对嵌套容器/调用内的自引用均可命中（直接单测 helper）。"""
        from plaita.dsl.codeflow._stmt import _references_assign_name

        expr = ast.parse('{"a": [x, F.concat("x", x)]}', mode="eval").body
        self.assertTrue(_references_assign_name(expr, "x"))
        self.assertFalse(_references_assign_name(expr, "y"))
        # INPUT.x / PARENT.NODE.x 不是对赋值名 x 的自引用
        expr2 = ast.parse("INPUT.x", mode="eval").body
        self.assertFalse(_references_assign_name(expr2, "x"))
        expr3 = ast.parse("PARENT.NODE.x", mode="eval").body
        self.assertFalse(_references_assign_name(expr3, "x"))
        # 保留命名空间同名不误报
        expr4 = ast.parse("NODE", mode="eval").body
        self.assertFalse(_references_assign_name(expr4, "NODE"))


class LoopBodySelfReferenceTest(unittest.TestCase):
    """循环体内同名赋值的自引用拦截（[clean C1-1] 评审后补钉）。

    为什么循环体内同名 RHS 也是自引用：_resolve_name 按名字解析时
    ``ctx.names`` 先于循环变量表（_while_child_flow/_compile_for 把循环变量
    合并进 names），而 _compile_assign 在编译 RHS **之前**就登记
    ``names[name] = $NODE.<name>``——同名 RHS 恒解析为本赋值节点自己，与
    顶层一致，运行期必然 NoneType 参与运算（HEAD 实证：for 体 ``x = x + 1``
    编译通过、运行期 ``TypeError: NoneType + int``）。编译期拦截是修同一
    个 bug，不是误报；不同名读循环变量（``y = x + 1``）不受影响。
    """

    def test_for_body_same_name_assign_rejected(self):
        with self.assertRaises(_CodeflowError) as cm:
            _compile_body(
                'def f(INPUT):\n'
                '    for x in MAP(INPUT.items, id="mp"):\n'
                '        x = x + 1\n'
                '        return x\n'
                '    return 0'
            )
        msg = str(cm.exception)
        self.assertIn("第 3 行", msg)
        self.assertIn("自引用", msg)

    def test_while_body_same_name_assign_rejected(self):
        with self.assertRaises(_CodeflowError) as cm:
            _compile_body(
                "def f(INPUT):\n"
                "    while INPUT.n > 0:\n"
                "        item = item + 1\n"
                "        return item\n"
                "    return 0"
            )
        self.assertIn("自引用", str(cm.exception))

    def test_for_body_read_loop_var_into_new_name_still_works(self):
        """不同名读循环变量（y = x + 1）不受检测影响（既有能力不回退）。"""
        fl = flow_from_source(
            'def f(INPUT):\n'
            '    for x in MAP(INPUT.items, id="mp"):\n'
            '        y = x + 1\n'
            '        return y\n'
            '    return NODE.mp'
        )
        self.assertEqual(fl.run({"items": [1, 2]}), [2, 3])


if __name__ == "__main__":
    unittest.main()
