"""@flow 语义化节点 id 与人类可读标签（name/desc）行为钉。

2026-09-30 起，if/while/end（无赋值 return）与裸表达式语句不再产出裸
``_n{n}``：id 用条件语义 slug（``INPUT.score >= 90`` → ``score_ge_90``，
用户约定不带 ``if_`` 前缀），节点带 ``name``（短标签）与 ``desc``
（带源码行号）。console 画布 / dry-run / 运行期报错据此可读。
"""
import pytest

from plaita.dsl.codeflow import compile_source
from plaita.dsl.codeflow._common import _cond_slug, _human_label


def _ids(src: str) -> list:
    return [n["id"] for n in compile_source(src)["nodes"]]


def _node(src: str, node_id: str) -> dict:
    nodes = compile_source(src)["nodes"]
    return next(n for n in nodes if n["id"] == node_id)


class TestCondSlug:
    def test_simple_compare(self):
        import ast
        node = ast.parse("INPUT.score >= 90", mode="eval").body
        assert _cond_slug(node) == "score_ge_90"

    def test_operators(self):
        import ast
        for expr, want in [
            ("INPUT.a == 1", "a_eq_1"),
            ("INPUT.a != None", "a_ne_none"),
            ("INPUT.name in INPUT.allow", "name_in_allow"),
            ("INPUT.a > 1 and INPUT.b < 2", "a_gt_1_and_b_lt_2"),
            ("not (INPUT.x == 1)", "not_x_eq_1"),
            ("F.mod(INPUT.x, 2) == 0", "mod_x_2_eq_0"),
        ]:
            node = ast.parse(expr, mode="eval").body
            assert _cond_slug(node) == want, expr

    def test_none_and_cjk_filtered(self):
        import ast
        # 中文常量 token 被 _ASCII 过滤，其余语义 token 保留
        node = ast.parse("INPUT.name == '中文条件'", mode="eval").body
        assert _cond_slug(node) == "name_eq"
        # 全部语义 token 均为中文 -> 只剩算子（够短，仍可读）
        node = ast.parse("'甲' == '乙'", mode="eval").body
        assert _cond_slug(node) == "eq"
        assert _cond_slug(None) is None

    def test_long_truncated(self):
        import ast
        node = ast.parse("INPUT.very_long_field_name_one >= 123456", mode="eval").body
        slug = _cond_slug(node)
        assert len(slug) <= 24


class TestHumanLabel:
    def test_truncate(self):
        assert _human_label("short") == "short"
        long = "a" * 40
        assert _human_label(long) == "a" * 27 + "…"
        assert len(_human_label(long, 10)) == 10

    def test_whitespace_collapsed(self):
        assert _human_label("a  b\nc") == "a b c"


class TestSemanticIdsInIR:
    SRC = '''
@flow("grade")
def grade(INPUT):
    if INPUT.score >= 90:
        return "A"
    elif INPUT.score >= 60:
        return "B"
    return "C"
'''

    def test_if_id_and_labels(self):
        n = _node(self.SRC, "score_ge_90")
        assert n["type"] == "if"
        assert n["name"] == "INPUT.score >= 90?"
        assert n["desc"] == "if INPUT.score >= 90（第 4 行）"
        assert n["source_line"] == 4

    def test_return_end_id_and_labels(self):
        n = _node(self.SRC, "ret_a")
        assert n["type"] == "end"
        assert n["name"] == "return 'A'"
        assert "第 5 行" in n["desc"]

    def test_elif_gets_own_slug(self):
        ids = _ids(self.SRC)
        assert "score_ge_90" in ids and "score_ge_60" in ids

    def test_duplicate_condition_gets_suffix(self):
        src = '''
@flow("dup")
def dup(INPUT):
    if INPUT.x > 1:
        if INPUT.x > 1:
            return 1
        return 2
    return 3
'''
        ids = _ids(src)
        assert ids.count("x_gt_1") == 1
        assert "x_gt_1_2" in ids

    def test_while_slug(self):
        src = '''
@flow("w")
def w(INPUT):
    while INPUT.rounds < 3:
        return INPUT.item
    return None
'''
        n = _node(src, "rounds_lt_3")
        assert n["type"] == "while"
        assert n["desc"].startswith("while INPUT.rounds < 3")

    def test_bare_expr_statement_labels(self):
        src = '''
@flow("e")
def e(INPUT):
    F.upper(INPUT.name)
    return 1
'''
        nodes = compile_source(src)["nodes"]
        assign = next(n for n in nodes if n["type"] == "assignment" and n["id"].startswith("_n"))
        assert assign["name"] == "F.upper(INPUT.name)"
        assert "第 4 行" in assign["desc"]

    def test_no_desc_on_start(self):
        ids = _ids(self.SRC)
        assert ids[0] == "start"

    def test_node_call_keeps_variable_name(self):
        src = '''
@flow("h")
def h(INPUT):
    r = HTTP.get(url="https://x")
    return r.data
'''
        n = _node(src, "r")
        # 赋值节点调用仍以变量名为 id，不引入 slug
        assert n["type"] == "http"
