"""修复包 A1 回归测试：裸真值条件走 truthy 算子（Python bool 语义）。

历史缺陷：codeflow 对 ``if x:`` 裸真值测试编译成 ``(x != False)``——空列表/
空串/None 与 False 不同值，全部误走真分支，``if items:`` 语义错误。

修复后口径：

* codeflow 裸真值 → ``{"operator": "truthy", "value": True}``，``not x`` →
  ``{"operator": "falsy", "value": True}``；
* ``Condition.match`` 对 truthy/falsy 在 None 短路分支之前拦截，空集合/
  空串/None/0/False 走假分支，非空走真分支；
* 显式 ``x != False``（旧语义）不受影响；
* 发射器 ``emit_source`` 识别 truthy/falsy 还原为裸写法 / not 写法，遗留
  ``ne False`` / ``eq True`` IR 仍可逆。
"""
from __future__ import annotations

import ast
import logging

import pyparsing as pp
import pytest

from plaita.core.expression_parser import ExpressionParser
from plaita.dsl.codeflow import compile_source, emit_source, flow
from plaita.dsl.codeflow._common import _CompileCtx
from plaita.dsl.codeflow._expr import _compile_condition
from plaita.node.decide import Condition, condition_matcher

_DERIVED_KEYS = ("name", "desc", "source_line")


def _fresh_ctx() -> _CompileCtx:
    return _CompileCtx()


def _cond(src: str) -> dict:
    return _compile_condition(ast.parse(src, mode="eval").body, _fresh_ctx())


# ---------------------------------------------------------------------------
# 算子表与 Condition.match 语义
# ---------------------------------------------------------------------------

def test_truthy_falsy_registered_in_matcher_table():
    assert condition_matcher["truthy"]([], None) is False
    assert condition_matcher["truthy"]([1], None) is True
    assert condition_matcher["falsy"]([], None) is True
    assert condition_matcher["falsy"]("x", None) is False


@pytest.mark.parametrize(
    "value,expect_truthy",
    [
        ([], False),
        ([1], True),
        ("", False),
        ("a", True),
        (None, False),
        (0, False),
        (1, True),
        (False, False),
        (True, True),
        ({}, False),
        ({"k": 1}, True),
    ],
)
def test_condition_match_truthy_falsy_matrix(value, expect_truthy):
    ctx = {"$x": value}
    assert Condition(field="$x", operator="truthy", value=True).match(ctx) is expect_truthy
    assert Condition(field="$x", operator="falsy", value=True).match(ctx) is not expect_truthy


def test_truthy_none_short_circuit_not_hit():
    """None 短路分支必须被 truthy/falsy 拦截：None 是 falsy，不是 eq/ne 特例。"""
    assert Condition(field="$x", operator="truthy", value=True).match({"$x": None}) is False
    assert Condition(field="$x", operator="falsy", value=True).match({"$x": None}) is True


# ---------------------------------------------------------------------------
# codeflow 编译产物
# ---------------------------------------------------------------------------

def test_bare_truth_compiles_to_truthy():
    assert _cond("INPUT.items") == {"field": "$INPUT.items", "operator": "truthy", "value": True}


def test_not_bare_truth_compiles_to_falsy():
    assert _cond("not INPUT.flag") == {"field": "$INPUT.flag", "operator": "falsy", "value": True}


def test_double_negation_roundtrip():
    assert _cond("not not INPUT.flag") == {"field": "$INPUT.flag", "operator": "truthy", "value": True}


def test_explicit_ne_false_unchanged():
    """旧语义显式比较不受影响：编译产物仍为 ne False，match 保持 `!=` 语义。"""
    assert _cond("INPUT.x != False") == {"field": "$INPUT.x", "operator": "ne", "value": False}
    c = Condition(field="$x", operator="ne", value=False)
    assert c.match({"$x": 0}) is False  # 0 == False → 0 != False 为 False（历史语义）
    assert c.match({"$x": False}) is False
    assert c.match({"$x": []}) is True  # 空列表 != False → True（显式比较的旧语义）


def test_bare_truth_compile_warning(caplog):
    with caplog.at_level(logging.WARNING, logger="plaita"):
        _cond("INPUT.items")
    assert any("truthy" in r.message and "INPUT.items" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# 端到端：if items: 空集合走假分支
# ---------------------------------------------------------------------------

def test_flow_empty_collection_takes_false_branch():
    @flow("rf_a1_empty_items")
    def rf_a1_empty_items(INPUT):
        if INPUT.items:
            return "true-branch"
        return "false-branch"

    assert rf_a1_empty_items.run(items=[]) == "false-branch"
    assert rf_a1_empty_items.run(items=[1]) == "true-branch"
    assert rf_a1_empty_items.run(items="") == "false-branch"
    assert rf_a1_empty_items.run(items=None) == "false-branch"
    assert rf_a1_empty_items.run(items="x") == "true-branch"


def test_flow_not_bare_truth():
    @flow("rf_a1_not_flag")
    def rf_a1_not_flag(INPUT):
        if not INPUT.flag:
            return "skipped"
        return "ran"

    assert rf_a1_not_flag.run(flag=0) == "skipped"
    assert rf_a1_not_flag.run(flag=1) == "ran"


def test_while_bare_truth_condition_compiles_to_truthy():
    """while 条件复用 _compile_condition（_while_cond）：裸真值同样走 truthy，
    循环变量 item 映射 $LOOP-ITEM。"""
    from plaita.dsl.codeflow._stmt import _while_cond

    cond = _while_cond(ast.parse("item", mode="eval").body, _fresh_ctx())
    assert cond == {"field": "$LOOP-ITEM", "operator": "truthy", "value": True}


# ---------------------------------------------------------------------------
# 发射器 round-trip
# ---------------------------------------------------------------------------

def test_emit_truthy_falsy_roundtrip():
    ir = compile_source('''
@flow("rf_a1_rt")
def rf_a1_rt(INPUT):
    if INPUT.items:
        return "has"
    return "none"
''')
    src = emit_source(ir)
    ir2 = compile_source(src)
    n1 = [n for n in ir["nodes"] if n["type"] == "if"][0]
    n2 = [n for n in ir2["nodes"] if n["type"] == "if"][0]
    for n in (n1, n2):
        for key in _DERIVED_KEYS:
            n.pop(key, None)
    assert n1["condition"] == n2["condition"] == {
        "field": "$INPUT.items", "operator": "truthy", "value": True,
    }


def test_emit_legacy_ne_false_still_inverted_to_bare():
    """遗留 IR（truthy 引入前的 ne False 产物）仍可逆为裸写法。"""
    src = emit_source({
        "runtime": "python", "flow_id": "legacy", "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "c"},
            {"type": "if", "id": "c", "condition": {"field": "$INPUT.x", "operator": "ne", "value": False},
             "next": "b", "else_next": "e"},
            {"type": "assignment", "id": "b", "output": "has", "next": "e"},
            {"type": "end", "id": "e", "output": "none", "resultType": "success"},
        ],
    })
    assert "if INPUT.x" in src and "!=" not in src


def test_emit_falsy_inverted_to_not():
    src = emit_source({
        "runtime": "python", "flow_id": "falsy_ir", "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "c"},
            {"type": "if", "id": "c", "condition": {"field": "$INPUT.x", "operator": "falsy", "value": True},
             "next": "b", "else_next": "e"},
            {"type": "assignment", "id": "b", "output": "falsy", "next": "e"},
            {"type": "end", "id": "e", "output": "truthy", "resultType": "success"},
        ],
    })
    assert "not INPUT.x" in src


# ---------------------------------------------------------------------------
# 边界
# ---------------------------------------------------------------------------

def test_truthy_value_literal_evaluates_without_none_shortcut():
    """truthy 的 value 恒为字面 True：求值安全，不参与比较。"""
    c = Condition(field="$x", operator="truthy", value=True)
    assert c.match({"$x": "anything"}) is True
    assert c.match({"$x": None}) is False
    # 根缺失（非字段缺失）历史语义：KeyError（与所有其他算子一致）
    with pytest.raises(KeyError):
        c.match({})


def test_parse_error_contract_untouched_by_truthy():
    """truthy 不影响表达式文法契约：非法前缀表达式仍抛 ParseException。"""
    with pytest.raises(pp.ParseException):
        ExpressionParser.for_prefix("$").evaluate("$F.add(1,", {})
