"""plaita#231：@flow 字符串字面量以 ``$`` 开头必须**保持字面量**。

历史缺陷：``_expr._render_arg`` 把所有 plain str 且 ``startswith("$")`` 的
值原样塞进 ``$F.func(...)``——``_compile_expr(ast.Constant)`` 返回的用户
字面量与编译器产出的 ``$INPUT.x`` 无法区分。于是 ``F.concat("$5", " USD")``
编译成 ``$F.concat($5, " USD")``，运行期 KeyError；``F.concat("$INPUT","x")``
更危险：在 ``$INPUT="VALUE"`` 上下文里静默变成 ``"VALUEx"``。

修复（本文件钉住）：
- 编译器产出的表达式用 ``_CompiledExpr(str)`` 标记，只有它允许裸渲染；
- ``$`` 开头的字面量用 ``_LiteralIR(str)``：IR 形态是
  ``$F.concat("<literal>")``（运行期还原为字面量），作为 ``$F`` 参数时按原值
  加引号，不会嵌套；
- 纯字面量 / dict / list 中的 ``$`` 字符串同样走该形态（运行期
  ``_eval_non_string`` 会递归求值容器，不能留裸 ``$5``）。

负控：还原 ``_render_arg`` 的旧规则（plain str 以 ``$`` 开头即原样返回）并
去掉 ``ast.Constant`` 的 ``_LiteralIR`` 包装，本文件所有字面量用例必红
（典型 ``KeyError: '$5' not found`` / IR 断言 `$F.concat($5...)`）。证据见
``docs/proofs/231-dollar-literals.md`` 与 ``docs/proofs/data/231-*``。
"""
from __future__ import annotations

import pytest

from plaita.dsl.codeflow import compile_source, emit_source
from plaita.io import evaluate


def _output(src: str):
    return compile_source(src)["nodes"][-1]["output"]


def _flow_src(body: str) -> str:
    return f'@flow("f")\ndef f(INPUT):\n    {body}\n'


# -- F 参数：字面量 -----------------------------------------------------------

def test_concat_dollar_literal_is_quoted_and_evaluates_literal():
    out = _output(_flow_src('return F.concat("$5", " USD")'))
    assert out == '$F.concat("$5", " USD")', out
    assert evaluate(out, {}) == "$5 USD"


def test_concat_dollar_root_name_is_literal_not_context_lookup():
    """``"$INPUT"`` 作为字面量：不得静默替换成上下文值（#231 的核心）。"""
    out = _output(_flow_src('return F.concat("$INPUT", "x")'))
    assert out == '$F.concat("$INPUT", "x")', out
    assert evaluate(out, {"$INPUT": "VALUE"}) == "$INPUTx"


def test_concat_home_literal_does_not_raise_keyerror():
    out = _output(_flow_src('return F.concat("$HOME", " 未设置")'))
    assert evaluate(out, {}) == "$HOME 未设置"


def test_mixed_literal_and_dynamic_args():
    """同一调用里字面量与动态表达式并存：只给字面量加引号。"""
    out = _output(_flow_src('return F.concat("$5", INPUT.x, " tail")'))
    assert out == '$F.concat("$5", $INPUT.x, " tail")', out
    assert evaluate(out, {"$INPUT": {"x": "ABC"}}) == "$5ABC tail"


def test_dynamic_expression_still_compiles_and_evaluates():
    """对照组：``INPUT.x`` 仍编译为动态表达式，不因本次修复被加引号。"""
    out = _output(_flow_src('return F.concat(INPUT.x, "y")'))
    assert out == '$F.concat($INPUT.x, "y")', out
    assert evaluate(out, {"$INPUT": {"x": "ABC"}}) == "ABCy"


def test_literal_with_dollar_not_at_start_is_unchanged():
    out = _output(_flow_src('return F.concat("a$b", "c")'))
    assert out == '$F.concat("a$b", "c")', out
    assert evaluate(out, {}) == "a$bc"


def test_multiline_literal_starting_with_dollar_preserved():
    out = _output(_flow_src('return F.concat("$line1\\nline2", " tail")'))
    assert "\\n" in out, out
    assert evaluate(out, {}) == "$line1\nline2 tail"


# -- 纯字面量 / dict / list ---------------------------------------------------

def test_pure_dollar_literal_preserved():
    out = _output(_flow_src('return "$5"'))
    assert isinstance(out, str) and out.startswith("$F.concat("), out
    assert evaluate(out, {"$5": "MUST-NOT-LEAK"}) == "$5"


def test_nested_dict_dollar_literals_preserved_dynamic_kept():
    src = _flow_src('return {"lit": "$5", "root": "$INPUT", "dyn": INPUT.x}')
    out = _output(src)
    result = evaluate(out, {"$INPUT": {"x": "ABC"}, "$INPUT.x": "WRONG"})
    assert result == {"lit": "$5", "root": "$INPUT", "dyn": "ABC"}, result


def test_nested_list_dollar_literals_preserved_dynamic_kept():
    out = _output(_flow_src('return ["$5", INPUT.x]'))
    assert evaluate(out, {"$INPUT": {"x": "ABC"}}) == ["$5", "ABC"]


def test_literal_dict_key_starting_with_dollar_is_untouched():
    """dict **键**是数据，运行期不求值：不得被包成表达式。"""
    out = _output(_flow_src('return {"$key": "$5"}'))
    assert evaluate(out, {}) == {"$key": "$5"}


# -- round-trip --------------------------------------------------------------

@pytest.mark.parametrize("body", [
    'return F.concat("$5", " USD")',
    'return F.concat("$INPUT", INPUT.x)',
    'return "$5"',
    'return {"lit": "$5", "dyn": INPUT.x}',
    'return ["$5", INPUT.x]',
])
def test_emit_roundtrip_preserves_dollar_literal_semantics(body):
    """IR → @flow → IR：字面量 $ 字符串的语义必须往返保真（#231 对抗复核）。"""
    ir = compile_source(_flow_src(body))
    before = ir["nodes"][-1]["output"]
    emitted = emit_source(ir)
    after = compile_source(emitted)["nodes"][-1]["output"]
    assert after == before, f"round-trip 漂移\nbefore={before!r}\nafter={after!r}"
