"""修复包 A5 回归测试：`$F.` 不再被 variable 路径吞掉，解析失败带上下文。

历史缺陷（评审描述的机制已被 2026-10 perf merge 的 thunk 化部分消除——
parse action 现在只编译不求值，解析期不再可能抛 KeyError）：``$F.concat``
这类残缺串恰好整体匹配 variable 路径时，求值期仍抛 ``KeyError: '$F' not
found``（假诊断）；带括号/引号的文法错误报裸 ``Expected end of text``，
无原文、无原因提示。

修复口径：

* variable 根加 ``$F.`` 负前瞻——``$F`` 是函数命名空间不是上下文根，
  ``$F.`` 开头的非函数调用串在文法层失败，报 ParseException；
* ``_eval_prefix`` 捕获 ParseException 时补「表达式原文 + 列号 + 常见原因」，
  异常类型保持 ParseException（``parse_function`` 契约不变）。
"""
from __future__ import annotations

import pyparsing as pp
import pytest

from plaita.core.expression_parser import ExpressionParser


@pytest.fixture()
def parser() -> ExpressionParser:
    return ExpressionParser.for_prefix("$")


# ---------------------------------------------------------------------------
# KeyError 假诊断消除
# ---------------------------------------------------------------------------

def test_bare_dollar_f_no_longer_fakes_keyerror(parser):
    """`$F.concat`（残缺函数调用）报 ParseException，不再是 KeyError '$F' not found。"""
    with pytest.raises(pp.ParseException) as ei:
        parser.evaluate("$F.concat", {})
    assert not isinstance(ei.value, KeyError)


def test_dollar_f_path_with_junk_fails_as_parse_error(parser):
    with pytest.raises(pp.ParseException):
        parser.evaluate("$F.concat.x", {})


# ---------------------------------------------------------------------------
# 注解上下文
# ---------------------------------------------------------------------------

def test_parse_error_includes_original_text_and_column(parser):
    with pytest.raises(pp.ParseException) as ei:
        parser.evaluate('$F.concat("x', {})
    msg = str(ei.value)
    assert "$F.concat" in msg          # 表达式原文
    assert "第" in msg and "列" in msg  # 列号
    assert "^" in msg                   # 列指示
    assert "转义" in msg                # 常见原因提示


def test_parse_error_still_parse_exception_type(parser):
    """异常类型必须是 ParseException——parse_function 与节点错误语义依赖它。"""
    with pytest.raises(pp.ParseException):
        parser.evaluate("$INPUT.[0]", {})


def test_parse_function_contract_returns_raw_string(parser):
    """parse_function：解析失败返回原串（历史契约保持）。"""
    bad = '$F.concat("x'
    assert parser.parse_function(bad, {}) == bad


# ---------------------------------------------------------------------------
# 正常路径不受负前瞻影响
# ---------------------------------------------------------------------------

def test_valid_function_calls_unaffected(parser):
    assert parser.evaluate("$F.add(1, 2)", {}) == 3
    assert parser.evaluate("$F.concat($F.concat('a', 'b'), 'c')", {}) == "abc"
    assert parser.evaluate('$F.concat("say \\"hi\\"")', {}) == 'say "hi"'


def test_flow_alias_root_unaffected(parser):
    assert parser.evaluate("$FLOW", {"$FLOW_ID": "f1"}) == "f1"
    assert parser.evaluate("$FLOW_ID", {"$FLOW_ID": "f1"}) == "f1"


def test_arbitrary_root_with_f_prefix_but_no_dot(parser):
    """`$F` 后无 `.` 的根（$FOO / $FLOW）不受负前瞻影响。"""
    assert parser.evaluate("$FOO.x", {"$FOO": {"x": 1}}) == 1


def test_real_missing_root_still_keyerror(parser):
    """真实缺失根（非 $F.）仍抛 KeyError——流程逻辑错误语义保持。"""
    with pytest.raises(KeyError):
        parser.evaluate("$NODE.no_such", {})


def test_bare_dollar_root_still_parses(parser):
    """裸 `$` 根（$.not_exist）历史语义保持：KeyError at lookup time。"""
    with pytest.raises(KeyError):
        parser.evaluate("$.not_exist", {})
