"""修复包 A2 回归测试：字符串常量转义与文法严格互逆（round-trip 金标）。

历史缺陷：``_render_arg`` 对 ``\\`` ``"`` ``\\n`` ``\\r`` ``\\t`` 做反斜杠
转义，但文法 ``QuotedString`` 没配 ``esc_char``——``\\"`` 让字符串匹配提前
终止（整个函数调用 ParseException），``\\\\`` 不回退、``\\t`` 被默认的
convert_whitespace_escapes 吃成制表符，求值结果逐字符静默腐蚀。

修复口径：任意含 ``"`` ``\\`` ``\\n`` ``\\t`` 中文等的常量，经
``_render_arg → 编译 → 求值`` 后逐字符相等。
"""
from __future__ import annotations

import pyparsing as pp
import pytest

from plaita.core.expression_parser import ExpressionParser
from plaita.dsl.codeflow._expr import _render_arg


@pytest.fixture()
def parser() -> ExpressionParser:
    return ExpressionParser.for_prefix("$")


def _roundtrip(parser: ExpressionParser, s: str) -> str:
    rendered = _render_arg(s)
    assert rendered.startswith('"') and rendered.endswith('"')
    got = parser.evaluate(f"$F.concat({rendered})", {})
    assert isinstance(got, str)
    return got


SAMPLES = [
    'say "hi"',
    "C:\\path\\to",
    "a\nb",
    "c\rd",
    "e\tf",
    "中文常量",
    "mix\\\"x",
    "back\\slash",
    "trail\\",
    "multi\nline\tmix\\end",
    "",
    "a'b",
    "引号\"与反斜杠\\与换行\n混排",
    "\\\\",
    "a\\\\b",
]


@pytest.mark.parametrize("s", SAMPLES)
def test_render_compile_eval_roundtrip(parser, s):
    assert _roundtrip(parser, s) == s


def test_double_quote_no_longer_breaks_function_call(parser):
    """修复前：`$F.concat("say \\"hi\\"")` → ParseException。"""
    assert parser.evaluate('$F.concat("say \\"hi\\"")', {}) == 'say "hi"'


def test_backslash_no_longer_corrupts_result(parser):
    """修复前：`C:\\path\\to` 求值成 'C:\\\\path\\<TAB>o' 且零报错。"""
    assert parser.evaluate('$F.concat("C:\\\\path\\\\to")', {}) == "C:\\path\\to"


def test_single_quoted_string_with_escaped_quote(parser):
    """手写表达式用单引号 + 转义引号也必须闭合。"""
    assert parser.evaluate("$F.concat('it\\'s ok')", {}) == "it's ok"


def test_ascii_exhaustive_inverse_property(parser):
    """ASCII 逐字符穷举：_render_arg → 文法 → 原字符（$ 除外——$ 前缀串按
    表达式透传渲染，是 _render_arg 的既有设计，非转义问题）。"""
    dq = pp.QuotedString('"', esc_char="\\", esc_quote='"')
    failures = []
    for code in range(1, 128):
        ch = chr(code)
        if ch == "$":
            continue  # 表达式透传，见 test_dollar_passthrough_known_edge
        rendered = _render_arg(ch)
        try:
            got = dq.parse_string(rendered, parse_all=True)[0]
        except pp.ParseException as exc:
            failures.append((ch, rendered, f"EXC: {exc}"))
            continue
        if got != ch:
            failures.append((ch, rendered, got))
    assert failures == []


def test_dollar_passthrough_known_edge(parser):
    """已知边界（不在本修复范围）：常量串以 $ 开头按表达式透传渲染。
    `$F.concat("$")` 会解析失败——记录现状，防止无意识回归。"""
    from plaita.dsl.codeflow._expr import _render_arg as ra

    assert ra("$") == "$"  # 透传，不加引号
    assert ra("$$INPUT") == "$$INPUT"


def test_whitespace_escapes_still_converted(parser):
    """convert_whitespace_escapes 语义保留：\\n \\t 还原成真实控制字符。"""
    assert parser.evaluate('$F.concat("a\\nb")', {}) == "a\nb"
    assert parser.evaluate('$F.concat("a\\tb")', {}) == "a\tb"
