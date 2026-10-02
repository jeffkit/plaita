"""修复包 A3 回归测试：sexpr 字符串转义用显式映射，非 ASCII 不乱码。

历史缺陷：``_decode_string`` 用 ``encode("utf-8").decode("unicode_escape")``
处理转义——unicode_escape 按 latin-1 逐字节解码，多字节 UTF-8 被静默腐蚀：
``"line1\\nline2 中文"`` → ``'line1\\nline2 ä¸\\xadæ\\x96\\x87'``。

修复口径：显式白名单映射（``\\n`` ``\\t`` ``\\r`` ``\\"`` ``\\\\``），
其余转义序列原样保留，多字节字符不动。
"""
from __future__ import annotations

import pytest

from plaita.dsl.sexpr import _decode_string, compile_sexpr


@pytest.mark.parametrize(
    "raw,expect",
    [
        ('"line1\\nline2 中文"', "line1\nline2 中文"),
        ('"tab\\tend 中文"', "tab\tend 中文"),
        ('"cr\\rend"', "cr\rend"),
        ('"引号\\"q\\"中文"', '引号"q"中文'),
        ('"back\\\\slash 中文"', "back\\slash 中文"),
        ('"no escapes 中文"', "no escapes 中文"),
        ('""', ""),
    ],
)
def test_decode_string_multibyte_safe(raw, expect):
    assert _decode_string(raw) == expect


def test_unknown_escape_preserved_verbatim():
    """非白名单转义序列原样保留（修复前 unicode_escape 会展开 \\x41 → A）。"""
    assert _decode_string(r'"raw \x41 \u4e2d"') == r"raw \x41 \u4e2d"


def test_multibyte_chars_never_corrupted():
    s = "中文🎉emoji"
    assert _decode_string(f'"{s}"') == s


def test_decode_is_inverse_of_encoding_for_common_escapes():
    """编码端 _expr_to_src 只转义 \\ 与 "；解码端对同一规则严格互逆。
    样例须含空白/引号——_expr_to_src 对「纯标识符形」字符串不加引号原样输出。"""
    from plaita.dsl.sexpr import _expr_to_src

    for s in ["a \"b", "a \\b", "中文 \"混 \\排", "plain 中文 值", "line1\nline2"]:
        encoded = _expr_to_src(s)
        assert encoded.startswith('"') and encoded.endswith('"')
        assert _decode_string(encoded) == s


def test_compile_sexpr_output_with_chinese_and_escape():
    src = '(flow rf_a3 (start -> e) (end e :output "line1\\nline2 中文"))'
    ir = compile_sexpr(src)
    end_node = [n for n in ir["nodes"] if n["type"] == "end"][0]
    assert end_node["output"] == "line1\nline2 中文"
