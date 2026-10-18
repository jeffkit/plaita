"""plaita#216：``expression_parser`` 9 条「诊断/缓存」幸存变异的测试债。

第五轮变异挖掘留下 9 条**非等价但仅影响诊断/缓存策略**的变异（当前行为正确、
无测试钉住）。本文件逐条补一个能杀它的用例；生产代码零改动（#216 边界：
「不改 warning/诊断文案或缓存实现，只补测试」）。

| # | 目标 | 变异 | 本文件用例 |
|---|------|------|-----------|
| 1 | ``_warn_suspicious_literal`` | 尾句 ``rewrite the string.`` → ``rewrite it.`` | ``test_suspicious_literal_warning_text_is_pinned`` |
| 2 | ``_warn_unmatched_template`` | 尾句 ``fix the inner expression`` → ``repair ...`` | ``test_unmatched_template_warning_text_is_pinned`` |
| 3 | ``_annotate_parse_error`` | ``常见原因：`` → ``可能原因：`` | ``test_parse_error_hint_uses_common_cause_phrase`` |
| 4 | ``_warned_literals`` 去重 | 重置阈值 ``> 512`` → ``> 1024`` | ``test_suspicious_literal_dedupe_resets_above_512`` |
| 5 | ``_warned_unmatched_templates`` | 阈值 ``> 512`` → ``> 1024`` | ``test_unmatched_template_dedupe_resets_above_512`` |
| 6 | LRU 命中 | 命中时不 ``cache.move_to_end(value)`` | ``test_lru_hit_moves_entry_to_newest`` |
| 7 | LRU 容量 | ``while len(cache) > maxsize`` → ``>=`` | ``test_lru_capacity_boundary_is_maxsize_not_less`` |
| 8 | ``_render_path`` | 字段段用 ``raw`` → ``target`` | ``test_render_path_uses_raw_field_name_for_special_roots`` |
| 9 | LRU 逐出端 | ``popitem(last=False)`` → ``popitem()`` | ``test_lru_evicts_oldest_not_newest`` |

诊断文案按**契约**钉完整字符串：它们是运维/作者自纠的唯一线索，短语漂移会让
「按日志文案排障」的手册与告警检索失效（#216 建议 1 的第一种口径）。
"""
from __future__ import annotations

import logging

import pytest
from pyparsing import ParseException

from plaita.core import expression_parser as ep
from plaita.core.expression_parser import ExpressionParser


@pytest.fixture()
def parser() -> ExpressionParser:
    ExpressionParser._instances.clear()
    p = ExpressionParser()
    yield p
    ExpressionParser._instances.clear()


@pytest.fixture(autouse=True)
def _clear_warn_dedup():
    """告警去重集合是模块级全局——每个用例前后清空，避免串扰。"""
    ep._warned_literals.clear()
    ep._warned_unmatched_templates.clear()
    yield
    ep._warned_literals.clear()
    ep._warned_unmatched_templates.clear()


@pytest.fixture()
def caplog_plaita(caplog):
    caplog.set_level(logging.WARNING, logger="plaita")
    return caplog


# ---------------------------------------------------------------------------
# 1 / 2：诊断文案契约
# ---------------------------------------------------------------------------

def test_suspicious_literal_warning_text_is_pinned(parser, caplog_plaita):
    """#216-1：``rewrite the string.`` 是契约，不能被改成 ``rewrite it.``。"""
    value = "hello {{ name }}"
    assert parser.evaluate(value, {}) == value
    messages = [r.getMessage() for r in caplog_plaita.records]
    expected = (
        f"expression {value!r} looks like templating/Python code from another "
        "language and was returned AS-IS (no evaluation happened). plaita "
        "expressions use $INPUT.x / $NODE.id / $F.fn(...) and {% ... %} for "
        "interpolation — rewrite the string."
    )
    assert expected in messages


def test_unmatched_template_warning_text_is_pinned(parser, caplog_plaita):
    """#216-2：``fix the inner expression`` 是契约，不能被改成 ``repair ...``。"""
    value = "value = {% 1 + 2 %}!"
    assert parser.evaluate(value, {}) == value
    messages = [r.getMessage() for r in caplog_plaita.records]
    expected = (
        f"expression {value!r} contains '{{%' but no valid {{% ... %}} "
        "expression matched (inner expression is malformed or unclosed); it "
        "was returned AS-IS without evaluation — fix the inner expression "
        "(e.g. {% $INPUT.x %})."
    )
    assert expected in messages


def test_parse_error_hint_uses_common_cause_phrase(parser):
    """#216-3：解析错误自纠提示以 ``常见原因：`` 开头（运维按此检索）。"""
    with pytest.raises(ParseException) as excinfo:
        parser.evaluate("$INPUT.", {})
    message = str(excinfo.value)
    assert "无法解析表达式：'$INPUT.'" in message
    assert "常见原因：字符串常量里的引号/反斜杠未转义" in message
    assert "可能原因：" not in message


# ---------------------------------------------------------------------------
# 4 / 5：告警去重集合的重置阈值（>512 时清空）
# ---------------------------------------------------------------------------

def test_suspicious_literal_dedupe_resets_above_512(caplog_plaita):
    """#216-4：集合在 >512 时清空 ⇒ 有条数上限，最老的串会再次告警。

    正确实现里：第 514 次调用看到 ``len == 513 > 512`` 先清空再加 ⇒ 集合大小
    恒 ≤ 513；第 1 条串已不在集合里，再次告警。阈值被改成 1024 后，600 次调用
    全部留在集合里（大小 600、第 1 条仍被去重）——两条断言都会红。
    """
    for i in range(600):
        ep._warn_suspicious_literal("{{ v%d }}" % i)
    assert len(ep._warned_literals) <= 513

    caplog_plaita.clear()
    ep._warn_suspicious_literal("{{ v0 }}")
    assert any("AS-IS" in r.getMessage() for r in caplog_plaita.records)


def test_unmatched_template_dedupe_resets_above_512(caplog_plaita):
    """#216-5：与 #4 同口径，作用在 ``_warned_unmatched_templates`` 上。"""
    for i in range(600):
        ep._warn_unmatched_template("a {%% 1 + %d %%} b" % i)
    assert len(ep._warned_unmatched_templates) <= 513

    caplog_plaita.clear()
    ep._warn_unmatched_template("a {% 1 + 0 %} b")
    assert any("no valid" in r.getMessage() for r in caplog_plaita.records)


# ---------------------------------------------------------------------------
# 6 / 7 / 9：LRU 命中顺序、容量边界、逐出端
# ---------------------------------------------------------------------------

@pytest.fixture()
def small_lru(parser):
    """把主编译缓存压到 3 条，便于观察顺序/边界。"""
    old = ExpressionParser._MAX_CACHE_ENTRIES
    ExpressionParser._MAX_CACHE_ENTRIES = 3
    parser._compile_cache.clear()
    try:
        yield parser
    finally:
        ExpressionParser._MAX_CACHE_ENTRIES = old
        parser._compile_cache.clear()


def _warm(p: ExpressionParser, keys) -> None:
    for key in keys:
        p.evaluate(f"$INPUT.{key}", {"$INPUT": {key: 1}})


def test_lru_hit_moves_entry_to_newest(small_lru):
    """#216-6：命中必须 ``move_to_end``——否则下次逐出的是刚被用过的条目。

    顺序 a,b,c → 命中 a → b,c,a；再插 d 应逐出 b（最久未用），a 保留。
    变异（命中不 move）下顺序仍是 a,b,c ⇒ d 进、a 出。
    """
    p = small_lru
    _warm(p, ("a", "b", "c"))
    assert list(p._compile_cache) == ["$INPUT.a", "$INPUT.b", "$INPUT.c"]

    p.evaluate("$INPUT.a", {"$INPUT": {"a": 1}})  # hit
    assert list(p._compile_cache) == ["$INPUT.b", "$INPUT.c", "$INPUT.a"]

    p.evaluate("$INPUT.d", {"$INPUT": {"d": 1}})
    assert "$INPUT.a" in p._compile_cache
    assert "$INPUT.b" not in p._compile_cache


def test_lru_capacity_boundary_is_maxsize_not_less(small_lru):
    """#216-7：容量判定必须是 ``len > maxsize``；``>=`` 会让缓存永远少存一条。"""
    p = small_lru
    _warm(p, ("a", "b", "c"))
    # 恰好 maxsize 条：一条都不该被逐出。
    assert len(p._compile_cache) == 3
    assert "$INPUT.a" in p._compile_cache

    # 第 maxsize+1 条：只逐出最旧的一条，仍保持 maxsize 条。
    _warm(p, ("d",))
    assert len(p._compile_cache) == 3
    assert "$INPUT.a" not in p._compile_cache
    assert "$INPUT.d" in p._compile_cache


def test_lru_evicts_oldest_not_newest(parser):
    """#216-9：``popitem(last=False)`` 逐出最旧；``popitem()`` 会逐出最新。"""
    old = ExpressionParser._MAX_CACHE_ENTRIES
    ExpressionParser._MAX_CACHE_ENTRIES = 2
    parser._compile_cache.clear()
    try:
        _warm(parser, ("a", "b", "c"))
        # 正确：逐出 a，保留 b、c（刚写入的最新条目）。
        assert "$INPUT.a" not in parser._compile_cache
        assert "$INPUT.b" in parser._compile_cache
        assert "$INPUT.c" in parser._compile_cache
    finally:
        ExpressionParser._MAX_CACHE_ENTRIES = old
        parser._compile_cache.clear()


# ---------------------------------------------------------------------------
# 8：``_render_path`` 的字段段拼写
# ---------------------------------------------------------------------------

def test_render_path_uses_raw_field_name_for_special_roots(parser):
    """#216-8：字段段发射 ``raw``（源码拼写），不是 prefixed ``target``。

    特殊根名（``NODE`` 等）在 ``_compile_variable`` 里 target 会加前缀
    （``NODE`` → ``$NODE``）。若 ``_render_path`` 误用 target，诊断路径会变成
    ``$INPUT.$NODE.x``，与作者写的 ``$INPUT.NODE.x`` 不符。
    """
    walked = [("field", "$NODE", "NODE"), ("field", "x", "x")]
    assert ep._render_path("$INPUT", walked) == "$INPUT.NODE.x"

    # 用户可见的错误文案：路径必须是源码里的拼写。
    with pytest.raises(ValueError) as excinfo:
        parser.evaluate("$INPUT.NODE.x", {"$INPUT": {"$NODE": len}})
    message = str(excinfo.value)
    assert "expression path '$INPUT.NODE.x'" in message
    assert "$INPUT.$NODE.x" not in message
