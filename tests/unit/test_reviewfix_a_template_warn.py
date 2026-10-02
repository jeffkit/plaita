"""修复包 A4 回归测试：模板 `{% ... %}` 内层不合法时原样返回但必须告警。

历史缺陷：``"value = {% 1 + 2 %}!"`` 内层表达式不合法 → 整串原样返回，
不抛错也不打日志，AI 作者把未求值模板当正确结果消费。

修复口径：返回行为不变（不抛错，保护存量）；含 ``{%`` 但无任何插值匹配
成功时 ``logger.warning``（含原文）；``_SUSPICIOUS_LITERAL`` 检查挪到模板
两条路径共用入口。
"""
from __future__ import annotations

import logging

import pytest

from plaita.core import expression_parser as ep
from plaita.core.expression_parser import ExpressionParser


@pytest.fixture()
def parser() -> ExpressionParser:
    return ExpressionParser.for_prefix("$")


@pytest.fixture(autouse=True)
def _clear_warn_dedup():
    """告警去重集合是模块级全局——每个用例前清空，避免用例间串扰。"""
    ep._warned_literals.clear()
    ep._warned_unmatched_templates.clear()
    yield
    ep._warned_literals.clear()
    ep._warned_unmatched_templates.clear()


@pytest.fixture()
def caplog_plaita(caplog):
    caplog.set_level(logging.WARNING, logger="plaita")
    return caplog


def test_unmatched_template_returns_value_unchanged_and_warns(parser, caplog_plaita):
    val = "value = {% 1 + 2 %}!"
    out = parser.evaluate(val, {})
    assert out == val  # 行为不变：原样返回、不抛错
    assert any("{%" in r.message and val in r.message for r in caplog_plaita.records)


def test_unclosed_template_warns(parser, caplog_plaita):
    val = "a {% b"
    assert parser.evaluate(val, {}) == val
    assert any("no valid" in r.message for r in caplog_plaita.records)


def test_valid_template_no_unmatched_warning(parser, caplog_plaita):
    out = parser.evaluate("a {% $INPUT.x %} b", {"$INPUT": {"x": 7}})
    assert out == "a 7 b"
    assert not any("no valid" in r.message for r in caplog_plaita.records)


def test_warning_dedup_same_string_once(parser, caplog_plaita):
    val = "dup = {% ?? %}"
    parser.evaluate(val, {})
    parser.evaluate(val, {})
    unmatched = [r for r in caplog_plaita.records if "no valid" in r.message]
    assert len(unmatched) == 1


def test_suspicious_literal_checked_on_plain_text_path(parser, caplog_plaita):
    """无 `{%` 的可疑字面量（Jinja 双花括号）仍告警——原有行为保持。"""
    val = "hello {{ name }}"
    assert parser.evaluate(val, {}) == val
    assert any("AS-IS" in r.message for r in caplog_plaita.records)


def test_suspicious_literal_checked_on_template_path_too(parser, caplog_plaita):
    """共用入口：含 `{%` 的串若同时可疑也告警（修复前该路径不查）。"""
    val = "{% $INPUT.x %} {{ name }}"
    out = parser.evaluate(val, {"$INPUT": {"x": 1}})
    assert out == "1 {{ name }}"  # 插值正常求值；{{ name }} 按字面保留
    assert any("AS-IS" in r.message for r in caplog_plaita.records)
