"""回归：$NODE 路径的普通字符串属性值不得被二次解析。

背景（mediaflow 试点实测）：节点输出含 "[promo] ..."、引号、换行等元字符时，
路径求值对属性值做无条件递归解析会把内容当表达式（列表/变量）误解析，
抛出误导性的 KeyError('$NODE')。语义应为：只有 $ 前缀变量 / {% %} 模板
才递归求值（"nested expression strings" 的文档语义）。
"""
from plaita.core.expression_parser import ExpressionParser


def test_plain_string_attr_not_reparsed():
    parser = ExpressionParser.for_prefix("$")
    context = {"$NODE": {"item": {"title": "[promo] [dry-run] would run: # prompt",
                                  "reason": "两轮审核未过，不入池"}}}
    assert parser.evaluate("$NODE.item.title", context) == context["$NODE"]["item"]["title"]
    assert parser.evaluate("$NODE.item.reason", context) == "两轮审核未过，不入池"


def test_template_like_attr_still_parsed():
    parser = ExpressionParser.for_prefix("$")
    context = {"$NODE": {"a": {"greeting": "{% $F.concat('hello ', 'world') %}"}}}
    assert parser.evaluate("$NODE.a.greeting", context) == "hello world"


def test_template_like_attr_still_parsed():
    parser = ExpressionParser.for_prefix("$")
    context = {"$NODE": {"a": {"greeting": "{% $F.concat('hello ', 'world') %}"}}}
    assert parser.evaluate("$NODE.a.greeting", context) == "hello world"
