"""回归：路径读取对运行期字符串一律原样返回（#268 契约：配置求值、运行期数据）。

历史三层：无条件递归解析（mediaflow 试点踩坑："[promo] ..." 被当列表）→
#104 方案 A 根扫描 → #130 raw 旁路。2026-10-21 终裁（#268）：嵌套求值整体
摘除——表达式只活在 flow 配置里，节点结果/输入中的字符串是数据；情况一
（$NODE.a.b.c 多层递进取值）不受影响。

plaita-go / plaita-rs 与本契约一致（plaita-go#46 wontfix；差分用例
runtime/string-is-data 双跑锚住）。
"""
from collections import defaultdict

from plaita.core.expression_parser import ExpressionParser


def test_plain_string_attr_not_reparsed():
    parser = ExpressionParser.for_prefix("$")
    context = {"$NODE": {"item": {"title": "[promo] [dry-run] would run: # prompt",
                                  "reason": "两轮审核未过，不入池"}}}
    assert parser.evaluate("$NODE.item.title", context) == context["$NODE"]["item"]["title"]
    assert parser.evaluate("$NODE.item.reason", context) == "两轮审核未过，不入池"


def test_pure_function_string_stays_data():
    """历史语义（求值成 "hello world"/"True"）已随 #268 摘除：字符串是数据。"""
    parser = ExpressionParser.for_prefix("$")
    context = {"$NODE": {
        "a": {"greeting": "{% $F.concat('hello ', 'world') %}"},
        "llm": {"text": "{% $F.eq(1,1) %}"},
        "expr": "$F.add(2,3)",
    }}
    assert parser.evaluate("$NODE.a.greeting", context) == "{% $F.concat('hello ', 'world') %}"
    assert parser.evaluate("$NODE.llm.text", context) == "{% $F.eq(1,1) %}"
    assert parser.evaluate("$NODE.expr", context) == "$F.add(2,3)"


def test_data_text_referencing_root_returned_raw():
    """#104 验收①（契约化后期望不变）：引用 $INPUT 的数据文本原样返回。"""
    parser = ExpressionParser.for_prefix("$")
    text = "see {% $INPUT.secret %} here"
    context = {"$NODE": {"llm": {"text": text}}, "$INPUT": {"secret": "S3CRET"}}
    assert parser.evaluate("$NODE.llm.text", context) == text
    # 对照：顶层求值路径不受影响
    assert parser.evaluate("$INPUT.secret", context) == "S3CRET"


# --- 历史"扫描判据"形态在数据契约下的统一结果：全部原样 --------------------


def test_quoted_dollar_literal_is_not_a_root_reference():
    parser = ExpressionParser.for_prefix("$")
    context = {"$NODE": {"a": {"greeting": "{% $F.concat('$INPUT', '!') %}"}}}
    assert parser.evaluate("$NODE.a.greeting", context) == "{% $F.concat('$INPUT', '!') %}"


def test_function_namespace_is_not_a_root_reference():
    parser = ExpressionParser.for_prefix("$")
    context = {"$NODE": {"a": {"greeting": "{% $F.concat('ab', 'cd') %}"}}}
    assert parser.evaluate("$NODE.a.greeting", context) == "{% $F.concat('ab', 'cd') %}"


def test_dollar_field_segment_is_not_a_root_reference():
    """`$PARENT.$INPUT.name` 的第二段是字段名：容器带 "$PARENT" 键的构造形态
    在旧语义下会解析出 "deep"；数据契约下原样返回。"""
    parser = ExpressionParser.for_prefix("$")
    context = {
        "$NODE": {
            "a": {
                "ref": "$PARENT.$INPUT.name",
                "$PARENT": {"$INPUT": {"name": "deep"}},
            }
        }
    }
    assert parser.evaluate("$NODE.a.ref", context) == "$PARENT.$INPUT.name"


def test_flow_alias_no_longer_triggers_evaluation():
    parser = ExpressionParser.for_prefix("$")
    context = {"$NODE": {"a": {"out": "{% $FLOW %}", "$FLOW_ID": "flow-1"}}}
    assert parser.evaluate("$NODE.a.out", context) == "{% $FLOW %}"


def test_read_style_does_not_change_semantics():
    """同一条数据，路径读与函数读结果一致（Python 旧语义下两者分裂——
    $NODE.x 求值 vs $F.get 原样——是实现残留的指纹，#268 后无此分裂）。"""
    parser = ExpressionParser.for_prefix("$")
    context = {"$NODE": {"x": "$F.add(2,3)"}}
    assert parser.evaluate("$NODE.x", context) == "$F.add(2,3)"
    assert parser.evaluate("$F.get($NODE, 'x')", context) == "$F.add(2,3)"


def test_config_level_evaluation_still_happens():
    """对照锚：表达式写在配置字段（节点 output）里仍被节点求值时就地算掉
    ——只有运行期产出的字符串才是数据。"""
    parser = ExpressionParser.for_prefix("$")
    context = {"$NODE": {"a": {"c": 5}}, "$INPUT": {"x": "$F.add(2,3)"}}
    # $INPUT.x 的值是数据文本 → 原样（运行期数据）
    assert parser.evaluate("$INPUT.x", context) == "$F.add(2,3)"
    # 但把同一个串当配置表达式求值（作者写进节点 output 的形态）→ 求值
    assert parser.evaluate("$F.add(2,3)", context) == 5


def test_defaultdict_parent_is_not_mutated_and_data_is_returned_raw():
    """defaultdict 父容器：读取不触发 `[]` 的自动建键、父容器保持原样。"""
    container: defaultdict = defaultdict(dict)
    container["text"] = "{% $INPUT.secret %}"
    parser = ExpressionParser.for_prefix("$")
    assert parser.evaluate("$NODE.llm.text", {"$NODE": {"llm": container}}) == (
        "{% $INPUT.secret %}"
    )
    assert "$INPUT" not in container          # 无副作用
