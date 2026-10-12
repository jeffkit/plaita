"""回归：$NODE 路径的普通字符串属性值不得被二次解析。

背景（mediaflow 试点实测）：节点输出含 "[promo] ..."、引号、换行等元字符时，
路径求值对属性值做无条件递归解析会把内容当表达式（列表/变量）误解析，
抛出误导性的 KeyError('$NODE')。语义应为：只有 $ 前缀变量 / {% %} 模板
才递归求值（"nested expression strings" 的文档语义）。

plaita#104 追加：即使 attr 确实是表达式/模板形态，递归也只在它引用的上下文根
**全部存在于持有它的父容器**里时才发生（方案 A）；否则它是数据文本，原样返回。
"""
import logging
from collections import defaultdict

import pytest

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


# ---------------------------------------------------------------------------
# plaita#104 —— 方案 A：递归前做根键扫描，只有 attr 引用的上下文根**全部**存在于
# 持有它的父容器里时才递归求值；否则按普通数据文本原样返回。
# ---------------------------------------------------------------------------


def test_data_text_referencing_root_absent_from_parent_returned_raw():
    """#104 验收①：字段是含 {% %} 的数据文本（LLM/agent 输出原文）。

    它引用的 `$INPUT` 只在流程根上下文里、不在父容器 `{"text": ...}` 里——
    修复前会以父容器为 context 递归求值，抛「'$INPUT' not found in expression
    context」的归因错误 KeyError（顶层明明有 $INPUT）。方案 A 下应原样返回。
    """
    parser = ExpressionParser.for_prefix("$")
    text = "see {% $INPUT.secret %} here"
    context = {"$NODE": {"llm": {"text": text}}, "$INPUT": {"secret": "S3CRET"}}
    assert parser.evaluate("$NODE.llm.text", context) == text
    # 对照：同一根键在顶层上下文里确实存在且可正常求值——证明 ① 与顶层缺键无关
    assert parser.evaluate("$INPUT.secret", context) == "S3CRET"


_AC2_XFAIL_REASON = (
    "#104 AC#2 vs the mandated regression: the root scan returns the same empty set for "
    "{% $F.eq(1,1) %} and {% $F.concat('hello ', 'world') %} (neither references a root), so "
    "plan A evaluates both. The two inputs are NOT identical, though — container key "
    "'text' vs 'greeting', and the eq(1,1) body carries no string literal while "
    "concat('hello ', 'world') does — and discriminating heuristics do exist (field-name "
    "data-sink; no-literal/no-root template). They were not adopted because they trade a loud "
    "failure for a silent non-evaluation ({% $F.uuid() %} / {% $F.now() %} would stop "
    "evaluating) — see jeffkit/plaita#130 for the explicit opt-in."
)


def test_pure_function_template_attr_not_reevaluated():
    """#104 验收②——**残留项，未修复**（交由 jeffkit/plaita#130 的显式 opt-in）。

    事实：方案 A 的根键扫描对 `{% $F.eq(1,1) %}`（本例）与回归用例的
    `{% $F.concat('hello ', 'world') %}` 都返回空集（两者都不引用任何根键），
    所以方案 A 下两者都会递归求值。两例的**输入确实不同**（容器键 `text` vs
    `greeting`；模板体有无引号字面量），理论上存在判别信号，但独立验收决定不采用
    基于字段名 / 字面量的启发式——那会把"静默改写"换成"静默不求值"
    （`{% $F.uuid() %}` 这类合法模板会悄悄不再求值），并可能把业务字段名硬编码进
    核心解析器。

    因此本用例在**冲突断言处** xfail（而不是用模块级 xfail 装饰器，避免未来别处的
    失败被静默吞掉）：一旦 #130 落地本用例自动转绿；求值抛别的异常会在断言处真红。
    """
    parser = ExpressionParser.for_prefix("$")
    context = {"$NODE": {"llm": {"text": "{% $F.eq(1,1) %}"}}}
    got = parser.evaluate("$NODE.llm.text", context)
    if got == "True":
        pytest.xfail(_AC2_XFAIL_REASON)
    assert got != "True"


# --- 根键扫描的判据（避免把 $F 等非根引用/字面量误判为根键）-----------------


def test_quoted_dollar_literal_is_not_a_root_reference():
    """`'$INPUT'` 是字符串字面量，不是根引用——扫描剥引号后父容器仍缺根键时，
    模板照旧递归求值（修复前/后行为一致；若扫描把字面量当引用，这里会退化成
    原样返回）。"""
    parser = ExpressionParser.for_prefix("$")
    context = {"$NODE": {"a": {"greeting": "{% $F.concat('$INPUT', '!') %}"}}}
    assert parser.evaluate("$NODE.a.greeting", context) == "$INPUT!"


def test_function_namespace_is_not_a_root_reference():
    """`$F` 是函数命名空间，不是上下文根：父容器里没有任何根键，模板仍递归求值。"""
    parser = ExpressionParser.for_prefix("$")
    context = {"$NODE": {"a": {"greeting": "{% $F.concat('ab', 'cd') %}"}}}
    assert parser.evaluate("$NODE.a.greeting", context) == "abcd"


def test_dollar_field_segment_is_not_a_root_reference():
    """`$PARENT.$INPUT.name` 的第二段是**字段名**而不是根：父容器里有 `$PARENT`
    但没有 `$INPUT`，仍必须递归求值（旧引擎支持的形态，见 changelog）。"""
    parser = ExpressionParser.for_prefix("$")
    context = {
        "$NODE": {
            "a": {
                "ref": "$PARENT.$INPUT.name",
                "$PARENT": {"$INPUT": {"name": "deep"}},
            }
        }
    }
    assert parser.evaluate("$NODE.a.ref", context) == "deep"


def test_flow_alias_resolves_to_flow_id_for_recursion_gate():
    """`$FLOW` 是 `$FLOW_ID` 的别名：父容器提供 `$FLOW_ID` 时应递归求值。"""
    parser = ExpressionParser.for_prefix("$")
    context = {"$NODE": {"a": {"out": "{% $FLOW %}", "$FLOW_ID": "flow-1"}}}
    assert parser.evaluate("$NODE.a.out", context) == "flow-1"


# --- 逃出扫描的 KeyError 仍需归因到嵌套字段 --------------------------------


def test_nested_keyerror_blames_the_nested_field_not_the_flow_root():
    """#104：扫描只认六个文档化根键，未知根（如 `$CUSTOM`）仍会走递归并抛
    KeyError——此时错误信息必须说明「这是嵌套字段的父容器缺键」，而不是让人
    误以为顶层流程上下文缺键（`$CUSTOM` 在顶层上下文里是存在的）。"""
    parser = ExpressionParser.for_prefix("$")
    context = {"$NODE": {"llm": {"text": "{% $CUSTOM.thing %}"}},
               "$CUSTOM": {"thing": 1}}
    with pytest.raises(KeyError) as excinfo:
        parser.evaluate("$NODE.llm.text", context)
    # str(KeyError) 会对 args[0] 再 repr 一次（转义内层引号），取原文更稳
    message = excinfo.value.args[0]
    assert "$NODE.llm.text" in message
    assert "父容器" in message
    assert "'$CUSTOM' not found in expression context" in message
    # 原始错误按原文嵌入，不再被 KeyError.__str__ 再 repr 一层（多一层引号）
    assert '原始错误："' not in message
    # 短字段值原样进摘要（不截断）
    assert repr("{% $CUSTOM.thing %}") in message


def test_defaultdict_parent_is_not_mutated_and_data_is_returned_raw():
    """`defaultdict` 父容器：门只做 `in` 探测，不触发 `[]` 的自动建键。

    基线（无条件递归）会执行 `container["$INPUT"]`，在 `defaultdict(dict)` 上**顺手创建**
    `$INPUT` 并让模板求出 `'None'`；方案 A 改为原文返回，父容器保持原样。这是"从抛错改为
    按数据返回"之外的可见行为变化（已写入 MIGRATION.md）。
    """
    container: defaultdict = defaultdict(dict)
    container["text"] = "{% $INPUT.secret %}"
    parser = ExpressionParser.for_prefix("$")
    assert parser.evaluate("$NODE.llm.text", {"$NODE": {"llm": container}}) == (
        "{% $INPUT.secret %}"
    )
    assert "$INPUT" not in container          # 无副作用：不再自动建键


@pytest.mark.parametrize("total_len", [19, 120, 200])
def test_attribution_preview_truncates_long_field_values(total_len):
    """归因信息里的字段值摘要：< 120 字符原样，>= 120 截断为 117 字符 + "..."。

    LLM/agent 输出原文可能很长，整段进异常会让日志与报错不可读。
    """
    parser = ExpressionParser.for_prefix("$")
    attr = "{% $CUSTOM.thing %}" + "x" * (total_len - 19)
    assert len(attr) == total_len
    context = {"$NODE": {"llm": {"text": attr}}, "$CUSTOM": {"thing": 1}}
    with pytest.raises(KeyError) as excinfo:
        parser.evaluate("$NODE.llm.text", context)
    message = excinfo.value.args[0]
    expected = attr if total_len < 120 else attr[:117] + "..."
    assert repr(expected) in message
    if total_len >= 120:
        assert repr(attr) not in message      # 全量原文绝不进异常


def test_data_text_passthrough_is_traced(caplog):
    """方案 A 的「原样返回」留 DEBUG 轨迹（字段路径 + 缺失根 + 原因）。

    这是迁移期排查「为什么这个字段没被求值」的唯一线索，故内容纳入断言。
    """
    parser = ExpressionParser.for_prefix("$")
    context = {"$NODE": {"llm": {"text": "see {% $INPUT.secret %} here"}},
               "$INPUT": {"secret": "S3CRET"}}
    with caplog.at_level(logging.DEBUG, logger="plaita"):
        assert parser.evaluate("$NODE.llm.text", context) == "see {% $INPUT.secret %} here"
    messages = [record.getMessage() for record in caplog.records]
    assert (
        "nested expression string in $NODE.llm.text references root(s) $INPUT missing "
        "from its parent container; returned AS-IS as data text"
    ) in messages

    # 数据文本落在**中间**段时，轨迹只报路径到该段为止（不含其后段）
    caplog.clear()
    with caplog.at_level(logging.DEBUG, logger="plaita"):
        assert parser.evaluate("$NODE.a.b.c", {"$NODE": {"a": {"b": "{% $INPUT.x %}", "c": 1}}}) is None
    messages = [record.getMessage() for record in caplog.records]
    assert (
        "nested expression string in $NODE.a.b references root(s) $INPUT missing "
        "from its parent container; returned AS-IS as data text"
    ) in messages

    # 多个缺失根按 ", " 连接
    caplog.clear()
    context_multi = {"$NODE": {"a": {"text": "{% $INPUT.x %}{% $NODE.y %}"}}}
    with caplog.at_level(logging.DEBUG, logger="plaita"):
        parser.evaluate("$NODE.a.text", context_multi)
    messages = [record.getMessage() for record in caplog.records]
    assert (
        "nested expression string in $NODE.a.text references root(s) $INPUT, $NODE missing "
        "from its parent container; returned AS-IS as data text"
    ) in messages
