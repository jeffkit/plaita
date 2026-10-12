"""plaita#130：`$NODE.raw.<node>[.<path>...]` 显式 opt-in——字符串永不被再次求值。

`$NODE.<node>.<field>` 对"看起来像表达式/模板"的字符串仍按历史语义递归求值
（#104 方案 A 只是收窄了递归条件，没有取消它）。节点产出的往往就是**数据**
（LLM 输出、文档正文、prompt 回显），而字符串本身看不出"这是数据还是模板"；
本版不改默认语义，只加显式访问器：

* `$NODE.raw.<node>...` —— 整条路径上的字符串**一律不求值**，原样取回
  （#104 的根键扫描与递归都被绕过）；非字符串值与非 raw 路径完全一致；
* `$NODE.<node>...` —— 语义与今天完全一致（文档化：可能求值）。

命名冲突规则：`$NODE` 后第一段**仅当** `$NODE` 容器里没有同名 id、且其后紧跟
一个字段段（节点 id 位）时才被当作修饰符。id 恰为 `raw` 的存量流程因此语义
不变（优先当节点），并对其 `$NODE.raw.<path>` 引用发出 once-per-string WARNING。
"""
import logging
from collections import defaultdict

import pytest

from plaita.core import expression_parser
from plaita.core.expression_parser import ExpressionParser

TEMPLATE = "{% $F.eq(1,1) %}"


# --- 显式取原文 / 默认求值不变 ---------------------------------------------


def test_raw_returns_stored_string_verbatim():
    """#130 验收①：`raw` 取回原文；非 raw 路径仍是文档化的 `"True"`。"""
    parser = ExpressionParser.for_prefix("$")
    context = {"$NODE": {"llm": {"text": TEMPLATE}}}
    assert parser.evaluate("$NODE.raw.llm.text", context) == TEMPLATE
    assert parser.evaluate("$NODE.llm.text", context) == "True"


def test_raw_also_holds_for_side_effect_free_templates_like_now():
    """issue 里点名的另一类：`{% $F.now() %}` 作为数据文本时非 raw 会静默变时间戳。"""
    parser = ExpressionParser.for_prefix("$")
    context = {"$NODE": {"llm": {"text": "{% $F.now() %}"}}}
    assert parser.evaluate("$NODE.raw.llm.text", context) == "{% $F.now() %}"
    assert parser.evaluate("$NODE.llm.text", context) != "{% $F.now() %}"


def test_mandated_regression_still_green_and_raw_shows_the_template():
    """#104 强制回归：`{% $F.concat('hello ', 'world') %}` 非 raw 仍求值。"""
    parser = ExpressionParser.for_prefix("$")
    context = {"$NODE": {"a": {"greeting": "{% $F.concat('hello ', 'world') %}"}}}
    assert parser.evaluate("$NODE.a.greeting", context) == "hello world"
    assert parser.evaluate("$NODE.raw.a.greeting", context) == (
        "{% $F.concat('hello ', 'world') %}"
    )


# --- raw 贯通整条路径 -------------------------------------------------------


def test_raw_whole_node_result_is_returned_as_stored():
    """`$NODE.raw.<node>`：整个节点结果原样（字典里的字符串不求值）。"""
    parser = ExpressionParser.for_prefix("$")
    context = {"$NODE": {"llm": {"text": TEMPLATE, "n": 1}}}
    assert parser.evaluate("$NODE.raw.llm", context) == {"text": TEMPLATE, "n": 1}


def test_raw_whole_node_result_when_node_result_is_a_string():
    """节点结果本身就是模板字符串时，raw 与默认路径的差别最直观。"""
    parser = ExpressionParser.for_prefix("$")
    context = {"$NODE": {"llm": TEMPLATE}}
    assert parser.evaluate("$NODE.raw.llm", context) == TEMPLATE
    assert parser.evaluate("$NODE.llm", context) == "True"


def test_raw_propagates_through_deep_path():
    parser = ExpressionParser.for_prefix("$")
    context = {"$NODE": {"job": {"out": {"a": {"b": TEMPLATE}}}}}
    assert parser.evaluate("$NODE.raw.job.out", context) == {"a": {"b": TEMPLATE}}
    assert parser.evaluate("$NODE.raw.job.out.a.b", context) == TEMPLATE
    assert parser.evaluate("$NODE.job.out.a.b", context) == "True"


def test_raw_bypasses_recursion_at_intermediate_segments():
    """中间段是"父容器兜得住"的嵌套表达式时，默认路径会递归后继续走；raw 不递归。

    `$NODE.a.ref` 的父容器自带 `$INPUT`，所以方案 A 允许递归：中间段 `ref` 本身
    就被求值成 `{"t": "deep"}`，`.t` 再作用其上。raw 模式下 `ref` 仍是原串——
    判别力来自 `$NODE.raw.a.ref`（base 上为 `None`），而不是 `.t` 的 `None`。
    """
    parser = ExpressionParser.for_prefix("$")
    context = {"$NODE": {"a": {"ref": "$INPUT.sub", "$INPUT": {"sub": {"t": "deep"}}}}}
    # 非 raw：中间段被求值
    assert parser.evaluate("$NODE.a.ref", context) == {"t": "deep"}
    assert parser.evaluate("$NODE.a.ref.t", context) == "deep"
    # raw：中间段不求值 ⇒ 取回原串；`.t` 自然取不到
    assert parser.evaluate("$NODE.raw.a.ref", context) == "$INPUT.sub"
    assert parser.evaluate("$NODE.raw.a.ref.t", context) is None


@pytest.mark.parametrize(
    "value, expected",
    [
        (3, 3),
        (1.5, 1.5),
        (True, True),
        (None, None),
        ([TEMPLATE, 2], [TEMPLATE, 2]),
        ({"k": TEMPLATE}, {"k": TEMPLATE}),
    ],
    ids=["int", "float", "bool", "none", "list", "dict"],
)
def test_raw_returns_non_string_leaves_exactly_like_default(value, expected):
    parser = ExpressionParser.for_prefix("$")
    context = {"$NODE": {"job": {"v": value}}}
    assert parser.evaluate("$NODE.raw.job.v", context) == expected
    assert parser.evaluate("$NODE.job.v", context) == expected


def test_raw_index_segments_still_index_without_recursion():
    parser = ExpressionParser.for_prefix("$")
    context = {"$NODE": {"job": {"out": {"items": [TEMPLATE, "tail"]}}}}
    assert parser.evaluate("$NODE.raw.job.out.items.0", context) == TEMPLATE
    assert parser.evaluate("$NODE.raw.job.out.items[1]", context) == "tail"


# --- 缺失路径 / 缺失根：错误文案与归因保持今天的样子 -----------------------


def test_missing_node_id_yields_none_for_both_forms(caplog):
    parser = ExpressionParser.for_prefix("$")
    context = {"$NODE": {"llm": {"text": TEMPLATE}}}
    with caplog.at_level(logging.WARNING, logger="plaita"):
        assert parser.evaluate("$NODE.raw.nope", context) is None
        assert parser.evaluate("$NODE.nope", context) is None
        assert parser.evaluate("$NODE.raw.nope.x", context) is None
        assert parser.evaluate("$NODE.nope.x", context) is None
    # 缺节点不是"冲突"，不产生任何新告警
    assert [r for r in caplog.records if r.levelno == logging.WARNING] == []


def test_missing_node_root_keeps_identical_keyerror_for_both_forms():
    """整个 `$NODE` 根不存在时保留今天的 KeyError（含可用根清单），两形态同文案。"""
    parser = ExpressionParser.for_prefix("$")
    with pytest.raises(KeyError) as plain:
        parser.evaluate("$NODE.nope.x", {})
    with pytest.raises(KeyError) as raw:
        parser.evaluate("$NODE.raw.nope.x", {})
    assert raw.value.args[0] == plain.value.args[0]
    assert "'$NODE' not found in expression context" in plain.value.args[0]


def test_raw_bypasses_the_nested_attribution_keyerror():
    """非 raw 对未知根 `$CUSTOM` 抛归因 KeyError；raw 只取原文，不抛也不新增诊断。"""
    parser = ExpressionParser.for_prefix("$")
    context = {"$NODE": {"llm": {"text": "{% $CUSTOM.thing %}"}}, "$CUSTOM": {"thing": 1}}
    with pytest.raises(KeyError) as excinfo:
        parser.evaluate("$NODE.llm.text", context)
    assert "$NODE.llm.text" in excinfo.value.args[0]
    assert parser.evaluate("$NODE.raw.llm.text", context) == "{% $CUSTOM.thing %}"


# --- id 恰为 `raw` 的命名冲突 ----------------------------------------------


def test_node_named_raw_wins_and_warns_once_per_string(monkeypatch, caplog):
    """冲突规则：节点优先（存量语义不变）+ once-per-string WARNING。"""
    monkeypatch.setattr(expression_parser, "_warned_raw_collisions", set())
    parser = ExpressionParser.for_prefix("$")
    context = {"$NODE": {"raw": {"x": TEMPLATE, "y": "{% $F.eq(2,2) %}"}}}
    with caplog.at_level(logging.WARNING, logger="plaita"):
        # 同一个字符串求值两次：只告警一次
        assert parser.evaluate("$NODE.raw.x", context) == "True"
        assert parser.evaluate("$NODE.raw.x", context) == "True"
        # 裸 `$NODE.raw`（无后续字段段）就是节点本身，不构成访问器冲突
        assert parser.evaluate("$NODE.raw", context) == {"x": TEMPLATE, "y": "{% $F.eq(2,2) %}"}
        # 另一个字符串：各自告警一次
        assert parser.evaluate("$NODE.raw.y", context) == "True"
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 2
    # 全文案断言（与 #104 对其新增 DEBUG 轨迹的做法一致）：文案本身就是"冲突不可
    # 静默"的对外承诺，参数/大小写/措辞被改都必须红。
    assert warnings[0] == (
        "expression $NODE.raw.x: the segment after $NODE is 'raw', but this flow also "
        "has a node/key with id 'raw' — it is resolved as that node (backwards "
        "compatibility), so the verbatim '$NODE.raw.<id>...' accessor (#130) is NOT "
        "available in this flow; rename the node to use the accessor."
    )
    assert warnings[1] == warnings[0].replace("$NODE.raw.x", "$NODE.raw.y")


def test_raw_then_index_is_not_the_modifier():
    """节点 id 位必须是字段段：`$NODE.raw.0` / `$NODE.raw[1]` 仍是节点 `raw`。"""
    parser = ExpressionParser.for_prefix("$")
    context = {"$NODE": {"raw": [10, 20], "0": "container-item"}}
    assert parser.evaluate("$NODE.raw.0", context) == 10
    assert parser.evaluate("$NODE.raw[1]", context) == 20


def test_raw_modifier_probe_does_not_create_keys_on_defaultdict():
    """与 #104 同纪律：冲突探测只做 `in`，不触发 `defaultdict.__missing__`。"""
    container: defaultdict = defaultdict(dict)
    container["llm"] = {"text": TEMPLATE}
    parser = ExpressionParser.for_prefix("$")
    assert parser.evaluate("$NODE.raw.llm.text", {"$NODE": container}) == TEMPLATE
    assert "raw" not in container


def test_no_collision_warning_when_no_node_named_raw(monkeypatch, caplog):
    monkeypatch.setattr(expression_parser, "_warned_raw_collisions", set())
    parser = ExpressionParser.for_prefix("$")
    context = {"$NODE": {"llm": {"text": TEMPLATE}}}
    with caplog.at_level(logging.WARNING, logger="plaita"):
        assert parser.evaluate("$NODE.raw.llm.text", context) == TEMPLATE
    assert [r for r in caplog.records if r.levelno == logging.WARNING] == []


def test_runtime_window_node_raw_declared_but_not_yet_produced(monkeypatch, caplog):
    """运行期窗口（显式文档化行为）：flow 声明了节点 `raw`、但它还没产出时，`$NODE`
    容器里没有该键，同一表达式按**访问器**解析（取原文、无 WARNING）；节点产出后
    翻转为节点语义（+1 条 once-per-string WARNING）。

    修饰符判定必须发生在运行期（要看当次 `$NODE` 容器），因此这个窗口是规则的
    必然结果而非暗坑——它不会改变既有流程的结果（两态在 base 上都是 `None`）。
    """
    monkeypatch.setattr(expression_parser, "_warned_raw_collisions", set())
    parser = ExpressionParser.for_prefix("$")
    declared_not_produced = {"$NODE": {"other": {"text": TEMPLATE}}}
    with caplog.at_level(logging.WARNING, logger="plaita"):
        # 窗口内：访问器语义——取原文，且不误报冲突
        assert parser.evaluate("$NODE.raw.other.text", declared_not_produced) == TEMPLATE
        assert [r for r in caplog.records if r.levelno == logging.WARNING] == []
        caplog.clear()
        # 节点 `raw` 产出后：同一表达式变节点语义（`text` 被求值）+ 1 条 WARNING
        produced = {
            "$NODE": {
                "raw": {"other": {"text": TEMPLATE}},
                "other": {"text": TEMPLATE},
            }
        }
        assert parser.evaluate("$NODE.raw.other.text", produced) == "True"
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert warnings[0].startswith("expression $NODE.raw.other.text: ")


def test_collision_dedupe_reset_threshold_is_exact(monkeypatch, caplog):
    """进程级去重集合的 512 阈值：白盒锁定 `len(_warned_raw_collisions) > 512`。

    与 `_warn_suspicious_literal` / `_warn_unmatched_template` 同形（>512 清零 +
    同串去重），但那两处的阈值从未被断言，`> 512` → `>= 512` / `> 513` 两个**非等价**
    变异因此长期幸存。这里显式锁边界：恰好 512 个已记路径时**不得**清零（否则记过的
    路径会二次告警），第 514 次调用时**必须**清零（否则集合无界增长）。

    白盒用例用 monkeypatch 换掉进程级集合，测完自动还原，不污染同进程其它用例。
    """
    monkeypatch.setattr(
        expression_parser, "_warned_raw_collisions", {f"p{i}" for i in range(512)}
    )
    with caplog.at_level(logging.WARNING, logger="plaita"):
        expression_parser._warn_raw_accessor_collision("Q")
        expression_parser._warn_raw_accessor_collision("p0")
        # 恰好 512 条时不清零：`p0` 仍被去重 ⇒ 只有 `Q` 一条
        assert len(caplog.records) == 1
        expression_parser._warn_raw_accessor_collision("R")   # 513 > 512 ⇒ 清零
        expression_parser._warn_raw_accessor_collision("p0")  # 清零后不再被去重
        assert len(caplog.records) == 3


# --- 其他根与原样语义 -------------------------------------------------------


@pytest.mark.parametrize("root", ["$INPUT", "$GLOBAL", "$PARENT", "$ENV"])
def test_raw_is_not_a_modifier_outside_the_node_root(root):
    """`raw` 只在 `$NODE` 后有修饰符含义；其他根的同名键照旧。"""
    parser = ExpressionParser.for_prefix("$")
    context = {root: {"raw": {"text": TEMPLATE}}}
    assert parser.evaluate(f"{root}.raw.text", context) == "True"


def test_raw_accessor_follows_a_custom_prefix():
    parser = ExpressionParser.for_prefix("@")
    template = "{% @F.eq(1,1) %}"
    context = {"@NODE": {"llm": {"text": template}}}
    assert parser.evaluate("@NODE.raw.llm.text", context) == template
    assert parser.evaluate("@NODE.llm.text", context) == "True"
